"""调度方案计算（纯函数式：输入当前状态，输出可解释方案）。

可解释性要求：每个动作都带
- readings: 实际采用的读数（值、质量、来源 telemetry/manual、来源时间、采用/弃用原因）
- reasons: 规则码与中文说明
- threshold_checks: 对照污染阈值与河道水位的逐项校核
- conflicts: 阻止或暂缓该口动作的冲突及其来源消息
"""

from dataclasses import dataclass, field
from datetime import timedelta

from . import ingest, reference, timeutil

# 由雨强触发预警的缺省阈值（mm/h）
RAIN_WARNING_RULES = [
    (60.0, "红色"), (40.0, "橙色"), (25.0, "黄色"), (10.0, "蓝色"),
]

# 方案预估的溢流持续时长（小时）：仅用于污染负荷与影响范围保守估算
PLAN_HORIZON_H = 1.0


class PlanningError(ValueError):
    pass


@dataclass
class Reading:
    sensor_id: str
    kind: str
    adopted: bool
    source: str                  # telemetry / manual / none
    quality: str
    value: object = None
    unit: str = None
    source_time: str = None
    reviewer: str = None
    note: str = None

    def as_doc(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Planner:
    store: object
    at: object = None
    readings: list = field(default_factory=list)

    def resolve(self, sensor_id):
        """返回 (数值或状态字典, Reading)。

        采用顺序：
        1) 有效的人工复核读数（不早于当前遥测来源时间，或遥测不可用时）；
        2) GOOD 且未超时的遥测；
        3) 缺测/失联 → None，质量 UNCONFIRMED，方案不得按零值继续。
        """
        sensor = reference.SENSORS[sensor_id]
        doc = self.store.state["measurements"].get(sensor_id)
        review = None
        outfall_id = sensor.get("outfall")
        if outfall_id:
            review = self.store.state.get("reviews", {}).get(outfall_id)

        telemetry = ingest._public_measurement(self.store, sensor_id, self.at) \
            if doc else {"quality": ingest.QUALITY_MISSING, "usable": False}
        manual = self._manual_override(sensor_id, review, telemetry)

        if manual is not None:
            value, quality, source_time, reviewer, note = manual
            r = Reading(sensor_id, sensor["kind"], True, "manual", "GOOD",
                        value, self._unit(sensor, value), source_time,
                        reviewer, note)
            self.readings.append(r)
            return value, r

        if telemetry.get("usable"):
            if sensor["kind"] == "gate":
                value = {"mode": doc["mode"], "opening_pct": doc.get("opening_pct")}
            else:
                value = doc["value"]
            r = Reading(sensor_id, sensor["kind"], True, "telemetry",
                        telemetry["quality"], value, doc.get("unit"),
                        doc["source_time"],
                        note=f"接收于 {doc['received_at']}，滞后 {telemetry['age_seconds']}s")
            self.readings.append(r)
            return value, r

        note = "传感器失联或数据不良，已降级为待确认，禁止按零值参与调度"
        r = Reading(sensor_id, sensor["kind"], False, "none",
                    telemetry.get("quality", ingest.QUALITY_MISSING),
                    note=note)
        self.readings.append(r)
        return None, r

    def _manual_override(self, sensor_id, review, telemetry):
        if not review:
            return None
        item = (review.get("readings") or {}).get(sensor_id)
        if not item:
            return None
        # 遥测良好且比人工读数新时，继续采用遥测，但人工读数仍在方案中展示
        if telemetry.get("usable"):
            doc = self.store.state["measurements"][sensor_id]
            if timeutil.parse(doc["source_time"]) >= timeutil.parse(item["source_time"]):
                return None
        kind = reference.SENSORS[sensor_id]["kind"]
        value = item["mode"] if kind == "gate" else float(item["value"])
        return (value, "GOOD", item["source_time"],
                review.get("reviewer"), item.get("note"))

    @staticmethod
    def _unit(sensor, value):
        return {"rain": "mm/h", "level": "m",
                "river_stage": "m", "gate": None}.get(sensor["kind"])


def plan_district(store, district: str, *, at=None):
    at = at or timeutil.now()
    if district not in reference.DISTRICTS:
        raise PlanningError(f"未知分区: {district}")
    p = Planner(store, at)
    dconf = reference.DISTRICTS[district]
    outfall_ids = dconf["outfalls"]
    river_name = dconf["river"]

    rain_sensor = next(sid for sid, s in reference.SENSORS.items()
                       if s["kind"] == "rain" and s["district"] == district)
    river_sensor = next(sid for sid, s in reference.SENSORS.items()
                        if s["kind"] == "river_stage" and s.get("river") == river_name)

    rain_i, rain_r = p.resolve(rain_sensor)
    stage, river_r = p.resolve(river_sensor)
    levels, gates = {}, {}
    for oid in outfall_ids:
        gate_id = reference.OUTFALLS[oid]["gate"]["gate_id"]
        lv_sensor = next(sid for sid, s in reference.SENSORS.items()
                         if s["kind"] == "level" and s.get("outfall") == oid)
        levels[oid] = p.resolve(lv_sensor)
        gates[oid] = p.resolve(gate_id)

    rain_event_id = store.state["district_open_event"].get(district)
    rain_event = store.state["rain_events"].get(rain_event_id) if rain_event_id else None

    river_bucket = _river_bucket(river_name, stage)
    unconfirmed = [r.sensor_id for r in p.readings if not r.adopted]

    # 分区水平衡
    balance = _balance(dconf, outfall_ids, rain_i, levels)
    warning = _warning_level(store, district, rain_i, river_bucket, balance, at)

    open_conflicts = {
        oid: _conflict_view(store, oid)
        for oid in outfall_ids
        if _conflict_view(store, oid)
    }
    takeovers = {oid: store.state["takeovers"][oid]
                 for oid in outfall_ids if oid in store.state["takeovers"]}

    actions = []
    for oid in outfall_ids:
        actions.append(_decide_outfall(
            oid, levels[oid][0], gates[oid][0], rain_i, river_bucket,
            balance, warning, open_conflicts.get(oid), takeovers.get(oid)))

    pollution = _pollution_summary(outfall_ids, actions, rain_i)
    impact_zone = _impact_zone(river_name, stage, pollution, outfall_ids, actions)
    notifications = _notifications(district, warning, pollution, takeovers, at)

    plan = {
        "district": district,
        "basis_time": timeutil.iso(at),
        "warning_level": warning["level"],
        "warning_source": warning["source"],
        "warning_policy": warning["policy"],
        "rain_event_id": rain_event_id,
        "rain_event_cross_midnight": rain_event.get("cross_midnight") if rain_event else None,
        "river": {"name": river_name, "stage_m": stage,
                  "bucket": river_bucket[0], **river_bucket[1]},
        "balance": balance,
        "readings": [r.as_doc() for r in p.readings],
        "unconfirmed_sensors": unconfirmed,
        "conflicts": list(open_conflicts.values()),
        "takeovers": list(takeovers.values()),
        "actions": actions,
        "pollution": pollution,
        "impact_zone": impact_zone,
        "notifications": notifications,
        "assumptions": [
            f"溢流污染负荷按 {PLAN_HORIZON_H:.0f} 小时预估时长保守估算",
            "入流同时采用雨量径流公式与液位反推，取两者较大值",
            "污染浓度采用雨强稀释经验式 EMC=基准/(1+0.04×雨强)",
            "影响范围为水动力简化估算，须以现场监测为准",
        ],
    }
    return plan


# ---------- 水位与水平衡 ----------
def _river_bucket(river_name, stage):
    levels = reference.RIVER_STAGES[river_name]
    if stage is None:
        return "unknown", levels
    if stage >= levels["top_bank"]:
        bucket = "top"
    elif stage >= levels["guarantee_stage"]:
        bucket = "guarantee"
    elif stage >= levels["warning_stage"]:
        bucket = "warning"
    else:
        bucket = "normal"
    return bucket, levels


def _occupancy(outfall_id, level):
    o = reference.OUTFALLS[outfall_id]
    if level is None:
        return None
    span = o["critical_level_m"] - o["dry_level_m"]
    return max(0.0, min(1.0, (level - o["dry_level_m"]) / span))


def _balance(dconf, outfall_ids, rain_i, levels):
    rows = []
    inflow_total = 0.0
    weighted_occ, weight_sum = 0.0, 0.0
    for oid in outfall_ids:
        o = reference.OUTFALLS[oid]
        level = levels[oid][0]
        q_runoff = (o["catchment_ha"] * 10.0 * (rain_i or 0.0)
                    * o["runoff_coef"])
        q_level = (max(0.0, level - o["dry_level_m"]) * reference.LEVEL_TO_INFLOW
                   if level is not None else None)
        q_in = o["dwf_m3h"]
        if q_level is not None:
            q_in += max(q_runoff, q_level)
        elif rain_i is not None:
            q_in += q_runoff
        else:
            q_in = None
        occ = _occupancy(oid, level)
        rows.append({"outfall_id": oid, "q_runoff_m3h": _r(q_runoff),
                     "q_level_m3h": _r(q_level) if q_level is not None else None,
                     "q_inflow_m3h": _r(q_in) if q_in is not None else None,
                     "occupancy": _r(occ) if occ is not None else None,
                     "pump_capacity_m3h": o["pump"]["capacity_m3h"]})
        if q_in is not None:
            inflow_total += q_in
        if occ is not None:
            weighted_occ += occ * o["catchment_ha"]
            weight_sum += o["catchment_ha"]

    pump_total = sum(reference.OUTFALLS[oid]["pump"]["capacity_m3h"]
                     for oid in outfall_ids)
    treatment = dconf["treatment_inflow_m3h"]
    known = all(levels[oid][0] is not None or rain_i is not None
                for oid in outfall_ids)
    net = inflow_total - pump_total - treatment if known else None
    occ_mean = weighted_occ / weight_sum if weight_sum else None
    free_storage = (dconf["storage_m3"] * (1 - occ_mean)
                    if occ_mean is not None else None)
    ttf = (free_storage / net if net is not None and net > 0
           and free_storage is not None else None)
    return {"rows": rows, "inflow_m3h": _r(inflow_total) if known else None,
            "pump_capacity_m3h": pump_total, "treatment_inflow_m3h": treatment,
            "net_m3h": _r(net) if net is not None else None,
            "storage_occupancy": _r(occ_mean) if occ_mean is not None else None,
            "free_storage_m3": _r(free_storage) if free_storage is not None else None,
            "time_to_fill_h": _r(ttf, 3) if ttf is not None else None,
            "complete": known}


def _warning_level(store, district, rain_i, river_bucket, balance, at):
    active = store.state.get("warnings", {}).get(district)
    rank = 0
    derived = "蓝色"
    if rain_i is not None:
        for threshold, level in RAIN_WARNING_RULES:
            if rain_i >= threshold:
                derived = level
                rank = reference.WARNING_LEVELS[level]["rank"]
                break
    river_rank = {"top": 4, "guarantee": 3, "warning": 2}.get(river_bucket[0], 0)
    ttf = balance.get("time_to_fill_h")
    storage_rank = 0
    if ttf is not None:
        storage_rank = 4 if ttf < 0.5 else 3 if ttf < 1 else 2 if ttf < 2 else 0
    rank = max(rank, river_rank, storage_rank)
    suggested = next((name for name, cfg in reference.WARNING_LEVELS.items()
                      if cfg["rank"] == rank), None)
    if active and active.get("active"):
        name = active["level"]
        return {"level": name, "rank": reference.WARNING_LEVELS[name]["rank"],
                "source": f"人工发布({active.get('source')})",
                "policy": reference.WARNING_LEVELS[name]["policy"],
                "suggested": suggested}
    name = suggested
    return {"level": name, "rank": rank,
            "source": "系统依据雨强/河道水位/调蓄余量自动研判",
            "policy": reference.WARNING_LEVELS[name]["policy"] if name else None,
            "suggested": suggested}


# ---------- 单口决策 ----------
def _decide_outfall(oid, level, gate_report, rain_i, river_bucket,
                    balance, warning, conflict, takeover):
    info = reference.OUTFALLS[oid]
    reasons, blocks, checks = [], [], []
    pump_cmd = {"pump_id": info["pump"]["pump_id"], "command": "HOLD",
                "setpoint_m3h": None}
    gate_cmd = {"gate_id": info["gate"]["gate_id"], "command": "HOLD",
                "mode": None, "opening_pct": None}

    data_ok = level is not None
    if not data_ok:
        reasons.append("E-DATA-UNCONFIRMED: 液位待确认，禁止按零值假设水位安全")
        blocks.append("level_unconfirmed")
    if river_bucket[0] == "unknown":
        reasons.append("E-RIVER-UNCONFIRMED: 河道水位待确认，不允许新增向河闸门动作")
        blocks.append("river_unconfirmed")
    if conflict:
        reasons.append(f"E-GATE-CONFLICT: 闸位冲突 {conflict['id']} 未消解，"
                       f"遥测={conflict['reported_mode']} "
                       f"应为={conflict['expected_mode']}（依据{conflict['expected_basis']}）")
        blocks.append("gate_conflict")
    if takeover:
        reasons.append(f"E-MANUAL-TAKEOVER: 已由 {takeover['actor']} 人工接管，"
                       "系统不得自动改闸")
        blocks.append("manual_takeover")

    # 泵：液位可信且偏高即抢排（进厂/调蓄），不受闸门冲突影响
    if level is not None and level >= info["alarm_level_m"]:
        pump_cmd.update(command="RUN_MAX",
                        setpoint_m3h=info["pump"]["capacity_m3h"])
        reasons.append("R-PUMP-MAX: 液位达到警戒线，泵组满负荷抢排")
    elif level is not None:
        row = next(r for r in balance["rows"] if r["outfall_id"] == oid)
        setpoint = min(info["pump"]["capacity_m3h"],
                       max(info["pump"]["min_run_m3h"],
                           row["q_inflow_m3h"] or 0.0))
        pump_cmd.update(command="RUN_AUTO", setpoint_m3h=_r(setpoint))
        reasons.append("R-PUMP-AUTO: 液位可控，按入流匹配抽排进厂")

    desired_mode, desired_pct = "CLOSED", None
    overflow = False
    emergency = False
    if data_ok and level >= info["critical_level_m"]:
        reasons.append("R-LEVEL-CRITICAL: 液位达到危急值，仅靠抽排已不能保管网安全")
        overflow = True
    elif data_ok and level >= info["alarm_level_m"] and \
            balance.get("time_to_fill_h") is not None and \
            balance["time_to_fill_h"] < 1.0:
        reasons.append("R-STORAGE-NEAR-FULL: 警戒线以上且调蓄余量不足 1 小时，需受控泄放")
        overflow = True

    bucket = river_bucket[0]
    if overflow:
        if bucket == "top":
            desired_mode, desired_pct = "CLOSED", None
            reasons.append("E-RIVER-TOP: 河道水位达到堤顶，开闸将倒灌，禁止溢流；"
                           "立即升级红色响应并请求上游错峰")
            blocks.append("river_top")
            overflow = False
        elif bucket == "guarantee":
            if warning["rank"] >= 4 and balance.get("net_m3h", 0) and \
                    balance["net_m3h"] > 0:
                desired_mode, desired_pct, emergency = "REGULATED", 40.0, True
                reasons.append("R-EMERGENCY-OVERFLOW: 保证水位下仅在红色响应、"
                               "管网净入流仍为正时应急有限泄放")
            else:
                desired_mode = "CLOSED"
                reasons.append("E-RIVER-GUARANTEE: 河道保证水位，非红色应急不得溢流")
                blocks.append("river_guarantee")
                overflow = False
        else:
            desired_mode, desired_pct = "REGULATED", _opening_pct(info, level)
            reasons.append("R-REGULATED-RELEASE: 受控开度泄放，削减管网峰值")
            if bucket == "warning":
                reasons.append("W-RIVER-WARNING: 河道已在警戒水位，溢流须同步上报污染影响")
        if "REGULATED" not in info["gate"]["capable_modes"] and desired_mode == "REGULATED":
            desired_mode = "OPEN"
            reasons.append("W-GATE-NO-REGULATION: 该闸无调节能力，以全开替代受控开度")

    checks.append({"check": "river_stage", "bucket": bucket,
                   "result": "block" if bucket == "top" else
                             "restricted" if bucket in ("guarantee", "warning") else "ok"})

    executable = not blocks
    if executable and overflow:
        gate_cmd.update(command="SET", mode=desired_mode, opening_pct=desired_pct)
    elif executable:
        gate_cmd.update(command="SET", mode="CLOSED", opening_pct=0.0)
        if not any(r.startswith(("R-LEVEL", "R-STORAGE")) for r in reasons):
            reasons.append("R-NORMAL: 维持落闸截污常态")
    else:
        gate_cmd.update(command="HOLD")

    q_overflow = 0.0
    if overflow and executable and desired_mode in ("REGULATED", "OPEN"):
        net = balance.get("net_m3h")
        q_overflow = _r(max(0.0, (net or 0.0)) *
                        info["catchment_ha"] /
                        sum(reference.OUTFALLS[r["outfall_id"]]["catchment_ha"]
                            for r in balance["rows"]))

    return {"outfall_id": oid, "gate": gate_cmd, "pump": pump_cmd,
            "executable": executable, "blocking_reasons": blocks,
            "reasons": reasons, "threshold_checks": checks,
            "overflow": overflow and executable,
            "emergency": emergency,
            "estimated_overflow_m3h": q_overflow if overflow and executable else 0.0,
            "reported_gate_mode": gate_report["mode"] if gate_report else None}


def _opening_pct(info, level):
    span = info["critical_level_m"] - info["alarm_level_m"]
    frac = max(0.0, min(1.0, (level - info["alarm_level_m"]) / span))
    return _r(30.0 + 50.0 * frac)


# ---------- 污染与影响 ----------
def _emc(base, rain_i):
    return {k: _r(v / (1.0 + 0.04 * (rain_i or 0.0))) for k, v in base.items()}


def _pollution_summary(outfall_ids, actions, rain_i):
    items, flags = [], []
    total_cod_load = 0.0
    for action in actions:
        oid = action["outfall_id"]
        q = action["estimated_overflow_m3h"]
        conc = _emc(reference.OUTFALLS[oid]["baseline_quality"], rain_i)
        cod_load = conc["COD_mg_L"] * q * PLAN_HORIZON_H / 1000.0
        total_cod_load += cod_load
        exceed = [k for k in ("COD_mg_L", "NH3N_mg_L", "TP_mg_L")
                  if conc[k] > reference.POLLUTION_THRESHOLDS[k]]
        report = cod_load >= reference.POLLUTION_THRESHOLDS["COD_load_kg_report"]
        if q > 0:
            items.append({"outfall_id": oid, "concentration_mg_L": conc,
                          "overflow_m3h": q,
                          f"cod_load_kg_{PLAN_HORIZON_H:.0f}h": _r(cod_load),
                          "threshold_exceeded": exceed,
                          "must_report_regulator": report})
            if exceed:
                flags.append(f"{oid} 溢流浓度超阈值: {','.join(exceed)}")
            if report:
                flags.append(f"{oid} COD 负荷达到监管上报阈值")
    return {"items": items, "total_cod_load_kg": _r(total_cod_load),
            "flags": flags, "report_regulator": any(i["must_report_regulator"]
                                                    for i in items)}


def _impact_zone(river_name, stage, pollution, outfall_ids, actions):
    active = [a["outfall_id"] for a in actions if a["overflow"]]
    if not active:
        return {"river": river_name, "overflow_active": False}
    load = pollution["total_cod_load_kg"]
    stage_for_dilution = stage if stage else reference.RIVER_STAGES[river_name]["top_bank"]
    reach_km = _r(min(8.0, max(0.5, 0.6 + load / 120.0
                               * (3.5 / max(1.0, stage_for_dilution)))), 2)
    sensitive = []
    for oid in active:
        o = reference.OUTFALLS[oid]
        if o["downstream_sensitive_km"] <= reach_km:
            sensitive.append({"outfall_id": oid,
                              "target": "下游敏感目标",
                              "distance_km": o["downstream_sensitive_km"]})
    return {"river": river_name, "overflow_active": True,
            "plume_reach_km": reach_km, "method": "简化估算，须以现场监测为准",
            "sensitive_targets_in_reach": sensitive,
            "affected_outfalls": active}


def _notifications(district, warning, pollution, takeovers, at):
    minutes = reference.WARNING_LEVELS[warning["level"]]["response_deadline_minutes"] \
        if warning["level"] else 120
    deadline = timeutil.iso(at + timedelta(minutes=minutes))
    duty = reference.DUTY_OFFICERS[district]
    notes = [
        {"target": f"{district}值班调度员({duty['name']})",
         "reason": f"{warning['level'] or '无'}预警响应确认",
         "deadline_minutes": minutes},
        {"target": "沿线泵站/闸门现场班组",
         "reason": "方案动作现场确认与回执", "deadline_minutes": minutes},
    ]
    if pollution["report_regulator"]:
        notes.append({"target": "区生态环境局监管值班",
                      "reason": "溢流 COD 负荷超上报阈值，报送污染影响范围",
                      "deadline_minutes": minutes})
    for oid, t in takeovers.items():
        notes.append({"target": f"{oid} 接管人 {t['actor']}",
                      "reason": "人工接管口的动作须由接管人回执",
                      "deadline_minutes": minutes})
    for n in notes:
        n["deadline_at"] = deadline
        n["status"] = "pending"
    return notes


def _r(value, nd=1):
    if value is None:
        return None
    return round(float(value), nd)


def _conflict_view(store, outfall_id):
    return next((c for c in store.state["conflicts"].values()
                 if c["outfall_id"] == outfall_id and c["status"] == "open"),
                None)
