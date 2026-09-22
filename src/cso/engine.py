"""调度规则引擎（纯函数，不写状态）。

输入：分区调蓄容量、泵闸能力、污染阈值、预警等级、河道水位与各类传感器工况。
输出：结构化、可解释的调度方案——每个动作都带采用读数、依据规则、冲突来源、
责任人、通知时限与河道影响范围。引擎只读状态，便于回放与单元测试。
"""
from __future__ import annotations

from datetime import timedelta

from .sensors import evaluate_gate, evaluate_station
from .timeutils import format_iso, now_utc, parse_iso

DUTY_DISPATCHER = {"id": "duty-dispatcher", "name": "值班调度员", "role": "调度员"}
FIELD_CREW = {"id": "field-crew", "name": "现场运维班组", "role": "运维人员"}

LEVEL_HIGH_RATIO = 0.85        # 集水井高液位
LEVEL_EMERGENCY_RATIO = 0.95   # 城区内涝风险临界
RIVER_CAUTION_FACTOR = 0.5     # 河道水位位于警戒区间时的泄放折扣
GATE_INTERVAL_S = 60           # 闸门 PLC 遥测周期（reference 未单列时的保守默认）


def _warning_level(config, rain_1h_mm):
    if rain_1h_mm is None:
        return {"level": "unknown", "name_cn": "未知", "rain_1h_mm_gte": None}
    chosen = {"level": "none", "name_cn": "无预警", "rain_1h_mm_gte": 0}
    for w in config.warning_levels:
        if rain_1h_mm >= w["rain_1h_mm_gte"]:
            chosen = w
    return chosen


def _rain_1h(state, station_id, as_of):
    """最近 1 小时雨量帧合计；缺帧返回 None（不补零）。"""
    frames_map = state["rain_frames"].get(station_id, {})
    window_start = as_of - timedelta(hours=1)
    used, total = [], 0.0
    for observed_at, frame in sorted(frames_map.items()):
        t = parse_iso(observed_at)
        if window_start < t <= as_of:
            total += frame["rain_mm"]
            used.append({"observed_at": observed_at, "rain_mm": frame["rain_mm"],
                         "quality": frame.get("quality", "ok")})
    return (total if used else None), used


def _current_storm(state, station_id):
    ids = state.get("storm_station", {}).get(station_id, [])
    return state["storms"][ids[-1]] if ids else None


def _takeover_active(state, scope, scope_id, as_of):
    rec = state["takeovers"].get(f"{scope}:{scope_id}")
    if not rec or not rec.get("active"):
        return None
    if rec.get("until") and parse_iso(rec["until"]) <= as_of:
        return None
    return rec


def _reading_brief(meta, evaluation):
    if not meta:
        return {"station_id": None, "status": "not_configured"}
    tel = evaluation["telemetry"] or {}
    man = evaluation["manual"] or {}
    return {
        "station_id": meta["id"], "name_cn": meta.get("name_cn"),
        "adopted_value": evaluation["adopted_value"],
        "basis": evaluation["basis"], "status": evaluation["status"],
        "unit": evaluation["telemetry"].get("unit") if evaluation["telemetry"] else meta.get("unit"),
        "telemetry_value": tel.get("value"), "telemetry_observed_at": tel.get("observed_at"),
        "manual_value": man.get("value"), "manual_observed_at": man.get("observed_at"),
        "reviewer": man.get("reviewer"),
        "age_seconds": evaluation["age_seconds"],
        "stale_after_seconds": evaluation["stale_after_seconds"],
    }


def build_plan(state, config, as_of=None):
    as_of = as_of or now_utc()
    p = config.params
    horizon_s = p["horizon_seconds"]
    stale_mult = p["sensor_stale_multiplier"]
    gate_tol = p["gate_conflict_tolerance_percent"]
    level_tol = p["level_conflict_tolerance_m"]
    quality_tol = p["quality_conflict_tolerance_mg_l"]
    threshold = config.thresholds["cso_pollution_mg_l"]

    seq_no = state["counters"].get("plan", 0) + 1
    plan_id = f"plan-{seq_no:04d}"
    ctx = {"n": 0}

    def next_action_id():
        ctx["n"] += 1
        return f"act-{plan_id}-{ctx['n']:02d}"

    ntf_ctx = {"n": 0}

    def make_notification(kind, role_cn, summary, warning_level):
        ntf_ctx["n"] += 1
        sla_map = p["notify_sla_minutes"]
        # 橙/红有强制 SLA；更低等级给 30 分钟默认值，监管通知从严取红色时限。
        minutes = sla_map.get(warning_level, 30)
        if kind == "regulator_cso":
            minutes = min(minutes, sla_map.get("red", 15))
        deadline = format_iso(as_of + timedelta(minutes=minutes))
        return {
            "notification_id": f"ntf-{plan_id}-{ntf_ctx['n']:02d}",
            "plan_id": plan_id, "kind": kind, "recipient_role": role_cn,
            "summary": summary, "warning_level": warning_level,
            "created_at": format_iso(as_of), "deadline_at": deadline,
            "sla_minutes": minutes, "status": "pending",
        }

    zone_results, all_actions, all_notifications = [], [], []
    impacts_by_reach = {}

    for zone_id, zone in config.zones.items():
        zr = _build_zone(
            state, config, zone, as_of, plan_id, threshold, horizon_s,
            stale_mult, gate_tol, level_tol, quality_tol,
            next_action_id, make_notification, all_actions,
            all_notifications, impacts_by_reach)
        zone_results.append(zr)

    order = ["none", "unknown", "blue", "yellow", "orange", "red"]
    max_level = "none"
    for zr in zone_results:
        lvl = zr["warning_level"]["level"]
        if order.index(lvl) > order.index(max_level):
            max_level = lvl

    decision = "hold_closed"
    for candidate in ("manual_takeover", "emergency_escalation",
                      "needs_confirmation", "controlled_spill"):
        if any(zr["decision"] == candidate for zr in zone_results):
            decision = candidate
            break

    return {
        "plan_id": plan_id,
        "generated_at": format_iso(as_of),
        "horizon_seconds": horizon_s,
        "overall_decision": decision,
        "overall_warning_level": max_level,
        "pollution_threshold_mg_l": threshold,
        "zones": zone_results,
        "actions": all_actions,
        "notifications": all_notifications,
        "impacts": _finalize_impacts(impacts_by_reach, p),
        "status": "proposed",
    }


def _build_zone(state, config, zone, as_of, plan_id, threshold, horizon_s,
                stale_mult, gate_tol, level_tol, quality_tol,
                next_action_id, make_notification, all_actions,
                all_notifications, impacts_by_reach):
    p = config.params
    station_metas = [s for s in config.raw_station_map().values()
                     if s.get("zone_id") == zone["id"]]

    def meta_of(kind):
        return next((s for s in station_metas if s["kind"] == kind), None)

    rain_meta, level_meta = meta_of("rainfall"), meta_of("level")
    quality_meta, river_meta = meta_of("quality"), meta_of("river_level")

    rain_1h, rain_frames = (None, [])
    rain_eval = storm = None
    if rain_meta:
        rain_1h, rain_frames = _rain_1h(state, rain_meta["id"], as_of)
        rain_eval = evaluate_station(state["readings"].get(rain_meta["id"]),
                                     rain_meta, as_of, stale_mult)
        storm = _current_storm(state, rain_meta["id"])

    level_eval = (evaluate_station(state["readings"].get(level_meta["id"]),
                                   level_meta, as_of, stale_mult, level_tol)
                  if level_meta else None)
    quality_eval = (evaluate_station(state["readings"].get(quality_meta["id"]),
                                     quality_meta, as_of, stale_mult, quality_tol)
                    if quality_meta else None)
    river_eval = (evaluate_station(state["readings"].get(river_meta["id"]),
                                   river_meta, as_of, stale_mult, level_tol)
                  if river_meta else None)

    warning = _warning_level(config, rain_1h)

    # ----- 水文估算（缺测即缺测，绝不补零） -----
    level_m = level_eval["adopted_value"] if level_eval else None
    level_full = level_meta.get("level_full_m") if level_meta else None
    level_ratio = level_m / level_full if level_m is not None and level_full else None
    storage_remaining = (zone["storage_capacity_m3"] * (1 - level_ratio)
                         if level_ratio is not None else None)
    inflow_volume = None
    if rain_1h is not None:
        inflow_volume = (zone["runoff_coefficient"] * zone["catchment_area_m2"]
                         * rain_1h / 1000.0)
    pump_capacity = sum(pm["capacity_m3_s"] for pm in zone.get("pumps", []))
    pump_volume_h = pump_capacity * horizon_s
    surplus = (max(0.0, inflow_volume - pump_volume_h - storage_remaining)
               if inflow_volume is not None and storage_remaining is not None else None)

    first_outlet = zone["outlets"][0]
    reach = config.river_reaches[first_outlet["river_reach_id"]]
    river_level = river_eval["adopted_value"] if river_eval else None
    river_unknown = river_level is None
    river_blocked = (not river_unknown
                     and river_level >= reach["backwater_level_m"])
    river_caution = (not river_unknown and reach["limit_level_m"]
                     <= river_level < reach["backwater_level_m"])

    cod = quality_eval["adopted_value"] if quality_eval else None
    cod_basis = quality_eval["basis"] if quality_eval else "none"
    if cod is None:
        cod = p["assumed_combined_sewage_quality_mg_l"]
        cod_basis = "assumed_combined_sewage"
    polluted = cod > threshold

    hydro = {
        "rain_1h_mm": rain_1h, "rain_frames_used": rain_frames,
        "rain_station_status": rain_eval["status"] if rain_eval else "unknown",
        "warning_level": warning["level"],
        "level_m": level_m, "level_full_m": level_full, "level_ratio": level_ratio,
        "storage_capacity_m3": zone["storage_capacity_m3"],
        "storage_remaining_m3": storage_remaining,
        "inflow_volume_m3": inflow_volume,
        "pump_capacity_m3_s": pump_capacity,
        "pump_volume_horizon_m3": pump_volume_h,
        "surplus_volume_m3": surplus,
        "river_reach_id": reach["id"], "river_level_m": river_level,
        "river_limit_level_m": reach["limit_level_m"],
        "river_backwater_level_m": reach["backwater_level_m"],
        "river_state": ("backwater" if river_blocked else "caution"
                        if river_caution else "unknown" if river_unknown else "normal"),
        "cod_mg_l": cod, "cod_basis": cod_basis, "polluted": polluted,
    }

    conflicts = []
    for ev in (level_eval, quality_eval, river_eval, rain_eval):
        if ev and ev.get("conflict"):
            conflicts.append(ev["conflict"])

    zone_to = _takeover_active(state, "zone", zone["id"], as_of)
    actions, notifications = [], []
    decision = "hold_closed"
    gate_evals = {}

    for outlet in zone["outlets"]:
        gate_meta = config.gates[outlet["gate_id"]]
        gate_meta = {**gate_meta, "interval_s": GATE_INTERVAL_S}
        command = state["commands"].get(gate_meta["id"])
        gate_eval = evaluate_gate(state["gate_reports"].get(gate_meta["id"]),
                                  gate_meta, as_of, stale_mult, gate_tol, command)
        gate_evals[outlet["id"]] = gate_eval
        conflicts.extend(gate_eval["conflicts"])
        gate_to = _takeover_active(state, "gate", gate_meta["id"], as_of)
        takeover = zone_to or gate_to

        action = {
            "action_id": next_action_id(), "plan_id": plan_id, "kind": "gate",
            "zone_id": zone["id"], "outlet_id": outlet["id"],
            "gate_id": gate_meta["id"], "gate_name_cn": gate_meta["name_cn"],
            "gate_capacity_m3_s": gate_meta["capacity_m3_s"],
            "current_percent_open": gate_eval["actual_percent_open"],
            "target_percent_open": None, "status": "proposed",
            "rationale": [], "constraints": [], "requires_confirmation": False,
            "owner": FIELD_CREW,
        }

        if takeover:
            scope_cn = "分区" if takeover["scope"] == "zone" else "闸门"
            action["target_percent_open"] = gate_eval["actual_percent_open"]
            action["status"] = "manual"
            action["owner"] = {"id": takeover["owner"], "name": takeover["owner"],
                               "role": takeover["role"]}
            action["rationale"].append(
                f"{scope_cn}处于人工接管（责任人 {takeover['owner']}，自 {takeover['since']}），"
                "系统不得自动改变闸门开度")
            decision = "manual_takeover"

        elif gate_eval["conflicts"]:
            action["target_percent_open"] = gate_eval["actual_percent_open"]
            action["status"] = "needs_confirmation"
            action["requires_confirmation"] = True
            action["owner"] = DUTY_DISPATCHER
            action["rationale"].append(
                "闸门遥测开度、人工复核开度或系统指令存在冲突，保持现场现状，待人工确认；"
                "不得按任一来源的旧值启闭")
            decision = "needs_confirmation"
            notifications.append(make_notification(
                "gate_conflict", "调度员",
                f"{gate_meta['name_cn']}（{gate_meta['id']}）状态来源冲突，需现场核实后确认",
                warning["level"]))

        elif level_eval is None or level_eval["status"] != "fresh":
            status = level_eval["status"] if level_eval else "not_configured"
            action["status"] = "needs_confirmation"
            action["requires_confirmation"] = True
            action["owner"] = DUTY_DISPATCHER
            action["rationale"].append(
                f"集水井液位工况为 {status}（超时未上报即降级为待确认，绝不按零值处理），"
                "缺少可信液位不得自动启闭溢流闸")
            decision = "needs_confirmation"
            notifications.append(make_notification(
                "sensor_unconfirmed", "运维人员",
                f"液位站 {level_meta['id'] if level_meta else '?'} 失联/不可信，需现场核实液位",
                warning["level"]))

        else:
            target, label, note = _decide_gate(
                hydro, gate_meta, horizon_s, river_unknown, river_blocked,
                river_caution, polluted, threshold, level_ratio, surplus, p, action)
            action["target_percent_open"] = target
            action["rationale"].append(note)
            if label == "controlled_spill":
                decision = "controlled_spill"
            elif label == "emergency_escalation":
                action["status"] = "needs_confirmation"
                action["requires_confirmation"] = True
                action["owner"] = DUTY_DISPATCHER
                decision = "emergency_escalation"
            elif label == "needs_confirmation":
                action["status"] = "needs_confirmation"
                action["requires_confirmation"] = True
                action["owner"] = DUTY_DISPATCHER
                if decision not in ("controlled_spill",):
                    decision = "needs_confirmation"
                notifications.append(make_notification(
                    "sensor_unconfirmed", "运维人员",
                    f"河道站 {river_meta['id'] if river_meta else '?'} 水位未知，禁止凭未知水位开闸",
                    warning["level"]))

            if action["target_percent_open"]:
                if polluted and action["requires_confirmation"]:
                    notifications.append(make_notification(
                        "regulator_cso", "监管人员",
                        f"{outlet['name_cn']} 计划超污染阈值受控溢流（COD {cod:.0f}mg/L），"
                        "须在开闸前完成监管确认", warning["level"]))
                spill_m3 = (action["target_percent_open"] / 100.0
                            * gate_meta["capacity_m3_s"] * horizon_s)
                _add_impact(impacts_by_reach, reach, outlet, warning["level"],
                            spill_m3, cod, plan_id, p)

        actions.append(action)
        all_actions.append(action)

    # 泵组
    for pump in zone.get("pumps", []):
        pa = {
            "action_id": next_action_id(), "plan_id": plan_id, "kind": "pump",
            "zone_id": zone["id"], "pump_id": pump["id"],
            "pump_name_cn": pump["name_cn"], "target_state": None,
            "status": "proposed", "rationale": [], "owner": FIELD_CREW,
        }
        if zone_to:
            pa["target_state"] = "manual"
            pa["status"] = "manual"
            pa["owner"] = {"id": zone_to["owner"], "name": zone_to["owner"],
                           "role": zone_to["role"]}
            pa["rationale"].append("分区人工接管中，泵组由接管责任人指挥")
        elif (level_ratio is not None and level_ratio >= LEVEL_HIGH_RATIO) or (
                surplus is not None and surplus > 0):
            pa["target_state"] = "on_full"
            ratio_txt = f"液位{level_ratio:.0%}" if level_ratio is not None else "调蓄余量不足"
            pa["rationale"].append(
                f"{ratio_txt}，提升泵 {pump['id']} 满负荷（{pump['capacity_m3_s']} m³/s）"
                f"送{pump['destination_cn']}，优先减少入河污染")
        else:
            pa["target_state"] = "on_standby"
            pa["rationale"].append("调蓄容量与进厂能力可容纳来水，泵组待命")
        actions.append(pa)
        all_actions.append(pa)

    all_notifications.extend(notifications)

    readings_used = {
        "rainfall": {
            "station_id": rain_meta["id"] if rain_meta else None,
            "adopted_value": rain_eval["adopted_value"] if rain_eval else None,
            "basis": rain_eval["basis"] if rain_eval else "none",
            "status": rain_eval["status"] if rain_eval else "unknown",
            "rain_1h_mm": rain_1h, "frames": rain_frames,
        },
        "level": _reading_brief(level_meta, level_eval) if level_eval else
        {"station_id": None, "status": "not_configured"},
        "river_level": _reading_brief(river_meta, river_eval) if river_eval else
        {"station_id": None, "status": "not_configured"},
        "quality": _reading_brief(quality_meta, quality_eval) if quality_eval else
        {"station_id": None, "status": "not_configured"},
        "gates": [
            {"gate_id": config.gates[o["gate_id"]]["id"],
             "actual_percent_open": gate_evals[o["id"]]["actual_percent_open"],
             "status": gate_evals[o["id"]]["status"],
             "command_percent_open": (state["commands"].get(o["gate_id"]) or {})
                 .get("command_percent_open")}
            for o in zone["outlets"]
        ],
    }

    return {
        "zone_id": zone["id"], "zone_name_cn": zone["name_cn"],
        "storm_id": storm["storm_id"] if storm else None,
        "warning_level": warning, "decision": decision,
        "hydrology": hydro, "readings_used": readings_used,
        "conflicts": conflicts, "manual_takeover": zone_to,
        "current_owner": _zone_owner(zone_to, actions, conflicts),
        "action_ids": [a["action_id"] for a in actions],
    }


def _decide_gate(hydro, gate_meta, horizon_s, river_unknown, river_blocked,
                 river_caution, polluted, threshold, level_ratio, surplus,
                 params, action):
    """返回 (目标开度%, 决策标签, 人类可读说明)。"""
    level = hydro["river_level_m"]

    if river_blocked:
        if level_ratio is not None and level_ratio >= LEVEL_EMERGENCY_RATIO:
            return (None, "emergency_escalation",
                    f"河道已回水（{level}m ≥ 回水限值 {hydro['river_backwater_level_m']}m），"
                    "开闸会倒灌并把高浓度污水推入河道；但集水井已接近满管、有城区内涝风险，"
                    "机器无权在两种危害间取舍，立即升级值班调度员决策")
        return (0, None,
                f"河道回水（{level}m ≥ {hydro['river_backwater_level_m']}m），溢流闸保持全关，"
                "防止倒灌与污水入河，依靠泵组抽排与调蓄")

    if river_unknown:
        return (None, "needs_confirmation",
                "河道水位缺失/失联，无法判断受纳能力，闸门保持关闭并转人工确认水位")

    need_spill = False
    notes = []
    if level_ratio is not None and level_ratio >= LEVEL_EMERGENCY_RATIO:
        need_spill = True
        notes.append(f"集水井液位 {level_ratio:.0%} ≥ 内涝临界 {LEVEL_EMERGENCY_RATIO:.0%}")
    elif surplus is not None and surplus > 0:
        need_spill = True
        notes.append(
            f"未来 {horizon_s//60} 分钟径流 {hydro['inflow_volume_m3']:.0f}m³，扣除泵抽 "
            f"{hydro['pump_volume_horizon_m3']:.0f}m³ 与调蓄余量 "
            f"{hydro['storage_remaining_m3']:.0f}m³ 后超量 {surplus:.0f}m³")

    if not need_spill:
        if level_ratio is not None and level_ratio >= LEVEL_HIGH_RATIO:
            notes.append(f"液位 {level_ratio:.0%} 偏高但未超临界，泵组可消化，闸门保持关闭")
        else:
            notes.append("来水可由调蓄容量与提升泵容纳，合流制溢流闸保持全关（默认保护态）")
        return 0, None, "；".join(notes)

    if polluted and not (level_ratio is not None
                         and level_ratio >= LEVEL_EMERGENCY_RATIO):
        return (0, None,
                f"水质 COD {hydro['cod_mg_l']:.0f}mg/L（依据：{hydro['cod_basis']}）超阈值 "
                f"{threshold}mg/L 且未到内涝临界，优先保护河道：闸门关闭、泵组满负荷抽排")

    needed_flow = (surplus or 0) / horizon_s
    if needed_flow <= 0:
        needed_flow = gate_meta["capacity_m3_s"]
    target = min(100, max(1, int(round(
        needed_flow / gate_meta["capacity_m3_s"] * 100))))
    if river_caution:
        target = max(1, int(target * RIVER_CAUTION_FACTOR))
        notes.append(
            f"河道水位 {level}m 处于警戒区间（{hydro['river_limit_level_m']}~"
            f"{hydro['river_backwater_level_m']}m），泄放开度折减 {int(RIVER_CAUTION_FACTOR*100)}%")
    if polluted:
        notes.append(
            f"已达内涝临界按受控溢流放行；COD {hydro['cod_mg_l']:.0f}mg/L 超阈值 "
            f"{threshold}mg/L，限时限量，开闸前必须监管确认")
        action["requires_confirmation"] = True
        action["constraints"].append("regulator_confirm_before_open")
    cap = (params["impact_spill_m3_per_km"]
           * params["impact_km_by_level"].get("red", 1))
    action["constraints"].append({"max_spill_volume_m3_horizon": cap})
    return target, "controlled_spill", "；".join(notes)


def _add_impact(impacts_by_reach, reach, outlet, warning_level, spill_m3, cod,
                plan_id, params):
    km = params["impact_km_by_level"].get(warning_level, 1)
    entry = impacts_by_reach.setdefault(reach["id"], {
        "impact_id": f"impact-{plan_id}-{reach['id']}", "plan_id": plan_id,
        "river_reach_id": reach["id"], "river_reach_name_cn": reach["name_cn"],
        "warning_level": warning_level, "segments": [],
        "spill_volume_m3": 0.0, "pollution_load_kg": 0.0,
    })
    entry["segments"].append({
        "outlet_id": outlet["id"], "from_km": outlet["river_km"],
        "to_km": round(min(outlet["river_km"] + km,
                           outlet["river_km"] + params["impact_max_km"]), 2),
        "spill_volume_m3": round(spill_m3, 1),
    })
    entry["spill_volume_m3"] += spill_m3
    entry["pollution_load_kg"] += spill_m3 * cod / 1000.0


def _finalize_impacts(impacts_by_reach, params):
    result = []
    for impact in impacts_by_reach.values():
        from_km = min(s["from_km"] for s in impact["segments"])
        to_km = min(max(s["to_km"] for s in impact["segments"]),
                    from_km + params["impact_max_km"])
        impact["from_km"] = from_km
        impact["to_km"] = round(to_km, 2)
        impact["spill_volume_m3"] = round(impact["spill_volume_m3"], 1)
        impact["pollution_load_kg"] = round(impact["pollution_load_kg"], 1)
        result.append(impact)
    return result


def _zone_owner(zone_to, actions, conflicts):
    if zone_to:
        return {"id": zone_to["owner"], "name": zone_to["owner"],
                "role": zone_to["role"], "source": "manual_takeover"}
    for a in actions:
        if a.get("status") == "manual":
            return {**a["owner"], "source": "manual"}
        if a.get("status") == "needs_confirmation":
            return {**a["owner"], "source": "needs_confirmation"}
    if conflicts:
        return {**DUTY_DISPATCHER, "source": "unresolved_conflict"}
    return {**FIELD_CREW, "source": "plan_execution"}
