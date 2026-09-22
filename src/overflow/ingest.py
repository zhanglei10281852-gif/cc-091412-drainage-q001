"""遥测接入：去重、失联降级、降雨事件归并、闸位冲突识别。

关键规则：
1. 同一 message_id（或缺省时的内容指纹）重传只返回 duplicated，绝不再次改变闸门状态；
2. 传感器失联/数据不良降级为 UNCONFIRMED（待确认），任何计算都不得把缺测当零值；
3. 降雨按"雨停间隙"归并到同一事件，跨午夜不拆分；
4. 遥测闸位与调度指令/人工复核不一致时登记冲突，供方案与管理接口展示来源。
"""

from . import reference, timeutil

QUALITY_GOOD = "GOOD"
QUALITY_BAD = "BAD"              # 传感器自检异常
QUALITY_STALE = "STALE"          # 超时未上报，待确认
QUALITY_MISSING = "MISSING"      # 从无上报，待确认
UNCONFIRMED = {QUALITY_BAD, QUALITY_STALE, QUALITY_MISSING}

# 各量测单位约定
VALID_UNITS = {
    "rain": {"mm/h", "mm"},
    "level": {"m"},
    "river_stage": {"m"},
    "gate": {None},
}


class TelemetryError(ValueError):
    pass


def _normalize_value(sensor_kind, value, extra):
    if sensor_kind == "gate":
        mode = extra.get("mode", value)
        if mode not in ("OPEN", "CLOSED", "REGULATED"):
            raise TelemetryError(f"闸门状态非法: {mode!r}")
        opening = extra.get("opening_pct")
        if opening is not None:
            opening = float(opening)
            if not 0.0 <= opening <= 100.0:
                raise TelemetryError("开度必须在 0~100 之间")
        return {"mode": mode, "opening_pct": opening}
    if value is None:
        raise TelemetryError("缺少 value")
    return {"value": float(value)}


def _fingerprint(sensor_id, source_time, norm):
    core = {"sensor_id": sensor_id, "source_time": source_time, **norm}
    return repr(tuple(sorted((k, str(v)) for k, v in core.items())))


def ingest_message(store, msg: dict, *, received_at=None):
    """处理一条遥测消息。

    返回 {duplicated, ...}。重复消息不产生任何状态变更与新事件。
    """
    received_at = received_at or timeutil.now()
    sensor_id = msg.get("sensor_id")
    if not sensor_id or sensor_id not in reference.SENSORS:
        raise TelemetryError(f"未知传感器: {sensor_id!r}")
    sensor = reference.SENSORS[sensor_id]
    kind = sensor["kind"]

    source_time = msg.get("source_time")
    if not source_time:
        raise TelemetryError("缺少 source_time")
    source_dt = timeutil.parse(source_time)
    if source_dt > received_at:
        raise TelemetryError("来源时间不能晚于接收时间")

    norm = _normalize_value(kind, msg.get("value"), msg)
    unit = msg.get("unit")
    if kind in VALID_UNITS and unit not in VALID_UNITS[kind]:
        raise TelemetryError(f"{kind} 量测单位非法: {unit!r}")

    message_id = msg.get("message_id")
    fp = _fingerprint(sensor_id, timeutil.iso(source_dt), norm)
    dedup_key = message_id or f"fp:{fp}"

    with store.lock:
        seen = store.state["processed_messages"]
        if dedup_key in seen:
            first_received, first_fp = seen[dedup_key]
            if first_fp != fp:
                raise TelemetryError(
                    f"message_id={message_id} 重传内容与首次不一致，拒绝处理")
            store.append_event(
                "telemetry.duplicate",
                {"sensor_id": sensor_id, "message_id": message_id,
                 "fingerprint": fp, "first_received_at": first_received},
                event_id=message_id,
            )
            return {"duplicated": True, "sensor_id": sensor_id,
                    "message_id": message_id, "fingerprint": fp}
        quality = msg.get("quality", QUALITY_GOOD)
        if quality not in (QUALITY_GOOD, QUALITY_BAD):
            raise TelemetryError("上行质量标记只能为 GOOD/BAD，降级由服务端判定")

        doc = {
            "sensor_id": sensor_id,
            "kind": kind,
            "quality": quality,
            "unit": unit,
            "source_time": timeutil.iso(source_dt),
            "received_at": timeutil.iso(received_at),
            "channel": msg.get("channel"),
            "seq": msg.get("seq"),
            "message_id": message_id,
            "fingerprint": fp,
            **norm,
        }
        store.state["measurements"][sensor_id] = doc
        seen[dedup_key] = [timeutil.iso(received_at), fp]

        event_payload = {"sensor_id": sensor_id, "kind": kind,
                         "source_time": doc["source_time"],
                         "received_at": doc["received_at"],
                         "quality": quality, "channel": doc["channel"]}
        if kind == "gate":
            event_payload["reported_mode"] = norm["mode"]
            event_payload["opening_pct"] = norm["opening_pct"]
            conflict = _record_gate_observation(store, sensor, doc, received_at)
            if conflict:
                event_payload["conflict_id"] = conflict["id"]
        elif kind == "rain":
            event_payload.update(norm)
            if unit:
                event_payload["unit"] = unit
            rain_event = _merge_rain(store, sensor, doc, norm, received_at)
            event_payload["rain_event_id"] = rain_event["id"]
        else:
            event_payload.update(norm)

        store.append_event("telemetry.received", event_payload,
                           event_id=message_id)
        store.save()
        return {"duplicated": False, "sensor_id": sensor_id,
                "message_id": message_id, "fingerprint": fp,
                "stored": _public_measurement(store, sensor_id, received_at)}


def sweep_stale(store, *, at=None):
    """按各传感器 stale_timeout 把超时量测降级为 STALE；状态迁移时记录事件。

    STALE 是待确认状态，读数仍保留旧值与来源，但任何计算不得把它当当前值/零值。
    """
    at = at or timeutil.now()
    changed = []
    with store.lock:
        for sensor_id, sensor in reference.SENSORS.items():
            doc = store.state["measurements"].get(sensor_id)
            timeout = sensor["stale_timeout_s"]
            if doc is None:
                continue
            if doc["quality"] == QUALITY_BAD:
                continue
            age = (at - timeutil.parse(doc["received_at"])).total_seconds()
            if age > timeout and doc["quality"] != QUALITY_STALE:
                doc["quality"] = QUALITY_STALE
                doc["stale_since"] = timeutil.iso(at)
                changed.append((sensor_id, doc))
                store.append_event(
                    "sensor.stale",
                    {"sensor_id": sensor_id, "kind": sensor["kind"],
                     "last_source_time": doc["source_time"],
                     "timeout_s": timeout,
                     "resolution": "降级为待确认(STALE)，禁止按零值参与调度"},
                )
        if changed:
            store.save()
        return [sid for sid, _ in changed]


def _record_gate_observation(store, sensor, doc, at):
    outfall_id = sensor["outfall"]
    gate_id = sensor.get("gate_id", doc["sensor_id"])
    gate_state = store.state["gates"].get(gate_id, {})
    reported = doc["mode"]
    commanded = gate_state.get("commanded_mode")
    review = store.state.get("reviews", {}).get(outfall_id)

    # 有明确的指令值，遥测却不一致，且该口未被人工接管/复核覆盖 -> 冲突
    takeover = store.state["takeovers"].get(outfall_id)
    basis = None
    if commanded and reported != commanded:
        basis = {"type": "command", "expected": commanded,
                 "plan_id": gate_state.get("commanded_plan_id")}
    elif review and review.get("gate_mode") and reported != review["gate_mode"]:
        basis = {"type": "manual_review", "expected": review["gate_mode"],
                 "reviewer": review.get("reviewer")}

    gate_state.update({
        "gate_id": gate_id, "outfall_id": outfall_id,
        "reported_mode": reported, "opening_pct": doc.get("opening_pct"),
        "reported_at": doc["source_time"],
        "reported_message_id": doc["message_id"],
        "reported_channel": doc.get("channel"),
        "updated_at": timeutil.iso(at),
    })
    if "commanded_mode" not in gate_state:
        gate_state["commanded_mode"] = None
    store.state["gates"][gate_id] = gate_state

    if not basis or (takeover and basis["type"] == "command"):
        return None

    open_conflict = next(
        (c for c in store.state["conflicts"].values()
         if c["outfall_id"] == outfall_id and c["status"] == "open"),
        None,
    )
    if open_conflict:
        sources = open_conflict.setdefault("telemetry_sources", [])
        sources.append({"message_id": doc["message_id"],
                        "source_time": doc["source_time"],
                        "reported_mode": reported})
        open_conflict["last_seen_at"] = timeutil.iso(at)
        return open_conflict

    cid = store.next_id("conflict_seq", "conflict")
    conflict = {
        "id": cid,
        "outfall_id": outfall_id,
        "gate_id": gate_id,
        "reported_mode": reported,
        "expected_mode": basis["expected"],
        "expected_basis": basis["type"],
        "plan_id": basis.get("plan_id"),
        "detected_at": timeutil.iso(at),
        "last_seen_at": timeutil.iso(at),
        "status": "open",
        "telemetry_sources": [{"message_id": doc["message_id"],
                               "source_time": doc["source_time"],
                               "reported_mode": reported}],
    }
    store.state["conflicts"][cid] = conflict
    store.append_event(
        "gate.conflict",
        {"conflict_id": cid, "outfall_id": outfall_id, "gate_id": gate_id,
         "reported_mode": reported, "expected_mode": basis["expected"],
         "expected_basis": basis["type"], "plan_id": basis.get("plan_id")},
        event_id=cid,
    )
    return conflict


def _merge_rain(store, sensor, doc, norm, at):
    """把雨量样本归并到分区降雨事件；雨停超过间隙则结束旧事件另开新事件。

    判定基于业务时区时间，仅看雨停间隙长度，与是否跨过 0 点无关。
    """
    district = sensor["district"]
    open_id = store.state["district_open_event"].get(district)
    wet = norm["value"] > 0

    if open_id:
        ev = store.state["rain_events"][open_id]
        gap = (timeutil.parse(doc["source_time"])
               - timeutil.parse(ev["last_rain_at"])).total_seconds() / 60.0
        if gap <= reference.RAIN_MERGE_GAP_MINUTES:
            if wet:
                _accumulate_rain(ev, doc, norm)
            ev["last_contact_at"] = doc["source_time"]
            ev["message_count"] += 1
            return ev
        # 间隙超时：关闭旧事件（结束于最后一条有雨记录）
        ev["status"] = "closed"
        ev["ended_at"] = ev["last_rain_at"]
        ev["cross_midnight"] = (timeutil.local_date_key(timeutil.parse(ev["started_at"]))
                                != timeutil.local_date_key(timeutil.parse(ev["ended_at"])))
        store.state["district_open_event"].pop(district, None)
        store.append_event("rain.event_closed",
                           {"rain_event_id": ev["id"], "district": district,
                            "started_at": ev["started_at"], "ended_at": ev["ended_at"],
                            "cross_midnight": ev["cross_midnight"],
                            "total_depth_mm": ev["total_depth_mm"]},
                           event_id=ev["id"])

    if not wet:
        # 干间隙期的采样不单独开事件
        return {"id": None}

    eid = store.next_id("rain_event_seq", "event")
    ev = {
        "id": eid,
        "district": district,
        "status": "open",
        "started_at": doc["source_time"],
        "last_rain_at": doc["source_time"],
        "last_contact_at": doc["source_time"],
        "ended_at": None,
        "start_local_date": timeutil.local_date_key(timeutil.parse(doc["source_time"])),
        "cross_midnight": False,
        "total_depth_mm": norm["value"] if doc.get("unit") == "mm" else 0.0,
        "peak_intensity_mmh": norm["value"] if doc.get("unit") == "mm/h" else None,
        "wet_samples": 1,
        "message_count": 1,
        "first_message_id": doc["message_id"],
        "last_message_id": doc["message_id"],
        "merge_gap_minutes": reference.RAIN_MERGE_GAP_MINUTES,
    }
    store.state["rain_events"][eid] = ev
    store.state["district_open_event"][district] = eid
    store.append_event("rain.event_opened",
                       {"rain_event_id": eid, "district": district,
                        "started_at": ev["started_at"],
                        "first_message_id": doc["message_id"]},
                       event_id=eid)
    return ev


def _accumulate_rain(ev, doc, norm):
    ev["last_rain_at"] = doc["source_time"]
    ev["wet_samples"] += 1
    ev["last_message_id"] = doc["message_id"]
    if doc.get("unit") == "mm":
        ev["total_depth_mm"] = round(ev["total_depth_mm"] + norm["value"], 3)
    if doc.get("unit") == "mm/h":
        peak = ev.get("peak_intensity_mmh")
        ev["peak_intensity_mmh"] = norm["value"] if peak is None else max(peak, norm["value"])


# ---------- 读视图 ----------
def _public_measurement(store, sensor_id, at=None):
    at = at or timeutil.now()
    doc = store.state["measurements"].get(sensor_id)
    sensor = reference.SENSORS[sensor_id]
    if doc is None:
        return {"sensor_id": sensor_id, "kind": sensor["kind"],
                "quality": QUALITY_MISSING, "usable": False,
                "note": "从无上报，按待确认处理，不得当零值"}
    out = dict(doc)
    out.pop("fingerprint", None)
    age = (at - timeutil.parse(doc["received_at"])).total_seconds()
    if doc["quality"] == QUALITY_GOOD and age > sensor["stale_timeout_s"]:
        out["quality"] = QUALITY_STALE
    out["usable"] = out["quality"] == QUALITY_GOOD
    out["age_seconds"] = int(age)
    return out


def sensor_snapshot(store, *, at=None):
    """全部传感器当前视图（管理接口/方案解释共用）。"""
    at = at or timeutil.now()
    sweep_stale(store, at=at)
    with store.lock:
        return [_public_measurement(store, sid, at) for sid in reference.SENSORS]
