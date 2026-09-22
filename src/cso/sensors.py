"""传感器工况判定。

失联语义：超过 interval × stale_multiplier 未收到帧即降级为 unconfirmed（待确认），
其数值绝不按零值参与调度；没有任何历史帧则为 unknown。人工复核始终作为独立来源保留。
"""
from __future__ import annotations

from datetime import timedelta

from .timeutils import parse_iso


def evaluate_station(pair: dict | None, station_meta: dict, as_of,
                     stale_multiplier: int, tolerance: float | None = None) -> dict:
    """综合遥测与人工复核，给出调度采用值与冲突说明。

    返回：
      status: fresh / unconfirmed / unknown / suspect
      adopted_value: 调度采用的数值（可能为 None —— 缺失就是缺失，不会被零值替代）
      basis: manual_review / telemetry / none
      conflict: 两来源不一致时的结构化描述（含双方读数、时间、复核人）
    """
    p = pair or {}
    tel = p.get("telemetry")
    man = p.get("manual")
    stale_after = station_meta.get("interval_s", 60) * stale_multiplier
    status = "unknown"
    age = None
    if tel is not None:
        age = (as_of - parse_iso(tel["observed_at"])).total_seconds()
        if tel.get("quality") not in (None, "ok"):
            status = "suspect"
        elif age > stale_after:
            status = "unconfirmed"
        else:
            status = "fresh"

    result = {
        "station_id": station_meta["id"], "kind": station_meta["kind"],
        "status": status, "telemetry": tel, "manual": man,
        "age_seconds": age, "stale_after_seconds": stale_after,
        "adopted_value": None, "basis": "none", "conflict": None,
    }

    if man is not None:
        result["adopted_value"] = man["value"] if "value" in man else man.get("percent_open")
        result["basis"] = "manual_review"
        if tel is not None and tolerance is not None:
            tel_value = tel.get("value", tel.get("percent_open"))
            man_value = result["adopted_value"]
            if tel_value is not None and abs(tel_value - man_value) > tolerance:
                result["conflict"] = {
                    "type": "telemetry_vs_manual",
                    "telemetry_value": tel_value,
                    "telemetry_observed_at": tel["observed_at"],
                    "manual_value": man_value,
                    "manual_observed_at": man["observed_at"],
                    "reviewer": man.get("reviewer"),
                    "reason": man.get("reason", ""),
                    "gap": abs(tel_value - man_value),
                    "tolerance": tolerance,
                }
        return result

    if tel is not None and status == "fresh":
        result["adopted_value"] = tel.get("value", tel.get("percent_open"))
        result["basis"] = "telemetry"
    # unconfirmed / unknown / suspect：adopted_value 保持 None（绝不补零）
    return result


def evaluate_gate(pair: dict | None, gate_meta: dict, as_of, stale_multiplier: int,
                  tolerance_percent: float, command: dict | None) -> dict:
    """闸门工况：遥测开度、人工复核开度与系统下发指令三方对照。"""
    p = pair or {}
    tel = p.get("telemetry")
    man = p.get("manual")
    stale_after = gate_meta.get("interval_s", 60) * stale_multiplier
    gate_meta = {**gate_meta, "kind": "gate", "interval_s": gate_meta.get("interval_s", 60)}
    base = evaluate_station(pair, gate_meta, as_of, stale_multiplier,
                            tolerance=tolerance_percent)
    base["station_id"] = gate_meta["id"]
    base["kind"] = "gate"
    base["command"] = command

    conflicts = []
    if base["conflict"]:
        conflicts.append(base["conflict"])
    # 现场实际（人工优先，否则用新鲜遥测）与最新指令不一致 → 闸门状态冲突
    actual = man.get("percent_open") if man else (
        tel.get("percent_open") if tel and base["status"] == "fresh" else None)
    if actual is not None and command is not None:
        if abs(actual - command["command_percent_open"]) > tolerance_percent:
            conflicts.append({
                "type": "field_vs_command",
                "field_percent_open": actual,
                "field_source": "manual" if man else "telemetry",
                "command_percent_open": command["command_percent_open"],
                "command_issued_at": command["issued_at"],
                "plan_id": command["plan_id"],
                "gap": abs(actual - command["command_percent_open"]),
                "tolerance": tolerance_percent,
            })
    base["conflicts"] = conflicts
    base["actual_percent_open"] = actual
    return base
