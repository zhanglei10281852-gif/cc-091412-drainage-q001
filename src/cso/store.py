"""事件溯源存储。

所有状态变更都是追加事件，事件之间用 SHA-256 哈希链接，形成可校验的事件链；
状态快照用“写临时文件 + os.replace”原子落盘。服务重启后先装快照，再重放
快照之后的事件，未完成动作、通知时限与影响范围都能恢复。
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from pathlib import Path

from .timeutils import format_iso, now_utc

JOURNAL_NAME = "events.jsonl"
SNAPSHOT_NAME = "state.snapshot.json"
SNAPSHOT_EVERY = 20


def now_iso() -> str:
    return format_iso(now_utc())


def canonical(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(seq: int, ts: str, etype: str, payload: object, prev_hash: str) -> str:
    h = hashlib.sha256()
    h.update(str(seq).encode())
    h.update(ts.encode())
    h.update(etype.encode())
    h.update(canonical(payload))
    h.update(prev_hash.encode())
    return h.hexdigest()


def initial_state() -> dict:
    return {
        "version": 1,
        "dedup": {},          # message_id -> 首次处理结果摘要
        "readings": {},       # station_id -> {"telemetry": 最新帧, "manual": 最新复核}
        "rain_frames": {},    # station_id -> {observed_at: 帧}
        "gate_reports": {},   # gate_id -> 最新闸门遥测/复核
        "commands": {},       # gate_id -> 最新下发指令（来自已确认方案）
        "storms": {},         # storm_id -> 降雨事件登记
        "storm_station": {},  # station_id -> [storm_id,...]
        "plans": {},
        "actions": {},
        "takeovers": {},      # zone:<id> / gate:<id> -> 接管状态
        "notifications": {},
        "impacts": {},
        "rejections": [],
        "counters": {},
    }


# ---------------------------------------------------------------- reducer

def _bump_counter(state: dict, kind: str) -> int:
    n = state["counters"].get(kind, 0) + 1
    state["counters"][kind] = n
    return n


def _upsert_rain_frame(state: dict, station_id: str, frame: dict, gap_minutes: int) -> str | None:
    """落一帧雨量；必要时开启新的降雨事件。返回新开启的 storm_id（否则 None）。

    事件按相邻雨量帧的时间间隔归并，与自然日无关，因此跨午夜的连续降雨属于同一事件；
    间隔超过 storm_gap_minutes（雨峰间歇）才另开事件。
    """
    from datetime import timedelta

    from .timeutils import parse_iso

    frames = state["rain_frames"].setdefault(station_id, {})
    existing = frames.get(frame["observed_at"])
    frames[frame["observed_at"]] = frame
    ordered = sorted(frames)
    idx = ordered.index(frame["observed_at"])
    prev_at = parse_iso(ordered[idx - 1]) if idx > 0 else None
    cur_at = parse_iso(frame["observed_at"])
    gap = timedelta(minutes=gap_minutes)

    opened_id = None
    if prev_at is None or cur_at - prev_at > gap:
        n = _bump_counter(state, "storm")
        storm_id = f"storm-{n:04d}"
        state["storms"][storm_id] = {
            "storm_id": storm_id,
            "station_id": station_id,
            "zone_id": frame["zone_id"],
            "started_at": frame["observed_at"],
            "last_frame_at": frame["observed_at"],
            "opened_seq": frame["seq"],
        }
        state["storm_station"].setdefault(station_id, []).append(storm_id)
        opened_id = storm_id
    else:
        candidates = state["storm_station"][station_id]
        storm_id = next(
            (sid for sid in reversed(candidates)
             if state["storms"][sid]["started_at"] <= frame["observed_at"]),
            candidates[0],
        )

    storm = state["storms"][storm_id]
    if parse_iso(frame["observed_at"]) > parse_iso(storm["last_frame_at"]):
        storm["last_frame_at"] = frame["observed_at"]

    # 同一观测时刻的不同报文按更正处理：帧被替换，但不影响事件归属。
    if existing is not None and existing["rain_mm"] != frame["rain_mm"]:
        frame["corrected_from"] = existing["rain_mm"]
    return opened_id


def reduce_event(state: dict, event: dict, config_params: dict) -> dict:
    """把事件归约到内存状态。不得修改 event/payload（哈希已按原文计算）。

    返回附加工况（例如新开启的降雨事件 id），供调用方写进响应。
    """
    p = event["payload"]
    t = event["type"]
    effects: dict = {}

    # 幂等表在回放时从事件本身重建：带 message_id 的摄入事件都登记首次落点。
    mid = p.get("message_id")
    if mid and mid not in state["dedup"]:
        first_result = None
        if t == "telemetry.gate":
            first_result = {"gate_id": p["gate_id"], "percent_open": p["percent_open"]}
        elif t.startswith("telemetry."):
            field = {"telemetry.rainfall": "rain_mm", "telemetry.level": "level_m",
                     "telemetry.river_level": "level_m",
                     "telemetry.quality": "cod_mg_l"}.get(t)
            first_result = {"station_id": p["station_id"], field: p[field]} if field else None
        state["dedup"][mid] = {"seq": event["seq"], "type": t,
                               "handled_at": event["ts"], "result": first_result}

    if t == "telemetry.rainfall":
        state["readings"].setdefault(p["station_id"], {})["telemetry"] = {
            "station_id": p["station_id"], "kind": "rainfall",
            "value": p["rain_mm"], "unit": "mm/帧",
            "observed_at": p["observed_at"], "received_at": event["ts"],
            "source": "telemetry", "quality": p.get("quality", "ok"),
            "message_id": p["message_id"], "seq": event["seq"],
        }
        frame = {
            "station_id": p["station_id"], "zone_id": p["zone_id"],
            "observed_at": p["observed_at"], "rain_mm": p["rain_mm"],
            "quality": p.get("quality", "ok"), "seq": event["seq"],
        }
        opened_id = _upsert_rain_frame(
            state, p["station_id"], frame, config_params["storm_gap_minutes"]
        )
        if opened_id:
            effects["storm_opened"] = opened_id

    elif t in ("telemetry.level", "telemetry.river_level", "telemetry.quality"):
        kind = t.split(".", 1)[1]
        field = {"level": "level_m", "river_level": "level_m", "quality": "cod_mg_l"}[kind]
        state["readings"].setdefault(p["station_id"], {})["telemetry"] = {
            "station_id": p["station_id"], "kind": kind,
            "value": p[field], "unit": "m" if field == "level_m" else "mg/L",
            "observed_at": p["observed_at"], "received_at": event["ts"],
            "source": "telemetry", "quality": p.get("quality", "ok"),
            "message_id": p["message_id"], "seq": event["seq"],
        }

    elif t == "telemetry.gate":
        state["gate_reports"].setdefault(p["gate_id"], {})["telemetry"] = {
            "gate_id": p["gate_id"], "percent_open": p["percent_open"],
            "observed_at": p["observed_at"], "received_at": event["ts"],
            "source": "telemetry", "quality": p.get("quality", "ok"),
            "message_id": p["message_id"], "seq": event["seq"],
        }

    elif t == "manual.reading":
        if p.get("gate_id"):
            state["gate_reports"].setdefault(p["gate_id"], {})["manual"] = {
                "gate_id": p["gate_id"], "percent_open": p["percent_open"],
                "observed_at": p["observed_at"], "received_at": event["ts"],
                "source": "manual", "quality": "ok",
                "reviewer": p["reviewer"], "reason": p.get("reason", ""),
                "seq": event["seq"],
            }
        else:
            prev = state["readings"].get(p["station_id"], {}).get("manual", {})
            state["readings"].setdefault(p["station_id"], {})["manual"] = {
                **prev,
                "station_id": p["station_id"], "kind": p["kind"],
                "value": p["value"], "unit": p.get("unit", prev.get("unit", "")),
                "observed_at": p["observed_at"], "received_at": event["ts"],
                "source": "manual", "quality": "ok",
                "reviewer": p["reviewer"], "reason": p.get("reason", ""),
                "seq": event["seq"],
            }

    elif t == "plan.generated":
        plan = p["plan"]
        _bump_counter(state, "plan")
        state["plans"][plan["plan_id"]] = plan
        for action in plan["actions"]:
            state["actions"][action["action_id"]] = action
        for impact in p.get("impacts", []):
            state["impacts"][impact["impact_id"]] = impact

    elif t == "plan.confirmed":
        plan = state["plans"][p["plan_id"]]
        plan["status"] = "confirmed"
        plan["confirmed_at"] = event["ts"]
        plan["confirmer"] = p["actor"]
        for action in plan["actions"]:
            rec = state["actions"][action["action_id"]]
            rec["status"] = "pending"
            rec["dispatched_at"] = event["ts"]
            if action["kind"] == "gate":
                state["commands"][action["gate_id"]] = {
                    "gate_id": action["gate_id"],
                    "command_percent_open": action["target_percent_open"],
                    "plan_id": plan["plan_id"],
                    "issued_at": event["ts"],
                }
        for n in plan.get("notifications", []):
            state["notifications"][n["notification_id"]] = n

    elif t == "plan.rejected":
        plan = state["plans"][p["plan_id"]]
        plan["status"] = "rejected"
        plan["rejected_at"] = event["ts"]
        plan["reject_reason"] = p["reason"]
        for action in plan["actions"]:
            rec = state["actions"][action["action_id"]]
            if rec["status"] == "proposed":
                rec["status"] = "rejected"
        state["rejections"].append({
            "plan_id": p["plan_id"], "reason": p["reason"],
            "actor": p["actor"], "at": event["ts"],
        })

    elif t == "plan.superseded":
        old = state["plans"][p["plan_id"]]
        old["status"] = "superseded"
        old["superseded_by"] = p["new_plan_id"]
        old["supersede_reason"] = p["reason"]
        for action in old.get("actions", []):
            rec = state["actions"][action["action_id"]]
            if rec["status"] in ("proposed", "pending"):
                rec["status"] = "cancelled"
                rec["cancelled_reason"] = f"方案 {p['plan_id']} 被 {p['new_plan_id']} 取代"
        for n in old.get("notifications", []):
            rec = state["notifications"].get(n["notification_id"])
            if rec and rec["status"] == "pending":
                rec["status"] = "cancelled"

    elif t == "action.feedback":
        rec = state["actions"][p["action_id"]]
        rec["status"] = p["status"]          # executed / rejected / failed
        rec.setdefault("history", []).append(
            {"at": event["ts"], "status": p["status"], "detail": p.get("detail", ""),
             "actor": p["actor"]}
        )

    elif t == "manual.takeover":
        key = f"{p['scope']}:{p['scope_id']}"
        state["takeovers"][key] = {
            "key": key, "scope": p["scope"], "scope_id": p["scope_id"],
            "owner": p["owner"], "role": p["role"], "reason": p.get("reason", ""),
            "since": event["ts"], "until": p.get("until"), "active": True,
        }

    elif t == "manual.release":
        key = f"{p['scope']}:{p['scope_id']}"
        rec = state["takeovers"].get(key)
        if rec:
            rec["active"] = False
            rec["released_at"] = event["ts"]
            rec["released_by"] = p["actor"]

    elif t == "notification.ack":
        rec = state["notifications"][p["notification_id"]]
        rec["status"] = "acknowledged"
        rec["acknowledged_at"] = event["ts"]
        rec["acknowledged_by"] = p["actor"]

    else:
        raise ValueError(f"未知事件类型: {t}")
    return state, effects


# ---------------------------------------------------------------- store

class Store:
    def __init__(self, data_dir: str | os.PathLike, config_params: dict):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.journal_path = self.dir / JOURNAL_NAME
        self.snapshot_path = self.dir / SNAPSHOT_NAME
        self.params = config_params
        self.lock = threading.RLock()
        self.state = initial_state()
        self.last_seq = 0
        self.last_hash = "0" * 64
        self._rebuild()

    # -- 回放 -----------------------------------------------------------
    def _rebuild(self) -> None:
        state = initial_state()
        seq = 0
        prev = "0" * 64
        snapshot_seq = 0
        if self.snapshot_path.exists():
            snap = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
            state = self._merge_init(snap["state"])
            seq = snap["last_seq"]
            prev = snap["last_hash"]
            snapshot_seq = seq
        if self.journal_path.exists():
            with self.journal_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    if event["seq"] <= snapshot_seq:
                        continue
                    self._verify_chain(event, prev)
                    state, _effects = reduce_event(state, event, self.params)
                    seq = event["seq"]
                    prev = event["hash"]
        self.state = state
        self.last_seq = seq
        self.last_hash = prev

    @staticmethod
    def _merge_init(snapshot_state: dict) -> dict:
        state = initial_state()
        state.update(snapshot_state)
        return state

    @staticmethod
    def _verify_chain(event: dict, expected_prev: str) -> None:
        want = digest(event["seq"], event["ts"], event["type"], event["payload"], event["prev_hash"])
        if event["prev_hash"] != expected_prev or event["hash"] != want:
            raise RuntimeError(f"事件链校验失败，断点 seq={event['seq']}")

    # -- 写入 -----------------------------------------------------------
    def append(self, etype: str, payload: dict, actor: dict | None = None,
               duplicate_for: str | None = None) -> dict:
        """追加事件。

        duplicate_for 给出时表示该 message_id 已处理过（重传）：直接返回首次处理摘要，
        绝不再次归约，闸门状态不会因重传改变。
        """
        with self.lock:
            if duplicate_for is not None:
                first = self.state["dedup"][duplicate_for]
                return {"duplicate": True, "message_id": duplicate_for,
                        "first_handled_at": first["handled_at"],
                        "first_result": first.get("result", {})}
            seq = self.last_seq + 1
            ts = now_iso()
            event = {
                "seq": seq, "ts": ts, "type": etype,
                "actor": actor or {"type": "system"},
                "payload": payload, "prev_hash": self.last_hash,
            }
            event["hash"] = digest(seq, ts, etype, payload, self.last_hash)
            # 先在状态副本上试归约：失败则不落盘，日志与状态不会分叉。
            trial = copy.deepcopy(self.state)
            _trial_state, effects = reduce_event(trial, event, self.params)
            with self.journal_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            self.state = _trial_state
            self.last_seq = seq
            self.last_hash = event["hash"]
            if seq % SNAPSHOT_EVERY == 0:
                self.snapshot()
            return {"duplicate": False, "seq": seq, "hash": event["hash"], "ts": ts, **effects}

    def remember_dedup_result(self, message_id: str, result: dict) -> None:
        """把首次处理的业务结果挂到幂等表，重传时原样返回。

        注意：这是纯内存提示；事件链与状态重建只依赖事件本身，不依赖该缓存。
        """
        with self.lock:
            rec = self.state["dedup"].get(message_id)
            if rec:
                rec["result"] = result

    def persist_now(self) -> None:
        with self.lock:
            self.snapshot()

    def snapshot(self) -> None:
        tmp = self.snapshot_path.with_suffix(".tmp")
        doc = {"last_seq": self.last_seq, "last_hash": self.last_hash,
               "taken_at": now_iso(), "state": self.state}
        tmp.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.snapshot_path)

    # -- 读取 -----------------------------------------------------------
    def events(self, after_seq: int = 0) -> list[dict]:
        with self.lock:
            if not self.journal_path.exists():
                return []
            out = []
            with self.journal_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        event = json.loads(line)
                        if event["seq"] > after_seq:
                            out.append(event)
            return out

    def verify_chain(self) -> dict:
        """从头重算哈希链，供管理接口核验事件链未被篡改。"""
        prev = "0" * 64
        count = 0
        with self.lock:
            if not self.journal_path.exists():
                return {"events": 0, "ok": True}
            with self.journal_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    self._verify_chain(event, prev)
                    prev = event["hash"]
                    count += 1
        return {"events": count, "ok": True, "last_hash": prev}
