"""溢流调度后端 HTTP 服务（标准库，无第三方依赖）。"""
from __future__ import annotations

import json
import os
import threading
import http.server
import urllib.parse
from datetime import datetime

from .config import load_config
from .engine import build_plan
from .store import Store
from .sensors import evaluate_gate, evaluate_station
from .timeutils import format_iso, now_utc, parse_iso

SERVICE_NAME = "cso-dispatch-service"

TELEMETRY_KINDS = {
    "rainfall": ("telemetry.rainfall", "rain_mm"),
    "level": ("telemetry.level", "level_m"),
    "river_level": ("telemetry.river_level", "level_m"),
    "quality": ("telemetry.quality", "cod_mg_l"),
}
GATE_INTERVAL_S = 60


class ApiError(Exception):
    def __init__(self, status: int, code: str, details=None):
        super().__init__(code)
        self.status = status
        self.code = code
        self.details = details


class Service:
    def __init__(self, config=None, data_dir: str | None = None):
        self.config = config or load_config()
        self.data_dir = data_dir or os.environ.get(
            "DATA_DIR", os.path.join(os.getcwd(), ".runtime", "data"))
        self.store = Store(self.data_dir, self.config.params)

    # ------------------------------------------------------------ 工具
    def _as_of(self, query: dict) -> datetime:
        raw = query.get("as_of")
        return parse_iso(raw[0]) if raw else now_utc()

    def _actor(self, body: dict) -> dict:
        actor = body.get("actor")
        if not actor or not actor.get("id") or not actor.get("role"):
            raise ApiError(400, "actor_required", "需要 actor.id 与 actor.role")
        if actor["role"] not in self.config.raw["roles"]:
            raise ApiError(400, "unknown_role", actor["role"])
        return {"id": actor["id"], "name": actor.get("name", actor["id"]),
                "role": actor["role"]}

    def _active_confirmed_plan(self):
        """存在已确认且仍有未完成动作的方案时返回它——新方案不得静默覆盖它。"""
        for plan in reversed(list(self.store.state["plans"].values())):
            if plan["status"] != "confirmed":
                continue
            if any(a["status"] in ("proposed", "pending") for a in plan["actions"]):
                return plan
        return None

    # ------------------------------------------------------------ 遥测
    def ingest_telemetry(self, body: dict) -> dict:
        with self.store.lock:
            return self._ingest_telemetry(body)

    def _ingest_telemetry(self, body: dict) -> dict:
        mid = body.get("message_id")
        if not mid:
            raise ApiError(400, "message_id_required",
                           "遥测报文必须携带 message_id，服务端据此去重")
        if mid in self.store.state["dedup"]:
            # 重传：原样返回首次处理结果，绝不再次归约，闸门状态不会变化。
            return self.store.append("", {}, duplicate_for=mid)

        kind = body.get("kind")
        observed_at = body.get("observed_at")
        if not observed_at:
            raise ApiError(400, "observed_at_required")
        parse_iso(observed_at)  # 校验带时区
        quality = body.get("quality", "ok")

        if kind == "gate":
            gate_id = body.get("gate_id")
            gate = self.config.gates.get(gate_id)
            if not gate:
                raise ApiError(404, "unknown_gate", gate_id)
            percent = _number(body, "percent_open", 0, 100)
            result = self.store.append("telemetry.gate", {
                "message_id": mid, "gate_id": gate_id,
                "percent_open": percent, "observed_at": observed_at,
                "quality": quality,
            })
            self.store.remember_dedup_result(mid, {"gate_id": gate_id,
                                                   "percent_open": percent})
            return {"accepted": True, **result}

        if kind not in TELEMETRY_KINDS:
            raise ApiError(400, "unknown_kind", str(kind))
        etype, field = TELEMETRY_KINDS[kind]
        station_id = body.get("station_id")
        station = self.config.stations.get(station_id)
        if not station:
            raise ApiError(404, "unknown_station", station_id)
        if station["kind"] != kind:
            raise ApiError(400, "station_kind_mismatch",
                           {"station": station_id, "expected": station["kind"],
                            "got": kind})
        value = _number(body, field, 0 if kind == "rainfall" else None, None)
        payload = {"message_id": mid, "station_id": station_id,
                   "zone_id": station["zone_id"],
                   "observed_at": observed_at, field: value, "quality": quality}
        result = self.store.append(etype, payload)
        self.store.remember_dedup_result(mid, {"station_id": station_id,
                                               field: value})
        return {"accepted": True, **result}

    # ------------------------------------------------------------ 复核
    def manual_review(self, body: dict) -> dict:
        with self.store.lock:
            return self._manual_review(body)

    def _manual_review(self, body: dict) -> dict:
        actor = self._actor(body)
        observed_at = body.get("observed_at")
        if not observed_at:
            raise ApiError(400, "observed_at_required")
        parse_iso(observed_at)
        reason = body.get("reason", "")

        if body.get("kind") == "gate" or body.get("gate_id"):
            gate_id = body.get("gate_id")
            if gate_id not in self.config.gates:
                raise ApiError(404, "unknown_gate", gate_id)
            percent = _number(body, "percent_open", 0, 100)
            result = self.store.append("manual.reading", {
                "gate_id": gate_id, "percent_open": percent,
                "observed_at": observed_at, "reviewer": actor["id"],
                "reason": reason,
            }, actor={"type": "user", **actor})
            return {"accepted": True, **result}

        station_id = body.get("station_id")
        station = self.config.stations.get(station_id)
        if not station:
            raise ApiError(404, "unknown_station", station_id)
        kind = station["kind"]
        field = {"rainfall": "rain_mm", "level": "level_m",
                 "river_level": "level_m", "quality": "cod_mg_l"}[kind]
        if "value" not in body:
            raise ApiError(400, "value_required")
        value = float(body["value"])
        result = self.store.append("manual.reading", {
            "station_id": station_id, "kind": kind, "value": value,
            "unit": station.get("unit", ""),
            "observed_at": observed_at, "reviewer": actor["id"],
            "reason": reason,
        }, actor={"type": "user", **actor})
        return {"accepted": True, **result}

    # ------------------------------------------------------------ 方案
    def generate_plan(self, body: dict, query: dict) -> dict:
        as_of = self._as_of(query)
        body = body or {}
        with self.store.lock:
            active = self._active_confirmed_plan()
            if active and not body.get("supersede_active"):
                raise ApiError(409, "confirmed_plan_active",
                               {"blocking_plan_id": active["plan_id"],
                                "hint": "已确认方案仍有未完成动作；如需覆盖，必须显式 "
                                        "supersede_active 并填写 reason（会记录 plan.superseded 事件）"})
            actor = None
            if active and body.get("supersede_active"):
                actor = self._actor(body)
                reason = body.get("reason", "").strip()
                if not reason:
                    raise ApiError(400, "supersede_reason_required")
                next_id = f"plan-{self.store.state['counters'].get('plan', 0) + 1:04d}"
                self.store.append("plan.superseded", {
                    "plan_id": active["plan_id"], "new_plan_id": next_id,
                    "reason": reason,
                }, actor={"type": "user", **actor})

            plan = build_plan(self.store.state, self.config, as_of)
            stored_plan = {k: v for k, v in plan.items() if k != "impacts"}
            result = self.store.append("plan.generated",
                                       {"plan": stored_plan, "impacts": plan["impacts"]},
                                       actor={"type": "user", **actor} if actor else None)
            self.store.persist_now()
        return {"plan": plan, "event_seq": result["seq"],
                "superseded_plan_id": active["plan_id"] if active else None}

    def confirm_plan(self, plan_id: str, body: dict) -> dict:
        with self.store.lock:
            plan = self.store.state["plans"].get(plan_id)
            if not plan:
                raise ApiError(404, "unknown_plan", plan_id)
            if plan["status"] == "confirmed":
                return {"plan_id": plan_id, "status": "confirmed",
                        "already_confirmed": True,
                        "confirmed_at": plan.get("confirmed_at")}
            if plan["status"] != "proposed":
                raise ApiError(409, "plan_not_confirmable",
                               {"plan_id": plan_id, "status": plan["status"]})
            actor = self._actor(body)
            if actor["role"] not in ("调度员", "运维人员"):
                raise ApiError(403, "forbidden_role", "只有调度员/运维人员可确认方案")
            result = self.store.append("plan.confirmed",
                                       {"plan_id": plan_id, "actor": actor["id"]},
                                       actor={"type": "user", **actor})
            self.store.persist_now()
            return {"plan_id": plan_id, "status": "confirmed",
                    "event_seq": result["seq"],
                    "actions": [a["action_id"] for a in plan["actions"]],
                    "notifications": [n["notification_id"]
                                      for n in plan.get("notifications", [])]}

    def reject_plan(self, plan_id: str, body: dict) -> dict:
        with self.store.lock:
            plan = self.store.state["plans"].get(plan_id)
            if not plan:
                raise ApiError(404, "unknown_plan", plan_id)
            if plan["status"] != "proposed":
                raise ApiError(409, "plan_not_rejectable",
                               {"plan_id": plan_id, "status": plan["status"]})
            actor = self._actor(body)
            reason = (body.get("reason") or "").strip()
            if not reason:
                raise ApiError(400, "reject_reason_required")
            result = self.store.append("plan.rejected", {
                "plan_id": plan_id, "reason": reason, "actor": actor["id"],
            }, actor={"type": "user", **actor})
            return {"plan_id": plan_id, "status": "rejected",
                    "event_seq": result["seq"]}

    def supersede_plan(self, plan_id: str, body: dict) -> dict:
        with self.store.lock:
            plan = self.store.state["plans"].get(plan_id)
            if not plan:
                raise ApiError(404, "unknown_plan", plan_id)
            if plan["status"] not in ("proposed", "confirmed"):
                raise ApiError(409, "plan_not_supersedeable",
                               {"plan_id": plan_id, "status": plan["status"]})
            actor = self._actor(body)
            reason = (body.get("reason") or "").strip()
            if not reason:
                raise ApiError(400, "supersede_reason_required")
            result = self.store.append("plan.superseded", {
                "plan_id": plan_id, "new_plan_id": body.get("new_plan_id"),
                "reason": reason,
            }, actor={"type": "user", **actor})
            self.store.persist_now()
            return {"plan_id": plan_id, "status": "superseded",
                    "event_seq": result["seq"]}

    # ------------------------------------------------------------ 动作反馈
    def action_feedback(self, action_id: str, body: dict) -> dict:
        with self.store.lock:
            action = self.store.state["actions"].get(action_id)
            if not action:
                raise ApiError(404, "unknown_action", action_id)
            status = body.get("status")
            if status not in ("executed", "rejected", "failed"):
                raise ApiError(400, "bad_feedback_status", status)
            actor = self._actor(body)
            if action["status"] not in ("pending",):
                raise ApiError(409, "action_not_dispatched",
                               {"action_id": action_id, "status": action["status"],
                                "hint": "只有已确认方案下发（pending）的动作可回执"})
            if status == "rejected" and not (body.get("reason") or "").strip():
                raise ApiError(400, "reject_reason_required")
            result = self.store.append("action.feedback", {
                "action_id": action_id, "status": status,
                "detail": body.get("detail") or body.get("reason", ""),
                "actor": actor["id"],
            }, actor={"type": "user", **actor})
            return {"action_id": action_id, "status": status,
                    "event_seq": result["seq"]}

    # ------------------------------------------------------------ 接管
    def takeover(self, body: dict) -> dict:
        with self.store.lock:
            return self._takeover(body)

    def _takeover(self, body: dict) -> dict:
        actor = self._actor(body)
        scope = body.get("scope")
        if scope not in ("zone", "gate"):
            raise ApiError(400, "bad_scope", "scope 必须是 zone 或 gate")
        scope_id = body.get("scope_id")
        known = (self.config.zones if scope == "zone" else self.config.gates)
        if scope_id not in known:
            raise ApiError(404, f"unknown_{scope}", scope_id)
        until = body.get("until")
        if until:
            parse_iso(until)
        result = self.store.append("manual.takeover", {
            "scope": scope, "scope_id": scope_id, "owner": actor["id"],
            "role": actor["role"], "reason": body.get("reason", ""),
            "until": until,
        }, actor={"type": "user", **actor})
        self.store.persist_now()
        return {"accepted": True, **result}

    def release_takeover(self, scope: str, scope_id: str, body: dict) -> dict:
        with self.store.lock:
            actor = self._actor(body)
            key = f"{scope}:{scope_id}"
            if key not in self.store.state["takeovers"]:
                raise ApiError(404, "unknown_takeover", key)
            result = self.store.append("manual.release",
                                       {"scope": scope, "scope_id": scope_id,
                                        "actor": actor["id"]},
                                       actor={"type": "user", **actor})
            return {"released": True, **result}

    # ------------------------------------------------------------ 通知
    def ack_notification(self, notification_id: str, body: dict) -> dict:
        with self.store.lock:
            rec = self.store.state["notifications"].get(notification_id)
            if not rec:
                raise ApiError(404, "unknown_notification", notification_id)
            actor = self._actor(body)
            result = self.store.append("notification.ack", {
                "notification_id": notification_id, "actor": actor["id"],
            }, actor={"type": "user", **actor})
            return {"notification_id": notification_id, "status": "acknowledged",
                    "event_seq": result["seq"]}

    # ------------------------------------------------------------ 读模型
    def sensor_overview(self, query: dict) -> dict:
        as_of = self._as_of(query)
        mult = self.config.params["sensor_stale_multiplier"]
        stations = []
        for sid, meta in self.config.stations.items():
            kind_tol = {
                "level": self.config.params["level_conflict_tolerance_m"],
                "river_level": self.config.params["level_conflict_tolerance_m"],
                "quality": self.config.params["quality_conflict_tolerance_mg_l"],
            }.get(meta["kind"])
            ev = evaluate_station(self.store.state["readings"].get(sid),
                                  meta, as_of, mult, kind_tol)
            stations.append({"station_id": sid, **ev})
        gates = []
        tol = self.config.params["gate_conflict_tolerance_percent"]
        for gid, meta in self.config.gates.items():
            ev = evaluate_gate(
                self.store.state["gate_reports"].get(gid),
                {**meta, "interval_s": GATE_INTERVAL_S}, as_of, mult, tol,
                self.store.state["commands"].get(gid))
            gates.append({"gate_id": gid,
                         "status": ev["status"],
                         "actual_percent_open": ev["actual_percent_open"],
                         "command_percent_open": (ev["command"] or {})
                         .get("command_percent_open"),
                         "conflicts": ev["conflicts"],
                         "telemetry": ev["telemetry"], "manual": ev["manual"],
                         "age_seconds": ev["age_seconds"]})
        return {"as_of": format_iso(as_of), "stations": stations, "gates": gates}

    def pending_actions(self) -> dict:
        actions = [a for a in self.store.state["actions"].values()
                   if a["status"] in ("proposed", "pending")]
        return {"actions": sorted(actions, key=lambda a: a["action_id"])}

    def pending_notifications(self, query: dict) -> dict:
        as_of = self._as_of(query)
        items = list(self.store.state["notifications"].values())
        status = query.get("status", [""])[0]
        if status:
            items = [n for n in items if n["status"] == status]
        overdue = [n for n in items if n["status"] == "pending"
                   and parse_iso(n["deadline_at"]) <= as_of]
        return {"as_of": format_iso(as_of), "notifications": items,
                "overdue": [n["notification_id"] for n in overdue]}

    def storms(self) -> dict:
        return {"storms": sorted(self.store.state["storms"].values(),
                                 key=lambda s: s["started_at"])}

    def storm(self, storm_id: str) -> dict:
        storm = self.store.state["storms"].get(storm_id)
        if not storm:
            raise ApiError(404, "unknown_storm", storm_id)
        frames = [f for f in self.store.state["rain_frames"]
                  .get(storm["station_id"], {}).values()
                  if storm["started_at"] <= f["observed_at"] <= storm["last_frame_at"]]
        return {**storm, "frames": sorted(frames, key=lambda f: f["observed_at"])}

    def management_overview(self, query: dict) -> dict:
        """管理视图：最新方案采用的读数、冲突来源、当前责任人。"""
        as_of = self._as_of(query)
        plans = self.store.state["plans"]
        latest = next(reversed(list(plans.values()))) if plans else None
        takeovers = [t for t in self.store.state["takeovers"].values()
                     if t.get("active")]
        return {
            "as_of": format_iso(as_of),
            "latest_plan": ({"plan_id": latest["plan_id"],
                             "status": latest["status"],
                             "overall_decision": latest["overall_decision"],
                             "overall_warning_level":
                                 latest["overall_warning_level"],
                             "generated_at": latest["generated_at"],
                             "confirmer": latest.get("confirmer")}
                            if latest else None),
            "zones": [
                {"zone_id": z["zone_id"], "zone_name_cn": z["zone_name_cn"],
                 "storm_id": z["storm_id"], "decision": z["decision"],
                 "warning_level": z["warning_level"]["level"],
                 "readings_used": z["readings_used"],
                 "conflicts": z["conflicts"],
                 "current_owner": z["current_owner"],
                 "manual_takeover": z["manual_takeover"]}
                for z in latest["zones"]
            ] if latest else [],
            "active_takeovers": takeovers,
            "pending_actions": sum(1 for a in self.store.state["actions"].values()
                                   if a["status"] in ("proposed", "pending")),
            "pending_notifications": sum(1 for n in
                                         self.store.state["notifications"].values()
                                         if n["status"] == "pending"),
            "impacts": list(self.store.state["impacts"].values()),
        }

    def load_sample(self, body: dict) -> dict:
        with self.store.lock:
            return self._load_sample(body)

    def _load_sample(self, body: dict) -> dict:
        sample_id = body.get("sample_id")
        sample = self.config.storm_samples.get(sample_id)
        if not sample:
            raise ApiError(404, "unknown_sample", sample_id)
        accepted, duplicates, opened = [], [], []
        for i, msg in enumerate(sample["messages"]):
            mid = f"sample:{sample_id}:{i}"
            payload = {
                "message_id": mid, "kind": "rainfall",
                "station_id": sample["station_id"],
                "zone_id": sample["zone_id"],
                "observed_at": msg["observed_at"],
                "rain_mm": msg["rain_mm"], "quality": "ok",
            }
            if mid in self.store.state["dedup"]:
                duplicates.append(mid)
                continue
            result = self.store.append("telemetry.rainfall", payload)
            accepted.append(mid)
            if result.get("storm_opened"):
                opened.append(result["storm_opened"])
        return {"sample_id": sample_id, "accepted": accepted,
                "duplicates": duplicates, "storms_opened": opened}


def _number(body: dict, field: str, low, high) -> float:
    if field not in body:
        raise ApiError(400, "field_required", field)
    try:
        value = float(body[field])
    except (TypeError, ValueError):
        raise ApiError(400, "bad_number", field)
    if low is not None and value < low:
        raise ApiError(400, "number_out_of_range", {field: value, "min": low})
    if high is not None and value > high:
        raise ApiError(400, "number_out_of_range", {field: value, "max": high})
    return value


# ------------------------------------------------------------ HTTP 装配

class Handler(http.server.BaseHTTPRequestHandler):
    service: Service = None

    def _send(self, status: int, payload: dict):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(400, "bad_json", str(exc))
        if not isinstance(body, dict):
            raise ApiError(400, "body_must_be_object")
        return body

    def do_GET(self):  # noqa: N802
        try:
            parsed = urllib.parse.urlparse(self.path)
            path, query = parsed.path, urllib.parse.parse_qs(parsed.query)
            svc = self.service
            if path == "/health":
                self._send(200, {"status": "ok", "service": SERVICE_NAME})
            elif path == "/api/sensors":
                self._send(200, svc.sensor_overview(query))
            elif path == "/api/actions/pending":
                self._send(200, svc.pending_actions())
            elif path == "/api/notifications":
                self._send(200, svc.pending_notifications(query))
            elif path == "/api/storms":
                self._send(200, svc.storms())
            elif path.startswith("/api/storms/"):
                self._send(200, svc.storm(path.rsplit("/", 1)[1]))
            elif path == "/api/plans/latest":
                plans = svc.store.state["plans"]
                if not plans:
                    raise ApiError(404, "no_plan")
                self._send(200, {"plan": next(reversed(list(plans.values())))})
            elif path.startswith("/api/plans/"):
                pid = path.rsplit("/", 1)[1]
                plan = svc.store.state["plans"].get(pid)
                if not plan:
                    raise ApiError(404, "unknown_plan", pid)
                self._send(200, {"plan": plan})
            elif path == "/api/impacts":
                self._send(200, {"impacts": list(
                    svc.store.state["impacts"].values())})
            elif path == "/api/events":
                since = int(query.get("since", ["0"])[0])
                self._send(200, {"events": svc.store.events(since)})
            elif path == "/api/audit/verify":
                self._send(200, svc.store.verify_chain())
            elif path == "/api/management/overview":
                self._send(200, svc.management_overview(query))
            else:
                self._send(404, {"error": "not_found"})
        except ApiError as exc:
            self._send(exc.status, {"error": exc.code, "details": exc.details})
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": "internal_error", "details": str(exc)})

    def do_POST(self):  # noqa: N802
        try:
            parsed = urllib.parse.urlparse(self.path)
            path, query = parsed.path, urllib.parse.parse_qs(parsed.query)
            body = self._read_json()
            svc = self.service
            if path == "/api/telemetry":
                self._send(200, svc.ingest_telemetry(body))
            elif path == "/api/review":
                self._send(200, svc.manual_review(body))
            elif path == "/api/plans/generate":
                self._send(200, svc.generate_plan(body, query))
            elif path.startswith("/api/plans/") and path.endswith("/confirm"):
                pid = path.split("/")[3]
                self._send(200, svc.confirm_plan(pid, body))
            elif path.startswith("/api/plans/") and path.endswith("/reject"):
                pid = path.split("/")[3]
                self._send(200, svc.reject_plan(pid, body))
            elif path.startswith("/api/plans/") and path.endswith("/supersede"):
                pid = path.split("/")[3]
                self._send(200, svc.supersede_plan(pid, body))
            elif path.startswith("/api/actions/") and path.endswith("/feedback"):
                aid = path.split("/")[3]
                self._send(200, svc.action_feedback(aid, body))
            elif path == "/api/takeovers":
                self._send(200, svc.takeover(body))
            elif (path.startswith("/api/takeovers/")
                  and path.endswith("/release")):
                parts = path.split("/")
                # /api/takeovers/<scope>/<id>/release
                self._send(200, svc.release_takeover(parts[3], parts[4], body))
            elif path.startswith("/api/notifications/") and path.endswith("/ack"):
                nid = path.split("/")[3]
                self._send(200, svc.ack_notification(nid, body))
            elif path == "/api/samples/load":
                self._send(200, svc.load_sample(body))
            else:
                self._send(404, {"error": "not_found"})
        except ApiError as exc:
            self._send(exc.status, {"error": exc.code, "details": exc.details})
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": "internal_error", "details": str(exc)})

    def log_message(self, *_args):
        return


def create_server(data_dir: str | None = None):
    config = load_config()
    service = Service(config=config, data_dir=data_dir)
    handler = type("BoundHandler", (Handler,), {"service": service})
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    server = http.server.ThreadingHTTPServer((host, port), handler)
    server.service = service
    return server
