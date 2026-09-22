"""HTTP 路由（标准库实现，无第三方依赖）。

路由总览：
  POST /api/telemetry                 遥测入库（幂等去重/失联判定在入库时完成）
  POST /api/tick                      触发失联降级与雨停关事件（可带 at 回放）
  POST /api/reviews                   人工复核读数/闸位
  POST /api/warnings                  人工发布预警
  GET  /api/sensors                   传感器当前视图（含失联/待确认）
  GET  /api/rain-events               降雨事件（跨午夜同事件可在此核对）
  POST /api/districts/{d}/plans       生成调度方案
  GET  /api/plans[?district=]         方案列表
  GET  /api/plans/{id}                方案详情（采用读数/规则解释/校核）
  POST /api/plans/{id}/confirm        现场确认（派发动作/通知/影响范围）
  POST /api/plans/{id}/supersede      显式取代已确认方案
  GET  /api/plans/{id}/chain          方案事件链
  POST /api/commands/{id}/execute     动作执行回执（request_id 幂等）
  POST /api/commands/{id}/reject      动作拒绝
  GET  /api/actions/pending           重启后仍未完成的动作
  POST /api/outfalls/{id}/takeover    人工接管
  DELETE /api/outfalls/{id}/takeover  解除接管
  GET  /api/conflicts                 冲突清单（含遥测来源）
  POST /api/conflicts/{id}/resolve    冲突消解
  GET  /api/notifications             通知与截止时间（?status=pending&overdue=1）
  POST /api/notifications/{id}/ack    通知签收
  GET  /api/impacts[?river=&active=1] 河道影响范围
  POST /api/impacts/{id}/close        关闭影响记录
  GET  /api/overview                  管理总览：读数/冲突来源/责任人
  GET  /api/reference                 静态台账与历史暴雨样例
  GET  /api/events                    原始事件链（?type=&chain_key=）
"""

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

from . import SERVICE_NAME, reference, timeutil
from .ingest import TelemetryError, sensor_snapshot
from .service import DispatchService, ServiceError
from .store import Store


def _json_default(obj):
    return str(obj)


class Handler(BaseHTTPRequestHandler):
    service = None  # create_server 时注入

    # ---------- 基础设施 ----------
    def _send(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False,
                          default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise _HttpError(400, "请求体不是合法 JSON")
        if not isinstance(body, dict):
            raise _HttpError(400, "请求体必须是 JSON 对象")
        return body

    def _at(self, body):
        value = body.pop("at", None)
        return timeutil.parse(value) if value else None

    def _handle_errors(self, fn):
        try:
            return fn()
        except _HttpError as exc:
            self._send(exc.status, {"error": exc.message})
        except (ServiceError, TelemetryError, ValueError) as exc:
            self._send(400, {"error": str(exc)})

    def do_GET(self):  # noqa: N802
        self._handle_errors(lambda: self._route("GET"))

    def do_POST(self):  # noqa: N802
        self._handle_errors(lambda: self._route("POST"))

    def do_DELETE(self):  # noqa: N802
        self._handle_errors(lambda: self._route("DELETE"))

    def log_message(self, *_args):
        return

    # ---------- 路由 ----------
    def _route(self, method):
        path = unquote(self.path.split("?", 1)[0]).rstrip("/") or "/"
        query = {}
        if "?" in self.path:
            for pair in self.path.split("?", 1)[1].split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    query[k] = v
        svc = self.service
        body = self._read_body() if method in ("POST", "DELETE") else {}
        at = self._at(body)

        if method == "GET" and path == "/health":
            return self._send(200, {"status": "ok", "service": SERVICE_NAME})

        if method == "POST" and path == "/api/telemetry":
            return self._send(200, svc.ingest(body, at=at))
        if method == "POST" and path == "/api/tick":
            return self._send(200, svc.tick(at=at))
        if method == "POST" and path == "/api/reviews":
            return self._send(201, svc.submit_review(body, at=at))
        if method == "POST" and path == "/api/warnings":
            return self._send(201, svc.issue_warning(body, at=at))
        if method == "GET" and path == "/api/sensors":
            return self._send(200, {"sensors": __import__(
                "overflow.ingest", fromlist=["sensor_snapshot"]).sensor_snapshot(
                svc.store, at=at)})
        if method == "GET" and path == "/api/rain-events":
            with svc.store.lock:
                events = list(svc.store.state["rain_events"].values())
            return self._send(200, {"rain_events": events})
        if method == "GET" and re.fullmatch(r"/api/rain-events/[\w-]+", path):
            eid = path.rsplit("/", 1)[-1]
            with svc.store.lock:
                ev = svc.store.state["rain_events"].get(eid)
            if not ev:
                raise _HttpError(404, "降雨事件不存在")
            return self._send(200, ev)

        m = re.fullmatch(r"/api/districts/([^/]+)/plans", path)
        if method == "POST" and m:
            plan = svc.generate_plan(m.group(1), actor=body.pop("actor", None),
                                     at=at, force=bool(body.pop("force", False)))
            return self._send(201, plan)

        if method == "GET" and path == "/api/plans":
            with svc.store.lock:
                plans = [p for p in svc.store.state["plans"].values()
                         if not query.get("district")
                         or p["district"] == query["district"]]
            return self._send(200, {"plans": plans})

        if method == "GET" and re.fullmatch(r"/api/plans/[\w-]+", path):
            pid = path.rsplit("/", 1)[-1]
            plan = svc.store.state["plans"].get(pid)
            if not plan:
                raise _HttpError(404, "方案不存在")
            return self._send(200, plan)
        if method == "POST" and re.fullmatch(r"/api/plans/[\w-]+/confirm", path):
            return self._send(200, svc.confirm_plan(path.split("/")[-2], body, at=at))
        if method == "POST" and re.fullmatch(r"/api/plans/[\w-]+/supersede", path):
            return self._send(201, svc.supersede_plan(path.split("/")[-2], body, at=at))
        if method == "GET" and re.fullmatch(r"/api/plans/[\w-]+/chain", path):
            return self._send(200, {"events": svc.plan_chain(path.split("/")[-2])})

        if method == "GET" and path == "/api/actions/pending":
            return self._send(200, {"pending": svc.pending_actions()})
        if method == "POST" and re.fullmatch(r"/api/commands/[\w-]+/execute", path):
            return self._send(200, svc.execute_command(path.split("/")[-2], body, at=at))
        if method == "POST" and re.fullmatch(r"/api/commands/[\w-]+/reject", path):
            return self._send(200, svc.reject_command(path.split("/")[-2], body, at=at))

        if method == "POST" and re.fullmatch(r"/api/outfalls/[^/]+/takeover", path):
            return self._send(201, svc.takeover(path.split("/")[-2], body, at=at))
        if method == "DELETE" and re.fullmatch(r"/api/outfalls/[^/]+/takeover", path):
            return self._send(200, svc.release_takeover(path.split("/")[-2], body, at=at))

        if method == "GET" and path == "/api/conflicts":
            with svc.store.lock:
                conflicts = list(svc.store.state["conflicts"].values())
            return self._send(200, {"conflicts": conflicts})
        if method == "POST" and re.fullmatch(r"/api/conflicts/[\w-]+/resolve", path):
            return self._send(200, svc.resolve_conflict(path.split("/")[-2], body, at=at))

        if method == "GET" and path == "/api/notifications":
            notes = svc.list_notifications(status=query.get("status"), at=at)
            if query.get("overdue") == "1":
                notes = [n for n in notes if n.get("overdue")]
            return self._send(200, {"notifications": notes})
        if method == "POST" and re.fullmatch(r"/api/notifications/[\w-]+/ack", path):
            return self._send(200, svc.ack_notification(path.split("/")[-2], body, at=at))

        if method == "GET" and path == "/api/impacts":
            return self._send(200, {"impacts": svc.impacts(
                river=query.get("river"),
                active_only=query.get("active") == "1")})
        if method == "POST" and re.fullmatch(r"/api/impacts/[\w-]+/close", path):
            return self._send(200, svc.close_impact(path.split("/")[-2], body, at=at))

        if method == "GET" and path == "/api/overview":
            return self._send(200, svc.overview(at=at))
        if method == "GET" and path == "/api/reference":
            return self._send(200, {"catalog_version": reference.CATALOG_VERSION,
                                    "outfalls": reference.OUTFALLS,
                                    "districts": reference.DISTRICTS,
                                    "river_stages": reference.RIVER_STAGES,
                                    "pollution_thresholds": reference.POLLUTION_THRESHOLDS,
                                    "warning_levels": reference.WARNING_LEVELS,
                                    "storm_samples": reference.STORM_SAMPLES,
                                    "sensors": reference.SENSORS})
        if method == "GET" and path == "/api/events":
            events = svc.store.read_events()
            if query.get("type"):
                events = [e for e in events if e["event_type"] == query["type"]]
            if query.get("chain_key"):
                events = [e for e in events if e.get("chain_key") == query["chain_key"]]
            return self._send(200, {"events": events})

        self._send(404, {"error": "not_found"})


class _HttpError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def create_server(data_dir=None, host=None, port=None):
    import os
    data_dir = data_dir or os.environ.get("DRAINAGE_DATA_DIR", ".runtime/data")
    host = host or os.environ.get("HOST", "0.0.0.0")
    port = int(port or os.environ.get("PORT", "8000"))
    store = Store(data_dir)
    service = DispatchService(store)

    handler = type("BoundHandler", (Handler,), {"service": service})
    return ThreadingHTTPServer((host, port), handler)
