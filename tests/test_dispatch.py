"""溢流调度后端业务测试。"""

import json
import sys
import tempfile
import threading
import unittest
import urllib.request
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from overflow import timeutil  # noqa: E402
from overflow.ingest import QUALITY_STALE, TelemetryError  # noqa: E402
from overflow.service import DispatchService, ServiceError  # noqa: E402
from overflow.store import Store  # noqa: E402


def make_service():
    tmp = tempfile.mkdtemp(prefix="drainage-test-")
    return DispatchService(Store(tmp)), tmp


def rain(svc, sensor, intensity, t, message_id, unit="mm/h"):
    return svc.ingest({"sensor_id": sensor, "value": intensity, "unit": unit,
                       "source_time": timeutil.iso(t), "message_id": message_id,
                       "channel": "scada"}, at=t)


def level(svc, sensor, value, t, mid):
    return svc.ingest({"sensor_id": sensor, "value": value, "unit": "m",
                       "source_time": timeutil.iso(t), "message_id": mid}, at=t)


def stage(svc, sensor, value, t, mid):
    return level(svc, sensor, value, t, mid)


def gate(svc, sensor, mode, t, mid, opening=None):
    msg = {"sensor_id": sensor, "mode": mode, "source_time": timeutil.iso(t),
           "message_id": mid, "channel": "scada"}
    if opening is not None:
        msg["opening_pct"] = opening
    return svc.ingest(msg, at=t)


class TelemetryRulesTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = make_service()
        self.t0 = timeutil.parse("2026-09-22T12:00:00+08:00")

    def test_duplicate_message_does_not_change_state_twice(self):
        msg = {"sensor_id": "G-01", "mode": "CLOSED",
               "source_time": "2026-09-22T12:00:00+08:00", "message_id": "M1"}
        first = self.svc.ingest(msg, at=self.t0)
        second = self.svc.ingest(msg, at=self.t0 + timedelta(seconds=5))
        self.assertFalse(first["duplicated"])
        self.assertTrue(second["duplicated"])
        gate_doc = self.svc.store.state["gates"]["G-01"]
        # 重传不得刷新报告时间
        self.assertEqual(gate_doc["reported_message_id"], "M1")
        types = [e["event_type"] for e in self.svc.store.read_events()]
        self.assertEqual(types.count("telemetry.received"), 1)
        self.assertEqual(types.count("telemetry.duplicate"), 1)

    def test_same_message_id_different_payload_rejected(self):
        base = {"sensor_id": "LV-01", "unit": "m",
                "source_time": "2026-09-22T12:00:00+08:00", "message_id": "M2"}
        self.svc.ingest({**base, "value": 3.5}, at=self.t0)
        with self.assertRaises(TelemetryError):
            self.svc.ingest({**base, "value": 4.2}, at=self.t0)

    def test_missing_sensor_is_unconfirmed_not_zero(self):
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.assertIn("LV-01", plan["unconfirmed_sensors"])
        action = next(a for a in plan["actions"] if a["outfall_id"] == "CSO-01")
        self.assertFalse(action["executable"])
        self.assertIn("level_unconfirmed", action["blocking_reasons"])
        # 绝不能按零液位计算出安全结论
        self.assertFalse(action["overflow"])
        self.assertEqual(action["pump"]["command"], "HOLD")

    def test_stale_sensor_degraded_to_pending_confirmation(self):
        level(self.svc, "LV-01", 3.8, self.t0, "L1")
        result = self.svc.tick(at=self.t0 + timedelta(seconds=301))
        self.assertIn("LV-01", result["staled_sensors"])
        doc = self.svc.store.state["measurements"]["LV-01"]
        self.assertEqual(doc["quality"], QUALITY_STALE)
        plan = self.svc.generate_plan("老城区", at=self.t0 + timedelta(seconds=301))
        self.assertIn("LV-01", plan["unconfirmed_sensors"])

    def test_manual_review_revives_stale_reading(self):
        level(self.svc, "LV-01", 3.8, self.t0, "L1")
        stage(self.svc, "RS-MX", 2.9, self.t0, "R1")
        self.svc.tick(at=self.t0 + timedelta(seconds=301))
        self.svc.submit_review({
            "outfall_id": "CSO-01", "reviewer": "王复核",
            "readings": {"LV-01": {"value": 4.45,
                                   "source_time": timeutil.iso(self.t0 + timedelta(minutes=6))}}},
            at=self.t0 + timedelta(minutes=6))
        plan = self.svc.generate_plan("老城区", at=self.t0 + timedelta(minutes=6))
        adopted = {(r["sensor_id"], r["source"]) for r in plan["readings"] if r["adopted"]}
        self.assertIn(("LV-01", "manual"), adopted)
        action = next(a for a in plan["actions"] if a["outfall_id"] == "CSO-01")
        self.assertTrue(action["executable"])


class RainEventTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = make_service()

    def test_rain_across_midnight_belongs_to_one_event(self):
        t1 = timeutil.parse("2026-09-22T23:50:00+08:00")
        t2 = timeutil.parse("2026-09-23T00:30:00+08:00")
        t3 = timeutil.parse("2026-09-23T01:10:00+08:00")
        r1 = rain(self.svc, "RAIN-LC", 30, t1, "A1")
        r2 = rain(self.svc, "RAIN-LC", 45, t2, "A2")
        r3 = rain(self.svc, "RAIN-LC", 20, t3, "A3")
        self.assertFalse(r1["duplicated"])
        events = list(self.svc.store.state["rain_events"].values())
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["status"], "open")
        self.assertEqual(ev["peak_intensity_mmh"], 45.0)
        # tick 在雨停 6 小时后关闭事件，结束时应标记跨午夜
        self.svc.tick(at=t3 + timedelta(minutes=361))
        ev = self.svc.store.state["rain_events"][ev["id"]]
        self.assertEqual(ev["status"], "closed")
        self.assertTrue(ev["cross_midnight"])

    def test_new_storm_after_gap_starts_new_event(self):
        t1 = timeutil.parse("2026-09-22T20:00:00+08:00")
        rain(self.svc, "RAIN-BJ", 20, t1, "B1")
        t2 = t1 + timedelta(hours=7)
        rain(self.svc, "RAIN-BJ", 50, t2, "B2")
        self.assertEqual(len(self.svc.store.state["rain_events"]), 2)


class PlanLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = make_service()
        self.t0 = timeutil.parse("2026-09-22T18:00:00+08:00")
        rain(self.svc, "RAIN-LC", 45, self.t0, "Q1")
        stage(self.svc, "RS-MX", 3.05, self.t0, "S1")
        level(self.svc, "LV-01", 3.95, self.t0, "V1")
        level(self.svc, "LV-02", 3.55, self.t0, "V2")
        gate(self.svc, "G-01", "CLOSED", self.t0, "W1")
        gate(self.svc, "G-02", "CLOSED", self.t0, "W2")

    def test_plan_is_explainable_with_readings_and_rules(self):
        plan = self.svc.generate_plan("老城区", actor="调度员甲", at=self.t0)
        # 雨强 45mm/h 为橙色，但调蓄余量不足 0.5 小时，规则升级到红色
        self.assertEqual(plan["warning_level"], "红色")
        adopted = {r["sensor_id"] for r in plan["readings"] if r["adopted"]}
        self.assertEqual(adopted, {"RAIN-LC", "RS-MX", "LV-01", "LV-02",
                                   "G-01", "G-02"})
        cso1 = next(a for a in plan["actions"] if a["outfall_id"] == "CSO-01")
        self.assertTrue(cso1["overflow"])
        self.assertIn("R-REGULATED-RELEASE", " ".join(cso1["reasons"]))
        self.assertTrue(plan["impact_zone"]["overflow_active"])
        self.assertTrue(plan["pollution"]["items"])

    def test_confirmed_plan_cannot_be_silently_overwritten(self):
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=self.t0)
        with self.assertRaises(ServiceError):
            self.svc.generate_plan("老城区", at=self.t0 + timedelta(minutes=1))
        new = self.svc.supersede_plan(plan["id"],
                                      {"actor": "调度主任", "reason": "雨强上修"},
                                      at=self.t0 + timedelta(minutes=2))
        self.assertEqual(new["supersedes"], plan["id"])
        self.assertEqual(self.svc.store.state["plans"][plan["id"]]["status"],
                         "superseded")

    def test_gate_telemetry_conflict_blocks_and_records_sources(self):
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=self.t0)
        # 现场却上报 G-01 仍为 CLOSED，与指令 REGULATED 冲突
        gate(self.svc, "G-01", "CLOSED", self.t0 + timedelta(minutes=3), "W3")
        conflict = next(c for c in self.svc.store.state["conflicts"].values())
        self.assertEqual(conflict["status"], "open")
        self.assertEqual(conflict["expected_mode"], "REGULATED")
        # 重传同一冲突消息不得追加来源
        gate(self.svc, "G-01", "CLOSED", self.t0 + timedelta(minutes=3), "W3")
        self.assertEqual(len(conflict["telemetry_sources"]), 1)
        # 下一次方案应体现冲突阻塞
        new = self.svc.supersede_plan(plan["id"],
                                      {"actor": "调度主任", "reason": "复核"},
                                      at=self.t0 + timedelta(minutes=4))
        self.svc.confirm_plan(new["id"], {"actor": "现场钱工"},
                              at=self.t0 + timedelta(minutes=4))
        action = next(a for a in new["actions"] if a["outfall_id"] == "CSO-01")
        self.assertIn("gate_conflict", action["blocking_reasons"])

    def test_execute_idempotent_and_takeover_governs_owner(self):
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=self.t0)
        cmds = {c["kind"]: c for c in plan["commands"]
                if c["outfall_id"] == "CSO-01"}
        r1 = self.svc.execute_command(cmds["gate"]["id"],
                                      {"actor": "现场钱工", "request_id": "REQ-1"},
                                      at=self.t0 + timedelta(minutes=2))
        self.assertFalse(r1["idempotent"])
        r2 = self.svc.execute_command(cmds["gate"]["id"],
                                      {"actor": "现场钱工", "request_id": "REQ-1"},
                                      at=self.t0 + timedelta(minutes=3))
        self.assertTrue(r2["idempotent"])
        with self.assertRaises(ServiceError):
            self.svc.execute_command(cmds["gate"]["id"],
                                     {"actor": "现场钱工", "request_id": "REQ-2"},
                                     at=self.t0 + timedelta(minutes=4))

        # CSO-02 被人工接管后，非接管人的执行必须被拒绝并入事件链
        self.svc.takeover("CSO-02", {"actor": "赵班长", "note": "现场操作权上收"},
                          at=self.t0)
        g02 = next(c for c in plan["commands"]
                   if c["outfall_id"] == "CSO-02" and c["kind"] == "gate")
        rejected = self.svc.execute_command(g02["id"], {"actor": "现场钱工"},
                                            at=self.t0 + timedelta(minutes=5))
        self.assertTrue(rejected["rejected"])
        self.assertEqual(g02["status"], "rejected")
        # 显式拒绝已终结动作也要报错
        with self.assertRaises(ServiceError):
            self.svc.reject_command(g02["id"],
                                    {"actor": "赵班长", "reason": "x"},
                                    at=self.t0)

    def test_execution_rejection_and_completion_chain(self):
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=self.t0)
        pump = next(c for c in plan["commands"] if c["kind"] == "pump")
        self.svc.reject_command(pump["id"],
                                {"actor": "赵班长", "reason": "泵组故障，已切备用泵"},
                                at=self.t0 + timedelta(minutes=2))
        for cmd in plan["commands"]:
            if cmd["status"] == "dispatched":
                self.svc.execute_command(cmd["id"], {"actor": "现场钱工"},
                                         at=self.t0 + timedelta(minutes=3))
        chain = self.svc.plan_chain(plan["id"])
        types = [e["event_type"] for e in chain]
        self.assertIn("action.rejected", types)
        self.assertEqual(types[-1], "plan.completed")
        # 链上每条事件都能回溯到上一条
        seqs = [(e["event_seq"], e["chain_after"]) for e in chain]
        for seq, after in seqs[1:]:
            self.assertIsNotNone(after)
            self.assertLess(after, seq)


class PersistenceTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.data_dir = make_service()
        self.t0 = timeutil.parse("2026-09-22T18:00:00+08:00")
        rain(self.svc, "RAIN-LC", 45, self.t0, "Q1")
        stage(self.svc, "RS-MX", 3.05, self.t0, "S1")
        level(self.svc, "LV-01", 3.95, self.t0, "V1")
        level(self.svc, "LV-02", 3.55, self.t0, "V2")
        gate(self.svc, "G-01", "CLOSED", self.t0, "W1")
        gate(self.svc, "G-02", "CLOSED", self.t0, "W2")
        plan = self.svc.generate_plan("老城区", at=self.t0)
        self.plan_id = plan["id"]
        self.svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=self.t0)

    def test_restart_recovers_pending_actions_notices_and_impacts(self):
        first_cmd = self.svc.store.state["plans"][self.plan_id]["commands"][0]
        self.svc.execute_command(first_cmd["id"], {"actor": "现场钱工"},
                                 at=self.t0 + timedelta(minutes=2))
        pending_before = len(self.svc.pending_actions())
        self.assertGreater(pending_before, 0)

        revived = DispatchService(Store(self.data_dir))
        self.assertEqual(len(revived.pending_actions()), pending_before)
        notices = revived.list_notifications()
        self.assertTrue(notices)
        for n in notices:
            self.assertIn("deadline_at", n)
        overdue = revived.list_notifications(
            at=self.t0 + timedelta(hours=5))
        self.assertTrue(all(n.get("overdue") for n in overdue
                            if n["status"] == "pending"))
        impacts = revived.impacts(active_only=True)
        self.assertEqual(len(impacts), 1)
        self.assertIn("plume_reach_km", impacts[0])
        # 事件链文件仍可完整回放
        self.assertTrue(revived.plan_chain(self.plan_id))
        # 重启后重传旧遥测仍然去重
        dup = revived.ingest({"sensor_id": "G-01", "mode": "CLOSED",
                              "source_time": timeutil.iso(self.t0),
                              "message_id": "W1"},
                             at=self.t0 + timedelta(hours=5))
        self.assertTrue(dup["duplicated"])


class OverviewTest(unittest.TestCase):
    def test_overview_shows_readings_conflict_sources_and_owner(self):
        svc, _ = make_service()
        t0 = timeutil.parse("2026-09-22T18:00:00+08:00")
        rain(svc, "RAIN-LC", 45, t0, "Q1")
        stage(svc, "RS-MX", 3.05, t0, "S1")
        level(svc, "LV-01", 3.95, t0, "V1")
        level(svc, "LV-02", 3.55, t0, "V2")
        gate(svc, "G-01", "CLOSED", t0, "W1")
        gate(svc, "G-02", "CLOSED", t0, "W2")
        plan = svc.generate_plan("老城区", at=t0)
        svc.confirm_plan(plan["id"], {"actor": "现场钱工"}, at=t0)
        gate(svc, "G-01", "CLOSED", t0 + timedelta(minutes=3), "W3")
        svc.takeover("CSO-02", {"actor": "赵班长"}, at=t0)

        view = svc.overview(at=t0 + timedelta(minutes=4))
        entry = view["plans"][plan["id"]]
        self.assertTrue(entry["adopted_readings"])
        self.assertEqual(entry["confirmed_by"], "现场钱工")
        conflict = view["conflicts"][0]
        self.assertTrue(conflict["sources"])
        self.assertEqual(conflict["sources"][0]["message_id"], "W3")
        self.assertEqual(view["responsibility"]["CSO-01"]["name"], "现场钱工")
        self.assertEqual(view["responsibility"]["CSO-02"]["basis"],
                         "manual_takeover")
        self.assertEqual(view["responsibility"]["CSO-02"]["name"], "赵班长")
        # CSO-03 无方案无接管，落到汛期值班表
        self.assertEqual(view["responsibility"]["CSO-03"]["basis"],
                         "duty_roster")


class HttpApiTest(unittest.TestCase):
    """HTTP 冒烟：服务通过端口暴露关键契约。"""

    def setUp(self):
        from app import create_server
        self.tmp = tempfile.mkdtemp(prefix="drainage-http-")
        self.server = create_server(data_dir=self.tmp, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def _req(self, method, path, body=None):
        from urllib.parse import quote
        path = quote(path, safe="/?=&")
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_reference_and_telemetry_flow(self):
        status, payload = self._req("GET", "/api/reference")
        self.assertEqual(status, 200)
        self.assertIn("STORM-20210725", {s["id"] for s in payload["storm_samples"]})

        t = "2026-09-22T18:00:00+08:00"
        for msg in [
            {"sensor_id": "RAIN-LC", "value": 45, "unit": "mm/h",
             "source_time": t, "message_id": "Q1"},
            {"sensor_id": "RS-MX", "value": 3.05, "unit": "m",
             "source_time": t, "message_id": "S1"},
            {"sensor_id": "LV-01", "value": 3.95, "unit": "m",
             "source_time": t, "message_id": "V1"},
            {"sensor_id": "LV-02", "value": 3.55, "unit": "m",
             "source_time": t, "message_id": "V2"},
            {"sensor_id": "G-01", "mode": "CLOSED",
             "source_time": t, "message_id": "W1"},
            {"sensor_id": "G-02", "mode": "CLOSED",
             "source_time": t, "message_id": "W2"},
        ]:
            status, _ = self._req("POST", "/api/telemetry", msg)
            self.assertEqual(status, 200)

        status, plan = self._req("POST", "/api/districts/老城区/plans", {})
        self.assertEqual(status, 201)
        status, confirmed = self._req(
            "POST", f"/api/plans/{plan['id']}/confirm", {"actor": "现场钱工"})
        self.assertEqual(status, 200)
        status, pending = self._req("GET", "/api/actions/pending")
        self.assertEqual(status, 200)
        self.assertTrue(pending["pending"])
        status, chain = self._req("GET", f"/api/plans/{plan['id']}/chain")
        self.assertEqual(status, 200)
        self.assertTrue(chain["events"])


if __name__ == "__main__":
    unittest.main()
