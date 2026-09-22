"""溢流调度后端业务测试。

每个用例使用独立临时 DATA_DIR，不依赖主机隐藏状态。
"""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cso.service import ApiError, Service, create_server  # noqa: E402

T0 = "2026-09-22T20:00:00+08:00"
ACTOR_DISPATCHER = {"id": "disp1", "name": "张调度", "role": "调度员"}
ACTOR_OP = {"id": "op1", "name": "李运维", "role": "运维人员"}
ACTOR_REG = {"id": "reg1", "name": "王监管", "role": "监管人员"}


class Case(Service):
    """测试夹具：临时数据目录 + 便捷投递。"""

    @classmethod
    def fresh(cls):
        tmp = tempfile.mkdtemp(prefix="cso-test-")
        return cls(data_dir=tmp), tmp

    def tel(self, mid, kind, target, t, field, value):
        sid = None if kind == "gate" else target
        gid = target if kind == "gate" else None
        body = {"message_id": mid, "kind": kind, "observed_at": t,
                field: value}
        if sid:
            body["station_id"] = sid
        if gid:
            body["gate_id"] = gid
        return self.ingest_telemetry(body)

    def gate_tel(self, mid, gid, t, percent):
        return self.ingest_telemetry(
            {"message_id": mid, "kind": "gate", "gate_id": gid,
             "observed_at": t, "percent_open": percent})

    def review_gate(self, gid, percent, t=T0, actor=None, reason="复核"):
        return self.manual_review(
            {"actor": actor or ACTOR_OP, "kind": "gate", "gate_id": gid,
             "observed_at": t, "percent_open": percent, "reason": reason})

    def plan(self, as_of=T0, **body):
        return self.generate_plan(body, {"as_of": [as_of]})["plan"]


class StormGroupingTest(unittest.TestCase):
    def test_rain_across_midnight_is_one_event_gap_opens_another(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        r = svc.load_sample({"sample_id": "sample-typhoon-cross-midnight"})
        self.assertEqual(r["storms_opened"], ["storm-0001", "storm-0002"])
        storms = svc.storms()["storms"]
        self.assertEqual(len(storms), 2)
        self.assertTrue(storms[0]["started_at"].startswith("2025-08-22T23:10"))
        self.assertTrue(storms[0]["last_frame_at"].startswith("2025-08-23T00:40"))
        frames = svc.storm("storm-0001")["frames"]
        self.assertEqual(len(frames), 5)  # 跨午夜的连续 5 帧同属一个事件
        self.assertTrue(storms[1]["started_at"].startswith("2025-08-23T03:25"))

    def test_short_thunderstorm_is_single_event(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        r = svc.load_sample({"sample_id": "sample-afternoon-thunderstorm"})
        self.assertEqual(r["storms_opened"], ["storm-0001"])


class IdempotencyTest(unittest.TestCase):
    def test_retransmitted_gate_message_does_not_change_state(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        first = svc.gate_tel("m1", "G1", T0, 30)
        self.assertFalse(first["duplicate"])
        dup = svc.gate_tel("m1", "G1", T0, 90)  # 重传且内容被篡改
        self.assertTrue(dup["duplicate"])
        self.assertEqual(dup["first_result"]["percent_open"], 30)
        report = svc.store.state["gate_reports"]["G1"]["telemetry"]
        self.assertEqual(report["percent_open"], 30)
        # 事件链里只有一帧闸门遥测
        gate_events = [e for e in svc.store.events() if e["type"] == "telemetry.gate"]
        self.assertEqual(len(gate_events), 1)

    def test_retransmitted_rain_is_ignored(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("r1", "rainfall", "rain-east", T0, "rain_mm", 5)
        dup = svc.tel("r1", "rainfall", "rain-east", T0, "rain_mm", 500)
        self.assertTrue(dup["duplicate"])
        frames = svc.store.state["rain_frames"]["rain-east"]
        self.assertEqual(frames[T0]["rain_mm"], 5)

    def test_message_id_required(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        with self.assertRaises(ApiError) as cm:
            svc.ingest_telemetry({"kind": "gate", "gate_id": "G1",
                                  "observed_at": T0, "percent_open": 0})
        self.assertEqual(cm.exception.code, "message_id_required")


class SensorDegradationTest(unittest.TestCase):
    def test_missing_level_is_unconfirmed_not_zero(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.gate_tel("g", "G1", T0, 0)
        plan = svc.plan()
        z = plan["zones"][0]
        self.assertEqual(z["decision"], "needs_confirmation")
        self.assertIsNone(z["hydrology"]["level_m"])
        self.assertIsNone(z["hydrology"]["storage_remaining_m3"])
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertTrue(gate["requires_confirmation"])

    def test_stale_sensor_degrades(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        # 液位帧停留在 4 分钟前，interval 60s × 3 = 180s，已超时
        svc.tel("l", "level", "lvl-east-well",
                "2026-09-22T19:56:00+08:00", "level_m", 3.9)
        overview = svc.sensor_overview({"as_of": [T0]})
        lvl = next(s for s in overview["stations"] if s["station_id"] == "lvl-east-well")
        self.assertEqual(lvl["status"], "unconfirmed")
        self.assertIsNone(lvl["adopted_value"])

    def test_quality_flag_suspect(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.ingest_telemetry({"message_id": "q", "kind": "quality",
                              "station_id": "qual-east", "observed_at": T0,
                              "cod_mg_l": 12, "quality": "suspect"})
        overview = svc.sensor_overview({"as_of": [T0]})
        q = next(s for s in overview["stations"] if s["station_id"] == "qual-east")
        self.assertEqual(q["status"], "suspect")
        self.assertIsNone(q["adopted_value"])


class GateConflictTest(unittest.TestCase):
    def _setup(self, svc):
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 80)
        svc.gate_tel("g", "G1", T0, 0)

    def test_conflicting_manual_review_holds_for_confirmation(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._setup(svc)
        svc.review_gate("G1", 100, reason="现场目测全开，与PLC不符")
        plan = svc.plan()
        z = plan["zones"][0]
        self.assertEqual(z["decision"], "needs_confirmation")
        conflict = next(c for c in z["conflicts"] if c["type"] == "telemetry_vs_manual")
        self.assertEqual(conflict["telemetry_value"], 0)
        self.assertEqual(conflict["manual_value"], 100)
        self.assertEqual(conflict["reviewer"], "op1")
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertEqual(gate["status"], "needs_confirmation")
        self.assertEqual(gate["owner"]["role"], "调度员")

    def test_conflict_cleared_after_correct_review(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._setup(svc)
        svc.review_gate("G1", 100)
        svc.review_gate("G1", 0, reason="二次复核确为全关")
        plan = svc.plan()
        z = plan["zones"][0]
        self.assertEqual(z["decision"], "controlled_spill")


class PlanLogicTest(unittest.TestCase):
    def _normal_inputs(self, svc, rain=0.0, level=1.0, river=2.0, cod=20):
        def zone(prefix, rain_sid, lvl_sid, rv_sid, q_sid, gid):
            if rain:
                svc.tel(f"r-{prefix}", "rainfall", rain_sid, T0, "rain_mm", rain)
            svc.tel(f"l-{prefix}", "level", lvl_sid, T0, "level_m", level)
            svc.tel(f"rv-{prefix}", "river_level", rv_sid, T0, "level_m", river)
            svc.tel(f"q-{prefix}", "quality", q_sid, T0, "cod_mg_l", cod)
            svc.gate_tel(f"g-{prefix}", gid, T0, 0)
        zone("e", "rain-east", "lvl-east-well", "river-east", "qual-east", "G1")
        zone("w", "rain-west", "lvl-west-well", "river-west", "qual-west", "G2")

    def test_dry_weather_keeps_gate_closed(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc)
        plan = svc.plan()
        self.assertEqual(plan["overall_decision"], "hold_closed")
        for gate in (a for a in plan["actions"] if a["kind"] == "gate"):
            self.assertEqual(gate["target_percent_open"], 0)

    def test_polluted_non_emergency_protects_river(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        # 有降雨超量，但液位仅 87.5%（未到 95% 应急临界），COD 超阈值
        self._normal_inputs(svc, rain=50, level=3.5, river=2.0, cod=80)
        plan = svc.plan()
        z = plan["zones"][0]
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertEqual(gate["target_percent_open"], 0)
        self.assertTrue(any("优先保护河道" in x for x in gate["rationale"]))

    def test_emergency_level_with_pollution_requires_regulator_confirm(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc, rain=50, level=3.9, river=2.0, cod=80)
        plan = svc.plan()
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertGreater(gate["target_percent_open"], 0)
        self.assertTrue(gate["requires_confirmation"])
        self.assertIn("regulator_confirm_before_open", gate["constraints"])
        reg = [n for n in plan["notifications"] if n["kind"] == "regulator_cso"]
        self.assertEqual(reg[0]["recipient_role"], "监管人员")

    def test_river_backwater_closes_gate(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc, rain=50, level=2.0, river=3.6, cod=20)
        plan = svc.plan()
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertEqual(gate["target_percent_open"], 0)
        self.assertTrue(any("回水" in x for x in gate["rationale"]))

    def test_backwater_plus_near_full_escalates(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc, rain=50, level=3.9, river=3.6, cod=20)
        plan = svc.plan()
        self.assertEqual(plan["overall_decision"], "emergency_escalation")

    def test_river_caution_reduces_opening(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        # 3.25m 同时落在 R1（3.2~3.5）与 R2（3.0~3.3）警戒区间
        self._normal_inputs(svc, rain=50, level=3.9, river=3.25, cod=20)
        plan_caution = svc.plan()
        for z in plan_caution["zones"]:
            gate_c = next(a for a in plan_caution["actions"]
                          if a["zone_id"] == z["zone_id"] and a["kind"] == "gate")
            self.assertTrue(any("折减" in x for x in gate_c["rationale"]),
                            z["zone_id"])

    def test_warning_levels_and_notification_deadlines(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc, rain=45, level=3.9, river=2.0, cod=80)
        plan = svc.plan()
        self.assertEqual(plan["overall_warning_level"], "red")
        for n in plan["notifications"]:
            self.assertIn("deadline_at", n)
        reg = next(n for n in plan["notifications"] if n["kind"] == "regulator_cso")
        self.assertEqual(reg["sla_minutes"], 15)  # 红色/监管 15 分钟

    def test_plan_explains_readings_used(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        self._normal_inputs(svc, rain=12, level=2.0, river=2.0)
        plan = svc.plan()
        z = plan["zones"][0]
        used = z["readings_used"]
        self.assertEqual(used["level"]["basis"], "telemetry")
        self.assertEqual(used["level"]["adopted_value"], 2.0)
        self.assertEqual(used["rainfall"]["rain_1h_mm"], 12.0)
        self.assertIsNotNone(z["hydrology"]["inflow_volume_m3"])


class ConfirmationProtectionTest(unittest.TestCase):
    def test_confirmed_plan_cannot_be_silently_overwritten(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        plan = svc.plan()
        svc.confirm_plan(plan["plan_id"], {"actor": ACTOR_DISPATCHER})
        with self.assertRaises(ApiError) as cm:
            svc.plan()
        self.assertEqual(cm.exception.code, "confirmed_plan_active")
        self.assertEqual(cm.exception.details["blocking_plan_id"], plan["plan_id"])

    def test_explicit_supersede_records_event_and_cancels_actions(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        p1 = svc.plan()
        svc.confirm_plan(p1["plan_id"], {"actor": ACTOR_DISPATCHER})
        pending_before = len(svc.pending_actions()["actions"])
        self.assertGreater(pending_before, 0)
        p2 = svc.plan(supersede_active=True, reason="台风路径北抬，重算",
                      actor=ACTOR_DISPATCHER)
        old = svc.store.state["plans"][p1["plan_id"]]
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["superseded_by"], p2["plan_id"])
        # 旧方案未完成动作已取消，不会与新方案双重下发
        self.assertTrue(all(a["status"] == "cancelled"
                            for a in old["actions"]
                            if a["status"] == "cancelled" or True))
        sup = [e for e in svc.store.events() if e["type"] == "plan.superseded"]
        self.assertEqual(sup[-1]["payload"]["reason"], "台风路径北抬，重算")

    def test_confirm_idempotent(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 1.0)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        p = svc.plan()
        svc.confirm_plan(p["plan_id"], {"actor": ACTOR_DISPATCHER})
        again = svc.confirm_plan(p["plan_id"], {"actor": ACTOR_DISPATCHER})
        self.assertTrue(again["already_confirmed"])

    def test_only_dispatch_role_can_confirm(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 1.0)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        p = svc.plan()
        with self.assertRaises(ApiError) as cm:
            svc.confirm_plan(p["plan_id"], {"actor": ACTOR_REG})
        self.assertEqual(cm.exception.code, "forbidden_role")


class ActionFeedbackTest(unittest.TestCase):
    def _confirmed_plan(self, svc):
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        plan = svc.plan()
        svc.confirm_plan(plan["plan_id"], {"actor": ACTOR_DISPATCHER})
        return plan

    def test_execute_and_reject_are_recorded_on_chain(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        plan = self._confirmed_plan(svc)
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        svc.action_feedback(gate["action_id"],
                            {"actor": ACTOR_OP, "status": "executed",
                             "detail": "G1 开至 46%"})
        pump = next(a for a in plan["actions"] if a["kind"] == "pump")
        svc.action_feedback(pump["action_id"],
                            {"actor": ACTOR_OP, "status": "rejected",
                             "reason": "P1 故障，无法启动"})
        rec_gate = svc.store.state["actions"][gate["action_id"]]
        rec_pump = svc.store.state["actions"][pump["action_id"]]
        self.assertEqual(rec_gate["status"], "executed")
        self.assertEqual(rec_pump["status"], "rejected")
        self.assertEqual(rec_pump["history"][0]["detail"], "P1 故障，无法启动")
        kinds = [e["type"] for e in svc.store.events()]
        self.assertIn("action.feedback", kinds)

    def test_reject_requires_reason(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        plan = self._confirmed_plan(svc)
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        with self.assertRaises(ApiError) as cm:
            svc.action_feedback(gate["action_id"],
                                {"actor": ACTOR_OP, "status": "rejected"})
        self.assertEqual(cm.exception.code, "reject_reason_required")

    def test_proposed_action_cannot_receive_feedback(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 1.0)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        plan = svc.plan()  # 未确认，动作仍是 proposed
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        with self.assertRaises(ApiError) as cm:
            svc.action_feedback(gate["action_id"],
                                {"actor": ACTOR_OP, "status": "executed"})
        self.assertEqual(cm.exception.code, "action_not_dispatched")


class TakeoverTest(unittest.TestCase):
    def test_zone_takeover_blocks_automatic_gate(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        svc.takeover({"actor": ACTOR_DISPATCHER, "scope": "zone",
                      "scope_id": "Z1", "reason": "台风现场指挥"})
        plan = svc.plan()
        z = plan["zones"][0]
        self.assertEqual(z["decision"], "manual_takeover")
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        self.assertEqual(gate["status"], "manual")
        self.assertEqual(gate["owner"]["id"], "disp1")
        self.assertEqual(z["current_owner"]["source"], "manual_takeover")

    def test_takeover_release_and_expiry(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 1.0)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 20)
        svc.gate_tel("g", "G1", T0, 0)
        svc.takeover({"actor": ACTOR_DISPATCHER, "scope": "gate",
                      "scope_id": "G1", "until": "2026-09-22T21:00:00+08:00"})
        # 未到期：接管生效
        self.assertEqual(svc.plan(as_of=T0)["zones"][0]["decision"],
                         "manual_takeover")
        # 到期后自动失效（事件仍保留）
        plan_after = svc.plan(as_of="2026-09-22T21:30:00+08:00")
        self.assertNotEqual(plan_after["zones"][0]["decision"], "manual_takeover")
        # 显式解除也落事件
        svc.takeover({"actor": ACTOR_DISPATCHER, "scope": "gate",
                      "scope_id": "G1"})
        svc.release_takeover("gate", "G1", {"actor": ACTOR_DISPATCHER})
        kinds = [e["type"] for e in svc.store.events()]
        self.assertIn("manual.release", kinds)


class PersistenceTest(unittest.TestCase):
    def test_restart_restores_actions_notifications_impacts_chain(self):
        tmp = tempfile.mkdtemp(prefix="cso-test-restart-")
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc = Case(data_dir=tmp)
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 45)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 80)
        svc.gate_tel("g", "G1", T0, 0)
        plan = svc.plan()
        svc.confirm_plan(plan["plan_id"], {"actor": ACTOR_DISPATCHER})
        gate = next(a for a in plan["actions"] if a["kind"] == "gate")
        svc.action_feedback(gate["action_id"],
                            {"actor": ACTOR_OP, "status": "executed"})
        svc.takeover({"actor": ACTOR_DISPATCHER, "scope": "zone",
                      "scope_id": "Z2", "reason": "西区人工指挥"})
        chain_head = svc.store.verify_chain()

        # 重启：新 Store 从快照+事件链重建
        svc2 = Case(data_dir=tmp)
        self.assertEqual(svc2.store.verify_chain(), chain_head)
        pending = svc2.pending_actions()["actions"]
        self.assertTrue(any(a["plan_id"] == plan["plan_id"] for a in pending))
        ntfs = svc2.pending_notifications({"as_of": [T0]})
        self.assertTrue(any(n["deadline_at"] for n in ntfs["notifications"]))
        impacts = svc2.store.state["impacts"]
        self.assertTrue(any(v["river_reach_id"] == "R1" for v in impacts.values()))
        self.assertTrue(svc2.store.state["takeovers"]["zone:Z2"]["active"])
        # 重传在重启后仍然被识别
        dup = svc2.gate_tel("g", "G1", T0, 70)
        self.assertTrue(dup["duplicate"])
        mgmt = svc2.management_overview({"as_of": [T0]})
        self.assertEqual(mgmt["latest_plan"]["plan_id"], plan["plan_id"])
        self.assertTrue(mgmt["zones"])

    def test_tampering_is_detected(self):
        tmp = tempfile.mkdtemp(prefix="cso-test-tamper-")
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc = Case(data_dir=tmp)
        svc.gate_tel("g", "G1", T0, 0)
        journal = Path(tmp) / "events.jsonl"
        lines = journal.read_text(encoding="utf-8").splitlines()
        evil = json.loads(lines[0])
        evil["payload"]["percent_open"] = 99
        lines[0] = json.dumps(evil, ensure_ascii=False)
        journal.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(RuntimeError):
            Case(data_dir=tmp)


class ManagementViewTest(unittest.TestCase):
    def test_overview_shows_readings_conflicts_owner(self):
        svc, tmp = Case.fresh()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        svc.tel("r", "rainfall", "rain-east", T0, "rain_mm", 50)
        svc.tel("l", "level", "lvl-east-well", T0, "level_m", 3.9)
        svc.tel("rv", "river_level", "river-east", T0, "level_m", 2.0)
        svc.tel("q", "quality", "qual-east", T0, "cod_mg_l", 80)
        svc.gate_tel("g", "G1", T0, 0)
        svc.review_gate("G1", 100)
        svc.plan()
        mgmt = svc.management_overview({"as_of": [T0]})
        z1 = next(z for z in mgmt["zones"] if z["zone_id"] == "Z1")
        self.assertTrue(z1["conflicts"])
        self.assertEqual(z1["current_owner"]["role"], "调度员")
        self.assertIn("readings_used", z1)
        self.assertEqual(z1["readings_used"]["river_level"]["adopted_value"], 2.0)


class HttpServerTest(unittest.TestCase):
    def test_http_ingest_plan_and_health(self):
        tmp = tempfile.mkdtemp(prefix="cso-test-http-")
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        server = create_server(data_dir=tmp)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        base = f"http://127.0.0.1:{server.server_port}"

        def call(method, path, body=None):
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(base + path, data=data, method=method,
                                         headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req) as resp:
                    return resp.status, json.load(resp)
            except urllib.error.HTTPError as exc:
                return exc.code, json.load(exc)

        status, health = call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["service"], "cso-dispatch-service")
        status, _ = call("POST", "/api/telemetry", {
            "message_id": "h1", "kind": "level",
            "station_id": "lvl-east-well", "observed_at": T0, "level_m": 3.9})
        self.assertEqual(status, 200)
        status, _ = call("POST", "/api/telemetry", {
            "message_id": "h2", "kind": "river_level",
            "station_id": "river-east", "observed_at": T0, "level_m": 2.0})
        self.assertEqual(status, 200)
        status, _ = call("POST", "/api/telemetry", {
            "message_id": "h3", "kind": "quality",
            "station_id": "qual-east", "observed_at": T0, "cod_mg_l": 20})
        self.assertEqual(status, 200)
        status, _ = call("POST", "/api/telemetry", {
            "message_id": "h4", "kind": "gate", "gate_id": "G1",
            "observed_at": T0, "percent_open": 0})
        self.assertEqual(status, 200)
        status, plan_doc = call("POST", "/api/plans/generate", {})
        self.assertEqual(status, 200)
        plan_id = plan_doc["plan"]["plan_id"]
        status, _ = call("POST", f"/api/plans/{plan_id}/confirm",
                         {"actor": ACTOR_DISPATCHER})
        self.assertEqual(status, 200)
        status, doc = call("POST", "/api/plans/generate", {})
        self.assertEqual(status, 409)
        self.assertEqual(doc["error"], "confirmed_plan_active")
        status, chain = call("GET", "/api/audit/verify")
        self.assertEqual(status, 200)
        self.assertTrue(chain["ok"])


if __name__ == "__main__":
    unittest.main()
