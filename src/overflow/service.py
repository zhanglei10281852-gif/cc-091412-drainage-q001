"""调度领域服务：方案生命周期、动作执行/拒绝、人工接管、通知与恢复查询。

事件链：
- plan:<plan_id>     方案生成 → 派发 → 执行/拒绝 → 确认完成/被取代
- outfall:<oid>      人工接管/解除
- conflict:<id>      发现 → 消解
- rain:<event_id>    开场 → 跨夜 → 结束
"""

from . import ingest, planning, reference, timeutil


class ServiceError(ValueError):
    pass


class DispatchService:
    def __init__(self, store):
        self.store = store

    # ---------- 遥测 ----------
    def ingest(self, msg, at=None):
        return ingest.ingest_message(self.store, msg, received_at=at)

    def tick(self, at=None):
        """周期性维护：失联降级、雨停间隙关闭降雨事件。"""
        at = at or timeutil.now()
        stale = ingest.sweep_stale(self.store, at=at)
        closed = self._close_timed_out_rain(at)
        return {"staled_sensors": stale, "closed_rain_events": closed,
                "at": timeutil.iso(at)}

    def _close_timed_out_rain(self, at):
        closed = []
        with self.store.lock:
            for district, eid in list(self.store.state["district_open_event"].items()):
                ev = self.store.state["rain_events"][eid]
                gap = (at - timeutil.parse(ev["last_contact_at"])).total_seconds() / 60.0
                if gap > reference.RAIN_MERGE_GAP_MINUTES:
                    ev["status"] = "closed"
                    ev["ended_at"] = ev["last_rain_at"]
                    ev["cross_midnight"] = (
                        timeutil.local_date_key(timeutil.parse(ev["started_at"]))
                        != timeutil.local_date_key(timeutil.parse(ev["ended_at"])))
                    self.store.state["district_open_event"].pop(district, None)
                    self.store.append_event(
                        "rain.event_closed",
                        {"rain_event_id": ev["id"], "district": district,
                         "started_at": ev["started_at"], "ended_at": ev["ended_at"],
                         "cross_midnight": ev["cross_midnight"],
                         "total_depth_mm": ev["total_depth_mm"]},
                        event_id=ev["id"], chain_key=f"rain:{ev['id']}")
                    closed.append(eid)
            if closed:
                self.store.save()
        return closed

    # ---------- 人工复核 ----------
    def submit_review(self, body, at=None):
        at = at or timeutil.now()
        oid = body.get("outfall_id")
        reviewer = body.get("reviewer")
        if oid not in reference.OUTFALLS:
            raise ServiceError(f"未知溢流口: {oid!r}")
        if not reviewer:
            raise ServiceError("缺少复核人 reviewer")
        readings = body.get("readings") or {}
        if not readings:
            raise ServiceError("复核至少包含一个读数 readings")
        normalized = {}
        for sensor_id, item in readings.items():
            if sensor_id not in reference.SENSORS:
                raise ServiceError(f"未知传感器: {sensor_id}")
            source_time = item.get("source_time") or timeutil.iso(at)
            kind = reference.SENSORS[sensor_id]["kind"]
            if kind == "gate":
                mode = item.get("mode")
                if mode not in ("OPEN", "CLOSED", "REGULATED"):
                    raise ServiceError("闸门复核状态非法")
                normalized[sensor_id] = {"mode": mode,
                                         "opening_pct": item.get("opening_pct"),
                                         "source_time": source_time,
                                         "note": item.get("note")}
            else:
                normalized[sensor_id] = {"value": float(item["value"]),
                                         "source_time": source_time,
                                         "note": item.get("note")}
        rid = self.store.next_id("review_seq", "review")
        doc = {"id": rid, "outfall_id": oid, "reviewer": reviewer,
               "readings": normalized, "submitted_at": timeutil.iso(at),
               "note": body.get("note")}
        with self.store.lock:
            self.store.state["reviews"][oid] = doc
            # 复核闸位可消解该口未决冲突
            gate_sensor = reference.OUTFALLS[oid]["gate"]["gate_id"]
            reviewed_gate = normalized.get(gate_sensor)
            resolved = []
            if reviewed_gate:
                for cid, c in list(self.store.state["conflicts"].items()):
                    if c["outfall_id"] == oid and c["status"] == "open" \
                            and c["reported_mode"] == reviewed_gate["mode"]:
                        c["status"] = "resolved"
                        c["resolved_at"] = timeutil.iso(at)
                        c["resolution"] = "manual_review_confirmed_telemetry"
                        c["resolver"] = reviewer
                        resolved.append(cid)
                        self.store.append_event(
                            "conflict.resolved",
                            {"conflict_id": cid, "resolution": c["resolution"],
                             "resolver": reviewer},
                            event_id=cid, chain_key=f"conflict:{cid}", actor=reviewer)
            self.store.append_event(
                "review.submitted",
                {"review_id": rid, "outfall_id": oid, "reviewer": reviewer,
                 "sensors": sorted(normalized), "resolved_conflicts": resolved},
                event_id=rid, actor=reviewer)
            self.store.save()
        return doc

    # ---------- 预警 ----------
    def issue_warning(self, body, at=None):
        at = at or timeutil.now()
        district = body.get("district")
        level = body.get("level")
        if district not in reference.DISTRICTS:
            raise ServiceError(f"未知分区: {district!r}")
        if level not in reference.WARNING_LEVELS:
            raise ServiceError(f"预警等级非法: {level!r}")
        doc = {"district": district, "level": level,
               "source": body.get("source", "manual"),
               "active": bool(body.get("active", True)),
               "issued_at": timeutil.iso(at), "issuer": body.get("issuer")}
        with self.store.lock:
            self.store.state["warnings"][district] = doc
            self.store.append_event("warning.issued", doc, actor=doc["issuer"])
            self.store.save()
        return doc

    # ---------- 方案 ----------
    def generate_plan(self, district, *, actor=None, at=None, force=False):
        """生成并持久化方案。已有现场确认且未结束的方案时拒绝静默覆盖。"""
        at = at or timeutil.now()
        with self.store.lock:
            active = self._active_plan(district)
            if active and not force:
                raise ServiceError(
                    f"分区 {district} 已有{active['status']}方案 {active['id']}，"
                    "现场确认后的方案不能静默覆盖；如需调整请显式取代(supersede)")
            plan = planning.plan_district(self.store, district, at=at)
            pid = self.store.next_id("plan_seq", "plan")
            plan.update({"id": pid, "status": "proposed",
                         "created_by": actor or "system", "created_at": timeutil.iso(at),
                         "supersedes": None, "superseded_by": None,
                         "confirmed_at": None, "confirmed_by": None,
                         "completed_at": None})
            self.store.state["plans"][pid] = plan
            self.store.append_event(
                "plan.generated",
                {"plan_id": pid, "district": district,
                 "warning_level": plan["warning_level"],
                 "unconfirmed_sensors": plan["unconfirmed_sensors"],
                 "conflict_ids": [c["id"] for c in plan["conflicts"]],
                 "overflow_outfalls": [a["outfall_id"] for a in plan["actions"]
                                       if a["overflow"]]},
                event_id=pid, chain_key=f"plan:{pid}", actor=actor)
            self.store.save()
            return plan

    def confirm_plan(self, pid, body, at=None):
        """现场确认：派发可执行动作，落定通知与影响范围；方案此后不可静默覆盖。"""
        at = at or timeutil.now()
        confirmer = (body or {}).get("actor")
        if not confirmer:
            raise ServiceError("现场确认必须记录确认人 actor")
        with self.store.lock:
            plan = self._require_plan(pid)
            if plan["status"] != "proposed":
                raise ServiceError(f"方案 {pid} 当前状态 {plan['status']}，不可确认")
            takeover_actors = {t["outfall_id"]: t["actor"]
                               for t in plan.get("takeovers", [])}
            commands, blocked = [], []
            for action in plan["actions"]:
                for kind, spec in (("gate", action["gate"]), ("pump", action["pump"])):
                    if spec["command"] == "HOLD":
                        blocked.append({"outfall_id": action["outfall_id"], "kind": kind,
                                        "reasons": action["blocking_reasons"]})
                        continue
                    cid = self.store.next_id("command_seq", "cmd")
                    owner = takeover_actors.get(action["outfall_id"])
                    cmd = {"id": cid, "plan_id": pid,
                           "outfall_id": action["outfall_id"], "kind": kind,
                           "device_id": spec["gate_id" if kind == "gate" else "pump_id"],
                           "command": spec["command"],
                           "mode": spec.get("mode"),
                           "opening_pct": spec.get("opening_pct"),
                           "setpoint_m3h": spec.get("setpoint_m3h"),
                           "status": "dispatched", "owner": owner,
                           "dispatched_at": timeutil.iso(at),
                           "executed_at": None, "result": None,
                           "result_request_id": None}
                    commands.append(cmd)
                    if kind == "gate":
                        g = self.store.state["gates"].setdefault(cmd["device_id"], {})
                        g.update({"commanded_mode": spec["mode"],
                                  "commanded_opening_pct": spec.get("opening_pct"),
                                  "commanded_plan_id": pid,
                                  "commanded_at": timeutil.iso(at)})
                    self.store.append_event(
                        "action.dispatched",
                        {"command_id": cid, "plan_id": pid,
                         "outfall_id": cmd["outfall_id"], "kind": kind,
                         "device_id": cmd["device_id"], "command": cmd["command"],
                         "mode": cmd["mode"], "opening_pct": cmd["opening_pct"],
                         "setpoint_m3h": cmd["setpoint_m3h"], "owner": owner},
                        event_id=pid, chain_key=f"plan:{pid}", actor=confirmer)

            notifications = []
            for n in plan["notifications"]:
                nid = self.store.next_id("notification_seq", "notice")
                doc = dict(n)
                doc.update({"id": nid, "plan_id": pid, "district": plan["district"],
                            "created_at": timeutil.iso(at)})
                self.store.state["notifications"][nid] = doc
                notifications.append(nid)
                self.store.append_event("notification.created",
                                        {"notification_id": nid, "plan_id": pid,
                                         "target": doc["target"], "reason": doc["reason"],
                                         "deadline_at": doc["deadline_at"]},
                                        event_id=pid, chain_key=f"plan:{pid}")

            impact_id = None
            if plan["impact_zone"].get("overflow_active"):
                impact_id = self.store.next_id("impact_seq", "impact")
                impact = {"id": impact_id, "plan_id": pid,
                          "district": plan["district"],
                          "river": plan["impact_zone"]["river"],
                          "plume_reach_km": plan["impact_zone"]["plume_reach_km"],
                          "affected_outfalls": plan["impact_zone"]["affected_outfalls"],
                          "sensitive_targets": plan["impact_zone"]["sensitive_targets_in_reach"],
                          "pollution": plan["pollution"],
                          "created_at": timeutil.iso(at), "status": "active"}
                self.store.state["impacts"].append(impact)
                self.store.append_event("impact.recorded",
                                        {"impact_id": impact_id, "plan_id": pid,
                                         "river": impact["river"],
                                         "plume_reach_km": impact["plume_reach_km"],
                                         "affected_outfalls": impact["affected_outfalls"]},
                                        event_id=pid, chain_key=f"plan:{pid}")

            plan["status"] = "confirmed"
            plan["confirmed_at"] = timeutil.iso(at)
            plan["confirmed_by"] = confirmer
            plan["commands"] = commands
            plan["blocked_actions"] = blocked
            plan["notification_ids"] = notifications
            plan["impact_id"] = impact_id
            self.store.append_event(
                "plan.confirmed",
                {"plan_id": pid, "confirmed_by": confirmer,
                 "commands": [c["id"] for c in commands],
                 "blocked_count": len(blocked),
                 "notifications": notifications, "impact_id": impact_id},
                event_id=pid, chain_key=f"plan:{pid}", actor=confirmer)
            self.store.save()
            return plan

    def execute_command(self, cmd_id, body, at=None):
        """现场执行回执。同一 request_id 重传幂等；不具备执行条件则拒绝并入链。"""
        at = at or timeutil.now()
        body = body or {}
        actor = body.get("actor")
        if not actor:
            raise ServiceError("执行回执必须记录执行人 actor")
        with self.store.lock:
            cmd = self._find_command(cmd_id)
            if cmd is None:
                raise ServiceError(f"未知动作: {cmd_id}")
            request_id = body.get("request_id")
            if cmd["status"] in ("executed", "rejected"):
                if request_id and cmd.get("result_request_id") == request_id:
                    return {"idempotent": True, "command": cmd}
                raise ServiceError(
                    f"动作 {cmd_id} 已{cmd['status']}，禁止重复改变设备状态")
            # 接管口只有接管人能执行
            takeover = self.store.state["takeovers"].get(cmd["outfall_id"])
            if takeover and takeover["actor"] != actor:
                return self._reject_command(
                    cmd, f"该口已由 {takeover['actor']} 人工接管，{actor} 无权执行",
                    actor, at, request_id)
            plan = self.store.state["plans"][cmd["plan_id"]]
            action = next(a for a in plan["actions"]
                          if a["outfall_id"] == cmd["outfall_id"])
            if not action["executable"]:
                return self._reject_command(
                    cmd, ";".join(action["blocking_reasons"]), actor, at, request_id)

            cmd.update(status="executed", executed_at=timeutil.iso(at),
                       result=body.get("result", "done"),
                       result_actor=actor, result_request_id=request_id)
            if cmd["kind"] == "gate":
                g = self.store.state["gates"].setdefault(cmd["device_id"], {})
                g["reported_mode"] = cmd["mode"]
                g["reported_at"] = timeutil.iso(at)
            self.store.append_event(
                "action.executed",
                {"command_id": cmd_id, "plan_id": cmd["plan_id"],
                 "outfall_id": cmd["outfall_id"], "kind": cmd["kind"],
                 "result": cmd["result"], "request_id": request_id},
                event_id=cmd["plan_id"], chain_key=f"plan:{cmd['plan_id']}",
                actor=actor)
            self._maybe_complete_plan(cmd["plan_id"], at)
            self.store.save()
            return {"idempotent": False, "command": cmd}

    def _reject_command(self, cmd, reason, actor, at, request_id):
        cmd.update(status="rejected", executed_at=timeutil.iso(at),
                   result=f"rejected: {reason}", result_actor=actor,
                   result_request_id=request_id)
        self.store.append_event(
            "action.rejected",
            {"command_id": cmd["id"], "plan_id": cmd["plan_id"],
             "outfall_id": cmd["outfall_id"], "kind": cmd["kind"],
             "reason": reason, "actor": actor, "request_id": request_id},
            event_id=cmd["plan_id"], chain_key=f"plan:{cmd['plan_id']}", actor=actor)
        self.store.save()
        return {"idempotent": False, "rejected": True, "reason": reason,
                "command": cmd}

    def reject_command(self, cmd_id, body, at=None):
        at = at or timeutil.now()
        body = body or {}
        actor = body.get("actor")
        reason = body.get("reason")
        if not actor or not reason:
            raise ServiceError("拒绝动作必须记录 actor 与 reason")
        with self.store.lock:
            cmd = self._find_command(cmd_id)
            if cmd is None:
                raise ServiceError(f"未知动作: {cmd_id}")
            if cmd["status"] in ("executed", "rejected"):
                raise ServiceError(f"动作 {cmd_id} 已终结（{cmd['status']}）")
            return self._reject_command(cmd, reason, actor, at,
                                        body.get("request_id"))

    def _maybe_complete_plan(self, pid, at):
        plan = self.store.state["plans"][pid]
        pending = [c for c in plan["commands"] if c["status"] == "dispatched"]
        if pending:
            return
        plan["status"] = "completed"
        plan["completed_at"] = timeutil.iso(at)
        self.store.append_event(
            "plan.completed",
            {"plan_id": pid,
             "executed": sum(1 for c in plan["commands"] if c["status"] == "executed"),
             "rejected": sum(1 for c in plan["commands"] if c["status"] == "rejected")},
            event_id=pid, chain_key=f"plan:{pid}")

    def supersede_plan(self, pid, body, at=None):
        """显式以新方案取代已确认方案（必须给出原因与授权人），不做静默覆盖。"""
        at = at or timeutil.now()
        actor = (body or {}).get("actor")
        reason = (body or {}).get("reason")
        if not actor or not reason:
            raise ServiceError("取代方案必须记录 actor 与 reason")
        with self.store.lock:
            old = self._require_plan(pid)
            district = old["district"]
            new = planning.plan_district(self.store, district, at=at)
            nid = self.store.next_id("plan_seq", "plan")
            new.update({"id": nid, "status": "proposed", "created_by": actor,
                        "created_at": timeutil.iso(at), "supersedes": pid,
                        "superseded_by": None, "confirmed_at": None,
                        "confirmed_by": None, "completed_at": None})
            self.store.state["plans"][nid] = new
            old["superseded_by"] = nid
            if old["status"] in ("proposed", "confirmed"):
                old["status"] = "superseded"
            self.store.append_event(
                "plan.superseded",
                {"old_plan_id": pid, "new_plan_id": nid, "reason": reason},
                event_id=pid, chain_key=f"plan:{pid}", actor=actor)
            self.store.append_event(
                "plan.generated",
                {"plan_id": nid, "district": district, "supersedes": pid,
                 "warning_level": new["warning_level"]},
                event_id=nid, chain_key=f"plan:{nid}", actor=actor)
            self.store.save()
            return new

    # ---------- 人工接管 ----------
    def takeover(self, oid, body, at=None):
        at = at or timeutil.now()
        body = body or {}
        actor = body.get("actor")
        if oid not in reference.OUTFALLS:
            raise ServiceError(f"未知溢流口: {oid}")
        if not actor:
            raise ServiceError("接管必须记录责任人 actor")
        with self.store.lock:
            existing = self.store.state["takeovers"].get(oid)
            if existing and existing["actor"] != actor:
                raise ServiceError(
                    f"{oid} 已由 {existing['actor']} 接管，需先由其解除或办理交接")
            doc = {"outfall_id": oid, "actor": actor,
                   "role": body.get("role", "现场负责人"),
                   "note": body.get("note"), "since": timeutil.iso(at)}
            self.store.state["takeovers"][oid] = doc
            self.store.append_event(
                "outfall.takeover",
                {"outfall_id": oid, "actor": actor, "note": body.get("note")},
                event_id=f"takeover:{oid}", chain_key=f"outfall:{oid}", actor=actor)
            self.store.save()
            return doc

    def release_takeover(self, oid, body, at=None):
        at = at or timeutil.now()
        actor = (body or {}).get("actor")
        with self.store.lock:
            doc = self.store.state["takeovers"].pop(oid, None)
            if not doc:
                raise ServiceError(f"{oid} 未处于人工接管")
            self.store.append_event(
                "outfall.released",
                {"outfall_id": oid, "actor": actor or doc["actor"],
                 "handback": True},
                event_id=f"takeover:{oid}", chain_key=f"outfall:{oid}",
                actor=actor or doc["actor"])
            self.store.save()
            return {"outfall_id": oid, "released": True}

    # ---------- 冲突消解 ----------
    def resolve_conflict(self, cid, body, at=None):
        at = at or timeutil.now()
        actor = (body or {}).get("actor")
        resolution = (body or {}).get("resolution")
        if resolution not in ("telemetry_correct", "command_correct",
                              "manual_review_confirmed_telemetry", "false_alarm"):
            raise ServiceError("消解方式非法")
        with self.store.lock:
            c = self.store.state["conflicts"].get(cid)
            if not c:
                raise ServiceError(f"未知冲突: {cid}")
            c.update(status="resolved", resolved_at=timeutil.iso(at),
                     resolution=resolution, resolver=actor,
                     note=(body or {}).get("note"))
            if resolution == "command_correct":
                g = self.store.state["gates"].get(c["gate_id"], {})
                g["commanded_mode"] = c["expected_mode"]
            self.store.append_event(
                "conflict.resolved",
                {"conflict_id": cid, "resolution": resolution, "resolver": actor},
                event_id=cid, chain_key=f"conflict:{cid}", actor=actor)
            self.store.save()
            return c

    # ---------- 通知 ----------
    def ack_notification(self, nid, body, at=None):
        at = at or timeutil.now()
        actor = (body or {}).get("actor")
        with self.store.lock:
            n = self.store.state["notifications"].get(nid)
            if not n:
                raise ServiceError(f"未知通知: {nid}")
            if n["status"] == "pending":
                n["status"] = "acked"
                n["acked_at"] = timeutil.iso(at)
                n["acked_by"] = actor
                self.store.append_event(
                    "notification.acked",
                    {"notification_id": nid, "plan_id": n["plan_id"],
                     "actor": actor},
                    event_id=n["plan_id"], chain_key=f"plan:{n['plan_id']}",
                    actor=actor)
                self.store.save()
            return n

    def list_notifications(self, status=None, at=None):
        at = at or timeutil.now()
        with self.store.lock:
            items = list(self.store.state["notifications"].values())
        for n in items:
            if n["status"] == "pending" and at > timeutil.parse(n["deadline_at"]):
                n["overdue"] = True
        if status:
            items = [n for n in items if n["status"] == status]
        return items

    # ---------- 查询 ----------
    def pending_actions(self):
        """服务重启后仍需执行的动作。"""
        with self.store.lock:
            out = []
            for plan in self.store.state["plans"].values():
                for cmd in plan.get("commands", []):
                    if cmd["status"] == "dispatched":
                        out.append({"command": cmd,
                                    "blocking_reasons": next(
                                        (a["blocking_reasons"]
                                         for a in plan["actions"]
                                         if a["outfall_id"] == cmd["outfall_id"]),
                                        [])})
            return out

    def impacts(self, river=None, active_only=False):
        with self.store.lock:
            items = list(self.store.state["impacts"])
        if river:
            items = [i for i in items if i["river"] == river]
        if active_only:
            items = [i for i in items if i["status"] == "active"]
        return items

    def close_impact(self, impact_id, body, at=None):
        at = at or timeutil.now()
        with self.store.lock:
            impact = next((i for i in self.store.state["impacts"]
                           if i["id"] == impact_id), None)
            if not impact:
                raise ServiceError(f"未知影响记录: {impact_id}")
            impact["status"] = "closed"
            impact["closed_at"] = timeutil.iso(at)
            impact["closed_by"] = (body or {}).get("actor")
            self.store.append_event(
                "impact.closed",
                {"impact_id": impact_id, "actor": impact["closed_by"]},
                event_id=impact["plan_id"], chain_key=f"plan:{impact['plan_id']}",
                actor=impact["closed_by"])
            self.store.save()
            return impact

    def plan_chain(self, pid):
        self._require_plan(pid)
        return [e for e in self.store.read_events()
                if e.get("chain_key") == f"plan:{pid}"]

    def overview(self, at=None):
        """管理接口：方案采用的读数、冲突来源、当前责任人一屏总览。"""
        at = at or timeutil.now()
        with self.store.lock:
            plans = {}
            for pid, p in self.store.state["plans"].items():
                adopted = [r for r in p["readings"] if r["adopted"]]
                rejected_readings = [r for r in p["readings"] if not r["adopted"]]
                plans[pid] = {
                    "id": pid, "district": p["district"], "status": p["status"],
                    "warning_level": p["warning_level"],
                    "created_at": p["created_at"],
                    "confirmed_by": p.get("confirmed_by"),
                    "supersedes": p.get("supersedes"),
                    "adopted_readings": adopted,
                    "rejected_readings": rejected_readings,
                    "conflict_ids": [c["id"] for c in p.get("conflicts", [])],
                    "overflow_outfalls": [a["outfall_id"] for a in p["actions"]
                                          if a["overflow"]],
                    "pending_commands": [c["id"] for c in p.get("commands", [])
                                         if c["status"] == "dispatched"],
                }
            conflicts = []
            for c in self.store.state["conflicts"].values():
                conflicts.append({
                    "id": c["id"], "outfall_id": c["outfall_id"],
                    "gate_id": c["gate_id"], "status": c["status"],
                    "reported_mode": c["reported_mode"],
                    "expected_mode": c["expected_mode"],
                    "expected_basis": c["expected_basis"],
                    "sources": c.get("telemetry_sources", []),
                    "detected_at": c["detected_at"],
                    "resolution": c.get("resolution"),
                })
            responsibility = {}
            for oid, info in reference.OUTFALLS.items():
                t = self.store.state["takeovers"].get(oid)
                if t:
                    owner = {"name": t["actor"], "basis": "manual_takeover",
                             "since": t["since"]}
                else:
                    active_plan = self._active_plan(info["district"])
                    if active_plan and active_plan.get("confirmed_by"):
                        owner = {"name": active_plan["confirmed_by"],
                                 "basis": f"plan_confirmed:{active_plan['id']}",
                                 "since": active_plan["confirmed_at"]}
                    else:
                        duty = reference.DUTY_OFFICERS[info["district"]]
                        owner = {"name": duty["name"], "basis": "duty_roster",
                                 "role": duty["role"]}
                responsibility[oid] = owner
            pending_notices = [n["id"] for n in
                               self.list_notifications(status="pending", at=at)]
            return {"at": timeutil.iso(at), "plans": plans,
                    "conflicts": conflicts, "takeovers": list(
                        self.store.state["takeovers"].values()),
                    "responsibility": responsibility,
                    "open_rain_events": dict(
                        self.store.state["district_open_event"]),
                    "pending_notifications": pending_notices,
                    "active_impacts": [i["id"] for i in self.state_impacts()
                                       if i["status"] == "active"]}

    def state_impacts(self):
        return self.store.state["impacts"]

    # ---------- 内部 ----------
    def _active_plan(self, district):
        candidates = [p for p in self.store.state["plans"].values()
                      if p["district"] == district
                      and p["status"] in ("proposed", "confirmed")]
        return candidates[-1] if candidates else None

    def _require_plan(self, pid):
        plan = self.store.state["plans"].get(pid)
        if not plan:
            raise ServiceError(f"未知方案: {pid}")
        return plan

    def _find_command(self, cmd_id):
        for plan in self.store.state["plans"].values():
            for cmd in plan.get("commands", []):
                if cmd["id"] == cmd_id:
                    return cmd
        return None
