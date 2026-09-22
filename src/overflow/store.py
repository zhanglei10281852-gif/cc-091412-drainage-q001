"""落盘状态与只追加事件链。

设计约束（来自业务约定）：
- 数据位置由运行时配置（环境变量 DRAINAGE_DATA_DIR，默认 ./.runtime/data）指定；
- 每次写入原子替换（tmp + os.replace），进程崩溃不留半截 JSON；
- 事件链只追加，不修改不删除；服务重启后状态完全从文件恢复；
- 进程内加锁保证 ThreadingHTTPServer 下的一致性。
"""

import json
import os
import tempfile
import threading

from . import timeutil

STATE_FILENAME = "state.json"
EVENTS_FILENAME = "events.jsonl"


def _new_state():
    return {
        "schema": 1,
        "created_at": timeutil.iso(timeutil.now()),
        # 遥测最新值：sensor_id -> {value, quality, source_time, received_time,
        #                          duplicated_of?, channel?, seq?}
        "measurements": {},
        # 已处理遥测消息指纹 -> 首次接收时间（幂等去重）
        "processed_messages": {},
        # 降雨事件（按区分场，跨午夜不切分）
        "rain_events": {},          # event_id -> doc
        "district_open_event": {},  # district -> event_id
        # 闸门当前状态：gate_id -> {mode, source, updated_at, message_id, ...}
        "gates": {},
        # 冲突（遥测闸位与调度/复核不一致）：conflict_id -> doc
        "conflicts": {},
        # 人工复核（每个溢流口最新一条），读数来源标记为 manual
        "reviews": {},
        # 分区预警：district -> {level, issued_at, source, active}
        "warnings": {},
        # 事件链游标：chain_key -> 上一条 event_seq
        "event_chains": {},
        # 调度方案：plan_id -> doc（含动作、读数、解释、确认/接管状态）
        "plans": {},
        # 通知（截止时间在重启后仍可查询）
        "notifications": {},
        # 人工接管锁：outfall_id -> doc
        "takeovers": {},
        # 污染影响记录（河道影响范围，重启可查）
        "impacts": [],
        "counters": {},
    }


class Store:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.state_path = os.path.join(self.data_dir, STATE_FILENAME)
        self.events_path = os.path.join(self.data_dir, EVENTS_FILENAME)
        self._lock = threading.RLock()
        self.state = self._load_state()

    # ---------- 基础读写 ----------
    def _load_state(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        return _new_state()

    def _persist_locked(self):
        directory = os.path.dirname(self.state_path) or "."
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self.state, fh, ensure_ascii=False, indent=1)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.state_path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    @property
    def lock(self):
        return self._lock

    def save(self):
        with self._lock:
            self._persist_locked()

    # ---------- 事件链 ----------
    def append_event(self, event_type: str, payload: dict, *,
                     event_id=None, actor=None, chain_after=None,
                     chain_key=None, at=None):
        """追加一条事件。返回事件文档。

        - event_id: 领域事件标识（如 plan-0001），同一领域事件的动作链共享 id 前缀
        - chain_after: 显式指定上一条事件的 event_seq
        - chain_key: 给定链式游标键（如 plan:<id>、outfall:<id>、rain:<id>），
          自动链接到该链上一条事件，执行/拒绝/接管由此串成事件链
        """
        with self._lock:
            if chain_after is None and chain_key is not None:
                chain_after = self.state["event_chains"].get(chain_key)
            seq = int(self.state["counters"].get("event_seq", 0)) + 1
            self.state["counters"]["event_seq"] = seq
            doc = {
                "event_seq": seq,
                "event_id": event_id or f"evt-{seq:04d}",
                "event_type": event_type,
                "actor": actor or "system",
                "occurred_at": timeutil.iso(at or timeutil.now()),
                "recorded_at": timeutil.iso(timeutil.now()),
                "chain_after": chain_after,
                "chain_key": chain_key,
                "payload": payload,
            }
            with open(self.events_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            if chain_key is not None:
                self.state["event_chains"][chain_key] = seq
            return doc

    def read_events(self):
        if not os.path.exists(self.events_path):
            return []
        with open(self.events_path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    # ---------- 标识生成 ----------
    def next_id(self, kind: str, prefix: str) -> str:
        with self._lock:
            n = int(self.state["counters"].get(kind, 0)) + 1
            self.state["counters"][kind] = n
            return f"{prefix}-{n:04d}"
