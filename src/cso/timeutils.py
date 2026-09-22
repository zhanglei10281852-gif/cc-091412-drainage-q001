"""时间工具：统一带时区的 ISO 8601。"""
from __future__ import annotations

from datetime import datetime, timezone


def parse_iso(value: str) -> datetime:
    """解析 ISO 8601；不带时区的时间一律拒绝，避免静默按本地时区解释。"""
    if not isinstance(value, str):
        raise ValueError("时间必须是 ISO 8601 字符串")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"时间 {value!r} 缺少时区信息")
    return dt.astimezone(timezone.utc)


def format_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("禁止序列化无时区时间")
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
