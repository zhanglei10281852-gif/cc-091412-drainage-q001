"""统一时间处理：全部使用带时区的 ISO 8601。

领域约定时间字段为 Asia/Shanghai，但任何带偏移量的 ISO 字符串都可解析；
内部一律存 UTC 感知 datetime，展示时转回 Asia/Shanghai。
"""

from datetime import datetime, timezone

try:  # Python 3.9+ 提供 ZoneInfo，3.11 基线一定可用
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

LOCAL_TZ_NAME = "Asia/Shanghai"
UTC = timezone.utc


def local_tz():
    return ZoneInfo(LOCAL_TZ_NAME) if ZoneInfo else UTC


def now() -> datetime:
    """当前时间（感知 UTC）。可被测试通过 timeutil.now 打桩。"""
    return datetime.now(UTC)


def parse(value) -> datetime:
    """解析 ISO 8601 字符串/日期时间，保证返回感知时区的 datetime（UTC 归一）。"""
    if value is None:
        raise ValueError("时间不能为空")
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    else:
        raise ValueError(f"无法识别的时间格式: {value!r}")
    if dt.tzinfo is None:
        # 裸时间按业务时区解释，而不是按宿主机本地时区
        dt = dt.replace(tzinfo=local_tz())
    return dt.astimezone(UTC)


def iso(dt: datetime) -> str:
    """序列化为带偏移量的 ISO 8601 字符串。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def local_iso(dt: datetime) -> str:
    """按业务时区展示的 ISO 字符串（跨午夜事件归并时人工核对用）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(local_tz()).isoformat()


def local_date_key(dt: datetime) -> str:
    """业务时区下的日期，用于跨午夜降雨的同一事件判定。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(local_tz()).date().isoformat()
