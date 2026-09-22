"""兼容基线入口：真正实现位于 cso 包。

可通过环境变量 DATA_DIR 指定落盘目录（事件链与状态快照），
PORT/HOST 控制监听地址。
"""
from cso.service import SERVICE_NAME, create_server  # noqa: F401

__all__ = ["SERVICE_NAME", "create_server"]
