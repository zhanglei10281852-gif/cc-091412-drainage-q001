"""进程入口：兼容基线测试的 `from app import create_server, SERVICE_NAME`。"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from overflow import SERVICE_NAME  # noqa: E402
from overflow.web import create_server  # noqa: E402

__all__ = ["create_server", "SERVICE_NAME"]
