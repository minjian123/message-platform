"""msg 消息平台服务包。

工程根覆盖（与 mdm 同口径）：`bms-core` 以路径依赖接入工作区 bms 源码，基座的工程根定位
（`resolve_backend_root` 向上搜 `alembic.ini`）会搜到 bms 仓根；故在导入期显式声明本仓
`backend/` 根（基座官方覆盖变量，取值无效时 fail-closed）。
"""

import os
from pathlib import Path

__version__ = "0.1.0"

os.environ.setdefault("BMS_BACKEND_ROOT", str(Path(__file__).resolve().parents[4]))

CONTRACT_VERSION = "0.1.0"
"""公开契约（OpenAPI）版本（服务自报；破坏性变更升主版本）。"""

SERVICE_NAME = "msg"
"""服务名（`[app].service` 为空时取本声明）。"""

SERVICE_TITLE = "msg 消息平台服务"
"""服务中文名（应用 title 与根路由）。"""

PRODUCT_KEY = "msg"
"""产品标识（与 `deploy/registry/msg.toml` 产品档案一致）。"""
