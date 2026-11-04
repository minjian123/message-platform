"""msg 错误码与产品码覆盖（段位 `35`：6 位码 = 2 位段 + 4 位序号）。

- **码值口径**：产品码 6 位（2 位段 + 4 位序号），段位 `35`——
  主题不存在 `350001`、位点非法 `350002`、消息不存在 `350003`、重投超限 `350007`、
  主题已存在 `350008`、分区数 / 分区号非法 `350009`、命名空间非法 `350010`、
  租户已停用 `350011`、租户不存在 `350012`、组不存在 `350013`、成员非法 `350014`
  （设计原文 5 位 `35010` / `35011` / `35012` 已在 01-1 详细设计「对齐记录」回写；
  `350004` / `350005` / `350006` 由架构 05 §5 预留为超时 / 背压 / 消息过大，本任务不得占用）。
- **声明**：`deploy/registry/msg.toml` 的 `[[errcodes]]` 是对外承诺的唯一声明，与本模块枚举逐条对账
  （用例 `tests/test_error_codes.py`）。
- **覆盖**：基座中间件在进入业务前就按平台码拒绝（停用 `80002` / 不存在 `80001`），msg 无改写点；
  故由 `MsgErrorMapper` 经基座 `[error_mapper].provider = "msg"` 在**统一错误响应单点**
  （`build_error_response`）把这两码改写为 msg 码，管理面与数据面口径一致（详细设计 §3.2、§3.4）。
- **判定顺序**：租户（`350012` / `350011`）→ 命名空间（`350010`）→ 主题（`350001` / `350008` / `350009`）
  → 组（`350013`）→ 分区号（`350009`）→ 成员与分区归属（`350014`）→ 位点 / 消息（`350002` / `350003` / `350007`）。
"""

from enum import IntEnum
from typing import ClassVar

from bms_core.core.exceptions import BizError, TenantNotFoundError, TenantSuspendedError
from bms_core.core.logging import get_logger
from bms_core.core.plugin import register_plugin
from bms_core.error_mapper.base import BaseErrorMapper

__all__ = [
    "MSG_ERROR_MAPPER",
    "MsgDeliveryExhaustedError",
    "MsgError",
    "MsgErrorCode",
    "MsgErrorMapper",
    "MsgGroupNotFoundError",
    "MsgMemberInvalidError",
    "MsgMessageNotFoundError",
    "MsgNamespaceInvalidError",
    "MsgOffsetInvalidError",
    "MsgPartitionCountInvalidError",
    "MsgTenantNotFoundError",
    "MsgTenantSuspendedError",
    "MsgTopicExistsError",
    "MsgTopicNotFoundError",
    "register_msg_error_mapper",
]

MSG_ERROR_MAPPER = "msg"
"""产品码覆盖实现名（配置 `[error_mapper].provider = "msg"`）。"""

_LOGGER = get_logger("bms_msg.errors")


class MsgErrorCode(IntEnum):
    """msg 错误码（段位 `35`；与 `deploy/registry/msg.toml` 的 `[[errcodes]]` 逐条对齐）。"""

    TOPIC_NOT_FOUND = 350001
    """主题不存在（不属于当前租户，或未处于 `active`）。"""

    OFFSET_INVALID = 350002
    """位点非法（提交值 `< 0`、`>= LEO`，或越过分区范围）。"""

    MESSAGE_NOT_FOUND = 350003
    """消息不存在（未投递过，或位点已被保留期清理）。"""

    DELIVERY_EXHAUSTED = 350007
    """消息重投超限、已转入死信（不可再确认）。"""

    TOPIC_EXISTS = 350008
    """主题已存在（同租户 / 命名空间 / 名称）。"""

    PARTITION_COUNT_INVALID = 350009
    """分区数 / 分区号非法（≤0、超上限、尝试减少，或分区号越界）；保留期越界同码。"""

    NAMESPACE_INVALID = 350010
    """命名空间非法（格式不符，或不是服务目录中已登记且启用的 `module_key`）。"""

    TENANT_SUSPENDED = 350011
    """租户已停用（拒绝生产与消费；数据保留，恢复后即时可用）。"""

    TENANT_NOT_FOUND = 350012
    """租户不存在（不泄露租户是否存在，与基座口径一致）。"""

    GROUP_NOT_FOUND = 350013
    """消费组不存在（未 `join` 过，或已被回收 / 删除）；位点与投递语义不因此改变。"""

    MEMBER_INVALID = 350014
    """成员非法（心跳 / 确认的成员不在组内、已超时清理、已离开，或所请求分区不在其分配内）。"""


class MsgError(BizError):
    """msg 业务异常基类（段位基；子类预置码位与 HTTP 状态）。"""

    code_: ClassVar[int]
    """错误码（子类预置）。"""

    http_status_: ClassVar[int] = 400
    """HTTP 状态（子类预置；业务失败缺省 400）。"""

    def __init__(self, message: str | None = None, *, data: object | None = None) -> None:
        """初始化 msg 业务异常。

        Args:
            message: 提示信息（缺省由前端按 `msg.error.*` 映射）。
            data: 随附数据（可选）。
        """
        super().__init__(type(self).code_, message, http_status=type(self).http_status_, data=data)


class MsgNamespaceInvalidError(MsgError):
    """命名空间非法（350010 / 400）。"""

    code_: ClassVar[int] = MsgErrorCode.NAMESPACE_INVALID
    http_status_: ClassVar[int] = 400


class MsgTopicNotFoundError(MsgError):
    """主题不存在（350001 / 404）。"""

    code_: ClassVar[int] = MsgErrorCode.TOPIC_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgTopicExistsError(MsgError):
    """主题已存在（350008 / 409）。"""

    code_: ClassVar[int] = MsgErrorCode.TOPIC_EXISTS
    http_status_: ClassVar[int] = 409


class MsgPartitionCountInvalidError(MsgError):
    """分区数 / 分区号非法、保留期越界（350009 / 400）。"""

    code_: ClassVar[int] = MsgErrorCode.PARTITION_COUNT_INVALID
    http_status_: ClassVar[int] = 400


class MsgOffsetInvalidError(MsgError):
    """位点非法（350002 / 400）。"""

    code_: ClassVar[int] = MsgErrorCode.OFFSET_INVALID
    http_status_: ClassVar[int] = 400


class MsgMessageNotFoundError(MsgError):
    """消息不存在（350003 / 404）。"""

    code_: ClassVar[int] = MsgErrorCode.MESSAGE_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgDeliveryExhaustedError(MsgError):
    """重投超限、已转入死信（350007 / 409）。"""

    code_: ClassVar[int] = MsgErrorCode.DELIVERY_EXHAUSTED
    http_status_: ClassVar[int] = 409


class MsgTenantSuspendedError(MsgError):
    """租户已停用（350011 / 403）。"""

    code_: ClassVar[int] = MsgErrorCode.TENANT_SUSPENDED
    http_status_: ClassVar[int] = 403


class MsgTenantNotFoundError(MsgError):
    """租户不存在（350012 / 404）。"""

    code_: ClassVar[int] = MsgErrorCode.TENANT_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgGroupNotFoundError(MsgError):
    """消费组不存在（350013 / 404）。"""

    code_: ClassVar[int] = MsgErrorCode.GROUP_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgMemberInvalidError(MsgError):
    """成员非法或分区不在其分配内（350014 / 409）。"""

    code_: ClassVar[int] = MsgErrorCode.MEMBER_INVALID
    http_status_: ClassVar[int] = 409


class MsgErrorMapper(BaseErrorMapper):
    """msg 产品码覆盖：把基座租户判定码改写为 msg 段位码。

    只映射两个「入口即拒绝」的码（停用 / 不存在）；其余异常原样返回，避免把基座其他能力域的
    码也纳入 msg 口径（越权改写会让调用方无法按基座文档定位问题）。

    **不透传基座文案**：基座文案可能含租户编码 / 内部原因，且与产品文案键冲突；对外一律按
    `msg.error.*` 文案键输出（不泄露租户是否存在），原始文案落日志供定位。
    """

    def map(self, exc: BizError) -> BizError:
        """按异常类型改写（未命中即原样返回）。

        Args:
            exc: 原始业务异常。

        Returns:
            BizError: 停用 → `350011`；不存在 → `350012`；其余原样。
        """
        if isinstance(exc, MsgError):
            return exc
        if isinstance(exc, TenantSuspendedError):
            _LOGGER.warning(
                "msg_error_code_mapped",
                source=exc.code,
                target=MsgErrorCode.TENANT_SUSPENDED,
                detail=exc.message,
            )
            return MsgTenantSuspendedError()
        if isinstance(exc, TenantNotFoundError):
            _LOGGER.warning(
                "msg_error_code_mapped",
                source=exc.code,
                target=MsgErrorCode.TENANT_NOT_FOUND,
                detail=exc.message,
            )
            return MsgTenantNotFoundError()
        return exc


def register_msg_error_mapper() -> None:
    """登记 msg 产品码覆盖实现（服务装配钩子内调用，须在插件注册表冻结前）。"""
    register_plugin("error_mapper", MSG_ERROR_MAPPER, MsgErrorMapper)
