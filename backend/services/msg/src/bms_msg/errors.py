"""msg 错误码与产品码覆盖（段位 `35`：6 位码 = 2 位段 + 4 位序号）。

- **码值口径**：产品码 6 位（2 位段 + 4 位序号），段位 `35`——
  主题不存在 `350001`、位点非法 `350002`、消息不存在 `350003`、重投超限 `350007`、
  主题已存在 `350008`、分区数 / 分区号非法 `350009`、命名空间非法 `350010`、
  租户已停用 `350011`、租户不存在 `350012`、组不存在 `350013`、成员非法 `350014`、
  延迟区已满 `350015`、重放任务不存在 `350016`、重放任务状态非法 `350017`、
  帧或消息体过大 `350006`、帧格式非法 `350018`、协议版本不支持 `350019`、
  连接数超限 `350020`、单连接长驻流超限 `350021`、生产限流 `350005`、存储水位拒绝 `350022`、
  RPC 方法未注册 `350023`
  （设计原文 5 位 `35010` / `35011` / `35012` 已在 01-1 详细设计「对齐记录」回写；
  `350004` 由 04-2 按「RPC 预算耗尽」启用（business / 不重试 / 504），`350005` 由 03-5 按「生产限流」启用；
  `350006` 由 03-2 按「上限 1 MiB」启用；`350020` / `350021` 由 03-4 按「连接数 / 单连接长驻流」启用；
  `350022` 由 02-6 按「存储水位拒绝」启用）。
- **声明**：`deploy/registry/msg.toml` 的 `[[errcodes]]` 是对外承诺的唯一声明，与本模块枚举逐条对账
  （用例 `tests/test_error_codes.py`）。
- **覆盖**：基座中间件在进入业务前就按平台码拒绝（停用 `80002` / 不存在 `80001`），msg 无改写点；
  故由 `MsgErrorMapper` 经基座 `[error_mapper].provider = "msg"` 在**统一错误响应单点**
  （`build_error_response`）把这两码改写为 msg 码，管理面与数据面口径一致（详细设计 §3.2、§3.4）。
- **判定顺序**：租户（`350012` / `350011`）→ 命名空间（`350010`）→ 主题（`350001` / `350008` / `350009`）
  → 组（`350013`）→ 分区号（`350009`）→ 成员与分区归属（`350014`）→ 位点 / 消息（`350002` / `350003` / `350007`）
  → 延迟区容量（`350015`，发布侧排在主题与档位判定之后）→ 存储水位（`350022`，排在主题与分区判定之后、
  延迟配额之前）→ 重放任务（`350016` / `350017`，排在租户判定之后）；
  连接类判定（`350020` / `350021`）**先于**上述全部——连接打满时连解析租户都不必做。

"""

from enum import IntEnum
from typing import ClassVar

from bms_core.core.exceptions import BizError, TenantNotFoundError, TenantSuspendedError
from bms_core.core.logging import get_logger
from bms_core.core.plugin import register_plugin
from bms_core.error_mapper.base import BaseErrorMapper

from bms_msg.core.config import RATE_LIMIT_RETRY_AFTER_MS

__all__ = [
    "MSG_ERROR_MAPPER",
    "MsgConnectionLimitExceededError",
    "MsgDelayQuotaExceededError",
    "MsgDeliveryExhaustedError",
    "MsgError",
    "MsgErrorCode",
    "MsgErrorMapper",
    "MsgFrameInvalidError",
    "MsgGroupNotFoundError",
    "MsgMemberInvalidError",
    "MsgMessageNotFoundError",
    "MsgNamespaceInvalidError",
    "MsgOffsetInvalidError",
    "MsgPartitionCountInvalidError",
    "MsgPayloadTooLargeError",
    "MsgRateLimitedError",
    "MsgReplayTaskInvalidStateError",
    "MsgReplayTaskNotFoundError",
    "MsgRpcDeadlineExceededError",
    "MsgRpcMethodNotFoundError",
    "MsgStorageWatermarkExceededError",
    "MsgStreamLimitExceededError",
    "MsgTenantNotFoundError",
    "MsgTenantSuspendedError",
    "MsgTopicExistsError",
    "MsgTopicNotFoundError",
    "MsgVersionUnsupportedError",
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

    RPC_DEADLINE_EXCEEDED = 350004
    """RPC 预算耗尽（`deadline_ms` 到期，或收到时已耗尽）：business / `retryable=false` / 504。

    **「超时 = 结果未知」，不是失败**：不得直接重试（需求 04-2 §3.5），是否重试由 04-4 按幂等键判定。
    `data` / `detail` 带 `outcome=unknown` 与已用 / 剩余毫秒。与相邻码分工：`350023` 方法未注册
    （改参无用，应查对端能力）、`350005` 速率超配额（退避可解）、`350006` 参数或结果过大。
    """

    RATE_LIMITED = 350005
    """生产限流（租户 / 主题速率超出配额）：429 + `Retry-After`，帧形态带 `retry_after_ms`。

    与相邻码分工：`350015` 延迟区满（容量）、`350020` 连接数满、`350021` 单连接流满、
    `350006` 消息过大；本码只表示「生产速率超出配额」。
    """

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

    DELAY_QUOTA_EXCEEDED = 350015
    """延迟区已满（每租户软上限到顶）；与全局背压 `350005` 分工：前者「延迟堆住了」，后者「整体过载」。"""

    REPLAY_TASK_NOT_FOUND = 350016
    """重放任务不存在（不属于当前租户，或已被清理）。"""

    REPLAY_TASK_INVALID_STATE = 350017
    """重放任务状态不允许该操作（终态不可逆；恢复只接受已暂停的任务）。"""

    PAYLOAD_TOO_LARGE = 350006
    """帧或消息体超过上限（1 MiB）：JSON 路径按 413 拒绝，帧路径在解码期以 error 帧（layer=transport）返回。"""

    FRAME_INVALID = 350018
    """帧格式非法（magic 之外的任何结构问题：长度不足 / 多余字节 / header 非 Map / 缺必填头 / 类型不匹配）。"""

    VERSION_UNSUPPORTED = 350019
    """协议主版本不支持（服务端只认同一主版本；返回后结束该 stream，由客户端断开连接）。"""

    CONNECTION_LIMIT_EXCEEDED = 350020
    """连接数超限（单副本有效连接数已达 `max_connections`；客户端应换实例或退避后重试）。"""

    STREAM_LIMIT_EXCEEDED = 350021
    """单连接长驻流超限（达 `max_streams_per_connection`；客户端应降并发或另开连接）。"""

    STORAGE_WATERMARK_EXCEEDED = 350022
    """存储水位拒绝（卷使用率超拒绝阈值或数据卷不可用）。

    与相邻码分工（2026-11-05 定）：`350005` 生产限流（退避重试即可）、`350015` 延迟区满（延迟档位的问题）、
    `350022` 存储水位（**必须停止发布并介入清理**，退避无用）；帧形态按系统层返回、`retryable=true`。
    """

    RPC_METHOD_NOT_FOUND = 350023
    """RPC 方法未注册（`service + method` 不在进程内注册表）。

    与 `10001` 分工：`10001` 是「参数形态非法」（改参可解、协议层）；本码是「对端没有这个方法」
    （改参无用、business 层、`retryable=false`），客户端应去查对端能力而不是重试。
    `350004` 是预算耗尽（04-2 已启用），与本码同为 business / 不重试但语义不同：那个是「没时间」，
    这个是「没这个方法」。本码不复用也不占用 `350004`。
    """


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


class MsgRateLimitedError(MsgError):
    """生产限流（350005 / 429）：携带建议重试毫秒，JSON 形态由专属处理器写 `Retry-After`。"""

    code_: ClassVar[int] = MsgErrorCode.RATE_LIMITED
    http_status_: ClassVar[int] = 429

    def __init__(
        self,
        message: str | None = None,
        *,
        retry_after_ms: int = RATE_LIMIT_RETRY_AFTER_MS,
        data: object | None = None,
    ) -> None:
        """初始化。

        Args:
            message: 提示信息（缺省由前端按 `msg.error.rate_limited` 映射）。
            retry_after_ms: 建议重试间隔（毫秒；缺省取 `RATE_LIMIT_RETRY_AFTER_MS`，桶给出等待时以桶为准）。
            data: 随附数据（可选）。
        """
        super().__init__(message, data=data)
        self.retry_after_ms = retry_after_ms


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


class MsgDelayQuotaExceededError(MsgError):
    """延迟区已满（350015 / 429）：拒绝进、不静默清——兜底通道不能因堆满而静默失效。"""

    code_: ClassVar[int] = MsgErrorCode.DELAY_QUOTA_EXCEEDED
    http_status_: ClassVar[int] = 429


class MsgReplayTaskNotFoundError(MsgError):
    """重放任务不存在（350016 / 404）：按主键 + 当前租户查不到。"""

    code_: ClassVar[int] = MsgErrorCode.REPLAY_TASK_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgReplayTaskInvalidStateError(MsgError):
    """重放任务状态不允许该操作（350017 / 409）：终态不可逆、恢复只接受已暂停。"""

    code_: ClassVar[int] = MsgErrorCode.REPLAY_TASK_INVALID_STATE
    http_status_: ClassVar[int] = 409


class MsgPayloadTooLargeError(MsgError):
    """帧或消息体超限（350006 / 413）：上限 1 MiB（header 另限 64 KiB；口径见 03-2 详细设计 §4.6）。"""

    code_: ClassVar[int] = MsgErrorCode.PAYLOAD_TOO_LARGE
    http_status_: ClassVar[int] = 413


class MsgFrameInvalidError(MsgError):
    """帧格式非法（350018 / 400）：结构、必填头、类型与路径匹配度等协议层判定。"""

    code_: ClassVar[int] = MsgErrorCode.FRAME_INVALID
    http_status_: ClassVar[int] = 400


class MsgVersionUnsupportedError(MsgError):
    """协议主版本不支持（350019 / 400）：返回后结束该 stream，由客户端断开连接（GOAWAY 归 03-4）。"""

    code_: ClassVar[int] = MsgErrorCode.VERSION_UNSUPPORTED
    http_status_: ClassVar[int] = 400


class MsgConnectionLimitExceededError(MsgError):
    """连接数超限（350020 / 429）：不静默丢弃，客户端据 `retry_after_ms` 换实例或退避重连。"""

    code_: ClassVar[int] = MsgErrorCode.CONNECTION_LIMIT_EXCEEDED
    http_status_: ClassVar[int] = 429


class MsgStreamLimitExceededError(MsgError):
    """单连接长驻流超限（350021 / 429）：客户端应降并发或另开连接。"""

    code_: ClassVar[int] = MsgErrorCode.STREAM_LIMIT_EXCEEDED
    http_status_: ClassVar[int] = 429


class MsgStorageWatermarkExceededError(MsgError):
    """存储水位拒绝（350022 / **507**）：盘满必须停止发布并介入清理，退避重试无用。

    `data` 五个字段（`used_ratio` / `threshold` / `volume` / `free_bytes` / `reason`）由
    `broker/watermark.py` 组装：JSON 形态进 `data`，帧形态进 error 帧的 `detail`。
    """

    code_: ClassVar[int] = MsgErrorCode.STORAGE_WATERMARK_EXCEEDED
    http_status_: ClassVar[int] = 507


class MsgRpcMethodNotFoundError(MsgError):
    """RPC 方法未注册（350023 / 404）：`service + method` 不在进程内注册表。

    `data` 带 `service` / `method` 两个字段（普通 dict，不用自定义 Mapping——pydantic 序列化
    自定义 Mapping 会 500，02-7 在 `350015` 上交过学费）：调用方据此确认「自己拼的名字」还是
    「对端没实现」。
    """

    code_: ClassVar[int] = MsgErrorCode.RPC_METHOD_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgRpcDeadlineExceededError(MsgError):
    """RPC 预算耗尽（350004 / 504）：`deadline_ms` 到期，或收到时预算已耗尽。

    `data` 三个字段：`outcome`（固定 `unknown`）、`used_ms`、`remaining_ms`——调用方据此知道
    「结果未知」而不是「确定失败」（需求 04-2 §3.5）。帧形态进 error 帧的 `detail`。
    """

    code_: ClassVar[int] = MsgErrorCode.RPC_DEADLINE_EXCEEDED
    http_status_: ClassVar[int] = 504


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
