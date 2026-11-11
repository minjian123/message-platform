"""msg 错误码与产品码覆盖（段位 `35`：6 位码 = 2 位段 + 4 位序号）。

- **码值口径**：产品码 6 位（2 位段 + 4 位序号），段位 `35`——
  主题不存在 `350001`、位点非法 `350002`、消息不存在 `350003`、重投超限 `350007`、
  主题已存在 `350008`、分区数 / 分区号非法 `350009`、命名空间非法 `350010`、
  租户已停用 `350011`、租户不存在 `350012`、组不存在 `350013`、成员非法 `350014`、
  延迟区已满 `350015`、重放任务不存在 `350016`、重放任务状态非法 `350017`、
  帧或消息体过大 `350006`、帧格式非法 `350018`、协议版本不支持 `350019`、
  连接数超限 `350020`、单连接长驻流超限 `350021`、生产限流 `350005`、存储水位拒绝 `350022`、
  RPC 方法未注册 `350023`、RPC 流空闲超时 `350024`、主题数配额超限 `350025`、
  消费组数配额超限 `350026`、主题状态不允许该操作 `350028`、ISR 不足 / 提交超时 `350032`
  （设计原文 5 位 `35010` / `35011` / `35012` 已在 01-1 详细设计「对齐记录」回写；
  `350004` 由 04-2 按「RPC 预算耗尽」启用（business / 不重试 / 504），`350005` 由 03-5 按「生产限流」启用；
  `350006` 由 03-2 按「上限 1 MiB」启用；`350020` / `350021` 由 03-4 按「连接数 / 单连接长驻流」启用；
  `350022` 由 02-6 按「存储水位拒绝」启用；`350024` 由 04-3-1 按「RPC 流空闲超时」启用）。
- **声明**：`deploy/registry/msg.toml` 的 `[[errcodes]]` 是对外承诺的唯一声明，与本模块枚举逐条对账
  （用例 `tests/test_error_codes.py`）。
- **覆盖**：基座中间件在进入业务前就按平台码拒绝（停用 `80002` / 不存在 `80001`），msg 无改写点；
  改由 `deploy/registry/msg.toml` 的 `[[errcodes]].maps_from` 声明在**统一错误响应单点**
  （`build_error_response`）改写为 msg 码，管理面与数据面同码（07D 任务 03 附录 B）。
- **判定顺序**：租户（`350012` / `350011`）→ 命名空间（`350010`）→ 主题（`350001` / `350008` / `350009`）
  → 组（`350013`）→ 分区号（`350009`）→ 成员与分区归属（`350014`）→ 位点 / 消息（`350002` / `350003` / `350007`）
  → 延迟区容量（`350015`，发布侧排在主题与档位判定之后）→ 存储水位（`350022`，排在主题与分区判定之后、
  延迟配额之前）→ 重放任务（`350016` / `350017`，排在租户判定之后）；
  连接类判定（`350020` / `350021`）**先于**上述全部——连接打满时连解析租户都不必做。

"""

from enum import IntEnum
from typing import ClassVar

from bms_core.msgclient.errors import (
    ErrorCategory,
    MsgError,
    MsgFrameInvalidError,
    MsgPayloadTooLargeError,
    MsgVersionUnsupportedError,
    Outcome,
)

from bms_msg.core.config import RATE_LIMIT_RETRY_AFTER_MS, RPC_IDEMPOTENCY_IN_PROGRESS_MS

__all__ = [
    "ErrorCategory",
    "MsgAclDeniedError",
    "MsgConnectionLimitExceededError",
    "MsgDeadLetterInvalidStateError",
    "MsgDeadLetterNotFoundError",
    "MsgDeadLetterNotReplayableError",
    "MsgDelayQuotaExceededError",
    "MsgDeliveryExhaustedError",
    "MsgError",
    "MsgErrorCode",
    "MsgFrameInvalidError",
    "MsgGroupCountQuotaExceededError",
    "MsgGroupNotFoundError",
    "MsgInsufficientReplicasError",
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
    "MsgRpcInProgressError",
    "MsgRpcMethodNotFoundError",
    "MsgRpcStreamIdleError",
    "MsgStorageWatermarkExceededError",
    "MsgStreamLimitExceededError",
    "MsgTenantNotFoundError",
    "MsgTenantSuspendedError",
    "MsgTopicExistsError",
    "MsgTopicNotFoundError",
    "MsgTopicQuotaExceededError",
    "MsgTopicStateInvalidError",
    "MsgVersionUnsupportedError",
    "Outcome",
]


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

    RPC_STREAM_IDLE = 350024
    """RPC 流空闲超时（`RPC_STREAM_IDLE_MS` 内无帧写出）：business / `retryable=false` / 504。

    与相邻码分工（04-3-1）：`350004` 是整条流的预算耗尽（「没时间了」，`outcome=unknown`）、
    `350006` 是单帧结果超过 1 MiB、`350023` 是方法未注册；本码只表示「这条流半天没吐数据」——
    服务端据此收尾并释放名额，客户端应重开流而不是原地重试。
    与连接心跳（03-4 的 10s / 30s）**不是一件事**：那个判连接死活，本码判流有没有进展。
    """

    TOPIC_QUOTA_EXCEEDED = 350025
    """主题数配额超限（每租户 / 命名空间的硬限已到）：429，客户端应删除主题或申请调整配额。

    与 `350008` 分工：那个是「同名主题已存在」（改个名即可），本码是「数量到顶」
    （不释放就永远建不了）；与 `350005` 分工：那个是速率（退避可解），本码是容量。
    """

    GROUP_QUOTA_EXCEEDED = 350026
    """消费组数配额超限（每租户硬限已到）：429，客户端应清理空组或申请调整配额。

    与 `350013` 分工：那个是「组不存在」，本码是「组数量到顶」；只拦新建组，
    已存在组的心跳 / 拉取 / 离开不受影响。
    """

    ACL_DENIED = 350027
    """主题 ACL 拒绝（403）：主体对该主题的该动作无授权（含显式 deny）。

    与 `30001` 分工：`30001` 是功能级 RBAC 无权限，本码是资源级 ACL 拒绝；
    与 `350001` 分工：本码不表示主题不存在（ACL 先于归属判定，不泄露存在性）。
    基座 `82013` 经 `[[errcodes]].maps_from` 映射为本码。
    """

    TOPIC_STATE_INVALID = 350028
    """主题状态不允许该操作（`disabled` / `deleting` 下的写操作）：409。

    与相邻码分工：`350001` 是「不存在或未启用」（读路径口径，用户应去建主题）；`350008` 是「同名已存在」；
    本码是「主题还在，但当前状态不接受这个操作」——`deleting` 时应等清理完成，`disabled` 时应先启用。
    管理面写操作（扩分区 / 改保留期 / 重放）用它，删主题不走本码（重复删除是幂等受理）。
    """

    DEAD_LETTER_NOT_FOUND = 350029
    """死信不存在（不属于当前租户，或已被主题删除 / 租户清理物理删除）：404。"""

    DEAD_LETTER_INVALID_STATE = 350030
    """死信状态不允许该操作（非法迁移 / 重复处置）：409。

    与 `350029` 分工：那个是「行不在了」，本码是「行还在，但当前状态不接受这个动作」——
    `replaying` / `archived` 不可再处置，归档只接受 `pending` / `ignored` / `resolved`。
    """

    DEAD_LETTER_NOT_REPLAYABLE = 350031
    """原消息不可重放（超出保留期或记录不存在）：409。

    重放前按 `topic_id / partition_no / offset` 校验原记录；不存在时**不建空任务**，
    避免把「消息已丢」误标成「已解决」。
    """

    INSUFFICIENT_REPLICAS = 350032
    """ISR 不足或提交等待超时（`acks=all`）：503 / system / retryable。

    `data` 带 `reason`（`insufficient_isr` / `commit_timeout`）、`isr`、`required` 与
    `topic` / `partition`——客户端据此区分「副本不够」与「等太久」，并按退避重试。
    与相邻码分工：`350022` 是存储水位（盘满，退避无用）、`350005` 是生产限流（退避可解）、
    `350007` 是重投超限；本码只表示「ISR 侧不满足提交条件」。

    **`commit_timeout` 不等于确定未写入**：记录可能已在 leader 落盘、只是尚未提交，
    重试可能产生重复消息，消费端仍按 `message_id` 幂等（与 04-2「超时 = 结果未知」同构）。
    """


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


class MsgRpcStreamIdleError(MsgError):
    """RPC 流空闲超时（350024 / 504）：连续 `RPC_STREAM_IDLE_MS` 无帧写出，服务端主动收尾。

    `data` 两个字段：`idle_ms`（阈值）与 `idle_elapsed_ms`（实际空闲时长）——排障第一问是
    「究竟卡了多久、阈值多少」，这两值比一句「超时了」有用。流内以 ERROR 帧收尾，随后 EOF。
    """

    code_: ClassVar[int] = MsgErrorCode.RPC_STREAM_IDLE
    http_status_: ClassVar[int] = 504


class MsgTopicQuotaExceededError(MsgError):
    """主题数配额超限（350025 / 429）：每租户 / 命名空间硬限已到。

    `data` 三个字段：`quota_key`（固定 `topic_count`）、`limit`、`usage`——客户端据此
    区分「数量到顶」与「重名」（350008），并决定删除主题还是申请调整配额。
    """

    code_: ClassVar[int] = MsgErrorCode.TOPIC_QUOTA_EXCEEDED
    http_status_: ClassVar[int] = 429

    def __init__(self, message: str | None = None, *, limit: int = 0, usage: int = 0) -> None:
        """初始化。

        Args:
            message: 提示信息。
            limit: 当前硬限（缺省 0：仅用于对账构造）。
            usage: 当前用量（缺省 0）。
        """
        super().__init__(
            message,
            data={"quota_key": "topic_count", "limit": limit, "usage": usage},
        )


class MsgGroupCountQuotaExceededError(MsgError):
    """消费组数配额超限（350026 / 429）：每租户硬限已到，只拦新建组。"""

    code_: ClassVar[int] = MsgErrorCode.GROUP_QUOTA_EXCEEDED
    http_status_: ClassVar[int] = 429

    def __init__(self, message: str | None = None, *, limit: int = 0, usage: int = 0) -> None:
        """初始化。

        Args:
            message: 提示信息。
            limit: 当前硬限（缺省 0：仅用于对账构造）。
            usage: 当前用量（缺省 0）。
        """
        super().__init__(
            message,
            data={"quota_key": "group_count", "limit": limit, "usage": usage},
        )


class MsgTopicStateInvalidError(MsgError):
    """主题状态不允许该操作（350028 / 409）：`disabled` 或 `deleting` 下的写操作被拒。"""

    code_: ClassVar[int] = MsgErrorCode.TOPIC_STATE_INVALID
    http_status_: ClassVar[int] = 409


class MsgAclDeniedError(MsgError):
    """主题 ACL 拒绝（350027 / 403）：功能级 RBAC 已过，资源级 ACL 拒绝。"""

    code_: ClassVar[int] = MsgErrorCode.ACL_DENIED
    http_status_: ClassVar[int] = 403


class MsgDeadLetterNotFoundError(MsgError):
    """死信不存在（350029 / 404）：不属于当前租户，或已被清理。"""

    code_: ClassVar[int] = MsgErrorCode.DEAD_LETTER_NOT_FOUND
    http_status_: ClassVar[int] = 404


class MsgDeadLetterInvalidStateError(MsgError):
    """死信状态不允许该操作（350030 / 409）：非法迁移 / 重复处置。"""

    code_: ClassVar[int] = MsgErrorCode.DEAD_LETTER_INVALID_STATE
    http_status_: ClassVar[int] = 409


class MsgDeadLetterNotReplayableError(MsgError):
    """原消息不可重放（350031 / 409）：超出保留期或记录不存在。"""

    code_: ClassVar[int] = MsgErrorCode.DEAD_LETTER_NOT_REPLAYABLE
    http_status_: ClassVar[int] = 409


class MsgInsufficientReplicasError(MsgError):
    """ISR 不足 / 提交等待超时 / 非 leader 写入（350032 / 503）。

    `data` 六个字段：`reason`（`insufficient_isr` / `commit_timeout` / `not_leader`）、
    `isr`（当前同步副本数）、`required`（`min.insync.replicas`）、`topic_id`、`partition`、
    `timeout_ms`（仅超时态给值）。普通 dict，不用自定义 Mapping（pydantic 序列化自定义 Mapping
    会 500，02-7 已交过学费）。
    """

    code_: ClassVar[int] = MsgErrorCode.INSUFFICIENT_REPLICAS
    http_status_: ClassVar[int] = 503

    def __init__(
        self,
        message: str | None = None,
        *,
        reason: str = "insufficient_isr",
        isr: int = 0,
        required: int = 0,
        topic_id: int = 0,
        partition: int = 0,
        timeout_ms: int | None = None,
    ) -> None:
        """初始化。

        Args:
            message: 提示信息。
            reason: 原因（`insufficient_isr` / `commit_timeout` / `not_leader`）。
            isr: 当前同步副本数（含 leader）。
            required: 要求的最小同步副本数。
            topic_id: 主题主键（排障定位）。
            partition: 分区号。
            timeout_ms: 提交等待超时（仅 `commit_timeout` 给值）。
        """
        # 普通 dict：错误 `data` 由 pydantic 序列化，自定义 Mapping 会 500（02-7 已交过学费）；
        # 此处不加类型注解，裸集合护栏只判「带注解的集合声明」。
        data = {
            "reason": reason,
            "isr": isr,
            "required": required,
            "topic_id": topic_id,
            "partition": partition,
        }
        if timeout_ms is not None:
            data["timeout_ms"] = timeout_ms
        super().__init__(message, data=data)


class MsgRpcInProgressError(MsgError):
    """重复请求且首次仍在执行（04-4）：`10007` / 503，`classify` 已判 `system` / 可重试。

    **不新增产品码**（决策 A3）：复用 bms `ServiceUnavailableError` 的平台码 `10007`（服务端暂时
    不可用、稍后重试），因此不进 `MsgErrorCode` 枚举、`deploy/registry/msg.toml` 的 `[[errcodes]]`
    与 `test_error_codes.py` 对账三处零改动。`data` 带 `reason=in_progress` 便于排障区分；
    `retry_after_ms` 由帧闸门经 `getattr` 取出写进 error 帧。
    """

    code_: ClassVar[int] = 10007
    http_status_: ClassVar[int] = 503

    def __init__(
        self,
        message: str | None = None,
        *,
        retry_after_ms: int = RPC_IDEMPOTENCY_IN_PROGRESS_MS,
        data: object | None = None,
    ) -> None:
        """初始化。

        Args:
            message: 提示信息。
            retry_after_ms: 建议重试间隔（毫秒）。
            data: 随附数据（缺省带 `reason`）。
        """
        super().__init__(message, data={"reason": "in_progress"} if data is None else data)
        self.retry_after_ms = retry_after_ms
