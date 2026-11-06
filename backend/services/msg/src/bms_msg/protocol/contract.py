"""帧协议契约（单一事实源；导出为公开契约的 `x-msg-frame`，任务 03-2）。

为什么要有它：`deploy/contracts/msg.json` 是**对外承诺的全量契约**，帧是同一路径上的另一形态，
不进 `/openapi.json`（否则生成式客户端会试图按 JSON 解析帧端点），但必须能被跨仓装载校验、
评审与后续 SDK 生成读到。因此把帧结构做成**代码里的常量表**，由 `ops.contract_snapshot` 在导出时
注入快照顶层——代码与快照只有一处事实源，零漂移由 `check` 自动保证。

口径来源：03-2 详细设计 §4（帧布局 / 类型 / 标志位 / 上限 / 版本规则）、03-3 详细设计 §4 与 §7
（内容类型取值 / 压缩白名单 / 消息体规则；`content_type` 键改名 `frame_media_type` 与消息体消歧）、
04-1 详细设计 §12（RPC 帧类型 48、响应位约定、请求头字段、方法清单——由 `rpc/contract.py` 摊平）。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.frame import (
    FRAME_CONTENT_TYPE,
    FRAME_HEADER_BYTES,
    FRAME_MAGIC,
    MAX_FRAME_HEADER_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
    PROTOCOL_MAJOR,
    PROTOCOL_MINOR,
    PROTOCOL_VERSION,
    FrameFlags,
    FrameType,
)
from bms_core.msgclient.payload import payload_contract

from bms_msg.core.config import (
    CONNECTION_HEARTBEAT_INTERVAL_MS,
    CONNECTION_HEARTBEAT_TIMEOUT_MS,
    INFLIGHT_MAX_BYTES,
    INFLIGHT_MAX_MESSAGES,
    MAX_CONNECTIONS,
    MAX_STREAMS_PER_CONNECTION,
    RATE_LIMIT_BURST_SECONDS,
    RATE_LIMIT_TENANT_PER_SECOND,
    RATE_LIMIT_TOPIC_PER_SECOND,
    SUBSCRIBE_MAX_WAIT_LIMIT_MS,
    SUBSCRIBE_MAX_WAIT_MS,
    SUBSCRIBE_PREFETCH,
    SUBSCRIBE_PREFETCH_LIMIT,
)
from bms_msg.errors import ErrorCategory, MsgErrorCode
from bms_msg.rpc.contract import rpc_contract

__all__ = ["FRAME_CONTRACT_KEY", "frame_contract"]

FRAME_CONTRACT_KEY = "x-msg-frame"
"""公开契约顶层扩展键（OpenAPI 规范允许 `x-` 扩展）。"""


def frame_contract() -> ConcurrentStableDict[str, Any]:
    """构造帧协议契约（导出期注入 `deploy/contracts/msg.json` 顶层）。

    Returns:
        ConcurrentStableDict[str, Any]: 帧结构 / 类型 / 标志位 / 上限 / 版本规则 / 错误帧字段。
    """
    flags: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for member in FrameFlags:
        flags.set(str(member.value.bit_length() - 1), str(member.name).lower())
    return ConcurrentStableDict(
        {
            "protocol_version": PROTOCOL_VERSION,
            "frame_media_type": FRAME_CONTENT_TYPE,
            "magic": FRAME_MAGIC.hex().upper(),
            "byte_order": "big-endian",
            "frame_header_bytes": FRAME_HEADER_BYTES,
            "fields": ConcurrentStableList(
                [
                    {"name": "magic", "width": 2, "type": "bytes", "note": "固定 4D53（ASCII MS）"},
                    {"name": "version", "width": 2, "type": "uint8 major + uint8 minor", "note": "主版本不等即 350019"},
                    {"name": "type", "width": 1, "type": "uint8", "note": "分组区间见 frame_types/ reserved_types"},
                    {"name": "flags", "width": 2, "type": "uint16", "note": "未定义位忽略"},
                    {"name": "header_len", "width": 4, "type": "uint32", "note": "header 字节数"},
                    {"name": "payload_len", "width": 4, "type": "uint32", "note": "payload 字节数"},
                    {"name": "header", "width": 0, "type": "messagepack map<str, any>", "note": "长度 = header_len"},
                    {"name": "payload", "width": 0, "type": "bytes", "note": "长度 = payload_len，broker 不解析"},
                ]
            ),
            "limits": ConcurrentStableDict(
                {"header_bytes": MAX_FRAME_HEADER_BYTES, "payload_bytes": MAX_FRAME_PAYLOAD_BYTES}
            ),
            "frame_types": ConcurrentStableDict({str(int(member)): member.name.lower() for member in FrameType}),
            "reserved_types": ConcurrentStableList(
                [
                    {"range": "7-15", "name": "data-plane-reserved"},
                    {"range": "18-31", "name": "control-plane-reserved"},
                    {"range": "32-47", "name": "admin"},
                    {"range": "48-63", "name": "rpc"},
                    {"range": "64-255", "name": "reserved"},
                ]
            ),
            "known_unsupported": ConcurrentStableDict(),
            "connection": ConcurrentStableDict(
                {
                    "identity": (
                        "tenant|client_host:client_port（同一 HTTP/2 连接内恒定；"
                        "服务器实现行为，非 ASGI 保证，换服务器须复验）"
                    ),
                    "heartbeat": ConcurrentStableDict(
                        {
                            "frame_type": int(FrameType.HEARTBEAT),
                            "endpoint": "POST /msg/connection/heartbeat",
                            "interval_ms": CONNECTION_HEARTBEAT_INTERVAL_MS,
                            "timeout_ms": CONNECTION_HEARTBEAT_TIMEOUT_MS,
                            "payload": "必须为空（心跳不带业务字段，避免成为第二个鉴权入口）",
                            "also_refreshed_by": "GET /msg/connection（无帧能力客户端的保活入口）",
                        }
                    ),
                    "idle_timeout": "max(2 × keep_alive_timeout, heartbeat_timeout)（由 [server] 配置派生）",
                    "limits": ConcurrentStableDict(
                        {
                            "max_connections": MAX_CONNECTIONS,
                            "max_streams_per_connection": MAX_STREAMS_PER_CONNECTION,
                            "protocol_layer": "hypercorn h2_max_concurrent_streams（SETTINGS 下发，客户端自限）",
                            "app_layer": "长驻流按连接键计数（350021）；连接数 350020",
                        }
                    ),
                    "drain": ConcurrentStableDict(
                        {
                            "signal_fields": ConcurrentStableList(["draining", "retry_after_ms"]),
                            "goaway": "由 hypercorn 停机路径发送；空闲回收不发 GOAWAY，客户端按普通断线处理",
                        }
                    ),
                    "close_reasons": ConcurrentStableList(["client_closed", "deadline", "member_lost", "error"]),
                }
            ),
            "flags": flags,
            "envelope": ConcurrentStableDict(
                {
                    "required": ConcurrentStableList(["request_id"]),
                    "optional": ConcurrentStableList(
                        ["protocol_version", "tenant", "trace_id", "content_type", "compression", "uncompressed_len"]
                    ),
                }
            ),
            "endpoint_fields": ConcurrentStableDict(
                {
                    "publish": {
                        "required": ConcurrentStableList(["namespace", "topic"]),
                        "optional": ConcurrentStableList(
                            [
                                "key",
                                "headers",
                                "trace_id",
                                "delay_level",
                                "content_type",
                                "compression",
                                "uncompressed_len",
                            ]
                        ),
                        "payload": "消息体原始字节（可压缩；broker 不解析）",
                    },
                    "consume": {
                        "required": ConcurrentStableList(["namespace", "topic", "group", "consumer_id", "partition"]),
                        "optional": ConcurrentStableList(["max_messages", "acks"]),
                        "payload": "必须为空",
                    },
                    "ack": {
                        "required": ConcurrentStableList(
                            ["namespace", "topic", "group", "consumer_id", "partition", "offset"]
                        ),
                        "optional": ConcurrentStableList([]),
                        "payload": "必须为空",
                    },
                    "nack": {
                        "required": ConcurrentStableList(
                            ["namespace", "topic", "group", "consumer_id", "partition", "offset", "reason"]
                        ),
                        "optional": ConcurrentStableList(["detail"]),
                        "payload": "必须为空（拒绝原因走 header）",
                    },
                }
            ),
            "error_frame": ConcurrentStableDict(
                {
                    "fields": ConcurrentStableList(
                        [
                            "code",
                            "message",
                            "retryable",
                            "layer",
                            "category",
                            "request_id",
                            "detail",
                            "retry_after_ms",
                        ]
                    ),
                    "layers": ConcurrentStableList(["transport", "protocol", "business", "system"]),
                    # 04-4：category 与 layer 正交——前者回答「调用方该怎么办」，后者回答「错在哪一层」。
                    "categories": ConcurrentStableList([category.value for category in ErrorCategory]),
                    "http_status": "帧可解析失败一律 200（错误在帧内）；magic 不符 / 长度越界为 400 + JSON",
                }
            ),
            "version_rule": ConcurrentStableDict(
                {
                    "accept_major": ConcurrentStableList([PROTOCOL_MAJOR]),
                    "current_minor": PROTOCOL_MINOR,
                    "minor": "any（只增可选字段 / 新增帧类型都不升次版本；未知字段与未知类型一律忽略）",
                    "incompatible": "350019 + 结束该 stream，由客户端断开连接（GOAWAY 归 03-4）",
                }
            ),
            "stream": ConcurrentStableDict(
                {
                    "endpoints": ConcurrentStableList(
                        [
                            "GET /msg/subscribe（SSE：text/event-stream）",
                            "POST /msg/subscribe（帧流：application/x-msg-frame）",
                        ]
                    ),
                    "subscribe_frame_type": int(FrameType.SUBSCRIBE),
                    "open_frame": (
                        "type=subscribe + flags.response；header 带 stream_id / max_seconds / max_wait_ms / "
                        "poll_interval_ms / prefetch / inflight_messages / inflight_bytes（等价 SSE 的 open 事件）"
                    ),
                    "message_frame_type": int(FrameType.MESSAGE),
                    "message_header_fields": ConcurrentStableList(
                        [
                            "request_id",
                            "message_id",
                            "topic",
                            "partition",
                            "offset",
                            "key",
                            "trace_id",
                            "delivery_count",
                            "published_at",
                            "headers",
                            "content_type",
                            "compression",
                            "uncompressed_len",
                        ]
                    ),
                    "message_payload": "消息体原始字节（不 base64；三字段随 header 回显，与 SSE / JSON 同口径）",
                    "end_of_stream": (
                        "正文 EOF 即正常结束（deadline / 客户端断开不发结束帧）；流内异常先发 error 帧再 EOF"
                    ),
                    "connection_window": (
                        "h2 连接级窗口由同连接所有 stream 共享：一条流停读会占满连接窗口、连带其它 stream 暂停发送；"
                        "应用层（协程 / CPU / inflight 配额）互不影响。这是协议语义，不是实现缺陷"
                    ),
                }
            ),
            "backpressure": ConcurrentStableDict(
                {
                    "max_wait_ms": ConcurrentStableDict(
                        {
                            "default": SUBSCRIBE_MAX_WAIT_MS,
                            "limit": SUBSCRIBE_MAX_WAIT_LIMIT_MS,
                            "zero": "不挂起（立即返回，等价 03-5 之前的行为）",
                            "on_timeout": "返回空批次（200），不是错误；350004 是 RPC 预算耗尽（04-2），与长轮询无关",
                        }
                    ),
                    "inflight": ConcurrentStableDict(
                        {
                            "scope": "(group, topic_id, partition_no)——01-4 成员制保证同分区单持有者",
                            "max_messages": INFLIGHT_MAX_MESSAGES,
                            "max_bytes": INFLIGHT_MAX_BYTES,
                            "bytes_basis": "len(payload) 线上字节（broker 不解压）",
                            "on_limit": "暂停推送：consume 返回空批 + paused=true；流只等不推；不返回错误码",
                            "acks_zero": "acks=0 不登记在途，不受阈值约束",
                        }
                    ),
                    "prefetch": ConcurrentStableDict(
                        {
                            "default": SUBSCRIBE_PREFETCH,
                            "limit": SUBSCRIBE_PREFETCH_LIMIT,
                            "counted_as_inflight": True,
                        }
                    ),
                    "rate_limit": ConcurrentStableDict(
                        {
                            "algorithm": "token-bucket",
                            "scopes": ConcurrentStableList(["tenant", "namespace+topic", "rpc"]),
                            "default_tenant_per_second": RATE_LIMIT_TENANT_PER_SECOND,
                            "default_topic_per_second": RATE_LIMIT_TOPIC_PER_SECOND,
                            "default_note": "0 = 不限（缺省关闭，数值由 05-3 / 8B_12 给）",
                            "burst_seconds": RATE_LIMIT_BURST_SECONDS,
                            "covers_delayed_publish": True,
                            "reject_code": int(MsgErrorCode.RATE_LIMITED),
                            "http": "429 + Retry-After（秒，向上取整）；帧形态 error 帧带 retry_after_ms",
                        }
                    ),
                    "metrics": ConcurrentStableList(
                        [
                            "msg_inflight_messages",
                            "msg_inflight_bytes",
                            "msg_window_stall_total",
                            "msg_rate_limit_rejected_total",
                            "msg_long_poll_waiters",
                            "msg_long_poll_changed_total",
                            "msg_long_poll_timeout_total",
                            "msg_long_poll_rejected_total",
                        ]
                    ),
                    "metrics_note": "本服务只提供 collector（snapshot），HTTP / Prometheus 出口归 06-3",
                }
            ),
            **payload_contract(),
            **rpc_contract(),
        }
    )
