"""msg 数据面包：存储 / 位点 / 死信端口与投递语义（文件段与复制归 8B_02 / 8B_05）。

分层（概要设计 §2）：`log` 不依赖任何上层模块；`offset` 与 `deadletter` 同理；`router` 只依赖元数据仓储；
`delivery` 编排以上四者——**只有它可以跨模块组合**，其余模块互不引用。
"""

from bms_msg.broker.deadletter import DeadLetter, DeadLetterQueue, InMemoryDeadLetterQueue
from bms_msg.broker.delivery import Delivery, DeliveryService, DeliveryState, InflightDelivery
from bms_msg.broker.log import InMemoryMessageStore, MessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore, OffsetStore
from bms_msg.broker.router import TopicRouter

__all__ = [
    "DeadLetter",
    "DeadLetterQueue",
    "Delivery",
    "DeliveryService",
    "DeliveryState",
    "InMemoryDeadLetterQueue",
    "InMemoryMessageStore",
    "InMemoryOffsetStore",
    "InflightDelivery",
    "MessageStore",
    "OffsetStore",
    "StoredMessage",
    "TopicRouter",
]
