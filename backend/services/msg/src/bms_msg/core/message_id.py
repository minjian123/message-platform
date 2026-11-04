"""message_id 生成封装（详细设计 §7）：复用基座雪花，字符串输出，与 offset 解耦。

**为什么不自己生成**：基座雪花已处理时钟回拨、序列耗尽、线程安全与 worker_id 租约，重复实现只会
引入 bug。`message_id` 是**全局唯一标识**（幂等与排障用），`offset` 是**分区内序号**，两者不绑定。

唯一性前提：同环境多副本部署须保证 `worker_id` 全局唯一（基座启动期租约保证）。
"""

from bms_core.core.id import id_generator

__all__ = ["new_message_id"]


def new_message_id() -> str:
    """生成新的 message_id（126 位雪花，十进制字符串）。

    Returns:
        str: 全局唯一的 message_id（超出 JS 安全整数，接口一律字符串）。
    """
    return str(id_generator.next_id())
