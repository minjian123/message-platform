"""msg 业务服务包：领域逻辑与事务边界（仓储只 flush，提交由工作单元负责）。"""

from bms_msg.services.topic import MAX_LIST_SIZE, TopicService

__all__ = ["MAX_LIST_SIZE", "TopicService"]
