"""msg 数据访问包：按模块成文件，仓储只 `flush`、事务边界归服务层工作单元。"""

from bms_msg.repositories.topic import PartitionRepository, TopicRepository

__all__ = ["PartitionRepository", "TopicRepository"]
