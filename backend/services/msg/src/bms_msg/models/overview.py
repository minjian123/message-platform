"""概览快照与入口计数模型（02-8-4）：跨实例统一口径的只读观测数据。

两张表都是**可重建的观测数据**（不是业务事实源）：

- `msg_overview_snapshot`：**每租户一行**，采样器周期覆盖写；接口优先读它，读不到 / 过期才回退进程内快照。
- `msg_ingress_counter`：**租户 × 时间窗**累加入口条数；各实例只写自己的增量，读侧求和即集群总量。

口径：

- 不做软删除（观测数据，按租户 / 保留期物理删除）；
- 采样失败保留上一份（不把「采样失败」写成 0）；
- 时间窗按采样周期取整（epoch 秒），同一窗口内多实例的增量叠加。
"""

from datetime import datetime

from bms_core.models.base import BaseModel, SnowflakeId
from sqlalchemy import BigInteger, DateTime, Float, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

OVERVIEW_SNAPSHOT_TABLE = "msg_overview_snapshot"
"""概览快照表名（每租户一行）。"""

INGRESS_COUNTER_TABLE = "msg_ingress_counter"
"""入口计数表名（租户 × 时间窗）。"""


class MsgOverviewSnapshot(BaseModel):
    """概览快照（`msg_overview_snapshot`）：四张数字卡的跨实例统一出口。"""

    __tablename__ = OVERVIEW_SNAPSHOT_TABLE
    __table_args__ = (
        UniqueConstraint("tenant_id", name="uq_msg_overview_snapshot_tenant"),
        Index("idx_msg_overview_snapshot_sampled", "sampled_at"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（每租户一行）")
    messages_in_per_second: Mapped[float] = mapped_column(Float, default=0.0, comment="入口速率（条/秒，最近窗口）")
    consumer_lag: Mapped[int] = mapped_column(BigInteger, default=0, comment="当前积压（该租户全部主题与组求和）")
    dlq_total: Mapped[int] = mapped_column(BigInteger, default=0, comment="死信存量（活跃状态）")
    disk_used_ratio: Mapped[float] = mapped_column(Float, default=0.0, comment="磁盘使用率（跨卷取最大）")
    sampled_at: Mapped[datetime] = mapped_column(DateTime, comment="采样时刻（UTC；超过阈值视为过期）")


class MsgIngressCounter(BaseModel):
    """入口计数（`msg_ingress_counter`）：租户 × 时间窗的入口条数累加。"""

    __tablename__ = INGRESS_COUNTER_TABLE
    __table_args__ = (
        UniqueConstraint("tenant_id", "window_start", name="uq_msg_ingress_counter_window"),
        Index("idx_msg_ingress_counter_window", "window_start"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份")
    window_start: Mapped[int] = mapped_column(BigInteger, comment="窗口起点（epoch 秒，按采样周期取整）")
    count: Mapped[int] = mapped_column(BigInteger, default=0, comment="该窗口累计入口条数（多实例增量累加）")


__all__ = ["INGRESS_COUNTER_TABLE", "OVERVIEW_SNAPSHOT_TABLE", "MsgIngressCounter", "MsgOverviewSnapshot"]
