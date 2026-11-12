"""运维 CLI 共用运行时（02-8）：配置 / 平台库 / 审计 / 控制通道 / 迁移服务的装配。

**为什么单独一层**：`ops.rebalance` 与 `ops.failover` 需要同一套装配（否则一个连库、一个连不通，
排障时无法互证）；本模块只做「按配置建好依赖」，不做任何业务判定。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import cast

from bms_core.audit.emit import AuditEventEmitter
from bms_core.core.config import Settings
from bms_core.core.plugin import resolve_plugin
from bms_core.db.engine import EngineFactory
from bms_core.db.keys import PLATFORM_DB_KEY
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import SessionFactory, session_scope
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.oauth.token import BaseServiceTokenIssuer

from bms_msg.broker.election import LeaderSwitchMetrics, LeaderSwitchService
from bms_msg.broker.network import (
    HttpLeaderControlTransport,
    InternalChannelConfig,
    ServiceTokenProvider,
    TransportErrorMetrics,
)
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.replication import ReplicaConfig
from bms_msg.services.leader_switch import DbLeaderSwitchStore
from bms_msg.services.migration_ledger import DbMigrationLedger, MigrationLedger
from bms_msg.services.partition_assignment import (
    PartitionAssigner,
    PartitionAssignmentConfig,
)
from bms_msg.services.partition_migration import (
    DbMigrationTopology,
    MigrationTopology,
    PartitionMigrationService,
)

__all__ = ["ClusterRuntime", "RuntimeBundle", "build_runtime"]


@dataclass
class ClusterRuntime:
    """一次 CLI 运行持有的资源与已装配服务。"""

    settings: Settings
    registry: EngineRegistry
    control: HttpLeaderControlTransport
    ledger: MigrationLedger
    topology: MigrationTopology
    switch: LeaderSwitchService
    assigner: PartitionAssigner
    migration: PartitionMigrationService
    switch_metrics: LeaderSwitchMetrics
    transport_metrics: TransportErrorMetrics
    _session_factory: SessionFactory
    _uow_factory: UowFactory

    async def aclose(self) -> None:
        """释放连接池与引擎（幂等）。"""
        await self.control.aclose()
        await self.registry.aclose()


RuntimeBundle = ClusterRuntime
"""兼容别名（用例按 `RuntimeBundle` 引用）。"""


def _uow_factory(registry: EngineRegistry, session_factory: SessionFactory) -> UowFactory:
    """构造平台库工作单元工厂（无请求上下文）。

    Args:
        registry: 引擎注册表。
        session_factory: 会话工厂。

    Returns:
        object: 工作单元工厂（签名见 `UowFactory`）。
    """

    @asynccontextmanager
    async def _open() -> AsyncGenerator[UnitOfWork]:
        """开一个平台库短会话。

        Yields:
            UnitOfWork: 工作单元。
        """
        async with session_scope(registry, db_key=PLATFORM_DB_KEY, factory=session_factory) as session:
            yield DbUnitOfWork(session)

    return _open


async def build_runtime(*, settings: Settings | None = None) -> ClusterRuntime:
    """按配置装配 CLI 运行时。

    Args:
        settings: 应用配置（None 取 `Settings()`）。

    Returns:
        ClusterRuntime: 已装配运行时（调用方负责 `aclose()`）。

    Raises:
        ConfigError: 多实例端点 / 端口配置非法（fail-closed）。
        PluginError: 服务令牌签发者不可用。
    """
    resolved = settings if settings is not None else Settings()
    replica_config = ReplicaConfig.from_env()
    channel = InternalChannelConfig.from_env(
        enabled=replica_config.enabled,
        node_id=replica_config.node_id,
        nodes=replica_config.nodes,
        business_port=int(resolved.server.port),
    )
    engine_factory = EngineFactory(resolved, allow_cross_service=True)
    registry = EngineRegistry(engine_factory)
    await registry.get(PLATFORM_DB_KEY)
    session_factory = SessionFactory()
    uow_factory = _uow_factory(registry, session_factory)
    audit = AuditEventEmitter(registry=registry, session_factory=session_factory, settings=resolved)
    issuer = cast(
        "BaseServiceTokenIssuer",
        resolve_plugin(
            "service_token",
            resolved.service_token.provider,
            expected_version=BaseServiceTokenIssuer.contract_version,
        ),
    )
    tokens = ServiceTokenProvider(issuer)
    transport_metrics = TransportErrorMetrics()
    control = HttpLeaderControlTransport(config=channel, tokens=tokens, metrics=transport_metrics)
    switch_metrics = LeaderSwitchMetrics()
    switch = LeaderSwitchService(
        enabled=replica_config.enabled,
        store=DbLeaderSwitchStore(uow_factory=uow_factory),
        control=control,
        audit=audit,
        grace_ms=replica_config.switch_grace_ms,
        follower_fresh_ms=replica_config.lag_time_max_ms,
        metrics=switch_metrics,
    )
    assigner = PartitionAssigner(
        PartitionAssignmentConfig(
            enabled=replica_config.enabled,
            node_id=replica_config.node_id,
            nodes=replica_config.nodes,
            replication_factor=replica_config.replication_factor,
            min_insync_replicas=replica_config.min_insync_replicas,
        )
    )
    ledger = DbMigrationLedger(uow_factory=uow_factory)
    topology = DbMigrationTopology(uow_factory=uow_factory)
    migration = PartitionMigrationService(
        assigner=assigner,
        ledger=ledger,
        topology=topology,
        switch=switch,
        control=control,
        audit=audit,
    )
    return ClusterRuntime(
        settings=resolved,
        registry=registry,
        control=control,
        ledger=ledger,
        topology=topology,
        switch=switch,
        assigner=assigner,
        migration=migration,
        switch_metrics=switch_metrics,
        transport_metrics=transport_metrics,
        _session_factory=session_factory,
        _uow_factory=uow_factory,
    )
