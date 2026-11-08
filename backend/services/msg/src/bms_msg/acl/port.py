"""基座 `resource_acl` 端口实现（05-3）：`MsgTopicAcl`。

- **判定只读快照**：`check()` 同步、纯函数，不查库、不写库、不缓存判定结果；
- 快照由 msg 异步守卫（`TopicAclService`）按版本戳装载；未经守卫装载时 `check` 直接拒绝
  （fail-closed，不静默放行）；
- 插件工厂持有应用引用，惰性读 `app.state.topic_acl_cache`（`configure_plugins` 早于
  `configure_service`，不能在构造期取缓存实例）。
"""

from __future__ import annotations

from collections.abc import Callable

from bms_core.core.exceptions import ResourceAccessDeniedError
from bms_core.core.factory import BasePluginFactory
from bms_core.core.plugin import DEFAULT_CONTRACT_VERSION
from bms_core.extension.base import RESOURCE_ACL_KEY
from bms_core.resource_scope.acl import AclContext, BaseResourceAcl

from bms_msg.acl.cache import TopicAclCache
from bms_msg.acl.engine import decide
from bms_msg.acl.keys import MSG_TOPIC_ACL_PLUGIN

__all__ = ["MSG_TOPIC_ACL_PLUGIN", "MsgTopicAcl", "MsgTopicAclFactory"]


class MsgTopicAcl(BaseResourceAcl):
    """msg 主题 ACL 端口实现：按进程内规则快照判定。"""

    plugin_name: str = MSG_TOPIC_ACL_PLUGIN
    contract_version: str = DEFAULT_CONTRACT_VERSION

    def __init__(self, *, cache_provider: Callable[[], TopicAclCache | None]) -> None:
        """初始化。

        Args:
            cache_provider: 快照缓存读取器（惰性取 `app.state.topic_acl_cache`）。
        """
        self._cache_provider = cache_provider

    def check(self, ctx: AclContext, *, action: str) -> None:
        """判定主体能否对主题执行动作（允许即返回，拒绝抛 82013）。

        Args:
            ctx: 判定入参（租户 / 命名空间 / 主题 / 操作人 / 角色）。
            action: 四类 ACL 动作之一。

        Raises:
            ResourceAccessDeniedError: 拒绝（82013 / 403；不区分资源是否存在）。
        """
        cache = self._cache_provider()
        if cache is None:
            raise ResourceAccessDeniedError("ACL 快照缓存未装配，拒绝访问")
        snapshot = cache.get(ctx.tenant_id, ctx.module_key)
        if snapshot is None:
            raise ResourceAccessDeniedError("ACL 快照未装载，拒绝访问")
        if not decide(snapshot, ctx, action=action):
            raise ResourceAccessDeniedError("无权访问该主题")


class MsgTopicAclFactory(BasePluginFactory[MsgTopicAcl]):
    """`resource_acl:msg.topic` 工厂（插件注册表构建期登记）。"""

    plugin_key: str = RESOURCE_ACL_KEY
    plugin_name: str = MSG_TOPIC_ACL_PLUGIN

    def __init__(self, app: object) -> None:
        """初始化。

        Args:
            app: 应用实例（惰性读取 `app.state.topic_acl_cache`）。
        """
        self._app = app

    def create(self, options: object = None) -> MsgTopicAcl:
        """构造端口实现。

        Args:
            options: 未使用（零参口径）。

        Returns:
            MsgTopicAcl: 端口实现。
        """
        del options
        state = getattr(self._app, "state", None)
        return MsgTopicAcl(cache_provider=lambda: getattr(state, "topic_acl_cache", None))
