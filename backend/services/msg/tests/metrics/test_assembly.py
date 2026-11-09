"""装配与埋点分支测试（06-3-1 / Kiwi 2460）。"""

from types import SimpleNamespace
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import BizError
from pytest import mark, raises

from bms_msg.api.data import messages as messages_module
from bms_msg.broker.group import (
    GROUP_STATE_EMPTY,
    ConsumerGroup,
    GroupStore,
    InMemoryGroupStore,
)
from bms_msg.broker.lifecycle import BackgroundLoops
from bms_msg.main import ApplicationFactory


def _empty_snapshot() -> ConcurrentStableDict[str, Any]:
    """空指标快照（装配降级用例）。

    Returns:
        ConcurrentStableDict[str, Any]: 空快照。
    """
    return ConcurrentStableDict()


class _MinimalGroupStore(GroupStore):
    """最小组存储实现（不覆盖 `list_all`，验证默认空实现）。"""

    async def load(self, *, tenant_id: int, topic_id: int, group: str) -> ConsumerGroup | None:
        """无记录。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            None: 恒无记录。
        """
        return None

    async def save(self, record: ConsumerGroup) -> None:
        """空保存。

        Args:
            record: 组记录。
        """

    async def list_for_tenant(
        self, *, tenant_id: int, topic_id: int | None = None
    ) -> ConcurrentStableList[ConsumerGroup]:
        """空列表。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题过滤。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 空。
        """
        return ConcurrentStableList()

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """无删除。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 0。
        """
        return 0


class _Loop:
    """可启动 / 停止的循环替身。"""

    def __init__(self) -> None:
        """初始化。"""
        self.started = 0
        self.stopped = 0
        self.running = False

    def ensure_started(self) -> None:
        """记一次启动。"""
        self.started += 1
        self.running = True

    async def stop(self) -> None:
        """记一次停止。"""
        self.stopped += 1
        self.running = False


class _Scheduler(_Loop):
    """延迟推进器替身。"""

    def __init__(self) -> None:
        """初始化。"""
        super().__init__()
        self.wheel = SimpleNamespace()


class _Flushable(_Loop):
    """位点存储替身。"""

    async def aclose(self) -> None:
        """空关闭。"""


class _Registry:
    """资源管理器替身。"""

    def __init__(self) -> None:
        """初始化。"""
        self.registered: ConcurrentStableList[object] = ConcurrentStableList()

    def register(self, resource: object) -> None:
        """登记资源。

        Args:
            resource: 资源对象。
        """
        self.registered.add(resource)


@mark.kiwi_id(2460)
async def test_group_store_default_list_all_is_empty() -> None:
    """未覆盖 `list_all` 的实现返回空清单（不抛错）。"""
    assert not await _MinimalGroupStore().list_all()


@mark.kiwi_id(2460)
async def test_inmemory_group_store_list_all_sorted() -> None:
    """内存实现返回全部组记录（稳定排序）。"""
    store = InMemoryGroupStore()
    await store.save(
        ConsumerGroup(
            tenant_id=2,
            topic_id=20,
            group="b",
            state=GROUP_STATE_EMPTY,
            generation=0,
            members=ConcurrentStableList(),
        )
    )
    await store.save(
        ConsumerGroup(
            tenant_id=1,
            topic_id=10,
            group="a",
            state=GROUP_STATE_EMPTY,
            generation=0,
            members=ConcurrentStableList(),
        )
    )
    records = await store.list_all()
    assert [record.group for record in records] == ["a", "b"]


@mark.kiwi_id(2460)
async def test_background_loops_stops_sampler() -> None:
    """停机时先停指标采样器（随后才停推进器 / 执行器）。"""
    sampler = _Loop()
    scheduler = _Scheduler()
    runner = _Loop()
    loops = BackgroundLoops(scheduler=cast("Any", scheduler), runner=cast("Any", runner), sampler=sampler)
    await loops.aclose()
    assert sampler.stopped == 1
    assert scheduler.stopped == 1
    assert runner.stopped == 1


@mark.kiwi_id(2460)
async def test_start_background_tasks_starts_sampler() -> None:
    """启动钩子会拉起指标采样器并登记回收句柄。"""
    sampler = _Loop()
    state = SimpleNamespace(
        delay_scheduler=_Scheduler(),
        replay_runner=_Loop(),
        offset_store=_Flushable(),
        delay_store=SimpleNamespace(),
        tenant_purge_service=None,
        metrics_sampler=sampler,
        resources=_Registry(),
    )
    app = SimpleNamespace(state=state)
    from bms_msg.broker.lifecycle import start_background_tasks

    await start_background_tasks(cast("Any", app))
    assert sampler.started == 1
    assert getattr(state, "background_loops", None) is not None


@mark.kiwi_id(2460)
def test_install_metrics_degrades_without_collector_support() -> None:
    """基座不支持自定义收集器时留 warning 并装配采样器（降级不报错）。"""

    class _NoCollectorMetrics:
        def register_collector(self, collector: object) -> bool:
            """不支持。

            Args:
                collector: 收集器。

            Returns:
                bool: False。
            """
            return False

    state = SimpleNamespace(
        metrics=_NoCollectorMetrics(),
        storage_metrics=SimpleNamespace(snapshot=_empty_snapshot),
        delay_metrics=SimpleNamespace(snapshot=_empty_snapshot),
        backpressure_metrics=SimpleNamespace(snapshot=_empty_snapshot),
        group_store=_MinimalGroupStore(),
        message_store=SimpleNamespace(),
        offset_store=SimpleNamespace(),
        dead_letter_queue=SimpleNamespace(),
        uow_factory=object(),
        storage_watermark=SimpleNamespace(),
        tenant_source=SimpleNamespace(),
    )
    app = SimpleNamespace(state=state)
    settings = SimpleNamespace(app=SimpleNamespace(service="msg"))
    install = ApplicationFactory._install_metrics  # pyright: ignore[reportPrivateUsage]
    install(cast("Any", app), cast("Any", settings))
    assert getattr(state, "metrics_sampler", None) is not None


@mark.kiwi_id(2460)
async def test_track_data_plane_swallows_metric_failures(monkeypatch: Any) -> None:
    """埋点自身抛错不影响请求返回；失败请求仍上抛业务异常。"""

    class _Boom:
        def record_publish(self, **kwargs: object) -> None:
            """故意抛错。"""
            raise RuntimeError("metrics down")

        def record_publish_error(self, **kwargs: object) -> None:
            """故意抛错。"""
            raise RuntimeError("metrics down")

    monkeypatch.setattr(messages_module, "get_traffic_metrics", lambda: _Boom())
    track = messages_module._track_data_plane  # pyright: ignore[reportPrivateUsage]

    @track("publish")
    async def _ok(req: object, tenant: object) -> object:
        return SimpleNamespace(data=SimpleNamespace(messages=[]))

    @track("publish")
    async def _fail(req: object, tenant: object) -> object:
        raise BizError(350005, "boom")

    req = SimpleNamespace(topic="orders", payload="", raw_payload=None)
    tenant = SimpleNamespace(code="t1")
    assert await _ok(req=req, tenant=tenant) is not None
    with raises(BizError):
        await _fail(req=req, tenant=tenant)

    @track("publish")
    async def _bare(**kwargs: object) -> object:
        return SimpleNamespace(data=None)

    assert await _bare() is not None
