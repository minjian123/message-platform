"""停机偏序与摘流（Kiwi 2473；任务 07-1-2 §8）：先日志 → 再 `.delay` → 最后位点；SIGTERM 后 `/readyz` 503。

为什么偏序要钉住：位点越过日志尾是**静默丢消息**——重启后对齐把位点夹回 LEO，被跳过的那段不会报错。
`ResourceManager` 逆序释放，所以「关闭序 = 日志 → `.delay` → 位点」等价于「注册序 = 位点 → `.delay` → 日志」，
与 `configure_service` / `startup_hooks` 里的登记顺序同源（02-3 / 02-7 设计）。
"""

from typing import cast

import pytest
from bms_core.api.health import router as health_router
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.resources import ResourceManager
from bms_core.core.service import ServiceIdentity, ServiceRuntime
from bms_core.health.base import get_health_check_registry
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bms_msg.broker.lifecycle import (
    CloseableStore,
    DelayCloser,
    FlushableOffsetStore,
    OffsetFlusher,
    RebuildableDelayStore,
    StorageCloser,
)
from bms_msg.health import MsgHealthCheckRegistry

pytestmark = pytest.mark.kiwi_id(2473)


class _RecordingStore:
    """关闭顺序记录替身（三个关闭器都会调 `aclose()`）。"""

    def __init__(self, name: str, order: ConcurrentStableList[str]) -> None:
        """初始化。

        Args:
            name: 在顺序里的标识。
            order: 共享顺序表。
        """
        self._name = name
        self._order = order

    async def aclose(self) -> None:
        """记录一次关闭（幂等性由各实现保证，这里只记序）。"""
        self._order.add(self._name)


async def test_resources_close_in_reverse_registration_order() -> None:
    """T10-a：按 msg 的登记序（位点 → `.delay` → 日志）登记，释放序必须是日志 → `.delay` → 位点。"""
    order: ConcurrentStableList[str] = ConcurrentStableList()
    manager = ResourceManager()
    manager.register(OffsetFlusher(store=cast("FlushableOffsetStore", _RecordingStore("offset", order))))
    manager.register(DelayCloser(store=cast("RebuildableDelayStore", _RecordingStore("delay", order))))
    manager.register(StorageCloser(store=cast("CloseableStore", _RecordingStore("storage", order))))

    await manager.aclose()

    assert order.to_list() == ["storage", "delay", "offset"], "先刷日志，再刷 .delay，最后刷位点"
    await manager.aclose()
    assert order.to_list() == ["storage", "delay", "offset"], "重复释放幂等（登记表已清空）"


async def test_drain_cancels_readiness() -> None:
    """T10-b：`start_drain()`（SIGTERM 首次信号）→ 摘流标记与就绪取消；重复调用幂等。"""
    app = FastAPI()
    app.include_router(health_router)
    app.state.startup_complete = True
    app.dependency_overrides[get_health_check_registry] = MsgHealthCheckRegistry
    runtime = ServiceRuntime(app, ServiceIdentity(name="msg", version="9.9.9"))

    assert runtime.draining is False
    runtime.start_drain()

    assert runtime.draining is True
    assert app.state.startup_complete is False
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://probe") as client:
        response = await client.get("/readyz")
    assert response.status_code == 503, "摘流中不接受新流量"

    runtime.start_drain()  # 幂等：第二次不改状态、不抛错
    assert runtime.draining is True
