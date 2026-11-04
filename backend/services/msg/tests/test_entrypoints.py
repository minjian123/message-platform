"""启动入口壳（Kiwi 2411；补 00-1 遗留的模块级覆盖缺口）。

`bms_msg/asgi.py`（模块级应用实例）与 `bms_msg/__main__.py`（`python -m bms_msg` 启动）是两个入口壳，
业务用例天然不触达，按《AI开发规范》「纯声明模块用可导入 / 可解析护栏用例覆盖」处理：
`asgi` 导入即装配应用（顺带验证 00-1 的「装配可启动」在真实 `create()` 下成立）；
`__main__` 以 `run_name="__main__"` 执行并替换 `run_service`，避免真的监听端口。
"""

import importlib
import runpy

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry

pytestmark = pytest.mark.kiwi_id(2411)


def test_asgi_entry_builds_application() -> None:
    """`bms_msg.asgi` 导入即完成装配（模块级 `app` 可读、有标题）。"""
    default_plugin_registry().reset()
    from bms_msg import asgi

    assert asgi.app.title
    assert "/healthz" in asgi.app.openapi()["paths"]


def test_module_entry_calls_run_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m bms_msg` 入口：以 `ApplicationFactory` 调 `run_service`（不真起服务）。"""
    import bms_core.core.run as run_module

    called: ConcurrentStableList[object] = ConcurrentStableList()

    def _record(factory: object) -> None:
        """记录 `run_service` 的入参（替身实现）。

        Args:
            factory: 被调方传入的工厂对象。
        """
        called.add(factory)

    monkeypatch.setattr(run_module, "run_service", _record)
    default_plugin_registry().reset()
    # 普通导入：走 `__name__ != "__main__"` 分支（不启服务）
    importlib.import_module("bms_msg.__main__")
    runpy.run_module("bms_msg", run_name="__main__")

    assert len(called) == 1
    assert getattr(called[0], "service_name", "") == "msg"
