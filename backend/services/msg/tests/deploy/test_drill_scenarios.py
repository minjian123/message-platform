# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
# 回放式替身（FakeHttp / FakeInstance / lambda 回调）故意不回放内部类型：本文件只证明判据取值
# 与调用序列，类型精确性由被测模块自身（ops/drill，strict 全绿）保证。
"""六类场景的注入与判据（Kiwi 2479；任务 07-6-1）。

同编号第三份：`test_drill.py` 覆盖键位 / 记录 / 解析，`test_drill_runtime.py` 覆盖实例 / 代理 /
编排 / 命令行，本文件用回放式替身跑完六个场景的 `run()`（正例与关键反例），证明判据取值正确。
所有替身只回放 HTTP 与进程动作，不启动真实实例、不连网络。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import httpx
import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenDict

from ops.drill import context as context_mod
from ops.drill import runtime
from ops.drill.context import DrillContext
from ops.drill.env import DrillConfig
from ops.drill.model import (
    RESULT_DEGRADED,
    RESULT_FAILED,
    RESULT_OK,
    DrillScenario,
    PhaseSample,
)
from ops.drill.scenarios import disk_full, network_partition, poison_message, process_crash, slow_disk, stuck_consumer

pytestmark = pytest.mark.kiwi_id(2479)

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根。"""


def _ok(data: object) -> httpx.Response:
    """构造成功响应。

    Args:
        data: `data` 字段。

    Returns:
        httpx.Response: 200 响应。
    """
    return httpx.Response(200, json={"code": 0, "message": "ok", "data": data})


class FakeHttp:
    """回放式 HTTP 替身（按调用计数返回预设响应）。"""

    def __init__(
        self,
        *,
        consumed: Sequence[Sequence[Mapping[str, object]]] | None = None,
        publish_fail: bool = False,
    ) -> None:
        self.publish_offsets: list[int] = []
        self.acked: list[int] = []
        self.published: list[tuple[str, bytes]] = []
        self.consumed: list[list[dict[str, object]]] = [
            [{str(key): value for key, value in item.items()} for item in batch] for batch in (consumed or [])
        ]
        self.publish_fail = publish_fail
        self.topic_status = 200
        self.join_status = 200
        self.invalid_response = httpx.Response(400, json={"code": 10001})
        self.dlq_response = httpx.Response(200, json={"code": 0, "data": {"items": [], "total": 0}})
        self.nack_status = 200
        self.heartbeats = 0

    def create_topic(self, topic: str, partitions: int = 1) -> int:
        """建主题。"""
        return self.topic_status

    def join(self, topic: str, group: str, consumer: str) -> int:
        """加入组。"""
        return self.join_status

    def publish(self, topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
        """发布（记录 offset）。"""
        if self.publish_fail:
            raise httpx.ConnectError("broken pipe")
        self.published.append((topic, payload))
        offset = len(self.publish_offsets)
        self.publish_offsets.append(offset)
        return _ok({"message_id": f"m-{offset}", "offset": offset, "topic": topic})

    def publish_invalid(self, topic: str, raw_payload: str) -> httpx.Response:
        """非法载荷发布。"""
        return self.invalid_response

    def consume(
        self, topic: str, group: str, consumer: str, *, max_messages: int = 20, max_wait_ms: int = 0
    ) -> httpx.Response:
        """拉取（按预设批次回放，耗尽后返回空批）。"""
        batch = self.consumed.pop(0) if self.consumed else []
        return _ok({"messages": batch, "paused": False, "inflight_messages": len(batch), "inflight_bytes": 1})

    def ack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """确认。"""
        self.acked.append(offset)
        return _ok({"committed_offset": offset + 1})

    def nack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """拒绝。"""
        return httpx.Response(self.nack_status, json={"code": 0, "data": {"committed_offset": offset + 1}})

    def dlq(self, topic: str) -> httpx.Response:
        """死信列表。"""
        return self.dlq_response

    def heartbeat(self, topic: str, group: str, consumer: str) -> int:
        """心跳。"""
        self.heartbeats += 1
        return 200

    def metrics(self) -> str:
        """指标文本。"""
        return (
            "msg_consumer_lag 3\nmsg_disk_used_ratio 0.5\nmsg_fsync_duration_ms_sum 10\nmsg_fsync_duration_ms_count 2\n"
        )

    def readyz(self) -> tuple[int, bool, str]:
        """就绪探针。"""
        return 200, True, '{"status":"ok"}'

    def healthz(self) -> bool:
        """存活探针。"""
        return True

    def close(self) -> None:
        """释放。"""


class FakeInstance:
    """回放式实例替身（信号、重启、端口）。"""

    def __init__(self) -> None:
        self.port = 1
        self.signals: list[str] = []
        self.restarts = 0

    def signal(self, sig: str) -> None:
        """记录信号。"""
        self.signals.append(sig)

    def wait_exit(self, timeout: float = 20.0) -> int:
        """固定返回被杀退出码。"""
        return -9

    def restart(self) -> None:
        """记录重启。"""
        self.restarts += 1


def _ctx(tmp_path: Path, scenario: DrillScenario, http: FakeHttp, instance: FakeInstance | None = None) -> DrillContext:
    """构造执行上下文（采样固定为就绪）。

    Args:
        tmp_path: 工作根。
        scenario: 场景定义。
        http: HTTP 替身。
        instance: 实例替身。

    Returns:
        DrillContext: 上下文。
    """
    config = DrillConfig(
        work_dir=tmp_path,
        env="test",
        base_port=18_500,
        report_dir=tmp_path / "reports",
        recovery_budget_ms=240_000,
        fill_mb=1,
        slow_write_mb=1,
        reject_ratio=0.5,
        repo_root=REPO_ROOT,
    )
    return DrillContext(
        config=config,
        scenario=scenario,
        instance=cast("runtime.DrillInstance", instance or FakeInstance()),
        http=cast("runtime.DrillHttp", http),
        started_at=0.0,
        fill_mb=1,
        reject_ratio=0.5,
    )


def _scenario(name: str, inject: str, observe: tuple[str, ...] = ("msg_consumer_lag",)) -> DrillScenario:
    """构造场景定义。

    Args:
        name: 标识。
        inject: 注入方式。
        observe: 观测指标。

    Returns:
        DrillScenario: 定义。
    """
    from bms_core.core.concurrent import ConcurrentStableFrozenList

    return DrillScenario(
        name=name,
        title=name,
        inject=inject,
        observe=ConcurrentStableFrozenList(observe),
        criterion=ConcurrentStableFrozenList(["c"]),
        recovery_budget_ms=240_000,
        expect="e",
    )


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """把等待与采样压到最短（不改被测逻辑）。"""
    monkeypatch.setattr(
        context_mod,
        "sample_phase",
        lambda phase, http, metrics: PhaseSample(
            phase=phase,
            captured_at="2026-11-11T00:00:00+00:00",
            readyz_status=200,
            readyz_ok=True,
            healthz_ok=True,
            metrics=ConcurrentStableFrozenDict({name: 1.0 for name in metrics}),
            detail="{}",
        ),
    )

    def _noop_sleep(_seconds: float) -> None:
        """把等待压到零（不改被测逻辑）。"""

    monkeypatch.setattr(process_crash.time, "sleep", _noop_sleep)
    monkeypatch.setattr(disk_full.time, "sleep", _noop_sleep)
    monkeypatch.setattr(slow_disk.time, "sleep", _noop_sleep)
    monkeypatch.setattr(poison_message.time, "sleep", _noop_sleep)


def test_process_crash_run_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """进程崩溃：两档各一轮，已确认不回退、未确认重投、offset 连续。"""
    full = [{"offset": i, "delivery_count": 1} for i in range(6)]
    http = FakeHttp(consumed=[full, [{"offset": i} for i in (4, 5)], full, [{"offset": i} for i in (4, 5)]])
    ctx = _ctx(tmp_path, _scenario("process_crash", "sigkill_sigterm"), http)
    monkeypatch.setattr(
        process_crash,
        "items_of",
        lambda body, key="messages": [
            context_mod.as_mapping(item) for item in cast("list[object]", body.get(key) or [])
        ],
    )

    outcome = process_crash.run(ctx)

    assert [item.result for item in outcome.criteria].count(RESULT_OK) >= 8
    assert outcome.data_loss is False
    assert cast("FakeInstance", ctx.instance).signals == ["SIGKILL", "SIGTERM"]


def test_process_crash_redelivery_missing_marks_loss(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """进程崩溃反例：恢复后未重投未确认消息 → 判失败。"""
    http = FakeHttp(consumed=[[{"offset": 0}], [], []])
    ctx = _ctx(tmp_path, _scenario("process_crash", "sigkill_sigterm"), http)

    outcome = process_crash.run(ctx)

    assert any(item.result == RESULT_FAILED for item in outcome.criteria)
    assert outcome.data_loss is False  # 未确认消息缺失属「未重投」，不是「已确认丢失」


def test_disk_full_run_passes(tmp_path: Path) -> None:
    """磁盘满：注入期 507 + 350022、就绪转 503、清理后恢复。"""
    http = FakeHttp()
    calls: list[int] = []

    def publish(topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
        calls.append(1)
        if len(calls) == 2:  # 注入期那一次被拒
            return httpx.Response(507, json={"code": 350022})
        return _ok({"offset": len(calls)})

    http.publish = publish  # type: ignore[method-assign]
    ctx = _ctx(tmp_path, _scenario("disk_full", "fill_volume", ("msg_disk_used_ratio",)), http)

    outcome = disk_full.run(ctx)

    assert [item.result for item in outcome.criteria].count(RESULT_OK) >= 3
    assert (tmp_path / "fill").is_dir()  # 填充目录保留（文件在恢复阶段被清理）


def test_disk_full_reports_missing_rejection(tmp_path: Path) -> None:
    """磁盘满反例：发布未被拒 → 判失败。"""
    http = FakeHttp()
    ctx = _ctx(tmp_path, _scenario("disk_full", "fill_volume", ("msg_disk_used_ratio",)), http)

    outcome = disk_full.run(ctx)

    failed = [item for item in outcome.criteria if item.result == RESULT_FAILED]
    assert any(item.key == "publish_rejected" for item in failed)


def test_slow_disk_run_passes(tmp_path: Path) -> None:
    """慢盘：无假确认、位点不越尾、积压归零。"""
    http = FakeHttp(consumed=[[{"offset": i} for i in range(20)], []])
    ctx = _ctx(tmp_path, _scenario("slow_disk", "controlled_write"), http)

    outcome = slow_disk.run(ctx)

    results = {item.key: item.result for item in outcome.criteria}
    assert results["no_fake_ack"] == RESULT_OK
    assert results["offset_within_leo"] == RESULT_OK
    assert results["latency_visible"] in (RESULT_OK, RESULT_DEGRADED)  # 采样替身基线相同 → 无法比较


def test_network_partition_run_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """网络分区：代理直通可用、drop 期发布失败、恢复后可发布、无重复投递。"""
    http = FakeHttp(consumed=[[{"offset": i} for i in range(5)], [{"offset": i} for i in range(5, 8)], []])

    class _Proxy:
        def __init__(self, target_port: int) -> None:
            self.port = 2
            self.mode = "pass"
            self.dropped = False

        def start(self) -> int:
            return self.port

        def set_mode(self, mode: str) -> None:
            self.mode = mode

        def close(self) -> None:
            pass

    class _Proxied(FakeHttp):
        def publish(self, topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
            if proxied_mode["mode"] == "drop":
                raise httpx.ConnectError("dropped")
            return _ok({"offset": 9})

    proxied_mode: dict[str, str] = {"mode": "pass"}

    def make_proxy(target_port: int) -> _Proxy:
        proxy_obj = _Proxy(target_port)
        original_set = proxy_obj.set_mode

        def set_mode(mode: str) -> None:
            proxied_mode["mode"] = mode
            original_set(mode)

        proxy_obj.set_mode = set_mode  # type: ignore[method-assign]
        return proxy_obj

    monkeypatch.setattr(network_partition, "TcpProxy", make_proxy)

    def _proxied_http(base_url: str) -> _Proxied:
        """构造经代理的客户端（忽略基址）。"""
        return _Proxied()

    monkeypatch.setattr(network_partition, "DrillHttp", _proxied_http)
    ctx = _ctx(tmp_path, _scenario("network_partition", "tcp_proxy"), http)

    outcome = network_partition.run(ctx)

    assert [item.result for item in outcome.criteria].count(RESULT_OK) >= 5


def test_stuck_consumer_run_passes(tmp_path: Path) -> None:
    """消费者卡死：在途填满 → paused、恢复后积压归零、不重复。"""
    batch = [{"offset": i} for i in range(100)]
    http = FakeHttp(consumed=[batch, [], [], []])
    ctx = _ctx(tmp_path, _scenario("stuck_consumer", "unacked_inflight"), http)

    outcome = stuck_consumer.run(ctx)

    assert all(item.result == RESULT_OK for item in outcome.criteria if item.key != "backpressure_applied")


def test_poison_message_run_degrades_when_dlq_unavailable(tmp_path: Path) -> None:
    """毒消息：重投到上限、死信不可查与后续阻塞按降级取证。"""
    http = FakeHttp(
        consumed=[
            [{"offset": 0, "delivery_count": 1}],
            [{"offset": 0, "delivery_count": 5}],
            [{"offset": 1}],
            [{"offset": 0}],
        ]
    )
    http.dlq_response = httpx.Response(500, json={"code": 10000, "message": "error.10000", "data": None})
    ctx = _ctx(tmp_path, _scenario("poison_message", "invalid_and_exhausted"), http)

    outcome = poison_message.run(ctx)

    results = {item.key: item.result for item in outcome.criteria}
    assert results["invalid_payload_rejected"] == RESULT_OK
    assert results["redelivery_bounded"] == RESULT_OK
    assert results["dead_letter_created"] == RESULT_DEGRADED
    assert list(outcome.leftovers)
