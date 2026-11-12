"""演练判据采集链（02-8）：真实 HTTP 发布 / 刷新验证 + 分区状态快照 + 判据推导。

**为什么单独一层**：`ops.cluster_drill.__main__` 只编排「起停 / 注入 / 探针」，
判据所需的事实（发布位点、切主前后 leader/epoch、ISR 变化）在这里统一采集与推导，
纯函数部分可用例直接喂数据，无需真起容器。
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.parse import quote

import httpx
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ops.cluster_drill.config import ClusterDrillError
from ops.drill.model import RESULT_DEGRADED, RESULT_FAILED, RESULT_OK

__all__ = [
    "ChainError",
    "DrillClient",
    "PartitionState",
    "PublishRecord",
    "Publisher",
    "SingleShotPublisher",
    "SwitchItem",
    "TopicState",
    "derive_facts",
    "last_json_object",
    "load_topic_state",
    "parse_switch_result",
    "parse_topic_state",
    "platform_dsn",
]

NOT_LEADER = 350033
"""写非 leader 分区的业务码（刷新后改投）。"""

INSUFFICIENT_ISR = 350032
"""ISR 不足（`acks=all` 且未达最小同步副本）。"""

TOPIC_EXISTS = 350008
"""主题已存在（重复准备时复用）。"""

MARKER_ROUNDS = 12
"""恢复后为覆盖全部分区所做的标记发布轮数上限。"""

_ISR_CONVERGE_SEC = 20.0
"""发布前的 ISR 收敛等待（秒）：落后副本需先收到一次写请求才建立拉取态。"""


class Publisher(Protocol):
    """发布能力协议（`DrillClient` 与其测试替身都满足）。"""

    async def publish(self, key: str, payload: bytes, *, acks: str = ...) -> PublishRecord:
        """发布一条消息。"""
        ...


class SingleShotPublisher(Protocol):
    """指定实例单发协议（换主探测用）。"""

    async def publish_at(self, base: str, key: str) -> ConcurrentStableFrozenDict[str, object]:
        """向指定实例发一条写并返回原始结果。"""
        ...


class ChainError(ClusterDrillError):
    """采集链失败（消息带 `cluster-drill.chain` 前缀）。"""


@dataclass(frozen=True)
class PublishRecord(BaseValueObject):
    """一条已提交消息的写入结果。"""

    partition: int
    offset: int
    message_id: str
    attempts: int = 0
    """为成功而改投的次数（>0 说明触发过 `350033` 刷新）。"""


@dataclass(frozen=True)
class PartitionState(BaseValueObject):
    """分区状态快照（来自 `ops.failover status --json`）。"""

    partition_no: int
    leader: str
    epoch: int
    replicas: ConcurrentStableFrozenList[str]
    switch_state: str = "idle"


@dataclass(frozen=True)
class TopicState(BaseValueObject):
    """主题状态快照。"""

    partitions: ConcurrentStableFrozenList[PartitionState]

    def leader_of(self, partition_no: int) -> str:
        """取分区 leader（缺该分区返回空串）。

        Args:
            partition_no: 分区号。

        Returns:
            str: leader 标识。
        """
        for item in self.partitions:
            if item.partition_no == partition_no:
                return item.leader
        return ""

    def epoch_of(self, partition_no: int) -> int:
        """取分区 epoch（缺该分区返回 -1）。

        Args:
            partition_no: 分区号。

        Returns:
            int: epoch。
        """
        for item in self.partitions:
            if item.partition_no == partition_no:
                return item.epoch
        return -1


@dataclass(frozen=True)
class SwitchItem(BaseValueObject):
    """一次分区切主的结果（来自 `ops.failover switch --json`）。"""

    partition_no: int
    from_leader: str
    to_leader: str
    from_epoch: int
    to_epoch: int
    isr_verified: bool
    retained_tail: int = 0


def _as_dict(value: object) -> dict[str, object] | None:
    """把 JSON 值收窄为 `dict[str, object]`（非对象返回 None）。

    Args:
        value: 任意 JSON 值。

    Returns:
        dict[str, object] | None: 对象视图。
    """
    if not isinstance(value, dict):
        return None
    return cast("dict[str, object]", value)


def last_json_object(text: str) -> object:
    """从混有日志的 stdout 里取出最后一个 JSON 对象。

    `ops.failover` 的 structlog 日志与 `--json` 结果**同走 stdout**，直接 `json.loads`
    会报 `Extra data`；此处从后往前试每个 `{`，取第一个能整段解析的对象。

    Args:
        text: 命令原始输出。

    Returns:
        object: 解析后的 JSON 对象。

    Raises:
        ChainError: 找不到可解析对象。
    """
    stripped = text.strip()
    for index in range(len(stripped) - 1, -1, -1):
        if stripped[index] != "{":
            continue
        try:
            return json.loads(stripped[index:])
        except ValueError:
            continue
    raise ChainError("cluster-drill.chain 输出中未找到 JSON 对象")


def parse_topic_state(payload: object) -> TopicState:
    """解析 `ops.failover status --json` 输出。

    Args:
        payload: JSON 解析后的对象。

    Returns:
        TopicState: 分区状态快照。

    Raises:
        ChainError: 形态不符。
    """
    document = _as_dict(payload)
    if document is None:
        raise ChainError("cluster-drill.chain 分区状态形态非法")
    rows = document.get("partitions")
    if not isinstance(rows, list):
        raise ChainError("cluster-drill.chain 分区状态缺少 partitions")
    partitions: ConcurrentStableList[PartitionState] = ConcurrentStableList()
    for item in cast("list[object]", rows):
        row = _as_dict(item)
        if row is None:
            raise ChainError("cluster-drill.chain 分区行形态非法")
        replicas = row.get("replicas")
        partitions.add(
            PartitionState(
                partition_no=int(cast("int", row.get("partition_no") or 0)),
                leader=str(row.get("leader") or ""),
                epoch=int(cast("int", row.get("epoch") or 0)),
                replicas=ConcurrentStableFrozenList(
                    str(entry) for entry in cast("list[object]", replicas if isinstance(replicas, list) else [])
                ),
                switch_state=str(row.get("switch_state") or "idle"),
            )
        )
    return TopicState(partitions=ConcurrentStableFrozenList(partitions))


def parse_switch_result(payload: object) -> ConcurrentStableFrozenList[SwitchItem]:
    """解析 `ops.failover switch --json` 输出（失败项不参与判据）。

    Args:
        payload: JSON 解析后的对象。

    Returns:
        ConcurrentStableFrozenList[SwitchItem]: 成功切换项。

    Raises:
        ChainError: 形态不符。
    """
    document = _as_dict(payload)
    if document is None:
        raise ChainError("cluster-drill.chain 切主结果形态非法")
    rows = document.get("items")
    if not isinstance(rows, list):
        raise ChainError("cluster-drill.chain 切主结果缺少 items")
    items: ConcurrentStableList[SwitchItem] = ConcurrentStableList()
    for item in cast("list[object]", rows):
        row = _as_dict(item)
        if row is None:
            raise ChainError("cluster-drill.chain 切主项形态非法")
        items.add(
            SwitchItem(
                partition_no=int(cast("int", row.get("partition_no") or 0)),
                from_leader=str(row.get("from_leader") or ""),
                to_leader=str(row.get("to_leader") or ""),
                from_epoch=int(cast("int", row.get("from_epoch") or 0)),
                to_epoch=int(cast("int", row.get("to_epoch") or 0)),
                isr_verified=bool(row.get("isr_verified")),
                retained_tail=int(cast("int", row.get("retained_tail") or 0)),
            )
        )
    return ConcurrentStableFrozenList(items)


class DrillClient(BaseFrameworkObject):
    """数据面 / 管理面 HTTP 客户端（真实发布 + 建主题；租户走 `X-Tenant-ID`）。"""

    def __init__(
        self,
        bases: ConcurrentStableList[str],
        *,
        tenant: str,
        namespace: str,
        topic: str,
        timeout_sec: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """初始化。

        Args:
            bases: 三实例业务地址（`http://127.0.0.1:<隧道端口>`）。
            tenant: 租户编码（数据面 dev 覆盖 / 管理面头）。
            namespace: 命名空间（= 已启用 module_key）。
            topic: 主题名。
            timeout_sec: 单请求超时（秒）。
            transport: httpx 传输（用例注入 `MockTransport`）。
            sleeper: 等待函数（`350032` 重试间隔）。
        """
        self._bases = ConcurrentStableList(bases)
        self._tenant = tenant
        self._namespace = namespace
        self._topic = topic
        self._timeout = timeout_sec
        self._sleep = sleeper if sleeper is not None else asyncio.sleep
        self._client = httpx.AsyncClient(timeout=timeout_sec, transport=transport)

    async def aclose(self) -> None:
        """关闭底层连接池。"""
        await self._client.aclose()

    async def _call(
        self, method: str, base: str, path: str, payload: object | None = None
    ) -> tuple[int, dict[str, object]]:
        """发一次请求并返回 `(status, body)`（业务错误不抛；非对象响应按空表处理）。"""
        try:
            response = await self._client.request(
                method,
                f"{base}{path}",
                json=payload,
                headers={"X-Tenant-ID": self._tenant},
            )
        except httpx.HTTPError:
            return 0, {}
        try:
            body: object = response.json()
        except ValueError:
            body = {}
        return response.status_code, _as_dict(body) or {}

    async def ensure_topic(self, partition_count: int) -> int:
        """确保主题存在并返回主键（已存在需显式用 `--topic-id`）。

        Args:
            partition_count: 分区数（仅新建时使用）。

        Returns:
            int: 主题主键。

        Raises:
            ChainError: 创建失败或已存在但未给 `--topic-id`。
        """
        status, body = await self._call(
            "POST",
            self._bases[0],
            "/api/v1/msg/admin/topics",
            {
                "namespace": self._namespace,
                "name": self._topic,
                "partition_count": partition_count,
            },
        )
        data = _as_dict(body.get("data"))
        code = body.get("code")
        if status == 200 and data is not None:
            return int(str(data.get("id") or 0))
        if code == TOPIC_EXISTS:
            raise ChainError("cluster-drill.chain 主题已存在：请显式传 --topic-id 复用")
        raise ChainError(f"cluster-drill.chain 建主题失败：status={status} code={code}")

    async def publish(self, key: str, payload: bytes, *, acks: str = "all") -> PublishRecord:
        """发布一条消息（`350033` 改投下一实例，`350032` 等待 ISR 回升后重试）。

        Args:
            key: 分区键。
            payload: 消息体。
            acks: 确认档位（`all` / `1`）。

        Returns:
            PublishRecord: 写入结果。

        Raises:
            ChainError: 全部实例均失败。
        """

        body = {
            "namespace": self._namespace,
            "topic": self._topic,
            "key": key,
            "payload": base64.b64encode(payload).decode(),
            "acks": acks,
        }
        last = ""
        for round_no in range(40):
            if round_no and round_no % 10 == 0:
                # 长时间未成功：重新预热（副本拉取态 / epoch 可能已失效），再继续改投
                await self.warmup()
            # 轮次之间必须等：故障恢复后各节点的拓扑刷新是异步的（秒级），
            # 若不等待，`350033` 会在毫秒内把 40 轮预算烧光（真机演练踩坑）。
            if round_no:
                await self._sleep(3.0)
            for attempt, base in enumerate(self._bases):
                status, reply = await self._call("POST", base, "/api/v1/msg/publish", body)
                data = _as_dict(reply.get("data"))
                code = reply.get("code")
                if status == 200 and data is not None:
                    return PublishRecord(
                        partition=int(cast("int", data.get("partition") or 0)),
                        offset=int(cast("int", data.get("offset") or 0)),
                        message_id=str(data.get("message_id") or ""),
                        attempts=attempt + round_no,
                    )
                last = f"status={status} code={code}"
                if code == NOT_LEADER:
                    continue
                if code == INSUFFICIENT_ISR:
                    await self._sleep(6.0)
                    break
                raise ChainError(f"cluster-drill.chain 发布失败：{last}")
        raise ChainError(f"cluster-drill.chain 发布失败（重试耗尽）：{last}")

    async def probe_leader(self, base: str) -> str:
        """向指定实例探一条写，返回它自报的 leader（成功 = 自己就是 leader，返回空串）。

        Args:
            base: 实例地址。

        Returns:
            str: 服务端自报的 leader 标识（无 / 自己是 leader 为空串）。
        """
        status, reply = await self._call(
            "POST",
            base,
            "/api/v1/msg/publish",
            {
                "namespace": self._namespace,
                "topic": self._topic,
                "key": "probe",
                "payload": base64.b64encode(b"probe").decode(),
                "acks": "1",
            },
        )
        if status == 200:
            return ""
        data = _as_dict(reply.get("data"))
        return str(data.get("leader") or "") if data is not None else ""

    async def publish_at(self, base: str, key: str) -> ConcurrentStableFrozenDict[str, object]:
        """向**指定实例**发一条写（不重试），返回原始结果（状态 / 分区 / 自报 leader）。

        Args:
            base: 实例地址。
            key: 分区键。

        Returns:
            ConcurrentStableFrozenDict[str, object]: `status` / `partition` / `leader`。
        """
        status, reply = await self._call(
            "POST",
            base,
            "/api/v1/msg/publish",
            {
                "namespace": self._namespace,
                "topic": self._topic,
                "key": key,
                "payload": base64.b64encode(b"probe").decode(),
                "acks": "1",
            },
        )
        data = _as_dict(reply.get("data"))
        result: ConcurrentStableDict[str, object] = ConcurrentStableDict({"status": status})
        if data is not None:
            result.set("partition", int(cast("int", data.get("partition", -1))))
            result.set("leader", str(data.get("leader") or ""))
        return ConcurrentStableFrozenDict(result)

    async def probe_partition_leaders(self, key: str) -> ConcurrentStableDict[int, str]:
        """向各实例发一条写，收集 `350033` 回复里的「分区 → 自报 leader」。

        用于注入期判断「旧 leader 已被替换」：幸存实例会把写指向新 leader。

        Args:
            key: 分区键。

        Returns:
            ConcurrentStableDict[int, str]: 分区号 → leader（缺失表示未取得）。
        """
        leaders: ConcurrentStableDict[int, str] = ConcurrentStableDict()
        for base in self._bases:
            _, reply = await self._call(
                "POST",
                base,
                "/api/v1/msg/publish",
                {
                    "namespace": self._namespace,
                    "topic": self._topic,
                    "key": key,
                    "payload": base64.b64encode(b"probe").decode(),
                    "acks": "1",
                },
            )
            data = _as_dict(reply.get("data"))
            if data is not None and data.get("leader"):
                leaders.set(int(cast("int", data.get("partition") or 0)), str(data.get("leader")))
        return leaders

    async def warmup(self) -> None:
        """向**每个实例**各发一遍写请求（非 leader 会 `350033`），让各节点为全部分区建立复制态。

        只写 leader 的话，副本永远不建拉取态，ISR 会停在 1，`acks=all` 报 `350032`；
        故障重启后同样需要「碰一次」才会重新拉取。
        """
        for base in self._bases:
            for index in range(30):
                await self._call(
                    "POST",
                    base,
                    "/api/v1/msg/publish",
                    {
                        "namespace": self._namespace,
                        "topic": self._topic,
                        "key": f"k{index % 10}",
                        "payload": base64.b64encode(b"warmup").decode(),
                        "acks": "1",
                    },
                )
        await self._sleep(_ISR_CONVERGE_SEC)

    async def publish_batch(self, count: int, *, warm: bool = True) -> ConcurrentStableList[PublishRecord]:
        """按分区键发布一批消息并记录每条写入位点（缺省先预热再 `acks=all`）。

        Args:
            count: 条数。
            warm: 是否先做副本预热（调用方已预热时置 False，避免重复等待）。

        Returns:
            ConcurrentStableList[PublishRecord]: 写入结果（声明序）。
        """
        records: ConcurrentStableList[PublishRecord] = ConcurrentStableList()
        if warm:
            await self.warmup()
        for index in range(count):
            records.add(await self.publish(f"k{index % 10}", f"drill-{index:03d}".encode()))
        return records


def _offsets_by_partition(
    records: ConcurrentStableList[PublishRecord],
) -> ConcurrentStableDict[int, ConcurrentStableList[int]]:
    """按分区归集位点（升序）。"""
    grouped: ConcurrentStableDict[int, ConcurrentStableList[int]] = ConcurrentStableDict()
    for record in records:
        bucket: ConcurrentStableList[int] | None = grouped.get(record.partition)
        if bucket is None:
            bucket = ConcurrentStableList[int]()
            grouped.set(record.partition, bucket)
        bucket.add(record.offset)
    for partition_no in ConcurrentStableList(grouped.keys()):
        grouped.set(partition_no, ConcurrentStableList(sorted(grouped[partition_no])))
    return grouped


def derive_facts(
    *,
    records: ConcurrentStableList[PublishRecord],
    markers: ConcurrentStableList[PublishRecord],
    before: TopicState,
    after: TopicState,
    switch_items: ConcurrentStableFrozenList[SwitchItem],
    isr_before: int,
    isr_during: int,
    isr_after: int,
    probe_failed_during: bool,
    recovery_ok: bool,
    old_leader_demoted: bool = False,
    switched_during: bool = False,
) -> ConcurrentStableDict[str, tuple[str, str]]:
    """由发布结果与前后快照推导判据事实（不默认通过，缺失留空）。

    Args:
        records: 注入前已提交的记录。
        markers: 恢复后写入的标记记录。
        before: 注入前分区快照。
        after: 恢复后分区快照。
        switch_items: 显式切主结果。
        isr_before: 注入前 ISR 合计。
        isr_during: 注入期 ISR 合计。
        isr_after: 恢复后 ISR 合计。
        probe_failed_during: 注入期写入是否被拒（分区 / 脑裂场景）。
        recovery_ok: 恢复后探针是否全部就绪。

    Returns:
        ConcurrentStableDict[str, tuple[str, str]]: 记录键 → `(result, actual)`。
    """
    facts: ConcurrentStableDict[str, tuple[str, str]] = ConcurrentStableDict()
    grouped = _offsets_by_partition(records)
    marker_offsets = _offsets_by_partition(markers)

    lost: ConcurrentStableList[int] = ConcurrentStableList()
    for partition_no, offsets in grouped.items():
        written = marker_offsets.get(partition_no)
        if not written or written[-1] <= offsets[-1]:
            lost.add(partition_no)
    facts.set(
        "committed_not_lost",
        (RESULT_FAILED, f"恢复后位点未越过已提交尾部：分区 {list(lost)}")
        if lost
        else (RESULT_OK, f"{len(grouped)} 个分区恢复后位点均越过已提交尾部"),
    )

    duplicated = [
        partition_no
        for partition_no, offsets in grouped.items()
        if len(set(offsets)) != len(offsets) or offsets != list(range(offsets[0], offsets[0] + len(offsets)))
    ]
    facts.set(
        "no_double_write",
        (RESULT_FAILED, f"位点重复或断档：分区 {duplicated}")
        if duplicated
        else (RESULT_OK, f"{len(grouped)} 个分区位点连续且无重复"),
    )

    refreshed = sum(1 for record in records if record.attempts > 0)
    facts.set(
        "client_refresh",
        (RESULT_OK, f"触发 350033 改投 {refreshed} 次后写入成功")
        if refreshed
        else (RESULT_DEGRADED, "本轮未触发 350033 改投（写到了 leader）"),
    )

    switched = [item for item in switch_items if item.to_leader != item.from_leader]
    changed = [item for item in before.partitions if after.leader_of(item.partition_no) not in ("", item.leader)]
    facts.set(
        "leader_switched",
        (RESULT_OK, f"{len(changed)} 个分区换主（显式切主 {len(switched)} 项）")
        if changed
        else (
            (RESULT_OK, "注入期写请求指向新 leader（旧 leader 已被替换）")
            if switched_during or old_leader_demoted
            else (RESULT_DEGRADED, "未观测到 leader 变化")
        ),
    )
    fenced = [item for item in changed if after.epoch_of(item.partition_no) > item.epoch]
    facts.set(
        "old_leader_fenced",
        (RESULT_OK, f"{len(fenced)} 个分区 epoch 提升，旧 leader 写入被围栏")
        if fenced
        else (
            (RESULT_OK, "旧 leader 不再持有写权（注入期已指向新 leader，租约围栏生效）")
            if switched_during or old_leader_demoted
            else (RESULT_DEGRADED, "未取得 epoch 提升证据")
        ),
    )
    if switch_items:
        isr_ok = all(item.isr_verified for item in switch_items)
        note = "显式切主时 ISR 验证通过" if isr_ok else "显式切主 isr_verified=false（不计通过）"
    else:
        isr_ok = isr_after >= isr_before > 0
        note = f"自动选举后 ISR 回升到 {isr_after}" if isr_ok else f"ISR 未回升：{isr_before}→{isr_after}"
    facts.set("isr_verified", (RESULT_OK, note) if isr_ok else (RESULT_DEGRADED, note))

    shrink = isr_before > 0 and isr_during < isr_before
    facts.set(
        "isr_shrink_rejoin",
        (RESULT_OK, f"ISR {isr_before}→{isr_during}→{isr_after}（收缩后回升）")
        if shrink and isr_after >= isr_before
        else (RESULT_DEGRADED, f"ISR 未呈现收缩后回升：{isr_before}→{isr_during}→{isr_after}"),
    )
    facts.set(
        "follower_isr_shrink",
        (RESULT_OK, f"ISR {isr_before}→{isr_during}（副本被踢出）")
        if shrink
        else (RESULT_DEGRADED, f"未见 ISR 收缩：{isr_before}→{isr_during}"),
    )
    facts.set(
        "leader_lease_expired",
        (RESULT_OK, "分区期写入被拒（leader 租约到期，未产生假确认）")
        if probe_failed_during
        else (RESULT_DEGRADED, "分区期未观测到写入被拒"),
    )
    facts.set(
        "stale_leader_rejected",
        (RESULT_OK, "隔离期旧 leader 写入被拒（未双写）")
        if probe_failed_during
        else (RESULT_DEGRADED, "隔离期未观测到写入被拒"),
    )
    facts.set(
        "retained_tail_documented",
        (RESULT_OK, "恢复后位点连续，无未提交尾部")
        if not lost
        else (RESULT_DEGRADED, "存在未提交尾部（见 committed_not_lost 明细）"),
    )
    demoted = [item for item in switched if after.leader_of(item.partition_no) == item.to_leader]
    facts.set(
        "rejoin_demotes",
        (RESULT_OK, f"{len(demoted)} 个分区重入后仍由新 leader 持有")
        if demoted
        else (RESULT_DEGRADED, "未取得重入降级证据"),
    )
    facts.set(
        "rto_within_budget",
        (RESULT_OK, "恢复后探针全部就绪") if recovery_ok else (RESULT_FAILED, "恢复后实例未全部就绪"),
    )
    return facts


def platform_dsn(
    variables: ConcurrentStableFrozenDict[str, str],
    *,
    db_prefix: str,
    service: str = "msg",
) -> str:
    """按演练环境键拼平台库连接串（密码并入 URL；只在本机进程内使用，不落日志）。

    Args:
        variables: `cluster.env` 键值。
        db_prefix: 库名前缀（`MSG_CLUSTER_DRILL_DB_PREFIX`）。
        service: 服务标识（缺省 `msg`）。

    Returns:
        str: SQLAlchemy 异步 URL。

    Raises:
        ChainError: 模板缺失或形态非法。
    """
    template = variables.get("BMS_DATABASE__PLATFORM__URL_TEMPLATE") or ""
    credential = variables.get("BMS_DATABASE__PLATFORM__PASSWORD") or ""
    if not template:
        raise ChainError("cluster-drill.chain 缺少 BMS_DATABASE__PLATFORM__URL_TEMPLATE")
    database = db_prefix if db_prefix == service else f"{db_prefix}_{service}"
    url = template.replace("{database}", database)
    if not credential:
        return url
    scheme, separator, rest = url.partition("://")
    if not separator or "@" not in rest:
        raise ChainError("cluster-drill.chain 平台库连接串形态非法")
    userinfo, _, host = rest.partition("@")
    return f"{scheme}://{userinfo}:{quote(credential, safe='')}@{host}"


async def load_topic_state(dsn: str, *, topic_id: int) -> TopicState:
    """直接从平台库读分区状态（容器内 `ops.*` CLI 需插件引导，本机直读更稳）。

    Args:
        dsn: 平台库连接串。
        topic_id: 主题主键。

    Returns:
        TopicState: 分区状态（按分区号升序）。

    Raises:
        ChainError: 查询失败。
    """
    engine = create_async_engine(dsn)
    rows: ConcurrentStableList[dict[str, object]] = ConcurrentStableList()
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "select partition_no, leader, epoch, replicas, switch_state "
                    "from msg_partition where topic_id = :topic_id order by partition_no"
                ),
                {"topic_id": int(topic_id)},
            )
            for row in result.mappings():
                rows.add(cast("dict[str, object]", dict(row)))
    except Exception as exc:  # pragma: no cover - 连接 / SQL 失败由调用方记失败
        raise ChainError(f"cluster-drill.chain 分区状态查询失败：{type(exc).__name__}") from exc
    finally:
        await engine.dispose()
    partitions: ConcurrentStableList[PartitionState] = ConcurrentStableList()
    for row in rows:
        raw_replicas = row.get("replicas")
        replicas = json.loads(str(raw_replicas)) if isinstance(raw_replicas, str | bytes) else raw_replicas
        partitions.add(
            PartitionState(
                partition_no=int(cast("int", row.get("partition_no") or 0)),
                leader=str(row.get("leader") or ""),
                epoch=int(cast("int", row.get("epoch") or 0)),
                replicas=ConcurrentStableFrozenList(str(item) for item in cast("list[object]", replicas or [])),
                switch_state=str(row.get("switch_state") or "idle"),
            )
        )
    return TopicState(partitions=ConcurrentStableFrozenList(partitions))
