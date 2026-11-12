"""多实例集群演练配置（02-8）：远程主机 / 端口隧道 / 隔离命名 / 记录与预算。

**凭据只落不入库的文件**：`deploy/drill/cluster.env`（已 gitignore）提供主机与依赖地址；
本模块只解析键位并做**生产拒绝**，不打印、不记录任何值。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableList,
)
from bms_core.core.objects import BaseValueObject

from bms_msg.core.config import (
    CLUSTER_DRILL_CONTAINER_PREFIX,
    CLUSTER_DRILL_DB_PREFIX,
    CLUSTER_DRILL_DEFAULT_ENV,
    CLUSTER_DRILL_ENV_FILE_ENV,
    CLUSTER_DRILL_PORT_BASE,
    CLUSTER_DRILL_PROD_ENVS,
    CLUSTER_DRILL_PROJECT,
    CLUSTER_DRILL_READY_TIMEOUT_SEC,
    CLUSTER_DRILL_REPORT_DIR,
    CLUSTER_DRILL_SSH_TIMEOUT_SEC,
    MS_PER_SECOND,
)

__all__ = [
    "ENV_FILE_DEFAULT",
    "ClusterDrillConfig",
    "ClusterDrillError",
    "load_config",
    "parse_env_file",
    "reject_production",
    "require_confirmation",
]

ENV_FILE_DEFAULT = Path("deploy") / "drill" / "cluster.env"
"""演练环境文件缺省路径（相对仓根；不入库）。"""


class ClusterDrillError(RuntimeError):
    """集群演练失败（参数 / 前置 / 远程执行）：消息带 `cluster-drill.<类别>` 前缀。"""


def parse_env_file(path: Path) -> ConcurrentStableFrozenDict[str, str]:
    """解析 `KEY=VALUE` 环境文件（忽略空行与 `#` 注释）。

    Args:
        path: 文件路径。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 键值（插入序）。

    Raises:
        ClusterDrillError: 文件不存在。
    """
    if not path.exists():
        raise ClusterDrillError(f"cluster-drill.config 演练环境文件不存在：{path}")
    values: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values.set(key.strip(), value.strip())
    return ConcurrentStableFrozenDict(values)


def _positive(value: str | None, key: str, default: int) -> int:
    """解析正整数（空取缺省，非法即失败并指名键）。

    Args:
        value: 原值。
        key: 键名。
        default: 缺省值。

    Returns:
        int: 生效值。

    Raises:
        ClusterDrillError: 取值非法。
    """
    if value is None or value.strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ClusterDrillError(f"cluster-drill.config {key} 须为正整数（当前 {value!r}）") from exc
    if parsed <= 0:
        raise ClusterDrillError(f"cluster-drill.config {key} 须为正整数（当前 {parsed}）")
    return parsed


@dataclass(frozen=True)
class ClusterDrillConfig(BaseValueObject):
    """一次集群演练的生效配置。"""

    env: str = CLUSTER_DRILL_DEFAULT_ENV
    """环境标识（prod / production 一律拒绝）。"""

    ssh_target: str = ""
    """SSH 目标（`user@host`）。"""

    ssh_port: int = 22
    """SSH 端口。"""

    work_dir: str = ""
    """服务器临时工作区（源码同步与镜像构建都落这里）。"""

    image_tag: str = ""
    """镜像标签（含提交短号；空 = 由编排器生成）。"""

    project: str = CLUSTER_DRILL_PROJECT
    """compose 项目名。"""

    container_prefix: str = CLUSTER_DRILL_CONTAINER_PREFIX
    """容器名前缀（`msg-drill`）。"""

    db_prefix: str = CLUSTER_DRILL_DB_PREFIX
    """演练库 / Redis 键前缀。"""

    ports: ConcurrentStableList[int] = field(default_factory=lambda: ConcurrentStableList[int]())
    """三实例宿主回环端口（供本机隧道）。"""

    report_dir: Path = Path("tmp/msg/cluster-drill")
    """记录落点。"""

    recovery_budget_ms: int = 300_000
    """单场景恢复预算（毫秒）。"""

    ready_timeout_sec: int = CLUSTER_DRILL_READY_TIMEOUT_SEC
    """实例就绪轮询超时（秒）。"""

    ssh_timeout_sec: int = CLUSTER_DRILL_SSH_TIMEOUT_SEC
    """远程命令超时（秒）。"""

    variables: ConcurrentStableFrozenDict[str, str] = field(
        default_factory=lambda: ConcurrentStableFrozenDict[str, str]()
    )
    """环境文件键值（凭据只读、不外显）。"""

    @property
    def container_names(self) -> ConcurrentStableList[str]:
        """三实例容器名。

        Returns:
            ConcurrentStableList[str]: `msg-drill-1..3`。
        """
        return ConcurrentStableList(f"{self.container_prefix}-{index}" for index in (1, 2, 3))

    @property
    def ready_timeout_ms(self) -> int:
        """就绪超时（毫秒）。

        Returns:
            int: 毫秒。
        """
        return self.ready_timeout_sec * MS_PER_SECOND


def _from_sources(sources: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]], key: str) -> str | None:
    """按优先级取键值（后者覆盖前者）。

    Args:
        sources: 键值来源（低 → 高优先级）。
        key: 键名。

    Returns:
        str | None: 取值。
    """
    result: str | None = None
    for source in sources:
        value = source.get(key)
        if value is not None and value != "":
            result = value
    return result


def load_config(
    *,
    repo_root: Path | None = None,
    env_path: Path | None = None,
    environ: ConcurrentStableFrozenDict[str, str] | None = None,
) -> ClusterDrillConfig:
    """从环境文件 + 进程环境解析配置（进程环境优先）。

    Args:
        repo_root: 仓库根（缺省由脚本位置自定位）。
        env_path: 环境文件路径（缺省 `deploy/drill/cluster.env`；可用环境变量覆盖）。
        environ: 进程环境（用例注入）。

    Returns:
        ClusterDrillConfig: 生效配置。

    Raises:
        ClusterDrillError: 必填键缺失或数值非法。
    """
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[3]
    process = environ if environ is not None else ConcurrentStableFrozenDict(os.environ)
    process_values: ConcurrentStableDict[str, str] = ConcurrentStableDict(process)
    selected = (process.get(CLUSTER_DRILL_ENV_FILE_ENV) or "").strip()
    default_path = env_path if env_path is not None else root / ENV_FILE_DEFAULT
    file_path = Path(selected).expanduser() if selected else default_path
    file_values = parse_env_file(file_path)
    sources: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList(
        (file_values, ConcurrentStableFrozenDict(process_values))
    )
    ssh_target = (_from_sources(sources, "MSG_CLUSTER_DRILL_SSH_TARGET") or "").strip()
    work_dir = (_from_sources(sources, "MSG_CLUSTER_DRILL_WORK_DIR") or "").strip()
    if not ssh_target:
        raise ClusterDrillError("cluster-drill.config 缺少必填键 MSG_CLUSTER_DRILL_SSH_TARGET")
    if not work_dir:
        raise ClusterDrillError("cluster-drill.config 缺少必填键 MSG_CLUSTER_DRILL_WORK_DIR")
    env_name = (_from_sources(sources, "MSG_CLUSTER_DRILL_ENV") or CLUSTER_DRILL_DEFAULT_ENV).strip().lower()
    report_raw = (_from_sources(sources, "MSG_CLUSTER_DRILL_REPORT_DIR") or CLUSTER_DRILL_REPORT_DIR).strip()
    ports: ConcurrentStableList[int] = ConcurrentStableList()
    for index in (1, 2, 3):
        port_key = f"MSG_CLUSTER_DRILL_PORT_{index}"
        ports.add(
            _positive(
                _from_sources(sources, port_key),
                port_key,
                CLUSTER_DRILL_PORT_BASE + index - 1,
            )
        )
    return ClusterDrillConfig(
        env=env_name,
        ssh_target=ssh_target,
        ssh_port=_positive(_from_sources(sources, "MSG_CLUSTER_DRILL_SSH_PORT"), "MSG_CLUSTER_DRILL_SSH_PORT", 22),
        work_dir=work_dir,
        image_tag=(_from_sources(sources, "MSG_CLUSTER_DRILL_IMAGE_TAG") or "").strip(),
        project=(_from_sources(sources, "MSG_CLUSTER_DRILL_PROJECT") or CLUSTER_DRILL_PROJECT).strip(),
        container_prefix=(
            _from_sources(sources, "MSG_CLUSTER_DRILL_CONTAINER_PREFIX") or CLUSTER_DRILL_CONTAINER_PREFIX
        ).strip(),
        db_prefix=(_from_sources(sources, "MSG_CLUSTER_DRILL_DB_PREFIX") or CLUSTER_DRILL_DB_PREFIX).strip(),
        ports=ports,
        report_dir=(root / report_raw).resolve(),
        recovery_budget_ms=_positive(
            _from_sources(sources, "MSG_CLUSTER_DRILL_RECOVERY_BUDGET_MS"),
            "MSG_CLUSTER_DRILL_RECOVERY_BUDGET_MS",
            300_000,
        ),
        ready_timeout_sec=_positive(
            _from_sources(sources, "MSG_CLUSTER_DRILL_READY_TIMEOUT_SEC"),
            "MSG_CLUSTER_DRILL_READY_TIMEOUT_SEC",
            CLUSTER_DRILL_READY_TIMEOUT_SEC,
        ),
        ssh_timeout_sec=_positive(
            _from_sources(sources, "MSG_CLUSTER_DRILL_SSH_TIMEOUT_SEC"),
            "MSG_CLUSTER_DRILL_SSH_TIMEOUT_SEC",
            CLUSTER_DRILL_SSH_TIMEOUT_SEC,
        ),
        variables=file_values,
    )


def reject_production(env: str) -> None:
    """生产环境一律拒绝（无逃生开关）。

    Args:
        env: 归一化环境标识。

    Raises:
        ClusterDrillError: 环境为生产。
    """
    if env in CLUSTER_DRILL_PROD_ENVS:
        raise ClusterDrillError(f"cluster-drill.guard 生产环境禁止注入故障（env={env}）")


def require_confirmation(scenario: str, confirm: str | None) -> None:
    """二次确认：`--confirm <场景名>` 必须一致。

    Args:
        scenario: 场景标识。
        confirm: 命令行确认值。

    Raises:
        ClusterDrillError: 未确认或值不一致。
    """
    if (confirm or "").strip() != scenario:
        raise ClusterDrillError(f"cluster-drill.guard 缺少二次确认：请显式传 --confirm {scenario}")
