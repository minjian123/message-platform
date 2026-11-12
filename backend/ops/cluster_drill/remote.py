"""远程 Docker 执行（02-8）：SSH 命令构造、镜像构建、compose 起停与源码同步。

**可测性**：命令构造是纯函数（用例断言 argv 与脚本文本）；真正执行集中在 `RemoteRunner.run`，
用例注入替身即可覆盖失败分支。
"""

from __future__ import annotations

import shlex
import socket
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from ops.cluster_drill.config import ClusterDrillConfig, ClusterDrillError

__all__ = [
    "RemoteResult",
    "RemoteRunner",
    "SshTunnel",
    "build_image_script",
    "compose_down_script",
    "compose_up_script",
    "image_reference",
    "restart_script",
    "ssh_argv",
    "sync_source_script",
    "tunnel_argv",
]

_COMPOSE_FILE = "deploy/compose/msg.yml"
_OVERRIDE_FILE = "deploy/compose/msg-drill.override.yml"
_ENV_FILE = "deploy/drill/cluster.env"
_MSG_DIR = "msg"
"""服务器工作区里的 msg 仓目录名（与 `bms/` 平级，即工作区根布局）。"""

_DOCKERFILE = f"{_MSG_DIR}/deploy/Dockerfile"
"""构建上下文是工作区根：Dockerfile 在 `msg/deploy/` 下（内部 `COPY bms/...`）。"""

_BASE_SOURCE = "bms/backend/libs/bms_core"
"""随 msg 一并同步的基座源码（路径依赖目标；镜像构建需要）。"""

_IMAGE_PREFIX = "drill"
"""演练镜像前缀（本地构建，不经 Registry；与 compose 的 `MSG_REGISTRY_IMAGE_PREFIX` 同值）。"""

_IMAGE_REPOSITORY = "msg"
"""镜像名（= 服务标识）。"""


def _image_tag(config: ClusterDrillConfig, commit: str) -> str:
    """镜像标签（显式配置优先；否则按提交短号，缺提交时用占位标签供 compose 插值）。

    Args:
        config: 演练配置。
        commit: msg 提交短号（空 = 只做 compose 插值，如 `down`）。

    Returns:
        str: 标签。
    """
    if config.image_tag:
        return config.image_tag
    return f"drill-{commit}" if commit else "drill-cleanup"


def image_reference(config: ClusterDrillConfig, *, commit: str) -> str:
    """演练镜像全名（构建与 compose 共用同一字符串，杜绝两处各写一套）。

    Args:
        config: 演练配置。
        commit: msg 提交短号。

    Returns:
        str: `<前缀>/msg:<标签>`。
    """
    return f"{_IMAGE_PREFIX}/{_IMAGE_REPOSITORY}:{_image_tag(config, commit)}"


@dataclass(frozen=True)
class RemoteResult(BaseValueObject):
    """一次远程命令的结果。"""

    code: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        """是否成功（退出码 0）。

        Returns:
            bool: 成功 True。
        """
        return self.code == 0


def ssh_argv(config: ClusterDrillConfig, script: str) -> ConcurrentStableList[str]:
    """构造 SSH 调用参数（非交互、显式端口、批处理模式）。

    Args:
        config: 演练配置。
        script: 远端 shell 脚本。

    Returns:
        ConcurrentStableList[str]: argv。
    """
    return ConcurrentStableList(
        (
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-p",
            str(config.ssh_port),
            config.ssh_target,
            script,
        )
    )


def _compose_prefix(config: ClusterDrillConfig, *, commit: str) -> str:
    """拼 compose 公共前缀（msg 仓目录 + 两份编排 + 演练环境文件 + 镜像引用）。

    Args:
        config: 演练配置。
        commit: msg 提交短号（决定 `MSG_TAG`；`down` 只用于插值）。

    Returns:
        str: shell 片段。
    """
    work = shlex.quote(f"{config.work_dir.rstrip('/')}/{_MSG_DIR}")
    tag = _image_tag(config, commit)
    return (
        f"cd {work} && MSG_REGISTRY_IMAGE_PREFIX={_IMAGE_PREFIX} MSG_TAG={tag} docker compose"
        f" -p {shlex.quote(config.project)}"
        f" -f {_COMPOSE_FILE} -f {_OVERRIDE_FILE}"
        f" --env-file {_ENV_FILE}"
    )


def compose_up_script(config: ClusterDrillConfig, *, commit: str) -> str:
    """构造三实例拉起脚本（显式列服务，避免连带启动默认 `msg`）。

    Args:
        config: 演练配置。
        commit: msg 提交短号。

    Returns:
        str: shell 脚本。
    """
    return f"{_compose_prefix(config, commit=commit)} --profile cluster up -d --remove-orphans msg-1 msg-2 msg-3"


def compose_down_script(config: ClusterDrillConfig, *, volumes: bool = True, commit: str = "") -> str:
    """构造三实例清理脚本（幂等；默认一并删演练卷）。

    Args:
        config: 演练配置。
        volumes: 是否删除卷.
        commit: msg 提交短号（缺省用占位标签，仅满足 compose 插值）。

    Returns:
        str: shell 脚本。
    """
    suffix = " -v" if volumes else ""
    return f"{_compose_prefix(config, commit=commit)} --profile cluster down --remove-orphans{suffix}"


def restart_script(config: ClusterDrillConfig) -> str:
    """重启三实例（每轮演练前置重置：清理上一轮的拉取态 / epoch 残留）。

    Args:
        config: 演练配置。

    Returns:
        str: shell 脚本。
    """
    names = " ".join(shlex.quote(name) for name in config.container_names)
    return f"docker restart {names} >/dev/null"


def build_image_script(config: ClusterDrillConfig, *, commit: str) -> str:
    """构造镜像构建脚本（上下文 = 工作区根；标签与 compose 引用同一字符串）。

    Args:
        config: 演练配置。
        commit: msg 提交短号。

    Returns:
        str: shell 脚本。
    """
    work = shlex.quote(config.work_dir)
    reference = image_reference(config, commit=commit)
    return (
        f"cd {work} && docker build --provenance=false -f {_DOCKERFILE} -t {reference} ."
        f" && docker image inspect {reference} --format '{{{{.Id}}}}'"
    )


def sync_source_script(config: ClusterDrillConfig, *, local_root: Path) -> str:
    """构造源码同步脚本（tar 经 SSH 传输；不带 `.env` / `.git` / 依赖目录）。

    Args:
        config: 演练配置。
        local_root: 本地 msg 仓根。

    Returns:
        str: 远端接收脚本（管道由 `RemoteRunner.sync_source` 负责）。
    """
    del local_root
    work = shlex.quote(config.work_dir)
    return f"mkdir -p {work} && tar -xf - -C {work}"


def _text(value: bytes | str | None) -> str:
    """把子进程输出归一为文本（字节管道按 UTF-8 容错解码）。

    Args:
        value: 原始输出。

    Returns:
        str: 文本。
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _result(completed: subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]) -> RemoteResult:
    """把子进程结果转成 RemoteResult。

    Args:
        completed: 子进程结果。

    Returns:
        RemoteResult: 归一结果。
    """
    return RemoteResult(
        code=int(completed.returncode),
        stdout=_text(completed.stdout),
        stderr=_text(completed.stderr),
    )


class RemoteRunner(BaseFrameworkObject):
    """远程执行器（SSH / 本地命令；用例注入 `runner` 替身）。"""

    def __init__(
        self,
        config: ClusterDrillConfig,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]] | None = None,
    ) -> None:
        """初始化。

        Args:
            config: 演练配置。
            runner: 子进程执行函数（签名同 `subprocess.run`）。
        """
        self._config = config
        self._runner = runner if runner is not None else subprocess.run

    @property
    def config(self) -> ClusterDrillConfig:
        """演练配置。

        Returns:
            ClusterDrillConfig: 配置。
        """
        return self._config

    def run(self, script: str) -> RemoteResult:
        """执行远程脚本（超时 / 失败不抛，由调用方判 `ok`）。

        Args:
            script: 远端 shell 脚本。

        Returns:
            RemoteResult: 执行结果。

        Raises:
            ClusterDrillError: SSH 不可用 / 超时。
        """
        argv = list(ssh_argv(self._config, script))
        try:
            completed = self._runner(
                argv,
                capture_output=True,
                text=True,
                timeout=self._config.ssh_timeout_sec,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ClusterDrillError(f"cluster-drill.remote 远程命令失败：{type(exc).__name__}") from exc
        return _result(completed)

    def sync_source(self, *, local_root: Path) -> RemoteResult:
        """把本地源码按**工作区根布局**同步到服务器（`msg/` 与 `bms/` 平级）。

        镜像构建上下文是工作区根（Dockerfile 内 `COPY bms/...` / `COPY msg/...`），
        故只同步 msg 仓 + 基座源码子目录，不带文档 / 依赖 / 版本库。

        Args:
            local_root: 本地 msg 仓根。

        Returns:
            RemoteResult: 执行结果。

        Raises:
            ClusterDrillError: 打包失败（本地）或基座源码缺失。
        """
        workspace = local_root.resolve().parent
        base_source = workspace / _BASE_SOURCE
        if not base_source.exists():
            raise ClusterDrillError(f"cluster-drill.remote 缺少基座源码：{base_source}")
        work = shlex.quote(self._config.work_dir)
        argv = list(ssh_argv(self._config, f"mkdir -p {work} && tar -xf - -C {work}"))
        try:
            tar = self._runner(
                [
                    "tar",
                    "-cf",
                    "-",
                    "--exclude=.git",
                    "--exclude=.env",
                    "--exclude=node_modules",
                    "--exclude=.venv",
                    "--exclude=.cache",
                    "--exclude=.ruff_cache",
                    "--exclude=tmp",
                    "--exclude=msg/frontend",
                    "--exclude=msg/msg文档",
                    "--exclude=msg/scripts",
                    local_root.resolve().name,
                    _BASE_SOURCE,
                ],
                cwd=str(workspace),
                capture_output=True,
                check=True,
            )
            completed = self._runner(
                argv,
                input=tar.stdout,
                capture_output=True,
                timeout=self._config.ssh_timeout_sec,
                check=False,
            )
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise ClusterDrillError(f"cluster-drill.remote 源码同步失败：{type(exc).__name__}") from exc
        return RemoteResult(
            code=int(completed.returncode), stdout=_text(completed.stdout), stderr=_text(completed.stderr)
        )

    def docker_exec(self, container: str, command: str) -> RemoteResult:
        """在容器内执行命令（运维 CLI / 指标探针）。

        Args:
            container: 容器名。
            command: 容器内命令。

        Returns:
            RemoteResult: 执行结果。
        """
        return self.run(f"docker exec {shlex.quote(container)} sh -lc {shlex.quote(command)}")

    def env_summary(self) -> ConcurrentStableDict[str, str]:
        """演练环境摘要（只含非敏感键，供记录 deviations）。

        Returns:
            ConcurrentStableDict[str, str]: 摘要。
        """
        return ConcurrentStableDict(
            {
                "env": self._config.env,
                "project": self._config.project,
                "container_prefix": self._config.container_prefix,
                "db_prefix": self._config.db_prefix,
                "ports": ",".join(str(port) for port in self._config.ports),
            }
        )


class _TunnelProcess(Protocol):
    """隧道子进程最小协议（`subprocess.Popen` 与其测试替身都满足）。"""

    def poll(self) -> int | None:
        """进程退出码（未退出 None）。"""
        ...

    def terminate(self) -> None:
        """请求终止。"""
        ...

    def kill(self) -> None:
        """强制终止。"""
        ...

    def wait(self, timeout: float | None = None) -> int:
        """等待退出。"""
        ...


class _Closable(Protocol):
    """可关闭对象（连接探测返回值只用 `close`）。"""

    def close(self) -> None:
        """关闭连接。"""
        ...


class _Connector(Protocol):
    """连接探测协议。"""

    def __call__(self, address: tuple[str, int], timeout: float = ...) -> _Closable:
        """尝试连接。"""
        ...


_TUNNEL_ATTEMPTS = 150
"""隧道就绪轮询次数（150 × 0.2s ≈ 30s）。"""

_TUNNEL_INTERVAL_SEC = 0.2
"""隧道就绪轮询间隔（秒）。"""


def tunnel_argv(config: ClusterDrillConfig) -> ConcurrentStableList[str]:
    """构造本机 SSH 隧道参数（本机回环端口 → 服务器回环端口，一一对应）。

    容器只把 8000 绑到服务器 `127.0.0.1`（不对外暴露），探针从本机经隧道访问；
    `-N` 表示不执行远端命令，`ExitOnForwardFailure` 让端口占用 / 拒绝时立即失败。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableList[str]: argv。
    """
    argv: ConcurrentStableList[str] = ConcurrentStableList(
        (
            "ssh",
            "-N",
            "-o",
            "BatchMode=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-p",
            str(config.ssh_port),
        )
    )
    for port in config.ports:
        argv.add("-L")
        argv.add(f"127.0.0.1:{port}:127.0.0.1:{port}")
    argv.add(config.ssh_target)
    return argv


class SshTunnel(BaseFrameworkObject):
    """本机 SSH 端口转发（`ssh -N -L`）：探针经隧道访问只绑服务器回环的实例端口。

    执行与判定分离：`open` / `wait_ready` / `close` 都可注入替身，用例无需真连服务器。
    """

    def __init__(
        self,
        config: ClusterDrillConfig,
        *,
        popen: Callable[..., _TunnelProcess] | None = None,
        connector: _Connector | None = None,
        attempts: int = _TUNNEL_ATTEMPTS,
        interval: float = _TUNNEL_INTERVAL_SEC,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        """初始化（不建立连接）。

        Args:
            config: 演练配置。
            popen: 子进程工厂（缺省 `subprocess.Popen`）。
            connector: 连接探测函数（缺省 `socket.create_connection`）。
            attempts: 就绪轮询次数。
            interval: 轮询间隔（秒）。
            sleeper: 等待函数（缺省 `time.sleep`；用例注入空实现免等待）。
        """
        self._config = config
        self._popen = popen if popen is not None else subprocess.Popen
        self._connector = connector if connector is not None else socket.create_connection
        self._attempts = attempts
        self._interval = interval
        self._sleep = sleeper if sleeper is not None else time.sleep
        self._process: _TunnelProcess | None = None

    def open(self) -> None:
        """拉起隧道进程（后台常驻；由 `close` 回收）。"""
        self._process = self._popen(
            list(tunnel_argv(self._config)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def wait_ready(self) -> bool:
        """等到全部本地端口可连接（进程提前退出或预算耗尽即 False）。

        Returns:
            bool: 就绪 True。
        """
        process = self._process
        if process is None:
            return False
        for _ in range(self._attempts):
            if process.poll() is not None:
                return False
            try:
                for port in self._config.ports:
                    self._connector(("127.0.0.1", int(port)), timeout=1).close()
            except OSError:
                self._sleep(self._interval)
                continue
            return True
        return False

    def close(self) -> None:
        """关闭隧道进程（幂等；超时不退则强杀）。"""
        process = self._process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self._process = None
