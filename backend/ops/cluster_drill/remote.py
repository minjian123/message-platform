"""远程 Docker 执行（02-8）：SSH 命令构造、镜像构建、compose 起停与源码同步。

**可测性**：命令构造是纯函数（用例断言 argv 与脚本文本）；真正执行集中在 `RemoteRunner.run`，
用例注入替身即可覆盖失败分支。
"""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from ops.cluster_drill.config import ClusterDrillConfig, ClusterDrillError

__all__ = [
    "RemoteResult",
    "RemoteRunner",
    "build_image_script",
    "compose_down_script",
    "compose_up_script",
    "ssh_argv",
    "sync_source_script",
]

_COMPOSE_FILE = "deploy/compose/msg.yml"
_OVERRIDE_FILE = "deploy/compose/msg-drill.override.yml"
_ENV_FILE = "deploy/drill/cluster.env"
_IMAGE_REPOSITORY = "msg"


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


def _compose_prefix(config: ClusterDrillConfig) -> str:
    """拼 compose 公共前缀（工作区 + 两份编排 + 演练环境文件）。

    Args:
        config: 演练配置。

    Returns:
        str: shell 片段。
    """
    work = shlex.quote(config.work_dir)
    return (
        f"cd {work} && docker compose"
        f" -p {shlex.quote(config.project)}"
        f" -f {_COMPOSE_FILE} -f {_OVERRIDE_FILE}"
        f" --env-file {_ENV_FILE}"
    )


def compose_up_script(config: ClusterDrillConfig) -> str:
    """构造三实例拉起脚本（显式列服务，避免连带启动默认 `msg`）。

    Args:
        config: 演练配置。

    Returns:
        str: shell 脚本。
    """
    return f"{_compose_prefix(config)} --profile cluster up -d --remove-orphans msg-1 msg-2 msg-3"


def compose_down_script(config: ClusterDrillConfig, *, volumes: bool = True) -> str:
    """构造三实例清理脚本（幂等；默认一并删演练卷）。

    Args:
        config: 演练配置。
        volumes: 是否删除卷。

    Returns:
        str: shell 脚本。
    """
    suffix = " -v" if volumes else ""
    return f"{_compose_prefix(config)} --profile cluster down --remove-orphans{suffix}"


def build_image_script(config: ClusterDrillConfig, *, commit: str) -> str:
    """构造镜像构建脚本（标签含提交短号；记录 digest 由调用方补）。

    Args:
        config: 演练配置。
        commit: msg 提交短号。

    Returns:
        str: shell 脚本。
    """
    tag = config.image_tag or f"drill-{commit}"
    work = shlex.quote(config.work_dir)
    return (
        f"cd {work} && docker build -f deploy/Dockerfile -t {_IMAGE_REPOSITORY}:{tag} ."
        f" && docker image inspect {_IMAGE_REPOSITORY}:{tag} --format '{{{{.Id}}}}'"
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


def _result(completed: subprocess.CompletedProcess[str]) -> RemoteResult:
    """把子进程结果转成 RemoteResult。

    Args:
        completed: 子进程结果。

    Returns:
        RemoteResult: 归一结果。
    """
    return RemoteResult(
        code=int(completed.returncode),
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


class RemoteRunner(BaseFrameworkObject):
    """远程执行器（SSH / 本地命令；用例注入 `runner` 替身）。"""

    def __init__(
        self,
        config: ClusterDrillConfig,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
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
        """把本地源码打包同步到服务器工作区。

        Args:
            local_root: 本地 msg 仓根。

        Returns:
            RemoteResult: 执行结果。

        Raises:
            ClusterDrillError: 打包失败（本地）。
        """
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
                    "--exclude=backend/.venv",
                    ".",
                ],
                cwd=str(local_root),
                capture_output=True,
                check=True,
            )
            completed = self._runner(
                argv,
                input=tar.stdout,
                capture_output=True,
                text=True,
                timeout=self._config.ssh_timeout_sec,
                check=False,
            )
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise ClusterDrillError(f"cluster-drill.remote 源码同步失败：{type(exc).__name__}") from exc
        return _result(completed)

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
