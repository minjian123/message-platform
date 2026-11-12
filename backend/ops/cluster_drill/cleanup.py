"""集群演练清理（02-8）：容器 / 卷 / 网络 / 演练库 / Redis / 宿主目录 / 镜像。

**为什么单独成模块**：清理脚本是纯函数（用例断言文本），执行仍集中在 `RemoteRunner.run`；
数据清理需要「一次性容器 + 演练环境文件」，与编排主流程解耦，避免 `__main__` 继续膨胀。

**安全纪律**：删库前必须满足「演练库前缀白名单」；Redis 拒绝 0 号库（疑似生产）；
宿主目录只删环境文件显式给出的绝对路径，且拒绝常见系统目录。
"""

from __future__ import annotations

import shlex
from pathlib import PurePosixPath

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList

from ops.cluster_drill.config import ClusterDrillConfig, ClusterDrillError
from ops.cluster_drill.remote import compose_down_script

__all__ = [
    "PURGE_FAILED_EXIT",
    "PURGE_SKIPPED_EXIT",
    "assert_safe_data_prefix",
    "cleanup_down_script",
    "purge_data_script",
    "remove_images_script",
    "remove_paths_script",
    "target_paths",
]

PURGE_SKIPPED_EXIT = 3
"""数据清理跳过（服务器上已无演练镜像 / 环境文件）的退出码。"""

PURGE_FAILED_EXIT = 4
"""数据清理前置不满足（环境文件缺键 / 前缀非法）的退出码。"""

_IMAGE_GLOB = "drill/msg:drill-*"
"""演练镜像引用（与 `remote.image_reference` 同前缀；只删这个前缀的标签）。"""

_IMAGE_FORMAT = "{{.Repository}}:{{.Tag}}"
"""`docker images` 输出格式（仓库:标签）。"""

_ENV_RELATIVE = "deploy/drill/cluster.env"
"""服务器工作区里的演练环境文件（含演练库 / Redis 连接串；清理时作 `--env-file`）。"""

_HOST_DIR_KEYS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        "MSG_DATA_HOST_DIR",
        "MSG_OFFSETS_HOST_DIR",
        "MSG_DATA_HOST_DIR_1",
        "MSG_OFFSETS_HOST_DIR_1",
        "MSG_DATA_HOST_DIR_2",
        "MSG_OFFSETS_HOST_DIR_2",
        "MSG_DATA_HOST_DIR_3",
        "MSG_OFFSETS_HOST_DIR_3",
    )
)
"""宿主机数据 / 位点目录键位（演练后随工作区一并清理）。"""

_DENY_PATHS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ("/", "/home", "/root", "/tmp", "/var", "/usr", "/etc", "/opt", "/srv", "/boot", "/dev", "/proc", "/sys")
)
"""拒绝删除的系统目录（防环境文件误填）。"""

_PURGE_PYTHON = """
import os
from urllib.parse import unquote, urlsplit

import pymysql
import redis

prefix = os.environ.get("MSG_CLUSTER_DRILL_DB_PREFIX", "").strip()
if not prefix or prefix == "msg":
    raise SystemExit("cluster-drill.cleanup 拒绝：演练库前缀为空或等于 msg")
template = os.environ.get("BMS_DATABASE__PLATFORM__URL_TEMPLATE", "").strip()
if not template:
    raise SystemExit("cluster-drill.cleanup 缺少 BMS_DATABASE__PLATFORM__URL_TEMPLATE")
credential = os.environ.get("BMS_DATABASE__PLATFORM__PASSWORD", "").strip()

parts = urlsplit(template.replace("{database}", prefix + "_msg"))
if not parts.hostname:
    raise SystemExit("cluster-drill.cleanup 平台库连接串无法解析")
secret = credential or unquote(parts.password or "")
options = {
    "host": parts.hostname,
    "port": parts.port or 3306,
    "user": unquote(parts.username or ""),
    "password": secret,
    "charset": "utf8mb4",
    "autocommit": True,
}
conn = pymysql.connect(**options)
try:
    with conn.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        names = [str(row[0]) for row in cursor.fetchall()]
        targets = [name for name in names if name.startswith(prefix + "_")]
        for name in targets:
            cursor.execute("DROP DATABASE `" + name.replace("`", "``") + "`")
            print("cluster-drill.cleanup dropped", name)
finally:
    conn.close()

redis_url = os.environ.get("BMS_REDIS__URL", "").strip()
if redis_url:
    parts = urlsplit(redis_url)
    db = int((parts.path or "/0").strip("/") or "0")
    if db == 0:
        raise SystemExit("cluster-drill.cleanup 拒绝：BMS_REDIS__URL 指向 0 号库（疑似生产），不执行 flush")
    redis_options = {"host": parts.hostname, "port": parts.port or 6379, "db": db}
    if parts.password:
        redis_options["password"] = unquote(parts.password)
    client = redis.Redis(**redis_options)
    print("cluster-drill.cleanup flushed redis db", db, int(client.flushdb()))
""".strip()
"""一次性容器里执行的数据清理脚本（读 `--env-file` 注入的键；前缀 / 0 号库双保险）。"""


def assert_safe_data_prefix(config: ClusterDrillConfig) -> str:
    """校验演练库前缀（删库前的白名单守卫）。

    Args:
        config: 演练配置。

    Returns:
        str: 生效前缀。

    Raises:
        ClusterDrillError: 前缀为空 / 等于 `msg` / 含非法字符。
    """
    prefix = (config.db_prefix or "").strip()
    if prefix == "msg":
        raise ClusterDrillError("cluster-drill.cleanup 拒绝：演练库前缀不得等于 msg（疑似开发 / 生产库）")
    if not prefix:
        raise ClusterDrillError("cluster-drill.cleanup 拒绝：演练库前缀为空")
    if not prefix.isascii() or not prefix.replace("_", "").isalnum() or prefix[0].isdigit():
        raise ClusterDrillError(f"cluster-drill.cleanup 拒绝：演练库前缀非法（{prefix}）")
    return prefix


def _image_pick() -> str:
    """构造「取一个演练镜像引用」的 shell 片段（无镜像时留空）。

    Returns:
        str: shell 片段。
    """
    return (
        f'IMAGE=$(docker images --filter=reference={shlex.quote(_IMAGE_GLOB)} --format "{_IMAGE_FORMAT}" | head -n 1)'
    )


def cleanup_down_script(config: ClusterDrillConfig) -> str:
    """构造容器 / 卷清理脚本（工作区不存在时视为已清理，保持幂等）。

    Args:
        config: 演练配置。

    Returns:
        str: shell 脚本。
    """
    work = shlex.quote(f"{config.work_dir.rstrip('/')}/msg")
    return (
        f"if cd {work} 2>/dev/null; then {compose_down_script(config)}; "
        'else echo "cluster-drill.cleanup 工作区不存在，跳过容器清理" >&2; fi'
    )


def purge_data_script(config: ClusterDrillConfig, *, network: str) -> str:
    """构造数据清理脚本（删 `{前缀}_*` 库 + flush 演练 Redis 库）。

    用一次性容器从**演练镜像**里跑 Python（镜像内已有 pymysql / redis 依赖），
    连接串与口令经 `--env-file` 注入，不出现在命令行 / 日志里。

    Args:
        config: 演练配置。
        network: 容器所接网络（平台网络，保证能访问 DB / Redis）。

    Returns:
        str: shell 脚本；退出码 `3` 表示跳过（无镜像 / 无环境文件），`4` 表示前置不满足。
    """
    work = shlex.quote(config.work_dir)
    env_file = shlex.quote(f"{config.work_dir.rstrip('/')}/{_ENV_RELATIVE}")
    quoted_python = shlex.quote(_PURGE_PYTHON)
    return "\n".join(
        (
            "set -u",
            _image_pick(),
            'if [ -z "$IMAGE" ]; then',
            '  echo "cluster-drill.cleanup 服务器上无演练镜像，跳过数据清理" >&2',
            f"  exit {PURGE_SKIPPED_EXIT}",
            "fi",
            f"if [ ! -f {env_file} ]; then",
            f'  echo "cluster-drill.cleanup 缺少演练环境文件：{_ENV_RELATIVE}" >&2',
            f"  exit {PURGE_FAILED_EXIT}",
            "fi",
            f"cd {work} || exit {PURGE_FAILED_EXIT}",
            (
                f"docker run --rm --network {shlex.quote(network)} --env-file {env_file} "
                f'"$IMAGE" python -c {quoted_python}'
            ),
        )
    )


def target_paths(config: ClusterDrillConfig) -> ConcurrentStableList[str]:
    """取清理目标路径（工作区 + 环境文件给出的宿主数据 / 位点目录）。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableList[str]: 绝对路径（去重、按配置声明序）。
    """
    raw: ConcurrentStableList[str] = ConcurrentStableList((config.work_dir,))
    for key in _HOST_DIR_KEYS:
        value = (config.variables.get(key) or "").strip()
        if value:
            raw.add(value)
    paths: ConcurrentStableList[str] = ConcurrentStableList()
    for item in raw:
        if not item or item in paths:
            continue
        candidate = PurePosixPath(item)
        if not candidate.is_absolute() or len(candidate.parts) < 3 or item.rstrip("/") in _DENY_PATHS:
            continue
        paths.add(item.rstrip("/"))
    return paths


def remove_paths_script(config: ClusterDrillConfig) -> str:
    """构造宿主目录删除脚本（用演练镜像以 root 删 uid 10001 的挂载目录）。

    Args:
        config: 演练配置。

    Returns:
        str: shell 脚本（目录不存在即跳过；无可删路径返回 `true`）。
    """
    paths = target_paths(config)
    if not paths:
        return "true"
    lines: ConcurrentStableList[str] = ConcurrentStableList(("set -u", _image_pick()))
    quoted = " ".join(shlex.quote(path) for path in paths)
    lines.add(f"for target in {quoted}; do")
    lines.add('  [ -e "$target" ] || continue')
    lines.add('  if [ -n "$IMAGE" ]; then')
    lines.add('    parent=$(dirname "$target")')
    lines.add('    base=$(basename "$target")')
    lines.add(
        '    docker run --rm -v "$parent":/cleanup-parent "$IMAGE" '
        'python -c "import shutil,sys; shutil.rmtree(\'/cleanup-parent/\' + sys.argv[1])" "$base" '
        '|| { echo "cluster-drill.cleanup 无法删除 $target" >&2; exit 1; }'
    )
    lines.add("  else")
    lines.add('    rm -rf "$target" || { echo "cluster-drill.cleanup 无法删除 $target" >&2; exit 1; }')
    lines.add("  fi")
    lines.add('  echo "cluster-drill.cleanup removed $target"')
    lines.add("done")
    return "\n".join(lines)


def remove_images_script() -> str:
    """构造演练镜像删除脚本（只删 `drill/msg:drill-*`；尽力而为，不因占用失败）。

    Returns:
        str: shell 脚本。
    """
    return "\n".join(
        (
            "set -u",
            f'docker images --filter=reference={shlex.quote(_IMAGE_GLOB)} --format "{_IMAGE_FORMAT}" | '
            "while IFS= read -r image; do",
            '  [ -n "$image" ] || continue',
            '  docker rmi "$image" >/dev/null 2>&1 || true',
            '  echo "cluster-drill.cleanup removed image $image"',
            "done",
        )
    )
