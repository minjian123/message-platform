"""部署资产护栏（Kiwi 2472）：编排端口 / 网络 / 双持久卷 / 停机宽限 + 镜像契约 + 凭据模板。

纯静态断言（解析 `deploy/compose/msg.yml` 与读 `deploy/Dockerfile` 文本），**不需要 Docker**：
容器起停与重启后数据保留属真机冒烟，见任务 07-1-1 的测试记录。

设计取舍：
- 编排用 `yaml.safe_load` 解析后断言结构，不做文本匹配——改缩进 / 换锚点不会误报，语义变了才会红；
- 镜像与忽略规则只能读文本（Dockerfile 无结构化解析器），断言「必须有 / 必须没有」两类关键契约：
  阶段数、锁文件口径、非 root、探针、入口，以及凭据 / 前端 / 其他产品仓一律不得进镜像。
"""

from pathlib import Path
from typing import Any, cast

import pytest
import yaml

pytestmark = pytest.mark.kiwi_id(2472)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_COMPOSE = _REPO / "deploy" / "compose" / "msg.yml"
_DOCKERFILE = _REPO / "deploy" / "Dockerfile"
_DOCKERIGNORE = _REPO / "deploy" / "Dockerfile.dockerignore"
_ENV_EXAMPLE = _REPO / "deploy" / ".env.example"

_DATA_MOUNT = "/var/lib/msg/data"
_OFFSETS_MOUNT = "/var/lib/msg/offsets"


def _load_yaml() -> dict[str, Any]:
    """解析编排文件。

    Returns:
        dict[str, Any]: 编排文档（顶层键 `services` / `networks` / `x-service-env`）。
    """
    raw: Any = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    return cast("dict[str, Any]", raw)


def _msg_service() -> dict[str, Any]:
    """取 `msg` 服务定义。

    Returns:
        dict[str, Any]: 服务定义。
    """
    services = cast("dict[str, Any]", _load_yaml()["services"])
    return cast("dict[str, Any]", services["msg"])


def _mount_pairs() -> dict[str, str]:
    """把卷声明解析成「容器内目标 → 宿主来源」。

    Returns:
        dict[str, str]: 目标路径到来源表达式的映射。
    """
    volumes = cast("list[str]", _msg_service()["volumes"])
    pairs: dict[str, str] = {}
    for entry in volumes:
        # 来源表达式里含 `:?` 冒号，只能按**最后一个**冒号切（目标路径不含冒号）
        source, _, target = entry.rpartition(":")
        assert source and target, f"卷声明应形如「来源:目标」：{entry}"
        assert not target.endswith(("ro", "rw")), f"卷不得声明只读 / 读写模式（服务要写）：{entry}"
        pairs[target] = source
    return pairs


def test_compose_parses_and_image_tag_is_mandatory() -> None:
    """编排可解析；镜像前缀与标签都用 `:?` 强制必填（无 latest 兜底）。"""
    service = _msg_service()
    image = cast("str", service["image"])
    assert image == (
        "${MSG_REGISTRY_IMAGE_PREFIX:?MSG_REGISTRY_IMAGE_PREFIX required}/msg:${MSG_TAG:?MSG_TAG required}"
    )
    assert "latest" not in image


def test_port_and_network() -> None:
    """容器内 8000 仅内部可达；加入平台外部网络，网络名由 `PLATFORM_NETWORK` 注入。"""
    service = _msg_service()
    assert service["expose"] == ["8000"]
    assert "ports" not in service, "不得映射宿主端口（经平台网关统一入口）"
    assert service["networks"] == ["platform"]

    platform = cast("dict[str, Any]", cast("dict[str, Any]", _load_yaml()["networks"])["platform"])
    assert platform["external"] is True
    assert platform["name"] == "${PLATFORM_NETWORK:-deploy_default}"


def test_two_persistent_volumes_are_writable_and_mandatory() -> None:
    """消息日志与位点各挂一独立卷、均可写；宿主目录变量必填；数据根固定为卷根。"""
    mounts = _mount_pairs()
    assert set(mounts) == {_DATA_MOUNT, _OFFSETS_MOUNT}
    for target, source in mounts.items():
        assert source.startswith("${MSG_") and ":?" in source, f"{target} 的来源变量须以 :? 强制必填：{source}"

    environment = cast("dict[str, Any]", _msg_service()["environment"])
    assert environment["MSG_DATA_DIR"] == "/var/lib/msg"


def test_stop_grace_and_restart_policy() -> None:
    """停机宽限覆盖基座在途等待；重启策略与平台其他服务同构；容器名即服务标识。"""
    service = _msg_service()
    assert service["stop_grace_period"] == "45s"
    assert service["restart"] == "unless-stopped"
    assert service["container_name"] == "msg"
    assert "healthcheck" not in service, "健康检查在镜像层声明，编排不重复"


def test_dockerfile_contract() -> None:
    """多阶段构建、锁文件口径、非 root、探针与入口齐备，且镜像内无凭据 / 前端 / 其他产品源码。"""
    text = _DOCKERFILE.read_text(encoding="utf-8")
    assert text.count("\nFROM ") >= 2, "须为多阶段构建（builder + runtime）"
    assert "--frozen" in text, "依赖必须按 uv.lock 锁定安装"
    assert "--no-dev" in text, "运行镜像不得装 dev 组（含跨仓路径依赖 engkit）"
    assert "\nUSER msg\n" in text, "须以非 root 用户运行（架构 08 §4）"
    assert "EXPOSE 8000" in text
    assert "HEALTHCHECK" in text and "/healthz" in text
    assert 'CMD ["python", "-m", "bms_msg"]' in text
    assert "BMS_BACKEND_ROOT=/opt/msg/backend" in text

    lowered = text.lower()
    assert "password=" not in lowered, "镜像内不得写死密码"
    assert "private key" not in lowered, "镜像内不得写入私钥"
    assert "msg/frontend" not in text, "前端源码不得进镜像"


def test_env_example_declares_required_keys_without_secrets() -> None:
    """凭据模板覆盖平台接入、镜像标签与两个卷目录，且只有占位值。"""
    text = _ENV_EXAMPLE.read_text(encoding="utf-8")
    for key in (
        "PLATFORM_NETWORK",
        "MSG_REGISTRY_IMAGE_PREFIX",
        "MSG_TAG",
        "MSG_DATA_HOST_DIR",
        "MSG_OFFSETS_HOST_DIR",
        "BMS_DATABASE__PLATFORM__URL_TEMPLATE",
        "BMS_REDIS__URL",
        "BMS_SERVICE_TOKEN__KEYS",
    ):
        assert f"\n{key}=" in f"\n{text}", f"模板缺少键：{key}"
    assert "PRIVATE KEY" not in text


def test_build_context_excludes_other_products_and_credentials() -> None:
    """构建上下文排除其他产品仓 / 前端 / 文档 / 部署态凭据。"""
    lines = {line.strip() for line in _DOCKERIGNORE.read_text(encoding="utf-8").splitlines()}
    for pattern in ("biz/", "cws/", "mdm/", "engkit/", "test/", "msg/frontend/", "msg/msg文档/", "**/deploy/.env"):
        assert pattern in lines, f"构建上下文未排除：{pattern}"
