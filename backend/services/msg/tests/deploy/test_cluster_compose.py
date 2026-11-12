"""集群编排资产（02-8）：profiles / 三实例 / 8001 / 每实例卷 / 覆盖层 / env 模板。"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
COMPOSE = REPO_ROOT / "deploy" / "compose" / "msg.yml"
GATEWAY = REPO_ROOT / "deploy" / "compose" / "msg-cluster.gateway.yml"
DRILL = REPO_ROOT / "deploy" / "compose" / "msg-drill.override.yml"
ENV_EXAMPLE = REPO_ROOT / "deploy" / ".env.example"
DOCKERFILE = REPO_ROOT / "deploy" / "Dockerfile"
CLUSTER_ENV = REPO_ROOT / "deploy" / "drill" / "cluster.env.example"


def test_compose_keeps_single_instance_default() -> None:
    """单实例：`msg` 服务仍在且不带 profile（默认即单实例）。"""
    text = COMPOSE.read_text(encoding="utf-8")

    assert "\n  msg:\n" in text
    assert "container_name: msg\n" in text
    assert "${MSG_DATA_HOST_DIR:?MSG_DATA_HOST_DIR required}:/var/lib/msg/data" in text
    assert "${MSG_OFFSETS_HOST_DIR:?MSG_OFFSETS_HOST_DIR required}:/var/lib/msg/offsets" in text


def test_compose_cluster_profile_has_three_instances() -> None:
    """三实例：cluster profile、8000+8001、每实例独立卷与节点地址。"""
    text = COMPOSE.read_text(encoding="utf-8")

    assert 'profiles: ["cluster"]' in text
    assert text.count('profiles: ["cluster"]') == 3
    for index in (1, 2, 3):
        assert f"container_name: msg-{index}" in text
        assert f"MSG_DATA_HOST_DIR_{index}:?" in text
        assert f"MSG_OFFSETS_HOST_DIR_{index}:?" in text
        assert f"MSG_REPLICATION_NODE_ID: msg-{index}" in text
    assert text.count('"8001"') >= 3
    assert "MSG_REPLICATION_NODE_ENDPOINTS: msg-1=http://msg-1:8001" in text
    assert "MSG_REPLICATION_INTERNAL_PORT" in text
    assert 'BMS_REGISTRY__REGISTER_ON_STARTUP: "false"' in text


def test_gateway_overlay_adds_alias_and_drill_does_not() -> None:
    """网关覆盖层：三实例加 `msg` 别名；drill 覆盖层不启用别名且改容器名 / 回环端口。"""
    gateway = GATEWAY.read_text(encoding="utf-8")
    drill = DRILL.read_text(encoding="utf-8")

    assert gateway.count("- msg") == 3
    assert "container_name: msg-drill" not in gateway
    for index in (1, 2, 3):
        assert f"container_name: msg-drill-{index}" in drill
        assert f"127.0.0.1:${{MSG_DRILL_PORT_{index}:-1880{index - 1}}}:8000" in drill
    assert "aliases" not in drill


def test_env_example_and_dockerfile_expose_new_keys() -> None:
    """模板：每实例卷与内部通道键位齐备；镜像声明 8001。"""
    env_text = ENV_EXAMPLE.read_text(encoding="utf-8")
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    cluster_env = CLUSTER_ENV.read_text(encoding="utf-8")

    for index in (1, 2, 3):
        assert f"MSG_DATA_HOST_DIR_{index}=" in env_text
        assert f"MSG_OFFSETS_HOST_DIR_{index}=" in env_text
    for key in (
        "MSG_REPLICATION_INTERNAL_PORT=",
        "MSG_REPLICATION_NODE_ENDPOINTS=",
        "MSG_REPLICATION_INTERNAL_TIMEOUT_MS=",
        "MSG_REPLICATION_FETCH_MAX_BYTES=",
    ):
        assert key in env_text
    assert "EXPOSE 8001" in dockerfile
    for key in ("MSG_CLUSTER_DRILL_SSH_TARGET=", "MSG_CLUSTER_DRILL_WORK_DIR=", "MSG_CLUSTER_DRILL_PROJECT="):
        assert key in cluster_env
