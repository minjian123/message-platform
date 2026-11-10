"""部署自检命令（Kiwi 2473；任务 07-1-2 §4）：声明装载 / 七段齐备 / 唯一性 / 服务行同值 / 路由 / 探针接线。

覆盖策略：

- **正例走真实仓**：`run(仓根)` 必须全项通过——这是「本仓清单此刻可被平台装载」的可回归证据；
- **反例走构造数据**：清单文本按需改写（删段 / 去必填字段 / 造重名），部署资产落临时仓根，
  断言自检能**指名**违规，而不是笼统失败；
- 反例只碰 `msg.toml` 与三个部署资产，不复制整仓——自检本身是「读文本 + 判据」，无需真实工程。
"""

import re
import runpy
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.registry.products import ProductManifest, load_product_manifest
from bms_core.services.module_registry import ModuleRecord

from ops.check_deployment import (
    CONFIG_FILES,
    EXPECTED_PRODUCT_PREFIX,
    EXPECTED_UPSTREAM,
    REPO_ROOT,
    check_manifest_loading,
    check_probe_wiring,
    check_routes,
    check_sections,
    check_service_row,
    check_uniqueness,
    main,
    run,
    sections_notes,
)

pytestmark = pytest.mark.kiwi_id(2473)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_MANIFEST = _REPO / "deploy" / "registry" / "msg.toml"
_DOCKERFILE = _REPO / "deploy" / "Dockerfile"
_COMPOSE = _REPO / "deploy" / "compose" / "msg.yml"


def _manifest_text() -> str:
    """读真实清单文本（反例在其上做定点改写）。

    Returns:
        str: 清单 TOML 文本。
    """
    return _MANIFEST.read_text(encoding="utf-8")


def _fake_repo(tmp_path: Path, *, manifest: str | None = None, dockerfile: str | None = None) -> Path:
    """搭一个只含部署资产的最小仓根（自检读什么就放什么）。

    Args:
        tmp_path: pytest 临时目录。
        manifest: 清单文本（缺省取真实清单）。
        dockerfile: 镜像定义文本（缺省取真实文件）。

    Returns:
        Path: 临时仓根。
    """
    root = tmp_path / "repo"
    shutil.copytree(_REPO / "deploy" / "registry", root / "deploy" / "registry")
    shutil.copytree(_REPO / "deploy" / "contracts", root / "deploy" / "contracts")
    (root / "deploy" / "compose").mkdir(parents=True)
    shutil.copy(_COMPOSE, root / "deploy" / "compose" / "msg.yml")
    if dockerfile is not None:
        (root / "deploy" / "Dockerfile").write_text(dockerfile, encoding="utf-8")
    else:
        shutil.copy(_DOCKERFILE, root / "deploy" / "Dockerfile")
    (root / "backend").mkdir()
    for name in CONFIG_FILES:
        shutil.copy(_REPO / "backend" / name, root / "backend" / name)
    if manifest is not None:
        (root / "deploy" / "registry" / "msg.toml").write_text(manifest, encoding="utf-8")
    return root


def _manifest_with(tmp_path: Path, text: str) -> ProductManifest:
    """把改写后的清单落盘并装载。

    Args:
        tmp_path: pytest 临时目录。
        text: 清单文本。

    Returns:
        ProductManifest: 装载结果（结构合法才会走到业务判据）。
    """
    path = tmp_path / "msg.toml"
    path.write_text(text, encoding="utf-8")
    return load_product_manifest(path)


def _strip_section(text: str, section: str, next_section: str) -> str:
    """删掉某个 `[[段]]`（从段头到下一段头）。

    Args:
        text: 清单文本。
        section: 段名（不含方括号）。
        next_section: 下一段名（定位删除边界）。

    Returns:
        str: 改写后的文本。
    """
    pattern = rf"\[\[{section}\]\].*?(?=\[\[{next_section}\]\])"
    return re.sub(pattern, "", text, count=1, flags=re.S)


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    """自检会导入代码清单（登记进基座视图）；用例结束后复位插件注册表，避免影响后续装配用例。"""
    yield
    default_plugin_registry().reset()


def test_self_check_passes_on_real_repo(capsys: pytest.CaptureFixture[str]) -> None:
    """T1：真实仓根全项通过；能力段为空时输出显式标注，不当作违规。"""
    assert run(_REPO) == 0

    out = capsys.readouterr().out
    assert "[OK] C1 结构装载：0 违规" in out
    assert "[OK] C6 探针接线：0 违规" in out
    assert "[NOTE] C2 能力段未声明" in out
    assert "检查项 6 项：通过 6，失败 0" in out


def test_main_accepts_root_argument(capsys: pytest.CaptureFixture[str]) -> None:
    """T1b：命令行入口按 `--root` 定位仓根，退出码透传。"""
    assert main(ConcurrentStableList(["--root", str(REPO_ROOT)])) == 0
    assert "检查项 6 项" in capsys.readouterr().out
    assert REPO_ROOT == _REPO, "缺省仓根须与本文件推导的仓根一致"


def test_missing_section_is_reported(tmp_path: Path) -> None:
    """T2-a：删掉产品档案段 → C2 指名该段为空。"""
    manifest = _manifest_with(tmp_path, _strip_section(_manifest_text(), "products", "services"))

    violations = check_sections(manifest)

    assert any("产品档案段为空" in item for item in violations)


def test_illegal_service_row_is_rejected_on_load(tmp_path: Path) -> None:
    """T2-b：服务行缺 `table_prefix` → 装载即拒（`CatalogError` 收敛成 C1 违规）。"""
    text = _manifest_text().replace('table_prefix = "msg_"\n', "")
    broken = tmp_path / "deploy" / "registry" / "msg.toml"
    broken.parent.mkdir(parents=True)
    broken.write_text(text, encoding="utf-8")
    shutil.copytree(_REPO / "deploy" / "contracts", tmp_path / "deploy" / "contracts")

    inspection = check_manifest_loading(broken)

    assert not inspection.manifests
    assert any("table_prefix" in item for item in inspection.errors)


def test_duplicate_seed_name_is_reported(tmp_path: Path) -> None:
    """T2-c：两个种子同名 → C3 报重复（基座按同值去重放行，自检更严）。"""
    text = _manifest_text().replace('name = "msg-permissions"', 'name = "msg-usage-forms"', 1)
    manifest = _manifest_with(tmp_path, text)

    violations = check_uniqueness(manifest)

    assert any("种子 name 重复" in item for item in violations)


def test_duplicate_error_code_is_rejected_on_load(tmp_path: Path) -> None:
    """T2-d：同值重复的错误码行 → 装载期即拒（C1 兜住；C3 只对基座肯去重的段补强）。"""
    block = re.search(r"\[\[errcodes\]\]\ncode = 350010\n.*?(?=\[\[errcodes\]\])", _manifest_text(), flags=re.S)
    assert block is not None
    path = tmp_path / "msg.toml"
    path.write_text(_manifest_text() + "\n" + block.group(0), encoding="utf-8")

    inspection = check_manifest_loading(path)

    assert not inspection.manifests
    assert any("错误码重复声明（350010）" in item for item in inspection.errors)


def test_service_row_mismatch_is_reported() -> None:
    """T3：声明与代码清单任一字段漂移 → C4 指名字段两侧取值。"""
    manifest = load_product_manifest(_MANIFEST)
    drifted = ModuleRecord(
        module_key="msg",
        service_key="msg",
        name="msg 消息平台服务",
        table_prefix="msg_",
        business_code="message",
        errcode_segment="35",
        event_domain="msg",
        service_group="product",
        product_key="msg",
    )

    violations = check_service_row(manifest, ConcurrentStableList([drifted]))

    assert len(violations) == 1
    assert "business_code" in violations[0]
    assert "'message'" in violations[0]


def test_service_row_count_mismatch_is_reported() -> None:
    """T3b：服务行不是恰好 1 条 → 先报条数，不再逐字段比对。"""
    manifest = load_product_manifest(_MANIFEST)

    violations = check_service_row(manifest, ConcurrentStableList[ModuleRecord]())

    assert violations[0].startswith("代码清单服务行应恰好 1 条")


def test_route_derivation_is_pinned() -> None:
    """T5-a：对外前缀 / 上游节点按清单派生为任务字面值（`/api/msg/v1/msg` → `msg:8000`）。"""
    manifest = load_product_manifest(_MANIFEST)

    violations = check_routes(
        manifest,
        ConcurrentStableList(["/api/v1/msg/topics"]),
        ConcurrentStableDict(),
    )

    assert violations.to_list() == []
    route = manifest.product_routes[0]
    assert EXPECTED_PRODUCT_PREFIX == "/api/msg/v1/msg"
    assert EXPECTED_UPSTREAM == "msg:8000"
    assert (route.product_key, route.domain, route.service_key) == ("msg", "msg", "msg")


def test_route_violations_are_reported() -> None:
    """T5-b：契约路径越出内部前缀、服务行非产品分组、源码自拼前缀 → C5 逐条指名。"""
    real = load_product_manifest(_MANIFEST)
    # 服务行分组用替身：把真实清单改成 foundation 会先撞上基座「tenant_base 仅限 product 分组」的装载校验，
    # 而本用例要断的是自检自己的判据（分组非 product → 会多出一条服务级路由）。
    drifted = cast(
        "ProductManifest",
        SimpleNamespace(
            product_routes=real.product_routes,
            services=ConcurrentStableList([SimpleNamespace(service_group="foundation", service_key="msg")]),
        ),
    )
    sources = ConcurrentStableDict({"services/msg/src/bms_msg/api/x.py": 'PREFIX = "/api/msg/v1"'})

    violations = check_routes(drifted, ConcurrentStableList(["/api/v2/msg/topics"]), sources)

    joined = "\n".join(violations)
    assert "契约路径不在内部前缀下：/api/v2/msg/topics" in joined
    assert "分组应为 product" in joined
    assert "源码自拼产品前缀（services/msg/src/bms_msg/api/x.py）" in joined


def test_route_requires_business_paths() -> None:
    """T5-c：契约快照没有业务路径（只剩探针）→ C5 报「无从取证」。"""
    manifest = load_product_manifest(_MANIFEST)

    violations = check_routes(manifest, ConcurrentStableList(["/", "/healthz"]), ConcurrentStableDict())

    assert any("契约快照无业务路径" in item for item in violations)


def test_probe_wiring_violations_are_reported() -> None:
    """T6-a：三份配置未接 msg 注册表、镜像打 /healthz、编排重复声明 → C6 逐条指名。"""
    configs = ConcurrentStableDict(
        {"config.toml": '[health_check_registry]\nprovider = "null"\n', "config.prod.toml": "[app]\nname = 'x'\n"}
    )
    dockerfile = 'HEALTHCHECK CMD ["python", "-c", "urlopen(\'http://127.0.0.1:8000/healthz\')"]\n'
    compose = "services:\n  msg:\n    healthcheck:\n      test: ['CMD', 'true']\n"

    violations = check_probe_wiring(configs, dockerfile, compose)

    joined = "\n".join(violations)
    assert "config.toml 未接 msg 自持注册表" in joined
    assert "config.prod.toml 未接 msg 自持注册表" in joined
    assert "镜像健康检查判据应为 /readyz" in joined
    assert "镜像健康检查不应打 /healthz" in joined
    assert "编排不得重复声明 healthcheck" in joined


def test_probe_wiring_requires_healthcheck() -> None:
    """T6-b：镜像没有 `HEALTHCHECK` → C6 报缺失（不让「没声明」被当成通过）。"""
    configs = ConcurrentStableDict({"config.toml": '[health_check_registry]\nprovider = "msg"\n'})

    violations = check_probe_wiring(configs, "FROM python:3.14-slim\n", "services:\n  msg: {}\n")

    assert violations.to_list() == ["镜像未声明 HEALTHCHECK"]


def test_sections_notes_reports_absent_capabilities() -> None:
    """能力段为空 → 给出标注；非空 → 不输出（用替身避免构造完整能力记录）。"""
    empty = load_product_manifest(_MANIFEST)
    with_capability = cast("ProductManifest", SimpleNamespace(collaborations=ConcurrentStableList(["msg.op"])))

    assert any("能力段未声明" in item for item in sections_notes(empty))
    assert sections_notes(with_capability).to_list() == []


def test_run_reports_asset_drift(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """C6 失败（镜像仍打 /healthz）→ 整轮退出码 1，并逐项打印违规。"""
    root = _fake_repo(
        tmp_path,
        dockerfile='HEALTHCHECK CMD ["python", "-c", "urlopen(\'http://127.0.0.1:8000/healthz\')"]\n',
    )

    assert run(root) == 1

    out = capsys.readouterr().out
    assert "[FAIL] C6 探针接线：2 项违规" in out
    assert "检查项 6 项：通过 5，失败 1" in out


def test_run_stops_when_manifest_unloadable(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """清单不可装载 → C1 失败即停（C2~C6 不执行），避免后续以空清单下结论。"""
    root = _fake_repo(tmp_path, manifest="product_key = [unclosed\n")

    assert run(root) == 1

    out = capsys.readouterr().out
    assert "[FAIL] C1 结构装载" in out
    assert "C2~C6 未执行" in out
    assert "检查项 6 项：通过 5，失败 1" in out


def test_service_row_declared_count_is_checked() -> None:
    """C4：声明侧服务行不止 1 条 → 只报条数，不再逐字段比对。"""
    service = SimpleNamespace(service_key="msg")
    manifest = cast("ProductManifest", SimpleNamespace(services=ConcurrentStableList([service, service])))

    violations = check_service_row(manifest, ConcurrentStableList([cast("ModuleRecord", service)]))

    assert violations[0].startswith("服务行应恰好 1 条")


def test_route_section_must_not_be_empty() -> None:
    """C5：产品路由段为空 → 对外命名空间无从派生。"""
    manifest = cast(
        "ProductManifest",
        SimpleNamespace(product_routes=ConcurrentStableList(), services=ConcurrentStableList()),
    )

    violations = check_routes(manifest, ConcurrentStableList(["/api/v1/msg/topics"]), ConcurrentStableDict())

    assert violations.to_list() == ["产品路由段为空（对外命名空间无法派生）"]


def test_route_derivation_mismatch_is_reported() -> None:
    """C5：域段 / 服务键被改乱 → 外部前缀与上游各报一条（不静默按缺省派生）。"""
    manifest = cast(
        "ProductManifest",
        SimpleNamespace(
            product_routes=ConcurrentStableList(
                [SimpleNamespace(product_key="msg", domain="other", service_key="other")]
            ),
            services=ConcurrentStableList([SimpleNamespace(service_group="product", service_key="other")]),
        ),
    )

    violations = check_routes(manifest, ConcurrentStableList(["/api/v1/msg/topics"]), ConcurrentStableDict())

    joined = " ".join(violations)
    assert "外部前缀派生不符：/api/msg/v1/other" in joined
    assert "路由上游不符：other:8000" in joined


def test_module_entrypoint_exits_with_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m ops.check_deployment` 的入口守卫可执行，且把自检结果转成退出码。"""
    monkeypatch.setattr(sys, "argv", ["ops.check_deployment"])

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("ops.check_deployment", run_name="__main__")

    assert excinfo.value.code == 0
