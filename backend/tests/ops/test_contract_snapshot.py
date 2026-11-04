"""契约快照脚本（任务 03-1；Kiwi 2420）：导出 / 零漂移 / 缺件多余件 / 构建失败分支。

`backend/ops/contract_snapshot.py` 是把「契约快照」接进预检的那一环（01-1 起人工重导挂了 3 轮），
本文件既验功能也把错误分支走一遍（覆盖率口径：`ops` 属统计包，要求行 + 分支 100%）。
"""

import runpy
import sys
from pathlib import Path
from types import ModuleType

import pytest
from bms_core.core.plugin import default_plugin_registry
from bms_core.services.service_contract import render_contract_json

from ops import contract_snapshot

pytestmark = pytest.mark.kiwi_id(2420)


@pytest.fixture(autouse=True)
def _plugins() -> None:
    """每个装配前重置插件注册表（`create()` 会冻结注册表，二次装配必须重置）。"""
    default_plugin_registry().reset()


def _fresh() -> None:
    """再装配前重置注册表（同一用例内多次 build 用）。"""
    default_plugin_registry().reset()


def test_export_then_check_passes(tmp_path: Path) -> None:
    """导出后在干净目录上 `check` 通过，且快照含本任务的根路径端点。"""
    root = tmp_path

    assert contract_snapshot.export(root) == 0
    snapshot = contract_snapshot.snapshot_path(root, "msg")
    assert snapshot.is_file()
    assert '"/msg/subscribe"' in snapshot.read_text(encoding="utf-8")

    _fresh()
    assert contract_snapshot.check(root) == 0


def test_export_print_writes_stdout(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """`--print` 把生成内容打到标准输出（CI / 调试时看差异用）。"""
    assert contract_snapshot.export(tmp_path, to_stdout=True) == 0

    captured = capsys.readouterr()
    assert '"openapi"' in captured.out


def test_check_reports_drift(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """快照被改动一个字符即判漂移（预检红，提示重新 export）。"""
    assert contract_snapshot.export(tmp_path) == 0
    snapshot = contract_snapshot.snapshot_path(tmp_path, "msg")
    snapshot.write_text(snapshot.read_text(encoding="utf-8") + " ", encoding="utf-8")

    _fresh()
    assert contract_snapshot.check(tmp_path) == 1
    assert "漂移" in capsys.readouterr().err


def test_check_reports_missing_and_extra(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """缺件与多余件各报一条（目录里既不能少也不能多）。"""
    assert contract_snapshot.export(tmp_path) == 0
    snapshot = contract_snapshot.snapshot_path(tmp_path, "msg")
    snapshot.unlink()
    (contract_snapshot.contracts_dir(tmp_path) / "ghost.json").write_text("{}", encoding="utf-8")

    assert contract_snapshot.check(tmp_path) == 1
    err = capsys.readouterr().err
    assert "缺快照文件" in err
    assert "多余快照文件" in err


def test_check_reports_validation_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """契约校验（结构 / 版本）失败也算不通过，而不是只比文本。"""
    assert contract_snapshot.export(tmp_path) == 0
    monkeypatch.setattr(contract_snapshot, "_validate", lambda record, openapi: ["契约版本不符"])

    _fresh()
    assert contract_snapshot.check(tmp_path) == 1
    assert "契约版本不符" in capsys.readouterr().err


def test_export_reports_validation_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """导出路径同样先校验：不通过则不写文件、退出码 1。"""
    monkeypatch.setattr(contract_snapshot, "_validate", lambda record, openapi: ["契约版本不符"])

    assert contract_snapshot.export(tmp_path) == 1
    assert "契约版本不符" in capsys.readouterr().err
    assert not contract_snapshot.snapshot_path(tmp_path, "msg").exists()


def test_build_openapi_reports_missing_package(monkeypatch: pytest.MonkeyPatch) -> None:
    """服务包不存在 → 可读错误（不是裸 ImportError）。"""

    def _boom(name: str) -> ModuleType:
        """模拟模块缺失。

        Args:
            name: 模块名。

        Raises:
            ModuleNotFoundError: 恒抛。
        """
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(contract_snapshot, "import_module", _boom)

    with pytest.raises(RuntimeError, match="服务包缺失"):
        contract_snapshot.build_openapi("msg")


def test_build_openapi_requires_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """模块没有 `ApplicationFactory` → 明确报缺失。"""
    monkeypatch.setattr(contract_snapshot, "import_module", lambda name: ModuleType(name))

    with pytest.raises(RuntimeError, match="缺少 ApplicationFactory"):
        contract_snapshot.build_openapi("msg")


def test_build_openapi_wraps_build_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """装配抛异常 → 包成 `RuntimeError` 并带原始原因（CLI 边界统一口径）。"""
    module = ModuleType("bms_msg.main")

    class _Factory:
        """装配即失败的工厂替身。"""

        def create(self, options: object) -> object:
            """恒抛。

            Args:
                options: 未使用。

            Raises:
                ValueError: 恒抛。
            """
            del options
            raise ValueError("boom")

    module.ApplicationFactory = _Factory  # type: ignore[attr-defined]
    monkeypatch.setattr(contract_snapshot, "import_module", lambda name: module)

    with pytest.raises(RuntimeError, match="服务应用构建失败"):
        contract_snapshot.build_openapi("msg")


def test_ensure_src_on_path_inserts_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """源码目录不在 `sys.path` 时补上；已在则不重复插入。"""
    src = str(contract_snapshot._BACKEND_ROOT / "services" / "msg" / "src")
    saved = list(sys.path)
    try:
        sys.path = [p for p in sys.path if p != src]
        contract_snapshot._ensure_src_on_path()
        assert sys.path.count(src) == 1
        contract_snapshot._ensure_src_on_path()
        assert sys.path.count(src) == 1
    finally:
        sys.path = saved


def test_service_package_and_paths(tmp_path: Path) -> None:
    """包名推导与快照路径口径（单服务简化：`bms` 前缀 + 服务键）。"""
    assert contract_snapshot.service_package("msg") == "bms_msg"
    assert contract_snapshot.contracts_dir(tmp_path) == tmp_path / "deploy" / "contracts"
    assert contract_snapshot.snapshot_path(tmp_path, "msg").name == "msg.json"
    assert [record.service_key for record in contract_snapshot.enabled_local_records()] == ["msg"]


def test_main_export_and_check(tmp_path: Path) -> None:
    """命令行入口：`export` 后 `check` 返回 0（预检步骤走的就是这条）。"""
    assert contract_snapshot.main(["export", "--root", str(tmp_path)]) == 0
    _fresh()
    assert contract_snapshot.main(["check", "--root", str(tmp_path)]) == 0


def test_module_entrypoint_exits_with_main_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m ops.contract_snapshot` 入口：返回码透传（缺快照即 1，预检据此判红）。"""
    monkeypatch.setattr(sys, "argv", ["ops.contract_snapshot", "check", "--root", str(tmp_path)])

    with pytest.raises(SystemExit) as exited:
        runpy.run_module("ops.contract_snapshot", run_name="__main__")

    assert exited.value.code == 1


def test_render_contract_json_is_deterministic(tmp_path: Path) -> None:
    """同代码重复构建产出同文本（零漂移校验成立的前提）。"""
    assert contract_snapshot.export(tmp_path) == 0
    first = contract_snapshot.snapshot_path(tmp_path, "msg").read_text(encoding="utf-8")

    _fresh()
    openapi = contract_snapshot.build_openapi("msg")

    assert render_contract_json(openapi) == first
