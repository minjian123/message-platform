"""部署自检（任务 07-1-2）：产品声明装载 + 七段齐备 + 唯一性 + 服务行同值 + 路由口径 + 探针接线。

用法::

    python -m ops.check_deployment                # 全项自检；任一违规 → 明细 + 退出码 1
    python -m ops.check_deployment --root .       # 指定仓库根

为什么单独做一条命令（而不是只写用例）：

- msg 仓没有 CI 流水线，发布前 / 容器内**没有可跑的离线自检**——`inspect_product_manifests` 只在
  装配期与 bms 侧跨仓 `check_modules` 里跑到，产品侧的人手上没有出口；
- 声明装载失败在部署态表现为「容器起即退」，排障成本高；把六项判据前置成一条命令，退出码即可门禁。

设计取舍：

1. **离线零依赖**：只读本仓声明 / 契约 / 配置 / 镜像 / 编排文本，不连库、不起应用、不读别的产品仓；
   依赖限定标准库 + `bms_core`。**不 import `yaml`**（`pyyaml` 在 dev 组，生产镜像内没有），
   编排文件按文本断言。
2. **判据不发明**：结构装载复用 `inspect_product_manifests`（与平台装配期同一实现），本模块只补
   平台不管的四件事——段齐备、段内唯一性、声明↔代码清单同值、部署资产（镜像 / 编排 / 配置）接线。
3. **唯一性比平台更严**：基座按「同键同值去重」放行重复行（保证注册幂等），本自检**任何重复都算违规**
   ——清单里的重复行是复制粘贴的典型痕迹，早报早改。
4. **可测**：每项检查都是「入参即数据」的纯函数，`run()` 只负责读文件与拼装，用例可直接喂构造数据。
"""

from __future__ import annotations

import argparse
import json
import tomllib
from pathlib import Path
from typing import Any, cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.registry.products import ManifestInspection, ProductManifest, inspect_product_manifests
from bms_core.services.gateway_catalog import product_route_prefix, upstream_node
from bms_core.services.module_registry import ModuleRecord, ServiceGroup

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
"""后端工程根（`backend/`，脚本位于 `backend/ops/`）。"""

REPO_ROOT = _BACKEND_ROOT.parent
"""仓库根（`backend/` 的父级）。"""

MANIFEST_RELATIVE = Path("deploy") / "registry" / "msg.toml"
"""产品声明（唯一声明源）。"""

CONTRACT_RELATIVE = Path("deploy") / "contracts" / "msg.json"
"""公开契约快照（内部路径口径的取证处）。"""

DOCKERFILE_RELATIVE = Path("deploy") / "Dockerfile"
"""镜像定义（健康检查判据的声明处）。"""

COMPOSE_RELATIVE = Path("deploy") / "compose" / "msg.yml"
"""容器编排（健康检查不得重复声明）。"""

CONFIG_FILES = ("config.toml", "config.prod.toml", "config.test.toml")
"""后端配置三份（探针接线须一致）。"""

SOURCE_RELATIVE = Path("backend") / "services" / "msg" / "src"
"""服务源码根（扫自拼产品前缀的取证处）。"""

PRODUCT_KEY = "msg"
"""产品标识（与清单 `product_key` 同值）。"""

API_PREFIX = f"/api/v1/{PRODUCT_KEY}"
"""服务内统一接口前缀（路由挂载口径）。"""

EXPECTED_PRODUCT_PREFIX = f"/api/{PRODUCT_KEY}/v1/{PRODUCT_KEY}"
"""对外命名空间（网关按产品级路由派生，见架构 08 §2 / 网关生成器）。"""

EXPECTED_UPSTREAM = f"{PRODUCT_KEY}:8000"
"""上游节点（Compose DNS + 容器内端口）。"""

HEALTH_PROVIDER = "msg"
"""健康检查注册表实现名（`[health_check_registry].provider`）。"""

CHECK_COUNT = 6
"""检查项总数（报告汇总用）。"""

SERVICE_FIELDS = ConcurrentStableFrozenList(
    [
        "module_key",
        "service_key",
        "name",
        "table_prefix",
        "business_code",
        "errcode_segment",
        "event_domain",
        "service_group",
        "product_key",
        "contract_version",
        "status",
    ]
)
"""服务行须与代码清单同值的字段（任务字面六键 + 装配期合并会拒启的四键）。"""

SECTION_LABELS = ConcurrentStableFrozenDict(
    {
        "products": "产品档案",
        "services": "服务行",
        "product_routes": "产品路由",
        "data_sources": "数据源",
        "seeds": "种子",
        "error_codes": "错误码",
        "domains": "业务域",
    }
)
"""七段的属性名与中文名（能力段单列：允许为空，但须显式标注）。"""


def check_manifest_loading(manifest_path: Path) -> ManifestInspection:
    """C1 结构装载：复用平台装载实现（解析 / 跨清单唯一性 / 契约反向对账 / 码段校验）。

    Args:
        manifest_path: 清单文件路径。

    Returns:
        ManifestInspection: 装载结果；`errors` 即违规明细。
    """
    return inspect_product_manifests([manifest_path])


def check_sections(manifest: ProductManifest) -> ConcurrentStableList[str]:
    """C2 七段齐备：产品档案 / 服务行 / 产品路由 / 数据源 / 种子 / 错误码 / 业务域均非空。

    Args:
        manifest: 产品声明。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    violations: ConcurrentStableList[str] = ConcurrentStableList()
    for attribute, label in SECTION_LABELS.items():
        records: object = getattr(manifest, attribute)
        if not records:
            violations.add(f"{label}段为空（七段须齐备）")
    return violations


def sections_notes(manifest: ProductManifest) -> ConcurrentStableList[str]:
    """七段齐备的补充说明：能力段（`[[collaborations]]`）为空时给出显式标注（空段不算违规，但不静默）。

    Args:
        manifest: 产品声明。

    Returns:
        ConcurrentStableList[str]: 标注文案；能力段非空时为空列表。
    """
    if manifest.collaborations:
        return ConcurrentStableList()
    return ConcurrentStableList(["能力段未声明：本产品不对外提供能力（`[[collaborations]]` 为空属预期）"])


def check_uniqueness(manifest: ProductManifest) -> ConcurrentStableList[str]:
    """C3 唯一性：七段主键各自不得重复（同值重复也算违规）。

    Args:
        manifest: 产品声明。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    groups: ConcurrentStableDict[str, ConcurrentStableList[str]] = ConcurrentStableDict()
    groups.set(
        "服务行 service_key",
        ConcurrentStableList(str(record.service_key) for record in manifest.services),
    )
    groups.set(
        "产品路由 product_key/domain",
        ConcurrentStableList(f"{record.product_key}/{record.domain}" for record in manifest.product_routes),
    )
    groups.set(
        "数据源 data_source_key",
        ConcurrentStableList(record.data_source_key for record in manifest.data_sources),
    )
    groups.set("种子 name", ConcurrentStableList(record.name for record in manifest.seeds))
    groups.set("错误码 code", ConcurrentStableList(str(record.code) for record in manifest.error_codes))
    groups.set("业务域 domain_key", ConcurrentStableList(record.domain_key for record in manifest.domains))
    groups.set(
        "资源类型 key",
        ConcurrentStableList(record.resource_type_key for record in manifest.resource_types),
    )

    violations: ConcurrentStableList[str] = ConcurrentStableList()
    for label, keys in groups.items():
        seen: ConcurrentStableSet[str] = ConcurrentStableSet()
        for key in keys:
            if key in seen:
                violations.add(f"{label} 重复：{key}")
            seen.add(key)
    return violations


def check_service_row(
    manifest: ProductManifest,
    coded: ConcurrentStableList[ModuleRecord],
) -> ConcurrentStableList[str]:
    """C4 服务行同值：声明服务行与代码清单逐字段一致（异值会让装配期合并拒启）。

    Args:
        manifest: 产品声明。
        coded: 代码清单（`services.module_registry.MSG_SERVICE_RECORDS`）。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    violations: ConcurrentStableList[str] = ConcurrentStableList()
    if len(manifest.services) != 1:
        violations.add(f"服务行应恰好 1 条（现 {len(manifest.services)} 条）")
    if len(coded) != 1:
        violations.add(f"代码清单服务行应恰好 1 条（现 {len(coded)} 条）")
    if violations:
        return violations
    declared = manifest.services[0]
    reference = coded[0]
    for field in SERVICE_FIELDS:
        declared_value = getattr(declared, field)
        coded_value = getattr(reference, field)
        if declared_value != coded_value:
            violations.add(f"字段 {field}：声明 = {declared_value!r}，代码 = {coded_value!r}")
    return violations


def check_routes(
    manifest: ProductManifest,
    contract_paths: ConcurrentStableList[str],
    sources: ConcurrentStableDict[str, str],
) -> ConcurrentStableList[str]:
    """C5 路由口径：对外命名空间派生 + 内部前缀一致 + 禁止自拼产品前缀 / 双路径。

    Args:
        manifest: 产品声明。
        contract_paths: 契约快照的全部路径。
        sources: 源码文本（`相对路径 → 内容`）。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    violations: ConcurrentStableList[str] = ConcurrentStableList()
    if not manifest.product_routes:
        violations.add("产品路由段为空（对外命名空间无法派生）")
    for route in manifest.product_routes:
        prefix = product_route_prefix(route.product_key, route.domain)
        if prefix != EXPECTED_PRODUCT_PREFIX:
            violations.add(f"外部前缀派生不符：{prefix}（应为 {EXPECTED_PRODUCT_PREFIX}）")
        node = upstream_node(route.service_key)
        if node != EXPECTED_UPSTREAM:
            violations.add(f"路由上游不符：{node}（应为 {EXPECTED_UPSTREAM}）")
    for service in manifest.services:
        if service.service_group != ServiceGroup.PRODUCT:
            # 非产品分组会额外生成服务级路由 `/api/{service}/v1`，与产品命名空间双路径并存。
            violations.add(
                f"服务行 {service.service_key} 分组应为 {ServiceGroup.PRODUCT}（现 {service.service_group}）"
            )

    business: ConcurrentStableList[str] = ConcurrentStableList(
        path for path in contract_paths if path.startswith("/api/")
    )
    if not business:
        violations.add("契约快照无业务路径（内部前缀无从取证）")
    for path in business:
        if not path.startswith(API_PREFIX):
            violations.add(f"契约路径不在内部前缀下：{path}（应 {API_PREFIX}/…）")
    for name, text in sources.items():
        if f"/api/{PRODUCT_KEY}" in text:
            violations.add(f"源码自拼产品前缀（{name}）：对外前缀只由网关派生")
    return violations


def check_probe_wiring(
    configs: ConcurrentStableDict[str, str],
    dockerfile: str,
    compose: str,
) -> ConcurrentStableList[str]:
    """C6 探针接线：三份配置接 msg 自持注册表；镜像判据为就绪；编排不重复声明。

    Args:
        configs: 后端配置文本（`文件名 → 内容`）。
        dockerfile: 镜像定义文本。
        compose: 容器编排文本。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    violations: ConcurrentStableList[str] = ConcurrentStableList()
    for name, text in configs.items():
        provider = _health_provider(text)
        if provider != HEALTH_PROVIDER:
            violations.add(f"{name} 未接 msg 自持注册表（[health_check_registry].provider = {provider!r}）")
    block = _healthcheck_block(dockerfile)
    if not block:
        violations.add("镜像未声明 HEALTHCHECK")
    else:
        if "/readyz" not in block:
            violations.add("镜像健康检查判据应为 /readyz（与网关摘流同一判据）")
        if "/healthz" in block:
            violations.add("镜像健康检查不应打 /healthz（存活由进程退出表达，Docker 不因 unhealthy 重启）")
    if "healthcheck:" in compose:
        violations.add("编排不得重复声明 healthcheck（判据单点，避免两处漂移）")
    return violations


def _health_provider(text: str) -> str | None:
    """取配置里的健康检查注册表实现名。

    Args:
        text: 配置文本。

    Returns:
        str | None: `[health_check_registry].provider` 的值；未声明为 None。
    """
    parsed: Any = tomllib.loads(text)
    root = cast("dict[str, Any]", parsed)
    section = cast("dict[str, Any]", root.get("health_check_registry") or {})
    provider = section.get("provider")
    return None if provider is None else str(provider)


def _healthcheck_block(dockerfile: str) -> str:
    """取 `HEALTHCHECK` 指令整块（含 `\\` 续行），供判据断言。

    Args:
        dockerfile: 镜像定义文本。

    Returns:
        str: 指令整块文本；未声明时为空串。
    """
    lines = dockerfile.splitlines()
    for index, line in enumerate(lines):
        if not line.startswith("HEALTHCHECK"):
            continue
        block: ConcurrentStableList[str] = ConcurrentStableList()
        cursor = index
        block.add(lines[cursor])
        while lines[cursor].rstrip().endswith("\\") and cursor + 1 < len(lines):
            cursor += 1
            block.add(lines[cursor])
        return " ".join(item.strip() for item in block)
    return ""


def _read_texts(root: Path, relatives: ConcurrentStableFrozenList[Path]) -> ConcurrentStableDict[str, str]:
    """按相对路径读文本（缺文件即抛，交给调用方按部署资产缺失处理）。

    Args:
        root: 仓库根。
        relatives: 相对路径序列。

    Returns:
        ConcurrentStableDict[str, str]: `相对路径 → 文本`。
    """
    texts: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for relative in relatives:
        texts.set(relative.as_posix(), (root / relative).read_text(encoding="utf-8"))
    return texts


def _source_texts(root: Path) -> ConcurrentStableDict[str, str]:
    """读服务源码文本（只为扫自拼产品前缀，按相对路径保序）。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableDict[str, str]: `相对路径 → 文本`。
    """
    texts: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for path in sorted((root / SOURCE_RELATIVE).rglob("*.py")):
        texts.set(path.relative_to(root).as_posix(), path.read_text(encoding="utf-8"))
    return texts


def _contract_paths(root: Path) -> ConcurrentStableList[str]:
    """取契约快照的全部路径（键序即生成序）。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 路径列表。
    """
    payload: Any = json.loads((root / CONTRACT_RELATIVE).read_text(encoding="utf-8"))
    paths = cast("dict[str, Any]", cast("dict[str, Any]", payload)["paths"])
    return ConcurrentStableList(paths.keys())


def _code_service_records() -> ConcurrentStableList[ModuleRecord]:
    """取代码清单服务行（延迟导入：导入即向基座视图登记，避免仅用自检时产生副作用）。

    Returns:
        ConcurrentStableList[ModuleRecord]: 代码侧服务行。
    """
    from services.module_registry import MSG_SERVICE_RECORDS

    return ConcurrentStableList(MSG_SERVICE_RECORDS)


def _report(code: str, title: str, violations: ConcurrentStableList[str]) -> int:
    """打印单项结果。

    Args:
        code: 检查项编号（C1~C6）。
        title: 检查项中文名。
        violations: 违规明细。

    Returns:
        int: 1 表示该项不通过，0 表示通过。
    """
    if not violations:
        print(f"[OK] {code} {title}：0 违规")
        return 0
    print(f"[FAIL] {code} {title}：{len(violations)} 项违规")
    for item in violations:
        print(f"       - {item}")
    return 1


def run(root: Path) -> int:
    """执行全项自检并打印报告。

    Args:
        root: 仓库根。

    Returns:
        int: 0 全项通过；1 存在违规（明细已逐项打印）。
    """
    inspection = check_manifest_loading(root / MANIFEST_RELATIVE)
    failed = _report("C1", "结构装载", ConcurrentStableList(inspection.errors))
    if not inspection.manifests:
        print(f"       声明未能装载，C2~C{CHECK_COUNT} 未执行")
        print(f"检查项 {CHECK_COUNT} 项：通过 {CHECK_COUNT - 1}，失败 1")
        return 1

    manifest = inspection.manifests[0]
    failed += _report("C2", "七段齐备", check_sections(manifest))
    for note in sections_notes(manifest):
        print(f"[NOTE] C2 {note}")
    failed += _report("C3", "唯一性", check_uniqueness(manifest))
    failed += _report("C4", "服务行同值", check_service_row(manifest, _code_service_records()))
    failed += _report(
        "C5",
        "路由口径",
        check_routes(manifest, _contract_paths(root), _source_texts(root)),
    )
    failed += _report(
        "C6",
        "探针接线",
        check_probe_wiring(
            _read_texts(root / "backend", ConcurrentStableFrozenList(Path(name) for name in CONFIG_FILES)),
            (root / DOCKERFILE_RELATIVE).read_text(encoding="utf-8"),
            (root / COMPOSE_RELATIVE).read_text(encoding="utf-8"),
        ),
    )
    print(f"检查项 {CHECK_COUNT} 项：通过 {CHECK_COUNT - failed}，失败 {failed}")
    return 0 if failed == 0 else 1


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码（0 通过 / 1 违规）。
    """
    parser = argparse.ArgumentParser(description="msg 部署自检：声明装载 / 路由口径 / 探针接线")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="仓库根（缺省自动定位）")
    args = parser.parse_args(None if argv is None else argv)
    return run(args.root)


if __name__ == "__main__":
    raise SystemExit(main())
