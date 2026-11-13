#!/bin/sh
# bms 基座克隆准备（2026-10-27 起）：`bms-core` 已改为**路径依赖**
# （`backend/pyproject.toml` 的 `../../bms/backend/libs/bms_core`），故后端 / 前端 job 都必须先让
# `../bms` 存在，`uv sync --frozen` / `pnpm install --frozen-lockfile` 才能按锁文件装齐基座。
#
# 用法：`. scripts/tools/ci/bms_setup.sh`（在 `cd backend` / `cd frontend` 之前执行；两个 job 共用）
#
# 口径：
#   1. 克隆目录固定为 `<仓根>/../bms`——与本地工作区布局（`msg/` 与 `bms/` 平级）同构，
#      使 pyproject / pnpm-workspace 里的相对路径在本地与 CI 一致；
#   2. `BMS_REF` 取值（**缺省 main，日常零维护**）：
#        - `main`（或 `latest`）：**bms 远端 main 头提交**（每次 fetch 解引用 `origin/main`，不复用旧本地分支）；
#        - `latest-tag`：bms 远端最新 `vX.Y.Z` 标签（无标签回落 main 头）——发布验收可显式钉到最新 tag；
#        - 其他分支 / tag / 提交 SHA：按给定值检出（钉版本时用；见 bms 仓 `scripts/tools/ci/bms_ref.py`）。
#   3. 检出统一 `--detach`：runner 工作区跨作业复用时看的是**远端当前值**，不会停在首次克隆的旧提交；
#      同时始终刷新 `origin/main`（`check_prototype_review --changed origin/main` 等差分门禁的基线）；
#   4. 拉取失败即失败（不静默跳过）——基座缺失时依赖安装必然报错，早失败可归因清晰。
#   5. **跨仓克隆需在目标项目授权**（2026-11-13）：GitLab job token 走**被访问项目**的入向白名单，
#      调用方 `mjs/msg` 必须登记在 `bms/bms` 与 `bms/engkit` 的 Settings › CI/CD › Job token permissions
#      里；未登记即 403（`remote: Authentication by CI/CD job token not allowed from msg to project #N`），
#      与代码无关。新增跨仓克隆依赖（或新增产品仓）时同步登记。口径见 bms《开发部署规划》
#      「开发工作流与 CI」节。

BMS_REPO_ROOT="${CI_PROJECT_DIR:-$(cd "$(dirname "$0")/../../.." && pwd)}"
BMS_CLONE_DIR="${BMS_REPO_ROOT}/../bms"
BMS_REF="${BMS_REF:-main}"

BMS_SCHEME="${CI_SERVER_URL%%://*}"
BMS_HOSTPORT="${CI_SERVER_URL#*://}"
if [ -n "${CI_SERVER_URL}" ]; then
    BMS_DEFAULT_URL="${BMS_SCHEME}://gitlab-ci-token:${CI_JOB_TOKEN:-}@${BMS_HOSTPORT}/bms/bms.git"
else
    BMS_DEFAULT_URL=""
fi
BMS_URL="${BMS_REPO_URL:-${BMS_DEFAULT_URL}}"

if [ ! -d "${BMS_CLONE_DIR}/.git" ]; then
    if [ -z "${BMS_URL}" ]; then
        echo "[bms] 缺少仓库地址：CI 外运行请设 BMS_REPO_URL（或 CI_SERVER_URL + CI_JOB_TOKEN）" >&2
        exit 1
    fi
    git clone --quiet --no-checkout "${BMS_URL}" "${BMS_CLONE_DIR}"
fi

# 复用目录里 origin 存的是**上一次 job** 的 URL（含当时的 CI_JOB_TOKEN，每个 job 都不同），
# 直接 fetch 会 401/403（2026-10-28 #1281 同类问题）——先刷新 remote URL。
if [ -n "${BMS_URL}" ]; then
    git -C "${BMS_CLONE_DIR}" remote set-url origin "${BMS_URL}"
fi

# 差分门禁基线：origin/main 始终刷新（失败不阻断——主目标仍由下面的解析决定）
git -C "${BMS_CLONE_DIR}" fetch --quiet --prune origin "+refs/heads/main:refs/remotes/origin/main" || true

latest_tag() {
    git ls-remote --quiet --refs --tags "${BMS_URL:-origin}" 'v*' 2>/dev/null \
        | sed -e 's#.*refs/tags/##' \
        | grep -E '^v[0-9]+([.][0-9]+)*$' \
        | sort -V \
        | tail -n 1
}

case "${BMS_REF}" in
    main | latest)
        RESOLVED="main"
        ;;
    latest-tag)
        RESOLVED="$(latest_tag)"
        if [ -z "${RESOLVED}" ]; then
            echo "[bms] 远端无 vX.Y.Z 标签，回落 main 头" >&2
            RESOLVED="main"
        fi
        ;;
    *)
        RESOLVED="${BMS_REF}"
        ;;
esac

if git -C "${BMS_CLONE_DIR}" fetch --quiet --prune origin \
    "+refs/heads/${RESOLVED}:refs/remotes/origin/${RESOLVED}" 2>/dev/null; then
    TARGET="refs/remotes/origin/${RESOLVED}"
elif git -C "${BMS_CLONE_DIR}" fetch --quiet --prune origin \
    "+refs/tags/${RESOLVED}:refs/tags/${RESOLVED}" 2>/dev/null; then
    TARGET="refs/tags/${RESOLVED}"
else
    # 提交 SHA：先试按裸 ref 取（GitLab 对可达 SHA 直接放行）；不支持时拉全部分支 / 标签再按对象检出
    if git -C "${BMS_CLONE_DIR}" fetch --quiet origin "${RESOLVED}" 2>/dev/null; then
        TARGET="FETCH_HEAD"
    else
        git -C "${BMS_CLONE_DIR}" fetch --quiet --prune origin \
            "+refs/heads/*:refs/remotes/origin/*" "+refs/tags/*:refs/tags/*" || {
            echo "[bms] 拉取失败：${RESOLVED}（分支 / tag / SHA 均未命中）" >&2
            exit 1
        }
        if ! git -C "${BMS_CLONE_DIR}" cat-file -e "${RESOLVED}^{commit}" 2>/dev/null; then
            echo "[bms] 提交不存在于基座仓：${RESOLVED}" >&2
            exit 1
        fi
        TARGET="${RESOLVED}"
    fi
fi

git -C "${BMS_CLONE_DIR}" checkout --quiet --force --detach "${TARGET}"

if [ ! -d "${BMS_CLONE_DIR}/backend/libs/bms_core" ]; then
    echo "[bms] 基座源码缺失：${BMS_CLONE_DIR}/backend/libs/bms_core（ref 异常）" >&2
    exit 1
fi
echo "[bms] 克隆就绪：${BMS_CLONE_DIR} @ BMS_REF=${BMS_REF}（${RESOLVED} → $(git -C "${BMS_CLONE_DIR}" rev-parse --short HEAD)）"
