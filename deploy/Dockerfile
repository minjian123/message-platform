# msg 服务运行镜像（服务标识 `msg`；服务工程目录 `services/msg`；容器内包名 `bms_msg`）
#
# 构建（在**工作区根**执行——`msg/` 与 `bms/` 平级并置）：
#   docker build --provenance=false -t <registry>/<项目路径>/msg:<tag> -f msg/deploy/Dockerfile .
#   （--provenance=false：BuildKit 默认 attestation 清单推送会被 GitLab Registry 拒绝，报 blob unknown）
# 运行（由 `deploy/compose/msg.yml` 拉起；单跑见该文件头部命令）：
#   docker run --rm -e BMS_ENV=dev -v <宿主数据目录>:/var/lib/msg/data \
#     -v <宿主位点目录>:/var/lib/msg/offsets <image>
#
# 口径：
# - 构建上下文是**工作区根**：`bms-core` 在 `msg/backend/pyproject.toml` 里是**路径依赖**
#   （`../../bms/backend/libs/bms_core`），镜像内须让该相对路径同样成立 → 基座源码放 `/opt/bms/...`，
#   本仓后端放 `/opt/msg/backend`，两者父级布局与工作区一致；
#   上下文排除规则见**本文件同级** `Dockerfile.dockerignore`（Dockerfile 专用 ignore，随仓库走；
#   BuildKit 优先级高于上下文根的 `.dockerignore`）；
# - **多阶段**：builder 只负责按 `uv.lock` 装依赖（`--frozen`），runtime 只带 venv + 源码 + 声明目录，
#   构建期工具链（uv / pip 缓存 / 编译中间物）不进最终层；
# - `--no-dev`：dev 组含跨仓路径依赖 `engkit`（`../../engkit`），镜像内无该源码，运行镜像也不需要开发件；
#   依赖重装后若 lock 漂移，`--frozen` 会直接构建失败——刷新 lock 在本地做，不在本文件里去 `--frozen`；
# - 工作目录 `/opt/msg/backend`：`services` 产品清单模块、`alembic.ini` 与 `config*.toml` 均相对该根定位；
#   声明目录相对该根的父级（`/opt/msg/deploy/...`）；
# - 工程根显式覆盖 `BMS_BACKEND_ROOT`：基座按「上溯 `alembic.ini`」定位工程根，基座源码在 `/opt/bms`
#   时会错位，故显式声明（取值无效 fail-closed）；
# - **非 root**：镜像内建 `msg`（uid/gid 10001），数据卷挂载点 `/var/lib/msg/{data,offsets}` 属主为该用户、
#   权限 0750（架构 08 §4「目录权限仅服务账号可读写」）；宿主绑定目录须部署时按同一 uid/gid 准备，
#   属主不匹配表现为 `/readyz` 503（存储写探测失败），不是启动失败；
# - 数据根固定 `MSG_DATA_DIR=/var/lib/msg`，位点仍是 `data/` 的平级子目录（代码与目录布局零改动），
#   两个子目录各挂一卷（`deploy/compose/msg.yml`）；
# - 镜像内**零凭据**：连接串 / 密码 / 令牌密钥一律运行时经 `BMS_` 环境变量注入，`deploy/.env` 不进上下文；
# - 启动入口 `python -m bms_msg`（服务包名 = `bms` 前缀 + 服务标识）。

# ---------- builder：只装依赖 ----------
FROM python:3.14-slim AS builder

ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir uv

# 共享基座源码（路径依赖目标）：放到 `/opt/bms/...`，使 pyproject 的相对路径在容器内成立
COPY bms/backend/libs/bms_core /opt/bms/backend/libs/bms_core

WORKDIR /opt/msg/backend
# 工作区元数据：uv.lock 与各成员 pyproject（工作区解析需要；成员源码后续按目标服务 COPY）；
# alembic.ini 供共享基座定位工程根；config*.toml 为配置基线 / 环境分层（密钥不入配置文件）
COPY msg/backend/pyproject.toml msg/backend/uv.lock msg/backend/alembic.ini msg/backend/config.toml msg/backend/config.*.toml ./
COPY msg/backend/services/msg/pyproject.toml services/msg/pyproject.toml
# 第三方依赖层（不装工作区成员；锁文件零漂移；共享基座按路径依赖就地安装）
RUN uv sync --frozen --no-dev --no-install-workspace --package bms-msg && rm -rf /root/.cache

# 工作区成员源码（产品服务清单模块 + 目标服务）
# `services/*.py` 是**产品自持清单**（服务目录 / 表归属 / 表单标识），启动期表归属注入与
# 平台装载校验都依赖它——只拷 src 会 MissingManifest。
COPY msg/backend/services/*.py services/
COPY msg/backend/services/msg/src services/msg/src
RUN uv sync --frozen --no-dev --package bms-msg && rm -rf /root/.cache

# ---------- runtime：只带运行所需 ----------
FROM python:3.14-slim AS runtime

ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    SERVICE=msg \
    BMS_BACKEND_ROOT=/opt/msg/backend \
    MSG_DATA_DIR=/var/lib/msg \
    PYTHONUNBUFFERED=1

# 系统包安全更新（镜像扫描 CRITICAL/HIGH 阻断发版）+ apt 换清华源（与平台侧镜像同口径）
RUN sed -i 's|deb.debian.org|mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

# 运行用户：固定 uid/gid 10001（宿主绑定目录须按同值准备属主，见文件头）
RUN groupadd --gid 10001 msg \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin msg

WORKDIR /opt/msg/backend
COPY --from=builder /opt/bms /opt/bms
COPY --from=builder /opt/msg /opt/msg

# 迁移能力保留：分链迁移脚本（当前无脚本，按新基线重建时使用）
COPY msg/backend/alembic ./alembic

# 产品自持声明 / 域布局 / 公开契约（相对工作目录父级：/opt/msg/deploy/...）
# 装配期装载清单时据此定位对端契约与域布局；缺件即装载校验失败（拒启），不静默跳过
COPY msg/deploy/registry ../deploy/registry
COPY msg/deploy/boundaries ../deploy/boundaries
COPY msg/deploy/contracts ../deploy/contracts

# 数据卷挂载点：属主固定、权限 0750（宿主绑定目录按同值准备；镜像内建目录供命名卷首挂继承）
RUN mkdir -p /var/lib/msg/data /var/lib/msg/offsets \
 && chown -R 10001:10001 /var/lib/msg \
 && chmod 0750 /var/lib/msg /var/lib/msg/data /var/lib/msg/offsets

# 运行时移除 pip（base 镜像自带；其 vendored 依赖会被镜像扫描按包计洞，运行时不需要）
RUN python -m pip uninstall -y pip

ENV PATH=/opt/msg/backend/.venv/bin:$PATH
USER msg
EXPOSE 8000
# 健康检查判据 = 就绪（`/readyz`，与网关摘流同一判据，任务 07-1-2）：
# 存活由进程是否退出表达（Docker 只在进程退出时按 restart 策略重启，**不因 unhealthy 重启**），
# 把判据放在就绪侧，`docker ps` 的 Health.Status 才与「能不能服务」一致。
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/readyz', timeout=2)"]
CMD ["python", "-m", "bms_msg"]
