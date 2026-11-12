# msg 容器编排与部署说明

> 服务名 `msg`、容器内 8000、仅内部网络经平台网关 `/api/msg/v1/msg/...` 访问；
> 镜像与卷口径见 `../Dockerfile` 与 `msg.yml`，此处只给可复现的操作序列。

## 1. 前置

| 项 | 要求 |
| --- | --- |
| 工作区 | `msg/` 与 `bms/` **平级并置**（镜像构建上下文是两者父目录） |
| Docker | 支持 BuildKit 的 Docker（`docker compose` 子命令可用） |
| 平台基础设施 | 已由 bms 部署就绪（网络 / 网关 / Redis / MySQL / 可观测后端），msg 不自建 |
| 凭据 | `deploy/.env`（由 `.env.example` 复制填写，**不入库**） |

`.env` 必填项：`MSG_REGISTRY_IMAGE_PREFIX`、`MSG_TAG`、`MSG_DATA_HOST_DIR`、`MSG_OFFSETS_HOST_DIR`、
平台 / 租户 / 归档三个库 URL、Redis URL 与 OTLP 端点（均由 `:?` 强制必填）；`PLATFORM_NETWORK` 按平台
实际网络名填写。密钥、证书路径与存储水位旋钮留空即沿用缺省，切换环境只改本文件。

**凭据纪律**：六类敏感项（库连接串与密码、Redis URL、OTLP 端点、服务令牌密钥、用户令牌密钥、
TLS 证书路径）只从环境变量读取；仓内只保留 `.env.example` 的空占位，真实值只落部署态 `.env` 或 CI
Secret 变量，不入配置文件、镜像与公开文档。`deploy/.env` 与 `backend/.env` 都已忽略。

## 2. 准备宿主目录（首次一次）

消息日志与位点**各一独立卷**，须放在数据盘（不与系统盘同盘），目录属主必须是镜像内运行用户
`uid/gid 10001`、权限 `0750`：

```bash
install -d -o 10001 -g 10001 -m 0750 <数据盘路径>/msg/data
install -d -o 10001 -g 10001 -m 0750 <数据盘路径>/msg/offsets
```

已存在数据目录时用 `chown -R 10001:10001 <dir>` 纠正属主。属主不对时容器**能起**，但 `/readyz`
返回 503（存储写探测失败），不是启动失败——先查这两个目录。

## 3. 构建镜像

在**工作区根**执行（`<registry>` / `<tag>` 按发布台账，禁 `latest`）：

```bash
docker build --provenance=false -t <registry>/<项目路径>/msg:<tag> -f msg/deploy/Dockerfile .
```

- `--provenance=false`：BuildKit 默认 attestation 清单会被 Registry 拒绝（`blob unknown`）；
- 镜像内只含后端工程、共享基座源码与 `msg/deploy/{registry,boundaries,contracts}`；前端、其他产品仓、
  文档、`.env` 均由 `msg/deploy/Dockerfile.dockerignore` 排除；
- 依赖以 `uv.lock` 锁定（`--frozen`）：lock 与 `pyproject.toml` 不一致时构建直接失败，先在本地
  `uv lock` 后提交，**不要**在 Dockerfile 里去掉 `--frozen`。

## 4. 启动 / 停止 / 重载

在 `msg/deploy/` 目录执行：

```bash
# 起（先校验再起；缺变量会在这里就报错）
docker compose -f compose/msg.yml --env-file .env config >/dev/null && \
docker compose -f compose/msg.yml --env-file .env up -d

# 状态（healthy 表示镜像内 /readyz 通过 = 能读写；就绪判据与网关摘流同源）
docker compose -f compose/msg.yml ps

# 换镜像版本后重建（一次只替换一个实例；首期单实例即停旧起新）
docker compose -f compose/msg.yml --env-file .env up -d

# 停（45s 宽限内按 GOAWAY / drain 收尾；超时才 SIGKILL）
docker compose -f compose/msg.yml stop

# 重启（卷与数据不动）
docker compose -f compose/msg.yml restart msg
```

**禁用**：`docker compose down -v`（会删卷，等于丢消息日志与位点）；`docker kill`（跳过 drain）。

## 5. 日志与排查

```bash
# 跟随日志（JSON / console 由 BMS_ENV 决定）
docker compose -f compose/msg.yml logs -f --tail=200 msg

# 进程内探针（不经网关，直连容器）
docker compose -f compose/msg.yml exec msg python -c \
  "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8000/healthz',timeout=3).status)"
docker compose -f compose/msg.yml exec msg python -c \
  "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8000/readyz',timeout=3).status)"

# 数据根内部结构（容器内）
docker compose -f compose/msg.yml exec msg ls -R /var/lib/msg | head -50
```

## 6. 卷检查

```bash
# 卷挂载点与来源（确认两个宿主目录分别挂到 data / offsets）
docker inspect msg --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'

# 宿主侧占用与属主（应为 10001:10001）
du -sh <数据盘路径>/msg/data <数据盘路径>/msg/offsets
stat -c '%U:%G %a %n' <数据盘路径>/msg/data <数据盘路径>/msg/offsets

# 容器内属主核对
docker compose -f compose/msg.yml exec msg id        # uid=10001(msg)
```

## 7. 重启后自检

```bash
docker compose -f compose/msg.yml restart msg
docker compose -f compose/msg.yml exec msg ls /var/lib/msg/data /var/lib/msg/offsets
```

文件仍在即卷正确；`/readyz` 200 表示恢复完成并已可写。三样持久化中，元数据库随平台库（不在本编排）。

## 8. 部署自检

在**构建上下文的本机仓**（`backend/` 目录，需仓内虚拟环境）执行——判据是声明 / 契约 / 配置 / 镜像 / 编排文本，
镜像内不含 `ops/`（运行镜像只装服务与基座），故不在容器内跑：

```bash
# 声明装载 / 七段齐备 / 唯一性 / 服务行同值 / 路由口径 / 探针接线（退出码 0 通过、1 违规）
python -m ops.check_deployment

# 四层配置 / 六类敏感项 / 凭据形态 / 两份 .env 纪律 / 生产禁用项（C1~C6，退出码 0 通过、1 违规）
python -m ops.check_credentials

# 平台侧识别情况另查：产品 / 服务行 / 产品路由登记行与网关生成件（见部署记录）
```

## 9. 排障速查

| 现象 | 先看 | 处置 |
| --- | --- | --- |
| `config` 报缺变量 | 报错里的变量名 | 按 `.env.example` 补齐（`:?` 是刻意设计，不做兜底） |
| `up` 报 network not found | `PLATFORM_NETWORK` | 先确认平台网络已创建，再改 `.env` |
| 容器 `unhealthy` | `docker inspect msg --format '{{json .State.Health}}'` 与 `/readyz` 明细 | 按 `checks` 失败项处置：目录属主 `chown -R 10001:10001`、磁盘水位清理或调保留期、元数据库连通性 |
| 容器长期 `starting` | 启动期恢复日志 | 恢复未完成时 `/readyz` 503 属预期（不提前接流量）；确为卡死再查恢复扫描 |
| 业务写返回 507 但容器 healthy | `msg_storage_rejected_total` 与水位日志 | 水位拒绝只拦发布、不摘实例：清盘或扩容后重试 |
| 停机 45s 后仍有 SIGKILL | 日志收尾行 | 一并调 `stop_grace_period` 与 `[server].graceful_timeout_seconds`（只改编排一处） |
| 启动即退、日志 `CatalogError` | 声明目录是否缺件 | 镜像必须带 `deploy/registry`（本镜像已 COPY，见 Dockerfile） |

## 8. 多实例与集群演练（02-8）

- **单实例默认不变**：`docker compose -f compose/msg.yml up -d` 只起 `msg`，复制关闭。
- **三实例**：`docker compose -f compose/msg.yml --profile cluster up -d msg-1 msg-2 msg-3`（显式列服务，避免连带启动默认 `msg`）；每实例独立 `data` / `offsets` 卷，容器内 8000 + 8001（内部复制仅平台网络）。
- **生产网关**：叠加 `-f compose/msg-cluster.gateway.yml` 给三实例加别名 `msg`，网关上游 `msg:8000` 口径不变；drill 不启用该层。
- **回退单实例**：先 `ops.rebalance` 收敛副本集 / 切主，再停三实例、`up -d` 起 `msg`；分配结果留在 `msg_partition.replicas`。
- **集群演练**：见 `deploy/drill/README.md`「多实例场景」节；记录落工作区根 `tmp/msg/cluster-drill/`，不污染平台数据。
