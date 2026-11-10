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
两个库 URL 模板；`PLATFORM_NETWORK` 按平台实际网络名填写（缺省值仅作参照）。

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

# 状态（healthy 表示镜像内 /healthz 通过；就绪另看 /readyz）
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

## 8. 排障速查

| 现象 | 先看 | 处置 |
| --- | --- | --- |
| `config` 报缺变量 | 报错里的变量名 | 按 `.env.example` 补齐（`:?` 是刻意设计，不做兜底） |
| `up` 报 network not found | `PLATFORM_NETWORK` | 先确认平台网络已创建，再改 `.env` |
| 容器 `unhealthy` | `logs` 中 `/healthz` 相关 | 进程未起或端口未监听 |
| 容器 healthy 但 `/readyz` 503 | 宿主目录属主 / 磁盘水位 | `chown -R 10001:10001`；清盘或调保留期（水位拒绝阈值见 `MSG_STORAGE_REJECT_RATIO`） |
| 停机 45s 后仍有 SIGKILL | 日志收尾行 | 一并调 `stop_grace_period` 与 `[server].graceful_timeout_seconds`（只改编排一处） |
| 启动即退、日志 `CatalogError` | 声明目录是否缺件 | 镜像必须带 `deploy/registry`（本镜像已 COPY，见 Dockerfile） |
