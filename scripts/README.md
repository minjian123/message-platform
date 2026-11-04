# scripts（8B_00 占位）

开发 / 预检 / CI 脚本入口；8B_01 起按 mdm 同构补：
- `tools/preflight/`（推送入口包装）
- `tools/ci/`（bms / engkit 克隆与镜像构建）
- `tools/dev/`（本机裸跑，由 bms 本地全套脚本装载 `deploy/local_stack.toml`）

已提供：

- `bench/http2_vs_http1.py`：HTTP/2 传输基准（HTTP/1.1+JSON vs h2c+JSON，判据见任务 03-1）。
  用法：`python scripts/bench/http2_vs_http1.py --base-url http://127.0.0.1:8000 --ensure-topic`。

8B 后续按 mdm 同构补：`tools/preflight/`（推送入口包装）、`tools/ci/`（bms / engkit 克隆与镜像构建）、
`tools/dev/`（本机裸跑，由 bms 本地全套脚本装载 `deploy/local_stack.toml`）。
