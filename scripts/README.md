# scripts（8B_00 占位）

开发 / 预检 / CI 脚本入口；8B_01 起按 mdm 同构补：
- `tools/preflight/`（推送入口包装）
- `tools/ci/`（bms / engkit 克隆与镜像构建）
- `tools/dev/`（本机裸跑，由 bms 本地全套脚本装载 `deploy/local_stack.toml`）

当前设计阶段不提供脚本。
