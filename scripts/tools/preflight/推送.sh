#!/usr/bin/env bash
# 推送入口（Linux）：预检通过后自动推送
# 档位按待推提交自动选（全文档→快速档 / 含代码→定向档）；工作树脏即拒
# 用法: ./推送.sh [--full] [--remote origin] [--branch <分支>] [--jobs auto|N|off]
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")/../../.." || exit 1   # 跳到仓库根目录
exec python3 scripts/tools/preflight/推送.py "$@"
