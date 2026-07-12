#!/usr/bin/env bash
# 双击启动入口：在 Terminal 里跑 scripts/launch.sh（自启 Redis/前端 + 8 步音频监控）
cd "$(dirname "$0")" || exit 1
bash scripts/launch.sh "$@"
