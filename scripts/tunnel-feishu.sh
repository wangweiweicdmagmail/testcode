#!/usr/bin/env bash
# 为飞书事件订阅暴露本地 :3000（需安装 cloudflared）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${NAUTILUS_PORT:-3000}"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "未安装 cloudflared。macOS: brew install cloudflared"
  exit 1
fi

echo "将公网 URL 填到飞书开放平台 → 事件订阅:"
echo "  https://<随机域名>.trycloudflare.com/api/feishu/webhook"
echo ""
echo "确保已运行: bash scripts/start-stack.sh"
echo ""

exec cloudflared tunnel --url "http://127.0.0.1:$PORT"
