#!/usr/bin/env bash
# source scripts/env.sh — 设置本仓库环境变量
_NAUTILUS_ENV_SH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export NAUTILUS_ROOT="$_NAUTILUS_ENV_SH"
export NAUTILUS_API_BASE="${NAUTILUS_API_BASE:-http://localhost:3000}"
export NAUTILUS_ENGINE_BASE="${NAUTILUS_ENGINE_BASE:-http://localhost:8888}"

# 加载 .env（TRADING_ENV 以 .env 为准）
_env_file="$_NAUTILUS_ENV_SH/.env"
_ENV_ALWAYS=(TRADING_ENV AUTO_STRATEGY_MODE)
if [[ -f "$_env_file" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" || "$line" != *"="* ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    val="${val%\"}"; val="${val#\"}"
    val="${val%\'}"; val="${val#\'}"
    [[ -z "$key" ]] && continue
    _force=0
    for _k in "${_ENV_ALWAYS[@]}"; do [[ "$key" == "$_k" ]] && _force=1 && break; done
    if [[ "$_force" == 1 || -z "${!key:-}" ]]; then
      export "$key=$val"
    fi
  done < "$_env_file"
fi
unset _NAUTILUS_ENV_SH _env_file line key val _force _k _ENV_ALWAYS
