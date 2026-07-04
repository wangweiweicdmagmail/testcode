#!/usr/bin/env python3
"""Alpha MCP 本地自检（对话测试前跑一遍）。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mcp.server.fastmcp import FastMCP  # noqa: F401 — 验证官方包

from nautilus_mcp import redis_io


def main() -> int:
    ok = True
    print("=== Alpha MCP 自检 ===\n")

    try:
        r = redis_io.get_redis()
        print("[OK] Redis 连接")
    except Exception as e:
        print(f"[FAIL] Redis: {e}")
        return 1

    health = redis_io.stack_health(r)
    print(f"[INFO] pending_proposals={health['pending_proposals']}")
    print(f"[INFO] engine_heartbeat={health['engine_heartbeat']!r}")
    print(f"[INFO] engine_heartbeat_age_s={health.get('engine_heartbeat_age_s')}")
    print(f"[INFO] engine_online={health.get('engine_online')}")

    if health.get("engine_online"):
        print("[OK] 引擎在线（心跳 ts 在阈值内）")
    elif health["engine_heartbeat"] is None:
        print("[WARN] 引擎未运行或未写 heartbeat — 触线测试需先 python main.py")
    else:
        print(f"[WARN] 心跳过旧 age={health.get('engine_heartbeat_age_s')}s")

    for sym in list(redis_io.DEFAULT_SYMBOLS)[:3]:
        active = redis_io.get_indicators_active(r, sym)
        m1 = redis_io.get_bars(r, "1m", sym, limit=1)
        if active:
            print(f"[OK] {sym} indicators:active ST={active.get('supertrend', {}).get('value')}")
        else:
            print(f"[WARN] {sym} 无 indicators:active（等 M5 收盘或启动引擎）")
        if m1:
            print(f"[OK] {sym} bars:1m 最新 close={m1[-1].get('close')} vwap={m1[-1].get('vwap')}")
        else:
            print(f"[WARN] {sym} 无 M1 bars")

    touches = redis_io.list_recent_touches(r, limit=5)
    print(f"[INFO] 最近触线 {len(touches)} 条")
    if touches:
        print(json.dumps(touches[0], ensure_ascii=False, indent=2))

    pending = redis_io.list_pending_proposals(r, limit=5)
    print(f"[INFO] pending proposals {len(pending)} 条")

    print("\n--- 对话测试步骤 ---")
    print("1. Cursor Settings → MCP → 刷新 alpha（绿点）")
    print("2. 新 Chat 只发: alpha")
    print("3. 期望: 调用 get_alpha_snapshot，无触线则回复 NO_OP")
    print("4. 有触线时: create_proposal + 摘要；/proposals.html 可见")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
