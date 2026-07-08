#!/usr/bin/env python3
"""TRADING_ENV 护栏单测。"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from portfolio.trading_env import (
    allow_fixed_qty,
    live_orders_allowed,
    production_safety_warnings,
    trading_env,
)


def run() -> int:
    fails = 0

    os.environ.pop("TRADING_ENV", None)
    if trading_env() != "paper":
        print("❌ 默认应为 paper")
        fails += 1
    else:
        print("✅ 默认 TRADING_ENV=paper")

    if live_orders_allowed():
        print("❌ paper 模式不应允许实盘")
        fails += 1
    else:
        print("✅ paper 禁止 live_orders")

    os.environ["TRADING_ENV"] = "live"
    if not live_orders_allowed():
        print("❌ live 模式应允许实盘")
        fails += 1
    else:
        print("✅ live 允许 live_orders")

    os.environ["TRADING_ENV"] = "bogus"
    if trading_env() != "paper":
        print("❌ 非法值应回退 paper")
        fails += 1
    else:
        print("✅ 非法值回退 paper")

    if allow_fixed_qty(10):
        print("❌ fixed_qty=10 无 ALLOW 应拒绝")
        fails += 1
    else:
        print("✅ fixed_qty 需 ALLOW_FIXED_QTY")
    os.environ["ALLOW_FIXED_QTY"] = "1"
    if not allow_fixed_qty(10):
        print("❌ ALLOW_FIXED_QTY=1 后应允许")
        fails += 1
    else:
        print("✅ ALLOW_FIXED_QTY 生效")

    os.environ["TRADING_ENV"] = "live"
    os.environ["NAUTILUS_BIND_HOST"] = "127.0.0.1"
    os.environ.pop("ORDER_GATEWAY_SECRET", None)
    os.environ.pop("NAUTILUS_API_SECRET", None)
    warns = production_safety_warnings()
    if len(warns) < 1 or not any("ORDER_GATEWAY_SECRET" in w for w in warns):
        print(f"❌ live 缺 ORDER_GATEWAY 应告警，got {warns}")
        fails += 1
    else:
        print("✅ production_safety_warnings 检测缺 ORDER_GATEWAY")

    os.environ["NAUTILUS_BIND_HOST"] = "0.0.0.0"
    warns_ext = production_safety_warnings()
    if not any("NAUTILUS_API_SECRET" in w for w in warns_ext):
        print(f"❌ 对外暴露时缺 NAUTILUS_API_SECRET 应告警，got {warns_ext}")
        fails += 1
    else:
        print("✅ production_safety_warnings 对外暴露检测 NAUTILUS_API_SECRET")

    total = 8
    print(f"\n结果: {total - fails}/{total} 通过" if fails == 0 else f"\n结果: {total - fails}/{total} 通过, {fails} 失败")
    return fails


if __name__ == "__main__":
    raise SystemExit(run())
