#!/usr/bin/env python3
"""止损方向校验单测 — stop_on_wrong_side。

确保 STOP_MARKET 不会挂在入场价的错误侧（即时触发 / 裸仓）。
对应 P0-1：审批延迟导致最新价穿过原止损线时，主校验与挂单前兜底均依赖此函数。
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from nautilus_trader.model.enums import OrderSide

from portfolio.risk_gate import stop_on_wrong_side


def test_wrong_side_long():
    # 做多：止损 98 在最新价 97 上方 → 错误侧（买入即被套，STOP 即时触发）
    assert stop_on_wrong_side(OrderSide.BUY, 97.0, 98.0) is True


def test_wrong_side_short():
    # 做空：止损 97 在最新价 98 下方 → 错误侧
    assert stop_on_wrong_side(OrderSide.SELL, 98.0, 97.0) is True


def test_correct_side_long():
    # 做多：止损 98 在最新价 100 下方 → 正确侧
    assert stop_on_wrong_side(OrderSide.BUY, 100.0, 98.0) is False


def test_correct_side_short():
    # 做空：止损 100 在最新价 98 上方 → 正确侧
    assert stop_on_wrong_side(OrderSide.SELL, 98.0, 100.0) is False


def test_equal_counts_as_wrong():
    # close==stop 视为错误侧（IBKR STOP_MARKET 触发条件含等于，会即时成交）
    assert stop_on_wrong_side(OrderSide.BUY, 100.0, 100.0) is True
    assert stop_on_wrong_side(OrderSide.SELL, 100.0, 100.0) is True


def test_invalid_prices():
    # 无效价格（<=0）视为不安全
    assert stop_on_wrong_side(OrderSide.BUY, 0.0, 98.0) is True
    assert stop_on_wrong_side(OrderSide.BUY, 100.0, 0.0) is True
    assert stop_on_wrong_side(OrderSide.BUY, -1.0, 98.0) is True


def test_accepts_string_side():
    # 字符串 side 与 OrderSide 等价（proposal 里 side 是字符串）
    assert stop_on_wrong_side("LONG", 97.0, 98.0) is True
    assert stop_on_wrong_side("SHORT", 98.0, 97.0) is True
    assert stop_on_wrong_side("long", 100.0, 98.0) is False   # 大小写不敏感
    assert stop_on_wrong_side("BUY", 100.0, 98.0) is False
    assert stop_on_wrong_side("SELL", 98.0, 100.0) is False


def test_unknown_side_is_unsafe():
    # 未知方向视为不安全（保守拒绝）
    assert stop_on_wrong_side("SIDEWAYS", 100.0, 98.0) is True


if __name__ == "__main__":
    fns = [
        test_wrong_side_long,
        test_wrong_side_short,
        test_correct_side_long,
        test_correct_side_short,
        test_equal_counts_as_wrong,
        test_invalid_prices,
        test_accepts_string_side,
        test_unknown_side_is_unsafe,
    ]
    for fn in fns:
        fn()
        print(f"✅ {fn.__name__}")
    print(f"\nOK: {len(fns)} stop_on_wrong_side tests passed")
