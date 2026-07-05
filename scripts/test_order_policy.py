"""OrderPolicy 单元测试。"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from portfolio.order_policy import (
    DataState,
    decide_entry_order,
    decide_close_order,
    marketable_limit,
    data_state_from_env,
    is_delayed_market_data,
)


def test_realtime_market():
    d = decide_entry_order(data_state=DataState.REALTIME, side="BUY", ref_price=100.0)
    assert not d.use_limit
    assert d.limit_price is None


def test_delayed_limit():
    d = decide_entry_order(data_state=DataState.DELAYED, side="BUY", ref_price=100.0, slippage_pct=0.5)
    assert d.use_limit
    assert d.limit_price == marketable_limit(100.0, "BUY", 0.5)


def test_sell_slippage_down():
    d = decide_entry_order(data_state=DataState.DELAYED, side="SELL", ref_price=200.0, slippage_pct=1.0)
    assert d.limit_price == 198.0


def test_invalid_price_raises():
    try:
        decide_entry_order(data_state=DataState.REALTIME, side="BUY", ref_price=0)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_env_delayed():
    os.environ["MARKET_DATA_DELAYED"] = "1"
    try:
        assert data_state_from_env() == DataState.DELAYED
        assert is_delayed_market_data()
    finally:
        os.environ.pop("MARKET_DATA_DELAYED", None)


def test_market_data_mode():
    os.environ["MARKET_DATA_MODE"] = "delayed_frozen"
    try:
        assert is_delayed_market_data()
        assert data_state_from_env() == DataState.DELAYED
    finally:
        os.environ.pop("MARKET_DATA_MODE", None)


def test_close_same_as_entry():
    d = decide_close_order(data_state=DataState.DELAYED, side="SELL", ref_price=50.0)
    assert d.use_limit
    assert d.limit_price == marketable_limit(50.0, "SELL", 0.5)


if __name__ == "__main__":
    test_realtime_market()
    test_delayed_limit()
    test_sell_slippage_down()
    test_invalid_price_raises()
    test_env_delayed()
    test_market_data_mode()
    test_close_same_as_entry()
    print("test_order_policy: OK")
