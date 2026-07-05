"""portfolio.auto_settings 单元测试。"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from portfolio.auto_settings import is_auto_managed, uses_auto_pm


class _FakeRedis:
    def __init__(self, data: dict):
        self._data = data

    def get(self, key: str):
        return self._data.get(key)


def test_uses_auto_pm_strategy():
    r = _FakeRedis({"settings:NVDA": json.dumps({"auto_strategy": True})})
    assert uses_auto_pm(r, "NVDA")
    assert is_auto_managed(r, "NVDA")


def test_uses_auto_pm_opening_breakout():
    r = _FakeRedis({"settings:QQQ": json.dumps({"opening_breakout_live": True})})
    assert uses_auto_pm(r, "QQQ")


def test_observe_not_auto_pm_close():
    r = _FakeRedis({"settings:SPY": json.dumps({"auto_observe": True})})
    assert is_auto_managed(r, "SPY")
    assert not uses_auto_pm(r, "SPY")


def test_off():
    r = _FakeRedis({})
    assert not uses_auto_pm(r, "AAPL")
    assert not is_auto_managed(r, "AAPL")


if __name__ == "__main__":
    test_uses_auto_pm_strategy()
    test_uses_auto_pm_opening_breakout()
    test_observe_not_auto_pm_close()
    test_off()
    print("test_auto_settings: OK")
