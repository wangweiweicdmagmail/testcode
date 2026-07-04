#!/usr/bin/env python3
"""Alpha 扫描过滤单元测试。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from approval.alpha_scan import side_matches_m5_trend


def test_trend_filter() -> None:
    assert side_matches_m5_trend("LONG", 1)
    assert side_matches_m5_trend("SHORT", -1)
    assert not side_matches_m5_trend("LONG", -1)
    assert not side_matches_m5_trend("SHORT", 1)
    assert not side_matches_m5_trend("LONG", 0)


if __name__ == "__main__":
    test_trend_filter()
    print("OK: alpha_scan filters")
