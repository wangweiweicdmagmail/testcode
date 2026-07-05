"""Redis settings:{sym} 读取 — 上层路由与引擎共用。"""
from __future__ import annotations

import json
from typing import Any, Optional


def read_symbol_settings(redis, sym: str) -> dict[str, Any]:
    if not redis:
        return {}
    try:
        raw = redis.get(f"settings:{sym.upper()}")
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def is_auto_managed(redis, sym: str) -> bool:
    """ExitManager 语义：auto 接管时不走 manual trail/EOD。"""
    s = read_symbol_settings(redis, sym)
    return bool(s.get("auto_strategy") or s.get("auto_observe"))


def uses_auto_pm(redis, sym: str) -> bool:
    """持仓由 AutoPM 管理（实盘 auto 或开盘突破 live）。"""
    s = read_symbol_settings(redis, sym)
    return bool(s.get("auto_strategy") or s.get("opening_breakout_live"))
