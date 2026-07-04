"""Alpha 信号层：纯逻辑，不下单。

MCP / 轻量脚本只 import 子模块（indicators、pullback_pricing）时不应拉 nautilus_trader。
重依赖通过 __getattr__ 延迟加载。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = [
    "BarContext",
    "IntentAction",
    "PositionContext",
    "SignalBarOutput",
    "SignalEngine",
    "SignalReject",
    "TradeIntent",
    "StDemaM5Config",
    "StDemaM5Engine",
    "PullbackCandidate",
    "scan_pullback_signals",
    "SIGNAL_VWAP",
    "SIGNAL_ST",
    "SIGNAL_DEMA20",
]

_LAZY = {
    "BarContext": ("signals.base", "BarContext"),
    "IntentAction": ("signals.base", "IntentAction"),
    "PositionContext": ("signals.base", "PositionContext"),
    "SignalBarOutput": ("signals.base", "SignalBarOutput"),
    "SignalEngine": ("signals.base", "SignalEngine"),
    "SignalReject": ("signals.base", "SignalReject"),
    "TradeIntent": ("signals.base", "TradeIntent"),
    "StDemaM5Config": ("signals.st_dema_m5", "StDemaM5Config"),
    "StDemaM5Engine": ("signals.st_dema_m5", "StDemaM5Engine"),
    "PullbackCandidate": ("signals.pullback_scanner", "PullbackCandidate"),
    "scan_pullback_signals": ("signals.pullback_scanner", "scan_pullback_signals"),
    "SIGNAL_VWAP": ("signals.pullback_scanner", "SIGNAL_VWAP"),
    "SIGNAL_ST": ("signals.pullback_scanner", "SIGNAL_ST"),
    "SIGNAL_DEMA20": ("signals.pullback_scanner", "SIGNAL_DEMA20"),
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    mod_path, attr = _LAZY[name]
    import importlib
    mod = importlib.import_module(mod_path)
    return getattr(mod, attr)


if TYPE_CHECKING:
    from signals.base import (
        BarContext,
        IntentAction,
        PositionContext,
        SignalBarOutput,
        SignalEngine,
        SignalReject,
        TradeIntent,
    )
    from signals.pullback_scanner import (
        SIGNAL_DEMA20,
        SIGNAL_ST,
        SIGNAL_VWAP,
        PullbackCandidate,
        scan_pullback_signals,
    )
    from signals.st_dema_m5 import StDemaM5Config, StDemaM5Engine
