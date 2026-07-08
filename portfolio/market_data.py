"""IBKR 行情类型 — 与 OrderPolicy 共用单一配置源。"""
from __future__ import annotations

import os

from portfolio.order_policy import is_delayed_market_data


def _ib_market_data_enum():
    """Nautilus 1.221 从 ibapi 取枚举；新版可能从 adapters.common 导出。"""
    try:
        from nautilus_trader.adapters.interactive_brokers.common import IBMarketDataTypeEnum

        return IBMarketDataTypeEnum
    except ImportError:
        from ibapi.common import MarketDataTypeEnum as IBMarketDataTypeEnum

        return IBMarketDataTypeEnum


def ib_market_data_type():
    """Nautilus InteractiveBrokersDataClientConfig.market_data_type"""
    IBMarketDataTypeEnum = _ib_market_data_enum()

    if not is_delayed_market_data():
        return IBMarketDataTypeEnum.REALTIME
    mode = os.environ.get("MARKET_DATA_MODE", "").strip().lower()
    if mode == "delayed":
        return IBMarketDataTypeEnum.DELAYED
    return IBMarketDataTypeEnum.DELAYED_FROZEN
