"""IBKR 行情类型 — 与 OrderPolicy 共用单一配置源。"""
from __future__ import annotations

import os

from portfolio.order_policy import is_delayed_market_data


def ib_market_data_type():
    """Nautilus InteractiveBrokersDataClientConfig.market_data_type"""
    from nautilus_trader.adapters.interactive_brokers.common import IBMarketDataTypeEnum

    if not is_delayed_market_data():
        return IBMarketDataTypeEnum.REALTIME
    mode = os.environ.get("MARKET_DATA_MODE", "").strip().lower()
    if mode == "delayed":
        return IBMarketDataTypeEnum.DELAYED
    return IBMarketDataTypeEnum.DELAYED_FROZEN
