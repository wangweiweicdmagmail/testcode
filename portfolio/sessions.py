"""
统一的"交易日"口径 — 所有日内风控键（熔断 / SOD / 冷却 / 日内次数）必须用
美东(ET)日历日，而非机器本地时钟。

背景：本项目部署机器常处于 UTC+8，若用 time.strftime('%Y%m%d')（本地时钟），
日期字符串会在北京时间 00:00 翻篇 = 美东约 11:00–12:00（盘中），导致：
  - risk:equity_sod 起始净值在盘中被重新锚定
  - risk:halt 熔断标志在盘中丢失（当日熔断被静默解除）
ET 日历日在 RTH(09:30–16:00) 全程稳定，翻篇发生在 ET 午夜（远离任何交易时段）。
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def et_session_date(fmt: str = "%Y%m%d") -> str:
    """当前美东日历日。用于所有日内风控 Redis 键。"""
    return datetime.now(tz=_ET).strftime(fmt)


def et_minute_now() -> int:
    """当前美东时间的"分钟数"（小时×60+分钟），用于判断是否已进入 RTH。"""
    now = datetime.now(tz=_ET)
    return now.hour * 60 + now.minute
