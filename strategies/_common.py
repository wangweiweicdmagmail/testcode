"""周期策略共享基础：ET 时区工具、Redis 去重写入、常量。

从老 strategy.py 的 staticmethod 与重复的 Redis 写入逻辑抽取为模块级
工具（设计原则 #1：复用，不重复造）。M1IndicatorStrategy / M5IndicatorStrategy
共用，保证两套周期的时间判定与 Redis 写入口径完全一致。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ── Redis 配置 ────────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
MAX_BARS = 500   # Redis 每个 key 保留最大根数

# ── ET 时段常量（秒；图表展示盘前最后 60 分钟 + RTH）─────────────────────
PREMARKET_CHART_START_ET_SEC = 8 * 3600 + 30 * 60  # 08:30 ET
RTH_OPEN_ET_SEC = 9 * 3600 + 30 * 60               # 09:30 ET
RTH_CLOSE_ET_SEC = 16 * 3600                        # 16:00 ET

_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc


# ── ET 时区工具（et_fake_utc：时分秒直接对应 ET 本地，无需再转换）─────────
def et_fake_utc(ts_ns: int) -> int:
    """纳秒级 UTC 时间戳 → ET fake-UTC 秒。"""
    ts_utc = ts_ns // 1_000_000_000
    et_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(_ET)
    return ts_utc + int(et_dt.utcoffset().total_seconds())


def is_rth(et_fake_utc: int) -> bool:
    """正式交易时段 [09:30, 16:00) ET。"""
    dt = datetime.fromtimestamp(et_fake_utc, tz=_UTC)
    m = dt.hour * 60 + dt.minute
    return (9 * 60 + 30) <= m < (16 * 60)


def is_premarket_chart_et(et_fake_utc: int) -> bool:
    """盘前图表窗口 [08:30, 09:30) ET。"""
    if is_rth(et_fake_utc):
        return False
    dt = datetime.fromtimestamp(et_fake_utc, tz=_UTC)
    sec = dt.hour * 3600 + dt.minute * 60 + dt.second
    return PREMARKET_CHART_START_ET_SEC <= sec < RTH_OPEN_ET_SEC


def is_premarket_et(et_fake_utc: int) -> bool:
    """盘前时段（< 09:30 ET），与 is_rth 互斥。"""
    if is_rth(et_fake_utc):
        return False
    dt = datetime.fromtimestamp(et_fake_utc, tz=_UTC)
    return dt.hour * 60 + dt.minute < 9 * 60 + 30


def is_chart_et(et_fake_utc: int) -> bool:
    """应写入 Redis / 前端图表的时段（盘前图表 + RTH）。"""
    return is_rth(et_fake_utc) or is_premarket_chart_et(et_fake_utc)


def session_date_from_et(et_fake_utc: int) -> str:
    """ET fake-UTC → 'YYYY-MM-DD'（用于 Session VWAP 日切 reset 等）。"""
    return datetime.fromtimestamp(et_fake_utc, tz=_UTC).strftime("%Y-%m-%d")


def bucket5m(et_fake_utc: int) -> int:
    """ET fake-UTC → M5 桶起始秒。"""
    return int(et_fake_utc) - (int(et_fake_utc) % 300)


# ── Redis 去重写入辅助 ────────────────────────────────────────────────────
def dedup_rpush_bar(redis, key: str, bar_dict: dict, max_bars: int = MAX_BARS) -> bool:
    """去重写入 Redis List：与最后一根时间戳相同则替换，否则追加并裁剪。

    返回 True 表示新增（rpush），False 表示替换（lset）。
    publish 由调用方负责（M1/M5 channel 不同）。
    """
    data_json = json.dumps(bar_dict)
    last_json = redis.lindex(key, -1)
    if last_json:
        last_bar = json.loads(last_json)
        if last_bar.get("time") == bar_dict.get("time"):
            redis.lset(key, -1, data_json)
            return False
    redis.rpush(key, data_json)
    redis.ltrim(key, -max_bars, -1)
    return True
