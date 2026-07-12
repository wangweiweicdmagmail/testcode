"""IndicatorRegistry — 跨周期策略共享的指标值表（进程内对象）。

设计要点：
  - NautilusTrader 无内置 registry；标准做法是 main.py 构造单例、经 config 注入
    各周期策略（M1IndicatorStrategy / M5IndicatorStrategy）。
  - 拉模式 API：策略算完指标 set 入；消费方（Phase 2 信号策略，M1 颗粒度 msg
    驱动）在 on_bar 时按需 get 跨周期指标（如 M5 策略读 M1 ATR、信号策略读 M5 ST）。
  - 数据结构：{(symbol, timeframe): {indicator_name: value}}，timeframe 如 "m1"/"m5"。
  - 跨 strategy 共享，且 _poll_history 等后台线程也可能访问，加 Lock 保护。

Phase 1：仅由两个指标策略写入，验收确认"写入正常有值"。
Phase 2：信号策略读取。
"""
from __future__ import annotations

import threading
from typing import Any


class IndicatorRegistry:
    """线程安全的指标值表。timeframe 用 "m1"/"m5"/"m30" 等小写串。"""

    def __init__(self) -> None:
        self._data: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    def set(self, symbol: str, timeframe: str, name: str, value: Any) -> None:
        """写入单个指标值。"""
        key = (symbol, timeframe)
        with self._lock:
            self._data.setdefault(key, {})[name] = value

    def set_many(self, symbol: str, timeframe: str, values: dict[str, Any]) -> None:
        """批量写入一个 (symbol, timeframe) 下的多个指标值。"""
        key = (symbol, timeframe)
        with self._lock:
            self._data.setdefault(key, {}).update(values)

    def get(self, symbol: str, timeframe: str, name: str, default: Any = None) -> Any:
        """读取单个指标值；不存在返回 default。"""
        with self._lock:
            bucket = self._data.get((symbol, timeframe))
            if bucket is None:
                return default
            return bucket.get(name, default)

    def get_all(self, symbol: str, timeframe: str) -> dict[str, Any]:
        """读取某 (symbol, timeframe) 下全部指标的快照（浅拷贝）。"""
        with self._lock:
            bucket = self._data.get((symbol, timeframe))
            return dict(bucket) if bucket else {}

    def timeframes(self, symbol: str) -> list[str]:
        """该 symbol 已有哪些周期写入过（调试/验收用）。"""
        with self._lock:
            return [tf for (sym, tf) in self._data if sym == symbol]
