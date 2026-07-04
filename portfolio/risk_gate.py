"""
组合风控层 — 所有 Alpha 策略共用。
负责：时段、熔断、冷却、组合上限、以损定量。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import redis as _redis

from execution.models import Unit, UnitState
from nautilus_trader.model.enums import OrderSide
from portfolio.config import PortfolioRiskConfig
from portfolio.sessions import et_session_date
from signals.base import IntentAction, TradeIntent

RTH_OPEN_ET_MINUTE = 9 * 60 + 30
OPENING_BREAKOUT_WINDOW_MIN = 5
RTH_CLOSE_ET_MINUTE = 16 * 60


@dataclass(frozen=True)
class RiskVerdict:
    allowed: bool
    qty: int = 0
    reason: str = ""
    meta: dict | None = None


class RiskGate:
    def __init__(
        self,
        config: PortfolioRiskConfig,
        redis: Optional[_redis.Redis],
        equity_fn: Callable[[], Optional[float]],
        units_fn: Callable[[], dict[str, list[Unit]]],
        log_fn: Callable[[str], None],
    ) -> None:
        self._cfg = config
        self._redis = redis
        self._equity_fn = equity_fn
        self._units_fn = units_fn
        self._log = log_fn
        self._cooldown_until: dict[str, int] = {}
        self._daily_trades: dict[str, dict[str, int]] = {}

    # ── 时段 ──────────────────────────────────────────────────────────
    def is_entry_window(self, et_min: int, *, opening_breakout: bool = False) -> bool:
        if opening_breakout:
            return (
                RTH_OPEN_ET_MINUTE <= et_min
                < RTH_OPEN_ET_MINUTE + OPENING_BREAKOUT_WINDOW_MIN
            )
        if et_min < RTH_OPEN_ET_MINUTE + self._cfg.rth_open_blackout_min:
            return False
        if et_min >= RTH_CLOSE_ET_MINUTE - self._cfg.pre_eod_blackout_min:
            return False
        return True

    def is_halted(self) -> bool:
        if not self._redis:
            return False
        try:
            today = et_session_date()  # ET 日历日，与 order_actor 熔断键一致
            return bool(self._redis.get(f"risk:halt:{today}"))
        except Exception:
            return False

    # ── 冷却 / 日限额（持久化到 Redis，重启不丢，防绕过）────────────────
    @staticmethod
    def _et_date(bar_time: int) -> str:
        # bar_time 是 ET fake-UTC，tz=utc 解析后即得 ET 日历日
        return datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime("%Y-%m-%d")

    def in_cooldown(self, sym: str, bar_time: int) -> bool:
        until = self._cooldown_until.get(sym, 0)
        if not until and self._redis:
            try:
                raw = self._redis.get(f"risk:cooldown:{sym}")
                if raw:
                    until = int(raw)
                    self._cooldown_until[sym] = until
            except Exception:
                pass
        return bar_time < until if until else False

    def set_cooldown(self, sym: str, bar_time: int) -> None:
        n = self._cfg.cooldown_bars_after_stop
        if n <= 0:
            return
        until = bar_time + n * 300
        self._cooldown_until[sym] = until
        if self._redis:
            try:
                self._redis.set(f"risk:cooldown:{sym}", until, ex=30 * 3600)
            except Exception:
                pass
        self._log(f"[Risk] {sym}: 止损冷却 {n}×M5")

    def daily_trade_count(self, sym: str, bar_time: int) -> int:
        d = self._et_date(bar_time)
        mem = self._daily_trades.get(d, {}).get(sym, 0)
        if self._redis:
            try:
                raw = self._redis.get(f"risk:trades:{d}:{sym}")
                if raw is not None:
                    return max(mem, int(raw))  # 取较大者，防重启后内存清零导致低估
            except Exception:
                pass
        return mem

    def record_daily_trade(self, sym: str, bar_time: int) -> None:
        d = self._et_date(bar_time)
        self._daily_trades.setdefault(d, {})[sym] = self._daily_trades.get(d, {}).get(sym, 0) + 1
        if self._redis:
            try:
                key = f"risk:trades:{d}:{sym}"
                self._redis.incr(key)
                self._redis.expire(key, 30 * 3600)
            except Exception:
                pass

    def open_symbol_count(self) -> int:
        count = 0
        for units in self._units_fn().values():
            if any(u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN, UnitState.PENDING_ENTRY)
                   for u in units):
                count += 1
        return count

    def open_symbols(self) -> set[str]:
        return {
            s for s, us in self._units_fn().items()
            if any(u.state != UnitState.CLOSED for u in us)
        }

    # ── 定量 ──────────────────────────────────────────────────────────
    def _size_with_risk_per_share(self, risk_per_share: float, ref_px: float) -> int:
        """以损定量核心：给定每股风险，按 risk_pct 预算反推股数并施加仓位上限。"""
        equity = self._equity_fn()
        if not equity or equity <= 0:
            self._log("[Risk] 净值不可用，跳过开仓")
            return 0
        if risk_per_share <= 0:
            return 0
        risk_amt = equity * self._cfg.risk_pct
        qty = int(risk_amt // risk_per_share)
        if ref_px > 0 and self._cfg.max_position_pct > 0:
            cap = int(equity * self._cfg.max_position_pct / ref_px)
            if cap > 0:
                qty = min(qty, cap)
        return qty

    def size_by_risk(self, atr: float, ref_px: float, atr_mult: float) -> int:
        """按 ATR×mult 估算每股风险定量（常规回踩路径，止损也按同口径）。"""
        return self._size_with_risk_per_share(atr * atr_mult, ref_px)

    def size_by_stop(self, ref_px: float, stop_px: float) -> int:
        """按实际结构止损距离定量 —— 用于止损价与 ATR 口径不一致的场景
        （如开盘突破，实际挂的是结构止损 planned_stop_px）。"""
        return self._size_with_risk_per_share(abs(ref_px - stop_px), ref_px)

    # ── 入场门禁（Alpha 意图 → 可执行订单）────────────────────────────
    def check_enter(
        self, intent: TradeIntent, atr_mult: float, max_units: int, live: bool,
    ) -> RiskVerdict:
        sym = intent.symbol
        units = [u for u in self._units_fn().get(sym, []) if u.state != UnitState.CLOSED]

        if live and self.is_halted():
            return RiskVerdict(False, reason="halted")

        if live and not units:
            open_syms = self.open_symbols()
            if sym not in open_syms and len(open_syms) >= self._cfg.max_portfolio_positions:
                return RiskVerdict(
                    False, reason="portfolio_position_limit",
                    meta={"open": len(open_syms), "max": self._cfg.max_portfolio_positions},
                )
            # 集中度约束：同相关组（如半导体/高 beta 科技）同时持仓上限
            if self._cfg.max_per_correlation_group > 0:
                grp = self._cfg.correlation_groups.get(sym)
                if grp:
                    same = sum(
                        1 for s in open_syms
                        if s != sym and self._cfg.correlation_groups.get(s) == grp
                    )
                    if same >= self._cfg.max_per_correlation_group:
                        return RiskVerdict(
                            False, reason="correlation_group_limit",
                            meta={"group": grp, "open": same,
                                  "max": self._cfg.max_per_correlation_group},
                        )

        if any(u.side != intent.side for u in units):
            return RiskVerdict(False, reason="opposite_side_blocked")

        if len(units) >= max_units:
            return RiskVerdict(False, reason="max_units")

        et_min = (intent.bar_time % 86400) // 60
        if intent.profile == "opening_breakout":
            if not self.is_entry_window(et_min, opening_breakout=True):
                return RiskVerdict(False, reason="outside_opening_window")
            if units:
                return RiskVerdict(False, reason="already_in_position")
            if self._cfg.fixed_qty > 0:
                qty = self._cfg.fixed_qty
            elif intent.stop_px and intent.ref_price:
                # 开盘突破实际挂结构止损 → 按真实止损距离定量，保证 $风险=risk_pct×净值
                qty = self.size_by_stop(intent.ref_price, float(intent.stop_px))
            else:
                qty = self.size_by_risk(intent.atr_ref, intent.ref_price, atr_mult)
            if qty < self._cfg.min_qty:
                return RiskVerdict(False, reason="qty_too_small", meta={"qty": qty})
            return RiskVerdict(True, qty=qty)

        if intent.action == IntentAction.ADD:
            if not units or not all(u.state == UnitState.BREAKEVEN for u in units):
                return RiskVerdict(False, reason="add_requires_breakeven")

        if intent.profile != "opening_breakout" and self.in_cooldown(sym, intent.bar_time):
            return RiskVerdict(False, reason="cooldown")

        if intent.profile != "opening_breakout":
            if self.daily_trade_count(sym, intent.bar_time) >= self._cfg.max_trades_per_sym_per_day:
                return RiskVerdict(False, reason="daily_trade_limit")

        if self._cfg.fixed_qty > 0:
            qty = self._cfg.fixed_qty
        elif (
            intent.stop_px and float(intent.stop_px) > 0 and intent.ref_price
            and intent.profile in ("st_super", "agent_proposal")
        ):
            qty = self.size_by_stop(intent.ref_price, float(intent.stop_px))
        else:
            qty = self.size_by_risk(intent.atr_ref, intent.ref_price, atr_mult)
        if qty < self._cfg.min_qty:
            return RiskVerdict(False, reason="qty_too_small", meta={"qty": qty})

        return RiskVerdict(True, qty=qty)
