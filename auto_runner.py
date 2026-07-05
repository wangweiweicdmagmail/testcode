"""
AutoRunner — 薄编排层（Nautilus Strategy 壳）

  bar.collected.m5 → Alpha(信号) → Portfolio(风控) → Execution(OMS) → IBKR

新增 Alpha：在 signals/ 下实现 SignalEngine，注册到 SIGNAL_REGISTRY。
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

import redis as _redis
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from approval.proposal_store import (
    get_proposal,
    mark_executed,
    mark_executing,
    mark_submit_failed,
    pop_approved_for_symbol,
    try_claim_execution,
)
from execution.auto_pm import AutoPositionManager
from portfolio.config import ExecutionConfig, PortfolioRiskConfig
from portfolio.risk_gate import RiskGate
from portfolio.trading_env import live_orders_allowed, trading_env
from signals.base import BarContext, IntentAction, TradeIntent
from signals.opening_breakout import OpeningBreakoutConfig, OpeningBreakoutEngine
from signals.st_dema_m5 import StDemaM5Config, StDemaM5Engine
from nautilus_trader.model.enums import OrderSide

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
EOD_CLOSE_ET_MINUTE = 15 * 60 + 45
# 超级信号单一路径：禁用 legacy st_dema_m5 与开盘突破（不经审批的 bypass）
ALPHA_SUPER_ONLY = os.environ.get("ALPHA_SUPER_ONLY", "1").strip().lower() in ("1", "true", "yes")

DEFAULT_SIGNAL_PROFILE = "st_dema_m5"
OPENING_BREAKOUT_PROFILE = "opening_breakout"


class AutoRunnerConfig(StrategyConfig, frozen=True):
    """全自动编排器配置（Alpha 参数 + 组合风控 + 执行）。"""
    instrument_ids: tuple[str, ...] = ()
    fa_group: str = ""
    fa_method: str = "NetLiq"
    # Alpha: ST+DEMA
    dema_period: int = 21
    atr_period: int = 14
    min_st_dema_spread_atr: float = 0.30
    require_close_confirm: bool = True
    # Portfolio risk
    risk_pct: float = 0.002
    max_position_pct: float = 0.10
    max_portfolio_positions: int = 3
    rth_open_blackout_min: int = 15
    pre_eod_blackout_min: int = 30
    cooldown_bars_after_stop: int = 3
    max_trades_per_sym_per_day: int = 3
    min_qty: int = 1
    fixed_qty: int = 0
    # Execution
    atr_mult: float = 1.5
    tp_rr: float = 2.0
    max_units: int = 2


class AutoRunner(Strategy):
    """三层架构编排：订阅 M5 bar，路由 Alpha → Risk → PM。"""

    def __init__(self, config: AutoRunnerConfig) -> None:
        super().__init__(config)
        self._redis: Optional[_redis.Redis] = None
        self._iid_map: dict[str, InstrumentId] = {}
        self._engines: dict[str, StDemaM5Engine | OpeningBreakoutEngine] = {}
        self._risk: Optional[RiskGate] = None
        self._pm: Optional[AutoPositionManager] = None
        self._eod_closed_dates: set[str] = set()
        self._pending_proposals: dict[str, dict] = {}

    def on_start(self) -> None:
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info(f"[Runner] Redis 已连接")
        except Exception as e:
            self.log.error(f"[Runner] Redis 连接失败: {e}")
            self._redis = None

        risk_cfg = PortfolioRiskConfig(
            risk_pct=self.config.risk_pct,
            max_position_pct=self.config.max_position_pct,
            max_portfolio_positions=self.config.max_portfolio_positions,
            rth_open_blackout_min=self.config.rth_open_blackout_min,
            pre_eod_blackout_min=self.config.pre_eod_blackout_min,
            cooldown_bars_after_stop=self.config.cooldown_bars_after_stop,
            max_trades_per_sym_per_day=self.config.max_trades_per_sym_per_day,
            min_qty=self.config.min_qty,
            fixed_qty=self.config.fixed_qty,
        )
        if risk_cfg.fixed_qty > 0:
            self.log.info(f"[Runner] 测试仓位: 固定 {risk_cfg.fixed_qty} 股/笔 (AUTO_FIXED_QTY，设 0 恢复动态)")
        else:
            self.log.info("[Runner] 仓位: 动态以损定量")
        te = trading_env()
        if not live_orders_allowed():
            self.log.warning(
                f"[Runner] TRADING_ENV={te} — AutoRunner 不会向 IBKR 提交实盘订单"
                "（批准观察仍可用；实盘需 TRADING_ENV=live 并重启引擎）"
            )
        else:
            self.log.info("[Runner] TRADING_ENV=live — 实盘报单已启用")
        # 暴露生效风控配置给前端（护栏：fixed_qty>0 时前端弹红条警告）
        if self._redis:
            try:
                self._redis.set("config:auto", json.dumps({
                    "fixed_qty": risk_cfg.fixed_qty,
                    "risk_pct": risk_cfg.risk_pct,
                    "max_position_pct": risk_cfg.max_position_pct,
                    "max_portfolio_positions": risk_cfg.max_portfolio_positions,
                    "max_trades_per_sym_per_day": risk_cfg.max_trades_per_sym_per_day,
                    "rth_open_blackout_min": risk_cfg.rth_open_blackout_min,
                    "pre_eod_blackout_min": risk_cfg.pre_eod_blackout_min,
                    "cooldown_bars_after_stop": risk_cfg.cooldown_bars_after_stop,
                    "min_qty": risk_cfg.min_qty,
                    "atr_mult": self.config.atr_mult,
                    "tp_rr": self.config.tp_rr,
                    "max_units": self.config.max_units,
                    "market_data_delayed": os.environ.get("MARKET_DATA_DELAYED", "0"),
                    "market_data_mode": os.environ.get("MARKET_DATA_MODE", "realtime"),
                    "ib_fa_group": self.config.fa_group,
                    "ib_fa_method": self.config.fa_method,
                    "trading_env": trading_env(),
                    "live_orders_allowed": live_orders_allowed(),
                    "ts": int(time.time()),
                }))
            except Exception as e:
                self.log.warning(f"[Runner] 写 config:auto 失败: {e}")
        exec_cfg = ExecutionConfig(
            atr_mult=self.config.atr_mult,
            tp_rr=self.config.tp_rr,
            max_units=self.config.max_units,
            fa_group=self.config.fa_group,
            fa_method=self.config.fa_method,
        )

        for x in self.config.instrument_ids:
            iid = InstrumentId.from_str(x)
            sym = iid.symbol.value
            self._iid_map[sym] = iid

        st_engine = StDemaM5Engine(
            StDemaM5Config(
                dema_period=self.config.dema_period,
                atr_period=self.config.atr_period,
                atr_mult=self.config.atr_mult,
                tp_rr=self.config.tp_rr,
                min_st_dema_spread_atr=self.config.min_st_dema_spread_atr,
                require_close_confirm=self.config.require_close_confirm,
            ),
            self._redis,
            self.log.info,
        )
        for sym in self._iid_map:
            st_engine.register_symbol(sym)
            st_engine.preheat(sym)
        self._engines[DEFAULT_SIGNAL_PROFILE] = st_engine

        ob_engine = OpeningBreakoutEngine(
            OpeningBreakoutConfig(),
            self._redis,
            self.log.info,
        )
        self._engines[OPENING_BREAKOUT_PROFILE] = ob_engine

        self._risk = RiskGate(
            risk_cfg, self._redis, self._account_equity,
            lambda: self._pm.units if self._pm else {},
            self.log.info,
        )
        self._pm = AutoPositionManager(
            self, exec_cfg, self._redis, self._iid_map,
            self._publish_signal,
            self._risk.set_cooldown,
            self._account_equity,
            on_proposal_exec=self._on_proposal_exec,
        )

        self.msgbus.subscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
        self.msgbus.subscribe(topic="bar.collected", handler=self._on_m1_bar)
        threading.Timer(25.0, self._recover).start()
        self.log.info(
            f"[Runner] 已启动 | {len(self._iid_map)} 标的 | "
            f"架构=Alpha→Risk→PM | profile={DEFAULT_SIGNAL_PROFILE} | "
            f"super_only={ALPHA_SUPER_ONLY}"
        )

    def on_stop(self) -> None:
        try:
            self.msgbus.unsubscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
            self.msgbus.unsubscribe(topic="bar.collected", handler=self._on_m1_bar)
        except Exception:
            pass
        if self._redis:
            self._redis.close()

    def _recover(self) -> None:
        if not self._pm:
            return
        recovered = self._pm.recover_all()
        summary = self._pm.reconcile_startup()
        if recovered:
            self.log.info(f"[Runner] 重启恢复: {recovered}")
        if summary.get("actions"):
            self.log.info(f"[Runner] 启动对账: {summary['actions']}")
        if self._redis:
            try:
                self._redis.set(
                    "reconcile:startup",
                    json.dumps({**summary, "ts": int(time.time())}),
                    ex=86400,
                )
            except Exception as e:
                self.log.warning(f"[Runner] 写 reconcile:startup 失败: {e}")

    def _on_m5_bar(self, event) -> None:
        sym = event.symbol
        if sym not in self._iid_map or not self._pm or not self._risk:
            return

        bar = event.bar
        bar_time = int(bar.get("time", 0))
        et_min = (bar_time % 86400) // 60

        try:
            c = float(bar["close"])
        except (KeyError, TypeError, ValueError):
            return

        self._pm.set_last_bar(sym, c, bar_time)
        self._engines[DEFAULT_SIGNAL_PROFILE].set_last_close(sym, c)

        # #12 周期账实对账：每根 M5 比对 broker 实际持仓与本地单元，
        # 修复 close 漏确认 / 宕机止损 / FA 回调缺失导致的分歧（含补兜底止损）。
        try:
            self._pm.reconcile(sym)
        except Exception as e:
            self.log.warning(f"[Runner] {sym}: 对账异常 {e}")

        if et_min >= EOD_CLOSE_ET_MINUTE:
            self._maybe_eod(bar_time)
            return

        if not self._legacy_alpha_enabled(sym):
            return

        mode = self._mode(sym)
        if mode == "off":
            return
        live = self._effective_live(mode)

        ctx = BarContext(symbol=sym, bar=bar, bar_time=bar_time, et_min=et_min, mode=mode)
        pos_ctx = self._pm.position_context(sym)

        if not pos_ctx.open_units and not self._risk.is_entry_window(et_min):
            return

        engine = self._engines[self._signal_profile(sym)]
        output = engine.on_bar(ctx, pos_ctx)

        if output.observe_note and mode == "observe":
            self.log.info(f"[Runner][观察] {output.observe_note}")

        for reject in output.rejects:
            self._publish_signal(sym, "rejected", mode, {"reason": reject.reason, **reject.meta})

        for intent in output.intents:
            if intent.action == IntentAction.EXIT:
                self._pm.execute_exit(intent, mode, live)
                if live:
                    engine.reset_symbol(sym)
                continue

            if intent.action in (IntentAction.ENTER, IntentAction.ADD):
                verdict = self._risk.check_enter(
                    intent, self.config.atr_mult, self.config.max_units, live,
                )
                if not verdict.allowed:
                    self._publish_signal(sym, "rejected", mode, {
                        "reason": verdict.reason, **(verdict.meta or {}),
                    })
                    continue
                if live:
                    self._risk.record_daily_trade(sym, bar_time)
                self._pm.execute_enter(intent, verdict.qty, mode, live)

    def _on_m1_bar(self, event) -> None:
        """M1：消费 reclaim 完成的 Agent 建议（低延迟执行）。"""
        sym = event.symbol
        if sym not in self._iid_map or not self._pm or not self._risk:
            return
        bar = event.bar
        bar_time = int(bar.get("time", 0))
        et_min = (bar_time % 86400) // 60
        try:
            c = float(bar["close"])
            self._pm.set_last_bar(sym, c, bar_time)
        except (KeyError, TypeError, ValueError):
            c = 0.0

        self._drain_close_request(sym)

        if et_min >= EOD_CLOSE_ET_MINUTE:
            return
        ob_mode = self._opening_breakout_mode(sym)
        if ob_mode != "off":
            self._on_opening_breakout_m1(sym, bar, bar_time, et_min, ob_mode)
            return
        mode = self._mode(sym)
        if mode == "off":
            return
        self._consume_agent_proposals(sym, bar, bar_time, et_min, mode)

    def _on_opening_breakout_m1(
        self, sym: str, bar: dict, bar_time: int, et_min: int, mode: str,
    ) -> None:
        if not self._pm or not self._risk:
            return
        live = self._effective_live(mode)
        allow_long, allow_short = self._opening_breakout_dirs(sym)
        engine = self._engines[OPENING_BREAKOUT_PROFILE]
        engine.set_directions(sym, allow_long, allow_short)

        ctx = BarContext(symbol=sym, bar=bar, bar_time=bar_time, et_min=et_min, mode=mode)
        pos_ctx = self._pm.position_context(sym)
        output = engine.on_m1(ctx, pos_ctx)

        for reject in output.rejects:
            if reject.reason not in ("no_breakout",):
                if reject.reason in ("not_armed", "premarket_ref_missing", "premarket_ref_stale"):
                    self.log.info(f"[Runner][OB] {sym} {reject.reason}")
                self._publish_signal(sym, "ob_rejected", mode, {
                    "reason": reject.reason, "profile": OPENING_BREAKOUT_PROFILE,
                })

        for intent in output.intents:
            if not self._risk.is_entry_window(et_min, opening_breakout=True):
                self._publish_signal(sym, "ob_rejected", mode, {"reason": "outside_opening_window"})
                continue
            verdict = self._risk.check_enter(
                intent, self.config.atr_mult, max_units=1, live=live,
            )
            if not verdict.allowed:
                self._publish_signal(sym, "ob_rejected", mode, {
                    "reason": verdict.reason, **(verdict.meta or {}),
                })
                continue
            if live:
                self._risk.record_daily_trade(sym, bar_time)
            self._pm.execute_enter(intent, verdict.qty, mode, live)
            ref = engine._load_ref(sym) or {}
            session_date = engine._session_date(bar_time)
            engine.confirm_trigger(sym, session_date, intent, ref, mode)
            self.log.info(
                f"[Runner][OB] {'实盘' if live else '观察'} {sym} {intent.side.name} "
                f"qty={verdict.qty} entry≈{intent.ref_price}"
            )

    def _opening_breakout_mode(self, sym: str) -> str:
        if ALPHA_SUPER_ONLY:
            return "off"
        if not self._redis:
            return "off"
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return "off"
            s = json.loads(raw)
            if s.get("opening_breakout_live"):
                return "live"
            if s.get("opening_breakout_observe"):
                return "observe"
        except Exception:
            pass
        return "off"

    def _opening_breakout_dirs(self, sym: str) -> tuple[bool, bool]:
        if not self._redis:
            return True, True
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return True, True
            s = json.loads(raw)
            return bool(s.get("opening_breakout_long", True)), bool(s.get("opening_breakout_short", True))
        except Exception:
            return True, True

    def _legacy_alpha_enabled(self, sym: str) -> bool:
        if ALPHA_SUPER_ONLY:
            return False
        if not self._redis:
            return False
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return False
            return bool(json.loads(raw).get("use_legacy_alpha"))
        except Exception:
            return False

    def _consume_agent_proposals(
        self, sym: str, bar: dict, bar_time: int, et_min: int, mode: str,
    ) -> None:
        if not self._redis or not self._pm or not self._risk:
            return
        live = self._effective_live(mode)
        decision = "approved_live" if mode == "live" else "approved_observe"
        proposals = pop_approved_for_symbol(self._redis, sym, decision=decision)
        if not proposals:
            return

        pos_ctx = self._pm.position_context(sym)
        if not pos_ctx.open_units and not self._risk.is_entry_window(et_min):
            return

        for p in proposals:
            pid = str(p.get("proposal_id") or "")
            if not pid or not try_claim_execution(self._redis, pid):
                continue
            intent = self._proposal_to_intent(p, bar_time)
            if not intent:
                mark_executed(self._redis, p, result="invalid_intent")
                continue
            if c > 0:
                intent = replace(intent, ref_price=c)
            verdict = self._risk.check_enter(
                intent, self.config.atr_mult, self.config.max_units, live,
            )
            if not verdict.allowed:
                self._publish_signal(sym, "rejected", mode, {
                    "reason": verdict.reason,
                    "proposal_id": p.get("proposal_id"),
                    **(verdict.meta or {}),
                })
                mark_executed(self._redis, p, result="risk_rejected", meta={"reason": verdict.reason})
                continue
            if live:
                self._risk.record_daily_trade(sym, bar_time)
            self._pending_proposals[pid] = dict(p)
            if live:
                mark_executing(
                    self._redis, p,
                    meta={"qty": verdict.qty, "live": True},
                )
                self._pm.execute_enter(intent, verdict.qty, mode, live)
                self.log.info(
                    f"[Runner][Agent] 已报单待成交 {pid} {sym} {p.get('side')} qty={verdict.qty}"
                )
            else:
                self._pm.execute_enter(intent, verdict.qty, mode, live)
                mark_executed(
                    self._redis, p,
                    result="observed",
                    meta={"qty": verdict.qty, "live": False},
                )
                self._pending_proposals.pop(pid, None)
                self.log.info(
                    f"[Runner][Agent] 已观察建议 {pid} {sym} {p.get('side')} qty={verdict.qty}"
                )

    def _proposal_to_intent(self, p: dict, bar_time: int) -> Optional[TradeIntent]:
        try:
            side_str = str(p.get("side", "")).upper()
            side = OrderSide.BUY if side_str == "LONG" else OrderSide.SELL
            entry = float(p["entry_price"])
            stop = float(p.get("stop_price") or 0)
            tp_raw = p.get("tp_half_price") if p.get("tp_half_price") is not None else p.get("tp_price")
            tp = float(tp_raw or 0)
            atr_ref = abs(entry - stop) / self.config.atr_mult if stop else entry * 0.004
            return TradeIntent(
                profile=str(p.get("signal_type") or "agent_proposal"),
                symbol=str(p["symbol"]),
                action=IntentAction.ENTER,
                side=side,
                ref_price=entry,
                atr_ref=atr_ref,
                bar_time=bar_time,
                stop_px=stop or None,
                tp_px=tp or None,
                meta={
                    "proposal_id": p.get("proposal_id"),
                    "confidence": p.get("confidence"),
                    "thesis": p.get("thesis"),
                    "approver": p.get("approver"),
                },
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _drain_close_request(self, sym: str) -> None:
        """消费 Redis auto:close:{sym}（上层 UI 路由的 AutoPM 平仓请求）。"""
        if not self._redis or not self._pm:
            return
        key = f"auto:close:{sym.upper()}"
        try:
            raw = self._redis.get(key)
            if not raw:
                return
            self._redis.delete(key)
            payload = json.loads(raw) if isinstance(raw, str) else {}
            reason = str(payload.get("reason") or "ui_close")
            self._pm.close_all(sym, reason)
            self._publish_signal(sym, "ui_close", "live", {"reason": reason})
            self.log.info(f"[Runner] AutoPM 平仓 {sym} ({reason})")
        except Exception as e:
            self.log.warning(f"[Runner] 消费 auto:close {sym} 失败: {e}")

    def _maybe_eod(self, bar_time: int) -> None:
        date = datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime("%Y-%m-%d")
        if date in self._eod_closed_dates:
            return
        self._eod_closed_dates.add(date)
        if not self._pm:
            return
        closed = self._pm.eod_close_all()
        for s in closed:
            self._publish_signal(s, "eod", "live", {"reason": "15:45 ET"})
            self._engines[DEFAULT_SIGNAL_PROFILE].reset_symbol(s)
        if closed:
            self.log.info(f"[Runner] EOD 全平: {closed}")

    def _on_proposal_exec(self, proposal_id: str, result: str, meta: Optional[dict] = None) -> None:
        """AutoPM 回调：入场成交 / 报单失败。"""
        if not self._redis:
            return
        pid = str(proposal_id)
        p = self._pending_proposals.pop(pid, None)
        if not p:
            p = get_proposal(self._redis, "approved", pid)
        if not p:
            p = get_proposal(self._redis, "executed", pid)
        if not p:
            self.log.warning(f"[Runner] proposal exec 回调无记录: {pid} result={result}")
            return
        if result == "filled":
            mark_executed(
                self._redis, p,
                result="executed",
                meta={**(meta or {}), "live": True},
            )
            self.log.info(f"[Runner][Agent] 建议成交完成 {pid} {p.get('symbol')}")
        elif result == "submit_failed":
            mark_submit_failed(
                self._redis, p,
                reason=str((meta or {}).get("reason") or "submit_failed"),
                meta=meta,
            )
            self.log.warning(
                f"[Runner][Agent] 建议报单失败 {pid} {p.get('symbol')}: "
                f"{(meta or {}).get('reason')}"
            )

    # ── 订单 / 仓位事件（转发给 PM）────────────────────────────────────
    def on_order_filled(self, event) -> None:
        if self._pm:
            self._pm.on_order_filled(event)

    def on_order_rejected(self, event) -> None:
        if self._pm:
            self._pm.on_order_terminal(event, "REJECTED")

    def on_order_canceled(self, event) -> None:
        if self._pm:
            self._pm.on_order_terminal(event, "CANCELED")

    def on_order_expired(self, event) -> None:
        if self._pm:
            self._pm.on_order_terminal(event, "EXPIRED")

    def on_position_opened(self, event) -> None:
        if self._pm:
            self._pm.on_position_changed(self._sym_of(event))

    def on_position_changed(self, event) -> None:
        if self._pm:
            self._pm.on_position_changed(self._sym_of(event))

    def on_position_closed(self, event) -> None:
        sym = self._sym_of(event)
        if sym and self._pm:
            self._pm.on_position_closed(sym)
            self._engines[DEFAULT_SIGNAL_PROFILE].reset_symbol(sym)

    # ── 工具 ──────────────────────────────────────────────────────────
    def _effective_live(self, mode: str) -> bool:
        """settings 为 live 且 TRADING_ENV=live 时才向 IBKR 报单。"""
        if mode != "live":
            return False
        if live_orders_allowed():
            return True
        return False

    def _mode(self, sym: str) -> str:
        if not self._redis:
            return "off"
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return "off"
            s = json.loads(raw)
            if s.get("auto_strategy"):
                return "live"
            if s.get("auto_observe"):
                return "observe"
            return "off"
        except Exception:
            return "off"

    def _signal_profile(self, sym: str) -> str:
        if not self._redis:
            return DEFAULT_SIGNAL_PROFILE
        try:
            raw = self._redis.get(f"settings:{sym}")
            if raw:
                p = json.loads(raw).get("signal_profile")
                if p and p in self._engines:
                    return p
        except Exception:
            pass
        return DEFAULT_SIGNAL_PROFILE

    def _account_equity(self) -> Optional[float]:
        if self._redis:
            try:
                raw = self._redis.get("account:funds")
                if raw:
                    usd = next((b for b in json.loads(raw).get("balances", [])
                                if b.get("currency") == "USD"), None)
                    if usd and usd.get("total"):
                        return float(usd["total"])
            except Exception:
                pass
        try:
            best = 0.0
            for acct in self.cache.accounts():
                t = acct.balance_total(USD)
                v = float(t.as_double()) if t else 0.0
                best = max(best, v)
            return best if best > 0 else None
        except Exception:
            return None

    def _sym_of(self, event) -> Optional[str]:
        try:
            return event.instrument_id.symbol.value
        except Exception:
            try:
                pos = self.cache.position(event.position_id)
                return pos.instrument_id.symbol.value if pos else None
            except Exception:
                return None

    def _publish_signal(
        self, sym: str, action: str, mode: str, extra: Optional[dict] = None,
    ) -> None:
        if not self._redis:
            return
        payload = {"symbol": sym, "action": action, "mode": mode, "ts": int(time.time())}
        if extra:
            payload.update(extra)
        try:
            self._redis.publish("auto:signal", json.dumps(payload))
            try:
                from measurement.signal_store import record_auto
                record_auto(payload)
            except Exception as e:
                self.log.debug(f"[Runner] signal_store: {e}")
        except Exception as e:
            self.log.warning(f"[Runner] auto:signal 推送失败: {e}")
