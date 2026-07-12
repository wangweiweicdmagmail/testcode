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
from events import AgentExecuteNowEvent, EntryExecuteNowEvent
from execution.auto_pm import AutoPositionManager
from execution.models import UnitState
from portfolio.config import ExecutionConfig, PortfolioRiskConfig
from portfolio.risk_gate import RiskGate, stop_on_wrong_side
from portfolio.trading_env import live_orders_allowed, trading_env
from signals.base import IntentAction, TradeIntent
from entry import ticket_store
from signals.entry_methods import (
    ENTRY_METHODS,
    EntryBuildError,
    parse_entry_request,
    req_from_dict,
    req_to_dict,
)
from nautilus_trader.model.enums import OrderSide

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
EOD_CLOSE_ET_MINUTE = 15 * 60 + 45


class AutoRunnerConfig(StrategyConfig, frozen=True):
    """全自动编排器配置（Alpha 参数 + 组合风控 + 执行）。"""
    instrument_ids: tuple[str, ...] = ()
    fa_group: str = ""
    fa_method: str = "NetLiq"
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
        self._risk: Optional[RiskGate] = None
        self._pm: Optional[AutoPositionManager] = None
        self._eod_closed_dates: set[str] = set()
        self._pending_proposals: dict[str, dict] = {}
        self._last_agent_off_warn: dict[str, float] = {}
        self._last_purge_ts: float = 0.0
        self._last_entry_expire_ts: float = 0.0

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
        self.msgbus.subscribe(topic=AgentExecuteNowEvent.TOPIC, handler=self._on_execute_now)
        self.msgbus.subscribe(topic=EntryExecuteNowEvent.TOPIC, handler=self._on_enter_now)
        threading.Timer(25.0, self._recover).start()
        self.log.info(
            f"[Runner] 已启动 | {len(self._iid_map)} 标的 | "
            f"架构=审批→Risk→PM（st_super 单一信号路径）"
        )

    def on_stop(self) -> None:
        try:
            self.msgbus.unsubscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
            self.msgbus.unsubscribe(topic="bar.collected", handler=self._on_m1_bar)
            self.msgbus.unsubscribe(topic=AgentExecuteNowEvent.TOPIC, handler=self._on_execute_now)
            self.msgbus.unsubscribe(topic=EntryExecuteNowEvent.TOPIC, handler=self._on_enter_now)
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

    def _maybe_purge_expired_proposals(self) -> None:
        """过期 pending 建议清理（全局节流，每 5 分钟一次；首个标的 M5 触发全量）。"""
        if not self._redis:
            return
        if (time.time() - self._last_purge_ts) < 300:
            return
        self._last_purge_ts = time.time()
        try:
            from approval.pending_cleanup import purge_pending_proposals
            result = purge_pending_proposals(
                self._redis,
                reject_expired=True,
                reject_stale_touch=False,
                reject_counter_trend=False,
                operator="auto_runner",
            )
            if result.purged:
                self.log.info(f"[Runner] 清理过期 pending: {len(result.purged)} 个")
        except Exception as e:
            self.log.warning(f"[Runner] 清理过期 pending 失败: {e}")

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

        # #12 周期账实对账：每根 M5 比对 broker 实际持仓与本地单元，
        # 修复 close 漏确认 / 宕机止损 / FA 回调缺失导致的分歧（含补兜底止损）。
        try:
            self._pm.reconcile(sym)
        except Exception as e:
            self.log.warning(f"[Runner] {sym}: 对账异常 {e}")

        self._maybe_purge_expired_proposals()

        if et_min >= EOD_CLOSE_ET_MINUTE:
            self._maybe_eod(bar_time)
            return

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
        self._drain_enter_request(sym)
        self._drain_entry_cmds()
        self._evaluate_armed_entries(sym, bar, bar_time)
        self._reconcile_pending_tickets(sym)

        if et_min >= EOD_CLOSE_ET_MINUTE:
            return
        mode = self._mode(sym)
        if mode == "off":
            if self._redis:
                pending = pop_approved_for_symbol(
                    self._redis, sym, decision="approved_live",
                ) + pop_approved_for_symbol(
                    self._redis, sym, decision="approved_observe",
                )
                if pending:
                    now = time.time()
                    if now - self._last_agent_off_warn.get(sym, 0) >= 300:
                        self._last_agent_off_warn[sym] = now
                        self.log.warning(
                            f"[Runner][Agent] {sym} 有 {len(pending)} 条已批准待执行，"
                            "但 Agent执行=off（请批准实盘/观察或手动开启 auto_strategy）"
                        )
            return
        self._consume_agent_proposals(sym, bar, bar_time, et_min, mode)

    def _load_latest_m1_bar(self, sym: str) -> Optional[dict]:
        if not self._redis:
            return None
        try:
            raw = self._redis.lindex(f"bars:1m:{sym.upper()}", -1)
            if raw:
                return json.loads(raw)
            raw = self._redis.get(f"bars:1m:{sym.upper()}:last")
            if raw:
                return json.loads(raw)
        except Exception as e:
            self.log.warning(f"[Runner] 读取最新 M1 bar 失败 {sym}: {e}")
        return None

    def _on_enter_now(self, event) -> None:
        """控制台进场立即触发（不等下一根 M1，市价单低延迟进场）。

        server.js 写 auto:enter:{sym} 后经 order_actor /enter-now 发布本事件；
        这里直接调用 _drain_enter_request 立即消费。M1 的 _drain_enter_request
        作为兜底（事件丢失 / 引擎重启时 key 仍在）。
        """
        sym = str(getattr(event, "symbol", "") or "").upper()
        if sym not in self._iid_map:
            self.log.warning(f"[Runner][Entry] 立即进场跳过 {sym}: 不在 instrument_ids")
            return
        try:
            self._drain_enter_request(sym)
        except Exception as e:
            self.log.warning(f"[Runner][Entry] 立即进场异常 {sym}: {e}")

    def _on_execute_now(self, event: AgentExecuteNowEvent) -> None:
        """审批通过后立即执行（不等下一根 M1）。"""
        sym = str(event.symbol).upper()
        if sym not in self._iid_map or not self._pm or not self._risk:
            return
        mode = self._mode(sym)
        if mode == "off":
            self.log.warning(
                f"[Runner][Agent] 立即执行跳过 {sym}: Agent执行=off"
            )
            return
        bar = self._load_latest_m1_bar(sym)
        if not bar:
            self.log.warning(f"[Runner][Agent] 立即执行跳过 {sym}: 无 M1 bar")
            return
        bar_time = int(bar.get("time", 0))
        et_min = (bar_time % 86400) // 60
        try:
            c = float(bar["close"])
            self._pm.set_last_bar(sym, c, bar_time)
        except (KeyError, TypeError, ValueError):
            pass
        if et_min >= EOD_CLOSE_ET_MINUTE:
            return

        live = self._effective_live(mode)
        decision = "approved_live" if mode == "live" else "approved_observe"
        pid_filter = str(getattr(event, "proposal_id", "") or "")
        if pid_filter:
            p = get_proposal(self._redis, "approved", pid_filter) if self._redis else None
            proposals = [p] if p else []
        else:
            proposals = (
                pop_approved_for_symbol(self._redis, sym, decision=decision)
                if self._redis else []
            )
        if not proposals:
            self.log.info(f"[Runner][Agent] 立即执行 {sym}: 无可执行建议")
            return

        pos_ctx = self._pm.position_context(sym)
        if not pos_ctx.open_units and not self._risk.is_entry_window(et_min):
            self.log.info(
                f"[Runner][Agent] 立即执行 {sym} 跳过: 非入场窗口 et_min={et_min}"
            )
            return

        self.log.info(
            f"[Runner][Agent] 立即执行 {sym} {len(proposals)} 条建议 "
            f"mode={mode} effective_live={live}"
        )
        for p in proposals:
            self._execute_one_proposal(sym, p, bar, bar_time, et_min, mode, live)

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

        try:
            bar_close = float(bar.get("close") or 0)
        except (TypeError, ValueError):
            bar_close = 0.0

        pos_ctx = self._pm.position_context(sym)
        if not pos_ctx.open_units and not self._risk.is_entry_window(et_min):
            self.log.info(
                f"[Runner][Agent] {sym} 跳过 {len(proposals)} 条建议: 非入场窗口 et_min={et_min}"
            )
            return

        self.log.info(
            f"[Runner][Agent] {sym} 消费 {len(proposals)} 条已批准建议 "
            f"mode={mode} effective_live={live} bar_close={bar_close:.2f}"
        )

        for p in proposals:
            self._execute_one_proposal(sym, p, bar, bar_time, et_min, mode, live)

    def _execute_one_proposal(
        self,
        sym: str,
        p: dict,
        bar: dict,
        bar_time: int,
        et_min: int,
        mode: str,
        live: bool,
    ) -> None:
        pid = str(p.get("proposal_id") or "")
        if not pid:
            self.log.warning(f"[Runner][Agent] {sym} 建议缺少 proposal_id，跳过")
            return
        if not try_claim_execution(self._redis, pid):
            self.log.info(f"[Runner][Agent] {sym} {pid} 执行权已被抢占，跳过")
            return
        intent = self._proposal_to_intent(p, bar_time)
        if not intent:
            self.log.warning(f"[Runner][Agent] {sym} {pid} invalid_intent，标记 executed")
            mark_executed(self._redis, p, result="invalid_intent")
            return
        try:
            bar_close = float(bar.get("close") or 0)
        except (TypeError, ValueError):
            bar_close = 0.0
        if bar_close > 0:
            intent = replace(intent, ref_price=bar_close)

        # 信号有效性校验：审批延迟期间价格可能已穿过原止损线。
        # 此时入场即触发止损（即时亏损）或被 IBKR 拒单（裸仓）→ 放弃，不重试、不消耗日限额。
        if intent.stop_px and stop_on_wrong_side(
            intent.side, float(intent.ref_price), float(intent.stop_px)
        ):
            self._publish_signal(sym, "rejected", mode, {
                "reason": "stop_side_breached",
                "proposal_id": pid,
                "ref_price": bar_close,
                "stop_price": float(intent.stop_px),
            })
            mark_executed(self._redis, p, result="signal_stale", meta={
                "reason": "stop_side_breached",
                "ref_price": bar_close,
                "stop_price": float(intent.stop_px),
            })
            self.log.info(
                f"[Runner][Agent] {sym} {pid} 信号失效: 最新价 {bar_close:.2f} "
                f"已穿过止损 {intent.stop_px:.2f}，放弃执行"
            )
            return

        verdict = self._risk.check_enter(
            intent, self.config.atr_mult, self.config.max_units, live,
        )
        if not verdict.allowed:
            self.log.info(
                f"[Runner][Agent] {sym} {pid} 风控拒绝: {verdict.reason}"
                + (f" meta={verdict.meta}" if verdict.meta else "")
            )
            self._publish_signal(sym, "rejected", mode, {
                "reason": verdict.reason,
                "proposal_id": p.get("proposal_id"),
                **(verdict.meta or {}),
            })
            mark_executed(self._redis, p, result="risk_rejected", meta={"reason": verdict.reason})
            return
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
                f"[Runner][Agent] ✓ 已报单待成交 {pid} {sym} {p.get('side')} "
                f"qty={verdict.qty} ref≈{intent.ref_price:.2f}"
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
                f"[Runner][Agent] ✓ 观察模式记录 {pid} {sym} {p.get('side')} qty={verdict.qty}"
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

    # ── 控制台进场（auto:enter / 条件触发 / 票据对账 / 撤改单）────────────
    def _load_levels(self, sym: str) -> dict:
        if not self._redis:
            return {}
        try:
            raw = self._redis.get(f"indicators:active:{sym.upper()}")
            if raw:
                return json.loads(raw)
        except Exception as e:
            self.log.debug(f"[Runner] indicators:active {sym} 读取失败: {e}")
        return {}

    def _latest_close(self, sym: str) -> float:
        bar = self._load_latest_m1_bar(sym)
        if bar:
            try:
                return float(bar.get("close") or 0)
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    def _entry_mode_live(self) -> tuple[str, bool]:
        live = live_orders_allowed()
        return ("live" if live else "observe"), live

    def _risk_and_execute(
        self, sym: str, intent: TradeIntent, bar_time: int, *, live: bool, bypass_window: bool,
    ) -> tuple[Optional[str], int, Optional[str]]:
        """控制台进场专用通道：熔断 → 窗口 → 止损侧 → check_enter → execute_enter。
        返回 (coid, qty, err)；err 非 None 即被拒（已 publish entry_rejected）。"""
        if self._risk.is_halted():
            self._publish_signal(sym, "entry_rejected", "live", {"reason": "halted"})
            return None, 0, "halted"
        et_min = (bar_time % 86400) // 60
        if not bypass_window and not self._risk.is_entry_window(et_min):
            self._publish_signal(sym, "entry_rejected", "live",
                                 {"reason": "outside_entry_window", "et_min": et_min})
            return None, 0, "outside_entry_window"
        if intent.stop_px and stop_on_wrong_side(
            intent.side, float(intent.ref_price), float(intent.stop_px)
        ):
            self._publish_signal(sym, "entry_rejected", "live", {"reason": "stop_side_breached"})
            return None, 0, "stop_side_breached"
        verdict = self._risk.check_enter(intent, self.config.atr_mult, self.config.max_units, live)
        if not verdict.allowed:
            self._publish_signal(sym, "entry_rejected", "live",
                                 {"reason": verdict.reason, **(verdict.meta or {})})
            return None, 0, verdict.reason
        if live:
            self._risk.record_daily_trade(sym, bar_time)
        coid = self._pm.execute_enter(intent, verdict.qty, "live" if live else "observe", live)
        return coid, int(verdict.qty), None

    def _drain_enter_request(self, sym: str) -> None:
        """消费 Redis auto:enter:{sym}（控制台发起的进场请求）。"""
        if not self._redis or not self._pm or not self._risk:
            return
        key = f"auto:enter:{sym.upper()}"
        try:
            raw = self._redis.get(key)
            if not raw:
                return
            self._redis.delete(key)
            payload = json.loads(raw) if isinstance(raw, str) else {}
        except Exception as e:
            self.log.warning(f"[Runner] 消费 auto:enter {sym} 失败: {e}")
            return
        payload["symbol"] = sym
        try:
            req = parse_entry_request(payload)
        except EntryBuildError as e:
            self._publish_signal(sym, "entry_rejected", "live", {"reason": e.reason, **e.meta})
            self.log.info(f"[Runner] 进场请求解析失败 {sym}: {e.reason}")
            return
        method = ENTRY_METHODS[req.method]
        bar_time = (self._pm._last_bar_time.get(sym) if self._pm else None) or int(time.time())
        try:
            intent, directive = method.build_intent(
                req, self._load_levels(sym), self._latest_close(sym), bar_time,
            )
        except EntryBuildError as e:
            self._publish_signal(sym, "entry_rejected", "live", {"reason": e.reason, **e.meta})
            self.log.info(f"[Runner] 进场构造失败 {sym}: {e.reason}")
            return

        if directive.kind == "arm":
            ticket = ticket_store.create_ticket(
                self._redis, req_dict=req_to_dict(req), state="ARMED", expire_ts=req.expire_ts,
            )
            self._publish_signal(sym, "entry_armed", "live", {
                "ticket_id": ticket["ticket_id"], "method": req.method,
                "side": req.side.name, "trigger": req.trigger,
            })
            self.log.info(
                f"[Runner] ARMED 条件进场 {sym} {req.side.name} ticket={ticket['ticket_id']}"
            )
            return

        # resting_limit（manual/EMA/ST）：建 RESTING 票 → 执行 → 回填 coid
        ticket = ticket_store.create_ticket(
            self._redis, req_dict=req_to_dict(req), state="RESTING", expire_ts=req.expire_ts,
            intent_meta={"limit_price": directive.limit_price, "stop": intent.stop_px,
                         "tp": intent.tp_px, "method": req.method},
        )
        mode, live = self._entry_mode_live()
        coid, qty, err = self._risk_and_execute(
            sym, intent, bar_time, live=live, bypass_window=req.bypass_window,
        )
        if err:
            ticket_store.mark_canceled(self._redis, ticket["ticket_id"], reason=err)
            self.log.info(f"[Runner] RESTING 进场被拒 {sym}: {err}")
            return
        if coid:
            ticket_store.update(self._redis, ticket["ticket_id"],
                                fields={"entry_coid": coid, "qty": qty}, event="armed")
            self._publish_signal(sym, "entry_armed", mode, {
                "ticket_id": ticket["ticket_id"], "method": req.method, "side": req.side.name,
                "resting": True, "limit_price": directive.limit_price,
                "entry_coid": coid, "qty": qty,
            })
            self.log.info(
                f"[Runner] RESTING 进场 {sym} {req.side.name} ticket={ticket['ticket_id']} "
                f"coid={coid} qty={qty} @ {directive.limit_price}"
            )
        else:
            # 观察模式 dry-run（AutoPM 已 publish would_open）
            ticket_store.mark_observed(self._redis, ticket["ticket_id"])
            self.log.info(f"[Runner] RESTING 进场（观察）{sym} {req.side.name} qty={qty}")

    def _evaluate_armed_entries(self, sym: str, bar: dict, bar_time: int) -> None:
        """每根 M1 评估该标的的 ARMED 条件票：过期清理 + 触发转 marketable。"""
        if not self._redis or not self._pm or not self._risk:
            return
        now = time.time()
        if now - self._last_entry_expire_ts > 60:
            self._last_entry_expire_ts = now
            try:
                expired = ticket_store.expire_due(self._redis)
                if expired:
                    self.log.info(f"[Runner] 过期条件票清理: {len(expired)}")
            except Exception as e:
                self.log.warning(f"[Runner] entry expire_due 失败: {e}")
        mode, live = self._entry_mode_live()
        levels = self._load_levels(sym)
        try:
            close = float(bar.get("close") or 0)
        except (TypeError, ValueError):
            close = 0.0
        for t in ticket_store.list_armed_for_symbol(self._redis, sym):
            tid = t["ticket_id"]
            try:
                req = req_from_dict(t.get("params") or {})
            except EntryBuildError:
                continue
            method = ENTRY_METHODS.get(req.method)
            if method is None or not method.is_conditional():
                continue
            try:
                if not method.check_trigger(req, bar, levels):
                    continue
            except Exception as e:
                self.log.warning(f"[Runner] 触发判定异常 {tid}: {e}")
                continue
            if not ticket_store.claim(self._redis, tid):
                continue
            ticket_store.mark_triggered(self._redis, tid, trigger_close=close, bar_time=bar_time)
            self._publish_signal(sym, "entry_triggered", mode,
                                 {"ticket_id": tid, "method": req.method, "trigger_close": close})
            self.log.info(f"[Runner] 条件进场触发 {sym} ticket={tid} @ {close}")
            try:
                intent, _directive = method.build_trigger_intent(req, close, bar_time)
            except EntryBuildError as e:
                ticket_store.mark_canceled(self._redis, tid, reason=e.reason)
                self._publish_signal(sym, "entry_rejected", mode,
                                     {"reason": e.reason, "ticket_id": tid})
                continue
            coid, qty, err = self._risk_and_execute(
                sym, intent, bar_time, live=live, bypass_window=req.bypass_window,
            )
            if err:
                ticket_store.mark_canceled(self._redis, tid, reason=err)
            elif coid:
                ticket_store.update(self._redis, tid,
                                    fields={"entry_coid": coid, "qty": qty}, event="triggered")
                self._publish_signal(sym, "entry_filled", mode, {"ticket_id": tid, "qty": qty})
            else:
                ticket_store.mark_observed(self._redis, tid)

    def _reconcile_pending_tickets(self, sym: str) -> None:
        """TRIGGERED 条件票：对应 AutoPM Unit 进入 ACTIVE → FILLED；coid 消失 → CANCELED。"""
        if not self._redis or not self._pm:
            return
        units = self._pm.units.get(sym, [])
        active = {u.entry_coid: u for u in units
                  if u.entry_coid and u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN)}
        gone = {u.entry_coid for u in units
                if u.entry_coid and u.state == UnitState.CLOSED}
        for t in ticket_store.list_pending(self._redis, symbol=sym, limit=200):
            if t.get("state") not in ("RESTING", "TRIGGERED"):
                continue
            coid = t.get("entry_coid") or ""
            if not coid:
                continue
            if coid in active:
                ticket_store.mark_filled(self._redis, t["ticket_id"],
                                         entry_coid=coid, qty=int(active[coid].qty))
            elif coid in gone:
                ticket_store.mark_canceled(self._redis, t["ticket_id"], reason="order_gone")

    def _drain_entry_cmds(self) -> None:
        """消费 Redis list entry:cmd（控制台 撤单/改价，按 ticket_id 或 entry_coid 寻址）。"""
        if not self._redis or not self._pm:
            return
        for _ in range(50):
            try:
                raw = self._redis.rpop("entry:cmd")
            except Exception:
                break
            if not raw:
                break
            try:
                cmd = json.loads(raw)
            except Exception:
                continue
            action = str(cmd.get("action") or "")
            tid = str(cmd.get("ticket_id") or "")
            coid = str(cmd.get("entry_coid") or "")
            sym = str(cmd.get("symbol") or "").upper()
            ticket = ticket_store.get_ticket(self._redis, tid) if tid else None
            if ticket:
                sym = str(ticket.get("symbol") or sym).upper()
                coid = coid or str(ticket.get("entry_coid") or "")
            if not sym:
                continue
            if action == "cancel":
                if coid:
                    try:
                        self._pm.cancel_pending_entry(sym, coid)
                    except Exception as e:
                        self.log.warning(f"[Runner] 撤挂单 {coid} 失败: {e}")
                if ticket:
                    ticket_store.mark_canceled(
                        self._redis, tid, reason=str(cmd.get("reason") or "ui_cancel"),
                    )
                self._publish_signal(sym, "entry_canceled", "live",
                                     {"ticket_id": tid or None, "entry_coid": coid or None})
            elif action == "modify":
                price = cmd.get("price")
                if coid and price is not None:
                    res = self._pm.modify_pending_entry(sym, coid, float(price))
                    if res.get("error"):
                        self._publish_signal(sym, "entry_rejected", "live",
                                             {"ticket_id": tid or None, "reason": res["error"]})
                        continue
                if ticket:
                    params = dict(ticket.get("params") or {})
                    if ticket.get("state") == "ARMED" and isinstance(params.get("trigger"), dict):
                        try:
                            params["trigger"]["level"] = float(price)
                        except (TypeError, ValueError):
                            pass
                    elif price is not None:
                        params["limit_price"] = float(price)
                    ticket_store.update(self._redis, tid,
                                        fields={"params": params}, event="modified")
                self._publish_signal(sym, "entry_modified", "live",
                                     {"ticket_id": tid or None, "entry_coid": coid or None,
                                      "price": price})

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
