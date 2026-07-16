"""
执行层 OMS — 单元状态机、bracket 订单、持久化、恢复。
唯一接触 NautilusTrader 订单 API 的自动交易模块。
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Callable, Optional

import redis as _redis
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.trading.strategy import Strategy

from events import TERMINAL_STATUS
from execution.models import Unit, UnitState
from portfolio.order_policy import data_state_from_env, decide_entry_order, stop_on_wrong_side
from portfolio.ib_orders import build_marketable_order, build_resting_limit
from portfolio.config import ExecutionConfig
from signals.base import IntentAction, PositionContext, TradeIntent


class AutoPositionManager:
    def __init__(
        self,
        host: Strategy,
        exec_cfg: ExecutionConfig,
        redis: Optional[_redis.Redis],
        iid_map: dict[str, InstrumentId],
        publish: Callable[[str, str, str, Optional[dict]], None],
        on_stop_cooldown: Callable[[str, int], None],
        equity_fn: Callable[[], Optional[float]],
        on_proposal_exec: Optional[Callable[[str, str, Optional[dict]], None]] = None,
    ) -> None:
        self._host = host
        self._cfg = exec_cfg
        self._redis = redis
        self._iid_map = iid_map
        self._publish = publish
        self._on_stop_cooldown = on_stop_cooldown
        self._equity_fn = equity_fn
        self._on_proposal_exec = on_proposal_exec
        self._units: dict[str, list[Unit]] = {s: [] for s in iid_map}
        self._coid_index: dict[str, tuple[str, str]] = {}
        self._last_close: dict[str, float] = {}
        self._last_bar_time: dict[str, int] = {}
        self._pending_close_reason: dict[str, str] = {}

    @property
    def units(self) -> dict[str, list[Unit]]:
        return self._units

    def set_last_bar(self, sym: str, close: float, bar_time: int) -> None:
        self._last_close[sym] = close
        self._last_bar_time[sym] = bar_time

    def position_context(self, sym: str) -> PositionContext:
        open_u = [u for u in self._units.get(sym, [])
                  if u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN)]
        if not open_u:
            return PositionContext(0, None, False, ())
        is_long = open_u[0].side == OrderSide.BUY
        return PositionContext(
            open_units=len(open_u),
            is_long=is_long,
            all_breakeven=all(u.state == UnitState.BREAKEVEN for u in open_u),
            unit_sides=tuple(u.side for u in open_u),
        )

    def _journal_map_coid(self, coid: str, proposal_id: Optional[str]) -> None:
        """journal 归因：client_order_id → proposal_id（24h TTL）"""
        if not self._redis or not coid or not proposal_id:
            return
        try:
            self._redis.setex(f"journal:coid:{coid}", 86400, proposal_id)
        except Exception:
            pass

    # ── 执行意图 ──────────────────────────────────────────────────────
    def execute_enter(self, intent: TradeIntent, qty: int, mode: str, live: bool) -> Optional[str]:
        sym = intent.symbol
        if not live:
            if intent.profile != "opening_breakout":
                self._publish_would_open(intent, qty, mode)
            else:
                self._host.log.info(
                    f"[PM] {sym}: 开盘突破观察 qty={qty} {intent.side.name} "
                    f"stop≈{intent.stop_px} tp≈{intent.tp_px}"
                )
            return None
        return self._submit_entry(sym, intent, qty)

    def _check_order_accepted_async(self, sym: str, coid: str) -> None:
        """submit 后异步核对 IBKR 是否真接受。

        IBKR 的 order error（如 10226 faMethod 无效）NautilusTrader 不事件化，
        on_order_rejected 收不到——submit 后看似成功但 IBKR 已静默拒。
        此处 2.5s 后查 order.status：ACCEPTED/PENDING=IBKR 接受；
        仍 INITIALIZED/CANCELLED/REJECTED=被拒，提示查 ExecClient error code。
        """
        import threading
        from nautilus_trader.model.identifiers import ClientOrderId

        def _check():
            import time as _t
            _t.sleep(2.5)
            try:
                order = self._host.cache.order(ClientOrderId(coid))
                if order is None:
                    self._host.log.warning(
                        f"[PM] ⚠️ {sym} coid={coid} 提交后 cache 无记录（IBKR 可能拒，查 ExecClient error）"
                    )
                    return
                st = order.status.name  # ACCEPTED / PENDING / REJECTED / ...
                if st in ("ACCEPTED", "PENDING"):
                    self._host.log.info(f"[PM] ✓ {sym} coid={coid} IBKR 已接受 status={st}")
                else:
                    self._host.log.warning(
                        f"[PM] ⚠️ {sym} coid={coid} 提交2.5s后 status={st}（IBKR 未接受，查 ExecClient error code）"
                    )
            except Exception as e:
                self._host.log.warning(f"[PM] {sym} order 状态核对异常: {e}")

        threading.Thread(target=_check, daemon=True).start()

    def execute_exit(self, intent: TradeIntent, mode: str, live: bool) -> None:
        sym = intent.symbol
        self._publish(sym, "reversal", mode, {
            "side": "LONG" if intent.side == OrderSide.BUY else "SHORT",
            "reason": intent.exit_reason,
        })
        if live:
            self.close_all(sym, f"reversal:{intent.exit_reason}")

    def _publish_would_open(self, intent: TradeIntent, qty: int, mode: str) -> None:
        self._host.log.info(
            f"[PM] {intent.symbol}: 观察应开仓 {intent.side.name} qty={qty} "
            f"stop≈{intent.stop_px} tp≈{intent.tp_px}"
        )
        self._publish(intent.symbol, "would_open", mode, {
            "side": intent.side.name,
            "seq": intent.seq,
            "qty": qty,
            "entry": round(intent.ref_price, 2),
            "stop": intent.stop_px,
            "tp": intent.tp_px,
            "profile": intent.profile,
        })

    def _submit_entry(self, sym: str, intent: TradeIntent, qty: int) -> None:
        pid = str(intent.meta.get("proposal_id") or "")
        iid = self._iid_map[sym]
        instrument = self._host.cache.instrument(iid)
        if instrument is None:
            self._host.log.error(f"[PM] {sym}: 合约未加载")
            if pid and self._on_proposal_exec:
                self._on_proposal_exec(pid, "submit_failed", {"reason": "instrument_not_loaded"})
            return

        ref_px = float(intent.ref_price or 0) or float(self._last_close.get(sym, 0))
        try:
            decide_entry_order(
                data_state=data_state_from_env(),
                side=intent.side.name,
                ref_price=ref_px,
            )
        except ValueError as e:
            self._host.log.error(f"[PM] {sym}: 入场价无效 {e}")
            if pid and self._on_proposal_exec:
                self._on_proposal_exec(pid, "submit_failed", {"reason": str(e)})
            return

        # 控制台限价进场（manual/EMA/ST）：meta 携带 resting_limit_price → 挂 GTC 限价；
        # 否则按行情类型走 MARKET / marketable DAY-LMT（信号链路既有路径）。
        resting_raw = intent.meta.get("resting_limit_price")
        try:
            resting_price = float(resting_raw) if resting_raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            resting_price = 0.0
        is_resting = resting_price > 0
        if is_resting:
            entry = build_resting_limit(
                self._host.order_factory,
                instrument=instrument,
                instrument_id=iid,
                side=intent.side,
                qty=qty,
                limit_price=resting_price,
                tags=self._fa_tags(),
                log_fn=lambda m: self._host.log.info(f"[PM] {sym}: {m}"),
            )
            self._host.log.info(
                f"[PM] {sym}: 入场 resting_limit GTC @ {resting_price} (ref={ref_px})"
            )
        else:
            entry, decision = build_marketable_order(
                self._host.order_factory,
                instrument=instrument,
                instrument_id=iid,
                side=intent.side,
                qty=qty,
                ref_price=ref_px,
                tags=self._fa_tags(),
                log_fn=lambda m: self._host.log.info(f"[PM] {sym}: {m}"),
            )
            if decision.use_limit:
                self._host.log.info(
                    f"[PM] {sym}: 入场 {decision.reason} @ {decision.limit_price} (ref={ref_px})"
                )
        unit = Unit(
            sym=sym, seq=intent.seq, side=intent.side,
            state=UnitState.PENDING_ENTRY, qty=qty,
            atr_ref=intent.atr_ref, entry_coid=entry.client_order_id.value,
            proposal_id=pid,
        )
        if intent.meta.get("opening_breakout") and intent.stop_px:
            unit.planned_stop_px = float(intent.stop_px)
            unit.planned_tp_rr = float(intent.meta.get("tp_rr") or self._cfg.tp_rr)
            unit.manual_remainder = bool(intent.meta.get("manual_remainder"))
        elif intent.stop_px and float(intent.stop_px) > 0:
            # Agent / 超级信号：使用提案中的结构止损（与审批一致）
            unit.planned_stop_px = float(intent.stop_px)
            ref = float(intent.ref_price or 0)
            risk = abs(ref - unit.planned_stop_px)
            if intent.tp_px and float(intent.tp_px) > 0 and risk > 0:
                unit.planned_tp_rr = abs(float(intent.tp_px) - ref) / risk
            else:
                unit.planned_tp_rr = self._cfg.tp_rr
        self._units[sym].append(unit)
        self._coid_index[unit.entry_coid] = (sym, "entry")
        self._host.submit_order(entry)
        self._journal_map_coid(unit.entry_coid, pid)
        coid = unit.entry_coid
        # submit 后异步核对 IBKR 是否真接受（10226 等 error 不事件化，需主动查 order 状态）
        self._check_order_accepted_async(sym, coid)
        self._publish(sym, "open", "live", {
            "side": intent.side.name, "seq": intent.seq, "qty": qty,
            "entry": round(intent.ref_price, 2),
            "stop": intent.stop_px, "tp": intent.tp_px,
            "proposal_id": pid or None,
            "resting": is_resting,
        })
        self._host.log.info(
            f"[PM] ✓ {sym}: 开仓单已提交 coid={coid} {intent.side.name} qty={qty} "
            f"proposal={pid or '—'}"
        )
        self._persist(sym)
        return unit.entry_coid

    def cancel_pending_entry(self, sym: str, coid: str) -> dict:
        """取消一张挂单中的限价入场单（控制台用户主动撤）。

        Unit 已在 PENDING_ENTRY；IBKR 端的 cancel/reject 也由 on_order_terminal 处理。
        本方法只补"用户主动撤"路径，撤后单元作废、索引清理、publish entry_canceled。
        """
        try:
            o = self._host.cache.order(ClientOrderId(coid))
        except Exception:
            o = None
        status = getattr(o.status, "name", "") if o else ""
        if o is not None and status not in TERMINAL_STATUS and o.is_open:
            try:
                self._host.cancel_order(o)
            except Exception as e:
                self._host.log.warning(f"[PM] {sym}: 撤挂单失败 {coid}: {e}")
        unit = self._unit_by_coid(sym, coid)
        if unit and unit.state == UnitState.PENDING_ENTRY:
            unit.state = UnitState.CLOSED
            self._host.log.info(f"[PM] {sym}: 主动撤挂单入场 {coid}")
        self._coid_index.pop(coid, None)
        self._persist(sym)
        self._publish(sym, "entry_canceled", "live", {"client_order_id": coid})
        return {"status": "canceled", "client_order_id": coid, "order_status": status}

    def modify_pending_entry(self, sym: str, coid: str, price: float) -> dict:
        """改挂单限价入场价（控制台用户主动改价）。"""
        try:
            o = self._host.cache.order(ClientOrderId(coid))
        except Exception:
            return {"error": f"找不到订单 {coid}"}
        if o is None:
            return {"error": f"找不到订单 {coid}"}
        status = getattr(o.status, "name", "")
        if status in TERMINAL_STATUS:
            return {"error": f"订单 {coid} 已终态 {status}"}
        instrument = self._host.cache.instrument(o.instrument_id)
        if instrument is None:
            return {"error": "合约未加载"}
        new_price = instrument.make_price(Decimal(str(price)))
        try:
            self._host.modify_order(o, price=new_price, trigger_price=None)
        except Exception as e:
            self._host.log.error(f"[PM] {sym}: 改挂单价失败 {coid}: {e}")
            return {"error": str(e)}
        self._host.log.info(f"[PM] {sym}: 改挂单入场 {coid} → {new_price}")
        return {"status": "modified", "client_order_id": coid, "price": str(new_price)}

    def close_all(self, sym: str, reason: str) -> None:
        iid = self._iid_map[sym]
        instrument = self._host.cache.instrument(iid)

        for unit in self._units.get(sym, []):
            for coid in (unit.entry_coid, unit.stop_coid, unit.tp_coid):
                if not coid:
                    continue
                o = self._host.cache.order(ClientOrderId(coid))
                if o is not None and o.is_open:
                    try:
                        self._host.cancel_order(o)
                    except Exception:
                        pass

        try:
            net = float(self._host.portfolio.net_position(iid))
        except Exception:
            net = 0.0

        if net == 0:
            self._host.log.info(f"[PM] {sym}: 平仓请求但净仓=0，直接清状态 ({reason})")
            self._finalize_symbol_close(sym)
            return

        if instrument is None:
            self._host.log.error(f"[PM] {sym}: 合约未加载，无法平仓")
            return

        ref_px = self._ref_price_for_sym(sym, iid)
        if ref_px <= 0:
            self._host.log.error(f"[PM] {sym}: 无参考价，无法提交平仓单")
            return

        side = OrderSide.SELL if net > 0 else OrderSide.BUY
        close, decision = build_marketable_order(
            self._host.order_factory,
            instrument=instrument,
            instrument_id=iid,
            side=side,
            qty=int(abs(net)),
            ref_price=ref_px,
            tags=self._fa_tags(),
            log_fn=lambda m: self._host.log.info(f"[PM] {sym}: 平仓 {m}"),
        )
        close_coid = close.client_order_id.value
        self._coid_index[close_coid] = (sym, "close")
        self._pending_close_reason[sym] = reason
        for unit in self._units.get(sym, []):
            if unit.state != UnitState.CLOSED:
                unit.state = UnitState.PENDING_CLOSE
        self._persist(sym)
        self._host.submit_order(close)
        kind = "LMT" if decision.use_limit else "MKT"
        self._host.log.info(
            f"[PM] {sym}: 全平 {kind} {side.name} qty={abs(net)} ({reason}) coid={close_coid}"
        )

    def _ref_price_for_sym(self, sym: str, iid: InstrumentId) -> float:
        ref = float(self._last_close.get(sym, 0))
        if ref > 0:
            return ref
        pos = next((p for p in self._host.cache.positions_open() if p.instrument_id == iid), None)
        if pos is not None:
            try:
                return float(pos.avg_px_open)
            except Exception:
                pass
        return 0.0

    def _finalize_symbol_close(self, sym: str) -> None:
        self._pending_close_reason.pop(sym, None)
        for unit in self._units.get(sym, []):
            unit.state = UnitState.CLOSED
        self._coid_index = {k: v for k, v in self._coid_index.items() if v[0] != sym}
        self._sync_position_redis(sym)
        self._clear_units_redis(sym)

    def eod_close_all(self) -> list[str]:
        closed = []
        for sym, units in self._units.items():
            if any(u.state != UnitState.CLOSED for u in units):
                self.close_all(sym, "eod")
                closed.append(sym)
        return closed

    def reset_symbol(self, sym: str) -> None:
        # 防裸仓：重置前先撤掉任何仍在挂的保护单（止损/止盈）。
        # 关键场景：引擎宕机期间止损在 broker 端成交，残留 TP 仍挂着；
        # 恢复时 net==0 走到这里——必须撤单，否则 TP 日后成交 = 无止损反向裸仓。
        self._cancel_open_orders(sym)
        for unit in self._units.get(sym, []):
            unit.state = UnitState.CLOSED
        self._coid_index = {k: v for k, v in self._coid_index.items() if v[0] != sym}
        self._clear_units_redis(sym)

    def _cancel_open_orders(self, sym: str) -> int:
        """撤销该标的在 broker 端仍开放的所有本策略挂单，返回撤单数。"""
        iid = self._iid_map.get(sym)
        if iid is None:
            return 0
        n = 0
        try:
            for o in self._host.cache.orders_open():
                if o.instrument_id != iid:
                    continue
                if getattr(o, "strategy_id", None) != self._host.id:
                    continue
                if not o.is_open:
                    continue
                try:
                    self._host.cancel_order(o)
                    n += 1
                except Exception:
                    pass
        except Exception:
            return n
        if n:
            self._host.log.warning(f"[PM] {sym}: 撤销残留挂单 {n} 张（防裸仓反向）")
        return n

    # ── 订单回调（由 Runner 转发）────────────────────────────────────
    def on_order_filled(self, event) -> None:
        coid = event.client_order_id.value
        ref = self._coid_index.get(coid)
        if ref is None:
            return
        sym, role = ref
        if role == "close":
            self._finalize_symbol_close(sym)
            reason = self._pending_close_reason.get(sym, "filled")
            self._host.log.info(f"[PM] {sym}: 平仓单成交 ({reason})")
            return
        unit = self._unit_by_coid(sym, coid)
        if unit is None:
            return

        last_qty = int(event.last_qty)
        try:
            req_qty = int(self._host.cache.order(event.client_order_id).quantity)
        except Exception:
            req_qty = last_qty

        if role == "entry":
            last_px = float(event.last_px)
            new_filled = unit.entry_filled + last_qty
            unit.entry_px = (unit.entry_px * unit.entry_filled + last_px * last_qty) / new_filled
            unit.entry_filled = new_filled
            if unit.entry_filled < req_qty:
                self._persist(sym)
                return
            self._on_entry_complete(sym, unit)
        elif role == "tp":
            unit.tp_filled += last_qty
            if unit.tp_filled < req_qty:
                return
            self._on_tp_complete(sym, unit)
        elif role == "stop":
            self._on_stop_filled(sym, unit)

    def on_order_terminal(self, event, why: str) -> None:
        coid = event.client_order_id.value
        ref = self._coid_index.pop(coid, None)
        if ref is None:
            return
        sym, role = ref
        unit = self._unit_by_coid(sym, coid)
        if role == "close":
            self._host.log.warning(f"[PM] {sym}: 平仓单 {why} — reconcile 将修复状态")
            for u in self._units.get(sym, []):
                if u.state == UnitState.PENDING_CLOSE:
                    u.state = UnitState.ACTIVE if u.stop_coid else UnitState.PENDING_ENTRY
            self._pending_close_reason.pop(sym, None)
            self._persist(sym)
            return
        if unit and role == "entry" and unit.state == UnitState.PENDING_ENTRY:
            if unit.entry_filled > 0:
                self._host.log.warning(
                    f"[PM] {sym}: 入场单 {why} 但已部分成交 {unit.entry_filled} 股，"
                    f"平残仓防裸仓"
                )
                pid = unit.proposal_id
                self.close_all(sym, f"partial_entry_{why}")
                if pid and self._on_proposal_exec:
                    self._on_proposal_exec(
                        pid, "submit_failed",
                        {"reason": f"partial_entry_{why}", "symbol": sym},
                    )
                self._persist(sym)
            else:
                unit.state = UnitState.CLOSED
                self._host.log.warning(f"[PM] {sym}: 入场单 {why}，单元作废")
                if unit.proposal_id and self._on_proposal_exec:
                    self._on_proposal_exec(
                        unit.proposal_id, "submit_failed",
                        {"reason": why, "symbol": sym},
                    )
                self._persist(sym)

    def on_position_closed(self, sym: str) -> None:
        self.reset_symbol(sym)
        if self._redis:
            try:
                self._redis.delete(f"position:{sym}")
                self._redis.publish("position:update", json.dumps({"symbol": sym, "closed": True}))
            except Exception:
                pass

    def on_position_changed(self, sym: Optional[str]) -> None:
        if sym:
            self._sync_position_redis(sym)

    # ── 内部状态机 ────────────────────────────────────────────────────
    def _on_entry_complete(self, sym: str, unit: Unit) -> None:
        iid = self._iid_map[sym]
        instrument = self._host.cache.instrument(iid)
        unit.qty = unit.entry_filled
        sign = 1 if unit.side == OrderSide.BUY else -1
        if unit.planned_stop_px > 0:
            unit.hard_stop_px = round(unit.planned_stop_px, 2)
            unit.risk_per_share = abs(unit.entry_px - unit.hard_stop_px)
            rr = unit.planned_tp_rr or self._cfg.tp_rr
            unit.tp_px = round(unit.entry_px + sign * rr * unit.risk_per_share, 2)
        else:
            unit.risk_per_share = unit.atr_ref * self._cfg.atr_mult
            unit.hard_stop_px = round(unit.entry_px - sign * unit.risk_per_share, 2)
            unit.tp_px = round(unit.entry_px + sign * self._cfg.tp_rr * unit.risk_per_share, 2)
        unit.state = UnitState.ACTIVE
        if instrument is None:
            self._host.log.error(f"[PM] {sym}: 合约未加载，无法挂止损")
            return

        # 防御兜底：成交价（含滑点/gap）穿过止损 → 不挂 STOP（会即时触发），立即平仓。
        # 极低概率（主校验已拦提交前场景）；close_all 失败时 reconcile 补兜底 emergency stop。
        if stop_on_wrong_side(unit.side, unit.entry_px, unit.hard_stop_px):
            self._host.log.error(
                f"[PM] {sym}: 成交价 {unit.entry_px} 穿过止损 {unit.hard_stop_px}，"
                f"不挂 STOP（防即时触发），立即平仓"
            )
            self._persist(sym)
            self.close_all(sym, "stop_wrong_side_guard")
            if unit.proposal_id and self._on_proposal_exec:
                self._on_proposal_exec(
                    unit.proposal_id, "submit_failed",
                    {"reason": "stop_wrong_side", "symbol": sym},
                )
            return

        opp = OrderSide.SELL if unit.side == OrderSide.BUY else OrderSide.BUY
        oca_group = f"PM-{sym}-{unit.entry_coid[-8:]}"
        stop = self._host.order_factory.stop_market(
            instrument_id=iid, order_side=opp,
            quantity=instrument.make_qty(Decimal(str(unit.qty))),
            trigger_price=instrument.make_price(Decimal(str(unit.hard_stop_px))),
            time_in_force=TimeInForce.GTC, tags=self._fa_tags(oca_group=oca_group),
        )
        unit.stop_coid = stop.client_order_id.value
        self._coid_index[unit.stop_coid] = (sym, "stop")
        self._host.submit_order(stop)
        self._journal_map_coid(unit.stop_coid, unit.proposal_id)

        tp_qty = unit.qty // 2
        if tp_qty < 1:
            tp_qty = unit.qty
        if tp_qty >= 1:
            tp = self._host.order_factory.limit(
                instrument_id=iid, order_side=opp,
                quantity=instrument.make_qty(Decimal(str(tp_qty))),
                price=instrument.make_price(Decimal(str(unit.tp_px))),
                time_in_force=TimeInForce.GTC, tags=self._fa_tags(oca_group=oca_group),
            )
            unit.tp_coid = tp.client_order_id.value
            self._coid_index[unit.tp_coid] = (sym, "tp")
            self._host.submit_order(tp)
            self._journal_map_coid(unit.tp_coid, unit.proposal_id)

        self._sync_position_redis(sym)
        self._persist(sym)
        if unit.proposal_id and self._on_proposal_exec:
            self._on_proposal_exec(unit.proposal_id, "filled", {
                "symbol": sym,
                "entry_px": unit.entry_px,
                "qty": unit.qty,
                "stop": unit.hard_stop_px,
                "tp": unit.tp_px,
            })

    def _on_tp_complete(self, sym: str, unit: Unit) -> None:
        iid = self._iid_map[sym]
        instrument = self._host.cache.instrument(iid)
        remaining = unit.qty - unit.tp_filled
        stop = self._host.cache.order(ClientOrderId(unit.stop_coid)) if unit.stop_coid else None
        if remaining <= 0:
            # #10 全量止盈成交（小仓退化场景）→ 仓位已平，撤掉残留止损防裸仓
            if stop and stop.is_open:
                try:
                    self._host.cancel_order(stop)
                except Exception:
                    pass
            unit.state = UnitState.CLOSED
            self._sync_position_redis(sym)
            self._persist(sym)
            return
        if stop and stop.is_open and instrument and remaining > 0:
            # 移保本但不放松：ExitManager(ST 跟踪)可能已把止损棘轮上移 → 取较紧者
            current_tp = float(stop.trigger_price) if stop.trigger_price else unit.entry_px
            be = round(unit.entry_px, 2)
            new_tp = max(current_tp, be) if unit.side == OrderSide.BUY else min(current_tp, be)
            try:
                self._host.modify_order(
                    order=stop,
                    quantity=instrument.make_qty(Decimal(str(remaining))),
                    trigger_price=instrument.make_price(Decimal(str(new_tp))),
                )
            except Exception as e:
                self._host.log.error(f"[PM] {sym}: 移保本失败: {e}")
        unit.state = UnitState.BREAKEVEN
        self._sync_position_redis(sym)
        self._persist(sym)

    def _on_stop_filled(self, sym: str, unit: Unit) -> None:
        tp = self._host.cache.order(ClientOrderId(unit.tp_coid)) if unit.tp_coid else None
        if tp and tp.is_open:
            try:
                self._host.cancel_order(tp)
            except Exception:
                pass
        unit.state = UnitState.CLOSED
        self._publish(sym, "unit_stop", "live", {"seq": unit.seq})
        bar_t = self._last_bar_time.get(sym, 0)
        if bar_t:
            self._on_stop_cooldown(sym, bar_t)
        if unit.seq > 0:
            self.close_all(sym, "unit2_stop")
        else:
            self._sync_position_redis(sym)
            self._persist(sym)

    # ── 持久化 / 恢复 ─────────────────────────────────────────────────
    def _units_key(self, sym: str) -> str:
        return f"auto:units:{sym}"

    def _serialize(self, u: Unit) -> dict:
        return {
            "sym": u.sym, "seq": u.seq, "side": u.side.name, "state": u.state.value,
            "qty": u.qty, "atr_ref": u.atr_ref, "entry_px": u.entry_px,
            "hard_stop_px": u.hard_stop_px, "risk_per_share": u.risk_per_share,
            "tp_px": u.tp_px, "entry_coid": u.entry_coid, "stop_coid": u.stop_coid,
            "tp_coid": u.tp_coid, "entry_filled": u.entry_filled, "tp_filled": u.tp_filled,
            "planned_stop_px": u.planned_stop_px, "planned_tp_rr": u.planned_tp_rr,
            "manual_remainder": u.manual_remainder,
            "proposal_id": u.proposal_id,
        }

    def _deserialize(self, d: dict) -> Unit:
        return Unit(
            sym=d["sym"], seq=int(d["seq"]), side=OrderSide[d["side"]],
            state=UnitState(d["state"]), qty=int(d.get("qty", 0)),
            atr_ref=float(d.get("atr_ref", 0)), entry_px=float(d.get("entry_px", 0)),
            hard_stop_px=float(d.get("hard_stop_px", 0)),
            risk_per_share=float(d.get("risk_per_share", 0)),
            tp_px=float(d.get("tp_px", 0)),
            entry_coid=d.get("entry_coid", ""), stop_coid=d.get("stop_coid", ""),
            tp_coid=d.get("tp_coid", ""), entry_filled=int(d.get("entry_filled", 0)),
            tp_filled=int(d.get("tp_filled", 0)),
            planned_stop_px=float(d.get("planned_stop_px", 0)),
            planned_tp_rr=float(d.get("planned_tp_rr", 0)),
            manual_remainder=bool(d.get("manual_remainder")),
            proposal_id=str(d.get("proposal_id") or ""),
        )

    def _persist(self, sym: str) -> None:
        if not self._redis:
            return
        open_u = [u for u in self._units.get(sym, []) if u.state != UnitState.CLOSED]
        key = self._units_key(sym)
        try:
            if not open_u:
                self._redis.delete(key)
                return
            self._redis.set(key, json.dumps([self._serialize(u) for u in open_u]))
            self._redis.expire(key, 48 * 3600)
        except Exception as e:
            self._host.log.warning(f"[PM] {sym}: 持久化失败: {e}")

    def _clear_units_redis(self, sym: str) -> None:
        if self._redis:
            try:
                self._redis.delete(self._units_key(sym))
            except Exception:
                pass

    def recover_all(self) -> list[str]:
        recovered = []
        for sym in self._iid_map:
            if self._recover_symbol(sym):
                recovered.append(sym)
        return recovered

    def _recover_symbol(self, sym: str) -> bool:
        if not self._redis:
            return False
        raw = self._redis.get(self._units_key(sym))
        if raw:
            try:
                self._units[sym] = [self._deserialize(d) for d in json.loads(raw)]
                self._rebuild_coid(sym)
                self._validate_recovered(sym)
            except Exception as e:
                self._host.log.warning(f"[PM] {sym}: 恢复失败: {e}")

        iid = self._iid_map[sym]
        try:
            net = float(self._host.portfolio.net_position(iid))
        except Exception:
            net = 0.0

        open_u = [u for u in self._units.get(sym, []) if u.state != UnitState.CLOSED]
        if net == 0:
            if open_u:
                # 护栏：止损单仍活跃 → 疑似 FA 子账户盲区，启动恢复不撤止损防裸仓（同 reconcile）
                if self._has_live_protective_stop(sym):
                    self._host.log.warning(
                        f"[PM-Recover] {sym}: net=0 但保护止损单仍活跃 "
                        f"→ 疑似 FA 子账户盲区，不撤止损防裸仓；请人工核查 TWS"
                    )
                else:
                    self.reset_symbol(sym)
            else:
                self._clear_units_redis(sym)
            return False
        if not open_u:
            self._recover_from_cache(sym, net)
            return bool(self._units.get(sym))
        self._sync_position_redis(sym)
        return True

    def _order_status(self, coid: str) -> str:
        if not coid:
            return ""
        try:
            o = self._host.cache.order(ClientOrderId(coid))
            return getattr(o.status, "name", "") if o else ""
        except Exception:
            return ""

    def _has_live_protective_stop(self, sym: str) -> bool:
        """本地单元的保护止损单是否仍有活跃（非终态）单。

        reconcile 护栏用：若 portfolio 判 net=0 但止损单仍活跃，极可能是 FA 子账户
        盲区——broker 真有仓位只是引擎持仓缓存看不见。此时撤掉止损 = 真实持仓变裸仓，
        故只告警不撤，等人工核查或子账户可见性修复。"""
        for u in self._units.get(sym, []):
            if u.state == UnitState.CLOSED or not u.stop_coid:
                continue
            st = self._order_status(u.stop_coid)
            if st and st not in TERMINAL_STATUS:
                return True
        return False

    def _validate_recovered(self, sym: str) -> None:
        for unit in list(self._units.get(sym, [])):
            if unit.state == UnitState.CLOSED:
                continue
            entry_st = self._order_status(unit.entry_coid)
            stop_st = self._order_status(unit.stop_coid)
            tp_st = self._order_status(unit.tp_coid)
            if unit.state == UnitState.PENDING_ENTRY and entry_st == "FILLED":
                o = self._host.cache.order(ClientOrderId(unit.entry_coid))
                if o:
                    fq = o.filled_qty
                    unit.entry_filled = int(fq.as_double()) if hasattr(fq, "as_double") else int(fq)
                    unit.entry_px = float(o.avg_px)
                    unit.qty = unit.entry_filled
                if not unit.stop_coid or stop_st in TERMINAL_STATUS:
                    self._on_entry_complete(sym, unit)
                else:
                    unit.state = UnitState.ACTIVE
            if stop_st == "FILLED":
                unit.state = UnitState.CLOSED
            elif tp_st == "FILLED" and unit.state == UnitState.ACTIVE:
                unit.state = UnitState.BREAKEVEN
            if entry_st in TERMINAL_STATUS and entry_st != "FILLED":
                unit.state = UnitState.CLOSED
        self._units[sym] = [u for u in self._units[sym] if u.state != UnitState.CLOSED]
        self._rebuild_coid(sym)
        if self._units[sym]:
            self._persist(sym)
        else:
            self._clear_units_redis(sym)

    def _recover_from_cache(self, sym: str, net: float) -> None:
        iid = self._iid_map[sym]
        my_orders = [
            o for o in self._host.cache.orders()
            if o.instrument_id == iid and o.strategy_id == self._host.id
        ]
        if not my_orders:
            return
        side = OrderSide.BUY if net > 0 else OrderSide.SELL
        stop_o = tp_o = None
        for o in my_orders:
            if getattr(o.status, "name", "") in TERMINAL_STATUS:
                continue
            tn = getattr(o.order_type, "name", "")
            if tn == "STOP_MARKET":
                stop_o = o
            elif tn == "LIMIT":
                tp_o = o
        pos = next((p for p in self._host.cache.positions_open() if p.instrument_id == iid), None)
        entry_px = float(pos.avg_px_open) if pos else 0.0
        stop_px = float(stop_o.trigger_price) if stop_o and stop_o.trigger_price else 0.0
        state = UnitState.BREAKEVEN if stop_px and abs(stop_px - entry_px) < 0.02 else UnitState.ACTIVE
        unit = Unit(
            sym=sym, seq=0, side=side, state=state, qty=int(abs(net)),
            entry_px=entry_px, entry_filled=int(abs(net)),
            stop_coid=stop_o.client_order_id.value if stop_o else "",
            tp_coid=tp_o.client_order_id.value if tp_o else "",
            hard_stop_px=stop_px,
        )
        self._units[sym] = [unit]
        self._coid_index = {k: v for k, v in self._coid_index.items() if v[0] != sym}
        self._rebuild_coid(sym)
        self._persist(sym)

    # ── 周期账实对账（#8/#11/#12）─────────────────────────────────────
    def reconcile(self, sym: str) -> Optional[str]:
        """比对 broker 实际净持仓与本地单元，修复分歧。每根 M5 调用一次。

        仅处理本策略持仓：接管逻辑依赖 cache 中 strategy_id 匹配的挂单，
        因此不会误接管用户手动持仓。
        """
        iid = self._iid_map.get(sym)
        if iid is None:
            return None
        try:
            net = float(self._host.portfolio.net_position(iid))
        except Exception:
            return None
        open_u = [u for u in self._units.get(sym, []) if u.state != UnitState.CLOSED]

        # A) broker 已平，本地仍以为持仓 → 撤残单 + 重置
        if net == 0 and open_u:
            pending_close = any(u.state == UnitState.PENDING_CLOSE for u in open_u)
            if pending_close:
                self._host.log.info(f"[Reconcile] {sym}: 平仓单已成交，本地 PENDING_CLOSE → 清状态")
                self._finalize_symbol_close(sym)
                return "close_confirmed"
            # 护栏：保护止损单仍活跃 → 疑似 FA 子账户盲区（broker 可能仍有仓引擎看不见），
            # 撤了会变裸仓 → 只告警不撤，等人工核查。
            if self._has_live_protective_stop(sym):
                self._host.log.warning(
                    f"[Reconcile] {sym}: broker net=0 但保护止损单仍活跃 "
                    f"→ 疑似 FA 子账户盲区，不撤止损防裸仓；请人工核查 TWS 真实持仓"
                )
                return "protective_stop_guard"
            self._host.log.warning(
                f"[Reconcile] {sym}: broker 已平但本地有 {len(open_u)} 单元 → 撤残单并重置"
            )
            self.reset_symbol(sym)
            return "engine_stale_flat"

        # B) broker 有仓，本地无单元（close 被拒 / FA 回调漏触发）→ 接管并补保护
        if net != 0 and not open_u:
            had = bool(self._units.get(sym))
            self._recover_from_cache(sym, net)
            adopted = [u for u in self._units.get(sym, []) if u.state != UnitState.CLOSED]
            if adopted:
                self._host.log.warning(
                    f"[Reconcile] {sym}: broker 持仓 {net} 本地无单元 → 已接管 {len(adopted)} 单元"
                )
                for u in adopted:
                    self._ensure_protective_stop(sym, u)
                return "engine_adopted_position"
            if not had:
                # 无本策略挂单可接管（可能是手动持仓）→ 不干预
                return None
            return None

        # C) 双方都有仓但数量不一致（FA 部分分配等）→ 同步止损数量
        if net != 0 and open_u:
            eng_qty = sum(u.qty for u in open_u
                          if u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN, UnitState.PENDING_CLOSE))
            broker_qty = int(abs(net))
            if eng_qty and abs(broker_qty - eng_qty) >= 1:
                self._host.log.warning(
                    f"[Reconcile] {sym}: 数量不一致 broker={net} 本地={eng_qty} → 同步止损"
                )
                self._sync_unit_qty_to_broker(sym, broker_qty, open_u)
            return None
        return None

    def _sync_unit_qty_to_broker(self, sym: str, broker_qty: int, open_u: list) -> None:
        iid = self._iid_map.get(sym)
        instrument = self._host.cache.instrument(iid) if iid else None
        if instrument is None:
            return
        for u in open_u:
            if u.state not in (UnitState.ACTIVE, UnitState.BREAKEVEN):
                continue
            if u.qty == broker_qty:
                continue
            u.qty = broker_qty
            if not u.stop_coid:
                self._persist(sym)
                continue
            stop = self._host.cache.order(ClientOrderId(u.stop_coid))
            if stop and stop.is_open:
                try:
                    self._host.modify_order(
                        order=stop,
                        quantity=instrument.make_qty(Decimal(str(broker_qty))),
                        trigger_price=stop.trigger_price,
                    )
                    self._host.log.info(f"[Reconcile] {sym}: 止损数量 → {broker_qty}")
                except Exception as e:
                    self._host.log.error(f"[Reconcile] {sym}: 同步止损数量失败: {e}")
            self._persist(sym)
            break

    def reconcile_all(self) -> None:
        for sym in self._iid_map:
            try:
                self.reconcile(sym)
            except Exception as e:
                self._host.log.warning(f"[Reconcile] {sym}: 异常 {e}")

    def reconcile_startup(self) -> dict:
        """启动对账：broker 持仓 + cache 挂单 + Redis 单元（crash 后防双买/幽灵单元）。"""
        actions: list[str] = []
        for sym in self._iid_map:
            tag = self.reconcile(sym)
            if tag:
                actions.append(f"{sym}:{tag}")
            changed = False
            for unit in list(self._units.get(sym, [])):
                if unit.state != UnitState.PENDING_ENTRY or not unit.entry_coid:
                    continue
                st = self._order_status(unit.entry_coid)
                if st == "FILLED":
                    self._validate_recovered(sym)
                    changed = True
                elif st in TERMINAL_STATUS:
                    self._host.log.warning(
                        f"[StartupReconcile] {sym}: entry {st} → 单元作废"
                    )
                    unit.state = UnitState.CLOSED
                    actions.append(f"{sym}:void_entry_{st}")
                    changed = True
                    if unit.proposal_id and self._on_proposal_exec:
                        self._on_proposal_exec(
                            unit.proposal_id, "submit_failed", {"reason": f"startup_{st}"},
                        )
                elif not st:
                    actions.append(f"{sym}:entry_pending_cache")
            if changed:
                self._persist(sym)

        open_strategy_orders = 0
        try:
            for o in self._host.cache.orders_open():
                if getattr(o, "strategy_id", None) == self._host.id:
                    open_strategy_orders += 1
        except Exception:
            pass
        return {
            "actions": actions,
            "open_strategy_orders": open_strategy_orders,
            "symbols_checked": len(self._iid_map),
        }

    def _ensure_protective_stop(self, sym: str, unit: Unit) -> None:
        """接管的持仓若无有效止损，补挂兜底止损，杜绝裸仓。"""
        if unit.stop_coid:
            st = self._order_status(unit.stop_coid)
            if st and st not in TERMINAL_STATUS:
                return  # 已有有效止损
        iid = self._iid_map.get(sym)
        instrument = self._host.cache.instrument(iid) if iid else None
        if instrument is None or unit.entry_px <= 0 or unit.qty <= 0:
            return
        sign = 1 if unit.side == OrderSide.BUY else -1
        if unit.hard_stop_px <= 0:
            dist = unit.entry_px * self._cfg.emergency_stop_pct
            unit.hard_stop_px = round(unit.entry_px - sign * dist, 2)
            unit.risk_per_share = abs(unit.entry_px - unit.hard_stop_px)
        opp = OrderSide.SELL if unit.side == OrderSide.BUY else OrderSide.BUY
        try:
            stop = self._host.order_factory.stop_market(
                instrument_id=iid, order_side=opp,
                quantity=instrument.make_qty(Decimal(str(unit.qty))),
                trigger_price=instrument.make_price(Decimal(str(unit.hard_stop_px))),
                time_in_force=TimeInForce.GTC, tags=self._fa_tags(),
            )
            unit.stop_coid = stop.client_order_id.value
            self._coid_index[unit.stop_coid] = (sym, "stop")
            self._host.submit_order(stop)
            self._journal_map_coid(unit.stop_coid, unit.proposal_id)
            self._host.log.warning(
                f"[Reconcile] {sym}: ⛑️ 接管持仓无止损 → 补挂兜底止损 @ {unit.hard_stop_px}"
            )
            self._persist(sym)
        except Exception as e:
            self._host.log.error(f"[Reconcile] {sym}: 补挂兜底止损失败: {e}")

    def _rebuild_coid(self, sym: str) -> None:
        for u in self._units.get(sym, []):
            if u.entry_coid:
                self._coid_index[u.entry_coid] = (sym, "entry")
            if u.stop_coid:
                self._coid_index[u.stop_coid] = (sym, "stop")
            if u.tp_coid:
                self._coid_index[u.tp_coid] = (sym, "tp")

    def _unit_by_coid(self, sym: str, coid: str) -> Optional[Unit]:
        for u in self._units.get(sym, []):
            if coid in (u.entry_coid, u.stop_coid, u.tp_coid):
                return u
        return None

    def _fa_tags(self, oca_group: Optional[str] = None) -> Optional[list[str]]:
        payload: dict = {}
        if self._cfg.fa_group:
            payload["faGroup"] = self._cfg.fa_group
            # faMethod 仅在显式指定时附加：FA Group 有自己的默认 allocation method，
            # 带 faMethod 值反而被 IBKR 拒（10226/321 "财务顾问方式无效"）。
            # 留空让 IBKR 用 group 默认 method（实测 code 2184 FA 分配生效）。
            if self._cfg.fa_method:
                payload["faMethod"] = self._cfg.fa_method
        if oca_group:
            payload["ocaGroup"] = oca_group
            payload["ocaType"] = 1  # One-Cancels-All（官方 OCA 模式）
        if not payload:
            return None
        return [f"IBOrderTags:{json.dumps(payload)}"]

    def _sync_position_redis(self, sym: Optional[str]) -> None:
        if not sym or not self._redis:
            return
        iid = self._iid_map.get(sym)
        if iid is None:
            return
        try:
            net = float(self._host.portfolio.net_position(iid))
        except Exception:
            net = 0.0
        if net == 0:
            try:
                self._redis.delete(f"position:{sym}")
                self._redis.publish("position:update", json.dumps({"symbol": sym, "closed": True}))
            except Exception:
                pass
            return
        pos = next((p for p in self._host.cache.positions_open() if p.instrument_id == iid), None)
        if pos is None:
            return
        sl = None
        for u in self._units.get(sym, []):
            if u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN) and u.stop_coid:
                o = self._host.cache.order(ClientOrderId(u.stop_coid))
                tp = getattr(o, "trigger_price", None) if o else None
                if tp is not None:
                    sl = float(tp)
                    break
        last_price = self._last_close.get(sym)
        upnl = None
        instrument = self._host.cache.instrument(iid)
        if last_price and instrument:
            try:
                money = pos.unrealized_pnl(instrument.make_price(Decimal(str(round(last_price, 2)))))
                upnl = float(money.as_double()) if money else None
            except Exception:
                pass
        data = {
            "symbol": sym, "side": "LONG" if pos.is_long else "SHORT",
            "entry_price": float(pos.avg_px_open), "quantity": float(pos.quantity),
            "stop_loss": sl, "unrealized_pnl": upnl,
            "realized_pnl": float(pos.realized_pnl.as_double()) if pos.realized_pnl else 0.0,
            "last_price": last_price,
        }
        try:
            self._redis.set(f"position:{sym}", json.dumps(data))
            self._redis.publish("position:update", json.dumps(data))
        except Exception as e:
            self._host.log.warning(f"[PM] {sym}: Redis 仓位写入失败: {e}")
