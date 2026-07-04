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
    ) -> None:
        self._host = host
        self._cfg = exec_cfg
        self._redis = redis
        self._iid_map = iid_map
        self._publish = publish
        self._on_stop_cooldown = on_stop_cooldown
        self._equity_fn = equity_fn
        self._units: dict[str, list[Unit]] = {s: [] for s in iid_map}
        self._coid_index: dict[str, tuple[str, str]] = {}
        self._last_close: dict[str, float] = {}
        self._last_bar_time: dict[str, int] = {}

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

    # ── 执行意图 ──────────────────────────────────────────────────────
    def execute_enter(self, intent: TradeIntent, qty: int, mode: str, live: bool) -> None:
        sym = intent.symbol
        if not live:
            if intent.profile != "opening_breakout":
                self._publish_would_open(intent, qty, mode)
            else:
                self._host.log.info(
                    f"[PM] {sym}: 开盘突破观察 qty={qty} {intent.side.name} "
                    f"stop≈{intent.stop_px} tp≈{intent.tp_px}"
                )
            return
        self._submit_entry(sym, intent, qty)

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
        iid = self._iid_map[sym]
        instrument = self._host.cache.instrument(iid)
        if instrument is None:
            self._host.log.error(f"[PM] {sym}: 合约未加载")
            return

        entry = self._host.order_factory.market(
            instrument_id=iid,
            order_side=intent.side,
            quantity=instrument.make_qty(Decimal(str(qty))),
            time_in_force=TimeInForce.DAY,
            tags=self._fa_tags(),
        )
        unit = Unit(
            sym=sym, seq=intent.seq, side=intent.side,
            state=UnitState.PENDING_ENTRY, qty=qty,
            atr_ref=intent.atr_ref, entry_coid=entry.client_order_id.value,
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
        self._publish(sym, "open", "live", {
            "side": intent.side.name, "seq": intent.seq, "qty": qty,
            "entry": round(intent.ref_price, 2),
            "stop": intent.stop_px, "tp": intent.tp_px,
        })
        self._host.log.info(f"[PM] {sym}: 开仓 seq={intent.seq} {intent.side.name} qty={qty}")
        self._persist(sym)

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
        if net != 0 and instrument is not None:
            side = OrderSide.SELL if net > 0 else OrderSide.BUY
            close = self._host.order_factory.market(
                instrument_id=iid, order_side=side,
                quantity=instrument.make_qty(Decimal(str(abs(net)))),
                time_in_force=TimeInForce.DAY, tags=self._fa_tags(),
            )
            self._host.submit_order(close)
            self._host.log.info(f"[PM] {sym}: 全平 {side.name} qty={abs(net)} ({reason})")

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
        if unit and role == "entry" and unit.state == UnitState.PENDING_ENTRY:
            if unit.entry_filled > 0:
                # #9 部分成交后入场单被撤/拒：已有持仓但尚无止损保护 →
                # 立即平掉残仓，避免留下无保护裸仓。
                self._host.log.warning(
                    f"[PM] {sym}: 入场单 {why} 但已部分成交 {unit.entry_filled} 股，"
                    f"平残仓防裸仓"
                )
                unit.state = UnitState.CLOSED
                self.close_all(sym, f"partial_entry_{why}")
            else:
                unit.state = UnitState.CLOSED
                self._host.log.warning(f"[PM] {sym}: 入场单 {why}，单元作废")
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

        opp = OrderSide.SELL if unit.side == OrderSide.BUY else OrderSide.BUY
        stop = self._host.order_factory.stop_market(
            instrument_id=iid, order_side=opp,
            quantity=instrument.make_qty(Decimal(str(unit.qty))),
            trigger_price=instrument.make_price(Decimal(str(unit.hard_stop_px))),
            time_in_force=TimeInForce.GTC, tags=self._fa_tags(),
        )
        unit.stop_coid = stop.client_order_id.value
        self._coid_index[unit.stop_coid] = (sym, "stop")
        self._host.submit_order(stop)

        # #10 半数止盈：qty>=2 取一半（保留 runner）；qty==1 无法半止盈 →
        # 退化为全量止盈（仍有明确止盈目标，不再是只能吃满止损）。
        tp_qty = unit.qty // 2
        if tp_qty < 1:
            tp_qty = unit.qty
        if tp_qty >= 1:
            tp = self._host.order_factory.limit(
                instrument_id=iid, order_side=opp,
                quantity=instrument.make_qty(Decimal(str(tp_qty))),
                price=instrument.make_price(Decimal(str(unit.tp_px))),
                time_in_force=TimeInForce.GTC, tags=self._fa_tags(),
            )
            unit.tp_coid = tp.client_order_id.value
            self._coid_index[unit.tp_coid] = (sym, "tp")
            self._host.submit_order(tp)

        self._sync_position_redis(sym)
        self._persist(sym)

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
            try:
                self._host.modify_order(
                    order=stop,
                    quantity=instrument.make_qty(Decimal(str(remaining))),
                    trigger_price=instrument.make_price(Decimal(str(round(unit.entry_px, 2)))),
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

        # A) broker 已平，本地仍以为持仓 → 撤残单 + 重置（覆盖 close 漏确认 / 宕机止损）
        if net == 0 and open_u:
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

        # C) 双方都有仓但数量不一致（FA 部分分配等）→ 仅告警
        if net != 0 and open_u:
            eng_qty = sum(u.qty for u in open_u
                          if u.state in (UnitState.ACTIVE, UnitState.BREAKEVEN))
            if eng_qty and abs(abs(net) - eng_qty) >= 1:
                self._host.log.warning(
                    f"[Reconcile] {sym}: 数量不一致 broker={net} 本地={eng_qty}（FA/部分成交?）"
                )
            return None
        return None

    def reconcile_all(self) -> None:
        for sym in self._iid_map:
            try:
                self.reconcile(sym)
            except Exception as e:
                self._host.log.warning(f"[Reconcile] {sym}: 异常 {e}")

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

    def _fa_tags(self) -> Optional[list[str]]:
        if not self._cfg.fa_group:
            return None
        payload = {"faGroup": self._cfg.fa_group, "faMethod": self._cfg.fa_method}
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
