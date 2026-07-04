#!/usr/bin/env python3
"""
离线 Alpha 回测引擎（真·策略回测，区别于 main.py 的「历史数据加载」）。

为什么需要它：
  main.py --mode backtest 在回放历史时用 publish=False，bar.collected.m5 不发布，
  AutoRunner 的 Alpha→Risk→PM 整条链路在回测中【完全不运行】，只把历史 K 线写进
  Redis 供前端画图。因此无法用历史数据评估策略边际（胜率/期望/回撤）。

本脚本复用【真实】信号与风控代码：
  - signals.st_dema_m5.StDemaM5Engine   （和实盘同一套信号逻辑）
  - portfolio.risk_gate.RiskGate         （和实盘同一套门禁/以损定量/冷却/集中度）
确定性成交模拟器（SimPortfolio）只负责把 TradeIntent 变成可计量的成交，
忠实复刻 execution.auto_pm 的半止盈→移保本→止损/反手/EOD 平仓语义。

数据来源：Redis 中的 bars:5m:{sym}（由 main.py --mode backtest 写入，已含 st_value/st_dir）。
为支持跨日累积，运行时会把当前 Redis 的一日快照写入本地缓存目录，并把缓存里
所有日期合并回放，从而你可以「逐日 main.py 回测 → 反复跑本脚本」积累多日样本。

用法：
  /opt/anaconda3/bin/python3 scripts/backtest_alpha.py                 # 跑全部已加载标的
  /opt/anaconda3/bin/python3 scripts/backtest_alpha.py --symbols NVDA,AMD
  /opt/anaconda3/bin/python3 scripts/backtest_alpha.py --equity 100000 --risk-pct 0.002
  /opt/anaconda3/bin/python3 scripts/backtest_alpha.py --no-cache       # 只用当前 Redis 一日
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import redis as _redis
from nautilus_trader.model.enums import OrderSide

from execution.models import Unit, UnitState
from portfolio.config import PortfolioRiskConfig
from portfolio.risk_gate import RiskGate
from signals.base import BarContext, IntentAction, PositionContext
from signals.st_dema_m5 import StDemaM5Config, StDemaM5Engine

CACHE_DIR = ROOT / ".run" / "bt_cache"
RTH_OPEN_MIN = 9 * 60 + 30
RTH_CLOSE_MIN = 16 * 60
EOD_CLOSE_MIN = 15 * 60 + 45


# ─────────────────────────────────────────────────────────────────────────
# 成交模拟器：把 RiskGate 批准的 TradeIntent 变成可计量成交，复刻 auto_pm 语义
# ─────────────────────────────────────────────────────────────────────────
@dataclass
class Trade:
    sym: str
    side: str            # LONG / SHORT
    entry_time: int
    entry_px: float
    qty: int
    stop_px: float
    tp_px: float
    risk_per_share: float
    exit_time: int = 0
    exit_reason: str = ""
    pnl: float = 0.0
    r_multiple: float = 0.0
    hold_bars: int = 0
    mae_r: float = 0.0   # 最大不利偏移（R）：持仓期间最深浮亏 / 每股风险
    mfe_r: float = 0.0   # 最大有利偏移（R）：持仓期间最高浮盈 / 每股风险
    legs: list = field(default_factory=list)   # [(qty, px, reason)]


@dataclass
class _OpenPos:
    sym: str
    side: OrderSide
    qty: int                 # 当前剩余股数
    orig_qty: int
    entry_px: float
    stop_px: float
    tp_px: float
    tp_qty: int              # 半止盈目标股数
    risk_per_share: float
    entry_time: int
    seq: int = 0
    breakeven: bool = False
    tp_done: bool = False
    mae: float = 0.0     # 最深不利偏移（$/share，正数）
    mfe: float = 0.0     # 最高有利偏移（$/share，正数）
    last_time: int = 0
    legs: list = field(default_factory=list)


class SimPortfolio:
    """每标的最多一条持仓（可含 ADD 第二单元，简化为合并管理）。"""

    def __init__(self, cfg, exec_atr_mult: float, tp_rr: float,
                 slippage_bps: float, commission_ps: float) -> None:
        self._cfg = cfg
        self._atr_mult = exec_atr_mult
        self._tp_rr = tp_rr
        self._slip = slippage_bps / 10000.0
        self._commission_ps = commission_ps
        self.open_pos: dict[str, _OpenPos] = {}
        self.closed: list[Trade] = []
        self.cash_pnl = 0.0

    # RiskGate 需要的 units_fn：返回 {sym: [Unit,...]}
    def units_view(self) -> dict[str, list[Unit]]:
        out: dict[str, list[Unit]] = {}
        for sym, p in self.open_pos.items():
            state = UnitState.BREAKEVEN if p.breakeven else UnitState.ACTIVE
            out[sym] = [Unit(sym=sym, seq=p.seq, side=p.side, state=state,
                             qty=p.qty, entry_px=p.entry_px,
                             hard_stop_px=p.stop_px, risk_per_share=p.risk_per_share,
                             tp_px=p.tp_px)]
        return out

    def position_context(self, sym: str) -> PositionContext:
        p = self.open_pos.get(sym)
        if not p:
            return PositionContext(0, None, False, ())
        return PositionContext(
            open_units=1, is_long=(p.side == OrderSide.BUY),
            all_breakeven=p.breakeven, unit_sides=(p.side,),
        )

    def _buy_fill(self, px: float, side: OrderSide, opening: bool) -> float:
        # 开多/平空在买方，加滑点；开空/平多在卖方，减滑点
        d = self._slip * px
        if opening:
            return px + d if side == OrderSide.BUY else px - d
        return px - d if side == OrderSide.BUY else px + d  # 平仓方向相反

    def open(self, sym: str, side: OrderSide, qty: int, entry_px: float,
             stop_px: float, tp_px: float, bar_time: int, seq: int) -> None:
        fill = self._buy_fill(entry_px, side, opening=True)
        rps = abs(fill - stop_px)
        tp_qty = qty // 2
        if tp_qty < 1:
            tp_qty = qty   # qty==1 退化为全量止盈（与 auto_pm #10 一致）
        self.open_pos[sym] = _OpenPos(
            sym=sym, side=side, qty=qty, orig_qty=qty, entry_px=fill,
            stop_px=stop_px, tp_px=tp_px, tp_qty=tp_qty,
            risk_per_share=rps if rps > 0 else 1e-9, entry_time=bar_time, seq=seq,
            legs=[],
        )

    def _close_leg(self, p: _OpenPos, qty: int, px: float, reason: str) -> None:
        fill = self._buy_fill(px, p.side, opening=False)
        sign = 1 if p.side == OrderSide.BUY else -1
        pnl = sign * (fill - p.entry_px) * qty - self._commission_ps * qty
        self.cash_pnl += pnl
        p.legs.append((qty, fill, reason, pnl))
        p.qty -= qty

    def _finalize(self, sym: str, exit_time: int, reason: str) -> None:
        p = self.open_pos.pop(sym)
        total_pnl = sum(leg[3] for leg in p.legs)
        rps = p.risk_per_share if p.risk_per_share > 0 else 1e-9
        r = total_pnl / (rps * p.orig_qty)
        hold_bars = max(0, (exit_time - p.entry_time) // 300)
        self.closed.append(Trade(
            sym=sym, side=("LONG" if p.side == OrderSide.BUY else "SHORT"),
            entry_time=p.entry_time, entry_px=round(p.entry_px, 4), qty=p.orig_qty,
            stop_px=p.stop_px, tp_px=p.tp_px, risk_per_share=round(rps, 4),
            exit_time=exit_time, exit_reason=reason,
            pnl=round(total_pnl, 2), r_multiple=round(r, 3),
            hold_bars=hold_bars,
            mae_r=round(p.mae / rps, 3), mfe_r=round(p.mfe / rps, 3),
            legs=[(q, round(px, 4), rs) for q, px, rs, _ in p.legs],
        ))

    def manage_bar(self, sym: str, high: float, low: float, bar_time: int) -> Optional[str]:
        """用本根 bar 的区间检验挂着的止损/止盈。返回平仓原因（若已离场）。
        保守口径：同根 bar 内若止损与止盈都触及，按【止损优先】（最坏情形）。"""
        p = self.open_pos.get(sym)
        if not p:
            return None
        is_long = p.side == OrderSide.BUY
        p.last_time = bar_time
        # 持仓期间浮动偏移（按本根 bar 区间的最坏/最好价）
        if is_long:
            p.mae = max(p.mae, p.entry_px - low)
            p.mfe = max(p.mfe, high - p.entry_px)
        else:
            p.mae = max(p.mae, high - p.entry_px)
            p.mfe = max(p.mfe, p.entry_px - low)
        stop_hit = (low <= p.stop_px) if is_long else (high >= p.stop_px)
        tp_hit = (not p.tp_done) and ((high >= p.tp_px) if is_long else (low <= p.tp_px))

        if stop_hit:
            self._close_leg(p, p.qty, p.stop_px, "stop" if not p.breakeven else "breakeven_stop")
            self._finalize(sym, bar_time, "stop" if not p.breakeven else "breakeven_stop")
            return "stop"

        if tp_hit:
            self._close_leg(p, min(p.tp_qty, p.qty), p.tp_px, "tp")
            p.tp_done = True
            if p.qty <= 0:
                self._finalize(sym, bar_time, "tp_full")
                return "tp_full"
            # 半止盈后移保本
            p.stop_px = p.entry_px
            p.breakeven = True
        return None

    def close_market(self, sym: str, px: float, bar_time: int, reason: str) -> None:
        p = self.open_pos.get(sym)
        if not p:
            return
        self._close_leg(p, p.qty, px, reason)
        self._finalize(sym, bar_time, reason)


# ─────────────────────────────────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────────────────────────────────
def _session_date_of(bars: list[dict]) -> str:
    for b in bars:
        t = int(b.get("time", 0))
        if (t % 86400) // 60 >= RTH_OPEN_MIN:
            return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    if bars:
        t = int(bars[0]["time"])
        return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    return "unknown"


def load_from_redis(r, symbols: list[str]) -> dict[str, list[dict]]:
    data: dict[str, list[dict]] = {}
    for sym in symbols:
        try:
            raw = r.lrange(f"bars:5m:{sym}", 0, -1)
        except Exception:
            continue
        bars = []
        for x in raw:
            try:
                bars.append(json.loads(x))
            except Exception:
                continue
        if bars:
            data[sym] = bars
    return data


def snapshot_to_cache(data: dict[str, list[dict]]) -> Optional[str]:
    if not data:
        return None
    any_bars = next(iter(data.values()))
    date = _session_date_of(any_bars)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{date}.json"
    try:
        path.write_text(json.dumps(data))
    except Exception:
        return None
    return date


def load_all_cache() -> dict[str, dict[str, list[dict]]]:
    """返回 {date: {sym: bars}}。"""
    out: dict[str, dict[str, list[dict]]] = {}
    if not CACHE_DIR.exists():
        return out
    for f in sorted(CACHE_DIR.glob("*.json")):
        try:
            out[f.stem] = json.loads(f.read_text())
        except Exception:
            continue
    return out


# ─────────────────────────────────────────────────────────────────────────
# 回测主循环（忠实复刻 AutoRunner._on_m5_bar 的门禁顺序）
# ─────────────────────────────────────────────────────────────────────────
class Backtester:
    def __init__(self, args) -> None:
        self.args = args
        self.risk_cfg = PortfolioRiskConfig(
            risk_pct=args.risk_pct,
            max_position_pct=args.max_position_pct,
            max_portfolio_positions=args.max_portfolio_positions,
            rth_open_blackout_min=args.rth_open_blackout_min,
            pre_eod_blackout_min=args.pre_eod_blackout_min,
            cooldown_bars_after_stop=args.cooldown_bars,
            max_trades_per_sym_per_day=args.max_trades_per_day,
            min_qty=1,
            fixed_qty=args.fixed_qty,
            correlation_groups=_parse_groups(args.correlation_groups),
            max_per_correlation_group=args.max_per_group,
        )
        self.sim = SimPortfolio(
            self.risk_cfg, args.atr_mult, args.tp_rr,
            args.slippage_bps, args.commission_ps,
        )
        self.risk = RiskGate(
            self.risk_cfg, None, lambda: args.equity,
            self.sim.units_view, lambda *_: None,
        )
        self.engines: dict[str, StDemaM5Engine] = {}
        self.rejects: dict[str, int] = {}

    def _engine_for(self, sym: str) -> StDemaM5Engine:
        eng = self.engines.get(sym)
        if eng is None:
            eng = StDemaM5Engine(
                StDemaM5Config(
                    dema_period=self.args.dema_period,
                    atr_period=self.args.atr_period,
                    atr_mult=self.args.atr_mult,
                    tp_rr=self.args.tp_rr,
                    min_st_dema_spread_atr=self.args.min_spread_atr,
                    require_close_confirm=not self.args.no_close_confirm,
                ),
                None, lambda *_: None,
            )
            eng.register_symbol(sym)
            self.engines[sym] = eng
        return eng

    def run(self, days: dict[str, dict[str, list[dict]]]) -> None:
        # 跨日跨标的：所有 bar 按 (time, sym) 排序，时间序回放（共享组合风控）
        events: list[tuple[int, str, dict]] = []
        for date in sorted(days):
            for sym, bars in days[date].items():
                if self.args.symbols and sym not in self.args.symbols:
                    continue
                for b in bars:
                    events.append((int(b["time"]), sym, b))
        events.sort(key=lambda e: (e[0], e[1]))

        for bar_time, sym, bar in events:
            self._on_bar(sym, bar, bar_time)

        # 收盘强平所有未平仓（用各自最后价）
        for sym in list(self.sim.open_pos):
            p = self.sim.open_pos[sym]
            self.sim.close_market(sym, self.engines[sym]._last_close.get(sym, p.entry_px),
                                  bar_time, "backtest_end")

    def _on_bar(self, sym: str, bar: dict, bar_time: int) -> None:
        et_min = (bar_time % 86400) // 60
        try:
            c = float(bar["close"]); h = float(bar["high"]); lo = float(bar["low"])
        except (KeyError, TypeError, ValueError):
            return

        # 1) 先用本根 bar 区间检验在挂的止损/止盈（resting orders）
        self.sim.manage_bar(sym, h, lo, bar_time)

        # EOD：15:45 ET 之后只平仓，不再开新仓
        if et_min >= EOD_CLOSE_MIN:
            if sym in self.sim.open_pos:
                self.sim.close_market(sym, c, bar_time, "eod")
            return

        eng = self._engine_for(sym)
        pos_ctx = self.sim.position_context(sym)

        # 2) 喂引擎（指标在回放流上连续预热；引擎在指标未就绪时自然返回空）
        ctx = BarContext(symbol=sym, bar=bar, bar_time=bar_time, et_min=et_min, mode="observe")
        out = eng.on_bar(ctx, pos_ctx)

        for rj in out.rejects:
            self.rejects[rj.reason] = self.rejects.get(rj.reason, 0) + 1

        for intent in out.intents:
            if intent.action == IntentAction.EXIT:
                if sym in self.sim.open_pos:
                    self.sim.close_market(sym, c, bar_time, f"reversal:{intent.exit_reason}")
                continue
            # ENTER/ADD：复刻 AutoRunner —— 平仓态下需在入场窗口内
            if not pos_ctx.open_units and not self.risk.is_entry_window(et_min):
                continue
            verdict = self.risk.check_enter(intent, self.args.atr_mult,
                                            self.args.max_units, live=True)
            if not verdict.allowed:
                self.rejects[verdict.reason] = self.rejects.get(verdict.reason, 0) + 1
                continue
            self.risk.record_daily_trade(sym, bar_time)
            self.sim.open(sym, intent.side, verdict.qty, intent.ref_price,
                          float(intent.stop_px), float(intent.tp_px), bar_time, intent.seq)


def _parse_groups(s: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not s:
        return out
    for pair in s.split(","):
        if ":" in pair:
            sym, grp = pair.split(":", 1)
            out[sym.strip()] = grp.strip()
    return out


# ─────────────────────────────────────────────────────────────────────────
# 统计与报告
# ─────────────────────────────────────────────────────────────────────────
def _fmt_t(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d %H:%M")


def _compute_stats(bt: "Backtester") -> dict:
    trades = bt.sim.closed
    n = len(trades)
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    gross_win = sum(t.pnl for t in wins)
    gross_loss = -sum(t.pnl for t in losses)
    total_pnl = sum(t.pnl for t in trades)
    rs = [t.r_multiple for t in trades]
    avg_r = sum(rs) / n if n else 0.0
    win_rate = len(wins) / n if n else 0.0
    expectancy = total_pnl / n if n else 0.0
    pf = (gross_win / gross_loss) if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0)
    eq = 0.0; peak = 0.0; max_dd = 0.0
    for t in sorted(trades, key=lambda t: t.exit_time):
        eq += t.pnl; peak = max(peak, eq); max_dd = max(max_dd, peak - eq)
    return {
        "trades": n, "total_pnl": total_pnl, "win_rate": win_rate,
        "avg_r": avg_r, "expectancy": expectancy, "pf": pf, "max_dd": max_dd,
        "wins": len(wins), "losses": len(losses), "rs": rs,
    }


def report(bt: Backtester) -> dict:
    trades = bt.sim.closed
    n = len(trades)
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    gross_win = sum(t.pnl for t in wins)
    gross_loss = -sum(t.pnl for t in losses)
    total_pnl = sum(t.pnl for t in trades)
    rs = [t.r_multiple for t in trades]
    avg_r = sum(rs) / n if n else 0.0
    win_rate = len(wins) / n if n else 0.0
    expectancy = total_pnl / n if n else 0.0
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf") if gross_win > 0 else 0.0

    # 权益曲线 & 最大回撤（按平仓顺序）
    eq = 0.0; peak = 0.0; max_dd = 0.0
    ordered = sorted(trades, key=lambda t: t.exit_time)
    for t in ordered:
        eq += t.pnl
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)

    # 连胜连亏
    cur = 0; max_w = 0; max_l = 0
    for t in ordered:
        if t.pnl > 0:
            cur = cur + 1 if cur > 0 else 1
            max_w = max(max_w, cur)
        else:
            cur = cur - 1 if cur < 0 else -1
            max_l = min(max_l, cur)

    print("\n" + "=" * 70)
    print("  离线 Alpha 回测报告  (profile=st_dema_m5, 真实信号+风控代码)")
    print("=" * 70)
    print(f"  样本交易数      : {n}")
    print(f"  净盈亏(模拟$)   : {total_pnl:,.2f}")
    print(f"  胜率            : {win_rate*100:.1f}%  ({len(wins)}胜 / {len(losses)}负)")
    print(f"  平均 R          : {avg_r:.3f}")
    print(f"  期望/笔($)      : {expectancy:,.2f}")
    print(f"  盈亏比(PF)      : {pf:.2f}")
    print(f"  最大回撤($)     : {max_dd:,.2f}")
    print(f"  最大连胜/连亏   : {max_w} / {abs(max_l)}")
    if rs:
        print(f"  R 分布          : min={min(rs):.2f}  max={max(rs):.2f}")
    if n:
        avg_hold = sum(t.hold_bars for t in trades) / n
        avg_mae = sum(t.mae_r for t in trades) / n
        avg_mfe = sum(t.mfe_r for t in trades) / n
        print(f"  平均持仓        : {avg_hold:.1f} 根 M5 ({avg_hold*5:.0f} 分钟)")
        print(f"  平均 MAE/MFE(R) : {avg_mae:.2f} / {avg_mfe:.2f}  （持仓内平均最深浮亏/最高浮盈）")
    print("-" * 70)
    if n:
        print("  离场原因分布（笔数 | 净$ | 平均R）：")
        by_reason: dict[str, list] = {}
        for t in trades:
            key = t.exit_reason.split(":")[0]
            by_reason.setdefault(key, []).append(t)
        for reason, ts in sorted(by_reason.items(), key=lambda x: -len(x[1])):
            pnl = sum(t.pnl for t in ts)
            ar = sum(t.r_multiple for t in ts) / len(ts)
            print(f"    {reason:<22} {len(ts):>3} | {pnl:>9.2f} | {ar:>6.2f}")
        print("-" * 70)
        print("  多空拆分（笔数 | 胜率 | 净$ | 平均R）：")
        for sd in ("LONG", "SHORT"):
            ts = [t for t in trades if t.side == sd]
            if not ts:
                continue
            w = sum(1 for t in ts if t.pnl > 0) / len(ts)
            pnl = sum(t.pnl for t in ts)
            ar = sum(t.r_multiple for t in ts) / len(ts)
            print(f"    {sd:<6} {len(ts):>3} | {w*100:>5.1f}% | {pnl:>9.2f} | {ar:>6.2f}")
        print("-" * 70)
        # 各标的净盈亏（最差/最好 5）
        by_sym: dict[str, float] = {}
        for t in trades:
            by_sym[t.sym] = by_sym.get(t.sym, 0.0) + t.pnl
        ranked = sorted(by_sym.items(), key=lambda x: x[1])
        worst = ranked[:5]; best = ranked[-5:][::-1]
        print("  标的净$  最差5: " + ", ".join(f"{s}:{v:.0f}" for s, v in worst))
        print("           最好5: " + ", ".join(f"{s}:{v:.0f}" for s, v in best))
        print("-" * 70)
    if bt.rejects:
        print("  拒绝原因分布：")
        for reason, cnt in sorted(bt.rejects.items(), key=lambda x: -x[1]):
            print(f"    {reason:<28} {cnt}")
        print("-" * 70)
    if trades:
        print("  成交明细（前 25 笔）：")
        print(f"    {'标的':<6}{'方向':<5}{'入场':<12}{'出场':<12}{'股':<5}{'R':>7}{'PnL$':>10}  原因")
        for t in trades[:25]:
            print(f"    {t.sym:<6}{t.side:<5}{_fmt_t(t.entry_time):<12}"
                  f"{_fmt_t(t.exit_time):<12}{t.qty:<5}{t.r_multiple:>7.2f}"
                  f"{t.pnl:>10.2f}  {t.exit_reason}")
    print("=" * 70 + "\n")

    summary = {
        "trades": n, "total_pnl": round(total_pnl, 2),
        "win_rate": round(win_rate, 4), "avg_r": round(avg_r, 3),
        "expectancy": round(expectancy, 2), "profit_factor": round(pf, 3)
        if pf != float("inf") else None,
        "max_drawdown": round(max_dd, 2),
        "max_consec_wins": max_w, "max_consec_losses": abs(max_l),
        "rejects": bt.rejects,
        "detail": [t.__dict__ for t in trades],
    }
    out_path = ROOT / ".run" / "logs" / "backtest_report.json"
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"  报告已写入: {out_path}")
    except Exception:
        pass
    return summary


def run_sweep(base_args, days: dict) -> None:
    import copy
    tp_grid = [0.8, 1.0, 1.2, 1.5, 2.0]
    atr_grid = [1.0, 1.5, 2.0]
    pos_grid = [3, 5, 8]
    rows = []
    for tp in tp_grid:
        for am in atr_grid:
            for mp in pos_grid:
                a = copy.copy(base_args)
                a.tp_rr = tp; a.atr_mult = am; a.max_portfolio_positions = mp
                bt = Backtester(a)
                bt.run(days)
                s = _compute_stats(bt)
                rows.append((tp, am, mp, s))
    # 按期望/笔排序
    rows.sort(key=lambda r: r[3]["expectancy"], reverse=True)
    print("\n" + "=" * 86)
    print("  参数扫描（按 期望/笔 降序）  grid: tp_rr × atr_mult × 组合位上限")
    print("=" * 86)
    print(f"  {'tp_rr':>6}{'atrM':>6}{'posN':>6}{'交易':>6}{'胜率':>8}"
          f"{'均R':>8}{'期望$':>9}{'PF':>7}{'净$':>10}{'回撤$':>10}")
    print("-" * 86)
    for tp, am, mp, s in rows:
        pf = s["pf"]; pf_s = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"  {tp:>6.1f}{am:>6.1f}{mp:>6d}{s['trades']:>6d}"
              f"{s['win_rate']*100:>7.1f}%{s['avg_r']:>8.3f}{s['expectancy']:>9.2f}"
              f"{pf_s:>7}{s['total_pnl']:>10.1f}{s['max_dd']:>10.1f}")
    print("=" * 86)
    best = rows[0]
    print(f"\n  最优(按期望): tp_rr={best[0]} atr_mult={best[1]} 组合位={best[2]} "
          f"→ 期望/笔=${best[3]['expectancy']:.2f} 均R={best[3]['avg_r']:.3f} "
          f"PF={'inf' if best[3]['pf']==float('inf') else round(best[3]['pf'],2)}\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="离线 Alpha 回测（真实信号+风控）")
    ap.add_argument("--symbols", default="", help="逗号分隔，留空=全部已加载标的")
    ap.add_argument("--equity", type=float, default=100000.0)
    ap.add_argument("--risk-pct", type=float, default=0.002)
    ap.add_argument("--max-position-pct", type=float, default=0.10)
    ap.add_argument("--max-portfolio-positions", type=int, default=3)
    ap.add_argument("--rth-open-blackout-min", type=int, default=15)
    ap.add_argument("--pre-eod-blackout-min", type=int, default=30)
    ap.add_argument("--cooldown-bars", type=int, default=3)
    ap.add_argument("--max-trades-per-day", type=int, default=3)
    ap.add_argument("--fixed-qty", type=int, default=0)
    ap.add_argument("--correlation-groups", default="",
                    help="如 NVDA:semis,AMD:semis,MU:semis,AVGO:semis")
    ap.add_argument("--max-per-group", type=int, default=0)
    ap.add_argument("--atr-mult", type=float, default=1.5)
    ap.add_argument("--tp-rr", type=float, default=2.0)
    ap.add_argument("--max-units", type=int, default=2)
    ap.add_argument("--dema-period", type=int, default=21)
    ap.add_argument("--atr-period", type=int, default=14)
    ap.add_argument("--min-spread-atr", type=float, default=0.30)
    ap.add_argument("--no-close-confirm", action="store_true")
    ap.add_argument("--slippage-bps", type=float, default=1.0)
    ap.add_argument("--commission-ps", type=float, default=0.0)
    ap.add_argument("--no-cache", action="store_true", help="只用当前 Redis 一日，不累积缓存")
    ap.add_argument("--sweep", action="store_true", help="参数扫描模式(tp_rr×atr_mult×组合位)")
    ap.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "localhost"))
    ap.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    args = ap.parse_args()
    args.symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    r = _redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    all_syms = args.symbols or sorted({
        k.split(":")[-1] for k in r.scan_iter("bars:5m:*")
    })
    today = load_from_redis(r, all_syms)
    if not today:
        print("Redis 中无 bars:5m 数据。请先 main.py --mode backtest 加载历史，再跑本脚本。")
        sys.exit(1)

    if args.no_cache:
        days = {_session_date_of(next(iter(today.values()))): today}
    else:
        snapped = snapshot_to_cache(today)
        days = load_all_cache()
        if not days:
            days = {snapped or "today": today}

    print(f"回测数据：{len(days)} 个交易日 {sorted(days)} | 标的={all_syms}")
    if args.sweep:
        run_sweep(args, days)
        return
    bt = Backtester(args)
    bt.run(days)
    report(bt)


if __name__ == "__main__":
    main()
