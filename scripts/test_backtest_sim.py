#!/usr/bin/env python3
"""SimPortfolio 成交模拟器单测 —— 锁定离线回测的成交语义与 auto_pm 对齐。

运行: /opt/anaconda3/bin/python3 scripts/test_backtest_sim.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from nautilus_trader.model.enums import OrderSide
from portfolio.config import PortfolioRiskConfig

import importlib.util
spec = importlib.util.spec_from_file_location("backtest_alpha", ROOT / "scripts" / "backtest_alpha.py")
bt = importlib.util.module_from_spec(spec)
sys.modules["backtest_alpha"] = bt   # dataclass 注解解析需模块在 sys.modules 中
spec.loader.exec_module(bt)
SimPortfolio = bt.SimPortfolio

PASS = 0
FAIL = 0


def check(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {extra}")


def new_sim(slip=0.0, comm=0.0) -> SimPortfolio:
    return SimPortfolio(PortfolioRiskConfig(), exec_atr_mult=1.5, tp_rr=2.0,
                        slippage_bps=slip, commission_ps=comm)


def test_long_tp_then_breakeven():
    print("[1] 多头半止盈 → 移保本 → 保本止损")
    s = new_sim()
    s.open("X", OrderSide.BUY, qty=10, entry_px=100.0, stop_px=98.0, tp_px=104.0,
           bar_time=1000, seq=0)
    # bar1: 触及 TP(104)，半止盈 5 股，移保本到 100
    r = s.manage_bar("X", high=104.5, low=99.0, bar_time=1300)
    check("TP 触发后仍持仓(半止盈)", r is None and "X" in s.open_pos)
    check("剩余 5 股", s.open_pos["X"].qty == 5)
    check("已移保本(stop=entry)", abs(s.open_pos["X"].stop_px - 100.0) < 1e-9)
    # bar2: 跌到保本价 100，剩余平在 100
    r = s.manage_bar("X", high=101.0, low=100.0, bar_time=1600)
    check("保本止损平仓", r == "stop")
    t = s.closed[-1]
    # 盈亏 = (104-100)*5 + (100-100)*5 = 20 ; R = 20/(2*10)=1.0
    check("净盈亏=20", abs(t.pnl - 20.0) < 1e-6, f"got {t.pnl}")
    check("R=1.0", abs(t.r_multiple - 1.0) < 1e-6, f"got {t.r_multiple}")


def test_long_stop_out():
    print("[2] 多头止损 ≈ -1R")
    s = new_sim()
    s.open("Y", OrderSide.BUY, qty=10, entry_px=100.0, stop_px=98.0, tp_px=104.0,
           bar_time=1000, seq=0)
    r = s.manage_bar("Y", high=99.0, low=97.5, bar_time=1300)
    check("止损平仓", r == "stop")
    t = s.closed[-1]
    check("净盈亏=-20", abs(t.pnl + 20.0) < 1e-6, f"got {t.pnl}")
    check("R=-1.0", abs(t.r_multiple + 1.0) < 1e-6, f"got {t.r_multiple}")


def test_qty1_full_tp():
    print("[3] 1 股仓位退化为全量止盈(与 auto_pm #10 一致)")
    s = new_sim()
    s.open("Z", OrderSide.BUY, qty=1, entry_px=100.0, stop_px=98.0, tp_px=104.0,
           bar_time=1000, seq=0)
    check("tp_qty 退化为全量 1", s.open_pos["Z"].tp_qty == 1)
    r = s.manage_bar("Z", high=104.2, low=100.0, bar_time=1300)
    check("全量止盈平仓", r == "tp_full")
    check("仓位已清", "Z" not in s.open_pos)
    t = s.closed[-1]
    check("净盈亏=4", abs(t.pnl - 4.0) < 1e-6, f"got {t.pnl}")


def test_short_stop_out():
    print("[4] 空头止损 ≈ -1R")
    s = new_sim()
    s.open("S", OrderSide.SELL, qty=10, entry_px=100.0, stop_px=102.0, tp_px=96.0,
           bar_time=1000, seq=0)
    r = s.manage_bar("S", high=102.5, low=101.0, bar_time=1300)
    check("空头止损平仓", r == "stop")
    t = s.closed[-1]
    check("净盈亏=-20", abs(t.pnl + 20.0) < 1e-6, f"got {t.pnl}")
    check("R=-1.0", abs(t.r_multiple + 1.0) < 1e-6, f"got {t.r_multiple}")


def test_reversal_close():
    print("[5] 反手/市价平仓")
    s = new_sim()
    s.open("R", OrderSide.BUY, qty=10, entry_px=100.0, stop_px=98.0, tp_px=104.0,
           bar_time=1000, seq=0)
    s.close_market("R", px=102.0, bar_time=1300, reason="reversal:x")
    t = s.closed[-1]
    check("净盈亏=20", abs(t.pnl - 20.0) < 1e-6, f"got {t.pnl}")
    check("离场原因=reversal:x", t.exit_reason == "reversal:x")


def test_mae_mfe():
    print("[6] MAE/MFE 偏移跟踪")
    s = new_sim()
    s.open("M", OrderSide.BUY, qty=10, entry_px=100.0, stop_px=90.0, tp_px=120.0,
           bar_time=1000, seq=0)
    s.manage_bar("M", high=105.0, low=95.0, bar_time=1300)  # 不触及止损/止盈
    s.close_market("M", px=100.0, bar_time=1600, reason="eod")
    t = s.closed[-1]
    # rps=10 → mae=5→0.5R, mfe=5→0.5R
    check("MAE≈0.5R", abs(t.mae_r - 0.5) < 1e-6, f"got {t.mae_r}")
    check("MFE≈0.5R", abs(t.mfe_r - 0.5) < 1e-6, f"got {t.mfe_r}")
    check("持仓时长=2根", t.hold_bars == 2, f"got {t.hold_bars}")


def test_slippage_commission():
    print("[7] 滑点+手续费纳入盈亏")
    s = new_sim(slip=10.0, comm=0.01)  # 10bps 滑点
    s.open("C", OrderSide.BUY, qty=10, entry_px=100.0, stop_px=98.0, tp_px=104.0,
           bar_time=1000, seq=0)
    # 开仓滑点：买入成交价=100*(1+0.001)=100.1
    check("开仓含滑点", abs(s.open_pos["C"].entry_px - 100.1) < 1e-6,
          f"got {s.open_pos['C'].entry_px}")


def main() -> None:
    test_long_tp_then_breakeven()
    test_long_stop_out()
    test_qty1_full_tp()
    test_short_stop_out()
    test_reversal_close()
    test_mae_mfe()
    test_slippage_commission()
    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
