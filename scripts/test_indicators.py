#!/usr/bin/env python3
"""第一部分：指标数据层自检（Redis bars / indicators:active / touch）。"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nautilus_mcp import redis_io
from signals.touch_detector import detect_m1_touch_only

SYMBOLS = list(redis_io.DEFAULT_SYMBOLS)
REQUIRED_M1 = ("time", "open", "high", "low", "close", "vwap")
REQUIRED_M5 = ("time", "close", "st_value", "st_dir", "dema20")
REQUIRED_ACTIVE = ("m5_bar_time", "supertrend", "dema20", "updated_at")


def _fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")


def _ok(msg: str) -> None:
    print(f"  [OK]   {msg}")


def _warn(msg: str) -> None:
    print(f"  [WARN] {msg}")


def check_symbol(r, sym: str) -> bool:
    ok = True
    print(f"\n── {sym} ──")

    m1_list = redis_io.get_bars(r, "1m", sym, limit=3)
    m5_list = redis_io.get_bars(r, "5m", sym, limit=30)
    active = redis_io.get_indicators_active(r, sym)

    if not m1_list:
        _fail("bars:1m 无数据（引擎未跑或标的未订阅）")
        return False
    last_m1 = m1_list[-1]
    for k in REQUIRED_M1:
        if last_m1.get(k) is None:
            _fail(f"M1 缺字段 {k}")
            ok = False
    if ok:
        _ok(
            f"M1 close={last_m1['close']} vwap={last_m1.get('vwap')} "
            f"time={last_m1.get('time')}"
        )

    if not m5_list:
        _fail("bars:5m 无数据")
        ok = False
    else:
        last_m5 = m5_list[-1]
        for k in REQUIRED_M5:
            if last_m5.get(k) is None:
                _fail(f"M5 缺字段 {k}")
                ok = False
        if ok:
            _ok(
                f"M5 ST={last_m5.get('st_value')} dir={last_m5.get('st_dir')} "
                f"dema20={last_m5.get('dema20')}"
            )

    if not active:
        _fail("indicators:active 缺失（等 M5 收盘）")
        ok = False
    else:
        for k in REQUIRED_ACTIVE:
            if k not in active:
                _fail(f"indicators:active 缺字段 {k}")
                ok = False
        st = active.get("supertrend") or {}
        if st.get("value") is None or st.get("dir") not in (1, -1):
            _fail("indicators:active.supertrend 无效")
            ok = False
        elif m5_list:
            m5_time = int(active.get("m5_bar_time") or 0)
            frozen = next((b for b in reversed(m5_list) if int(b.get("time") or 0) == m5_time), None)
            ref = frozen or last_m5
            lv = float(ref.get("st_value") or 0)
            av = float(st.get("value") or 0)
            if abs(lv - av) > 0.02:
                _warn(f"active ST {av} 与 M5@{m5_time} ST {lv} 不一致")
            else:
                _ok(
                    f"indicators:active 冻结 m5_time={m5_time} "
                    f"ST={av} dir={st.get('dir')} dema20={active.get('dema20')}"
                )

    # 触线检测干跑：用最新 M1 + active，不写入 Redis
    if m1_list and active:
        prev = m1_list[-2] if len(m1_list) >= 2 else None
        touches = detect_m1_touch_only(sym, last_m1, prev, active)
        if touches:
            t = touches[0]
            _ok(
                f"干跑触线 {t.signal_type} {t.side} @ {t.trigger_level} "
                f"reclaim={t.reclaim}"
            )
        else:
            _ok("干跑触线：当前 M1 未碰 VWAP/ST/DEMA")

    raw_touch = r.lrange(f"signals:touch:{sym}", -1, -1)
    if raw_touch:
        try:
            t0 = json.loads(raw_touch[0])
            _ok(f"Redis 触线记录 {t0.get('signal_type')} @ {t0.get('trigger_level')}")
        except json.JSONDecodeError:
            _warn("signals:touch 最新条 JSON 损坏")
    else:
        _ok("今日尚无触线写入（正常）")

    return ok


def main() -> int:
    print("=== 第一部分：指标数据自检 ===\n")
    try:
        r = redis_io.get_redis()
        _ok("Redis 连接")
    except Exception as e:
        _fail(f"Redis: {e}")
        return 1

    health = redis_io.stack_health(r)
    hb = health.get("engine_heartbeat")
    age = health.get("engine_heartbeat_age_s")
    if health.get("engine_online"):
        _ok(f"引擎在线 heartbeat age={age}s ts={hb.get('ts') if isinstance(hb, dict) else hb}")
    elif hb:
        _warn(f"引擎离线 heartbeat age={age}s（阈值 {health.get('engine_heartbeat_max_age_s')}s）")
    else:
        _warn("engine:heartbeat 缺失（需 python main.py 且重启后写入 Redis key）")

    pending = health.get("pending_proposals", 0)
    _ok(f"pending proposals={pending}")

    touches = redis_io.list_recent_touches(r, limit=3)
    _ok(f"signals:touch:index 最近 {len(touches)} 条")

    all_ok = True
    for sym in SYMBOLS:
        if not check_symbol(r, sym):
            all_ok = False

    print("\n=== 第二部分 MCP（手动）===")
    print("  .venv/bin/python3 nautilus_mcp/self_test.py")
    print("  Cursor MCP alpha → get_alpha_snapshot")

    print("\n=== 第三部分 Skills（手动）===")
    print("  新 Chat 只发: alpha")
    print("  期望: get_alpha_snapshot → 无新触线则 NO_OP")

    print(f"\n{'[PASS] 指标数据层' if all_ok else '[FAIL] 见上方 FAIL'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
