"""
data_feeder.py — 【离线调试工具】仅用于生成模拟数据供前端 UI 调试。

⚠️  真实交易数据来源：
    - 历史 K 线：由 strategy.py（鹦鹉螺引擎）在 on_start() 通过 IBKR request_bars() 拉取
    - 实时 K 线：由 strategy.py on_bar() 实时写入 Redis
    - 无需外部数据源（已移除 yfinance 依赖）

运行方式（仅限离线调试）:
    python data_feeder.py --all --mock   # 生成四个标的的模拟数据写入 Redis
    python data_feeder.py --mock         # 单标的模拟

实盘启动只需:
    python main.py                       # 连接 IBKR，自动拉取历史+实时
"""
import argparse
import json
import math
import sys

import redis


# ---------------------------------------------------------------------------
# 指标计算
# ---------------------------------------------------------------------------

def calc_ema(closes: list[float], period: int) -> list[float | None]:
    """计算 EMA，前 period-1 根返回 None"""
    result = [None] * len(closes)
    if len(closes) < period:
        return result
    k = 2 / (period + 1)
    # 用前 period 根的 SMA 作为第一个 EMA 值
    sma = sum(closes[:period]) / period
    result[period - 1] = sma
    for i in range(period, len(closes)):
        result[i] = closes[i] * k + result[i - 1] * (1 - k)
    return result


def calc_atr(bars: list[dict], period: int) -> list[float | None]:
    """计算 ATR"""
    trs = [None]
    for i in range(1, len(bars)):
        h = bars[i]["high"]
        l = bars[i]["low"]
        pc = bars[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    atrs = [None] * len(bars)
    if len(bars) < period + 1:
        return atrs

    # 第一个 ATR = 前 period 个 TR 的平均（跳过第一个 None）
    init_sum = sum(trs[1:period + 1])
    atrs[period] = init_sum / period
    for i in range(period + 1, len(bars)):
        atrs[i] = (atrs[i - 1] * (period - 1) + trs[i]) / period
    return atrs


def calc_supertrend(bars: list[dict], period: int = 10, multiplier: float = 3.0):
    """
    计算 SuperTrend，返回 (upper_list, lower_list, direction_list)
      direction: 1=多头（显示下轨绿线），-1=空头（显示上轨红线）
    """
    n = len(bars)
    atrs = calc_atr(bars, period)

    final_upper = [None] * n
    final_lower = [None] * n
    direction =   [None] * n   # 1=up, -1=down
    supertrend =  [None] * n

    for i in range(period, n):
        if atrs[i] is None:
            continue

        hl2 = (bars[i]["high"] + bars[i]["low"]) / 2
        basic_upper = hl2 + multiplier * atrs[i]
        basic_lower = hl2 - multiplier * atrs[i]

        # Final Upper
        if i == period:
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            # 初始方向
            direction[i] = 1 if bars[i]["close"] > basic_upper else -1
        else:
            prev_upper = final_upper[i - 1]
            prev_lower = final_lower[i - 1]
            prev_close = bars[i - 1]["close"]
            prev_dir   = direction[i - 1]

            # 收紧上轨（只能单向移动）
            if prev_upper is not None:
                final_upper[i] = basic_upper if (
                    basic_upper < prev_upper or prev_close > prev_upper
                ) else prev_upper
            else:
                final_upper[i] = basic_upper

            if prev_lower is not None:
                final_lower[i] = basic_lower if (
                    basic_lower > prev_lower or prev_close < prev_lower
                ) else prev_lower
            else:
                final_lower[i] = basic_lower

            # 方向判断
            if prev_dir == -1 and bars[i]["close"] > final_upper[i]:
                direction[i] = 1
            elif prev_dir == 1 and bars[i]["close"] < final_lower[i]:
                direction[i] = -1
            else:
                direction[i] = prev_dir

        # SuperTrend 值 = 下轨（多头）或 上轨（空头）
        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

    return supertrend, direction, final_upper, final_lower


def enrich_bars(bars: list[dict], st_period=10, st_mult=3.0, ema_period=21) -> list[dict]:
    """给 K 线计算所有指标并附加到每根 bar"""
    closes = [b["close"] for b in bars]

    # EMA21
    ema21 = calc_ema(closes, ema_period)

    # SuperTrend
    st_values, st_dirs, st_upper, st_lower = calc_supertrend(bars, st_period, st_mult)

    enriched = []
    for i, bar in enumerate(bars):
        b = dict(bar)
        # EMA21
        b["ema21"] = round(ema21[i], 4) if ema21[i] is not None else None
        # SuperTrend
        b["st_value"]  = round(st_values[i], 4) if st_values[i] is not None else None
        b["st_dir"]    = st_dirs[i]        # 1=多 -1=空 None=无效
        b["st_upper"]  = round(st_upper[i], 4) if st_upper[i] is not None else None
        b["st_lower"]  = round(st_lower[i], 4) if st_lower[i] is not None else None
        enriched.append(b)

    return enriched


def aggregate_to_m5(m1_bars: list[dict]) -> list[dict]:
    """将 1 分钟 K 线聚合为 5 分钟 K 线（按时间戳 300s 对齐，避免边界错位）"""
    from collections import defaultdict
    buckets: dict[int, list] = {}
    for bar in m1_bars:
        # 将时间戳向下对齐到 300s（5分钟）边界
        bucket_key = bar["time"] - (bar["time"] % 300)
        buckets.setdefault(bucket_key, []).append(bar)

    m5_bars = []
    for t in sorted(buckets):
        chunk = buckets[t]
        m5_bars.append({
            "time":   t,
            "open":   chunk[0]["open"],
            "high":   max(b["high"] for b in chunk),
            "low":    min(b["low"]  for b in chunk),
            "close":  chunk[-1]["close"],
            "volume": sum(b["volume"] for b in chunk),
        })
    return m5_bars


# ---------------------------------------------------------------------------
# 数据获取
# ---------------------------------------------------------------------------

def fetch_real_data(symbol: str, target_date: str | None = None) -> list[dict]:
    """用 yfinance 获取指定交易日的 1 分钟 K 线

    参数
    ----
    symbol      : 股票代码，如 'QQQ'
    target_date : 指定日期字符串 'YYYY-MM-DD'，None 则自动选最近交易日
    """
    try:
        import yfinance as yf
        import pytz
        import datetime as _dt

        ny_tz = pytz.timezone("America/New_York")
        now = _dt.datetime.now(ny_tz)

        if target_date:
            # 使用指定日期
            target = _dt.date.fromisoformat(target_date)
            print(f"   指定日期: {target}")
        else:
            # 自动选最近已收盘的交易日
            today = now.date()
            market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            if now.weekday() < 5 and now >= market_close:
                target = today
            else:
                target = today - _dt.timedelta(days=1)
                while target.weekday() >= 5:
                    target -= _dt.timedelta(days=1)
            print(f"   目标日期: {target} (ET 当前: {now.strftime('%m-%d %H:%M')})")

        start_str = target.strftime("%Y-%m-%d")
        end_str   = (target + _dt.timedelta(days=1)).strftime("%Y-%m-%d")

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_str, end=end_str, interval="1m", prepost=False)

        if df.empty:
            # 节假日 fallback：往前找
            for n in range(1, 6):
                fallback = target - _dt.timedelta(days=n)
                while fallback.weekday() >= 5:
                    fallback -= _dt.timedelta(days=1)
                fb_start = fallback.strftime("%Y-%m-%d")
                fb_end   = (fallback + _dt.timedelta(days=1)).strftime("%Y-%m-%d")
                df = ticker.history(start=fb_start, end=fb_end, interval="1m", prepost=False)
                if not df.empty:
                    target = fallback
                    print(f"   节假日 fallback → {fallback}")
                    break

        if df.empty:
            raise ValueError("yfinance 返回空数据")

        df.index = df.index.tz_convert(ny_tz)
        result = []
        for ts, row in df.iterrows():
            # 使用 NY 本地时间作为假·UTC（chart X轴直接显示 ET 时间）
            utc_offset_secs = int(ts.utcoffset().total_seconds())
            et_fake_utc = int(ts.timestamp()) + utc_offset_secs
            result.append({
                "time":   et_fake_utc,
                "open":   round(float(row["Open"]),   4),
                "high":   round(float(row["High"]),   4),
                "low":    round(float(row["Low"]),    4),
                "close":  round(float(row["Close"]),  4),
                "volume": int(row["Volume"]),
            })

        result.sort(key=lambda x: x["time"])
        print(f"✅ yfinance 获取 {len(result)} 根真实 K 线 [{symbol} {target}]")
        return result

    except Exception as e:
        print(f"⚠️  yfinance 获取失败: {e}，改用模拟数据")
        return generate_mock_bars(symbol)


def generate_mock_bars(symbol: str) -> list[dict]:
    """生成昨日模拟数据"""
    import random
    import pytz

    BASE_PRICES = {
        "QQQ": 480.0, "AAPL": 220.0, "NVDA": 850.0, "MSFT": 400.0,
        "AMZN": 185.0, "GOOGL": 175.0, "META": 540.0, "TSLA": 250.0,
    }
    base = BASE_PRICES.get(symbol, 200.0)

    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)
    yesterday = now - timedelta(days=1)
    while yesterday.weekday() >= 5:
        yesterday -= timedelta(days=1)
    start_dt = yesterday.replace(hour=9, minute=30, second=0, microsecond=0)

    bars = []
    price = base
    trend = 0.0
    random.seed(42)

    for i in range(390):
        dt = start_dt + timedelta(minutes=i)
        ts = int(dt.timestamp())

        trend += random.gauss(0, 0.001)
        trend = max(-0.005, min(0.005, trend))
        change_pct = random.gauss(trend, 0.0015)

        o = round(price, 2)
        c = round(o * (1 + change_pct), 2)
        h = round(max(o, c) * (1 + abs(random.gauss(0, 0.0008))), 2)
        l = round(min(o, c) * (1 - abs(random.gauss(0, 0.0008))), 2)
        v = max(1000, int(random.gauss(50000, 15000)))

        bars.append({"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        price = c

    print(f"✅ 生成 {len(bars)} 根模拟 K 线 ({symbol}，起始价={base})")
    return bars


def create_mock_position(symbol: str, bars: list[dict]) -> dict:
    """生成测试仓位（开仓在数据中间偏早位置）"""
    entry_idx = len(bars) // 4
    entry_bar = bars[entry_idx]
    entry_price = round(entry_bar["close"], 2)
    current_price = round(bars[-1]["close"], 2)
    stop_loss = round(entry_price * 0.99, 2)
    quantity = 1
    pnl = round((current_price - entry_price) * quantity, 2)

    return {
        "symbol":        symbol,
        "entry_price":   entry_price,
        "entry_time":    entry_bar["time"],
        "stop_loss":     stop_loss,
        "quantity":      quantity,
        "current_price": current_price,
        "pnl":           pnl,
        "pnl_pct":       round((current_price - entry_price) / entry_price * 100, 2),
    }


def write_to_redis(symbol: str, m1_bars: list[dict], m5_bars: list[dict],
                   position: dict, r: redis.Redis) -> None:
    """写 K 线（含指标）和仓位数据到 Redis"""
    r.set(f"bars:1m:{symbol}", json.dumps(m1_bars))
    r.set(f"bars:5m:{symbol}", json.dumps(m5_bars))
    # 保留已有仓位，不覆盖 Redis 里现存的真实仓位
    existing_pos = r.get(f"position:{symbol}")
    if not existing_pos:
        r.set(f"position:{symbol}", json.dumps(position))
        print(f"   仓位: position:{symbol} → entry={position['entry_price']} SL={position['stop_loss']}")
    else:
        print(f"   仓位: position:{symbol} 已存在，保留不覆盖")

    print(f"✅ Redis 写入:")
    print(f"   M1: bars:1m:{symbol} → {len(m1_bars)} 根")
    print(f"   M5: bars:5m:{symbol} → {len(m5_bars)} 根")
    print(f"   仓位: position:{symbol} → entry={position['entry_price']} SL={position['stop_loss']} PnL={position['pnl']}")


# ---------------------------------------------------------------------------
# 主函数
# ---------------------------------------------------------------------------

ALL_SYMBOLS = ["QQQ", "AAPL", "NVDA", "TSLA"]


def process_symbol(symbol: str, r: redis.Redis, target_date: str | None = None) -> None:
    """处理单个标的：从 yfinance 获取真实 K 线 → 计算指标 → 写入 Redis

    ⚠️ 项目要求：禁止使用模拟数据，所有 K 线必须来自真实市场数据（yfinance）。
    """
    print(f"\n{'='*50}")
    print(f"  处理标的: {symbol}" + (f"  日期: {target_date}" if target_date else ""))
    print(f"{'='*50}")

    # 1. 获取 M1 真实数据（yfinance）
    raw_bars = fetch_real_data(symbol, target_date=target_date)

    # 2. 计算 M1 指标（SuperTrend 10,2 + EMA21）
    print("计算 M1 指标 (SuperTrend 10,2 + EMA21)...")
    m1_bars = enrich_bars(raw_bars, st_period=10, st_mult=2.0, ema_period=21)

    # 3. 聚合 M5 并计算指标
    print("聚合 M5 并计算指标...")
    m5_raw = aggregate_to_m5(raw_bars)
    m5_bars = enrich_bars(m5_raw, st_period=10, st_mult=2.0, ema_period=21)

    # 4. 创建测试仓位
    position = create_mock_position(symbol, raw_bars)

    # 5. 写 Redis
    write_to_redis(symbol, m1_bars, m5_bars, position, r)


def main():
    # ⚠️ 项目要求：禁止模拟数据 —— 所有 K 线数据必须来自 yfinance 真实市场数据
    parser = argparse.ArgumentParser(
        description="从 yfinance 拉取真实 K 线写入 Redis（禁止使用模拟数据）"
    )
    parser.add_argument("--symbol", default="QQQ", help="单个标的，如 AAPL")
    parser.add_argument("--all", action="store_true",
                        help=f"加载所有默认标的: {ALL_SYMBOLS}")
    parser.add_argument("--date", default=None,
                        help="指定交易日期 YYYY-MM-DD，默认自动选最近已收盘交易日")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    try:
        r.ping()
        print(f"✅ Redis 连接: {args.redis_host}:{args.redis_port}")
    except Exception as e:
        print(f"❌ Redis 连接失败: {e}")
        sys.exit(1)

    print("⚠️  本项目禁止使用模拟数据，所有 K 线来自 yfinance 真实市场数据")

    # 确定要处理的标的列表
    symbols = ALL_SYMBOLS if args.all else [args.symbol.upper()]

    for sym in symbols:
        process_symbol(sym, r, target_date=args.date)

    print(f"\n{'='*50}")
    print(f"✅ 全部完成！共加载标的: {', '.join(symbols)}"
          + (f"  日期: {args.date}" if args.date else ""))
    print(f"   启动前端: cd frontend && node server.js")
    print(f"   浏览器: http://localhost:3000?symbol=QQQ")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
