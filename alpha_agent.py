"""
AlphaAgent — M1 触线 → 条件执行审批建议

⚠️ LEGACY：推荐改用 Cursor + MCP（mcp/server.py）+ Skill（.cursor/skills/nautilus-alpha）。
本脚本保留作无 Cursor 时的后备，需单独配置 DEEPSEEK_API_KEY。

每 60s（M1）读取 Redis M1 K 线 + indicators:active，检测 VWAP/ST/DEMA20 触线，
计算结构止损 / 半仓止盈 / R:R，写入待审批池。批准后不立即下单，等待 Reclaim（由后续执行层实现）。
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import redis

from signals.indicators import session_vwap
from signals.pullback_pricing import (
    compute_pullback_pricing,
    reclaim_label,
    reclaim_rule_for_side,
)
from approval.proposal_store import PROPOSAL_REDIS_RETENTION_SECONDS
from approval.alpha_scan import run_incremental_scan
from signals.touch_detector import TouchEvent, dedup_key, detect_m1_touch_only


def _load_dotenv(path: str = ".env") -> None:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        pass


_load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

ALPHA_SYMBOLS = tuple(
    s.strip().upper()
    for s in os.environ.get("ALPHA_SYMBOLS", "NVDA,TSLA,AAPL").split(",")
    if s.strip()
)
POLL_SECONDS = int(os.environ.get("ALPHA_POLL_SECONDS", 60))
TTL_SECONDS = int(os.environ.get("ALPHA_PROPOSAL_TTL_SECONDS", 30 * 60))
M1_BAR_LOOKBACK = int(os.environ.get("ALPHA_M1_LOOKBACK", 30))

PENDING_INDEX_KEY = "proposal:pending:index"
PROPOSAL_CHANNEL = "proposal:update"
TOUCH_DEDUP_PREFIX = "signals:touch:dedup:"

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_TIMEOUT = int(os.environ.get("DEEPSEEK_TIMEOUT_SECONDS", 15))

EXECUTION_MODE = "conditional_reclaim"
POSITION_PLAN = "half_tp_then_trail"


@dataclass
class TradeProposal:
    proposal_id: str
    symbol: str
    side: str
    signal_type: str
    thesis: str
    confidence: float
    entry_price: float
    stop_price: float
    tp_price: float
    trigger_level: float
    bar_time: int
    ttl_seconds: int
    expires_at: int
    created_at: int
    input_hash: str
    status: str = "pending"
    execution_mode: str = EXECUTION_MODE
    execution_phase: str = "pending"
    reclaim_rule: str = ""
    reclaim_label: str = ""
    touch_time: int = 0
    touch_reclaimed_at_submit: bool = False
    tp_half_price: float = 0.0
    pullback_extreme: float = 0.0
    prior_swing: float = 0.0
    rr_half_est: Optional[float] = None
    risk_est: float = 0.0
    reward_half_est: float = 0.0
    position_plan: str = POSITION_PLAN
    extra: dict[str, Any] = field(default_factory=dict)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _proposal_id(symbol: str, signal_type: str, side: str, touch_time: int) -> str:
    raw = f"{symbol}|{signal_type}|{side}|{touch_time}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _build_input_hash(event: TouchEvent) -> str:
    raw = json.dumps(event.to_dict(), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _call_deepseek_json(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not DEEPSEEK_API_KEY:
        return None
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions",
        method="POST",
        data=body,
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=DEEPSEEK_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def _deepseek_enrich(event: TouchEvent, pricing: Any) -> tuple[float, str]:
    prompt = (
        "你是量化交易研究员。基于 M1 触线事件与结构定价，"
        "返回 JSON：{confidence:0~1, thesis:'不超过50字中文'}。"
        "强调：批准后为条件执行，需等 reclaim 后才下单；半仓止盈，剩余 trailing 另策。"
    )
    user_payload = {
        "symbol": event.symbol,
        "side": event.side,
        "signal_type": event.signal_type,
        "trigger_level": event.trigger_level,
        "touch_reclaimed_same_bar": event.reclaim,
        "rule_confidence": event.rule_confidence,
        "rule_thesis": event.rule_thesis,
        "stop_price": pricing.stop_price,
        "tp_half_price": pricing.tp_half_price,
        "rr_half_est": pricing.rr_half_est,
    }
    req_payload = {
        "model": DEEPSEEK_MODEL,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    }
    resp = _call_deepseek_json(req_payload)
    if not resp:
        return event.rule_confidence, event.rule_thesis
    try:
        content = resp["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        confidence = float(parsed.get("confidence", event.rule_confidence))
        confidence = max(0.0, min(1.0, confidence))
        thesis = str(parsed.get("thesis", "")).strip() or event.rule_thesis
        return round(confidence, 3), thesis
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return event.rule_confidence, event.rule_thesis


def _make_proposal(
    *,
    event: TouchEvent,
    m1_bars: list[dict[str, Any]],
    confidence: float,
    thesis: str,
) -> Optional[TradeProposal]:
    pricing = compute_pullback_pricing(
        side=event.side,
        m1_bars=m1_bars,
        touch_bar_time=event.touch_time,
        entry_est=event.m1_close,
    )
    if not pricing:
        return None

    now_ts = int(time.time())
    pid = _proposal_id(event.symbol, event.signal_type, event.side, event.touch_time)
    side = event.side.upper()
    rule = reclaim_rule_for_side(side)

    return TradeProposal(
        proposal_id=pid,
        symbol=event.symbol,
        side=side,
        signal_type=event.signal_type,
        thesis=thesis,
        confidence=round(confidence, 3),
        entry_price=pricing.entry_price_est,
        stop_price=pricing.stop_price,
        tp_price=pricing.tp_half_price,
        tp_half_price=pricing.tp_half_price,
        trigger_level=round(event.trigger_level, 2),
        bar_time=event.m1_bar_time,
        touch_time=event.touch_time,
        ttl_seconds=TTL_SECONDS,
        expires_at=now_ts + TTL_SECONDS,
        created_at=now_ts,
        input_hash=_build_input_hash(event),
        execution_mode=EXECUTION_MODE,
        execution_phase="pending",
        reclaim_rule=rule,
        reclaim_label=reclaim_label(side),
        touch_reclaimed_at_submit=bool(event.reclaim),
        pullback_extreme=pricing.pullback_extreme,
        prior_swing=pricing.prior_swing,
        rr_half_est=pricing.rr_half_est,
        risk_est=pricing.risk_est,
        reward_half_est=pricing.reward_half_est,
        position_plan=POSITION_PLAN,
    )


def _store_pending(r: redis.Redis, proposal: TradeProposal) -> bool:
    dedup_key = f"proposal:dedup:{proposal.proposal_id}"
    if not r.set(dedup_key, "1", nx=True, ex=2 * 24 * 3600):
        return False

    payload = asdict(proposal)
    key = f"proposal:pending:{proposal.proposal_id}"
    pipe = r.pipeline()
    pipe.hset(key, mapping={k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()})
    pipe.expire(key, PROPOSAL_REDIS_RETENTION_SECONDS)
    pipe.zadd(PENDING_INDEX_KEY, {proposal.proposal_id: proposal.created_at})
    pipe.publish(PROPOSAL_CHANNEL, json.dumps(payload, ensure_ascii=False))
    pipe.execute()
    return True


def _read_m1_bars(r: redis.Redis, symbol: str, *, limit: int = M1_BAR_LOOKBACK) -> list[dict[str, Any]]:
    raw_list = r.lrange(f"bars:1m:{symbol}", -limit, -1)
    out: list[dict[str, Any]] = []
    for raw in raw_list:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return _attach_session_vwap(out)


def _attach_session_vwap(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """若 bar 无 vwap 字段，按 session 累计计算并写入副本。"""
    if not bars:
        return bars
    enriched: list[dict[str, Any]] = []
    session_bars: list[dict[str, Any]] = []
    last_date = ""
    for b in bars:
        bar = dict(b)
        from signals.indicators import bar_et_date
        d = bar_et_date(bar) or ""
        if d != last_date:
            session_bars = []
            last_date = d
        session_bars.append(bar)
        if bar.get("vwap") is None:
            v = session_vwap(session_bars)
            if v is not None:
                bar["vwap"] = v
        enriched.append(bar)
    return enriched


def _read_active(r: redis.Redis, symbol: str) -> Optional[dict[str, Any]]:
    raw = r.get(f"indicators:active:{symbol}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _touch_already_processed(r: redis.Redis, symbol: str, event: TouchEvent) -> bool:
    key = f"{TOUCH_DEDUP_PREFIX}{symbol}:{dedup_key(event)}"
    return not r.set(key, "1", nx=True, ex=24 * 3600)


def _scan_m1_touches(r: redis.Redis, symbol: str) -> list[TouchEvent]:
    bars = _read_m1_bars(r, symbol)
    if len(bars) < 2:
        return []
    prev, last = bars[-2], bars[-1]
    active = _read_active(r, symbol)
    events = detect_m1_touch_only(symbol, last, prev, active)
    return [e for e in events if not _touch_already_processed(r, symbol, e)]


def _read_redis_touch_events(r: redis.Redis, symbol: str) -> list[TouchEvent]:
    """兼容 SignalDetector 写入的 signals:touch:{SYMBOL} 列表。"""
    raw = r.lrange(f"signals:touch:{symbol}", -5, -1)
    out: list[TouchEvent] = []
    for item in raw:
        try:
            d = json.loads(item)
            ev = TouchEvent(
                symbol=d["symbol"],
                signal_type=d["signal_type"],
                side=d["side"],
                trigger_level=float(d["trigger_level"]),
                touch_time=int(d["touch_time"]),
                m1_bar_time=int(d.get("m1_bar_time") or d["touch_time"]),
                m5_context_bar_time=d.get("m5_context_bar_time"),
                session_date=str(d.get("session_date") or ""),
                m1_high=float(d["m1_high"]),
                m1_low=float(d["m1_low"]),
                m1_close=float(d["m1_close"]),
                reclaim=bool(d.get("reclaim")),
                rule_confidence=float(d.get("rule_confidence", 0.5)),
                rule_thesis=str(d.get("rule_thesis") or ""),
            )
            if not _touch_already_processed(r, symbol, ev):
                out.append(ev)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return out


def scan_symbol(r: redis.Redis, symbol: str) -> list[TradeProposal]:
    """Legacy 扫描：已统一走 run_incremental_scan（仅 st_super）。"""
    result = run_incremental_scan(r, [symbol])
    return [_payload_to_proposal(p) for p in result.created]


def process_symbol(r: redis.Redis, symbol: str) -> list[TradeProposal]:
    """单标的增量扫描（legacy 循环兼容）。"""
    result = run_incremental_scan(r, [symbol])
    return [_payload_to_proposal(p) for p in result.created]


def _payload_to_proposal(p: dict[str, Any]) -> TradeProposal:
    return TradeProposal(
        proposal_id=str(p["proposal_id"]),
        symbol=str(p["symbol"]),
        side=str(p["side"]),
        signal_type=str(p["signal_type"]),
        thesis=str(p.get("thesis") or ""),
        confidence=float(p.get("confidence") or 0),
        entry_price=float(p["entry_price"]),
        stop_price=float(p["stop_price"]),
        tp_price=float(p.get("tp_price") or p.get("tp_half_price") or 0),
        tp_half_price=float(p.get("tp_half_price") or 0),
        trigger_level=float(p["trigger_level"]),
        bar_time=int(p.get("bar_time") or 0),
        touch_time=int(p.get("touch_time") or 0),
        ttl_seconds=int(p.get("ttl_seconds") or TTL_SECONDS),
        expires_at=int(p.get("expires_at") or 0),
        created_at=int(p.get("created_at") or 0),
        input_hash=str(p.get("input_hash") or ""),
        reclaim_rule=str(p.get("reclaim_rule") or ""),
        reclaim_label=str(p.get("reclaim_label") or ""),
        touch_reclaimed_at_submit=bool(p.get("touch_reclaimed_at_submit")),
        pullback_extreme=float(p.get("pullback_extreme") or 0),
        prior_swing=float(p.get("prior_swing") or 0),
        rr_half_est=p.get("rr_half_est"),
        risk_est=float(p.get("risk_est") or 0),
        reward_half_est=float(p.get("reward_half_est") or 0),
    )


def _sleep_to_next_m1() -> None:
    now = time.time()
    next_min = int(now // 60 + 1) * 60 + 2
    wait = max(1, int(next_min - now))
    time.sleep(wait)


def run() -> None:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=3,
    )
    r.ping()
    print(
        f"[AlphaAgent] started @ {datetime.now(timezone.utc).isoformat()} "
        f"| symbols={ALPHA_SYMBOLS} | poll={POLL_SECONDS}s | "
        f"mode={EXECUTION_MODE} | deepseek={'on' if bool(DEEPSEEK_API_KEY) else 'off'}",
        flush=True,
    )

    while True:
        try:
            result = run_incremental_scan(r, ALPHA_SYMBOLS)
            for p in result.created:
                print(
                    f"[AlphaAgent] + pending {p['symbol']} {p['side']} "
                    f"{p['signal_type']} lvl={p['trigger_level']} "
                    f"stop={p['stop_price']} tp½={p['tp_half_price']} "
                    f"rr={p.get('rr_half_est')} id={p['proposal_id']}",
                    flush=True,
                )
            if result.no_op:
                print("[AlphaAgent] NO_OP", flush=True)
        except Exception as e:  # pragma: no cover
            print(f"[AlphaAgent] loop error: {e}", flush=True)

        if POLL_SECONDS <= 60:
            _sleep_to_next_m1()
        else:
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()
