#!/usr/bin/env python3
"""
Nautilus Alpha MCP Server — 信号只读 + create_proposal

启动: nautilus_mcp/run.sh（Cursor 见 .cursor/mcp.json）
"""
import json
import sys
from pathlib import Path

# 先导入官方 mcp 包，再插入项目根目录（避免与本地目录名冲突）
from mcp.server.fastmcp import FastMCP

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from approval.alpha_scan import run_incremental_scan, try_create_proposal_from_touch
from approval.pending_cleanup import purge_pending_proposals
from approval.proposal_builder import touch_from_dict
from nautilus_mcp.redis_io import (
    DEFAULT_SYMBOLS,
    get_bars,
    get_indicators_active,
    get_redis,
    list_pending_proposals,
    list_recent_touches,
    stack_health,
)
from signals.m5_st_audit import audit_m5_st

mcp = FastMCP(
    "alpha",
    instructions=(
        "Nautilus IBKR 量化交易信号 MCP。"
        "只读 bars / indicators:active / signals:touch；"
        "create_proposal 写入待审批池（条件执行 conditional_reclaim）。"
        "run_alpha_scan 为 cron 推荐入口：增量过滤 + 每轮限量。"
        "purge_pending_proposals 在扫描前清理过期/逆势/过时 pending。"
        "audit_m5_st 对账 Redis M5 K 线与 indicators:active。"
        "Agent 回复须列出 purge/created/skipped 全部分支及中文 reason，NO_OP 也要解释原因。"
        "审批走 Web / 飞书，勿直接改 approved 状态。"
    ),
)


def _json(obj: object) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


@mcp.tool()
def get_alpha_snapshot(symbols: str = "") -> str:
    """
    一轮 Agent 扫描的聚合快照：各标的 active 水平线、最近触线、待审批 id。

    symbols: 逗号分隔，留空用 ALPHA_SYMBOLS 默认列表。
    """
    r = get_redis()
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()] or list(DEFAULT_SYMBOLS)
    snap = {"symbols": syms, "active": {}, "touches": [], "pending": []}
    for sym in syms:
        active = get_indicators_active(r, sym)
        if active:
            snap["active"][sym] = active
    snap["touches"] = list_recent_touches(r, limit=30)
    if syms:
        snap["touches"] = [t for t in snap["touches"] if t.get("symbol", "").upper() in syms]
    snap["pending"] = list_pending_proposals(r, limit=50)
    snap["health"] = stack_health(r)
    return _json(snap)


@mcp.tool()
def get_indicators_active_tool(symbol: str) -> str:
    """读取 M5 冻结水平线 indicators:active:{SYMBOL}。"""
    r = get_redis()
    data = get_indicators_active(r, symbol.upper())
    return _json({"symbol": symbol.upper(), "active": data})


@mcp.tool()
def get_m1_bars(symbol: str, limit: int = 30) -> str:
    """最近 M1 K 线（含 vwap 字段若引擎已写）。"""
    r = get_redis()
    bars = get_bars(r, "1m", symbol.upper(), limit=min(limit, 120))
    return _json({"symbol": symbol.upper(), "count": len(bars), "bars": bars})


@mcp.tool()
def get_m5_bars(symbol: str, limit: int = 30) -> str:
    """最近 M5 K 线（st_value / dema20 等）。"""
    r = get_redis()
    bars = get_bars(r, "5m", symbol.upper(), limit=min(limit, 120))
    return _json({"symbol": symbol.upper(), "count": len(bars), "bars": bars})


@mcp.tool()
def list_recent_touches_tool(symbol: str = "", limit: int = 20) -> str:
    """最近 M1 触线事件（signals:touch）。"""
    r = get_redis()
    sym = symbol.upper() if symbol else None
    touches = list_recent_touches(r, symbol=sym, limit=min(limit, 50))
    return _json({"count": len(touches), "touches": touches})


@mcp.tool()
def list_pending_proposals_tool(symbol: str = "", limit: int = 20) -> str:
    """待审批 proposal 列表。"""
    r = get_redis()
    sym = symbol.upper() if symbol else None
    items = list_pending_proposals(r, symbol=sym, limit=min(limit, 50))
    return _json({"count": len(items), "proposals": items})


@mcp.tool()
def get_stack_health() -> str:
    """Redis + 待审批数量 + 引擎 heartbeat。"""
    r = get_redis()
    return _json(stack_health(r))


@mcp.tool()
def create_proposal(
    symbol: str,
    signal_type: str,
    side: str,
    touch_time: int,
    thesis: str,
    confidence: float = 0.6,
) -> str:
    """
    从已发生的 st_super 触线创建 pending 建议（Python 计算止损/半仓TP/R:R）。

    signal_type: 仅 st_super（回踩类已禁用）
    side: LONG | SHORT
    touch_time: 触线 M1 bar 的 time（秒，ET fake-UTC）
    thesis: 中文理由（≤80字）
    confidence: 0~1
    """
    sym = symbol.upper()
    if signal_type.strip().lower() != "st_super":
        return _json({
            "ok": False,
            "error": "pullback_disabled",
            "hint": "仅支持 st_super 超级信号",
        })
    if sym not in DEFAULT_SYMBOLS:
        return _json({
            "ok": False,
            "error": "symbol_excluded",
            "hint": f"Alpha 扫描标的为 {','.join(DEFAULT_SYMBOLS)}，不含 {sym}",
        })
    r = get_redis()
    touches = list_recent_touches(r, symbol=sym, limit=50)
    match = next(
        (
            t for t in touches
            if int(t.get("touch_time", 0)) == int(touch_time)
            and str(t.get("signal_type")) == signal_type
            and str(t.get("side", "")).upper() == side.upper()
        ),
        None,
    )
    if not match:
        return _json({
            "ok": False,
            "error": "touch_not_found",
            "hint": "先调用 list_recent_touches 确认 touch_time / signal_type / side",
        })

    event = touch_from_dict(match)
    ok, out = try_create_proposal_from_touch(
        r, event, thesis=thesis, confidence=confidence,
    )
    if not ok:
        hint = {
            "pullback_disabled": "回踩类信号已禁用，仅 st_super",
            "counter_trend": "方向与 M5 ST 不一致",
            "rr_below_min": "半仓 R:R < 1",
            "pending_same_side": "同标的同方向已有 pending",
            "touch_too_old": "触线超出 ALPHA_TOUCH_MAX_AGE_SECONDS",
            "duplicate_proposal": "该触线已建过建议",
        }.get(str(out.get("error")), "")
        return _json({**out, "hint": hint} if hint else out)
    return _json({
        "ok": True,
        "proposal_id": out.get("proposal_id"),
        "proposal": out.get("proposal"),
    })


@mcp.tool()
def purge_pending_proposals_tool(
    symbols: str = "",
    dry_run: bool = False,
    reject_expired: bool = True,
    reject_stale_touch: bool = True,
    reject_counter_trend: bool = True,
) -> str:
    """
    扫描前清理 pending：移至 rejected。

    默认驳回：expires_at 已过 / 触线超出 ALPHA_TOUCH_MAX_AGE / 与当前 M5 ST 逆势。
    dry_run=true 仅列出将清理的 id，不写 Redis。
    symbols: 逗号分隔，留空表示全部 pending。
    """
    r = get_redis()
    syms = tuple(s.strip().upper() for s in symbols.split(",") if s.strip()) or None
    result = purge_pending_proposals(
        r,
        symbols=syms,
        dry_run=dry_run,
        reject_expired=reject_expired,
        reject_stale_touch=reject_stale_touch,
        reject_counter_trend=reject_counter_trend,
    )
    out = result.to_dict()
    out["health"] = stack_health(r)
    return _json(out)


@mcp.tool()
def audit_m5_st_tool(symbol: str, limit: int = 80) -> str:
    """
    从 bars:5m 重放 M5 SuperTrend(10,3)，与 Redis 内 st_dir/st_value、indicators:active 对账。
    用于诊断 ST 翻向 bug（方向不一致时 note 会标注）。
    """
    r = get_redis()
    result = audit_m5_st(r, symbol.upper(), limit=min(limit, 200))
    return _json(result.to_dict())


@mcp.tool()
def run_alpha_scan(symbols: str = "", incremental: bool = True, purge_before: bool = True) -> str:
    """
    推荐 cron 入口：增量扫描触线 → 过滤 → 写入 pending（每标的≤1，全市场≤3）。

    symbols: 逗号分隔，留空用 ALPHA_SYMBOLS。
    incremental: True 时仅处理最近 ALPHA_TOUCH_MAX_AGE_SECONDS 内触线（默认 300s）。
    purge_before: True 时先执行 purge_pending_proposals（默认开启）。
    无新建议时 no_op=true。
    """
    r = get_redis()
    syms = tuple(s.strip().upper() for s in symbols.split(",") if s.strip()) or DEFAULT_SYMBOLS
    purge_out = None
    if purge_before:
        purge_out = purge_pending_proposals(r, symbols=syms).to_dict()
    result = run_incremental_scan(r, syms, incremental=incremental)
    result_dict = result.to_dict()
    if purge_out is not None:
        result_dict["purge"] = purge_out
    result_dict["health"] = stack_health(r)
    return _json(result_dict)


if __name__ == "__main__":
    mcp.run()
