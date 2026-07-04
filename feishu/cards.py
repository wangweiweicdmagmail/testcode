"""飞书交互卡片模板。"""
from __future__ import annotations

from typing import Any


def _fmt_price(v: Any) -> str:
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return str(v)


def build_proposal_card(proposal: dict[str, Any]) -> dict[str, Any]:
    pid = str(proposal.get("proposal_id", ""))
    symbol = str(proposal.get("symbol", "?"))
    side = str(proposal.get("side", "?"))
    signal = str(proposal.get("signal_type", "?"))
    conf = proposal.get("confidence", 0)
    thesis = str(proposal.get("thesis", ""))[:500]
    entry = _fmt_price(proposal.get("entry_price"))
    stop = _fmt_price(proposal.get("stop_price"))
    tp_half = _fmt_price(proposal.get("tp_half_price") or proposal.get("tp_price"))
    trigger = _fmt_price(proposal.get("trigger_level"))
    rr = proposal.get("rr_half_est")
    rr_s = f"{rr:.2f}" if isinstance(rr, (int, float)) else "—"
    cond = proposal.get("execution_mode") == "conditional_reclaim"
    reclaim = str(proposal.get("reclaim_label") or "等待 reclaim 后执行")

    side_cn = "做多" if side.upper() == "LONG" else "做空"
    signal_cn = {
        "pullback_vwap": "回踩 VWAP",
        "pullback_supertrend": "回踩 SuperTrend",
        "pullback_dema20": "回踩 DEMA20",
        "pullback_dema": "回踩 DEMA",
    }.get(signal, signal)

    md = (
        f"**{symbol}** {side_cn} · {signal_cn}\n"
        f"置信度 **{conf}** · 触发线 {trigger} / 估入场 {entry}\n"
        f"止损 {stop} / 半仓止盈 {tp_half} / R:R½ **{rr_s}**\n"
        + (f"⚠️ **条件执行**：批准≠立即下单。{reclaim}\n" if cond else "")
        + f"ID: `{pid}`\n\n"
        f"{thesis}"
    )

    def btn(label: str, decision: str, btn_type: str = "default") -> dict[str, Any]:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": btn_type,
            "value": {"proposal_id": pid, "decision": decision},
        }

    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"交易建议 · {symbol}"},
            "template": "blue" if side.upper() == "LONG" else "orange",
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": md}},
            {
                "tag": "action",
                "actions": [
                    btn("批准", "approved_live", "primary"),
                    btn("驳回", "rejected", "danger"),
                ],
            },
        ],
    }


def decision_toast(decision: str, symbol: str = "") -> str:
    label = {
        "approved_live": "已批准实盘（Agent执行已开启，下一根 M1）",
        "approved_observe": "已批准观察（Agent观察已开启）",
        "rejected": "已驳回",
    }.get(decision, "已处理")
    sym = f" {symbol}" if symbol else ""
    return f"{label}{sym}"
