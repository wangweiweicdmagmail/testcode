/**
 * 交易/决策落库 — append-only JSONL，独立于 Redis 8h 过期。
 *
 * 事件类型：
 *   kind='proposal'  审批生命周期（按 proposal_id 保留最新快照 + 全量时间线）
 *   kind='signal'    触线 (signal:touch) + 自动策略 (auto:signal)
 *   kind='order'     IBKR 订单状态 (order:update)
 *   kind='position'  持仓快照 (position:update，非 closed)
 *   kind='trade'     往返交易结算 (position closed)
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', '.run');
const JOURNAL_PATH = path.join(DATA_DIR, 'journal.jsonl');

// ── 内存索引 ────────────────────────────────────────────────────────────
const proposals = new Map();            // proposal_id -> 最新提案状态
const proposalEvents = [];              // 全量提案事件（时间线）
const signalsArr = [];
const ordersArr = [];
const positionsArr = [];
const tradesArr = [];
const openState = new Map();
const lastExecBySymbol = new Map();
const activeProposalBySymbol = new Map(); // symbol -> proposal_id（executing/open 期间）
const coidProposalMap = new Map();        // client_order_id -> proposal_id

function ensureDir() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) { /* ignore */ }
}

function appendLine(obj) {
  ensureDir();
  try {
    fs.appendFileSync(JOURNAL_PATH, JSON.stringify(obj) + '\n');
  } catch (e) {
    console.error(`[journal] 写入失败: ${e.message}`);
  }
}

function etDateStr(tsSec) {
  const ts = tsSec || Math.floor(Date.now() / 1000);
  return new Date(ts * 1000).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
}

function _ingestEvent(ev) {
  if (ev.kind === 'proposal' && ev.proposal_id) {
    proposalEvents.push(ev);
    proposals.set(ev.proposal_id, ev);
    if (ev.event === 'executed' && ev.symbol) lastExecBySymbol.set(ev.symbol, ev);
    if (ev.event === 'executing' && ev.symbol && ev.proposal_id) {
      activeProposalBySymbol.set(ev.symbol, ev.proposal_id);
    }
    if (ev.event === 'submit_failed' && ev.symbol) {
      activeProposalBySymbol.delete(ev.symbol);
    }
  } else if (ev.kind === 'signal') {
    signalsArr.push(ev);
    if (ev.source === 'auto' && ev.action === 'open' && ev.proposal_id && ev.symbol) {
      activeProposalBySymbol.set(ev.symbol, ev.proposal_id);
    }
  } else if (ev.kind === 'order') {
    ordersArr.push(ev);
    if (ev.client_order_id && ev.proposal_id) {
      coidProposalMap.set(ev.client_order_id, ev.proposal_id);
    }
  } else if (ev.kind === 'position') {
    positionsArr.push(ev);
  } else if (ev.kind === 'trade') {
    tradesArr.push(ev);
  }
}

function load() {
  ensureDir();
  if (!fs.existsSync(JOURNAL_PATH)) return;
  let lines;
  try {
    lines = fs.readFileSync(JOURNAL_PATH, 'utf8').split('\n');
  } catch (e) {
    console.error(`[journal] 读取失败: ${e.message}`);
    return;
  }
  for (const line of lines) {
    if (!line.trim()) continue;
    try { _ingestEvent(JSON.parse(line)); } catch (_) { /* skip bad line */ }
  }
  tradesArr.sort((a, b) => (a.ts || 0) - (b.ts || 0));
  console.log(
    `[journal] 回放 ${proposals.size} 提案 / ${signalsArr.length} 信号 / `
    + `${ordersArr.length} 订单 / ${tradesArr.length} 成交`,
  );
}

// ── 提案生命周期 ────────────────────────────────────────────────────────
function recordProposalUpdate(data) {
  if (!data || typeof data !== 'object') return;
  const pid = data.proposal_id || data.id;
  if (!pid) return;
  const prev = proposals.get(pid) || {};
  const rec = {
    kind: 'proposal',
    ts: data.ts || Math.floor(Date.now() / 1000),
    proposal_id: pid,
    symbol: data.symbol || prev.symbol,
    side: data.side || prev.side,
    signal_type: data.signal_type || prev.signal_type,
    status: data.status || prev.status,
    decision: data.decision ?? prev.decision,
    execution_phase: data.execution_phase ?? prev.execution_phase,
    entry_price: data.entry_price ?? prev.entry_price,
    stop_price: data.stop_price ?? prev.stop_price,
    tp_price: data.tp_half_price ?? data.tp_price ?? prev.tp_price,
    rr_half_est: data.rr_half_est ?? prev.rr_half_est,
    confidence: data.confidence ?? prev.confidence,
    event: data.event || prev.event,
    reason: data.reason ?? data.comment ?? prev.reason,
    operator: data.operator ?? data.approver ?? prev.operator,
    decided_at: data.decided_at ?? prev.decided_at,
    created_at: data.created_at ?? prev.created_at,
    result: data.result ?? prev.result,
    exec_qty: data.exec_qty ?? data.qty ?? prev.exec_qty,
    exec_entry_px: data.exec_entry_px ?? data.entry_px ?? prev.exec_entry_px,
    submit_error: data.submit_error ?? prev.submit_error,
  };
  _ingestEvent(rec);
  appendLine(rec);
}

// ── 信号：触线 + auto:signal ────────────────────────────────────────────
function recordSignalTouch(data, channel) {
  if (!data || typeof data !== 'object') return;
  const rec = {
    kind: 'signal',
    source: 'touch',
    channel: channel || 'signal:touch',
    ts: data.emitted_at || data.touch_time || Math.floor(Date.now() / 1000),
    symbol: data.symbol,
    signal_type: data.signal_type,
    side: data.side,
    touch_time: data.touch_time,
    trigger_level: data.trigger_level,
    reclaim: data.reclaim,
    rule_confidence: data.rule_confidence,
    rule_thesis: data.rule_thesis,
    session_date: data.session_date,
    m1_close: data.m1_close,
  };
  _ingestEvent(rec);
  appendLine(rec);
}

function recordAutoSignal(data) {
  if (!data || typeof data !== 'object') return;
  const rec = {
    kind: 'signal',
    source: 'auto',
    ts: data.ts || Math.floor(Date.now() / 1000),
    symbol: data.symbol,
    action: data.action,
    mode: data.mode,
    proposal_id: data.proposal_id || null,
    reason: data.reason,
    side: data.side,
    qty: data.qty,
    entry: data.entry,
    stop: data.stop,
    tp: data.tp,
    seq: data.seq,
  };
  _ingestEvent(rec);
  appendLine(rec);
}

// ── 订单状态 ────────────────────────────────────────────────────────────
function recordOrderUpdate(data) {
  if (!data || typeof data !== 'object') return;
  const coid = data.client_order_id;
  let pid = data.proposal_id || null;
  const sym = data.symbol;
  if (!pid && coid && coidProposalMap.has(coid)) pid = coidProposalMap.get(coid);
  if (!pid && sym && activeProposalBySymbol.has(sym)) pid = activeProposalBySymbol.get(sym);
  if (pid && coid) coidProposalMap.set(coid, pid);

  const rec = {
    kind: 'order',
    ts: data.ts || Math.floor(Date.now() / 1000),
    proposal_id: pid,
    symbol: sym || null,
    client_order_id: coid,
    venue_order_id: data.venue_order_id,
    status: data.status,
    side: data.side,
    order_type: data.order_type,
    quantity: data.quantity,
    price: data.price,
    trigger_price: data.trigger_price,
    last_px: data.last_px,
    last_qty: data.last_qty,
    filled_qty: data.filled_qty,
    leaves_qty: data.leaves_qty,
    commission: data.commission,
    reason: data.reason,
  };
  _ingestEvent(rec);
  appendLine(rec);
}

// ── 持仓快照 / 平仓结算 ──────────────────────────────────────────────────
function recordPositionUpdate(data) {
  if (!data || typeof data !== 'object') return;
  const sym = data.symbol;
  if (!sym) return;

  if (data.closed === true || data.closed === 'true') {
    finalizeTrade(sym);
    return;
  }

  const entry = data.avg_px_open ?? data.entry_price;
  const qty = data.quantity ?? data.qty;
  if (entry == null || qty == null || Math.abs(Number(qty)) === 0) return;

  const prev = openState.get(sym);
  const attrib = lastExecBySymbol.get(sym);
  const activePid = activeProposalBySymbol.get(sym);
  openState.set(sym, {
    symbol: sym,
    side: data.side || prev?.side || 'LONG',
    qty: Number(qty),
    entry_price: Number(entry),
    stop_loss: data.stop_loss ?? prev?.stop_loss ?? null,
    realized_pnl: data.realized_pnl ?? prev?.realized_pnl ?? 0,
    unrealized_pnl: data.unrealized_pnl ?? prev?.unrealized_pnl ?? 0,
    entry_ts: prev?.entry_ts || Math.floor(Date.now() / 1000),
    signal_type: prev?.signal_type || attrib?.signal_type || proposals.get(activePid)?.signal_type || 'manual',
    proposal_id: prev?.proposal_id || attrib?.proposal_id || activePid || null,
    proposed_entry: prev?.proposed_entry ?? attrib?.entry_price ?? proposals.get(activePid)?.entry_price ?? null,
  });

  const snap = {
    kind: 'position',
    ts: Math.floor(Date.now() / 1000),
    symbol: sym,
    side: data.side || prev?.side,
    qty: Number(qty),
    entry_price: Number(entry),
    stop_loss: data.stop_loss ?? null,
    unrealized_pnl: data.unrealized_pnl,
    realized_pnl: data.realized_pnl,
    proposal_id: openState.get(sym)?.proposal_id || null,
  };
  _ingestEvent(snap);
  appendLine(snap);
}

function finalizeTrade(sym) {
  const st = openState.get(sym);
  openState.delete(sym);
  activeProposalBySymbol.delete(sym);
  if (!st) return;

  const realized = Number(st.realized_pnl || 0) + Number(st.unrealized_pnl || 0);
  const riskPerShare = (st.stop_loss != null)
    ? Math.abs(Number(st.entry_price) - Number(st.stop_loss))
    : null;
  const riskAmt = riskPerShare != null ? riskPerShare * Math.abs(st.qty) : null;
  const rMultiple = (riskAmt && riskAmt > 0) ? Number((realized / riskAmt).toFixed(3)) : null;
  const slippage = (st.proposed_entry != null)
    ? Number((((st.side === 'SHORT' ? -1 : 1) * (st.entry_price - st.proposed_entry))).toFixed(4))
    : null;

  const now = Math.floor(Date.now() / 1000);
  const trade = {
    kind: 'trade',
    ts: now,
    symbol: sym,
    side: st.side,
    qty: Math.abs(st.qty),
    entry_price: st.entry_price,
    stop_loss: st.stop_loss,
    realized_pnl: Number(realized.toFixed(2)),
    risk_amt: riskAmt != null ? Number(riskAmt.toFixed(2)) : null,
    r_multiple: rMultiple,
    signal_type: st.signal_type || 'manual',
    proposal_id: st.proposal_id || null,
    proposed_entry: st.proposed_entry,
    slippage: slippage,
    duration_s: now - (st.entry_ts || now),
    pnl_source: 'snapshot_estimate',
  };
  _ingestEvent(trade);
  appendLine(trade);
}

// ── 时间线 / 日审计 ─────────────────────────────────────────────────────
function _collectEvents(filter) {
  const { proposal_id, symbol, et_date } = filter;
  const propSym = proposal_id ? (symbol || proposals.get(proposal_id)?.symbol) : symbol;

  const match = (ev) => {
    if (et_date && etDateStr(ev.ts) !== et_date) return false;
    if (proposal_id) {
      if (ev.proposal_id === proposal_id) return true;
      if (ev.kind === 'signal' && ev.source === 'touch' && propSym && ev.symbol === propSym) {
        return true;
      }
      return false;
    }
    if (propSym && ev.symbol && ev.symbol !== propSym) return false;
    return true;
  };

  const all = [
    ...proposalEvents,
    ...signalsArr,
    ...ordersArr,
    ...positionsArr,
    ...tradesArr,
  ].filter(match);

  all.sort((a, b) => (a.ts || 0) - (b.ts || 0) || String(a.kind).localeCompare(b.kind));
  return all;
}

function getTimeline(opts = {}) {
  const proposal_id = opts.proposal_id || null;
  const symbol = opts.symbol || (proposal_id ? proposals.get(proposal_id)?.symbol : null);
  const limit = Math.min(opts.limit || 500, 5000);
  const events = _collectEvents({ proposal_id, symbol }).slice(-limit);
  const summary = proposal_id ? (proposals.get(proposal_id) || null) : null;
  const trade = proposal_id
    ? tradesArr.find((t) => t.proposal_id === proposal_id) || null
    : null;
  return { proposal_id, symbol, summary, trade, count: events.length, events };
}

function getDayAudit(dateStr) {
  const et_date = dateStr || etDateStr();
  const events = _collectEvents({ et_date });
  const proposalIds = new Set();
  for (const ev of events) {
    if (ev.proposal_id) proposalIds.add(ev.proposal_id);
    if (ev.kind === 'proposal') proposalIds.add(ev.proposal_id);
  }
  const dayProposals = Array.from(proposalIds)
    .map((id) => proposals.get(id))
    .filter(Boolean)
    .sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const dayTrades = tradesArr.filter((t) => etDateStr(t.ts) === et_date);
  const stats = {
    signals: events.filter((e) => e.kind === 'signal').length,
    orders: events.filter((e) => e.kind === 'order').length,
    proposals: dayProposals.length,
    trades: dayTrades.length,
    pnl: Number(dayTrades.reduce((s, t) => s + Number(t.realized_pnl || 0), 0).toFixed(2)),
  };
  return { date: et_date, stats, proposals: dayProposals, trades: dayTrades, events };
}

// ── 聚合查询 ────────────────────────────────────────────────────────────
function getTrades(limit = 100) {
  return tradesArr.slice(-limit).reverse();
}

function getEquityCurve() {
  let cum = 0;
  return tradesArr.map((t) => {
    cum += Number(t.realized_pnl || 0);
    return { ts: t.ts, pnl: Number(t.realized_pnl || 0), equity: Number(cum.toFixed(2)), symbol: t.symbol };
  });
}

function _statsFor(list) {
  const n = list.length;
  const wins = list.filter((t) => Number(t.realized_pnl) > 0);
  const losses = list.filter((t) => Number(t.realized_pnl) < 0);
  const sum = (a, f) => a.reduce((s, t) => s + Number(f(t) || 0), 0);
  const totalPnl = sum(list, (t) => t.realized_pnl);
  const rVals = list.map((t) => t.r_multiple).filter((v) => v != null);
  const avgR = rVals.length ? rVals.reduce((s, v) => s + v, 0) / rVals.length : null;
  const avgWin = wins.length ? sum(wins, (t) => t.realized_pnl) / wins.length : 0;
  const avgLoss = losses.length ? sum(losses, (t) => t.realized_pnl) / losses.length : 0;
  const slips = list.map((t) => t.slippage).filter((v) => v != null);
  const avgSlip = slips.length ? slips.reduce((s, v) => s + v, 0) / slips.length : null;
  const grossProfit = sum(wins, (t) => t.realized_pnl);
  const grossLoss = Math.abs(sum(losses, (t) => t.realized_pnl));
  const profitFactor = grossLoss > 0 ? Number((grossProfit / grossLoss).toFixed(2))
    : (grossProfit > 0 ? null : 0);

  let maxW = 0, maxL = 0, curW = 0, curL = 0, curStreak = 0;
  for (const t of list) {
    const p = Number(t.realized_pnl);
    if (p > 0) { curW++; curL = 0; if (curW > maxW) maxW = curW; }
    else if (p < 0) { curL++; curW = 0; if (curL > maxL) maxL = curL; }
    else { curW = 0; curL = 0; }
  }
  curStreak = curW > 0 ? curW : (curL > 0 ? -curL : 0);

  let cum = 0, peak = 0, maxDD = 0;
  for (const t of list) {
    cum += Number(t.realized_pnl || 0);
    if (cum > peak) peak = cum;
    const dd = peak - cum;
    if (dd > maxDD) maxDD = dd;
  }

  return {
    trades: n,
    wins: wins.length,
    losses: losses.length,
    win_rate: n ? Number((wins.length / n * 100).toFixed(1)) : null,
    total_pnl: Number(totalPnl.toFixed(2)),
    expectancy: n ? Number((totalPnl / n).toFixed(2)) : null,
    avg_r: avgR != null ? Number(avgR.toFixed(2)) : null,
    avg_win: Number(avgWin.toFixed(2)),
    avg_loss: Number(avgLoss.toFixed(2)),
    avg_slippage: avgSlip != null ? Number(avgSlip.toFixed(4)) : null,
    profit_factor: profitFactor,
    max_drawdown: Number(maxDD.toFixed(2)),
    max_consecutive_wins: maxW,
    max_consecutive_losses: maxL,
    current_streak: curStreak,
  };
}

function getRDistribution(bucket = 0.5) {
  const rVals = tradesArr.map((t) => t.r_multiple).filter((v) => v != null);
  const LO = -3, HI = 3;
  const buckets = [];
  for (let lo = LO; lo < HI; lo += bucket) {
    buckets.push({ lo: Number(lo.toFixed(2)), hi: Number((lo + bucket).toFixed(2)), count: 0 });
  }
  for (const r of rVals) {
    const v = Math.max(LO, Math.min(HI - 1e-9, r));
    const idx = Math.min(buckets.length - 1, Math.floor((v - LO) / bucket));
    buckets[idx].count++;
  }
  return { bucket, total: rVals.length, buckets };
}

function getStats() {
  const bySignal = {};
  const groups = {};
  for (const t of tradesArr) {
    const k = t.signal_type || 'manual';
    (groups[k] = groups[k] || []).push(t);
  }
  for (const [k, list] of Object.entries(groups)) {
    bySignal[k] = _statsFor(list);
  }
  return {
    overall: _statsFor(tradesArr),
    by_signal: bySignal,
    proposals_total: proposals.size,
  };
}

function getProposals(limit = 200) {
  return Array.from(proposals.values())
    .sort((a, b) => (b.ts || 0) - (a.ts || 0))
    .slice(0, limit);
}

function getOrders(limit = 200) {
  return ordersArr.slice(-limit).reverse();
}

function getSignals(limit = 200) {
  return signalsArr.slice(-limit).reverse();
}

function classifyProposal(p) {
  const ev = p.event || '';
  const status = p.status || '';
  const phase = p.execution_phase || '';
  if (ev === 'executed' || status === 'executed') return 'executed';
  if (ev === 'rejected' || ev === 'cancelled' || status === 'rejected') return 'rejected';
  if (ev === 'executing' || ev === 'submit_failed') return 'executing';
  if (ev === 'approved' || phase === 'approved_wait' || phase === 'ready_to_execute') return 'approved';
  return 'pending';
}

function getDecisionStats() {
  const all = Array.from(proposals.values());
  const funnel = { total: all.length, pending: 0, approved: 0, executed: 0, rejected: 0, executing: 0 };
  const bySignal = {};
  const rejectReasons = {};
  for (const p of all) {
    const cls = classifyProposal(p);
    funnel[cls] = (funnel[cls] || 0) + 1;
    const sig = p.signal_type || 'unknown';
    const g = bySignal[sig] || (bySignal[sig] = { proposed: 0, approved: 0, executed: 0, rejected: 0 });
    g.proposed++;
    if (cls === 'approved' || cls === 'executing') g.approved++;
    else if (cls === 'executed') g.executed++;
    else if (cls === 'rejected') g.rejected++;
    if (cls === 'rejected') {
      const reason = (p.reason || '未注明').toString().slice(0, 60);
      rejectReasons[reason] = (rejectReasons[reason] || 0) + 1;
    }
  }
  for (const g of Object.values(bySignal)) {
    const decided = g.approved + g.executed + g.rejected;
    g.acted = g.approved + g.executed;
    g.approval_rate = decided ? Number((g.acted / decided * 100).toFixed(1)) : null;
  }
  const decided = funnel.approved + funnel.executed + funnel.rejected + (funnel.executing || 0);
  return {
    funnel,
    acted: funnel.approved + funnel.executed + (funnel.executing || 0),
    decided,
    approval_rate: decided ? Number(((funnel.approved + funnel.executed + (funnel.executing || 0)) / decided * 100).toFixed(1)) : null,
    execution_rate: decided ? Number((funnel.executed / decided * 100).toFixed(1)) : null,
    by_signal: bySignal,
    reject_reasons: rejectReasons,
  };
}

load();

module.exports = {
  recordProposalUpdate,
  recordPositionUpdate,
  recordSignalTouch,
  recordAutoSignal,
  recordOrderUpdate,
  getTrades,
  getEquityCurve,
  getStats,
  getRDistribution,
  getProposals,
  getDecisionStats,
  getTimeline,
  getDayAudit,
  getOrders,
  getSignals,
  etDateStr,
  _paths: { JOURNAL_PATH },
};
