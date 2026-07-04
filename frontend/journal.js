/**
 * 交易/决策落库 — append-only JSONL，独立于 Redis 8h 过期。
 *
 * 设计：
 *  - 零依赖：纯 Node fs，逐行 JSON（可 git diff / 断电不丢 / 易审计）。
 *  - 事件类型：
 *      kind='proposal'  审批生命周期快照（按 proposal_id upsert）
 *      kind='trade'     一笔完成的往返交易（持仓 closed 时结算）
 *  - 已实现盈亏：引擎平仓事件只发 {symbol, closed:true}，不带 PnL，
 *    故缓存每标的最后一次持仓快照（realized+unrealized），平仓时据此估算。
 *
 * 暴露聚合：按信号类型的胜率 / 平均 R / 期望 / 总盈亏、资金曲线、最近成交。
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', '.run');
const JOURNAL_PATH = path.join(DATA_DIR, 'journal.jsonl');

// ── 内存索引 ────────────────────────────────────────────────────────────
const proposals = new Map();            // proposal_id -> 最新提案状态
const tradesArr = [];                   // 已完成往返交易（按 ts 升序）
const openState = new Map();            // symbol -> 最近持仓快照
const lastExecBySymbol = new Map();     // symbol -> 最近一次 executed 提案（用于信号归因）

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

// ── 启动时回放历史 JSONL 重建内存索引 ────────────────────────────────────
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
    let ev;
    try { ev = JSON.parse(line); } catch (_) { continue; }
    if (ev.kind === 'proposal' && ev.proposal_id) {
      proposals.set(ev.proposal_id, ev);
      if (ev.event === 'executed' && ev.symbol) {
        lastExecBySymbol.set(ev.symbol, ev);
      }
    } else if (ev.kind === 'trade') {
      tradesArr.push(ev);
    }
  }
  tradesArr.sort((a, b) => (a.ts || 0) - (b.ts || 0));
  console.log(`[journal] 已回放 ${proposals.size} 提案 / ${tradesArr.length} 成交`);
}

// ── 提案生命周期 ────────────────────────────────────────────────────────
function recordProposalUpdate(data) {
  if (!data || typeof data !== 'object') return;
  const pid = data.proposal_id || data.id;
  if (!pid) return;
  const prev = proposals.get(pid) || {};
  const rec = {
    kind: 'proposal',
    ts: Math.floor(Date.now() / 1000),
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
  };
  proposals.set(pid, rec);
  appendLine(rec);
  if (rec.event === 'executed' && rec.symbol) {
    lastExecBySymbol.set(rec.symbol, rec);
  }
}

// ── 持仓快照 / 平仓结算 ──────────────────────────────────────────────────
function recordPositionUpdate(data) {
  if (!data || typeof data !== 'object') return;
  const sym = data.symbol;
  if (!sym) return;

  // 平仓事件 {symbol, closed:true} → 用最近快照结算一笔交易
  if (data.closed === true || data.closed === 'true') {
    finalizeTrade(sym);
    return;
  }

  const entry = data.avg_px_open ?? data.entry_price;
  const qty = data.quantity ?? data.qty;
  if (entry == null || qty == null || Math.abs(Number(qty)) === 0) return;

  const prev = openState.get(sym);
  const isNew = !prev;
  const attrib = lastExecBySymbol.get(sym);
  openState.set(sym, {
    symbol: sym,
    side: data.side || prev?.side || 'LONG',
    qty: Number(qty),
    entry_price: Number(entry),
    stop_loss: data.stop_loss ?? prev?.stop_loss ?? null,
    realized_pnl: data.realized_pnl ?? prev?.realized_pnl ?? 0,
    unrealized_pnl: data.unrealized_pnl ?? prev?.unrealized_pnl ?? 0,
    entry_ts: prev?.entry_ts || Math.floor(Date.now() / 1000),
    // 信号归因：仅在首次开仓时绑定，避免被后续快照覆盖
    signal_type: prev?.signal_type || attrib?.signal_type || 'manual',
    proposal_id: prev?.proposal_id || attrib?.proposal_id || null,
    proposed_entry: prev?.proposed_entry ?? attrib?.entry_price ?? null,
  });
  void isNew;
}

function finalizeTrade(sym) {
  const st = openState.get(sym);
  openState.delete(sym);
  if (!st) return; // 没有快照可结算（例如重启后丢失），跳过

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
  tradesArr.push(trade);
  appendLine(trade);
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

  // 盈亏比 = 毛利 / 毛损（绝对值）
  const grossProfit = sum(wins, (t) => t.realized_pnl);
  const grossLoss = Math.abs(sum(losses, (t) => t.realized_pnl));
  const profitFactor = grossLoss > 0 ? Number((grossProfit / grossLoss).toFixed(2))
    : (grossProfit > 0 ? null : 0); // 无亏损且有盈利 → ∞（null 表示）

  // 最长连胜 / 连亏 + 当前连续（基于时间序，按已排序的 list）
  let maxW = 0, maxL = 0, curW = 0, curL = 0, curStreak = 0;
  for (const t of list) {
    const p = Number(t.realized_pnl);
    if (p > 0) { curW++; curL = 0; if (curW > maxW) maxW = curW; }
    else if (p < 0) { curL++; curW = 0; if (curL > maxL) maxL = curL; }
    else { curW = 0; curL = 0; }
  }
  curStreak = curW > 0 ? curW : (curL > 0 ? -curL : 0);

  // 最大回撤（基于该组累计已实现盈亏曲线的峰谷）
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

// R 倍数分布直方图（默认 0.5R 一档，[-3, +3] 截断到两端桶）
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

// 把一条提案归类到终态：executed / rejected / approved(进行中) / pending
function classifyProposal(p) {
  const ev = p.event || '';
  const status = p.status || '';
  const phase = p.execution_phase || '';
  if (ev === 'executed' || status === 'executed') return 'executed';
  if (ev === 'rejected' || ev === 'cancelled' || status === 'rejected') return 'rejected';
  if (ev === 'approved' || phase === 'approved_wait' || phase === 'ready_to_execute') return 'approved';
  return 'pending';
}

// 决策复盘聚合：漏斗、按信号转化率、驳回原因分布
function getDecisionStats() {
  const all = Array.from(proposals.values());
  const funnel = { total: all.length, pending: 0, approved: 0, executed: 0, rejected: 0 };
  const bySignal = {};
  const rejectReasons = {};
  for (const p of all) {
    const cls = classifyProposal(p);
    funnel[cls] = (funnel[cls] || 0) + 1;
    const sig = p.signal_type || 'unknown';
    const g = bySignal[sig] || (bySignal[sig] = { proposed: 0, approved: 0, executed: 0, rejected: 0 });
    g.proposed++;
    if (cls === 'approved') g.approved++;
    else if (cls === 'executed') g.executed++;
    else if (cls === 'rejected') g.rejected++;
    if (cls === 'rejected') {
      const reason = (p.reason || '未注明').toString().slice(0, 60);
      rejectReasons[reason] = (rejectReasons[reason] || 0) + 1;
    }
  }
  // 转化率：已批准+已执行 占 已决策（剔除仍 pending）的比例
  for (const g of Object.values(bySignal)) {
    const decided = g.approved + g.executed + g.rejected;
    g.acted = g.approved + g.executed;
    g.approval_rate = decided ? Number((g.acted / decided * 100).toFixed(1)) : null;
  }
  const decided = funnel.approved + funnel.executed + funnel.rejected;
  return {
    funnel,
    acted: funnel.approved + funnel.executed,
    decided,
    approval_rate: decided ? Number(((funnel.approved + funnel.executed) / decided * 100).toFixed(1)) : null,
    execution_rate: decided ? Number((funnel.executed / decided * 100).toFixed(1)) : null,
    by_signal: bySignal,
    reject_reasons: rejectReasons,
  };
}

load();

module.exports = {
  recordProposalUpdate,
  recordPositionUpdate,
  getTrades,
  getEquityCurve,
  getStats,
  getRDistribution,
  getProposals,
  getDecisionStats,
  _paths: { JOURNAL_PATH },
};
