/**
 * 每格 Alpha 流水线 + 审批 — multi.html 四宫格用
 */
(function (global) {
  const SIGNAL_LABEL = {
    pullback_vwap: 'VWAP',
    pullback_supertrend: 'ST',
    pullback_dema20: 'DEMA20',
    pullback_dema: 'DEMA',
    st_super: '超级',
  };

  let cache = { pending: {}, wait: {}, ready: {}, bySymbol: {} };

  function ingestProposals(pendingList, waitList, approvedList) {
    cache.pending = {};
    cache.wait = {};
    cache.ready = {};
    (pendingList || []).forEach((p) => { cache.pending[p.symbol] = p; });
    (waitList || []).forEach((p) => { cache.wait[p.symbol] = p; });
    (approvedList || []).forEach((p) => {
      if (p.execution_phase === 'ready_to_execute') cache.ready[p.symbol] = p;
    });
    cache.bySymbol = {};
    for (const sym of new Set([
      ...Object.keys(cache.pending),
      ...Object.keys(cache.wait),
      ...Object.keys(cache.ready),
    ])) {
      cache.bySymbol[sym] = cache.pending[sym] || cache.wait[sym] || cache.ready[sym];
    }
    return cache;
  }

  async function loadAll() {
    const [pending, wait, approved] = await Promise.all([
      fetch('/api/proposals?status=pending&limit=200').then((r) => r.json()).catch(() => ({})),
      fetch('/api/proposals?status=approved&execution_phase=approved_wait&limit=200').then((r) => r.json()).catch(() => ({})),
      fetch('/api/proposals?status=approved&limit=200').then((r) => r.json()).catch(() => ({})),
    ]);
    return ingestProposals(pending.proposals, wait.proposals, approved.proposals);
  }

  function phaseForSymbol(symbol) {
    if (cache.pending[symbol]) return { phase: 'pending', proposal: cache.pending[symbol] };
    if (cache.wait[symbol]) return { phase: 'wait', proposal: cache.wait[symbol] };
    if (cache.ready[symbol]) return { phase: 'ready', proposal: cache.ready[symbol] };
    return { phase: 'none', proposal: null };
  }

  function pillForSymbol(symbol) {
    const { phase } = phaseForSymbol(symbol);
    if (phase === 'pending') return { cls: 'pill-pending', text: '待审批' };
    if (phase === 'wait') return { cls: 'pill-wait', text: '等待回踩' };
    if (phase === 'ready') return { cls: 'pill-ready', text: '待执行' };
    return { cls: 'pill-none', text: '无信号' };
  }

  function formatProposalDetail(proposal) {
    if (!proposal) return '暂无活跃建议';
    const sig = SIGNAL_LABEL[proposal.signal_type] || proposal.signal_type || '';
    const side = proposal.side === 'SHORT' ? '空' : '多';
    const trig = proposal.trigger_level != null ? Number(proposal.trigger_level).toFixed(2) : '—';
    const entry = proposal.entry_price != null ? Number(proposal.entry_price).toFixed(2) : '—';
    const stop = proposal.stop_price != null ? Number(proposal.stop_price).toFixed(2) : '—';
    const rr = proposal.rr_half_est != null ? Number(proposal.rr_half_est).toFixed(1) : '—';
    return `${side}·${sig} 触发${trig}\n入${entry} 损${stop} R:R½ ${rr}`;
  }

  function renderSteps(symbol) {
    const { phase } = phaseForSymbol(symbol);
    const steps = [
      { label: '①扫描' },
      { label: '②审批' },
      { label: '③回踩' },
      { label: '④执行' },
    ];
    let activeIdx = phase === 'none' ? 0 : phase === 'pending' ? 1 : phase === 'wait' ? 2 : 3;
    const doneUntil = phase === 'none' ? -1 : activeIdx - 1;

    return steps.map((s, i) => {
      let cls = 'astep';
      if (i <= doneUntil) cls += ' done';
      if (i === activeIdx) cls += ' active';
      return `<span class="${cls}">${s.label}</span>`;
    }).join('');
  }

  function renderPanel(symbol) {
    const stepsEl = document.getElementById(`alpha-steps-${symbol}`);
    const detailEl = document.getElementById(`alpha-detail-${symbol}`);
    const actionsEl = document.getElementById(`alpha-actions-${symbol}`);
    if (!stepsEl) return;

    const { phase, proposal } = phaseForSymbol(symbol);
    stepsEl.innerHTML = renderSteps(symbol);

    if (detailEl) {
      detailEl.textContent = formatProposalDetail(proposal);
    }

    if (actionsEl) {
      if (phase === 'pending' && proposal) {
        actionsEl.innerHTML = `
          <button class="c-btn-alpha approve" data-id="${proposal.proposal_id}" data-decision="approved_live">批准</button>
          <button class="c-btn-alpha reject" data-id="${proposal.proposal_id}" data-decision="rejected">驳回</button>`;
        actionsEl.querySelectorAll('button').forEach((btn) => {
          btn.onclick = () => decide(btn.dataset.id, btn.dataset.decision, symbol, btn);
        });
      } else if (phase === 'wait' && proposal) {
        actionsEl.innerHTML = `<button class="c-btn-alpha cancel" data-id="${proposal.proposal_id}">取消批准</button>`;
        actionsEl.querySelector('button').onclick = (e) => cancelApproved(proposal.proposal_id, symbol, e.target);
      } else {
        actionsEl.innerHTML = '';
      }
    }

    const cell = document.getElementById(`cell-${symbol}`);
    if (cell) {
      cell.classList.remove('cell-alpha-pending', 'cell-alpha-wait', 'cell-alpha-ready');
      if (phase === 'pending') cell.classList.add('cell-alpha-pending');
      else if (phase === 'wait') cell.classList.add('cell-alpha-wait');
      else if (phase === 'ready') cell.classList.add('cell-alpha-ready');
    }
  }

  function renderAll(symbols) {
    (symbols || []).forEach((sym) => renderPanel(sym));
  }

  function firstSymbolForPhase(phase) {
    const map = phase === 'pending' ? cache.pending : phase === 'wait' ? cache.wait : cache.ready;
    const syms = global.__ALPHA_SYMBOL_LIST || Object.keys(map);
    return syms.find((s) => map[s]) || Object.keys(map)[0] || null;
  }

  async function decide(id, decision, symbol, btn) {
    if (decision === 'approved_live' && !confirm(`确认【批准】${symbol} 建议？`)) return;
    if (decision === 'rejected' && !confirm(`确认【驳回】${symbol} 建议？`)) return;
    if (btn) btn.disabled = true;
    try {
      const res = await (global.NautilusAuth
        ? global.NautilusAuth.authFetch(`/api/proposals/${id}/decision`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ decision, approver: 'operator', comment: '' }),
          })
        : fetch(`/api/proposals/${id}/decision`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ decision, approver: 'operator', comment: '' }),
          }));
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '审批失败');
      if (global.refreshAlphaPanels) {
        await global.refreshAlphaPanels();
      } else {
        await loadAll();
        const syms = global.__ALPHA_SYMBOL_LIST || Object.keys(global.__ALPHA_SYMBOLS || {});
        renderAll(syms);
        if (global.StatusBar) await global.StatusBar.refresh();
      }
      if (global.onAlphaUpdated) global.onAlphaUpdated();
      if (global.showToast) {
        global.showToast(`${symbol} ${decision === 'approved_live' ? '已批准' : '已驳回'}`);
      }
    } catch (e) {
      if (global.showToast) global.showToast('❌ ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function cancelApproved(id, symbol, btn) {
    if (!confirm(`确认【取消批准】${symbol}？`)) return;
    if (btn) btn.disabled = true;
    try {
      const res = await (global.NautilusAuth
        ? global.NautilusAuth.authFetch(`/api/proposals/${id}/cancel`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ approver: 'operator' }),
          })
        : fetch(`/api/proposals/${id}/cancel`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ approver: 'operator' }),
          }));
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '取消失败');
      if (global.refreshAlphaPanels) {
        await global.refreshAlphaPanels();
      } else {
        await loadAll();
        const syms = global.__ALPHA_SYMBOL_LIST || Object.keys(global.__ALPHA_SYMBOLS || {});
        renderAll(syms);
        if (global.StatusBar) await global.StatusBar.refresh();
      }
      if (global.onAlphaUpdated) global.onAlphaUpdated();
    } catch (e) {
      if (global.showToast) global.showToast('❌ ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function refresh(symbols, snapshot) {
    if (Array.isArray(snapshot?.pending) && Array.isArray(snapshot?.approved)) {
      // 复用 StatusBar 快照，避免重复请求 approved
      ingestProposals(snapshot.pending, snapshot.wait, snapshot.approved);
    } else if (Array.isArray(snapshot?.pending) && Array.isArray(snapshot?.wait)) {
      const approved = await fetch('/api/proposals?status=approved&limit=200')
        .then((r) => r.json()).catch(() => ({}));
      ingestProposals(snapshot.pending, snapshot.wait, approved.proposals);
    } else {
      await loadAll();
    }
    renderAll(symbols);
    return cache;
  }

  global.AlphaCell = {
    loadAll,
    refresh,
    ingestProposals,
    renderPanel,
    renderAll,
    phaseForSymbol,
    pillForSymbol,
    firstSymbolForPhase,
    getCache: () => cache,
  };
})(window);
