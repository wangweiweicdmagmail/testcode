/**
 * 每格 Alpha 流水线 + 审批 — multi.html 四宫格用
 */
(function (global) {
  const Copy = () => global.AlphaCopy;

  let cache = { pending: {}, wait: {}, ready: {}, bySymbol: {} };
  const executing = {}; // sym → { proposal_id, since }

  function setExecuting(symbol, proposalId) {
    const sym = String(symbol || '').toUpperCase();
    if (!sym) return;
    executing[sym] = { proposal_id: proposalId || '', since: Date.now() };
    renderPanel(sym);
  }

  function clearExecuting(symbol) {
    const sym = String(symbol || '').toUpperCase();
    if (!sym || !executing[sym]) return;
    delete executing[sym];
    renderPanel(sym);
  }

  function isExecuting(symbol) {
    return !!executing[String(symbol || '').toUpperCase()];
  }

  function ingestProposals(pendingList, waitList, approvedList) {
    cache.pending = {};
    cache.wait = {};
    cache.ready = {};
    (pendingList || []).forEach((p) => {
      const sym = String(p.symbol || '').toUpperCase();
      if (sym) cache.pending[sym] = p;
    });
    (waitList || []).forEach((p) => {
      const sym = String(p.symbol || '').toUpperCase();
      if (sym) cache.wait[sym] = p;
    });
    (approvedList || []).forEach((p) => {
      const sym = String(p.symbol || '').toUpperCase();
      if (!sym) return;
      const phase = String(p.execution_phase || '');
      if (phase === 'ready_to_execute' || phase === 'executing') {
        cache.ready[sym] = p;
      }
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
    const sym = String(symbol || '').toUpperCase();
    const readyP = cache.ready[sym];
    if (readyP && String(readyP.execution_phase || '') === 'executing') {
      return { phase: 'executing', proposal: readyP };
    }
    if (executing[sym]) {
      const proposal = cache.ready[sym] || cache.wait[sym] || cache.bySymbol[sym];
      return { phase: 'executing', proposal: proposal || null };
    }
    if (cache.pending[sym]) return { phase: 'pending', proposal: cache.pending[sym] };
    if (cache.wait[sym]) return { phase: 'wait', proposal: cache.wait[sym] };
    if (cache.ready[sym]) return { phase: 'ready', proposal: cache.ready[sym] };
    return { phase: 'none', proposal: null };
  }

  function pillForSymbol(symbol) {
    const { phase, proposal } = phaseForSymbol(symbol);
    if (phase === 'executing') {
      return Copy()?.pillForPhase('executing') || { cls: 'pill-executing', text: '执行中' };
    }
    if (Copy() && proposal) {
      if (phase === 'pending') return Copy().pillForPhase('pending');
      if (phase === 'ready') return Copy().pillForPhase('ready');
      if (phase === 'wait') {
        return Copy().isStSuper(proposal)
          ? Copy().pillForPhase('ready')
          : Copy().pillForPhase('wait');
      }
    }
    if (phase === 'pending') return { cls: 'pill-pending', text: '待审批' };
    if (phase === 'ready') return { cls: 'pill-ready', text: '待执行' };
    if (phase === 'wait') return { cls: 'pill-wait', text: '等待回踩' };
    return { cls: 'pill-none', text: '无信号' };
  }

  function renderSteps(symbol) {
    const { phase, proposal } = phaseForSymbol(symbol);
    if (phase === 'none' && global.getGridSignalContext) {
      const ctx = global.getGridSignalContext(symbol);
      const labels = ['①信号', '②建议', '③审批', '④执行'];
      return labels.map((label, i) => {
        let cls = 'astep';
        if (ctx?.superSide && i === 0) cls += ' done';
        else if (!ctx?.superSide && i === 0) cls += ' active';
        return `<span class="${cls}">${label}</span>`;
      }).join('');
    }
    const steps = Copy()
      ? Copy().workflowSteps(proposal, phase)
      : [
        { label: '①信号', state: 'todo' },
        { label: '②建议', state: 'todo' },
        { label: '③审批', state: 'active' },
        { label: '④执行', state: 'todo' },
      ];
    return steps.map((s) => {
      let cls = 'astep';
      if (s.state === 'done') cls += ' done';
      if (s.state === 'active') {
        cls += (phase === 'pending' ? ' active-pending' : phase === 'executing' ? ' active-executing' : ' active');
      }
      return `<span class="${cls}">${s.label}</span>`;
    }).join('');
  }

  function formatProposalDetail(proposal) {
    if (Copy()) return Copy().formatProposalDetail(proposal);
    if (!proposal) return '暂无活跃建议';
    return `${proposal.symbol} ${proposal.side}`;
  }

  function engineOnline() {
    const snap = global.StatusBar?.getSnapshot?.();
    return snap?.engineOk !== false;
  }

  function renderPanel(symbol) {
    const stepsEl = document.getElementById(`alpha-steps-${symbol}`);
    const detailEl = document.getElementById(`alpha-detail-${symbol}`);
    const actionsEl = document.getElementById(`alpha-actions-${symbol}`);
    if (!stepsEl) return;

    const { phase, proposal } = phaseForSymbol(symbol);
    const idle = phase === 'none';
    stepsEl.className = idle ? 'alpha-steps compact' : 'alpha-steps';
    stepsEl.innerHTML = renderSteps(symbol);

    if (detailEl) {
      if (idle && global.getGridSignalContext) {
        const ctx = global.getGridSignalContext(symbol);
        detailEl.innerHTML = Copy()?.formatIdleDetail(ctx)
          || (ctx?.superSide ? `${ctx.sideLabel}` : '暂无超级信号 · 无待审批建议');
      } else {
        detailEl.textContent = formatProposalDetail(proposal);
      }
    }

    const canApprove = engineOnline();

    if (actionsEl) {
      actionsEl.classList.remove('alpha-actions-highlight');
      if (phase === 'pending' && proposal) {
        actionsEl.classList.add('alpha-actions-highlight');
        const offlineHint = canApprove ? '' : ' title="引擎离线，暂不可批准"';
        actionsEl.innerHTML = `
          <button class="c-btn-alpha approve-live" data-id="${proposal.proposal_id}" data-decision="approved_live"${offlineHint}${canApprove ? '' : ' disabled'}>批准实盘</button>
          <button class="c-btn-alpha reject" data-id="${proposal.proposal_id}" data-decision="rejected">驳回</button>`;
        actionsEl.querySelectorAll('button').forEach((btn) => {
          btn.onclick = () => decide(btn.dataset.id, btn.dataset.decision, symbol, btn);
        });
        if (global.AlphaWorkflow) {
          global.AlphaWorkflow.updateRibbon(symbol, proposal);
        }
      } else if ((phase === 'wait' || phase === 'ready') && proposal) {
        const hint = phase === 'ready'
          ? (Copy()?.postApproveHint(proposal) || '待执行')
          : (Copy()?.phaseLabel(proposal, 'approved_wait') || '等待回踩');
        actionsEl.innerHTML = `
          <span style="font-size:11px;color:${phase === 'ready' ? '#26a69a' : '#787b86'}">${hint}</span>
          <button class="c-btn-alpha cancel" data-id="${proposal.proposal_id}">取消批准</button>`;
        actionsEl.querySelector('button').onclick = (e) => cancelApproved(proposal.proposal_id, symbol, e.target);
      } else if (phase === 'executing' && proposal) {
        actionsEl.innerHTML = `<span style="font-size:11px;color:#f0b90b">Agent 报单中…</span>`;
      } else {
        actionsEl.innerHTML = '';
      }
    }

    const cell = (global.findAlphaCell && global.findAlphaCell(symbol))
      || document.getElementById(`cell-${symbol}`);
    if (cell) {
      cell.classList.remove('cell-alpha-pending', 'cell-alpha-wait', 'cell-alpha-ready', 'cell-alpha-executing');
      if (phase === 'pending') cell.classList.add('cell-alpha-pending');
      else if (phase === 'wait') cell.classList.add('cell-alpha-wait');
      else if (phase === 'ready') cell.classList.add('cell-alpha-ready');
      else if (phase === 'executing') cell.classList.add('cell-alpha-executing');
    }
  }

  function renderAll(symbols) {
    (symbols || []).forEach((sym) => renderPanel(sym));
  }

  function listPending() {
    return Object.values(cache.pending);
  }

  function firstSymbolForPhase(phase) {
    if (phase === 'wait' || phase === 'active' || phase === 'ready') {
      const readyList = Object.values(cache.ready).sort(
        (a, b) => (Number(b.decided_at) || 0) - (Number(a.decided_at) || 0),
      );
      if (readyList.length) return readyList[0].symbol;
      const waitList = Object.values(cache.wait).sort(
        (a, b) => (Number(b.decided_at) || 0) - (Number(a.decided_at) || 0),
      );
      if (waitList.length) return waitList[0].symbol;
      return null;
    }
    const map = phase === 'pending' ? cache.pending : cache.ready;
    const list = Object.values(map);
    if (list.length) {
      list.sort((a, b) => (Number(b.created_at) || 0) - (Number(a.created_at) || 0));
      return list[0].symbol;
    }
    const syms = global.__ALPHA_SYMBOL_LIST || Object.keys(map);
    return syms.find((s) => map[s]) || Object.keys(map)[0] || null;
  }

  async function decide(id, decision, symbol, btn) {
    if (decision === 'rejected' && !confirm(`确认【驳回】${symbol} 建议？`)) return;

    const { proposal } = phaseForSymbol(symbol);
    const run = async () => {
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
        if (global.AlphaWorkflow) global.AlphaWorkflow.onApprovalResolved(symbol);
        if (global.showToast) {
          const exec = data.agent_exec;
          let msg = `${symbol} ${decision === 'approved_live' ? '已批准实盘' : decision === 'approved_observe' ? '已批准观察' : '已驳回'}`;
          if (exec?.mode === 'live') msg += ' · Agent执行已开';
          else if (exec?.mode === 'observe') msg += ' · Agent观察已开';
          global.showToast(msg);
        }
      } catch (e) {
        if (global.showToast) global.showToast('❌ ' + e.message);
        if (btn) btn.disabled = false;
      }
    };

    if (decision === 'approved_live') {
      if (!engineOnline()) {
        if (global.showToast) global.showToast('❌ 引擎离线，无法批准', 'warn');
        return;
      }
      if (global.ApprovalModal && proposal) {
        global.ApprovalModal.show({
          proposal,
          engineOk: true,
          onConfirm: run,
        });
        return;
      }
      if (!confirm(`确认【批准实盘】${symbol}？`)) return;
    }
    await run();
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
      if (global.AlphaWorkflow) global.AlphaWorkflow.onApprovalResolved(symbol);
      clearExecuting(symbol);
    } catch (e) {
      if (global.showToast) global.showToast('❌ ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function refresh(symbols, snapshot) {
    if (Array.isArray(snapshot?.pending) && Array.isArray(snapshot?.approved)) {
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
    listPending,
    setExecuting,
    clearExecuting,
    isExecuting,
    getCache: () => cache,
  };
})(window);
