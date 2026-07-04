/**
 * 四宫格 Alpha 审批工作流：信号「叮」+ 建议语音分离，视觉聚焦待审批格。
 */
(function (global) {
  const COALESCE_MS = 800;
  const FOCUS_DURATION_MS = 45000;

  let _audioCtx = null;
  const _pending = new Map(); // sym -> { timers, signal, proposal, flushed }
  let _focusSym = null;
  let _focusTimer = null;

  function voiceEnabled() {
    return !global.StatusBar || global.StatusBar.isVoiceEnabled();
  }

  function getAudioCtx() {
    if (!_audioCtx) {
      try {
        _audioCtx = new (global.AudioContext || global.webkitAudioContext)();
      } catch (_) { /* ignore */ }
    }
    return _audioCtx;
  }

  /** 超级信号：短促「叮」，不用 TTS */
  function playSignalDing() {
    if (!voiceEnabled()) return;
    const ctx = getAudioCtx();
    if (!ctx) return;
    if (ctx.state === 'suspended') ctx.resume().catch(() => {});
    const t0 = ctx.currentTime;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.setValueAtTime(1046.5, t0);
    osc.frequency.exponentialRampToValueAtTime(880, t0 + 0.08);
    gain.gain.setValueAtTime(0.0001, t0);
    gain.gain.linearRampToValueAtTime(0.22, t0 + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.42);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(t0);
    osc.stop(t0 + 0.45);
  }

  function formatProposalSpeech(proposal) {
    if (!proposal) return '';
    const sym = String(proposal.symbol || '').toUpperCase();
    const side = proposal.side === 'SHORT' ? '空' : '多';
    const stop = proposal.stop_price != null ? Number(proposal.stop_price).toFixed(2) : '';
    const entry = proposal.entry_price != null ? Number(proposal.entry_price).toFixed(2) : '';
    const rr = proposal.rr_half_est != null ? Number(proposal.rr_half_est).toFixed(1) : '';
    let text = `${sym}，${side}向建议，待审批`;
    if (entry) text += `，入场约 ${entry}`;
    if (stop) text += `，止损 ${stop}`;
    if (rr) text += `，半仓盈亏比 ${rr}`;
    return text;
  }

  function speakProposal(proposal) {
    if (!voiceEnabled()) return;
    const text = formatProposalSpeech(proposal);
    if (!text) return;
    global.speechSynthesis.cancel();
    const u = new SpeechSynthesisUtterance(text);
    u.lang = 'zh-CN';
    u.rate = 1.05;
    global.speechSynthesis.speak(u);
  }

  function clearApprovalFocus() {
    _focusSym = null;
    if (_focusTimer) {
      clearTimeout(_focusTimer);
      _focusTimer = null;
    }
    const grid = document.getElementById('grid');
    if (grid) grid.classList.remove('grid-approval-mode');
    document.querySelectorAll('.cell-approval-focus').forEach((el) => {
      el.classList.remove('cell-approval-focus');
    });
    document.querySelectorAll('.alpha-approval-ribbon').forEach((el) => {
      el.classList.add('hidden');
    });
  }

  function setApprovalFocus(sym, proposal) {
    if (!sym) return;
    clearApprovalFocus();
    _focusSym = sym;
    const cell = document.getElementById(`cell-${sym}`);
    const grid = document.getElementById('grid');
    if (grid) grid.classList.add('grid-approval-mode');
    if (cell) {
      cell.classList.add('cell-approval-focus');
      cell.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
    updateRibbon(sym, proposal);
    if (global.AlphaCell) global.AlphaCell.renderPanel(sym);
    _focusTimer = setTimeout(clearApprovalFocus, FOCUS_DURATION_MS);
  }

  function updateRibbon(sym, proposal) {
    const ribbon = document.getElementById(`alpha-ribbon-${sym}`);
    if (!ribbon) return;
    if (!proposal) {
      ribbon.classList.add('hidden');
      return;
    }
    const side = proposal.side === 'SHORT' ? '空' : '多';
    const stop = proposal.stop_price != null ? Number(proposal.stop_price).toFixed(2) : '—';
    const entry = proposal.entry_price != null ? Number(proposal.entry_price).toFixed(2) : '—';
    ribbon.innerHTML = `<span class="ribbon-tag">待审批</span>`
      + `<span class="ribbon-body">${side} · 入 ${entry} · 损 ${stop}</span>`
      + `<span class="ribbon-hint">↓ 右侧批准</span>`;
    ribbon.classList.remove('hidden');
  }

  function stashForReload(payload) {
    try {
      const key = 'multiWorkflowAlert';
      let prev = {};
      try {
        prev = JSON.parse(sessionStorage.getItem(key) || '{}');
      } catch (_) { /* ignore */ }
      const sym = payload.sym || prev.sym;
      const merged = {
        ...prev,
        ...payload,
        sym,
        signal: payload.signal || prev.signal || null,
        proposal: payload.proposal || prev.proposal || null,
        ts: Date.now(),
      };
      sessionStorage.setItem(key, JSON.stringify(merged));
    } catch (_) { /* ignore */ }
  }

  function readStash() {
    try {
      const raw = sessionStorage.getItem('multiWorkflowAlert');
      if (!raw) return null;
      sessionStorage.removeItem('multiWorkflowAlert');
      const data = JSON.parse(raw);
      if (Date.now() - (data.ts || 0) > 120000) return null;
      return data;
    } catch (_) {
      return null;
    }
  }

  function flushWorkflow(sym) {
    const bucket = _pending.get(sym);
    if (!bucket || bucket.flushed) return;
    bucket.flushed = true;
    if (bucket.timer) clearTimeout(bucket.timer);
    _pending.delete(sym);

    const { signal, proposal } = bucket;
    playSignalDing();

    const toastSignal = signal
      ? `🔔 ${sym} 超级${signal.side === 'SHORT' ? '空' : '多'}信号`
      : null;
    if (toastSignal && global.showToast) {
      global.showToast(toastSignal, 'warn');
    }

    if (proposal) {
      setTimeout(() => {
        speakProposal(proposal);
        if (global.showToast) {
          global.showToast(`📋 ${sym} 操作建议待审批`, 'tip');
        }
        if (global.focusApprovalCell) global.focusApprovalCell(sym, proposal);
        else setApprovalFocus(sym, proposal);
      }, 480);
    } else if (global.focusApprovalCell) {
      setTimeout(() => global.focusApprovalCell(sym, null), 200);
    }
  }

  function scheduleFlush(sym, delayMs) {
    const bucket = _pending.get(sym) || { flushed: false };
    if (bucket.flushed) return;
    if (bucket.timer) clearTimeout(bucket.timer);
    bucket.timer = setTimeout(() => flushWorkflow(sym), delayMs);
    _pending.set(sym, bucket);
  }

  function handleSignal(touch, { deferGrid = false } = {}) {
    const sym = String(touch?.symbol || '').toUpperCase();
    if (!sym) return;
    const bucket = _pending.get(sym) || { flushed: false };
    bucket.signal = touch;
    _pending.set(sym, bucket);
    if (bucket.proposal) {
      scheduleFlush(sym, 120);
    } else {
      scheduleFlush(sym, COALESCE_MS);
    }
    if (deferGrid) {
      stashForReload({ sym, signal: touch, proposal: bucket.proposal || null });
    }
  }

  function handleProposal(proposal, { deferGrid = false } = {}) {
    const sym = String(proposal?.symbol || '').toUpperCase();
    if (!sym) return;
    const bucket = _pending.get(sym) || { flushed: false };
    bucket.proposal = proposal;
    _pending.set(sym, bucket);
    scheduleFlush(sym, bucket.signal ? 120 : 80);
    if (deferGrid) {
      stashForReload({ sym, signal: bucket.signal || null, proposal });
    }
  }

  function playAfterReload(sym, { signal, proposal } = {}) {
    const bucket = { signal, proposal, flushed: false };
    _pending.set(sym, bucket);
    flushWorkflow(sym);
  }

  function onApprovalResolved(sym) {
    if (_focusSym === sym) clearApprovalFocus();
  }

  global.AlphaWorkflow = {
    playSignalDing,
    speakProposal,
    formatProposalSpeech,
    handleSignal,
    handleProposal,
    playAfterReload,
    readStash,
    stashForReload,
    setApprovalFocus,
    clearApprovalFocus,
    updateRibbon,
    onApprovalResolved,
  };
})(window);
