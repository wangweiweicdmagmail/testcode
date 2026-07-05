/**
 * 全局 Status Bar — multi / indicators / proposals 共用
 */
(function (global) {
  const NAV = [
    { href: '/', label: '单图', key: 'index' },
    { href: '/multi.html', label: '四宫格', key: 'multi' },
    { href: '/indicators.html', label: '指标', key: 'indicators' },
    { href: '/proposals.html', label: '历史', key: 'proposals' },
    { href: '/performance.html', label: '绩效', key: 'performance' },
    { href: '/decisions.html', label: '决策', key: 'decisions' },
    { href: '/audit.html', label: '审计', key: 'audit' },
    { href: '/settings.html', label: '配置', key: 'settings' },
    { href: '/docs.html', label: '文档', key: 'docs' },
  ];

  let voiceEnabled = true;
  let lastSnapshot = { engineOk: null, pending: 0, wait: 0 };
  let onRefreshCb = null;
  let pollTimer = null;

  function navHtml(activeKey) {
    return NAV.map((n) => {
      const cls = n.key === activeKey ? ' class="active"' : '';
      const badge = n.key === 'proposals'
        ? `<span id="sb-pending-badge" class="sb-badge hidden">0</span>`
        : '';
      return `<a href="${n.href}"${cls}>${n.label}${badge}</a>`;
    }).join('');
  }

  function mount(rootId, activeKey) {
    const root = document.getElementById(rootId);
    if (!root) return;
    root.innerHTML = `
      <div id="status-bar-wrap">
        <div id="status-bar">
          <div class="sb-left">
            <span class="sb-title">📈 Nautilus</span>
            <span class="sb-chip" id="sb-engine"><span class="dot"></span>引擎</span>
            <span class="sb-chip" id="sb-redis"><span class="dot"></span>Redis</span>
            <span class="sb-chip info clickable" id="sb-pending" title="点击定位待审批格子">待审批 <b id="sb-pending-n">—</b></span>
            <span class="sb-chip info clickable" id="sb-wait" title="点击定位待执行/等待回踩格子（含 ready_to_execute）">待执行 <b id="sb-wait-n">—</b></span>
            <span class="sb-chip clickable" id="sb-pos" title="点击定位持仓格子">持仓 <b id="sb-pos-n">—</b></span>
            <span class="sb-chip" id="sb-pnl">日PnL <b id="sb-pnl-v">—</b></span>
            <span class="sb-chip" id="sb-risk">距熔断 <b id="sb-risk-v">—</b></span>
          </div>
          <div class="sb-nav">
            ${navHtml(activeKey)}
            <button type="button" id="sb-voice-toggle" class="sb-voice on" title="语音提醒">🔔</button>
            <button type="button" id="sb-killswitch" class="sb-kill" title="一键全平所有持仓并熔断当日">⏻ 全平</button>
          </div>
        </div>
        <div id="sb-engine-banner">⚠️ 引擎离线 — 图表仍可用 Redis 数据；下单/审批后执行需启动引擎</div>
        <div id="sb-paper-banner">ℹ️ 模拟环境（TRADING_ENV=<b>paper</b>）— 禁止批准实盘，AutoRunner 不会向 IBKR 提交自动订单</div>
        <div id="sb-prod-banner">⛔ 生产安全配置不完整 — 见控制台 /api/config/public</div>
        <div id="sb-fixedqty-banner">⚠️ 固定股数模式（AUTO_FIXED_QTY=<b id="sb-fq-n">?</b>）— 自动/提案下单已<b>跳过以损定量</b>，实盘前请将 AUTO_FIXED_QTY 设为 0</div>
        <div id="sb-pending-queue" aria-label="待审批队列"></div>
      </div>`;

    const voiceBtn = document.getElementById('sb-voice-toggle');
    if (voiceBtn) {
      voiceBtn.addEventListener('click', () => {
        voiceEnabled = !voiceEnabled;
        voiceBtn.classList.toggle('on', voiceEnabled);
        voiceBtn.textContent = voiceEnabled ? '🔔' : '🔕';
      });
    }

    document.getElementById('sb-pending')?.addEventListener('click', (e) => {
      e.stopPropagation();
      const q = document.getElementById('sb-pending-queue');
      const list = lastSnapshot.pendingList || [];
      const n = list.length;
      if (n === 1) {
        q?.classList.remove('open');
        global.dispatchEvent(new CustomEvent('nautilus:focus-pending', {
          detail: { symbol: String(list[0].symbol).toUpperCase(), proposal_id: list[0].proposal_id },
        }));
        return;
      }
      if (q && n > 1) {
        q.classList.toggle('open');
        if (q.classList.contains('open')) return;
      }
      global.dispatchEvent(new CustomEvent('nautilus:focus-alpha', { detail: { phase: 'pending' } }));
    });
    document.addEventListener('click', () => {
      document.getElementById('sb-pending-queue')?.classList.remove('open');
    });
    document.getElementById('sb-pending-queue')?.addEventListener('click', (e) => {
      e.stopPropagation();
    });
    document.getElementById('sb-wait')?.addEventListener('click', () => {
      global.dispatchEvent(new CustomEvent('nautilus:focus-alpha', { detail: { phase: 'active' } }));
    });
    document.getElementById('sb-pos')?.addEventListener('click', () => {
      global.dispatchEvent(new CustomEvent('nautilus:focus-position'));
    });

    document.getElementById('sb-killswitch')?.addEventListener('click', killAll);
  }

  function notify(msg) {
    if (global.showToast) global.showToast(msg);
    else alert(msg);
  }

  async function killAll(e) {
    const btn = e?.currentTarget;
    if (!confirm('⏻ 确认【一键全平】所有持仓？\n\n将立即市价平掉全部仓位，并触发当日熔断（停止再开仓）。')) return;
    if (btn) btn.disabled = true;
    try {
      const authFetch = global.NautilusAuth?.authFetch || fetch;
      const res = await authFetch('/api/close-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || data.ok === false) throw new Error(data.error || '全平失败');
      notify('⏻ 已发出全平指令，请在 TWS 核对仓位');
      refresh().catch(() => {});
    } catch (err) {
      notify('❌ 全平失败：' + err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function fetchJson(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    }
  }

  function setChip(id, ok, text) {
    const el = document.getElementById(id);
    if (!el) return;
    const keep = ['info', 'clickable', 'alert-pending'].filter((c) => el.classList.contains(c));
    el.className = ['sb-chip', ...keep, ok === true ? 'ok' : ok === false ? 'bad' : ok === 'warn' ? 'warn' : ''].filter(Boolean).join(' ');
    if (text != null) el.innerHTML = `<span class="dot"></span>${text}`;
  }

  function normalizePositionSymbol(p) {
    if (p.symbol) return String(p.symbol).toUpperCase();
    const raw = String(p.instrument_id || '');
    const m = raw.match(/\.([A-Z0-9]+)\./) || raw.match(/^([A-Z0-9]+)\./);
    return (m ? m[1] : raw.split('.')[0] || '').toUpperCase();
  }

  async function fetchSlotPositions(symbols) {
    if (!symbols?.length) return {};
    const data = await fetchJson(`/api/positions-redis?symbols=${symbols.join(',')}`);
    return data && typeof data === 'object' && !data.error ? data : {};
  }

  async function refresh() {
    const slotSymbols = global.__ALPHA_SYMBOL_LIST || ['QQQ', 'AAPL', 'NVDA', 'TSLA'];
    // 一次性拉取 approved 全量；wait（approved_wait）从中本地筛出，避免重复请求
    const [engine, stack, pending, approved, positions, risk, autoCfg, pubCfg] = await Promise.all([
      fetchJson('/api/engine-status'),
      fetchJson('/api/stack-health'),
      fetchJson('/api/proposals?status=pending&limit=200'),
      fetchJson('/api/proposals?status=approved&limit=200'),
      fetchJson('/api/positions'),
      fetchJson('/api/risk'),
      fetchJson('/api/auto-config'),
      fetchJson('/api/config/public'),
    ]);

    const approvedList = approved?.proposals || [];
    const waitList = approvedList.filter((p) => p.execution_phase === 'approved_wait');
    const readyList = approvedList.filter((p) => {
      const ph = p.execution_phase;
      return ph === 'ready_to_execute' || ph === 'executing';
    });
    const activeCount = waitList.length + readyList.length;

    const engineOk = engine?.engine_online === true;
    lastSnapshot.engineOk = engineOk;
    setChip('sb-engine', engineOk, engineOk ? '引擎●在线' : '引擎●离线');

    const banner = document.getElementById('sb-engine-banner');
    if (banner) banner.classList.toggle('visible', !engineOk);

    // 护栏：固定股数模式（跳过以损定量）红条警告
    const fq = Number(autoCfg?.fixed_qty || 0);
    const fqBanner = document.getElementById('sb-fixedqty-banner');
    if (fqBanner) {
      fqBanner.classList.toggle('visible', fq > 0);
      const fqN = document.getElementById('sb-fq-n');
      if (fqN && fq > 0) fqN.textContent = String(fq);
    }
    lastSnapshot.fixedQty = fq;

    const paperMode = pubCfg?.live_trading_allowed === false
      || (autoCfg?.live_orders_allowed === false);
    const paperBanner = document.getElementById('sb-paper-banner');
    if (paperBanner) paperBanner.classList.toggle('visible', paperMode);

    const prodWarns = pubCfg?.production_warnings || [];
    const prodBanner = document.getElementById('sb-prod-banner');
    if (prodBanner) {
      prodBanner.classList.toggle('visible', prodWarns.length > 0);
      if (prodWarns.length) {
        prodBanner.innerHTML = `⛔ 生产安全: ${prodWarns.map((w) => `<span>${w}</span>`).join(' · ')}`;
      }
    }

    const redisOk = stack?.checks?.redis?.ok !== false;
    setChip('sb-redis', redisOk, redisOk ? 'Redis●' : 'Redis●异常');

    const pn = pending?.count ?? 0;
    const wn = activeCount;
    lastSnapshot.pending = pn;
    lastSnapshot.wait = wn;
    lastSnapshot.readyCount = readyList.length;
    lastSnapshot.waitOnlyCount = waitList.length;
    lastSnapshot.pendingList = pending?.proposals || [];

    renderPendingQueue(lastSnapshot.pendingList);

    const pnEl = document.getElementById('sb-pending-n');
    const wnEl = document.getElementById('sb-wait-n');
    if (pnEl) pnEl.textContent = String(pn);
    if (wnEl) wnEl.textContent = String(wn);

    const pendingChip = document.getElementById('sb-pending');
    if (pendingChip) pendingChip.classList.toggle('alert-pending', pn > 0);

    const badge = document.getElementById('sb-pending-badge');
    if (badge) {
      badge.textContent = String(pn);
      badge.classList.toggle('hidden', pn <= 0);
    }

    let posCount = 0;
    let positionsBySymbol = {};
    // 引擎在线时用 IBKR 列表（含空仓）；离线时才回退 Redis 四格仓位
    if (engineOk && Array.isArray(positions)) {
      positions.forEach((p) => {
        const sym = normalizePositionSymbol(p);
        const q = p.quantity ?? p.qty ?? 0;
        if (sym && Math.abs(q) > 0) {
          positionsBySymbol[sym] = p;
          posCount += 1;
        }
      });
    } else {
      positionsBySymbol = await fetchSlotPositions(slotSymbols);
      posCount = Object.keys(positionsBySymbol).length;
    }

    const posEl = document.getElementById('sb-pos-n');
    if (posEl) posEl.textContent = `${posCount}/4`;
    const posChip = document.getElementById('sb-pos');
    if (posChip) posChip.classList.toggle('ok', posCount > 0);

    const pnlEl = document.getElementById('sb-pnl-v');
    if (pnlEl) {
      // /api/risk 的 daily_pnl_pct 是小数（-0.01 = -1%），需 ×100 显示
      const pnlPct = risk?.daily_pnl_pct ?? risk?.pnl_pct;
      if (pnlPct != null) {
        const v = Number(pnlPct) * 100;
        pnlEl.textContent = `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
        pnlEl.style.color = v >= 0 ? '#26a69a' : '#ef5350';
      } else {
        pnlEl.textContent = '—';
        pnlEl.style.color = '';
      }
    }

    const riskEl = document.getElementById('sb-risk-v');
    const riskChip = document.getElementById('sb-risk');
    if (riskEl) {
      // remaining_loss_pct 为百分比单位（如 1.0 表示离 -1% 熔断还有 1 个百分点）
      // 日内预算 1pp，按预算占比着色：剩 <0.3pp（耗 70%）危险，<0.6pp（耗 40%）警告
      const rem = risk?.remaining_loss_pct ?? risk?.remaining_pct ?? risk?.distance_to_halt_pct;
      if (rem != null) {
        const v = Number(rem);
        riskEl.textContent = `${v.toFixed(2)}%`;
        if (riskChip) {
          riskChip.classList.toggle('warn', v < 0.6 && v >= 0.3);
          riskChip.classList.toggle('bad', v < 0.3);
        }
      } else {
        riskEl.textContent = '—';
        if (riskChip) {
          riskChip.classList.remove('warn', 'bad');
        }
      }
    }

    const snapshot = {
      engineOk,
      pending: pending?.proposals || [],
      wait: waitList,
      ready: readyList,
      approved: approvedList,
      pendingCount: pn,
      waitCount: wn,
      readyCount: readyList.length,
      positions: positions || [],
      positionsBySymbol,
      posCount,
    };
    if (onRefreshCb) onRefreshCb(snapshot);
    return snapshot;
  }

  function startPolling(intervalMs, onRefresh) {
    onRefreshCb = onRefresh || null;
    if (pollTimer) clearInterval(pollTimer);
    const tick = () => refresh().catch(() => {});
    tick();
    pollTimer = setInterval(tick, intervalMs || 15000);
  }

  function isVoiceEnabled() { return voiceEnabled; }

  function renderPendingQueue(list) {
    const el = document.getElementById('sb-pending-queue');
    if (!el) return;
    const items = list || [];
    if (!items.length) {
      el.innerHTML = '<div class="sb-queue-empty">暂无待审批</div>';
      return;
    }
    const sorted = [...items].sort((a, b) => (Number(b.created_at) || 0) - (Number(a.created_at) || 0));
    const Copy = global.AlphaCopy;
    el.innerHTML = `<div class="sb-queue-hdr">待审批 (${sorted.length})</div>`
      + sorted.map((p) => {
        const sym = String(p.symbol || '').toUpperCase();
        const side = Copy ? Copy.sideZh(p.side) : (p.side === 'SHORT' ? '空' : '多');
        const stop = p.stop_price != null ? Number(p.stop_price).toFixed(2) : '—';
        const ttl = Copy ? Copy.ttlText(p) : '';
        return `<div class="sb-queue-item" data-symbol="${sym}" data-id="${p.proposal_id || ''}">
          <span class="q-sym">${sym}</span>${side} · 损 ${stop}
          <div class="q-meta">${ttl || '点击定位到宫格'}</div>
        </div>`;
      }).join('');
    el.querySelectorAll('.sb-queue-item').forEach((node) => {
      node.addEventListener('click', () => {
        const sym = node.dataset.symbol;
        el.classList.remove('open');
        global.dispatchEvent(new CustomEvent('nautilus:focus-pending', {
          detail: { symbol: sym, proposal_id: node.dataset.id },
        }));
      });
    });
  }

  global.StatusBar = {
    mount, refresh, startPolling, isVoiceEnabled, getSnapshot: () => ({ ...lastSnapshot }),
  };
})(window);
