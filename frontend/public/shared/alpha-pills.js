/** 指标页 Alpha pill 状态 */
(function (global) {
  let map = {};

  function ingestFromLists(pendingList, waitList, approvedList) {
    map = {};
    (pendingList || []).forEach((p) => { map[p.symbol] = { cls: 'ip-pending', text: '待审批' }; });
    (waitList || []).forEach((p) => { map[p.symbol] = { cls: 'ip-wait', text: '等回踩' }; });
    (approvedList || []).forEach((p) => {
      if (p.execution_phase === 'ready_to_execute') {
        map[p.symbol] = { cls: 'ip-ready', text: '待执行' };
      }
    });
    return map;
  }

  async function refresh(snapshot) {
    if (Array.isArray(snapshot?.pending) && Array.isArray(snapshot?.approved)) {
      // 复用 StatusBar 快照，避免重复请求 approved
      return ingestFromLists(snapshot.pending, snapshot.wait, snapshot.approved);
    }
    if (Array.isArray(snapshot?.pending) && Array.isArray(snapshot?.wait)) {
      const approved = await fetch('/api/proposals?status=approved&limit=200')
        .then((r) => r.json()).catch(() => ({}));
      return ingestFromLists(snapshot.pending, snapshot.wait, approved.proposals);
    }
    map = {};
    try {
      const [pending, wait, approved] = await Promise.all([
        fetch('/api/proposals?status=pending&limit=200').then((r) => r.json()),
        fetch('/api/proposals?status=approved&execution_phase=approved_wait&limit=200').then((r) => r.json()),
        fetch('/api/proposals?status=approved&limit=200').then((r) => r.json()),
      ]);
      return ingestFromLists(pending.proposals, wait.proposals, approved.proposals);
    } catch (_) { /* ignore */ }
    return map;
  }

  function pillHtml(symbol) {
    const p = map[symbol];
    if (!p) return '';
    return `<span class="ind-alpha-pill ${p.cls}">${p.text}</span>`;
  }

  function symCell(symbol) {
    return `<a href="/multi.html?focus=${symbol}" class="sym">${symbol}</a>${pillHtml(symbol)}`;
  }

  function patchTable() {
    document.querySelectorAll('td a.sym').forEach((a) => {
      const sym = a.textContent.trim();
      const td = a.parentElement;
      if (!td) return;
      td.querySelectorAll('.ind-alpha-pill').forEach((el) => el.remove());
      const extra = pillHtml(sym);
      if (extra) td.insertAdjacentHTML('beforeend', extra);
    });
  }

  global.AlphaPills = { refresh, ingestFromLists, pillHtml, symCell, patchTable, getMap: () => map };
})(window);
