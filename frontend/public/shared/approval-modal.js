/**
 * 批准确认 Modal — 价量摘要 + 引擎状态 + 执行提示
 */
(function (global) {
  let backdrop = null;

  function ensure() {
    if (backdrop) return backdrop;
    backdrop = document.createElement('div');
    backdrop.className = 'approval-modal-backdrop hidden';
    backdrop.id = 'approval-modal-backdrop';
    backdrop.innerHTML = `
      <div class="approval-modal" role="dialog" aria-modal="true">
        <div class="approval-modal-hdr" id="approval-modal-title">批准实盘</div>
        <div class="approval-modal-body">
          <table class="approval-modal-table" id="approval-modal-rows"></table>
          <div class="approval-modal-hint" id="approval-modal-hint"></div>
          <div class="approval-modal-engine ok" id="approval-modal-engine"></div>
        </div>
        <div class="approval-modal-ft">
          <button type="button" class="btn-cancel" id="approval-modal-cancel">取消</button>
          <button type="button" class="btn-approve-live" id="approval-modal-confirm">确认批准实盘</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);
    backdrop.addEventListener('click', (e) => {
      if (e.target === backdrop) hide();
    });
    return backdrop;
  }

  function hide() {
    if (backdrop) backdrop.classList.add('hidden');
  }

  /**
   * @param {object} opts
   * @param {object} opts.proposal
   * @param {boolean} opts.engineOk
   * @param {function} opts.onConfirm
   */
  function show(opts) {
    const { proposal, engineOk, onConfirm } = opts || {};
    if (!proposal || !global.AlphaCopy) {
      if (global.confirm && global.confirm('确认批准？')) onConfirm?.();
      return;
    }
    ensure();
    const body = global.AlphaCopy.approveModalBody(proposal, engineOk);
    document.getElementById('approval-modal-title').textContent = body.title;
    const tbody = document.getElementById('approval-modal-rows');
    tbody.innerHTML = body.rows.map(([k, v]) =>
      `<tr><td>${k}</td><td>${v}</td></tr>`).join('');
    document.getElementById('approval-modal-hint').textContent = body.execHint;
    const engEl = document.getElementById('approval-modal-engine');
    engEl.textContent = body.engineText;
    engEl.className = 'approval-modal-engine ' + (body.engineOk ? 'ok' : 'bad');
    const confirmBtn = document.getElementById('approval-modal-confirm');
    confirmBtn.disabled = !body.engineOk;
    confirmBtn.onclick = () => {
      if (!body.engineOk) return;
      hide();
      onConfirm?.();
    };
    document.getElementById('approval-modal-cancel').onclick = hide;
    backdrop.classList.remove('hidden');
  }

  global.ApprovalModal = { show, hide };
})(window);
