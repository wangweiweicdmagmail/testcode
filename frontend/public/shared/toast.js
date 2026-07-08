/**
 * 全站 Toast — index / multi / proposals 共用
 */
(function (global) {
  function ensureContainer() {
    let el = document.getElementById('toast-container');
    if (!el) {
      el = document.createElement('div');
      el.id = 'toast-container';
      document.body.appendChild(el);
    }
    return el;
  }

  function showToast(msg, type) {
    const box = ensureContainer();
    const el = document.createElement('div');
    el.className = 'toast-item' + (type ? ` ${type}` : '');
    el.textContent = msg;
    box.appendChild(el);
    requestAnimationFrame(() => el.classList.add('show'));
    const hideMs = type === 'warn' ? 5500 : 4000;
    setTimeout(() => {
      el.classList.remove('show');
      el.classList.add('hide');
      setTimeout(() => el.remove(), 280);
    }, hideMs);
  }

  global.showToast = showToast;
  global.NautilusToast = { show: showToast };
})(window);
