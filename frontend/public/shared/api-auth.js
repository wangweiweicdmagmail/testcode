/**
 * NAUTILUS_API_SECRET 客户端令牌（sessionStorage，同源页面使用）
 */
(function (global) {
  const STORAGE_KEY = 'nautilus_api_token';

  function getToken() {
    try {
      return sessionStorage.getItem(STORAGE_KEY) || '';
    } catch (_) {
      return '';
    }
  }

  function setToken(token) {
    try {
      if (token) sessionStorage.setItem(STORAGE_KEY, token);
      else sessionStorage.removeItem(STORAGE_KEY);
    } catch (_) { /* ignore */ }
  }

  async function ensureToken() {
    let cfg = { api_auth_required: false };
    try {
      cfg = await fetch('/api/config/public').then((r) => r.json());
    } catch (_) { /* ignore */ }
    if (!cfg.api_auth_required) return '';
    let tok = getToken();
    if (tok) return tok;
    tok = prompt('请输入 NAUTILUS_API_SECRET（.env 中配置）:') || '';
    tok = tok.trim();
    if (tok) setToken(tok);
    return tok;
  }

  function authHeaders(extra) {
    const h = { ...(extra || {}) };
    const tok = getToken();
    if (tok) h['X-Nautilus-Token'] = tok;
    return h;
  }

  async function authFetch(url, options) {
    const opts = { ...(options || {}) };
    await ensureToken();
    opts.headers = authHeaders(opts.headers);
    const res = await fetch(url, opts);
    if (res.status === 401) {
      setToken('');
      await ensureToken();
      opts.headers = authHeaders(opts.headers);
      return fetch(url, opts);
    }
    return res;
  }

  global.NautilusAuth = {
    getToken, setToken, ensureToken, authHeaders, authFetch,
  };
})(typeof window !== 'undefined' ? window : globalThis);
