/**
 * Конфигурация API: URL, домены, заголовки авторизации.
 * Остальные модули используют App.getApiBase(), App.setApiBase(), App.getAuthHeaders().
 */
(function() {
  const params = new URLSearchParams(location.search);
  const tg = window.Telegram?.WebApp;
  const startParam = tg?.startParam;
  let rawApi = (params.get('api') ? decodeURIComponent(params.get('api')).trim() : null)
    || (startParam ? decodeURIComponent(startParam) : null)
    || (document.getElementById('app')?.dataset?.apiUrl || 'http://localhost:8080');
  if (!rawApi.startsWith('http://') && !rawApi.startsWith('https://')) {
    rawApi = 'https://' + rawApi.replace(/^\/+/, '');
  }
  if (rawApi && !rawApi.match(/:\d+(\/|$)/)) {
    if (!rawApi.startsWith('https://')) {
      rawApi = rawApi.replace(/:?\/?$/, '') + ':8080';
    } else {
      rawApi = rawApi.replace(/\/$/, '');
    }
  }
  let API_BASE = rawApi.replace(/\/$/, '');
  const appEl = document.getElementById('app');
  const API_DOMAIN = appEl?.dataset?.apiDomain || 'api.arbmarketsystem.ru';
  const MARKET_DOMAIN = appEl?.dataset?.marketDomain || 'market.arbmarketsystem.ru';
  const isFrontendOnly = typeof location !== 'undefined' && new RegExp(MARKET_DOMAIN.replace(/\./g, '\\.') + '$', 'i').test(location.hostname || '');
  const API_FALLBACK = (typeof location !== 'undefined' && location.origin && !isFrontendOnly) ? location.origin.replace(/\/$/, '') : '';

  window.App = window.App || {};
  window.App.getApiBase = function() { return API_BASE; };
  window.App.setApiBase = function(base) {
    API_BASE = base;
    const el = document.getElementById('api-display');
    if (el) el.textContent = base;
  };
  window.App.getAuthHeaders = function() {
    const initData = window.Telegram?.WebApp?.initData;
    return initData ? { 'X-Telegram-Init-Data': initData } : {};
  };
  window.App.getApiDomain = function() { return API_DOMAIN; };
  window.App.getApiFallback = function() { return API_FALLBACK; };
  const el = document.getElementById('api-display');
  if (el) el.textContent = API_BASE;
})();
