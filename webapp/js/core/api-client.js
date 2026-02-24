/**
 * Централизованный API-клиент: единая точка для всех HTTP-запросов.
 * Использует config для base URL и auth headers.
 * Соблюдает DRY и Single Responsibility.
 */
(function() {
  'use strict';

  function getConfig() {
    return {
      base: window.App.getApiBase(),
      headers: window.App.getAuthHeaders(),
      setBase: window.App.setApiBase,
      getDomain: window.App.getApiDomain,
      getFallback: window.App.getApiFallback
    };
  }

  function buildUrl(path, params) {
    const base = getConfig().base.replace(/\/+$/, '');
    const url = new URL(base + path);
    if (params) {
      Object.entries(params).forEach(function(entry) {
        url.searchParams.set(entry[0], entry[1]);
      });
    }
    return url.toString();
  }

  function getDefaultHeaders() {
    return Object.assign({}, getConfig().headers);
  }

  function parseErrorResponse(response, defaultMsg) {
    var status = response.status;
    var msg = window.App.Constants.MESSAGES;
    if (status === 401) return msg.AUTH_REQUIRED;
    if (status === 429) return msg.RATE_LIMIT_SHORT;
    if (status === 404) return defaultMsg || 'Not found';
    return 'HTTP ' + status;
  }

  /**
   * GET запрос
   * @param {string} path - путь (начинается с /)
   * @param {Object} [params] - query params
   * @returns {Promise<*>}
   */
  async function get(path, params) {
    var config = getConfig();
    var url = buildUrl(path, params);
    var response = await fetch(url, { cache: 'no-store', headers: getDefaultHeaders() });
    if (!response.ok) {
      var err = new Error(parseErrorResponse(response));
      err.status = response.status;
      throw err;
    }
    var contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
      throw new Error('Response is not JSON');
    }
    return response.json();
  }

  /**
   * POST запрос
   */
  async function post(path, body) {
    var config = getConfig();
    var url = buildUrl(path);
    var headers = Object.assign({ 'Content-Type': 'application/json' }, getDefaultHeaders());
    var response = await fetch(url, {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(body || {})
    });
    if (!response.ok) {
      var err = new Error(parseErrorResponse(response));
      err.status = response.status;
      throw err;
    }
    return response.json();
  }

  /**
   * PATCH запрос
   */
  async function patch(path, body) {
    var url = buildUrl(path);
    var headers = Object.assign({ 'Content-Type': 'application/json' }, getDefaultHeaders());
    var response = await fetch(url, {
      method: 'PATCH',
      headers: headers,
      body: JSON.stringify(body || {})
    });
    if (!response.ok) {
      var err = new Error(parseErrorResponse(response));
      err.status = response.status;
      throw err;
    }
    return response.json();
  }

  /**
   * DELETE запрос
   */
  async function del(path, params) {
    var url = buildUrl(path, params);
    var response = await fetch(url, {
      method: 'DELETE',
      headers: getDefaultHeaders()
    });
    if (!response.ok) {
      var err = new Error(parseErrorResponse(response));
      err.status = response.status;
      throw err;
    }
    return response.json();
  }

  /**
   * Специфичный для stats: с fallback и кастомными сообщениями об ошибках
   */
  async function fetchStatsWithFallback(period) {
    var config = getConfig();
    var path = '/api/stats?period=' + encodeURIComponent(period);
    var bases = [config.base, config.getFallback()].filter(Boolean);
    var lastError = null;
    var domain = config.getDomain();
    var msg = window.App.Constants.MESSAGES;

    for (var i = 0; i < bases.length; i++) {
      var base = bases[i];
      if (base === config.getFallback() && config.getFallback() === config.base) continue;
      try {
        var url = base.replace(/\/+$/, '') + path;
        var r = await fetch(url, { cache: 'no-store', headers: getDefaultHeaders() });
        if (!r.ok) {
          var errMsg = r.status === 401 ? msg.AUTH_REQUIRED :
            r.status === 404 ? msg.API_NOT_FOUND.replace('{domain}', domain) :
            r.status === 429 ? msg.RATE_LIMIT : 'HTTP ' + r.status;
          lastError = new Error(errMsg);
          lastError.status = r.status;
          throw lastError;
        }
        var data;
        try {
          data = await r.json();
        } catch (parseErr) {
          var ct = r.headers.get('content-type') || '';
          throw new Error(ct.includes('text/html') ? msg.API_HTML_RESPONSE.replace(/\{domain\}/g, domain) : (parseErr.message || String(parseErr)));
        }
        if (!Array.isArray(data)) data = [];
        if (base !== config.base) config.setBase(base);
        return data;
      } catch (e) {
        lastError = e;
        if (e.status === 401 || e.status === 404 || e.status === 429) break;
      }
    }
    if (lastError) throw lastError;
    return [];
  }

  window.App = window.App || {};
  window.App.ApiClient = {
    get: get,
    post: post,
    patch: patch,
    del: del,
    fetchStatsWithFallback: fetchStatsWithFallback,
    buildUrl: buildUrl,
    getDefaultHeaders: getDefaultHeaders
  };
})();
