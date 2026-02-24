/**
 * Консоль логов: загрузка, авто-обновление.
 */
(function() {
  const getApiBase = () => window.App.getApiBase();
  const getAuthHeaders = () => window.App.getAuthHeaders();
  const escapeHtml = (s) => window.App.escapeHtml(s);

  let consoleRefreshInterval = null;

  function startConsoleAutoRefresh() {
    stopConsoleAutoRefresh();
    const cb = document.getElementById('console-auto-refresh');
    const sel = document.getElementById('console-refresh-interval');
    if (cb?.checked && sel) {
      const ms = parseInt(sel.value, 10) * 1000;
      consoleRefreshInterval = setInterval(window.App.console.fetchConsoleLogs, ms);
    }
  }

  function stopConsoleAutoRefresh() {
    if (consoleRefreshInterval) {
      clearInterval(consoleRefreshInterval);
      consoleRefreshInterval = null;
    }
  }

  async function fetchConsoleLogs() {
    const el = document.getElementById('console-output');
    el.textContent = 'Загрузка...';
    try {
      const r = await fetch(getApiBase() + '/api/logs?limit=100', { headers: getAuthHeaders() });
      if (!r.ok) {
        if (r.status === 404) {
          el.innerHTML = '<div class="console-empty">Консоль отключена в настройках сервера</div>';
          return;
        }
        if (r.status === 401) throw new Error('Откройте дашборд через Telegram (кнопка «Навигация»)');
        if (r.status === 429) throw new Error('Слишком много запросов. Подождите минуту.');
        throw new Error('HTTP ' + r.status);
      }
      const data = await r.json();
      const lines = data.lines || [];
      if (lines.length === 0) {
        el.innerHTML = '<div class="console-empty">Нет логов</div>';
        return;
      }
      el.innerHTML = lines.map(line => {
        const cls = line.includes('ERROR') ? 'error' : line.includes('WARNING') ? 'warning' : '';
        return '<div class="console-line ' + cls + '">' + escapeHtml(line) + '</div>';
      }).join('');
      el.scrollTop = el.scrollHeight;
    } catch (e) {
      el.innerHTML = '<div class="console-empty">Ошибка: ' + escapeHtml(e.message || String(e)) + '</div>';
    }
  }

  window.App = window.App || {};
  window.App.console = {
    fetchConsoleLogs,
    startConsoleAutoRefresh,
    stopConsoleAutoRefresh
  };
})();
