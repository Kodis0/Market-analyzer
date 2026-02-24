/**
 * Консоль логов: загрузка, авто-обновление.
 * Рефакторинг: использует ApiClient.
 */
(function() {
  'use strict';

  var M = window.App.Constants.MESSAGES;
  var escapeHtml = function(s) { return window.App.escapeHtml(s); };
  var api = window.App.ApiClient;

  var consoleRefreshInterval = null;

  function startConsoleAutoRefresh() {
    stopConsoleAutoRefresh();
    var cb = document.getElementById('console-auto-refresh');
    var sel = document.getElementById('console-refresh-interval');
    if (cb && cb.checked && sel) {
      var ms = parseInt(sel.value, 10) * 1000;
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
    var el = document.getElementById('console-output');
    el.textContent = M.LOADING;
    try {
      var data = await api.get('/api/logs', { limit: 100 });
      var lines = data.lines || [];
      if (lines.length === 0) {
        el.innerHTML = '';
        return;
      }
      el.innerHTML = lines.map(function(line) {
        var cls = line.includes('ERROR') ? 'error' : line.includes('WARNING') ? 'warning' : '';
        return '<div class="console-line ' + cls + '">' + escapeHtml(line) + '</div>';
      }).join('');
      el.scrollTop = el.scrollHeight;
    } catch (e) {
      if (e.status === 404) {
        el.innerHTML = '';
        return;
      }
      el.innerHTML = '';
    }
  }

  window.App = window.App || {};
  window.App.console = {
    fetchConsoleLogs: fetchConsoleLogs,
    startConsoleAutoRefresh: startConsoleAutoRefresh,
    stopConsoleAutoRefresh: stopConsoleAutoRefresh
  };
})();
