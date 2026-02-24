/**
 * История сигналов: загрузка, отображение, пометка устаревшим, удаление.
 * Рефакторинг: использует ApiClient, Constants; разделение ответственности.
 */
(function() {
  'use strict';

  var M = window.App.Constants.MESSAGES;
  var escapeHtml = function(s) { return window.App.escapeHtml(s); };
  var formatSignalTime = function(ts) { return window.App.formatSignalTime(ts); };
  var formatDirection = function(dir) { return window.App.formatDirection(dir); };
  var api = window.App.ApiClient;

  var historyPeriod = 'all';
  var lastHistoryData = [];
  var historyCard = document.getElementById('history-card');
  var historyList = document.getElementById('history-list');

  function normalizeItem(s) {
    if (!s || typeof s !== 'object') return null;
    return {
      id: s.id,
      ts: s.ts != null ? s.ts : (s.timestamp != null ? s.timestamp : 0),
      token: s.token || '',
      direction: s.direction || '',
      profit_usd: s.profit_usd != null ? s.profit_usd : (s.profitUsd != null ? s.profitUsd : 0),
      notional_usd: s.notional_usd != null ? s.notional_usd : (s.notionalUsd != null ? s.notionalUsd : 0),
      status: s.status || 'active',
      is_stale: s.is_stale != null ? s.is_stale : (s.isStale != null ? s.isStale : (s.status === 'stale'))
    };
  }

  function showAlert(msg) {
    if (window.Telegram && window.Telegram.WebApp && window.Telegram.WebApp.showAlert) {
      window.Telegram.WebApp.showAlert(msg);
    }
  }

  async function markSignalStale(id) {
    try {
      var data = await api.patch('/api/signal-history', { id: id, status: 'stale' });
      if (data && data.ok) window.App.history.fetchSignalHistory();
    } catch (e) {
      showAlert('Ошибка: ' + (e.message || e));
    }
  }

  async function deleteSignal(id) {
    if (!confirm(M.DELETE_CONFIRM)) return;
    try {
      var data = await api.del('/api/signal-history', { id: id });
      if (data && data.ok) window.App.history.fetchSignalHistory();
    } catch (e) {
      showAlert('Ошибка: ' + (e.message || e));
    }
  }

  function renderHistory(data) {
    if (!historyList) return;
    var raw = Array.isArray(data) ? data : [];
    var hideStale = document.getElementById('history-hide-stale') ? document.getElementById('history-hide-stale').checked : false;
    var normalized = raw.map(normalizeItem).filter(Boolean);
    var filtered = hideStale ? normalized.filter(function(s) { return !(s.is_stale || s.status === 'stale'); }) : normalized;

    if (filtered.length === 0) {
      historyList.innerHTML = hideStale && normalized.length > 0
        ? '<li class="history-empty">' + M.ALL_STALE + '</li>'
        : '<li class="history-empty">' + M.NO_SIGNALS + '</li>';
      return;
    }

    historyList.innerHTML = filtered.map(function(s) {
      var stale = s.is_stale || s.status === 'stale';
      var hasId = s.id != null && !isNaN(Number(s.id));
      var token = String(s.token || '').trim() || '—';
      var dir = formatDirection(s.direction || '');
      var profit = Number(s.profit_usd);
      var ts = Number(s.ts) || 0;
      return (
        '<li class="history-item ' + (stale ? 'stale' : '') + '" data-id="' + (hasId ? s.id : '') + '">' +
        '<div><div class="history-item-token">' + escapeHtml(token) + (stale ? ' <span class="history-item-stale-label">(устарел)</span>' : '') + '</div>' +
        '<div class="history-item-direction">' + escapeHtml(dir) + '</div></div>' +
        '<div class="history-item-right">' +
        '<div class="history-item-actions">' +
        (hasId && !stale ? '<button type="button" class="history-btn stale-btn" title="Пометить устаревшим">⏱</button>' : '') +
        (hasId ? '<button type="button" class="history-btn del-btn" title="Удалить">✕</button>' : '') +
        '</div>' +
        '<div class="history-item-meta">' +
        '<div class="history-item-profit">+' + (isNaN(profit) ? '0.00' : profit.toFixed(2)) + '$</div>' +
        '<div class="history-item-time">' + escapeHtml(ts ? formatSignalTime(ts) : '') + '</div>' +
        '</div></div></li>'
      );
    }).join('');

    historyList.querySelectorAll('.history-btn.stale-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var li = btn.closest('.history-item');
        var id = li ? li.dataset.id : null;
        if (id) markSignalStale(parseInt(id, 10));
      });
    });
    historyList.querySelectorAll('.history-btn.del-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var li = btn.closest('.history-item');
        var id = li ? li.dataset.id : null;
        if (id) deleteSignal(parseInt(id, 10));
      });
    });
  }

  async function fetchSignalHistory() {
    if (!historyList) return;
    historyList.innerHTML = '<li class="history-empty">' + M.LOADING + '</li>';

    var base = window.App.getApiBase();
    if (!base || typeof base !== 'string') {
      historyList.innerHTML = '<li class="history-empty">' + M.API_ADDRESS_REQUIRED + '</li>';
      return;
    }

    try {
      var raw = await api.get('/api/signal-history', { period: historyPeriod });

      if (Array.isArray(raw)) {
        lastHistoryData = raw;
      } else if (raw && typeof raw === 'object' && Array.isArray(raw.data)) {
        lastHistoryData = raw.data;
      } else if (raw && typeof raw === 'object' && Array.isArray(raw.signals)) {
        lastHistoryData = raw.signals;
      } else if (raw && typeof raw === 'object' && Array.isArray(raw.items)) {
        lastHistoryData = raw.items;
      } else {
        lastHistoryData = [];
      }

      if (lastHistoryData.length === 0) {
        historyList.innerHTML = '<li class="history-empty">' + M.NO_SIGNALS + '</li>';
        return;
      }
      renderHistory(lastHistoryData);
    } catch (e) {
      var errMsg = e.status === 401 ? M.AUTH_REQUIRED : e.status === 429 ? M.RATE_LIMIT_SHORT : (e.message || String(e));
      historyList.innerHTML = '<li class="history-empty">Ошибка загрузки: ' + escapeHtml(errMsg) + '</li>';
    }
  }

  window.App = window.App || {};
  window.App.history = {
    fetchSignalHistory: fetchSignalHistory,
    getLastHistoryData: function() { return lastHistoryData; },
    setHistoryPeriod: function(p) { historyPeriod = p; },
    getHistoryPeriod: function() { return historyPeriod; },
    getHistoryCard: function() { return historyCard; },
    renderHistory: renderHistory
  };
})();
