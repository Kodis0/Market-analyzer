/**
 * История сигналов: загрузка, отображение, пометка устаревшим, удаление.
 */
(function() {
  const getApiBase = () => window.App.getApiBase();
  const getAuthHeaders = () => window.App.getAuthHeaders();
  const escapeHtml = (s) => window.App.escapeHtml(s);
  const formatSignalTime = (ts) => window.App.formatSignalTime(ts);
  const formatDirection = (dir) => window.App.formatDirection(dir);

  let historyPeriod = 'all';
  let lastHistoryData = [];
  const historyCard = document.getElementById('history-card');
  const historyHeader = document.getElementById('history-header');
  const historyList = document.getElementById('history-list');

  async function markSignalStale(id) {
    try {
      const r = await fetch(getApiBase() + '/api/signal-history', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ id, status: 'stale' })
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      if (data.ok) window.App.history.fetchSignalHistory();
    } catch (e) {
      if (window.Telegram?.WebApp?.showAlert) window.Telegram.WebApp.showAlert('Ошибка: ' + (e.message || e));
    }
  }

  async function deleteSignal(id) {
    if (!confirm('Удалить сигнал из истории?')) return;
    try {
      const r = await fetch(getApiBase() + '/api/signal-history?id=' + id, {
        method: 'DELETE',
        headers: getAuthHeaders()
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      if (data.ok) window.App.history.fetchSignalHistory();
    } catch (e) {
      if (window.Telegram?.WebApp?.showAlert) window.Telegram.WebApp.showAlert('Ошибка: ' + (e.message || e));
    }
  }

  function renderHistory(data) {
    const hideStale = document.getElementById('history-hide-stale')?.checked ?? false;
    const filtered = hideStale ? data.filter(s => !(s.is_stale || s.status === 'stale')) : data;
    if (filtered.length === 0) {
      historyList.innerHTML = hideStale && data.length > 0
        ? '<li class="history-empty">Все сигналы устарели. Снимите галочку «Скрыть устаревшие»</li>'
        : '<li class="history-empty">Нет сигналов за выбранный период</li>';
      return;
    }
    historyList.innerHTML = filtered.map(s => {
      const stale = s.is_stale || s.status === 'stale';
      const hasId = s.id != null && !isNaN(Number(s.id));
      return `
      <li class="history-item ${stale ? 'stale' : ''}" data-id="${hasId ? s.id : ''}">
        <div>
          <div class="history-item-token">${escapeHtml(s.token)}${stale ? ' <span style="font-size:0.7em;color:var(--text-dim)">(устарел)</span>' : ''}</div>
          <div class="history-item-direction">${escapeHtml(formatDirection(s.direction))}</div>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <div class="history-item-actions">
            ${hasId && !stale ? '<button type="button" class="history-btn stale-btn" title="Пометить устаревшим">⏱</button>' : ''}
            ${hasId ? '<button type="button" class="history-btn del-btn" title="Удалить">✕</button>' : ''}
          </div>
          <div style="text-align: right;">
            <div class="history-item-profit">+${Number(s.profit_usd).toFixed(2)}$</div>
            <div class="history-item-time">${formatSignalTime(s.ts)}</div>
          </div>
        </div>
      </li>
    `}).join('');
    historyList.querySelectorAll('.history-btn.stale-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const li = btn.closest('.history-item');
        const id = li?.dataset?.id;
        if (id) markSignalStale(parseInt(id, 10));
      });
    });
    historyList.querySelectorAll('.history-btn.del-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const li = btn.closest('.history-item');
        const id = li?.dataset?.id;
        if (id) deleteSignal(parseInt(id, 10));
      });
    });
  }

  async function fetchSignalHistory() {
    historyList.innerHTML = '<li class="history-empty">Загрузка...</li>';
    try {
      const r = await fetch(getApiBase() + '/api/signal-history?period=' + historyPeriod, { headers: getAuthHeaders() });
      if (!r.ok) {
        const msg = r.status === 401 ? 'Откройте дашборд через Telegram (кнопка «Навигация»)' : r.status === 429 ? 'Слишком много запросов. Подождите минуту.' : 'HTTP ' + r.status;
        throw new Error(msg);
      }
      const data = await r.json();
      lastHistoryData = Array.isArray(data) ? data : [];
      if (lastHistoryData.length === 0) {
        const hint = historyPeriod !== 'all' ? ' Попробуйте период «Всё время».' : '';
        historyList.innerHTML = '<li class="history-empty">Нет сигналов за выбранный период.' + hint + '</li>';
        return;
      }
      renderHistory(lastHistoryData);
    } catch (e) {
      historyList.innerHTML = '<li class="history-empty">Ошибка загрузки: ' + escapeHtml(e.message || String(e)) + '</li>';
    }
  }

  window.App = window.App || {};
  window.App.history = {
    fetchSignalHistory,
    getLastHistoryData: function() { return lastHistoryData; },
    setHistoryPeriod: function(p) { historyPeriod = p; },
    getHistoryPeriod: function() { return historyPeriod; },
    getHistoryCard: function() { return historyCard; },
    renderHistory
  };
})();
