/**
 * Точка входа: инициализация, привязка событий, запуск загрузки данных.
 * Зависит от: config, utils, chart, history, settings, console (подключаются до этого скрипта).
 * Размер canvas задаётся в chart.js (DPR + ResizeObserver); здесь только запрос данных и отложенная перерисовка для Telegram Desktop.
 */
(function() {
  const historyCard = window.App.history.getHistoryCard();

  function loadChart() {
    window.App.chart.fetchAndDraw();
  }

  window.addEventListener('resize', loadChart);
  loadChart();
  // Telegram Desktop: контейнер может получить размер с задержкой — перерисовка графика без повторного запроса
  setTimeout(function() { if (window.App.chart?.redraw) window.App.chart.redraw(); }, 100);
  setTimeout(function() { if (window.App.chart?.redraw) window.App.chart.redraw(); }, 400);

  historyCard.classList.add('expanded');
  window.App.history.fetchSignalHistory();

  if (window.Telegram && window.Telegram.WebApp) {
    window.Telegram.WebApp.ready();
    window.Telegram.WebApp.expand();
  }

  document.getElementById('btn-refresh').addEventListener('click', () => {
    window.App.chart.fetchAndDraw();
    window.App.history.fetchSignalHistory();
  });

  document.getElementById('period-select').addEventListener('change', (e) => {
    window.App.chart.setCurrentPeriod(e.target.value);
    window.App.chart.fetchAndDraw();
  });

  document.getElementById('history-header').addEventListener('click', () => {
    historyCard.classList.toggle('expanded');
    if (historyCard.classList.contains('expanded')) window.App.history.fetchSignalHistory();
  });

  document.getElementById('history-period').addEventListener('change', (e) => {
    window.App.history.setHistoryPeriod(e.target.value);
    window.App.history.fetchSignalHistory();
  });

  document.getElementById('history-hide-stale').addEventListener('change', () => {
    const data = window.App.history.getLastHistoryData();
    if (data && data.length) window.App.history.renderHistory(data);
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState !== 'visible') return;
    if (document.getElementById('tab-main')?.classList.contains('active')) {
      window.App.history.fetchSignalHistory();
    }
    if (document.getElementById('tab-settings')?.classList.contains('active')) {
      window.App.settings.fetchStatusAndSettings();
    }
  });

  document.querySelectorAll('.nav-item').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
      document.getElementById('tab-' + tab).classList.add('active');
      btn.classList.add('active');
      if (tab === 'main') {
        window.App.history.fetchSignalHistory();
      }
      if (tab === 'settings') {
        window.App.settings.fetchStatusAndSettings();
        fetch(window.App.getApiBase() + '/api/auto_tune', { headers: window.App.getAuthHeaders() })
          .then(r => r.ok ? r.json() : null)
          .then(d => {
            const autoTuneToggle = window.App.settings.getAutoTuneToggle();
            if (d && autoTuneToggle) {
              autoTuneToggle.classList.toggle('on', !!d.enabled);
              autoTuneToggle.classList.toggle('off', !d.enabled);
            }
          });
      }
      if (tab === 'console') {
        window.App.console.fetchConsoleLogs();
        window.App.console.startConsoleAutoRefresh();
      } else {
        window.App.console.stopConsoleAutoRefresh();
      }
    });
  });

  document.getElementById('console-auto-refresh')?.addEventListener('change', function() {
    if (document.getElementById('tab-console')?.classList.contains('active')) {
      if (this.checked) window.App.console.startConsoleAutoRefresh();
      else window.App.console.stopConsoleAutoRefresh();
    }
  });

  document.getElementById('console-refresh-interval')?.addEventListener('change', function() {
    if (document.getElementById('tab-console')?.classList.contains('active') && document.getElementById('console-auto-refresh')?.checked) {
      window.App.console.startConsoleAutoRefresh();
    }
  });

  document.getElementById('btn-console-refresh').addEventListener('click', window.App.console.fetchConsoleLogs);
})();
