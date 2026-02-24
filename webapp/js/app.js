/**
 * Точка входа: инициализация, привязка событий, запуск загрузки данных.
 * Зависит от: config, utils, constants, api-client, chart, history, settings, console.
 * Размер canvas задаётся в chart.js (DPR + ResizeObserver); здесь только запрос данных и отложенная перерисовка для Telegram Desktop.
 */
(function() {
  'use strict';

  var MAIN = (window.App && window.App.Constants && window.App.Constants.MAIN) || {
    REFRESH_INTERVAL_SEC: 5,
    REFRESH_LABEL: 'Refresh',
    REFRESHING_LABEL: 'Refresh...'
  };
  var historyCard = document.getElementById('history-card');
  var btnRefresh = document.getElementById('btn-refresh');
  var btnRefreshLabel = btnRefresh ? btnRefresh.querySelector('.btn-refresh-label') : null;
  var mainAutoRefreshTimer = null;

  function loadChart() {
    if (window.App && window.App.chart && window.App.chart.fetchAndDraw) {
      window.App.chart.fetchAndDraw();
    }
  }

  function redrawChart() {
    if (window.App && window.App.chart && window.App.chart.redraw) {
      window.App.chart.redraw();
    }
  }

  function setRefreshButtonLoading(loading) {
    if (!btnRefresh) return;
    if (loading) {
      btnRefresh.classList.add('loading');
      btnRefresh.disabled = true;
      if (btnRefreshLabel) btnRefreshLabel.textContent = MAIN.REFRESHING_LABEL;
    } else {
      btnRefresh.classList.remove('loading');
      btnRefresh.disabled = false;
      if (btnRefreshLabel) btnRefreshLabel.textContent = MAIN.REFRESH_LABEL;
    }
  }

  async function refreshMainData() {
    if (!btnRefresh) return;
    setRefreshButtonLoading(true);
    var start = Date.now();
    await new Promise(function(r) { requestAnimationFrame(r); });
    try {
      var chart = window.App && window.App.chart;
      var history = window.App && window.App.history;
      var promises = [];
      if (chart && chart.fetchAndDraw) promises.push(chart.fetchAndDraw());
      if (history && history.fetchSignalHistory) promises.push(history.fetchSignalHistory());
      if (promises.length) await Promise.all(promises);
    } catch (e) {
      console.error('Refresh failed:', e);
    } finally {
      var elapsed = Date.now() - start;
      var minVisible = 300;
      if (elapsed < minVisible) {
        await new Promise(function(r) { setTimeout(r, minVisible - elapsed); });
      }
      setRefreshButtonLoading(false);
    }
  }

  function startMainAutoRefresh() {
    stopMainAutoRefresh();
    mainAutoRefreshTimer = setInterval(function() {
      refreshMainData();
    }, MAIN.REFRESH_INTERVAL_SEC * 1000);
  }

  function stopMainAutoRefresh() {
    if (mainAutoRefreshTimer) {
      clearInterval(mainAutoRefreshTimer);
      mainAutoRefreshTimer = null;
    }
  }

  window.addEventListener('resize', loadChart);
  setTimeout(redrawChart, 100);
  setTimeout(redrawChart, 400);

  if (historyCard) historyCard.classList.add('expanded');
  refreshMainData();

  if (window.Telegram && window.Telegram.WebApp) {
    window.Telegram.WebApp.ready();
    window.Telegram.WebApp.expand();
  }

  if (btnRefresh) {
    btnRefresh.addEventListener('click', function() {
      refreshMainData();
    });
  }

  document.getElementById('period-select').addEventListener('change', function(e) {
    window.App.chart.setCurrentPeriod(e.target.value);
    window.App.chart.fetchAndDraw();
  });

  document.getElementById('history-header').addEventListener('click', function() {
    if (historyCard) historyCard.classList.toggle('expanded');
    if (historyCard && historyCard.classList.contains('expanded')) {
      window.App.history.fetchSignalHistory();
    }
  });

  document.getElementById('history-period').addEventListener('change', function(e) {
    window.App.history.setHistoryPeriod(e.target.value);
    window.App.history.fetchSignalHistory();
  });

  document.getElementById('history-hide-stale').addEventListener('change', function() {
    var data = window.App.history.getLastHistoryData();
    if (data && data.length) window.App.history.renderHistory(data);
  });

  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState !== 'visible') return;
    var tabMain = document.getElementById('tab-main');
    var tabSettings = document.getElementById('tab-settings');
    if (tabMain && tabMain.classList.contains('active')) {
      refreshMainData();
      startMainAutoRefresh();
    }
    if (tabSettings && tabSettings.classList.contains('active')) {
      window.App.settings.fetchStatusAndSettings();
    }
  });

  document.querySelectorAll('.nav-item').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var tab = btn.dataset.tab;
      document.querySelectorAll('.tab-panel').forEach(function(p) {
        p.classList.remove('active');
      });
      document.querySelectorAll('.nav-item').forEach(function(b) {
        b.classList.remove('active');
      });
      var tabEl = document.getElementById('tab-' + tab);
      if (tabEl) tabEl.classList.add('active');
      btn.classList.add('active');
      if (tab === 'main') {
        window.App.history.fetchSignalHistory();
        startMainAutoRefresh();
      } else {
        stopMainAutoRefresh();
      }
      if (tab === 'settings') {
        window.App.settings.fetchStatusAndSettings();
      }
      if (tab === 'console') {
        window.App.console.fetchConsoleLogs();
        window.App.console.startConsoleAutoRefresh();
      } else {
        window.App.console.stopConsoleAutoRefresh();
      }
    });
  });

  startMainAutoRefresh();

  var consoleAutoRefresh = document.getElementById('console-auto-refresh');
  if (consoleAutoRefresh) {
    consoleAutoRefresh.addEventListener('change', function() {
      var tabConsole = document.getElementById('tab-console');
      if (tabConsole && tabConsole.classList.contains('active')) {
        if (this.checked) window.App.console.startConsoleAutoRefresh();
        else window.App.console.stopConsoleAutoRefresh();
      }
    });
  }

  var consoleRefreshInterval = document.getElementById('console-refresh-interval');
  if (consoleRefreshInterval) {
    consoleRefreshInterval.addEventListener('change', function() {
      var tabConsole = document.getElementById('tab-console');
      var cb = document.getElementById('console-auto-refresh');
      if (tabConsole && tabConsole.classList.contains('active') && cb && cb.checked) {
        window.App.console.startConsoleAutoRefresh();
      }
    });
  }

  document.getElementById('btn-console-refresh').addEventListener('click', window.App.console.fetchConsoleLogs);
})();
