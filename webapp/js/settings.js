/**
 * Настройки бота: статус API/биржи, список параметров, переключатели.
 * Рефакторинг: использует ApiClient, Constants; разделение ответственности.
 */
(function() {
  'use strict';

  var M = window.App.Constants.MESSAGES;
  var S = window.App.Constants.SETTINGS;
  var escapeHtml = function(s) { return window.App.escapeHtml(s); };
  var api = window.App.ApiClient;

  var apiDot = document.getElementById('status-api-dot');
  var apiVal = document.getElementById('status-api-val');
  var exchangeDot = document.getElementById('status-exchange-dot');
  var exchangeVal = document.getElementById('status-exchange-val');
  var exchangeToggle = document.getElementById('exchange-toggle');
  var autoTuneToggle = document.getElementById('auto-tune-toggle');
  var deleteStaleToggle = document.getElementById('delete-stale-toggle');
  var exchangeWrap = document.getElementById('exchange-toggle-wrap');
  var autoTuneWrap = document.getElementById('auto-tune-wrap');
  var deleteStaleWrap = document.getElementById('delete-stale-wrap');
  var settingsList = document.getElementById('settings-list');

  var fetchDebounceTimer = null;

  function setStatusDot(el, ok) {
    if (!el) return;
    el.classList.remove('ok', 'err', 'unknown');
    el.classList.add(ok === true ? 'ok' : (ok === false ? 'err' : 'unknown'));
  }

  function setToggleState(toggleEl, on) {
    if (!toggleEl) return;
    toggleEl.classList.toggle('on', on);
    toggleEl.classList.toggle('off', !on);
  }

  function showError(msg) {
    if (window.Telegram && window.Telegram.WebApp && window.Telegram.WebApp.showAlert) {
      window.Telegram.WebApp.showAlert(msg);
    } else {
      console.error(msg);
    }
  }

  async function fetchStatusAndSettingsImpl() {
    setStatusDot(apiDot, null);
    apiVal.textContent = M.CHECKING;
    setStatusDot(exchangeDot, null);
    exchangeVal.textContent = '—';

    try {
      var base = window.App.getApiBase();
      var headers = api.getDefaultHeaders();
      var statusUrl = base.replace(/\/+$/, '') + '/api/status';
      var settingsUrl = base.replace(/\/+$/, '') + '/api/settings';

      var statusRes = await fetch(statusUrl, { headers: headers });
      var settingsRes = await fetch(settingsUrl, { headers: headers });

      if (!statusRes.ok) {
        var msg = statusRes.status === 401 ? M.AUTH_REQUIRED :
          statusRes.status === 429 ? M.RATE_LIMIT_SHORT : 'Status ' + statusRes.status;
        throw new Error(msg);
      }

      var status = await statusRes.json();
      setStatusDot(apiDot, true);
      apiVal.textContent = M.ONLINE;
      var exEnabled = !!status.exchange_enabled;
      setStatusDot(exchangeDot, exEnabled);
      exchangeVal.textContent = exEnabled ? M.ENABLED : M.DISABLED;
      setToggleState(exchangeToggle, exEnabled);
      if ('auto_tune_enabled' in status) {
        setToggleState(autoTuneToggle, !!status.auto_tune_enabled);
      }

      if (settingsRes.ok) {
        var data = await settingsRes.json();
        var s = data.settings || {};
        var labels = data.labels || {};
        if ('delete_stale' in s) {
          setToggleState(deleteStaleToggle, !!s.delete_stale);
        }
        renderSettingsList(s, labels);
      }
    } catch (e) {
      setStatusDot(apiDot, false);
      apiVal.textContent = M.OFFLINE;
      setStatusDot(exchangeDot, null);
      exchangeVal.textContent = '—';
      setToggleState(exchangeToggle, false);
      setToggleState(autoTuneToggle, false);
      setToggleState(deleteStaleToggle, false);
      if (settingsList) settingsList.innerHTML = '';
    }
  }

  function fetchStatusAndSettings() {
    if (fetchDebounceTimer) clearTimeout(fetchDebounceTimer);
    fetchDebounceTimer = setTimeout(function() {
      fetchDebounceTimer = null;
      fetchStatusAndSettingsImpl();
    }, S.DEBOUNCE_MS);
  }

  async function updateSetting(key, value) {
    try {
      var body = {};
      body[key] = value;
      var data = await api.post('/api/settings', body);
      if (!data || !data.updated) return;
      if (data.updated[key] === undefined) return;

      var updatedVal = data.updated[key];
      var safeKey = CSS.escape ? CSS.escape(key) : key.replace(/[!"#$%&'()*+,.\/:;<=>?@[\\\]^`{|}~]/g, '\\$&');
      var item = settingsList.querySelector('[data-key="' + safeKey + '"]');
      if (!item) return;

      var toggle = item.querySelector('.settings-toggle');
      var input = item.querySelector('.settings-input');
      if (toggle) setToggleState(toggle, updatedVal);
      if (input) {
        input.value = updatedVal;
        input.dataset.lastValid = String(updatedVal);
      }
    } catch (e) {
      fetchStatusAndSettings();
    }
  }

  async function updateToggle(id, endpoint, bodyKey, bodyValue, onSuccess) {
    var toggleEl = id === 'exchange' ? exchangeToggle : id === 'auto_tune' ? autoTuneToggle : deleteStaleToggle;
    var next = !toggleEl.classList.contains('on');
    setToggleState(toggleEl, next);

    try {
      var body = bodyKey ? {} : { enabled: next };
      if (bodyKey) body[bodyKey] = bodyValue !== undefined ? bodyValue : next;

      var data = await api.post(endpoint, body);

      if (id === 'exchange') {
        var ok = data.exchange_enabled !== undefined ? data.exchange_enabled : next;
        setToggleState(exchangeToggle, ok);
        setStatusDot(exchangeDot, ok);
        exchangeVal.textContent = ok ? M.ENABLED : M.DISABLED;
      } else if (id === 'auto_tune') {
        var ok = !!(data && (data.auto_tune && data.auto_tune.enabled !== undefined ? data.auto_tune.enabled : (data.enabled !== undefined ? data.enabled : next)));
        setToggleState(autoTuneToggle, ok);
      } else if (id === 'delete_stale') {
        var ok = data.updated && data.updated.delete_stale !== undefined ? data.updated.delete_stale : next;
        setToggleState(deleteStaleToggle, ok);
        if (onSuccess) onSuccess();
      }
    } catch (e) {
      setToggleState(toggleEl, !next);
      if (id === 'exchange') {
        setStatusDot(exchangeDot, null);
        exchangeVal.textContent = '—';
      }
      fetchStatusAndSettings();
      showError(e.message || M.SAVE_ERROR);
    }
  }

  function toggleExchange() {
    updateToggle('exchange', '/api/exchange', 'enabled', undefined);
  }

  function toggleAutoTune() {
    updateToggle('auto_tune', '/api/auto_tune', 'enabled', undefined);
  }

  function toggleDeleteStale() {
    updateToggle('delete_stale', '/api/settings', 'delete_stale', undefined, function() {
      if (window.App.history && window.App.history.fetchSignalHistory) {
        window.App.history.fetchSignalHistory();
      }
    });
  }

  function bindToggleWrap(wrap, handler) {
    if (!wrap) return;
    wrap.addEventListener('click', function(e) { e.preventDefault(); handler(); });
    wrap.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        handler();
      }
    });
  }

  function renderSettingsList(s, labels) {
    if (!settingsList) return;
    settingsList.innerHTML = Object.entries(s)
      .filter(function(entry) { return !S.HIDDEN_KEYS.has(entry[0]); })
      .map(function(entry) {
        var k = entry[0];
        var v = entry[1];
        var label = escapeHtml(labels[k] || k);
        var safeK = escapeHtml(k);
        var isBool = typeof v === 'boolean';
        if (isBool) {
          var on = v ? 'on' : 'off';
          return '<div class="settings-item" data-key="' + safeK + '"><span class="settings-item-key">' + label + '</span><div class="settings-toggle ' + on + '" data-key="' + safeK + '" role="button" tabindex="0"></div></div>';
        }
        var step = Number.isInteger(v) ? '1' : '0.01';
        return '<div class="settings-item" data-key="' + safeK + '"><span class="settings-item-key">' + label + '</span><input type="number" class="settings-input" data-key="' + safeK + '" value="' + escapeHtml(String(v)) + '" step="' + step + '"></div>';
      }).join('');

    settingsList.querySelectorAll('.settings-toggle').forEach(function(el) {
      el.addEventListener('click', function() {
        updateSetting(el.dataset.key, !el.classList.contains('on'));
      });
    });

    settingsList.querySelectorAll('.settings-input').forEach(function(inp) {
      inp.dataset.lastValid = inp.value;
      inp.addEventListener('change', function() {
        var num = parseFloat(String(inp.value).replace(',', '.'));
        if (!isNaN(num) && isFinite(num)) {
          var val = S.INT_KEYS.includes(inp.dataset.key) ? Math.round(num) : num;
          updateSetting(inp.dataset.key, val);
        } else {
          inp.value = inp.dataset.lastValid || '';
        }
      });
      inp.addEventListener('blur', function() {
        var num = parseFloat(String(inp.value).replace(',', '.'));
        if (isNaN(num) || !isFinite(num)) inp.value = inp.dataset.lastValid || '';
      });
      inp.addEventListener('keydown', function(e) { if (e.key === 'Enter') inp.blur(); });
    });
  }

  document.getElementById('btn-settings-refresh').addEventListener('click', function() {
    if (fetchDebounceTimer) clearTimeout(fetchDebounceTimer);
    fetchDebounceTimer = null;
    fetchStatusAndSettingsImpl();
  });

  bindToggleWrap(exchangeWrap, toggleExchange);
  bindToggleWrap(autoTuneWrap, toggleAutoTune);
  bindToggleWrap(deleteStaleWrap, toggleDeleteStale);

  window.App = window.App || {};
  window.App.settings = {
    fetchStatusAndSettings: fetchStatusAndSettings
  };
})();
