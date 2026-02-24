/**
 * Настройки бота: статус API/биржи, список параметров, переключатели.
 */
(function() {
  const getApiBase = () => window.App.getApiBase();
  const getAuthHeaders = () => window.App.getAuthHeaders();
  const escapeHtml = (s) => window.App.escapeHtml(s);

  const apiDot = document.getElementById('status-api-dot');
  const apiVal = document.getElementById('status-api-val');
  const exchangeDot = document.getElementById('status-exchange-dot');
  const exchangeVal = document.getElementById('status-exchange-val');
  const exchangeToggle = document.getElementById('exchange-toggle');
  const autoTuneToggle = document.getElementById('auto-tune-toggle');
  const deleteStaleToggle = document.getElementById('delete-stale-toggle');
  const exchangeWrap = document.getElementById('exchange-toggle-wrap');
  const autoTuneWrap = document.getElementById('auto-tune-wrap');
  const deleteStaleWrap = document.getElementById('delete-stale-wrap');
  const settingsList = document.getElementById('settings-list');

  const SETTINGS_HIDDEN_KEYS = new Set(['exchange_enabled', 'auto_tune_enabled', 'auto_tune_bounds', 'delete_stale']);
  const DEBOUNCE_MS = 150;

  let fetchDebounceTimer = null;

  function setStatusDot(el, ok) {
    el.classList.remove('ok', 'err', 'unknown');
    el.classList.add(ok === true ? 'ok' : (ok === false ? 'err' : 'unknown'));
  }

  function setToggleState(toggleEl, on) {
    if (!toggleEl) return;
    toggleEl.classList.toggle('on', on);
    toggleEl.classList.toggle('off', !on);
  }

  function showError(msg) {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(msg);
    } else {
      console.error(msg);
    }
  }

  const AUTH_MSG = 'Откройте дашборд через Telegram (кнопка «Навигация»)';

  async function fetchStatusAndSettingsImpl() {
    setStatusDot(apiDot, null);
    apiVal.textContent = 'Проверка...';
    setStatusDot(exchangeDot, null);
    exchangeVal.textContent = '—';

    try {
      const [statusRes, settingsRes] = await Promise.all([
        fetch(getApiBase() + '/api/status', { headers: getAuthHeaders() }),
        fetch(getApiBase() + '/api/settings', { headers: getAuthHeaders() })
      ]);

      if (!statusRes.ok) {
        const msg = statusRes.status === 401 ? AUTH_MSG : statusRes.status === 429 ? 'Слишком много запросов. Подождите минуту.' : 'Status ' + statusRes.status;
        throw new Error(msg);
      }
      const status = await statusRes.json();
      setStatusDot(apiDot, true);
      apiVal.textContent = 'Онлайн';
      const exEnabled = !!status.exchange_enabled;
      setStatusDot(exchangeDot, exEnabled);
      exchangeVal.textContent = exEnabled ? 'Включена' : 'Выключена';
      setToggleState(exchangeToggle, exEnabled);
      if ('auto_tune_enabled' in status) {
        setToggleState(autoTuneToggle, !!status.auto_tune_enabled);
      }

      if (settingsRes.ok) {
        const data = await settingsRes.json();
        const s = data.settings || {};
        const labels = data.labels || {};
        if ('delete_stale' in s) {
          setToggleState(deleteStaleToggle, !!s.delete_stale);
        }
        renderSettingsList(s, labels);
      }
    } catch (e) {
      setStatusDot(apiDot, false);
      apiVal.textContent = 'Офлайн';
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
    fetchDebounceTimer = setTimeout(() => {
      fetchDebounceTimer = null;
      fetchStatusAndSettingsImpl();
    }, DEBOUNCE_MS);
  }

  async function updateSetting(key, value) {
    try {
      const r = await fetch(getApiBase() + '/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ [key]: value })
      });
      if (!r.ok) throw new Error(r.status === 401 ? AUTH_MSG : 'HTTP ' + r.status);
      const data = await r.json();
      if (data.updated && data.updated[key] !== undefined) {
        const safeKey = CSS.escape ? CSS.escape(key) : key.replace(/[!"#$%&'()*+,.\/:;<=>?@[\\\]^`{|}~]/g, '\\$&');
        const item = settingsList.querySelector('[data-key="' + safeKey + '"]');
        if (item) {
          const toggle = item.querySelector('.settings-toggle');
          const input = item.querySelector('.settings-input');
          if (toggle) {
            setToggleState(toggle, data.updated[key]);
          }
          if (input) {
            input.value = data.updated[key];
            input.dataset.lastValid = String(data.updated[key]);
          }
        }
      }
    } catch (e) {
      fetchStatusAndSettings();
    }
  }

  async function updateToggle(id, endpoint, bodyKey, bodyValue, onSuccess) {
    const toggleEl = id === 'exchange' ? exchangeToggle : id === 'auto_tune' ? autoTuneToggle : deleteStaleToggle;
    const next = !toggleEl.classList.contains('on');
    setToggleState(toggleEl, next);

    try {
      const body = bodyKey ? { [bodyKey]: bodyValue !== undefined ? bodyValue : next } : { enabled: next };
      const r = await fetch(getApiBase() + endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify(body)
      });
      if (!r.ok) throw new Error(r.status === 401 ? AUTH_MSG : 'HTTP ' + r.status);
      const data = await r.json();

      if (id === 'exchange') {
        const ok = data.exchange_enabled !== undefined ? data.exchange_enabled : next;
        setToggleState(exchangeToggle, ok);
        setStatusDot(exchangeDot, ok);
        exchangeVal.textContent = ok ? 'Включена' : 'Выключена';
      } else if (id === 'auto_tune') {
        const ok = !!(data?.auto_tune?.enabled ?? data?.enabled ?? next);
        setToggleState(autoTuneToggle, ok);
      } else if (id === 'delete_stale') {
        const ok = data.updated?.delete_stale !== undefined ? data.updated.delete_stale : next;
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
      showError(e.message || 'Ошибка сохранения');
    }
  }

  function toggleExchange() {
    updateToggle('exchange', '/api/exchange', 'enabled', undefined);
  }

  function toggleAutoTune() {
    updateToggle('auto_tune', '/api/auto_tune', 'enabled', undefined);
  }

  function toggleDeleteStale() {
    updateToggle('delete_stale', '/api/settings', 'delete_stale', undefined, () => {
      if (window.App.history?.fetchSignalHistory) window.App.history.fetchSignalHistory();
    });
  }

  function bindToggleWrap(wrap, handler) {
    if (!wrap) return;
    wrap.addEventListener('click', (e) => { e.preventDefault(); handler(); });
    wrap.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        handler();
      }
    });
  }

  function renderSettingsList(s, labels) {
    if (!settingsList) return;
    settingsList.innerHTML = Object.entries(s)
      .filter(([k]) => !SETTINGS_HIDDEN_KEYS.has(k))
      .map(([k, v]) => {
        const label = escapeHtml(labels[k] || k);
        const safeK = escapeHtml(k);
        const isBool = typeof v === 'boolean';
        if (isBool) {
          const on = v ? 'on' : 'off';
          return '<div class="settings-item" data-key="' + safeK + '"><span class="settings-item-key">' + label + '</span><div class="settings-toggle ' + on + '" data-key="' + safeK + '" role="button" tabindex="0"></div></div>';
        }
        const step = Number.isInteger(v) ? '1' : '0.01';
        return '<div class="settings-item" data-key="' + safeK + '"><span class="settings-item-key">' + label + '</span><input type="number" class="settings-input" data-key="' + safeK + '" value="' + escapeHtml(String(v)) + '" step="' + step + '"></div>';
      }).join('');
    settingsList.querySelectorAll('.settings-toggle').forEach(el => {
      el.addEventListener('click', () => updateSetting(el.dataset.key, !el.classList.contains('on')));
    });
    const intKeys = ['persistence_hits', 'cooldown_sec', 'engine_tick_hz', 'max_ob_age_ms', 'stale_ttl_sec'];
    settingsList.querySelectorAll('.settings-input').forEach(inp => {
      inp.dataset.lastValid = inp.value;
      inp.addEventListener('change', () => {
        const num = parseFloat(String(inp.value).replace(',', '.'));
        if (!isNaN(num) && isFinite(num)) {
          const val = intKeys.includes(inp.dataset.key) ? Math.round(num) : num;
          updateSetting(inp.dataset.key, val);
        } else {
          inp.value = inp.dataset.lastValid || '';
        }
      });
      inp.addEventListener('blur', () => {
        const num = parseFloat(String(inp.value).replace(',', '.'));
        if (isNaN(num) || !isFinite(num)) inp.value = inp.dataset.lastValid || '';
      });
      inp.addEventListener('keydown', (e) => { if (e.key === 'Enter') inp.blur(); });
    });
  }

  document.getElementById('btn-settings-refresh').addEventListener('click', () => {
    if (fetchDebounceTimer) clearTimeout(fetchDebounceTimer);
    fetchDebounceTimer = null;
    fetchStatusAndSettingsImpl();
  });

  bindToggleWrap(exchangeWrap, toggleExchange);
  bindToggleWrap(autoTuneWrap, toggleAutoTune);
  bindToggleWrap(deleteStaleWrap, toggleDeleteStale);

  window.App = window.App || {};
  window.App.settings = {
    fetchStatusAndSettings
  };
})();
