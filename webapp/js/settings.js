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
  const settingsList = document.getElementById('settings-list');

  const SETTINGS_HIDDEN_KEYS = new Set(['exchange_enabled', 'auto_tune_enabled', 'auto_tune_bounds', 'delete_stale']);

  function setStatusDot(el, ok) {
    el.classList.remove('ok', 'err', 'unknown');
    el.classList.add(ok === true ? 'ok' : (ok === false ? 'err' : 'unknown'));
  }

  async function fetchStatusAndSettings() {
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
        const msg = statusRes.status === 401 ? 'Откройте дашборд через Telegram (кнопка «Навигация»)' : statusRes.status === 429 ? 'Слишком много запросов. Подождите минуту.' : 'Status ' + statusRes.status;
        throw new Error(msg);
      }
      const status = await statusRes.json();
      setStatusDot(apiDot, true);
      apiVal.textContent = 'Онлайн';
      const exEnabled = !!status.exchange_enabled;
      setStatusDot(exchangeDot, exEnabled);
      exchangeVal.textContent = exEnabled ? 'Включена' : 'Выключена';
      exchangeToggle.classList.toggle('on', exEnabled);
      exchangeToggle.classList.toggle('off', !exEnabled);
      if ('auto_tune_enabled' in status) {
        autoTuneToggle.classList.toggle('on', !!status.auto_tune_enabled);
        autoTuneToggle.classList.toggle('off', !status.auto_tune_enabled);
      }

      if (settingsRes.ok) {
        const data = await settingsRes.json();
        const s = data.settings || {};
        const labels = data.labels || {};
        renderSettingsList(s, labels);
      }
    } catch (e) {
      setStatusDot(apiDot, false);
      apiVal.textContent = 'Офлайн';
      setStatusDot(exchangeDot, null);
      exchangeVal.textContent = '—';
      exchangeToggle.classList.add('off');
      exchangeToggle.classList.remove('on');
      autoTuneToggle?.classList.add('off');
      autoTuneToggle?.classList.remove('on');
      settingsList.innerHTML = '';
    }
  }

  async function updateSetting(key, value) {
    try {
      const r = await fetch(getApiBase() + '/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ [key]: value })
      });
      if (!r.ok) throw new Error(r.status === 401 ? 'Откройте дашборд через Telegram (кнопка «Навигация»)' : 'HTTP ' + r.status);
      const data = await r.json();
      if (data.updated && data.updated[key] !== undefined) {
        const item = settingsList.querySelector('[data-key="' + key + '"]');
        if (item) {
          const toggle = item.querySelector('.settings-toggle');
          const input = item.querySelector('.settings-input');
          if (toggle) {
            toggle.classList.toggle('on', data.updated[key]);
            toggle.classList.toggle('off', !data.updated[key]);
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

  function renderSettingsList(s, labels) {
    settingsList.innerHTML = Object.entries(s)
      .filter(([k]) => !SETTINGS_HIDDEN_KEYS.has(k))
      .map(([k, v]) => {
        const label = escapeHtml(labels[k] || k);
        const isBool = typeof v === 'boolean';
        if (isBool) {
          const on = v ? 'on' : 'off';
          return '<div class="settings-item" data-key="' + escapeHtml(k) + '"><span class="settings-item-key">' + label + '</span><div class="settings-toggle ' + on + '" data-key="' + escapeHtml(k) + '" role="button" tabindex="0"></div></div>';
        }
        const step = Number.isInteger(v) ? '1' : '0.01';
        return '<div class="settings-item" data-key="' + escapeHtml(k) + '"><span class="settings-item-key">' + label + '</span><input type="number" class="settings-input" data-key="' + escapeHtml(k) + '" value="' + escapeHtml(String(v)) + '" step="' + step + '"></div>';
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

  document.getElementById('btn-settings-refresh').addEventListener('click', fetchStatusAndSettings);

  autoTuneToggle.addEventListener('click', async () => {
    const next = !autoTuneToggle.classList.contains('on');
    autoTuneToggle.classList.toggle('on', next);
    autoTuneToggle.classList.toggle('off', !next);
    try {
      const r = await fetch(getApiBase() + '/api/auto_tune', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ enabled: next })
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const d = await r.json();
      const ok = !!(d?.auto_tune?.enabled ?? d?.enabled);
      autoTuneToggle.classList.toggle('on', ok);
      autoTuneToggle.classList.toggle('off', !ok);
    } catch (_) {
      autoTuneToggle.classList.toggle('on', !next);
      autoTuneToggle.classList.toggle('off', next);
      fetchStatusAndSettings();
    }
  });

  exchangeToggle.addEventListener('click', async () => {
    const next = !exchangeToggle.classList.contains('on');
    try {
      const r = await fetch(getApiBase() + '/api/exchange', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ enabled: next })
      });
      if (!r.ok) throw new Error(r.status === 401 ? 'Откройте дашборд через Telegram (кнопка «Навигация»)' : 'HTTP ' + r.status);
      const data = await r.json();
      exchangeToggle.classList.toggle('on', data.exchange_enabled);
      exchangeToggle.classList.toggle('off', !data.exchange_enabled);
      setStatusDot(exchangeDot, data.exchange_enabled);
      exchangeVal.textContent = data.exchange_enabled ? 'Включена' : 'Выключена';
    } catch (e) {
      fetchStatusAndSettings();
    }
  });

  window.App = window.App || {};
  window.App.settings = {
    fetchStatusAndSettings,
    getAutoTuneToggle: function() { return autoTuneToggle; }
  };
})();
