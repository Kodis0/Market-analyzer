(function() {
const params = new URLSearchParams(location.search);
const tg = window.Telegram?.WebApp;
const startParam = tg?.startParam;
let rawApi = (params.get('api') ? decodeURIComponent(params.get('api')).trim() : null)
  || (startParam ? decodeURIComponent(startParam) : null)
  || (document.getElementById('app').dataset.apiUrl || 'http://localhost:8080');
if (!rawApi.startsWith('http://') && !rawApi.startsWith('https://')) {
  rawApi = 'https://' + rawApi.replace(/^\/+/, '');
}
if (rawApi && !rawApi.match(/:\d+(\/|$)/)) {
  if (!rawApi.startsWith('https://')) {
    rawApi = rawApi.replace(/:?\/?$/, '') + ':8080';
  } else {
    rawApi = rawApi.replace(/\/$/, '');
  }
}
let API_BASE = rawApi.replace(/\/$/, '');
const appEl = document.getElementById('app');
const API_DOMAIN = appEl?.dataset?.apiDomain || 'api.arbmarketsystem.ru';
const MARKET_DOMAIN = appEl?.dataset?.marketDomain || 'market.arbmarketsystem.ru';
const isFrontendOnly = typeof location !== 'undefined' && new RegExp(MARKET_DOMAIN.replace(/\./g, '\\.') + '$', 'i').test(location.hostname || '');
const API_FALLBACK = (typeof location !== 'undefined' && location.origin && !isFrontendOnly) ? location.origin.replace(/\/$/, '') : '';
document.getElementById('api-display').textContent = API_BASE;

function getAuthHeaders() {
  const initData = window.Telegram?.WebApp?.initData;
  return initData ? { 'X-Telegram-Init-Data': initData } : {};
}

let currentPeriod = '1h';
const ctx = document.getElementById('chart').getContext('2d');

function formatTime(ts, period) {
  const d = new Date(ts * 1000);
  if (period === '1h') return d.getHours() + ':' + String(d.getMinutes()).padStart(2, '0');
  if (period === '1d') return d.getHours() + ':00';
  if (period === '1w' || period === 'all') return d.getDate() + '/' + (d.getMonth() + 1);
  return d.toLocaleTimeString();
}

let chartState = { data: [], padding: null, chartW: 0, chartH: 0, yMax: 1, w: 0, h: 0 };

function smoothCurveThrough(ctx, points, padding, chartW, chartH, yMax) {
  const pts = points.map((v, i) => ({
    x: padding.left + (i / Math.max(1, points.length - 1)) * chartW,
    y: padding.top + chartH - (v / yMax) * chartH
  }));
  if (pts.length < 2) return;
  const tension = 0.3;
  ctx.moveTo(pts[0].x, pts[0].y);
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(0, i - 1)];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[Math.min(pts.length - 1, i + 2)];
    const cp1x = p1.x + (p2.x - p0.x) * tension / 6;
    const cp1y = p1.y + (p2.y - p0.y) * tension / 6;
    const cp2x = p2.x - (p3.x - p1.x) * tension / 6;
    const cp2y = p2.y - (p3.y - p1.y) * tension / 6;
    ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
  }
}

function drawChart(data) {
  const w = ctx.canvas.width || 300;
  const h = ctx.canvas.height || 220;
  ctx.clearRect(0, 0, w, h);
  chartState.data = [];
  chartState.padding = null;

  if (!data || data.length === 0) {
    ctx.fillStyle = '#9e9e9e';
    ctx.font = '14px Inter, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('Нет данных', w / 2, h / 2);
    return;
  }

  const padding = { top: 24, right: 24, bottom: 32, left: 48 };
  const chartW = w - padding.left - padding.right;
  const chartH = h - padding.top - padding.bottom;

  const jupMax = Math.max(1, ...data.map(d => d.jupiter || 0));
  const bybitMax = Math.max(1, ...data.map(d => d.bybit || 0));
  const yMax = Math.max(jupMax, bybitMax, 1);

  chartState = { data, padding, chartW, chartH, yMax, w, h };

  const jupColor = '#3B82F6';
  const bybitColor = '#FFC107';

  function drawSmoothLine(key, color, fill) {
    const arr = data.map(d => d[key] || 0);
    ctx.beginPath();
    smoothCurveThrough(ctx, arr, padding, chartW, chartH, yMax);
    if (fill) {
      ctx.lineTo(padding.left + chartW, padding.top + chartH);
      ctx.lineTo(padding.left, padding.top + chartH);
      ctx.closePath();
      ctx.fillStyle = color === jupColor ? 'rgba(59, 130, 246, 0.15)' : 'rgba(255, 193, 7, 0.12)';
      ctx.fill();
      ctx.beginPath();
      smoothCurveThrough(ctx, arr, padding, chartW, chartH, yMax);
    }
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.5;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();
  }

  drawSmoothLine('jupiter', jupColor, true);
  drawSmoothLine('bybit', bybitColor, true);

  ctx.fillStyle = '#9e9e9e';
  ctx.font = '10px Inter, sans-serif';
  ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(data.length / 5));
  for (let i = 0; i < data.length; i += step) {
    const x = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
    ctx.fillText(formatTime(data[i].ts, currentPeriod), x, h - 10);
  }
}

function showChartTooltip(e) {
  const wrap = document.getElementById('chart-wrap');
  const tooltip = document.getElementById('chart-tooltip');
  const { data, padding, chartW, chartH, w } = chartState;
  if (!data || data.length === 0 || !padding) {
    tooltip.classList.remove('visible');
    return;
  }
  const rect = wrap.getBoundingClientRect();
  const x = e.clientX - rect.left;
  if (x < padding.left || x > padding.left + chartW) {
    tooltip.classList.remove('visible');
    return;
  }
  const idx = Math.round(((x - padding.left) / chartW) * (data.length - 1));
  const i = Math.max(0, Math.min(idx, data.length - 1));
  const d = data[i];
  const jup = d.jupiter || 0;
  const bybit = d.bybit || 0;
  const maxVal = Math.max(jup, bybit);
  const minVal = Math.min(jup, bybit);
  tooltip.innerHTML =
    '<div style="font-weight:600;margin-bottom:6px;color:var(--text-dim)">' + formatTime(d.ts, currentPeriod) + '</div>' +
    '<div class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter: <b>' + jup + '</b></div>' +
    '<div class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit: <b>' + bybit + '</b></div>' +
    (maxVal > 0 ? '<div style="margin-top:6px;font-size:0.75rem;color:var(--text-dim)">Макс: ' + maxVal + ' · Мин: ' + minVal + '</div>' : '');
  tooltip.style.left = (x + 14) + 'px';
  tooltip.style.top = (e.clientY - rect.top - 80) + 'px';
  tooltip.classList.add('visible');
  const ttRect = tooltip.getBoundingClientRect();
  let left = x + 14;
  if (left + ttRect.width > rect.width - 8) left = x - ttRect.width - 14;
  let top = e.clientY - rect.top - ttRect.height - 10;
  if (top < 8) top = e.clientY - rect.top + 10;
  tooltip.style.left = Math.max(8, Math.min(left, rect.width - ttRect.width - 8)) + 'px';
  tooltip.style.top = Math.max(8, top) + 'px';
}

function hideChartTooltip() {
  document.getElementById('chart-tooltip').classList.remove('visible');
}

const chartWrap = document.getElementById('chart-wrap');
chartWrap.addEventListener('mousemove', showChartTooltip);
chartWrap.addEventListener('mouseleave', hideChartTooltip);
chartWrap.addEventListener('touchmove', (e) => { if (e.touches[0]) showChartTooltip({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY }); });
chartWrap.addEventListener('touchend', hideChartTooltip);

function updateStats(data) {
  const el = document.getElementById('stats-row');
  if (!data || data.length === 0) {
    el.style.display = 'none';
    return;
  }
  const totalJup = data.reduce((s, d) => s + (d.jupiter || 0), 0);
  const totalBybit = data.reduce((s, d) => s + (d.bybit || 0), 0);
  if (totalJup === 0 && totalBybit === 0) {
    el.style.display = 'none';
    return;
  }
  el.style.display = 'flex';
  el.innerHTML = '<span class="stat-badge jupiter">Jupiter: ' + totalJup + '</span><span class="stat-badge bybit">Bybit: ' + totalBybit + '</span>';
}

async function fetchFromUrl(base) {
  const url = base + '/api/stats?period=' + currentPeriod;
  const r = await fetch(url, { cache: 'no-store', headers: getAuthHeaders() });
  if (!r.ok) {
    let msg = 'HTTP ' + r.status;
    if (r.status === 401) msg = 'Откройте дашборд через Telegram (кнопка «Навигация»)';
    else if (r.status === 404) msg = 'API не найден. Проверь URL (должен быть ' + API_DOMAIN + '), бот и туннель.';
    else if (r.status === 429) msg = 'Слишком много запросов. Подождите минуту и обновите страницу.';
    const err = new Error(msg);
    err.status = r.status;
    throw err;
  }
  let data;
  try {
    data = await r.json();
  } catch (e) {
    const msg = (r.headers.get('content-type') || '').includes('text/html')
      ? 'API вернул HTML вместо JSON. Проверь: ' + API_DOMAIN + ' (не market), бот запущен, туннель cloudflared. Тест: curl https://' + API_DOMAIN + '/api/stats?period=1h'
      : (e.message || String(e));
    throw new Error(msg);
  }
  if (!Array.isArray(data)) data = [];
  return data;
}

async function fetchAndDraw() {
  const statusEl = document.getElementById('status');
  statusEl.className = 'status loading';
  statusEl.textContent = 'Загрузка...';

  let data = [];
  let lastError = null;
  for (const base of [API_BASE, API_FALLBACK].filter(Boolean)) {
    if (base === API_FALLBACK && API_FALLBACK === API_BASE) continue;
    try {
      data = await fetchFromUrl(base);
      if (base !== API_BASE) {
        API_BASE = base;
        document.getElementById('api-display').textContent = base;
      }
      break;
    } catch (e) {
      lastError = e;
      if (e.status === 401 || e.status === 404 || e.status === 429) break;
    }
  }
  if (data.length > 0) {
    statusEl.className = 'status success';
    statusEl.textContent = 'Данные загружены';
    drawChart(data);
    updateStats(data);
  } else if (lastError) {
    statusEl.className = 'status error';
    const suffix = lastError.status === 429 ? '' : '. Проверь, что бот запущен, туннель работает, и API доступен.';
    statusEl.textContent = 'Ошибка: ' + lastError.message + suffix;
    drawChart([]);
    document.getElementById('stats-row').style.display = 'none';
  } else {
    statusEl.className = 'status success';
    statusEl.textContent = 'Нет данных за период. Бот записывает запросы Jupiter/Bybit — подожди или выбери другой период.';
    drawChart([]);
    document.getElementById('stats-row').style.display = 'none';
  }
}

document.getElementById('btn-refresh').addEventListener('click', () => {
  fetchAndDraw();
  fetchSignalHistory();
});
document.getElementById('period-select').addEventListener('change', (e) => {
  currentPeriod = e.target.value;
  fetchAndDraw();
});

let historyPeriod = 'all';
const historyCard = document.getElementById('history-card');
const historyHeader = document.getElementById('history-header');
const historyList = document.getElementById('history-list');

historyHeader.addEventListener('click', () => {
  historyCard.classList.toggle('expanded');
  if (historyCard.classList.contains('expanded')) fetchSignalHistory();
});

document.getElementById('history-period').addEventListener('change', (e) => {
  historyPeriod = e.target.value;
  fetchSignalHistory();
});

document.getElementById('history-hide-stale').addEventListener('change', () => {
  if (lastHistoryData) renderHistory(lastHistoryData);
});

let lastHistoryData = [];

function formatSignalTime(ts) {
  const d = new Date(ts * 1000);
  const now = new Date();
  const diff = Math.floor((now - d) / 1000);
  if (diff < 60) return 'только что';
  if (diff < 3600) return Math.floor(diff / 60) + ' мин назад';
  if (diff < 86400) return Math.floor(diff / 3600) + ' ч назад';
  return d.getDate() + '.' + (d.getMonth() + 1) + ' ' + d.getHours() + ':' + String(d.getMinutes()).padStart(2, '0');
}

function formatDirection(dir) {
  if (dir === 'JUP->BYBIT') return 'Jupiter → Bybit';
  if (dir === 'BYBIT->JUP') return 'Bybit → Jupiter';
  return dir;
}

async function markSignalStale(id) {
  try {
    const r = await fetch(API_BASE.replace(/\/$/, '') + '/api/signal-history', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify({ id, status: 'stale' })
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    if (data.ok) fetchSignalHistory();
  } catch (e) {
    if (window.Telegram?.WebApp?.showAlert) window.Telegram.WebApp.showAlert('Ошибка: ' + (e.message || e));
  }
}

async function deleteSignal(id) {
  if (!confirm('Удалить сигнал из истории?')) return;
  try {
    const r = await fetch(API_BASE.replace(/\/$/, '') + '/api/signal-history?id=' + id, {
      method: 'DELETE',
      headers: getAuthHeaders()
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    if (data.ok) fetchSignalHistory();
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
    const r = await fetch(API_BASE.replace(/\/$/, '') + '/api/signal-history?period=' + historyPeriod, { headers: getAuthHeaders() });
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

function escapeHtml(s) {
  if (s == null || s === undefined) return '';
  const div = document.createElement('div');
  div.textContent = String(s);
  return div.innerHTML;
}

const canvas = document.getElementById('chart');
function resize() {
  const rect = canvas.getBoundingClientRect();
  if (rect.width && rect.height) {
    canvas.width = rect.width;
    canvas.height = rect.height;
  }
  fetchAndDraw();
}
window.addEventListener('resize', resize);
resize();

if (window.Telegram && window.Telegram.WebApp) {
  window.Telegram.WebApp.ready();
  window.Telegram.WebApp.expand();
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible' && document.getElementById('tab-settings')?.classList.contains('active')) {
    fetchStatusAndSettings();
  }
});

document.querySelectorAll('.nav-item').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    btn.classList.add('active');
    if (tab === 'settings') {
      fetchStatusAndSettings();
      fetch(API_BASE + '/api/auto_tune', { headers: getAuthHeaders() })
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (d && autoTuneToggle) { autoTuneToggle.classList.toggle('on', !!d.enabled); autoTuneToggle.classList.toggle('off', !d.enabled); } });
    }
    if (tab === 'console') {
      fetchConsoleLogs();
      startConsoleAutoRefresh();
    } else {
      stopConsoleAutoRefresh();
    }
  });
});

let consoleRefreshInterval = null;
function startConsoleAutoRefresh() {
  stopConsoleAutoRefresh();
  const cb = document.getElementById('console-auto-refresh');
  const sel = document.getElementById('console-refresh-interval');
  if (cb?.checked && sel) {
    const ms = parseInt(sel.value, 10) * 1000;
    consoleRefreshInterval = setInterval(fetchConsoleLogs, ms);
  }
}
function stopConsoleAutoRefresh() {
  if (consoleRefreshInterval) {
    clearInterval(consoleRefreshInterval);
    consoleRefreshInterval = null;
  }
}
document.getElementById('console-auto-refresh')?.addEventListener('change', function() {
  if (document.getElementById('tab-console')?.classList.contains('active')) {
    if (this.checked) startConsoleAutoRefresh();
    else stopConsoleAutoRefresh();
  }
});
document.getElementById('console-refresh-interval')?.addEventListener('change', function() {
  if (document.getElementById('tab-console')?.classList.contains('active') && document.getElementById('console-auto-refresh')?.checked) {
    startConsoleAutoRefresh();
  }
});

const apiDot = document.getElementById('status-api-dot');
const apiVal = document.getElementById('status-api-val');
const exchangeDot = document.getElementById('status-exchange-dot');
const exchangeVal = document.getElementById('status-exchange-val');
const exchangeToggle = document.getElementById('exchange-toggle');
const autoTuneToggle = document.getElementById('auto-tune-toggle');
const settingsList = document.getElementById('settings-list');

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
      fetch(API_BASE + '/api/status', { headers: getAuthHeaders() }),
      fetch(API_BASE + '/api/settings', { headers: getAuthHeaders() })
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
    apiVal.textContent = 'Офлайн: ' + e.message;
    setStatusDot(exchangeDot, null);
    exchangeVal.textContent = '—';
    exchangeToggle.classList.add('off');
    exchangeToggle.classList.remove('on');
    autoTuneToggle?.classList.add('off');
    autoTuneToggle?.classList.remove('on');
    settingsList.innerHTML = '<div class="history-empty">Не удалось загрузить настройки</div>';
  }
}

document.getElementById('btn-settings-refresh').addEventListener('click', fetchStatusAndSettings);

autoTuneToggle.addEventListener('click', async () => {
  const next = !autoTuneToggle.classList.contains('on');
  autoTuneToggle.classList.toggle('on', next);
  autoTuneToggle.classList.toggle('off', !next);
  try {
    const r = await fetch(API_BASE + '/api/auto_tune', {
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

async function fetchConsoleLogs() {
  const el = document.getElementById('console-output');
  el.textContent = 'Загрузка...';
  try {
    const r = await fetch(API_BASE.replace(/\/$/, '') + '/api/logs?limit=100', { headers: getAuthHeaders() });
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

document.getElementById('btn-console-refresh').addEventListener('click', fetchConsoleLogs);

const SETTING_TOOLTIPS = {
  bybit_taker_fee_bps: 'Комиссия Bybit в базисных пунктах (1 bps = 0.01%)',
  solana_tx_fee_usd: 'Примерная комиссия сети Solana в $',
  latency_buffer_bps: 'Запас на задержку исполнения (bps)',
  usdt_usdc_buffer_bps: 'Буфер на разницу USDT/USDC (bps)',
  min_profit_usd: 'Минимальная чистая прибыль в $ для отправки сигнала',
  notional_usd: 'Объём сделки в USDC (сколько тратим на арбитраж)',
  max_cex_slippage_bps: 'Макс. допустимый слип на CEX (Bybit)',
  max_dex_price_impact_pct: 'Макс. импакт на DEX (Jupiter) в %',
  persistence_hits: 'Сколько раз подряд должен быть профит перед отправкой',
  cooldown_sec: 'Пауза между повторными сигналами по одной паре (сек)',
  min_delta_profit_usd_to_resend: 'На сколько $ должен вырасти профит для ресэнда',
  price_ratio_max: 'Макс. отношение цен Jupiter/Bybit (защита от аномалий)',
  gross_profit_cap_pct: 'Макс. gross profit в % от объёма',
  max_spread_bps: 'Макс. спред стакана (выше = пропускаем пару)',
  min_depth_coverage_pct: 'Мин. % покрытия объёма глубиной стакана',
  engine_tick_hz: 'Как часто проверять арбитраж (раз в секунду)',
  jupiter_poll_interval_sec: 'Интервал опроса котировок Jupiter',
  max_ob_age_ms: 'Макс. возраст стакана в мс (старше = пропускаем)',
  stale_ttl_sec: 'Через сколько сек сигнал считается устаревшим (0 = выключено)',
  delete_stale: 'true = удалять сообщения, false = редактировать на «устарел»'
};

const SETTINGS_HIDDEN_KEYS = new Set(['exchange_enabled', 'auto_tune_enabled', 'auto_tune_bounds']);

function renderSettingsList(s, labels) {
  settingsList.innerHTML = Object.entries(s)
    .filter(([k]) => !SETTINGS_HIDDEN_KEYS.has(k))
    .map(([k, v]) => {
      const label = escapeHtml(labels[k] || k);
      const tooltip = SETTING_TOOLTIPS[k];
      const helpIcon = tooltip ? '<span class="settings-help" data-tooltip="' + escapeHtml(tooltip) + '" title="' + escapeHtml(tooltip) + '">?</span>' : '';
      const isBool = typeof v === 'boolean';
      if (isBool) {
        const on = v ? 'on' : 'off';
        return '<div class="settings-item" data-key="' + escapeHtml(k) + '"><span class="settings-item-key">' + label + helpIcon + '</span><div class="settings-toggle ' + on + '" data-key="' + escapeHtml(k) + '" role="button" tabindex="0"></div></div>';
      }
      const step = Number.isInteger(v) ? '1' : '0.01';
      return '<div class="settings-item" data-key="' + escapeHtml(k) + '"><span class="settings-item-key">' + label + helpIcon + '</span><input type="number" class="settings-input" data-key="' + escapeHtml(k) + '" value="' + escapeHtml(String(v)) + '" step="' + step + '"></div>';
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

async function updateSetting(key, value) {
  try {
    const r = await fetch(API_BASE + '/api/settings', {
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

exchangeToggle.addEventListener('click', async () => {
  const next = !exchangeToggle.classList.contains('on');
  try {
    const r = await fetch(API_BASE + '/api/exchange', {
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
})();
