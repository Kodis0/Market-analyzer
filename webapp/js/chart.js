/**
 * График запросов Jupiter/Bybit: отрисовка, тултип, статистика, загрузка данных.
 */
(function() {
  const getApiBase = () => window.App.getApiBase();
  const setApiBase = (b) => window.App.setApiBase(b);
  const getAuthHeaders = () => window.App.getAuthHeaders();
  const getApiFallback = () => window.App.getApiFallback();
  const getApiDomain = () => window.App.getApiDomain();
  const formatTime = (ts, period) => window.App.formatTime(ts, period);

  let currentPeriod = '1h';
  const ctx = document.getElementById('chart').getContext('2d');
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
      else if (r.status === 404) msg = 'API не найден. Проверь URL (должен быть ' + getApiDomain() + '), бот и туннель.';
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
        ? 'API вернул HTML вместо JSON. Проверь: ' + getApiDomain() + ' (не market), бот запущен, туннель cloudflared. Тест: curl https://' + getApiDomain() + '/api/stats?period=1h'
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
    const fallback = getApiFallback();
    for (const base of [getApiBase(), fallback].filter(Boolean)) {
      if (base === fallback && fallback === getApiBase()) continue;
      try {
        data = await fetchFromUrl(base);
        if (base !== getApiBase()) setApiBase(base);
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
      statusEl.textContent = 'Ошибка';
      drawChart([]);
      document.getElementById('stats-row').style.display = 'none';
    } else {
      statusEl.className = 'status success';
      statusEl.textContent = '';
      drawChart([]);
      document.getElementById('stats-row').style.display = 'none';
    }
  }

  window.App = window.App || {};
  window.App.chart = {
    fetchAndDraw,
    setCurrentPeriod: function(p) { currentPeriod = p; },
    getCurrentPeriod: function() { return currentPeriod; }
  };
})();
