/**
 * График запросов Jupiter/Bybit: DPR, сетка, оси, градиенты, hover с линией и точками, тултип.
 */
(function() {
  const getApiBase = () => window.App.getApiBase();
  const setApiBase = (b) => window.App.setApiBase(b);
  const getAuthHeaders = () => window.App.getAuthHeaders();
  const getApiFallback = () => window.App.getApiFallback();
  const getApiDomain = () => window.App.getApiDomain();
  const formatTime = (ts, period) => window.App.formatTime(ts, period);

  let currentPeriod = '1h';
  const canvas = document.getElementById('chart');
  const ctx = canvas.getContext('2d');
  let chartState = { data: [], padding: null, chartW: 0, chartH: 0, yMax: 1, w: 0, h: 0 };

  // ---------- DPR и чёткость (в т.ч. Telegram) ----------
  function setupCanvasDPR(canvasEl, cssW, cssH) {
    const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    canvasEl.style.width = cssW + 'px';
    canvasEl.style.height = cssH + 'px';
    canvasEl.width = Math.round(cssW * dpr);
    canvasEl.height = Math.round(cssH * dpr);
    const c = canvasEl.getContext('2d');
    c.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx: c, dpr };
  }

  function formatTick(v) {
    if (v >= 1000) return (v / 1000).toFixed(v % 1000 === 0 ? 0 : 1) + 'K';
    return String(Math.round(v));
  }

  function niceMax(max) {
    const steps = [1, 2, 2.5, 5, 10];
    const p = Math.pow(10, Math.floor(Math.log10(Math.max(1, max))));
    const n = max / p;
    let m = steps[steps.length - 1];
    for (const s of steps) { if (n <= s) { m = s; break; } }
    return m * p;
  }

  function buildTicks(max, count = 5) {
    const m = niceMax(max);
    const step = m / count;
    const ticks = [];
    for (let i = 0; i <= count; i++) ticks.push(i * step);
    return { yMax: m, ticks };
  }

  function makeGradient(ctx, x0, y0, x1, y1, stops) {
    const g = ctx.createLinearGradient(x0, y0, x1, y1);
    for (const [p, c] of stops) g.addColorStop(p, c);
    return g;
  }

  // Catmull-Rom → Bezier (плавная кривая как в дашбордах)
  function drawSmoothCatmullRom(ctx, pts, tension) {
    if (pts.length < 2) return;
    tension = tension ?? 0.35;
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

  function computePoints(series, padding, chartW, chartH, yMax) {
    const n = Math.max(1, series.length);
    return series.map((v, i) => ({
      x: padding.left + (i / Math.max(1, n - 1)) * chartW,
      y: padding.top + chartH - (v / yMax) * chartH,
      v
    }));
  }

  function drawGrid(ctx, padding, chartW, chartH, ticks) {
    ctx.save();
    ctx.translate(0.5, 0.5);
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = 1;
    const yMax = ticks[ticks.length - 1];
    for (const t of ticks) {
      const y = padding.top + chartH - (t / yMax) * chartH;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(padding.left + chartW, y);
      ctx.stroke();
    }
    ctx.strokeStyle = 'rgba(255,255,255,0.08)';
    ctx.beginPath();
    ctx.rect(padding.left, padding.top, chartW, chartH);
    ctx.stroke();
    ctx.restore();
  }

  function drawYAxisLabels(ctx, padding, chartW, chartH, ticks) {
    ctx.save();
    ctx.fillStyle = 'rgba(255,255,255,0.55)';
    ctx.font = '11px Inter, system-ui, -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    const yMax = ticks[ticks.length - 1];
    for (const t of ticks) {
      const y = padding.top + chartH - (t / yMax) * chartH;
      ctx.fillText(formatTick(t), padding.left - 10, y);
    }
    ctx.restore();
  }

  function drawXAxisLabels(ctx, data, padding, chartW, h, period, formatTimeFn) {
    ctx.save();
    ctx.fillStyle = 'rgba(255,255,255,0.45)';
    ctx.font = '11px Inter, system-ui, -apple-system, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'alphabetic';
    const step = Math.max(1, Math.floor(data.length / 5));
    for (let i = 0; i < data.length; i += step) {
      const x = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      ctx.fillText(formatTimeFn(data[i].ts, period), x, h - 10);
    }
    ctx.restore();
  }

  function drawSeries(ctx, points, padding, chartW, chartH, color, areaStops) {
    if (points.length < 2) return;
    ctx.save();
    ctx.beginPath();
    drawSmoothCatmullRom(ctx, points, 0.4);
    ctx.lineTo(padding.left + chartW, padding.top + chartH);
    ctx.lineTo(padding.left, padding.top + chartH);
    ctx.closePath();
    ctx.fillStyle = makeGradient(ctx, 0, padding.top, 0, padding.top + chartH, areaStops);
    ctx.fill();
    ctx.beginPath();
    drawSmoothCatmullRom(ctx, points, 0.4);
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.25;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.shadowColor = color;
    ctx.shadowBlur = 10;
    ctx.globalAlpha = 0.9;
    ctx.stroke();
    ctx.restore();
  }

  function drawHover(ctx, hoverX, p1, p2, padding, chartH) {
    if (hoverX == null) return;
    ctx.save();
    ctx.strokeStyle = 'rgba(255,255,255,0.14)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(hoverX + 0.5, padding.top);
    ctx.lineTo(hoverX + 0.5, padding.top + chartH);
    ctx.stroke();
    const drawPoint = (p, color) => {
      if (!p) return;
      ctx.beginPath();
      ctx.arc(p.x, p.y, 4.5, 0, Math.PI * 2);
      ctx.fillStyle = '#0b0b0e';
      ctx.fill();
      ctx.lineWidth = 2;
      ctx.strokeStyle = color;
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(p.x, p.y, 2, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    };
    drawPoint(p1, '#2F80FF');
    drawPoint(p2, '#00D18F');
    ctx.restore();
  }

  function drawChart(data, hoverIndex) {
    const cssW = canvas.getBoundingClientRect().width || 300;
    const cssH = canvas.getBoundingClientRect().height || 220;
    const out = setupCanvasDPR(canvas, cssW, cssH);
    const c = out.ctx;
    const w = cssW;
    const h = cssH;
    c.clearRect(0, 0, w, h);
    chartState.data = [];
    chartState.padding = null;

    if (!data || data.length === 0) {
      c.fillStyle = 'rgba(255,255,255,0.55)';
      c.font = '14px Inter, sans-serif';
      c.textAlign = 'center';
      c.fillText('Нет данных', w / 2, h / 2);
      return;
    }

    const padding = { top: 18, right: 18, bottom: 30, left: 46 };
    const chartW = w - padding.left - padding.right;
    const chartH = h - padding.top - padding.bottom;
    const jArr = data.map(d => d.jupiter || 0);
    const bArr = data.map(d => d.bybit || 0);
    const rawMax = Math.max(1, ...jArr, ...bArr);
    const { yMax, ticks } = buildTicks(rawMax, 5);
    chartState = { data, padding, chartW, chartH, yMax, w, h };

    c.save();
    c.fillStyle = makeGradient(c, 0, 0, 0, h, [
      [0, 'rgba(255,255,255,0.02)'],
      [1, 'rgba(0,0,0,0)']
    ]);
    c.fillRect(0, 0, w, h);
    c.restore();

    drawGrid(c, padding, chartW, chartH, ticks);
    drawYAxisLabels(c, padding, chartW, chartH, ticks);
    const jPoints = computePoints(jArr, padding, chartW, chartH, yMax);
    const bPoints = computePoints(bArr, padding, chartW, chartH, yMax);

    drawSeries(c, jPoints, padding, chartW, chartH, '#2F80FF', [
      [0.0, 'rgba(47,128,255,0.22)'],
      [0.65, 'rgba(47,128,255,0.06)'],
      [1.0, 'rgba(47,128,255,0)']
    ]);
    drawSeries(c, bPoints, padding, chartW, chartH, '#00D18F', [
      [0.0, 'rgba(0,209,143,0.18)'],
      [0.65, 'rgba(0,209,143,0.05)'],
      [1.0, 'rgba(0,209,143,0)']
    ]);

    drawXAxisLabels(c, data, padding, chartW, h, currentPeriod, formatTime);

    if (hoverIndex != null) {
      const i = Math.max(0, Math.min(hoverIndex, data.length - 1));
      const hoverX = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      drawHover(c, hoverX, jPoints[i], bPoints[i], padding, chartH);
    }
  }

  let hoverIndex = null;
  let rafHover = 0;

  function pickIndexByClientX(clientX) {
    const wrap = document.getElementById('chart-wrap');
    const { data, padding, chartW } = chartState;
    if (!data || !padding) return null;
    const rect = wrap.getBoundingClientRect();
    const x = clientX - rect.left;
    if (x < padding.left || x > padding.left + chartW) return null;
    const idx = Math.round(((x - padding.left) / chartW) * (data.length - 1));
    return Math.max(0, Math.min(idx, data.length - 1));
  }

  function showChartTooltip(e) {
    const tooltip = document.getElementById('chart-tooltip');
    const idx = pickIndexByClientX(e.clientX);

    if (idx == null || !chartState.data?.length) {
      hoverIndex = null;
      tooltip.classList.remove('visible');
      drawChart(chartState.data, null);
      return;
    }

    if (rafHover) cancelAnimationFrame(rafHover);
    rafHover = requestAnimationFrame(() => {
      hoverIndex = idx;
      drawChart(chartState.data, hoverIndex);

      const d = chartState.data[hoverIndex];
      const jup = d.jupiter || 0;
      const byb = d.bybit || 0;
      tooltip.innerHTML =
        '<div style="font-weight:700;margin-bottom:6px;color:rgba(255,255,255,0.75)">' +
          formatTime(d.ts, currentPeriod) +
        '</div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter: <b>' + jup + '</b></div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit: <b>' + byb + '</b></div>';

      const wrap = document.getElementById('chart-wrap');
      const rect = wrap.getBoundingClientRect();
      const { padding, chartW } = chartState;
      const hoverX = padding.left + (hoverIndex / Math.max(1, chartState.data.length - 1)) * chartW;

      tooltip.classList.add('visible');
      let left = rect.left + hoverX + 12;
      let top = e.clientY - 60;
      const tt = tooltip.getBoundingClientRect();
      const pad = 10;
      if (left + tt.width > window.innerWidth - pad) left = rect.left + hoverX - tt.width - 12;
      if (top < pad) top = rect.top + 10;
      if (top + tt.height > window.innerHeight - pad) top = window.innerHeight - tt.height - pad;
      tooltip.style.left = Math.max(pad, Math.min(left, window.innerWidth - tt.width - pad)) + 'px';
      tooltip.style.top = Math.max(pad, top) + 'px';
    });
  }

  function hideChartTooltip() {
    const tooltip = document.getElementById('chart-tooltip');
    tooltip.classList.remove('visible');
    hoverIndex = null;
    if (chartState.data?.length) drawChart(chartState.data, null);
  }

  const chartWrap = document.getElementById('chart-wrap');
  chartWrap.addEventListener('mousemove', showChartTooltip, { passive: true });
  chartWrap.addEventListener('mouseleave', hideChartTooltip, { passive: true });
  chartWrap.addEventListener('touchmove', (e) => {
    if (e.touches[0]) showChartTooltip({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY });
  }, { passive: true });
  chartWrap.addEventListener('touchend', hideChartTooltip, { passive: true });

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

  function resize() {
    fetchAndDraw();
  }
  window.addEventListener('resize', resize);

  window.App = window.App || {};
  window.App.chart = {
    fetchAndDraw,
    setCurrentPeriod: function(p) { currentPeriod = p; },
    getCurrentPeriod: function() { return currentPeriod; }
  };
})();
