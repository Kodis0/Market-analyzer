/**
 * График запросов Jupiter/Bybit: DPR, сетка, оси, градиенты, hover с линией и точками,
 * тултип, индикаторы min/max при наведении, плавная анимация при обновлении данных.
 * Совместимость с Telegram WebView (Desktop и Mobile).
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

  // ---------- Анимация при обновлении данных ----------
  const ANIM_DURATION_MS = 380;
  const EASE_OUT_CUBIC = t => 1 - Math.pow(1 - t, 3);

  let animStart = null;
  let prevJPoints = [];
  let prevBPoints = [];
  let prevYMax = 1;
  let nextData = null;
  let nextJPoints = [];
  let nextBPoints = [];
  let nextYMax = 1;
  let animRaf = 0;

  function lerpPoints(prev, next, t) {
    if (!next.length) return next;
    if (!prev.length) return next.slice();
    const n = next.length;
    const out = [];
    for (let i = 0; i < n; i++) {
      const ratio = n > 1 ? i / (n - 1) : 0;
      const pi = Math.min(Math.round(ratio * (prev.length - 1)), prev.length - 1);
      const p = prev[pi];
      const q = next[i];
      out.push({
        x: p.x + (q.x - p.x) * t,
        y: p.y + (q.y - p.y) * t,
        v: p.v + (q.v - p.v) * t
      });
    }
    return out;
  }

  function tickAnimation(now) {
    if (animStart == null) return;
    const elapsed = now - animStart;
    const t = Math.min(1, elapsed / ANIM_DURATION_MS);
    const eased = EASE_OUT_CUBIC(t);

    const jLerp = lerpPoints(prevJPoints, nextJPoints, eased);
    const bLerp = lerpPoints(prevBPoints, nextBPoints, eased);
    const yMaxLerp = prevYMax + (nextYMax - prevYMax) * eased;

    chartState.yMax = yMaxLerp;
    drawChartWithPoints(nextData, null, jLerp, bLerp, yMaxLerp);

    if (t < 1) {
      animRaf = requestAnimationFrame(tickAnimation);
    } else {
      animStart = null;
      chartState.data = nextData;
      chartState.yMax = nextYMax;
      if (hoverIndex != null) drawChart(nextData, hoverIndex);
    }
  }

  function startTransition(data, jPoints, bPoints, yMax) {
    if (animRaf) cancelAnimationFrame(animRaf);
    if (!chartState.data?.length || !data?.length) {
      prevJPoints = [];
      prevBPoints = [];
      prevYMax = yMax;
      nextData = data;
      nextJPoints = jPoints;
      nextBPoints = bPoints;
      nextYMax = yMax;
      chartState.data = data || [];
      chartState.yMax = yMax;
      drawChartWithPoints(data, hoverIndex, jPoints, bPoints, yMax);
      if (data?.length) {
        chartState._lastJPoints = jPoints;
        chartState._lastBPoints = bPoints;
      }
      return;
    }
    chartState.data = data;
    prevJPoints = chartState.data.length ? (chartState._lastJPoints || []) : [];
    prevBPoints = chartState.data.length ? (chartState._lastBPoints || []) : [];
    prevYMax = chartState.yMax;
    nextData = data;
    nextJPoints = jPoints;
    nextBPoints = bPoints;
    nextYMax = yMax;
    chartState.data = data;
    animStart = performance.now();
    animRaf = requestAnimationFrame(tickAnimation);
  }

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
    ctx.strokeStyle = 'rgba(255,255,255,0.07)';
    ctx.lineWidth = 1;
    const yMax = ticks[ticks.length - 1];
    for (const t of ticks) {
      const y = padding.top + chartH - (t / yMax) * chartH;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(padding.left + chartW, y);
      ctx.stroke();
    }
    ctx.strokeStyle = 'rgba(255,255,255,0.1)';
    ctx.beginPath();
    ctx.rect(padding.left, padding.top, chartW, chartH);
    ctx.stroke();
    ctx.restore();
  }

  function drawYAxisLabels(ctx, padding, chartW, chartH, ticks) {
    ctx.save();
    ctx.fillStyle = 'rgba(255,255,255,0.5)';
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
    ctx.fillStyle = 'rgba(255,255,255,0.42)';
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
    ctx.lineWidth = 2.5;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.shadowColor = color;
    ctx.shadowBlur = 12;
    ctx.globalAlpha = 0.92;
    ctx.stroke();
    ctx.restore();
  }

  function findMinMaxIndices(arr) {
    if (!arr.length) return { minIdx: 0, maxIdx: 0 };
    let minIdx = 0, maxIdx = 0;
    for (let i = 1; i < arr.length; i++) {
      if (arr[i] < arr[minIdx]) minIdx = i;
      if (arr[i] > arr[maxIdx]) maxIdx = i;
    }
    return { minIdx, maxIdx };
  }

  function drawHover(ctx, hoverX, p1, p2, padding, chartH, data, jPoints, bPoints) {
    if (hoverX == null || !data?.length) return;
    const jArr = data.map(d => d.jupiter || 0);
    const bArr = data.map(d => d.bybit || 0);
    const jMinMax = findMinMaxIndices(jArr);
    const bMinMax = findMinMaxIndices(bArr);

    ctx.save();

    // Вертикальная линия под курсором
    ctx.strokeStyle = 'rgba(255,255,255,0.2)';
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(hoverX + 0.5, padding.top);
    ctx.lineTo(hoverX + 0.5, padding.top + chartH);
    ctx.stroke();
    ctx.setLineDash([]);

    const drawPoint = (p, color, size = 5) => {
      if (!p) return;
      ctx.beginPath();
      ctx.arc(p.x, p.y, size + 2, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(11,11,14,0.9)';
      ctx.fill();
      ctx.lineWidth = 2;
      ctx.strokeStyle = color;
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(p.x, p.y, size * 0.5, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    };

    const drawMinMaxMarker = (point, color, label) => {
      if (!point) return;
      ctx.beginPath();
      ctx.arc(point.x, point.y, 6, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(11,11,14,0.95)';
      ctx.fill();
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.stroke();
      ctx.fillStyle = color;
      ctx.font = '9px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillText(label, point.x, point.y + 8);
    };

    // Точки под курсором (Jupiter, Bybit)
    drawPoint(p1, '#2F80FF');
    drawPoint(p2, '#00D18F');

    // Маркеры min/max для Jupiter (если не под курсором)
    if (jPoints[jMinMax.minIdx] && jMinMax.minIdx !== hoverIndex) {
      drawMinMaxMarker(jPoints[jMinMax.minIdx], '#2F80FF', 'min');
    }
    if (jPoints[jMinMax.maxIdx] && jMinMax.maxIdx !== hoverIndex) {
      drawMinMaxMarker(jPoints[jMinMax.maxIdx], '#2F80FF', 'max');
    }
    if (bPoints[bMinMax.minIdx] && bMinMax.minIdx !== hoverIndex) {
      drawMinMaxMarker(bPoints[bMinMax.minIdx], '#00D18F', 'min');
    }
    if (bPoints[bMinMax.maxIdx] && bMinMax.maxIdx !== hoverIndex) {
      drawMinMaxMarker(bPoints[bMinMax.maxIdx], '#00D18F', 'max');
    }

    ctx.restore();
  }

  function drawChartWithPoints(data, hoverIndexVal, jPoints, bPoints, yMaxVal) {
    const wrap = document.getElementById('chart-wrap');
    const rect = wrap ? wrap.getBoundingClientRect() : { width: 300, height: 220 };
    let cssW = (rect && rect.width) || 300;
    let cssH = (rect && rect.height) || 220;
    if (cssW <= 0 || cssH <= 0) { cssW = 300; cssH = 220; }

    const out = setupCanvasDPR(canvas, cssW, cssH);
    const c = out.ctx;
    const w = cssW;
    const h = cssH;
    c.clearRect(0, 0, w, h);

    if (!data || data.length === 0) {
      c.fillStyle = 'rgba(255,255,255,0.5)';
      c.font = '14px Inter, sans-serif';
      c.textAlign = 'center';
      c.fillText('Нет данных', w / 2, h / 2);
      chartState.data = [];
      chartState.padding = null;
      return;
    }

    const padding = { top: 20, right: 20, bottom: 32, left: 48 };
    const chartW = w - padding.left - padding.right;
    const chartH = h - padding.top - padding.bottom;
    const { yMax, ticks } = buildTicks(yMaxVal, 5);
    chartState.padding = padding;
    chartState.chartW = chartW;
    chartState.chartH = chartH;
    chartState.yMax = yMax;
    chartState.w = w;
    chartState.h = h;
    chartState._lastJPoints = jPoints;
    chartState._lastBPoints = bPoints;

    c.save();
    c.fillStyle = makeGradient(c, 0, 0, 0, h, [
      [0, 'rgba(255,255,255,0.025)'],
      [1, 'rgba(0,0,0,0)']
    ]);
    c.fillRect(0, 0, w, h);
    c.restore();

    drawGrid(c, padding, chartW, chartH, ticks);
    drawYAxisLabels(c, padding, chartW, chartH, ticks);
    drawSeries(c, jPoints, padding, chartW, chartH, '#2F80FF', [
      [0.0, 'rgba(47,128,255,0.25)'],
      [0.6, 'rgba(47,128,255,0.06)'],
      [1.0, 'rgba(47,128,255,0)']
    ]);
    drawSeries(c, bPoints, padding, chartW, chartH, '#00D18F', [
      [0.0, 'rgba(0,209,143,0.2)'],
      [0.6, 'rgba(0,209,143,0.05)'],
      [1.0, 'rgba(0,209,143,0)']
    ]);
    drawXAxisLabels(c, data, padding, chartW, h, currentPeriod, formatTime);
    if (hoverIndexVal != null) {
      const i = Math.max(0, Math.min(hoverIndexVal, data.length - 1));
      const hoverX = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      drawHover(c, hoverX, jPoints[i], bPoints[i], padding, chartH, data, jPoints, bPoints);
    }
  }

  function drawChart(data, hoverIndexVal) {
    const wrap = document.getElementById('chart-wrap');
    const rect = wrap ? wrap.getBoundingClientRect() : { width: 300, height: 220 };
    let cssW = (rect && rect.width) || 300;
    let cssH = (rect && rect.height) || 220;
    if (cssW <= 0 || cssH <= 0) { cssW = 300; cssH = 220; }

    const out = setupCanvasDPR(canvas, cssW, cssH);
    const c = out.ctx;
    const w = cssW;
    const h = cssH;
    c.clearRect(0, 0, w, h);
    chartState.data = [];
    chartState.padding = null;

    if (!data || data.length === 0) {
      c.fillStyle = 'rgba(255,255,255,0.5)';
      c.font = '14px Inter, sans-serif';
      c.textAlign = 'center';
      c.fillText('Нет данных', w / 2, h / 2);
      return;
    }

    const padding = { top: 20, right: 20, bottom: 32, left: 48 };
    const chartW = w - padding.left - padding.right;
    const chartH = h - padding.top - padding.bottom;
    const jArr = data.map(d => d.jupiter || 0);
    const bArr = data.map(d => d.bybit || 0);
    const rawMax = Math.max(1, ...jArr, ...bArr);
    const { yMax, ticks } = buildTicks(rawMax, 5);
    chartState = { data, padding, chartW, chartH, yMax, w, h };

    const jPoints = computePoints(jArr, padding, chartW, chartH, yMax);
    const bPoints = computePoints(bArr, padding, chartW, chartH, yMax);
    chartState._lastJPoints = jPoints;
    chartState._lastBPoints = bPoints;

    c.save();
    c.fillStyle = makeGradient(c, 0, 0, 0, h, [
      [0, 'rgba(255,255,255,0.025)'],
      [1, 'rgba(0,0,0,0)']
    ]);
    c.fillRect(0, 0, w, h);
    c.restore();

    drawGrid(c, padding, chartW, chartH, ticks);
    drawYAxisLabels(c, padding, chartW, chartH, ticks);
    drawSeries(c, jPoints, padding, chartW, chartH, '#2F80FF', [
      [0.0, 'rgba(47,128,255,0.25)'],
      [0.6, 'rgba(47,128,255,0.06)'],
      [1.0, 'rgba(47,128,255,0)']
    ]);
    drawSeries(c, bPoints, padding, chartW, chartH, '#00D18F', [
      [0.0, 'rgba(0,209,143,0.2)'],
      [0.6, 'rgba(0,209,143,0.05)'],
      [1.0, 'rgba(0,209,143,0)']
    ]);
    drawXAxisLabels(c, data, padding, chartW, h, currentPeriod, formatTime);
    if (hoverIndexVal != null) {
      const i = Math.max(0, Math.min(hoverIndexVal, data.length - 1));
      const hoverX = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      drawHover(c, hoverX, jPoints[i], bPoints[i], padding, chartH, data, jPoints, bPoints);
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

  function getMinMaxForTooltip(data) {
    if (!data?.length) return null;
    const jArr = data.map(d => d.jupiter || 0);
    const bArr = data.map(d => d.bybit || 0);
    const jMin = Math.min(...jArr);
    const jMax = Math.max(...jArr);
    const bMin = Math.min(...bArr);
    const bMax = Math.max(...bArr);
    const jMinIdx = jArr.indexOf(jMin);
    const jMaxIdx = jArr.indexOf(jMax);
    const bMinIdx = bArr.indexOf(bMin);
    const bMaxIdx = bArr.indexOf(bMax);
    return {
      jMin, jMax, jMinIdx, jMaxIdx,
      bMin, bMax, bMinIdx, bMaxIdx
    };
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
      const jPoints = chartState._lastJPoints || [];
      const bPoints = chartState._lastBPoints || [];
      drawChartWithPoints(chartState.data, hoverIndex, jPoints, bPoints, chartState.yMax);

      const d = chartState.data[hoverIndex];
      const jup = d.jupiter || 0;
      const byb = d.bybit || 0;
      const mm = getMinMaxForTooltip(chartState.data);
      let extra = '';
      if (mm) {
        extra =
          '<div class="chart-tooltip-minmax">' +
          '<span class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter min: <b>' + mm.jMin + '</b> · max: <b>' + mm.jMax + '</b></span>' +
          '<span class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit min: <b>' + mm.bMin + '</b> · max: <b>' + mm.bMax + '</b></span>' +
          '</div>';
      }
      tooltip.innerHTML =
        '<div class="chart-tooltip-time">' + formatTime(d.ts, currentPeriod) + '</div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter: <b>' + jup + '</b></div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit: <b>' + byb + '</b></div>' +
        extra;

      const wrap = document.getElementById('chart-wrap');
      const rect = wrap.getBoundingClientRect();
      const { padding, chartW } = chartState;
      const hoverX = padding.left + (hoverIndex / Math.max(1, chartState.data.length - 1)) * chartW;

      tooltip.classList.add('visible');
      let left = rect.left + hoverX + 14;
      let top = e.clientY - 70;
      const tt = tooltip.getBoundingClientRect();
      const pad = 12;
      if (left + tt.width > window.innerWidth - pad) left = rect.left + hoverX - tt.width - 14;
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
  if (chartWrap) {
    chartWrap.addEventListener('mousemove', showChartTooltip, { passive: true });
    chartWrap.addEventListener('mouseleave', hideChartTooltip, { passive: true });
    chartWrap.addEventListener('touchmove', (e) => {
      if (e.touches[0]) showChartTooltip({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY });
    }, { passive: true });
    chartWrap.addEventListener('touchend', hideChartTooltip, { passive: true });
  }

  // ---------- Telegram Desktop: размер контейнера может появиться с задержкой ----------
  function scheduleRedraw() {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (chartState.data?.length) drawChart(chartState.data, hoverIndex);
      });
    });
  }

  if (typeof ResizeObserver !== 'undefined' && chartWrap) {
    const ro = new ResizeObserver(() => scheduleRedraw());
    ro.observe(chartWrap);
  }
  window.addEventListener('resize', scheduleRedraw);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') scheduleRedraw();
  });
  if (window.Telegram?.WebApp) {
    window.Telegram.WebApp.onEvent('viewportChanged', scheduleRedraw);
  }

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
      const wrap = document.getElementById('chart-wrap');
      const rect = wrap ? wrap.getBoundingClientRect() : {};
      let cssW = (rect.width > 0 && rect.height > 0) ? rect.width : 300;
      let cssH = (rect.width > 0 && rect.height > 0) ? rect.height : 220;
      if (cssW <= 0) cssW = 300;
      if (cssH <= 0) cssH = 220;
      const padding = { top: 20, right: 20, bottom: 32, left: 48 };
      const chartW = cssW - padding.left - padding.right;
      const chartH = cssH - padding.top - padding.bottom;
      const jArr = data.map(d => d.jupiter || 0);
      const bArr = data.map(d => d.bybit || 0);
      const rawMax = Math.max(1, ...jArr, ...bArr);
      const { yMax } = buildTicks(rawMax, 5);
      const jPoints = computePoints(jArr, padding, chartW, chartH, yMax);
      const bPoints = computePoints(bArr, padding, chartW, chartH, yMax);
      startTransition(data, jPoints, bPoints, yMax);
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
    getCurrentPeriod: function() { return currentPeriod; },
    redraw: function() {
      if (chartState.data?.length) drawChart(chartState.data, hoverIndex);
    }
  };
})();
