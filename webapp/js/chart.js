/**
 * График запросов Jupiter/Bybit: DPR, сетка, оси, градиенты, hover с линией и точками,
 * тултип, индикаторы min/max при наведении, плавная анимация при обновлении данных.
 * Совместимость с Telegram WebView (Desktop и Mobile).
 * Рефакторинг: использует ApiClient, Constants; разделение ответственности.
 */
(function() {
  'use strict';

  var C = window.App.Constants.CHART;
  var M = window.App.Constants.MESSAGES;
  var EASE = window.App.Constants.EASING.easeOutCubic;
  var formatTime = function(ts, period) { return window.App.formatTime(ts, period); };

  var currentPeriod = '1h';
  var canvas = document.getElementById('chart');
  var ctx = canvas.getContext('2d');
  var chartState = { data: [], padding: null, chartW: 0, chartH: 0, yMax: 1, w: 0, h: 0 };

  var animStart = null;
  var prevJPoints = [];
  var prevBPoints = [];
  var prevYMax = 1;
  var nextData = null;
  var nextJPoints = [];
  var nextBPoints = [];
  var nextYMax = 1;
  var animRaf = 0;
  var hoverIndex = null;
  var rafHover = 0;

  function lerpPoints(prev, next, t) {
    if (!next.length) return next;
    if (!prev.length) return next.slice();
    var n = next.length;
    var out = [];
    for (var i = 0; i < n; i++) {
      var ratio = n > 1 ? i / (n - 1) : 0;
      var pi = Math.min(Math.round(ratio * (prev.length - 1)), prev.length - 1);
      var p = prev[pi];
      var q = next[i];
      out.push({ x: p.x + (q.x - p.x) * t, y: p.y + (q.y - p.y) * t, v: p.v + (q.v - p.v) * t });
    }
    return out;
  }

  function tickAnimation(now) {
    if (animStart == null) return;
    var elapsed = now - animStart;
    var t = Math.min(1, elapsed / C.ANIM_DURATION_MS);
    var eased = EASE(t);
    var jLerp = lerpPoints(prevJPoints, nextJPoints, eased);
    var bLerp = lerpPoints(prevBPoints, nextBPoints, eased);
    var yMaxLerp = prevYMax + (nextYMax - prevYMax) * eased;
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
    if (!chartState.data || !chartState.data.length || !data || !data.length) {
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
      if (data && data.length) {
        chartState._lastJPoints = jPoints;
        chartState._lastBPoints = bPoints;
      }
      return;
    }
    chartState.data = data;
    prevJPoints = chartState._lastJPoints || [];
    prevBPoints = chartState._lastBPoints || [];
    prevYMax = chartState.yMax;
    nextData = data;
    nextJPoints = jPoints;
    nextBPoints = bPoints;
    nextYMax = yMax;
    animStart = performance.now();
    animRaf = requestAnimationFrame(tickAnimation);
  }

  function setupCanvasDPR(canvasEl, cssW, cssH) {
    var dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    canvasEl.style.width = cssW + 'px';
    canvasEl.style.height = cssH + 'px';
    canvasEl.width = Math.round(cssW * dpr);
    canvasEl.height = Math.round(cssH * dpr);
    var c = canvasEl.getContext('2d');
    c.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx: c, dpr: dpr };
  }

  function formatTick(v) {
    if (v >= 1000) return (v / 1000).toFixed(v % 1000 === 0 ? 0 : 1) + 'K';
    return String(Math.round(v));
  }

  function niceMax(max) {
    var steps = [1, 2, 2.5, 5, 10];
    var p = Math.pow(10, Math.floor(Math.log10(Math.max(1, max))));
    var n = max / p;
    var m = steps[steps.length - 1];
    for (var i = 0; i < steps.length; i++) { if (n <= steps[i]) { m = steps[i]; break; } }
    return m * p;
  }

  function buildTicks(max, count) {
    count = count || C.TICK_COUNT;
    var m = niceMax(max);
    var step = m / count;
    var ticks = [];
    for (var i = 0; i <= count; i++) ticks.push(i * step);
    return { yMax: m, ticks: ticks };
  }

  function makeGradient(ctx, x0, y0, x1, y1, stops) {
    var g = ctx.createLinearGradient(x0, y0, x1, y1);
    for (var i = 0; i < stops.length; i++) g.addColorStop(stops[i][0], stops[i][1]);
    return g;
  }

  function drawSmoothCatmullRom(ctx, pts, tension) {
    if (pts.length < 2) return;
    tension = tension != null ? tension : C.SPLINE_TENSION;
    ctx.moveTo(pts[0].x, pts[0].y);
    for (var i = 0; i < pts.length - 1; i++) {
      var p0 = pts[Math.max(0, i - 1)];
      var p1 = pts[i];
      var p2 = pts[i + 1];
      var p3 = pts[Math.min(pts.length - 1, i + 2)];
      var cp1x = p1.x + (p2.x - p0.x) * tension / 6;
      var cp1y = p1.y + (p2.y - p0.y) * tension / 6;
      var cp2x = p2.x - (p3.x - p1.x) * tension / 6;
      var cp2y = p2.y - (p3.y - p1.y) * tension / 6;
      ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
    }
  }

  function computePoints(series, padding, chartW, chartH, yMax) {
    var n = Math.max(1, series.length);
    return series.map(function(v, i) {
      return {
        x: padding.left + (i / Math.max(1, n - 1)) * chartW,
        y: padding.top + chartH - (v / yMax) * chartH,
        v: v
      };
    });
  }

  function drawGrid(ctx, padding, chartW, chartH, ticks) {
    ctx.save();
    ctx.translate(0.5, 0.5);
    ctx.strokeStyle = C.COLORS.GRID;
    ctx.lineWidth = 1;
    var yMax = ticks[ticks.length - 1];
    for (var i = 0; i < ticks.length; i++) {
      var y = padding.top + chartH - (ticks[i] / yMax) * chartH;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(padding.left + chartW, y);
      ctx.stroke();
    }
    ctx.strokeStyle = C.COLORS.BORDER;
    ctx.beginPath();
    ctx.rect(padding.left, padding.top, chartW, chartH);
    ctx.stroke();
    ctx.restore();
  }

  function drawYAxisLabels(ctx, padding, chartW, chartH, ticks) {
    ctx.save();
    ctx.fillStyle = C.COLORS.Y_LABELS;
    ctx.font = '11px Inter, system-ui, -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    var yMax = ticks[ticks.length - 1];
    for (var i = 0; i < ticks.length; i++) {
      var y = padding.top + chartH - (ticks[i] / yMax) * chartH;
      ctx.fillText(formatTick(ticks[i]), padding.left - 10, y);
    }
    ctx.restore();
  }

  function drawXAxisLabels(ctx, data, padding, chartW, h, period, formatTimeFn) {
    ctx.save();
    ctx.fillStyle = C.COLORS.X_LABELS;
    ctx.font = '11px Inter, system-ui, -apple-system, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'alphabetic';
    var step = Math.max(1, Math.floor(data.length / 5));
    for (var i = 0; i < data.length; i += step) {
      var x = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      ctx.fillText(formatTimeFn(data[i].ts, period), x, h - 10);
    }
    ctx.restore();
  }

  function drawSeries(ctx, points, padding, chartW, chartH, color, areaStops) {
    if (points.length < 2) return;
    ctx.save();
    ctx.beginPath();
    drawSmoothCatmullRom(ctx, points, C.SERIES_TENSION);
    ctx.lineTo(padding.left + chartW, padding.top + chartH);
    ctx.lineTo(padding.left, padding.top + chartH);
    ctx.closePath();
    ctx.fillStyle = makeGradient(ctx, 0, padding.top, 0, padding.top + chartH, areaStops);
    ctx.fill();
    ctx.beginPath();
    drawSmoothCatmullRom(ctx, points, C.SERIES_TENSION);
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
    var minIdx = 0, maxIdx = 0;
    for (var i = 1; i < arr.length; i++) {
      if (arr[i] < arr[minIdx]) minIdx = i;
      if (arr[i] > arr[maxIdx]) maxIdx = i;
    }
    return { minIdx: minIdx, maxIdx: maxIdx };
  }

  function drawHover(ctx, hoverX, p1, p2, padding, chartH, data, jPoints, bPoints) {
    if (hoverX == null || !data || !data.length) return;
    var jArr = data.map(function(d) { return d.jupiter || 0; });
    var bArr = data.map(function(d) { return d.bybit || 0; });
    var jMinMax = findMinMaxIndices(jArr);
    var bMinMax = findMinMaxIndices(bArr);
    ctx.save();
    ctx.strokeStyle = C.COLORS.HOVER_LINE;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(hoverX + 0.5, padding.top);
    ctx.lineTo(hoverX + 0.5, padding.top + chartH);
    ctx.stroke();
    ctx.setLineDash([]);
    var drawPoint = function(p, color, size) {
      size = size || 5;
      if (!p) return;
      ctx.beginPath();
      ctx.arc(p.x, p.y, size + 2, 0, Math.PI * 2);
      ctx.fillStyle = C.COLORS.POINT_BG;
      ctx.fill();
      ctx.lineWidth = 2;
      ctx.strokeStyle = color;
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(p.x, p.y, size * 0.5, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    };
    var drawMinMaxMarker = function(point, color, label) {
      if (!point) return;
      ctx.beginPath();
      ctx.arc(point.x, point.y, 6, 0, Math.PI * 2);
      ctx.fillStyle = C.COLORS.MARKER_BG;
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
    drawPoint(p1, C.COLORS.JUPITER);
    drawPoint(p2, C.COLORS.BYBIT);
    if (jPoints[jMinMax.minIdx] && jMinMax.minIdx !== hoverIndex) drawMinMaxMarker(jPoints[jMinMax.minIdx], C.COLORS.JUPITER, 'min');
    if (jPoints[jMinMax.maxIdx] && jMinMax.maxIdx !== hoverIndex) drawMinMaxMarker(jPoints[jMinMax.maxIdx], C.COLORS.JUPITER, 'max');
    if (bPoints[bMinMax.minIdx] && bMinMax.minIdx !== hoverIndex) drawMinMaxMarker(bPoints[bMinMax.minIdx], C.COLORS.BYBIT, 'min');
    if (bPoints[bMinMax.maxIdx] && bMinMax.maxIdx !== hoverIndex) drawMinMaxMarker(bPoints[bMinMax.maxIdx], C.COLORS.BYBIT, 'max');
    ctx.restore();
  }

  function getChartDimensions() {
    var wrap = document.getElementById('chart-wrap');
    var rect = wrap ? wrap.getBoundingClientRect() : { width: C.DEFAULT_WIDTH, height: C.DEFAULT_HEIGHT };
    var cssW = (rect && rect.width) || C.DEFAULT_WIDTH;
    var cssH = (rect && rect.height) || C.DEFAULT_HEIGHT;
    if (cssW <= 0 || cssH <= 0) { cssW = C.DEFAULT_WIDTH; cssH = C.DEFAULT_HEIGHT; }
    return { w: cssW, h: cssH };
  }

  function drawChartWithPoints(data, hoverIndexVal, jPoints, bPoints, yMaxVal) {
    var dims = getChartDimensions();
    var w = dims.w, h = dims.h;
    var out = setupCanvasDPR(canvas, w, h);
    var c = out.ctx;
    c.clearRect(0, 0, w, h);

    if (!data || data.length === 0) {
      c.fillStyle = 'rgba(255,255,255,0.5)';
      c.font = '14px Inter, sans-serif';
      c.textAlign = 'center';
      c.fillText(M.NO_DATA, w / 2, h / 2);
      chartState.data = [];
      chartState.padding = null;
      return;
    }

    var padding = { top: C.PADDING.top, right: C.PADDING.right, bottom: C.PADDING.bottom, left: C.PADDING.left };
    var chartW = w - padding.left - padding.right;
    var chartH = h - padding.top - padding.bottom;
    var ticksResult = buildTicks(yMaxVal, C.TICK_COUNT);
    var yMax = ticksResult.yMax;
    var ticks = ticksResult.ticks;
    chartState.padding = padding;
    chartState.chartW = chartW;
    chartState.chartH = chartH;
    chartState.yMax = yMax;
    chartState.w = w;
    chartState.h = h;
    chartState._lastJPoints = jPoints;
    chartState._lastBPoints = bPoints;

    c.save();
    c.fillStyle = makeGradient(c, 0, 0, 0, h, [[0, 'rgba(255,255,255,0.025)'], [1, 'rgba(0,0,0,0)']]);
    c.fillRect(0, 0, w, h);
    c.restore();

    drawGrid(c, padding, chartW, chartH, ticks);
    drawYAxisLabels(c, padding, chartW, chartH, ticks);
    drawSeries(c, jPoints, padding, chartW, chartH, C.COLORS.JUPITER, C.COLORS.JUPITER_GRADIENT);
    drawSeries(c, bPoints, padding, chartW, chartH, C.COLORS.BYBIT, C.COLORS.BYBIT_GRADIENT);
    drawXAxisLabels(c, data, padding, chartW, h, currentPeriod, formatTime);
    if (hoverIndexVal != null) {
      var i = Math.max(0, Math.min(hoverIndexVal, data.length - 1));
      var hoverX = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      drawHover(c, hoverX, jPoints[i], bPoints[i], padding, chartH, data, jPoints, bPoints);
    }
  }

  function drawChart(data, hoverIndexVal) {
    var dims = getChartDimensions();
    var w = dims.w, h = dims.h;
    var out = setupCanvasDPR(canvas, w, h);
    var c = out.ctx;
    c.clearRect(0, 0, w, h);
    chartState.data = [];
    chartState.padding = null;

    if (!data || data.length === 0) {
      c.fillStyle = 'rgba(255,255,255,0.5)';
      c.font = '14px Inter, sans-serif';
      c.textAlign = 'center';
      c.fillText(M.NO_DATA, w / 2, h / 2);
      return;
    }

    var padding = { top: C.PADDING.top, right: C.PADDING.right, bottom: C.PADDING.bottom, left: C.PADDING.left };
    var chartW = w - padding.left - padding.right;
    var chartH = h - padding.top - padding.bottom;
    var jArr = data.map(function(d) { return d.jupiter || 0; });
    var bArr = data.map(function(d) { return d.bybit || 0; });
    var rawMax = Math.max(1, ...jArr, ...bArr);
    var ticksResult = buildTicks(rawMax, C.TICK_COUNT);
    var yMax = ticksResult.yMax;
    var ticks = ticksResult.ticks;
    chartState = { data: data, padding: padding, chartW: chartW, chartH: chartH, yMax: yMax, w: w, h: h };

    var jPoints = computePoints(jArr, padding, chartW, chartH, yMax);
    var bPoints = computePoints(bArr, padding, chartW, chartH, yMax);
    chartState._lastJPoints = jPoints;
    chartState._lastBPoints = bPoints;

    c.save();
    c.fillStyle = makeGradient(c, 0, 0, 0, h, [[0, 'rgba(255,255,255,0.025)'], [1, 'rgba(0,0,0,0)']]);
    c.fillRect(0, 0, w, h);
    c.restore();

    drawGrid(c, padding, chartW, chartH, ticks);
    drawYAxisLabels(c, padding, chartW, chartH, ticks);
    drawSeries(c, jPoints, padding, chartW, chartH, C.COLORS.JUPITER, C.COLORS.JUPITER_GRADIENT);
    drawSeries(c, bPoints, padding, chartW, chartH, C.COLORS.BYBIT, C.COLORS.BYBIT_GRADIENT);
    drawXAxisLabels(c, data, padding, chartW, h, currentPeriod, formatTime);
    if (hoverIndexVal != null) {
      var i = Math.max(0, Math.min(hoverIndexVal, data.length - 1));
      var hoverX = padding.left + (i / Math.max(1, data.length - 1)) * chartW;
      drawHover(c, hoverX, jPoints[i], bPoints[i], padding, chartH, data, jPoints, bPoints);
    }
  }

  function pickIndexByClientX(clientX) {
    var wrap = document.getElementById('chart-wrap');
    var state = chartState;
    if (!state.data || !state.padding) return null;
    var rect = wrap.getBoundingClientRect();
    var x = clientX - rect.left;
    if (x < state.padding.left || x > state.padding.left + state.chartW) return null;
    var idx = Math.round(((x - state.padding.left) / state.chartW) * (state.data.length - 1));
    return Math.max(0, Math.min(idx, state.data.length - 1));
  }

  function getMinMaxForTooltip(data) {
    if (!data || !data.length) return null;
    var jArr = data.map(function(d) { return d.jupiter || 0; });
    var bArr = data.map(function(d) { return d.bybit || 0; });
    return {
      jMin: Math.min(...jArr),
      jMax: Math.max(...jArr),
      bMin: Math.min(...bArr),
      bMax: Math.max(...bArr)
    };
  }

  function showChartTooltip(e) {
    var tooltip = document.getElementById('chart-tooltip');
    var idx = pickIndexByClientX(e.clientX);

    if (idx == null || !chartState.data || !chartState.data.length) {
      hoverIndex = null;
      tooltip.classList.remove('visible');
      drawChart(chartState.data, null);
      return;
    }

    if (rafHover) cancelAnimationFrame(rafHover);
    rafHover = requestAnimationFrame(function() {
      hoverIndex = idx;
      var jPoints = chartState._lastJPoints || [];
      var bPoints = chartState._lastBPoints || [];
      drawChartWithPoints(chartState.data, hoverIndex, jPoints, bPoints, chartState.yMax);

      var d = chartState.data[hoverIndex];
      var jup = d.jupiter || 0;
      var byb = d.bybit || 0;
      var mm = getMinMaxForTooltip(chartState.data);
      var extra = '';
      if (mm) {
        extra = '<div class="chart-tooltip-minmax">' +
          '<span class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter min: <b>' + mm.jMin + '</b> · max: <b>' + mm.jMax + '</b></span>' +
          '<span class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit min: <b>' + mm.bMin + '</b> · max: <b>' + mm.bMax + '</b></span>' +
          '</div>';
      }
      tooltip.innerHTML =
        '<div class="chart-tooltip-time">' + formatTime(d.ts, currentPeriod) + '</div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot jupiter"></span> Jupiter: <b>' + jup + '</b></div>' +
        '<div class="chart-tooltip-row"><span class="chart-tooltip-dot bybit"></span> Bybit: <b>' + byb + '</b></div>' +
        extra;

      var wrap = document.getElementById('chart-wrap');
      var rect = wrap.getBoundingClientRect();
      var padding = chartState.padding;
      var chartW = chartState.chartW;
      var hoverX = padding.left + (hoverIndex / Math.max(1, chartState.data.length - 1)) * chartW;

      tooltip.classList.add('visible');
      var left = rect.left + hoverX + 14;
      var top = e.clientY - 70;
      var tt = tooltip.getBoundingClientRect();
      var pad = 12;
      if (left + tt.width > window.innerWidth - pad) left = rect.left + hoverX - tt.width - 14;
      if (top < pad) top = rect.top + 10;
      if (top + tt.height > window.innerHeight - pad) top = window.innerHeight - tt.height - pad;
      tooltip.style.left = Math.max(pad, Math.min(left, window.innerWidth - tt.width - pad)) + 'px';
      tooltip.style.top = Math.max(pad, top) + 'px';
    });
  }

  function hideChartTooltip() {
    var tooltip = document.getElementById('chart-tooltip');
    tooltip.classList.remove('visible');
    hoverIndex = null;
    if (chartState.data && chartState.data.length) drawChart(chartState.data, null);
  }

  function scheduleRedraw() {
    requestAnimationFrame(function() {
      requestAnimationFrame(function() {
        if (chartState.data && chartState.data.length) drawChart(chartState.data, hoverIndex);
      });
    });
  }

  function updateStats(data) {
    var el = document.getElementById('stats-row');
    if (!data || data.length === 0) {
      el.style.display = 'none';
      return;
    }
    var totalJup = data.reduce(function(s, d) { return s + (d.jupiter || 0); }, 0);
    var totalBybit = data.reduce(function(s, d) { return s + (d.bybit || 0); }, 0);
    if (totalJup === 0 && totalBybit === 0) {
      el.style.display = 'none';
      return;
    }
    el.style.display = 'flex';
    el.innerHTML = '<span class="stat-badge jupiter">Jupiter: ' + totalJup + '</span><span class="stat-badge bybit">Bybit: ' + totalBybit + '</span>';
  }

  async function fetchAndDraw() {
    var statusEl = document.getElementById('status');
    statusEl.className = 'status loading';
    statusEl.textContent = M.LOADING;

    var data = [];
    var lastError = null;
    try {
      data = await window.App.ApiClient.fetchStatsWithFallback(currentPeriod);
    } catch (e) {
      lastError = e;
    }

    var dims = getChartDimensions();
    var padding = C.PADDING;
    var chartW = dims.w - padding.left - padding.right;
    var chartH = dims.h - padding.top - padding.bottom;

    if (data && data.length > 0) {
      statusEl.className = 'status success';
      statusEl.textContent = M.DATA_LOADED;
      var jArr = data.map(function(d) { return d.jupiter || 0; });
      var bArr = data.map(function(d) { return d.bybit || 0; });
      var rawMax = Math.max(1, ...jArr, ...bArr);
      var ticksResult = buildTicks(rawMax, C.TICK_COUNT);
      var jPoints = computePoints(jArr, padding, chartW, chartH, ticksResult.yMax);
      var bPoints = computePoints(bArr, padding, chartW, chartH, ticksResult.yMax);
      startTransition(data, jPoints, bPoints, ticksResult.yMax);
      updateStats(data);
    } else if (lastError) {
      statusEl.className = 'status error';
      statusEl.textContent = M.ERROR;
      drawChart([]);
      document.getElementById('stats-row').style.display = 'none';
    } else {
      statusEl.className = 'status success';
      statusEl.textContent = '';
      drawChart([]);
      document.getElementById('stats-row').style.display = 'none';
    }
  }

  var chartWrap = document.getElementById('chart-wrap');
  if (chartWrap) {
    chartWrap.addEventListener('mousemove', showChartTooltip, { passive: true });
    chartWrap.addEventListener('mouseleave', hideChartTooltip, { passive: true });
    chartWrap.addEventListener('touchmove', function(e) {
      if (e.touches[0]) showChartTooltip({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY });
    }, { passive: true });
    chartWrap.addEventListener('touchend', hideChartTooltip, { passive: true });
  }

  if (typeof ResizeObserver !== 'undefined' && chartWrap) {
    var ro = new ResizeObserver(function() { scheduleRedraw(); });
    ro.observe(chartWrap);
  }
  window.addEventListener('resize', scheduleRedraw);
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') scheduleRedraw();
  });
  if (window.Telegram && window.Telegram.WebApp) {
    window.Telegram.WebApp.onEvent('viewportChanged', scheduleRedraw);
  }

  window.App = window.App || {};
  window.App.chart = {
    fetchAndDraw: fetchAndDraw,
    setCurrentPeriod: function(p) { currentPeriod = p; },
    getCurrentPeriod: function() { return currentPeriod; },
    redraw: function() {
      if (chartState.data && chartState.data.length) drawChart(chartState.data, hoverIndex);
    }
  };
})();
