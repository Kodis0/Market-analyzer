/**
 * Утилиты: экранирование HTML, форматирование времени и направлений.
 */
(function() {
  function escapeHtml(s) {
    if (s == null || s === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(s);
    return div.innerHTML;
  }

  function formatTime(ts, period) {
    const d = new Date(ts * 1000);
    if (period === '1h') return d.getHours() + ':' + String(d.getMinutes()).padStart(2, '0');
    if (period === '1d') return d.getHours() + ':00';
    if (period === '1w' || period === 'all') return d.getDate() + '/' + (d.getMonth() + 1);
    return d.toLocaleTimeString();
  }

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

  window.App = window.App || {};
  window.App.escapeHtml = escapeHtml;
  window.App.formatTime = formatTime;
  window.App.formatSignalTime = formatSignalTime;
  window.App.formatDirection = formatDirection;
})();