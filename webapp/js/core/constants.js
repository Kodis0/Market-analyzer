/**
 * Константы приложения: magic numbers, строки сообщений, настройки UI.
 * Централизованное хранение для соблюдения DRY и упрощения поддержки.
 */
(function() {
  'use strict';

  const CHART = {
    ANIM_DURATION_MS: 380,
    TICK_COUNT: 5,
    SPLINE_TENSION: 0.35,
    SERIES_TENSION: 0.4,
    PADDING: { top: 20, right: 20, bottom: 32, left: 48 },
    DEFAULT_WIDTH: 300,
    DEFAULT_HEIGHT: 220,
    COLORS: {
      JUPITER: '#2F80FF',
      BYBIT: '#00D18F',
      JUPITER_GRADIENT: [[0.0, 'rgba(47,128,255,0.25)'], [0.6, 'rgba(47,128,255,0.06)'], [1.0, 'rgba(47,128,255,0)']],
      BYBIT_GRADIENT: [[0.0, 'rgba(0,209,143,0.2)'], [0.6, 'rgba(0,209,143,0.05)'], [1.0, 'rgba(0,209,143,0)']],
      GRID: 'rgba(255,255,255,0.07)',
      BORDER: 'rgba(255,255,255,0.1)',
      Y_LABELS: 'rgba(255,255,255,0.5)',
      X_LABELS: 'rgba(255,255,255,0.42)',
      HOVER_LINE: 'rgba(255,255,255,0.2)',
      POINT_BG: 'rgba(11,11,14,0.9)',
      MARKER_BG: 'rgba(11,11,14,0.95)'
    }
  };

  const MESSAGES = {
    AUTH_REQUIRED: 'Откройте дашборд через Telegram (кнопка «Навигация»)',
    RATE_LIMIT: 'Слишком много запросов. Подождите минуту и обновите страницу.',
    RATE_LIMIT_SHORT: 'Слишком много запросов. Подождите минуту.',
    API_NOT_FOUND: 'API не найден. Проверь URL (должен быть {domain}), бот и туннель.',
    API_HTML_RESPONSE: 'API вернул HTML вместо JSON. Проверь: {domain} (не market), бот запущен, туннель cloudflared. Тест: curl https://{domain}/api/stats?period=1h',
    LOADING: 'Загрузка...',
    NO_DATA: 'Нет данных',
    DATA_LOADED: 'Данные загружены',
    ERROR: 'Ошибка',
    SAVE_ERROR: 'Ошибка сохранения',
    API_ADDRESS_REQUIRED: 'Ошибка: не задан адрес API',
    SERVER_NOT_JSON: 'Ошибка: сервер вернул не JSON',
    PARSE_ERROR: 'Ошибка разбора ответа сервера',
    NO_SIGNALS: 'Нет сигналов за выбранный период',
    ALL_STALE: 'Все сигналы устарели. Снимите галочку «Скрыть устаревшие»',
    DELETE_CONFIRM: 'Удалить сигнал из истории?',
    ONLINE: 'Онлайн',
    OFFLINE: 'Офлайн',
    CHECKING: 'Проверка...',
    ENABLED: 'Включена',
    DISABLED: 'Выключена'
  };

  const SETTINGS = {
    HIDDEN_KEYS: new Set(['exchange_enabled', 'auto_tune_enabled', 'auto_tune_bounds', 'delete_stale']),
    DEBOUNCE_MS: 150,
    INT_KEYS: ['persistence_hits', 'cooldown_sec', 'engine_tick_hz', 'max_ob_age_ms', 'stale_ttl_sec']
  };

  const EASING = {
    easeOutCubic: function(t) { return 1 - Math.pow(1 - t, 3); }
  };

  const MAIN = {
    REFRESH_INTERVAL_SEC: 5,
    REFRESH_LABEL: 'Refresh',
    REFRESHING_LABEL: 'Refresh...'
  };

  window.App = window.App || {};
  window.App.Constants = {
    CHART,
    MESSAGES,
    SETTINGS,
    EASING,
    MAIN
  };
})();
