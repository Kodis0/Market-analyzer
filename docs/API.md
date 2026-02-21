# API Reference

HTTP API для Mini App дашборда. Защита: Telegram initData, auth_date TTL, allowlist user_id, rate limiting.

## Request ID

Все ответы содержат заголовок `X-Request-ID` — уникальный ID запроса для трассировки в логах. Клиент может передать свой ID в заголовке `X-Request-ID`, он будет использован вместо генерации нового.

## Аутентификация

Все `/api/*` эндпоинты (кроме OPTIONS) требуют заголовок:

```
X-Telegram-Init-Data: <initData от Telegram Web App>
```

Открой дашборд через кнопку «Навигация» в боте — initData подставится автоматически.

## Эндпоинты

### GET /

Корневой эндпоинт. Возвращает статус API и БД.

**Ответ:** `200 OK`

```json
{
  "ok": true,
  "api": "market-analyzer",
  "db_ok": true,
  "exchange_enabled": true,
  "auto_tune_enabled": false
}
```

---

### GET /api/stats

Статистика запросов к Jupiter и Bybit (для графика).

**Query:**
| Параметр | Тип   | По умолчанию | Описание                    |
|----------|-------|--------------|-----------------------------|
| period   | string| 1h           | 1h, 1d, 1w, all            |

**Ответ:** `200 OK` — массив объектов

```json
[
  { "ts": 1708000000, "jupiter": 120, "bybit": 450 },
  { "ts": 1708000060, "jupiter": 115, "bybit": 440 }
]
```

---

### GET /api/signal-history

История арбитражных сигналов.

**Query:**
| Параметр | Тип   | По умолчанию | Описание                    |
|----------|-------|--------------|-----------------------------|
| period   | string| 1d           | 1h, 1d, 1w, all            |
| limit    | int   | 200          | 1–500                       |

**Ответ:** `200 OK` — массив объектов

```json
[
  {
    "id": 1,
    "ts": 1708000000,
    "token": "BONK",
    "direction": "JUP->BYBIT",
    "profit_usd": 2.5,
    "notional_usd": 1000,
    "status": "active",
    "is_stale": false
  }
]
```

---

### PATCH /api/signal-history

Обновить статус сигнала.

**Body:** `application/json`

```json
{ "id": 1, "status": "stale" }
```

**status:** `active` | `stale`

**Ответ:** `200 OK` — `{ "ok": true }`

---

### DELETE /api/signal-history

Удалить сигнал.

**Query:** `id` — ID сигнала

**Ответ:** `200 OK` — `{ "ok": true }`

---

### GET /api/status

Статус бота (exchange_enabled, auto_tune_enabled).

**Ответ:** `200 OK`

```json
{
  "exchange_enabled": true,
  "auto_tune_enabled": false
}
```

---

### GET /api/settings

Текущие настройки (read-only).

**Ответ:** `200 OK`

```json
{
  "settings": { "min_profit_usd": 1.0, "notional_usd": 1000, ... },
  "labels": { "min_profit_usd": "Мин. прибыль ($)", ... }
}
```

---

### POST /api/settings

Обновить настройки.

**Body:** `application/json`

```json
{ "min_profit_usd": 2.0, "cooldown_sec": 120 }
```

**Ответ:** `200 OK`

```json
{
  "ok": true,
  "updated": { "min_profit_usd": 2.0, "cooldown_sec": 120 },
  "settings": { ... }
}
```

---

### POST /api/exchange

Включить/выключить биржевую логику.

**Body:** `application/json`

```json
{ "enabled": true }
```

**Ответ:** `200 OK` — `{ "ok": true, "exchange_enabled": true }`

---

### GET /api/auto_tune

Состояние AutoTune.

**Ответ:** `200 OK`

```json
{
  "enabled": false,
  "metrics": { ... },
  "history": [],
  "bounds": { ... }
}
```

---

### POST /api/auto_tune

Обновить AutoTune (enabled, bounds, action: reset_to_defaults).

**Body:** `application/json`

```json
{ "enabled": true }
```

---

### GET /api/logs

Последние N строк логов (если включено в config).

**Query:** `limit` — 1–200, по умолчанию 100

**Ответ:** `200 OK` — `{ "lines": ["...", "..."], "total": N }`  
**Ответ:** `404` — логи отключены

---

## Коды ошибок

| Код | Описание                                      |
|-----|-----------------------------------------------|
| 400 | Неверный запрос (тело, параметры)             |
| 401 | Не авторизован (нет/неверный initData)        |
| 404 | Ресурс не найден (логи отключены)             |
| 429 | Rate limit (слишком много запросов)           |
