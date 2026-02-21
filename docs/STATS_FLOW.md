# Как записывается статистика запросов (дашборд)

## Схема

```
Jupiter API (quote)  →  record("jupiter", 1)   →  request_stats
Bybit WebSocket      →  record("bybit", N)     →  request_stats
Heartbeat (раз в мин) →  record("jupiter", 1)   →  request_stats
                       record("bybit", 1)
```

## Когда пишется в БД

| Источник | Когда | Условие |
|----------|-------|---------|
| **Jupiter** | Каждый запрос к Jupiter API (quote) | `exchange_enabled=true`, движок опрашивает котировки |
| **Bybit** | Каждое N-е сообщение WebSocket | `stats_bybit_sample` (по умолчанию 10), т.е. 1 из 10 сообщений |
| **Heartbeat** | Раз в 60 сек | `exchange_enabled=true`, бот запущен |

## Почему «Нет данных»?

1. **Биржевая логика выключена** — `exchange_enabled=false`. Включи в настройках или `/exchange on`.
2. **Бот не запущен** — API и БД на том же процессе. Нет бота = нет записей.
3. **Туннель не работает** — API недоступен снаружи. Проверь: `curl https://api.arbmarketsystem.ru/api/stats?period=1h`.
4. **Бот только запустился** — heartbeat пишет раз в минуту. Подожди 1–2 минуты и обнови дашборд.

## Файл БД

`request_stats.db` — рядом с `config.yaml`. Таблица `request_stats` (ts_bucket, source, count).
