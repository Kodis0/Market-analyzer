# Market-analyzer

Мониторинг арбитража между Jupiter (Solana DEX) и Bybit (CEX). Отправляет сигналы в Telegram при обнаружении возможностей.

## Возможности

- WebSocket Bybit — стакан в реальном времени
- Jupiter API — котировки для свопов
- Арбитражный движок — расчёт профита, фильтры
- Telegram — сигналы, настройки через /settings, Mini App дашборд
- API для дашборда — статистика, история сигналов, управление

## Требования

- Python 3.10+
- `.env` с `TG_BOT_TOKEN`, `JUP_API_KEY` (скопируй `cp .env.example .env` и заполни)
- `config.yaml` (см. `config.yaml` в репозитории)

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python app.py -c config.yaml
```

Бот + API запускаются в одном процессе. API слушает порт 8080 (или `PORT` из env).

## Конфигурация

- `config.yaml` — основная конфигурация (токены, символы, фильтры)
- `settings.json` — runtime-настройки (перезаписываются через /settings)
- `.env` — секреты (TG_BOT_TOKEN, JUP_API_KEY)

### settings.json

При первом запуске `settings.json` создаётся из дефолтов. Для кастомной настройки скопируй шаблон:

```bash
cp settings.json.example settings.json
```

Затем отредактируй и настрой параметры (min_profit_usd, notional_usd и др.).

Подробнее: [webapp/MINIAPP_SETUP.md](webapp/MINIAPP_SETUP.md), [RAILWAY.md](RAILWAY.md), [docs/STATS_FLOW.md](docs/STATS_FLOW.md).

## Структура

```
app.py              — точка входа
core/               — движок, конфиг, state
connectors/         — Bybit WS, Jupiter HTTP
notifier/           — Telegram
api/                — HTTP API для Mini App
webapp/             — фронт дашборда (Cloudflare Pages)
tools/              — gen_watchlist, quarantine_refresh
```
