# Деплой на Railway

Бот + API в одном процессе. Фронт на Cloudflare Pages.

## Архитектура

```
Railway (один сервис)
├── Бот (Telegram, Bybit, Jupiter)
└── API :PORT → /api/stats

Фронт (Cloudflare) → GET /api/stats
```

## Шаги

1. **New Project** → **Deploy from GitHub** → репозиторий
2. **Variables:** `TG_BOT_TOKEN`, `JUP_API_KEY`
3. **Add Volume** (опционально): mount `/data` — для settings.json, request_stats.db
4. **Generate Domain** → URL: `https://xxx.up.railway.app`
5. В `config.yaml` → `telegram.api_base_url: https://xxx.up.railway.app` (для кнопки Mini App)

## Локально или на своём сервере

```bash
python app.py -c config.yaml
```

Всё в одном: бот + API на порту 8080 (или `PORT` из env).
