# Настройка Mini App с доменом arbmarketsystem.ru

Пошаговая инструкция, чтобы дашборд работал в Telegram и показывал статистику.

---

## Схема

```
Telegram Mini App (кнопка «Навигация»)
    ↓ открывает
Фронт (HTTPS): market.arbmarketsystem.ru  ← Cloudflare Pages
    ↓ запрашивает
API (HTTPS): api.arbmarketsystem.ru       ← Cloudflare Tunnel → localhost:8080
```

Оба должны быть **HTTPS** (Telegram блокирует mixed content).

---

## 1. Домен в Cloudflare

Домен arbmarketsystem.ru уже на reg.ru и подключён к Cloudflare (NS записи).

В **Cloudflare Dashboard** → **arbmarketsystem.ru** → **DNS** проверь:
- Есть ли записи для `api` и `market` (или `www`) — их создадим ниже.

---

## 2. API: api.arbmarketsystem.ru

### Вариант A: Cloudflare Tunnel (рекомендуется)

Туннель пробрасывает трафик на бота без открытия портов.

**На сервере (VPS):**

```bash
# Установка cloudflared (если ещё нет)
# Ubuntu/Debian:
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
chmod +x /usr/local/bin/cloudflared

# Вход в Cloudflare (один раз)
cloudflared tunnel login
# Откроется браузер — выбери домен arbmarketsystem.ru

# Создание туннеля
cloudflared tunnel create market-api

# Конфиг (замени TUNNEL_ID на выданный ID)
mkdir -p ~/.cloudflared
cat > ~/.cloudflared/config.yml << 'EOF'
tunnel: TUNNEL_ID
credentials-file: /root/.cloudflared/TUNNEL_ID.json

ingress:
  - hostname: api.arbmarketsystem.ru
    service: http://localhost:8080
  - service: http_status:404
EOF

# Запуск туннеля (или через systemd)
cloudflared tunnel run market-api
```

**В Cloudflare Dashboard** → **Zero Trust** → **Tunnels** → твой туннель → **Public Hostname**:
- `api.arbmarketsystem.ru` → `http://localhost:8080`

Cloudflare сам создаст CNAME для `api.arbmarketsystem.ru`.

**Проверка:**
```bash
curl https://api.arbmarketsystem.ru/api/stats?period=1h
```

### Вариант B: A‑запись + Cloudflare Proxy

Если туннель не нужен:

1. **DNS** → Add record: `api` → A → IP сервера → Proxy (оранжевое облако).
2. На сервере: nginx с SSL (Let's Encrypt) и проксирование на `localhost:8080`.

---

## 3. Фронт: market.arbmarketsystem.ru (Cloudflare Pages)

### Шаг 1: Создать Pages-проект

1. **Cloudflare Dashboard** → **Workers & Pages** → **Create** → **Pages** → **Connect to Git**.
2. Подключи репозиторий Market-analyzer.
3. Build settings:
   - **Framework preset:** None
   - **Build command:** (пусто)
   - **Build output directory:** `webapp` (или **Root directory:** `webapp`, **Build output:** `.`)
4. **Save and Deploy**.

### Шаг 2: Свой домен

1. В проекте Pages → **Custom domains** → **Set up a custom domain**.
2. Введи `market.arbmarketsystem.ru`.
3. Cloudflare создаст CNAME и выдаст SSL.

### Шаг 3: Альтернатива без Git

Если не хочешь подключать Git:
- **Create** → **Pages** → **Upload assets**.
- Перетащи содержимое папки `webapp` (index.html и т.п.).
- Добавь домен `market.arbmarketsystem.ru`.

---

## 4. BotFather: Mini App URL

1. Открой [@BotFather](https://t.me/BotFather).
2. `/mybots` → выбери бота → **Bot Settings** → **Menu Button** → **Configure menu button**.
3. **Menu button URL:** `https://market.arbmarketsystem.ru`
4. **Menu button name:** `Навигация` (или как удобно).

Либо при создании Web App:
- **Web App URL:** `https://market.arbmarketsystem.ru`

---

## 5. config.yaml

```yaml
telegram:
  chat_id: ...
  thread_id: ...
  web_app_url: https://market.arbmarketsystem.ru
  pinned_message_text: "..."  # опционально
```

---

## 6. Проверка

1. **API:** `curl https://api.arbmarketsystem.ru/api/stats?period=1h` — должен вернуть JSON.
2. **Фронт:** открой в браузере `https://market.arbmarketsystem.ru` — график и статистика.
3. **Telegram:** открой бота → кнопка «Навигация» (или Menu) → должен открыться дашборд.

---

## Частые проблемы

| Проблема | Решение |
|----------|---------|
| «Ошибка: Failed to fetch» | API недоступен. Проверь туннель и `curl https://api.arbmarketsystem.ru/api/stats`. |
| «Mixed Content» | Фронт и API должны быть HTTPS. |
| Пустой график | Бот должен работать и писать в `request_stats.db`. Запусти бота, подожди накопления данных. |
| Кнопка не открывается | Проверь URL в BotFather и что домен market.arbmarketsystem.ru открывается в браузере. |

---

## Текущие URL в проекте

- **data-api-url** в `index.html`: `https://api.arbmarketsystem.ru` — используется по умолчанию.
- **config.yaml** `web_app_url`: должен указывать на `https://market.arbmarketsystem.ru` (или твой фронт).
