# Проверка туннеля api.arbmarketsystem.ru

Если Mini App не показывает данные — скорее всего API недоступен. API идёт через Cloudflare Tunnel.

## Быстрая проверка

**С твоего ПК:**
```bash
curl -s https://api.arbmarketsystem.ru/api/stats?period=1h
```

- Если JSON с данными — туннель работает.
- Если таймаут / connection refused — туннель не запущен или не настроен.

## На сервере

```bash
# Туннель запущен?
systemctl status cloudflared-tunnel
# или
ps aux | grep cloudflared

# Бот и API слушают 8080?
ss -tlnp | grep 8080

# Локально API отвечает?
curl -s http://localhost:8080/api/stats?period=1h
```

## Если туннеля нет — поставить

```bash
# 1. Установить cloudflared
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
chmod +x /usr/local/bin/cloudflared

# 2. Войти (ВАЖНО — создаёт cert.pem, без него "Cannot determine origin certificate")
cloudflared tunnel login
# На сервере без браузера: откроется URL — скопируй и открой на своём ПК в браузере
# Выбери домен arbmarketsystem.ru, авторизуй
# cert.pem появится в ~/.cloudflared/

# 3. Создать туннель
cloudflared tunnel create market-api
# Сохрани выданный TUNNEL_ID (uuid)

# 4. Конфиг
mkdir -p ~/.cloudflared
nano ~/.cloudflared/config.yml
# Вставь из deploy/cloudflared-config.yml.example, подставь TUNNEL_ID в tunnel и credentials-file

# 5. Cloudflare Dashboard -> Zero Trust -> Tunnels -> market-api -> Public Hostname
#    api.arbmarketsystem.ru -> http://localhost:8080

# 6. Запустить
cloudflared tunnel run market-api
```

Для автозапуска — systemd unit в `deploy/cloudflared-tunnel.service`.
