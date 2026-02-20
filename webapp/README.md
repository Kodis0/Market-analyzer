# Web App для кнопки «Навигация»

Дашборд с графиком запросов к Jupiter и Bybit. Открывается по нажатию кнопки в Telegram.

## API

График получает данные от API, который запускается вместе с ботом (порт 8080 по умолчанию, `API_PORT` в .env).

**Важно:** API должен быть доступен по HTTPS с того же домена или с CORS. Для продакшена:
1. Запусти бота на VPS
2. Настрой nginx/reverse proxy с SSL для порта API
3. В BotFather при создании Direct Link укажи URL с параметром: `https://твой-домен.com?startapp=https://api.твой-домен.com`

Либо измени `data-api-url` в index.html на URL твоего API.

## Деплой (бесплатно)

### Вариант 1: Netlify Drop
1. Открой https://app.netlify.com/drop
2. Перетащи папку `webapp` (или её содержимое) в окно
3. Получишь URL вида `https://random-name-123.netlify.app`
4. Добавь в `config.yaml`:
   ```yaml
   telegram:
     web_app_url: https://random-name-123.netlify.app
   ```

### Вариант 2: Surge.sh
```bash
npx surge webapp/ https://my-nav.surge.sh
```
Добавь URL в config.yaml.

### Вариант 3: GitHub Pages
1. Создай репозиторий
2. Залей содержимое webapp/ в ветку `gh-pages` или включи Pages в настройках
3. URL: `https://username.github.io/repo-name/`

## После деплоя
- Запусти бота и напиши `/pin_setup` в группе
- Закрепи отправленное сообщение
- Кнопка «НАВИГАЦИЯ» будет видна на телефоне и ПК
