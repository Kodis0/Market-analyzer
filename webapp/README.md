# Web App для кнопки «Навигация»

Дашборд с графиком запросов к Jupiter и Bybit. Открывается по нажатию кнопки в Telegram.

## API

График получает данные от API, который запускается вместе с ботом (порт 8080 по умолчанию, `API_PORT` в .env).

**Важно:** API должен быть доступен с фронта (CORS включён). Для продакшена:
1. Запусти бота на VPS (API на порту 8080)
2. Открой порт 8080 в firewall или настрой nginx с SSL
3. В BotFather при создании Mini App укажи URL с параметром: `https://твой-фронт.pages.dev?api=http://IP:8080`

Пример: `https://market-analyzer-9kk.pages.dev?api=http://109.73.199.154:8080`

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
