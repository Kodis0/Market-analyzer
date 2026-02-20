# Web App для кнопки «Навигация»

Минимальное приложение с чёрным экраном. Открывается по нажатию кнопки в Telegram.

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
