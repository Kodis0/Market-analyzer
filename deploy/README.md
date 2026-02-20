# Деплой на свой сервер (Timeweb и др.)

Автодеплой: при каждом push в `main` GitHub Actions подключается по SSH, подтягивает код и перезапускает бота.

---

## Один раз: настройка сервера

Подключись к серверу по SSH (логин/пароль или уже настроенный ключ).

### 1. Клонировать репозиторий

```bash
cd ~
git clone https://github.com/YOUR_USERNAME/Market-analyzer.git
cd Market-analyzer
```

Замени `YOUR_USERNAME` на свой GitHub username или полный URL репо.

### 2. Python venv и зависимости

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Конфиг и переменные

Создай `.env` в корне проекта:

```bash
nano .env
```

Добавь строки:
```
TG_BOT_TOKEN=твой_токен
JUP_API_KEY=твой_ключ
```

И скопируй `config.yaml` с локальной машины или отредактируй под сервер.

### 4. Systemd-сервис

```bash
# Подставляет текущего пользователя и его домашнюю директорию (работает и для root)
sed -e "s/YOUR_USER/$USER/g" -e "s|/home/YOUR_USER|$HOME|g" deploy/market-analyzer.service | sudo tee /etc/systemd/system/market-analyzer.service
sudo systemctl daemon-reload
sudo systemctl enable market-analyzer
sudo systemctl start market-analyzer
```

### 5. Добавить SSH-ключ для деплоя в authorized_keys

На **Windows** (PowerShell):

```powershell
Get-Content $env:USERPROFILE\.ssh\deploy_key.pub
```

Скопируй вывод. На **сервере**:

```bash
mkdir -p ~/.ssh
nano ~/.ssh/authorized_keys
```

Вставь скопированный ключ в одну строку. Сохрани (Ctrl+O, Enter, Ctrl+X).

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

---

## GitHub Secrets (для автодеплоя)

Репозиторий → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.

| Secret           | Значение                                      |
|------------------|-----------------------------------------------|
| `SERVER_HOST`    | IP или домен сервера (например `185.125.103.15`) |
| `SERVER_USER`    | Пользователь SSH (например `root`)            |
| `SERVER_SSH_KEY` | Приватный ключ `deploy_key` (см. ниже)        |
| `DEPLOY_PATH`    | Путь к проекту (для root: `/root/Market-analyzer`) |
| `SERVER_PORT`    | Порт SSH (по умолчанию 22, можно не задавать)  |

**Как получить приватный ключ (Windows PowerShell):**

```powershell
Get-Content $env:USERPROFILE\.ssh\deploy_key
```

Скопируй **весь** вывод, включая строки `-----BEGIN ...` и `-----END ...`, и вставь в `SERVER_SSH_KEY`.

---

## Как это работает

1. Ты пушишь в `main` или `master`.
2. GitHub Actions запускает workflow.
3. Подключение по SSH к серверу.
4. `git fetch && git reset --hard origin/main` — подтягивает последний код.
5. `pip install -r requirements.txt` — обновляет зависимости.
6. `systemctl restart market-analyzer` — перезапускает бота.

---

## Полезные команды

```bash
# Статус сервиса
sudo systemctl status market-analyzer

# Логи
sudo journalctl -u market-analyzer -f

# Ручной деплой (если нужно)
cd ~/Market-analyzer && git pull && sudo systemctl restart market-analyzer
```
