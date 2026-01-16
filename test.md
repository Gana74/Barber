Отлично! Помогу развернуть вашего Telegram-бота на VDS с Ubuntu 24.04. Вот пошаговая инструкция:

## 1. Подготовка VDS

### Подключитесь к серверу по SSH:
```bash
ssh root@185.251.88.99
```

### Обновите систему:
```bash
apt update && apt upgrade -y
```

## 2. Установка необходимого ПО

### Установите Node.js 18+:
```bash
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt install -y nodejs
node --version  # Проверьте установку
```

### Установите PM2 для управления процессом:
```bash
npm install -g pm2
```

### Установите Git:
```bash
apt install -y git
```

### Установите Nginx (опционально, для будущего использования):
```bash
apt install -y nginx
```

## 3. Настройка проекта

### Клонируйте репозиторий:
```bash
cd /opt
git clone <ваш-git-репозиторий> barber-bot
cd barber-bot
```

### Установите зависимости:
```bash
npm install
```

## 4. Настройка переменных окружения

### Создайте файл .env:
```bash
nano .env
```

### Добавьте ваши переменные (как в локальном .env):
```
BOT_TOKEN=ваш_токен_бота
GOOGLE_SHEETS_ID=ID_вашей_таблицы
GOOGLE_CLIENT_EMAIL=service-account@project.iam.gserviceaccount.com
GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
DEFAULT_TIMEZONE=Asia/Yekaterinburg
MANAGER_CHAT_ID=123456789
```

### Убедитесь, что файл защищён:
```bash
chmod 600 .env
```

## 5. Проверка работоспособности

### Запустите бота в тестовом режиме:
```bash
npm run dev
```

Если всё работает (CTRL+C для остановки), переходите к следующему шагу.

## 6. Настройка PM2 для продакшена

### Создайте конфигурационный файл для PM2:
```bash
pm2 init simple
```

Отредактируйте созданный файл `ecosystem.config.js`:
```javascript
module.exports = {
  apps: [{
    name: "barber-bot",
    script: "index.js",
    env: {
      NODE_ENV: "production"
    },
    log_date_format: "YYYY-MM-DD HH:mm:ss",
    error_file: "logs/error.log",
    out_file: "logs/out.log",
    time: true
  }]
}
```

### Создайте директории для логов:
```bash
mkdir -p logs
```

## 7. Запуск и настройка автозапуска

### Запустите бота через PM2:
```bash
pm2 start ecosystem.config.js
```

### Сохраните конфигурацию PM2 для автозапуска:
```bash
pm2 save
pm2 startup
```


### Проверьте статус бота:
```bash
pm2 status
pm2 logs barber-bot
```


## 10. Полезные команды для управления

### Просмотр логов:
```bash
pm2 logs barber-bot
tail -f logs/out.log
tail -f logs/error.log
```

### Перезапуск бота:
```bash
pm2 restart barber-bot
```

### Перезагрузка после изменений в коде:
```bash
cd /opt/barber-bot
git pull
npm install
pm2 restart barber-bot
```

### Просмотр потребления ресурсов:
```bash
pm2 monit
```

