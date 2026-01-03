// Точка входа бота
// Загружает конфиг, инициализирует Google Sheets, бота и cron-напоминания

// Модуль конфигурации приложения
// Загружает переменные окружения и предоставляет доступ к настройкам

require("dotenv").config();

// Комментарий: функция инициализации конфигурации
function initConfig() {
  // Комментарий: проверяем наличие критичных переменных окружения
  const requiredEnvVars = ["BOT_TOKEN", "GOOGLE_SHEETS_ID", "GOOGLE_CLIENT_EMAIL", "GOOGLE_PRIVATE_KEY"];
  
  for (const envVar of requiredEnvVars) {
    if (!process.env[envVar]) {
      console.warn(`⚠️  Внимание: переменная окружения ${envVar} не задана!`);
    }
  }

  const config = {
    // Токен Telegram-бота
    botToken: process.env.BOT_TOKEN,
    
    // Настройки Google Sheets
    google: {
      sheetsId: process.env.GOOGLE_SHEETS_ID,
      clientEmail: process.env.GOOGLE_CLIENT_EMAIL,
      privateKey: process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, "\n"),
    },
    
    // Таймзона по умолчанию (можно переопределить в Google Sheets)
    defaultTimezone: process.env.DEFAULT_TIMEZONE || "Asia/Yekaterinburg",
    
    // Chat ID менеджера для уведомлений
    managerChatId: process.env.MANAGER_CHAT_ID,
    
    // Контакты для уведомлений
    barberPhone: process.env.BARBER_PHONE,
    barberAddress: process.env.BARBER_ADDRESS,
    
    // Рабочие часы (в формате 24h)
    workday: {
      startHour: parseInt(process.env.WORKDAY_START_HOUR, 10) || 10,
      endHour: parseInt(process.env.WORKDAY_END_HOUR, 10) || 20,
    },
    
    // Включить приветственные напоминания за день
    enableWelcomeReminder: process.env.ENABLE_WELCOME_REMINDER === "true",
  };
  
  return config;
}

module.exports = {
  initConfig,
};
