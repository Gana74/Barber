// Точка входа бота
// Загружает конфиг, инициализирует Google Sheets, бота и cron-напоминания

const { initConfig } = require("./src/config");
const { createBot } = require("./src/bot");
const { createSheetsService } = require("./src/services/googleSheets");
const { createCalendarService } = require("./src/services/googleCalendar");
const { setupReminders } = require("./src/services/reminders");

// Глобальные обработчики ошибок для предотвращения краха процесса
// Обработчик необработанных отклоненных промисов
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
  
  // Если это ошибка Telegram API (например, пользователь заблокировал бота)
  if (reason && reason.response) {
    const errorCode = reason.response.error_code;
    const errorDescription = reason.response.description || reason.message;
    
    // 403 Forbidden - пользователь заблокировал бота (не критично)
    if (errorCode === 403) {
      console.warn(
        `[Global Error Handler] User blocked the bot. Error: ${errorDescription}`,
      );
      return; // Не завершаем процесс для этой ошибки
    }
    
    // 429 Too Many Requests - превышен лимит (не критично)
    if (errorCode === 429) {
      console.warn(
        `[Global Error Handler] Rate limit exceeded. Error: ${errorDescription}`,
      );
      return; // Не завершаем процесс для этой ошибки
    }
    
    // 400 Bad Request - неверный запрос (не критично)
    if (errorCode === 400) {
      console.warn(
        `[Global Error Handler] Bad request. Error: ${errorDescription}`,
      );
      return; // Не завершаем процесс для этой ошибки
    }
  }
  
  // Для других ошибок логируем, но не завершаем процесс
  // чтобы бот продолжал работать
  console.error("[Global Error Handler] Unhandled rejection logged, continuing...");
});

// Обработчик необработанных исключений
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  
  // Если это ошибка Telegram API, не завершаем процесс
  if (error && error.response) {
    const errorCode = error.response.error_code;
    
    // 403, 429, 400 - не критичные ошибки Telegram API
    if (errorCode === 403 || errorCode === 429 || errorCode === 400) {
      console.warn(
        `[Global Error Handler] Telegram API error (${errorCode}), continuing...`,
      );
      return; // Не завершаем процесс
    }
  }
  
  // Для критических ошибок логируем и завершаем процесс
  console.error("[Global Error Handler] Critical error, exiting...");
  process.exit(1);
});

async function main() {
  // Комментарий: базовый лог старта приложения
  console.log("Bootstrapping barber bot...");

  // Инициализация конфига (env и т.п.)
  const config = initConfig();

  // Инициализация Google Sheets API
  const sheetsService = await createSheetsService(config);

  // Инициализация Google Calendar (опционально, если указан calendarId)
  let calendarService = null;
  if (config.google && config.google.calendarId) {
    try {
      calendarService = await createCalendarService(config);
    } catch (e) {
      console.warn("Не удалось инициализировать Google Calendar:", e.message);
      calendarService = null;
    }
  }

  // Автосоздание листов/заголовков/таймзоны, если их нет
  await sheetsService.ensureSheetsStructure();

  // Инициализация и запуск бота
  const bot = createBot({ config, sheetsService, calendarService });

  // Настройка cron-напоминаний
  setupReminders({ bot, config, sheetsService, calendarService });

  console.log("Launching Telegram bot...");
  // Запуск long polling
  await bot.launch();
  console.log("Bot is up. Waiting for updates...");

  // Комментарий: graceful shutdown
  process.once("SIGINT", () => bot.stop("SIGINT"));
  process.once("SIGTERM", () => bot.stop("SIGTERM"));
}

// Комментарий: запуск main c логированием ошибок
main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("Fatal error in main:", err);
  process.exit(1);
});
