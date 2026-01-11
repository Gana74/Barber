// Точка входа бота
// Загружает конфиг, инициализирует Google Sheets, бота и cron-напоминания

const { initConfig } = require("./src/config");
const { createBot } = require("./src/bot");
const { createSheetsService } = require("./src/services/googleSheets");
const { createCalendarService } = require("./src/services/googleCalendar");
const { setupReminders } = require("./src/services/reminders");
const { setupScheduledBackups } = require("./src/utils/backup");

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

  // Настройка периодических резервных копий (раз в день)
  setupScheduledBackups(24);

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
