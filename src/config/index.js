// Конфиг проекта: env, таймзона, ID таблицы и пр.
// Комментарий: используй .env для локальной разработки

const path = require("path");
const dotenv = require("dotenv");

let isEnvLoaded = false;

function loadEnv() {
  if (isEnvLoaded) return;

  // Комментарий: загружаем .env из корня проекта
  dotenv.config({
    path: path.resolve(process.cwd(), ".env"),
  });

  isEnvLoaded = true;
}

function initConfig() {
  loadEnv();

  const config = {
    botToken: process.env.BOT_TOKEN,
    google: {
      sheetsId: process.env.GOOGLE_SHEETS_ID,
      clientEmail: process.env.GOOGLE_CLIENT_EMAIL,
      privateKey: process.env.GOOGLE_PRIVATE_KEY
        ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n")
        : undefined,
    },
    defaultTimezone: process.env.DEFAULT_TIMEZONE || "Asia/Yekaterinburg",
    managerChatId: process.env.MANAGER_CHAT_ID
      ? Number(process.env.MANAGER_CHAT_ID)
      : undefined,
    // Комментарий: рабочие часы салона по умолчанию
    workday: {
      startHour: 10,
      endHour: 20,
    },
  };

  if (!config.botToken) {
    throw new Error("BOT_TOKEN is required");
  }
  if (!config.google.sheetsId) {
    throw new Error("GOOGLE_SHEETS_ID is required");
  }
  if (!config.google.clientEmail || !config.google.privateKey) {
    throw new Error("Google service account credentials are required");
  }

  return config;
}

module.exports = {
  initConfig,
};
