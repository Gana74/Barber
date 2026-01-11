// Структурированное логирование с асинхронной буферизацией для оптимизации I/O
// Критичные действия логируются синхронно, остальные - через буфер

const fs = require("fs").promises;
const path = require("path");

const LOG_DIR = path.resolve(process.cwd(), "logs");
const LOG_FILE = path.resolve(LOG_DIR, "security.log");
const MAX_LOG_FILE_SIZE = 10 * 1024 * 1024; // 10MB

// Буфер для асинхронных логов
let logBuffer = [];
let bufferFlushTimer = null;
const BUFFER_FLUSH_INTERVAL = 2000; // 2 секунды
const BUFFER_MAX_SIZE = 50; // 50 записей

// Обеспечиваем существование директории логов
async function ensureLogDir() {
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
  } catch (e) {
    // Игнорируем ошибки, если директория уже существует
  }
}

/**
 * Форматирование записи лога
 */
function formatLogEntry(level, userId, action, details, result) {
  const timestamp = new Date().toISOString();
  const entry = {
    timestamp,
    level,
    userId: userId || "unknown",
    action: action || "unknown",
    details: details || {},
    result: result || "unknown",
  };
  return JSON.stringify(entry) + "\n";
}

/**
 * Ротация лога по размеру
 */
async function rotateLogIfNeeded() {
  try {
    const stats = await fs.stat(LOG_FILE).catch(() => null);
    if (stats && stats.size > MAX_LOG_FILE_SIZE) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
      const rotatedFile = path.resolve(LOG_DIR, `security.${timestamp}.log`);
      await fs.rename(LOG_FILE, rotatedFile);
      
      // Удаляем старые ротированные файлы (старше 30 дней)
      const files = await fs.readdir(LOG_DIR).catch(() => []);
      const now = Date.now();
      const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;
      
      for (const file of files) {
        if (file.startsWith("security.") && file.endsWith(".log") && file !== "security.log") {
          const filePath = path.resolve(LOG_DIR, file);
          const fileStats = await fs.stat(filePath).catch(() => null);
          if (fileStats && fileStats.mtimeMs < thirtyDaysAgo) {
            await fs.unlink(filePath).catch(() => {});
          }
        }
      }
    }
  } catch (e) {
    // Игнорируем ошибки ротации
  }
}

/**
 * Синхронная запись в лог (для критичных действий)
 */
async function writeLogSync(level, userId, action, details, result) {
  try {
    await ensureLogDir();
    await rotateLogIfNeeded();
    
    const entry = formatLogEntry(level, userId, action, details, result);
    await fs.appendFile(LOG_FILE, entry, "utf8");
  } catch (e) {
    // В случае ошибки логирования не прерываем выполнение
    console.error("Failed to write log:", e.message);
  }
}

/**
 * Добавление записи в буфер для асинхронной записи
 */
function addToBuffer(level, userId, action, details, result) {
  logBuffer.push({ level, userId, action, details, result });
  
  // Если буфер заполнен, сбрасываем немедленно
  if (logBuffer.length >= BUFFER_MAX_SIZE) {
    flushBuffer();
  } else if (!bufferFlushTimer) {
    // Запускаем таймер для периодического сброса
    bufferFlushTimer = setTimeout(flushBuffer, BUFFER_FLUSH_INTERVAL);
  }
}

/**
 * Сброс буфера в файл
 */
async function flushBuffer() {
  if (bufferFlushTimer) {
    clearTimeout(bufferFlushTimer);
    bufferFlushTimer = null;
  }
  
  if (logBuffer.length === 0) {
    return;
  }
  
  const entries = [...logBuffer];
  logBuffer = [];
  
  try {
    await ensureLogDir();
    await rotateLogIfNeeded();
    
    const lines = entries.map((entry) =>
      formatLogEntry(
        entry.level,
        entry.userId,
        entry.action,
        entry.details,
        entry.result
      )
    );
    
    await fs.appendFile(LOG_FILE, lines.join(""), "utf8");
  } catch (e) {
    console.error("Failed to flush log buffer:", e.message);
  }
}

/**
 * Логирование критичного действия (синхронно)
 */
async function logCriticalAction(userId, action, details, result) {
  await writeLogSync("CRITICAL", userId, action, details, result);
}

/**
 * Логирование админ-действия (через буфер)
 */
function logAdminAction(userId, action, details, result) {
  addToBuffer("ADMIN", userId, action, details, result);
}

/**
 * Логирование ошибки (синхронно для критичных ошибок)
 */
async function logError(userId, action, error, details = {}) {
  const errorDetails = {
    ...details,
    error: error.message || String(error),
    stack: error.stack,
  };
  await writeLogSync("ERROR", userId, action, errorDetails, "failed");
}

/**
 * Логирование обычного действия (через буфер)
 */
function logAction(userId, action, details, result) {
  addToBuffer("INFO", userId, action, details, result);
}

// Принудительный сброс буфера при завершении процесса
process.on("SIGINT", async () => {
  await flushBuffer();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await flushBuffer();
  process.exit(0);
});

module.exports = {
  logCriticalAction,
  logAdminAction,
  logError,
  logAction,
  flushBuffer, // Для тестирования или принудительного сброса
};
