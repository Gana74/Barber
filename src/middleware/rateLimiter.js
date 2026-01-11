// Rate Limiting Middleware для Telegraf
// Оптимизация памяти: TTL cleanup, LRU eviction, ограничение размера Map

const MAX_MAP_SIZE = 5000;
const CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 минут
const INACTIVE_THRESHOLD = 10 * 60 * 1000; // 10 минут

// LRU структура для отслеживания использования
class LRUEntry {
  constructor(userId, timestamp) {
    this.userId = userId;
    this.timestamp = timestamp;
    this.lastAccess = Date.now();
  }
}

// Map для хранения запросов пользователей
const userRequests = new Map();
let cleanupTimer = null;

// Лимиты по умолчанию (запросов в минуту)
const DEFAULT_LIMITS = {
  general: 30, // Общие команды
  admin: 10, // Админ-команды
  scene: 5, // Сцены (booking и т.д.)
};

/**
 * Очистка неактивных пользователей и старых записей
 */
function cleanup() {
  const now = Date.now();
  const toDelete = [];

  // Находим записи для удаления
  for (const [userId, entry] of userRequests.entries()) {
    // Удаляем если неактивен более 10 минут
    if (now - entry.lastAccess > INACTIVE_THRESHOLD) {
      toDelete.push(userId);
    }
  }

  // Удаляем неактивные записи
  for (const userId of toDelete) {
    userRequests.delete(userId);
  }

  // Если Map все еще слишком большой, удаляем самые старые (LRU)
  if (userRequests.size > MAX_MAP_SIZE) {
    const entries = Array.from(userRequests.entries())
      .map(([userId, entry]) => ({ userId, lastAccess: entry.lastAccess }))
      .sort((a, b) => a.lastAccess - b.lastAccess);

    const toRemove = entries.slice(0, userRequests.size - MAX_MAP_SIZE);
    for (const entry of toRemove) {
      userRequests.delete(entry.userId);
    }
  }
}

/**
 * Запуск периодической очистки
 */
function startCleanupTimer() {
  if (cleanupTimer) {
    return;
  }
  cleanupTimer = setInterval(cleanup, CLEANUP_INTERVAL);
}

/**
 * Остановка таймера очистки
 */
function stopCleanupTimer() {
  if (cleanupTimer) {
    clearInterval(cleanupTimer);
    cleanupTimer = null;
  }
}

/**
 * Проверка rate limit для пользователя
 * @param {string|number} userId - ID пользователя
 * @param {string} type - Тип запроса: 'general', 'admin', 'scene'
 * @param {number} limit - Лимит запросов в минуту (опционально)
 * @returns {boolean} - true если лимит не превышен
 */
function checkRateLimit(userId, type = "general", limit = null) {
  const userIdStr = String(userId);
  const now = Date.now();
  const windowMs = 60 * 1000; // 1 минута
  const actualLimit = limit || DEFAULT_LIMITS[type] || DEFAULT_LIMITS.general;

  // Получаем или создаем запись для пользователя
  let entry = userRequests.get(userIdStr);
  if (!entry) {
    entry = {
      requests: [],
      lastAccess: now,
    };
    userRequests.set(userIdStr, entry);
  }

  entry.lastAccess = now;

  // Удаляем запросы старше 1 минуты
  entry.requests = entry.requests.filter(
    (timestamp) => now - timestamp < windowMs
  );

  // Проверяем лимит
  if (entry.requests.length >= actualLimit) {
    return false; // Лимит превышен
  }

  // Добавляем текущий запрос
  entry.requests.push(now);

  // Если Map слишком большой, запускаем очистку
  if (userRequests.size >= MAX_MAP_SIZE) {
    cleanup();
  }

  return true; // Лимит не превышен
}

/**
 * Создание middleware для Telegraf
 * @param {object} options - Опции rate limiting
 * @param {number} options.generalLimit - Лимит для общих команд
 * @param {number} options.adminLimit - Лимит для админ-команд
 * @param {number} options.sceneLimit - Лимит для сцен
 * @returns {Function} - Telegraf middleware
 */
function createRateLimiter(options = {}) {
  // Обновляем лимиты если переданы
  if (options.generalLimit) {
    DEFAULT_LIMITS.general = options.generalLimit;
  }
  if (options.adminLimit) {
    DEFAULT_LIMITS.admin = options.adminLimit;
  }
  if (options.sceneLimit) {
    DEFAULT_LIMITS.scene = options.sceneLimit;
  }

  // Запускаем таймер очистки
  startCleanupTimer();

  return async (ctx, next) => {
    // Пропускаем если нет пользователя
    if (!ctx.from || !ctx.from.id) {
      return next();
    }

    const userId = ctx.from.id;
    let type = "general";

    // Определяем тип запроса
    if (ctx.session && ctx.session.mode === "admin") {
      type = "admin";
    } else if (ctx.scene && ctx.scene.current) {
      type = "scene";
    }

    // Проверяем rate limit
    const allowed = checkRateLimit(userId, type);

    if (!allowed) {
      // Лимит превышен
      try {
        await ctx.reply(
          "Слишком много запросов. Пожалуйста, подождите немного."
        );
      } catch (e) {
        // Игнорируем ошибки отправки сообщения
      }
      return; // Не вызываем next()
    }

    // Лимит не превышен, продолжаем
    return next();
  };
}

// Очистка при завершении процесса
process.on("SIGINT", () => {
  stopCleanupTimer();
  userRequests.clear();
});

process.on("SIGTERM", () => {
  stopCleanupTimer();
  userRequests.clear();
});

module.exports = {
  createRateLimiter,
  checkRateLimit, // Для прямого использования если нужно
  cleanup, // Для тестирования
};
