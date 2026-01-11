// Утилиты безопасности: валидация и санитизация входных данных
// Включает LRU кэш для оптимизации производительности

// Простая реализация LRU кэша для кэширования результатов валидации
class LRUCache {
  constructor(maxSize = 100) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  get(key) {
    if (!this.cache.has(key)) {
      return null;
    }
    // Перемещаем в конец (самый недавно использованный)
    const value = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }

  set(key, value) {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      // Удаляем самый старый элемент (первый в Map)
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }

  clear() {
    this.cache.clear();
  }
}

// LRU кэш для результатов валидации (100 записей)
const validationCache = new LRUCache(100);

/**
 * Валидация Telegram ID
 * @param {string|number} id - Telegram ID для валидации
 * @returns {boolean} - true если валиден
 */
function validateTelegramId(id) {
  if (id === null || id === undefined) return false;

  const cacheKey = `tgid:${id}`;
  const cached = validationCache.get(cacheKey);
  if (cached !== null) {
    return cached;
  }

  const idStr = String(id).trim();
  // Telegram ID: только цифры, длина от 1 до 15 символов
  const isValid = /^\d{1,15}$/.test(idStr);

  validationCache.set(cacheKey, isValid);
  return isValid;
}

/**
 * Валидация телефонного номера
 * @param {string} phone - Номер телефона
 * @returns {boolean} - true если валиден
 */
function validatePhone(phone) {
  if (!phone || typeof phone !== "string") return false;

  const cacheKey = `phone:${phone}`;
  const cached = validationCache.get(cacheKey);
  if (cached !== null) {
    return cached;
  }

  const phoneStr = phone.trim();
  // Телефон: начинается с +, затем цифры, длина 10-15 символов после +
  const isValid = /^\+?\d{10,15}$/.test(phoneStr);

  validationCache.set(cacheKey, isValid);
  return isValid;
}

/**
 * Валидация имени
 * @param {string} name - Имя для валидации
 * @param {number} minLength - Минимальная длина (по умолчанию 1)
 * @param {number} maxLength - Максимальная длина (по умолчанию 50)
 * @returns {boolean} - true если валидно
 */
function validateName(name, minLength = 1, maxLength = 50) {
  if (!name || typeof name !== "string") return false;

  // Ранний выход: проверка длины перед regex
  const trimmed = name.trim();
  if (trimmed.length < minLength || trimmed.length > maxLength) {
    return false;
  }

  const cacheKey = `name:${trimmed}:${minLength}:${maxLength}`;
  const cached = validationCache.get(cacheKey);
  if (cached !== null) {
    return cached;
  }

  // Имя: буквы (включая кириллицу и латиницу), пробелы, дефисы, апострофы
  // Защита от XSS: запрещаем HTML-теги и спецсимволы
  const isValid = /^[\p{L}\s\-']+$/u.test(trimmed);

  validationCache.set(cacheKey, isValid);
  return isValid;
}

/**
 * Санитизация текста для защиты от XSS
 * @param {string} text - Текст для санитизации
 * @param {number} maxLength - Максимальная длина (по умолчанию 500)
 * @returns {string} - Санитизированный текст
 */
function sanitizeText(text, maxLength = 500) {
  if (!text || typeof text !== "string") return "";

  // Ограничение длины (ранний выход)
  let sanitized = text.trim();
  if (sanitized.length > maxLength) {
    sanitized = sanitized.substring(0, maxLength);
  }

  // Экранирование HTML-спецсимволов для защиты от XSS
  sanitized = sanitized
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#x27;")
    .replace(/\//g, "&#x2F;");

  return sanitized;
}

/**
 * Валидация ID записи
 * @param {string} id - ID записи для валидации
 * @returns {boolean} - true если валиден
 */
function validateAppointmentId(id) {
  if (!id || typeof id !== "string") return false;

  const cacheKey = `appid:${id}`;
  const cached = validationCache.get(cacheKey);
  if (cached !== null) {
    return cached;
  }

  const idStr = id.trim();
  // ID записи: буквы и цифры, длина от 1 до 50 символов
  // Формат обычно: "A123456" или подобный
  const isValid = /^[A-Za-z0-9]{1,50}$/.test(idStr);

  validationCache.set(cacheKey, isValid);
  return isValid;
}

/**
 * Валидация размера данных (для защиты от DoS)
 * @param {any} data - Данные для проверки
 * @param {number} maxSizeKB - Максимальный размер в KB (по умолчанию 10)
 * @returns {boolean} - true если размер допустим
 */
function validateDataSize(data, maxSizeKB = 10) {
  try {
    const jsonStr = JSON.stringify(data);
    const sizeBytes = Buffer.byteLength(jsonStr, "utf8");
    const sizeKB = sizeBytes / 1024;
    return sizeKB <= maxSizeKB;
  } catch (e) {
    return false;
  }
}

/**
 * Очистка кэша валидации (для тестирования или при необходимости)
 */
function clearValidationCache() {
  validationCache.clear();
}

module.exports = {
  validateTelegramId,
  validatePhone,
  validateName,
  sanitizeText,
  validateAppointmentId,
  validateDataSize,
  clearValidationCache,
};
