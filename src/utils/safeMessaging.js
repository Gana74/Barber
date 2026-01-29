// Утилиты для безопасной отправки сообщений с обработкой ошибок
// Предотвращает крах бота при отправке сообщений заблокировавшим пользователям

/**
 * Безопасная отправка текстового сообщения через bot.telegram.sendMessage
 * @param {Object} bot - Экземпляр Telegraf бота
 * @param {string|number} chatId - ID чата для отправки
 * @param {string} text - Текст сообщения
 * @param {Object} options - Дополнительные опции для sendMessage
 * @returns {Promise<Object|null>} Результат отправки или null при ошибке
 */
async function safeSendMessage(bot, chatId, text, options = {}) {
  // Поддерживаем как bot (объект с полем telegram), так и ctx.telegram (экземпляр Telegram)
  let telegram = null;
  if (bot && bot.telegram) {
    telegram = bot.telegram;
  } else if (bot && typeof bot.sendMessage === 'function') {
    // Это уже экземпляр Telegram API (ctx.telegram)
    telegram = bot;
  } else {
    console.error("[safeMessaging] Invalid bot instance");
    return null;
  }

  if (!chatId) {
    console.error("[safeMessaging] Invalid chatId");
    return null;
  }

  try {
    const result = await telegram.sendMessage(chatId, text, options);
    return result;
  } catch (err) {
    const errorCode = err.response?.error_code;
    const errorDescription = err.response?.description || err.message;

    // 403 Forbidden - пользователь заблокировал бота
    if (errorCode === 403) {
      console.warn(
        `[safeMessaging] User ${chatId} blocked the bot. Message not sent.`,
      );
      return null;
    }

    // 400 Bad Request - неверный запрос (например, неверный chat_id)
    if (errorCode === 400) {
      console.warn(
        `[safeMessaging] Bad request for chat ${chatId}: ${errorDescription}`,
      );
      return null;
    }

    // 429 Too Many Requests - превышен лимит запросов
    if (errorCode === 429) {
      console.warn(
        `[safeMessaging] Rate limit exceeded for chat ${chatId}. Retry after: ${err.response?.parameters?.retry_after || 'unknown'}s`,
      );
      return null;
    }

    // Другие ошибки - логируем с деталями
    console.error(
      `[safeMessaging] Error sending message to ${chatId}:`,
      errorDescription,
      `(code: ${errorCode || 'unknown'})`,
    );
    return null;
  }
}

/**
 * Безопасная отправка фото через bot.telegram.sendPhoto
 * @param {Object} bot - Экземпляр Telegraf бота
 * @param {string|number} chatId - ID чата для отправки
 * @param {string} photo - File ID или URL фото
 * @param {Object} options - Дополнительные опции для sendPhoto
 * @returns {Promise<Object|null>} Результат отправки или null при ошибке
 */
async function safeSendPhoto(bot, chatId, photo, options = {}) {
  // Поддерживаем как bot (объект с полем telegram), так и ctx.telegram (экземпляр Telegram)
  let telegram = null;
  if (bot && bot.telegram) {
    telegram = bot.telegram;
  } else if (bot && typeof bot.sendPhoto === 'function') {
    // Это уже экземпляр Telegram API (ctx.telegram)
    telegram = bot;
  } else {
    console.error("[safeMessaging] Invalid bot instance");
    return null;
  }

  if (!chatId) {
    console.error("[safeMessaging] Invalid chatId");
    return null;
  }

  try {
    const result = await telegram.sendPhoto(chatId, photo, options);
    return result;
  } catch (err) {
    const errorCode = err.response?.error_code;
    const errorDescription = err.response?.description || err.message;

    // 403 Forbidden - пользователь заблокировал бота
    if (errorCode === 403) {
      console.warn(
        `[safeMessaging] User ${chatId} blocked the bot. Photo not sent.`,
      );
      return null;
    }

    // 400 Bad Request
    if (errorCode === 400) {
      console.warn(
        `[safeMessaging] Bad request for chat ${chatId}: ${errorDescription}`,
      );
      return null;
    }

    // 429 Too Many Requests
    if (errorCode === 429) {
      console.warn(
        `[safeMessaging] Rate limit exceeded for chat ${chatId}. Retry after: ${err.response?.parameters?.retry_after || 'unknown'}s`,
      );
      return null;
    }

    // Другие ошибки
    console.error(
      `[safeMessaging] Error sending photo to ${chatId}:`,
      errorDescription,
      `(code: ${errorCode || 'unknown'})`,
    );
    return null;
  }
}

/**
 * Безопасный ответ через ctx.reply
 * @param {Object} ctx - Контекст Telegraf
 * @param {string} text - Текст сообщения
 * @param {Object} extra - Дополнительные опции (keyboard, parse_mode и т.д.)
 * @returns {Promise<Object|null>} Результат отправки или null при ошибке
 */
async function safeReply(ctx, text, extra = {}) {
  if (!ctx) {
    console.error("[safeMessaging] Invalid ctx");
    return null;
  }

  if (!ctx.reply) {
    console.error("[safeMessaging] ctx.reply is not available");
    return null;
  }

  try {
    const result = await ctx.reply(text, extra);
    return result;
  } catch (err) {
    const errorCode = err.response?.error_code;
    const errorDescription = err.response?.description || err.message;
    const userId = ctx.from?.id || "unknown";

    // 403 Forbidden - пользователь заблокировал бота
    if (errorCode === 403) {
      console.warn(
        `[safeMessaging] User ${userId} blocked the bot. Reply not sent.`,
      );
      return null;
    }

    // 400 Bad Request
    if (errorCode === 400) {
      console.warn(
        `[safeMessaging] Bad request for user ${userId}: ${errorDescription}`,
      );
      return null;
    }

    // 429 Too Many Requests
    if (errorCode === 429) {
      console.warn(
        `[safeMessaging] Rate limit exceeded for user ${userId}. Retry after: ${err.response?.parameters?.retry_after || 'unknown'}s`,
      );
      return null;
    }

    // Другие ошибки
    console.error(
      `[safeMessaging] Error replying to user ${userId}:`,
      errorDescription,
      `(code: ${errorCode || 'unknown'})`,
    );
    return null;
  }
}

/**
 * Проверяет, является ли ошибка ошибкой блокировки бота пользователем
 * @param {Error} err - Объект ошибки
 * @returns {boolean} true если пользователь заблокировал бота
 */
function isBlockedError(err) {
  return err.response?.error_code === 403;
}

/**
 * Проверяет, является ли ошибка ошибкой превышения лимита запросов
 * @param {Error} err - Объект ошибки
 * @returns {boolean} true если превышен лимит запросов
 */
function isRateLimitError(err) {
  return err.response?.error_code === 429;
}

module.exports = {
  safeSendMessage,
  safeSendPhoto,
  safeReply,
  isBlockedError,
  isRateLimitError,
};
