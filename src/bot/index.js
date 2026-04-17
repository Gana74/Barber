// Инициализация Telegraf, сессий и сцен

const { Telegraf, Scenes, Markup } = require("telegraf");
const LocalSession = require("telegraf-session-local");
const fs = require("fs");
const path = require("path");
const { createBookingService, getServiceList } = require("../services/booking");
const adminService = require("../services/admin");
const { createBookingScene } = require("./scenes/bookingScene");
const { formatDate } = require("../utils/formatDate");
const servicesService = require("../services/services");
const { createRateLimiter } = require("../middleware/rateLimiter");
const {
  validateTelegramId,
  validateAppointmentId,
  sanitizeText,
  validateDataSize,
} = require("../utils/security");
const {
  logCriticalAction,
  logAdminAction,
  logError,
  logAction,
} = require("../utils/logger");
const { safeSendMessage } = require("../utils/safeMessaging");
const dayjs = require("dayjs");
const timezonePlugin = require("dayjs/plugin/timezone");
const utc = require("dayjs/plugin/utc");
const revenueStats = require("../services/revenueStats");

dayjs.extend(timezonePlugin);
dayjs.extend(utc);

/**
 * Очистка файла sessions.json:
 * - удаляет сессии с lastActivity/updatedAt старше inactiveDays
 * - сортирует по последней активности и оставляет только maxSessions самых свежих
 */
function cleanupSessionsFile({ maxSessions = 150, inactiveDays = 30 } = {}) {
  try {
    const sessionsPath = path.resolve(process.cwd(), "sessions.json");
    if (!fs.existsSync(sessionsPath)) {
      return;
    }

    const raw = fs.readFileSync(sessionsPath, { encoding: "utf8" });
    let parsed = null;
    try {
      parsed = JSON.parse(raw || "{}");
    } catch (e) {
      console.warn("Failed to parse sessions.json for cleanup:", e.message);
      return;
    }

    if (!parsed || !Array.isArray(parsed.sessions)) {
      return;
    }

    const now = Date.now();
    const inactiveMs = inactiveDays * 24 * 60 * 60 * 1000;
    const cutoff = now - inactiveMs;

    // Фильтруем сессии: удаляем сессии без id и с последней активностью старше cutoff
    let filteredSessions = parsed.sessions.filter((s) => {
      if (!s || !s.id) return false;
      const lastActivity = s.lastActivity || s.updatedAt || s.createdAt || now;
      return lastActivity > cutoff;
    });

    // Сортируем по последней активности (новые первыми)
    filteredSessions.sort((a, b) => {
      const aTime = a.lastActivity || a.updatedAt || a.createdAt || 0;
      const bTime = b.lastActivity || b.updatedAt || b.createdAt || 0;
      return bTime - aTime;
    });

    if (filteredSessions.length > maxSessions) {
      filteredSessions = filteredSessions.slice(0, maxSessions);
    }

    if (filteredSessions.length !== parsed.sessions.length) {
      parsed.sessions = filteredSessions;
      try {
        fs.writeFileSync(sessionsPath, JSON.stringify(parsed, null, 2), {
          encoding: "utf8",
        });
        console.log(
          `Cleaned up sessions.json: kept ${filteredSessions.length} sessions`,
        );
      } catch (e) {
        console.warn("Failed to write cleaned sessions.json:", e.message);
      }
    }
  } catch (err) {
    console.warn("Error while cleaning sessions.json:", err.message);
  }
}

function createBot({ config, sheetsService, calendarService }) {
  const bot = new Telegraf(config.botToken);

  // Предотвращаем застревание пользователей в сценах при рестарте бота.
  // Если в файле сессий есть активные сцены — удаляем поле __scenes,
  // чтобы при следующем апдейте бот не продолжал вызывать шаги визарда.
  try {
    const sessionsPath = path.resolve(process.cwd(), "sessions.json");
    if (fs.existsSync(sessionsPath)) {
      const raw = fs.readFileSync(sessionsPath, { encoding: "utf8" });
      let parsed = null;
      try {
        parsed = JSON.parse(raw || "{}");
      } catch (e) {
        parsed = null;
      }

      if (parsed && Array.isArray(parsed.sessions)) {
        let changed = false;
        parsed.sessions = parsed.sessions.map((s) => {
          if (s && s.data && s.data.__scenes) {
            const copy = Object.assign({}, s);
            const dataCopy = Object.assign({}, copy.data);
            delete dataCopy.__scenes;
            copy.data = dataCopy;
            changed = true;
            return copy;
          }
          return s;
        });

        if (changed) {
          try {
            fs.writeFileSync(sessionsPath, JSON.stringify(parsed, null, 2), {
              encoding: "utf8",
            });
            console.log("Cleaned up stale scenes in sessions.json");
          } catch (e) {
            console.warn("Failed to write cleaned sessions.json:", e.message);
          }
        }
      }
    }
  } catch (err) {
    console.warn("Error while sanitizing sessions.json:", err.message);
  }

  // Ограничение количества сессий и удаление неактивных (30+ дней)
  cleanupSessionsFile({ maxSessions: 150, inactiveDays: 30 });

  const localSession = new LocalSession({
    database: "sessions.json",
  });

  bot.use(localSession.middleware());

  // Middleware для отметки последней активности пользователя в сессии
  bot.use(async (ctx, next) => {
    if (ctx && ctx.session) {
      // Сохраняем время в миллисекундах с начала эпохи
      ctx.session.lastActivity = Date.now();
    }
    return next();
  });

  const bookingService = createBookingService({
    sheetsService,
    config,
    calendarService,
  });

  const stage = new Scenes.Stage([
    createBookingScene({ bookingService, sheetsService, config }),
  ]);

  bot.use(stage.middleware());

  // Rate limiting middleware - подключаем перед всеми обработчиками
  const rateLimiter = createRateLimiter({
    generalLimit: 30, // Общие команды: 30/минуту
    adminLimit: 10, // Админ-команды: 10/минуту
    sceneLimit: 5, // Сцены: 5/минуту
  });
  bot.use(rateLimiter);

  // Middleware для защиты сессий: проверка размера и валидация структуры
  bot.use(async (ctx, next) => {
    if (ctx.session) {
      // Проверяем размер сессии (максимум 10KB)
      if (!validateDataSize(ctx.session, 10)) {
        // Сессия слишком большая, очищаем её
        ctx.session = {};
        console.warn(
          `Session too large for user ${ctx.from?.id}, cleared session`,
        );
      }
    }
    return next();
  });

  // Настройка меню команд (кнопка меню в левой части поля ввода)
  bot.telegram
    .setMyCommands([
      { command: "start", description: "Начать общение с начала" },
      { command: "services", description: "Посмотреть список услуг" },
      { command: "user", description: "Пользовательское меню" },
      { command: "admin", description: "Админ-меню" },
    ])
    .catch((err) => {
      console.warn("Failed to set bot commands menu:", err.message);
    });

  function isAdmin(ctx) {
    try {
      const mgr = String(config.managerChatId || "");
      const fromId = String(ctx.from && ctx.from.id ? ctx.from.id : "");
      return mgr && mgr === fromId;
    } catch (e) {
      return false;
    }
  }

  bot.start(async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    ctx.session = {};

    const name = ctx.from.first_name || "друг";

    await ctx.reply(
      `Привет, ${name}! Я бот мастера по услугам красоты. Здесь можно записаться на стрижку.`,
    );

    // Премиальная "визитка": показываем 5-6 лучших работ из портфолио по file_id
    try {
      const ids = (await sheetsService.getPortfolioFileIds()) || [];
      const best = ids.slice(0, 6);
      if (best.length) {
        const chatId = ctx.chat?.id || ctx.from?.id;
        const media = best.map((fileId) => ({
          type: "photo",
          media: fileId,
        }));
        await ctx.telegram.sendMediaGroup(chatId, media);
      }
    } catch (e) {
      console.warn("Failed to send portfolio media group:", e.message || e);
    }

    await ctx.reply(
      "👇 Выберите действие с помощью кнопок ниже:",
      Markup.keyboard([
        ["Записаться 💇‍♂️"],
        ["Мои записи"],
        ["Как добраться 🗺️"],
      ]).resize(),
    );
  });

  bot.hears("Записаться 💇‍♂️", async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    ctx.session = ctx.session || {};
    const banned = await adminService.isBanned(ctx.from.id);
    if (banned) {
      await ctx.reply("Извините, вы заблокированы и не можете записываться.");
      return;
    }
    await ctx.scene.enter("booking");
  });

  bot.hears("Мои записи", async (ctx) => {
    const timezone = await sheetsService.getTimezone();
    const list = await sheetsService.getFutureAppointmentsForTelegram(
      ctx.from.id,
      timezone,
    );

    if (!list.length) {
      await ctx.reply("У тебя пока нет будущих записей.");
      return;
    }

    const lines = list.map(
      (app, idx) =>
        `${idx + 1}. ${app.service} — ${formatDate(app.date)} ${app.timeStart}`,
    );

    const keyboard = list.map((app) => [
      Markup.button.callback(
        `Отменить ${formatDate(app.date)} ${app.timeStart}`,
        `cancel_app:${app.id}`,
      ),
    ]);

    await ctx.reply(
      `Будущие записи:\n\n${lines.join("\n")}`,
      Markup.inlineKeyboard(keyboard),
    );
  });

  bot.hears(["Как добраться", "Как добраться 🗺️"], async (ctx) => {
    // Пользовательская кнопка из главного меню
    // Если пользователь сейчас в сцене, просто выходим, чтобы не ломать визард.
    try {
      await ctx.scene.leave();
    } catch (e) {}

    const yandexLink = await sheetsService.getLocationLink();
    const gisLink = await sheetsService.getLocationLink2gis();
    if (!yandexLink && !gisLink) {
      await ctx.reply("Локация не настроена. Обратитесь к администратору.");
      return;
    }

    const buttons = [];
    if (yandexLink) {
      buttons.push(Markup.button.url("Открыть в Яндекс.Картах", yandexLink));
    }
    if (gisLink) {
      buttons.push(Markup.button.url("Открыть в 2ГИС", gisLink));
    }

    await ctx.reply(
      "Как добраться:",
      Markup.inlineKeyboard(buttons.map((button) => [button])),
    );
  });

  bot.command("book", async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    const banned = await adminService.isBanned(ctx.from.id);
    if (banned) {
      await ctx.reply("Извините, вы заблокированы и не можете записываться.");
      return;
    }
    await ctx.scene.enter("booking");
  });

  bot.command("services", async (ctx) => {
    const services = getServiceList();
    const text = services
      .map((s) => {
        const priceText = s.price !== null ? ` — ${s.price} ₽` : "";
        return `- ${s.name}${priceText} (${s.durationMin} мин)`;
      })
      .join("\n");
    await ctx.reply(`Список услуг:\n${text}`);
  });

  bot.command("cancel", async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    await ctx.reply(
      "Отменено. Для новой записи используй /book",
      Markup.removeKeyboard(),
    );
  });

  bot.action(/cancel_app:(.+)/, async (ctx) => {
    const id = ctx.match[1];

    // Валидация ID записи
    if (!validateAppointmentId(id)) {
      await ctx.answerCbQuery("Неверный формат ID записи.");
      return;
    }

    await ctx.answerCbQuery("Отменяем запись...");

    const appointment = await sheetsService.getAppointmentById(id);
    if (!appointment || appointment.status !== bookingService.STATUSES.ACTIVE) {
      await ctx.reply(
        "Не удалось отменить запись: она не найдена или уже отменена.",
      );
      return;
    }

    if (String(appointment.telegramId) !== String(ctx.from.id)) {
      await ctx.reply("Эта запись принадлежит другому пользователю.");
      return;
    }

    const cancelledAtUtc = new Date().toISOString();
    const ok = await sheetsService.updateAppointmentStatus(
      id,
      bookingService.STATUSES.CANCELLED,
      { cancelledAtUtc },
    );

    if (!ok) {
      await ctx.reply(
        "Не удалось отменить запись: она не найдена или уже отменена.",
      );
      return;
    }

    // Логирование отмены записи пользователем
    logAction(
      ctx.from.id,
      "appointment_cancelled",
      {
        appointmentId: id,
        date: appointment.date,
        time: appointment.timeStart,
      },
      "success",
    );

    await ctx.reply(
      `Запись на ${formatDate(appointment.date)} ${
        appointment.timeStart
      } отменена. Спасибо, что предупредил(а)!`,
    );

    // Попытка удалить событие в календаре
    try {
      if (calendarService && calendarService.deleteEventForAppointmentId) {
        await calendarService.deleteEventForAppointmentId(id);
      }
    } catch (e) {
      console.warn(
        "Calendar delete failed for appointment (user cancel):",
        e.message || e,
      );
    }

    if (config.managerChatId) {
      // Безопасная отправка уведомления менеджеру с обработкой ошибок
      await safeSendMessage(
        ctx.telegram,
        config.managerChatId,
        `Клиент отменил запись:\nУслуга: ${
          appointment.service
        }\nДата: ${formatDate(appointment.date)}\nВремя: ${
          appointment.timeStart
        }–${appointment.timeEnd}\nКлиент: ${appointment.clientName}\nТелефон: ${
          appointment.phone
        }\nКод отмены: ${appointment.cancelCode}`,
      );
    }
  });

  // --- Admin menu (manager only) ---
  // reply-style keyboard for admin (visual like user)
  const adminKeyboard = Markup.keyboard([
    ["Просмотр записей", "Статистика"],
    ["Отменить запись (по коду)"],
    ["Массовая рассылка", "📊 Статус рассылки"],
    ["📊 Финансовая статистика"],
    ["⚙️ Настройки"],
    ["Вернуться в пользовательский режим"],
  ]).resize();

  const settingsKeyboard = Markup.keyboard([
    ["Настройки расписания"],
    ["Управление услугами"],
    ["Редактировать напоминание 28 дней"],
    ["Редактировать ссылку на чаевые"],
    ["Изменить контакты"],
    ["Забанить пользователя", "Разбанить пользователя"],
    ["Загрузить фото", "Удалить фото"],
    ["Сохранить ссылку на 2ГИС"],
    ["Сохранить локацию"],
    ["Назад в админ-меню"],
  ]).resize();

  const servicesKeyboard = Markup.keyboard([
    ["Добавить услугу", "Изменить услугу"],
    ["Удалить услугу", "Список услуг"],
    ["Назад в админ-меню"],
  ]).resize();

  bot.command("admin", async (ctx) => {
    if (!isAdmin(ctx)) return;
    ctx.session = ctx.session || {};
    ctx.session.mode = "admin";
    logAdminAction(ctx.from.id, "admin_mode_enabled", {}, "success");
    await ctx.reply(
      "Включён режим администратора. Выберите действие:",
      adminKeyboard,
    );
  });

  bot.command("user", async (ctx) => {
    ctx.session = ctx.session || {};
    ctx.session.mode = "user";
    await ctx.reply(
      "Режим пользователя.\n\n👇 Выберите действие с помощью кнопок ниже:",
      Markup.keyboard([
        ["Записаться 💇‍♂️"],
        ["Мои записи"],
        ["Как добраться 🗺️"],
      ]).resize(),
    );
  });

  async function handleAdminAction(ctx, action) {
    if (!isAdmin(ctx)) return;
    if (!action) return;

    if (action === "all_bookings") {
      const all = await sheetsService.getAllActiveAppointments();
      if (!all.length) {
        await ctx.reply("Нет активных записей.");
        return;
      }
      const lines = all
        .slice(0, 50)
        .map(
          (a) =>
            `Код отмены: ${a.cancelCode || "N/A"} — ${a.service} ${formatDate(
              a.date,
            )} ${a.timeStart}-${a.timeEnd} — ${a.clientName} (${a.phone})`,
        );
      await ctx.reply(
        `Активные записи (показано ${lines.length} из ${all.length}):\n` +
          lines.join("\n"),
      );
      return;
    }

    if (action === "stats") {
      const all = await sheetsService.getAllActiveAppointments();
      const clients = await sheetsService.getAllClients();
      const upcoming = all.length;
      const uniqueClients = new Set(
        clients.map((c) => String(c.telegramId)).filter(Boolean),
      ).size;
      await ctx.reply(
        `Статистика:\nАктивных записей: ${upcoming}\nКлиентов в базе: ${uniqueClients}`,
      );
      return;
    }

    const inputActions = new Set([
      "cancel_booking_by_code",
      "ban",
      "unban",
      "broadcast",
      "edit_28day_reminder",
      "edit_tips_link",
      "edit_contacts",
    ]);

    if (inputActions.has(action)) {
      ctx.session.adminAction = { type: action };
      await ctx.reply(
        action === "broadcast"
          ? "Отправьте текст для рассылки или пришлите фото с подписью. Для отмены напишите /admin_cancel"
          : action === "cancel_booking_by_code"
            ? "Отправьте код отмены записи (например: A3K9X2). Для отмены напишите /admin_cancel"
            : action === "ban"
              ? "Отправьте Telegram ID или @username пользователя для бана. Для отмены напишите /admin_cancel"
              : action === "unban"
                ? "Отправьте Telegram ID пользователя для разбанивания. Для отмены напишите /admin_cancel"
                : action === "edit_28day_reminder"
                  ? "Отправьте новый текст для напоминания через 28 дней. Используйте {clientName} для подстановки имени клиента. Для отмены напишите /admin_cancel"
                  : action === "edit_tips_link"
                    ? "Отправьте ссылку на чаевые (http://, https://, t.me/) или номер телефона. Для отмены напишите /admin_cancel"
                    : action === "edit_contacts"
                      ? "Отправьте контакты в формате:\nТелефон (первая строка)\nАдрес (вторая строка)\n\nДля отмены напишите /admin_cancel"
                      : "Неизвестное действие",
      );
      return;
    }
  }

  // keep callback handlers for broadcast confirm/cancel
  bot.action(/admin:(.+)/, async (ctx, next) => {
    if (!isAdmin(ctx)) return next();
    const action = ctx.match[1];
    await ctx.answerCbQuery();
    await handleAdminAction(ctx, action);
    return next();
  });

  // map reply-keyboard presses to admin actions
  bot.hears("Просмотр записей", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "all_bookings");
    }
  });

  bot.hears("Статистика", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "stats");
    }
  });

  bot.hears("Отменить запись (по коду)", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "cancel_booking_by_code");
    }
  });

  bot.hears("Забанить пользователя", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "ban");
    }
  });

  bot.hears("Разбанить пользователя", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "unban");
    }
  });

  bot.hears("Массовая рассылка", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "broadcast");
    }
  });

  bot.hears("📊 Статус рассылки", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode !== "admin") return;

    try {
      // Получаем клиентов для рассылки (доступные сегодня)
      const clientsForBroadcast = sheetsService.getClientsForBroadcast
        ? await sheetsService.getClientsForBroadcast()
        : [];

      // Получаем всех клиентов
      const allClients = await sheetsService.getAllClients();
      const allClientsWithTelegram = allClients.filter(
        (c) => c && c.telegramId,
      );

      // Вычисляем следующий понедельник для сброса меток
      const timezone = await sheetsService.getTimezone();
      const nowTz = dayjs().tz(timezone);
      let nextMonday = nowTz.day(1); // Понедельник текущей недели
      if (nextMonday.isBefore(nowTz) || nextMonday.isSame(nowTz, "day")) {
        // Если понедельник уже прошел или сегодня понедельник после 00:00
        nextMonday = nextMonday.add(7, "day");
      }
      // Устанавливаем время на 00:00
      nextMonday = nextMonday.hour(0).minute(0).second(0).millisecond(0);

      const availableToday = clientsForBroadcast.length;
      const totalClients = allClientsWithTelegram.length;
      const waitingCount = Math.max(0, totalClients - availableToday);

      // Форматируем дату следующего сброса
      const nextResetDate = nextMonday.format("DD.MM.YYYY HH:mm");

      const MAX_RECIPIENTS = 250;
      const canSendToday = Math.min(availableToday, MAX_RECIPIENTS);
      const remainingToday = Math.max(0, availableToday - MAX_RECIPIENTS);

      let message = `📊 Статус рассылки\n\n`;
      message += `📤 Доступно сегодня: ${canSendToday} из ${MAX_RECIPIENTS}\n`;
      if (remainingToday > 0) {
        message += `⏳ Ожидают (после лимита): ${remainingToday}\n`;
      }
      message += `👥 Всего клиентов: ${totalClients}\n`;
      if (waitingCount > 0) {
        message += `⏱ Отправленных за последние 24 часа: ${waitingCount}\n`;
      }
      message += `🔄 Следующий сброс меток: ${nextResetDate} (${timezone})\n`;

      await ctx.reply(message);
    } catch (err) {
      console.error("Ошибка при получении статуса рассылки:", err);
      await ctx.reply(`Ошибка при получении статуса рассылки: ${err.message}`);
    }
  });

  bot.hears("Редактировать напоминание 28 дней", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Показываем текущее сообщение
      try {
        const currentMessage = await sheetsService.get28DayReminderMessage();
        await ctx.reply(
          `Текущий текст напоминания:\n\n${currentMessage}\n\nОтправьте новый текст. Используйте {clientName} для подстановки имени клиента. Для отмены напишите /admin_cancel`,
        );
        await handleAdminAction(ctx, "edit_28day_reminder");
      } catch (err) {
        await ctx.reply(
          `Ошибка при получении текущего сообщения: ${err.message}`,
        );
      }
    }
  });

  bot.hears("Редактировать ссылку на чаевые", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Показываем текущие данные для чаевых
      try {
        const currentTips = await sheetsService.getTipsLink();
        await ctx.reply(
          `Текущие данные для чаевых:\n\n${
            currentTips || "не установлены"
          }\n\nОтправьте новую ссылку (http://, https://, t.me/) или номер телефона. Для отмены напишите /admin_cancel`,
        );
        await handleAdminAction(ctx, "edit_tips_link");
      } catch (err) {
        await ctx.reply(`Ошибка при получении данных: ${err.message}`);
      }
    }
  });

  bot.hears("Изменить контакты", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Показываем текущие контакты
      try {
        const currentPhone = await sheetsService.getBarberPhone();
        const currentAddress = await sheetsService.getBarberAddress();
        await ctx.reply(
          `Текущие контакты:\n\n📞 Телефон: ${
            currentPhone || "не установлен"
          }\n📍 Адрес: ${
            currentAddress || "не установлен"
          }\n\nОтправьте новые контакты в формате:\nТелефон (первая строка)\nАдрес (вторая строка)\n\nДля отмены напишите /admin_cancel`,
        );
        await handleAdminAction(ctx, "edit_contacts");
      } catch (err) {
        await ctx.reply(
          `Ошибка при получении текущих контактов: ${err.message}`,
        );
      }
    }
  });

  bot.hears("Загрузить фото", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      ctx.session.adminAction = { type: "portfolio_upload" };
      await ctx.reply(
        "Пришлите фото для портфолио.\nДля отмены напишите /admin_cancel",
      );
    }
  });

  bot.hears("Удалить фото", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      try {
        const ids = (await sheetsService.getPortfolioFileIds()) || [];
        const best = ids.slice(0, 6);

        if (!best.length) {
          await ctx.reply("Портфолио пустое. Сначала загрузите фото.");
          return;
        }

        await ctx.reply(
          "Текущие фото (самые свежие) для удаления. Сейчас показываю их на экране:",
        );

        for (let i = 0; i < best.length; i += 1) {
          await ctx.replyWithPhoto(best[i], { caption: `Фото №${i + 1}` });
        }

        ctx.session.adminAction = {
          type: "portfolio_delete",
          maxIndex: best.length, // 1..maxIndex в UI
        };

        await ctx.reply(
          `Отправьте номер фото для удаления: 1..${best.length}.\nДля отмены напишите /admin_cancel`,
        );
      } catch (e) {
        await ctx.reply(`Ошибка при получении портфолио: ${e.message || e}`);
      }
    }
  });

  bot.hears("Сохранить ссылку на 2ГИС", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      try {
        const current = await sheetsService.getLocationLink2gis();
        await ctx.reply(
          `Текущая ссылка на 2ГИС:\n${current || "не установлена"}\n\nПришлите новую ссылку на 2ГИС (http:// или https://).\nДля отмены напишите /admin_cancel`,
        );
        ctx.session.adminAction = { type: "save_location_2gis" };
      } catch (e) {
        await ctx.reply(`Ошибка при получении ссылки 2ГИС: ${e.message || e}`);
      }
    }
  });

  bot.hears("Сохранить локацию", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      try {
        const current = await sheetsService.getLocationLink();
        await ctx.reply(
          `Текущая ссылка на локацию:\n${current || "не установлена"}\n\nПришлите новую ссылку на маршрут (http:// или https://).\nДля отмены напишите /admin_cancel`,
        );
        ctx.session.adminAction = { type: "save_location" };
      } catch (e) {
        await ctx.reply(`Ошибка при получении локации: ${e.message || e}`);
      }
    }
  });

  bot.hears("⚙️ Настройки", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Устанавливаем флаг, что пользователь находится в настройках
      ctx.session.fromSettings = true;
      await ctx.reply("Настройки. Выберите действие:", settingsKeyboard);
    }
  });

  bot.hears("Настройки расписания", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    ctx.session.scheduleAction = null;

    const keyboard = Markup.keyboard([
      ["Просмотр расписания на дату"],
      ["Изменить/добавить расписание на дату"],
      ["Удалить расписание на дату"],
      ["Посмотреть всё расписание"],
      ["Шаблоны по дням недели"],
      ["Назад в админ-меню"],
    ]).resize();

    await ctx.reply("Настройки расписания. Выберите действие:", keyboard);
  });

  bot.hears("Просмотр расписания на дату", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    ctx.session.scheduleAction = { type: "view", step: "date" };
    await ctx.reply(
      "Укажите дату в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД для просмотра расписания:",
    );
  });

  bot.hears("Изменить/добавить расписание на дату", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    ctx.session.scheduleAction = { type: "edit", step: "date" };
    await ctx.reply(
      "Укажите дату в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД для изменения/добавления расписания:",
    );
  });

  bot.hears("Удалить расписание на дату", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    ctx.session.scheduleAction = { type: "delete", step: "date" };
    await ctx.reply(
      "Укажите дату в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД для удаления расписания:",
    );
  });

  bot.hears("Посмотреть всё расписание", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    try {
      const rows = (await sheetsService.getWorkHoursRaw()) || [];
      const nonEmpty = rows.filter(
        (r) => (r.date || r.rawDate || "").trim() || (r.weekday || "").trim(),
      );

      if (!nonEmpty.length) {
        await ctx.reply("Расписание пусто в окне из 50 строк.");
        return;
      }

      const lines = nonEmpty.map((r, idx) => {
        const label =
          (r.date || r.rawDate || "").trim() || `шаблон: ${r.weekday}`;
        const base = `#${idx + 1}. ${label}`;
        const work = r.start && r.end ? ` ${r.start}–${r.end}` : "";
        const lunch =
          r.lunchStart && r.lunchEnd
            ? `, обед ${r.lunchStart}–${r.lunchEnd}`
            : "";
        return base + work + lunch;
      });

      await ctx.reply(
        'Текущее окно расписания (первые 50 строк листа "Расписание"):\n\n' +
          lines.join("\n"),
      );
      await ctx.reply(
        'Чтобы отредактировать конкретную дату, используйте пункт "Изменить/добавить расписание на дату" и укажите нужную дату.',
      );
    } catch (e) {
      await ctx.reply(`Ошибка при получении расписания: ${e.message || e}`);
    }
  });

  bot.hears("Шаблоны по дням недели", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (!(ctx.session && ctx.session.mode === "admin")) return;

    try {
      const rows = (await sheetsService.getWorkHoursRaw()) || [];
      const templates = rows.filter((r) => !r.date && (r.weekday || "").trim());

      if (!templates.length) {
        await ctx.reply(
          "Шаблоны по дням недели ещё не заданы. Можно настроить их, ответив на вопросы ниже.",
        );
      } else {
        const lines = templates.map((r) => {
          const work =
            r.start && r.end ? ` ${r.start}–${r.end}` : " (время не задано)";
          const lunch =
            r.lunchStart && r.lunchEnd
              ? `, обед ${r.lunchStart}–${r.lunchEnd}`
              : "";
          return `${r.weekday}${work}${lunch}`;
        });
        await ctx.reply(
          "Текущие шаблоны по дням недели:\n\n" + lines.join("\n"),
        );
      }

      ctx.session.scheduleAction = { type: "weekday_edit", step: "weekday" };
      await ctx.reply(
        'Укажите день недели (например, Пн, Вт, Ср или mon/tue/...)\nили напишите "удалить Пн" чтобы удалить шаблон для Пн:',
      );
    } catch (e) {
      await ctx.reply(`Ошибка при получении шаблонов: ${e.message || e}`);
    }
  });

  bot.hears("Вернуться в пользовательский режим", async (ctx) => {
    if (!isAdmin(ctx)) return;
    ctx.session = ctx.session || {};
    ctx.session.mode = "user";
    await ctx.reply(
      "Режим пользователя.\n\n👇 Выберите действие с помощью кнопок ниже:",
      Markup.keyboard([
        ["Записаться 💇‍♂️"],
        ["Мои записи"],
        ["Как добраться 🗺️"],
      ]).resize(),
    );
  });

  // --- Финансовая статистика ---
  bot.hears("📊 Финансовая статистика", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      const keyboard = Markup.inlineKeyboard([
        [Markup.button.callback("Сегодня", "revenue:today")],
        [Markup.button.callback("Вчера", "revenue:yesterday")],
        [Markup.button.callback("Эта неделя", "revenue:this_week")],
        [Markup.button.callback("Прошлая неделя", "revenue:last_week")],
        [Markup.button.callback("Этот месяц", "revenue:this_month")],
        [Markup.button.callback("Прошлый месяц", "revenue:last_month")],
        [Markup.button.callback("По услугам", "revenue:by_services")],
        [Markup.button.callback("Назад в админ-меню", "revenue:back")],
      ]);

      await ctx.reply("Выберите период для просмотра статистики:", keyboard);
    }
  });

  bot.action(/revenue:(.+)/, async (ctx) => {
    if (!isAdmin(ctx)) {
      await ctx.answerCbQuery("Доступ запрещен");
      return;
    }

    const period = ctx.match[1];
    await ctx.answerCbQuery();

    if (period === "back") {
      await ctx.reply(
        "Включён режим администратора. Выберите действие:",
        adminKeyboard,
      );
      return;
    }

    try {
      const timezone = await sheetsService.getTimezone();
      let startDate = null;
      let endDate = null;
      let periodLabel = "";

      const now = dayjs().tz(timezone);

      switch (period) {
        case "today":
          startDate = now.startOf("day").format("YYYY-MM-DD");
          endDate = now.endOf("day").format("YYYY-MM-DD");
          periodLabel = formatDate(startDate);
          break;

        case "yesterday":
          const yesterday = now.subtract(1, "day");
          startDate = yesterday.startOf("day").format("YYYY-MM-DD");
          endDate = yesterday.endOf("day").format("YYYY-MM-DD");
          periodLabel = formatDate(startDate);
          break;

        case "this_week":
          // Понедельник текущей недели до сегодня
          const monday = now.startOf("week").add(1, "day"); // dayjs считает воскресенье первым днем
          startDate = monday.format("YYYY-MM-DD");
          endDate = now.format("YYYY-MM-DD");
          periodLabel = `с ${formatDate(startDate)} по ${formatDate(endDate)}`;
          break;

        case "last_week":
          // Понедельник прошлой недели до воскресенья прошлой недели
          const lastMonday = now
            .subtract(1, "week")
            .startOf("week")
            .add(1, "day");
          const lastSunday = lastMonday.add(6, "day");
          startDate = lastMonday.format("YYYY-MM-DD");
          endDate = lastSunday.format("YYYY-MM-DD");
          periodLabel = `с ${formatDate(startDate)} по ${formatDate(endDate)}`;
          break;

        case "this_month":
          startDate = now.startOf("month").format("YYYY-MM-DD");
          endDate = now.format("YYYY-MM-DD");
          periodLabel = `${now.format("MMMM YYYY")} (по ${formatDate(
            endDate,
          )})`;
          break;

        case "last_month":
          const lastMonth = now.subtract(1, "month");
          startDate = lastMonth.startOf("month").format("YYYY-MM-DD");
          endDate = lastMonth.endOf("month").format("YYYY-MM-DD");
          periodLabel = lastMonth.format("MMMM YYYY");
          break;

        case "by_services":
          // Все завершенные записи без фильтра по дате
          startDate = null;
          endDate = null;
          periodLabel = "все время";
          break;

        default:
          await ctx.reply("Неизвестный период.");
          return;
      }

      const appointments = await sheetsService.getCompletedAppointments({
        startDate,
        endDate,
      });

      let extraMetrics = null;

      // Дополнительные показатели считаем только для периодов с датами
      if (startDate || endDate) {
        const [cancelledAppointments, newClientsCount] = await Promise.all([
          sheetsService.getCancelledAppointmentsInPeriod({
            startDate,
            endDate,
          }),
          sheetsService.getNewClientsCountInPeriod({
            startDate,
            endDate,
          }),
        ]);

        extraMetrics = {
          newClientsCount,
          cancelledCount: cancelledAppointments.length,
        };
      }

      const stats = revenueStats.calculateRevenueStats(appointments);
      const formatted = revenueStats.formatRevenueStats(
        stats,
        periodLabel,
        extraMetrics,
      );

      await ctx.reply(formatted);
    } catch (error) {
      console.error("Ошибка при получении статистики доходов:", error);
      await ctx.reply(
        `Ошибка при получении статистики: ${
          error.message || "Неизвестная ошибка"
        }`,
      );
    }
  });

  // --- Управление услугами ---
  bot.hears("Управление услугами", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Сохраняем информацию о том, что пользователь пришел из настроек
      ctx.session.fromSettings = true;
      await ctx.reply(
        "Управление услугами. Выберите действие:",
        servicesKeyboard,
      );
    }
  });

  bot.hears("Назад в админ-меню", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      // Если пользователь находится в управлении услугами и пришел из настроек
      if (ctx.session.servicesAction && ctx.session.fromSettings) {
        delete ctx.session.servicesAction;
        await ctx.reply("Настройки. Выберите действие:", settingsKeyboard);
      } else {
        // Возврат из настроек в главное меню или из других мест
        delete ctx.session.servicesAction;
        delete ctx.session.scheduleAction;
        delete ctx.session.fromSettings;
        await ctx.reply(
          "Включён режим администратора. Выберите действие:",
          adminKeyboard,
        );
      }
    }
  });

  bot.hears("Список услуг", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      const services = servicesService.getAllServices();
      if (!services.length) {
        await ctx.reply("Нет услуг в системе.");
        return;
      }
      const text = services
        .map(
          (s) =>
            `• ${s.name}\n  Ключ: ${s.key}\n  Цена: ${
              s.price !== null ? s.price + " ₽" : "не указана"
            }\n  Продолжительность: ${s.durationMin} мин`,
        )
        .join("\n\n");
      await ctx.reply(`Список услуг:\n\n${text}`);
    }
  });

  bot.hears("Добавить услугу", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      ctx.session.servicesAction = { type: "create", step: "key" };
      await ctx.reply(
        "Добавление новой услуги.\n\nОтправьте ключ услуги (латинские буквы, цифры, подчёркивания, например: NEW_SERVICE):\nДля отмены напишите /admin_cancel",
      );
    }
  });

  bot.hears("Изменить услугу", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      const services = servicesService.getAllServices();
      if (!services.length) {
        await ctx.reply("Нет услуг для изменения.");
        return;
      }
      const buttons = services.map((s) => [
        Markup.button.callback(`${s.name} (${s.key})`, `service_edit:${s.key}`),
      ]);
      buttons.push([Markup.button.callback("Отменить", "service_cancel")]);
      await ctx.reply(
        "Выберите услугу для изменения:",
        Markup.inlineKeyboard(buttons),
      );
    }
  });

  bot.hears("Удалить услугу", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      const services = servicesService.getAllServices();
      if (!services.length) {
        await ctx.reply("Нет услуг для удаления.");
        return;
      }
      const buttons = services.map((s) => [
        Markup.button.callback(
          `${s.name} (${s.key})`,
          `service_delete:${s.key}`,
        ),
      ]);
      buttons.push([Markup.button.callback("Отменить", "service_cancel")]);
      await ctx.reply(
        "Выберите услугу для удаления:",
        Markup.inlineKeyboard(buttons),
      );
    }
  });

  bot.action(/service_edit:(.+)/, async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    const key = ctx.match[1];
    const service = servicesService.getServiceByKey(key);
    if (!service) {
      await ctx.reply("Услуга не найдена.");
      return;
    }
    ctx.session.servicesAction = {
      type: "update",
      key,
      step: "field",
    };
    const buttons = [
      [Markup.button.callback("Название", `service_field:name`)],
      [Markup.button.callback("Цена", `service_field:price`)],
      [
        Markup.button.callback(
          "Продолжительность",
          `service_field:durationMin`,
        ),
      ],
      [Markup.button.callback("Отменить", "service_cancel")],
    ];
    await ctx.reply(
      `Редактирование услуги: ${service.name}\n\nТекущие значения:\nНазвание: ${
        service.name
      }\nЦена: ${
        service.price !== null ? service.price + " ₽" : "не указана"
      }\nПродолжительность: ${
        service.durationMin
      } мин\n\nВыберите поле для изменения:`,
      Markup.inlineKeyboard(buttons),
    );
  });

  bot.action(/service_field:(.+)/, async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    const field = ctx.match[1];
    if (
      !ctx.session.servicesAction ||
      ctx.session.servicesAction.type !== "update"
    ) {
      await ctx.reply("Сессия истекла. Начните заново.");
      return;
    }
    ctx.session.servicesAction.step = field;
    const fieldNames = {
      name: "название",
      price: "цену (число или 'удалить' для очистки)",
      durationMin: "продолжительность в минутах",
    };
    await ctx.reply(
      `Отправьте новое значение для поля "${fieldNames[field]}":\nДля отмены напишите /admin_cancel`,
    );
  });

  bot.action(/service_delete:(.+)/, async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    const key = ctx.match[1];
    const service = servicesService.getServiceByKey(key);
    if (!service) {
      await ctx.reply("Услуга не найдена.");
      return;
    }
    const result = servicesService.deleteService(key);
    if (result.ok) {
      await ctx.reply(`Услуга "${service.name}" удалена.`);
    } else {
      await ctx.reply(`Ошибка: ${result.error}`);
    }
  });

  bot.action("service_cancel", async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    delete ctx.session.servicesAction;
    await ctx.reply("Отменено.");
  });

  bot.action("admin:broadcast_confirm", async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    const act = ctx.session && ctx.session.adminAction;
    if (!act || act.type !== "broadcast") {
      await ctx.reply("Нет ожидаемой рассылки.");
      return;
    }

    const recipients = act.recipients || [];
    if (!recipients.length) {
      await ctx.reply("Нет получателей для рассылки.");
      delete ctx.session.adminAction;
      return;
    }

    await ctx.reply(`Запускаю рассылку на ${recipients.length} клиентов...`);
    const results = await adminService.broadcastToClients(
      bot,
      sheetsService,
      act.payload || act.message,
      { recipients, throttleMs: 750, skipBanned: true },
    );
    const ok = results.filter((r) => r.ok).length;
    const fail = results.length - ok;

    // Логирование критичного действия (массовая рассылка)
    logCriticalAction(
      ctx.from.id,
      "admin_broadcast",
      {
        recipientsCount: recipients.length,
        sentCount: ok,
        failedCount: fail,
        payloadKind: act.payload?.kind || "text",
      },
      ok > 0 ? "success" : "failed",
    );

    await ctx.reply(`Рассылка завершена. Отправлено: ${ok}. Ошибок: ${fail}.`);
    delete ctx.session.adminAction;
  });

  bot.action("admin:broadcast_cancel", async (ctx) => {
    if (!isAdmin(ctx)) return;
    await ctx.answerCbQuery();
    delete ctx.session.adminAction;
    await ctx.reply("Рассылка отменена.");
  });

  bot.command("admin_cancel", async (ctx) => {
    if (!isAdmin(ctx)) return;
    delete ctx.session.adminAction;
    delete ctx.session.servicesAction;
    await ctx.reply("Действие админа отменено.");
  });

  bot.on("text", async (ctx, next) => {
    if (!isAdmin(ctx) || !(ctx.session && ctx.session.mode === "admin"))
      return next();

    // Обработка настроек расписания
    const scheduleAction = ctx.session && ctx.session.scheduleAction;
    if (scheduleAction) {
      const text = ctx.message.text && ctx.message.text.trim();

      // Помощник для конвертации даты ДД.ММ.ГГГГ -> ГГГГ-ММ-ДД
      const toIsoDate = (raw) => {
        const value = (raw || "").trim();
        if (!value) return null;
        if (/^\d{2}\.\d{2}\.\d{4}$/.test(value)) {
          const [dd, mm, yyyy] = value.split(".");
          return `${yyyy}-${mm}-${dd}`;
        }
        return value;
      };

      if (scheduleAction.step === "date") {
        const input = (text || "").trim();
        if (!input) {
          await ctx.reply(
            "Дата не может быть пустой. Укажите дату в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД:",
          );
          return;
        }

        ctx.session.scheduleAction.dateInput = input;
        const isoDate = toIsoDate(input);

        if (scheduleAction.type === "view") {
          try {
            const workHours = await sheetsService.getWorkHoursForDate(isoDate);
            if (!workHours || !workHours.start || !workHours.end) {
              await ctx.reply("На эту дату расписание не задано (выходной).");
            } else {
              const infoLines = [
                `Расписание на ${input}:`,
                `Рабочее время: ${workHours.start}–${workHours.end}`,
              ];
              if (workHours.lunchStart && workHours.lunchEnd) {
                infoLines.push(
                  `Обед: ${workHours.lunchStart}–${workHours.lunchEnd}`,
                );
              }
              await ctx.reply(infoLines.join("\n"));
            }
          } catch (e) {
            await ctx.reply(
              `Ошибка при получении расписания: ${e.message || e}`,
            );
          }
          ctx.session.scheduleAction = null;
          return;
        }

        if (scheduleAction.type === "edit") {
          ctx.session.scheduleAction.step = "start";
          await ctx.reply(
            `Укажите время начала рабочего дня для ${input} в формате HH:MM (например, 10:00):`,
          );
          return;
        }

        if (scheduleAction.type === "delete") {
          try {
            await sheetsService.deleteWorkHoursForDate(isoDate);
            await ctx.reply(
              `Расписание на дату ${input} удалено (если было задано).`,
            );
          } catch (e) {
            await ctx.reply(
              `Ошибка при удалении расписания: ${e.message || e}`,
            );
          }
          ctx.session.scheduleAction = null;
          return;
        }
      } else if (scheduleAction.type === "edit") {
        const dateInput = scheduleAction.dateInput;
        if (!dateInput) {
          ctx.session.scheduleAction = null;
          await ctx.reply(
            "Сессия настройки расписания истекла. Начните заново.",
          );
          return;
        }

        if (scheduleAction.step === "start") {
          if (!/^\d{2}:\d{2}$/.test(text || "")) {
            await ctx.reply(
              "Некорректный формат времени. Укажите время в формате HH:MM (например, 10:00):",
            );
            return;
          }
          ctx.session.scheduleAction.start = text;
          ctx.session.scheduleAction.step = "end";
          await ctx.reply(
            "Укажите время окончания рабочего дня в формате HH:MM (например, 20:00):",
          );
          return;
        }

        if (scheduleAction.step === "end") {
          if (!/^\d{2}:\d{2}$/.test(text || "")) {
            await ctx.reply(
              "Некорректный формат времени. Укажите время в формате HH:MM (например, 20:00):",
            );
            return;
          }
          ctx.session.scheduleAction.end = text;
          ctx.session.scheduleAction.step = "lunch_start";
          await ctx.reply(
            'Укажите время начала обеда в формате HH:MM или "-" если без обеда:',
          );
          return;
        }

        if (scheduleAction.step === "lunch_start") {
          let lunchStart = "";
          if (text && text.trim() !== "-") {
            if (!/^\d{2}:\d{2}$/.test(text || "")) {
              await ctx.reply(
                'Некорректный формат времени. Укажите время в формате HH:MM или "-" если без обеда:',
              );
              return;
            }
            lunchStart = text;
          }
          ctx.session.scheduleAction.lunchStart = lunchStart;
          ctx.session.scheduleAction.step = "lunch_end";
          if (lunchStart) {
            await ctx.reply(
              "Укажите время окончания обеда в формате HH:MM (должно быть позже начала обеда):",
            );
          } else {
            await ctx.reply(
              'Обед будет отсутствовать. Подтвердите сохранение: напишите "Да" или "Нет".',
            );
            ctx.session.scheduleAction.step = "confirm";
          }
          return;
        }

        if (scheduleAction.step === "lunch_end") {
          const lunchStart = scheduleAction.lunchStart;
          let lunchEnd = "";
          if (text && text.trim() !== "-") {
            if (!/^\d{2}:\d{2}$/.test(text || "")) {
              await ctx.reply(
                "Некорректный формат времени. Укажите время окончания обеда в формате HH:MM:",
              );
              return;
            }
            lunchEnd = text;
          }
          ctx.session.scheduleAction.lunchEnd = lunchEnd;
          ctx.session.scheduleAction.step = "confirm";

          const summary = [
            `Расписание на ${dateInput}:`,
            `Рабочее время: ${scheduleAction.start}–${scheduleAction.end}`,
            lunchStart && lunchEnd
              ? `Обед: ${lunchStart}–${lunchEnd}`
              : "Обед: нет",
            "",
            'Подтвердите сохранение: напишите "Да" или "Нет".',
          ].join("\n");

          await ctx.reply(summary);
          return;
        }

        if (scheduleAction.step === "confirm") {
          const answer = (text || "").trim().toLowerCase();
          if (answer !== "да" && answer !== "нет") {
            await ctx.reply(
              'Ответьте "Да" для сохранения или "Нет" для отмены.',
            );
            return;
          }
          if (answer === "нет") {
            ctx.session.scheduleAction = null;
            await ctx.reply("Изменение расписания отменено.");
            return;
          }

          try {
            await sheetsService.setWorkHoursForDate(scheduleAction.dateInput, {
              start: scheduleAction.start,
              end: scheduleAction.end,
              lunchStart: scheduleAction.lunchStart,
              lunchEnd: scheduleAction.lunchEnd,
            });
            await ctx.reply(
              `Расписание на дату ${scheduleAction.dateInput} сохранено.`,
            );
          } catch (e) {
            await ctx.reply(
              `Ошибка при сохранении расписания: ${e.message || e}`,
            );
          }

          ctx.session.scheduleAction = null;
          return;
        }
      }
    }

    // Обработка шаблонов по дням недели
    const scheduleAction2 = ctx.session && ctx.session.scheduleAction;
    if (scheduleAction2 && scheduleAction2.type === "weekday_edit") {
      const text = ctx.message.text && ctx.message.text.trim();

      if (scheduleAction2.step === "weekday") {
        if (!text) {
          await ctx.reply(
            'Укажите день недели (например, Пн, Вт, Ср или mon/tue/...) или "удалить Пн":',
          );
          return;
        }

        const lower = text.toLowerCase();
        if (lower.startsWith("удалить")) {
          const parts = text.split(/\s+/);
          const dayToken = parts[1];
          if (!dayToken) {
            await ctx.reply(
              'Укажите день для удаления, например: "удалить Пн".',
            );
            return;
          }
          try {
            await sheetsService.deleteWeekdayTemplate(dayToken);
            await ctx.reply(
              `Шаблон для дня \"${dayToken}\" удалён (если существовал).`,
            );
          } catch (e) {
            await ctx.reply(`Ошибка при удалении шаблона: ${e.message || e}`);
          }
          ctx.session.scheduleAction = null;
          return;
        }

        ctx.session.scheduleAction.weekdayKey = text;
        ctx.session.scheduleAction.step = "weekday_start";
        await ctx.reply(
          `Укажите время начала рабочего дня для шаблона \"${text}\" в формате HH:MM:`,
        );
        return;
      }

      if (scheduleAction2.step === "weekday_start") {
        if (!/^\d{2}:\d{2}$/.test(text || "")) {
          await ctx.reply(
            "Некорректный формат времени. Укажите время в формате HH:MM:",
          );
          return;
        }
        ctx.session.scheduleAction.start = text;
        ctx.session.scheduleAction.step = "weekday_end";
        await ctx.reply(
          "Укажите время окончания рабочего дня в формате HH:MM:",
        );
        return;
      }

      if (scheduleAction2.step === "weekday_end") {
        if (!/^\d{2}:\d{2}$/.test(text || "")) {
          await ctx.reply(
            "Некорректный формат времени. Укажите время в формате HH:MM:",
          );
          return;
        }
        ctx.session.scheduleAction.end = text;
        ctx.session.scheduleAction.step = "weekday_lunch_start";
        await ctx.reply(
          'Укажите время начала обеда в формате HH:MM или "-" если без обеда:',
        );
        return;
      }

      if (scheduleAction2.step === "weekday_lunch_start") {
        let lunchStart = "";
        if (text && text.trim() !== "-") {
          if (!/^\d{2}:\d{2}$/.test(text || "")) {
            await ctx.reply(
              'Некорректный формат времени. Укажите время в формате HH:MM или "-" если без обеда:',
            );
            return;
          }
          lunchStart = text;
        }
        ctx.session.scheduleAction.lunchStart = lunchStart;

        if (!lunchStart) {
          ctx.session.scheduleAction.lunchEnd = "";
          ctx.session.scheduleAction.step = "weekday_confirm";
          const d = scheduleAction2.weekdayKey;
          const summary = [
            `Шаблон для дня \"${d}\":`,
            `Рабочее время: ${scheduleAction2.start}–${scheduleAction2.end}`,
            "Обед: нет",
            "",
            'Подтвердите сохранение шаблона: напишите "Да" или "Нет".',
          ].join("\n");
          await ctx.reply(summary);
          return;
        }

        ctx.session.scheduleAction.step = "weekday_lunch_end";
        await ctx.reply(
          "Укажите время окончания обеда в формате HH:MM (должно быть позже начала обеда):",
        );
        return;
      }

      if (scheduleAction2.step === "weekday_lunch_end") {
        if (!/^\d{2}:\d{2}$/.test(text || "")) {
          await ctx.reply(
            "Некорректный формат времени. Укажите время окончания обеда в формате HH:MM:",
          );
          return;
        }
        ctx.session.scheduleAction.lunchEnd = text;
        ctx.session.scheduleAction.step = "weekday_confirm";

        const d = scheduleAction2.weekdayKey;
        const summary = [
          `Шаблон для дня \"${d}\":`,
          `Рабочее время: ${scheduleAction2.start}–${scheduleAction2.end}`,
          `Обед: ${scheduleAction2.lunchStart}–${scheduleAction2.lunchEnd}`,
          "",
          'Подтвердите сохранение шаблона: напишите "Да" или "Нет".',
        ].join("\n");
        await ctx.reply(summary);
        return;
      }

      if (scheduleAction2.step === "weekday_confirm") {
        const answer = (text || "").trim().toLowerCase();
        if (answer !== "да" && answer !== "нет") {
          await ctx.reply('Ответьте "Да" для сохранения или "Нет" для отмены.');
          return;
        }
        if (answer === "нет") {
          ctx.session.scheduleAction = null;
          await ctx.reply("Изменение шаблона отменено.");
          return;
        }

        try {
          await sheetsService.setWeekdayTemplate(scheduleAction2.weekdayKey, {
            start: scheduleAction2.start,
            end: scheduleAction2.end,
            lunchStart: scheduleAction2.lunchStart,
            lunchEnd: scheduleAction2.lunchEnd,
          });
          await ctx.reply(
            `Шаблон для дня \"${scheduleAction2.weekdayKey}\" сохранён.`,
          );
        } catch (e) {
          await ctx.reply(`Ошибка при сохранении шаблона: ${e.message || e}`);
        }

        ctx.session.scheduleAction = null;
        return;
      }
    }

    // Обработка управления услугами
    const servicesAction = ctx.session && ctx.session.servicesAction;
    if (servicesAction) {
      const text = ctx.message.text && ctx.message.text.trim();

      if (servicesAction.type === "create") {
        if (servicesAction.step === "key") {
          const key = text.toUpperCase();
          const existing = servicesService.getServiceByKey(key);
          if (existing) {
            await ctx.reply(
              "Услуга с таким ключом уже существует. Попробуйте другой ключ или /admin_cancel для отмены.",
            );
            return;
          }
          if (!/^[A-Za-z0-9_]+$/.test(key)) {
            await ctx.reply(
              "Ключ должен содержать только латинские буквы, цифры и подчёркивания. Попробуйте снова или /admin_cancel для отмены.",
            );
            return;
          }
          ctx.session.servicesAction = { type: "create", step: "name", key };
          await ctx.reply("Отправьте название услуги:");
          return;
        }
        if (servicesAction.step === "name") {
          if (!text || text.trim().length === 0) {
            await ctx.reply(
              "Название не может быть пустым. Попробуйте снова или /admin_cancel для отмены.",
            );
            return;
          }
          ctx.session.servicesAction = {
            type: "create",
            step: "price",
            key: servicesAction.key,
            name: text.trim(),
          };
          await ctx.reply(
            "Отправьте цену услуги (число в рублях) или 'нет' если цена не указана:",
          );
          return;
        }
        if (servicesAction.step === "price") {
          let price = null;
          if (text.toLowerCase() !== "нет" && text.trim() !== "") {
            const priceNum = Number(text);
            if (isNaN(priceNum) || priceNum < 0) {
              await ctx.reply(
                "Цена должна быть неотрицательным числом или 'нет'. Попробуйте снова или /admin_cancel для отмены.",
              );
              return;
            }
            price = priceNum;
          }
          ctx.session.servicesAction = {
            type: "create",
            step: "duration",
            key: servicesAction.key,
            name: servicesAction.name,
            price,
          };
          await ctx.reply("Отправьте продолжительность услуги в минутах:");
          return;
        }
        if (servicesAction.step === "duration") {
          const durationNum = Number(text);
          if (isNaN(durationNum) || durationNum <= 0) {
            await ctx.reply(
              "Продолжительность должна быть положительным числом. Попробуйте снова или /admin_cancel для отмены.",
            );
            return;
          }
          const result = servicesService.createService({
            key: servicesAction.key,
            name: servicesAction.name,
            price: servicesAction.price,
            durationMin: durationNum,
          });
          if (result.ok) {
            await ctx.reply(
              `Услуга "${result.service.name}" успешно создана!\nКлюч: ${
                result.service.key
              }\nЦена: ${
                result.service.price !== null
                  ? result.service.price + " ₽"
                  : "не указана"
              }\nПродолжительность: ${result.service.durationMin} мин`,
            );
          } else {
            await ctx.reply(`Ошибка при создании услуги: ${result.error}`);
          }
          delete ctx.session.servicesAction;
          return;
        }
      }

      if (servicesAction.type === "update") {
        const field = servicesAction.step;
        if (field === "name") {
          if (!text || text.trim().length === 0) {
            await ctx.reply(
              "Название не может быть пустым. Попробуйте снова или /admin_cancel для отмены.",
            );
            return;
          }
          const result = servicesService.updateService(servicesAction.key, {
            name: text.trim(),
          });
          if (result.ok) {
            await ctx.reply(
              `Название услуги обновлено: "${result.service.name}"`,
            );
          } else {
            await ctx.reply(`Ошибка: ${result.error}`);
          }
          delete ctx.session.servicesAction;
          return;
        }
        if (field === "price") {
          let price = null;
          if (
            text.toLowerCase() !== "удалить" &&
            text.toLowerCase() !== "нет" &&
            text.trim() !== ""
          ) {
            const priceNum = Number(text);
            if (isNaN(priceNum) || priceNum < 0) {
              await ctx.reply(
                "Цена должна быть неотрицательным числом, 'удалить' или 'нет'. Попробуйте снова или /admin_cancel для отмены.",
              );
              return;
            }
            price = priceNum;
          }
          const result = servicesService.updateService(servicesAction.key, {
            price,
          });
          if (result.ok) {
            await ctx.reply(
              `Цена услуги обновлена: ${
                result.service.price !== null
                  ? result.service.price + " ₽"
                  : "не указана"
              }`,
            );
          } else {
            await ctx.reply(`Ошибка: ${result.error}`);
          }
          delete ctx.session.servicesAction;
          return;
        }
        if (field === "durationMin") {
          const durationNum = Number(text);
          if (isNaN(durationNum) || durationNum <= 0) {
            await ctx.reply(
              "Продолжительность должна быть положительным числом. Попробуйте снова или /admin_cancel для отмены.",
            );
            return;
          }
          const result = servicesService.updateService(servicesAction.key, {
            durationMin: durationNum,
          });
          if (result.ok) {
            await ctx.reply(
              `Продолжительность услуги обновлена: ${result.service.durationMin} мин`,
            );
          } else {
            await ctx.reply(`Ошибка: ${result.error}`);
          }
          delete ctx.session.servicesAction;
          return;
        }
      }
    }

    const action =
      ctx.session && ctx.session.adminAction && ctx.session.adminAction.type;
    if (!action) return next();

    const text = ctx.message.text && ctx.message.text.trim();

    if (action === "cancel_booking_by_code") {
      const cancelCode = text.toUpperCase().trim();

      // Валидация кода отмены (должен быть 6 символов, буквы и цифры)
      if (
        !cancelCode ||
        cancelCode.length !== 6 ||
        !/^[A-Z0-9]+$/.test(cancelCode)
      ) {
        await ctx.reply(
          "Неверный формат кода отмены. Код должен состоять из 6 символов (буквы и цифры). /admin_cancel для отмены.",
        );
        return;
      }

      const result = await bookingService.cancelAppointmentByCode(cancelCode);

      if (!result.ok) {
        if (result.reason === "appointment_not_found") {
          await ctx.reply(
            "Запись с таким кодом отмены не найдена. /admin_cancel для отмены.",
          );
        } else if (result.reason === "already_cancelled") {
          await ctx.reply("Эта запись уже отменена. /admin_cancel для отмены.");
        } else {
          await ctx.reply(
            "Не удалось отменить запись. /admin_cancel для отмены.",
          );
        }
        logAdminAction(
          ctx.from.id,
          "admin_cancel_booking_by_code",
          { cancelCode, reason: result.reason },
          "failed",
        );
      } else {
        const appointment = result.appointment;
        await ctx.reply(
          `Запись отменена по коду ${cancelCode}.\n` +
            `ID: ${appointment.id}\n` +
            `Клиент: ${appointment.clientName}\n` +
            `Дата: ${formatDate(appointment.date)} ${appointment.timeStart}`,
        );
        // Логирование критичного действия (админ отменил запись по коду)
        logCriticalAction(
          ctx.from.id,
          "admin_cancel_booking_by_code",
          {
            appointmentId: appointment.id,
            cancelCode,
            clientTelegramId: appointment.telegramId,
            date: appointment.date,
            time: appointment.timeStart,
          },
          "success",
        );
        if (appointment.telegramId) {
          // Безопасная отправка уведомления пользователю с обработкой ошибок
          await safeSendMessage(
            ctx.telegram,
            String(appointment.telegramId),
            `Ваша запись на ${formatDate(appointment.date)} ${
              appointment.timeStart
            } отменена менеджером.`,
          );
        }
      }
      delete ctx.session.adminAction;
      return;
    }

    if (action === "ban") {
      let target = text;
      let telegramId = null;
      if (target.startsWith("@")) {
        const clients = await sheetsService.getAllClients();
        const found = clients.find(
          (c) => c.username && `@${c.username}` === target,
        );
        if (found) telegramId = found.telegramId;
      } else {
        telegramId = target;
      }

      // Валидация Telegram ID
      if (!telegramId || !validateTelegramId(telegramId)) {
        await ctx.reply(
          "Неверный формат Telegram ID. /admin_cancel для отмены.",
        );
        return;
      }

      await adminService.banUser(telegramId, "", sheetsService);
      // Логирование критичного действия (бан пользователя)
      logCriticalAction(
        ctx.from.id,
        "admin_ban_user",
        {
          bannedUserId: telegramId,
          target: text,
        },
        "success",
      );
      await ctx.reply(`Пользователь ${telegramId} забанен.`);
      delete ctx.session.adminAction;
      return;
    }

    if (action === "unban") {
      const telegramId = text;

      // Валидация Telegram ID
      if (!telegramId || !validateTelegramId(telegramId)) {
        await ctx.reply(
          "Неверный формат Telegram ID. /admin_cancel для отмены.",
        );
        return;
      }

      await adminService.unbanUser(telegramId, sheetsService);
      // Логирование критичного действия (разбан пользователя)
      logCriticalAction(
        ctx.from.id,
        "admin_unban_user",
        {
          unbannedUserId: telegramId,
        },
        "success",
      );
      await ctx.reply(`Пользователь ${telegramId} разбанен.`);
      delete ctx.session.adminAction;
      return;
    }

    if (action === "edit_28day_reminder") {
      const message = text;
      if (!message || message.trim().length === 0) {
        await ctx.reply(
          "Текст не может быть пустым. /admin_cancel для отмены.",
        );
        return;
      }

      // Санитизация текста (максимум 2000 символов для напоминания)
      const sanitizedMessage = sanitizeText(message, 2000);
      if (sanitizedMessage.length === 0) {
        await ctx.reply("Текст после очистки пуст. /admin_cancel для отмены.");
        return;
      }

      try {
        await sheetsService.set28DayReminderMessage(sanitizedMessage);

        // Логирование действия админа
        logAdminAction(
          ctx.from.id,
          "admin_edit_28day_reminder",
          { messageLength: sanitizedMessage.length },
          "success",
        );

        await ctx.reply(
          `Текст напоминания через 28 дней успешно обновлен!\n\nНовый текст:\n${sanitizedMessage}`,
        );
      } catch (err) {
        await ctx.reply(
          `Ошибка при сохранении текста: ${err.message}\n/admin_cancel для отмены.`,
        );
        logError(
          ctx.from.id,
          "admin_edit_28day_reminder",
          { error: err.message },
          "error",
        );
        return;
      }

      delete ctx.session.adminAction;
      return;
    }

    if (action === "edit_tips_link") {
      const trimmedInput = text.trim();

      if (!trimmedInput || trimmedInput.length === 0) {
        await ctx.reply(
          "Данные не могут быть пустыми. /admin_cancel для отмены.",
        );
        return;
      }

      // Валидация: может быть либо URL, либо номер телефона
      const isValidUrl =
        trimmedInput.startsWith("http://") ||
        trimmedInput.startsWith("https://") ||
        trimmedInput.startsWith("t.me/");

      const isPhoneNumber =
        /^[\d\s\-+()]+$/.test(trimmedInput) && trimmedInput.length >= 5;

      if (!isValidUrl && !isPhoneNumber) {
        await ctx.reply(
          "Укажите ссылку (http://, https://, t.me/) или номер телефона. /admin_cancel для отмены.",
        );
        return;
      }

      try {
        await sheetsService.setTipsLink(trimmedInput);

        // Логирование действия админа
        logAdminAction(
          ctx.from.id,
          "admin_edit_tips_link",
          {
            isLink: isValidUrl,
            isPhone: isPhoneNumber,
          },
          "success",
        );

        const typeText = isValidUrl ? "Ссылка" : "Номер телефона";
        await ctx.reply(
          `✅ ${typeText} для чаевых успешно обновлен!\n\n${trimmedInput}`,
        );
      } catch (err) {
        await ctx.reply(
          `Ошибка при сохранении: ${err.message}\n/admin_cancel для отмены.`,
        );
        logError(
          ctx.from.id,
          "admin_edit_tips_link",
          { error: err.message },
          "error",
        );
        return;
      }

      delete ctx.session.adminAction;
      return;
    }

    if (action === "edit_contacts") {
      const lines = text
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean);

      if (lines.length < 2) {
        await ctx.reply(
          "Необходимо указать телефон и адрес в двух строках:\nПервая строка - телефон\nВторая строка - адрес\n\n/admin_cancel для отмены.",
        );
        return;
      }

      const phone = lines[0];
      const address = lines.slice(1).join(" "); // Объединяем остальные строки в адрес

      if (!phone || phone.trim().length === 0) {
        await ctx.reply(
          "Телефон не может быть пустым. /admin_cancel для отмены.",
        );
        return;
      }

      if (!address || address.trim().length === 0) {
        await ctx.reply(
          "Адрес не может быть пустым. /admin_cancel для отмены.",
        );
        return;
      }

      try {
        await sheetsService.setBarberPhone(phone.trim());
        await sheetsService.setBarberAddress(address.trim());

        // Логирование действия админа
        logAdminAction(
          ctx.from.id,
          "admin_edit_contacts",
          {
            phoneLength: phone.trim().length,
            addressLength: address.trim().length,
          },
          "success",
        );

        await ctx.reply(
          `Контакты успешно обновлены!\n\n📞 Телефон: ${phone.trim()}\n📍 Адрес: ${address.trim()}`,
        );
      } catch (err) {
        await ctx.reply(
          `Ошибка при сохранении контактов: ${err.message}\n/admin_cancel для отмены.`,
        );
        logError(
          ctx.from.id,
          "admin_edit_contacts",
          { error: err.message },
          "error",
        );
        return;
      }

      delete ctx.session.adminAction;
      return;
    }

    if (action === "portfolio_delete") {
      const trimmed = (text || "").trim();
      const displayNumber = Number(trimmed);

      const ids = (await sheetsService.getPortfolioFileIds()) || [];
      const maxInStore = Math.min(6, ids.length);

      if (
        isNaN(displayNumber) ||
        displayNumber < 1 ||
        displayNumber > maxInStore
      ) {
        await ctx.reply(
          `Некорректный номер. Введите число от 1 до ${maxInStore}.\nДля отмены напишите /admin_cancel`,
        );
        return;
      }

      try {
        const ok = await sheetsService.deletePortfolioFileIdByIndex(
          displayNumber - 1, // UI: 1..N => 0..N-1
        );

        if (!ok) {
          await ctx.reply("Не удалось удалить фото. Попробуйте другой номер.");
          return;
        }

        await ctx.reply(`✅ Фото №${displayNumber} удалено из портфолио.`);
        delete ctx.session.adminAction;
        return;
      } catch (e) {
        await ctx.reply(`Ошибка при удалении фото: ${e.message || e}`);
        return;
      }
    }

    if (action === "save_location") {
      const trimmed = (text || "").trim();
      if (!trimmed) {
        await ctx.reply(
          "Ссылка не может быть пустой. /admin_cancel для отмены.",
        );
        return;
      }

      const isHttpUrl =
        trimmed.startsWith("http://") || trimmed.startsWith("https://");
      if (!isHttpUrl) {
        await ctx.reply(
          "Ссылка должна начинаться с http:// или https://. /admin_cancel для отмены.",
        );
        return;
      }

      try {
        await sheetsService.setLocationLink(trimmed);
        await ctx.reply("✅ Локация сохранена.");
        delete ctx.session.adminAction;
        return;
      } catch (e) {
        await ctx.reply(`Ошибка при сохранении локации: ${e.message || e}`);
        return;
      }
    }

    if (action === "save_location_2gis") {
      const trimmed = (text || "").trim();
      if (!trimmed) {
        await ctx.reply(
          "Ссылка не может быть пустой. /admin_cancel для отмены.",
        );
        return;
      }

      const isHttpUrl =
        trimmed.startsWith("http://") || trimmed.startsWith("https://");
      if (!isHttpUrl) {
        await ctx.reply(
          "Ссылка должна начинаться с http:// или https://. /admin_cancel для отмены.",
        );
        return;
      }

      try {
        await sheetsService.setLocationLink2gis(trimmed);
        await ctx.reply("✅ Ссылка на 2ГИС сохранена.");
        delete ctx.session.adminAction;
        return;
      } catch (e) {
        await ctx.reply(`Ошибка при сохранении ссылки 2ГИС: ${e.message || e}`);
        return;
      }
    }

    if (action === "broadcast") {
      const message = text;
      if (!message) {
        await ctx.reply("Текст пуст. /admin_cancel для отмены.");
        return;
      }

      // Санитизация текста рассылки (максимум 4000 символов для Telegram)
      const sanitizedMessage = sanitizeText(message, 4000);
      if (sanitizedMessage.length === 0) {
        await ctx.reply("Текст после очистки пуст. /admin_cancel для отмены.");
        return;
      }

      // Используем getClientsForBroadcast() для получения клиентов, которым можно отправить сегодня
      const clientsForBroadcast = sheetsService.getClientsForBroadcast
        ? await sheetsService.getClientsForBroadcast()
        : await sheetsService.getAllClients();

      const bans = await adminService.getBans();
      const recipients = clientsForBroadcast
        .filter((c) => c && c.telegramId)
        .map((c) => String(c.telegramId))
        .filter((id) => id && !bans.some((b) => String(b) === String(id)));

      if (!recipients.length) {
        await ctx.reply(
          "Нет получателей для рассылки (нет клиентов с telegramId или все в бане).",
        );
        delete ctx.session.adminAction;
        return;
      }

      // Получаем общее количество клиентов для информации
      const allClients = await sheetsService.getAllClients();
      const allClientsWithTelegram = allClients.filter(
        (c) => c && c.telegramId,
      ).length;

      // Проверка максимального количества получателей (250)
      const MAX_RECIPIENTS = 250;
      const recipientsToSend = recipients.slice(0, MAX_RECIPIENTS);
      const waitingCount = Math.max(
        0,
        allClientsWithTelegram - recipients.length,
      );

      ctx.session.adminAction = {
        type: "broadcast",
        payload: { kind: "text", text: sanitizedMessage },
        recipients: recipientsToSend,
      };

      const sample = recipientsToSend.slice(0, 6).join(", ");
      const keyboard = Markup.inlineKeyboard([
        [
          Markup.button.callback(
            "Подтвердить рассылку ✅",
            "admin:broadcast_confirm",
          ),
        ],
        [Markup.button.callback("Отменить ❌", "admin:broadcast_cancel")],
      ]);

      let previewMessage = `Предпросмотр рассылки:\n\nТекст:\n${message}\n\n`;
      previewMessage += `📤 Будет отправлено сегодня: ${recipientsToSend.length} из ${MAX_RECIPIENTS}\n`;
      if (waitingCount > 0) {
        previewMessage += `⏳ Заблокированных пользователей: ${waitingCount}\n`;
      }
      if (recipients.length > MAX_RECIPIENTS) {
        previewMessage += `⚠️ Всего доступно: ${recipients.length}. Будет отправлено ${MAX_RECIPIENTS}, остальные получат рассылку завтра.\n`;
      }

      await ctx.reply(previewMessage, keyboard);

      return;
    }

    return next();
  });

  // Приём фото от админа для массовой рассылки
  bot.on("photo", async (ctx, next) => {
    if (!isAdmin(ctx) || !(ctx.session && ctx.session.mode === "admin"))
      return next();
    const action =
      ctx.session && ctx.session.adminAction && ctx.session.adminAction.type;
    if (action !== "broadcast" && action !== "portfolio_upload") return next();

    const photos = ctx.message.photo || [];
    if (!photos.length) return next();
    // Выбираем наибольшее доступное превью (последний элемент массива)
    const best = photos[photos.length - 1];
    const fileId = best.file_id;
    const caption = (ctx.message.caption || "").trim();

    if (action === "portfolio_upload") {
      try {
        await sheetsService.addPortfolioFileId(fileId);
        await ctx.reply("✅ Фото добавлено в портфолио.");
      } catch (e) {
        await ctx.reply(
          `Ошибка при сохранении фото в портфолио: ${e.message || e}`,
        );
      }
      delete ctx.session.adminAction;
      return;
    }

    // Используем getClientsForBroadcast() для получения клиентов, которым можно отправить сегодня
    const clientsForBroadcast = sheetsService.getClientsForBroadcast
      ? await sheetsService.getClientsForBroadcast()
      : await sheetsService.getAllClients();

    const bans = await adminService.getBans();
    const recipients = clientsForBroadcast
      .filter((c) => c && c.telegramId)
      .map((c) => String(c.telegramId))
      .filter((id) => id && !bans.some((b) => String(b) === String(id)));

    if (!recipients.length) {
      await ctx.reply(
        "Нет получателей для рассылки (нет клиентов с telegramId или все в бане).",
      );
      delete ctx.session.adminAction;
      return;
    }

    // Получаем общее количество клиентов для информации
    const allClients = await sheetsService.getAllClients();
    const allClientsWithTelegram = allClients.filter(
      (c) => c && c.telegramId,
    ).length;

    // Проверка максимального количества получателей (250)
    const MAX_RECIPIENTS = 250;
    const recipientsToSend = recipients.slice(0, MAX_RECIPIENTS);
    const waitingCount = Math.max(
      0,
      allClientsWithTelegram - recipients.length,
    );

    ctx.session.adminAction = {
      type: "broadcast",
      payload: { kind: "photo", fileId, caption },
      recipients: recipientsToSend,
    };

    const sample = recipientsToSend.slice(0, 6).join(", ");
    const keyboard = Markup.inlineKeyboard([
      [
        Markup.button.callback(
          "Подтвердить рассылку ✅",
          "admin:broadcast_confirm",
        ),
      ],
      [Markup.button.callback("Отменить ❌", "admin:broadcast_cancel")],
    ]);

    await ctx.reply(
      "Предпросмотр фото-письма. Подпись:" +
        (caption ? `\n${caption}` : " (без подписи)"),
    );
    await ctx.replyWithPhoto(fileId);

    let previewMessage = `📤 Будет отправлено сегодня: ${recipientsToSend.length} из ${MAX_RECIPIENTS}\n`;
    if (waitingCount > 0) {
      previewMessage += `⏳ Заблокированных пользователей: ${waitingCount}\n`;
    }
    if (recipients.length > MAX_RECIPIENTS) {
      previewMessage += `⚠️ Всего доступно: ${recipients.length}. Будет отправлено ${MAX_RECIPIENTS}, остальные получат рассылку завтра.\n`;
    }

    await ctx.reply(previewMessage, keyboard);
  });

  return bot;
}

module.exports = {
  createBot,
  cleanupSessionsFile,
};
