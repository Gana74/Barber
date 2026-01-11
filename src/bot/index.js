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
const { scheduleBackup } = require("../utils/backup");

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

  const localSession = new LocalSession({
    database: "sessions.json",
  });

  bot.use(localSession.middleware());

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
          `Session too large for user ${ctx.from?.id}, cleared session`
        );
      }
    }
    return next();
  });

  // Настройка меню команд (кнопка меню в левой части поля ввода)
  bot.telegram
    .setMyCommands([
      { command: "start", description: "Начать общение с начала" },
      { command: "book", description: "Записаться на стрижку" },
      { command: "services", description: "Посмотреть список услуг" },
      { command: "admin", description: "Режим администратора" },
      { command: "user", description: "Режим пользователя" },
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
      `Привет, ${name}! Я бот барбершопа. Здесь можно записаться на стрижку.`,
      Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]])
        .resize()
        .oneTime()
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
      timezone
    );

    if (!list.length) {
      await ctx.reply("У тебя пока нет будущих записей.");
      return;
    }

    const lines = list.map(
      (app, idx) =>
        `${idx + 1}. ${app.service} — ${formatDate(app.date)} ${app.timeStart}`
    );

    const keyboard = list.map((app) => [
      Markup.button.callback(
        `Отменить ${formatDate(app.date)} ${app.timeStart}`,
        `cancel_app:${app.id}`
      ),
    ]);

    await ctx.reply(
      `Будущие записи:\n\n${lines.join("\n")}`,
      Markup.inlineKeyboard(keyboard)
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
      Markup.removeKeyboard()
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
        "Не удалось отменить запись: она не найдена или уже отменена."
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
      { cancelledAtUtc }
    );

    if (!ok) {
      await ctx.reply(
        "Не удалось отменить запись: она не найдена или уже отменена."
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
      "success"
    );

    await ctx.reply(
      `Запись на ${formatDate(appointment.date)} ${
        appointment.timeStart
      } отменена. Спасибо, что предупредил(а)!`
    );

    // Попытка удалить событие в календаре
    try {
      if (calendarService && calendarService.deleteEventForAppointmentId) {
        await calendarService.deleteEventForAppointmentId(id);
      }
    } catch (e) {
      console.warn(
        "Calendar delete failed for appointment (user cancel):",
        e.message || e
      );
    }

    if (config.managerChatId) {
      await ctx.telegram.sendMessage(
        config.managerChatId,
        `Клиент отменил запись:\nУслуга: ${
          appointment.service
        }\nДата: ${formatDate(appointment.date)}\nВремя: ${
          appointment.timeStart
        }–${appointment.timeEnd}\nКлиент: ${appointment.clientName}\nТелефон: ${
          appointment.phone
        }\nКод отмены: ${appointment.cancelCode}`
      );
    }
  });

  // --- Admin menu (manager only) ---
  // reply-style keyboard for admin (visual like user)
  const adminKeyboard = Markup.keyboard([
    ["Просмотр записей", "Статистика"],
    ["Отменить запись (по коду)"],
    ["Забанить пользователя", "Разбанить пользователя"],
    ["Массовая рассылка"],
    ["Управление услугами"],
    ["Вернуться в пользовательский режим"],
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
      adminKeyboard
    );
  });

  bot.command("user", async (ctx) => {
    ctx.session = ctx.session || {};
    ctx.session.mode = "user";
    await ctx.reply(
      "Режим пользователя. Выберите действие:",
      Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]])
        .resize()
        .oneTime()
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
              a.date
            )} ${a.timeStart}-${a.timeEnd} — ${a.clientName} (${a.phone})`
        );
      await ctx.reply(
        `Активные записи (показано ${lines.length} из ${all.length}):\n` +
          lines.join("\n")
      );
      return;
    }

    if (action === "stats") {
      const all = await sheetsService.getAllActiveAppointments();
      const clients = await sheetsService.getAllClients();
      const upcoming = all.length;
      const uniqueClients = new Set(
        clients.map((c) => String(c.telegramId)).filter(Boolean)
      ).size;
      await ctx.reply(
        `Статистика:\nАктивных записей: ${upcoming}\nКлиентов в базе: ${uniqueClients}`
      );
      return;
    }

    const inputActions = new Set([
      "cancel_booking_by_code",
      "ban",
      "unban",
      "broadcast",
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
          : "Отправьте Telegram ID пользователя для разбанивания. Для отмены напишите /admin_cancel"
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

  bot.hears("Вернуться в пользовательский режим", async (ctx) => {
    if (!isAdmin(ctx)) return;
    ctx.session = ctx.session || {};
    ctx.session.mode = "user";
    await ctx.reply(
      "Режим пользователя. Выберите действие:",
      Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]])
        .resize()
        .oneTime()
    );
  });

  // --- Управление услугами ---
  bot.hears("Управление услугами", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await ctx.reply(
        "Управление услугами. Выберите действие:",
        servicesKeyboard
      );
    }
  });

  bot.hears("Назад в админ-меню", async (ctx) => {
    if (!isAdmin(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      delete ctx.session.servicesAction;
      await ctx.reply(
        "Включён режим администратора. Выберите действие:",
        adminKeyboard
      );
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
            }\n  Продолжительность: ${s.durationMin} мин`
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
        "Добавление новой услуги.\n\nОтправьте ключ услуги (латинские буквы, цифры, подчёркивания, например: NEW_SERVICE):\nДля отмены напишите /admin_cancel"
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
        Markup.inlineKeyboard(buttons)
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
          `service_delete:${s.key}`
        ),
      ]);
      buttons.push([Markup.button.callback("Отменить", "service_cancel")]);
      await ctx.reply(
        "Выберите услугу для удаления:",
        Markup.inlineKeyboard(buttons)
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
          `service_field:durationMin`
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
      Markup.inlineKeyboard(buttons)
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
      `Отправьте новое значение для поля "${fieldNames[field]}":\nДля отмены напишите /admin_cancel`
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
      { recipients, throttleMs: 200, skipBanned: true }
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
      ok > 0 ? "success" : "failed"
    );

    // Планирование резервного копирования (с дебаунсингом)
    scheduleBackup();

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
              "Услуга с таким ключом уже существует. Попробуйте другой ключ или /admin_cancel для отмены."
            );
            return;
          }
          if (!/^[A-Za-z0-9_]+$/.test(key)) {
            await ctx.reply(
              "Ключ должен содержать только латинские буквы, цифры и подчёркивания. Попробуйте снова или /admin_cancel для отмены."
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
              "Название не может быть пустым. Попробуйте снова или /admin_cancel для отмены."
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
            "Отправьте цену услуги (число в рублях) или 'нет' если цена не указана:"
          );
          return;
        }
        if (servicesAction.step === "price") {
          let price = null;
          if (text.toLowerCase() !== "нет" && text.trim() !== "") {
            const priceNum = Number(text);
            if (isNaN(priceNum) || priceNum < 0) {
              await ctx.reply(
                "Цена должна быть неотрицательным числом или 'нет'. Попробуйте снова или /admin_cancel для отмены."
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
              "Продолжительность должна быть положительным числом. Попробуйте снова или /admin_cancel для отмены."
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
              }\nПродолжительность: ${result.service.durationMin} мин`
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
              "Название не может быть пустым. Попробуйте снова или /admin_cancel для отмены."
            );
            return;
          }
          const result = servicesService.updateService(servicesAction.key, {
            name: text.trim(),
          });
          if (result.ok) {
            await ctx.reply(
              `Название услуги обновлено: "${result.service.name}"`
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
                "Цена должна быть неотрицательным числом, 'удалить' или 'нет'. Попробуйте снова или /admin_cancel для отмены."
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
              }`
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
              "Продолжительность должна быть положительным числом. Попробуйте снова или /admin_cancel для отмены."
            );
            return;
          }
          const result = servicesService.updateService(servicesAction.key, {
            durationMin: durationNum,
          });
          if (result.ok) {
            await ctx.reply(
              `Продолжительность услуги обновлена: ${result.service.durationMin} мин`
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
          "Неверный формат кода отмены. Код должен состоять из 6 символов (буквы и цифры). /admin_cancel для отмены."
        );
        return;
      }

      const result = await bookingService.cancelAppointmentByCode(cancelCode);

      if (!result.ok) {
        if (result.reason === "appointment_not_found") {
          await ctx.reply(
            "Запись с таким кодом отмены не найдена. /admin_cancel для отмены."
          );
        } else if (result.reason === "already_cancelled") {
          await ctx.reply("Эта запись уже отменена. /admin_cancel для отмены.");
        } else {
          await ctx.reply(
            "Не удалось отменить запись. /admin_cancel для отмены."
          );
        }
        logAdminAction(
          ctx.from.id,
          "admin_cancel_booking_by_code",
          { cancelCode, reason: result.reason },
          "failed"
        );
      } else {
        const appointment = result.appointment;
        await ctx.reply(
          `Запись отменена по коду ${cancelCode}.\n` +
            `ID: ${appointment.id}\n` +
            `Клиент: ${appointment.clientName}\n` +
            `Дата: ${formatDate(appointment.date)} ${appointment.timeStart}`
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
          "success"
        );
        if (appointment.telegramId) {
          try {
            await ctx.telegram.sendMessage(
              String(appointment.telegramId),
              `Ваша запись на ${formatDate(appointment.date)} ${
                appointment.timeStart
              } отменена менеджером.`
            );
          } catch (e) {}
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
          (c) => c.username && `@${c.username}` === target
        );
        if (found) telegramId = found.telegramId;
      } else {
        telegramId = target;
      }

      // Валидация Telegram ID
      if (!telegramId || !validateTelegramId(telegramId)) {
        await ctx.reply(
          "Неверный формат Telegram ID. /admin_cancel для отмены."
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
        "success"
      );
      // Планирование резервного копирования (с дебаунсингом)
      scheduleBackup();
      await ctx.reply(`Пользователь ${telegramId} забанен.`);
      delete ctx.session.adminAction;
      return;
    }

    if (action === "unban") {
      const telegramId = text;

      // Валидация Telegram ID
      if (!telegramId || !validateTelegramId(telegramId)) {
        await ctx.reply(
          "Неверный формат Telegram ID. /admin_cancel для отмены."
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
        "success"
      );
      // Планирование резервного копирования (с дебаунсингом)
      scheduleBackup();
      await ctx.reply(`Пользователь ${telegramId} разбанен.`);
      delete ctx.session.adminAction;
      return;
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

      const clients = await sheetsService.getAllClients();
      const bans = await adminService.getBans();
      const recipients = clients
        .filter((c) => c && c.telegramId)
        .map((c) => String(c.telegramId))
        .filter((id) => id && !bans.some((b) => String(b) === String(id)));

      if (!recipients.length) {
        await ctx.reply(
          "Нет получателей для рассылки (нет клиентов с telegramId или все в бане)."
        );
        delete ctx.session.adminAction;
        return;
      }

      // Проверка максимального количества получателей (1000)
      const MAX_RECIPIENTS = 1000;
      if (recipients.length > MAX_RECIPIENTS) {
        await ctx.reply(
          `Превышен лимит получателей: ${recipients.length} (максимум ${MAX_RECIPIENTS}). Ограничьте список получателей.`
        );
        delete ctx.session.adminAction;
        return;
      }

      ctx.session.adminAction = {
        type: "broadcast",
        payload: { kind: "text", text: sanitizedMessage },
        recipients,
      };

      const sample = recipients.slice(0, 6).join(", ");
      const keyboard = Markup.inlineKeyboard([
        [
          Markup.button.callback(
            "Подтвердить рассылку ✅",
            "admin:broadcast_confirm"
          ),
        ],
        [Markup.button.callback("Отменить ❌", "admin:broadcast_cancel")],
      ]);

      await ctx.reply(
        `Предпросмотр рассылки:\n\nТекст:\n${message}\n\nПолучателей: ${recipients.length}\nПримеры: ${sample}\n\nПодтвердите отправку или отмените.`,
        keyboard
      );

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
    if (action !== "broadcast") return next();

    const photos = ctx.message.photo || [];
    if (!photos.length) return next();
    // Выбираем наибольшее доступное превью (последний элемент массива)
    const best = photos[photos.length - 1];
    const fileId = best.file_id;
    const caption = (ctx.message.caption || "").trim();

    const clients = await sheetsService.getAllClients();
    const bans = await adminService.getBans();
    const recipients = clients
      .filter((c) => c && c.telegramId)
      .map((c) => String(c.telegramId))
      .filter((id) => id && !bans.some((b) => String(b) === String(id)));

    if (!recipients.length) {
      await ctx.reply(
        "Нет получателей для рассылки (нет клиентов с telegramId или все в бане)."
      );
      delete ctx.session.adminAction;
      return;
    }

    ctx.session.adminAction = {
      type: "broadcast",
      payload: { kind: "photo", fileId, caption },
      recipients,
    };

    const sample = recipients.slice(0, 6).join(", ");
    const keyboard = Markup.inlineKeyboard([
      [
        Markup.button.callback(
          "Подтвердить рассылку ✅",
          "admin:broadcast_confirm"
        ),
      ],
      [Markup.button.callback("Отменить ❌", "admin:broadcast_cancel")],
    ]);

    await ctx.reply(
      "Предпросмотр фото-письма. Подпись:" +
        (caption ? `\n${caption}` : " (без подписи)")
    );
    await ctx.replyWithPhoto(fileId);
    await ctx.reply(
      `Получателей: ${recipients.length}\nПримеры: ${sample}\n\nПодтвердите отправку или отмените.`,
      keyboard
    );
  });

  return bot;
}

module.exports = {
  createBot,
};
