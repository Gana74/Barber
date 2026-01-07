// Инициализация Telegraf, сессий и сцен

const { Telegraf, Scenes, Markup } = require("telegraf");
const LocalSession = require("telegraf-session-local");
const fs = require("fs");
const path = require("path");
const { createBookingService, getServiceList } = require("../services/booking");
const adminService = require("../services/admin");
const { createBookingScene } = require("./scenes/bookingScene");
const { formatDate } = require("../utils/formatDate");

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

  function isManager(ctx) {
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
      .map((s) => `- ${s.name} (${s.durationMin} мин)`)
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
        }\nid=${appointment.id}`
      );
    }
  });

  // --- Admin menu (manager only) ---
  // reply-style keyboard for admin (visual like user)
  const adminKeyboard = Markup.keyboard([
    ["Просмотр записей", "Статистика"],
    ["Отменить запись (по ID)"],
    ["Забанить пользователя", "Разбанить пользователя"],
    ["Массовая рассылка"],
    ["Вернуться в пользовательский режим"],
  ]).resize();

  bot.command("admin", async (ctx) => {
    if (!isManager(ctx)) return;
    ctx.session = ctx.session || {};
    ctx.session.mode = "admin";
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
    if (!isManager(ctx)) return;
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
            `${a.id} — ${a.service} ${formatDate(a.date)} ${a.timeStart}-${
              a.timeEnd
            } — ${a.clientName} (${a.phone})`
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
      "cancel_booking",
      "ban",
      "unban",
      "broadcast",
    ]);

    if (inputActions.has(action)) {
      ctx.session.adminAction = { type: action };
      await ctx.reply(
        action === "broadcast"
          ? "Отправьте текст для рассылки или пришлите фото с подписью. Для отмены напишите /admin_cancel"
          : action === "cancel_booking"
          ? "Отправьте ID записи, которую нужно отменить. Для отмены напишите /admin_cancel"
          : action === "ban"
          ? "Отправьте Telegram ID или @username пользователя для бана. Для отмены напишите /admin_cancel"
          : "Отправьте Telegram ID пользователя для разбанивания. Для отмены напишите /admin_cancel"
      );
      return;
    }
  }

  // keep callback handlers for broadcast confirm/cancel
  bot.action(/admin:(.+)/, async (ctx, next) => {
    if (!isManager(ctx)) return next();
    const action = ctx.match[1];
    await ctx.answerCbQuery();
    await handleAdminAction(ctx, action);
    return next();
  });

  // map reply-keyboard presses to admin actions
  bot.hears("Просмотр записей", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "all_bookings");
    }
  });

  bot.hears("Статистика", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "stats");
    }
  });

  bot.hears("Отменить запись (по ID)", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "cancel_booking");
    }
  });

  bot.hears("Забанить пользователя", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "ban");
    }
  });

  bot.hears("Разбанить пользователя", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "unban");
    }
  });

  bot.hears("Массовая рассылка", async (ctx) => {
    if (!isManager(ctx)) return;
    if (ctx.session && ctx.session.mode === "admin") {
      await handleAdminAction(ctx, "broadcast");
    }
  });

  bot.hears("Вернуться в пользовательский режим", async (ctx) => {
    if (!isManager(ctx)) return;
    ctx.session = ctx.session || {};
    ctx.session.mode = "user";
    await ctx.reply(
      "Режим пользователя. Выберите действие:",
      Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]])
        .resize()
        .oneTime()
    );
  });

  bot.action("admin:broadcast_confirm", async (ctx) => {
    if (!isManager(ctx)) return;
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
    await ctx.reply(`Рассылка завершена. Отправлено: ${ok}. Ошибок: ${fail}.`);
    delete ctx.session.adminAction;
  });

  bot.action("admin:broadcast_cancel", async (ctx) => {
    if (!isManager(ctx)) return;
    await ctx.answerCbQuery();
    delete ctx.session.adminAction;
    await ctx.reply("Рассылка отменена.");
  });

  bot.command("admin_cancel", async (ctx) => {
    if (!isManager(ctx)) return;
    delete ctx.session.adminAction;
    await ctx.reply("Действие админа отменено.");
  });

  bot.on("text", async (ctx, next) => {
    if (!isManager(ctx) || !(ctx.session && ctx.session.mode === "admin"))
      return next();
    const action =
      ctx.session && ctx.session.adminAction && ctx.session.adminAction.type;
    if (!action) return next();

    const text = ctx.message.text && ctx.message.text.trim();

    if (action === "cancel_booking") {
      const id = text;
      const appointment = await sheetsService.getAppointmentById(id);
      if (!appointment) {
        await ctx.reply("Запись не найдена. /admin_cancel для отмены.");
        return;
      }
      const cancelledAtUtc = new Date().toISOString();
      const ok = await sheetsService.updateAppointmentStatus(
        id,
        bookingService.STATUSES.CANCELLED,
        { cancelledAtUtc }
      );
      if (!ok) {
        await ctx.reply("Не удалось отменить запись.");
      } else {
        await ctx.reply(`Запись ${id} отменена.`);
        try {
          if (calendarService && calendarService.deleteEventForAppointmentId) {
            await calendarService.deleteEventForAppointmentId(id);
          }
        } catch (e) {
          console.warn(
            "Calendar delete failed for appointment (admin cancel):",
            e.message || e
          );
        }
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
      if (!telegramId) {
        await ctx.reply("Пользователь не найден. /admin_cancel для отмены.");
        return;
      }
      await adminService.banUser(telegramId, "", sheetsService);
      await ctx.reply(`Пользователь ${telegramId} забанен.`);
      delete ctx.session.adminAction;
      return;
    }

    if (action === "unban") {
      const telegramId = text;
      if (!telegramId) {
        await ctx.reply("Укажите Telegram ID. /admin_cancel для отмены.");
        return;
      }
      await adminService.unbanUser(telegramId, sheetsService);
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

      ctx.session.adminAction = { type: "broadcast", payload: { kind: "text", text: message }, recipients };

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
    if (!isManager(ctx) || !(ctx.session && ctx.session.mode === "admin")) return next();
    const action = ctx.session && ctx.session.adminAction && ctx.session.adminAction.type;
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
      await ctx.reply("Нет получателей для рассылки (нет клиентов с telegramId или все в бане).");
      delete ctx.session.adminAction;
      return;
    }

    ctx.session.adminAction = { type: "broadcast", payload: { kind: "photo", fileId, caption }, recipients };

    const sample = recipients.slice(0, 6).join(", ");
    const keyboard = Markup.inlineKeyboard([
      [Markup.button.callback("Подтвердить рассылку ✅", "admin:broadcast_confirm")],
      [Markup.button.callback("Отменить ❌", "admin:broadcast_cancel")],
    ]);

    await ctx.reply("Предпросмотр фото-письма. Подпись:" + (caption ? `\n${caption}` : " (без подписи)"));
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
