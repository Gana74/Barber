// Инициализация Telegraf, сессий и сцен

const { Telegraf, Scenes, Markup } = require("telegraf");
const LocalSession = require("telegraf-session-local");
const fs = require("fs");
const path = require("path");
const { createBookingService, getServiceList } = require("../services/booking");
const { createBookingScene } = require("./scenes/bookingScene");

function createBot({ config, sheetsService }) {
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
            // Удаляем текущую сцену и курсор — вернём пользователя в обычное состояние
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

  const bookingService = createBookingService({ sheetsService, config });

  const stage = new Scenes.Stage([
    createBookingScene({ bookingService, sheetsService, config }),
  ]);

  bot.use(stage.middleware());

  bot.start(async (ctx) => {
    // Сбрасываем возможную зависшую сцену и состояние сессии при старте
    try {
      await ctx.scene.leave();
    } catch (e) {
      // игнорируем, если сцены не было
    }
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
    // На всякий случай выходим из любой текущей сцены и идём в бронирование
    try {
      await ctx.scene.leave();
    } catch (e) {}
    ctx.session = ctx.session || {};
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
      (app, idx) => `${idx + 1}. ${app.service} — ${app.date} ${app.timeStart}`
    );

    const keyboard = list.map((app) => [
      Markup.button.callback(
        `Отменить ${app.date} ${app.timeStart}`,
        `cancel_app:${app.id}`
      ),
    ]);

    await ctx.reply(
      `Будущие записи:\n\n${lines.join("\n")}`,
      Markup.inlineKeyboard(keyboard)
    );
  });

  // Команда на прямой запуск сцены записи
  bot.command("book", async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    await ctx.scene.enter("booking");
  });

  // Хелпер: список услуг по команде
  bot.command("services", async (ctx) => {
    const services = getServiceList();
    const text = services
      .map((s) => `- ${s.name} (${s.durationMin} мин)`)
      .join("\n");
    await ctx.reply(`Список услуг:\n${text}`);
  });

  // Команда отмены и сброса сцены
  bot.command("cancel", async (ctx) => {
    try {
      await ctx.scene.leave();
    } catch (e) {}
    await ctx.reply(
      "Отменено. Для новой записи используй /book",
      Markup.removeKeyboard()
    );
  });

  // Обработка inline-кнопки «Отменить запись»
  bot.action(/cancel_app:(.+)/, async (ctx) => {
    const id = ctx.match[1];
    await ctx.answerCbQuery("Отменяем запись...");

    const appointment = await sheetsService.getAppointmentById(id);
    if (!appointment || appointment.status !== "active") {
      await ctx.reply(
        "Не удалось отменить запись: она не найдена или уже отменена."
      );
      return;
    }

    // Комментарий: не даём отменять чужие записи
    if (String(appointment.telegramId) !== String(ctx.from.id)) {
      await ctx.reply("Эта запись принадлежит другому пользователю.");
      return;
    }

    const cancelledAtUtc = new Date().toISOString();
    const ok = await sheetsService.updateAppointmentStatus(id, "cancelled", {
      cancelledAtUtc,
    });

    if (!ok) {
      await ctx.reply(
        "Не удалось отменить запись: она не найдена или уже отменена."
      );
      return;
    }

    await ctx.reply(
      `Запись на ${appointment.date} ${appointment.timeStart} отменена. Спасибо, что предупредил(а)!`
    );

    if (config.managerChatId) {
      await ctx.telegram.sendMessage(
        config.managerChatId,
        `Клиент отменил запись:\nУслуга: ${appointment.service}\nДата: ${appointment.date}\nВремя: ${appointment.timeStart}–${appointment.timeEnd}\nКлиент: ${appointment.clientName}\nТелефон: ${appointment.phone}\nid=${appointment.id}`
      );
    }
  });

  return bot;
}

module.exports = {
  createBot,
};
