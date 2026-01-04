// Сцена записи: выбор услуги -> даты -> времени -> контактов -> подтверждение

const { Scenes, Markup } = require("telegraf");
const dayjs = require("dayjs");
const timezonePlugin = require("dayjs/plugin/timezone");

dayjs.extend(timezonePlugin);
const { formatDate } = require("../../utils/formatDate");

function formatDateLabel(d) {
  return d.format("DD.MM (dd)");
}

function formatDateValue(d) {
  return d.format("YYYY-MM-DD");
}

function monthLabel(d) {
  return d.format("MMMM YYYY");
}

function createCalendarKeyboard(baseDate, timezone, allowedMonths) {
  const start = dayjs(baseDate).tz(timezone).startOf("month");
  const end = dayjs(baseDate).tz(timezone).endOf("month");

  const firstWeekday = start.day();

  // Weekday short names (Ru locale assumed in dayjs setup)
  const weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"];

  const rows = [];

  // Navigation row
  const prev = start.subtract(1, "month");
  const next = start.add(1, "month");
  const prevKey = prev.format("YYYY-MM");
  const nextKey = next.format("YYYY-MM");
  const canPrev = !allowedMonths || allowedMonths.includes(prevKey);
  const canNext = !allowedMonths || allowedMonths.includes(nextKey);

  rows.push([
    Markup.button.callback("⬅️", canPrev ? `cal:${prevKey}` : "cal:noop"),
    Markup.button.callback(monthLabel(start), `cal:noop`),
    Markup.button.callback("➡️", canNext ? `cal:${nextKey}` : "cal:noop"),
  ]);

  // Weekday header
  rows.push(weekdays.map((w) => Markup.button.callback(w, "cal:noop")));

  // Fill blanks before first day (make Monday the first column)
  let day = start.startOf("month");
  const offset = (firstWeekday + 6) % 7; // convert Sunday(0) to position 6
  for (let i = 0; i < offset; i += 1) day = day.subtract(1, "day");

  // Build 6 weeks grid
  for (let week = 0; week < 6; week += 1) {
    const weekRow = [];
    for (let d = 0; d < 7; d += 1) {
      const isCurrentMonth = day.month() === start.month();
      const monthKey = monthKeyFromDate(day);
      const monthAllowed = !allowedMonths || allowedMonths.includes(monthKey);
      const today = dayjs().tz(timezone).startOf("day");
      const isPast = day.isBefore(today, "day");

      // Не показываем прошедшие дни и дни из неразрешённых месяцев
      const showDate = isCurrentMonth && monthAllowed && !isPast;
      const label = showDate ? `${day.date()}` : " ";
      const callback = showDate
        ? `date:${day.format("YYYY-MM-DD")}`
        : "cal:noop";
      weekRow.push(Markup.button.callback(label, callback));
      day = day.add(1, "day");
    }
    rows.push(weekRow);
  }

  // Back button
  rows.push([Markup.button.callback("Назад ⬅️", "back_to_services")]);

  return Markup.inlineKeyboard(rows);
}

// Вернёт список ключей месяцев (формат YYYY-MM), в которых разрешена запись.
// Правило: запись только в текущем месяце; начиная с 15-го числа — также открывается следующий месяц.
function getAllowedMonthKeys(timezone) {
  const now = dayjs().tz(timezone);
  const current = now.startOf("month");
  const keys = [current.format("YYYY-MM")];
  if (now.date() >= 15) {
    keys.push(current.add(1, "month").format("YYYY-MM"));
  }
  return keys;
}

function monthKeyFromDate(d) {
  return dayjs(d).format("YYYY-MM");
}

function createBookingScene({ bookingService, sheetsService, config }) {
  const bookingScene = new Scenes.WizardScene(
    "booking",
    // Шаг 1: выбор услуги
    async (ctx) => {
      const services = bookingService.getServiceList();
      const buttons = services.map((s) => [s.name]);

      // Добавляем кнопку "Назад" в конец
      buttons.push(["Назад ⬅️"]);

      ctx.wizard.state.booking = {};

      await ctx.reply(
        "Выбери услугу:",
        Markup.keyboard(buttons).oneTime().resize()
      );
      return ctx.wizard.next();
    },
    // Шаг 2: обработка выбора услуги и предложение даты
    async (ctx) => {
      const text = ctx.message && ctx.message.text;
      const services = bookingService.getServiceList();

      // Обработка кнопки "Назад": возвращаем пользователя в главное меню
      if (text === "Назад ⬅️") {
        try {
          await ctx.scene.leave();
        } catch (e) {
          // игнорируем ошибки при выходе из сцены
        }

        await ctx.reply(
          "Ок, возвращаю в главное меню.",
          Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize()
        );

        return;
      }

      const service = services.find((s) => s.name === text);

      if (!service) {
        await ctx.reply("Пожалуйста, выбери услугу из списка кнопок.");
        return;
      }

      ctx.wizard.state.booking.serviceKey = service.key;

      const timezone = await sheetsService.getTimezone();
      const now = dayjs().tz(timezone);
      const allowed = getAllowedMonthKeys(timezone);

      const calendar = createCalendarKeyboard(now, timezone, allowed);

      await ctx.reply("Выбери дату:", calendar);

      return ctx.wizard.next();
    },
    // Шаг 3: выбор времени (обработка callback с датой)
    async (ctx) => {
      if (!("callback_query" in ctx.update)) {
        await ctx.reply("Выбери дату по кнопке ниже.");
        return;
      }

      const data = ctx.update.callback_query.data;

      // Навигация назад к услугам
      if (data === "back_to_services") {
        delete ctx.wizard.state.booking.dateStr;
        await ctx.answerCbQuery("Возвращаемся к выбору услуги");
        return ctx.wizard.selectStep(0);
      }

      // Обработка навигации календаря (смена месяца)
      if (data && data.startsWith("cal:")) {
        await ctx.answerCbQuery();
        const payload = data.slice("cal:".length);
        if (payload === "noop") return;

        // payload expected as YYYY-MM
        const timezone = await sheetsService.getTimezone();
        const allowed = getAllowedMonthKeys(timezone);

        if (!allowed.includes(payload)) {
          await ctx.answerCbQuery("Запись на этот месяц недоступна.");
          return;
        }

        const base = dayjs.tz(`${payload}-01`, timezone);
        const calendar = createCalendarKeyboard(base, timezone, allowed);

        try {
          await ctx.editMessageReplyMarkup(calendar.reply_markup);
        } catch (e) {
          // если не получилось отредактировать (например, нет прав), отправим новый
          await ctx.reply("Выбери дату:", calendar);
        }

        return;
      }

      // Игнорируем noop и другие не-date колбэки
      if (!data || !data.startsWith("date:")) {
        await ctx.answerCbQuery();
        return;
      }

      const dateStr = data.slice("date:".length);
      // Проверяем, что выбранный месяц разрешён
      const timezone = await sheetsService.getTimezone();
      const allowed = getAllowedMonthKeys(timezone);
      const monthKey = monthKeyFromDate(dateStr);
      if (!allowed.includes(monthKey)) {
        await ctx.answerCbQuery("Выбрана недоступная дата");
        const base = dayjs.tz(dateStr, timezone);
        const calendar = createCalendarKeyboard(base, timezone, allowed);
        try {
          await ctx.reply("Выбери дату:", calendar);
        } catch (e) {}
        return;
      }
      ctx.wizard.state.booking.dateStr = dateStr;

      await ctx.answerCbQuery();

      const { serviceKey } = ctx.wizard.state.booking;

      const { slots } = await bookingService.getAvailableSlotsForService(
        serviceKey,
        dateStr
      );

      if (!slots.length) {
        // Уточняем, возможно день закрыт или просто нет слотов
        const wh =
          (sheetsService.getWorkHoursForDate &&
            (await sheetsService.getWorkHoursForDate(dateStr))) ||
          null;

        if (!wh) {
          await ctx.reply("В этот день барбершоп закрыт. Выбери другую дату.");
        } else {
          await ctx.reply(
            `На этот день нет свободных слотов. Рабочие часы: ${wh.start}–${wh.end}. Попробуй выбрать другую дату.`
          );
        }

        // Покажем календарь снова, чтобы пользователь мог выбрать другую дату
        const timezone = await sheetsService.getTimezone();
        const allowed = getAllowedMonthKeys(timezone);
        const base = dayjs.tz(dateStr, timezone);
        const calendar = createCalendarKeyboard(base, timezone, allowed);

        try {
          await ctx.reply("Выбери дату:", calendar);
        } catch (e) {
          // Игнорируем ошибки отправки повторного календаря
        }

        // Остаёмся в сцене (шаг обработки дат), чтобы обработать следующий callback
        return;
      }

      const keyboard = [];
      let row = [];

      slots.forEach((slot, idx) => {
        const buttonData = `time:${slot.timeStr}`;
        row.push(Markup.button.callback(slot.timeStr, buttonData));
        if ((idx + 1) % 4 === 0) {
          keyboard.push(row);
          row = [];
        }
      });
      if (row.length) keyboard.push(row);

      // Добавляем кнопку "Назад" в конец
      keyboard.push([Markup.button.callback("Назад ⬅️", "back_to_dates")]);

      await ctx.reply("Выбери время:", Markup.inlineKeyboard(keyboard));

      return ctx.wizard.next();
    },
    // Шаг 4: контакты (обработка времени)
    async (ctx) => {
      if (!("callback_query" in ctx.update)) {
        await ctx.reply("Выбери время по кнопке ниже.");
        return;
      }

      const data = ctx.update.callback_query.data;

      if (data === "back_to_dates") {
        // Обработка кнопки "Назад" - возвращаемся к выбору даты
        delete ctx.wizard.state.booking.timeStr;
        await ctx.answerCbQuery("Возвращаемся к выбору даты");

        // Покажем календарь снова (в том месяце, который был выбран, если есть)
        const timezone = await sheetsService.getTimezone();
        const allowed = getAllowedMonthKeys(timezone);
        const dateBase =
          (ctx.wizard.state.booking && ctx.wizard.state.booking.dateStr) ||
          dayjs().tz(timezone).format("YYYY-MM-DD");
        const base = dayjs.tz(dateBase, timezone);
        const calendar = createCalendarKeyboard(base, timezone, allowed);

        try {
          await ctx.reply("Выбери дату:", calendar);
        } catch (e) {
          // Игнорируем ошибки отправки
        }

        return ctx.wizard.selectStep(2);
      }

      if (!data.startsWith("time:")) {
        console.log(
          "DEBUG: Data does not start with 'time:', answering callback and staying on same step"
        );
        await ctx.answerCbQuery();
        return;
      }

      const timeStr = data.slice("time:".length);
      ctx.wizard.state.booking.timeStr = timeStr;

      await ctx.answerCbQuery();

      const name = ctx.from.first_name || "";

      await ctx.reply(
        "Введи, пожалуйста, своё имя (можно оставить как в профиле), затем отправь свой контакт по кнопке ниже."
      );
      ctx.wizard.state.booking.step = "name";

      return ctx.wizard.next();
    },
    // Шаг 5: имя и контакт + комментарий
    async (ctx) => {
      const booking = ctx.wizard.state.booking;

      // Обработка отправки контакта
      if (ctx.message && ctx.message.contact) {
        if (booking.step === "contact") {
          const phone = ctx.message.contact.phone_number;
          booking.phone = phone.startsWith("+") ? phone : `+${phone}`;
          booking.step = "comment";
          await ctx.reply(
            'Если хочешь, добавь комментарий к записи. Или напиши "-".'
          );
          return;
        }
      }

      if (booking.step === "name") {
        booking.name = ctx.message.text.trim();
        booking.step = "contact";
        await ctx.reply(
          "Теперь отправь свой контакт по кнопке ниже:",
          Markup.keyboard([
            [Markup.button.contactRequest("Отправить контакт 📱")],
          ])
            .oneTime()
            .resize()
        );
        return;
      }

      if (booking.step === "comment") {
        const comment = ctx.message.text.trim();
        booking.comment = comment === "-" ? "" : comment;

        const { serviceKey, dateStr, timeStr, name, phone } = booking;
        const service = bookingService.getServiceByKey(serviceKey);

        const summary = [
          "Проверь, всё ли верно:",
          `Услуга: ${service.name}`,
          `Дата: ${formatDate(dateStr)}`,
          `Время: ${timeStr}`,
          `Имя: ${name}`,
          `Телефон: ${phone}`,
          `Комментарий: ${booking.comment || "нет"}`,
        ].join("\n");

        await ctx.reply(
          summary,
          Markup.inlineKeyboard([
            [Markup.button.callback("Подтвердить ✅", "confirm")],
            [Markup.button.callback("Отмена ❌", "cancel")],
          ])
        );

        // Комментарий: переводим визард на следующий шаг, чтобы обработать callback confirm/cancel
        booking.step = "confirm";
        return ctx.wizard.next();
      }

      await ctx.reply(
        "Что-то пошло не так, начнём заново: /book",
        Markup.removeKeyboard()
      );
      return ctx.scene.leave();
    },
    // Шаг 6: подтверждение (callback confirm/cancel)
    async (ctx) => {
      if (!("callback_query" in ctx.update)) {
        await ctx.reply("Подтверди или отмени запись по кнопкам.");
        return;
      }

      const data = ctx.update.callback_query.data;
      const booking = ctx.wizard.state.booking;

      if (data === "cancel") {
        await ctx.answerCbQuery("Запись отменена.");
        await ctx.reply(
          "Ок, ничего не записываю. Если нужно — начни заново: /book",
          Markup.removeKeyboard()
        );
        return ctx.scene.leave();
      }

      if (data !== "confirm") {
        await ctx.answerCbQuery();
        return;
      }

      await ctx.answerCbQuery("Создаём запись...");

      const { serviceKey, dateStr, timeStr } = booking;

      const result = await bookingService.bookAppointment({
        serviceKey,
        dateStr,
        timeStr,
        client: {
          name: booking.name,
          phone: booking.phone,
          username: ctx.from.username,
          telegramId: ctx.from.id,
          chatId: ctx.chat.id,
        },
        comment: booking.comment,
      });

      if (!result.ok) {
        if (result.reason === "limit_exceeded") {
          await ctx.reply(
            "Нельзя создать запись: превышен лимит — не более 3 записей в день от одного пользователя. Отмените ненужные записи или свяжитесь с администрацией."
          );
          return ctx.scene.leave();
        }

        if (result.reason === "slot_taken") {
          await ctx.reply(
            "К сожалению, пока мы бронировали, это время уже заняли. Выбери другое время на эту же дату."
          );

          // Возвращаем к выбору времени, сохраняя все остальные данные
          const { serviceKey, dateStr } = ctx.wizard.state.booking;

          // Очищаем выбранное время
          delete ctx.wizard.state.booking.timeStr;

          // Получаем обновленные доступные слоты
          const { slots } = await bookingService.getAvailableSlotsForService(
            serviceKey,
            dateStr
          );

          if (!slots.length) {
            await ctx.reply(
              "На этот день больше нет свободных слотов. Попробуй выбрать другую дату командой /book."
            );
            return ctx.scene.leave();
          }

          const keyboard = [];
          let row = [];

          slots.forEach((slot, idx) => {
            row.push(
              Markup.button.callback(slot.timeStr, `time:${slot.timeStr}`)
            );
            if ((idx + 1) % 4 === 0) {
              keyboard.push(row);
              row = [];
            }
          });
          if (row.length) keyboard.push(row);

          await ctx.reply("Выбери время:", Markup.inlineKeyboard(keyboard));

          // Возвращаемся к шагу выбора времени (шаг 3, так как индексация с 0)
          return ctx.wizard.selectStep(2);
        } else {
          if (result.reason === "closed") {
            await ctx.reply(
              "Нельзя создать запись: в этот день барбершоп закрыт. Попробуй другую дату."
            );
            return ctx.scene.leave();
          }
          await ctx.reply(
            "Не удалось создать запись из-за ошибки. Попробуй ещё раз позже.",
            Markup.removeKeyboard()
          );
          return ctx.scene.leave();
        }
      }

      const { appointment } = result;

      const confirmation = [
        "Готово! Ты записан(а) в барбершоп 👌",
        `Услуга: ${appointment.service}`,
        `Дата: ${formatDate(appointment.date)}`,
        `Время: ${appointment.timeStart}–${appointment.timeEnd}`,
        "",
        "Если планы изменятся — можно отменить запись по кнопке ниже.",
      ].join("\n");

      await ctx.reply(
        confirmation,
        Markup.inlineKeyboard([
          [
            Markup.button.callback(
              "Отменить эту запись ❌",
              `cancel_app:${appointment.id}`
            ),
          ],
        ])
      );

      // Уведомление менеджеру
      if (config.managerChatId) {
        const managerMsg = [
          "Новая запись:",
          `Услуга: ${appointment.service}`,
          `Дата: ${formatDate(appointment.date)}`,
          `Время: ${appointment.timeStart}–${appointment.timeEnd}`,
          `Клиент: ${appointment.clientName}`,
          `Телефон: ${appointment.phone}`,
          `TG: @${appointment.username || "нет"}`,
          `Комментарий: ${appointment.comment || "нет"}`,
          `ID: ${appointment.id}`,
          `Код отмены (служебно): ${appointment.cancelCode}`,
        ].join("\n");

        await ctx.telegram.sendMessage(config.managerChatId, managerMsg);
      }

      // Возвращаем пользователя в главное меню
      await ctx.reply(
        "Запись завершена! Вы вернулись в главное меню.",
        Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize()
      );

      return ctx.scene.leave();
    }
  );

  return bookingScene;
}

module.exports = {
  createBookingScene,
};
