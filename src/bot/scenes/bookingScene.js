// Сцена записи: выбор услуги -> даты -> времени -> контактов -> подтверждение

const { Scenes, Markup } = require("telegraf");
const dayjs = require("dayjs");
const timezonePlugin = require("dayjs/plugin/timezone");

dayjs.extend(timezonePlugin);
const { formatDate } = require("../../utils/formatDate");
const { validateName, sanitizeText } = require("../../utils/security");
const { logAction } = require("../../utils/logger");
const { safeSendMessage } = require("../../utils/safeMessaging");

function formatDateLabel(d) {
  return d.format("DD.MM (dd)");
}

function formatDateValue(d) {
  return d.format("YYYY-MM-DD");
}

function monthLabel(d) {
  return d.format("MMMM YYYY");
}

function createCalendarKeyboard(
  baseDate,
  timezone,
  allowedMonths,
  availableDates,
) {
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
      const showDate =
        isCurrentMonth &&
        monthAllowed &&
        !isPast &&
        (!availableDates || availableDates.has(day.format("YYYY-MM-DD")));
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

// Собираем множество дат с рабочими часами (без проверки слотов для ускорения)
async function buildAvailableDateSet({
  timezone,
  allowedMonths,
  sheetsService,
}) {
  const result = new Set();
  if (!allowedMonths || !allowedMonths.length) return result;

  // диапазон от первого разрешённого месяца до последнего
  const monthStarts = allowedMonths.map((k) => dayjs.tz(`${k}-01`, timezone));
  const rangeStart = monthStarts.reduce((min, d) =>
    d.isBefore(min) ? d : min,
  );
  const rangeEnd = monthStarts.reduce((max, d) => {
    const end = d.endOf("month");
    return end.isAfter(max) ? end : max;
  }, rangeStart.endOf("month"));

  const today = dayjs().tz(timezone).startOf("day");
  let cursor = rangeStart.startOf("day");

  // Проверяем рабочие часы для всех дат в разрешенных месяцах
  while (!cursor.isAfter(rangeEnd)) {
    const monthKey = monthKeyFromDate(cursor);
    if (!allowedMonths.includes(monthKey) || cursor.isBefore(today)) {
      cursor = cursor.add(1, "day");
      continue;
    }

    const dateStr = cursor.format("YYYY-MM-DD");

    // Проверяем только рабочие часы (быстро, без проверки слотов)
    try {
      const wh = await sheetsService.getWorkHoursForDate(dateStr);
      if (wh && wh.start && wh.end) {
        result.add(dateStr);
      }
    } catch (e) {
      // игнорируем ошибки, движемся дальше
    }

    cursor = cursor.add(1, "day");
  }

  return result;
}

function createBookingScene({ bookingService, sheetsService, config }) {
  const bookingScene = new Scenes.WizardScene(
    "booking",
    // Шаг 1: выбор услуги
    async (ctx) => {
      // Ранняя проверка бана пользователя
      try {
        if (
          ctx.from &&
          ctx.from.id &&
          sheetsService &&
          sheetsService.getUserBanStatus
        ) {
          const st = await sheetsService.getUserBanStatus(ctx.from.id);
          if (st && st.banned) {
            await ctx.reply(
              "Ваш аккаунт заблокирован для записи. Свяжитесь с администратором.",
              Markup.removeKeyboard(),
            );
            try {
              await ctx.scene.leave();
            } catch (e) {}
            return;
          }
        }
      } catch (e) {
        // игнорируем ошибки проверки
      }

      const services = bookingService.getServiceList();
      const buttons = services.map((s) => {
        const priceText = s.price !== null ? ` (${s.price} ₽)` : "";
        return [s.name + priceText];
      });

      // Добавляем кнопку "Назад" в конец
      buttons.push(["Назад ⬅️"]);

      ctx.wizard.state.booking = {};

      await ctx.reply(
        "Выбери услугу:",
        Markup.keyboard(buttons).oneTime().resize(),
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
          "Ок, возвращаю в главное меню.\n\n👇 Выберите действие с помощью кнопок ниже:",
          Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize(),
        );

        return;
      }

      // Ищем услугу по названию (кнопка может содержать цену в скобках)
      const service = services.find((s) => {
        const priceText = s.price !== null ? ` (${s.price} ₽)` : "";
        return text === s.name + priceText || text === s.name;
      });

      if (!service) {
        await ctx.reply("Пожалуйста, выбери услугу из списка кнопок.");
        return;
      }

      ctx.wizard.state.booking.serviceKey = service.key;

      // Важно: чтобы изменения в листе «Расписание» (рабочие часы) вступали сразу — сбрасываем кэш перед чтением
      if (sheetsService.invalidateWorkHoursCache) {
        try {
          sheetsService.invalidateWorkHoursCache();
        } catch (e) {}
      }

      const timezone = await sheetsService.getTimezone();
      const now = dayjs().tz(timezone);
      const allowed = getAllowedMonthKeys(timezone);
      const availableDates = await buildAvailableDateSet({
        timezone,
        allowedMonths: allowed,
        sheetsService,
      });

      ctx.wizard.state.booking.availableDates = Array.from(availableDates);

      const calendar = createCalendarKeyboard(
        now,
        timezone,
        allowed,
        availableDates,
      );

      await ctx.reply("Выбери дату:", calendar);

      return ctx.wizard.next();
    },
    // Шаг 3: выбор времени (обработка callback с датой)
    async (ctx) => {
      // Обработка текстового сообщения "Назад"
      if (ctx.message && ctx.message.text === "Назад ⬅️") {
        try {
          await ctx.scene.leave();
        } catch (e) {
          // игнорируем ошибки при выходе из сцены
        }
        await ctx.reply(
          "Ок, возвращаю в главное меню.\n\n👇 Выберите действие с помощью кнопок ниже:",
          Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize(),
        );
        return;
      }

      if (!("callback_query" in ctx.update)) {
        await ctx.reply("Выбери дату по кнопке ниже.");
        return;
      }

      const data = ctx.update.callback_query.data;

      // Навигация назад к услугам
      if (data === "back_to_services") {
        delete ctx.wizard.state.booking.dateStr;
        delete ctx.wizard.state.booking.availableDates;
        await ctx.answerCbQuery("Возвращаемся к выбору услуги");

        // Возвращаемся к шагу выбора услуги
        const services = bookingService.getServiceList();
        const buttons = services.map((s) => {
          const priceText = s.price !== null ? ` (${s.price} ₽)` : "";
          return [s.name + priceText];
        });
        buttons.push(["Назад ⬅️"]);

        await ctx.reply(
          "Выбери услугу:",
          Markup.keyboard(buttons).oneTime().resize(),
        );

        return ctx.wizard.selectStep(1);
      }

      // Обработка навигации календаря (смена месяца)
      if (data && data.startsWith("cal:")) {
        await ctx.answerCbQuery();
        const payload = data.slice("cal:".length);
        if (payload === "noop") return;

        // payload expected as YYYY-MM
        if (sheetsService.invalidateWorkHoursCache) {
          try {
            sheetsService.invalidateWorkHoursCache();
          } catch (e) {}
        }
        const timezone = await sheetsService.getTimezone();
        const allowed = getAllowedMonthKeys(timezone);
        const availableDates = await buildAvailableDateSet({
          timezone,
          allowedMonths: allowed,
          sheetsService,
        });
        ctx.wizard.state.booking.availableDates = Array.from(availableDates);

        if (!allowed.includes(payload)) {
          await ctx.answerCbQuery("Запись на этот месяц недоступна.");
          return;
        }

        const base = dayjs.tz(`${payload}-01`, timezone);
        const calendar = createCalendarKeyboard(
          base,
          timezone,
          allowed,
          availableDates,
        );

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
      // Проверяем, что выбранный месяц разрешён и дата не в прошлом
      const timezone = await sheetsService.getTimezone();
      const allowed = getAllowedMonthKeys(timezone);
      const monthKey = monthKeyFromDate(dateStr);
      const today = dayjs().tz(timezone).startOf("day");
      const selectedDate = dayjs.tz(dateStr, timezone).startOf("day");

      if (!allowed.includes(monthKey) || selectedDate.isBefore(today, "day")) {
        await ctx.answerCbQuery("Выбрана недоступная дата");
        const availableDates = new Set(
          (ctx.wizard.state.booking &&
            ctx.wizard.state.booking.availableDates) ||
            [],
        );
        const base = dayjs.tz(dateStr, timezone);
        const calendar = createCalendarKeyboard(
          base,
          timezone,
          allowed,
          availableDates,
        );
        try {
          await ctx.reply("Выбери дату:", calendar);
        } catch (e) {}
        return;
      }

      // Проверяем рабочие часы для выбранной даты
      const workHours = await sheetsService.getWorkHoursForDate(dateStr);
      if (!workHours || !workHours.start || !workHours.end) {
        await ctx.answerCbQuery("В этот день выходной");
        const availableDates = new Set(
          (ctx.wizard.state.booking &&
            ctx.wizard.state.booking.availableDates) ||
            [],
        );
        const base = dayjs.tz(dateStr, timezone);
        const calendar = createCalendarKeyboard(
          base,
          timezone,
          allowed,
          availableDates,
        );
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
        dateStr,
      );

      if (!slots.length) {
        // Уточняем, возможно день закрыт или просто нет слотов
        const wh =
          (sheetsService.getWorkHoursForDate &&
            (await sheetsService.getWorkHoursForDate(dateStr))) ||
          null;

        if (!wh) {
          await ctx.reply("В этот день у меня выходной. Выбери другую дату.");
        } else {
          await ctx.reply(
            `На этот день нет свободных слотов. Рабочие часы: ${wh.start}–${wh.end}. Попробуй выбрать другую дату.`,
          );
        }

        // Покажем календарь снова, чтобы пользователь мог выбрать другую дату
        const timezone = await sheetsService.getTimezone();
        const allowed = getAllowedMonthKeys(timezone);
        const base = dayjs.tz(dateStr, timezone);
        const availableDates = new Set(
          (ctx.wizard.state.booking &&
            ctx.wizard.state.booking.availableDates) ||
            [],
        );
        const calendar = createCalendarKeyboard(
          base,
          timezone,
          allowed,
          availableDates,
        );

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
        const availableDates = new Set(
          (ctx.wizard.state.booking &&
            ctx.wizard.state.booking.availableDates) ||
            [],
        );
        const calendar = createCalendarKeyboard(
          base,
          timezone,
          allowed,
          availableDates,
        );

        try {
          await ctx.reply("Выбери дату:", calendar);
        } catch (e) {
          // Игнорируем ошибки отправки
        }

        return ctx.wizard.selectStep(2);
      }

      if (!data.startsWith("time:")) {
        console.log(
          "DEBUG: Data does not start with 'time:', answering callback and staying on same step",
        );
        await ctx.answerCbQuery();
        return;
      }

      const timeStr = data.slice("time:".length);
      ctx.wizard.state.booking.timeStr = timeStr;

      await ctx.answerCbQuery();

      const name = ctx.from.first_name || "";

      await ctx.reply(
        "Введи, пожалуйста, своё имя (можно оставить как в профиле), затем отправь свой контакт по кнопке ниже.",
      );
      ctx.wizard.state.booking.step = "name";

      return ctx.wizard.next();
    },
    // Шаг 5: имя и контакт + комментарий
    async (ctx) => {
      const booking = ctx.wizard.state.booking;

      // Обработка callback_query (если пользователь нажал на кнопку времени)
      if ("callback_query" in ctx.update) {
        const data = ctx.update.callback_query.data;

        // Если пользователь выбрал другое время, возвращаемся к шагу выбора времени
        if (data && data.startsWith("time:")) {
          const timeStr = data.slice("time:".length);
          ctx.wizard.state.booking.timeStr = timeStr;
          await ctx.answerCbQuery();
          await ctx.reply(
            "Введи, пожалуйста, своё имя (можно оставить как в профиле), затем отправь свой контакт по кнопке ниже.",
          );
          ctx.wizard.state.booking.step = "name";
          return; // Остаемся на том же шаге
        }

        // Обработка кнопки "Назад" к выбору времени
        if (data === "back_to_dates") {
          delete ctx.wizard.state.booking.timeStr;
          await ctx.answerCbQuery("Возвращаемся к выбору времени");

          const { serviceKey, dateStr } = ctx.wizard.state.booking;
          const { slots } = await bookingService.getAvailableSlotsForService(
            serviceKey,
            dateStr,
          );

          const keyboard = [];
          let row = [];
          slots.forEach((slot, idx) => {
            row.push(
              Markup.button.callback(slot.timeStr, `time:${slot.timeStr}`),
            );
            if ((idx + 1) % 4 === 0) {
              keyboard.push(row);
              row = [];
            }
          });
          if (row.length) keyboard.push(row);
          keyboard.push([Markup.button.callback("Назад ⬅️", "back_to_dates")]);

          await ctx.reply("Выбери время:", Markup.inlineKeyboard(keyboard));
          return ctx.wizard.selectStep(3); // Возвращаемся к шагу выбора времени
        }

        // Для других callback_query просто отвечаем и игнорируем
        await ctx.answerCbQuery();
        return;
      }

      // Обработка отправки контакта
      if (ctx.message && ctx.message.contact) {
        if (booking.step === "contact") {
          const phone = ctx.message.contact.phone_number;
          booking.phone = phone.startsWith("+") ? phone : `+${phone}`;
          booking.step = "comment";
          await ctx.reply(
            'Для продолжения записи добавь комментарий. Или напиши "-".',
            Markup.removeKeyboard(),
          );
          return;
        }
      }

      if (booking.step === "name") {
        // Проверяем, что это текстовое сообщение
        if (!ctx.message || !ctx.message.text) {
          await ctx.reply("Пожалуйста, введите своё имя текстом.");
          return;
        }

        const nameInput = ctx.message.text.trim();

        // Валидация имени: длина 1-50 символов
        if (!validateName(nameInput, 1, 50)) {
          await ctx.reply(
            "Имя должно содержать от 1 до 50 символов и состоять только из букв, пробелов, дефисов и апострофов. Попробуйте снова.",
          );
          return;
        }

        // Санитизация имени
        booking.name = sanitizeText(nameInput, 50);
        booking.step = "contact";
        await ctx.reply(
          "Теперь отправь свой контакт по кнопке ниже:",
          Markup.keyboard([
            [Markup.button.contactRequest("Отправить контакт 📱")],
          ])
            .oneTime()
            .resize(),
        );
        return;
      }

      if (booking.step === "comment") {
        // Проверяем, что это текстовое сообщение
        if (!ctx.message || !ctx.message.text) {
          await ctx.reply(
            'Пожалуйста, введите комментарий текстом или напишите "-" для пропуска.',
          );
          return;
        }

        const commentInput = ctx.message.text.trim();

        // Обработка пустого комментария
        if (commentInput === "-") {
          booking.comment = "";
        } else {
          // Валидация и санитизация комментария (максимум 200 символов)
          const sanitizedComment = sanitizeText(commentInput, 200);
          if (sanitizedComment.length === 0 && commentInput.length > 0) {
            await ctx.reply(
              "Комментарий содержит недопустимые символы. Попробуйте снова или напишите '-' для пропуска.",
            );
            return;
          }
          booking.comment = sanitizedComment;
        }

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
          ]),
        );

        // Комментарий: переводим визард на следующий шаг, чтобы обработать callback confirm/cancel
        booking.step = "confirm";
        return ctx.wizard.next();
      }

      await ctx.reply(
        "Что-то пошло не так, начнём заново: /book",
        Markup.removeKeyboard(),
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
          Markup.removeKeyboard(),
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
        },
        comment: booking.comment,
      });

      if (!result.ok) {
        // Логирование неудачной попытки создания записи
        logAction(
          ctx.from.id,
          "appointment_creation_failed",
          {
            reason: result.reason,
            serviceKey,
            dateStr,
            timeStr,
          },
          "failed",
        );

        if (result.reason === "limit_exceeded") {
          const existingCount = result.existingCount || 3;
          await ctx.reply(
            `❌ Нельзя создать запись: превышен лимит!\n\n` +
              `У вас уже ${existingCount} активных записей.\n` +
              `Ограничение: не более 3 активных записей от одного пользователя.\n\n` +
              `Пожалуйста, отмените ненужные записи через "Мои записи" или свяжитесь с администрацией.`,
            Markup.removeKeyboard(),
          );
          await ctx.reply(
            "Вы вернулись в главное меню.\n\n👇 Выберите действие с помощью кнопок ниже:",
            Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize(),
          );
          return ctx.scene.leave();
        }

        if (result.reason === "slot_taken") {
          await ctx.reply(
            "К сожалению, пока мы бронировали, это время уже заняли. Выбери другое время на эту же дату.",
          );

          // Возвращаем к выбору времени, сохраняя все остальные данные
          const { serviceKey, dateStr } = ctx.wizard.state.booking;

          // Очищаем выбранное время
          delete ctx.wizard.state.booking.timeStr;

          // Получаем обновленные доступные слоты
          const { slots } = await bookingService.getAvailableSlotsForService(
            serviceKey,
            dateStr,
          );

          if (!slots.length) {
            await ctx.reply(
              "На этот день больше нет свободных слотов. Попробуй выбрать другую дату командой /book.",
            );
            return ctx.scene.leave();
          }

          const keyboard = [];
          let row = [];

          slots.forEach((slot, idx) => {
            row.push(
              Markup.button.callback(slot.timeStr, `time:${slot.timeStr}`),
            );
            if ((idx + 1) % 4 === 0) {
              keyboard.push(row);
              row = [];
            }
          });
          if (row.length) keyboard.push(row);

          // Добавляем кнопку "Назад" для возврата к выбору даты
          keyboard.push([Markup.button.callback("Назад ⬅️", "back_to_dates")]);

          await ctx.reply("Выбери время:", Markup.inlineKeyboard(keyboard));

          // Возвращаемся к шагу выбора времени (шаг 4, индексация с 0)
          return ctx.wizard.selectStep(3);
        } else {
          if (result.reason === "closed") {
            await ctx.reply(
              "Нельзя создать запись: в этот день у меня выходной. Попробуй другую дату.",
            );
            return ctx.scene.leave();
          }
          await ctx.reply(
            "Не удалось создать запись из-за ошибки. Попробуй ещё раз позже.",
            Markup.removeKeyboard(),
          );
          return ctx.scene.leave();
        }
      }

      const { appointment } = result;

      // Логирование успешного создания записи
      logAction(
        ctx.from.id,
        "appointment_created",
        {
          appointmentId: appointment.id,
          service: appointment.service,
          date: appointment.date,
          timeStart: appointment.timeStart,
          timeEnd: appointment.timeEnd,
        },
        "success",
      );

      const confirmation = [
        "Готово! Ты записан(а)👌",
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
              `cancel_app:${appointment.id}`,
            ),
          ],
        ]),
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
          `Код отмены: ${appointment.cancelCode}`,
        ].join("\n");

        // Безопасная отправка с обработкой ошибок
        await safeSendMessage(ctx.telegram, config.managerChatId, managerMsg);
      }

      // Возвращаем пользователя в главное меню
      await ctx.reply(
        "Запись завершена! Вы вернулись в главное меню.\n\n👇 Выберите действие с помощью кнопок ниже:",
        Markup.keyboard([["Записаться 💇‍♂️"], ["Мои записи"]]).resize(),
      );

      return ctx.scene.leave();
    },
  );

  return bookingScene;
}

module.exports = {
  createBookingScene,
};
