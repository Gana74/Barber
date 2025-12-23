// Сцена записи: выбор услуги -> даты -> времени -> контактов -> подтверждение

const { Scenes, Markup } = require("telegraf");
const dayjs = require("dayjs");
const timezonePlugin = require("dayjs/plugin/timezone");

dayjs.extend(timezonePlugin);

function formatDateLabel(d) {
  return d.format("DD.MM (dd)");
}

function formatDateValue(d) {
  return d.format("YYYY-MM-DD");
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

      const days = [];
      for (let i = 0; i < 7; i += 1) {
        days.push(now.add(i, "day"));
      }

      const keyboard = days.map((d) => [
        Markup.button.callback(
          formatDateLabel(d),
          `date:${formatDateValue(d)}`
        ),
      ]);

      // Добавляем кнопку "Назад" в конец
      keyboard.push([Markup.button.callback("Назад ⬅️", "back_to_services")]);

      await ctx.reply("Выбери дату:", Markup.inlineKeyboard(keyboard));

      return ctx.wizard.next();
    },
    // Шаг 3: выбор времени (обработка callback с датой)
    async (ctx) => {
      if (!("callback_query" in ctx.update)) {
        await ctx.reply("Выбери дату по кнопке ниже.");
        return;
      }

      const data = ctx.update.callback_query.data;
      if (data === "back_to_services") {
        // Обработка кнопки "Назад" - возвращаемся к выбору услуги
        delete ctx.wizard.state.booking.dateStr;
        await ctx.answerCbQuery("Возвращаемся к выбору услуги");
        return ctx.wizard.selectStep(0);
      }

      if (!data.startsWith("date:")) {
        await ctx.answerCbQuery();
        return;
      }

      const dateStr = data.slice("date:".length);
      ctx.wizard.state.booking.dateStr = dateStr;

      await ctx.answerCbQuery();

      const { serviceKey } = ctx.wizard.state.booking;

      const { slots } = await bookingService.getAvailableSlotsForService(
        serviceKey,
        dateStr
      );

      if (!slots.length) {
        await ctx.reply(
          "На этот день нет свободных слотов. Попробуй выбрать другую дату командой /book.",
          Markup.removeKeyboard()
        );
        return ctx.scene.leave();
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
        return ctx.wizard.selectStep(1);
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
          `Дата: ${dateStr}`,
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
        `Дата: ${appointment.date}`,
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
          `Дата: ${appointment.date}`,
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
