// Ежедневные напоминания о завтрашних записях + напоминание за 2 часа до услуги

const cron = require("node-cron");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezonePlugin = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezonePlugin);

// Комментарий: простая in-memory защита от дублей напоминаний за 2 часа
const twoHourRemindedIds = new Set();

function setupReminders({ bot, config, sheetsService }) {
  // Комментарий: читаем таймзону салона из таблицы (асинхронно внутри cron)
  cron.schedule(
    "0 10 * * *",
    async () => {
      try {
        const timezone = await sheetsService.getTimezone();
        const nowTz = dayjs().tz(timezone);
        const tomorrow = nowTz.add(1, "day").format("YYYY-MM-DD");

        const appointments = await sheetsService.getAppointmentsByDate(
          tomorrow
        );

        for (const app of appointments) {
          if (!app.telegramId) continue;

          const msg = [
            "Напоминание о записи в барбершоп:",
            `Услуга: ${app.service}`,
            `Дата: ${app.date}`,
            `Время: ${app.timeStart}–${app.timeEnd}`,
            "",
            "Если нужно отменить или перенести запись — напишите сюда или воспользуйтесь кнопкой отмены в боте.",
          ].join("\n");

          await bot.telegram.sendMessage(app.telegramId, msg);
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error("Error in daily reminders cron job:", err);
      }
    },
    {
      timezone: config.defaultTimezone,
    }
  );

  // Напоминание за 2 часа до услуги: проверяем каждые 5 минут
  cron.schedule(
    "*/5 * * * *",
    async () => {
      try {
        const timezone = await sheetsService.getTimezone();
        const nowTz = dayjs().tz(timezone);

        // Берём сегодняшние и завтрашние записи, чтобы покрыть переход через полночь
        const today = nowTz.format("YYYY-MM-DD");
        const tomorrow = nowTz.add(1, "day").format("YYYY-MM-DD");

        const todayApps = await sheetsService.getAppointmentsByDate(today);
        const tomorrowApps = await sheetsService.getAppointmentsByDate(
          tomorrow
        );

        const all = [...todayApps, ...tomorrowApps];

        for (const app of all) {
          if (!app.telegramId) continue;

          const start = dayjs.tz(`${app.date}T${app.timeStart}:00`, timezone);
          const diffMinutes = start.diff(nowTz, "minute");

          // Окно: от 115 до 125 минут до начала (±5 минут из-за периодичности cron)
          if (diffMinutes <= 125 && diffMinutes >= 115) {
            if (twoHourRemindedIds.has(app.id)) continue;

            const msg = [
              "Напоминание: до твоей записи в барбершоп осталось ~2 часа.",
              `Услуга: ${app.service}`,
              `Дата: ${app.date}`,
              `Время: ${app.timeStart}–${app.timeEnd}`,
              "",
              "Если планы изменились — можно отменить запись через бота.",
            ].join("\n");

            await bot.telegram.sendMessage(app.telegramId, msg);
            twoHourRemindedIds.add(app.id);
          }
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error("Error in 2-hours reminders cron job:", err);
      }
    },
    {
      timezone: config.defaultTimezone,
    }
  );
}

module.exports = {
  setupReminders,
};
