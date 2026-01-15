// Ежедневные напоминания о завтрашних записях + напоминание за 2 часа до услуги + автоматическое завершение записей

const cron = require("node-cron");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezonePlugin = require("dayjs/plugin/timezone");
const { createBookingService } = require("./booking");
const { formatDate } = require("../utils/formatDate");

dayjs.extend(utc);
dayjs.extend(timezonePlugin);

// Комментарий: простая in-memory защита от дублей напоминаний за 2 часа
const twoHourRemindedIds = new Set();

// Очистка старых ID каждый день в полночь
function setupReminderCleanup() {
  cron.schedule(
    "0 0 * * *",
    () => {
      twoHourRemindedIds.clear();
    },
    {
      timezone: "UTC",
    }
  );
}

function setupReminders({
  bot,
  config,
  sheetsService,
  bookingService,
  calendarService,
}) {
  // Комментарий: читаем таймзону салона из таблицы (асинхронно внутри cron)

  // Создаем bookingService если не передан (для доступа к STATUSES)
  const booking =
    bookingService ||
    createBookingService({ sheetsService, config, calendarService });

  // Инициализируем очистку кэша напоминаний
  setupReminderCleanup();

  // Напоминания за день записи (в 10:00 по времени салона)
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

        // Фильтруем только активные записи
        const activeAppointments = appointments.filter(
          (app) => app.status === booking.STATUSES.ACTIVE
        );

        let sentCount = 0;
        let errorCount = 0;

        for (const app of activeAppointments) {
          if (!app.telegramId) continue;

          // Пропускаем напоминания для дат, где салон закрыт (на всякий случай)
          if (sheetsService.getWorkHoursForDate) {
            const wh = await sheetsService.getWorkHoursForDate(app.date);
            if (!wh) continue;
          }

          const msg = [
            "💈 *Напоминание о записи в барбершоп*",
            "",
            `📅 *Дата:* ${app.date}`,
            `⏰ *Время:* ${app.timeStart}–${app.timeEnd}`,
            `✂️ *Услуга:* ${app.service}`,
            "",
            "🔧 *Если нужно отменить или перенести:*",
            "1. Откройте бота",
            "2. Нажмите кнопку *«Мои записи»*",
            "3. Выберите запись для отмены",
            "",
            "📞 *Контакты барбершопа:*",
            config.barberPhone || "+7 XXX XXX-XX-XX",
            config.barberAddress || "Адрес уточняйте у администратора",
          ].join("\n");
          try {
            await bot.telegram.sendMessage(app.telegramId, msg, {
              parse_mode: "Markdown",
            });
            sentCount++;

            // Добавляем небольшую задержку между сообщениями
            await new Promise((resolve) => setTimeout(resolve, 100));
          } catch (err) {
            errorCount++;
            console.error(
              `Ошибка отправки напоминания пользователю ${app.telegramId}:`,
              err.message
            );
          }
        }

        // Логируем результат
        console.log(
          `[${dayjs().format(
            "YYYY-MM-DD HH:mm:ss"
          )}] Напоминания за день записи отправлены: ${sentCount} успешно, ${errorCount} с ошибкой`
        );

        // Отправляем отчет менеджеру если настроен
        if (config.managerChatId && (sentCount > 0 || errorCount > 0)) {
          const reportMsg = [
            "📊 *Отчет по напоминаниям за день записи*",
            `📅 Дата: ${tomorrow}`,
            `✅ Отправлено: ${sentCount}`,
            `❌ Ошибок: ${errorCount}`,
            `⏰ Время отправки: ${dayjs().tz(timezone).format("HH:mm:ss")}`,
          ].join("\n");

          try {
            await bot.telegram.sendMessage(config.managerChatId, reportMsg, {
              parse_mode: "Markdown",
            });
          } catch (err) {
            console.error("Ошибка отправки отчета менеджеру:", err.message);
          }
        }
      } catch (err) {
        console.error("Критическая ошибка в напоминаниях за день записи:", err);
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
        const currentDate = nowTz.format("YYYY-MM-DD");

        // Берём сегодняшние и завтрашние записи, чтобы покрыть переход через полночь
        const todayApps = await sheetsService.getAppointmentsByDate(
          currentDate
        );
        const tomorrowDate = nowTz.add(1, "day").format("YYYY-MM-DD");
        const tomorrowApps = await sheetsService.getAppointmentsByDate(
          tomorrowDate
        );

        const all = [...todayApps, ...tomorrowApps];

        // Фильтруем только активные записи
        const activeApps = all.filter(
          (app) => app.status === booking.STATUSES.ACTIVE
        );

        let sentCount = 0;
        let errorCount = 0;

        for (const app of activeApps) {
          if (!app.telegramId) continue;

          // Пропускаем, если для этой даты нет рабочих часов (защитная проверка)
          if (sheetsService.getWorkHoursForDate) {
            const wh = await sheetsService.getWorkHoursForDate(app.date);
            if (!wh) continue;
          }

          const start = dayjs.tz(`${app.date}T${app.timeStart}:00`, timezone);
          const diffMinutes = start.diff(nowTz, "minute");

          // Окно: от 115 до 125 минут до начала (±5 минут из-за периодичности cron)
          if (diffMinutes <= 125 && diffMinutes >= 115) {
            // Проверяем, не отправляли ли уже напоминание
            const reminderKey = `${app.id}_${app.date}_${app.timeStart}`;
            if (twoHourRemindedIds.has(reminderKey)) continue;

            const timeUntil = Math.round((diffMinutes / 60) * 10) / 10; // Округление до 0.1 часа

            const msg = [
              "⏰ *Скоро ваша запись в барбершоп!*",
              "",
              `⏳ *До начала осталось:* ${timeUntil} часа`,
              `📅 *Дата:* ${formatDate(app.date)}`,
              `🕐 *Время:* ${app.timeStart}–${app.timeEnd}`,
              `✂️ *Услуга:* ${app.service}`,
              "",
              "📍 *Не забудьте подойти за 5-10 минут до начала.*",
              "",
              "❌ *Если планы изменились:*",
              "Отмените запись через бота в разделе «Мои записи».",
              "",
              "📞 *Контакты:*",
              config.barberPhone || "+7 XXX XXX-XX-XX",
            ].join("\n");

            try {
              await bot.telegram.sendMessage(app.telegramId, msg, {
                parse_mode: "Markdown",
              });
              twoHourRemindedIds.add(reminderKey);
              sentCount++;

              // Добавляем небольшую задержку между сообщениями
              await new Promise((resolve) => setTimeout(resolve, 100));
            } catch (err) {
              errorCount++;
              console.error(
                `Ошибка отправки 2-часового напоминания пользователю ${app.telegramId}:`,
                err.message
              );

              // Если пользователь заблокировал бота, помечаем запись?
              if (err.response && err.response.error_code === 403) {
                console.warn(
                  `Пользователь ${app.telegramId} заблокировал бота, запись ID: ${app.id}`
                );
              }
            }
          }
        }

        // Логируем результат если были отправки
        if (sentCount > 0 || errorCount > 0) {
          console.log(
            `[${dayjs().format(
              "YYYY-MM-DD HH:mm:ss"
            )}] 2-часовые напоминания: ${sentCount} отправлено, ${errorCount} ошибок`
          );
        }
      } catch (err) {
        console.error("Критическая ошибка в 2-часовых напоминаниях:", err);
      }
    },
    {
      timezone: config.defaultTimezone,
    }
  );

  // Напоминание за 1 день до записи для новых клиентов (опционально)
  if (config.enableWelcomeReminder) {
    cron.schedule(
      "0 18 * * *",
      async () => {
        try {
          const timezone = await sheetsService.getTimezone();
          const nowTz = dayjs().tz(timezone);
          const tomorrow = nowTz.add(1, "day").format("YYYY-MM-DD");

          const appointments = await sheetsService.getAppointmentsByDate(
            tomorrow
          );
          const activeApps = appointments.filter(
            (app) => app.status === booking.STATUSES.ACTIVE
          );

          // Можно добавить логику для новых клиентов (первая запись)
          // Это требует доработки базы клиентов
        } catch (err) {
          console.error("Ошибка в приветственных напоминаниях:", err);
        }
      },
      {
        timezone: config.defaultTimezone,
      }
    );
  }

  // Автоматическое завершение записей: каждые 30 минут проверяем прошедшие записи
  // Статус меняется на "исполнено" сразу после окончания времени услуги
  cron.schedule(
    "*/30 * * * *",
    async () => {
      try {
        const timezone = await sheetsService.getTimezone();
        const nowTz = dayjs().tz(timezone);

        // Получаем все активные записи
        const activeAppointments =
          await sheetsService.getAllActiveAppointments();

        let completedCount = 0;
        let errorCount = 0;

        for (const app of activeAppointments) {
          if (!app.date || !app.timeEnd) continue;

          try {
            // Создаем момент окончания записи в таймзоне салона
            const endTime = dayjs.tz(`${app.date}T${app.timeEnd}:00`, timezone);

            // Проверяем, прошло ли время окончания записи
            if (endTime.isBefore(nowTz) || endTime.isSame(nowTz)) {
              // Обновляем статус на "исполнено"
              const completedAtUtc = dayjs().utc().toISOString();
              const success = await sheetsService.updateAppointmentStatus(
                app.id,
                booking.STATUSES.COMPLETED,
                { completedAtUtc }
              );

              if (success) {
                completedCount++;
                console.log(
                  `[${dayjs().format("YYYY-MM-DD HH:mm:ss")}] Запись ${
                    app.id
                  } автоматически завершена (${app.date} ${app.timeEnd})`
                );
              } else {
                errorCount++;
                console.error(
                  `Ошибка при завершении записи ${app.id}: не удалось обновить статус`
                );
              }
            }
          } catch (err) {
            errorCount++;
            console.error(
              `Ошибка при обработке записи ${app.id} для автоматического завершения:`,
              err.message
            );
          }
        }

        // Логируем результат если были изменения
        if (completedCount > 0 || errorCount > 0) {
          console.log(
            `[${dayjs().format(
              "YYYY-MM-DD HH:mm:ss"
            )}] Автоматическое завершение записей: ${completedCount} завершено, ${errorCount} ошибок`
          );
        }

        // Отправляем отчет менеджеру если настроен и были завершения
        if (config.managerChatId && completedCount > 0) {
          const reportMsg = [
            "✅ *Отчет по автоматическому завершению записей*",
            `📅 Дата: ${formatDate(nowTz.format("YYYY-MM-DD"))}`,
            `⏰ Время: ${nowTz.format("HH:mm:ss")}`,
            `✅ Завершено записей: ${completedCount}`,
            errorCount > 0 ? `❌ Ошибок: ${errorCount}` : "",
          ]
            .filter(Boolean)
            .join("\n");

          try {
            await bot.telegram.sendMessage(config.managerChatId, reportMsg, {
              parse_mode: "Markdown",
            });
          } catch (err) {
            console.error("Ошибка отправки отчета менеджеру:", err.message);
          }
        }
      } catch (err) {
        console.error(
          "Критическая ошибка в автоматическом завершении записей:",
          err
        );
      }
    },
    {
      timezone: config.defaultTimezone,
    }
  );

  // Напоминание клиентам, которые не подстригались более 21 дня
  cron.schedule(
    "0 11 * * *",
    async () => {
      try {
        const timezone = await sheetsService.getTimezone();
        const nowTz = dayjs().tz(timezone);

        const clientsForReminder =
          await sheetsService.getClientsFor21DayReminder();

        if (!clientsForReminder || clientsForReminder.length === 0) {
          console.log(
            `[${dayjs().format(
              "YYYY-MM-DD HH:mm:ss"
            )}] Напоминания 21 день: нет клиентов для напоминания`
          );
          return;
        }

        let sentCount = 0;
        let errorCount = 0;

        // Получаем текст сообщения из настроек
        const messageTemplate = await sheetsService.get21DayReminderMessage();

        for (const client of clientsForReminder) {
          if (!client.telegramId) continue;

          const clientName = client.name || client.username || "друг";

          // Заменяем плейсхолдер {clientName} на реальное имя
          const msg = messageTemplate.replace(/{clientName}/g, clientName);

          try {
            await bot.telegram.sendMessage(client.telegramId, msg, {
              parse_mode: "Markdown",
            });

            // Помечаем напоминание как отправленное
            await sheetsService.mark21DayReminderSent(client.telegramId);
            sentCount++;

            // Добавляем небольшую задержку между сообщениями
            await new Promise((resolve) => setTimeout(resolve, 200));
          } catch (err) {
            errorCount++;
            console.error(
              `Ошибка отправки напоминания 21 день пользователю ${client.telegramId}:`,
              err.message
            );

            // Если пользователь заблокировал бота, не помечаем напоминание как отправленное
            if (err.response && err.response.error_code === 403) {
              console.warn(
                `Пользователь ${client.telegramId} заблокировал бота, напоминание не отправлено`
              );
            }
          }
        }

        // Логируем результат
        console.log(
          `[${dayjs().format(
            "YYYY-MM-DD HH:mm:ss"
          )}] Напоминания 21 день отправлены: ${sentCount} успешно, ${errorCount} с ошибкой`
        );

        // Отправляем отчет менеджеру если настроен
        if (config.managerChatId && (sentCount > 0 || errorCount > 0)) {
          const reportMsg = [
            "📊 *Отчет по напоминаниям 21 день*",
            `📅 Дата: ${nowTz.format("YYYY-MM-DD")}`,
            `✅ Отправлено: ${sentCount}`,
            `❌ Ошибок: ${errorCount}`,
            `⏰ Время отправки: ${nowTz.format("HH:mm:ss")}`,
          ].join("\n");

          try {
            await bot.telegram.sendMessage(config.managerChatId, reportMsg, {
              parse_mode: "Markdown",
            });
          } catch (err) {
            console.error("Ошибка отправки отчета менеджеру:", err.message);
          }
        }
      } catch (err) {
        console.error("Критическая ошибка в напоминаниях 21 день:", err);
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
