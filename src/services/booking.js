// Бизнес-логика записи: услуги, слоты, проверка пересечений, создание записи

const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezonePlugin = require("dayjs/plugin/timezone");
const {
  getAllServices,
  getServiceByKey: getServiceByKeyFromFile,
} = require("./services");

dayjs.extend(utc);
dayjs.extend(timezonePlugin);

// Статусы на русском
const STATUSES = {
  ACTIVE: "активна",
  CANCELLED: "отменена",
  COMPLETED: "исполнено",
  BLOCKED: "заблокировано",
};

function getServiceList() {
  return getAllServices();
}

function getServiceByKey(key) {
  return getServiceByKeyFromFile(key);
}

function generateId(prefix) {
  // Комментарий: простой уникальный ID без внешних зависимостей
  const rand = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${Date.now().toString(36)}_${rand}`;
}

function generateCancelCode() {
  return Math.random().toString(36).slice(2, 8).toUpperCase();
}

function intervalsOverlap(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && bStart < aEnd;
}

function buildSlotsForDay({
  dateStr,
  service,
  timezone,
  workday,
  schedule,
  appointments,
}) {
  const serviceDuration = service.durationMin;
  let dayStart;
  let dayEnd;

  if (workday && workday.startHour != null) {
    dayStart = dayjs.tz(
      `${dateStr}T${String(workday.startHour).padStart(2, "0")}:00:00`,
      timezone
    );
    dayEnd = dayjs.tz(
      `${dateStr}T${String(workday.endHour).padStart(2, "0")}:00:00`,
      timezone
    );
  } else if (workday && workday.start && workday.end) {
    // workday.start/end expected as 'HH:mm'
    dayStart = dayjs.tz(`${dateStr}T${workday.start}:00`, timezone);
    dayEnd = dayjs.tz(`${dateStr}T${workday.end}:00`, timezone);
  } else {
    // No working hours provided -> closed
    return [];
  }

  const busyIntervals = [];

  // blocked из Schedule (русский статус)
  schedule.forEach((row) => {
    if (row.status === STATUSES.BLOCKED) {
      const start = dayjs.tz(`${row.date}T${row.timeStart}:00`, timezone);
      const end = dayjs.tz(`${row.date}T${row.timeEnd}:00`, timezone);
      busyIntervals.push({ start, end });
    }
  });

  // занятые записи (только активные)
  appointments.forEach((row) => {
    if (row.status === STATUSES.ACTIVE) {
      const start = dayjs.tz(`${row.date}T${row.timeStart}:00`, timezone);
      const end = dayjs.tz(`${row.date}T${row.timeEnd}:00`, timezone);
      busyIntervals.push({ start, end });
    }
  });

  // учитываем обед, если задан в рабочих часах
  try {
    if (workday && workday.lunchStart && workday.lunchEnd) {
      const lunchStart = dayjs.tz(
        `${dateStr}T${workday.lunchStart}:00`,
        timezone
      );
      const lunchEnd = dayjs.tz(`${dateStr}T${workday.lunchEnd}:00`, timezone);
      if (lunchStart.isBefore(lunchEnd)) {
        busyIntervals.push({ start: lunchStart, end: lunchEnd });
      }
    }
  } catch (e) {
    // ignore malformed lunch times
  }

  const slots = [];
  const now = dayjs().tz(timezone);

  let cursor = dayStart;
  while (cursor.add(serviceDuration, "minute") <= dayEnd) {
    const slotStart = cursor;
    const slotEnd = cursor.add(serviceDuration, "minute");

    // Комментарий: не даём выбирать прошлое время
    if (slotStart.isBefore(now)) {
      cursor = cursor.add(15, "minute");
      continue;
    }

    const isBusy = busyIntervals.some((interval) =>
      intervalsOverlap(
        slotStart.valueOf(),
        slotEnd.valueOf(),
        interval.start.valueOf(),
        interval.end.valueOf()
      )
    );

    if (!isBusy) {
      slots.push({
        timeStr: slotStart.format("HH:mm"),
        start: slotStart,
        end: slotEnd,
      });
    }

    // Шаг 15 минут для гибкости
    cursor = cursor.add(15, "minute");
  }

  return slots;
}

function createBookingService({ sheetsService, config, calendarService }) {
  async function getAvailableSlotsForService(serviceKey, dateStr) {
    const service = getServiceByKey(serviceKey);
    if (!service) {
      throw new Error(`Unknown service key: ${serviceKey}`);
    }

    const timezone = await sheetsService.getTimezone();
    const { schedule, appointments } = await sheetsService.getDaySchedule(
      dateStr
    );

    const workHours =
      (sheetsService.getWorkHoursForDate &&
        (await sheetsService.getWorkHoursForDate(dateStr))) ||
      null;

    if (!workHours) {
      return { service, timezone, slots: [] };
    }

    const slots = buildSlotsForDay({
      dateStr,
      service,
      timezone,
      workday: workHours,
      schedule,
      appointments,
    });

    return { service, timezone, slots };
  }

  async function isSlotFree({ dateStr, timeStr, service }) {
    const timezone = await sheetsService.getTimezone();
    const { schedule, appointments } = await sheetsService.getDaySchedule(
      dateStr
    );
    const workHours =
      (sheetsService.getWorkHoursForDate &&
        (await sheetsService.getWorkHoursForDate(dateStr))) ||
      null;

    if (!workHours) return false;

    const slots = buildSlotsForDay({
      dateStr,
      service,
      timezone,
      workday: workHours,
      schedule,
      appointments,
    });

    return slots.some((slot) => slot.timeStr === timeStr);
  }

  async function bookAppointment({
    serviceKey,
    dateStr,
    timeStr,
    client,
    comment,
  }) {
    const service = getServiceByKey(serviceKey);
    if (!service) {
      throw new Error(`Unknown service key: ${serviceKey}`);
    }

    // Блокируем создание записи для забаненных пользователей
    try {
      if (
        client &&
        client.telegramId &&
        sheetsService &&
        sheetsService.getUserBanStatus
      ) {
        const st = await sheetsService.getUserBanStatus(client.telegramId);
        if (st && st.banned) {
          return { ok: false, reason: "banned" };
        }
      }
    } catch (e) {
      // не блокируем при ошибке проверки
    }

    const timezone = await sheetsService.getTimezone();
    // Проверяем рабочие часы дня
    const workHours =
      (sheetsService.getWorkHoursForDate &&
        (await sheetsService.getWorkHoursForDate(dateStr))) ||
      null;

    if (!workHours) {
      return { ok: false, reason: "closed" };
    }

    // Повторная проверка: слот ещё свободен?
    const free = await isSlotFree({ dateStr, timeStr, service });
    if (!free) {
      return { ok: false, reason: "slot_taken" };
    }

    // Защита от спама: не более 3 активных записей в день от одного пользователя
    try {
      const dayAppointments = await sheetsService.getAppointmentsByDate(
        dateStr
      );

      const ownerKey =
        client.telegramId || client.chatId || client.phone || null;
      if (ownerKey) {
        const sameUserCount = dayAppointments.filter((a) => {
          if (a.status !== STATUSES.ACTIVE) return false;
          if (client.telegramId && a.telegramId)
            return String(a.telegramId) === String(client.telegramId);
          if (client.chatId && a.chatId)
            return String(a.chatId) === String(client.chatId);
          if (client.phone && a.phone)
            return String(a.phone) === String(client.phone);
          return false;
        }).length;

        if (sameUserCount >= 3) {
          return { ok: false, reason: "limit_exceeded" };
        }
      }
    } catch (e) {
      // Если проверка не удалась, не блокируем создание записи — логируем молча
    }

    const start = dayjs.tz(`${dateStr}T${timeStr}:00`, timezone);
    const end = start.add(service.durationMin, "minute");

    const id = generateId("A");
    const cancelCode = generateCancelCode();
    const createdAtUtc = dayjs().utc().toISOString();

    const appointment = {
      id,
      createdAtUtc,
      service: service.name,
      date: dateStr,
      timeStart: start.format("HH:mm"),
      timeEnd: end.format("HH:mm"),
      clientName: client.name,
      phone: client.phone,
      username: client.username || "",
      comment: comment || "",
      status: STATUSES.ACTIVE,
      cancelCode,
      telegramId: client.telegramId,
      chatId: client.chatId,
    };

    await sheetsService.appendAppointment(appointment);

    // Обновляем/создаём клиента
    await sheetsService.upsertClient({
      telegramId: client.telegramId,
      username: client.username,
      name: client.name,
      phone: client.phone,
      lastAppointmentAtUtc: createdAtUtc,
    });

    // Очищаем флаг отправки 21-дневного напоминания при новой записи
    if (client.telegramId && sheetsService.clear21DayReminderSentAt) {
      try {
        await sheetsService.clear21DayReminderSentAt(client.telegramId);
      } catch (e) {
        // не блокируем при ошибке очистки
      }
    }

    // Попытка создать событие в Google Calendar (опционально)
    try {
      if (calendarService && calendarService.createEventForAppointment) {
        // не ждём успешного результата, но логируем ID если вернулся
        const eventId = await calendarService.createEventForAppointment(
          appointment,
          timezone
        );
        if (eventId) {
          // логируем успешную синхронизацию
          console.log(
            `Google Calendar event created: ${eventId} for appointment ${appointment.id}`
          );
        }
      }
    } catch (e) {
      console.warn("Calendar sync failed for appointment:", e.message || e);
    }

    // Дополнительная проверка на гонку: читаем активные записи на этот день
    // и если есть пересечение более чем одной записи на тот же интервал,
    // отменяем позднюю (те, что созданы позже). Это делает операцию
    // идемпотентной при параллельных запросах к одному слоту.
    try {
      const dayAppointments = await sheetsService.getAppointmentsByDate(
        dateStr
      );

      const overlapping = dayAppointments.filter((a) => {
        const aStart = dayjs.tz(`${a.date}T${a.timeStart}:00`, timezone);
        const aEnd = dayjs.tz(`${a.date}T${a.timeEnd}:00`, timezone);
        return intervalsOverlap(
          start.valueOf(),
          end.valueOf(),
          aStart.valueOf(),
          aEnd.valueOf()
        );
      });

      if (overlapping.length > 1) {
        overlapping.sort((x, y) => {
          if (x.createdAtUtc === y.createdAtUtc)
            return x.id.localeCompare(y.id);
          return x.createdAtUtc < y.createdAtUtc ? -1 : 1;
        });
        const winner = overlapping[0];
        if (winner.id !== id) {
          const cancelledAtUtc = dayjs().utc().toISOString();
          await sheetsService.updateAppointmentStatus(id, STATUSES.CANCELLED, {
            cancelledAtUtc,
          });
          return { ok: false, reason: "slot_taken" };
        }
      }
    } catch (e) {
      // В случае ошибки проверки — не ломаем основной поток: считаем запись успешной.
    }

    return {
      ok: true,
      appointment,
    };
  }

  async function cancelAppointment(id, telegramId) {
    // Получаем запись для проверки владельца
    const appointment = await sheetsService.getAppointmentById(id);

    if (!appointment) {
      return { ok: false, reason: "appointment_not_found" };
    }

    // Проверяем, что отменяет владелец записи
    if (String(appointment.telegramId) !== String(telegramId)) {
      return { ok: false, reason: "not_owner" };
    }

    // Проверяем, что запись ещё активна
    if (appointment.status !== STATUSES.ACTIVE) {
      return { ok: false, reason: "already_cancelled" };
    }

    const cancelledAtUtc = dayjs().utc().toISOString();
    const success = await sheetsService.updateAppointmentStatus(
      id,
      STATUSES.CANCELLED,
      { cancelledAtUtc }
    );

    if (!success) {
      return { ok: false, reason: "update_failed" };
    }

    // Удаляем/обновляем событие в календаре при наличии сервиса
    try {
      if (calendarService && calendarService.deleteEventForAppointmentId) {
        await calendarService.deleteEventForAppointmentId(id);
      }
    } catch (e) {
      console.warn("Calendar delete failed for appointment:", e.message || e);
    }
    return {
      ok: true,
      appointment: { ...appointment, status: STATUSES.CANCELLED },
    };
  }

  async function cancelAppointmentByCode(cancelCode) {
    // Комментарий: отмена записи по коду отмены (для админа, без проверки владельца)
    const appointment = await sheetsService.getAppointmentByCancelCode(cancelCode);

    if (!appointment) {
      return { ok: false, reason: "appointment_not_found" };
    }

    // Проверяем, что запись ещё активна
    if (appointment.status !== STATUSES.ACTIVE) {
      return { ok: false, reason: "already_cancelled" };
    }

    const cancelledAtUtc = dayjs().utc().toISOString();
    const success = await sheetsService.updateAppointmentStatus(
      appointment.id,
      STATUSES.CANCELLED,
      { cancelledAtUtc }
    );

    if (!success) {
      return { ok: false, reason: "update_failed" };
    }

    // Удаляем/обновляем событие в календаре при наличии сервиса
    try {
      if (calendarService && calendarService.deleteEventForAppointmentId) {
        await calendarService.deleteEventForAppointmentId(appointment.id);
      }
    } catch (e) {
      console.warn("Calendar delete failed for appointment:", e.message || e);
    }
    return {
      ok: true,
      appointment: { ...appointment, status: STATUSES.CANCELLED },
    };
  }

  return {
    getAvailableSlotsForService,
    bookAppointment,
    cancelAppointment,
    cancelAppointmentByCode,
    getServiceList,
    getServiceByKey,
    STATUSES,
  };
}

module.exports = {
  createBookingService,
  getServiceList,
  getServiceByKey,
};
