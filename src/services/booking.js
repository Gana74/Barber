// Бизнес-логика записи: услуги, слоты, проверка пересечений, создание записи

const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezonePlugin = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezonePlugin);

// Комментарий: перечень услуг и длительность в минутах
const SERVICES = {
  MEN_HAIRCUT: { key: "MEN_HAIRCUT", name: "Мужская стрижка", durationMin: 60 },
  BEARD: { key: "BEARD", name: "Оформление бороды", durationMin: 30 },
  BUZZCUT: { key: "BUZZCUT", name: "Стрижка под машинку", durationMin: 30 },
  WOMEN_HAIRCUT: {
    key: "WOMEN_HAIRCUT",
    name: "Женская стрижка",
    durationMin: 60,
  },
};

function getServiceList() {
  return Object.values(SERVICES);
}

function getServiceByKey(key) {
  return SERVICES[key] || null;
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

  const dayStart = dayjs.tz(
    `${dateStr}T${String(workday.startHour).padStart(2, "0")}:00:00`,
    timezone
  );
  const dayEnd = dayjs.tz(
    `${dateStr}T${String(workday.endHour).padStart(2, "0")}:00:00`,
    timezone
  );

  const busyIntervals = [];

  // blocked из Schedule
  schedule.forEach((row) => {
    if (row.status === "blocked") {
      const start = dayjs.tz(`${row.date}T${row.timeStart}:00`, timezone);
      const end = dayjs.tz(`${row.date}T${row.timeEnd}:00`, timezone);
      busyIntervals.push({ start, end });
    }
  });

  // занятые записи
  appointments.forEach((row) => {
    const start = dayjs.tz(`${row.date}T${row.timeStart}:00`, timezone);
    const end = dayjs.tz(`${row.date}T${row.timeEnd}:00`, timezone);
    busyIntervals.push({ start, end });
  });

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

function createBookingService({ sheetsService, config }) {
  async function getAvailableSlotsForService(serviceKey, dateStr) {
    const service = getServiceByKey(serviceKey);
    if (!service) {
      throw new Error(`Unknown service key: ${serviceKey}`);
    }

    const timezone = await sheetsService.getTimezone();
    const { schedule, appointments } = await sheetsService.getDaySchedule(
      dateStr
    );

    const slots = buildSlotsForDay({
      dateStr,
      service,
      timezone,
      workday: config.workday,
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

    const { slots } = {
      slots: buildSlotsForDay({
        dateStr,
        service,
        timezone,
        workday: config.workday,
        schedule,
        appointments,
      }),
    };

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

    const timezone = await sheetsService.getTimezone();

    // Повторная проверка: слот ещё свободен?
    const free = await isSlotFree({ dateStr, timeStr, service });
    if (!free) {
      return { ok: false, reason: "slot_taken" };
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
      status: "active",
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

    return {
      ok: true,
      appointment,
    };
  }

  return {
    getAvailableSlotsForService,
    bookAppointment,
    getServiceList,
    getServiceByKey,
  };
}

module.exports = {
  createBookingService,
  getServiceList,
  getServiceByKey,
};
