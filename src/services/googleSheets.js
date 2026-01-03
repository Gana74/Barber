// Сервис работы с Google Sheets
// Здесь: авторизация по service account, кэш расписания, базовые CRUD по листам

const { google } = require("googleapis");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");

dayjs.extend(utc);

// Комментарий: имена листов в таблице
const SHEET_NAMES = {
  SETTINGS: "Настройки",
  SCHEDULE: "Расписание",
  APPOINTMENTS: "Записи",
  CLIENTS: "Клиенты",
  WORKHOURS: "WorkHours",
};

// Комментарий: заголовки на русском
const HEADERS = {
  [SHEET_NAMES.SETTINGS]: ["Ключ", "Значение"],
  [SHEET_NAMES.SCHEDULE]: [
    "Дата",
    "Время_начала",
    "Время_окончания",
    "Статус",
    "Услуга",
    "Примечание",
  ],
  [SHEET_NAMES.APPOINTMENTS]: [
    "ID_записи",
    "Создано_UTC",
    "Услуга",
    "Дата",
    "Время_начала",
    "Время_окончания",
    "Имя_клиента",
    "Телефон",
    "Username_Telegram",
    "Комментарий",
    "Статус",
    "Код_отмены",
    "Telegram_ID",
    "Chat_ID",
    "Исполнено_UTC",
    "Отменено_UTC",
  ],
  [SHEET_NAMES.CLIENTS]: [
    "ID_клиента",
    "Первое_посещение_UTC",
    "Telegram_ID",
    "Username_Telegram",
    "Имя",
    "Телефон",
    "Последняя_запись_UTC",
    "Всего_записей",
  ],
  [SHEET_NAMES.WORKHOURS]: [
    "Дата",
    "День_недели",
    "Время_начала",
    "Время_окончания",
  ],
};

// Комментарий: простой in-memory кэш по датам
const dayCache = {
  // '2025-01-01': { expiresAt: 123456789, schedule: [...], appointments: [...] }
};

// Кэш для рабочих часов (work hours)
let workHoursCache = { byDate: {}, byWeekday: {}, expiresAt: 0 };

function createGoogleAuth(config) {
  // Комментарий: создаём JWT-клиент для сервисного аккаунта
  return new google.auth.JWT(
    config.google.clientEmail,
    undefined,
    config.google.privateKey,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
}

async function createSheetsService(config) {
  const auth = createGoogleAuth(config);
  const sheets = google.sheets({ version: "v4", auth });

  async function fetchWorkHours() {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.WORKHOURS}!A2:D1000`,
    });

    const rows = res.data.values || [];

    const byDate = {};
    const byWeekday = {};

    // aliases mapping common english and russian names to canonical keys
    const WEEKDAY_ALIAS = {
      mon: "mon",
      monday: "mon",
      понедельник: "mon",
      пн: "mon",

      tue: "tue",
      tuesday: "tue",
      вторник: "tue",
      вт: "tue",

      wed: "wed",
      wednesday: "wed",
      среда: "wed",
      ср: "wed",

      thu: "thu",
      thursday: "thu",
      четверг: "thu",
      чт: "thu",

      fri: "fri",
      friday: "fri",
      пятница: "fri",
      пт: "fri",

      sat: "sat",
      saturday: "sat",
      суббота: "sat",
      сб: "sat",

      sun: "sun",
      sunday: "sun",
      воскресенье: "sun",
      вс: "sun",
    };

    rows.forEach((row) => {
      const [dateCell, weekdayCell, timeStartCell, timeEndCell] = row;
      const start = (timeStartCell || "").trim();
      const end = (timeEndCell || "").trim();

      if (dateCell) {
        byDate[String(dateCell).trim()] = { start, end };
      } else if (weekdayCell) {
        const raw = String(weekdayCell).trim().toLowerCase();
        let key = null;
        if (WEEKDAY_ALIAS[raw]) key = WEEKDAY_ALIAS[raw];
        else if (raw.length >= 3) key = raw.slice(0, 3);
        if (key) byWeekday[key] = { start, end };
      }
    });

    workHoursCache = {
      byDate,
      byWeekday,
      expiresAt: Date.now() + 120 * 1000,
    };

    return workHoursCache;
  }

  async function getWorkHoursForDate(dateStr) {
    const now = Date.now();
    if (!workHoursCache.expiresAt || workHoursCache.expiresAt < now) {
      await fetchWorkHours();
    }

    if (workHoursCache.byDate && workHoursCache.byDate[dateStr]) {
      const v = workHoursCache.byDate[dateStr];
      if (v.start && v.end) return v;
      return null;
    }

    try {
      const d = dayjs(dateStr);
      const day = d.day(); // 0 - Sunday, 1 - Monday ...
      const map = {
        1: "mon",
        2: "tue",
        3: "wed",
        4: "thu",
        5: "fri",
        6: "sat",
        0: "sun",
      };
      const wk = map[day];
      if (workHoursCache.byWeekday && workHoursCache.byWeekday[wk]) {
        const v = workHoursCache.byWeekday[wk];
        if (v.start && v.end) return v;
      }
    } catch (e) {
      // ignore
    }

    return null;
  }

  function invalidateWorkHoursCache() {
    workHoursCache = { byDate: {}, byWeekday: {}, expiresAt: 0 };
  }
  async function getSettings() {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SETTINGS}!A2:B100`,
    });

    const rows = res.data.values || [];
    const settings = {};
    rows.forEach((row) => {
      const [key, value] = row;
      if (key) settings[key] = value;
    });
    return settings;
  }

  async function getTimezone() {
    const settings = await getSettings();
    return settings.таймзона || config.defaultTimezone;
  }
  async function ensureSheetsStructure() {
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: config.google.sheetsId,
      fields: "sheets.properties.title",
    });

    const existingTitles = new Set(
      (meta.data.sheets || []).map((s) => s.properties.title)
    );

    const requests = [];
    Object.values(SHEET_NAMES).forEach((title) => {
      if (!existingTitles.has(title)) {
        requests.push({ addSheet: { properties: { title } } });
      }
    });

    if (requests.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: config.google.sheetsId,
        requestBody: { requests },
      });
    }

    for (const title of Object.values(SHEET_NAMES)) {
      const header = HEADERS[title];
      if (!header) continue;

      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${title}!A1:${String.fromCharCode(65 + header.length - 1)}1`,
        valueInputOption: "RAW",
        requestBody: { values: [header] },
      });
    }

    const settings = await getSettings();
    if (!settings.таймзона) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [["таймзона", config.defaultTimezone]] },
      });
    }
  }

  function invalidateWorkHoursCache() {
    workHoursCache = { byDate: {}, byWeekday: {}, expiresAt: 0 };
  }

  async function getDaySchedule(dateStr) {
    // Комментарий: используем кэш
    const now = Date.now();
    const cached = dayCache[dateStr];
    if (cached && cached.expiresAt > now) {
      return {
        schedule: cached.schedule,
        appointments: cached.appointments,
      };
    }

    // Читаем Расписание
    const scheduleRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SCHEDULE}!A2:F1000`,
    });
    const scheduleRows = scheduleRes.data.values || [];
    const schedule = scheduleRows
      .map((row) => {
        const [
          Дата,
          Время_начала,
          Время_окончания,
          Статус,
          Услуга,
          Примечание,
        ] = row;
        return {
          date: Дата,
          timeStart: Время_начала,
          timeEnd: Время_окончания,
          status: Статус,
          service: Услуга,
          note: Примечание,
        };
      })
      .filter((row) => row.date === dateStr);

    // Читаем Записи
    const appRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const appRows = appRes.data.values || [];
    const appointments = appRows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Дата,
          Время_начала,
          Время_окончания,
          Имя_клиента,
          Телефон,
          Username_Telegram,
          Комментарий,
          Статус,
          Код_отмены,
          Telegram_ID,
          Chat_ID,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          date: Дата,
          timeStart: Время_начала,
          timeEnd: Время_окончания,
          clientName: Имя_клиента,
          phone: Телефон,
          username: Username_Telegram,
          comment: Комментарий,
          status: Статус,
          cancelCode: Код_отмены,
          telegramId: Telegram_ID,
          chatId: Chat_ID,
          cancelledAtUtc: Отменено_UTC,
        };
      })
      .filter(
        (row) =>
          row.date === dateStr &&
          row.status !== "отменена" &&
          row.status !== "исполнено"
      );

    dayCache[dateStr] = {
      schedule,
      appointments,
      expiresAt: now + 60 * 1000, // TTL 60 секунд
    };

    return { schedule, appointments };
  }

  function invalidateDayCache(dateStr) {
    // Комментарий: сбрасываем кэш на конкретный день
    if (dayCache[dateStr]) {
      delete dayCache[dateStr];
    }
  }

  async function appendAppointment(appointment) {
    const {
      id,
      createdAtUtc,
      service,
      date,
      timeStart,
      timeEnd,
      clientName,
      phone,
      username,
      comment,
      status,
      cancelCode,
      telegramId,
      chatId,
    } = appointment;

    await sheets.spreadsheets.values.append({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:R2`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: {
        values: [
          [
            id,
            createdAtUtc,
            service,
            date,
            timeStart,
            timeEnd,
            clientName,
            phone,
            username || "",
            comment || "",
            status || "активна",
            cancelCode,
            String(telegramId || ""),
            String(chatId || ""),
            "", // Отменено_UTC
            "", // Исполнено_UTC - пусто при создании
          ],
        ],
      },
    });

    invalidateDayCache(date);
  }

  async function updateAppointmentStatus(
    id,
    status,
    { cancelledAtUtc, completedAtUtc } = {}
  ) {
    // Комментарий: для простоты читаем весь список и находим строку по id
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    let targetDate = null;

    rows.forEach((row, idx) => {
      if (row[0] === id) {
        targetRowIndex = idx;
        targetDate = row[3];
      }
    });

    if (targetRowIndex === -1) {
      return false;
    }

    const rowNumber = targetRowIndex + 2; // сдвиг из-за заголовков

    const rowValues = rows[targetRowIndex];
    // Статус в колонке K (index 10), Отменено_UTC в колонке P (index 15)
    rowValues[10] = status;
    if (status === "отменена" && cancelledAtUtc) {
      rowValues[15] = cancelledAtUtc;
    }
    if (status === "исполнено" && completedAtUtc) {
      rowValues[16] = completedAtUtc; // Исполнено_UTC
    }

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A${rowNumber}:Q${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [rowValues],
      },
    });

    if (targetDate) {
      invalidateDayCache(targetDate);
    }

    return true;
  }

  async function upsertClient(client) {
    const { telegramId, username, name, phone, lastAppointmentAtUtc } = client;

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:H2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    let existingTotal = 0;

    rows.forEach((row, idx) => {
      const existingTelegramId = row[2];
      if (String(existingTelegramId) === String(telegramId)) {
        targetRowIndex = idx;
        existingTotal = Number(row[7] || 0);
      }
    });

    if (targetRowIndex === -1) {
      // Вставка нового клиента
      const clientId = `C_${telegramId}`;
      const firstSeenUtc = dayjs().utc().toISOString();

      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.CLIENTS}!A2:H2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: {
          values: [
            [
              clientId,
              firstSeenUtc,
              String(telegramId),
              username || "",
              name || "",
              phone || "",
              lastAppointmentAtUtc || "",
              1,
            ],
          ],
        },
      });
    } else {
      // Обновление существующего клиента
      const rowNumber = targetRowIndex + 2;
      const rowValues = rows[targetRowIndex];

      rowValues[3] = username || rowValues[3] || "";
      rowValues[4] = name || rowValues[4] || "";
      rowValues[5] = phone || rowValues[5] || "";
      rowValues[6] = lastAppointmentAtUtc || rowValues[6] || "";
      rowValues[7] = existingTotal + 1;

      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.CLIENTS}!A${rowNumber}:H${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: {
          values: [rowValues],
        },
      });
    }
  }

  async function getFutureAppointmentsForTelegram(telegramId, timezone) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    const now = dayjs().tz ? dayjs().tz(timezone) : dayjs();

    return rows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Дата,
          Время_начала,
          Время_окончания,
          Имя_клиента,
          Телефон,
          Username_Telegram,
          Комментарий,
          Статус,
          Код_отмены,
          Telegram_ID,
          Chat_ID,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          date: Дата,
          timeStart: Время_начала,
          timeEnd: Время_окончания,
          clientName: Имя_клиента,
          phone: Телефон,
          username: Username_Telegram,
          comment: Комментарий,
          status: Статус,
          cancelCode: Код_отмены,
          telegramId: Telegram_ID,
          chatId: Chat_ID,
        };
      })
      .filter(
        (row) =>
          String(row.telegramId) === String(telegramId) &&
          row.status === "активна"
      )
      .filter((row) => {
        const dt = dayjs.utc(`${row.date}T${row.timeStart}:00Z`).tz
          ? dayjs.utc(`${row.date}T${row.timeStart}:00Z`).tz(timezone)
          : dayjs.utc(`${row.date}T${row.timeStart}:00Z`);
        return dt.isAfter(now);
      });
  }

  async function getAppointmentsByDate(dateStr) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    return rows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Дата,
          Время_начала,
          Время_окончания,
          Имя_клиента,
          Телефон,
          Username_Telegram,
          Комментарий,
          Статус,
          Код_отмены,
          Telegram_ID,
          Chat_ID,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          date: Дата,
          timeStart: Время_начала,
          timeEnd: Время_окончания,
          clientName: Имя_клиента,
          phone: Телефон,
          username: Username_Telegram,
          comment: Комментарий,
          status: Статус,
          cancelCode: Код_отмены,
          telegramId: Telegram_ID,
          chatId: Chat_ID,
        };
      })
      .filter((row) => row.date === dateStr && row.status === "активна");
  }

  async function getAllActiveAppointments() {
    // Комментарий: получаем все активные записи для автоматического завершения
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    return rows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Дата,
          Время_начала,
          Время_окончания,
          Имя_клиента,
          Телефон,
          Username_Telegram,
          Комментарий,
          Статус,
          Код_отмены,
          Telegram_ID,
          Chat_ID,
          Отменено_UTC,
          Исполнено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          date: Дата,
          timeStart: Время_начала,
          timeEnd: Время_окончания,
          clientName: Имя_клиента,
          phone: Телефон,
          username: Username_Telegram,
          comment: Комментарий,
          status: Статус,
          cancelCode: Код_отмены,
          telegramId: Telegram_ID,
          chatId: Chat_ID,
          cancelledAtUtc: Отменено_UTC,
          completedAtUtc: Исполнено_UTC,
        };
      })
      .filter(
        (row) => row.status === "активна" && row.id && row.date && row.timeEnd
      );
  }

  async function getAppointmentById(id) {
    // Комментарий: ищем одну конкретную запись по id
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    const row = rows.find((r) => r[0] === id);
    if (!row) return null;

    const [
      ID_записи,
      Создано_UTC,
      Услуга,
      Дата,
      Время_начала,
      Время_окончания,
      Имя_клиента,
      Телефон,
      Username_Telegram,
      Комментарий,
      Статус,
      Код_отмены,
      Telegram_ID,
      Chat_ID,
      Отменено_UTC,
      Исполнено_UTC,
    ] = row;

    return {
      id: ID_записи,
      createdAtUtc: Создано_UTC,
      service: Услуга,
      date: Дата,
      timeStart: Время_начала,
      timeEnd: Время_окончания,
      clientName: Имя_клиента,
      phone: Телефон,
      username: Username_Telegram,
      comment: Комментарий,
      status: Статус,
      cancelCode: Код_отмены,
      telegramId: Telegram_ID,
      chatId: Chat_ID,
      cancelledAtUtc: Отменено_UTC,
      completedAtUtc: Исполнено_UTC,
    };
  }

  return {
    ensureSheetsStructure,
    getSettings,
    getTimezone,
    getDaySchedule,
    appendAppointment,
    updateAppointmentStatus,
    upsertClient,
    getFutureAppointmentsForTelegram,
    getAppointmentsByDate,
    getAllActiveAppointments,
    getAppointmentById,
    getWorkHoursForDate,
    invalidateWorkHoursCache,
  };
}

module.exports = {
  createSheetsService,
};
