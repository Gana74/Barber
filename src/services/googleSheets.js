// Сервис работы с Google Sheets
// Здесь: авторизация по service account, кэш расписания, базовые CRUD по листам

const { google } = require("googleapis");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");

dayjs.extend(utc);

// Комментарий: имена листов в таблице
const SHEET_NAMES = {
  SETTINGS: "Settings",
  SCHEDULE: "Schedule",
  APPOINTMENTS: "Appointments",
  CLIENTS: "Clients",
};

// Комментарий: заголовки по умолчанию
const HEADERS = {
  [SHEET_NAMES.SETTINGS]: ["key", "value"],
  [SHEET_NAMES.SCHEDULE]: [
    "date",
    "time_start",
    "time_end",
    "status",
    "service",
    "note",
  ],
  [SHEET_NAMES.APPOINTMENTS]: [
    "id",
    "created_at_utc",
    "service",
    "date",
    "time_start",
    "time_end",
    "client_name",
    "phone",
    "username",
    "comment",
    "status",
    "cancel_code",
    "telegram_id",
    "chat_id",
    "cancelled_at_utc",
  ],
  [SHEET_NAMES.CLIENTS]: [
    "client_id",
    "first_seen_utc",
    "telegram_id",
    "username",
    "name",
    "phone",
    "last_appointment_at_utc",
    "total_appointments",
  ],
};

// Комментарий: простой in-memory кэш по датам
const dayCache = {
  // '2025-01-01': { expiresAt: 123456789, schedule: [...], appointments: [...] }
};

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

  async function ensureSheetsStructure() {
    // Комментарий: создаём недостающие листы и заголовки
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
        requests.push({
          addSheet: {
            properties: { title },
          },
        });
      }
    });

    if (requests.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: config.google.sheetsId,
        requestBody: { requests },
      });
    }

    // Проставляем заголовки на каждой вкладке
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

    // Дефолтная таймзона, если нет записи
    const settings = await getSettings();
    if (!settings.timezone) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: {
          values: [["timezone", config.defaultTimezone]],
        },
      });
    }
  }

  async function getSettings() {
    // Комментарий: читаем лист Settings (key, value)
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
    return settings.timezone || config.defaultTimezone;
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

    // Читаем Schedule
    const scheduleRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SCHEDULE}!A2:F1000`,
    });
    const scheduleRows = scheduleRes.data.values || [];
    const schedule = scheduleRows
      .map((row) => {
        const [date, timeStart, timeEnd, status, service, note] = row;
        return { date, timeStart, timeEnd, status, service, note };
      })
      .filter((row) => row.date === dateStr);

    // Читаем Appointments
    const appRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const appRows = appRes.data.values || [];
    const appointments = appRows
      .map((row) => {
        const [
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
          cancelledAtUtc,
        ] = row;
        return {
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
          cancelledAtUtc,
        };
      })
      .filter((row) => row.date === dateStr && row.status !== "cancelled");

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
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2`,
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
            status || "active",
            cancelCode,
            String(telegramId || ""),
            String(chatId || ""),
            "",
          ],
        ],
      },
    });

    invalidateDayCache(date);
  }

  async function updateAppointmentStatus(id, status, { cancelledAtUtc } = {}) {
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
    // status в колонке K (index 10), cancelledAtUtc в колонке P (index 15)
    rowValues[10] = status;
    if (status === "cancelled" && cancelledAtUtc) {
      rowValues[15] = cancelledAtUtc;
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
          tId,
          chatId,
        ] = row;
        return {
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
          telegramId: tId,
          chatId,
        };
      })
      .filter(
        (row) =>
          String(row.telegramId) === String(telegramId) &&
          row.status === "active"
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
        ] = row;
        return {
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
        };
      })
      .filter((row) => row.date === dateStr && row.status === "active");
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
      aId,
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
      cancelledAtUtc,
    ] = row;

    return {
      id: aId,
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
      cancelledAtUtc,
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
    getAppointmentById,
  };
}

module.exports = {
  createSheetsService,
};
