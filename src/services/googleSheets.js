// Сервис работы с Google Sheets
// Здесь: авторизация по service account, кэш расписания, базовые CRUD по листам

const { google } = require("googleapis");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const cron = require("node-cron");

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
    "Цена",
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
    "BanStatus",
    "BanReason",
    "Напоминание_21день_UTC",
    "Последняя_рассылка_UTC",
  ],
  [SHEET_NAMES.WORKHOURS]: [
    "Дата",
    "День_недели",
    "Время_начала",
    "Время_окончания",
    "Обед_начало",
    "Обед_окончание",
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
      range: `${SHEET_NAMES.WORKHOURS}!A2:F1000`,
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
      const [
        dateCell,
        weekdayCell,
        timeStartCell,
        timeEndCell,
        lunchStartCell,
        lunchEndCell,
      ] = row;
      const start = (timeStartCell || "").trim();
      const end = (timeEndCell || "").trim();
      const lunchStart = (lunchStartCell || "").trim();
      const lunchEnd = (lunchEndCell || "").trim();

      if (dateCell) {
        byDate[String(dateCell).trim()] = { start, end, lunchStart, lunchEnd };
      } else if (weekdayCell) {
        const raw = String(weekdayCell).trim().toLowerCase();
        let key = null;
        if (WEEKDAY_ALIAS[raw]) key = WEEKDAY_ALIAS[raw];
        else if (raw.length >= 3) key = raw.slice(0, 3);
        if (key) byWeekday[key] = { start, end, lunchStart, lunchEnd };
      }
    });

    workHoursCache = {
      byDate,
      byWeekday,
      // TTL 30 минут для рабочих часов
      expiresAt: Date.now() + 30 * 60 * 1000,
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
      if (key && typeof key === "string") {
        // Нормализуем ключ: убираем пробелы в начале и конце
        const normalizedKey = key.trim();
        // Нормализуем значение: если value undefined или null, используем пустую строку
        // Если value существует, преобразуем в строку и убираем пробелы
        const normalizedValue =
          value !== undefined && value !== null ? String(value).trim() : "";
        if (normalizedKey) {
          settings[normalizedKey] = normalizedValue;
        }
      }
    });
    return settings;
  }

  async function getTimezone() {
    const settings = await getSettings();
    return settings.таймзона || config.defaultTimezone;
  }

  async function get21DayReminderMessage() {
    const settings = await getSettings();
    return (
      settings.напоминание_21день_текст ||
      "Привет, {clientName}! Тебя давно небыло на стрижке, пора подстричься!"
    );
  }

  async function set21DayReminderMessage(message) {
    if (
      !message ||
      typeof message !== "string" ||
      message.trim().length === 0
    ) {
      throw new Error("Сообщение не может быть пустым");
    }

    // Получаем все настройки
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SETTINGS}!A2:B100`,
    });

    const rows = res.data.values || [];
    let found = false;
    let rowIndex = -1;

    // Ищем существующую запись (нормализуем ключ для сравнения)
    for (let i = 0; i < rows.length; i++) {
      const [key] = rows[i];
      if (
        key &&
        typeof key === "string" &&
        key.trim() === "напоминание_21день_текст"
      ) {
        found = true;
        rowIndex = i + 2; // +2 потому что A1 - заголовок, A2 - первая строка данных
        break;
      }
    }

    if (found) {
      // Обновляем существующую запись
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!B${rowIndex}`,
        valueInputOption: "RAW",
        requestBody: { values: [[message.trim()]] },
      });
    } else {
      // Добавляем новую запись
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [["напоминание_21день_текст", message.trim()]] },
      });
    }

    return true;
  }

  async function getTipsLink() {
    const settings = await getSettings();
    const link = settings.ссылка_на_чаевые;
    // Проверяем, что ссылка существует и не пустая
    if (link && typeof link === "string" && link.trim().length > 0) {
      return link.trim();
    }
    return "";
  }

  async function setTipsLink(link) {
    if (!link || typeof link !== "string" || link.trim().length === 0) {
      throw new Error("Ссылка не может быть пустой");
    }

    // Валидация URL
    const trimmedLink = link.trim();
    const isValidUrl =
      trimmedLink.startsWith("http://") ||
      trimmedLink.startsWith("https://") ||
      trimmedLink.startsWith("t.me/");

    if (!isValidUrl || trimmedLink.length < 5) {
      throw new Error(
        "Ссылка должна начинаться с http://, https:// или t.me/ и быть не менее 5 символов"
      );
    }

    // Получаем все настройки
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SETTINGS}!A2:B100`,
    });

    const rows = res.data.values || [];
    let found = false;
    let rowIndex = -1;

    // Ищем существующую запись (нормализуем ключ для сравнения)
    for (let i = 0; i < rows.length; i++) {
      const [key] = rows[i];
      if (key && typeof key === "string" && key.trim() === "ссылка_на_чаевые") {
        found = true;
        rowIndex = i + 2; // +2 потому что A1 - заголовок, A2 - первая строка данных
        break;
      }
    }

    if (found) {
      // Обновляем существующую запись
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!B${rowIndex}`,
        valueInputOption: "RAW",
        requestBody: { values: [[trimmedLink]] },
      });
    } else {
      // Добавляем новую запись
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [["ссылка_на_чаевые", trimmedLink]] },
      });
    }

    return true;
  }

  async function getBarberPhone() {
    const settings = await getSettings();
    return settings.телефон_мастера || "";
  }

  async function getBarberAddress() {
    const settings = await getSettings();
    return settings.адрес_мастера || "";
  }

  async function setBarberPhone(phone) {
    if (!phone || typeof phone !== "string" || phone.trim().length === 0) {
      throw new Error("Телефон не может быть пустым");
    }

    const trimmedPhone = phone.trim();

    // Получаем все настройки
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SETTINGS}!A2:B100`,
    });

    const rows = res.data.values || [];
    let found = false;
    let rowIndex = -1;

    // Ищем существующую запись
    for (let i = 0; i < rows.length; i++) {
      const [key] = rows[i];
      if (key && typeof key === "string" && key.trim() === "телефон_мастера") {
        found = true;
        rowIndex = i + 2;
        break;
      }
    }

    if (found) {
      // Обновляем существующую запись
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!B${rowIndex}`,
        valueInputOption: "RAW",
        requestBody: { values: [[trimmedPhone]] },
      });
    } else {
      // Добавляем новую запись
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [["телефон_мастера", trimmedPhone]] },
      });
    }

    return true;
  }

  async function setBarberAddress(address) {
    if (
      !address ||
      typeof address !== "string" ||
      address.trim().length === 0
    ) {
      throw new Error("Адрес не может быть пустым");
    }

    const trimmedAddress = address.trim();

    // Получаем все настройки
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.SETTINGS}!A2:B100`,
    });

    const rows = res.data.values || [];
    let found = false;
    let rowIndex = -1;

    // Ищем существующую запись
    for (let i = 0; i < rows.length; i++) {
      const [key] = rows[i];
      if (key && typeof key === "string" && key.trim() === "адрес_мастера") {
        found = true;
        rowIndex = i + 2;
        break;
      }
    }

    if (found) {
      // Обновляем существующую запись
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!B${rowIndex}`,
        valueInputOption: "RAW",
        requestBody: { values: [[trimmedAddress]] },
      });
    } else {
      // Добавляем новую запись
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [["адрес_мастера", trimmedAddress]] },
      });
    }

    return true;
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
    // Инициализируем дефолтное сообщение напоминания, если его нет
    if (!settings.напоминание_21день_текст) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: {
          values: [
            [
              "напоминание_21день_текст",
              "Привет, {clientName}! Тебя давно небыло на стрижке, пора подстричься!",
            ],
          ],
        },
      });
    }
    // Инициализируем дефолтную ссылку на чаевые, если её нет
    if (!settings.ссылка_на_чаевые) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.SETTINGS}!A2:B2`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: {
          values: [["ссылка_на_чаевые", ""]],
        },
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
          Цена,
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
          Исполнено_UTC,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          completedAtUtc: Исполнено_UTC,
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
      // TTL 30 минут для кэша расписания по дням
      expiresAt: now + 30 * 60 * 1000,
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
      price,
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
            price !== null && price !== undefined ? String(price) : "",
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
            "", // Исполнено_UTC - пусто при создании
            "", // Отменено_UTC
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
        targetDate = row[4]; // Дата теперь в индексе 4 (после Цена)
      }
    });

    if (targetRowIndex === -1) {
      return false;
    }

    const rowNumber = targetRowIndex + 2; // сдвиг из-за заголовков

    const rowValues = rows[targetRowIndex];
    // Статус в колонке L (index 11), Исполнено_UTC в колонке O (index 14), Отменено_UTC в колонке P (index 15)
    rowValues[11] = status;
    if (status === "отменена" && cancelledAtUtc) {
      rowValues[15] = cancelledAtUtc;
    }
    if (status === "исполнено" && completedAtUtc) {
      rowValues[14] = completedAtUtc; // Исполнено_UTC
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
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
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
        range: `${SHEET_NAMES.CLIENTS}!A2:L2`,
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
              "", // BanStatus
              "", // BanReason
              "", // Напоминание_21день_UTC
              "", // Последняя_рассылка_UTC
            ],
          ],
        },
      });
    } else {
      // Обновление существующего клиента
      const rowNumber = targetRowIndex + 2;
      const rowValues = rows[targetRowIndex];

      // Убедимся, что массив достаточно длинный
      while (rowValues.length < 12) {
        rowValues.push("");
      }

      rowValues[3] = username || rowValues[3] || "";
      rowValues[4] = name || rowValues[4] || "";
      rowValues[5] = phone || rowValues[5] || "";
      rowValues[6] = lastAppointmentAtUtc || rowValues[6] || "";
      rowValues[7] = existingTotal + 1;
      // Напоминание_21день_UTC (индекс 10) не обновляем при upsert
      // Последняя_рассылка_UTC (индекс 11) не обновляем при upsert

      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.CLIENTS}!A${rowNumber}:L${rowNumber}`,
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
          Цена,
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
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          Цена,
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
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          Цена,
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
          Исполнено_UTC,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          completedAtUtc: Исполнено_UTC,
          cancelledAtUtc: Отменено_UTC,
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
      Цена,
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
      Исполнено_UTC,
      Отменено_UTC,
    ] = row;

    return {
      id: ID_записи,
      createdAtUtc: Создано_UTC,
      service: Услуга,
      price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
      completedAtUtc: Исполнено_UTC,
      cancelledAtUtc: Отменено_UTC,
    };
  }

  async function getAppointmentByCancelCode(cancelCode) {
    // Комментарий: ищем запись по коду отмены
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    // Код отмены находится в колонке с индексом 12 (после добавления колонки Цена)
    const row = rows.find(
      (r) =>
        r[12] &&
        String(r[12]).toUpperCase() === String(cancelCode).toUpperCase()
    );
    if (!row) return null;

    const [
      ID_записи,
      Создано_UTC,
      Услуга,
      Цена,
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
      Исполнено_UTC,
      Отменено_UTC,
    ] = row;

    return {
      id: ID_записи,
      createdAtUtc: Создано_UTC,
      service: Услуга,
      price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
      completedAtUtc: Исполнено_UTC,
      cancelledAtUtc: Отменено_UTC,
    };
  }

  async function getAllClients() {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    return rows.map((row) => {
      const [
        ID_клиента,
        Первое_посещение_UTC,
        Telegram_ID,
        Username_Telegram,
        Имя,
        Телефон,
        Последняя_запись_UTC,
        Всего_записей,
        BanStatus,
        BanReason,
        Напоминание_21день_UTC,
        Последняя_рассылка_UTC,
      ] = row;
      return {
        id: ID_клиента,
        firstSeenUtc: Первое_посещение_UTC,
        telegramId: Telegram_ID,
        username: Username_Telegram,
        name: Имя,
        phone: Телефон,
        lastAppointmentAtUtc: Последняя_запись_UTC,
        total: Number(Всего_записей) || 0,
        banned: String(BanStatus || "").toLowerCase() === "banned",
        banReason: BanReason || "",
        reminder21DaySentAtUtc: Напоминание_21день_UTC || "",
        lastBroadcastSentAtUtc: Последняя_рассылка_UTC || "",
      };
    });
  }

  async function getClientsForBroadcast() {
    // Комментарий: возвращает клиентов, которым можно отправить рассылку
    // Исключает тех, кому уже отправляли менее 24 часов назад
    const clients = await getAllClients();
    const now = dayjs().utc();
    const hours24 = 24 * 60 * 60 * 1000; // 24 часа в миллисекундах

    return clients.filter((client) => {
      // Исключаем клиентов без telegramId
      if (!client.telegramId) {
        return false;
      }

      // Исключаем забаненных
      if (client.banned) {
        return false;
      }

      // Если метка пуста - клиент доступен
      if (!client.lastBroadcastSentAtUtc || client.lastBroadcastSentAtUtc.trim() === "") {
        return true;
      }

      // Проверяем, прошло ли 24 часа с последней рассылки
      try {
        const lastSent = dayjs.utc(client.lastBroadcastSentAtUtc);
        const diffMs = now.diff(lastSent);
        return diffMs >= hours24;
      } catch (e) {
        // Если ошибка парсинга - считаем клиента доступным
        return true;
      }
    });
  }

  async function markBroadcastSent(telegramIds) {
    // Комментарий: отмечает клиентов меткой времени рассылки (массовое обновление)
    if (!Array.isArray(telegramIds) || telegramIds.length === 0) {
      return true;
    }

    const telegramIdsStr = telegramIds.map(String);
    const now = dayjs().utc().toISOString();

    // Получаем все клиенты
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    // Находим строки для обновления
    const updates = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const telegramId = row[2]; // Telegram_ID в индексе 2
      
      if (telegramId && telegramIdsStr.includes(String(telegramId))) {
        const rowNumber = i + 2; // +2 из-за заголовка и сдвига индекса
        // Убедимся, что массив достаточно длинный
        while (row.length < 12) {
          row.push("");
        }
        // Обновляем колонку L (индекс 11) - Последняя_рассылка_UTC
        row[11] = now;
        updates.push({ rowNumber, row });
      }
    }

    // Массовое обновление (батчинг по 100 строк для надежности)
    for (let i = 0; i < updates.length; i += 100) {
      const batch = updates.slice(i, i + 100);
      const batchRequests = batch.map(({ rowNumber, row }) => ({
        updateCells: {
          range: {
            sheetId: undefined, // Используется имя листа в range
            startRowIndex: rowNumber - 1,
            endRowIndex: rowNumber,
            startColumnIndex: 11, // Колонка L (индекс 11)
            endColumnIndex: 12,
          },
          values: [[now]],
          fields: "userEnteredValue",
        },
      }));

      // Обновляем по одной строке (Google Sheets API требует имя листа)
      for (const { rowNumber } of batch) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: config.google.sheetsId,
          range: `${SHEET_NAMES.CLIENTS}!L${rowNumber}`,
          valueInputOption: "RAW",
          requestBody: {
            values: [[now]],
          },
        });
      }
    }

    return true;
  }

  async function clearBroadcastMarks() {
    // Комментарий: очищает все метки рассылки (для cron задачи)
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    // Находим все строки с заполненной меткой
    const rowsToClear = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      // Проверяем, что метка заполнена (индекс 11)
      if (row.length > 11 && row[11] && row[11].trim() !== "") {
        const rowNumber = i + 2;
        rowsToClear.push(rowNumber);
      }
    }

    // Очищаем метки пакетами (по одной для надежности)
    for (const rowNumber of rowsToClear) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.CLIENTS}!L${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: {
          values: [[""]],
        },
      });
    }

    return rowsToClear.length;
  }

  async function getClientByTelegramId(telegramId) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    let targetRow = null;
    rows.forEach((row, idx) => {
      const existingTelegramId = row[2];
      if (String(existingTelegramId) === String(telegramId)) {
        targetRowIndex = idx;
        targetRow = row;
      }
    });

    if (targetRowIndex === -1) return null;
    return { rowNumber: targetRowIndex + 2, rowValues: targetRow };
  }

  async function getUserBanStatus(telegramId) {
    const entry = await getClientByTelegramId(telegramId);
    if (!entry) return { banned: false, reason: "" };
    const row = entry.rowValues || [];
    const ban = String(row[8] || "").toLowerCase() === "banned";
    const reason = row[9] || "";
    return { banned: ban, reason };
  }

  async function setUserBanStatus(telegramId, banned, reason = "") {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    rows.forEach((row, idx) => {
      const existingTelegramId = row[2];
      if (String(existingTelegramId) === String(telegramId)) {
        targetRowIndex = idx;
      }
    });

    const banValue = banned ? "banned" : "";

    if (targetRowIndex === -1) {
      // Пользователь не найден в таблице - не создаем новую строку при блокировке
      // Новая строка должна создаваться только при регистрации через другие механизмы
      return true;
    } else {
      const rowNumber = targetRowIndex + 2;
      // Обновляем только столбцы I (индекс 8) и J (индекс 9) - статус бана и причина
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.CLIENTS}!I${rowNumber}:J${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: {
          values: [[banValue, reason || ""]],
        },
      });
    }

    return true;
  }

  async function getAllAppointmentsForClient(telegramId) {
    // Комментарий: получаем все записи клиента (включая завершенные и отмененные)
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
          Цена,
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
          Исполнено_UTC,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          completedAtUtc: Исполнено_UTC,
          cancelledAtUtc: Отменено_UTC,
        };
      })
      .filter((row) => String(row.telegramId) === String(telegramId));
  }

  async function getClientsFor21DayReminder() {
    // Комментарий: получаем клиентов, которым нужно отправить напоминание
    console.log(
      `[getClientsFor21DayReminder] Начало проверки в ${dayjs().utc().toISOString()}`
    );
    const clients = await getAllClients();
    const allAppointments = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const appointmentRows = allAppointments.data.values || [];

    // Преобразуем все записи в объекты
    const appointments = appointmentRows.map((row) => {
      const [
        ID_записи,
        Создано_UTC,
        Услуга,
        Цена,
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
        Исполнено_UTC,
        Отменено_UTC,
      ] = row;
      return {
        id: ID_записи,
        createdAtUtc: Создано_UTC,
        service: Услуга,
        price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
        completedAtUtc: Исполнено_UTC,
        cancelledAtUtc: Отменено_UTC,
      };
    });

    const now = dayjs().utc();
    const clientsForReminder = [];

    console.log(
      `[getClientsFor21DayReminder] Проверка ${clients.length} клиентов, текущее время UTC: ${now.toISOString()}`
    );

    for (const client of clients) {
      // Проверяем базовые условия
      if (!client.telegramId || client.banned) {
        continue;
      }

      // Проверяем, что напоминание еще не отправлялось
      if (
        client.reminder21DaySentAtUtc &&
        client.reminder21DaySentAtUtc.trim() !== ""
      ) {
        continue;
      }

      // Получаем все записи клиента
      const clientAppointments = appointments.filter(
        (app) => String(app.telegramId) === String(client.telegramId)
      );

      // Проверяем, что нет активных записей
      const hasActiveAppointments = clientAppointments.some(
        (app) => app.status === "активна"
      );
      if (hasActiveAppointments) {
        continue;
      }

      // Находим последнюю завершенную запись
      const completedAppointments = clientAppointments.filter(
        (app) => app.status === "исполнено" && app.completedAtUtc
      );

      let lastHaircutDate = null;

      if (completedAppointments.length > 0) {
        // Сортируем по дате завершения (по убыванию)
        completedAppointments.sort((a, b) => {
          const dateA = dayjs.utc(a.completedAtUtc);
          const dateB = dayjs.utc(b.completedAtUtc);
          return dateB.isAfter(dateA) ? 1 : -1;
        });
        lastHaircutDate = dayjs.utc(completedAppointments[0].completedAtUtc);
      } else if (
        client.lastAppointmentAtUtc &&
        client.lastAppointmentAtUtc.trim() !== ""
      ) {
        // Fallback: используем Последняя_запись_UTC из таблицы клиентов
        lastHaircutDate = dayjs.utc(client.lastAppointmentAtUtc);
      } else {
        // Нет данных о последней стрижке - пропускаем
        continue;
      }

      // Вычисляем разницу в днях
      // Используем разницу в миллисекундах и делим на количество миллисекунд в дне
      // Это более точный расчет, чем dayjs.diff с "day", который округляет вниз
      const diffMs = now.valueOf() - lastHaircutDate.valueOf();
      const diffDays = diffMs / (24 * 60 * 60 * 1000);
      const daysSinceLastHaircut = Math.floor(diffDays);

      // Проверяем, что прошло >= 21 день (21 * 24 часа = 504 часа)
      // Используем точное сравнение: если прошло 21 день или больше (>= 21 * 24 часа)
      if (diffDays >= 21) {
        console.log(
          `[getClientsFor21DayReminder] Клиент ${client.telegramId}: последняя запись ${lastHaircutDate.toISOString()}, прошло ${diffDays.toFixed(2)} дней (${daysSinceLastHaircut} полных дней)`
        );
        clientsForReminder.push({
          ...client,
          daysSinceLastHaircut,
          lastHaircutDate: lastHaircutDate.toISOString(),
        });
      }
    }

    console.log(
      `[getClientsFor21DayReminder] Найдено клиентов для напоминания: ${clientsForReminder.length}`
    );

    return clientsForReminder;
  }

  async function mark21DayReminderSent(telegramId) {
    // Комментарий: помечаем, что напоминание отправлено
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    rows.forEach((row, idx) => {
      const existingTelegramId = row[2];
      if (String(existingTelegramId) === String(telegramId)) {
        targetRowIndex = idx;
      }
    });

    if (targetRowIndex === -1) {
      return false;
    }

    const rowNumber = targetRowIndex + 2;
    const reminderSentAtUtc = dayjs().utc().toISOString();

    // Обновляем только столбец K (индекс 10) - Напоминание_21день_UTC
    await sheets.spreadsheets.values.update({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!K${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[reminderSentAtUtc]],
      },
    });

    return true;
  }

  async function clear21DayReminderSentAt(telegramId) {
    // Комментарий: очищаем поле напоминания при создании новой записи
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!A2:L2000`,
    });
    const rows = res.data.values || [];

    let targetRowIndex = -1;
    rows.forEach((row, idx) => {
      const existingTelegramId = row[2];
      if (String(existingTelegramId) === String(telegramId)) {
        targetRowIndex = idx;
      }
    });

    if (targetRowIndex === -1) {
      return false;
    }

    const rowNumber = targetRowIndex + 2;

    // Очищаем столбец K (индекс 10) - Напоминание_21день_UTC
    await sheets.spreadsheets.values.update({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.CLIENTS}!K${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[""]],
      },
    });

    return true;
  }

  async function getCompletedAppointments({ startDate, endDate } = {}) {
    // Комментарий: получаем все завершенные записи за период
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    const timezone = await getTimezone();

    // Преобразуем все записи в объекты
    let appointments = rows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Цена,
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
          Исполнено_UTC,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          completedAtUtc: Исполнено_UTC,
          cancelledAtUtc: Отменено_UTC,
        };
      })
      .filter((row) => row.status === "исполнено");

    // Фильтруем по датам, если указаны
    if (startDate || endDate) {
      appointments = appointments.filter((app) => {
        // Используем Исполнено_UTC (приоритет) или Дата (если Исполнено_UTC пусто)
        let dateToCheck = null;

        if (app.completedAtUtc && app.completedAtUtc.trim() !== "") {
          // Используем дату из Исполнено_UTC, конвертируем в таймзону салона
          try {
            dateToCheck = dayjs
              .utc(app.completedAtUtc)
              .tz(timezone)
              .startOf("day");
          } catch (e) {
            // Если не удалось распарсить, используем дату записи
            if (app.date) {
              dateToCheck = dayjs.tz(app.date, timezone).startOf("day");
            }
          }
        } else if (app.date) {
          // Используем дату записи
          dateToCheck = dayjs.tz(app.date, timezone).startOf("day");
        }

        if (!dateToCheck) {
          return false;
        }

        // Проверяем диапазон
        if (startDate) {
          const start = dayjs.tz(startDate, timezone).startOf("day");
          if (dateToCheck.isBefore(start)) {
            return false;
          }
        }

        if (endDate) {
          const end = dayjs.tz(endDate, timezone).endOf("day");
          if (dateToCheck.isAfter(end)) {
            return false;
          }
        }

        return true;
      });
    }

    return appointments;
  }

  async function getCancelledAppointmentsInPeriod({ startDate, endDate } = {}) {
    // Если период не задан, возвращаем пустой список, чтобы не считать «за всё время»
    if (!startDate && !endDate) {
      return [];
    }

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
    });
    const rows = res.data.values || [];

    const timezone = await getTimezone();

    let appointments = rows
      .map((row) => {
        const [
          ID_записи,
          Создано_UTC,
          Услуга,
          Цена,
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
          Исполнено_UTC,
          Отменено_UTC,
        ] = row;
        return {
          id: ID_записи,
          createdAtUtc: Создано_UTC,
          service: Услуга,
          price: Цена ? (isNaN(Number(Цена)) ? null : Number(Цена)) : null,
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
          completedAtUtc: Исполнено_UTC,
          cancelledAtUtc: Отменено_UTC,
        };
      })
      .filter((row) => row.status === "отменена" && row.cancelledAtUtc);

    // Фильтруем по дате отмены (Отменено_UTC), приведённой к таймзоне салона
    if (startDate || endDate) {
      appointments = appointments.filter((app) => {
        let dateToCheck = null;

        try {
          dateToCheck = dayjs
            .utc(app.cancelledAtUtc)
            .tz(timezone)
            .startOf("day");
        } catch (e) {
          // Если не удалось распарсить дату отмены — пропускаем
          return false;
        }

        if (!dateToCheck) {
          return false;
        }

        if (startDate) {
          const start = dayjs.tz(startDate, timezone).startOf("day");
          if (dateToCheck.isBefore(start)) {
            return false;
          }
        }

        if (endDate) {
          const end = dayjs.tz(endDate, timezone).endOf("day");
          if (dateToCheck.isAfter(end)) {
            return false;
          }
        }

        return true;
      });
    }

    return appointments;
  }

  async function getNewClientsCountInPeriod({ startDate, endDate } = {}) {
    // Если период не задан, считаем, что показатель не используется
    if (!startDate && !endDate) {
      return 0;
    }

    const timezone = await getTimezone();
    const clients = await getAllClients();

    const count = clients.filter((client) => {
      if (!client.firstSeenUtc || String(client.firstSeenUtc).trim() === "") {
        return false;
      }

      let dateToCheck = null;
      try {
        dateToCheck = dayjs
          .utc(client.firstSeenUtc)
          .tz(timezone)
          .startOf("day");
      } catch (e) {
        return false;
      }

      if (!dateToCheck) {
        return false;
      }

      if (startDate) {
        const start = dayjs.tz(startDate, timezone).startOf("day");
        if (dateToCheck.isBefore(start)) {
          return false;
        }
      }

      if (endDate) {
        const end = dayjs.tz(endDate, timezone).endOf("day");
        if (dateToCheck.isAfter(end)) {
          return false;
        }
      }

      return true;
    }).length;

    return count;
  }

  // Очистка устаревшего in-memory кэша Google Sheets (расписание и рабочие часы)
  function cleanupSheetsCache() {
    const now = Date.now();
    let removedDayKeys = 0;

    Object.keys(dayCache).forEach((key) => {
      const entry = dayCache[key];
      if (!entry || !entry.expiresAt || entry.expiresAt < now) {
        delete dayCache[key];
        removedDayKeys += 1;
      }
    });

    if (
      workHoursCache &&
      workHoursCache.expiresAt &&
      workHoursCache.expiresAt < now
    ) {
      workHoursCache = { byDate: {}, byWeekday: {}, expiresAt: 0 };
    }

    if (removedDayKeys > 0) {
      console.log(
        `[googleSheets] Cleaned dayCache: removed ${removedDayKeys} expired keys`
      );
    }
  }

  // Ежечасная очистка кэша Google Sheets (лёгкая операция, допустима в рабочее время)
  cron.schedule(
    "0 * * * *",
    () => {
      try {
        cleanupSheetsCache();
      } catch (e) {
        console.error(
          "[googleSheets] Error during hourly cache cleanup:",
          e.message || e
        );
      }
    },
    {
      timezone: config.defaultTimezone || "UTC",
    }
  );

  return {
    ensureSheetsStructure,
    getSettings,
    getTimezone,
    get21DayReminderMessage,
    set21DayReminderMessage,
    getTipsLink,
    setTipsLink,
    getBarberPhone,
    getBarberAddress,
    setBarberPhone,
    setBarberAddress,
    getDaySchedule,
    appendAppointment,
    updateAppointmentStatus,
    upsertClient,
    getFutureAppointmentsForTelegram,
    getAppointmentsByDate,
    getAllActiveAppointments,
    getAppointmentById,
    getAppointmentByCancelCode,
    getAllClients,
    getClientsForBroadcast,
    markBroadcastSent,
    clearBroadcastMarks,
    getWorkHoursForDate,
    invalidateWorkHoursCache,
    getClientByTelegramId,
    getUserBanStatus,
    setUserBanStatus,
    getAllAppointmentsForClient,
    getClientsFor21DayReminder,
    mark21DayReminderSent,
    clear21DayReminderSentAt,
    getCompletedAppointments,
    getCancelledAppointmentsInPeriod,
    getNewClientsCountInPeriod,
  };
}

module.exports = {
  createSheetsService,
};
