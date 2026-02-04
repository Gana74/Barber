// Сервис работы с Google Sheets
// Здесь: авторизация по service account, кэш расписания, базовые CRUD по листам

const { google } = require("googleapis");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezonePlugin = require("dayjs/plugin/timezone");
const cron = require("node-cron");

dayjs.extend(utc);
dayjs.extend(timezonePlugin);

// Комментарий: имена листов в таблице
// ВНИМАНИЕ: фактические названия листов в Google Sheets:
// - "Расписание" — лист с рабочими часами (ранее WorkHours)
// - "Блокировка времени" — лист с блокировками слотов (ранее Расписание)
const SHEET_NAMES = {
  SETTINGS: "Настройки",
  // Лист с блокировками времени
  SCHEDULE: "Блокировка времени",
  APPOINTMENTS: "Записи",
  APPOINTMENTS_ARCHIVE: "Записи_Архив",
  CLIENTS: "Клиенты",
  // Лист с рабочими часами
  WORKHOURS: "Расписание",
};

// Комментарий: константы для архивирования
const ARCHIVE_MONTHS = 2; // период хранения активных записей (месяцев)
const ACTIVE_RANGE = "A2:Q500"; // уменьшенный диапазон для активных записей

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
  [SHEET_NAMES.APPOINTMENTS_ARCHIVE]: [
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
    ["https://www.googleapis.com/auth/spreadsheets"],
  );
}

async function createSheetsService(config) {
  const auth = createGoogleAuth(config);
  const sheets = google.sheets({ version: "v4", auth });

  // Комментарий: вспомогательная функция для парсинга строки записи в объект
  function parseAppointmentRow(row) {
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

  // Комментарий: вспомогательная функция для чтения всех записей из активных и архива
  async function getAllAppointmentsFromBothSheets() {
    const appointments = [];

    // Читаем активные записи
    try {
      const activeRes = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
      });
      const activeRows = activeRes.data.values || [];
      appointments.push(...activeRows.map(parseAppointmentRow));
    } catch (e) {
      console.warn("[getAllAppointmentsFromBothSheets] Ошибка при чтении активных записей:", e.message || e);
    }

    // Читаем архивные записи
    try {
      const archiveRes = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q10000`,
      });
      const archiveRows = archiveRes.data.values || [];
      appointments.push(...archiveRows.map(parseAppointmentRow));
    } catch (e) {
      // Если архив еще не создан или пуст, игнорируем ошибку
      console.warn("[getAllAppointmentsFromBothSheets] Ошибка при чтении архива:", e.message || e);
    }

    return appointments;
  }

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
      throw new Error("Данные для чаевых не могут быть пустыми");
    }

    const trimmedLink = link.trim();

    // Валидация: может быть либо URL, либо номер телефона
    const isValidUrl =
      trimmedLink.startsWith("http://") ||
      trimmedLink.startsWith("https://") ||
      trimmedLink.startsWith("t.me/");

    const isPhoneNumber =
      /^[\d\s\-+()]+$/.test(trimmedLink) && trimmedLink.length >= 5;

    if (!isValidUrl && !isPhoneNumber) {
      throw new Error(
        "Укажите ссылку (http://, https://, t.me/) или номер телефона",
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
      (meta.data.sheets || []).map((s) => s.properties.title),
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

    // Автоматическая миграция существующих данных в архив (только при первом запуске)
    // Проверяем, есть ли записи в архиве - если очень мало или нет, выполняем миграцию
    try {
      const archiveCheck = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q10`,
      });
      const archiveRows = archiveCheck.data.values || [];
      
      // Если в архиве меньше 5 записей, считаем что миграция еще не выполнялась
      if (archiveRows.length < 5) {
        console.log(
          "[ensureSheetsStructure] Запуск автоматической миграции существующих данных в архив",
        );
        try {
          await migrateExistingDataToArchive(ARCHIVE_MONTHS);
          console.log(
            "[ensureSheetsStructure] Автоматическая миграция завершена",
          );
        } catch (e) {
          console.error(
            "[ensureSheetsStructure] Ошибка при автоматической миграции (не критично):",
            e.message || e,
          );
          // Не прерываем инициализацию при ошибке миграции
        }
      }
    } catch (e) {
      // Если архив еще не создан или пуст, это нормально - миграция выполнится при следующем запуске
      console.log(
        "[ensureSheetsStructure] Архив пуст или не создан, миграция будет выполнена позже",
      );
    }
  }

  function invalidateWorkHoursCache() {
    workHoursCache = { byDate: {}, byWeekday: {}, expiresAt: 0 };
  }

  async function getDaySchedule(dateStr, { fresh = false } = {}) {
    // Комментарий: используем кэш (если не запрошен свежий результат)
    const now = Date.now();
    if (!fresh) {
      const cached = dayCache[dateStr];
      if (cached && cached.expiresAt > now) {
        return {
          schedule: cached.schedule,
          appointments: cached.appointments,
        };
      }
    }

    // Читаем блокировки времени (лист "Блокировка времени")
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

    // Читаем Записи (только активные, из уменьшенного диапазона)
    const appRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
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
          row.status !== "исполнено",
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
    { cancelledAtUtc, completedAtUtc } = {},
  ) {
    // Комментарий: ищем запись по id в обоих листах (сначала активные, потом архив)
    // Сначала ищем в активных записях
    const activeRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
    });
    const activeRows = activeRes.data.values || [];

    let targetRowIndex = -1;
    let targetDate = null;
    let sheetName = SHEET_NAMES.APPOINTMENTS;
    let rows = activeRows;

    activeRows.forEach((row, idx) => {
      if (row[0] === id) {
        targetRowIndex = idx;
        targetDate = row[4]; // Дата теперь в индексе 4 (после Цена)
      }
    });

    // Если не найдено в активных, ищем в архиве
    if (targetRowIndex === -1) {
      try {
        const archiveRes = await sheets.spreadsheets.values.get({
          spreadsheetId: config.google.sheetsId,
          range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q10000`,
        });
        const archiveRows = archiveRes.data.values || [];
        
        archiveRows.forEach((row, idx) => {
          if (row[0] === id) {
            targetRowIndex = idx;
            targetDate = row[4];
            sheetName = SHEET_NAMES.APPOINTMENTS_ARCHIVE;
            rows = archiveRows;
          }
        });
      } catch (e) {
        // Если архив еще не создан или пуст, игнорируем ошибку
        console.warn("[updateAppointmentStatus] Ошибка при чтении архива:", e.message || e);
      }
    }

    if (targetRowIndex === -1) {
      return false;
    }

    const rowNumber = targetRowIndex + 2; // сдвиг из-за заголовков

    const rowValues = rows[targetRowIndex];
    // Статус в колонке L (index 11), Исполнено_UTC в колонке O (index 14), Отменено_UTC в колонке P (index 15)
    // Telegram_ID в колонке N (index 13)
    rowValues[11] = status;
    if (status === "отменена" && cancelledAtUtc) {
      rowValues[15] = cancelledAtUtc;
    }
    if (status === "исполнено" && completedAtUtc) {
      rowValues[14] = completedAtUtc; // Исполнено_UTC
      
      // Обновляем lastAppointmentAtUtc в таблице клиентов
      const telegramId = rowValues[13]; // Telegram_ID
      if (telegramId && String(telegramId).trim() !== "") {
        try {
          await upsertClient({
            telegramId: String(telegramId),
            lastAppointmentAtUtc: completedAtUtc,
          });
        } catch (e) {
          // Логируем ошибку, но не блокируем обновление статуса записи
          console.error(
            `[updateAppointmentStatus] Ошибка при обновлении lastAppointmentAtUtc для клиента ${telegramId}:`,
            e.message || e,
          );
        }
      }
    }

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.google.sheetsId,
      range: `${sheetName}!A${rowNumber}:Q${rowNumber}`,
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
    // Комментарий: читаем только из активных записей (уменьшенный диапазон)
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
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
          row.status === "активна",
      )
      .filter((row) => {
        const dt = dayjs.utc(`${row.date}T${row.timeStart}:00Z`).tz
          ? dayjs.utc(`${row.date}T${row.timeStart}:00Z`).tz(timezone)
          : dayjs.utc(`${row.date}T${row.timeStart}:00Z`);
        return dt.isAfter(now);
      });
  }

  async function getAppointmentsByDate(dateStr) {
    // Комментарий: читаем только из активных записей (уменьшенный диапазон)
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
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
    // Комментарий: получаем все активные записи для автоматического завершения (только из активных, уменьшенный диапазон)
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
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
        (row) => row.status === "активна" && row.id && row.date && row.timeEnd,
      );
  }

  async function getAppointmentById(id) {
    // Комментарий: ищем одну конкретную запись по id (сначала в активных, потом в архиве)
    // Сначала ищем в активных записях
    const activeRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
    });
    const activeRows = activeRes.data.values || [];
    const activeRow = activeRows.find((r) => r[0] === id);
    
    if (activeRow) {
      return parseAppointmentRow(activeRow);
    }

    // Если не найдено в активных, ищем в архиве
    try {
      const archiveRes = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q10000`,
      });
      const archiveRows = archiveRes.data.values || [];
      const archiveRow = archiveRows.find((r) => r[0] === id);
      
      if (archiveRow) {
        return parseAppointmentRow(archiveRow);
      }
    } catch (e) {
      // Если архив еще не создан или пуст, игнорируем ошибку
      console.warn("[getAppointmentById] Ошибка при чтении архива:", e.message || e);
    }

    return null;
  }

  async function getAppointmentByCancelCode(cancelCode) {
    // Комментарий: ищем запись по коду отмены (сначала в активных, потом в архиве)
    // Сначала ищем в активных записях
    const activeRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.google.sheetsId,
      range: `${SHEET_NAMES.APPOINTMENTS}!${ACTIVE_RANGE}`,
    });
    const activeRows = activeRes.data.values || [];
    
    // Код отмены находится в колонке с индексом 12 (после добавления колонки Цена)
    const activeRow = activeRows.find(
      (r) =>
        r[12] &&
        String(r[12]).toUpperCase() === String(cancelCode).toUpperCase(),
    );
    
    if (activeRow) {
      return parseAppointmentRow(activeRow);
    }

    // Если не найдено в активных, ищем в архиве
    try {
      const archiveRes = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q10000`,
      });
      const archiveRows = archiveRes.data.values || [];
      const archiveRow = archiveRows.find(
        (r) =>
          r[12] &&
          String(r[12]).toUpperCase() === String(cancelCode).toUpperCase(),
      );
      
      if (archiveRow) {
        return parseAppointmentRow(archiveRow);
      }
    } catch (e) {
      // Если архив еще не создан или пуст, игнорируем ошибку
      console.warn("[getAppointmentByCancelCode] Ошибка при чтении архива:", e.message || e);
    }

    return null;
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
      if (
        !client.lastBroadcastSentAtUtc ||
        client.lastBroadcastSentAtUtc.trim() === ""
      ) {
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
    // Комментарий: получаем все записи клиента (включая завершенные и отмененные) из активных и архива
    const appointments = await getAllAppointmentsFromBothSheets();
    return appointments.filter((row) => String(row.telegramId) === String(telegramId));
  }

  async function getClientsFor21DayReminder() {
    // Комментарий: оптимизированная версия - использует lastAppointmentAtUtc как основной источник
    console.log(
      `[getClientsFor21DayReminder] Начало проверки в ${dayjs().utc().toISOString()}`,
    );
    const clients = await getAllClients();
    
    // Получаем только активные записи для быстрой проверки
    const activeAppointments = await getAllActiveAppointments();

    // Создаем Set для быстрой проверки активных записей
    const clientsWithActiveAppointments = new Set(
      activeAppointments
        .map((app) => String(app.telegramId))
        .filter(Boolean),
    );

    const now = dayjs().utc();
    const clientsForReminder = [];

    console.log(
      `[getClientsFor21DayReminder] Проверка ${clients.length} клиентов, текущее время UTC: ${now.toISOString()}`,
    );

    for (const client of clients) {
      // Быстрые фильтры в начале
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

      // Проверка активных записей (быстрая проверка через Set)
      if (clientsWithActiveAppointments.has(String(client.telegramId))) {
        continue;
      }

      // Используем lastAppointmentAtUtc как основной источник данных
      if (!client.lastAppointmentAtUtc || client.lastAppointmentAtUtc.trim() === "") {
        continue;
      }

      const lastHaircutDate = dayjs.utc(client.lastAppointmentAtUtc);

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
          `[getClientsFor21DayReminder] Клиент ${client.telegramId}: последняя запись ${lastHaircutDate.toISOString()}, прошло ${diffDays.toFixed(2)} дней (${daysSinceLastHaircut} полных дней)`,
        );
        clientsForReminder.push({
          ...client,
          daysSinceLastHaircut,
          lastHaircutDate: lastHaircutDate.toISOString(),
        });
      }
    }

    console.log(
      `[getClientsFor21DayReminder] Найдено клиентов для напоминания: ${clientsForReminder.length}`,
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
    // Комментарий: получаем все завершенные записи за период (из активных и архива)
    const timezone = await getTimezone();

    // Получаем все записи из обоих листов
    let appointments = await getAllAppointmentsFromBothSheets();
    
    // Фильтруем только завершенные
    appointments = appointments.filter((row) => row.status === "исполнено");

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

    const timezone = await getTimezone();

    // Получаем все записи из обоих листов
    let appointments = await getAllAppointmentsFromBothSheets();
    
    // Фильтруем только отмененные с датой отмены
    appointments = appointments.filter((row) => row.status === "отменена" && row.cancelledAtUtc);

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
        `[googleSheets] Cleaned dayCache: removed ${removedDayKeys} expired keys`,
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
          e.message || e,
        );
      }
    },
    {
      timezone: config.defaultTimezone || "UTC",
    },
  );

  // Ежемесячная ротация записей в архив (3:00 ночи, 1-го числа каждого месяца)
  cron.schedule(
    "0 3 1 * *",
    async () => {
      try {
        console.log(
          `[googleSheets] Запуск ежемесячной ротации записей в архив (${dayjs().format("YYYY-MM-DD HH:mm")})`,
        );
        const result = await archiveOldAppointments(ARCHIVE_MONTHS);
        console.log(
          `[googleSheets] Ротация завершена. Перенесено записей: ${result.archived}, ошибок: ${result.errors.length}`,
        );
      } catch (e) {
        console.error(
          "[googleSheets] Критическая ошибка при ежемесячной ротации:",
          e.message || e,
        );
      }
    },
    {
      timezone: config.defaultTimezone || "UTC",
    },
  );

  // Комментарий: архивирование старых записей
  async function archiveOldAppointments(monthsToKeep = ARCHIVE_MONTHS) {
    const timezone = await getTimezone();
    const now = dayjs().tz(timezone);
    const cutoffDate = now.subtract(monthsToKeep, "month").startOf("day");

    console.log(
      `[archiveOldAppointments] Начало архивирования. Граничная дата: ${cutoffDate.format("YYYY-MM-DD")}`,
    );

    try {
      // Читаем все активные записи
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: config.google.sheetsId,
        range: `${SHEET_NAMES.APPOINTMENTS}!A2:Q2000`,
      });
      const rows = res.data.values || [];

      if (rows.length === 0) {
        console.log("[archiveOldAppointments] Нет записей для архивирования");
        return { archived: 0, errors: [] };
      }

      const appointmentsToArchive = [];
      const rowIndicesToDelete = [];

      // Определяем, какие записи нужно архивировать
      rows.forEach((row, idx) => {
        if (!row[0]) return; // Пропускаем пустые строки

        const appointment = parseAppointmentRow(row);
        let dateToCheck = null;

        // Для завершенных записей используем дату завершения
        if (
          appointment.status === "исполнено" &&
          appointment.completedAtUtc &&
          appointment.completedAtUtc.trim() !== ""
        ) {
          try {
            dateToCheck = dayjs
              .utc(appointment.completedAtUtc)
              .tz(timezone)
              .startOf("day");
          } catch (e) {
            // Если не удалось распарсить, используем дату записи
            if (appointment.date) {
              dateToCheck = dayjs.tz(appointment.date, timezone).startOf("day");
            }
          }
        } else if (appointment.date) {
          // Для остальных используем дату записи
          dateToCheck = dayjs.tz(appointment.date, timezone).startOf("day");
        }

        if (dateToCheck && dateToCheck.isBefore(cutoffDate)) {
          appointmentsToArchive.push(row);
          rowIndicesToDelete.push(idx + 2); // +2 из-за заголовка и индексации с 1
        }
      });

      if (appointmentsToArchive.length === 0) {
        console.log("[archiveOldAppointments] Нет записей для архивирования");
        return { archived: 0, errors: [] };
      }

      console.log(
        `[archiveOldAppointments] Найдено записей для архивирования: ${appointmentsToArchive.length}`,
      );

      // Переносим записи в архив (batch append)
      if (appointmentsToArchive.length > 0) {
        await sheets.spreadsheets.values.append({
          spreadsheetId: config.google.sheetsId,
          range: `${SHEET_NAMES.APPOINTMENTS_ARCHIVE}!A2:Q2`,
          valueInputOption: "RAW",
          insertDataOption: "INSERT_ROWS",
          requestBody: {
            values: appointmentsToArchive,
          },
        });
      }

      // Удаляем записи из активных (получаем sheetId и удаляем батчами)
      const errors = [];
      
      // Получаем sheetId для листа "Записи"
      const spreadsheet = await sheets.spreadsheets.get({
        spreadsheetId: config.google.sheetsId,
      });
      const appointmentsSheet = spreadsheet.data.sheets.find(
        (s) => s.properties.title === SHEET_NAMES.APPOINTMENTS,
      );
      
      if (!appointmentsSheet) {
        throw new Error(`Лист "${SHEET_NAMES.APPOINTMENTS}" не найден`);
      }
      
      const sheetId = appointmentsSheet.properties.sheetId;
      
      // Сортируем индексы по убыванию для правильного удаления
      const sortedIndices = [...rowIndicesToDelete].sort((a, b) => b - a);
      
      // Удаляем строки батчами (по 50 за раз для надежности)
      const batchSize = 50;
      for (let i = 0; i < sortedIndices.length; i += batchSize) {
        const batch = sortedIndices.slice(i, i + batchSize);
        const deleteRequests = batch.map((rowNumber) => ({
          deleteDimension: {
            range: {
              sheetId: sheetId,
              dimension: "ROWS",
              startIndex: rowNumber - 1, // Google Sheets использует 0-based индексы
              endIndex: rowNumber,
            },
          },
        }));
        
        try {
          await sheets.spreadsheets.batchUpdate({
            spreadsheetId: config.google.sheetsId,
            requestBody: {
              requests: deleteRequests,
            },
          });
        } catch (e) {
          errors.push({
            rows: batch,
            error: e.message || e,
          });
          console.error(
            `[archiveOldAppointments] Ошибка при удалении батча строк:`,
            e.message || e,
          );
        }
      }

      // Инвалидируем кэш для всех затронутых дат
      appointmentsToArchive.forEach((row) => {
        const appointment = parseAppointmentRow(row);
        if (appointment.date) {
          invalidateDayCache(appointment.date);
        }
      });

      console.log(
        `[archiveOldAppointments] Архивирование завершено. Перенесено записей: ${appointmentsToArchive.length}, ошибок: ${errors.length}`,
      );

      return {
        archived: appointmentsToArchive.length,
        errors: errors,
      };
    } catch (e) {
      console.error(
        "[archiveOldAppointments] Критическая ошибка при архивировании:",
        e.message || e,
      );
      throw e;
    }
  }

  // Комментарий: миграция существующих данных в архив (однократная операция)
  async function migrateExistingDataToArchive(monthsToKeep = ARCHIVE_MONTHS) {
    console.log(
      `[migrateExistingDataToArchive] Начало миграции существующих данных (период: ${monthsToKeep} месяцев)`,
    );

    try {
      const result = await archiveOldAppointments(monthsToKeep);
      console.log(
        `[migrateExistingDataToArchive] Миграция завершена. Перенесено записей: ${result.archived}`,
      );
      return result;
    } catch (e) {
      console.error(
        "[migrateExistingDataToArchive] Ошибка при миграции:",
        e.message || e,
      );
      throw e;
    }
  }

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
    archiveOldAppointments,
    migrateExistingDataToArchive,
  };
}

module.exports = {
  createSheetsService,
};
