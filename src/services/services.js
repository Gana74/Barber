// Сервис для работы с услугами (CRUD операции)
// Хранение в JSON-файле services.json

const fs = require("fs");
const path = require("path");

const SERVICES_FILE = path.resolve(process.cwd(), "services.json");

// Хардкод услуг для миграции (из booking.js)
const HARDCODED_SERVICES = {
  MEN_HAIRCUT: {
    key: "MEN_HAIRCUT",
    name: "Мужская стрижка",
    price: null,
    durationMin: 60,
  },
  BEARD: {
    key: "BEARD",
    name: "Оформление бороды",
    price: null,
    durationMin: 30,
  },
  BUZZCUT: {
    key: "BUZZCUT",
    name: "Стрижка под машинку",
    price: null,
    durationMin: 30,
  },
  WOMEN_HAIRCUT: {
    key: "WOMEN_HAIRCUT",
    name: "Женская стрижка",
    price: null,
    durationMin: 60,
  },
};

function loadServices() {
  try {
    if (!fs.existsSync(SERVICES_FILE)) {
      // Файл не существует - выполняем миграцию
      migrateFromHardcoded();
    }
    const content = fs.readFileSync(SERVICES_FILE, { encoding: "utf8" });
    const parsed = JSON.parse(content || "{}");
    return parsed;
  } catch (e) {
    console.warn("Failed to load services.json:", e.message);
    // При ошибке возвращаем хардкод
    return HARDCODED_SERVICES;
  }
}

function saveServices(services) {
  try {
    const content = JSON.stringify(services, null, 2);
    fs.writeFileSync(SERVICES_FILE, content, { encoding: "utf8" });
    return true;
  } catch (e) {
    console.error("Failed to save services.json:", e.message);
    return false;
  }
}

function migrateFromHardcoded() {
  try {
    if (fs.existsSync(SERVICES_FILE)) {
      // Файл уже существует, не мигрируем
      return;
    }
    console.log("Migrating services from hardcoded to services.json...");
    saveServices(HARDCODED_SERVICES);
    console.log("Services migration completed.");
  } catch (e) {
    console.error("Failed to migrate services:", e.message);
  }
}

function getAllServices() {
  const services = loadServices();
  return Object.values(services);
}

function getServiceByKey(key) {
  const services = loadServices();
  return services[key] || null;
}

function validateKey(key) {
  if (!key || typeof key !== "string") return false;
  // Только латинские буквы, цифры и подчёркивания
  return /^[A-Za-z0-9_]+$/.test(key);
}

function validateService({ key, name, price, durationMin }) {
  if (!validateKey(key)) {
    return {
      valid: false,
      error:
        "Ключ должен содержать только латинские буквы, цифры и подчёркивания",
    };
  }
  if (!name || typeof name !== "string" || name.trim().length === 0) {
    return { valid: false, error: "Название не может быть пустым" };
  }
  if (price !== null && price !== undefined) {
    const priceNum = Number(price);
    if (isNaN(priceNum) || priceNum < 0) {
      return { valid: false, error: "Цена должна быть неотрицательным числом" };
    }
  }
  const durationNum = Number(durationMin);
  if (isNaN(durationNum) || durationNum <= 0) {
    return {
      valid: false,
      error: "Продолжительность должна быть положительным числом",
    };
  }
  return { valid: true };
}

function createService({ key, name, price, durationMin }) {
  const validation = validateService({ key, name, price, durationMin });
  if (!validation.valid) {
    return { ok: false, error: validation.error };
  }

  const services = loadServices();
  if (services[key]) {
    return { ok: false, error: "Услуга с таким ключом уже существует" };
  }

  services[key] = {
    key,
    name: name.trim(),
    price: price !== null && price !== undefined ? Number(price) : null,
    durationMin: Number(durationMin),
  };

  const saved = saveServices(services);
  if (!saved) {
    return { ok: false, error: "Не удалось сохранить услугу" };
  }

  return { ok: true, service: services[key] };
}

function updateService(key, { name, price, durationMin }) {
  const services = loadServices();
  if (!services[key]) {
    return { ok: false, error: "Услуга не найдена" };
  }

  const updated = { ...services[key] };
  if (name !== undefined) {
    if (!name || typeof name !== "string" || name.trim().length === 0) {
      return { ok: false, error: "Название не может быть пустым" };
    }
    updated.name = name.trim();
  }
  if (price !== undefined) {
    if (price !== null) {
      const priceNum = Number(price);
      if (isNaN(priceNum) || priceNum < 0) {
        return { ok: false, error: "Цена должна быть неотрицательным числом" };
      }
      updated.price = priceNum;
    } else {
      updated.price = null;
    }
  }
  if (durationMin !== undefined) {
    const durationNum = Number(durationMin);
    if (isNaN(durationNum) || durationNum <= 0) {
      return {
        ok: false,
        error: "Продолжительность должна быть положительным числом",
      };
    }
    updated.durationMin = durationNum;
  }

  services[key] = updated;
  const saved = saveServices(services);
  if (!saved) {
    return { ok: false, error: "Не удалось сохранить изменения" };
  }

  return { ok: true, service: updated };
}

function deleteService(key) {
  const services = loadServices();
  if (!services[key]) {
    return { ok: false, error: "Услуга не найдена" };
  }

  delete services[key];
  const saved = saveServices(services);
  if (!saved) {
    return { ok: false, error: "Не удалось удалить услугу" };
  }

  return { ok: true };
}

// Инициализация при загрузке модуля
migrateFromHardcoded();

module.exports = {
  getAllServices,
  getServiceByKey,
  createService,
  updateService,
  deleteService,
  loadServices,
  saveServices,
};
