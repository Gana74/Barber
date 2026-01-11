// Резервное копирование чувствительных данных
// Асинхронное копирование с дебаунсингом для оптимизации производительности

const fs = require("fs").promises;
const path = require("path");

const BACKUP_DIR = path.resolve(process.cwd(), "backups");
const DEBOUNCE_DELAY = 30 * 1000; // 30 секунд
const BACKUP_RETENTION_DAYS = 30;

// Файлы для резервного копирования
const FILES_TO_BACKUP = [
  "sessions.json",
  "banned.json",
  "services.json",
];

// Таймер для дебаунсинга
let debounceTimer = null;
let pendingBackup = false;

/**
 * Обеспечиваем существование директории бэкапов
 */
async function ensureBackupDir() {
  try {
    await fs.mkdir(BACKUP_DIR, { recursive: true });
  } catch (e) {
    // Игнорируем ошибки, если директория уже существует
  }
}

/**
 * Очистка старых бэкапов (старше 30 дней)
 */
async function cleanupOldBackups() {
  try {
    const files = await fs.readdir(BACKUP_DIR).catch(() => []);
    const now = Date.now();
    const retentionTime = BACKUP_RETENTION_DAYS * 24 * 60 * 60 * 1000;

    for (const file of files) {
      if (file.startsWith("backup-") && file.endsWith(".tar")) {
        const filePath = path.resolve(BACKUP_DIR, file);
        const stats = await fs.stat(filePath).catch(() => null);
        if (stats && stats.mtimeMs < now - retentionTime) {
          await fs.unlink(filePath).catch(() => {});
        }
      }
    }
  } catch (e) {
    // Игнорируем ошибки очистки
  }
}

/**
 * Выполнение резервного копирования
 */
async function performBackup() {
  pendingBackup = false;

  try {
    await ensureBackupDir();
    await cleanupOldBackups();

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const backupSubDir = path.resolve(BACKUP_DIR, `backup-${timestamp}`);

    // Создаем поддиректорию для этого бэкапа
    await fs.mkdir(backupSubDir, { recursive: true });

    // Копируем файлы
    for (const fileName of FILES_TO_BACKUP) {
      const sourcePath = path.resolve(process.cwd(), fileName);
      const destPath = path.resolve(backupSubDir, fileName);

      try {
        // Проверяем существование файла
        await fs.access(sourcePath);
        // Копируем файл
        await fs.copyFile(sourcePath, destPath);
      } catch (e) {
        // Игнорируем отсутствующие файлы
        if (e.code !== "ENOENT") {
          console.warn(`Failed to backup ${fileName}:`, e.message);
        }
      }
    }

    // Создаем архив (простое копирование в tar-подобную структуру)
    // Для простоты создаем мета-файл с информацией о бэкапе
    const metaFile = path.resolve(backupSubDir, "backup-meta.json");
    const backedUpFiles = [];
    for (const fileName of FILES_TO_BACKUP) {
      try {
        await fs.access(path.resolve(process.cwd(), fileName));
        backedUpFiles.push(fileName);
      } catch {
        // Файл не существует, пропускаем
      }
    }
    const meta = {
      timestamp: new Date().toISOString(),
      files: backedUpFiles,
    };
    await fs.writeFile(metaFile, JSON.stringify(meta, null, 2), "utf8");

    console.log(`Backup created: ${backupSubDir}`);
  } catch (e) {
    console.error("Failed to create backup:", e.message);
  }
}

/**
 * Запланировать резервное копирование с дебаунсингом
 * Если несколько вызовов подряд, выполняется один бэкап через 30 секунд
 */
function scheduleBackup() {
  // Если уже запланирован, сбрасываем таймер
  if (debounceTimer) {
    clearTimeout(debounceTimer);
  }

  pendingBackup = true;

  // Устанавливаем новый таймер
  debounceTimer = setTimeout(async () => {
    if (pendingBackup) {
      // Выполняем бэкап асинхронно (не блокирует поток)
      performBackup().catch((e) => {
        console.error("Backup error:", e.message);
      });
    }
    debounceTimer = null;
  }, DEBOUNCE_DELAY);
}

/**
 * Немедленное резервное копирование (без дебаунсинга)
 * Используется для запланированных бэкапов
 */
async function backupNow() {
  // Отменяем дебаунсинг если есть
  if (debounceTimer) {
    clearTimeout(debounceTimer);
    debounceTimer = null;
  }
  pendingBackup = false;

  // Выполняем бэкап асинхронно
  performBackup().catch((e) => {
    console.error("Backup error:", e.message);
  });
}

/**
 * Настройка периодического резервного копирования
 * @param {number} intervalHours - Интервал в часах (по умолчанию 24 - раз в день)
 */
function setupScheduledBackups(intervalHours = 24) {
  const intervalMs = intervalHours * 60 * 60 * 1000;

  // Первый бэкап через час после запуска
  setTimeout(() => {
    backupNow();
    // Затем по расписанию
    setInterval(backupNow, intervalMs);
  }, 60 * 60 * 1000); // 1 час
}

module.exports = {
  scheduleBackup, // Для вызова при критичных операциях (с дебаунсингом)
  backupNow, // Для немедленного бэкапа (по расписанию)
  setupScheduledBackups, // Для настройки периодических бэкапов
};
