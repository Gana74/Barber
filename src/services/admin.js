const fs = require("fs").promises;
const path = require("path");

const BANS_FILE = path.resolve(process.cwd(), "banned.json");

async function readBans() {
  try {
    const raw = await fs.readFile(BANS_FILE, { encoding: "utf8" });
    return JSON.parse(raw || "[]");
  } catch (e) {
    return [];
  }
}

async function writeBans(list) {
  await fs.writeFile(BANS_FILE, JSON.stringify(list, null, 2), {
    encoding: "utf8",
  });
}

async function isBanned(telegramId) {
  const bans = await readBans();
  return bans.some((b) => String(b) === String(telegramId));
}

async function banUser(telegramId, reason = "", sheetsService = null) {
  const bans = await readBans();
  if (!bans.some((b) => String(b) === String(telegramId))) {
    bans.push(String(telegramId));
    await writeBans(bans);
  }
  // Синхронизируем с таблицей, если сервис передан
  try {
    if (sheetsService && sheetsService.setUserBanStatus) {
      await sheetsService.setUserBanStatus(telegramId, true, reason || "");
    }
  } catch (e) {
    // не прерываем, если не удалось записать в таблицу
  }
  return true;
}

async function unbanUser(telegramId, sheetsService = null) {
  const telegramIdStr = String(telegramId);
  let bans = await readBans();
  const initialLength = bans.length;
  bans = bans.filter((b) => String(b) !== telegramIdStr);
  const removed = initialLength !== bans.length;

  // Удаляем пользователя из banned.json, если он там был
  if (removed) {
    await writeBans(bans);
  }

  // Всегда синхронизируем с таблицей, если сервис передан
  // Это нужно, чтобы очистить статус бана в таблице, даже если пользователя не было в banned.json
  try {
    if (sheetsService && sheetsService.setUserBanStatus) {
      await sheetsService.setUserBanStatus(telegramIdStr, false, "");
    }
  } catch (e) {
    console.error("Ошибка при обновлении статуса бана в таблице:", e);
    // не прерываем, если не удалось записать в таблицу
  }
  return true;
}

async function getBans() {
  return await readBans();
}

async function broadcastToClients(bot, sheetsService, payload, options = 200) {
  // Поддерживаем два режима: передан список получателей или отправка всем клиентам
  // Если передан опциональный параметр `options.recipients` - используем его (массив telegramId строк).
  // options: { recipients: string[] | null, throttleMs: number, skipBanned: boolean }
  const MAX_RECIPIENTS = 250; // Максимальное количество получателей
  
  const results = [];

  // Normalize options for backward compatibility (old style: throttleMs number or object)
  let recipients = null;
  let optsThrottle = 750;
  let skipBanned = true;
  if (typeof options === "number") {
    optsThrottle = options;
  } else if (typeof options === "object" && options !== null) {
    const o = options;
    recipients = Array.isArray(o.recipients) ? o.recipients.map(String) : null;
    optsThrottle = typeof o.throttleMs === "number" ? o.throttleMs : 750;
    skipBanned = o.skipBanned !== false;
  }

  const bans = await readBans();

  // Build targets: either from recipients array or from clientsForBroadcast with telegramId
  // Если передан явный список получателей - используем его, иначе используем getClientsForBroadcast()
  const targets = [];
  if (recipients && recipients.length) {
    recipients.forEach((id) => targets.push({ telegramId: String(id) }));
  } else {
    // Используем getClientsForBroadcast() вместо getAllClients() для автоматической фильтрации
    const clientsForBroadcast = await sheetsService.getClientsForBroadcast();
    clientsForBroadcast.forEach((c) => {
      if (c && c.telegramId) targets.push({ telegramId: String(c.telegramId) });
    });
  }

  // Ограничение максимального количества получателей - берем первые 250
  const targetsToSend = targets.slice(0, MAX_RECIPIENTS);
  
  if (targets.length > MAX_RECIPIENTS) {
    // Предупреждение будет показано в предпросмотре, здесь просто ограничиваем
  }

  // Список успешно отправленных для отметки
  const sentIds = [];

  for (const c of targetsToSend) {
    const tid = String(c.telegramId || "");
    if (!tid) continue;

    // Пропускаем забаненных пользователей
    if (skipBanned) {
      if (bans.some((b) => String(b) === tid)) {
        continue;
      }
      if (sheetsService && sheetsService.getUserBanStatus) {
        try {
          const st = await sheetsService.getUserBanStatus(tid);
          if (st && st.banned) continue;
        } catch (e) {
          // игнорируем ошибки таблицы
        }
      }
    }

    try {
      if (payload && typeof payload === "object" && payload.kind === "photo") {
        await bot.telegram.sendPhoto(tid, payload.fileId, {
          caption: payload.caption || undefined,
        });
      } else {
        const text =
          typeof payload === "string"
            ? payload
            : (payload && payload.text) || "";
        await bot.telegram.sendMessage(tid, text);
      }
      results.push({ id: tid, ok: true });
      sentIds.push(tid);
    } catch (e) {
      results.push({ id: tid, ok: false, error: e.message });
    }
    if (optsThrottle) await new Promise((r) => setTimeout(r, optsThrottle));
  }

  // Отмечаем успешно отправленных клиентов меткой рассылки
  if (sentIds.length > 0 && sheetsService && sheetsService.markBroadcastSent) {
    try {
      await sheetsService.markBroadcastSent(sentIds);
    } catch (e) {
      // Логируем ошибку, но не прерываем выполнение
      console.error("Ошибка при отметке клиентов в рассылке:", e.message || e);
    }
  }

  return results;
}

module.exports = {
  isBanned,
  banUser,
  unbanUser,
  getBans,
  broadcastToClients,
};
