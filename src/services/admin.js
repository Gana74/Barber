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

async function banUser(telegramId) {
  const bans = await readBans();
  if (!bans.some((b) => String(b) === String(telegramId))) {
    bans.push(String(telegramId));
    await writeBans(bans);
  }
  return true;
}

async function unbanUser(telegramId) {
  let bans = await readBans();
  bans = bans.filter((b) => String(b) !== String(telegramId));
  await writeBans(bans);
  return true;
}

async function getBans() {
  return await readBans();
}

async function broadcastToClients(
  bot,
  sheetsService,
  message,
  throttleMs = 200
) {
  // Поддерживаем два режима: передан список получателей или отправка всем клиентам
  // Если передан опциональный параметр `options.recipients` - используем его (массив telegramId строк).
  // options: { recipients: string[] | null, throttleMs: number, skipBanned: boolean }
  const clientsAll = await sheetsService.getAllClients();
  const results = [];

  // Normalize options if called in old style
  let recipients = null;
  let optsThrottle = throttleMs;
  let skipBanned = true;
  if (typeof throttleMs === "object" && throttleMs !== null) {
    const o = throttleMs;
    recipients = Array.isArray(o.recipients) ? o.recipients.map(String) : null;
    optsThrottle = typeof o.throttleMs === "number" ? o.throttleMs : 200;
    skipBanned = o.skipBanned !== false;
  }

  const bans = await readBans();

  // Build targets: either from recipients array or from clientsAll with telegramId
  const targets = [];
  if (recipients && recipients.length) {
    recipients.forEach((id) => targets.push({ telegramId: String(id) }));
  } else {
    clientsAll.forEach((c) => {
      if (c && c.telegramId) targets.push({ telegramId: String(c.telegramId) });
    });
  }

  for (const c of targets) {
    const tid = String(c.telegramId || "");
    if (!tid) continue;
    if (skipBanned && bans.some((b) => String(b) === tid)) continue;
    try {
      await bot.telegram.sendMessage(tid, message);
      results.push({ id: tid, ok: true });
    } catch (e) {
      results.push({ id: tid, ok: false, error: e.message });
    }
    if (optsThrottle) await new Promise((r) => setTimeout(r, optsThrottle));
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
