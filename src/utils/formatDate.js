const dayjs = require("dayjs");

function formatDate(d) {
  if (!d) return "";
  // Accept Date, ISO string (YYYY-MM-DD), or dayjs
  try {
    return dayjs(d).format("DD/MM/YYYY");
  } catch (e) {
    return String(d);
  }
}

module.exports = {
  formatDate,
};
