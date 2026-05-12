const { Markup } = require("telegraf");

function userKeyboard() {
  return Markup.keyboard([
    ["Записаться 💇‍♂️"],
    ["Мои записи"],
    ["Как добраться 🗺️"],
    ["Прайс"],
  ]).resize();
}

module.exports = {
  userKeyboard,
};
