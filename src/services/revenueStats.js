// Сервис для расчета и форматирования статистики доходов

function calculateRevenueStats(appointments) {
  // Фильтруем записи с ценой
  const appointmentsWithPrice = appointments.filter(
    (app) =>
      app.price !== null && app.price !== undefined && !isNaN(Number(app.price))
  );

  // Общая сумма
  const total = appointmentsWithPrice.reduce((sum, app) => {
    return sum + Number(app.price);
  }, 0);

  // Группировка по услугам
  const byServiceMap = {};

  appointmentsWithPrice.forEach((app) => {
    const serviceName = app.service || "Неизвестная услуга";
    if (!byServiceMap[serviceName]) {
      byServiceMap[serviceName] = {
        service: serviceName,
        revenue: 0,
        count: 0,
      };
    }
    byServiceMap[serviceName].revenue += Number(app.price);
    byServiceMap[serviceName].count += 1;
  });

  // Преобразуем в массив и сортируем по сумме (убывание)
  const byService = Object.values(byServiceMap).sort(
    (a, b) => b.revenue - a.revenue
  );

  return {
    total: Math.round(total * 100) / 100, // Округляем до 2 знаков после запятой
    count: appointmentsWithPrice.length,
    byService,
  };
}

function formatRevenueStats(stats, periodLabel) {
  const lines = [
    `📈 Выручка за ${periodLabel}:`,
    `• Всего: ${formatNumber(stats.total)} ₽`,
    `• Записей исполнено: ${stats.count}`,
    ``,
  ];

  if (stats.byService.length > 0) {
    lines.push(`Топ услуг:`);
    stats.byService.forEach((item) => {
      lines.push(
        `• ${item.service} — ${formatNumber(item.revenue)} ₽ (${
          item.count
        } ${getRecordWord(item.count)})`
      );
    });
  } else {
    lines.push(`Нет данных о доходах за этот период.`);
  }

  return lines.join("\n");
}

function formatNumber(num) {
  // Форматируем число с пробелами для тысяч
  return String(Math.round(num * 100) / 100).replace(
    /\B(?=(\d{3})+(?!\d))/g,
    " "
  );
}

function getRecordWord(count) {
  // Правильное склонение слова "запись"
  const lastDigit = count % 10;
  const lastTwoDigits = count % 100;

  if (lastTwoDigits >= 11 && lastTwoDigits <= 14) {
    return "записей";
  }
  if (lastDigit === 1) {
    return "запись";
  }
  if (lastDigit >= 2 && lastDigit <= 4) {
    return "записи";
  }
  return "записей";
}

module.exports = {
  calculateRevenueStats,
  formatRevenueStats,
};
