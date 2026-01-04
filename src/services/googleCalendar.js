const { google } = require("googleapis");

function createGoogleAuthForCalendar(config) {
  return new google.auth.JWT(
    config.google.clientEmail,
    undefined,
    config.google.privateKey,
    ["https://www.googleapis.com/auth/calendar"]
  );
}

async function createCalendarService(config) {
  const auth = createGoogleAuthForCalendar(config);
  const calendar = google.calendar({ version: "v3", auth });
  const calendarId = config.google.calendarId;

  async function createEventForAppointment(appointment, timezone) {
    if (!calendarId) return null;
    try {
      const start = `${appointment.date}T${appointment.timeStart}:00`;
      const end = `${appointment.date}T${appointment.timeEnd}:00`;

      const res = await calendar.events.insert({
        calendarId,
        requestBody: {
          summary: `Запись: ${appointment.service} — ${
            appointment.clientName || ""
          }`,
          description: `appointmentId:${appointment.id}\nphone:${
            appointment.phone || ""
          }\ncomment:${appointment.comment || ""}`,
          start: { dateTime: start, timeZone: timezone },
          end: { dateTime: end, timeZone: timezone },
          extendedProperties: {
            private: {
              appointmentId: appointment.id,
            },
          },
        },
      });

      return res.data && res.data.id ? res.data.id : null;
    } catch (err) {
      console.error(
        "googleCalendar.createEventForAppointment error:",
        err.message || err
      );
      return null;
    }
  }

  async function findEventByAppointmentId(appointmentId) {
    if (!calendarId) return null;
    try {
      // list events in reasonable time window to find by extendedProperties
      const res = await calendar.events.list({
        calendarId,
        q: appointmentId,
        maxResults: 2500,
        showDeleted: false,
      });
      const items = res.data.items || [];
      const found = items.find((it) => {
        try {
          return (
            it.extendedProperties &&
            it.extendedProperties.private &&
            it.extendedProperties.private.appointmentId === appointmentId
          );
        } catch (e) {
          return false;
        }
      });
      return found || null;
    } catch (err) {
      console.error(
        "googleCalendar.findEventByAppointmentId error:",
        err.message || err
      );
      return null;
    }
  }

  async function deleteEventForAppointmentId(appointmentId) {
    if (!calendarId) return false;
    try {
      const event = await findEventByAppointmentId(appointmentId);
      if (!event) return false;
      await calendar.events.delete({ calendarId, eventId: event.id });
      return true;
    } catch (err) {
      console.error(
        "googleCalendar.deleteEventForAppointmentId error:",
        err.message || err
      );
      return false;
    }
  }

  return {
    createEventForAppointment,
    findEventByAppointmentId,
    deleteEventForAppointmentId,
  };
}

module.exports = { createCalendarService };
