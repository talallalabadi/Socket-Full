// calendar.js — stands in for the Google Calendar API.
//
// In a production build, `syncEvent` would call calendar.events.insert /
// update / delete via googleapis, using OAuth credentials for the business's
// calendar. Every appointment in this app already carries the fields that
// call needs (start, end, summary, attendee), so swapping this module is a
// matter of replacing the body of syncEvent — nothing upstream changes.

const log = [];

function syncEvent(appointment, action) {
  const entry = {
    action, // 'created' | 'updated' | 'cancelled'
    appointmentId: appointment.id,
    summary: `${appointment.serviceName} — ${appointment.clientName || 'Guest'}`,
    start: appointment.start,
    end: appointment.end,
    syncedAt: new Date().toISOString(),
    externalEventId: `mock_gcal_${appointment.id}`
  };
  log.push(entry);
  // console.log(`[calendar:mock] ${action} ->`, entry.summary);
  return entry;
}

function getSyncLog() {
  return log;
}

module.exports = { syncEvent, getSyncLog };
