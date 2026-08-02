// scheduler.js — the actual "smarts" of the system. No AI here on purpose:
// availability, conflict detection, and rescheduling are deterministic rules,
// which is exactly what makes them safe to automate.

const db = require('./db');
const calendar = require('./calendar');

const DAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

function pad(n) {
  return String(n).padStart(2, '0');
}

function formatTime(date) {
  return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function formatSlotLabel(date) {
  const day = date.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
  const time = date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
  return `${day}, ${time}`;
}

function isActive(apt) {
  return apt.status === 'confirmed' || apt.status === 'rescheduled';
}

function overlaps(startA, endA, startB, endB) {
  return startA < endB && startB < endA;
}

function getService(serviceId) {
  const business = db.getBusiness();
  return business.services.find(s => s.id === serviceId) || null;
}

// Generates open slots for a service over the next `days` days.
function getAvailability(serviceId, days = 7) {
  const business = db.getBusiness();
  const service = getService(serviceId);
  if (!service) throw new Error(`Unknown service: ${serviceId}`);

  const durationMs = service.duration * 60 * 1000;
  const stepMs = business.slotStepMinutes * 60 * 1000;
  const existing = db.listAppointments().filter(isActive);
  const now = new Date();
  const slots = [];

  for (let d = 0; d < days; d++) {
    const day = new Date(now);
    day.setDate(day.getDate() + d);
    const hours = business.hours[DAY_KEYS[day.getDay()]];
    if (!hours) continue; // closed that day

    const [openH, openM] = hours[0].split(':').map(Number);
    const [closeH, closeM] = hours[1].split(':').map(Number);

    const dayStart = new Date(day);
    dayStart.setHours(openH, openM, 0, 0);
    const dayClose = new Date(day);
    dayClose.setHours(closeH, closeM, 0, 0);

    for (let t = dayStart.getTime(); t + durationMs <= dayClose.getTime(); t += stepMs) {
      const slotStart = new Date(t);
      const slotEnd = new Date(t + durationMs);
      if (slotStart < now) continue; // no booking in the past

      const conflict = existing.some(a =>
        overlaps(slotStart, slotEnd, new Date(a.start), new Date(a.end))
      );
      if (!conflict) {
        slots.push({
          start: slotStart.toISOString(),
          end: slotEnd.toISOString(),
          label: formatSlotLabel(slotStart)
        });
      }
    }
  }

  return { service, slots };
}

function isSlotFree(start, end, ignoreAppointmentId = null) {
  const existing = db.listAppointments().filter(
    a => isActive(a) && a.id !== ignoreAppointmentId
  );
  return !existing.some(a => overlaps(start, end, new Date(a.start), new Date(a.end)));
}

function bookAppointment({ serviceId, start, clientName, clientContact }) {
  const service = getService(serviceId);
  if (!service) throw new Error('Unknown service');

  const startDate = new Date(start);
  const endDate = new Date(startDate.getTime() + service.duration * 60 * 1000);

  if (!isSlotFree(startDate, endDate)) {
    throw new Error('That slot was just taken. Please pick another.');
  }

  const client = db.upsertClient({ name: clientName, contact: clientContact });

  const appointment = db.insertAppointment({
    serviceId,
    serviceName: service.name,
    clientId: client.id,
    clientName,
    clientContact,
    start: startDate.toISOString(),
    end: endDate.toISOString(),
    status: 'confirmed'
  });

  db.linkAppointmentToClient(client.id, appointment.id);
  calendar.syncEvent(appointment, 'created');

  return appointment;
}

function rescheduleAppointment(appointmentId, newStart) {
  const apt = db.getAppointment(appointmentId);
  if (!apt) throw new Error('Appointment not found');
  const service = getService(apt.serviceId);

  const startDate = new Date(newStart);
  const endDate = new Date(startDate.getTime() + service.duration * 60 * 1000);

  if (!isSlotFree(startDate, endDate, appointmentId)) {
    throw new Error('That slot was just taken. Please pick another.');
  }

  const updated = db.updateAppointment(appointmentId, {
    start: startDate.toISOString(),
    end: endDate.toISOString(),
    status: 'rescheduled'
  });

  calendar.syncEvent(updated, 'updated');
  return updated;
}

function cancelAppointment(appointmentId) {
  const apt = db.getAppointment(appointmentId);
  if (!apt) throw new Error('Appointment not found');
  const updated = db.updateAppointment(appointmentId, { status: 'cancelled' });
  calendar.syncEvent(updated, 'cancelled');
  return updated;
}

module.exports = {
  getService,
  getAvailability,
  bookAppointment,
  rescheduleAppointment,
  cancelAppointment,
  formatSlotLabel,
  formatTime
};
