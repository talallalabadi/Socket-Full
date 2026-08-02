// notifications.js — simulates sending email/SMS confirmations.
// Every "send" is logged to the db so the admin dashboard can show a real
// notification history. Swap sendToClient's body for a real provider
// (SendGrid, Twilio, etc.) later — callers never change.

const db = require('./db');

function sendToClient(appointment, type) {
  const messages = {
    confirmation: `You're booked! ${appointment.serviceName} on ${new Date(appointment.start).toLocaleString()}. Reply "reschedule" or "cancel" any time.`,
    reschedule: `Your appointment is now ${appointment.serviceName} on ${new Date(appointment.start).toLocaleString()}.`,
    cancellation: `Your ${appointment.serviceName} appointment has been cancelled. Reply "book" to schedule a new one.`
  };

  return db.insertNotification({
    type,
    channel: 'email/sms (simulated)',
    to: appointment.clientContact,
    appointmentId: appointment.id,
    message: messages[type] || 'Update on your appointment.'
  });
}

function notifyOwner(appointment, action) {
  const actionText = {
    booked: 'New booking',
    rescheduled: 'Rescheduled',
    cancelled: 'Cancelled'
  }[action] || 'Update';

  return db.insertNotification({
    type: 'owner_alert',
    channel: 'dashboard/email (simulated)',
    to: 'owner',
    appointmentId: appointment.id,
    message: `${actionText}: ${appointment.serviceName} with ${appointment.clientName || 'a client'} — ${new Date(appointment.start).toLocaleString()}`
  });
}

module.exports = { sendToClient, notifyOwner };
