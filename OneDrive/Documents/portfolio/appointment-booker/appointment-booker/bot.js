// bot.js — the "AI" is a finite state machine, on purpose.
//
// Booking an appointment is a fixed sequence of questions (service -> time
// -> name -> contact -> confirm), so it doesn't need a language model to
// drive it — it needs a state machine that can't go off-script. Every reply
// below is a template, not a generation. Swapping this for an LLM later
// only means changing how `handleMessage` decides what to say, not the
// scheduler/db/calendar layers it calls into.

const db = require('./db');
const scheduler = require('./scheduler');

const YES = ['yes', 'y', 'yep', 'yeah', 'confirm', 'correct', 'sounds good'];
const NO = ['no', 'n', 'nope', 'cancel that', 'nevermind', 'never mind'];

function listServicesText() {
  const { services } = db.getBusiness();
  const lines = services.map((s, i) => `${i + 1}. ${s.name} — ${s.duration} min, $${s.price}`);
  return `What would you like to book?\n${lines.join('\n')}`;
}

function matchService(text) {
  const { services } = db.getBusiness();
  const asNumber = parseInt(text, 10);
  if (!isNaN(asNumber) && services[asNumber - 1]) return services[asNumber - 1];
  const lower = text.toLowerCase();
  return services.find(s => s.name.toLowerCase().includes(lower)) || null;
}

function listSlotsText(slots) {
  if (slots.length === 0) {
    return "I don't have any open slots in the next week — please check back soon, or type 'menu' to pick a different service.";
  }
  const shown = slots.slice(0, 6);
  const lines = shown.map((s, i) => `${i + 1}. ${s.label}`);
  return `Here's what's open:\n${lines.join('\n')}\n\nReply with a number to book that time.`;
}

function matchSlot(text, slots) {
  const asNumber = parseInt(text, 10);
  const shown = slots.slice(0, 6);
  if (!isNaN(asNumber) && shown[asNumber - 1]) return shown[asNumber - 1];
  return null;
}

function summaryText(context) {
  const service = scheduler.getService(context.serviceId);
  const start = new Date(context.selectedSlot.start);
  return (
    `Here's what I've got:\n` +
    `• ${service.name} (${service.duration} min, $${service.price})\n` +
    `• ${scheduler.formatSlotLabel(start)}\n` +
    `• Name: ${context.name}\n` +
    `• Contact: ${context.contact}\n\n` +
    `Reply 'yes' to confirm or 'no' to start over.`
  );
}

function resetToServiceSelection(convo, prefix = '') {
  convo.state = 'awaiting_service';
  convo.context = {};
  return `${prefix}${listServicesText()}`;
}

function handleMessage(convo, rawText) {
  const text = (rawText || '').trim();
  const lower = text.toLowerCase();
  let result = { reply: '', action: null, appointment: null };

  // ---- global commands ----
  if (lower === 'help') {
    result.reply =
      "I can help you book, reschedule, or cancel an appointment. " +
      "Type 'book' any time to start over, 'reschedule' or 'cancel' if you already have an appointment.";
    return result;
  }

  if (['menu', 'restart', 'book', 'start over'].includes(lower) && convo.state !== 'awaiting_service') {
    result.reply = resetToServiceSelection(convo, "Sure, let's start fresh. ");
    return result;
  }

  if (lower === 'reschedule' && convo.state === 'booked' && convo.context.appointmentId) {
    const { slots } = scheduler.getAvailability(convo.context.serviceId);
    convo.context.slots = slots;
    convo.state = 'awaiting_reschedule_slot';
    result.reply = `No problem. ${listSlotsText(slots)}`;
    return result;
  }

  if (lower === 'cancel') {
    if (convo.state === 'booked' && convo.context.appointmentId) {
      convo.state = 'awaiting_cancel_confirm';
      result.reply = "Just to confirm — you'd like to cancel this appointment? Reply 'yes' or 'no'.";
      return result;
    }
    if (convo.state !== 'awaiting_service') {
      result.reply = resetToServiceSelection(convo, "No problem, cancelled that. ");
      return result;
    }
  }

  // ---- state machine ----
  switch (convo.state) {
    case 'awaiting_service': {
      const service = matchService(text);
      if (!service) {
        result.reply = `Sorry, I didn't catch that. ${listServicesText()}`;
        break;
      }
      convo.context.serviceId = service.id;
      const { slots } = scheduler.getAvailability(service.id);
      convo.context.slots = slots;
      convo.state = 'awaiting_slot';
      result.reply = `${service.name} it is. ${listSlotsText(slots)}`;
      break;
    }

    case 'awaiting_slot': {
      const slot = matchSlot(text, convo.context.slots || []);
      if (!slot) {
        result.reply = "Please reply with a number from the list, or type 'menu' to start over.";
        break;
      }
      convo.context.selectedSlot = slot;
      convo.state = 'awaiting_name';
      result.reply = "Great — what name should I book this under?";
      break;
    }

    case 'awaiting_name': {
      if (!text) {
        result.reply = "I didn't catch a name — what should I put down?";
        break;
      }
      convo.context.name = text;
      convo.state = 'awaiting_contact';
      result.reply = `Thanks, ${text}! What's the best email or phone number for your confirmation?`;
      break;
    }

    case 'awaiting_contact': {
      if (!text) {
        result.reply = "I'll need an email or phone number to send your confirmation to.";
        break;
      }
      convo.context.contact = text;
      convo.state = 'awaiting_confirm';
      result.reply = summaryText(convo.context);
      break;
    }

    case 'awaiting_confirm': {
      if (YES.includes(lower)) {
        const apt = scheduler.bookAppointment({
          serviceId: convo.context.serviceId,
          start: convo.context.selectedSlot.start,
          clientName: convo.context.name,
          clientContact: convo.context.contact
        });
        convo.context.appointmentId = apt.id;
        convo.state = 'booked';
        result.action = 'booked';
        result.appointment = apt;
        result.reply =
          `You're all set! ${apt.serviceName} on ${scheduler.formatSlotLabel(new Date(apt.start))}. ` +
          `A confirmation is on its way to ${apt.clientContact}. ` +
          `Type 'reschedule' or 'cancel' any time if plans change.`;
      } else if (NO.includes(lower)) {
        result.reply = resetToServiceSelection(convo, "No problem, let's start over. ");
      } else {
        result.reply = "Please reply 'yes' to confirm or 'no' to start over.";
      }
      break;
    }

    case 'booked': {
      const apt = db.getAppointment(convo.context.appointmentId);
      const when = apt ? scheduler.formatSlotLabel(new Date(apt.start)) : 'your booked time';
      result.reply =
        `You're already booked for ${when}. ` +
        `Type 'reschedule' to change the time, 'cancel' to cancel, or 'book' for a new appointment.`;
      break;
    }

    case 'awaiting_reschedule_slot': {
      const slot = matchSlot(text, convo.context.slots || []);
      if (!slot) {
        result.reply = "Please reply with a number from the list, or type 'menu' to start over.";
        break;
      }
      const apt = scheduler.rescheduleAppointment(convo.context.appointmentId, slot.start);
      convo.state = 'booked';
      result.action = 'rescheduled';
      result.appointment = apt;
      result.reply = `Done — you're now booked for ${scheduler.formatSlotLabel(new Date(apt.start))}.`;
      break;
    }

    case 'awaiting_cancel_confirm': {
      if (YES.includes(lower)) {
        const apt = scheduler.cancelAppointment(convo.context.appointmentId);
        result.action = 'cancelled';
        result.appointment = apt;
        result.reply = resetToServiceSelection(
          convo,
          "Done — that appointment is cancelled. Let me know if you'd like to book another. "
        );
      } else if (NO.includes(lower)) {
        convo.state = 'booked';
        result.reply = "Okay, keeping your appointment as-is.";
      } else {
        result.reply = "Reply 'yes' to confirm the cancellation or 'no' to keep your appointment.";
      }
      break;
    }

    default: {
      result.reply = resetToServiceSelection(convo);
    }
  }

  return result;
}

module.exports = { handleMessage, listServicesText };
