const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const db = require('./db');
const scheduler = require('./scheduler');
const bot = require('./bot');
const notifications = require('./notifications');
const calendar = require('./calendar');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------
// REST API — snapshot reads for the admin dashboard on page load.
// Everything that needs to be "live" (new bookings, notifications) goes
// over the socket instead, the same split used in the chatbot project.
// ---------------------------------------------------------------------

app.get('/api/business', (req, res) => {
  res.json(db.getBusiness());
});

app.get('/api/availability/:serviceId', (req, res) => {
  try {
    const days = parseInt(req.query.days, 10) || 7;
    res.json(scheduler.getAvailability(req.params.serviceId, days));
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/api/appointments', (req, res) => {
  const appointments = db.listAppointments().sort((a, b) => new Date(a.start) - new Date(b.start));
  res.json(appointments);
});

app.get('/api/clients', (req, res) => {
  res.json(db.listClients());
});

app.get('/api/conversations', (req, res) => {
  res.json(db.listConversations());
});

app.get('/api/notifications', (req, res) => {
  const notifs = db.listNotifications().sort((a, b) => new Date(b.ts) - new Date(a.ts));
  res.json(notifs);
});

app.get('/api/analytics', (req, res) => {
  const appointments = db.listAppointments();
  const now = new Date();
  const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

  const byStatus = appointments.reduce((acc, a) => {
    acc[a.status] = (acc[a.status] || 0) + 1;
    return acc;
  }, {});

  const bookedThisWeek = appointments.filter(a => new Date(a.createdAt) >= weekAgo).length;

  const byService = appointments.reduce((acc, a) => {
    if (a.status === 'cancelled') return acc;
    acc[a.serviceName] = (acc[a.serviceName] || 0) + 1;
    return acc;
  }, {});

  const bySlot = appointments.reduce((acc, a) => {
    if (a.status === 'cancelled') return acc;
    const hour = new Date(a.start).getHours();
    const label = `${hour % 12 === 0 ? 12 : hour % 12}${hour < 12 ? 'am' : 'pm'}`;
    acc[label] = (acc[label] || 0) + 1;
    return acc;
  }, {});

  const cancellationRate = appointments.length
    ? Math.round(((byStatus.cancelled || 0) / appointments.length) * 100)
    : 0;

  res.json({
    totalAppointments: appointments.length,
    active: (byStatus.confirmed || 0) + (byStatus.rescheduled || 0),
    cancelled: byStatus.cancelled || 0,
    bookedThisWeek,
    cancellationRate,
    byService,
    byPopularHour: bySlot,
    totalClients: db.listClients().length,
    totalConversations: db.listConversations().length
  });
});

app.post('/api/reset', (req, res) => {
  res.json(db.reset());
});

// ---------------------------------------------------------------------
// Socket.io — the live chat + live dashboard updates.
// Two rooms: one per conversation (customer <-> bot), and a shared
// "agents" room the admin dashboard joins to get notified in real time.
// ---------------------------------------------------------------------

io.on('connection', socket => {
  socket.on('customer:join', ({ conversationId, clientContact }) => {
    socket.join(conversationId);
    socket.data.conversationId = conversationId;

    const convo = db.getOrCreateConversation(conversationId, clientContact);
    if (convo.messages.length === 0) {
      const greeting = `Hi! I'm the booking assistant for ${db.getBusiness().name}. ${bot.listServicesText()}`;
      convo.messages.push({ from: 'bot', text: greeting, ts: new Date().toISOString() });
      db.saveConversation(convo);
      socket.emit('bot:reply', { text: greeting });
    } else {
      // reconnect: replay history so the widget can rebuild the thread
      socket.emit('conversation:history', convo.messages);
    }
  });

  socket.on('customer:message', ({ conversationId, text }) => {
    const convo = db.getOrCreateConversation(conversationId);
    convo.messages.push({ from: 'customer', text, ts: new Date().toISOString() });

    let outcome;
    try {
      outcome = bot.handleMessage(convo, text);
    } catch (err) {
      outcome = { reply: `Sorry, something went wrong: ${err.message}`, action: null, appointment: null };
    }

    convo.messages.push({ from: 'bot', text: outcome.reply, ts: new Date().toISOString() });
    db.saveConversation(convo);

    socket.emit('bot:reply', {
      text: outcome.reply,
      ticket: outcome.appointment
        ? { action: outcome.action, appointment: outcome.appointment }
        : null
    });
    io.to('agents').emit('admin:conversation_update', convo);

    if (outcome.action && outcome.appointment) {
      const apt = outcome.appointment;

      if (outcome.action === 'booked') {
        notifications.sendToClient(apt, 'confirmation');
        notifications.notifyOwner(apt, 'booked');
      } else if (outcome.action === 'rescheduled') {
        notifications.sendToClient(apt, 'reschedule');
        notifications.notifyOwner(apt, 'rescheduled');
      } else if (outcome.action === 'cancelled') {
        notifications.sendToClient(apt, 'cancellation');
        notifications.notifyOwner(apt, 'cancelled');
      }

      io.to('agents').emit('admin:appointment_update', { action: outcome.action, appointment: apt });
      io.to('agents').emit('admin:notifications_update', db.listNotifications());
    }
  });

  socket.on('admin:join', () => {
    socket.join('agents');
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Appointment booker running on http://localhost:${PORT}`);
  console.log(`Admin dashboard at http://localhost:${PORT}/admin.html`);
});
