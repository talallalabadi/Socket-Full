// db.js — tiny JSON-file "database".
// Swap this module for a real PostgreSQL client later without touching
// scheduler.js, bot.js, or server.js — every function here keeps its
// signature and return shape.

const fs = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'data', 'db.json');
const SEED_PATH = path.join(__dirname, 'data', 'seed.json');

function load() {
  if (!fs.existsSync(DB_PATH)) {
    const seed = JSON.parse(fs.readFileSync(SEED_PATH, 'utf-8'));
    fs.writeFileSync(DB_PATH, JSON.stringify(seed, null, 2));
  }
  return JSON.parse(fs.readFileSync(DB_PATH, 'utf-8'));
}

function save(data) {
  fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2));
}

function reset() {
  const seed = JSON.parse(fs.readFileSync(SEED_PATH, 'utf-8'));
  save(seed);
  return seed;
}

function nextId(prefix) {
  return `${prefix}_${Date.now().toString(36)}${Math.floor(Math.random() * 1000)}`;
}

// ---- business config ----
function getBusiness() {
  return load().business;
}

// ---- appointments ----
function listAppointments() {
  return load().appointments;
}

function getAppointment(id) {
  return load().appointments.find(a => a.id === id) || null;
}

function insertAppointment(apt) {
  const data = load();
  const record = { id: nextId('apt'), createdAt: new Date().toISOString(), ...apt };
  data.appointments.push(record);
  save(data);
  return record;
}

function updateAppointment(id, patch) {
  const data = load();
  const idx = data.appointments.findIndex(a => a.id === id);
  if (idx === -1) return null;
  data.appointments[idx] = { ...data.appointments[idx], ...patch, updatedAt: new Date().toISOString() };
  save(data);
  return data.appointments[idx];
}

// ---- clients ----
function upsertClient({ name, contact }) {
  const data = load();
  let client = data.clients.find(c => c.contact === contact);
  if (!client) {
    client = { id: nextId('cli'), name, contact, appointmentIds: [], createdAt: new Date().toISOString() };
    data.clients.push(client);
  } else if (name && client.name !== name) {
    client.name = name;
  }
  save(data);
  return client;
}

function linkAppointmentToClient(clientId, appointmentId) {
  const data = load();
  const client = data.clients.find(c => c.id === clientId);
  if (client && !client.appointmentIds.includes(appointmentId)) {
    client.appointmentIds.push(appointmentId);
    save(data);
  }
  return client;
}

function listClients() {
  return load().clients;
}

// ---- conversations (for the "AI conversations" dashboard tab) ----
function getOrCreateConversation(conversationId, clientContact) {
  const data = load();
  let convo = data.conversations.find(c => c.id === conversationId);
  if (!convo) {
    convo = {
      id: conversationId,
      clientContact: clientContact || null,
      state: 'awaiting_service',
      context: {},
      messages: [],
      createdAt: new Date().toISOString()
    };
    data.conversations.push(convo);
    save(data);
  }
  return convo;
}

function saveConversation(convo) {
  const data = load();
  const idx = data.conversations.findIndex(c => c.id === convo.id);
  if (idx === -1) {
    data.conversations.push(convo);
  } else {
    data.conversations[idx] = convo;
  }
  save(data);
  return convo;
}

function listConversations() {
  return load().conversations;
}

// ---- notifications (confirmations, reschedules, cancellations, owner alerts) ----
function insertNotification(notif) {
  const data = load();
  const record = { id: nextId('ntf'), ts: new Date().toISOString(), ...notif };
  data.notifications.push(record);
  save(data);
  return record;
}

function listNotifications() {
  return load().notifications;
}

module.exports = {
  reset,
  getBusiness,
  listAppointments,
  getAppointment,
  insertAppointment,
  updateAppointment,
  upsertClient,
  linkAppointmentToClient,
  listClients,
  getOrCreateConversation,
  saveConversation,
  listConversations,
  insertNotification,
  listNotifications
};
