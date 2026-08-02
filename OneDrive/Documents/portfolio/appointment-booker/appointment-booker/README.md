# Appointment Booker — automated booking + admin dashboard

A full-stack appointment booking system: a customer-facing chat widget that checks
availability, books, reschedules, and cancels appointments — and an admin dashboard
showing the calendar, clients, analytics, and every conversation, live.

The "AI" is deliberately **not** an LLM. Booking an appointment is a fixed sequence
(pick a service → pick a time → give your name/contact → confirm), so it's driven by
a rule-based state machine instead. That's a design choice, not a shortcut: it's
instant, free to run, and never invents an appointment time that doesn't exist.

## Why it's built this way (architecture)

```
                     HTTP (REST)
   Customer widget  ───────────────►   Express server
   (index.html)                        (server.js)
        ▲  │                                │
        │  │      WebSocket (Socket.io)      │
        │  ▼                                 ▼
        └──────────────◄────────────►   scheduler.js ── calendar.js (mock GCal sync)
                                            │
   Admin dashboard   ◄──WebSocket + REST──  bot.js (fixed-flow state machine)
   (admin.html)                             │
                                            db.js (JSON-file "database")
                                            notifications.js (simulated confirmations)
```

- **HTTP/REST** (`/api/appointments`, `/api/clients`, `/api/analytics`, …) is used for
  anything that just needs a snapshot on page load — e.g. the dashboard's initial data.
- **WebSocket (Socket.io)** carries the live chat and live dashboard updates, because
  it's bidirectional and push-based: the server can notify the owner's dashboard the
  instant a booking happens, instead of the dashboard polling for changes.
- **Rooms** keep things separated. Each customer's chat lives in its own conversation
  room; the admin dashboard joins a shared `agents` room so it hears about every
  booking, reschedule, and cancellation as it happens.
- **The bot is a finite state machine** (`bot.js`), not a model call. Each conversation
  has a `state` (`awaiting_service` → `awaiting_slot` → `awaiting_name` →
  `awaiting_contact` → `awaiting_confirm` → `booked`, plus reschedule/cancel branches)
  and a fixed template reply for each state. No prompt, no token cost, no
  hallucinated appointment slots.
- **Appointments are the shared state.** A booking is a record with a `status`
  (`confirmed` → `rescheduled`/`cancelled`) and a start/end time. `scheduler.js` is the
  only place that decides whether a slot is free, so conflict-checking logic lives in
  exactly one function (`isSlotFree`) and every booking path — new, reschedule — has to
  go through it.
- **`calendar.js` mocks the Google Calendar API.** It logs the same shape of event
  (start, end, summary, external ID) that a real `googleapis` call would need. Swapping
  in the real API later means replacing the body of `syncEvent` — nothing that calls it
  changes.
- **`notifications.js` mocks email/SMS.** Every "sent" confirmation, reschedule notice,
  cancellation notice, and owner alert is logged to the database, so the dashboard has
  a real notification history to show even without a real provider wired up.

## Features

**Automated booking flow**
- Checks live availability against business hours + existing bookings
- Books appointments (with conflict detection)
- Sends confirmations (simulated)
- Reschedules to a new open slot
- Cancels
- Notifies the owner in real time on every action

**Admin dashboard**
- **Calendar** — upcoming appointments, grouped by day, as ticket-style cards
- **Clients** — everyone who's booked, contact info, booking history
- **Analytics** — total/active bookings, cancellation rate, bookings by service, most
  popular start times
- **AI Conversations** — a live, readable transcript of every automated chat

## Project structure

```
appointment-booker/
├── package.json
├── server.js          # Express + Socket.io wiring, REST API, socket event handlers
├── db.js              # Tiny JSON-file "database" for appointments/clients/etc.
├── scheduler.js        # Availability, booking, reschedule, cancel — conflict logic lives here
├── bot.js              # Rule-based conversation state machine (the "AI")
├── calendar.js          # Mock Google Calendar sync (drop-in swap for the real API)
├── notifications.js     # Mock email/SMS sender + owner alerts
├── data/
│   └── seed.json       # Starting business config: hours, services, prices
└── public/
    ├── index.html       # Customer booking widget
    ├── customer.js
    ├── admin.html        # Owner dashboard
    ├── admin.js
    └── style.css
```

## Running it locally

```bash
npm install
npm start
```

- Customer widget: **http://localhost:3000**
- Admin dashboard: **http://localhost:3000/admin.html**

Open both in separate tabs — book something in the widget and watch it appear on the
dashboard instantly.

To reset all data back to the seed state:

```bash
npm run reset
```

## Customizing the business

Edit `data/seed.json` before first run (or delete `data/db.json` and restart) to change:

- Business name and timezone
- Weekly hours (`null` for a closed day)
- Services, durations, and prices

## Swapping in real infrastructure later

This project is intentionally built so each "mock" module can be replaced without
touching the others:

| Mock module | Replace with | What changes |
|---|---|---|
| `db.js` (JSON file) | PostgreSQL (e.g. via `pg` or Prisma) | Only `db.js` — every function keeps its signature |
| `calendar.js` | Google Calendar API (`googleapis`, OAuth) | Only the body of `syncEvent` |
| `notifications.js` | SendGrid / Twilio | Only the body of `sendToClient` / `notifyOwner` |
| `bot.js` (fixed flow) | An LLM-driven assistant | Only how `handleMessage` decides what to say |

## Deploying (e.g. on Render)

1. Push this project to a GitHub repo.
2. On Render: **New → Web Service**, connect the repo.
3. Build command: `npm install`. Start command: `npm start`.
4. Render sets `PORT` automatically — `server.js` already reads `process.env.PORT`.
5. Note: the JSON-file "database" resets on every deploy/restart on Render's free tier
   (ephemeral filesystem). Fine for a demo; swap in PostgreSQL (see table above) before
   using this for a real business.
