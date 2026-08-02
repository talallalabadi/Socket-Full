(function () {
  const socket = io();
  socket.emit('admin:join');

  // ---- tab switching ----
  const tabs = document.querySelectorAll('.tab');
  const panels = document.querySelectorAll('.panel');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      panels.forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(`panel-${tab.dataset.panel}`).classList.add('active');
    });
  });

  function fmt(dateStr) {
    return new Date(dateStr).toLocaleString(undefined, {
      weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit'
    });
  }
  function dayKey(dateStr) {
    return new Date(dateStr).toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric' });
  }

  function ticketCard(apt) {
    const status = apt.status === 'confirmed' || apt.status === 'rescheduled' ? apt.status : 'cancelled';
    return `
      <div class="ticket">
        <div class="ticket-stamp ${status}">${status}</div>
        <div class="ticket-service">${apt.serviceName}</div>
        <div class="ticket-meta">${fmt(apt.start)}</div>
        <div class="ticket-meta">${apt.clientName || 'Guest'} · ${apt.clientContact || '—'}</div>
      </div>
    `;
  }

  // ---- Calendar ----
  function renderCalendar(appointments) {
    const target = document.getElementById('calendar-content');
    const active = appointments
      .filter(a => a.status !== 'cancelled')
      .sort((a, b) => new Date(a.start) - new Date(b.start));

    if (active.length === 0) {
      target.innerHTML = `<div class="empty-state">No appointments yet — bookings made through the widget will show up here.</div>`;
      return;
    }

    const groups = {};
    active.forEach(a => {
      const key = dayKey(a.start);
      (groups[key] = groups[key] || []).push(a);
    });

    target.innerHTML = Object.entries(groups).map(([day, apts]) => `
      <div class="day-group">
        <div class="day-label">${day}</div>
        <div class="ticket-grid">
          ${apts.map(ticketCard).join('')}
        </div>
      </div>
    `).join('');
  }

  // ---- Clients ----
  function renderClients(clients, appointments) {
    const target = document.getElementById('clients-content');
    if (clients.length === 0) {
      target.innerHTML = `<div class="empty-state">No clients yet.</div>`;
      return;
    }

    const rows = clients.map(c => {
      const apts = appointments.filter(a => a.clientId === c.id);
      const active = apts.filter(a => a.status !== 'cancelled').length;
      const last = apts.sort((a, b) => new Date(b.start) - new Date(a.start))[0];
      return `
        <tr>
          <td>${c.name || '—'}</td>
          <td>${c.contact}</td>
          <td>${apts.length}</td>
          <td>${active}</td>
          <td>${last ? fmt(last.start) : '—'}</td>
        </tr>
      `;
    }).join('');

    target.innerHTML = `
      <table class="data-table">
        <thead>
          <tr><th>Name</th><th>Contact</th><th>Total bookings</th><th>Active</th><th>Most recent</th></tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  // ---- Analytics ----
  function renderAnalytics(stats) {
    const target = document.getElementById('analytics-content');
    const serviceRows = Object.entries(stats.byService)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => `<tr><td>${name}</td><td>${count}</td></tr>`)
      .join('') || `<tr><td colspan="2">No data yet</td></tr>`;

    const hourRows = Object.entries(stats.byPopularHour)
      .sort((a, b) => b[1] - a[1])
      .map(([hour, count]) => `<tr><td>${hour}</td><td>${count}</td></tr>`)
      .join('') || `<tr><td colspan="2">No data yet</td></tr>`;

    target.innerHTML = `
      <div class="stat-row">
        <div class="stat-card"><div class="stat-num">${stats.totalAppointments}</div><div class="stat-label">Total bookings</div></div>
        <div class="stat-card"><div class="stat-num">${stats.active}</div><div class="stat-label">Active</div></div>
        <div class="stat-card"><div class="stat-num">${stats.bookedThisWeek}</div><div class="stat-label">Booked this week</div></div>
        <div class="stat-card"><div class="stat-num">${stats.cancellationRate}%</div><div class="stat-label">Cancellation rate</div></div>
        <div class="stat-card"><div class="stat-num">${stats.totalClients}</div><div class="stat-label">Clients</div></div>
        <div class="stat-card"><div class="stat-num">${stats.totalConversations}</div><div class="stat-label">Conversations</div></div>
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 20px;">
        <div>
          <div class="day-label">Bookings by service</div>
          <table class="data-table"><thead><tr><th>Service</th><th>Bookings</th></tr></thead><tbody>${serviceRows}</tbody></table>
        </div>
        <div>
          <div class="day-label">Most popular start times</div>
          <table class="data-table"><thead><tr><th>Hour</th><th>Bookings</th></tr></thead><tbody>${hourRows}</tbody></table>
        </div>
      </div>
    `;
  }

  // ---- Conversations ----
  function renderConversations(conversations) {
    const target = document.getElementById('conversations-content');
    const sorted = [...conversations].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    if (sorted.length === 0) {
      target.innerHTML = `<div class="empty-state">No conversations yet.</div>`;
      return;
    }

    target.innerHTML = `<div class="convo-list">${sorted.map(c => `
      <div class="convo-card">
        <div class="convo-card-head">
          <span>${c.id}</span>
          <span>state: ${c.state}</span>
        </div>
        ${c.messages.slice(-6).map(m => `
          <div class="convo-line ${m.from}"><span class="who">${m.from}</span>${m.text.replace(/\n/g, ' · ')}</div>
        `).join('')}
      </div>
    `).join('')}</div>`;
  }

  // ---- data loading ----
  let state = { appointments: [], clients: [], conversations: [] };

  function loadAll() {
    Promise.all([
      fetch('/api/business').then(r => r.json()),
      fetch('/api/appointments').then(r => r.json()),
      fetch('/api/clients').then(r => r.json()),
      fetch('/api/analytics').then(r => r.json()),
      fetch('/api/conversations').then(r => r.json())
    ]).then(([biz, appointments, clients, analytics, conversations]) => {
      document.getElementById('side-biz-name').textContent = biz.name;
      state = { appointments, clients, conversations };
      renderCalendar(appointments);
      renderClients(clients, appointments);
      renderAnalytics(analytics);
      renderConversations(conversations);
    });
  }

  function toast(text) {
    const wrap = document.getElementById('toasts');
    const div = document.createElement('div');
    div.className = 'toast';
    div.textContent = text;
    wrap.appendChild(div);
    setTimeout(() => div.remove(), 5000);
  }

  socket.on('admin:appointment_update', ({ action, appointment }) => {
    toast(`${action === 'booked' ? 'New booking' : action === 'rescheduled' ? 'Rescheduled' : 'Cancelled'}: ${appointment.serviceName} — ${appointment.clientName || 'Guest'}`);
    loadAll();
  });

  socket.on('admin:conversation_update', () => {
    fetch('/api/conversations').then(r => r.json()).then(renderConversations);
  });

  loadAll();
})();
