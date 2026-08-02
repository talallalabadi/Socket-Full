(function () {
  const log = document.getElementById('log');
  const form = document.getElementById('form');
  const input = document.getElementById('input');

  // Persist the conversation id across page reloads so a returning
  // customer picks up where they left off instead of restarting.
  let conversationId = localStorage.getItem('booking_conversation_id');
  if (!conversationId) {
    conversationId = 'conv_' + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem('booking_conversation_id', conversationId);
  }

  const socket = io();

  fetch('/api/business')
    .then(r => r.json())
    .then(biz => {
      document.getElementById('biz-name').textContent = biz.name;
      const today = new Date();
      const dayKeys = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
      const hoursToday = biz.hours[dayKeys[today.getDay()]];
      document.getElementById('biz-hours').textContent = hoursToday
        ? `Open today ${hoursToday[0]}–${hoursToday[1]}`
        : 'Closed today';
    });

  function addBubble(from, text) {
    const div = document.createElement('div');
    div.className = `msg ${from}`;
    div.textContent = text;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;
  }

  function addTicket(ticket) {
    const apt = ticket.appointment;
    const wrap = document.createElement('div');
    wrap.className = 'msg-ticket-wrap';

    const statusLabel = ticket.action === 'cancelled' ? 'cancelled'
      : ticket.action === 'rescheduled' ? 'rescheduled'
      : 'confirmed';

    wrap.innerHTML = `
      <div class="ticket">
        <div class="ticket-stamp ${statusLabel}">${statusLabel}</div>
        <div class="ticket-service">${apt.serviceName}</div>
        <div class="ticket-meta">${new Date(apt.start).toLocaleString(undefined, { weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}</div>
        <div class="ticket-meta">Ticket #${apt.id.slice(-6).toUpperCase()}</div>
      </div>
    `;
    log.appendChild(wrap);
    log.scrollTop = log.scrollHeight;
  }

  socket.on('connect', () => {
    socket.emit('customer:join', { conversationId });
  });

  socket.on('conversation:history', messages => {
    log.innerHTML = '';
    messages.forEach(m => addBubble(m.from, m.text));
  });

  socket.on('bot:reply', ({ text, ticket }) => {
    addBubble('bot', text);
    if (ticket) addTicket(ticket);
  });

  function sendMessage(text) {
    if (!text.trim()) return;
    addBubble('customer', text);
    socket.emit('customer:message', { conversationId, text });
  }

  form.addEventListener('submit', e => {
    e.preventDefault();
    const text = input.value;
    input.value = '';
    sendMessage(text);
  });

  document.querySelectorAll('.chip').forEach(chip => {
    chip.addEventListener('click', () => sendMessage(chip.dataset.text));
  });
})();
