// TARE client support chat — talks to agents over WebSocket.
(function () {
  const launcher = document.getElementById('chat-launcher');
  const panel = document.getElementById('chat-panel');
  const closeBtn = document.getElementById('chat-close');
  const log = document.getElementById('chat-log');
  const form = document.getElementById('chat-form');
  const input = document.getElementById('chat-input');
  const statusEl = document.getElementById('chat-status');
  if (!launcher || !panel) return;

  // per-visitor id (stable in this tab session)
  let clientId = sessionStorage.getItem('tare_client');
  if (!clientId) {
    clientId = 'v-' + Math.random().toString(36).slice(2, 9);
    sessionStorage.setItem('tare_client', clientId);
  }

  let ws = null;
  let opened = false;
  let retry = null;

  function setStatus(t, ok) {
    statusEl.textContent = t;
    statusEl.classList.toggle('online', !!ok);
  }

  function addLine(role, text) {
    const div = document.createElement('div');
    div.className = 'chat-msg ' + role;
    div.textContent = text;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;
  }

  function connect() {
    clearTimeout(retry);
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    try {
      ws = new WebSocket(`${proto}://${location.host}/ws?client=${encodeURIComponent(clientId)}`);
    } catch (e) {
      setStatus('offline', false);
      retry = setTimeout(connect, 3000);
      return;
    }
    ws.onopen = () => setStatus('online — an agent will reply', true);
    ws.onmessage = (ev) => {
      let m;
      try { m = JSON.parse(ev.data); } catch (e) { return; }
      addLine(m.role === 'agent' ? 'agent' : 'visitor', m.text);
    };
    ws.onclose = () => { setStatus('reconnecting…', false); retry = setTimeout(connect, 3000); };
    ws.onerror = () => {};
  }

  function openPanel() {
    panel.classList.add('open');
    panel.setAttribute('aria-hidden', 'false');
    launcher.setAttribute('aria-expanded', 'true');
    if (!opened) {
      opened = true;
      addLine('agent', 'Connected to TARE support. Describe what you want extracted — ideas, items, scrape, scripts, agent tools or a hosted channel.');
      connect();
    }
    input.focus();
  }

  function closePanel() {
    panel.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
    launcher.setAttribute('aria-expanded', 'false');
  }

  launcher.addEventListener('click', () => {
    panel.classList.contains('open') ? closePanel() : openPanel();
  });
  closeBtn.addEventListener('click', closePanel);

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = input.value.trim();
    if (!text) return;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addLine('system', 'Not connected yet — retrying…');
      connect();
      return;
    }
    ws.send(JSON.stringify({ text }));
    input.value = '';
  });
})();
