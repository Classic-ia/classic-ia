/**
 * m_shell.js — Trans Anacleto Mobile Shell
 *
 * Layout mobile-first com topbar (titulo + voltar) e bottom-nav.
 * Reaproveita auth.js, api.js, config.js do desktop.
 *
 * Uso:
 *   MShell.render({ title, back: 'm_home.html' });
 */
const MShell = (function () {

  const NAV = [
    { id: 'home', label: 'Home', icon: 'H', href: 'm_home.html' },
    { id: 'pneus', label: 'Pneus', icon: 'O', href: 'm_pneus_scan.html' },
    { id: 'estoque', label: 'Estoque', icon: '[]', href: 'm_estoque_scan.html' },
    { id: 'manut', label: 'Manut', icon: 'M', href: 'm_manutencao_buscar.html' },
    { id: 'sair', label: 'Sair', icon: 'x', href: '#', onClick: 'MShell.logout()' }
  ];

  function render(opts = {}) {
    const current = opts.currentNav || '';
    const title = opts.title || 'Trans Anacleto';
    const backHref = opts.back || '';
    const root = document.getElementById('m-shell');
    if (!root) return;

    const topbar = `
      <header class="m-topbar">
        <div class="m-topbar-left">
          ${backHref ? `<a href="${backHref}" class="m-back">&larr;</a>` : ''}
        </div>
        <div class="m-topbar-title">${escHtml(title)}</div>
        <div class="m-topbar-right"></div>
      </header>
    `;

    const bottomnav = `
      <nav class="m-bottomnav">
        ${NAV.map(n => `
          <a href="${n.href}" ${n.onClick ? `onclick="${n.onClick};return false"` : ''}
             class="m-bn-item ${current === n.id ? 'active' : ''}">
            <span class="m-bn-icon">${n.icon}</span>
            <span class="m-bn-label">${n.label}</span>
          </a>
        `).join('')}
      </nav>
    `;

    root.innerHTML = topbar + bottomnav;
  }

  async function logout() {
    try {
      const STORE_KEY = (typeof TRANS_CONFIG !== 'undefined' && TRANS_CONFIG.SESSION_KEY) || 'trans_sess_v1';
      localStorage.removeItem(STORE_KEY);
    } catch {}
    window.location.href = 'm_login.html';
  }

  async function init(opts = {}) {
    const STORE_KEY = (typeof TRANS_CONFIG !== 'undefined' && TRANS_CONFIG.SESSION_KEY) || 'trans_sess_v1';
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) { window.location.href = 'm_login.html'; return null; }
    let sess = null;
    try { sess = JSON.parse(raw); } catch { localStorage.removeItem(STORE_KEY); window.location.href = 'm_login.html'; return null; }
    if (!sess || !sess.user || !sess.access_token) {
      localStorage.removeItem(STORE_KEY);
      window.location.href = 'm_login.html';
      return null;
    }
    try {
      const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=refresh_token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'apikey': SB_KEY },
        body: JSON.stringify({ refresh_token: sess.refresh_token }),
      });
      if (r.ok) {
        const data = await r.json();
        sess.access_token = data.access_token;
        sess.refresh_token = data.refresh_token;
        localStorage.setItem(STORE_KEY, JSON.stringify(sess));
      }
    } catch {}
    try {
      const r = await fetch(`${SB_URL}/rest/v1/rpc/trans_meu_perfil`, {
        method: 'POST',
        headers: {
          'Content-Type':'application/json','apikey':SB_KEY,
          'Authorization':'Bearer '+sess.access_token
        },
        body: '{}',
      });
      if (r.ok) {
        const rows = await r.json();
        const perfilDB = (rows && rows.length > 0) ? rows[0] : null;
        if (perfilDB) {
          sess.user.id = perfilDB.id;
          sess.user.nome = perfilDB.nome;
          sess.user.perfil = perfilDB.perfil;
          sess.user.email = perfilDB.email;
          localStorage.setItem(STORE_KEY, JSON.stringify(sess));
        } else {
          localStorage.removeItem(STORE_KEY);
          window.location.href = 'm_login.html';
          return null;
        }
      }
    } catch {}
    if (typeof API !== 'undefined' && API.setPerfil) API.setPerfil(sess.user.perfil);
    if (opts.perfisPermitidos && !opts.perfisPermitidos.includes(sess.user.perfil)) {
      window.location.href = 'm_home.html';
      return null;
    }
    return sess.user;
  }

  function getUser() {
    const STORE_KEY = (typeof TRANS_CONFIG !== 'undefined' && TRANS_CONFIG.SESSION_KEY) || 'trans_sess_v1';
    try { return JSON.parse(localStorage.getItem(STORE_KEY) || '{}').user || null; } catch { return null; }
  }

  function toast(msg, tipo = 'ok', ms = 2500) {
    const prev = document.getElementById('m-toast');
    if (prev) prev.remove();
    const el = document.createElement('div');
    el.id = 'm-toast';
    el.className = 'm-toast m-toast-' + tipo;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => { if (el.parentNode) el.remove(); }, ms);
  }

  function escHtml(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
  }

  return { render, logout, init, getUser, toast, escHtml };
})();
