/**
 * m_shell.js - JR Terraplanagem Mobile
 *
 * Shell mobile com topbar (titulo + back + logout) e bottom nav fixa.
 * Inclui estilos inline para evitar dependencia extra de CSS.
 *
 * Uso:
 *   MShell.mount({
 *     title: 'Abrir OS',
 *     back: 'm_home.html',     // opcional; se ausente nao mostra seta
 *     active: 'home',          // id do tab ativo: home, os, manut, abast, mais
 *   });
 */
const MShell = (function () {

  const NAV = [
    { id: 'home',  label: 'Inicio',   href: 'm_home.html',             icon: svgHome() },
    { id: 'os',    label: 'OS',       href: 'm_os_periodo.html',       icon: svgClip() },
    { id: 'manut', label: 'Manut.',   href: 'm_manutencao_buscar.html',icon: svgTool() },
    { id: 'abast', label: 'Abast.',   href: 'm_abastecimento.html',    icon: svgDrop() },
    { id: 'mais',  label: 'Mais',     href: 'm_mais.html',             icon: svgMenu() },
  ];

  function _style() {
    if (document.getElementById('m-shell-style')) return;
    const s = document.createElement('style');
    s.id = 'm-shell-style';
    s.textContent = `
      * { box-sizing:border-box; }
      html, body { margin:0; padding:0; font-family:'Inter',system-ui,sans-serif; background:#F1F5F9; color:#0F172A; -webkit-tap-highlight-color:transparent; }
      .m-topbar { position:sticky; top:0; z-index:50; background:#D97706; color:#fff; padding:14px 16px; display:flex; align-items:center; gap:10px; box-shadow:0 2px 6px rgba(0,0,0,0.08); }
      .m-topbar .m-back { background:none; border:none; color:#fff; font-size:22px; padding:0 6px; cursor:pointer; }
      .m-topbar .m-title { font-size:17px; font-weight:700; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
      .m-topbar .m-logout { background:rgba(255,255,255,0.2); border:none; color:#fff; font-size:13px; font-weight:600; padding:6px 10px; border-radius:6px; cursor:pointer; }
      .m-page { padding:16px 14px 84px 14px; min-height:calc(100vh - 56px); }
      .m-bottomnav { position:fixed; left:0; right:0; bottom:0; background:#fff; border-top:1px solid #E2E8F0; display:flex; justify-content:space-around; padding:6px 0 10px 0; z-index:40; }
      .m-bottomnav a { flex:1; text-align:center; color:#64748B; text-decoration:none; font-size:11px; font-weight:600; padding:6px 4px; display:flex; flex-direction:column; align-items:center; gap:2px; }
      .m-bottomnav a.active { color:#D97706; }
      .m-bottomnav svg { width:22px; height:22px; }
      .m-toast { position:fixed; bottom:80px; left:50%; transform:translateX(-50%); background:#0F172A; color:#fff; padding:12px 18px; border-radius:8px; font-size:14px; z-index:200; opacity:0; transition:opacity 0.2s; max-width:92vw; text-align:center; }
      .m-toast.ok  { background:#059669; }
      .m-toast.err { background:#DC2626; }
      .m-toast.warn{ background:#D97706; }
      .m-toast.show { opacity:1; }
      .m-card { background:#fff; border:1px solid #E2E8F0; border-radius:12px; padding:16px; margin-bottom:12px; }
      .m-card h3 { margin:0 0 10px 0; font-size:15px; color:#0F172A; }
      .m-tile { display:block; background:#fff; border:1px solid #E2E8F0; border-radius:12px; padding:18px; text-align:center; text-decoration:none; color:#0F172A; font-weight:600; font-size:14px; transition:transform 0.1s; }
      .m-tile:active { transform:scale(0.97); background:#FEF3C7; }
      .m-tile .m-icon { width:36px; height:36px; margin:0 auto 10px; background:#FEF3C7; color:#D97706; border-radius:10px; display:flex; align-items:center; justify-content:center; }
      .m-tile-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
      .m-btn-primary { display:block; width:100%; padding:14px; background:#D97706; color:#fff; border:none; border-radius:10px; font-weight:700; font-size:15px; cursor:pointer; font-family:inherit; }
      .m-btn-primary:disabled { background:#CBD5E1; }
      .m-btn-secondary { display:block; width:100%; padding:12px; background:#fff; color:#475569; border:1px solid #CBD5E1; border-radius:10px; font-weight:600; font-size:14px; cursor:pointer; font-family:inherit; }
      .m-btn-danger { display:block; width:100%; padding:12px; background:#DC2626; color:#fff; border:none; border-radius:10px; font-weight:600; font-size:14px; cursor:pointer; font-family:inherit; }
      .m-field { margin-bottom:14px; }
      .m-field label { display:block; font-size:12px; color:#475569; font-weight:600; margin-bottom:5px; text-transform:uppercase; letter-spacing:0.3px; }
      .m-field input, .m-field select, .m-field textarea {
        width:100%; padding:12px 14px; font-size:15px; border:1px solid #CBD5E1;
        border-radius:10px; background:#fff; font-family:inherit; color:#0F172A;
      }
      .m-field input:focus, .m-field select:focus, .m-field textarea:focus {
        outline:none; border-color:#D97706; box-shadow:0 0 0 3px rgba(217,119,6,0.15);
      }
      .m-row { display:flex; gap:10px; }
      .m-row > * { flex:1; }
      .m-hint { font-size:12px; color:#64748B; margin-top:4px; }
      .m-info-label { font-size:11px; color:#94A3B8; text-transform:uppercase; font-weight:700; letter-spacing:0.3px; }
      .m-info-value { font-size:15px; color:#0F172A; font-weight:600; margin-top:2px; }
      .m-empty { padding:40px 20px; text-align:center; color:#94A3B8; font-size:13px; }
      .m-pill { display:inline-block; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:700; text-transform:uppercase; }
      .m-pill-aberta { background:#FEF3C7; color:#92400E; }
      .m-pill-andamento { background:#DBEAFE; color:#1E3A8A; }
      .m-pill-concluida { background:#D1FAE5; color:#065F46; }
      .m-pill-cancelada { background:#F1F5F9; color:#64748B; }
    `;
    document.head.appendChild(s);
  }

  function mount(opts = {}) {
    _style();
    const title = opts.title || 'JR';
    const back  = opts.back;
    const active = opts.active || 'home';

    // Topbar
    const top = document.createElement('div');
    top.className = 'm-topbar';
    top.innerHTML = `
      ${back ? `<button class="m-back" onclick="location.href='${back}'">&larr;</button>` : ''}
      <div class="m-title">${escape(title)}</div>
      <button class="m-logout" onclick="MShell.logout()">Sair</button>
    `;

    // Bottom nav
    const nav = document.createElement('div');
    nav.className = 'm-bottomnav';
    nav.innerHTML = NAV.map(n => `
      <a href="${n.href}" class="${n.id === active ? 'active' : ''}">
        ${n.icon}
        <span>${n.label}</span>
      </a>
    `).join('');

    document.body.insertBefore(top, document.body.firstChild);
    document.body.appendChild(nav);

    const main = document.getElementById('m-main');
    if (main) main.classList.add('m-page');
  }

  async function init(opts = {}) {
    if (typeof JrAuth === 'undefined') {
      window.location.href = 'm_login.html';
      return null;
    }
    const user = await JrAuth.init({ perfisPermitidos: opts.perfisPermitidos });
    if (!user) { window.location.href = 'm_login.html'; return null; }
    return user;
  }

  function logout() {
    if (typeof JrAuth !== 'undefined') JrAuth.logout();
    window.location.href = 'm_login.html';
  }

  function toast(msg, tipo = 'ok') {
    const t = document.createElement('div');
    t.className = 'm-toast ' + tipo;
    t.textContent = msg;
    document.body.appendChild(t);
    requestAnimationFrame(() => t.classList.add('show'));
    setTimeout(() => {
      t.classList.remove('show');
      setTimeout(() => t.remove(), 250);
    }, 3200);
  }

  function escape(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function svgHome() { return '<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M3 12l9-9 9 9M5 10v10h14V10"/></svg>'; }
  function svgClip() { return '<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M9 5h6a2 2 0 012 2v12a2 2 0 01-2 2H9a2 2 0 01-2-2V7a2 2 0 012-2zm0 0V3h6v2"/></svg>'; }
  function svgTool() { return '<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/></svg>'; }
  function svgDrop() { return '<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M12 2s-6 7-6 12a6 6 0 0012 0c0-5-6-12-6-12z"/></svg>'; }
  function svgMenu() { return '<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M4 6h16M4 12h16M4 18h16"/></svg>'; }

  return { mount, init, logout, toast };
})();
