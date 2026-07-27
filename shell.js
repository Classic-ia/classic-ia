/**
 * shell.js — Shell compartilhado do Classic RH
 * Renderiza topbar + layout base em qualquer pagina
 *
 * USO:
 *   <link rel="stylesheet" href="ds.css">
 *   <script src="config.js"></script>
 *   <script src="rh_auth.js"></script>
 *   <script src="shell.js"></script>
 *   <body class="page-bg">
 *     <div id="shell"></div>
 *     <div id="shell-content" class="content-area page-padding">
 *       <!-- conteudo da pagina aqui -->
 *     </div>
 *   </body>
 *   <script>Shell.init({ page: 'colaboradores', title: 'Colaboradores' });</script>
 */

const Shell = (() => {
  let _perfil = null;
  let _userName = '';

  function renderTopbar(title, breadcrumb) {
    const initials = _userName.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase();
    return `
    <header class="topbar">
      <div class="topbar-left">
        <a href="app.html" class="topbar-back" title="Voltar ao Hub">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
        </a>
        <div class="topbar-breadcrumb">
          <a href="app.html">Inicio</a> &rsaquo; <span>${breadcrumb || title}</span>
        </div>
      </div>
      <div class="topbar-right">
        <div class="topbar-user">
          <span class="topbar-user-name">${_userName}</span>
          <span class="topbar-user-role">${(_perfil || '').replace('_', ' ')}</span>
        </div>
        <div class="topbar-avatar">${initials}</div>
        <button class="btn-logout-shell" onclick="RHAuth.logout()" title="Sair">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        </button>
      </div>
    </header>`;
  }

  async function init(opts = {}) {
    const { page, title, breadcrumb, perfisPermitidos } = opts;

    const user = await RHAuth.init(perfisPermitidos ? { perfisPermitidos } : undefined);
    if (!user) return null;

    _perfil = user.perfil;
    _userName = user.nome || 'Usuario';

    if (page === 'app') {
      if (_perfil === 'gestor') { window.location.href = 'gestor.html'; return null; }
      if (_perfil === 'gestor_confianca') { window.location.href = 'painel_gestao.html'; return null; }
    }

    const shellEl = document.getElementById('shell');
    if (shellEl) {
      shellEl.innerHTML = `<div class="shell-main">${renderTopbar(title || 'Classic RH', breadcrumb)}<div class="content-area page-padding" id="page-content"></div></div>`;

      const content = document.getElementById('shell-content');
      const target = document.getElementById('page-content');
      if (content && target) {
        while (content.firstChild) {
          target.appendChild(content.firstChild);
        }
        content.remove();
      }
    }

    return user;
  }

  return { init };
})();
