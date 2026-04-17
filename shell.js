/**
 * shell.js — Shell compartilhado (root)
 * Renderiza topbar + layout base em paginas RH e CQ na raiz do projeto.
 *
 * USO RH (default):
 *   <link rel="stylesheet" href="ds.css">
 *   <script src="config.js"></script>
 *   <script src="rh_auth.js"></script>
 *   <script src="shell.js"></script>
 *   <body class="page-bg">
 *     <div id="shell"></div>
 *     <div id="shell-content" class="content-area page-padding">...</div>
 *   </body>
 *   <script>Shell.init({ page: 'x', title: 'X' });</script>
 *
 * USO CQ:
 *   <script src="cq_auth.js"></script>
 *   <script>Shell.init({ authModule: 'cq', page: 'x', title: 'X', hubHref: 'app.html' });</script>
 */

const Shell = (() => {
  const MODULES = {
    rh: {
      brand: 'Classic RH',
      hubHref: 'home.html',
      getAuth: () => (typeof RHAuth !== 'undefined' ? RHAuth : null),
    },
    cq: {
      brand: 'Classic CQ',
      hubHref: 'app.html',
      getAuth: () => (typeof CQAuth !== 'undefined' ? CQAuth : null),
    },
  };

  let _perfil = null;
  let _userName = '';
  let _mod = MODULES.rh;
  let _authName = 'RHAuth';

  function renderTopbar(title, breadcrumb, hubHref) {
    const initials = _userName.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase();
    return `
    <header class="topbar">
      <div class="topbar-left">
        <a href="${hubHref}" class="topbar-back" title="Voltar ao Hub">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
        </a>
        <div class="topbar-breadcrumb">
          <a href="${hubHref}">Inicio</a> &rsaquo; <span>${breadcrumb || title}</span>
        </div>
      </div>
      <div class="topbar-right">
        <div class="topbar-user">
          <span class="topbar-user-name">${_userName}</span>
          <span class="topbar-user-role">${(_perfil || '').replace('_', ' ')}</span>
        </div>
        <div class="topbar-avatar">${initials}</div>
        <button class="btn-logout-shell" onclick="${_authName}.logout()" title="Sair">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        </button>
      </div>
    </header>`;
  }

  async function init(opts = {}) {
    const { page, title, breadcrumb, perfisPermitidos, authModule, hubHref } = opts;

    const modKey = authModule || 'rh';
    _mod = MODULES[modKey] || MODULES.rh;
    _authName = modKey === 'cq' ? 'CQAuth' : 'RHAuth';

    const Auth = _mod.getAuth();
    if (!Auth) {
      console.error(`[Shell] Modulo de auth nao carregado: ${_authName}. Verifique a ordem dos scripts.`);
      return null;
    }

    const user = await Auth.init(perfisPermitidos ? { perfisPermitidos } : undefined);
    if (!user) return null;

    _perfil = user.perfil;
    _userName = user.nome || 'Usuario';

    // Redirecionamento especifico do RH para perfis de gestor
    if (modKey === 'rh' && page === 'app') {
      if (_perfil === 'gestor') { window.location.href = 'gestor.html'; return null; }
      if (_perfil === 'gestor_confianca') { window.location.href = 'painel_gestao.html'; return null; }
    }

    const shellEl = document.getElementById('shell');
    if (shellEl) {
      const effectiveHub = hubHref || _mod.hubHref;
      shellEl.innerHTML = `<div class="shell-main">${renderTopbar(title || _mod.brand, breadcrumb, effectiveHub)}<div class="content-area page-padding" id="page-content"></div></div>`;

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
