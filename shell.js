/**
 * shell.js — Shell compartilhado do Classic RH
 * Renderiza sidebar + topbar + layout base em qualquer pagina
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
  const ROUTES = [
    { group: 'Inicio', id: 'app', label: 'Dashboard', href: 'home.html', icon: 'home', perfis: ['all'] },
    { group: 'Pessoas', id: 'colaboradores', label: 'Colaboradores', href: 'colaboradores.html', icon: 'users', perfis: ['all'] },
    { group: 'Pessoas', id: 'admissoes', label: 'Admissoes', href: 'recrutamento_selecao.html', icon: 'user-plus', perfis: ['administrador','rh'] },
    { group: 'Pessoas', id: 'desligamentos', label: 'Desligamentos', href: 'desligamentos_turnover.html', icon: 'user-minus', perfis: ['administrador','rh'] },
    { group: 'Processos', id: 'ocorrencias', label: 'Ocorrencias', href: 'ocorrencias.html', icon: 'file-text', perfis: ['administrador','rh'] },
    { group: 'Processos', id: 'experiencia', label: 'Contratos Exp.', href: 'apuracao.html', icon: 'calendar', perfis: ['administrador','rh'] },
    { group: 'SST', id: 'sst_dashboard', label: 'Painel SST', href: 'dashboard_sst.html', icon: 'shield', perfis: ['all'] },
    { group: 'SST', id: 'asos', label: 'ASOs', href: 'asos.html', icon: 'activity', perfis: ['administrador','rh'] },
    { group: 'SST', id: 'treinamentos', label: 'Treinamentos', href: 'treinamentos.html', icon: 'book', perfis: ['all'] },
    { group: 'SST', id: 'epis', label: 'EPIs', href: 'epis.html', icon: 'shield', perfis: ['all'] },
    { group: 'SST', id: 'acidentes', label: 'Acidentes', href: 'acidentes.html', icon: 'alert-triangle', perfis: ['administrador','rh'] },
    { group: 'SST', id: 'entrega_epi', label: 'Entrega EPI', href: 'entrega_epi.html', icon: 'package', perfis: ['administrador','rh'] },
    { group: 'SST', id: 'estoque_epi', label: 'Estoque EPI', href: 'estoque_epi.html', icon: 'box', perfis: ['administrador','rh'] },
    { group: 'SST', id: 'ordem_servico', label: 'Ordem Servico', href: 'ordem_servico.html', icon: 'clipboard', perfis: ['all'] },
    { group: 'Operacional', id: 'ponto', label: 'Ponto', href: 'importar_secullum.html', icon: 'clock', perfis: ['administrador','rh'] },
    { group: 'Operacional', id: 'beneficios', label: 'Beneficios', href: 'beneficios.html', icon: 'heart', perfis: ['administrador','rh'] },
    { group: 'Operacional', id: 'ferias', label: 'Ferias', href: 'ferias.html', icon: 'sun', perfis: ['administrador','rh'] },
    { group: 'Gestao', id: 'motor_decisoes', label: 'Motor Decisao', href: 'motor_decisoes.html', icon: 'layers', perfis: ['administrador','rh'] },
    { group: 'Gestao', id: 'pendencias', label: 'Pendencias', href: 'alertas_rh.html', icon: 'bell', badge: true, perfis: ['all'] },
    { group: 'Gestao', id: 'plano_acao', label: 'Plano de Acao', href: 'alertas_rh.html', icon: 'check-circle', perfis: ['administrador','rh'] },
    { group: 'Gestao', id: 'lideranca', label: 'Lideranca', href: 'lideranca.html', icon: 'award', perfis: ['administrador','rh'] },
    { group: 'Inteligencia', id: 'dashboard_exec', label: 'Dashboard Exec.', href: 'dashboard_gerencial.html', icon: 'bar-chart', perfis: ['administrador','rh'] },
    { group: 'Inteligencia', id: 'motor_v2', label: 'Motor v2 Score', href: 'motor_v2.html', icon: 'trending-up', perfis: ['administrador','rh','gestor_confianca'] },
    { group: 'Inteligencia', id: 'auditoria_score', label: 'Auditoria Score', href: 'motor_v2_auditoria.html', icon: 'search', perfis: ['administrador','rh'] },
    { group: 'Inteligencia', id: 'rel_atestados', label: 'Rel. Atestados', href: 'relatorio_atestados.html', icon: 'file-text', perfis: ['administrador','rh','gestor'] },
    { group: 'RH Avancado', id: 'organograma', label: 'Organograma', href: 'organograma.html', icon: 'grid', perfis: ['all'] },
    { group: 'RH Avancado', id: 'notificacoes', label: 'Notificacoes', href: 'notificacoes.html', icon: 'bell', perfis: ['administrador','rh'] },
    { group: 'RH Avancado', id: 'saude_ocupacional', label: 'Saude Ocupacional', href: 'saude_mental.html', icon: 'heart', perfis: ['administrador','rh'] },
    { group: 'Sistema', id: 'integracoes', label: 'Integracoes', href: 'integracoes.html', icon: 'code', perfis: ['administrador','rh'] },
    { group: 'Sistema', id: 'relatorios', label: 'Relatorios', href: 'relatorio_beneficios.html', icon: 'download', perfis: ['administrador','rh'] },
  ];

  const ICONS = {
    'home': '<path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    'users': '<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
    'user-plus': '<path d="M16 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/>',
    'user-minus': '<path d="M16 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="18" y1="11" x2="23" y2="11"/>',
    'file-text': '<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>',
    'calendar': '<rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    'shield': '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    'activity': '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    'book': '<path d="M2 3h6a4 4 0 014 4v14a3 3 0 00-3-3H2z"/><path d="M22 3h-6a4 4 0 00-4 4v14a3 3 0 013-3h7z"/>',
    'alert-triangle': '<path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
    'package': '<path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/>',
    'box': '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/>',
    'clipboard': '<path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/>',
    'clock': '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    'heart': '<path d="M20.84 4.61a5.5 5.5 0 00-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 00-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 000-7.78z"/>',
    'sun': '<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>',
    'layers': '<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>',
    'bell': '<path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/>',
    'check-circle': '<path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
    'award': '<circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/>',
    'bar-chart': '<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
    'trending-up': '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>',
    'search': '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    'grid': '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>',
    'code': '<polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>',
    'download': '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>',
  };

  function svgIcon(name) {
    return `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${ICONS[name] || ICONS['file-text']}</svg>`;
  }

  let _perfil = null;
  let _userName = '';

  function renderSidebar(activePage) {
    const groups = {};
    ROUTES.forEach(r => {
      if (!r.perfis.includes('all') && _perfil && !r.perfis.includes(_perfil)) return;
      if (!groups[r.group]) groups[r.group] = [];
      groups[r.group].push(r);
    });

    let html = `
    <aside class="sidebar" id="sidebar">
      <div class="sidebar-brand">
        <img src="logo-classic-icon.jpg" alt="" style="height:28px;width:auto;border-radius:6px;" onerror="this.style.display='none'">
        <span class="sidebar-brand-text">Classic RH</span>
      </div>
      <button class="sidebar-toggle" id="sidebarToggle" title="Recolher menu">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
      </button>
      <nav class="sidebar-nav">`;

    for (const [group, items] of Object.entries(groups)) {
      html += `<div class="sidebar-group"><div class="sidebar-group-label">${group}</div>`;
      items.forEach(r => {
        const active = r.id === activePage ? ' active' : '';
        html += `<a href="${r.href}" class="sidebar-item${active}" data-page="${r.id}">
          <span class="sidebar-item-icon">${svgIcon(r.icon)}</span>
          <span class="sidebar-item-label">${r.label}</span>
          ${r.badge ? '<span class="sidebar-badge" id="badge-' + r.id + '" style="display:none">0</span>' : ''}
        </a>`;
      });
      html += '</div>';
    }

    html += '</nav></aside>';
    html += '<div class="sidebar-mobile-overlay" id="sidebarOverlay"></div>';
    return html;
  }

  function renderTopbar(title, breadcrumb) {
    const initials = _userName.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase();
    return `
    <header class="topbar">
      <div class="topbar-left">
        <button class="topbar-hamburger" id="hamburgerBtn">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
        </button>
        <div class="topbar-breadcrumb">
          <a href="home.html">Inicio</a> > <span>${breadcrumb || title}</span>
        </div>
      </div>
      <div class="topbar-right">
        <div class="topbar-user">
          <span class="topbar-user-name">${_userName}</span>
          <span class="topbar-user-role">${(_perfil || '').replace('_', ' ')}</span>
        </div>
        <div class="topbar-avatar">${initials}</div>
      </div>
    </header>`;
  }

  function bindEvents() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebarOverlay');
    const toggle = document.getElementById('sidebarToggle');
    const hamburger = document.getElementById('hamburgerBtn');

    if (toggle) toggle.addEventListener('click', () => {
      document.body.classList.toggle('sidebar-collapsed');
      localStorage.setItem('sidebar_collapsed', document.body.classList.contains('sidebar-collapsed'));
    });

    if (hamburger) hamburger.addEventListener('click', () => {
      sidebar?.classList.toggle('mobile-open');
      overlay?.classList.toggle('active');
    });

    if (overlay) overlay.addEventListener('click', () => {
      sidebar?.classList.remove('mobile-open');
      overlay?.classList.remove('active');
    });

    if (localStorage.getItem('sidebar_collapsed') === 'true') {
      document.body.classList.add('sidebar-collapsed');
    }
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
      shellEl.innerHTML = renderSidebar(page) + `<div class="main-content">${renderTopbar(title || 'Classic RH', breadcrumb)}<div class="content-area page-padding" id="page-content"></div></div>`;

      const content = document.getElementById('shell-content');
      const target = document.getElementById('page-content');
      if (content && target) {
        while (content.firstChild) {
          target.appendChild(content.firstChild);
        }
        content.remove();
      }

      bindEvents();
    }

    return user;
  }

  function getRoutes() { return ROUTES; }
  function getRoute(id) { return ROUTES.find(r => r.id === id); }

  return { init, getRoutes, getRoute, ROUTES };
})();
