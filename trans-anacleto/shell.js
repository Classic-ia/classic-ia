/**
 * shell.js — Trans Anacleto
 * Sidebar + topbar + layout base compartilhado entre todas as paginas desktop.
 */

const Shell = (() => {
  const ROUTES = [
    { group: 'Inicio', id: 'home', label: 'Painel', href: 'index.html', icon: 'home', perfis: ['all'] },

    // FROTA
    { group: 'Frota', id: 'veiculos', label: 'Veiculos', href: 'veiculos.html', icon: 'truck', perfis: ['all'] },
    { group: 'Frota', id: 'motoristas', label: 'Motoristas', href: 'motoristas.html', icon: 'users', perfis: ['all'] },
    { group: 'Frota', id: 'pneus', label: 'Pneus', href: 'pneus.html', icon: 'disc', perfis: ['all'] },

    // ESTOQUE
    { group: 'Estoque', id: 'produtos', label: 'Produtos (SKU)', href: 'produtos.html', icon: 'box', perfis: ['admin', 'almoxarife', 'gestor'] },
    { group: 'Estoque', id: 'entrada', label: 'Entrada', href: 'entrada.html', icon: 'download', perfis: ['admin', 'almoxarife'] },
    { group: 'Estoque', id: 'saida', label: 'Saida', href: 'saida.html', icon: 'upload', perfis: ['admin', 'almoxarife', 'gestor'] },
    { group: 'Estoque', id: 'lotes', label: 'Lotes', href: 'lotes.html', icon: 'layers', perfis: ['admin', 'almoxarife', 'gestor'] },
    { group: 'Estoque', id: 'estoque_usado', label: 'Estoque Usado', href: 'estoque_usado.html', icon: 'package', perfis: ['admin', 'almoxarife', 'gestor'] },
    { group: 'Estoque', id: 'conversao', label: 'Conversoes', href: 'conversao.html', icon: 'refresh', perfis: ['admin', 'almoxarife'] },
    { group: 'Estoque', id: 'inventario', label: 'Inventario', href: 'inventario.html', icon: 'clipboard', perfis: ['admin', 'almoxarife'] },
    { group: 'Estoque', id: 'entrada_nfe', label: 'Entrada por NFe', href: 'entrada_nfe.html', icon: 'file-text', perfis: ['admin', 'almoxarife'] },
    { group: 'Estoque', id: 'etiquetas', label: 'Etiquetas', href: 'etiquetas.html', icon: 'tag', perfis: ['admin', 'almoxarife'] },

    // OFICINA
    { group: 'Oficina', id: 'manutencoes', label: 'Manutencoes', href: 'manutencoes.html', icon: 'tool', perfis: ['all'] },
    { group: 'Oficina', id: 'planos_manutencao', label: 'Planos', href: 'planos_manutencao.html', icon: 'calendar', perfis: ['admin', 'gestor'] },
    { group: 'Oficina', id: 'alertas_manut', label: 'Alertas', href: 'manutencoes_alertas.html', icon: 'bell', perfis: ['all'] },
    { group: 'Oficina', id: 'servicos', label: 'Servicos Externos', href: 'servicos.html', icon: 'clipboard', perfis: ['admin', 'almoxarife', 'gestor'] },

    // ABASTECIMENTO
    { group: 'Abastecimento', id: 'abastecimentos', label: 'Abastecimentos', href: 'abastecimentos.html', icon: 'droplet', perfis: ['all'] },
    { group: 'Abastecimento', id: 'tanque', label: 'Tanque Proprio', href: 'tanque.html', icon: 'database', perfis: ['admin', 'almoxarife', 'gestor'] },

    // TELEMETRIA
    { group: 'Telemetria', id: 'viagens', label: 'Viagens', href: 'viagens.html', icon: 'map', perfis: ['all'] },
    { group: 'Telemetria', id: 'geofences', label: 'Geofences', href: 'geofences.html', icon: 'map-pin', perfis: ['admin', 'gestor'] },
    { group: 'Telemetria', id: 'score', label: 'Score Motoristas', href: 'motoristas_score.html', icon: 'trending-up', perfis: ['admin', 'gestor'] },
    { group: 'Telemetria', id: 'telemetria_eventos', label: 'Eventos Raw', href: 'telemetria_eventos.html', icon: 'activity', perfis: ['admin'] },
    { group: 'Telemetria', id: 'telemetria_config', label: 'Config Onixsat', href: 'telemetria_config.html', icon: 'code', perfis: ['admin'] },

    // CADASTROS
    { group: 'Cadastros', id: 'fornecedores', label: 'Fornecedores', href: 'fornecedores.html', icon: 'briefcase', perfis: ['admin', 'almoxarife', 'gestor'] },
    { group: 'Cadastros', id: 'categorias', label: 'Categorias', href: 'categorias.html', icon: 'grid', perfis: ['admin', 'almoxarife'] },
    { group: 'Cadastros', id: 'importar', label: 'Importar CSV', href: 'importar.html', icon: 'upload', perfis: ['admin'] },

    // SISTEMA
    { group: 'Sistema', id: 'tms_sync', label: 'Sync TMS', href: 'tms_sync.html', icon: 'refresh', perfis: ['admin'] },
    { group: 'Sistema', id: 'usuarios', label: 'Usuarios', href: 'usuarios.html', icon: 'user-plus', perfis: ['admin'] },
    { group: 'Sistema', id: 'config', label: 'Configuracoes', href: 'config.html', icon: 'settings', perfis: ['admin'] },
  ];

  const ICONS = {
    'home': '<path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    'users': '<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
    'user-plus': '<path d="M16 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/>',
    'truck': '<rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/>',
    'disc': '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="3"/>',
    'box': '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/>',
    'package': '<path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/>',
    'download': '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>',
    'upload': '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>',
    'layers': '<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>',
    'refresh': '<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/>',
    'clipboard': '<path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/>',
    'file-text': '<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>',
    'tag': '<path d="M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/>',
    'tool': '<path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/>',
    'calendar': '<rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    'bell': '<path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/>',
    'droplet': '<path d="M12 2.69l5.66 5.66a8 8 0 11-11.31 0z"/>',
    'database': '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
    'map': '<polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"/><line x1="8" y1="2" x2="8" y2="18"/><line x1="16" y1="6" x2="16" y2="22"/>',
    'map-pin': '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/>',
    'trending-up': '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>',
    'activity': '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    'code': '<polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>',
    'briefcase': '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/>',
    'grid': '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>',
    'settings': '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/>',
  };

  function svgIcon(name) {
    return `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${ICONS[name] || ICONS['file-text']}</svg>`;
  }

  let _perfil = null;
  let _userName = '';

  function renderSidebar(activePage) {
    const brandText = 'Trans Anacleto';
    const groups = {};
    ROUTES.forEach(r => {
      if (!r.perfis.includes('all') && _perfil && !r.perfis.includes(_perfil)) return;
      if (!groups[r.group]) groups[r.group] = [];
      groups[r.group].push(r);
    });

    let html = `
    <aside class="sidebar" id="sidebar">
      <div class="sidebar-brand">
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#2563EB" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>
        <span class="sidebar-brand-text">${brandText}</span>
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

    html += `<div class="sidebar-group" style="margin-top:auto;padding-top:12px;border-top:1px solid var(--border);">
      <a href="m/m_home.html" class="sidebar-item" style="color:var(--text-muted);">
        <span class="sidebar-item-icon">${svgIcon('map-pin')}</span>
        <span class="sidebar-item-label">Modo Mobile</span>
      </a>
      <a href="#" onclick="TransAuth.logout();return false;" class="sidebar-item" style="color:var(--text-muted);">
        <span class="sidebar-item-icon">${svgIcon('code')}</span>
        <span class="sidebar-item-label">Sair</span>
      </a>
    </div>`;
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
          <a href="index.html">Inicio</a> › <span>${breadcrumb || title}</span>
        </div>
      </div>
      <div class="topbar-center"></div>
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
      localStorage.setItem('trans_sidebar_collapsed', document.body.classList.contains('sidebar-collapsed'));
    });

    if (hamburger) hamburger.addEventListener('click', () => {
      sidebar?.classList.toggle('mobile-open');
      overlay?.classList.toggle('active');
    });

    if (overlay) overlay.addEventListener('click', () => {
      sidebar?.classList.remove('mobile-open');
      overlay?.classList.remove('active');
    });

    if (localStorage.getItem('trans_sidebar_collapsed') === 'true') {
      document.body.classList.add('sidebar-collapsed');
    }
  }

  async function init(opts = {}) {
    const { page, title, breadcrumb, perfisPermitidos } = opts;

    const user = await TransAuth.init(perfisPermitidos ? { perfisPermitidos } : undefined);
    if (!user) return null;

    _perfil = user.perfil;
    _userName = user.nome || 'Usuario';

    const shellEl = document.getElementById('shell');
    if (shellEl) {
      shellEl.innerHTML = renderSidebar(page) + `<div class="main-content">${renderTopbar(title || 'Trans Anacleto', breadcrumb)}<div class="content-area page-padding" id="page-content"></div></div>`;

      const content = document.getElementById('shell-content');
      const target = document.getElementById('page-content');
      if (content && target) {
        target.innerHTML = content.innerHTML;
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
