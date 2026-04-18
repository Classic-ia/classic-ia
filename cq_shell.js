/**
 * cq_shell.js — Shell compartilhado do Classic CQ
 * Sidebar + topbar com navegacao para todas as paginas do modulo CQ
 *
 * USO:
 *   <link rel="stylesheet" href="ds.css">
 *   <script src="config.js"></script>
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_shell.js"></script>
 *   <body class="page-bg">
 *     <div id="shell"></div>
 *     <div id="shell-content" class="content-area">
 *       <!-- conteudo da pagina -->
 *     </div>
 *   </body>
 *   <script>CQShell.init({ page: 'dashboard_cq', title: 'Dashboard CQ' });</script>
 */

const CQShell = (() => {
  // ============================================================
  // MAPA DE ROTAS (fonte unica de verdade - 44 paginas CQ)
  // ============================================================
  const ROUTES = [
    // INICIO
    { group: 'Inicio', id: 'dashboard_cq', label: 'Dashboard CQ', href: 'dashboard_cq.html', icon: 'home' },

    // INSPECAO
    { group: 'Inspecao', id: 'fila_inspecoes', label: 'Fila de Inspecoes', href: 'fila_inspecoes.html', icon: 'clipboard' },
    { group: 'Inspecao', id: 'inspecao', label: 'Nova Inspecao', href: 'inspecao.html', icon: 'plus-square' },
    { group: 'Inspecao', id: 'inspecao_qualidade', label: 'Inspecao Qualidade', href: 'inspecao_qualidade.html', icon: 'check-circle' },
    { group: 'Inspecao', id: 'formulario_cq', label: 'Formulario CQ', href: 'formulario_cq.html', icon: 'file-text' },
    { group: 'Inspecao', id: 'conferencia_quantidade', label: 'Conf. Quantidade', href: 'conferencia_quantidade.html', icon: 'check' },
    { group: 'Inspecao', id: 'ranking_cq', label: 'Ranking Qualidade', href: 'ranking_cq.html', icon: 'award' },

    // PCR / RECEBIMENTO
    { group: 'PCR / Recebimento', id: 'lista_pcr', label: 'Lista de PCRs', href: 'lista_pcr.html', icon: 'list' },
    { group: 'PCR / Recebimento', id: 'detalhe_pcr', label: 'Detalhe PCR', href: 'detalhe_pcr.html', icon: 'file-text' },
    { group: 'PCR / Recebimento', id: 'recebimento_lote', label: 'Recebimento de Lote', href: 'recebimento_lote.html', icon: 'package' },
    { group: 'PCR / Recebimento', id: 'dashboard_pcr', label: 'Dashboard PCR', href: 'dashboard_pcr.html', icon: 'bar-chart' },

    // CARGAS / EMBARQUES
    { group: 'Cargas', id: 'cargas', label: 'Cargas', href: 'cargas.html', icon: 'truck' },
    { group: 'Cargas', id: 'dashboard_cargas', label: 'Dashboard Cargas', href: 'dashboard_cargas.html', icon: 'bar-chart' },
    { group: 'Cargas', id: 'lancamento_cargas', label: 'Lancamento Cargas', href: 'lancamento_cargas.html', icon: 'plus-square' },
    { group: 'Cargas', id: 'importar_embarques', label: 'Importar Embarques', href: 'importar_embarques.html', icon: 'download' },
    { group: 'Cargas', id: 'dashboard_embarques', label: 'Dashboard Embarques', href: 'dashboard_embarques.html', icon: 'bar-chart' },
    { group: 'Cargas', id: 'lancamento_cq_bucho', label: 'Lancamento Bucho', href: 'lancamento_cq_bucho.html', icon: 'plus-square' },
    { group: 'Cargas', id: 'processo_bucho', label: 'Processo Bucho', href: 'processo_bucho.html', icon: 'file-text' },

    // FORNECEDORES
    { group: 'Fornecedores', id: 'gestao_fornecedores', label: 'Gestao Fornecedores', href: 'gestao_fornecedores.html', icon: 'users' },
    { group: 'Fornecedores', id: 'dashboard_fornecedores', label: 'Dashboard Fornecedores', href: 'dashboard_fornecedores.html', icon: 'bar-chart' },
    { group: 'Fornecedores', id: 'dashboard_fornecedores_por_estado', label: 'Fornecedores por Estado', href: 'dashboard_fornecedores_por_estado.html', icon: 'map' },
    { group: 'Fornecedores', id: 'dashboard_supplier_score', label: 'Score Confiabilidade', href: 'dashboard_supplier_score.html', icon: 'award' },
    { group: 'Fornecedores', id: 'gestao_filiais', label: 'Empresas & Filiais', href: 'gestao_filiais.html', icon: 'grid' },

    // PRODUCAO
    { group: 'Producao', id: 'dashboard_producao', label: 'Dashboard Producao', href: 'dashboard_producao.html', icon: 'bar-chart' },
    { group: 'Producao', id: 'lote_base_producao', label: 'Lote Base Producao', href: 'lote_base_producao.html', icon: 'layers' },
    { group: 'Producao', id: 'rastreabilidade_lote', label: 'Rastreabilidade', href: 'rastreabilidade_lote.html', icon: 'search' },
    { group: 'Producao', id: 'estoque', label: 'Estoque', href: 'estoque.html', icon: 'box' },
    { group: 'Producao', id: 'importar_producao', label: 'Importar Producao', href: 'importar_producao.html', icon: 'download' },

    // QUALIDADE / ANALISE
    { group: 'Qualidade', id: 'dashboard_qualidade', label: 'Dashboard Qualidade', href: 'dashboard_qualidade.html', icon: 'bar-chart' },
    { group: 'Qualidade', id: 'dashboard_qualidade_por_estado', label: 'Qualidade por Estado', href: 'dashboard_qualidade_por_estado.html', icon: 'map' },
    { group: 'Qualidade', id: 'dashboard_produto_estado', label: 'Produto por Estado', href: 'dashboard_produto_estado.html', icon: 'map' },
    { group: 'Qualidade', id: 'dashboard_defect_origin', label: 'Mapa de Defeitos', href: 'dashboard_defect_origin.html', icon: 'alert-triangle' },
    { group: 'Qualidade', id: 'dashboard_quality_timeline', label: 'Quality Timeline', href: 'dashboard_quality_timeline.html', icon: 'clock' },
    { group: 'Qualidade', id: 'dashboard_risk_radar', label: 'Radar de Risco', href: 'dashboard_risk_radar.html', icon: 'activity' },
    { group: 'Qualidade', id: 'dashboard_nc_planos', label: 'NCs & Planos de Acao', href: 'dashboard_nc_planos.html', icon: 'alert-triangle' },
    { group: 'Qualidade', id: 'dashboard_cq_efficiency', label: 'Eficiencia do CQ', href: 'dashboard_cq_efficiency.html', icon: 'trending-up' },
    { group: 'Qualidade', id: 'dashboard_omaso', label: 'Controle Omaso', href: 'dashboard_omaso.html', icon: 'activity' },

    // IMPORTACAO / ETL
    { group: 'Importacao', id: 'importar_atak', label: 'Importar ATAK', href: 'importar_atak.html', icon: 'download' },
    { group: 'Importacao', id: 'importar_pcr', label: 'Importar PCR', href: 'importar_pcr.html', icon: 'download' },
    { group: 'Importacao', id: 'importar_planilha', label: 'Importar Planilha', href: 'importar_planilha.html', icon: 'download' },
    { group: 'Importacao', id: 'historico_importacoes', label: 'Historico Importacoes', href: 'historico_importacoes.html', icon: 'clock' },

    // RELATORIOS / FERRAMENTAS
    { group: 'Ferramentas', id: 'calculadora_financeira', label: 'Calculadora Financeira', href: 'calculadora_financeira.html', icon: 'dollar' },
    { group: 'Ferramentas', id: 'relatorio_cargas', label: 'Relatorio de Cargas', href: 'relatorio_cargas.html', icon: 'file-text' },
    { group: 'Ferramentas', id: 'layout_exportacao', label: 'Layout Exportacao', href: 'layout_exportacao.html', icon: 'globe' },

    // ADMINISTRACAO
    { group: 'Admin', id: 'gestao_usuarios', label: 'Gestao de Usuarios', href: 'gestao_usuarios.html', icon: 'shield', perfis: ['administrador'] },
  ];

  // SVG icons
  const ICONS = {
    'home': '<path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    'users': '<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
    'file-text': '<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>',
    'clipboard': '<path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/>',
    'plus-square': '<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/>',
    'check-circle': '<path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
    'check': '<polyline points="20 6 9 17 4 12"/>',
    'award': '<circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/>',
    'list': '<line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/>',
    'package': '<path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/>',
    'bar-chart': '<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
    'truck': '<rect x="1" y="3" width="15" height="13"/><path d="M16 8h4l3 4v5h-7V8z"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/>',
    'download': '<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>',
    'map': '<polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"/><line x1="8" y1="2" x2="8" y2="18"/><line x1="16" y1="6" x2="16" y2="22"/>',
    'grid': '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>',
    'layers': '<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>',
    'search': '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    'box': '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/>',
    'alert-triangle': '<path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
    'clock': '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    'activity': '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    'trending-up': '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>',
    'dollar': '<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/>',
    'globe': '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/>',
    'shield': '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    'log-out': '<path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/>',
  };

  function svgIcon(name) {
    return `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${ICONS[name] || ICONS['file-text']}</svg>`;
  }

  let _perfil = null;
  let _userName = '';

  function renderSidebar(activePage) {
    const brandText = 'Classic CQ';
    const groups = {};
    ROUTES.forEach(r => {
      if (r.perfis && _perfil && !r.perfis.includes(_perfil)) return;
      if (!groups[r.group]) groups[r.group] = [];
      groups[r.group].push(r);
    });

    let html = `
    <aside class="sidebar" id="sidebar">
      <div class="sidebar-brand">
        <img src="logo-classic-icon.jpg" alt="" onerror="this.style.display='none'">
        <span class="sidebar-brand-text">${brandText}</span>
      </div>
      <button class="sidebar-toggle" id="sidebarToggle" title="Recolher menu">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
      </button>
      <nav class="sidebar-nav">`;

    for (const [group, items] of Object.entries(groups)) {
      html += `<div class="sidebar-group"><div class="sidebar-group-label">${group}</div>`;
      items.forEach(r => {
        const active = r.id === activePage ? ' active' : '';
        html += `<a href="${r.href}" class="sidebar-item${active}" data-page="${r.id}">
          <span class="sidebar-item-icon">${svgIcon(r.icon)}</span>
          <span class="sidebar-item-label">${r.label}</span>
        </a>`;
      });
      html += '</div>';
    }

    html += `<div class="sidebar-group" style="margin-top:auto;padding-top:12px;border-top:1px solid var(--border);">
      <a href="portal.html" class="sidebar-item" style="color:var(--text-muted);">
        <span class="sidebar-item-icon">${svgIcon('home')}</span>
        <span class="sidebar-item-label">Voltar ao Portal</span>
      </a>
      <a href="#" onclick="CQAuth.logout();return false;" class="sidebar-item" style="color:var(--text-muted);">
        <span class="sidebar-item-icon">${svgIcon('log-out')}</span>
        <span class="sidebar-item-label">Sair</span>
      </a>
    </div>`;
    html += '</nav></aside>';
    html += '<div class="sidebar-mobile-overlay" id="sidebarOverlay"></div>';
    return html;
  }

  function renderTopbar(title, breadcrumb, actions) {
    const initials = _userName.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase();
    return `
    <header class="topbar">
      <div class="topbar-left">
        <button class="topbar-hamburger" id="hamburgerBtn" aria-label="Menu">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
        </button>
        <div class="topbar-breadcrumb">
          <a href="dashboard_cq.html">Classic CQ</a> &rsaquo; <span>${breadcrumb || title}</span>
        </div>
      </div>
      <div class="topbar-right">
        ${actions || ''}
        <div class="topbar-user">
          <span class="topbar-user-name">${_userName}</span>
          <span class="topbar-user-role">${(_perfil || '').replace('_', ' ')}</span>
        </div>
        <div class="topbar-avatar">${initials || '--'}</div>
      </div>
    </header>`;
  }

  async function init(opts = {}) {
    const { page, title, breadcrumb, perfisPermitidos, actions } = opts;

    const user = await CQAuth.init(perfisPermitidos ? { perfisPermitidos } : undefined);
    if (!user) return null;

    _perfil = user.perfil;
    _userName = user.nome || user.nome_completo || user.email || 'Usuario';

    const shellEl = document.getElementById('shell');
    if (shellEl) {
      shellEl.innerHTML = renderSidebar(page) +
        `<div class="main-content">${renderTopbar(title || 'Classic CQ', breadcrumb, actions)}<div class="content-area" id="page-content"></div></div>`;

      const content = document.getElementById('shell-content');
      const target = document.getElementById('page-content');
      if (content && target) {
        while (content.firstChild) target.appendChild(content.firstChild);
        content.remove();
      }
    }

    bindEvents();
    return user;
  }

  function bindEvents() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebarOverlay');
    const toggle = document.getElementById('sidebarToggle');
    const hamburger = document.getElementById('hamburgerBtn');

    if (toggle) toggle.addEventListener('click', () => {
      sidebar?.classList.toggle('collapsed');
      localStorage.setItem('cq_sidebar_collapsed', sidebar?.classList.contains('collapsed'));
    });

    if (hamburger) hamburger.addEventListener('click', () => {
      sidebar?.classList.toggle('mobile-open');
      overlay?.classList.toggle('active');
    });

    if (overlay) overlay.addEventListener('click', () => {
      sidebar?.classList.remove('mobile-open');
      overlay?.classList.remove('active');
    });

    if (localStorage.getItem('cq_sidebar_collapsed') === 'true') {
      sidebar?.classList.add('collapsed');
    }
  }

  return { init, ROUTES };
})();
