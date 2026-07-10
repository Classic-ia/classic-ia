// ══════════════════════════════════════════════════════
// dashboard.js — Controlador do Dashboard shell
// Sprint: S0.2 · BLOCO 002 (Dashboard shell — pós-login, navegação lateral)
// Monta a sidebar (gated por perfil), a topbar e roteia os domínios por hash.
// ══════════════════════════════════════════════════════
import { APP } from './config.js';
import { exigirAuth, logout, temPerfil } from './auth.js';
import { Router } from './router.js';
import { Logger } from './logger.js';

// ── Ícones (SVG stroke, herdam currentColor) ───────────
const ico = {
  inicio:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>',
  ativos:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg>',
  producao:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 20h20"/><path d="M4 20V9l6 4V9l6 4V9l4 3v8"/></svg>',
  qualidade:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 12l2 2 4-4"/><path d="M12 3l7 4v5c0 5-3.5 7.5-7 9-3.5-1.5-7-4-7-9V7z"/></svg>',
  recebimento:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3h5v5"/><path d="M21 3l-9 9"/><path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"/></svg>',
  expedicao:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><path d="M16 8h4l3 3v5h-7z"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
  bi:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><rect x="7" y="12" width="3" height="6"/><rect x="12" y="8" width="3" height="10"/><rect x="17" y="4" width="3" height="14"/></svg>',
  usuarios:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
  menu:
    '<svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>',
};

// ── Mapa de navegação ──────────────────────────────────
// perfil = perfil MÍNIMO exigido (hierarquia herda para cima). Ver auth.HIERARQUIA.
const NAV = [
  {
    secao: 'Operação',
    itens: [
      { hash: 'inicio', label: 'Dashboard', ico: ico.inicio, perfil: 'operador' },
      { hash: 'ativos', label: 'Ativos', ico: ico.ativos, perfil: 'operador' },
      { hash: 'producao', label: 'Produção', ico: ico.producao, perfil: 'producao' },
      { hash: 'recebimento', label: 'Recebimento', ico: ico.recebimento, perfil: 'producao' },
      { hash: 'expedicao', label: 'Expedição', ico: ico.expedicao, perfil: 'expedicao' },
    ],
  },
  {
    secao: 'Qualidade',
    itens: [
      { hash: 'qualidade', label: 'Qualidade & Compliance', ico: ico.qualidade, perfil: 'laboratorio' },
    ],
  },
  {
    secao: 'Gestão',
    itens: [
      { hash: 'bi', label: 'BI & Conformidade', ico: ico.bi, perfil: 'gerente' },
      { hash: 'usuarios', label: 'Usuários', ico: ico.usuarios, perfil: 'admin' },
    ],
  },
];

// Domínios ainda não implementados (renderizam placeholder do shell).
const DOMINIOS = {
  ativos: { titulo: 'Ativos', desc: 'Gestão de equipamentos (digestores, tanques). Pré-requisito dos demais domínios.', sprint: 'Sprint 1' },
  producao: { titulo: 'Produção', desc: 'Cargas, PCC e lotes automáticos (DT-PROD-v1.1).', sprint: 'Sprint 2' },
  recebimento: { titulo: 'Recebimento', desc: 'Rastreabilidade de matéria-prima na entrada.', sprint: 'Sprint 4' },
  expedicao: { titulo: 'Expedição', desc: 'Saída de produto acabado e rastreabilidade end-to-end.', sprint: 'Sprint 4' },
  qualidade: { titulo: 'Qualidade & Compliance', desc: 'Análises, liberação do RT e certificados (DT-QUAL-v1.0).', sprint: 'Sprint 3' },
  bi: { titulo: 'BI & Conformidade', desc: 'Dashboards, relatório MAPA e autocontroles.', sprint: 'Sprint 5' },
  usuarios: { titulo: 'Usuários', desc: 'Administração de perfis e permissões.', sprint: 'A definir' },
};

let _perfil = null;

// ── Renderização ───────────────────────────────────────
function el(id) {
  return document.getElementById(id);
}

function montarSidebar() {
  const nav = el('sidebarNav');
  let html = '';
  for (const grupo of NAV) {
    const itensVisiveis = grupo.itens.filter((i) => temPerfil(_perfil.perfil, [i.perfil]));
    if (!itensVisiveis.length) continue;
    html += `<div class="sidebar-nav-sec">${grupo.secao}</div>`;
    for (const i of itensVisiveis) {
      html += `<a class="nav-item" href="#${i.hash}" data-hash="${i.hash}">${i.ico}<span>${i.label}</span></a>`;
    }
  }
  nav.innerHTML = html;

  // Rodapé do usuário
  const iniciais = (_perfil.nome || '?')
    .split(' ')
    .slice(0, 2)
    .map((p) => p[0])
    .join('')
    .toUpperCase();
  el('sidebarFooter').innerHTML = `
    <div class="sidebar-brand-logo" style="width:34px;height:34px;font-size:13px">${iniciais}</div>
    <div class="sidebar-user">
      <strong>${_perfil.nome || 'Usuário'}</strong>
      <span>${(_perfil.perfil || '').replace('_', ' ')}</span>
    </div>
    <button class="btn-ghost" id="btnLogout" title="Sair" style="color:var(--sidebar-texto-suave);padding:6px">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
    </button>`;
  el('btnLogout').addEventListener('click', () => logout());
}

function marcarAtivo(hash) {
  document.querySelectorAll('.nav-item').forEach((a) => {
    a.classList.toggle('ativo', a.dataset.hash === hash);
  });
  const item = NAV.flatMap((g) => g.itens).find((i) => i.hash === hash);
  el('topbarTitle').textContent = item ? item.label : 'Dashboard';
}

// View: home do dashboard. Sem dados falsos — KPIs em estado "aguardando dados".
function viewInicio() {
  el('view').innerHTML = `
    <div class="page-head">
      <h1>Bem-vindo(a), ${_perfil.nome ? _perfil.nome.split(' ')[0] : ''}</h1>
      <p>Painel operacional · Planta Jardim Alegre (JA-01) · ${APP.nome} ${APP.versao}</p>
    </div>
    <div class="grid-kpi">
      ${kpi('Cargas hoje', '—', 'Aguardando domínio Produção')}
      ${kpi('Lotes em análise', '—', 'Aguardando domínio Qualidade')}
      ${kpi('Ativos ativos', '—', 'Aguardando domínio Ativos')}
      ${kpi('Não conformidades', '—', 'Aguardando domínio Qualidade')}
    </div>
    <div class="card card-pad mt-16">
      <h2 style="font-size:15px;margin-bottom:6px">Sprint 0.2 — Frontend Shell</h2>
      <p class="muted">Este é o shell de navegação. Os KPIs e domínios operacionais são
      preenchidos conforme cada sprint é entregue (Ativos → Produção → Qualidade → Expedição → BI).</p>
    </div>`;
}

function kpi(label, valor, sub) {
  return `<div class="kpi"><div class="kpi-label">${label}</div><div class="kpi-valor">${valor}</div><div class="kpi-sub">${sub}</div></div>`;
}

// View: placeholder de domínio ainda não implementado.
function viewDominio(hash) {
  const d = DOMINIOS[hash] || { titulo: hash, desc: 'Módulo em desenvolvimento.', sprint: '' };
  el('view').innerHTML = `
    <div class="placeholder">
      <div class="placeholder-icone">
        <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      </div>
      <h2>${d.titulo}</h2>
      <p>${d.desc}</p>
      <p class="muted mt-16">Módulo ainda não implementado neste shell.</p>
      ${d.sprint ? `<span class="tag-sprint">Previsto: ${d.sprint}</span>` : ''}
    </div>`;
}

function viewNaoEncontrada(hash) {
  el('view').innerHTML = `
    <div class="placeholder">
      <h2>Página não encontrada</h2>
      <p>A rota <code>#${hash}</code> não existe.</p>
      <p class="mt-16"><a class="btn btn-primario" href="#inicio">Voltar ao dashboard</a></p>
    </div>`;
}

// ── Mobile ─────────────────────────────────────────────
function ligarMenuMobile() {
  const shell = el('shell');
  el('menuToggle').addEventListener('click', () => shell.classList.toggle('sidebar-aberta'));
  el('sidebarOverlay').addEventListener('click', () => shell.classList.remove('sidebar-aberta'));
}

// ── Boot ───────────────────────────────────────────────
async function iniciar() {
  _perfil = await exigirAuth(); // qualquer perfil autenticado e ativo
  if (!_perfil) return; // exigirAuth já redirecionou

  Logger.info('Dashboard carregado', { perfil: _perfil.perfil });

  montarSidebar();
  ligarMenuMobile();

  Router.registrar('inicio', viewInicio, { default: true })
    .registrar('ativos', viewDominio)
    .registrar('producao', viewDominio)
    .registrar('recebimento', viewDominio)
    .registrar('expedicao', viewDominio)
    .registrar('qualidade', viewDominio)
    .registrar('bi', viewDominio)
    .registrar('usuarios', viewDominio)
    .naoEncontrada(viewNaoEncontrada)
    .aoNavegar((hash) => {
      marcarAtivo(hash);
      el('shell').classList.remove('sidebar-aberta'); // fecha menu mobile ao navegar
    })
    .iniciar();
}

iniciar();
