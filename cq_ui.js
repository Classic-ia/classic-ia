/**
 * cq_ui.js — Classic CQ · Componentes Reutilizáveis
 *
 * Detecção de contexto (mobile/desktop) + componentes HTML + helpers.
 * Requer: cq_layout.css, config.js, cq_api.js, cq_auth.js
 *
 * Uso:
 *   <script src="cq_ui.js"></script>
 *
 *   CQ.isMobile        // true se < 1025px
 *   CQ.shell(options)  // renderiza header + sidebar + bottomnav
 *   CQ.table(dados, colunas) // tabela desktop + card-list mobile
 *   CQ.counter(id, opts)     // botão +/- para inspeção
 */

const CQ = (function () {

  // ══════════════════════════════════════════════════════════════
  // DETECÇÃO DE CONTEXTO
  // ══════════════════════════════════════════════════════════════
  const MOBILE_BREAK = 1025;
  let _isMobile = window.innerWidth < MOBILE_BREAK;
  let _listeners = [];

  window.addEventListener('resize', () => {
    const was = _isMobile;
    _isMobile = window.innerWidth < MOBILE_BREAK;
    if (was !== _isMobile) _listeners.forEach(fn => fn(_isMobile));
  });

  function onContextChange(fn) { _listeners.push(fn); }


  // ══════════════════════════════════════════════════════════════
  // SHELL — Header + Sidebar + Bottom Nav
  // ══════════════════════════════════════════════════════════════
  const NAV_ITEMS = [
    // Mobile bottom bar (max 5)
    { id: 'dashboard',  label: 'Dashboard',    icon: '&#9632;',  href: 'dashboard_qualidade.html', mobile: true },
    { id: 'cargas',     label: 'Cargas',       icon: '&#9881;',  href: 'lancamento_cargas.html',   mobile: true },
    { id: 'inspecao',   label: 'Inspecao',     icon: '&#9998;',  href: 'inspecao_qualidade.html',  mobile: true },
    { id: 'relatorios', label: 'Relatorios',   icon: '&#9776;',  href: 'ranking_cq.html',          mobile: true },
    { id: 'config',     label: 'Config',       icon: '&#9881;',  href: 'gestao_usuarios.html',     mobile: true },

    // Desktop sidebar extras (seções)
    { section: 'Qualidade' },
    { id: 'dashboard',    label: 'Dashboard',        icon: '&#9632;', href: 'dashboard_qualidade.html' },
    { id: 'fila',         label: 'Fila Inspecoes',    icon: '&#9776;', href: 'fila_inspecoes.html' },
    { id: 'inspecao',     label: 'Nova Inspecao',     icon: '&#9998;', href: 'inspecao_qualidade.html' },
    { id: 'ranking',      label: 'Ranking CQ',        icon: '&#9733;', href: 'ranking_cq.html' },
    { id: 'nc',           label: 'NCs & Planos',      icon: '&#9888;', href: 'dashboard_nc_planos.html' },

    { section: 'Producao' },
    { id: 'cargas',       label: 'Cargas',             icon: '&#9881;', href: 'lancamento_cargas.html' },
    { id: 'recebimento',  label: 'Recebimento',        icon: '&#8615;', href: 'recebimento_lote.html' },
    { id: 'producao',     label: 'Producao',            icon: '&#9881;', href: 'dashboard_producao.html' },
    { id: 'rastreio',     label: 'Rastreabilidade',     icon: '&#8634;', href: 'rastreabilidade_lote.html' },

    { section: 'Cadastros' },
    { id: 'fornecedores', label: 'Fornecedores',       icon: '&#9971;', href: 'gestao_fornecedores.html' },
    { id: 'importar',     label: 'Importar Planilha',  icon: '&#8679;', href: 'importar_planilha.html' },
    { id: 'historico',    label: 'Historico Import.',    icon: '&#8635;', href: 'historico_importacoes.html' },

    { section: 'Sistema' },
    { id: 'usuarios',     label: 'Usuarios',           icon: '&#9881;', href: 'gestao_usuarios.html' },
  ];

  /**
   * Renderiza o shell (header + sidebar + bottomnav).
   * @param {Object} opts
   * @param {string} opts.activePage — id da página ativa
   * @param {Object} opts.usuario — { nome, email, perfil }
   */
  function shell(opts = {}) {
    const activePage = opts.activePage || '';
    const u = opts.usuario || {};

    // Header
    const header = document.querySelector('.cq-header');
    if (header) {
      const logo = localStorage.getItem('classic_logo');
      const logoImg = header.querySelector('.cq-header-logo');
      if (logoImg && logo) logoImg.src = logo;

      const userEl = header.querySelector('.cq-header-user');
      if (userEl && u.nome) {
        userEl.innerHTML = `<strong>${_esc(u.nome || u.email)}</strong><br>${(u.perfil || '').toUpperCase()}`;
      }
      const avatarEl = header.querySelector('.cq-header-avatar');
      if (avatarEl && u.nome) {
        avatarEl.textContent = (u.nome || u.email || '?').charAt(0).toUpperCase();
      }
    }

    // Sidebar (desktop)
    const sidebar = document.querySelector('.cq-sidebar');
    if (sidebar) {
      let html = '';
      NAV_ITEMS.forEach(item => {
        if (item.mobile && !item.section) return; // skip mobile-only duplicates in sidebar
        if (item.section) {
          html += `<div class="nav-section">${item.section}</div>`;
          return;
        }
        const active = item.id === activePage ? ' active' : '';
        html += `<a href="${item.href}" class="${active}"><span class="nav-icon">${item.icon}</span>${item.label}</a>`;
      });
      sidebar.innerHTML = html;
    }

    // Bottom nav (mobile)
    const bnav = document.querySelector('.cq-bottomnav');
    if (bnav) {
      const mobileItems = NAV_ITEMS.filter(i => i.mobile);
      bnav.innerHTML = mobileItems.map(item => {
        const active = item.id === activePage ? ' active' : '';
        return `<a href="${item.href}" class="${active}"><span class="bnav-icon">${item.icon}</span>${item.label}</a>`;
      }).join('');
    }
  }


  // ══════════════════════════════════════════════════════════════
  // TABLE — Desktop table + Mobile card-list
  // ══════════════════════════════════════════════════════════════
  /**
   * Gera HTML de tabela (desktop) + card-list (mobile).
   * @param {Array} dados
   * @param {Array} colunas — [{ key, label, align, format, mobile, mobileLabel, color }]
   * @param {Object} opts — { critFn, onRowClick }
   * @returns {string} HTML
   */
  function table(dados, colunas, opts = {}) {
    if (!dados || !dados.length) {
      return `<div class="empty-state">${opts.emptyMsg || 'Sem dados disponiveis.'}</div>`;
    }

    const critFn = opts.critFn || (() => false);

    // Desktop table
    const ths = colunas.map(c => `<th style="${c.align ? 'text-align:' + c.align : ''}">${c.label}</th>`).join('');
    const trs = dados.map(d => {
      const cls = critFn(d) ? ' class="row-crit"' : '';
      const tds = colunas.map(c => {
        const val = c.format ? c.format(d[c.key], d) : (d[c.key] ?? '—');
        const style = [];
        if (c.align) style.push('text-align:' + c.align);
        if (c.mono) style.push("font-family:'DM Mono',monospace;font-size:11px");
        if (c.color) style.push('color:' + (typeof c.color === 'function' ? c.color(d[c.key], d) : c.color));
        if (c.bold) style.push("font-family:'Syne',sans-serif;font-weight:700");
        return `<td style="${style.join(';')}">${val}</td>`;
      }).join('');
      return `<tr${cls}>${tds}</tr>`;
    }).join('');

    const desktopHTML = `
      <div class="table-panel desktop-only">
        <div class="table-wrap">
          <table class="cq-table">
            <thead><tr>${ths}</tr></thead>
            <tbody>${trs}</tbody>
          </table>
        </div>
      </div>`;

    // Mobile card-list
    const mobileCols = colunas.filter(c => c.mobile !== false);
    const primaryCol = mobileCols[0];
    const cardItems = dados.map(d => {
      const cls = critFn(d) ? ' crit' : '';
      const title = primaryCol.format
        ? primaryCol.format(d[primaryCol.key], d)
        : (d[primaryCol.key] || '—');
      const rows = mobileCols.slice(1).map(c => {
        const val = c.format ? c.format(d[c.key], d) : (d[c.key] ?? '—');
        const colorStyle = c.color ? `color:${typeof c.color === 'function' ? c.color(d[c.key], d) : c.color}` : '';
        return `<div class="card-list-row">
          <span class="card-list-label">${c.mobileLabel || c.label}</span>
          <span class="card-list-value" style="${colorStyle}">${val}</span>
        </div>`;
      }).join('');

      return `<div class="card-list-item${cls}">
        <div style="font-weight:700;margin-bottom:6px">${title}</div>
        ${rows}
      </div>`;
    }).join('');

    const mobileHTML = `<div class="card-list mobile-only">${cardItems}</div>`;

    return desktopHTML + mobileHTML;
  }


  // ══════════════════════════════════════════════════════════════
  // COUNTER — +/- para inspeção
  // ══════════════════════════════════════════════════════════════
  function counter(containerId, opts = {}) {
    const el = document.getElementById(containerId);
    if (!el) return;

    const min = opts.min ?? 0;
    const max = opts.max ?? 9999;
    let val = opts.initial ?? 0;
    const onChange = opts.onChange || (() => {});

    // Event delegation: single listener, no leaks on re-render
    let _bound = false;
    const render = () => {
      el.innerHTML = `
        <div class="counter">
          <button class="counter-btn minus" data-dir="-1">&minus;</button>
          <div class="counter-value">${val}</div>
          <button class="counter-btn plus" data-dir="1">&plus;</button>
        </div>`;
      if (!_bound) {
        _bound = true;
        el.addEventListener('click', (e) => {
          const btn = e.target.closest('.counter-btn');
          if (!btn) return;
          const dir = parseInt(btn.dataset.dir);
          val = Math.max(min, Math.min(max, val + dir));
          render();
          onChange(val);
        });
      }
    };
    render();

    return {
      get value() { return val; },
      set value(v) { val = Math.max(min, Math.min(max, v)); render(); },
    };
  }


  // ══════════════════════════════════════════════════════════════
  // WIZARD — Fluxo por etapas
  // ══════════════════════════════════════════════════════════════
  function wizard(containerId, steps) {
    let current = 0;
    const container = document.getElementById(containerId);
    if (!container) return;

    function render() {
      // Steps bar
      const stepsHTML = `<div class="wizard-steps">${
        steps.map((_, i) => `<div class="wizard-step ${i < current ? 'done' : ''} ${i === current ? 'active' : ''}"></div>`).join('')
      }</div>`;

      // Panels
      const panelsHTML = steps.map((step, i) =>
        `<div class="wizard-panel ${i === current ? 'active' : ''}" data-step="${i}">${step.render(i)}</div>`
      ).join('');

      container.innerHTML = stepsHTML + panelsHTML;

      // Bind events
      steps[current].onMount?.(container.querySelector(`[data-step="${current}"]`));
    }

    render();

    return {
      next() { if (current < steps.length - 1) { current++; render(); } },
      prev() { if (current > 0) { current--; render(); } },
      goto(i) { if (i >= 0 && i < steps.length) { current = i; render(); } },
      get current() { return current; },
      get isLast() { return current === steps.length - 1; },
    };
  }


  // ══════════════════════════════════════════════════════════════
  // LOOKUP — Auto buscar por documento ATAK
  // ══════════════════════════════════════════════════════════════
  async function lookupDocumento(documento) {
    if (!documento || documento.length < 3) return null;

    // Tentar buscar na vw_atak_cq_consolidado
    const dados = await CQ_API.get(
      `vw_atak_cq_consolidado?documento=eq.${encodeURIComponent(documento)}&limit=1`
    );

    if (dados && dados.length > 0) {
      const d = dados[0];
      return {
        documento: d.documento,
        fornecedor: d.participante_nome || d.fornecedor_nome_cadastro,
        fornecedor_codigo: d.participante_codigo,
        produto: d.produto || d.id_atak_produto,
        quantidade: d.quantidade_erp,
        data: d.data_movimentacao,
      };
    }

    // Fallback: buscar em atak_cargas_raw
    const cargas = await CQ_API.get(
      `atak_cargas_raw?or=(numero_documento.eq.${encodeURIComponent(documento)},numero_pcr.eq.${encodeURIComponent(documento)})&limit=1`
    );

    if (cargas && cargas.length > 0) {
      const c = cargas[0];
      return {
        documento: c.numero_documento || c.numero_pcr,
        fornecedor: c.fornecedor_nome,
        fornecedor_codigo: c.fornecedor_codigo,
        produto: c.produto,
        quantidade: c.qtd_frigo,
        data: c.data_chegada || c.data_coleta,
      };
    }

    return null;
  }


  // ══════════════════════════════════════════════════════════════
  // HELPERS
  // ══════════════════════════════════════════════════════════════
  function _esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function fmtNum(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString('pt-BR');
  }

  function fmtPct(n) {
    if (n == null) return '—';
    return Number(n).toFixed(1) + '%';
  }

  function fmtDate(d) {
    if (!d || typeof d !== 'string') return '—';
    const parts = d.split('-');
    if (parts.length === 3) return parts[2] + '/' + parts[1];
    return d;
  }

  function scoreColor(s) {
    if (s == null) return 'var(--muted)';
    if (s < 50) return 'var(--danger)';
    if (s < 70) return 'var(--warn)';
    return 'var(--ok)';
  }

  function scoreBarFill(s) {
    if (s == null) return 'var(--muted2)';
    if (s < 50) return 'var(--danger)';
    if (s < 70) return 'var(--warn)';
    return 'var(--good)';
  }

  function tendIcon(t) {
    if (t === 'melhorando') return '<span class="badge badge-up" title="Melhorando">&#9650;</span>';
    if (t === 'piorando')   return '<span class="badge badge-down" title="Piorando">&#9660;</span>';
    return '<span class="badge badge-flat" title="Estavel">&#9654;</span>';
  }

  function classBadge(cls) {
    const c = (cls || '').toUpperCase();
    if (c === 'A') return '<span class="badge badge-a">A</span>';
    if (c === 'B') return '<span class="badge badge-b">B</span>';
    if (c === 'C') return '<span class="badge badge-c">C</span>';
    return '<span class="badge badge-flat">—</span>';
  }


  // ══════════════════════════════════════════════════════════════
  // API PÚBLICA
  // ══════════════════════════════════════════════════════════════
  return {
    // Contexto
    get isMobile() { return _isMobile; },
    onContextChange,

    // Layout
    shell,

    // Componentes
    table,
    counter,
    wizard,

    // Lookup
    lookupDocumento,

    // Formatadores
    esc: _esc,
    fmtNum,
    fmtPct,
    fmtDate,
    scoreColor,
    scoreBarFill,
    tendIcon,
    classBadge,
  };

})();
