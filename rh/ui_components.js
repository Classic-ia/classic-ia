/**
 * ui_components.js — Componentes UI padrao
 * PROIBIDO criar UI fora deste padrao em paginas novas
 */
const UI = (() => {

  // ============================================================
  // TABELA PADRAO
  // ============================================================
  function table(headers, rows, opts = {}) {
    const { id, emptyMsg, onRowClick } = opts;
    if (!rows || rows.length === 0) return emptyState(emptyMsg);

    let html = `<div class="table-scroll"><table class="data-table"${id ? ' id="'+id+'"' : ''}>`;
    html += '<thead><tr>';
    headers.forEach(h => {
      const align = h.align === 'right' ? ' style="text-align:right"' : '';
      html += `<th${align}>${API.esc(h.label || h)}</th>`;
    });
    html += '</tr></thead><tbody>';

    rows.forEach((row, i) => {
      const click = onRowClick ? ` onclick="${onRowClick}(${i})" style="cursor:pointer"` : '';
      html += `<tr${click}>`;
      row.forEach((cell, j) => {
        const align = headers[j]?.align === 'right' ? ' style="text-align:right"' : '';
        html += `<td${align}>${cell}</td>`;
      });
      html += '</tr>';
    });

    html += '</tbody></table></div>';
    return html;
  }

  // ============================================================
  // KPI CARD
  // ============================================================
  function kpiCard(value, label, opts = {}) {
    const { color, icon, href, id } = opts;
    const tag = href ? 'a' : 'div';
    const hrefAttr = href ? ` href="${href}" style="text-decoration:none;color:inherit;"` : '';
    const colorStyle = color ? ` style="color:${color}"` : '';
    return `<${tag} class="kpi-card"${hrefAttr}>
      ${icon ? `<div class="kpi-icon">${icon}</div>` : ''}
      <div class="kpi-value"${colorStyle}${id ? ' id="'+id+'"' : ''}>${value}</div>
      <div class="kpi-label">${API.esc(label)}</div>
    </${tag}>`;
  }

  // ============================================================
  // LOADING / SKELETON
  // ============================================================
  function skeleton(w = '100%', h = '20px') {
    return `<div style="width:${w};height:${h};border-radius:6px;background:linear-gradient(90deg,#E2E8F0 25%,#F1F5F9 50%,#E2E8F0 75%);background-size:200% 100%;animation:shimmer 1.5s infinite;"></div>`;
  }

  function spinner(size = 24) {
    return `<div style="display:inline-block;width:${size}px;height:${size}px;border:3px solid #E2E8F0;border-top-color:#2563EB;border-radius:50%;animation:spin .7s linear infinite;"></div>`;
  }

  function loadingCenter(msg = 'Carregando...') {
    return `<div style="text-align:center;padding:40px 20px;color:#64748B;">
      ${spinner(28)}
      <div style="margin-top:12px;font-size:13px;">${msg}</div>
    </div>`;
  }

  // ============================================================
  // EMPTY STATE
  // ============================================================
  function emptyState(msg = 'Nenhum registro encontrado', icon = '📋') {
    return `<div style="text-align:center;padding:40px 20px;color:#94A3B8;">
      <div style="font-size:32px;margin-bottom:8px;">${icon}</div>
      <div style="font-size:14px;">${msg}</div>
    </div>`;
  }

  // ============================================================
  // TOAST
  // ============================================================
  function toast(msg, tipo = 'ok') {
    const colors = {
      ok: ['#DCFCE7','#166534','#86EFAC'],
      warn: ['#FEF3C7','#92400E','#FCD34D'],
      err: ['#FEE2E2','#991B1B','#FECACA'],
      info: ['#EFF6FF','#1E40AF','#BFDBFE']
    };
    const [bg, color, border] = colors[tipo] || colors.ok;
    const el = document.createElement('div');
    el.style.cssText = `position:fixed;bottom:24px;right:24px;padding:12px 20px;border-radius:8px;font-size:13px;font-weight:500;z-index:9999;box-shadow:0 4px 12px rgba(0,0,0,.15);background:${bg};color:${color};border:1px solid ${border};transition:opacity .3s;font-family:Inter,sans-serif;`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3500);
  }

  // ============================================================
  // BADGE STATUS
  // ============================================================
  function badge(text, color) {
    if (!color) {
      const colors = {
        ativo:'#059669', afastado:'#D97706', ferias:'#2563EB', experiencia:'#0891B2', desligado:'#DC2626',
        critica:'#DC2626', alta:'#D97706', media:'#EAB308', baixa:'#2563EB', informativa:'#64748B',
        consistente:'#059669', regular:'#0891B2', monitorar:'#D97706', atencao_operacional:'#DC2626', dado_insuficiente:'#64748B',
        aberta:'#DC2626', em_tratativa:'#D97706', resolvida:'#059669', pendente:'#EAB308',
        saudavel:'#059669', atencao:'#D97706', degradado:'#DC2626', critico:'#DC2626',
      };
      color = colors[(text || '').toLowerCase()] || '#64748B';
    }
    return `<span style="display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600;background:${color}15;color:${color}">${API.esc(text || '—')}</span>`;
  }

  // ============================================================
  // MODAL
  // ============================================================
  function modal(id, title, bodyHtml, footerHtml) {
    return `<div class="modal-overlay" id="${id}" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:1000;display:none;align-items:center;justify-content:center;">
      <div style="background:#fff;border-radius:12px;max-width:600px;width:90%;max-height:85vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.2);">
        <div style="padding:16px 24px;border-bottom:1px solid #E2E8F0;display:flex;align-items:center;justify-content:space-between;">
          <h3 style="font-size:16px;font-weight:600;color:#1E293B;margin:0;">${API.esc(title)}</h3>
          <button onclick="UI.closeModal('${id}')" style="background:none;border:none;font-size:20px;cursor:pointer;color:#64748B;">&times;</button>
        </div>
        <div style="padding:20px 24px;">${bodyHtml}</div>
        ${footerHtml ? `<div style="padding:12px 24px;border-top:1px solid #E2E8F0;display:flex;justify-content:flex-end;gap:8px;">${footerHtml}</div>` : ''}
      </div>
    </div>`;
  }

  function openModal(id) {
    const el = document.getElementById(id);
    if (el) { el.style.display = 'flex'; }
  }

  function closeModal(id) {
    const el = document.getElementById(id);
    if (el) { el.style.display = 'none'; }
  }

  // ============================================================
  // PROGRESS BAR
  // ============================================================
  function progressBar(value, max = 100, color) {
    const pct = Math.min(Math.round((value / max) * 100), 100);
    if (!color) color = pct >= 80 ? '#059669' : pct >= 50 ? '#D97706' : '#DC2626';
    return `<div style="background:#E2E8F0;border-radius:4px;height:8px;overflow:hidden;">
      <div style="width:${pct}%;height:100%;background:${color};border-radius:4px;transition:width .3s;"></div>
    </div>`;
  }

  // ============================================================
  // CARD SECTION
  // ============================================================
  function card(title, content, opts = {}) {
    const { id, borderColor, className } = opts;
    const border = borderColor ? `border-left:4px solid ${borderColor};` : '';
    return `<div class="card ${className || ''}" ${id ? 'id="'+id+'"' : ''} style="${border}">
      ${title ? `<div class="card-header"><h3 style="font-size:14px;font-weight:600;color:#1E293B;margin:0;">${title}</h3></div>` : ''}
      <div class="card-body">${content}</div>
    </div>`;
  }

  // Inject animation CSS
  if (typeof document !== 'undefined' && !document.getElementById('ui-comp-styles')) {
    const s = document.createElement('style');
    s.id = 'ui-comp-styles';
    s.textContent = '@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}} @keyframes spin{to{transform:rotate(360deg)}}';
    document.head.appendChild(s);
  }

  return {
    table, kpiCard, skeleton, spinner, loadingCenter, emptyState,
    toast, badge, modal, openModal, closeModal, progressBar, card
  };
})();
