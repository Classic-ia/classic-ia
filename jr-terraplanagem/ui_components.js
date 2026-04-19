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

  // ============================================================
  // PAGE HEADER (config-driven)
  // ============================================================
  function pageHeader(config) {
    const { title, subtitle, actions } = config;
    let actionsHtml = '';
    if (actions && actions.length) {
      actionsHtml = '<div class="page-header-actions">' +
        actions.map(a => `<button class="btn ${a.class || 'btn-outline'}" onclick="${a.onclick || ''}">${a.icon || ''}${API.esc(a.label)}</button>`).join('') +
        '</div>';
    }
    return `<div class="page-header">
      <div class="page-header-text">
        <h1 class="page-header-title">${API.esc(title)}</h1>
        ${subtitle ? `<p class="page-header-subtitle">${API.esc(subtitle)}</p>` : ''}
      </div>
      ${actionsHtml}
    </div>`;
  }

  // ============================================================
  // ACTION BAR (config-driven)
  // ============================================================
  function actionBar(config) {
    const { left = [], right = [] } = config;
    const renderBtn = b => `<button class="btn ${b.class || 'btn-outline'}" onclick="${b.onclick || ''}" ${b.disabled ? 'disabled' : ''}>${b.icon ? b.icon + ' ' : ''}${API.esc(b.label)}</button>`;
    return `<div class="action-bar">
      <div class="action-bar-group">${left.map(renderBtn).join('')}</div>
      <div class="action-bar-spacer"></div>
      <div class="action-bar-group">${right.map(renderBtn).join('')}</div>
    </div>`;
  }

  // ============================================================
  // FILTER BAR (config-driven)
  // ============================================================
  function filterBar(config) {
    const { id, filters = [], onApply, onClear } = config;
    let html = `<div class="filter-bar" id="${id || 'filterBar'}">`;
    filters.forEach(f => {
      html += '<div class="filter-group">';
      html += `<label>${API.esc(f.label)}</label>`;
      if (f.type === 'text') {
        html += `<input type="text" id="${f.id}" placeholder="${f.placeholder || ''}" value="${f.value || ''}">`;
      } else if (f.type === 'select') {
        html += `<select id="${f.id}">`;
        (f.options || []).forEach(o => {
          const val = typeof o === 'object' ? o.value : o;
          const lbl = typeof o === 'object' ? o.label : o;
          html += `<option value="${val}" ${val === f.value ? 'selected' : ''}>${API.esc(lbl)}</option>`;
        });
        html += '</select>';
      } else if (f.type === 'date') {
        html += `<input type="date" id="${f.id}" value="${f.value || ''}">`;
      } else if (f.type === 'month') {
        html += `<input type="month" id="${f.id}" value="${f.value || ''}">`;
      }
      html += '</div>';
    });
    html += '<div class="filter-actions">';
    html += `<button class="btn btn-primary btn-sm" onclick="${onApply || ''}">Aplicar</button>`;
    html += `<button class="btn btn-outline btn-sm" onclick="${onClear || ''}">Limpar</button>`;
    html += '</div></div>';
    return html;
  }

  // ============================================================
  // DATA TABLE (config-driven, sort + pagination + actions)
  // ============================================================
  function dataTable(config) {
    const { columns, rows, emptyMsg, pageSize = 50, currentPage = 1, totalRows, onSort, onPageChange, onRowClick } = config;
    if (!rows || rows.length === 0) return emptyState(emptyMsg);

    let html = '<div class="table-scroll"><table class="data-table"><thead><tr>';
    columns.forEach(col => {
      const sortable = col.sortable ? ' style="cursor:pointer"' : '';
      const sortIcon = col.sortDir === 'asc' ? ' ↑' : col.sortDir === 'desc' ? ' ↓' : '';
      const onclick = col.sortable && onSort ? ` onclick="${onSort}('${col.key}')"` : '';
      const align = col.align === 'right' ? ' class="text-right"' : col.align === 'center' ? ' class="text-center"' : '';
      html += `<th${sortable}${onclick}${align}>${API.esc(col.label)}${sortIcon}</th>`;
    });
    html += '</tr></thead><tbody>';

    rows.forEach((row, i) => {
      const click = onRowClick ? ` onclick="${onRowClick}(${i})" style="cursor:pointer"` : '';
      html += `<tr${click}>`;
      columns.forEach(col => {
        const val = col.render ? col.render(row) : (row[col.key] ?? '');
        const align = col.align === 'right' ? ' class="text-right"' : col.align === 'center' ? ' class="text-center"' : '';
        html += `<td${align}>${val}</td>`;
      });
      html += '</tr>';
    });
    html += '</tbody></table></div>';

    // Pagination
    if (totalRows && pageSize) {
      const totalPages = Math.ceil(totalRows / pageSize);
      if (totalPages > 1) {
        html += pagination({ currentPage, totalPages, totalRows, pageSize, onChange: onPageChange });
      } else {
        html += `<div class="pagination"><span class="pagination-info">${totalRows} registro${totalRows !== 1 ? 's' : ''}</span></div>`;
      }
    }
    return html;
  }

  // ============================================================
  // PAGINATION
  // ============================================================
  function pagination(config) {
    const { currentPage, totalPages, totalRows, pageSize, onChange } = config;
    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(currentPage * pageSize, totalRows);
    let html = `<div class="pagination">
      <span class="pagination-info">Exibindo ${start}-${end} de ${totalRows}</span>
      <div class="pagination-controls">`;
    html += `<button class="pagination-btn" onclick="${onChange}(1)" ${currentPage <= 1 ? 'disabled' : ''}>&#171;</button>`;
    html += `<button class="pagination-btn" onclick="${onChange}(${currentPage - 1})" ${currentPage <= 1 ? 'disabled' : ''}>&#8249;</button>`;
    const maxVisible = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);
    if (endPage - startPage < maxVisible - 1) startPage = Math.max(1, endPage - maxVisible + 1);
    for (let p = startPage; p <= endPage; p++) {
      html += `<button class="pagination-btn ${p === currentPage ? 'active' : ''}" onclick="${onChange}(${p})">${p}</button>`;
    }
    html += `<button class="pagination-btn" onclick="${onChange}(${currentPage + 1})" ${currentPage >= totalPages ? 'disabled' : ''}>&#8250;</button>`;
    html += `<button class="pagination-btn" onclick="${onChange}(${totalPages})" ${currentPage >= totalPages ? 'disabled' : ''}>&#187;</button>`;
    html += '</div></div>';
    return html;
  }

  // ============================================================
  // FORM SECTION (config-driven)
  // ============================================================
  function formSection(config) {
    const { title, fields = [], columns } = config;
    let html = '<div class="form-section">';
    if (title) html += `<h3 class="form-section-title">${API.esc(title)}</h3>`;
    html += `<div class="form-grid" ${columns ? `style="grid-template-columns:repeat(${columns},1fr)"` : ''}>`;
    fields.forEach(f => {
      const fullClass = f.full ? ' form-field-full' : '';
      html += `<div class="form-field${fullClass}">`;
      html += `<label for="${f.id}">${API.esc(f.label)}${f.required ? ' *' : ''}</label>`;
      if (f.type === 'select') {
        html += `<select id="${f.id}" ${f.required ? 'required' : ''}>`;
        (f.options || []).forEach(o => {
          const val = typeof o === 'object' ? o.value : o;
          const lbl = typeof o === 'object' ? o.label : o;
          html += `<option value="${val}" ${val === f.value ? 'selected' : ''}>${API.esc(lbl)}</option>`;
        });
        html += '</select>';
      } else if (f.type === 'textarea') {
        html += `<textarea id="${f.id}" placeholder="${f.placeholder || ''}" ${f.required ? 'required' : ''}>${f.value || ''}</textarea>`;
      } else if (f.type === 'checkbox') {
        html += `<label style="display:flex;align-items:center;gap:8px;cursor:pointer"><input type="checkbox" id="${f.id}" ${f.checked ? 'checked' : ''}> ${API.esc(f.checkLabel || '')}</label>`;
      } else {
        html += `<input type="${f.type || 'text'}" id="${f.id}" value="${f.value || ''}" placeholder="${f.placeholder || ''}" ${f.required ? 'required' : ''} ${f.disabled ? 'disabled' : ''}>`;
      }
      html += '</div>';
    });
    html += '</div></div>';
    return html;
  }

  // ============================================================
  // KPI GRID (wrapper responsivo para kpiCards)
  // ============================================================
  function kpiGrid(items, opts = {}) {
    const cls = opts.compact ? 'kpi-grid-compact' : 'kpi-grid';
    return `<div class="${cls}">${items.map(i => kpiCard(i.value, i.label, i)).join('')}</div>`;
  }

  // ============================================================
  // CONFIRM DIALOG
  // ============================================================
  function confirmDialog(config) {
    const { title = 'Confirmar', message, confirmLabel = 'Confirmar', cancelLabel = 'Cancelar', onConfirm, danger } = config;
    const id = 'confirm-' + Date.now();
    const overlay = document.createElement('div');
    overlay.className = 'confirm-overlay';
    overlay.id = id;
    overlay.innerHTML = `<div class="confirm-dialog">
      <h3 class="confirm-dialog-title">${API.esc(title)}</h3>
      <p class="confirm-dialog-message">${API.esc(message)}</p>
      <div class="confirm-dialog-actions">
        <button class="btn btn-outline" onclick="document.getElementById('${id}').remove()">${API.esc(cancelLabel)}</button>
        <button class="btn ${danger ? 'btn-danger' : 'btn-primary'}" id="${id}-ok">${API.esc(confirmLabel)}</button>
      </div>
    </div>`;
    document.body.appendChild(overlay);
    overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
    document.getElementById(id + '-ok').addEventListener('click', () => { overlay.remove(); if (onConfirm) onConfirm(); });
  }

  // ============================================================
  // STATUS BADGE (expanded)
  // ============================================================
  function statusBadge(text, status) {
    const map = {
      ativo: 'badge-success', success: 'badge-success', ok: 'badge-success', valido: 'badge-success',
      pendente: 'badge-warning', warning: 'badge-warning', vencer: 'badge-warning',
      vencido: 'badge-danger', danger: 'badge-danger', erro: 'badge-danger', bloqueado: 'badge-danger',
      info: 'badge-info', desligado: 'badge-muted', inativo: 'badge-muted', encerrada: 'badge-muted'
    };
    const cls = map[status] || map[text?.toLowerCase()] || 'badge-default';
    return `<span class="badge ${cls}">${API.esc(text)}</span>`;
  }

  // Inject animation CSS
  if (typeof document !== 'undefined' && !document.getElementById('ui-comp-styles')) {
    const s = document.createElement('style');
    s.id = 'ui-comp-styles';
    s.textContent = '@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}} @keyframes spin{to{transform:rotate(360deg)}} @keyframes fadeIn{from{opacity:0}to{opacity:1}}';
    document.head.appendChild(s);
  }

  return {
    // Legacy (backward compat)
    table, kpiCard, skeleton, spinner, loadingCenter, emptyState,
    toast, badge, modal, openModal, closeModal, progressBar, card,
    // New ERP components (config-driven)
    pageHeader, actionBar, filterBar, dataTable, pagination,
    formSection, kpiGrid, confirmDialog, statusBadge
  };
})();
