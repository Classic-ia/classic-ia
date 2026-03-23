/**
 * cq_toast.js — Classic CQ v2
 * Sistema global de notificações toast + tratamento de erros
 *
 * Uso:
 *   <script src="cq_toast.js"></script>
 *   CQToast.success('Inspeção salva com sucesso');
 *   CQToast.error('Falha ao carregar dados');
 *   CQToast.warn('Fornecedor próximo do limite de %C');
 *   CQToast.info('Sync em andamento...');
 *
 * Intercepta erros globais e fetch failures automaticamente.
 */

const CQToast = (function () {

  // ── CONFIG ────────────────────────────────────────────────
  const DURACAO_MS = {
    success: 3000,
    error:   6000,
    warn:    5000,
    info:    4000,
  };

  const ESTILOS = {
    success: { bg: '#27ae6018', border: '#27ae60', cor: '#27ae60', icon: '\u2705' },
    error:   { bg: '#e74c3c18', border: '#e74c3c', cor: '#e74c3c', icon: '\u274C' },
    warn:    { bg: '#f39c1218', border: '#f39c12', cor: '#f39c12', icon: '\u26A0\uFE0F' },
    info:    { bg: '#3498db18', border: '#3498db', cor: '#3498db', icon: '\u2139\uFE0F' },
  };

  let _container = null;
  let _count = 0;

  // ── CONTAINER ─────────────────────────────────────────────
  function _getContainer() {
    if (_container && document.body.contains(_container)) return _container;
    _container = document.createElement('div');
    _container.id = 'cq-toast-container';
    _container.style.cssText = `
      position:fixed;top:16px;right:16px;z-index:999999;
      display:flex;flex-direction:column;gap:8px;
      max-width:420px;width:calc(100vw - 32px);
      pointer-events:none;
    `;
    document.body.appendChild(_container);
    return _container;
  }

  // ── MOSTRAR TOAST ─────────────────────────────────────────
  function _show(tipo, mensagem, opts = {}) {
    const cfg = ESTILOS[tipo] || ESTILOS.info;
    const duracao = opts.duracao || DURACAO_MS[tipo] || 4000;
    const id = `cq-toast-${++_count}`;

    const el = document.createElement('div');
    el.id = id;
    el.style.cssText = `
      background:#0d1b2a;border:1px solid ${cfg.border};border-radius:10px;
      padding:12px 16px;display:flex;align-items:flex-start;gap:10px;
      box-shadow:0 4px 20px rgba(0,0,0,.5);pointer-events:auto;
      font-family:'DM Sans',sans-serif;opacity:0;
      transform:translateX(40px);transition:all .3s ease;
      max-width:100%;cursor:pointer;
    `;

    el.innerHTML = `
      <span style="font-size:18px;flex-shrink:0;line-height:1">${cfg.icon}</span>
      <div style="flex:1;min-width:0;">
        <div style="font-size:13px;color:${cfg.cor};font-weight:600;line-height:1.3;word-break:break-word;">
          ${_escape(mensagem)}
        </div>
        ${opts.detalhe ? `<div style="font-size:11px;color:#5d7f95;margin-top:4px;line-height:1.3;">${_escape(opts.detalhe)}</div>` : ''}
      </div>
      <button onclick="CQToast.dismiss('${id}')" style="
        background:none;border:none;color:#5d7f95;cursor:pointer;
        font-size:14px;padding:0;flex-shrink:0;line-height:1;
      ">\u2715</button>
    `;

    el.onclick = (e) => {
      if (e.target.tagName !== 'BUTTON') _dismiss(id);
    };

    _getContainer().appendChild(el);

    // Animar entrada
    requestAnimationFrame(() => {
      el.style.opacity = '1';
      el.style.transform = 'translateX(0)';
    });

    // Auto-dismiss
    if (duracao > 0) {
      setTimeout(() => _dismiss(id), duracao);
    }

    return id;
  }

  // ── DISMISS ───────────────────────────────────────────────
  function _dismiss(id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.opacity = '0';
    el.style.transform = 'translateX(40px)';
    setTimeout(() => el.remove(), 300);
  }

  // ── ESCAPE HTML ───────────────────────────────────────────
  function _escape(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // ── INTERCEPTAR ERROS GLOBAIS ─────────────────────────────
  function _initGlobalHandlers() {
    // Erros JS não capturados
    window.addEventListener('error', (e) => {
      if (e.message === 'ResizeObserver loop limit exceeded') return;
      _show('error', 'Erro inesperado', {
        detalhe: e.message || 'Erro desconhecido',
        duracao: 8000,
      });
    });

    // Promises não capturadas
    window.addEventListener('unhandledrejection', (e) => {
      const msg = e.reason?.message || e.reason || 'Operação falhou';
      // Não mostrar para erros de navegação (redirect)
      if (typeof msg === 'string' && msg.includes('navigation')) return;
      _show('error', 'Falha na operação', {
        detalhe: typeof msg === 'string' ? msg : 'Erro desconhecido',
        duracao: 6000,
      });
    });
  }

  // ── WRAPPER PARA FETCH COM TOAST ──────────────────────────
  /**
   * Wrapper que exibe toast em caso de erro HTTP.
   * @param {string} url
   * @param {RequestInit} opts
   * @param {string} contexto — ex: 'Carregar inspeções'
   * @returns {Promise<Response>}
   */
  async function fetchComToast(url, opts = {}, contexto = '') {
    try {
      const r = await fetch(url, opts);
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        const msg = body.message || body.error || `HTTP ${r.status}`;
        _show('error', contexto || 'Erro na requisição', { detalhe: msg });
        throw new Error(msg);
      }
      return r;
    } catch (e) {
      if (e.name === 'TypeError' && e.message.includes('fetch')) {
        _show('error', 'Sem conexão', {
          detalhe: 'Verifique sua conexão com a internet e tente novamente.',
          duracao: 8000,
        });
      }
      throw e;
    }
  }

  // ── INIT ──────────────────────────────────────────────────
  if (typeof window !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _initGlobalHandlers);
    } else {
      _initGlobalHandlers();
    }
  }

  // ── API PÚBLICA ───────────────────────────────────────────
  return {
    success: (msg, opts) => _show('success', msg, opts),
    error:   (msg, opts) => _show('error',   msg, opts),
    warn:    (msg, opts) => _show('warn',    msg, opts),
    info:    (msg, opts) => _show('info',    msg, opts),
    dismiss: _dismiss,
    fetch:   fetchComToast,
  };

})();
