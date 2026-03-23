/**
 * cq_cache.js — Classic CQ v2
 * Cache + Paginação reutilizável para dados Supabase
 *
 * Uso:
 *   <script src="config.js"></script>
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_cache.js"></script>
 *
 *   // Cache de cadastros (TTL 15 min)
 *   const fornecedores = await CQCache.get('cadastros_atak', {
 *     query: 'tipo=eq.fornecedor&ativo=eq.true&select=codigo,nome,apelido,uf',
 *     ttl: 15 * 60 * 1000,
 *   });
 *
 *   // Paginação
 *   const { dados, total, pagina, totalPaginas } = await CQCache.paginar(
 *     'registros_cq_inspecao', { filtros: 'status_final=eq.aprovado', porPagina: 20, pagina: 1 }
 *   );
 */

const CQCache = (function () {

  // ── STORAGE ───────────────────────────────────────────────
  const _cache = {};
  const DEFAULT_TTL = 15 * 60 * 1000; // 15 minutos

  // ── HELPERS ───────────────────────────────────────────────
  function _sbUrl() { return CQ_CONFIG.SB_URL; }

  function _headers() {
    const authToken = typeof CQAuth !== 'undefined' ? CQAuth.getAuthToken() : null;
    return {
      'Content-Type': 'application/json',
      'apikey': CQ_CONFIG.SB_KEY,
      'Authorization': 'Bearer ' + (authToken || CQ_CONFIG.SB_KEY),
    };
  }

  function _cacheKey(tabela, query) {
    return `${tabela}::${query || ''}`;
  }

  // ── GET COM CACHE ─────────────────────────────────────────
  /**
   * Busca dados com cache em memória (sessionStorage como fallback).
   * @param {string} tabela — nome da tabela/view Supabase
   * @param {Object} opts
   * @param {string} opts.query — query string PostgREST (sem ?)
   * @param {number} opts.ttl — tempo de vida em ms (default 15 min)
   * @param {boolean} opts.forceRefresh — ignorar cache
   * @param {boolean} opts.persistir — salvar em sessionStorage
   * @returns {Promise<Array>}
   */
  async function get(tabela, opts = {}) {
    const query = opts.query || 'select=*';
    const ttl = opts.ttl || DEFAULT_TTL;
    const key = _cacheKey(tabela, query);

    // Checar cache em memória
    if (!opts.forceRefresh && _cache[key]) {
      const entry = _cache[key];
      if (Date.now() - entry.ts < ttl) {
        return entry.data;
      }
    }

    // Checar sessionStorage
    if (!opts.forceRefresh && opts.persistir !== false) {
      try {
        const stored = sessionStorage.getItem(`cqc_${key}`);
        if (stored) {
          const entry = JSON.parse(stored);
          if (Date.now() - entry.ts < ttl) {
            _cache[key] = entry;
            return entry.data;
          }
        }
      } catch { /* sessionStorage indisponível */ }
    }

    // Fetch do Supabase
    const url = `${_sbUrl()}/rest/v1/${tabela}?${query}`;
    const r = await fetch(url, { headers: _headers() });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.message || `HTTP ${r.status}`);
    }
    const data = await r.json();

    // Salvar em cache
    const entry = { data, ts: Date.now() };
    _cache[key] = entry;

    if (opts.persistir !== false) {
      try {
        sessionStorage.setItem(`cqc_${key}`, JSON.stringify(entry));
      } catch { /* quota excedida */ }
    }

    return data;
  }

  // ── PAGINAR ───────────────────────────────────────────────
  /**
   * Busca paginada com contagem total.
   * @param {string} tabela — nome da tabela/view
   * @param {Object} opts
   * @param {string} opts.filtros — filtros PostgREST (ex: 'status=eq.aberta&fornecedor=ilike.*ABC*')
   * @param {string} opts.select — campos (default '*')
   * @param {string} opts.order — ordenação (default 'criado_em.desc')
   * @param {number} opts.pagina — página atual (1-based, default 1)
   * @param {number} opts.porPagina — itens por página (default 20)
   * @returns {Promise<{ dados: Array, total: number, pagina: number, totalPaginas: number }>}
   */
  async function paginar(tabela, opts = {}) {
    const select = opts.select || '*';
    const order = opts.order || 'criado_em.desc';
    const pagina = Math.max(1, opts.pagina || 1);
    const porPagina = opts.porPagina || 20;
    const offset = (pagina - 1) * porPagina;

    let qs = `select=${select}&order=${order}&limit=${porPagina}&offset=${offset}`;
    if (opts.filtros) qs += `&${opts.filtros}`;

    const url = `${_sbUrl()}/rest/v1/${tabela}?${qs}`;
    const r = await fetch(url, {
      headers: {
        ..._headers(),
        'Prefer': 'count=exact',
        'Range-Unit': 'items',
        'Range': `${offset}-${offset + porPagina - 1}`,
      },
    });

    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.message || `HTTP ${r.status}`);
    }

    // Extrair total do header Content-Range
    const contentRange = r.headers.get('content-range');
    let total = 0;
    if (contentRange) {
      const match = contentRange.match(/\/(\d+)/);
      if (match) total = parseInt(match[1], 10);
    }

    const dados = await r.json();

    return {
      dados,
      total,
      pagina,
      porPagina,
      totalPaginas: Math.ceil(total / porPagina) || 1,
    };
  }

  // ── RENDERIZAR PAGINAÇÃO ──────────────────────────────────
  /**
   * Gera HTML de controles de paginação.
   * @param {{ pagina, totalPaginas, total }} resultado — retorno de paginar()
   * @param {Function} onPagina — callback(novaPagina)
   * @returns {string} HTML
   */
  function renderPaginacao(resultado, callbackName) {
    const { pagina, totalPaginas, total } = resultado;
    if (totalPaginas <= 1) {
      return `<div style="font-size:12px;color:#5d7f95;padding:8px 0;">${total} registro${total !== 1 ? 's' : ''}</div>`;
    }

    let html = `<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 0;font-family:'DM Sans',sans-serif;">`;

    html += `<span style="font-size:12px;color:#5d7f95;">${total} registros \u2022 P\u00e1gina ${pagina} de ${totalPaginas}</span>`;

    html += `<div style="display:flex;gap:4px;">`;

    // Anterior
    html += `<button onclick="${callbackName}(${pagina - 1})" ${pagina <= 1 ? 'disabled' : ''} style="
      padding:6px 12px;border-radius:6px;font-size:12px;cursor:${pagina <= 1 ? 'default' : 'pointer'};
      background:${pagina <= 1 ? '#0d1b2a' : '#1a3a52'};color:${pagina <= 1 ? '#3d5a6e' : '#e8f0f5'};
      border:1px solid ${pagina <= 1 ? '#1a3a52' : '#2E6B8A'};
    ">\u2190 Anterior</button>`;

    // Páginas
    const inicio = Math.max(1, pagina - 2);
    const fim = Math.min(totalPaginas, pagina + 2);
    for (let i = inicio; i <= fim; i++) {
      const ativo = i === pagina;
      html += `<button onclick="${callbackName}(${i})" style="
        padding:6px 10px;border-radius:6px;font-size:12px;cursor:pointer;min-width:32px;
        background:${ativo ? '#2E6B8A' : '#0d1b2a'};color:${ativo ? '#fff' : '#5d7f95'};
        border:1px solid ${ativo ? '#2E6B8A' : '#1a3a52'};font-weight:${ativo ? '700' : '400'};
      ">${i}</button>`;
    }

    // Próximo
    html += `<button onclick="${callbackName}(${pagina + 1})" ${pagina >= totalPaginas ? 'disabled' : ''} style="
      padding:6px 12px;border-radius:6px;font-size:12px;cursor:${pagina >= totalPaginas ? 'default' : 'pointer'};
      background:${pagina >= totalPaginas ? '#0d1b2a' : '#1a3a52'};color:${pagina >= totalPaginas ? '#3d5a6e' : '#e8f0f5'};
      border:1px solid ${pagina >= totalPaginas ? '#1a3a52' : '#2E6B8A'};
    ">Pr\u00f3ximo \u2192</button>`;

    html += `</div></div>`;
    return html;
  }

  // ── INVALIDAR CACHE ───────────────────────────────────────
  function invalidar(tabela) {
    Object.keys(_cache).forEach(key => {
      if (key.startsWith(tabela + '::')) delete _cache[key];
    });
    try {
      Object.keys(sessionStorage).forEach(key => {
        if (key.startsWith(`cqc_${tabela}::`)) sessionStorage.removeItem(key);
      });
    } catch { /* */ }
  }

  function invalidarTudo() {
    Object.keys(_cache).forEach(key => delete _cache[key]);
    try {
      Object.keys(sessionStorage).forEach(key => {
        if (key.startsWith('cqc_')) sessionStorage.removeItem(key);
      });
    } catch { /* */ }
  }

  // ── API PÚBLICA ───────────────────────────────────────────
  return {
    get,
    paginar,
    renderPaginacao,
    invalidar,
    invalidarTudo,
  };

})();
