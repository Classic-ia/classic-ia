/**
 * api.js v2 — Camada padrao de consumo backend
 * CONTRATO: Toda resposta segue { ok, data, error, status }
 * NUNCA retorna array/objeto cru
 */
const API = (() => {
  // ============================================================
  // AUTH
  // ============================================================
  function getToken() {
    const raw = localStorage.getItem('rh_sess_v2');
    if (!raw) return SB_KEY;
    try { return JSON.parse(raw).access_token || SB_KEY; } catch { return SB_KEY; }
  }

  function hdrs(extra = {}) {
    return { 'Content-Type': 'application/json', 'apikey': SB_KEY, 'Authorization': 'Bearer ' + getToken(), ...extra };
  }

  // ============================================================
  // RESPONSE HANDLER (contrato unico)
  // ============================================================
  function handleResponse(res, raw) {
    if (res.status === 401) {
      logEvent('SESSION_EXPIRED', { status: 401 });
      window.location.href = 'login.html';
      return { ok: false, data: null, error: 'Sessao expirada', status: 401 };
    }
    if (res.status === 403) {
      logEvent('ACCESS_DENIED', { status: 403 });
      return { ok: false, data: null, error: 'Acesso negado', status: 403 };
    }
    if (!res.ok) {
      return { ok: false, data: null, error: `Erro ${res.status}`, status: res.status };
    }
    return { ok: true, data: raw, error: null, status: res.status };
  }

  function handleError(endpoint, params, err) {
    const msg = err.name === 'AbortError' ? 'Timeout' : (err.message || 'Erro de rede');
    console.error('[API]', { endpoint, params, error: msg, timestamp: new Date().toISOString() });
    logEvent('API_ERROR', { endpoint, params, error: msg });
    return { ok: false, data: null, error: msg, status: 0 };
  }

  // ============================================================
  // GET — supports { head: true } for server-side count via Prefer: count=exact
  // ============================================================
  async function get(endpoint, opts = {}) {
    try {
      const url = endpoint.startsWith('http') ? endpoint : `${SB_URL}/rest/v1/${endpoint}`;
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 15000);

      if (opts.head) {
        // HEAD request with count=exact — returns count from content-range header
        const res = await fetch(url, {
          method: 'HEAD',
          headers: hdrs({ 'Prefer': 'count=exact' }),
          signal: ctrl.signal
        });
        clearTimeout(tid);
        if (!res.ok) return { ok: false, data: null, count: null, error: `Erro ${res.status}`, status: res.status };
        // Parse content-range: "0-0/25288" → 25288
        const range = res.headers.get('content-range') || '';
        const match = range.match(/\/(\d+)/);
        const count = match ? parseInt(match[1], 10) : null;
        return { ok: true, data: null, count, error: null, status: res.status };
      }

      // Normal GET
      const res = await fetch(url, { headers: hdrs(), signal: ctrl.signal });
      clearTimeout(tid);
      if (!res.ok) return handleResponse(res, null);
      const raw = await res.json();
      return { ok: true, data: raw || [], error: null, status: res.status };
    } catch (e) { return handleError(endpoint, {}, e); }
  }

  // ============================================================
  // RPC
  // ============================================================
  async function rpc(fnName, params = {}) {
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 30000);
      const res = await fetch(`${SB_URL}/rest/v1/rpc/${fnName}`, {
        method: 'POST', headers: hdrs(), body: JSON.stringify(params), signal: ctrl.signal
      });
      clearTimeout(tid);
      if (!res.ok) return handleResponse(res, null);
      const raw = await res.json();
      const data = Array.isArray(raw) ? (raw[0] || null) : raw;
      return { ok: true, data, error: null, status: res.status };
    } catch (e) { return handleError(`rpc/${fnName}`, params, e); }
  }

  // ============================================================
  // POST
  // ============================================================
  async function post(table, payload, prefer = 'return=minimal') {
    try {
      const res = await fetch(`${SB_URL}/rest/v1/${table}`, {
        method: 'POST', headers: hdrs({ 'Prefer': prefer }), body: JSON.stringify(payload)
      });
      if (!res.ok) return handleResponse(res, null);
      const data = prefer.includes('representation') ? await res.json() : null;
      return { ok: true, data, error: null, status: res.status };
    } catch (e) { return handleError(`POST ${table}`, payload, e); }
  }

  // ============================================================
  // PATCH
  // ============================================================
  async function patch(endpoint, payload) {
    try {
      const url = endpoint.startsWith('http') ? endpoint : `${SB_URL}/rest/v1/${endpoint}`;
      const res = await fetch(url, {
        method: 'PATCH', headers: hdrs({ 'Prefer': 'return=minimal' }), body: JSON.stringify(payload)
      });
      if (!res.ok) return handleResponse(res, null);
      return { ok: true, data: null, error: null, status: res.status };
    } catch (e) { return handleError(`PATCH ${endpoint}`, payload, e); }
  }

  // ============================================================
  // DELETE
  // ============================================================
  async function del(endpoint) {
    try {
      const url = endpoint.startsWith('http') ? endpoint : `${SB_URL}/rest/v1/${endpoint}`;
      const res = await fetch(url, { method: 'DELETE', headers: hdrs() });
      if (!res.ok) return handleResponse(res, null);
      return { ok: true, data: null, error: null, status: res.status };
    } catch (e) { return handleError(`DELETE ${endpoint}`, {}, e); }
  }

  // ============================================================
  // FORMATACAO
  // ============================================================
  function fmtNum(v, dec) {
    if (v == null || v === '') return '\u2014';
    const n = Number(v);
    if (isNaN(n)) return String(v);
    return n.toLocaleString('pt-BR', dec != null ? { minimumFractionDigits: dec, maximumFractionDigits: dec } : {});
  }
  function fmtMoeda(v) { return v == null ? '\u2014' : 'R$ ' + fmtNum(v, 2); }
  function fmtData(d) {
    if (!d) return '\u2014';
    try { return new Date(d + (d.length === 10 ? 'T00:00' : '')).toLocaleDateString('pt-BR'); } catch { return d; }
  }
  function fmtPct(v) { return v == null ? '\u2014' : fmtNum(v, 1) + '%'; }
  function fmtCPF(cpf) {
    if (!cpf || cpf.length < 11) return cpf || '\u2014';
    const c = cpf.replace(/\D/g, '').padStart(11, '0');
    return `${c.slice(0,3)}.${c.slice(3,6)}.${c.slice(6,9)}-${c.slice(9)}`;
  }
  function fmtTempo(dias) {
    if (!dias || dias < 0) return '\u2014';
    if (dias < 30) return dias + 'd';
    const m = Math.floor(dias / 30.44);
    if (m < 12) return m + 'm';
    const a = Math.floor(m / 12), rm = m % 12;
    return rm ? `${a}a ${rm}m` : `${a}a`;
  }
  function esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = String(s); return d.innerHTML; }

  // ============================================================
  // OBSERVABILIDADE
  // ============================================================
  const _events = [];
  function logEvent(type, payload = {}) {
    const evt = { type, ...payload, timestamp: new Date().toISOString(), page: window.location.pathname.split('/').pop() };
    _events.push(evt);
    if (_events.length > 100) _events.shift();
    if (type === 'API_ERROR' || type === 'UI_ERROR') console.warn('[Event]', evt);
  }
  function getEvents() { return _events; }

  // Global error handlers
  if (typeof window !== 'undefined') {
    window.addEventListener('error', e => logEvent('UI_ERROR', { message: e.message, filename: e.filename, line: e.lineno }));
    window.addEventListener('unhandledrejection', e => logEvent('UI_ERROR', { message: e.reason?.message || String(e.reason) }));
  }

  // ============================================================
  // PERMISSOES
  // ============================================================
  const PERMISSIONS = {
    dados_sensiveis: ['administrador', 'rh'],
    salario: ['administrador', 'rh', 'gestor_confianca'],
    score_individual: ['administrador', 'rh', 'gestor_confianca'],
    atestado_individual: ['administrador', 'rh'],
    cid: ['administrador', 'rh'],
    editar_funcionario: ['administrador', 'rh'],
    registrar_acompanhamento: ['administrador', 'rh', 'gestor_confianca'],
    ver_integracao: ['administrador', 'rh'],
    ver_plano_acao: ['administrador', 'rh', 'gestor_confianca'],
    ver_motor_decisao: ['administrador', 'rh'],
    ver_auditoria: ['administrador', 'rh'],
  };

  let _currentPerfil = null;
  function setPerfil(p) { _currentPerfil = p; }
  function getPerfil() { return _currentPerfil; }

  function canAccess(recurso, acao) {
    if (!_currentPerfil) return false;
    if (_currentPerfil === 'administrador') return true;
    const key = acao ? `${recurso}_${acao}` : recurso;
    const allowed = PERMISSIONS[key];
    if (!allowed) return true;
    return allowed.includes(_currentPerfil);
  }

  // ============================================================
  // VIEWS PADRAO
  // ============================================================
  const VIEWS = {
    funcionarios_ativos: 'vw_funcionario_base?status=eq.ativo&order=nome',
    funcionarios_equipe: 'vw_funcionario_base?status=in.(ativo,afastado,ferias,experiencia)&order=nome',
    funcionarios_todos: 'vw_funcionario_base?order=nome',
    setores: 'rh_setores?ativo=eq.true&order=nome',
    cargos: 'rh_cargos?order=nome',
    catalogo_epi: 'sst_catalogo_epi?ativo=eq.true&order=descricao',
  };

  return {
    get, rpc, post, patch, del,
    fmtNum, fmtMoeda, fmtData, fmtPct, fmtCPF, fmtTempo, esc,
    logEvent, getEvents,
    canAccess, setPerfil, getPerfil,
    VIEWS,
    handleResponse, handleError
  };
})();
