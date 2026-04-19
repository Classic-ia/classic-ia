/**
 * api.js — Trans Anacleto
 * Camada padrao de consumo backend (Supabase PostgREST/RPC).
 * CONTRATO: Toda resposta segue { ok, data, error, status }.
 * NUNCA retorna array/objeto cru.
 */
const API = (() => {
  const STORE_KEY = (typeof TRANS_CONFIG !== 'undefined' && TRANS_CONFIG.SESSION_KEY) || 'trans_sess_v1';

  // ============================================================
  // AUTH
  // ============================================================
  function getToken() {
    const raw = localStorage.getItem(STORE_KEY);
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
  // GET
  // ============================================================
  async function get(endpoint) {
    try {
      const url = endpoint.startsWith('http') ? endpoint : `${SB_URL}/rest/v1/${endpoint}`;
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 15000);
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
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (isNaN(n)) return String(v);
    return n.toLocaleString('pt-BR', dec != null ? { minimumFractionDigits: dec, maximumFractionDigits: dec } : {});
  }
  function fmtMoeda(v) { return v == null ? '—' : 'R$ ' + fmtNum(v, 2); }
  function fmtData(d) {
    if (!d) return '—';
    try { return new Date(d + (d.length === 10 ? 'T00:00' : '')).toLocaleDateString('pt-BR'); } catch { return d; }
  }
  function fmtDataHora(d) {
    if (!d) return '—';
    try { return new Date(d).toLocaleString('pt-BR'); } catch { return d; }
  }
  function fmtPct(v) { return v == null ? '—' : fmtNum(v, 1) + '%'; }
  function fmtKm(v) { return v == null ? '—' : fmtNum(v, 0) + ' km'; }
  function fmtLitros(v) { return v == null ? '—' : fmtNum(v, 2) + ' L'; }
  function fmtCNPJ(v) {
    if (!v) return '—';
    const c = String(v).replace(/\D/g, '').padStart(14, '0');
    return `${c.slice(0,2)}.${c.slice(2,5)}.${c.slice(5,8)}/${c.slice(8,12)}-${c.slice(12)}`;
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

  if (typeof window !== 'undefined') {
    window.addEventListener('error', e => logEvent('UI_ERROR', { message: e.message, filename: e.filename, line: e.lineno }));
    window.addEventListener('unhandledrejection', e => logEvent('UI_ERROR', { message: e.reason?.message || String(e.reason) }));
  }

  // ============================================================
  // PERMISSOES (RBAC Trans Anacleto)
  //   admin       — full CRUD
  //   gestor      — SELECT tudo; INSERT/UPDATE operacional; sem DELETE
  //   almoxarife  — CRUD estoque/pneus/etiquetas/tanque; SELECT em veic/motoristas
  //   motorista   — SELECT proprio; INSERT abastecimentos
  // ============================================================
  const PERMISSIONS = {
    editar_veiculo:          ['admin', 'gestor'],
    editar_motorista:        ['admin', 'gestor'],
    editar_produto:          ['admin', 'almoxarife'],
    entrada_estoque:         ['admin', 'almoxarife'],
    saida_estoque:           ['admin', 'almoxarife', 'gestor'],
    alterar_status_lote:     ['admin'],
    override_bloqueio_lote:  ['admin'],
    conversao_estoque:       ['admin', 'almoxarife'],
    instalar_pneu:           ['admin', 'almoxarife', 'gestor'],
    retirar_pneu:            ['admin', 'almoxarife', 'gestor'],
    sucatar_pneu:            ['admin', 'almoxarife'],
    abrir_manutencao:        ['admin', 'almoxarife', 'gestor'],
    finalizar_manutencao:    ['admin', 'almoxarife', 'gestor'],
    cancelar_manutencao:     ['admin'],
    editar_plano_manutencao: ['admin', 'gestor'],
    registrar_abastecimento: ['admin', 'almoxarife', 'motorista'],
    editar_tanque:           ['admin', 'almoxarife'],
    importar_nfe:            ['admin', 'almoxarife'],
    ver_custos:              ['admin', 'gestor'],
    editar_config:           ['admin'],
    sync_tms:                ['admin'],
    editar_geofences:        ['admin', 'gestor'],
    ver_telemetria_raw:      ['admin'],
    editar_onixsat_config:   ['admin'],
    editar_usuario:          ['admin'],
  };

  let _currentPerfil = null;
  function setPerfil(p) { _currentPerfil = p; }
  function getPerfil() { return _currentPerfil; }

  function canAccess(recurso, acao) {
    if (!_currentPerfil) return false;
    if (_currentPerfil === 'admin') return true;
    const key = acao ? `${recurso}_${acao}` : recurso;
    const allowed = PERMISSIONS[key];
    if (!allowed) return true;
    return allowed.includes(_currentPerfil);
  }

  return {
    get, rpc, post, patch, del,
    fmtNum, fmtMoeda, fmtData, fmtDataHora, fmtPct, fmtKm, fmtLitros, fmtCNPJ, esc,
    logEvent, getEvents,
    canAccess, setPerfil, getPerfil,
    handleResponse, handleError
  };
})();
