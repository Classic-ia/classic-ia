/**
 * api.js — JR Terraplanagem
 * Camada padrao de consumo backend (Supabase PostgREST/RPC).
 * CONTRATO: Toda resposta segue { ok, data, error, status }.
 */
const API = (() => {
  const STORE_KEY = (typeof JR_CONFIG !== 'undefined' && JR_CONFIG.SESSION_KEY) || 'jr_sess_v1';

  function getToken() {
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) return SB_KEY;
    try { return JSON.parse(raw).access_token || SB_KEY; } catch { return SB_KEY; }
  }

  function hdrs(extra = {}) {
    return { 'Content-Type': 'application/json', 'apikey': SB_KEY, 'Authorization': 'Bearer ' + getToken(), ...extra };
  }

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

  async function rpc(fnName, params = {}) {
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 60000);
      const res = await fetch(`${SB_URL}/rest/v1/rpc/${fnName}`, {
        method: 'POST', headers: hdrs(), body: JSON.stringify(params), signal: ctrl.signal
      });
      clearTimeout(tid);
      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        return { ok: false, data: null, error: errText || `Erro ${res.status}`, status: res.status };
      }
      const raw = await res.json();
      const data = Array.isArray(raw) ? (raw[0] || null) : raw;
      return { ok: true, data, error: null, status: res.status };
    } catch (e) { return handleError(`rpc/${fnName}`, params, e); }
  }

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

  async function del(endpoint) {
    try {
      const url = endpoint.startsWith('http') ? endpoint : `${SB_URL}/rest/v1/${endpoint}`;
      const res = await fetch(url, { method: 'DELETE', headers: hdrs() });
      if (!res.ok) return handleResponse(res, null);
      return { ok: true, data: null, error: null, status: res.status };
    } catch (e) { return handleError(`DELETE ${endpoint}`, {}, e); }
  }

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
  function fmtHorimetro(v) { return v == null ? '—' : fmtNum(v, 1) + ' h'; }
  function fmtKm(v) { return v == null ? '—' : fmtNum(v, 0) + ' km'; }
  function fmtLitros(v) { return v == null ? '—' : fmtNum(v, 2) + ' L'; }
  function fmtCNPJ(v) {
    if (!v) return '—';
    const c = String(v).replace(/\D/g, '');
    if (c.length === 11) return `${c.slice(0,3)}.${c.slice(3,6)}.${c.slice(6,9)}-${c.slice(9)}`;
    if (c.length === 14) return `${c.slice(0,2)}.${c.slice(2,5)}.${c.slice(5,8)}/${c.slice(8,12)}-${c.slice(12)}`;
    return v;
  }
  function esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = String(s); return d.innerHTML; }

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
  // PERMISSOES (RBAC JR)
  //   admin       - full CRUD
  //   gestor      - SELECT tudo, INSERT/UPDATE operacional
  //   operador    - apontamento OS/abastecimento/manutencao no campo
  //   financeiro  - pagamentos, despesas, relatorios
  // ============================================================
  const PERMISSIONS = {
    editar_cadastros:         ['admin', 'gestor'],
    editar_maquina:           ['admin', 'gestor'],
    editar_veiculo:           ['admin', 'gestor'],
    abrir_os:                 ['admin', 'gestor'],
    registrar_periodo:        ['admin', 'gestor', 'operador'],
    finalizar_os:             ['admin', 'gestor'],
    registrar_pagamento:      ['admin', 'gestor', 'financeiro'],
    registrar_despesa:        ['admin', 'gestor', 'financeiro', 'operador'],
    registrar_deslocamento:   ['admin', 'gestor', 'operador'],
    abrir_manutencao:         ['admin', 'gestor', 'operador'],
    finalizar_manutencao:     ['admin', 'gestor'],
    registrar_abastecimento:  ['admin', 'gestor', 'operador'],
    editar_tanque:            ['admin', 'gestor'],
    editar_estoque:           ['admin', 'gestor'],
    ver_relatorios:           ['admin', 'gestor', 'financeiro'],
    ver_custos:               ['admin', 'gestor', 'financeiro'],
    editar_plano_manutencao:  ['admin', 'gestor'],
    editar_config:            ['admin'],
    importar_legacy:          ['admin'],
    editar_usuario:           ['admin'],
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
    fmtNum, fmtMoeda, fmtData, fmtDataHora, fmtPct, fmtHorimetro, fmtKm, fmtLitros, fmtCNPJ, esc,
    logEvent, getEvents,
    canAccess, setPerfil, getPerfil,
    handleResponse, handleError
  };
})();
