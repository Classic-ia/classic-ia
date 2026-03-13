/**
 * cq_auth.js — Classic CQ v2
 * Módulo central de autenticação, sessão e auditoria
 *
 * Uso em qualquer página:
 *   <script src="cq_auth.js"></script>
 *   const user = await CQAuth.init({ perfisPermitidos: ['gestor','analisador'] });
 */

const CQAuth = (function () {

  // ── CONFIGURAÇÃO ──────────────────────────────────────────
  const SB_URL = 'https://nvqxsulntpftcwtkjedu.supabase.co';
  const SB_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im52cXhzdWxudHBmdGN3dGtqZWR1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMzNTIwODksImV4cCI6MjA4ODkyODA4OX0.JvahlmaqJUzd2FQp-27uADe3mL7Wccg68PA2_3YWRhw';

  const TIMEOUT_MS = 60 * 60 * 1000;    // 1 hora de inatividade → expira
  const WARN_MS    = 5  * 60 * 1000;    // aviso 5 min antes
  const BEAT_MS    = 60 * 1000;         // heartbeat a cada 1 min
  const STORE_KEY  = 'cq_sess_v2';

  // Planta Classic Couros — Jardim Alegre/PR
  // Ajuste as coordenadas conforme o endereço real
  const PLANTA = { lat: -24.1820, lng: -51.6920, raio_m: 500 };

  // ── ESTADO INTERNO ────────────────────────────────────────
  let _user  = null;   // { id, email, nome, perfil }
  let _token = null;   // session_token (UUID gerado localmente)
  let _geo   = null;   // { latitude, longitude, dentro_planta, distancia_m }
  let _tExp  = null;   // timer expiração
  let _tWarn = null;   // timer aviso
  let _tBeat = null;   // intervalo heartbeat

  // ── SUPABASE HELPERS ──────────────────────────────────────
  const _h = () => ({
    'Content-Type': 'application/json',
    'apikey':       SB_KEY,
    'Authorization':'Bearer ' + SB_KEY,
  });

  async function _get(tabela, qs) {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/${tabela}?${qs}`, { headers: _h() });
      return r.ok ? r.json() : [];
    } catch { return []; }
  }

  async function _post(tabela, dados) {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/${tabela}`, {
        method: 'POST',
        headers: { ..._h(), 'Prefer': 'return=minimal' },
        body: JSON.stringify(dados),
      });
      return r.ok;
    } catch { return false; }
  }

  async function _patch(tabela, qs, dados) {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/${tabela}?${qs}`, {
        method: 'PATCH',
        headers: { ..._h(), 'Prefer': 'return=minimal' },
        body: JSON.stringify(dados),
      });
      return r.ok;
    } catch { return false; }
  }

  // ── GEOLOCALIZAÇÃO ────────────────────────────────────────
  function _dist(lat1, lng1, lat2, lng2) {
    const R = 6371000;
    const dL = (lat2 - lat1) * Math.PI / 180;
    const dG = (lng2 - lng1) * Math.PI / 180;
    const a  = Math.sin(dL/2)**2 +
               Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dG/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  }

  async function _geo_capture() {
    return new Promise(resolve => {
      if (!navigator.geolocation) { resolve(null); return; }
      navigator.geolocation.getCurrentPosition(
        pos => {
          const lat = pos.coords.latitude;
          const lng = pos.coords.longitude;
          const d   = Math.round(_dist(lat, lng, PLANTA.lat, PLANTA.lng));
          _geo = { latitude: lat, longitude: lng, distancia_m: d, dentro_planta: d <= PLANTA.raio_m };
          resolve(_geo);
        },
        () => resolve(null),
        { timeout: 8000, maximumAge: 300000, enableHighAccuracy: true }
      );
    });
  }

  // ── AUDIT LOG ─────────────────────────────────────────────
  async function _log(acao, detalhes = {}) {
    if (!_user) return;
    await _post('cq_audit_log', {
      usuario_id:    _user.id,
      usuario_email: _user.email,
      usuario_nome:  _user.nome,
      acao,
      detalhes:      JSON.stringify(detalhes),
      latitude:      _geo?.latitude  || null,
      longitude:     _geo?.longitude || null,
      dentro_planta: _geo?.dentro_planta ?? null,
      criado_em:     new Date().toISOString(),
    });
  }

  // ── TOKEN ─────────────────────────────────────────────────
  function _mkToken() {
    const arr = new Uint8Array(24);
    crypto.getRandomValues(arr);
    return Array.from(arr).map(b => b.toString(16).padStart(2,'0')).join('');
  }

  // ── SESSÃO ÚNICA ──────────────────────────────────────────
  async function _abrirSessao() {
    // Invalidar sessões antigas do mesmo usuário
    await _patch('cq_sessoes',
      `usuario_id=eq.${_user.id}&ativa=eq.true`,
      { ativa: false, encerrada_em: new Date().toISOString() }
    );
    // Criar nova sessão
    _token = _mkToken();
    const expira = new Date(Date.now() + TIMEOUT_MS).toISOString();
    await _post('cq_sessoes', {
      usuario_id:    _user.id,
      usuario_email: _user.email,
      session_token: _token,
      dispositivo:   navigator.userAgent.slice(0, 150),
      expira_em:     expira,
      ativa:         true,
      criado_em:     new Date().toISOString(),
    });
    // Persistir no localStorage
    localStorage.setItem(STORE_KEY, JSON.stringify({
      token: _token, user: _user, ts: Date.now()
    }));
  }

  async function _validarSessao() {
    const rows = await _get('cq_sessoes',
      `session_token=eq.${_token}&ativa=eq.true`);
    return rows.length > 0;
  }

  async function _heartbeat() {
    if (!_token) return;
    const expira = new Date(Date.now() + TIMEOUT_MS).toISOString();
    await _patch('cq_sessoes',
      `session_token=eq.${_token}&ativa=eq.true`,
      { ultimo_acesso: new Date().toISOString(), expira_em: expira }
    );
  }

  // ── TIMERS ────────────────────────────────────────────────
  function _resetTimers() {
    clearTimeout(_tExp); clearTimeout(_tWarn);
    _tWarn = setTimeout(_mostrarAviso, TIMEOUT_MS - WARN_MS);
    _tExp  = setTimeout(_expirar,      TIMEOUT_MS);
  }

  function _onAtividade() {
    _resetTimers();
    const el = document.getElementById('_cq_warn');
    if (el) el.remove();
  }

  // ── UI: AVISO DE EXPIRAÇÃO ────────────────────────────────
  function _mostrarAviso() {
    if (document.getElementById('_cq_warn')) return;
    const el = document.createElement('div');
    el.id = '_cq_warn';
    el.innerHTML = `
      <div style="
        position:fixed;bottom:20px;left:50%;transform:translateX(-50%);
        background:#0d1b2a;border:2px solid #f39c12;border-radius:12px;
        padding:16px 22px;z-index:99999;max-width:400px;width:90%;
        box-shadow:0 8px 32px rgba(0,0,0,.6);font-family:'DM Sans',sans-serif;
        display:flex;align-items:center;gap:14px;
      ">
        <span style="font-size:28px;">⏰</span>
        <div style="flex:1">
          <div style="font-weight:700;color:#f39c12;font-size:14px;margin-bottom:3px">
            Sessão expira em 5 minutos
          </div>
          <div style="font-size:12px;color:#8bafc4">
            Clique em Continuar para manter o acesso.
          </div>
        </div>
        <button id="_cq_warn_btn" style="
          padding:9px 18px;background:#f39c12;border:none;border-radius:8px;
          color:#111;font-weight:700;font-size:13px;cursor:pointer;
        ">Continuar</button>
      </div>`;
    document.body.appendChild(el);
    document.getElementById('_cq_warn_btn').onclick = () => {
      _onAtividade(); _heartbeat();
    };
  }

  // ── UI: TELA DE SESSÃO EXPIRADA ───────────────────────────
  function _expirar() {
    _log('session_expired', { motivo: '1h_inatividade' });
    _limparLocal();
    document.body.innerHTML = `
      <div style="
        min-height:100vh;background:#07111c;display:flex;align-items:center;
        justify-content:center;font-family:'DM Sans',sans-serif;padding:20px;
      ">
        <div style="
          background:#0d1b2a;border:1px solid #e74c3c;border-radius:14px;
          padding:40px 32px;max-width:360px;width:100%;text-align:center;
        ">
          <div style="font-size:52px;margin-bottom:14px">🔒</div>
          <div style="font-size:20px;font-weight:800;color:#e74c3c;
            font-family:'Syne',sans-serif;margin-bottom:8px">
            Sessão Expirada
          </div>
          <div style="font-size:13px;color:#8bafc4;line-height:1.6;margin-bottom:24px">
            Sua sessão expirou por <strong style="color:#e8f0f5">1 hora de inatividade</strong>.<br>
            Faça login novamente para continuar.
          </div>
          <a href="login.html" style="
            display:block;padding:13px;background:#2E6B8A;border-radius:8px;
            color:white;font-size:14px;font-weight:600;text-decoration:none;
          ">Fazer Login</a>
        </div>
      </div>`;
  }

  // ── UI: HEADER USER CHIP ──────────────────────────────────
  function _renderChip() {
    const el = document.getElementById('cq-user-chip');
    if (!el || !_user) return;
    const ini = (_user.nome || _user.email)[0].toUpperCase();
    el.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;">
        <div style="
          width:28px;height:28px;border-radius:50%;background:#2E6B8A;
          display:flex;align-items:center;justify-content:center;
          font-weight:800;font-size:13px;color:white;flex-shrink:0;
        ">${ini}</div>
        <div style="line-height:1.2">
          <div style="font-size:12px;font-weight:600;color:#e8f0f5">
            ${_user.nome || _user.email}
          </div>
          <div style="font-size:10px;color:#c8a96e;text-transform:uppercase;letter-spacing:.05em">
            ${_user.perfil || ''}
          </div>
        </div>
        <button onclick="CQAuth.logout()" title="Sair" style="
          background:none;border:1px solid #1e3a52;border-radius:6px;
          color:#5d7f95;cursor:pointer;padding:4px 8px;font-size:11px;
          transition:all .2s;margin-left:4px;
        " onmouseover="this.style.color='#e74c3c';this.style.borderColor='#e74c3c'"
           onmouseout="this.style.color='#5d7f95';this.style.borderColor='#1e3a52'">
          Sair
        </button>
      </div>`;
  }

  // ── UI: GEO BADGE ─────────────────────────────────────────
  function _renderGeo(geo) {
    const el = document.getElementById('cq-geo-badge');
    if (!el) return;
    if (!geo) {
      el.innerHTML = `<span style="font-size:11px;color:#5d7f95">📍 Sem localização</span>`;
      return;
    }
    const cor   = geo.dentro_planta ? '#27ae60' : '#f39c12';
    const label = geo.dentro_planta ? 'Na planta' : `${geo.distancia_m}m da planta`;
    el.innerHTML = `
      <span style="font-size:11px;color:${cor};display:flex;align-items:center;gap:4px;">
        <span style="width:6px;height:6px;border-radius:50%;background:${cor};display:inline-block"></span>
        📍 ${label}
      </span>`;
    // Aviso se fora da planta
    if (!geo.dentro_planta) {
      const aviso = document.getElementById('cq-geo-aviso');
      if (aviso) {
        aviso.style.display = 'flex';
        aviso.textContent = `⚠️ Inspeção registrada a ${geo.distancia_m}m da planta.`;
      }
    }
  }

  // ── UI: CAMPO INSPETOR READONLY ───────────────────────────
  function _preencherInspetor() {
    // Suporta tanto <select> quanto <input> no campo f-inspetor
    const el = document.getElementById('f-inspetor');
    if (!el || !_user) return;
    const nome = _user.nome || _user.email;
    if (el.tagName === 'SELECT') {
      // Substituir select por input readonly
      const inp = document.createElement('input');
      inp.type      = 'text';
      inp.id        = 'f-inspetor';
      inp.className = el.className;
      inp.value     = nome;
      inp.readOnly  = true;
      inp.setAttribute('data-usuario-id', _user.id);
      inp.style.cssText =
        'cursor:not-allowed;background:var(--bg3,#162535);' +
        'border-color:var(--border,#1e3a52);color:var(--muted,#5d7f95);';
      inp.title = '🔒 Preenchido automaticamente pelo login';
      el.replaceWith(inp);
    } else {
      el.value    = nome;
      el.readOnly = true;
      el.setAttribute('data-usuario-id', _user.id);
      el.style.cssText += ';cursor:not-allowed;color:var(--muted,#5d7f95);';
      el.title = '🔒 Preenchido automaticamente pelo login';
    }
    // Atualizar hint se existir
    const hint = document.querySelector('[data-hint="inspetor"]');
    if (hint) hint.textContent = '🔒 Definido pelo usuário autenticado';
  }

  // ── LIMPAR LOCAL ──────────────────────────────────────────
  function _limparLocal() {
    clearTimeout(_tExp); clearTimeout(_tWarn); clearInterval(_tBeat);
    localStorage.removeItem(STORE_KEY);
    _user = null; _token = null; _geo = null;
  }

  // ═════════════════════════════════════════════════════════
  // API PÚBLICA
  // ═════════════════════════════════════════════════════════

  /**
   * Inicializar auth em qualquer página protegida.
   * Se não há sessão válida → redireciona para login.html
   */
  async function init(opts = {}) {
    // 1. Carregar sessão do localStorage
    const raw = localStorage.getItem(STORE_KEY);
    if (raw) {
      try { const s = JSON.parse(raw); _user = s.user; _token = s.token; } catch { /* */ }
    }

    // 2. Sem sessão → redirecionar
    if (!_user || !_token) {
      window.location.href = 'login.html';
      return null;
    }

    // 3. Verificar validade no Supabase
    const ok = await _validarSessao();
    if (!ok) {
      localStorage.removeItem(STORE_KEY);
      window.location.href = 'login.html';
      return null;
    }

    // 4. Verificar perfil permitido
    if (opts.perfisPermitidos && !opts.perfisPermitidos.includes(_user.perfil)) {
      window.location.href = 'index.html';
      return null;
    }

    // 5. Geolocalização em background
    _geo_capture().then(geo => {
      _renderGeo(geo);
      if (opts.onGeo) opts.onGeo(geo);
    });

    // 6. Timers de inatividade
    _resetTimers();
    ['click','keydown','touchstart','scroll'].forEach(ev =>
      document.addEventListener(ev, _onAtividade, { passive: true })
    );

    // 7. Heartbeat
    _tBeat = setInterval(_heartbeat, BEAT_MS);

    // 8. UI
    _renderChip();
    _preencherInspetor();

    return _user;
  }

  /**
   * Login com email + senha via Supabase Auth
   */
  async function login(email, senha) {
    const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=password`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': SB_KEY },
      body: JSON.stringify({ email, password: senha }),
    });

    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      throw new Error(e.error_description || e.message || 'Credenciais inválidas');
    }

    const data = await r.json();
    const u    = data.user;
    _user = {
      id:     u.id,
      email:  u.email,
      nome:   u.user_metadata?.nome || u.user_metadata?.name || email.split('@')[0],
      perfil: u.user_metadata?.perfil || 'analisador',
    };

    await _geo_capture();
    await _abrirSessao();
    await _log('login', { email, dispositivo: navigator.userAgent.slice(0, 80) });
    _resetTimers();
    return _user;
  }

  /** Logout manual */
  async function logout() {
    await _log('logout', {});
    await _patch('cq_sessoes',
      `session_token=eq.${_token}&ativa=eq.true`,
      { ativa: false, encerrada_em: new Date().toISOString() }
    );
    _limparLocal();
    window.location.href = 'login.html';
  }

  /** Renovar sessão — botão "Continuar" no aviso */
  function renovarSessao() {
    _onAtividade();
    _heartbeat();
  }

  /** Registrar ação no audit log */
  async function log(acao, detalhes = {}) {
    await _log(acao, detalhes);
  }

  /** Dados para incluir em qualquer registro salvo */
  function dadosRegistro() {
    return {
      usuario_id:        _user?.id    || null,
      usuario_email:     _user?.email || null,
      usuario_nome:      _user?.nome  || null,
      session_token:     _token       || null,
      latitude:          _geo?.latitude        || null,
      longitude:         _geo?.longitude       || null,
      geo_dentro_planta: _geo?.dentro_planta   ?? null,
    };
  }

  function getUser()  { return _user; }
  function getGeo()   { return _geo; }
  function getToken() { return _token; }

  return { init, login, logout, renovarSessao, log, dadosRegistro, getUser, getGeo, getToken };

})();
