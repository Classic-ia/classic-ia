/**
 * rh_auth.js — Classic RH & SST
 * Módulo de autenticação, sessão e auditoria para o sistema RH
 *
 * Uso: <script src="config_rh.js"></script>
 *      <script src="rh_auth.js"></script>
 *      const user = await RHAuth.init();
 */

const RHAuth = (function () {

  const SB_URL = RH_CONFIG.SB_URL;
  const SB_KEY = RH_CONFIG.SB_KEY;

  const TIMEOUT_MS = 60 * 60 * 1000;
  const WARN_MS    = 5  * 60 * 1000;
  const BEAT_MS    = 60 * 1000;
  const STORE_KEY  = 'rh_sess_v2';

  // Planta Classic Couros — Jardim Alegre/PR
  const PLANTA = { lat: -24.1820, lng: -51.6920, raio_m: 500 };

  let _user      = null;
  let _token     = null;
  let _authToken = null;
  let _geo       = null;
  let _tExp      = null;
  let _tWarn     = null;
  let _tBeat     = null;

  const _h = () => ({
    'Content-Type': 'application/json',
    'apikey':       SB_KEY,
    'Authorization':'Bearer ' + (_authToken || SB_KEY),
  });

  const _hAnon = () => ({
    'Content-Type': 'application/json',
    'apikey':       SB_KEY,
    'Authorization':'Bearer ' + SB_KEY,
  });

  async function _get(tabela, qs) {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/${tabela}?${qs}`, { headers: _h() });
      if (r.ok) return r.json();
      return null;
    } catch { return null; }
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

  async function _revalidarPerfil() {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/rpc/rh_meu_perfil`, {
        method: 'POST',
        headers: _h(),
        body: '{}',
      });
      if (!r.ok) return null;
      const rows = await r.json();
      if (rows && rows.length > 0) return rows[0];
      return null;
    } catch { return null; }
  }

  async function _refreshToken() {
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) return null;
    try {
      const s = JSON.parse(raw);
      if (!s.refresh_token) return null;
      const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=refresh_token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'apikey': SB_KEY },
        body: JSON.stringify({ refresh_token: s.refresh_token }),
      });
      if (!r.ok) return null;
      const data = await r.json();
      _authToken = data.access_token;
      s.access_token = data.access_token;
      s.refresh_token = data.refresh_token;
      localStorage.setItem(STORE_KEY, JSON.stringify(s));
      return data;
    } catch { return null; }
  }

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

  async function _log(acao, detalhes = {}) {
    if (!_user) return;
    try { await _post('rh_audit_log', {
      usuario_id:    _user.id,
      usuario_email: _user.email,
      usuario_nome:  _user.nome,
      acao,
      detalhes:      JSON.stringify(detalhes),
      latitude:      _geo?.latitude  || null,
      longitude:     _geo?.longitude || null,
      dentro_planta: _geo?.dentro_planta ?? null,
      criado_em:     new Date().toISOString(),
    }); } catch(e) { console.warn('[RHAuth] audit_log:', e.message); }
  }

  function _mkToken() {
    const arr = new Uint8Array(24);
    crypto.getRandomValues(arr);
    return Array.from(arr).map(b => b.toString(16).padStart(2,'0')).join('');
  }

  async function _abrirSessao(refreshToken) {
    try { await _patch('rh_sessoes',
      `usuario_id=eq.${_user.id}&ativa=eq.true`,
      { ativa: false, encerrada_em: new Date().toISOString() }
    ); } catch(e) {}
    _token = _mkToken();
    const expira = new Date(Date.now() + TIMEOUT_MS).toISOString();
    try { await _post('rh_sessoes', {
      usuario_id:    _user.id,
      usuario_email: _user.email,
      session_token: _token,
      dispositivo:   navigator.userAgent.slice(0, 150),
      expira_em:     expira,
      ativa:         true,
      criado_em:     new Date().toISOString(),
    }); } catch(e) { console.warn('[RHAuth] Tabela rh_sessoes:', e.message); }
    localStorage.setItem(STORE_KEY, JSON.stringify({
      token: _token,
      user: _user,
      access_token: _authToken,
      refresh_token: refreshToken || null,
      ts: Date.now()
    }));
  }

  async function _validarSessao() {
    if (!_user || !_token) return false;
    try {
      const r = await fetch(
        `${SB_URL}/rest/v1/rh_sessoes?session_token=eq.${_token}&ativa=eq.true`,
        { headers: _h() }
      );
      if (r.ok) {
        const rows = await r.json();
        return rows.length > 0;
      }
      return false;
    } catch {
      return false;
    }
  }

  async function _heartbeat() {
    if (!_token) return;
    const expira = new Date(Date.now() + TIMEOUT_MS).toISOString();
    await _patch('rh_sessoes',
      `session_token=eq.${_token}&ativa=eq.true`,
      { ultimo_acesso: new Date().toISOString(), expira_em: expira }
    );
  }

  function _resetTimers() {
    clearTimeout(_tExp); clearTimeout(_tWarn);
    _tWarn = setTimeout(_mostrarAviso, TIMEOUT_MS - WARN_MS);
    _tExp  = setTimeout(_expirar,      TIMEOUT_MS);
  }

  function _onAtividade() {
    _resetTimers();
    const el = document.getElementById('_rh_warn');
    if (el) el.remove();
  }

  function _mostrarAviso() {
    if (document.getElementById('_rh_warn')) return;
    const el = document.createElement('div');
    el.id = '_rh_warn';
    el.innerHTML = `
      <div style="
        position:fixed;bottom:20px;left:50%;transform:translateX(-50%);
        background:#0d1b2a;border:2px solid #f39c12;border-radius:12px;
        padding:16px 22px;z-index:99999;max-width:400px;width:90%;
        box-shadow:0 8px 32px rgba(0,0,0,.6);font-family:'DM Sans',sans-serif;
        display:flex;align-items:center;gap:14px;
      ">
        <span style="font-size:28px;">&#x23F0;</span>
        <div style="flex:1">
          <div style="font-weight:700;color:#f39c12;font-size:14px;margin-bottom:3px">
            Sessao expira em 5 minutos
          </div>
          <div style="font-size:12px;color:#8bafc4">
            Clique em Continuar para manter o acesso.
          </div>
        </div>
        <button id="_rh_warn_btn" style="
          padding:9px 18px;background:#f39c12;border:none;border-radius:8px;
          color:#111;font-weight:700;font-size:13px;cursor:pointer;
        ">Continuar</button>
      </div>`;
    document.body.appendChild(el);
    document.getElementById('_rh_warn_btn').onclick = () => {
      _onAtividade(); _heartbeat();
    };
  }

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
          <div style="font-size:52px;margin-bottom:14px">&#x1F512;</div>
          <div style="font-size:20px;font-weight:800;color:#e74c3c;
            font-family:'Syne',sans-serif;margin-bottom:8px">
            Sessao Expirada
          </div>
          <div style="font-size:13px;color:#8bafc4;line-height:1.6;margin-bottom:24px">
            Sua sessao expirou por <strong style="color:#e8f0f5">1 hora de inatividade</strong>.<br>
            Faca login novamente para continuar.
          </div>
          <a href="login_rh.html" style="
            display:block;padding:13px;background:#2E6B8A;border-radius:8px;
            color:white;font-size:14px;font-weight:600;text-decoration:none;
          ">Fazer Login</a>
        </div>
      </div>`;
  }

  function _renderChip() {
    const el = document.getElementById('rh-user-chip');
    if (!el || !_user) return;
    const nameOrEmail = _user.nome || _user.email || '?';
    const ini = nameOrEmail.charAt(0).toUpperCase();
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
        <button onclick="RHAuth.logout()" title="Sair" style="
          background:none;border:1px solid #1e3a52;border-radius:6px;
          color:#5d7f95;cursor:pointer;padding:4px 8px;font-size:11px;
          transition:all .2s;margin-left:4px;
        " onmouseover="this.style.color='#e74c3c';this.style.borderColor='#e74c3c'"
           onmouseout="this.style.color='#5d7f95';this.style.borderColor='#1e3a52'">
          Sair
        </button>
      </div>`;
  }

  function _limparLocal() {
    clearTimeout(_tExp); clearTimeout(_tWarn); clearInterval(_tBeat);
    localStorage.removeItem(STORE_KEY);
    _user = null; _token = null; _geo = null;
  }

  // ══════════════════════════════════════════════
  // API PUBLICA
  // ══════════════════════════════════════════════

  async function init(opts = {}) {
    const raw = localStorage.getItem(STORE_KEY);
    if (raw) {
      try {
        const s = JSON.parse(raw);
        _user = s.user;
        _token = s.token;
        _authToken = s.access_token || null;
      } catch { /* */ }
    }

    if (!_user || !_token) {
      window.location.href = 'login_rh.html';
      return null;
    }

    if (_authToken) {
      const refreshed = await _refreshToken();
      if (!refreshed) {
        _limparLocal();
        window.location.href = 'login_rh.html';
        return null;
      }

      const perfilDB = await _revalidarPerfil();
      if (!perfilDB) {
        _limparLocal();
        window.location.href = 'login_rh.html';
        return null;
      }

      _user.id = perfilDB.id;
      _user.nome = perfilDB.nome;
      _user.perfil = perfilDB.perfil;
      _user.email = perfilDB.email;

      try {
        const stored = JSON.parse(localStorage.getItem(STORE_KEY));
        if (stored) {
          stored.user = _user;
          localStorage.setItem(STORE_KEY, JSON.stringify(stored));
        }
      } catch { }
    } else {
      const ok = await _validarSessao();
      if (!ok) {
        _limparLocal();
        window.location.href = 'login_rh.html';
        return null;
      }
    }

    if (opts.perfisPermitidos && !opts.perfisPermitidos.includes(_user.perfil)) {
      window.location.href = 'app_rh.html';
      return null;
    }

    _geo_capture().then(geo => {
      if (opts.onGeo) opts.onGeo(geo);
    });

    _resetTimers();
    ['click','keydown','touchstart','scroll'].forEach(ev =>
      document.addEventListener(ev, _onAtividade, { passive: true })
    );

    _tBeat = setInterval(_heartbeat, BEAT_MS);
    _renderChip();

    return _user;
  }

  async function login(email, senha) {
    const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=password`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': SB_KEY },
      body: JSON.stringify({ email, password: senha }),
    });

    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      throw new Error(e.error_description || e.message || 'Credenciais invalidas');
    }

    const data = await r.json();
    const u    = data.user;

    _authToken = data.access_token;

    _user = {
      id:     u.id,
      email:  u.email,
      nome:   u.user_metadata?.nome || u.user_metadata?.name || email.split('@')[0],
      perfil: u.user_metadata?.perfil || 'rh',
    };

    const perfilDB = await _revalidarPerfil();
    if (perfilDB) {
      _user.id     = perfilDB.id;
      _user.nome   = perfilDB.nome;
      _user.perfil = perfilDB.perfil;
      _user.email  = perfilDB.email;
    } else {
      throw new Error('Usuario nao cadastrado ou desativado no sistema RH. Contate o administrador.');
    }

    await _geo_capture();
    await _abrirSessao(data.refresh_token);
    await _log('login', { email, dispositivo: navigator.userAgent.slice(0, 80) });
    _resetTimers();
    return _user;
  }

  async function logout() {
    await _log('logout', {});
    await _patch('rh_sessoes',
      `session_token=eq.${_token}&ativa=eq.true`,
      { ativa: false, encerrada_em: new Date().toISOString() }
    );
    _limparLocal();
    window.location.href = 'login_rh.html';
  }

  function renovarSessao() {
    _onAtividade();
    _heartbeat();
  }

  async function log(acao, detalhes = {}) {
    await _log(acao, detalhes);
  }

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

  function getUser()      { return _user; }
  function getGeo()       { return _geo; }
  function getToken()     { return _token; }
  function getAuthToken() { return _authToken; }
  function getSbUrl()     { return SB_URL; }
  function getSbKey()     { return SB_KEY; }

  return { init, login, logout, renovarSessao, log, dadosRegistro, getUser, getGeo, getToken, getAuthToken, getSbUrl, getSbKey };

})();
