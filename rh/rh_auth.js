/**
 * rh_auth.js — Classic RH & SST (v2)
 * Módulo de autenticação via Supabase Auth + rh_usuarios (RBAC)
 * Compatível com backend v2 (rh_funcionarios, sem rh_sessoes)
 */

const RHAuth = (function () {

  const TIMEOUT_MS = 60 * 60 * 1000;
  const WARN_MS    = 5  * 60 * 1000;
  const STORE_KEY  = 'rh_sess_v2';

  let _user      = null;
  let _token     = null;
  let _authToken = null;
  let _tExp      = null;
  let _tWarn     = null;

  const _h = () => ({
    'Content-Type': 'application/json',
    'apikey':       SB_KEY,
    'Authorization':'Bearer ' + (_authToken || SB_KEY),
  });

  async function _revalidarPerfil() {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/rpc/rh_meu_perfil`, {
        method: 'POST', headers: _h(), body: '{}',
      });
      if (!r.ok) return null;
      const rows = await r.json();
      return (rows && rows.length > 0) ? rows[0] : null;
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

  function _mkToken() {
    const arr = new Uint8Array(24);
    crypto.getRandomValues(arr);
    return Array.from(arr).map(b => b.toString(16).padStart(2,'0')).join('');
  }

  function _resetTimers() {
    clearTimeout(_tExp); clearTimeout(_tWarn);
    _tWarn = setTimeout(_mostrarAviso, TIMEOUT_MS - WARN_MS);
    _tExp  = setTimeout(_expirar, TIMEOUT_MS);
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
    el.className = 'rh-session-warn';
    el.innerHTML = `
      <span class="rh-session-warn-icon">&#9200;</span>
      <div class="rh-session-warn-body">
        <div class="rh-session-warn-title">Sessao expira em 5 minutos</div>
        <div class="rh-session-warn-sub">Clique em Continuar para manter o acesso.</div>
      </div>
      <button id="_rh_warn_btn" class="rh-session-warn-btn">Continuar</button>`;
    document.body.appendChild(el);
    document.getElementById('_rh_warn_btn').onclick = () => { _onAtividade(); };
  }

  function _expirar() {
    _limparLocal();
    document.body.innerHTML = `
      <div class="rh-session-expired">
        <div class="rh-session-expired-card">
          <div class="rh-session-expired-icon">&#128274;</div>
          <div class="rh-session-expired-title">Sessao Expirada</div>
          <div class="rh-session-expired-text">
            Sua sessao expirou por <strong>1 hora de inatividade</strong>.<br>
            Faca login novamente para continuar.
          </div>
          <a href="login.html" class="rh-session-expired-btn">Fazer Login</a>
        </div>
      </div>`;
  }

  function _renderChip() {
    const el = document.getElementById('rh-user-chip');
    if (!el || !_user) return;
    const ini = (_user.nome || _user.email)[0].toUpperCase();
    const nome = _user.nome || _user.email;
    const perfil = _user.perfil || '';
    el.innerHTML = `
      <div class="user-chip">
        <div class="user-chip-avatar">${ini}</div>
        <div class="user-chip-info">
          <div class="user-chip-name">${nome}</div>
          <div class="user-chip-role">${perfil}</div>
        </div>
        <button class="user-chip-logout" onclick="RHAuth.logout()" title="Sair">Sair</button>
      </div>`;
  }

  function _limparLocal() {
    clearTimeout(_tExp); clearTimeout(_tWarn);
    localStorage.removeItem(STORE_KEY);
    // Limpar chave antiga também
    localStorage.removeItem('rh_sess_v1');
    _user = null; _token = null; _authToken = null;
  }

  // ═══ API PÚBLICA ═══

  async function init(opts = {}) {
    // Tentar v2 primeiro, fallback para v1 (migração)
    let raw = localStorage.getItem(STORE_KEY) || localStorage.getItem('rh_sess_v1');
    if (raw) {
      try {
        const s = JSON.parse(raw);
        _user = s.user; _token = s.token; _authToken = s.access_token || null;
      } catch {}
    }
    if (!_user) { window.location.href = 'login.html'; return null; }

    if (_authToken) {
      const refreshed = await _refreshToken();
      if (!refreshed) { _limparLocal(); window.location.href = 'login.html'; return null; }
      const perfilDB = await _revalidarPerfil();
      if (!perfilDB) { _limparLocal(); window.location.href = 'login.html'; return null; }
      _user.id = perfilDB.id; _user.nome = perfilDB.nome;
      _user.perfil = perfilDB.perfil; _user.email = perfilDB.email;
      localStorage.setItem(STORE_KEY, JSON.stringify({
        token: _token, user: _user,
        access_token: _authToken,
        refresh_token: JSON.parse(raw)?.refresh_token || null,
        ts: Date.now()
      }));
    } else {
      // Sem auth token = sessão inválida, relogin
      _limparLocal(); window.location.href = 'login.html'; return null;
    }

    if (opts.perfisPermitidos && !opts.perfisPermitidos.includes(_user.perfil)) {
      window.location.href = 'app.html'; return null;
    }

    _resetTimers();
    ['click','keydown','touchstart','scroll'].forEach(ev =>
      document.addEventListener(ev, _onAtividade, { passive: true })
    );
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
    _authToken = data.access_token;
    _user = {
      id: data.user.id, email: data.user.email,
      nome: data.user.user_metadata?.nome || data.user.user_metadata?.name || email.split('@')[0],
      perfil: data.user.user_metadata?.perfil || 'rh',
    };
    const perfilDB = await _revalidarPerfil();
    if (perfilDB) {
      _user.id = perfilDB.id; _user.nome = perfilDB.nome;
      _user.perfil = perfilDB.perfil; _user.email = perfilDB.email;
    } else {
      throw new Error('Usuario nao cadastrado ou desativado no sistema RH. Contate o administrador.');
    }
    _token = _mkToken();
    localStorage.setItem(STORE_KEY, JSON.stringify({
      token: _token, user: _user,
      access_token: _authToken,
      refresh_token: data.refresh_token,
      ts: Date.now()
    }));
    _resetTimers();
    return _user;
  }

  async function logout() {
    _limparLocal();
    window.location.href = 'login.html';
  }

  function getUser()      { return _user; }
  function getToken()     { return _token; }
  function getAuthToken() { return _authToken; }

  return { init, login, logout, getUser, getToken, getAuthToken };
})();
