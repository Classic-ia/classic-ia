// rh_api.js — Utilitário centralizado de chamadas API do Classic RH
// Requer: config.js e rh_auth.js carregados antes deste arquivo.

const RH_API = {
  async fetch(endpoint, options = {}) {
    const url = endpoint.startsWith('http')
      ? endpoint
      : `${SB_URL}/rest/v1/${endpoint}`;

    const jwt = (typeof RHAuth !== 'undefined' && RHAuth.getAuthToken())
      ? RHAuth.getAuthToken()
      : SB_KEY;

    const headers = {
      'apikey': SB_KEY,
      'Authorization': 'Bearer ' + jwt,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    };

    try {
      const res = await fetch(url, {
        method: options.method || 'GET',
        headers,
        body: options.body ? (typeof options.body === 'string' ? options.body : JSON.stringify(options.body)) : undefined,
      });

      if (!res.ok && res.status >= 500) {
        console.error('[RH_API] Server error:', res.status, endpoint);
        RH_API._showError('Erro no servidor (' + res.status + '). Tente novamente.');
      } else if (res.status === 401 || res.status === 403) {
        console.warn('[RH_API] Auth error:', res.status, endpoint);
        RH_API._showError('Sessão expirada. Faça login novamente.');
      }

      return res;
    } catch (e) {
      console.error('[RH_API] Network error:', e.message, endpoint);
      RH_API._showError('Erro de conexão. Verifique sua internet.');
      return { ok: false, status: 0, json: async () => [], text: async () => '' };
    }
  },

  async get(endpoint) {
    const res = await this.fetch(endpoint);
    if (!res.ok) {
      if (res.status !== 0) console.warn('[RH_API] GET failed:', res.status, endpoint);
      return [];
    }
    try {
      return await res.json();
    } catch (e) {
      console.error('[RH_API] JSON parse error:', endpoint);
      return [];
    }
  },

  async post(endpoint, data, prefer) {
    const headers = {};
    if (prefer) headers['Prefer'] = prefer;
    return this.fetch(endpoint, { method: 'POST', headers, body: data });
  },

  async patch(endpoint, data) {
    return this.fetch(endpoint, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: data,
    });
  },

  async delete(endpoint) {
    return this.fetch(endpoint, {
      method: 'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
  },

  // Toast de erro global
  _showError(msg) {
    if (typeof UI !== 'undefined' && UI.toast) {
      UI.toast(msg, 'error');
    } else {
      // Fallback: toast inline
      var t = document.createElement('div');
      t.style.cssText = 'position:fixed;bottom:20px;right:20px;background:#1e293b;color:#fff;padding:12px 20px;border-radius:10px;font-size:13px;z-index:9999;box-shadow:0 4px 12px rgba(0,0,0,.15);max-width:360px;border-left:4px solid #ef4444;';
      t.textContent = msg;
      document.body.appendChild(t);
      setTimeout(function() { t.remove(); }, 5000);
    }
  }
};
