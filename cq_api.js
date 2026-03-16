// cq_api.js — Utilitário centralizado de chamadas API do Classic CQ
// Requer: config.js e cq_auth.js carregados antes deste arquivo.

const CQ_API = {
  async fetch(endpoint, options = {}) {
    const url = endpoint.startsWith('http')
      ? endpoint
      : `${SB_URL}/rest/v1/${endpoint}`;

    const jwt = (typeof CQAuth !== 'undefined' && CQAuth.getAuthToken())
      ? CQAuth.getAuthToken()
      : SB_KEY;

    const headers = {
      'apikey': SB_KEY,
      'Authorization': 'Bearer ' + jwt,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    };

    const res = await fetch(url, {
      method: options.method || 'GET',
      headers,
      body: options.body ? (typeof options.body === 'string' ? options.body : JSON.stringify(options.body)) : undefined,
    });

    return res;
  },

  async get(endpoint) {
    const res = await this.fetch(endpoint);
    return res.ok ? res.json() : [];
  },

  async post(endpoint, data, prefer) {
    const headers = {};
    if (prefer) headers['Prefer'] = prefer;
    const res = await this.fetch(endpoint, {
      method: 'POST',
      headers,
      body: data,
    });
    return res;
  },
};

// Alias global para compatibilidade com páginas que já definem apiFetch localmente
if (typeof apiFetch === 'undefined') {
  var apiFetch = function(endpoint, options) {
    return CQ_API.fetch(endpoint, options);
  };
}
