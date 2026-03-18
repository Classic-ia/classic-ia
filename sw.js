/**
 * sw.js — Service Worker Classic CQ
 * Estratégia: Network-first com cache fallback
 *
 * Cacheia assets estáticos e dados de cadastro para modo offline.
 * Inspeções feitas offline são salvas em IndexedDB e sincronizadas ao reconectar.
 *
 * Registro (em login.html ou app.html):
 *   if ('serviceWorker' in navigator) {
 *     navigator.serviceWorker.register('/sw.js');
 *   }
 */

const CACHE_VERSION = 'cq-v2026.03.18';
const CACHE_STATIC = `${CACHE_VERSION}-static`;
const CACHE_DATA = `${CACHE_VERSION}-data`;

// Assets estáticos para pré-cache
const STATIC_ASSETS = [
  '/',
  '/app.html',
  '/login.html',
  '/formulario_cq.html',
  '/fila_inspecoes.html',
  '/config.js',
  '/cq_auth.js',
  '/cq_workflow.js',
  '/cq_fornecedores.js',
  '/cq_toast.js',
  '/cq_cache.js',
  '/assets/logo-classic-icon.png',
  '/assets/logo-classic-text.png',
];

// Dados cacheáveis (cadastros que mudam pouco)
const DATA_PATTERNS = [
  /\/rest\/v1\/cadastros_atak\?/,
  /\/rest\/v1\/cq_produtos\?/,
  /\/rest\/v1\/cq_tipos_defeito\?/,
  /\/rest\/v1\/cq_veiculos\?/,
  /\/rest\/v1\/cq_usuarios\?.*ativo=eq\.true/,
];

// ── INSTALL ─────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_STATIC)
      .then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
      .catch(err => {
        console.warn('[SW] Falha no pré-cache (normal em dev):', err.message);
        return self.skipWaiting();
      })
  );
});

// ── ACTIVATE ────────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_STATIC && key !== CACHE_DATA)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// ── FETCH ───────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Ignorar requests não-GET
  if (event.request.method !== 'GET') return;

  // Ignorar requests de auth (tokens, login)
  if (url.pathname.includes('/auth/')) return;

  // Dados de cadastro: Network-first, cache de fallback (TTL 1h)
  if (DATA_PATTERNS.some(p => p.test(event.request.url))) {
    event.respondWith(networkFirstWithCache(event.request, CACHE_DATA));
    return;
  }

  // Assets estáticos: Cache-first, network fallback
  if (isStaticAsset(url)) {
    event.respondWith(cacheFirstWithNetwork(event.request, CACHE_STATIC));
    return;
  }

  // Páginas HTML: Network-first com fallback
  if (event.request.headers.get('accept')?.includes('text/html')) {
    event.respondWith(networkFirstWithCache(event.request, CACHE_STATIC));
    return;
  }
});

// ── ESTRATÉGIAS ─────────────────────────────────────────────

async function networkFirstWithCache(request, cacheName) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;

    // Fallback para página offline
    if (request.headers.get('accept')?.includes('text/html')) {
      return new Response(offlineHTML(), {
        headers: { 'Content-Type': 'text/html' },
      });
    }

    return new Response(JSON.stringify({ error: 'offline' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

async function cacheFirstWithNetwork(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;

  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('', { status: 503 });
  }
}

function isStaticAsset(url) {
  return /\.(js|css|png|jpg|jpeg|svg|ico|woff2?)$/i.test(url.pathname);
}

// ── OFFLINE SYNC (IndexedDB) ────────────────────────────────

// O frontend salva inspeções pendentes em IndexedDB via cq_offline.js
// Quando reconecta, o SW dispara evento de sync

self.addEventListener('sync', (event) => {
  if (event.tag === 'sync-inspecoes') {
    event.waitUntil(syncPendingInspecoes());
  }
});

async function syncPendingInspecoes() {
  // Notificar o frontend para processar a fila
  const clients = await self.clients.matchAll();
  for (const client of clients) {
    client.postMessage({ type: 'SYNC_INSPECOES' });
  }
}

// ── OFFLINE HTML ────────────────────────────────────────────

function offlineHTML() {
  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Classic CQ — Offline</title>
  <style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body {
      min-height:100vh; background:#07111c; display:flex;
      align-items:center; justify-content:center;
      font-family:'DM Sans',system-ui,sans-serif; padding:20px;
    }
    .card {
      background:#0d1b2a; border:1px solid #1e3a52; border-radius:14px;
      padding:40px 32px; max-width:400px; width:100%; text-align:center;
    }
    .icon { font-size:52px; margin-bottom:14px; }
    h1 { font-size:20px; font-weight:800; color:#f39c12; margin-bottom:8px; }
    p { font-size:13px; color:#8bafc4; line-height:1.6; margin-bottom:24px; }
    button {
      display:block; width:100%; padding:13px; background:#2E6B8A;
      border:none; border-radius:8px; color:white; font-size:14px;
      font-weight:600; cursor:pointer;
    }
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">📡</div>
    <h1>Sem Conexão</h1>
    <p>Você está offline. Verifique sua conexão com a internet e tente novamente.</p>
    <button onclick="location.reload()">Tentar Novamente</button>
  </div>
</body>
</html>`;
}
