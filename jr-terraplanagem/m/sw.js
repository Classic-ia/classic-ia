/**
 * JR Terraplanagem Mobile - Service Worker
 *
 * Estrategia:
 *   - App shell (JS/CSS/SVG/font) : cache-first com stale-while-revalidate.
 *   - Paginas HTML                 : network-first com fallback cache.
 *   - API Supabase (/rest/v1/*)    : network-only (fila offline fica a cargo
 *                                    de m_offline.js, nao do SW).
 *
 * Bump SW_VERSION quando mudar assets para invalidar cache.
 */
const SW_VERSION = 'jr-mobile-v1-2026-04-20';
const ASSET_CACHE = 'jr-assets-' + SW_VERSION;
const PAGE_CACHE  = 'jr-pages-'  + SW_VERSION;

const CORE_ASSETS = [
  'm_shell.js',
  'm_scan.js',
  'm_offline.js',
  '../config.js',
  '../auth.js',
  '../api.js',
  'manifest.json',
  'icon.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(ASSET_CACHE)
      .then((cache) => cache.addAll(CORE_ASSETS).catch(() => { /* tolerar 404 em dev */ }))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter(k => k !== ASSET_CACHE && k !== PAGE_CACHE).map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Nunca interceptar chamadas Supabase - deixar o app lidar via m_offline.js.
  if (url.pathname.startsWith('/rest/v1/') ||
      url.pathname.startsWith('/auth/v1/') ||
      url.pathname.startsWith('/functions/v1/') ||
      url.hostname.endsWith('.supabase.co')) {
    return;
  }

  // Pagina HTML - network-first com fallback ao cache.
  const isHtml = req.mode === 'navigate' ||
                 (req.headers.get('accept') || '').includes('text/html');
  if (isHtml) {
    event.respondWith(
      fetch(req)
        .then((resp) => {
          if (resp && resp.status === 200) {
            const copy = resp.clone();
            caches.open(PAGE_CACHE).then((c) => c.put(req, copy));
          }
          return resp;
        })
        .catch(() => caches.match(req).then((hit) => hit || caches.match('m_home.html')))
    );
    return;
  }

  // Demais assets - cache-first com revalidacao em background.
  event.respondWith(
    caches.match(req).then((cached) => {
      const network = fetch(req).then((resp) => {
        if (resp && resp.status === 200 && (resp.type === 'basic' || resp.type === 'cors')) {
          const copy = resp.clone();
          caches.open(ASSET_CACHE).then((c) => c.put(req, copy));
        }
        return resp;
      }).catch(() => cached);
      return cached || network;
    })
  );
});

self.addEventListener('message', (event) => {
  if (event.data === 'SW_SKIP_WAITING') self.skipWaiting();
});
