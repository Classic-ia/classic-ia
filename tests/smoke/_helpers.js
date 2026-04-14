// tests/smoke/_helpers.js
// Utilitários compartilhados pelos smoke tests.

/**
 * Coleta erros fatais de console + erros de página durante um bloco de teste.
 * Retorna um array que é populado automaticamente. Use antes de navegar.
 *
 * Uso:
 *   const errors = collectConsoleErrors(page);
 *   await page.goto(...);
 *   expect(errors.filter(isFatal)).toHaveLength(0);
 */
function collectConsoleErrors(page) {
  const errors = [];

  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      errors.push({ type: 'console', text: msg.text(), url: page.url() });
    }
  });

  page.on('pageerror', (err) => {
    errors.push({ type: 'pageerror', text: err.message, url: page.url() });
  });

  return errors;
}

/**
 * Filtra erros conhecidos não-fatais (rede/403/404 de dados que dependem de login).
 * Útil para distinguir "página quebrou" de "página pede login e backend responde 401".
 */
function isFatal(err) {
  const t = (err.text || '').toLowerCase();
  // Ignora erros esperados em páginas sem auth
  const ignorableSubstrings = [
    'failed to load resource',          // 4xx/5xx de recurso
    'net::err_',                         // erros de rede (offline/DNS)
    '401',                               // unauthorized esperado sem login
    '403',                               // forbidden esperado sem login
    'jwt expired',
    'favicon',                           // favicon ausente é ruído
    'refreshed token',
    'service worker',                    // avisos do SW
    'manifest',                          // manifest PWA
  ];
  if (ignorableSubstrings.some((s) => t.includes(s))) return false;
  return true;
}

module.exports = { collectConsoleErrors, isFatal };
