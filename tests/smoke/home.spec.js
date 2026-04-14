// tests/smoke/home.spec.js
// Smoke: app.html não deve crashar. Sem sessão, deve redirecionar para login.
const { test, expect } = require('@playwright/test');
const { collectConsoleErrors, isFatal } = require('./_helpers');

test.describe('app.html (smoke)', () => {
  test('carrega sem erros fatais de JS (ou redireciona p/ login)', async ({ page }) => {
    const errors = collectConsoleErrors(page);

    await page.goto('/app.html', { waitUntil: 'domcontentloaded' });

    // Aguarda estabilização: ou redireciona (sem sessão) ou renderiza
    await page.waitForTimeout(1500);

    const url = page.url();
    const html = await page.content();

    // Critério: ou estamos em login (redirect esperado sem sessão), ou
    // a página app.html renderizou um elemento esperado.
    const noSession = /login/i.test(url);
    const temMenu = html.includes('menu-card') || html.includes('Classic');

    expect(noSession || temMenu, `URL=${url}`).toBe(true);

    // Em nenhum dos dois casos deveria haver erro fatal
    const fatais = errors.filter(isFatal);
    expect(fatais, JSON.stringify(fatais, null, 2)).toHaveLength(0);
  });

  test('home.html responde com HTML (quando disponível)', async ({ page }) => {
    const errors = collectConsoleErrors(page);
    const response = await page.goto('/home.html', { waitUntil: 'domcontentloaded' });

    // Em algumas rotas sem sessão o shell redireciona — ambos são aceitáveis
    expect(response && response.status()).toBeLessThan(500);

    const fatais = errors.filter(isFatal);
    expect(fatais, JSON.stringify(fatais, null, 2)).toHaveLength(0);
  });
});
