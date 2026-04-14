// tests/smoke/gestao_rh_sst.spec.js
// Smoke: página de gestão renderiza sem crash.
const { test, expect } = require('@playwright/test');
const { collectConsoleErrors, isFatal } = require('./_helpers');

test.describe('gestao_rh_sst.html (smoke)', () => {
  test('carrega sem exception fatal de JS', async ({ page }) => {
    const errors = collectConsoleErrors(page);

    const response = await page.goto('/gestao_rh_sst.html', {
      waitUntil: 'domcontentloaded',
    });

    // Resposta HTTP não deve ser erro de servidor
    expect(response && response.status()).toBeLessThan(500);

    // Aguarda shell/scripts estabilizarem
    await page.waitForTimeout(2000);

    // HTML mínimo presente
    const html = await page.content();
    expect(html.length).toBeGreaterThan(500);

    // URL final: ou a própria página ou redirect de auth
    const url = page.url();
    expect(url).toMatch(/gestao_rh_sst\.html|login/i);

    // Nenhum erro fatal não-ignorável no console
    const fatais = errors.filter(isFatal);
    expect(fatais, JSON.stringify(fatais, null, 2)).toHaveLength(0);
  });
});
