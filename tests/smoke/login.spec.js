// tests/smoke/login.spec.js
// Smoke: tela de login carrega, rejeita inválido, aceita válido (opcional).
const { test, expect } = require('@playwright/test');
const { collectConsoleErrors, isFatal } = require('./_helpers');

const TEST_EMAIL = process.env.SMOKE_TEST_EMAIL;
const TEST_PASS = process.env.SMOKE_TEST_PASSWORD;

test.describe('Login (smoke)', () => {
  test('login.html carrega e exibe formulário', async ({ page }) => {
    const errors = collectConsoleErrors(page);

    await page.goto('/login.html');

    // Campos essenciais presentes e visíveis
    await expect(page.locator('input[type="email"]')).toBeVisible();
    await expect(page.locator('input[type="password"]')).toBeVisible();

    // Algum botão de submit/entrar
    const btn = page.locator('button', { hasText: /entrar|login|acessar/i });
    await expect(btn.first()).toBeVisible();

    // Nenhum erro fatal no carregamento
    expect(errors.filter(isFatal), JSON.stringify(errors.filter(isFatal), null, 2)).toHaveLength(0);
  });

  test('rejeita credenciais inválidas e permanece na página de login', async ({ page }) => {
    await page.goto('/login.html');

    await page.locator('input[type="email"]').fill('invalido+smoke@example.test');
    await page.locator('input[type="password"]').fill('senhaerrada-smoke');

    const btn = page.locator('button', { hasText: /entrar|login|acessar/i }).first();
    await btn.click();

    // Deve permanecer em /login* (não redirecionar para app/home)
    await page.waitForTimeout(1500); // aguarda resposta auth
    const url = page.url();
    expect(url).toMatch(/login/i);
  });

  // Login válido — pulado se credenciais não fornecidas
  const smokeOk = TEST_EMAIL && TEST_PASS ? test : test.skip;
  smokeOk('login válido redireciona para home/app', async ({ page }) => {
    await page.goto('/login.html');

    await page.locator('input[type="email"]').fill(TEST_EMAIL);
    await page.locator('input[type="password"]').fill(TEST_PASS);

    await Promise.all([
      page.waitForURL(/home|app/i, { timeout: 15_000 }),
      page.locator('button', { hasText: /entrar|login|acessar/i }).first().click(),
    ]);

    // Após login deve estar em home ou app
    expect(page.url()).toMatch(/home\.html|app\.html/);
  });
});
