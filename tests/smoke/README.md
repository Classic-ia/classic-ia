# Smoke Tests — Classic IA (Playwright)

Testes que detectam quebra crítica em produção/staging sem depender de dados específicos.
Complementa os testes em `tests/e2e/` (que usam Cypress). Esta pasta é exclusivamente Playwright.

## Como rodar

```bash
npm install
npx playwright install --with-deps chromium
npx playwright test tests/smoke
```

## Variáveis de ambiente

| Variável                  | Default                          | Descrição |
|---------------------------|----------------------------------|-----------|
| `BASE_URL`                | `http://localhost:3000`          | URL base do site (use `https://app.classiccouros.com.br` para staging) |
| `SMOKE_TEST_EMAIL`        | *(nenhum)*                       | Se definido, habilita o teste de login válido |
| `SMOKE_TEST_PASSWORD`     | *(nenhum)*                       | Senha pareada com `SMOKE_TEST_EMAIL` |

> ⚠️ **Nunca comitar credenciais.** Use `.env.smoke` (git-ignored) ou CI secrets.

## Cobertura

- `login.spec.js` — tela de login carrega, rejeita credenciais inválidas, aceita credenciais válidas (opcional)
- `home.spec.js` — `app.html` não quebra em JS console (unauth redireciona ou carrega)
- `gestao_rh_sst.spec.js` — página de gestão carrega sem erros fatais no console

## Critério de sucesso

Todos os testes passam sem erros de JS no console da página. Warnings são permitidos.
