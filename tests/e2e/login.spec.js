/**
 * Classic CQ — Testes E2E com Cypress
 * Fluxo: Login → Dashboard → Navegação
 *
 * Para rodar:
 *   npx cypress run --spec tests/e2e/login.spec.js
 *
 * Configurar em cypress.config.js:
 *   e2e: { specPattern: 'tests/e2e/**/*.spec.js', baseUrl: 'http://localhost:3000' }
 */

describe('Classic CQ — Login e Navegação', () => {

  const TEST_EMAIL = Cypress.env('CQ_TEST_EMAIL') || 'teste@classiccarnes.com.br';
  const TEST_PASS = Cypress.env('CQ_TEST_PASS') || 'test123';

  beforeEach(() => {
    cy.clearLocalStorage();
  });

  // ── Login ──────────────────────────────────────────────────

  it('Deve exibir formulário de login', () => {
    cy.visit('/login.html');
    cy.get('input[type="email"]').should('be.visible');
    cy.get('input[type="password"]').should('be.visible');
    cy.get('button[type="submit"], button').contains(/entrar|login/i).should('exist');
  });

  it('Deve rejeitar credenciais inválidas', () => {
    cy.visit('/login.html');
    cy.get('input[type="email"]').type('invalido@teste.com');
    cy.get('input[type="password"]').type('senhaerrada');
    cy.get('button[type="submit"], button').contains(/entrar|login/i).click();
    // Deve permanecer na página de login
    cy.url().should('include', 'login');
  });

  it('Deve redirecionar para login quando não autenticado', () => {
    cy.visit('/app.html');
    cy.url().should('include', 'login');
  });

  it('Deve redirecionar para login ao acessar dashboard sem sessão', () => {
    cy.visit('/dashboard_cq.html');
    cy.url().should('include', 'login');
  });

  // ── Páginas carregam sem erros JS ─────────────────────────

  const PAGINAS_PUBLICAS = [
    '/login.html',
  ];

  PAGINAS_PUBLICAS.forEach(pagina => {
    it(`Página ${pagina} carrega sem erros JS`, () => {
      cy.visit(pagina, { failOnStatusCode: false });
      cy.window().then(win => {
        // Verificar que não houve erros fatais
        cy.document().should('exist');
      });
    });
  });

  // ── Config.js carregado corretamente ──────────────────────

  it('Config.js define CQ_CONFIG global', () => {
    cy.visit('/login.html');
    cy.window().then(win => {
      expect(win.CQ_CONFIG).to.exist;
      expect(win.CQ_CONFIG.SB_URL).to.be.a('string').and.include('supabase');
      expect(win.CQ_CONFIG.SB_KEY).to.be.a('string').and.have.length.greaterThan(10);
      expect(win.CQ_CONFIG.APP_NAME).to.equal('Classic CQ');
    });
  });

  // ── Fluxo autenticado (quando credenciais estão configuradas) ──

  describe('Fluxo autenticado', () => {
    before(() => {
      // Skip se não houver credenciais de teste
      if (!Cypress.env('CQ_TEST_EMAIL')) {
        cy.log('⚠️ Credenciais de teste não configuradas. Set CQ_TEST_EMAIL e CQ_TEST_PASS.');
      }
    });

    it('Login com credenciais válidas redireciona para app', function () {
      if (!Cypress.env('CQ_TEST_EMAIL')) this.skip();

      cy.visit('/login.html');
      cy.get('input[type="email"]').type(TEST_EMAIL);
      cy.get('input[type="password"]').type(TEST_PASS);
      cy.get('button[type="submit"], button').contains(/entrar|login/i).click();

      // Deve redirecionar para app.html
      cy.url({ timeout: 10000 }).should('include', 'app');
    });

    it('Usuário autenticado vê o menu principal', function () {
      if (!Cypress.env('CQ_TEST_EMAIL')) this.skip();

      cy.visit('/login.html');
      cy.get('input[type="email"]').type(TEST_EMAIL);
      cy.get('input[type="password"]').type(TEST_PASS);
      cy.get('button[type="submit"], button').contains(/entrar|login/i).click();

      cy.url({ timeout: 10000 }).should('include', 'app');
      // Menu deve ter links para as seções
      cy.contains(/qualidade|dashboard|inspe/i).should('exist');
    });

    it('Logout limpa sessão e redireciona para login', function () {
      if (!Cypress.env('CQ_TEST_EMAIL')) this.skip();

      // Login
      cy.visit('/login.html');
      cy.get('input[type="email"]').type(TEST_EMAIL);
      cy.get('input[type="password"]').type(TEST_PASS);
      cy.get('button[type="submit"], button').contains(/entrar|login/i).click();
      cy.url({ timeout: 10000 }).should('include', 'app');

      // Logout
      cy.contains(/sair|logout/i).click();
      cy.url({ timeout: 5000 }).should('include', 'login');

      // Sessão limpa
      cy.window().then(win => {
        const stored = win.localStorage.getItem('cq_sess_v2');
        expect(stored).to.be.null;
      });
    });
  });
});
