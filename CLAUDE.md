# CLAUDE.md - Classic-IA Project Context

> Arquivo de contexto persistente para sessoes com Claude Code.
> Atualizado em: 2026-04-13

## O que e este projeto

**Classic-IA** e um sistema ERP dual composto por dois modulos principais para a **Classic Couros** (industria de couro):

1. **CQ (Controle de Qualidade)** - Inspecao de produtos, rastreio de defeitos, scoring de fornecedores
2. **RH & SST** - Gestao de colaboradores, seguranca do trabalho, saude ocupacional, beneficios, ferias

---

## Tech Stack

| Camada       | Tecnologia                                    |
|--------------|-----------------------------------------------|
| Frontend     | HTML + Vanilla JS (sem framework SPA)         |
| CSS          | Design system custom (ds.css, cq_layout.css)  |
| Charts       | Chart.js (CDN)                                |
| Backend      | Supabase (PostgreSQL + REST API + Auth)        |
| Edge Funcs   | Deno/TypeScript (Supabase Edge Functions)      |
| ETL          | Python 3 (parsers, watch_folders)              |
| Automacao    | n8n (workflows JSON)                           |
| Deploy       | Vercel                                         |
| CI/CD        | GitHub Actions (.github/workflows/ci.yml)      |
| Testes       | Cypress (E2E)                                  |

---

## Dois Projetos Supabase Separados

| Projeto       | Config         | Escopo                |
|---------------|----------------|-----------------------|
| Classic CQ    | config.js      | Qualidade, inspecoes   |
| Classic RH    | config_rh.js   | RH, SST, beneficios   |

Cada um tem seu proprio banco, auth e chaves. **Nunca misture as configuracoes.**

---

## Estrutura de Diretórios

```
/classic-ia
├── *.html (~80 arquivos)        # Paginas SPA (inline JS)
├── config.js / config_rh.js     # Constantes e chaves Supabase
├── api.js / rh_api.js           # Wrappers REST padronizados
├── auth.js / cq_auth.js         # Autenticacao CQ
├── rh_auth.js                   # Autenticacao RH
├── shell.js                     # Topbar/navegacao RH
├── ds.css / cq_layout.css       # Design system
├── cq_*.js                      # Modulos CQ (workflow, ranking, cache, offline, etc.)
├── ui_components.js             # Componentes reutilizaveis
├── filial_context.js            # Contexto multi-filial
├── assets/                      # Imagens/logos
├── js/                          # Utilitarios JS adicionais
├── supabase/functions/          # Edge Functions (NFe SEFAZ)
├── migrations/                  # Schemas SQL
├── integracao_atak_cq/          # Migrations versionadas (0-16) + n8n workflows
├── etl/                         # Watch folders + config ETL
├── classic-rh-migration/        # Templates de migracao RH
├── tests/e2e/                   # Cypress specs
├── .github/workflows/ci.yml     # Pipeline CI
├── n8n/                         # Workflows n8n auxiliares
├── SISTEMA_CONTROLE_DE_QUALIDADE/ # Docs CQ
└── vercel.json                  # Deploy + rewrites
```

---

## Padroes de Codigo

### Modulo JS (IIFE pattern)
```javascript
const NomeModulo = (function() {
  let _estado = {};
  function _helper() {}
  async function metodoPublico() {}
  return { metodoPublico };
})();
```

### Contrato de resposta API
```javascript
// api.js e rh_api.js SEMPRE retornam:
{ ok: true/false, data: any, error: string, status: number }
// Nunca retorna array puro
```

### Bootstrap de pagina
1. Carrega `config.js` ou `config_rh.js`
2. Carrega `auth.js` / `cq_auth.js` ou `rh_auth.js`
3. Chama `auth.init()` → valida sessao → renderiza chip usuario
4. Carrega modulos especificos da pagina
5. DOM ready → inicializa com contexto do usuario

### CSS
- Design tokens em `:root` (cores, espacamento, tipografia)
- Mobile-first, breakpoints: 640px, 1024px
- Dark theme: `#080808` base, `#c8a96e` accent (dourado)
- Sem frameworks CSS (nao usa Tailwind/Bootstrap)

---

## Autenticacao

- JWT via Supabase Auth (email/senha)
- Tokens em localStorage: `cq_sess_v2` (CQ) / `rh_sess_v1` (RH)
- Heartbeat a cada 60s, expira 1h inatividade, alerta 55min
- Sessoes rastreadas em `cq_sessoes` / `rh_sessoes`
- Geolocalizacao para inspetores CQ mobile
- Audit log em `cq_audit_log` / `rh_audit_log`

---

## Roles/Perfis

Coluna `perfil` controla acesso: `administrador`, `qualidade`, `rh`, `gestor`, `operador`, etc.

---

## Banco de Dados - Tabelas Principais

### CQ
- `cadastros_atak` - Fornecedores/motoristas do ERP ATAK
- `cq_produtos` - Produtos (Omaso, Aorta, etc.)
- `cq_tipos_defeito` - Tipos de defeito
- `registros_cq_inspecao` - Registros de inspecao (11 colunas de defeito)
- `cq_fornecedores` - Fornecedores + score
- `cq_ranking` - Ranking historico

### RH
- `rh_colaboradores` - Funcionarios
- `rh_contratos` - Contratos
- `rh_setores` / `rh_cargos` - Departamentos/cargos
- `sst_catalogo_epi` - Catalogo de EPIs

### Workflow CQ
`rascunho` → `em_analise` → `aguardando_revisao` → `aprovada`

---

## Integracoes Externas

| Sistema   | Como                                    |
|-----------|-----------------------------------------|
| ATAK (ERP)| PDFs → parsers Python → Supabase       |
| OneDrive  | watch_folders.py monitora pastas        |
| SEFAZ     | Edge Function SOAP + certificado A1     |
| n8n       | Webhooks Supabase → scoring/notif       |

---

## ETL Pipeline

1. `watch_folders.py` monitora OneDrive a cada 300s
2. Detecta novos .xlsx/.xls/.pdf
3. Roteia para parser correto (entity_type)
4. Parser (common.py) → ParseResult com rows, logs, summary
5. Hash de arquivo previne reprocessamento
6. Arquivos movidos para `_processados/` ou `_erros/`
7. Threshold: >10% erros por linha = critico

---

## Deploy & CI/CD

### Vercel
- Producao: `app.classiccouros.com.br`
- Rewrite: `/rhesst/*` → `classic-rh.vercel.app`
- Redirect: `classiccouros.com.br` → `app.classiccouros.com.br`

### GitHub Actions (ci.yml)
1. SQL lint (sqlfluff)
2. Validacao ordem de migrations (0-16 sequencial)
3. Validacao JSON n8n
4. JS lint (node --check)
5. Security check (chaves service_role, .env)

---

## Convencoes de Commit

Usa commits semanticos em ingles:
```
feat: description
fix: description
refactor: description
improve: description
```

---

## Comandos Uteis

```bash
# Rodar testes E2E
npx cypress run --spec tests/e2e/login.spec.js

# Validar JS
node --check *.js

# Lint SQL
sqlfluff lint migrations/
```

---

## Regras para Claude

1. **Sempre leia o arquivo antes de editar** - nunca proponha mudancas sem ler o codigo atual
2. **Mantenha o padrao IIFE** para modulos JS
3. **Respeite o contrato de API** `{ ok, data, error, status }`
4. **Nunca exponha service_role keys** no frontend
5. **SQL migrations versionadas** - novas migrations devem seguir a numeracao existente
6. **Dark theme** - respeite os design tokens existentes
7. **Sem frameworks CSS** - use o design system custom (ds.css)
8. **Sem frameworks SPA** - paginas sao HTML com JS inline/modular
9. **Dois projetos Supabase** - nao misture CQ e RH
10. **Atualize este CLAUDE.md** ao final de sessoes com mudancas significativas

---

## Historico de Sessoes

### Sessao 1 - 2026-04-13
- Criacao do CLAUDE.md com contexto completo do projeto
- Mapeamento de arquitetura, stack, padroes e convencoes
