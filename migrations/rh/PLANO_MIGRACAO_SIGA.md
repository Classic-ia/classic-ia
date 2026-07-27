# Plano de Migracao: Classic RH → Modulo RH/SST do SIGA

**Data:** 2026-07-27
**Referencia:** SIGA Guia Tecnico Definitivo v3.0
**Baseline:** Classic RH atual (branch `claude/consolidate-hr-frontend-NkdCK`)

## Inventario do sistema atual

| Metrica | Quantidade |
|---|---|
| Paginas HTML | 61 |
| Arquivos JS compartilhados | 7 (config, auth, api, shell, ui_components, page_bootstrap, rh_api) |
| Design system | 1 CSS (ds.css, 2811 linhas) |
| Linhas de JS em paginas HTML | 26.388 |
| Migrations SQL | 32 arquivos |
| RPCs (FUNCOES_APLICACAO) | 20 |
| RLS policies | 180 |
| Views | 37 |
| Tabelas COM empresa_id | 11 |
| Tabelas SEM empresa_id | 169 |
| Funcionarios reais no banco | 629 |
| Tabelas em producao | 272 |
| Views em producao | 91+ (nao versionadas) |

## Premissas

1. O SIGA sera construido como sistema novo com fundacao propria (PostgreSQL 15+, Node.js 20+)
2. O Classic RH atual e **fonte de dominio e dados** — nao e a fundacao do SIGA
3. O frontend atual (HTML estatico + PostgREST) sera substituido por frontend que consome API v1 do SIGA
4. Os 629 funcionarios reais serao migrados para o schema SIGA
5. O Supabase RH atual continua operando durante a transicao (cutover planejado)

## Fases

### Fase 0 — Fundacao SIGA (pre-requisito, antes de qualquer modulo)

**Estimativa:** 1-2 semanas
**Dependencia:** nenhuma (greenfield)

| Item | Acao | Fonte Classic RH |
|---|---|---|
| Migrations 001-007 | Criar conforme guia SIGA (grupo, empresas, estabelecimentos, centros_custo, usuarios, concessoes_acesso, auditoria_log, integracoes_externas) | `rh_empresas` (FUNDACAO_BANCO_v2:89), `rh_filiais` (:125), `rh_centros_custo` (:141), `rh_usuarios` (:204), `rh_audit_log` (:577) — aproveitar dados, refazer DDL |
| Seed 6 empresas | Inserir Classic, Trans, J.A., Confianca, Mundial, Prensas | CNPJs ja confirmados no SIGA v3 |
| Seed 8 estabelecimentos | Inserir 3 Classic + 1 por empresa | `rh_filiais` em prod tem dados reais |
| `tem_acesso()` | Criar funcao + indices | Substituir `has_perfil()` / `rh_perfis_acesso` |
| Auditoria imutavel | PARTITION BY RANGE + CREATE RULE no_update/no_delete | Migrar dados do `rh_audit_log` atual |
| Extensoes | pgcrypto, uuid-ossp, pg_stat_statements | uuid-ossp ja ativa |
| .env.example | Criar com todas as variaveis SIGA | `rh/config.js` tem SB_URL e SB_KEY |

**Criterio de saida:** Simulacao 1 do SIGA (empresa 7 sem codigo novo) passa.

---

### Fase 1 — Entidades corporativas (funcionarios, veiculos, fornecedores)

**Estimativa:** 1-2 semanas
**Dependencia:** Fase 0 concluida

| Item | Acao | Fonte Classic RH |
|---|---|---|
| `funcionarios` | Tabela com CPF criptografado (pgcrypto) | `rh_funcionarios` (FUNDACAO_BANCO_v2:230) — 41 colunas, empresa_id ja existe |
| `funcionario_vinculo` | Vinculo por empresa + estabelecimento | Derivar de `rh_funcionarios.status` + `empresa_id` + `filial_id` |
| `funcionario_dados_restritos` | Salario + conta bancaria criptografados | `rh_funcionarios.salario_base` + `rh_dados_bancarios` (se existir em prod) |
| `funcionario_saude` | Tabela separada para ASO, exames, PPP | Unificar `rh_asos` + `sst_aso` + dados de `rh_saude_mental` |
| Migracao de dados | Script ETL: 629 funcionarios → novo schema | Query de producao via PostgREST |

**Mapeamento de colunas (rh_funcionarios → SIGA):**

| Classic RH | SIGA `funcionarios` | SIGA `funcionario_vinculo` | Acao |
|---|---|---|---|
| `id` | `id` | — | manter UUID |
| `nome_completo` | `nome` | — | renomear |
| `cpf` | `cpf` (criptografado) | — | criptografar com pgcrypto |
| `data_nascimento` | `data_nascimento` | — | copiar |
| `empresa_id` | — | `empresa_id` | mover para vinculo |
| `filial_id` | — | `estabelecimento_id` | renomear |
| `setor_id` | — | — | manter como FK (setor pode ser vinculado ao vinculo) |
| `cargo_id` | — | `cargo` (desnormalizado ou FK) | mapear |
| `tipo_vinculo` | — | `tipo_vinculo` | copiar (CLT/APAC/PJ/Terceiro → valores SIGA) |
| `data_admissao` | — | `data_admissao` | mover para vinculo |
| `data_desligamento` | — | `data_desligamento` | mover para vinculo |
| `status` | — | `ativo` (boolean) | mapear: ativo/experiencia/ferias → true; desligado → false |
| `salario_base` | — | — em `funcionario_dados_restritos` | criptografar |

**Criterio de saida:** Funcionario com 2 vinculos (Classic + Trans) aparece 1x em `funcionarios` e 2x em `funcionario_vinculo`.

---

### Fase 2 — Seguranca e LGPD

**Estimativa:** 1 semana
**Dependencia:** Fase 1 concluida

| Item | Acao | Impacto |
|---|---|---|
| Criptografia CPF | `pgp_sym_encrypt(cpf, ENCRYPTION_KEY)` em todas as insercoes | 629 registros a migrar |
| Criptografia conta bancaria | idem para `rh_dados_bancarios` | dados sensiveis |
| Capacidades RH | Criar catalogo: `rh.funcionario.consultar`, `rh.salario.consultar`, `rh.admissao.aprovar`, `sst.aso.consultar`, `sst.saude.consultar`, `sst.ppp.emitir` | Mapear de perfis atuais |
| Mascaramento na API | Usuarios sem `rh.salario.consultar` recebem `salario: "***"` | Middleware Node.js |
| Dados de saude: acesso restrito | `sst.saude.consultar` necessario para ASO, PPP, afastamento | Separacao por tabela ja parcial |
| Auditoria sanitizada | Remover CPF e salario do `dados_novos` JSONB no audit log | Funcao `sanitizar_para_log()` |

**Mapeamento de perfis Classic RH → capacidades SIGA:**

| Perfil Classic | Capacidades SIGA |
|---|---|
| `administrador` | `admin.*`, `rh.*`, `sst.*`, `financeiro.*` |
| `rh` | `rh.funcionario.*`, `rh.salario.*`, `rh.admissao.*`, `sst.aso.*` |
| `gestor` | `rh.funcionario.consultar`, `sst.aso.consultar` (so equipe) |
| `visualizador` | `rh.funcionario.consultar` (dados mascarados) |
| `sst` (prod-only) | `sst.*` |

**Criterio de saida:** T07 do SIGA (acesso a dado de saude sem SESMT → 403) passa.

---

### Fase 3 — API Node.js (/api/v1/)

**Estimativa:** 3-4 semanas
**Dependencia:** Fase 2 concluida

Esta e a fase mais trabalhosa. O Classic RH tem 247 queries PostgREST distribuidas em 61 paginas HTML. Cada uma precisa virar um endpoint na API Node.js com `requerAcesso()`.

| Subgrupo | Endpoints | Queries Classic RH a migrar |
|---|---|---|
| RH Core | `GET/POST /rh/funcionarios`, vinculos, historico | ~40 queries em colaboradores.html, ficha.html, ficha_360.html |
| SST | `GET/POST /sst/asos`, treinamentos, EPIs, acidentes | ~50 queries em asos.html, treinamentos.html, epis.html, acidentes.html, sst_dashboard.html |
| Processos | ocorrencias, ferias, ponto, beneficios, desligamentos | ~40 queries em 6 telas |
| Gestao | motor_score, alertas, planos de acao, decisoes | ~30 queries em motor_v2.html, alertas_rh.html, motor_decisoes.html |
| Dashboards | KPIs, graficos, relatorios | ~50 queries em 8 telas de dashboard |
| Inteligencia | auditoria, relatatorio_atestados, absenteismo | ~20 queries |
| Admin | usuarios, integracoes, convenia, importadores | ~17 queries |

**Estrutura proposta:**

```
siga/
├── modules/
│   └── rh/
│       ├── routes.js              (endpoints /api/v1/:empresa_id/rh/...)
│       ├── routes_sst.js          (endpoints /api/v1/:empresa_id/sst/...)
│       ├── service.js             (logica de negocio)
│       ├── repository.js          (queries SQL parametrizadas)
│       ├── repository_sst.js
│       ├── schema.sql             (referencia das tabelas do modulo)
│       ├── README.md
│       └── rh.test.js
```

**20 RPCs existentes a migrar para a API:**

| RPC Classic | Endpoint SIGA | Acao |
|---|---|---|
| `rh_meu_perfil` | `GET /api/v1/auth/me` | Mover para core/auth |
| `sst_dashboard_completo` | `GET /api/v1/:eid/sst/dashboard` | Mover para modules/rh |
| `ferias_dashboard` | `GET /api/v1/:eid/rh/ferias/dashboard` | idem |
| `gestor_dashboard` | `GET /api/v1/:eid/rh/gestor/dashboard` | idem |
| `rh_registrar_decisao_experiencia` | `POST /api/v1/:eid/rh/decisao-experiencia` | ja pronto |
| `rh_listar_decisoes_experiencia` | `GET /api/v1/:eid/rh/decisao-experiencia` | ja pronto |
| ... (14 restantes) | ... | migrar 1 a 1 |

**Criterio de saida:** T01-T06 do SIGA passam. Endpoint de listagem filtra por empresa_id.

---

### Fase 4 — Frontend (consumir API v1)

**Estimativa:** 3-4 semanas
**Dependencia:** Fase 3 concluida (API estavel)

| Decisao | Opcoes | Recomendacao |
|---|---|---|
| Stack frontend | A) Manter HTML estatico, trocar PostgREST por fetch na API v1 | **Opcao A** — menor risco, aproveita 26K linhas de JS ja testadas |
| | B) Migrar para React/Vue/Svelte | Opcao B quando houver equipe frontend dedicada |
| Design system | Manter ds.css (2811 linhas) | Ja validado, coeso, responsivo |
| Auth | Trocar Supabase Auth → JWT do SIGA | `rh_auth.js` refatorado para chamar `/api/v1/auth/login` |
| Shell | Manter shell.js com rotas | Adicionar seletor de empresa no topbar |

**Mudancas no frontend por tela:**

| Bloco de telas | Qtd | Mudanca principal |
|---|---|---|
| Login + Hub | 2 | Chamar API SIGA em vez de Supabase Auth |
| Dashboards (app, gerencial, sst, custos, rh) | 6 | Trocar `API.get('tabela?...')` por `fetch('/api/v1/'+eid+'/rh/...')` |
| CRUD (colaboradores, asos, treinamentos, epis, etc) | 15 | Trocar GET/POST/PATCH |
| Relatorios/BI (motor, auditoria, absenteismo) | 10 | Trocar GET |
| Importadores (secullum, custos, cte, folha) | 6 | Trocar POST |
| Configuracao (integracoes, convenia, usuarios) | 5 | Trocar GET/POST |
| Fichas (ficha, ficha_360, organograma) | 4 | Trocar GET |
| SST avancado (cipa, dds, extintores, inspecoes, restricoes) | 8 | Trocar GET/POST |
| Outros | 5 | Avaliar caso a caso |
| **Total** | **61** | |

**Padrao de refatoracao por pagina:**

```javascript
// ANTES (Classic RH — PostgREST direto)
const res = await API.get('rh_colaboradores?status=eq.ativo&select=id,nome,cargo');

// DEPOIS (SIGA — API v1 com empresa no path)
const eid = Shell.getEmpresaId(); // seletor no topbar
const res = await fetch(`/api/v1/${eid}/rh/funcionarios?status=ativo`);
const data = await res.json(); // {ok, data, meta}
```

**Criterio de saida:** Usuario logado com acesso a Classic ve so funcionarios da Classic. Troca empresa no topbar → ve funcionarios da outra.

---

### Fase 5 — Testes e governanca

**Estimativa:** 1-2 semanas (em paralelo com Fase 4)
**Dependencia:** Fase 3 concluida

| Item | Qtd | Fonte |
|---|---|---|
| Testes T01-T10 (isolamento multiempresa) | 10 | SIGA v3 secao 11.1 |
| Testes B01-B06 (integridade banco) | 6 | SIGA v3 secao 11.2 |
| Testes unitarios por modulo | ~20 | Endpoints criticos do RH |
| Simulacao 1 (empresa 7) | 1 | SIGA v3 secao 12 |
| Simulacao 2 (modulo novo) | 1 | SIGA v3 secao 12 |
| ADRs | 3-5 | Decisoes de migracao |
| README por modulo | 1 (modules/rh/) | Endpoints, tabelas, regras |
| Seeds | 4-6 | Grupo, empresas, estabelecimentos, admin, capacidades |
| Numeracao de migrations | Renumerar 32 → 001-0XX | ORDEM_REBUILD como guia |

**Criterio de saida:** `npm test` → T01-T10 e B01-B06 passam 100%.

---

### Fase 6 — Migracao de dados e cutover

**Estimativa:** 1 semana
**Dependencia:** Fases 0-5 concluidas

| Passo | Acao | Risco |
|---|---|---|
| 1 | Exportar dados do Supabase RH atual (629 funcionarios, ASOs, treinamentos, ocorrencias, etc) | Volume baixo — queries simples |
| 2 | Script ETL: transformar schema Classic → schema SIGA | Mapeamento ja documentado (Fase 1) |
| 3 | Criptografar CPF e dados bancarios durante ETL | Irreversivel — validar antes |
| 4 | Carregar no banco SIGA | Validar contagem: 629 funcionarios, N ASOs, N treinamentos |
| 5 | Smoke test completo (61 telas) | Usuario real valida |
| 6 | DNS: apontar app.classiccouros.com.br → SIGA | Cutover definitivo |
| 7 | Desligar Supabase RH (ou manter read-only por 30 dias) | Rollback window |

---

## Cronograma consolidado

| Fase | Semana | Entregavel |
|---|---|---|
| 0 — Fundacao SIGA | S1-S2 | Banco fundacional + seed + tem_acesso() |
| 1 — Entidades | S3-S4 | funcionarios, vinculos, veiculos, fornecedores |
| 2 — Seguranca | S5 | LGPD, capacidades, mascaramento |
| 3 — API v1 | S6-S9 | ~50 endpoints, 20 RPCs migradas |
| 4 — Frontend | S10-S13 | 61 telas adaptadas |
| 5 — Testes | S10-S13 (paralelo) | 16+ testes, ADRs, seeds, README |
| 6 — Cutover | S14 | Migracao de dados + DNS |
| **Total** | **~14 semanas** | **Modulo RH/SST do SIGA operacional** |

## O que aproveitar do Classic RH (ROI da base existente)

| Ativo | Linhas/itens | % aproveitavel |
|---|---|---|
| Schema `rh_funcionarios` (41 colunas) | 80 linhas SQL | ~90% (renomear + adicionar crypto) |
| 20 RPCs | 1200 linhas SQL | ~70% (adaptar empresa_id) |
| 37 views de BI | 600 linhas SQL | ~80% (adicionar filtro empresa) |
| ds.css (design system) | 2811 linhas CSS | 100% (reutilizar intacto) |
| shell.js + ui_components.js | 741 linhas JS | ~90% (adicionar empresa selector) |
| Logica de negocio nas 61 telas | 26.388 linhas JS | ~60% (trocar API.get → fetch v1) |
| 180 RLS policies | 180 policies | ~30% (reescrever com empresa filter) |
| 629 funcionarios + dados reais | dados de producao | 100% (migrar via ETL) |

## Riscos

| Risco | Probabilidade | Impacto | Mitigacao |
|---|---|---|---|
| Duplicidade de dados durante transicao | Alta | Medio | Periodo de freeze: parar insercoes no Classic 48h antes do cutover |
| Schema SIGA diverge do guia durante construcao | Media | Alto | Code review contra checklist do SIGA v3 secao 16 |
| Frontend com 26K linhas de JS tem bugs ocultos | Media | Medio | Testes E2E nas 10 telas mais criticas |
| Perfis granulares causam bloqueio de usuarios reais | Media | Alto | Mapeamento conservador (conceder capacidades generosas, refinar depois) |
| Supabase RH fica indisponivel durante cutover | Baixa | Alto | Backup completo + modo read-only antes do switch |
