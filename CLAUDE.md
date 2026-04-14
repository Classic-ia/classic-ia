# Classic IA — Contexto do Projeto

## Arquitetura

- **Frontend:** HTML/JS/CSS estático, deploy via Vercel (auto-deploy do GitHub main)
- **Backend:** 2 projetos Supabase (PostgREST + RLS)
  - **RH/SST:** `muiqmtnfvyffborgiwdw.supabase.co` (rh/config.js)
  - **CQ:** `nvqxsulntpftcwtkjedu.supabase.co` (config.js)
- **Dominio:** app.classiccouros.com.br
- **Empresa:** Classic Importacao e Exportacao de Couros EIRELI, Jardim Alegre-PR
- **CNPJ:** 08.849.964/0001-10
- **Funcionarios:** 133 ativos, 10 afastados, ~483 desligados (629 total)

## Estrutura do Monorepo (pos-unificacao abril/2026)

```
classic-ia/
├── rh/                          # Modulo RH & SST (57 paginas)
│   ├── config.js                # Config Supabase RH
│   ├── rh_auth.js               # Auth RH (v2, rh_sess_v2)
│   ├── api.js                   # API layer (contrato {ok,data,error,status})
│   ├── shell.js                 # Sidebar + topbar + 46 rotas
│   ├── ds.css                   # Design system (42 secoes, 2141 linhas)
│   ├── ui_components.js         # 22 componentes reutilizaveis
│   ├── page_bootstrap.js        # Lifecycle de pagina
│   └── *.html                   # 57 paginas RH
├── migrations/rh/               # 28 arquivos SQL versionados
│   ├── ORDEM_REBUILD.sql        # Sequencia oficial (18 scripts)
│   ├── README_REBUILD.md        # Documentacao de governanca
│   ├── 00_PREAMBULO.sql         # Stubs Supabase para rebuild local
│   ├── FUNCOES_APLICACAO.sql    # 19 RPCs do frontend
│   ├── VIEWS_APLICACAO.sql      # 16 views de dashboard
│   ├── INDEXES_APLICACAO.sql    # 12 unique indexes
│   ├── RLS_TABELAS_NOVAS.sql    # RLS para 11 tabelas sensiveis
│   ├── TABELAS_RH_CORE.sql     # 17 tabelas (processos, ponto, ferias)
│   ├── TABELAS_SST.sql         # 18 tabelas (estoque, CIPA, eSocial)
│   ├── TABELAS_GESTOR.sql      # 24 tabelas (motor, agentes, orch)
│   ├── TABELAS_FINANCEIRO.sql  # 16 tabelas (custos, CTE, folha)
│   └── TABELAS_ETL.sql         # 39 tabelas (staging, integ, producao)
├── ds.css                       # Design system (copia para CQ)
├── config.js                    # Config Supabase CQ
├── config_rh.js                 # Config Supabase RH (legacy, raiz)
├── *.html                       # 53 paginas CQ + 2 RH legacy
├── supabase/functions/          # Edge Functions (NFe/SEFAZ)
├── n8n/                         # Docker + workflows n8n
├── etl/                         # Pipeline Python ETL
└── vercel.json                  # Routing (rh/ local, /rhesst/ redirect)
```

## Design System (ds.css)

- **Tema:** Claro corporativo (#F1F5F9 bg, #FFFFFF surface, #2563EB primary)
- **Fonte:** Inter (unica)
- **42 secoes:** reset, layout, sidebar, topbar, cards, KPIs, buttons, forms,
  tables, badges, tabs, modal, drawer, empty/loading states, breadcrumb,
  pagination, filter bar, action bar, form sections, confirm dialog, etc.
- **110 paginas** usam ds.css exclusivamente (0 CSS inline)

## Componentes (ui_components.js)

22 funcoes reutilizaveis:
- **Legacy:** table, kpiCard, skeleton, spinner, loadingCenter, emptyState,
  toast, badge, modal, openModal, closeModal, progressBar, card
- **ERP (config-driven):** pageHeader, actionBar, filterBar, dataTable,
  pagination, formSection, kpiGrid, confirmDialog, statusBadge

## Banco de Dados RH (rebuild homologado)

- **177 tabelas** | 30 views | 67+ functions | 60 triggers
- **64 tabelas com RLS** | 219 policies | 0 tabelas sensiveis expostas
- **Rebuild:** 18 scripts, 0 erros, ordem em ORDEM_REBUILD.sql
- **RPCs:** 19 validadas com dados reais e perfis simulados

## Modulos RH (rh/)

### Pessoas
- `colaboradores.html` — CRUD funcionarios (629 registros)
- `recrutamento_selecao.html` — Vagas, candidatos, pipeline

### Processos
- `ocorrencias.html` — Atestados, advertencias
- `absenteismo.html` — Faltas, licencas, INSS (NOVO)
- `apuracao.html` — Contratos experiencia
- `desligamentos_turnover.html` — Analise de turnover

### SST
- `sst_dashboard.html` — KPIs SST (ASOs, treinamentos, EPIs, acidentes)
- `asos.html` — Exames ocupacionais
- `treinamentos.html` — NRs e treinamentos
- `epis.html` — Gestao de EPIs (6 tabs)
- `acidentes.html` — Registro de acidentes
- `restricoes_medicas.html` — Restricoes medicas (NOVO)
- `cipa.html` — CIPA (mandatos, reunioes, membros)
- `monitor-esocial.html` — eSocial

### Operacional
- `importar_secullum.html` — Ponto (planilha Secullum)
- `beneficios.html` — VA, deslocamento, alianca
- `ferias.html` — Controle de ferias
- `dashboard_custos.html` — CT-e + custos pessoal

### Gestao
- `motor_v2.html` — Motor Score v2 (133 funcionarios)
- `motor_decisoes.html` — Motor de decisao (absenteismo, SST)
- `alertas_rh.html` — Pendencias e planos de acao
- `notificacoes.html` — Central de notificacoes
- `lideranca.html` — Avaliacoes e metas

### Inteligencia
- `dashboard_gerencial.html` — Dashboard executivo
- `motor_v2_auditoria.html` — Auditoria de scores
- `relatorio_atestados.html` — Relatorio de atestados (CID, medico, setor)

## Integracoes Externas

| Sistema | Tipo | Status | Heartbeat |
|---------|------|--------|-----------|
| Convenia | REST API (n8n) | Funcional 85% | Ativo (a cada 65min) |
| BuscaEPI | REST API (n8n) | Parcial 60% | Verificar n8n |
| Secullum | File upload (XLSX) | Funcional 80% | Manual |
| ATAK | REST API (n8n) | Funcional | Via n8n workflows |
| SEFAZ | Edge Function (mTLS) | Nao deployado | Requer certificado A1 |

## Convencoes

- Idioma do codigo: portugues (variaveis, tabelas, comentarios)
- Prefixo tabelas: `rh_` (RH), `sst_` (SST), `cq_` (CQ), `stg_` (staging)
- Parametros RPC: prefixo `p_` (ex: `p_funcionario_id`)
- Campo nome: `nome_completo` em rh_funcionarios, `nome` em rh_colaboradores (view)
- Datas: DD/MM/YYYY no display, ISO no banco
- IDs: UUID com `gen_random_uuid()`
- Auth: JWT via Supabase Auth, perfis em `rh_usuarios`
- CSS: ds.css exclusivo (PROIBIDO CSS inline)
- Componentes: ui_components.js (PROIBIDO HTML duplicado)
- Font: Inter (PROIBIDO Syne, DM Sans, DM Mono)

## Git

- Branch de trabalho: `claude/classic-ia-dev-sK92D`
- Push para `main` faz auto-deploy Vercel
- Commits com link da sessao no final

## Pendencias

1. **Homologacao browser** — Testar todas as telas em browser real (localhost:3000)
2. **BuscaEPI n8n** — Verificar se sync esta ativo (heartbeat 5 dias atras)
3. **Convenia ferias/afastamentos** — Endpoints existem mas ETL nao sync
4. **Parser AFD Secullum** — 41 funcionarios sem matricula precisam AFD por PIS
5. **gestao_rh_sst.html** — Concentrador legado na raiz, desligamento workflow exclusivo
6. **Padronizacao CQ** — Mesma infra (ds.css + componentes) ja aplicada, falta Shell.init
