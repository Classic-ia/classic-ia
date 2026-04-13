# Classic IA — Contexto do Projeto

## Arquitetura

- **Frontend:** HTML/JS/CSS estático, deploy via Vercel (auto-deploy do GitHub main)
- **Backend:** 2 projetos Supabase (PostgREST + RLS)
  - **RH/SST:** `muiqmtnfvyffborgiwdw.supabase.co` (config_rh.js)
  - **CQ:** `nvqxsulntpftcwtkjedu.supabase.co` (config.js)
- **Dominio:** app.classiccoouros.com.br
- **Empresa:** Classic Importacao e Exportacao de Couros EIRELI, Jardim Alegre-PR
- **CNPJ:** 08.849.964/0001-10
- **Funcionarios:** 133 ativos, 10 afastados, ~483 desligados (629 total)

## Infraestrutura Compartilhada (RH)

- `shell.js` — Topbar-only (sidebar removido em abril/2026), breadcrumb + user + logout
- `ds.css` — Design system (variáveis CSS, componentes, responsivo 768px + 480px)
- `api.js` — API layer padronizado para módulo RH (get/post/patch/delete/rpc)
- `rh_api.js` — API wrapper legacy (usa SB_URL direto, agora com toast de erro)
- `ui_components.js` — Componentes reutilizáveis (table, kpi, toast, modal, badge)
- `rh_auth.js` — Auth JWT com refresh, perfis: administrador, rh, gestor, diretoria, sst
- `filial_context.js` — Contexto multi-filial (localStorage)
- `home.html` — Hub principal com cards agrupados por role

## Módulos Principais

### RH & SST
- `gestao_rh_sst.html` — Funcionários, ASO, treinamentos, EPIs, acidentes (paginação 50, sort clicável)
- `ferias.html` — Controle de férias
- `beneficios.html` — Gestão de benefícios
- `epis.html` — Gestão de EPIs
- `cipa.html` — CIPA
- `saude_mental.html` — Saúde ocupacional / NR-1
- `lideranca.html` — Avaliações, metas, engajamento (módulo "soft")
- `recrutamento_selecao.html` — Recrutamento

### Dashboards
- `dashboard_custos.html` — CT-e + custos pessoal + transporte colaboradores (evolução mensal)
- `dashboard_sst.html` — KPIs SST
- `painel_conformidade.html` — Conformidade SST
- `painel_gestor.html` — Decisões operacionais, score de risco, ações rastreáveis
- `monitor-esocial.html` — eSocial
- `ficha_360.html` — Ficha completa do colaborador

### Importadores
- `importar_custos.html` — Planilha Excel de custos (aliases: deslocamento, assiduidade)
- `importar_folha.html` — PDF de folha de pagamento (Folhamatic), cola texto
- `importar_ponto.html` — AFD (cola texto) ou planilha Secullum (.xlsx)
- `importar_cte.html` — CT-e

## Banco de Dados (Supabase RH)

### Tabelas Criticas
- `rh_funcionarios` — 629 registros, campos: pis_pasep(14), matricula, cargo_id, setor_id
- `rh_custo_colaborador` — Custos mensais (salario, HE, deslocamento, assiduidade, FGTS)
- `rh_ponto` — Registros diários de ponto (batidas, HE, faltas, noturno)
- `rh_acoes` — Ações rastreáveis do painel do gestor (resolver/justificar/delegar)
- `rh_ferias` — Controle de férias
- `rh_audit_trail` — Auditoria automática (triggers em 5 tabelas)

### RPCs Importantes
- `painel_gestor_dados()` — KPIs, score de risco, vencimentos, experiência, afastados
- `custo_transporte_consolidado(p_from, p_to)` — Custos transporte por rota/mês
- `salvar_colaborador(...)` — Upsert de funcionário (aceita p_pis_pasep, p_apac, p_utiliza_transporte)
- `hub_resumo(p_filial_id)` — Resumo para home.html

### Score de Risco
```
Score = (faltas_90d * 8) + (advertencias_90d * 15) + (atestados_90d * 5) + (experiencia * 10)
```
Tabela de ocorrências: `rh_acidentes` (campo `tipo`, não `tipo_acidente`; campo `criado_em`, não `created_at`)

### Nomes de Colunas (cuidado)
- `rh_acidentes`: tipo (não tipo_acidente), criado_em (não created_at), funcionario_id
- `rh_funcionarios`: nome_completo, cargo_id (FK rh_cargos), setor_id (FK rh_setores)
- `sst_aso`: data_validade (não data_vencimento), funcionario_id
- `rh_treinamentos`: treinamento (não nome_treinamento), colaborador_id, data_vencimento
- `rh_aso` e `rh_treinamentos` são VIEWS, não tabelas (não criar índices nelas)

## Transporte

- 5 rotas ativas: R-SPI-01 (São Pedro/São João, 14 pass), R-GOD-01 (Godoy, 11), R-IVA-01 (Ivaiporã, 9), R-LID-01, R-LUN-01
- R-SJI-01 desativada (unificada com R-SPI-01, mesmo ônibus)
- CT-e São Pedro: R$14.000/mês (cobre São Pedro + São João)
- CT-e Godoy: ~R$10.000/mês, Ivaiporã: R$6.000/mês

## Ponto Eletrônico (Secullum)

- Sistema: Secullum Ponto Web (Pontual Maringá)
- Batida: biometria
- Identificação: Nº Folha (campo matricula no banco)
- AFD: usa PIS (só funciona para quem tem PIS cadastrado — 92 de 133)
- Planilha: identifica por Nº Folha/nome (115 de 133 com matrícula)
- **PENDENTE:** Parser da planilha Excel do Secullum ainda não funciona (formato muito largo)

## Afastados (atualizado abril/2026)

- 8 afastados INSS doença + 2 licença maternidade
- Henrique Schawan Martins: acidente de trajeto, sem previsão de retorno
- Dora Maria e Henrique: retorno em abril/2026

## Decisões Arquiteturais

- Sidebar removido → topbar-only com seta de voltar ao hub
- Painel do Gestor separado do Liderança (hard vs soft)
- custo_total da folha = Total Proventos (não soma de campos)
- outros_proventos = residual (total - campos conhecidos)
- Alias 'proventos' movido para custo_total (não outros_proventos)
- Audit trail genérico com fn_audit_trail() reutilizável

## Pendências (próximas sessões)

1. **Parser planilha ponto Secullum** — formato muito largo, precisa debug
2. **Backup do banco** — página para admin baixar JSON das tabelas críticas
3. **Organizar migrations SQL** — 80+ arquivos na raiz sem ordem
4. **Unificar auth CQ + RH** — dois sistemas paralelos
5. **Integração documentos OneDrive** — 3.732 docs catalogados
6. **Cadastrar PIS faltantes no Secullum** — pedir ao escritório contábil
7. **Paginação real com offset** — hoje só "carregar mais" no client

## Git

- Branch de trabalho: `claude/deploy-vacation-module-1qap4`
- Push sempre para `main` (auto-deploy Vercel)
- Commits com link da sessão no final
