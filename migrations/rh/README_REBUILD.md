# Rebuild do Banco RH/SST — Documentacao Tecnica

**Versao:** 2.0  
**Homologado em:** 2026-04-13  
**Resultado:** APROVADO (0 erros, PostgreSQL 16 local)  
**Branch:** `claude/classic-ia-dev-sK92D`  
**Banco de producao:** `muiqmtnfvyffborgiwdw.supabase.co` (Rh Classic)

---

## Inventario Final Oficial

| Tipo | Quantidade |
|------|-----------|
| Arquivos SQL | 18 |
| Tabelas | 177 |
| Views | 30 |
| Functions | 67 |
| Triggers | 60 |
| Tabelas com RLS | 64 |
| Policies | 219 |
| Tabelas sensiveis expostas | 0 |

---

## Ordem de Execucao

| # | Arquivo | Dominio |
|---|---------|---------|
| 1 | `00_PREAMBULO.sql` | Ambiente (stubs, extensions, roles) |
| 2 | `FUNDACAO_BANCO_v2.sql` | RH Core (estrutura organizacional) |
| 3 | `00_schema_supabase.sql` | RH Core (colaboradores, ponto, custos) |
| 4 | `schema_rh_sst_completo.sql` | RH + SST (funcionarios, ASO, EPIs) |
| 5 | `MODULO_SST_v2.sql` | SST (acidentes, OS, treinamentos) |
| 6 | `schema_recrutamento_selecao.sql` | Recrutamento e Selecao |
| 7 | `schema_lideranca.sql` | Lideranca e Engajamento |
| 8 | `SEGURANCA_SUPABASE.sql` | Seguranca (RLS, RBAC, perfis) |
| 9 | `MODULO_DESLIGAMENTOS_TURNOVER.sql` | Desligamentos e Turnover |
| 10 | `MODULO_SAUDE_MENTAL.sql` | Saude Ocupacional |
| 11 | `TABELAS_RH_CORE.sql` | Processos, Ponto, Ferias, Documentos |
| 12 | `TABELAS_SST.sql` | Estoque EPI, Faturas, CIPA, eSocial |
| 13 | `TABELAS_GESTOR.sql` | Motor Score, Agentes IA, Orquestracao |
| 14 | `TABELAS_FINANCEIRO.sql` | Custos, CTE, Folha de Pagamento |
| 15 | `TABELAS_ETL.sql` | Staging, Integracao, Producao |
| 16 | `RLS_TABELAS_NOVAS.sql` | Patch RLS para tabelas sensiveis |
| 17 | `VIEWS_APLICACAO.sql` | Views de dashboard e absenteismo |
| 18 | `FUNCOES_APLICACAO.sql` | 18 RPCs do frontend |

---

## Justificativa Tecnica da Ordem

### Fase 1: Ambiente (script 1)

O preambulo cria dependencias que em Supabase existem nativamente mas em
PostgreSQL local nao: schema `auth`, funcoes `auth.uid()` e `auth.role()`,
tabela `auth.users`, roles `authenticated`/`anon`/`service_role`, e extensoes
`fuzzystrmatch` e `unaccent`. Tambem cria stub de `rh_perfil_atual()` para
que policies nos scripts seguintes possam referenciar essa funcao antes de
ela ser redefinida por SEGURANCA.

### Fase 2: Estrutura base (scripts 2-7)

Os scripts 2-4 criam as tabelas fundamentais (`rh_funcionarios`, `rh_setores`,
`rh_cargos`, `rh_empresas`, etc.) que sao referenciadas por FKs em todos os
modulos posteriores. A ordem entre eles e fixa: FUNDACAO cria a estrutura
organizacional, 00_schema cria colaboradores e views legadas,
schema_rh_sst_completo cria as tabelas SST com indexes.

O script 5 (MODULO_SST) depende de `sst_tipo_treinamento` e `sst_catalogo_epi`
ja existirem (criados por schema_rh_sst_completo).

Scripts 6-7 (recrutamento, lideranca) sao independentes entre si mas dependem
das tabelas base.

### Fase 3: Seguranca ANTES dos modulos avancados (script 8)

**Mudanca critica em relacao a versao anterior.** SEGURANCA foi antecipado para
a posicao 8 (antes era 14) porque:

- Define `has_perfil()`, `get_user_perfis()`, `get_meu_colaborador_id()` e
  `get_setores_lider()` — funcoes usadas em RLS policies de MODULO_SAUDE_MENTAL.
- Na versao anterior, SAUDE_MENTAL rodava antes de SEGURANCA, causando 29 erros
  por funcoes inexistentes.
- SEGURANCA tambem cria tabelas (`rh_asos`, `rh_perfis_acesso`, `rh_producao`)
  que sao referenciadas por views e indexes em scripts posteriores.

### Fase 4: Modulos avancados (scripts 9-10)

DESLIGAMENTOS e SAUDE_MENTAL agora rodam depois de SEGURANCA, eliminando
todas as dependencias circulares. DESLIGAMENTOS cria views que referenciam
`rh_setores` e `rh_cargos` via JOINs (corrigido de referencia direta a
coluna `setor` que nao existe em `rh_funcionarios`).

### Fase 5: Tabelas de aplicacao (scripts 11-15)

Cinco arquivos modulares por dominio funcional, cada um com `CREATE TABLE
IF NOT EXISTS`. Nao tem dependencias entre si e podem rodar em qualquer
ordem relativa. Foram separados do arquivo monolitico original para refletir
a arquitetura modular do hub.

### Fase 6: Seguranca complementar (script 16)

Patch dedicado que habilita RLS e cria policies para 11 tabelas criadas nos
scripts 11-15 que contem dados sensiveis. Roda apos as tabelas existirem.

### Fase 7: Views e RPCs (scripts 17-18)

Views dependem de todas as tabelas e de `motor_score` (criada no script 13).
RPCs dependem das views (ex: `motor_decisoes` usa `vw_absenteismo_recorrencia`)
e das tabelas de aplicacao. Por isso sao os ultimos.

---

## Objetivo de Cada Script

| # | Script | Objetivo |
|---|--------|----------|
| 1 | `00_PREAMBULO` | Tornar o rebuild reproduzivel fora do Supabase |
| 2 | `FUNDACAO_BANCO_v2` | Estrutura organizacional: empresas, filiais, setores, cargos, funcionarios, historico, audit |
| 3 | `00_schema_supabase` | Schema legado: colaboradores (view bridge), ponto, custos, deslocamento, apuracao |
| 4 | `schema_rh_sst_completo` | Schema unificado RH+SST: ASOs, treinamentos, EPIs, acidentes, ausencias, restricoes, saude mental |
| 5 | `MODULO_SST_v2` | Complemento SST: tipos treinamento, catalogo EPI, cargo-EPI, cargo-treinamento, nao conformidades |
| 6 | `schema_recrutamento_selecao` | Vagas, candidatos, pipeline, entrevistas, avaliacoes, banco de talentos |
| 7 | `schema_lideranca` | Metas, pulsos de engajamento, avaliacoes, rondas |
| 8 | `SEGURANCA_SUPABASE` | RBAC completo: funcoes de perfil, RLS granular por tabela, policies por operacao |
| 9 | `MODULO_DESLIGAMENTOS_TURNOVER` | Workflow de desligamento com entrevista estruturada, views de turnover e correlacao |
| 10 | `MODULO_SAUDE_MENTAL` | Saude ocupacional: sessoes, encaminhamentos, agenda, alertas, gatilhos automaticos |
| 11 | `TABELAS_RH_CORE` | Processos (admissao/desligamento/afastamento/movimentacao), ferias, beneficios, ponto, documentos |
| 12 | `TABELAS_SST` | Estoque EPI, faturas SST, ordens de servico, CIPA (mandatos/reunioes/membros), eSocial |
| 13 | `TABELAS_GESTOR` | Motor de score (execucao/scores/alertas/indicadores/padroes), agentes IA, orquestracao, acoes |
| 14 | `TABELAS_FINANCEIRO` | Custos colaborador, transporte (rateio/rotas), CT-e, folha de pagamento (lotes/lancamentos/rubricas) |
| 15 | `TABELAS_ETL` | Staging (Convenia/BuscaEPI/ATAK), ETL (importacao/mapa/depara), integracao (conectores/fila), producao, reconciliacao |
| 16 | `RLS_TABELAS_NOVAS` | RLS + policies para tabelas sensiveis dos scripts 11-15 |
| 17 | `VIEWS_APLICACAO` | 16 views: 3 absenteismo, 1 atestados, 12 motor (scores, setores, alertas, padroes, auditoria) |
| 18 | `FUNCOES_APLICACAO` | 18 RPCs: 12 dashboards, 1 CRUD (acompanhamento), 5 ETL (ingestores Python) |

---

## Pre-requisitos de Ambiente

### Em Supabase (producao)

Nenhum pre-requisito adicional. Todos os objetos de `auth` schema, roles e
extensoes ja existem nativamente. O script 1 (`00_PREAMBULO`) detecta o
ambiente e ignora stubs que ja existem.

### Em PostgreSQL local (rebuild/teste)

| Dependencia | Criado por | Observacao |
|-------------|-----------|-----------|
| Extension `fuzzystrmatch` | 00_PREAMBULO | Obrigatoria |
| Extension `unaccent` | 00_PREAMBULO | Obrigatoria |
| Extension `http` | 00_PREAMBULO | Opcional — falha silenciosa aceitavel |
| Schema `auth` | 00_PREAMBULO | Stub |
| Tabela `auth.users` | 00_PREAMBULO | Stub (referenciada por FKs) |
| Funcao `auth.uid()` | 00_PREAMBULO | Stub (retorna UUID zero) |
| Funcao `auth.role()` | 00_PREAMBULO | Stub (retorna 'authenticated') |
| Funcao `auth.jwt()` | 00_PREAMBULO | Stub (retorna JSON basico) |
| Role `authenticated` | 00_PREAMBULO | Stub (NOLOGIN) |
| Role `anon` | 00_PREAMBULO | Stub (NOLOGIN) |
| Role `service_role` | 00_PREAMBULO | Stub (NOLOGIN) |
| Funcao `rh_perfil_atual()` | 00_PREAMBULO | Stub (retorna 'administrador'), redefinida por SEGURANCA |

### Versao minima

- PostgreSQL 16+
- Supabase com PostgREST habilitado (para RPCs via REST)

---

## Criterio de Aprovacao do Rebuild

O rebuild so e considerado APROVADO quando **todos** os criterios abaixo
forem atendidos simultaneamente:

1. **Zero erros** durante a execucao dos 18 scripts em sequencia
2. **Zero dependencias quebradas** (views, triggers, functions, policies)
3. **Contagem de objetos consistente** com o inventario oficial
4. **Nenhuma tabela sensivel sem RLS** (lista de 11 tabelas criticas)
5. **RPCs do frontend integras** (pelo menos 3 testadas: sst_dashboard_completo,
   ferias_dashboard, contar_notificacoes)
6. **Nenhum ajuste manual** durante a execucao
7. **Todas as funcoes SECURITY DEFINER** com `search_path` explicito

---

## Principais Mudancas: v1.0 -> v2.0

### Estruturais

| Mudanca | Antes (v1.0) | Depois (v2.0) |
|---------|-------------|--------------|
| Erros no rebuild | 108 | 0 |
| Arquivos | 16 | 18 (+PREAMBULO, +RLS_TABELAS_NOVAS) |
| Tabelas versionadas | 63 | 177 (+114 exportadas de producao) |
| Views versionadas | 14 | 30 (+16 de aplicacao) |
| RPCs versionadas | 34 | 67 (+18 de aplicacao + correcoes) |
| Ordem: SEGURANCA | Posicao 14 | Posicao 8 (antecipada) |
| Tabelas modulares | 1 arquivo monolitico | 5 por dominio funcional |

### Seguranca

| Mudanca | Antes | Depois |
|---------|-------|--------|
| Tabelas com RLS | 36 | 64 |
| Policies | 69 | 219 |
| Tabelas sensiveis expostas | 11 | 0 |
| SECURITY DEFINER sem search_path | 20 | 0 |

### Correcoes em scripts legados

| Script | Correcoes aplicadas |
|--------|-------------------|
| `FUNDACAO_BANCO_v2` | IF NOT EXISTS em CREATEs, DROP TRIGGER IF EXISTS, DROP POLICY IF EXISTS, funcoes movidas apos tabelas, secao destrutiva desativada, seeds idempotentes |
| `00_schema_supabase` | IF NOT EXISTS em indexes, ON CONFLICT DO NOTHING em seeds, colunas setor/setor_id/auth_uid em rh_colaboradores |
| `schema_rh_sst_completo` | Index corrigido: setor -> setor_id |
| `SEGURANCA_SUPABASE` | DROP POLICY IF EXISTS em todas as policies, colaborador_id -> funcionario_id em treinamentos/epi, column refs corrigidas em indexes, dynamic policies idempotentes |
| `MODULO_DESLIGAMENTOS_TURNOVER` | f.setor -> s.nome com JOINs a rh_setores, f.cargo -> c.nome com JOINs a rh_cargos, 5 views reescritas com FROM correto |
| `MODULO_SAUDE_MENTAL` | DROP POLICY/TRIGGER IF EXISTS |
| `FUNCOES_APLICACAO` | SET search_path em 13 funcoes SECURITY DEFINER, correcao de contar_notificacoes (bug ORDER BY) |

---

## Governanca

### Adicionar novas tabelas

1. Identificar o dominio (RH Core, SST, Gestor, Financeiro, ETL)
2. Adicionar o CREATE TABLE IF NOT EXISTS no arquivo TABELAS_*.sql correspondente
3. Se contem dados sensiveis, adicionar RLS + policy em RLS_TABELAS_NOVAS.sql
4. Rodar rebuild de teste para validar

### Adicionar novas RPCs

1. Adicionar CREATE OR REPLACE FUNCTION em FUNCOES_APLICACAO.sql
2. Incluir SET search_path TO 'public' se for SECURITY DEFINER
3. Documentar qual pagina do frontend chama a RPC
4. Rodar rebuild de teste para validar

### Adicionar novas views

1. Verificar se todas as tabelas referenciadas existem nos scripts anteriores
2. Adicionar em VIEWS_APLICACAO.sql respeitando ordem de dependencia
3. Rodar rebuild de teste para validar

### Alterar ordem de execucao

A ordem e determinada por dependencias. Qualquer mudanca requer:

1. Justificativa tecnica documentada
2. Rebuild de teste com 0 erros
3. Atualizacao de ORDEM_REBUILD.sql e deste README

---

## Rastreabilidade

| Commit | Descricao |
|--------|----------|
| `35ecad6` | feat: unify classic-rh into classic-ia monorepo |
| `bd02a6b` | feat: phase 2 — sanitation, RPC versioning, dedup |
| `5ff46be` | feat: phase 3.1 — decompose monolith, version views |
| `2e239df` | feat: modular table migrations + rebuild validation |
| `b098397` | fix: rebuild passes with 0 errors |
| `ce80ea9` | docs: add ORDEM_REBUILD.sql |
