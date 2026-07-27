# Drift map — Repo × Produção (Supabase RH)

**Data da medição:** 2026-04-15
**Banco analisado:** `muiqmtnfvyffborgiwdw.supabase.co` (RH)
**Método:** `information_schema.tables` em produção vs rebuild local do repo (167 tabelas)

## Sumário

| Lado | Qtd | Observação |
|---|---|---|
| Tabelas/views na produção | 272 | |
| Tabelas no repo (após rebuild local) | 167 | |
| Intersecção (objetos em ambos) | 160 | schema canônico |
| Só em produção | 112 | objetos não versionados |
| Só no repo | 7 | não deployados em produção |

## Objetos só em produção (não versionados no repo)

### 13 tabelas `cq_*` — cross-domain não esperado

`cq_audit_trail_v2`, `cq_classificacoes_recebimento`, `cq_defeitos`, `cq_fornecedores`, `cq_inspecao_defeitos`, `cq_inspecoes`, `cq_iqf_historico`, `cq_lotes`, `cq_nao_conformidade`, `cq_overrides_v2`, `cq_parametros_produto`, `cq_produtos_v2`, `cq_recebimentos_v2`

Provavelmente cópia/sincronização do banco CQ para uso em analytics cruzados. Não são consumidas pelo frontend RH — confirmado via grep.

### 6 tabelas `rh_*` — DDL aplicada manualmente

| Tabela | Frontend que consome |
|---|---|
| `rh_acidentes` | `acidentes.html` |
| `rh_aso` (singular) | `monitor-esocial.html` |
| `rh_asos_config` | — (tabela de config) |
| `rh_desligamentos` | `desligamentos_turnover.html`, `fluxo_funcionarios.html` |
| `rh_epi_entregas` | `epis.html` |
| `rh_treinamentos_tipos` | `treinamentos.html` |

**Duplicidade crítica:** `rh_aso` (singular) + `rh_asos` (plural) coexistem com dados provavelmente divergentes. `monitor-esocial.html` lê do singular; `asos.html` e `alertas_rh.html` lêem do plural.

### 91 views `vw_*` — camada de BI não versionada

Domínios: absenteismo (7), acao (6), agente (9), arm (6), cq (4), fp (1), funcionario (3), gestor (5), headcount (2), integ (2), intel (8), log (4), motor (14), orch (3), producao (3), reconc (2), sst (5), outras (8).

Representam o grosso da lógica de dashboards. Se forem DROPADAS inadvertidamente, dezenas de telas quebram em cascata.

### 1 tabela lixo

`_diag_api_test` — relíquia de diagnóstico, seguro dropar.

## Objetos só no repo (não deployados em produção)

| Objeto | Origem | Impacto |
|---|---|---|
| `rh_perfis_acesso` | `SEGURANCA_SUPABASE.sql` | **Bloqueante:** `has_perfil()` e toda RLS baseada nele quebram em prod. |
| `rh_decisao_experiencia` + `vw_decisao_experiencia` | `MODULO_DECISAO_EXPERIENCIA.sql` (sessão atual) | Aguardando deploy |
| `rh_desligamento_taxonomia` | `MODULO_DESLIGAMENTOS_TURNOVER.sql` | Parcialmente deployado (tabela `rh_desligamentos` foi, a taxonomia não) |
| `rh_entrevista_perguntas` | `schema_recrutamento_selecao.sql` | Não deployado |
| `rh_producao`, `rh_rondas` | diversos | Não deployados |

## Tabelas que o frontend referencia mas não existem nem em prod nem no repo

**São páginas fantasma:** o botão abre a tela, mas a query retorna 404/erro silencioso e o usuário vê a página vazia.

| Tabela ausente | Tela afetada |
|---|---|
| `rh_cipa_mandatos`, `rh_cipa_membros`, `rh_cipa_reunioes`, `rh_cipa_acoes` | `cipa.html` — módulo CIPA inteiro |
| `rh_dds`, `rh_dds_temas` | `dds.html` — Diálogo Diário de Segurança |
| `rh_extintores`, `rh_extintores_inspecao` | `extintores.html` |
| `rh_inspecoes`, `rh_inspecao_itens`, `rh_acoes_corretivas` | `inspecoes.html` |
| `rh_dados_bancarios` | `beneficios.html`, `exportar_pagamentos.html` |
| `rh_lotes_pagamento` | `exportar_pagamentos.html` |
| `rh_import_log`, `rh_log_exportacao` | logs genéricos |

**Nota:** em prod, algumas podem ter nomes levemente diferentes. Ex.: `rh_cipa_mandatos` (plural, frontend) vs `cipa_mandato` (singular, prod — existe). Mesmo problema de naming que `rh_aso`/`rh_asos`.

### Correções de nome detectadas (do frontend não batem com prod)

| Frontend chama | Prod tem |
|---|---|
| `rh_cipa_mandatos` | `cipa_mandato` (singular, sem prefixo `rh_`) |
| `rh_cipa_membros` | `cipa_membros` |
| `rh_cipa_reunioes` | `cipa_reunioes` |
| `rh_cipa_acoes` | (não existe — possivelmente `acao_plano` filtrado) |

## Decisões tomadas nesta sessão

1. **Migration `MODULO_DECISAO_EXPERIENCIA.sql` corrigida** para usar `rh_usuarios` (RBAC real em prod) em vez de `has_perfil()` (depende de `rh_perfis_acesso` inexistente).

2. Cross-check frontend atualizado com o schema real: **0 bugs de drift** remanescentes em tabelas que existem nos dois lados.

## O que falta para fechar o cerco

| # | Item | Esforço |
|---|---|---|
| 1 | Dumpar DDL completa das 91 views + 6 tabelas `rh_*` de prod e versionar no repo | `pg_dump --schema-only` |
| 2 | Resolver duplicidade `rh_aso` ↔ `rh_asos` (migrar dados, dropar a errada) | análise de uso + migração |
| 3 | Corrigir nomes `rh_cipa_*` → `cipa_*` no frontend CIPA | 1 arquivo, ~5 refs |
| 4 | Decidir o destino das 16 tabelas ausentes: implementar DDL ou remover telas | produto |
| 5 | Aplicar `MODULO_DECISAO_EXPERIENCIA.sql` + `VIEWS_FRONTEND_COMPATIBILIDADE.sql` em prod | DBA |
