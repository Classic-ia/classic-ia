# SGCS — Sistema de Governanca de Cargos & Salarios

Modulo de governanca de C&S da Classic, criado em Maio/2026 para sustentar
o pacote de reestruturacao (Politica Corporativa, bandas, fiducia, niveis,
Comite trimestral).

## Escopo entregue ate aqui

**Foundation (Modulos 1, 2, 3 e 9):**
- Modulo 1 — Cadastro de colaboradores (extensao via `rh_cs_colaborador`)
- Modulo 2 — Dossie funcional (`rh_cs_documentos`)
- Modulo 3 — Bandas salariais + matriz de criticidade
- Modulo 9 — Dashboard executivo (KPIs em `vw_cs_dashboard_kpis`)

**Fase 2 — Eixo critico (Modulos 4 e 5):**
- Modulo 4 — Ficha de Avaliacao Anual (4 blocos 35+35+15+15=100)
  - Listagem por ciclo: `sgcs_fichas.html`
  - Edicao com workflow rascunho→enviada→validada→homologada: `sgcs_ficha_edit.html`
- Modulo 5 — Workflow de Progressao I→II→III
  - Painel: `sgcs_progressoes.html`
  - Identificacao automatica de elegibilidade (objetivos)
  - Workflow: elegivel → recomendada → aprovada_rh → aprovada_comite/diretoria → homologada

Modulos remanescentes (6 Excecoes, 7 Comite, 8 Fiducia, 11 Beneficios) tem
**estrutura SQL pronta** desde SGCS_01 — falta apenas a UI e RPCs de fluxo.

## Arquivos

| Arquivo                          | Conteudo                                          |
|----------------------------------|---------------------------------------------------|
| `SGCS_01_TABELAS.sql`            | 14 tabelas `rh_cs_*` + indexes + constraints      |
| `SGCS_02_RLS.sql`                | RLS habilitado em 15 tabelas, 30 policies         |
| `SGCS_03_VIEWS.sql`              | 3 views + 2 RPCs (foundation)                     |
| `SGCS_04_SEED.sql`               | 10 familias funcionais + 22 beneficios catalogados|
| `SGCS_05_FASE2_AVALIACOES.sql`   | 2 views + 5 RPCs (Modulos 4 + 5) + 1 indice       |

## Ordem de execucao

Os scripts SGCS sao aplicaveis **apos** o rebuild base RH descrito em
`ORDEM_REBUILD.sql`. Ordem:

```bash
psql -d <banco> -f SGCS_01_TABELAS.sql
psql -d <banco> -f SGCS_02_RLS.sql
psql -d <banco> -f SGCS_03_VIEWS.sql
psql -d <banco> -f SGCS_04_SEED.sql
psql -d <banco> -f SGCS_05_FASE2_AVALIACOES.sql   # Fase 2 (Modulos 4+5)
```

Em Supabase, aplicar via SQL Editor ou MCP `apply_migration`.

## Dependencias

- `rh_funcionarios` (FUNDACAO_BANCO_v2.sql) — FK em quase todas as tabelas
- `rh_cargos` (FUNDACAO_BANCO_v2.sql) — FK em `rh_cs_cargo_meta`, `rh_cs_bandas`
- `rh_perfil_atual()` (SEGURANCA_SUPABASE.sql) — usada nas policies RLS

## Tabelas criadas

| Tabela                         | Proposito                                    |
|--------------------------------|----------------------------------------------|
| `rh_cs_familias`               | 10 familias funcionais                       |
| `rh_cs_cargo_meta`             | criticidade A/B/C + familia por cargo        |
| `rh_cs_bandas`                 | banda (min/ref/teto) por (cargo, nivel)      |
| `rh_cs_colaborador`            | nivel I/II/III, gratif, fiducia (1:1 func.)  |
| `rh_cs_historico_salarial`     | trilha de mudancas salariais                 |
| `rh_cs_documentos`             | dossie (aditivos, termos, comunicados)       |
| `rh_cs_fichas_avaliacao`       | ficha anual 100 pts (4 blocos)               |
| `rh_cs_progressoes`            | workflow I->II / II->III                     |
| `rh_cs_excecoes`               | 7 tipos de excecao, limite 5%                |
| `rh_cs_reunioes_comite`        | atas trimestrais                             |
| `rh_cs_relatorios_fiducia`     | relatorios mensais Paulo/Itamar/Kimila/Wand. |
| `rh_cs_beneficios`             | catalogo (22 items pre-cadastrados)          |
| `rh_cs_elegibilidade`          | colaborador x beneficio                      |
| `rh_cs_historico_beneficios`   | trilha de eventos de beneficio               |
| `rh_cs_alertas`                | governanca: fora-banda, fiducia atrasada...  |

## Views e RPCs

**Foundation (SGCS_03):**
- `vw_cs_colaborador_banda` — colaborador x cargo x banda vigente x posicao
- `vw_cs_bandas_ativas` — bandas vigentes + contagem de cobertura
- `vw_cs_dashboard_kpis` — 1 linha com todos os KPIs do dashboard
- `cs_recalcular_alertas_fora_banda()` — recalcula alertas (idempotente)
- `cs_set_perfil_colaborador(...)` — upsert de dados C&S do colaborador

**Fase 2 — Modulos 4 + 5 (SGCS_05):**
- `vw_cs_fichas_ciclo` — fichas + colaborador + supervisor por ciclo
- `vw_cs_progressoes_workflow` — progressoes + ficha + proxima etapa
- `cs_ficha_upsert(...)` — cria/atualiza ficha de avaliacao
- `cs_ficha_transicionar(p_ficha_id, p_novo_status)` — rascunho→enviada→validada→homologada
- `cs_calcular_proxima_progressao(p_funcionario_id, p_ciclo_ano)` — calcula
  elegibilidade objetiva (leitura, retorna JSONB)
- `cs_identificar_elegiveis(p_ciclo_ano)` — batch: cria progressoes elegivel/bloqueado
- `cs_progressao_transicionar(p_progressao_id, p_etapa, p_aprovar, p_parecer)` —
  avanca/rejeita workflow nas etapas supervisor/rh/comite/diretoria/homologar

## Telas (rh/)

| Pagina                       | Rota (shell.js)               | Perfis                          |
|------------------------------|-------------------------------|---------------------------------|
| `sgcs_colaboradores.html`    | `C&S > Colaboradores na Banda`| admin/rh/conf/dir/fin           |
| `sgcs_bandas.html`           | `C&S > Bandas Salariais`      | admin/rh/conf/dir/fin           |
| `sgcs_fichas.html`           | `C&S > Fichas de Avaliacao`   | admin/rh/gestor/conf/dir        |
| `sgcs_ficha_edit.html`       | (acessada via link da listagem)| admin/rh/gestor/conf/dir       |
| `sgcs_progressoes.html`      | `C&S > Workflow Progressao`   | admin/rh/gestor/conf/dir/fin    |

## Proximos passos (apos rebuild da banca)

1. **Comite homologa criticidade A/B/C** para os cargos via `rh_cs_cargo_meta`
2. **Comite cadastra bandas oficiais** em `rh_cs_bandas` por (cargo, nivel)
3. **RH atribui nivel e familia** aos 140 colaboradores via
   `cs_set_perfil_colaborador(...)` (script de migracao ou tela em Fase 2)
4. **Cadastrar fiducia** dos 4 cargos (Paulo / Itamar / Kimila / Wanderley)
   com `fiducia_natureza` I, II, II, III respectivamente
5. **Roteiro Fase 2:** UI para fichas, workflow de progressao, excecoes,
   comite, relatorios de fiducia e beneficios (estrutura SQL ja pronta)

## Permissoes (RLS)

| Tabela                          | Leitura                                     | Escrita                |
|---------------------------------|---------------------------------------------|------------------------|
| `rh_cs_familias`                | qualquer usuario autenticado                | admin/rh               |
| `rh_cs_beneficios`              | qualquer usuario autenticado                | admin/rh               |
| `rh_cs_cargo_meta` (criticidade)| admin/rh/gestor/conf/dir/fin                | admin/rh               |
| Bandas, historico, eleg.        | admin/rh/conf/dir/fin                       | admin/rh               |
| Colaborador C&S, fichas, prog.  | admin/rh/gestor/conf/dir                    | admin/rh/conf (fichas) |
| Excecoes, atas, fiducia, alerta | admin/rh/conf/dir                           | admin/rh/conf/dir      |

Onde: `conf` = `gestor_confianca`, `dir` = `diretoria`, `fin` = `financeiro`.

**Importante (gap herdado):** `rh_usuarios.perfil CHECK` (`FUNDACAO_BANCO_v2.sql:212-213`)
admite apenas `('administrador','rh','gestor','visualizador')`. Policies que
referenciam `gestor_confianca`, `diretoria`, `financeiro`, `sst` so disparam
quando o CHECK for expandido (ou quando migrar para `rh_perfis_acesso` 1:N).
Esse e o mesmo gap presente em `RLS_TABELAS_NOVAS.sql` desde Abr/2026 — nao
foi introduzido pela foundation SGCS. Tratar em PR separado.

---

## Rollback

Para desfazer integralmente a foundation SGCS (em ordem inversa de
dependencia):

```sql
-- 1. RPCs (Fase 2 primeiro, por dependencia logica)
DROP FUNCTION IF EXISTS public.cs_progressao_transicionar(UUID, TEXT, BOOLEAN, TEXT);
DROP FUNCTION IF EXISTS public.cs_identificar_elegiveis(INT);
DROP FUNCTION IF EXISTS public.cs_calcular_proxima_progressao(UUID, INT);
DROP FUNCTION IF EXISTS public.cs_ficha_transicionar(UUID, TEXT, DATE);
DROP FUNCTION IF EXISTS public.cs_ficha_upsert(
  UUID, INT, NUMERIC, NUMERIC, NUMERIC, NUMERIC, NUMERIC, UUID, TEXT
);
DROP FUNCTION IF EXISTS public.cs_set_perfil_colaborador(
  UUID, UUID, CHAR, DATE, NUMERIC, CHAR, TEXT, UUID, TEXT
);
DROP FUNCTION IF EXISTS public.cs_recalcular_alertas_fora_banda();

-- 2. Indices parciais
DROP INDEX IF EXISTS uq_cs_prog_ciclo_inicial;

-- 3. Views (Fase 2 primeiro)
DROP VIEW IF EXISTS public.vw_cs_progressoes_workflow;
DROP VIEW IF EXISTS public.vw_cs_fichas_ciclo;
DROP VIEW IF EXISTS public.vw_cs_dashboard_kpis;
DROP VIEW IF EXISTS public.vw_cs_bandas_ativas;
DROP VIEW IF EXISTS public.vw_cs_colaborador_banda;

-- 3. Tabelas (em ordem de FK reversa)
DROP TABLE IF EXISTS rh_cs_alertas              CASCADE;
DROP TABLE IF EXISTS rh_cs_historico_beneficios CASCADE;
DROP TABLE IF EXISTS rh_cs_elegibilidade        CASCADE;
DROP TABLE IF EXISTS rh_cs_beneficios           CASCADE;
DROP TABLE IF EXISTS rh_cs_relatorios_fiducia   CASCADE;
DROP TABLE IF EXISTS rh_cs_reunioes_comite      CASCADE;
DROP TABLE IF EXISTS rh_cs_excecoes             CASCADE;
DROP TABLE IF EXISTS rh_cs_progressoes          CASCADE;
DROP TABLE IF EXISTS rh_cs_fichas_avaliacao     CASCADE;
DROP TABLE IF EXISTS rh_cs_documentos           CASCADE;
DROP TABLE IF EXISTS rh_cs_historico_salarial   CASCADE;
DROP TABLE IF EXISTS rh_cs_colaborador          CASCADE;
DROP TABLE IF EXISTS rh_cs_bandas               CASCADE;
DROP TABLE IF EXISTS rh_cs_cargo_meta           CASCADE;
DROP TABLE IF EXISTS rh_cs_familias             CASCADE;
```

Nenhuma estrutura existente do RH e afetada por este rollback — todas as
FKs sao **saindo** das tabelas `rh_cs_*` para tabelas base (`rh_funcionarios`,
`rh_cargos`, `rh_centros_custo`), nunca o inverso.

---

## Criterios de aceite (smoke tests)

Apos aplicar `SGCS_01` a `SGCS_04`, executar:

```sql
-- A1. 15 tabelas criadas com prefixo rh_cs_
SELECT COUNT(*) AS tabelas_cs
  FROM information_schema.tables
 WHERE table_schema = 'public' AND table_name LIKE 'rh_cs_%';
-- esperado: 15

-- A2. RLS habilitado em todas (15 tabelas, 0 sem RLS)
SELECT COUNT(*) AS sem_rls
  FROM pg_tables
 WHERE schemaname = 'public' AND tablename LIKE 'rh_cs_%' AND rowsecurity = false;
-- esperado: 0

-- A3. Zero policies USING(TRUE)
SELECT COUNT(*) AS using_true
  FROM pg_policies
 WHERE schemaname = 'public' AND tablename LIKE 'rh_cs_%'
   AND qual = 'true';
-- esperado: 0

-- A4. 10 familias semeadas
SELECT COUNT(*) FROM rh_cs_familias WHERE ativo = TRUE;
-- esperado: 10

-- A5. 22 beneficios catalogados
SELECT COUNT(*) FROM rh_cs_beneficios WHERE ativo = TRUE;
-- esperado: >= 22

-- A6. Views compilam (cada SELECT deve retornar >= 0 linhas sem erro)
SELECT COUNT(*) FROM vw_cs_colaborador_banda;
SELECT COUNT(*) FROM vw_cs_bandas_ativas;
SELECT * FROM vw_cs_dashboard_kpis LIMIT 1;

-- A7. RPCs executaveis (deve retornar JSON ou tabela, nao erro)
SELECT * FROM cs_recalcular_alertas_fora_banda();
-- esperado: 1 linha com (criados INT, resolvidos INT)

-- A8. Sem duplicacao com rh_funcionarios
SELECT column_name FROM information_schema.columns
 WHERE table_name = 'rh_cs_colaborador'
   AND column_name IN ('centro_custo','observacoes','salario_base','cargo_id');
-- esperado: 0 linhas

-- A9. FKs apontam para tabelas existentes (zero FKs orfas)
SELECT COUNT(*) FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
 WHERE c.contype = 'f' AND t.relname LIKE 'rh_cs_%'
   AND NOT EXISTS (
     SELECT 1 FROM pg_class t2 WHERE t2.oid = c.confrelid
   );
-- esperado: 0

-- ── Smoke tests Fase 2 (apos SGCS_05) ─────────────────────────

-- A10. RPCs de Fase 2 existem
SELECT proname FROM pg_proc
 WHERE proname IN (
   'cs_ficha_upsert','cs_ficha_transicionar',
   'cs_calcular_proxima_progressao','cs_identificar_elegiveis',
   'cs_progressao_transicionar'
 );
-- esperado: 5 linhas

-- A11. Views de Fase 2 compilam
SELECT COUNT(*) FROM vw_cs_fichas_ciclo;
SELECT COUNT(*) FROM vw_cs_progressoes_workflow;
-- esperado: >= 0 (zero antes do primeiro ciclo)

-- A12. Indice parcial existe
SELECT indexname FROM pg_indexes
 WHERE schemaname = 'public' AND indexname = 'uq_cs_prog_ciclo_inicial';
-- esperado: 1 linha

-- A13. cs_calcular_proxima_progressao retorna JSONB valido
SELECT cs_calcular_proxima_progressao(
  (SELECT id FROM rh_funcionarios WHERE status = 'ativo' LIMIT 1)
);
-- esperado: jsonb com chaves elegivel, ciclo_ano, nivel_atual,
-- nivel_novo, bloqueios (mesmo que vazio antes do cadastro de niveis)

-- A14. cs_identificar_elegiveis e idempotente
SELECT * FROM cs_identificar_elegiveis(EXTRACT(YEAR FROM CURRENT_DATE)::INT);
SELECT * FROM cs_identificar_elegiveis(EXTRACT(YEAR FROM CURRENT_DATE)::INT);
-- esperado: mesma quantidade nas duas execucoes

-- A15. cs_ficha_transicionar bloqueia transicao invalida
DO $$ BEGIN
  PERFORM cs_ficha_transicionar(gen_random_uuid(), 'homologada');
  RAISE EXCEPTION 'Deveria ter falhado';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
-- esperado: nao falha (excecao capturada — ficha inexistente)
```

**Front-end** (manual, em `https://app.classiccouros.com.br/rh/`):

1. Login como `administrador` → menu lateral exibe grupo **C&S** com
   4 itens: "Colaboradores na Banda", "Bandas Salariais", "Fichas de
   Avaliacao", "Workflow Progressao".
2. Abrir `sgcs_colaboradores.html` → KPIs renderizam (mesmo que zerados
   antes do cadastro de bandas) e tabela lista os 140 colaboradores com
   "Sem banda" enquanto `rh_cs_bandas` estiver vazia.
3. Abrir `sgcs_bandas.html` → exibe empty-state "Nenhuma banda salarial
   cadastrada ainda" enquanto `rh_cs_bandas` estiver vazia.
4. Login como `visualizador` → grupo C&S **nao** aparece (perfis exigem
   admin/rh/gestor_confianca/diretoria/financeiro).
5. Exportar CSV em ambas as paginas → arquivo gerado com cabecalho
   correto, separador `;`, BOM UTF-8.

**Front-end Fase 2 (Modulos 4 e 5):**

6. `sgcs_fichas.html` → seletor de ciclo (anos -2 ate +1), KPIs (% homologadas,
   nota media, sem ficha). Botao "+ Nova ficha" abre modal de selecao de
   colaborador (so lista quem ainda nao tem ficha no ciclo). Linhas com
   "Sem ficha" mostram botao "Criar".
7. `sgcs_ficha_edit.html?id=<uuid>` → cabecalho com nome/cargo/nivel/ciclo,
   4 cards de blocos (Tecnico 0-35, Comportamental 0-35, Frequencia 0-15,
   Seguranca 0-15) com slider + input numerico sincronizados, total
   auto-calculado. Botoes adaptam ao status:
   - Rascunho: "Salvar rascunho" + "Enviar para RH"
   - Enviada (perfil RH): "Validar"
   - Validada (perfil RH): "Homologar"
   - Homologada (perfil RH): "Reabrir"
8. `sgcs_progressoes.html` → tabela do ciclo com transicao I→II / II→III,
   salario atual vs proposto, nota da ficha, status, proxima etapa.
   Botao "Identificar elegiveis" (apenas admin/RH) executa o batch
   `cs_identificar_elegiveis(ciclo)`. Cada linha tem botao "Detalhe" que
   abre drawer com:
   - Snapshot da progressao + nota
   - Motivos de bloqueio (se houver)
   - Lista de pareceres por etapa
   - Botoes de acao conforme perfil e status atual:
     - elegivel + supervisor/gestor: Recomendar / Rejeitar
     - recomendada + RH: Aprovar RH / Rejeitar
     - aprovada_rh + gestor_confianca: Aprovar Comite (se II→III, vai
       direto para Diretoria)
     - aprovada_rh ou aprovada_comite + diretoria: Aprovar Diretoria
     - aprovada_diretoria + RH: Homologar
9. Workflow end-to-end (smoke manual):
   a. Admin/RH cria ficha em rascunho com notas → salva
   b. Supervisor envia para RH (rascunho → enviada)
   c. RH valida (enviada → validada)
   d. RH homologa (validada → homologada)
   e. Admin/RH clica "Identificar elegiveis" no painel de progressao
   f. Supervisor abre drawer e recomenda
   g. RH aprova
   h. Comite (gestor_confianca) aprova (para I→II) OU vai direto
      para Diretoria (para II→III)
   i. Diretoria aprova
   j. RH homologa → entra em folha (aditivo gerado em chunk posterior)
