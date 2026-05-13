# SGCS — Sistema de Governanca de Cargos & Salarios

Modulo de governanca de C&S da Classic, criado em Maio/2026 para sustentar
o pacote de reestruturacao (Politica Corporativa, bandas, fiducia, niveis,
Comite trimestral).

## Escopo desta entrega (Foundation)

Cobre **Modulos 1, 2, 3 e 9** do prompt mestre (MVP):

- Modulo 1 — Cadastro de colaboradores (extensao via `rh_cs_colaborador`)
- Modulo 2 — Dossie funcional (`rh_cs_documentos`)
- Modulo 3 — Bandas salariais + matriz de criticidade
- Modulo 9 — Dashboard executivo (KPIs em `vw_cs_dashboard_kpis`)

A estrutura completa para Fase 2 (fichas, progressao, excecoes, comite,
fiducia, beneficios) **ja esta criada no banco** — falta apenas a UI.

## Arquivos

| Arquivo                       | Conteudo                                          |
|-------------------------------|---------------------------------------------------|
| `SGCS_01_TABELAS.sql`         | 14 tabelas `rh_cs_*` + indexes + constraints      |
| `SGCS_02_RLS.sql`             | RLS habilitado em 15 tabelas, 30 policies         |
| `SGCS_03_VIEWS.sql`           | 3 views (`vw_cs_*`) + 2 RPCs (`cs_*`)             |
| `SGCS_04_SEED.sql`            | 10 familias funcionais + 22 beneficios catalogados|

## Ordem de execucao

Os scripts SGCS sao aplicaveis **apos** o rebuild base RH descrito em
`ORDEM_REBUILD.sql`. Ordem:

```bash
psql -d <banco> -f SGCS_01_TABELAS.sql
psql -d <banco> -f SGCS_02_RLS.sql
psql -d <banco> -f SGCS_03_VIEWS.sql
psql -d <banco> -f SGCS_04_SEED.sql
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

- `vw_cs_colaborador_banda` — colaborador x cargo x banda vigente x posicao
- `vw_cs_bandas_ativas` — bandas vigentes + contagem de cobertura
- `vw_cs_dashboard_kpis` — 1 linha com todos os KPIs do dashboard
- `cs_recalcular_alertas_fora_banda()` — recalcula alertas (idempotente)
- `cs_set_perfil_colaborador(...)` — upsert de dados C&S do colaborador

## Telas (rh/)

| Pagina                       | Rota (shell.js)   | Perfis                  |
|------------------------------|-------------------|-------------------------|
| `sgcs_colaboradores.html`    | `C&S > Colab.`    | admin/rh/conf/dir/fin   |
| `sgcs_bandas.html`           | `C&S > Bandas`    | admin/rh/conf/dir/fin   |

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
-- 1. RPCs
DROP FUNCTION IF EXISTS public.cs_set_perfil_colaborador(
  UUID, UUID, CHAR, DATE, NUMERIC, CHAR, TEXT, UUID, TEXT
);
DROP FUNCTION IF EXISTS public.cs_recalcular_alertas_fora_banda();

-- 2. Views
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
```

**Front-end** (manual, em `https://app.classiccouros.com.br/rh/`):

1. Login como `administrador` → menu lateral exibe grupo **C&S** com
   2 itens: "Colaboradores na Banda" e "Bandas Salariais".
2. Abrir `sgcs_colaboradores.html` → KPIs renderizam (mesmo que zerados
   antes do cadastro de bandas) e tabela lista os 140 colaboradores com
   "Sem banda" enquanto `rh_cs_bandas` estiver vazia.
3. Abrir `sgcs_bandas.html` → exibe empty-state "Nenhuma banda salarial
   cadastrada ainda" enquanto `rh_cs_bandas` estiver vazia.
4. Login como `visualizador` → grupo C&S **nao** aparece (perfis exigem
   admin/rh/gestor_confianca/diretoria/financeiro).
5. Exportar CSV em ambas as paginas → arquivo gerado com cabecalho
   correto, separador `;`, BOM UTF-8.
