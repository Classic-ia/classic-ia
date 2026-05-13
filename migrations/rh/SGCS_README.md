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
| Catalogo (familias, beneficios) | publico (RLS ativo mas USING=TRUE)          | admin/rh               |
| Bandas, historico, beneficios   | admin/rh/conf/dir/fin                       | admin/rh               |
| Colaborador C&S, fichas, prog.  | admin/rh/gestor/conf/dir                    | admin/rh/conf (fichas) |
| Excecoes, atas, fiducia, alerta | admin/rh/conf/dir                           | admin/rh/conf/dir      |

Onde: `conf` = `gestor_confianca`, `dir` = `diretoria`, `fin` = `financeiro`.
