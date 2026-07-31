# F-39 — Recrutamento e Selecao

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-39 |
| Titulo | Recrutamento e Selecao (Vagas, Candidatos, Pipeline, Entrevistas, Banco de Talentos) |
| Modulo | RH (Recursos Humanos — Processos de Entrada) |
| Sistema | Classic IA — Classic Importacao e Exportacao de Couros EIRELI |
| CNPJ | 08.849.964/0001-10 |
| Sede | Jardim Alegre - PR |
| Versao | 1.0 |
| Data | 28/07/2026 |
| Autor | Equipe Classic IA |
| Supabase RH | muiqmtnfvyffborgiwdw.supabase.co |
| Dominio | app.classiccouros.com.br |

---

## 1. Identificacao

**F-39 — Recrutamento e Selecao** e o modulo responsavel pelo ciclo de entrada de colaboradores na Classic Couros: abertura e distribuicao de vagas por setor, cadastro de candidatos, pipeline de selecao em kanban (8 etapas), agenda de entrevistas, ficha de avaliacao padronizada e banco de talentos. E o inicio do ciclo de vida do colaborador (contraparte do F-38 Desligamentos).

O modulo compreende **1 tela principal**, **3 arquivos SQL** de backend (1 schema + 1 migration + tabelas ETL), e opera sobre o projeto Supabase RH (`muiqmtnfvyffborgiwdw.supabase.co`).

**Tela do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/recrutamento_selecao.html` | Vagas, candidatos, pipeline kanban, agenda, banco de talentos (5 abas) | 1513 |

**Bibliotecas compartilhadas (RH):**

| Arquivo | Funcao |
|---------|--------|
| `config.js` | Config Supabase RH (SB_URL `muiqmtnfvyffborgiwdw`, SB_KEY) |
| `rh_api.js` | Camada de API `RH_API.get/post/fetch` (PostgREST com JWT) |
| `rh_auth.js` | Autenticacao RH (`RHAuth.init`), redireciona para `login_rh.html` |
| `ds.css` | Design system |

> **Nota:** ao contrario das demais telas RH, `recrutamento_selecao.html` **nao** usa `api.js`/`shell.js`/`ui_components.js`/`page_bootstrap.js` — usa o cliente `rh_api.js` (`RH_API`) e navega via `hub.html`/`login_rh.html`. Nao ha Chart.js nem graficos. Ver secao 24 (divergencias).

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `schema_recrutamento_selecao.sql` (= `migrations/rh/schema_recrutamento_selecao.sql`) | 6 tabelas (rs_vagas, rs_candidatos, rs_pipeline, rs_entrevistas, rs_avaliacoes, rs_banco_talentos) + triggers + RLS |
| `migration_recrutamento_selecao.sql` | Documentacao (tabelas reais substituindo views dummy) + seed vaga "Auxiliar de Producao" (30 posicoes) |
| `TABELAS_ETL.sql` | `rs_vagas_distribuicao` (distribuicao por setor) + `reconc_duplicata_candidato` (dedup) |

---

## 2. Problema

A Classic Couros precisa repor constantemente o quadro operacional (alta rotatividade — ver F-38) com processos de contratacao em massa (ex: 30 auxiliares de producao de uma vez). Antes do sistema:

1. **Vagas sem controle** — Abertura de vagas em planilhas, sem distribuicao por setor, prioridade, motivo ou prazo.
2. **Candidatos dispersos** — Curriculos e contatos em papel/WhatsApp, sem base pesquisavel.
3. **Pipeline invisivel** — Impossivel saber em que etapa cada candidato esta (triagem, entrevista, teste, exame, documentacao).
4. **Entrevistas sem agenda** — Sem controle de agendamentos, entrevistadores e resultados.
5. **Avaliacao subjetiva** — Sem roteiro padronizado de entrevista nem criterios comparaveis.
6. **Talentos perdidos** — Candidatos bons nao aproveitados eram esquecidos (sem banco de talentos com tags).

**Solucao:** modulo que (a) gerencia vagas com distribuicao por setor, (b) cadastra candidatos com fonte/origem, (c) move candidatos por um pipeline kanban de 8 etapas com notas, (d) agenda entrevistas, (e) padroniza a avaliacao, (f) mantem banco de talentos.

---

## 3. Processo Operacional

### 3.1 Abertura de Vaga (aba "Vagas")

1. RH cadastra a vaga: titulo, cargo, setor, tipo de contrato (CLT/PJ/temporario/estagio/jovem_aprendiz), turno, quantidade, faixa salarial, prioridade, motivo de abertura, data limite, requisitos, descricao.
2. Opcionalmente distribui a vaga por setor (`rs_vagas_distribuicao`): quantidade + sexo preferencial + turno por setor (ex: Carga e Descarga 7 M, Miudos 6 F...).
3. Status da vaga: `aberta → em_selecao → suspensa → fechada → cancelada`. Ao fechar, grava `data_fechamento`.

### 3.2 Cadastro de Candidato (aba "Candidatos")

1. Cadastra candidato: nome, CPF, telefone, email, cidade/UF, escolaridade, experiencia (resumo), pretensao salarial, disponibilidade, fonte (indicacao/site/agencia/linkedin/whatsapp/presencial), indicado por.
2. Status: `ativo → contratado → desistiu → banco_talentos → bloqueado`.

### 3.3 Pipeline de Selecao (aba "Pipeline" — Kanban)

1. Vincula candidato a uma vaga (`rs_pipeline`, etapa inicial `triagem`) — sem duplicata (vaga+candidato).
2. Move o candidato pelas **8 etapas** (clique no card → modal, **nao** drag & drop): triagem → entrevista_rh → teste_pratico → entrevista_gestor → exame_admissional → documentacao → aprovado/reprovado (etapa `desistiu` so no select).
3. A cada movimento grava notas (triagem/entrevista/teste/gestor, 1-5), pareceres (RH/gestor), motivo de reprovacao e `data_ultima_etapa`.
4. **Ao mover para `aprovado`**, o status do candidato vira `contratado` automaticamente.

### 3.4 Agenda de Entrevistas (aba "Agenda")

1. Agenda entrevista vinculada ao pipeline: tipo, data/hora, duracao, local, entrevistador.
2. Status: agendada → confirmada → realizada → cancelada/reagendada/nao_compareceu. Se `realizada`, informa resultado (aprovado/reprovado/pendente).

### 3.5 Ficha de Avaliacao (`rs_avaliacoes`)

Roteiro padronizado: 6 criterios (1-5) — apresentacao, comunicacao, experiencia, motivacao, adequacao a vaga, trabalho em equipe — + 5 perguntas abertas + nota geral (0-10) + parecer (recomendado/recomendado_restricao/nao_recomendado) + justificativa.

### 3.6 Banco de Talentos (aba "Banco")

Candidatos marcados como `banco_talentos` com tags (ex: "solda,empilhadeira,noturno"), area de interesse, ultima interacao e proxima acao, para busca futura.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Acesso pretendido (migration) |
|--------|-------------------------------|
| `administrador` | Total: CRUD em vagas, candidatos, pipeline, entrevistas |
| `rh` | CRUD completo do modulo |
| `gestor` | CRUD (INSERT/UPDATE/DELETE) — avaliacao de gestor |
| demais autenticados | Somente leitura (pretendido) |

### 4.2 Permissao Real (Frontend + RLS)

- **Frontend:** apenas `RHAuth.init()` — acesso **binario** (autenticado ou nao). **Nao ha** checagem de perfil/role granular na tela (`API.setPerfil`/`Shell` ausentes).
- **RLS (schema aplicado):** policy `FOR ALL TO authenticated USING(true) WITH CHECK(true)` **e** policy identica para `anon` em todas as 6 tabelas. **Divergente** da migration (que documenta INSERT/UPDATE/DELETE so para admin/rh/gestor). Ver secao 24 / SIGA-01.

---

## 5. Fluxograma

```
VAGAS                          CANDIDATOS                    PIPELINE (KANBAN 8 etapas)
=====                          ==========                    ==========================
Abrir vaga                     Cadastrar candidato           Vincular (vaga+candidato)
  titulo/setor/cargo             nome/cpf/contato              etapa inicial: triagem
  tipo/turno/qtd                 escolaridade/fonte              |
  prioridade/motivo              disponibilidade                v (clique no card -> modal)
  |                              pretensao                    triagem
  v                              |                            entrevista_rh
Distribuicao por setor           v                            teste_pratico
  rs_vagas_distribuicao        status: ativo                  entrevista_gestor
  (qtd, sexo, turno)             |                            exame_admissional
  |                              |                            documentacao
  v                              |                              |
status: aberta ->                |                              v
  em_selecao -> fechada          |                            aprovado ----+ (status cand.
  (data_fechamento)              +--------------------------->  reprovado    -> contratado)
                                                              desistiu
AGENDA                         AVALIACAO                     BANCO DE TALENTOS
======                        =========                      =================
Agendar (pipeline)            6 criterios (1-5)              tags, area interesse
  tipo/data/duracao             apresentacao, comunicacao,   ultima interacao
  local/entrevistador           experiencia, motivacao,      proxima acao
  |                             adequacao, equipe              |
  v                           5 perguntas abertas            candidato.status =
status: agendada ->           nota geral 0-10                  banco_talentos
  realizada (resultado)       parecer + justificativa        (remover -> ativo)

[PONTE NAO IMPLEMENTADA] aprovado -> rh_proc_admissao / rh_executar_admissao (cria funcionario)
```

---

## 6. Regras de Negocio

### RN-01 — Tipo de Contrato (CHECK)

`rs_vagas.tipo_contrato IN (CLT, PJ, temporario, estagio, jovem_aprendiz)` (default CLT). Turno IN (diurno, noturno, misto, comercial).

### RN-02 — Status da Vaga (CHECK)

`aberta → em_selecao → suspensa → fechada → cancelada`. Ao mudar para `fechada`, grava `data_fechamento = hoje`.

### RN-03 — Prioridade e Motivo

Prioridade IN (baixa, normal, alta, urgente). Motivo de abertura IN (substituicao, aumento_quadro, nova_funcao, temporario, outro).

### RN-04 — Distribuicao de Vagas por Setor

`rs_vagas_distribuicao` (FK rs_vagas): setor, quantidade, sexo_preferencial, turno, `preenchidas` (contador). Vagas em aberto por setor = `max(0, quantidade - preenchidas)`.

### RN-05 — Criticidade de Setor (chip)

`vagas_abertas_setor > 5` → red; `>= 2` → yellow; senao green. So lista setores com aberta > 0. "Em processo" = pipelines nao finalizados por vaga.

### RN-06 — Status do Candidato (CHECK)

`ativo → contratado → desistiu → banco_talentos → bloqueado`. Escolaridade IN (fundamental, medio, tecnico, superior, pos_graduacao). Fonte IN (indicacao, site, agencia, linkedin, whatsapp, presencial, outro). Disponibilidade IN (imediata, 15_dias, 30_dias, a_combinar).

### RN-07 — Pipeline: 8 Etapas (CHECK)

`rs_pipeline.etapa IN (triagem, entrevista_rh, teste_pratico, entrevista_gestor, exame_admissional, documentacao, aprovado, reprovado, desistiu)` (default triagem). Kanban exibe 8 colunas; `desistiu` so no select de mover.

### RN-08 — Unicidade de Pipeline

Um candidato nao pode ser vinculado 2x a mesma vaga (bloqueio de duplicata vaga+candidato no frontend).

### RN-09 — Notas de Etapa (CHECK)

`nota_triagem`, `nota_entrevista`, `nota_teste`, `nota_gestor` ∈ [1,5]. Card do kanban exibe a **media** das notas preenchidas em `x/5` + estrelas.

### RN-10 — Aprovacao = Contratacao

Ao mover pipeline para `etapa='aprovado'`, PATCH em `rs_candidatos` seta `status='contratado'`. **Nao** cria funcionario nem processo de admissao (ver RN-19 / D-01).

### RN-11 — Reprovacao com Motivo

Etapa `reprovado` exibe campo `motivo_reprovacao` (gravado em `rs_pipeline`).

### RN-12 — Agenda de Entrevistas (CHECK)

`rs_entrevistas.tipo IN (entrevista_rh, teste_pratico, entrevista_gestor, exame_admissional, outro)`. Status IN (agendada, confirmada, realizada, cancelada, reagendada, nao_compareceu). Se `realizada`, informa `resultado IN (aprovado, reprovado, pendente)`. Duracao default 30 min.

### RN-13 — Ficha de Avaliacao (CHECK)

6 criterios ∈ [1,5]; `nota_geral NUMERIC(3,1)` (frontend 0-10, step 0.5); `parecer IN (recomendado, recomendado_restricao, nao_recomendado)` (NOT NULL). 5 perguntas abertas (p1-p5).

### RN-14 — Banco de Talentos

Adicionar seta candidato → `banco_talentos`; remover (`DELETE rs_banco_talentos`) volta candidato → `ativo`. Tags em texto CSV (ex: "solda,empilhadeira,noturno").

### RN-15 — KPI Vagas Abertas

Vagas com status `aberta` ou `em_selecao`. Se houver distribuicoes, exibe entre parenteses as posicoes em aberto: `total_posicoes - total_preenchidas`.

### RN-16 — KPI Em Selecao / Contratados

Em Selecao = pipelines com etapa NOT IN (aprovado, reprovado, desistiu). Contratados (mes) = pipelines etapa=aprovado com `data_ultima_etapa >= inicio do mes`.

### RN-17 — KPI Entrevistas da Semana

Entrevistas com `data_hora` na semana corrente (domingo → +6) e status != cancelada.

### RN-18 — Validacoes Obrigatorias

Vaga: titulo + setor + cargo. Candidato: nome. Pipeline: vaga + candidato (sem duplicata). Entrevista: pipeline + data_hora. Avaliacao: avaliador. Banco: candidato (sem duplicata). UF em maiusculas. Sem mascara/validacao de CPF ou telefone (apenas maxlength). Sem upload de curriculo.

### RN-19 — Workflow de Admissao (backend, nao integrado)

O backend possui `rh_proc_admissao` + RPC `rh_executar_admissao(p_processo_id)` que cria o funcionario a partir de um processo aprovado (`rh_processo` tipo=admissao). **O frontend de R&S nao aciona esse fluxo** — a contratacao para em `rs_candidatos.status='contratado'`. Ver D-01.

### RN-20 — Reconciliacao de Duplicatas de Candidato

`reconc_duplicata_candidato` (ETL): detecta candidatos/funcionarios similares (score_similaridade, motivo, detalhes jsonb) para evitar duplicidade na base — pipeline de reconciliacao, nao consumido diretamente pela tela.

---

## 7. Campos

### 7.1 Formulario de Vaga (`#modalVaga`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Titulo | `vagaTitulo` | text | Sim |
| Cargo | `vagaCargo` | text | Sim |
| Setor | `vagaSetor` | text | Sim |
| Tipo Contrato | `vagaTipo` | select | Nao (CLT default) |
| Turno | `vagaTurno` | select | Nao |
| Quantidade | `vagaQtd` | number (min 1) | Nao (default 1) |
| Faixa Salarial | `vagaSalario` | text | Nao |
| Prioridade | `vagaPrioridade` | select | Nao (normal) |
| Motivo Abertura | `vagaMotivo` | select | Nao |
| Data Limite | `vagaDataLimite` | date | Nao |
| Responsavel | `vagaResponsavel` | text | Nao |
| Requisitos | `vagaRequisitos` | textarea | Nao |
| Descricao | `vagaDescricao` | textarea | Nao |
| Observacoes | `vagaObs` | textarea | Nao |

### 7.2 Formulario de Candidato (`#modalCandidato`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Nome Completo | `candNome` | text | Sim |
| CPF | `candCpf` | text (maxlength 11) | Nao |
| Telefone | `candTelefone` | text | Nao |
| Email | `candEmail` | email | Nao |
| Cidade | `candCidade` | text | Nao |
| UF | `candUf` | text (maxlength 2, uppercase) | Nao |
| Escolaridade | `candEscolaridade` | select | Nao |
| Disponibilidade | `candDisp` | select | Nao |
| Fonte | `candFonte` | select | Nao |
| Indicado por | `candIndicado` | text | Nao |
| Pretensao Salarial | `candPretensao` | number (step .01) | Nao |
| Experiencia (resumo) | `candExperiencia` | textarea | Nao |
| Observacoes | `candObs` | textarea | Nao |

### 7.3 Mover Etapa (`#modalMoverEtapa`)

`moveEtapa` (select 9 etapas), `moveNotaTriagem/Entrevista/Teste/Gestor` (number 1-5), `moveParecerRH`, `moveParecerGestor`, `moveMotivoReprov` (so se reprovado), `moveObs`.

### 7.4 Agendar Entrevista (`#modalEntrevista`)

`entrevPipeline` (select, obrig.), `entrevTipo` (obrig.), `entrevDataHora` (datetime-local, obrig.), `entrevDuracao` (number, default 30), `entrevLocal`, `entrevEntrevistador`, `entrevObs`.

### 7.5 Ficha de Avaliacao (`#modalAvaliacao`)

`avalAvaliador` (obrig.), criterios 1-5 (`avalApresentacao/Comunicacao/Experiencia/Motivacao/Adequacao/Equipe`), roteiro (`avalP1..P5`), `avalParecer` (obrig.), `avalNotaGeral` (0-10 step .5), `avalJustificativa`, `avalPipeId` (hidden).

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rs_vagas` | Vagas de emprego | id, titulo, setor, cargo, tipo_contrato (CHECK 5), turno (CHECK 4), quantidade, salario_faixa, requisitos, descricao, prioridade (CHECK 4), status (CHECK 5), data_abertura, data_limite, data_fechamento, responsavel, motivo_abertura (CHECK 5), observacoes |
| `rs_vagas_distribuicao` | Distribuicao por setor (ETL) | id, vaga_id (FK), setor, quantidade, sexo_preferencial, turno, preenchidas, observacoes |
| `rs_candidatos` | Candidatos | id, nome_completo, cpf, telefone, email, cidade, uf, escolaridade (CHECK 5), experiencia_resumo, pretensao_salarial, disponibilidade (CHECK 4), fonte (CHECK 7), indicado_por, observacoes, status (CHECK 5) |
| `rs_pipeline` | Pipeline candidato↔vaga | id, vaga_id (FK), candidato_id (FK), etapa (CHECK 9), nota_triagem/entrevista/teste/gestor (1-5), parecer_rh, parecer_gestor, data_entrada, data_ultima_etapa, motivo_reprovacao, observacoes |
| `rs_entrevistas` | Agenda de entrevistas | id, pipeline_id (FK), tipo (CHECK 5), data_hora, duracao_min, local, entrevistador, status (CHECK 6), resultado (CHECK 3), observacoes |
| `rs_avaliacoes` | Ficha de avaliacao | id, pipeline_id (FK), avaliador, data_avaliacao, 6 criterios (1-5), p1_experiencia..p5_disponibilidade, nota_geral (3,1), parecer (CHECK 3), justificativa |
| `rs_banco_talentos` | Banco de talentos | id, candidato_id (FK), tags, area_interesse, ultima_interacao, proxima_acao, observacoes |
| `reconc_duplicata_candidato` | Dedup (ETL) | id, funcionario_a_id, funcionario_b_id, score_similaridade, motivo, detalhes (jsonb), status, resolvido_por/em |

Indices: `idx_vagas_status/setor`, `idx_cand_status/nome`, `idx_pipe_vaga/cand/etapa`, `idx_entrev_data/status`, `idx_aval_pipe`, `idx_banco_cand`.

**Ponte de admissao (backend):** `rh_proc_admissao` (processo_id, nome_completo, cpf, cargo_id, setor_id, empresa_id, filial_id, tipo_vinculo, salario_proposto, data_admissao_prevista, turno, carga_horaria...) — destino natural de um candidato aprovado, hoje nao integrado.

---

## 9. RPCs

**Nenhuma RPC especifica de R&S** — todas as operacoes da tela sao REST PostgREST (GET/POST/PATCH/DELETE) via `RH_API`.

**RPC de backend relacionada (nao chamada pela tela):**

### RPC-01 — `rh_executar_admissao(p_processo_id uuid)`

**Arquivo:** `MODULO_PROCESSOS_RH_v2.sql`. Cria o funcionario a partir de um `rh_processo` (tipo=admissao) em status `em_execucao`. Seria o destino de um candidato aprovado (RN-19). Fluxo: INSERT `rh_processo` + `rh_proc_admissao` → avanca status (`rascunho → pendente_documentos → pendente_aprovacao → aprovado → em_execucao`, validado por trigger `fn_proc_validar_transicao`) → `rh_executar_admissao`.

---

## 10. Functions (Backend SQL)

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| `rh_set_updated_at` | TRIGGER | Atualiza `updated_at` nas 6 tabelas rs_ |
| `rh_executar_admissao` | FUNCTION | Cria funcionario (ponte de admissao, nao usada pela tela) |
| `fn_proc_validar_transicao` | TRIGGER | Maquina de estados do processo de admissao |
| `fn_proc_validar_documentos` | TRIGGER | Bloqueia aprovacao sem documentos |

---

## 11. Triggers

| Trigger | Tabela | Evento | Funcao |
|---------|--------|--------|--------|
| `trg_rs_vagas_updated` | `rs_vagas` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rs_candidatos_updated` | `rs_candidatos` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rs_pipeline_updated` | `rs_pipeline` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rs_entrevistas_updated` | `rs_entrevistas` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rs_avaliacoes_updated` | `rs_avaliacoes` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rs_banco_talentos_updated` | `rs_banco_talentos` | BEFORE UPDATE | `rh_set_updated_at()` |

---

## 12. APIs (Chamadas Frontend)

### 12.1 Supabase REST (PostgREST via `RH_API`)

**Leitura (GET):**
- `rs_vagas?order=created_at.desc`
- `rs_vagas_distribuicao?order=setor.asc`
- `rs_candidatos?order=created_at.desc`
- `rs_pipeline?order=created_at.desc`
- `rs_entrevistas?order=data_hora.asc`
- `rs_banco_talentos?order=created_at.desc`

**Escrita (POST):** `rs_vagas`, `rs_candidatos`, `rs_pipeline` (etapa:'triagem'), `rs_entrevistas`, `rs_avaliacoes`, `rs_banco_talentos`.

**Atualizacao (PATCH, via `RH_API.fetch`):** `rs_vagas?id=eq.{id}` (edita/fecha), `rs_candidatos?id=eq.{id}` (status contratado/banco_talentos/ativo), `rs_pipeline?id=eq.{id}` (mover etapa + notas), `rs_entrevistas?id=eq.{id}` (edita/status/resultado).

**Exclusao (DELETE):** `rs_banco_talentos?id=eq.{id}`.

**Nenhuma view `vw_` e nenhuma RPC** consumidas.

### 12.2 Bibliotecas Externas

Nenhuma (sem Chart.js). UI usa barras de progresso, estrelas e chips proprios do `ds.css`.

---

## 13. Integracoes

### 13.1 Ponte com Admissao (RH Processos)

Candidato aprovado deveria gerar `rh_proc_admissao` → `rh_executar_admissao` → funcionario em `rh_funcionarios`. **Integracao pendente** (hoje so seta `status='contratado'`).

### 13.2 Reconciliacao ETL

`reconc_duplicata_candidato` cruza candidatos/funcionarios por similaridade (nome/CPF) para deduplicacao — parte do pipeline ETL de reconciliacao.

### 13.3 Convenia (potencial)

Admissao efetivada alimentaria o Convenia (folha) — fora do escopo atual da tela.

---

## 14. Dashboards

### 14.1 KPI Strip (`recrutamento_selecao.html`)

**6 KPIs:** Vagas Abertas (`kpiVagasAbertas`, com posicoes entre parenteses), Candidatos Ativos (`kpiCandidatos`), Em Selecao (`kpiEmSelecao`), Entrevistas Semana (`kpiEntrevistas`), Contratados no mes (`kpiContratados`), Banco Talentos (`kpiBancoTalentos`).

### 14.2 Kanban de Pipeline (aba Pipeline)

8 colunas (`data-etapa`): Triagem (badge com nº de vagas abertas), Entrevista RH, Teste Pratico, Entrev. Gestor, Exame Admissional, Documentacao, Aprovado (verde), Reprovado (vermelho). Cards clicaveis → modal mover etapa. Painel "Vagas Abertas por Setor" com criticidade por cor.

### 14.3 Agenda (aba Agenda)

Cards agrupados por dia (`.agenda-day`/`.agenda-item`): hora, candidato, tipo·vaga·local·entrevistador, badge de status, acoes.

### 14.4 Tabelas

Vagas, Candidatos, Banco de Talentos (colunas na secao 15.1).

---

## 15. Relatorios

### 15.1 Tabelas de Dados Renderizadas

- **Vagas (`#tbVagas`):** Vaga (titulo+cargo) | Setor | Tipo (badge) | Qtd | Distribuicao (preenchidas/total + barra) | Prioridade (badge) | Status (badge) | Candidatos (count) | Abertura | Acoes. Sub-linhas de distribuicao: Setor | Sexo (M/F) | Turno | Qtd | preenchidas/qtd | Obs.
- **Candidatos (`#tbCandidatos`):** Nome | Telefone | Cidade/UF | Escolaridade | Fonte | Disponibilidade | Status | Acoes.
- **Banco (`#tbBanco`):** Nome | Telefone | Area | Tags | Ultima Interacao | Proxima Acao | Acoes.

### 15.2 Indicadores Derivados

Funil de conversao (em selecao, contratados), vagas abertas por setor com criticidade, media de notas por candidato no kanban.

---

## 16. Documentos

### 16.1 Ficha de Avaliacao Padronizada

Roteiro de entrevista (`rs_avaliacoes`): 6 criterios objetivos (1-5), 5 perguntas abertas (experiencia, motivo, desafio, expectativa, disponibilidade), nota geral e parecer. Serve como documento de decisao da contratacao.

### 16.2 Vaga (descricao/requisitos)

`rs_vagas.requisitos` + `descricao` compoem o anuncio da vaga.

---

## 17. Evidencias

### 17.1 Notas e Pareceres

`rs_pipeline` (notas por etapa, pareceres RH/gestor) e `rs_avaliacoes` (criterios + justificativa) sao a evidencia da decisao de selecao.

### 17.2 Historico de Etapas

`rs_pipeline.data_entrada` / `data_ultima_etapa` + `updated_at` registram a progressao. `rs_entrevistas` registra agendamentos e resultados.

### 17.3 Origem do Candidato

`rs_candidatos.fonte` + `indicado_por` evidenciam o canal de captacao (para medir efetividade de fontes).

---

## 18. Permissoes

### 18.1 RLS (schema aplicado)

6 tabelas rs_: policy `FOR ALL TO authenticated USING(true) WITH CHECK(true)` **+** policy identica para `anon`. **Permissivo** — divergente da migration (que documenta SELECT para todos, INSERT/UPDATE/DELETE so admin/rh/gestor). Ver SIGA-01.

### 18.2 Frontend

Gate binario por `RHAuth.init()`; sem role granular; redireciona para `login_rh.html` se nao autenticado.

---

## 19. Auditoria

### 19.1 Timestamps

`created_at`/`updated_at` (via trigger `rh_set_updated_at`) em todas as tabelas rs_.

### 19.2 Reconciliacao

`reconc_duplicata_candidato` registra resolucao de duplicatas (resolvido_por/em, status).

> **Lacuna:** o modulo **nao** grava em `rh_audit_log` (diferente do F-38). Mudancas de etapa/status nao tem trilha de auditoria de "quem fez".

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Nova vaga | POST `rs_vagas` |
| Editar/fechar vaga | PATCH `rs_vagas` (data_fechamento se fechada) |
| Novo candidato | POST `rs_candidatos` |
| Vincular ao pipeline | POST `rs_pipeline` (etapa=triagem) |
| Mover etapa | PATCH `rs_pipeline` (etapa, notas, pareceres, data_ultima_etapa) |
| Aprovar candidato | PATCH `rs_candidatos` status=contratado |
| Agendar entrevista | POST `rs_entrevistas` |
| Alterar status entrevista | PATCH `rs_entrevistas` (+resultado se realizada) — via `prompt()` |
| Avaliar | POST `rs_avaliacoes` |
| Adicionar ao banco | POST `rs_banco_talentos` + PATCH candidato=banco_talentos |
| Remover do banco | DELETE `rs_banco_talentos` + PATCH candidato=ativo |
| Trocar aba | JS puro (classe active) |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico | Detalhe |
|------------|---------|---------|
| Banco | Supabase PostgreSQL | `muiqmtnfvyffborgiwdw.supabase.co` (RH) |
| Auth | Supabase Auth | JWT, `RHAuth` |
| Frontend | Vercel (auto-deploy main) | `app.classiccouros.com.br` |

### 21.2 Bibliotecas Internas

config.js, rh_api.js, rh_auth.js, ds.css.

### 21.3 Dependencias de Dados

`rh_set_updated_at()` (funcao compartilhada). Ponte: `rh_processo`, `rh_proc_admissao`, `rh_funcionarios`, `rh_cargos`, `rh_setores` (para admissao futura).

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Desligamentos e Turnover | F-38 RH | Contraparte do ciclo (saida gera nova vaga) |
| Gestao de Colaboradores | F-10 RH | Destino do candidato aprovado (admissao) |
| Processos RH (admissao) | RH | `rh_proc_admissao` / `rh_executar_admissao` |
| Producao (distribuicao de vagas) | F-37 CQ | Setores operacionais das vagas (Sebo, Miudos, Classificacao...) |
| Reconciliacao ETL | ETL | Dedup de candidatos/funcionarios |

---

## 23. Melhorias SIGA

### SIGA-01 — Hardening RLS

Remover policies `anon` e `USING(true)`; aplicar o RBAC documentado na migration (SELECT autenticado, escrita para admin/rh/gestor).

### SIGA-02 — Integrar Contratacao com Admissao

Ao mover para `aprovado`, gerar `rh_proc_admissao` e disparar `rh_executar_admissao` para criar o funcionario — fechando o ciclo candidato → colaborador (hoje so status=contratado).

### SIGA-03 — Migrar para Stack Padrao RH

Alinhar a tela ao `api.js`/`shell.js`/`ui_components.js`/`page_bootstrap.js` do restante do RH (hoje usa `rh_api.js`, `hub.html`, `login_rh.html`).

### SIGA-04 — Auditoria de Movimentacoes

Registrar em `rh_audit_log` quem moveu etapa, aprovou/reprovou e alterou status (hoje sem trilha).

### SIGA-05 — Validacao de CPF/Telefone e Anti-Duplicidade

Mascara e validacao de CPF/telefone no cadastro; checar `reconc_duplicata_candidato` antes de criar candidato.

### SIGA-06 — Upload de Curriculo

Campo de anexo (Supabase Storage) para curriculo/documentos do candidato.

### SIGA-07 — Drag & Drop no Kanban

Substituir o modal de mover etapa por drag & drop (como o F-38 fluxo_funcionarios), com confirmacao.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Aprovacao nao cria funcionario/`rh_proc_admissao` — so seta candidato=contratado | Ciclo de admissao incompleto; RPC `rh_executar_admissao` ociosa |
| D-02 | RLS permissivo com `anon` vs migration (admin/rh/gestor) | Exposicao de dados de candidatos (LGPD) |
| D-03 | Frontend usa `rh_api.js` (RH_API), nao o `api.js` padrao; navega por `hub.html`/`login_rh.html` | Inconsistencia de stack e de gate de perfil |
| D-04 | Sem checagem de perfil na tela (acesso binario) | Qualquer autenticado edita tudo |
| D-05 | Sem `rh_audit_log` de movimentacoes | Sem rastreabilidade de decisoes |
| D-06 | Sem validacao de CPF/telefone nem upload de curriculo | Qualidade de dados / documentacao |
| D-07 | Tabelas eram views dummy (`SELECT ... WHERE false`), substituidas por tabelas reais em 2026-04-09 | Historico anterior inexistente |

### 24.2 Multiempresa

1. Coluna `empresa_id`/`estabelecimento_id` em `rs_vagas`, `rs_candidatos`, `rs_pipeline`
2. RLS por empresa
3. Vagas e pipeline filtrados por empresa no topbar
4. Banco de talentos compartilhado ou por empresa (config)
5. Catalogo de setores/cargos por empresa (distribuicao)

---

## 25. Criterios de Aceite

### CA-01 — Abrir Vaga com Distribuicao

Criar vaga com titulo/setor/cargo + distribuicao por setor; KPI de vagas abertas reflete posicoes.

### CA-02 — Cadastrar Candidato

Criar candidato (nome obrigatorio) com fonte/origem; aparece na aba e no KPI de ativos.

### CA-03 — Pipeline Kanban

Vincular candidato a vaga (etapa triagem); mover por 8 etapas via modal com notas 1-5; card mostra media/estrelas.

### CA-04 — Aprovacao

Mover para aprovado → candidato vira contratado; KPI de contratados do mes incrementa.

### CA-05 — Agenda

Agendar entrevista vinculada ao pipeline; alterar status; se realizada, registrar resultado; KPI de entrevistas da semana.

### CA-06 — Avaliacao

Ficha com 6 criterios + 5 perguntas + parecer + nota geral gravada em `rs_avaliacoes`.

### CA-07 — Banco de Talentos

Adicionar/remover candidato do banco com tags; status alterna banco_talentos/ativo.

### CA-08 — Criticidade por Setor

Chips de vagas abertas por setor com cor por criticidade (>5 red, >=2 yellow).

### CA-09 — Filtros

Busca e filtros por status/prioridade/fonte/periodo funcionam client-side.

### CA-10 — Seguranca (pretendida)

Acesso restrito por perfil (SIGA-01) e trilha de auditoria (SIGA-04).

---

## 26. Casos de Teste

### CT-01 — Vaga com 30 Posicoes

Criar "Auxiliar de Producao" (30), distribuir por 6 setores → soma = 30; KPI mostra 30 posicoes.

### CT-02 — Duplicata no Pipeline

Vincular candidato ja vinculado a mesma vaga → bloqueado.

### CT-03 — Mover Etapa com Notas

triagem → entrevista_rh com nota_triagem=4 → card mostra media 4/5.

### CT-04 — Aprovar

Mover para aprovado → `rs_candidatos.status='contratado'`; contratados do mes +1.

### CT-05 — Reprovar com Motivo

Mover para reprovado → campo motivo obrigatorio visivel; grava `motivo_reprovacao`.

### CT-06 — Agendar e Realizar

Agendar entrevista_rh; alterar para realizada + resultado=aprovado.

### CT-07 — Avaliacao Completa

6 criterios + parecer recomendado + nota 8.5 → gravado.

### CT-08 — Banco de Talentos

Adicionar candidato com tags "solda,noturno" → status banco_talentos; remover → ativo.

### CT-09 — Filtro por Fonte

Filtrar candidatos fonte=indicacao → lista so indicados.

### CT-10 — Criticidade

Setor com 7 vagas abertas → chip vermelho.

---

## 27. Casos Extremos

### CE-01 — Vaga sem Distribuicao

KPI mostra so o numero de vagas abertas (sem parenteses).

### CE-02 — Candidato sem CPF

Aceito (CPF opcional) — risco de duplicidade (sem validacao).

### CE-03 — Distribuicao > Quantidade

Soma das distribuicoes pode exceder a quantidade da vaga (sem validacao cruzada).

### CE-04 — Etapa Desistiu

Selecionavel no modal mas sem coluna no kanban; sai do funil "em selecao".

### CE-05 — Nota Fora de 1-5

CHECK do banco rejeita (`nota BETWEEN 1 AND 5`).

### CE-06 — Parecer Nulo na Avaliacao

CHECK NOT NULL rejeita; frontend exige `avalParecer`.

### CE-07 — Entrevista Realizada sem Resultado

Status realizada sem resultado → `resultado` fica pendente/null.

### CE-08 — Aprovado sem Admissao

Candidato "contratado" sem funcionario criado (D-01) — funcionario nao existe em `rh_funcionarios`.

### CE-09 — Acesso Anon

RLS permite anon (D-02) — leitura/escrita indevida possivel.

### CE-10 — Remover do Banco Reativa Candidato

DELETE `rs_banco_talentos` → candidato volta a `ativo` mesmo se ja contratado (sem checagem).

---

## 28. Melhorias Futuras

### MF-01 — Portal do Candidato

Auto-cadastro externo (site/QR) alimentando `rs_candidatos` com curriculo.

### MF-02 — Integracao com Job Boards

Publicacao automatica de vagas (LinkedIn, Indeed, Gupy) e importacao de candidatos.

### MF-03 — Triagem por IA

Match automatico candidato x requisitos da vaga (score de aderencia).

### MF-04 — Assinatura Digital de Documentos

Coleta de documentos admissionais com assinatura eletronica (etapa documentacao).

### MF-05 — Metricas de Recrutamento

Time-to-hire, taxa de conversao por etapa/fonte, custo por contratacao.

### MF-06 — Comunicacao Automatizada

WhatsApp/email automatico ao candidato em cada mudanca de etapa (agendamento, aprovacao, reprovacao).

### MF-07 — Onboarding Integrado

Apos admissao, gerar checklist de onboarding (EPIs, treinamentos, ASO admissional).

### MF-08 — Reaproveitamento do Banco de Talentos

Sugerir candidatos do banco automaticamente ao abrir vaga compativel (por tags/area).

---

*Documento gerado em 28/07/2026 — Classic IA — Recrutamento e Selecao*
*Versao 1.0 — Especificacao Funcional Completa F-39*
