# F-38 — Desligamentos e Turnover

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-38 |
| Titulo | Desligamentos, Turnover e Auditoria de Rescisao |
| Modulo | RH (Recursos Humanos — Processos de Saida) |
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

**F-38 — Desligamentos e Turnover** e o modulo responsavel por todo o ciclo de saida de colaboradores da Classic Couros: registro estruturado do desligamento (tipo, causa multinivel e entrevista de saida), workflow de processo com checklist rescisorio, analise de turnover (geral, curto prazo, por setor, por causa), inteligencia de RH (correlacao com produtividade, absenteismo, score e SST), auditoria/conferencia do TRCT enviado pela contabilidade e visualizacao Kanban da movimentacao de pessoal.

O modulo compreende **3 telas principais**, **4 arquivos SQL** de migracao (backend), **7 bibliotecas JS compartilhadas** do RH, e opera sobre o projeto Supabase RH (`muiqmtnfvyffborgiwdw.supabase.co`). E o script **#9** da sequencia oficial de rebuild (`ORDEM_REBUILD.sql`).

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/desligamentos_turnover.html` | Dashboard de turnover, inteligencia RH, registro e importacao Convenia (6 abas) | 1980 |
| 2 | `rh/auditoria_rescisao.html` | Conferencia do TRCT — recalculo de verbas rescisorias vs contabilidade (3 abas) | 1080 |
| 3 | `rh/fluxo_funcionarios.html` | Kanban de movimentacao (por status / por setor) com drag & drop | 246 |

**Bibliotecas compartilhadas (RH):**

| Arquivo | Funcao |
|---------|--------|
| `config.js` | Config Supabase RH (SB_URL `muiqmtnfvyffborgiwdw`, SB_KEY) |
| `rh_auth.js` | Autenticacao RH v2 (`rh_sess_v2`), `RHAuth.init/log` |
| `api.js` | Camada API (contrato `{ok,data,error,status}`), `API.get/post/patch/del/rpc`, `API.setPerfil`, `API.fmtData` |
| `shell.js` | Sidebar + topbar + rotas (`Shell.init({page,title})`) |
| `ui_components.js` | Componentes reutilizaveis |
| `page_bootstrap.js` | Lifecycle de pagina |
| `ds.css` | Design system |

**Arquivos SQL (backend):**

| Arquivo | Objetos criados |
|---------|-----------------|
| `MODULO_DESLIGAMENTOS_TURNOVER.sql` | 4 tabelas (taxonomia, categorias, entrevista_perguntas, entrevista) + 3 RPCs + 6 views + RLS + triggers |
| `migration_workflow_desligamento.sql` | RPCs `rh_abrir_desligamento`/`rh_executar_desligamento` + trigger de bloqueio (workflow via `rh_processo`) |
| `schema_rh_sst_completo.sql` | tabela `rh_desligamentos` |
| `TABELAS_RH_CORE.sql` | tabelas `rh_processo`, `rh_proc_desligamento` |
| `SEED_DESLIGAMENTOS_CONVENIA.sql` | Importacao classificada dos desligamentos historicos do Convenia |

> **Nota arquitetural (3 subsistemas coexistentes):** o desligamento tem **tres representacoes** no banco — (a) `rh_desligamentos` (analitico, usado pelo frontend), (b) `rh_processo`+`rh_proc_desligamento` (workflow com checklist), (c) `rh_funcionarios.status`. Ver secao 24 (divergencias).

---

## 2. Problema

A Classic Couros tem alta rotatividade tipica da industria de couros (~483 desligados de 629 registros historicos). Antes do sistema:

1. **Sem visibilidade de turnover** — Nao havia calculo de taxa de rotatividade geral, de curto prazo (<90 dias) ou por setor.
2. **Causas nao estruturadas** — Motivos de saida em texto livre, impossiveis de agregar. Sem taxonomia (tipo → categoria → descricao).
3. **Entrevista de saida perdida** — Feedback do colaborador que sai nao era capturado nem analisado.
4. **Desligamento sem governanca** — Alteracao direta do status do funcionario, sem checklist (exame demissional, devolucao de EPIs/patrimonio, bloqueio de acessos).
5. **TRCT sem conferencia** — O calculo de rescisao vinha da contabilidade sem uma segunda checagem independente, arriscando pagar verbas erradas.
6. **Sem correlacao preditiva** — Impossivel cruzar quem saiu com produtividade, absenteismo e score para identificar padroes e perfis de risco entre os ativos.

**Solucao:** modulo que (a) registra desligamento estruturado com entrevista, (b) calcula turnover e cruza indicadores, (c) governa o processo via workflow + checklist, (d) audita o TRCT verba a verba, (e) importa historico do Convenia.

---

## 3. Processo Operacional

### 3.1 Registro de Desligamento (`desligamentos_turnover.html` aba "+ Registrar")

1. Seleciona colaborador **ativo** (`des-colaborador`).
2. Informa data de desligamento e a **causa em 3 niveis**:
   - **Nivel 1 (tipo):** pedido_demissao / demissao_empresa / termino_contrato
   - **Nivel 2 (categoria):** admissao / operacional / financeiro / pessoal
   - **Nivel 3 (descricao):** texto detalhado obrigatorio
3. Preenche a **entrevista de saida** (5 perguntas p1-p5, opcionais).
4. Ao salvar: POST em `rh_desligamentos` (motivo gravado como `[CATEGORIA] descricao`, mais campos denormalizados e `tempo_empresa_dias`), depois PATCH em `rh_funcionarios` para `status='desligado'`.

> **Divergencia critica:** o PATCH direto de `status='desligado'` e **bloqueado** pelo trigger `fn_bloquear_desligamento_direto()` (se o workflow estiver deployado). O proprio codigo tem TODO: "Tabela rh_desligamentos em migracao para modulo de processos". Ver secao 24.

### 3.2 Workflow de Desligamento (backend `migration_workflow_desligamento.sql`)

Fluxo governado (via `rh_processo`), APLICADO no banco em 2026-04-09:

1. Frontend chama `rh_abrir_desligamento()` com os dados do formulario.
2. RPC cria `rh_processo` (pai, tipo='desligamento') + `rh_proc_desligamento` (filho) e avanca o status: `rascunho → pendente_aprovacao → aprovado → em_execucao`.
3. Usuario preenche o **checklist rescisorio** (PATCH `rh_proc_desligamento`): exame demissional, devolucao EPIs, devolucao patrimonio, bloqueio de acessos, calculo de rescisao.
4. Usuario efetiva via `rh_executar_desligamento(processo_id)`.
5. RPC verifica o checklist (4 itens obrigatorios), muda `rh_funcionarios.status='desligado'` (autorizado por session variable) e conclui o processo.

### 3.3 Analise de Turnover (aba "Dashboard")

9 KPIs, 6 graficos e insights automaticos (ciclo de demissoes, plano de acao, rotatividade por setor). Ver secao 14.

### 3.4 Inteligencia RH (aba "Inteligencia RH")

Cruza os desligados com: score (`motor_score`), ASO/treinamentos (`rh_asos`, `rh_treinamentos`), absenteismo (`rh_ocorrencias`), entrevista (P2/P4) e custo estimado do turnover (3x salario). Identifica perfil do desligado e qualidade da contratacao.

### 3.5 Analise Cruzada (aba "Analise Cruzada")

Desligados <30 dias, produtivos que sairam, setores de alto turnover, absenteismo pre-desligamento. Espelha a RPC `analise_cruzada_desligamentos` (mas calculado no cliente).

### 3.6 Importacao Convenia (aba "Importar Convenia")

- Via **API publica Convenia** (`public-api.convenia.com.br/api/v3/employees`, header `api-token` do localStorage) OU
- Via **CSV** (separador auto-detectado, so linhas "Concluida", colunas Nome/Data/Tipo obrigatorias).
- Classifica tipo (`classificarTipo`) e causa (`classificarCausa`) automaticamente. Dedup por `nome|data`.

### 3.7 Auditoria de Rescisao (`auditoria_rescisao.html`)

1. Seleciona colaborador (`rh_colaboradores`).
2. Digita os valores do **TRCT da contabilidade** (verba a verba + descontos + totais).
3. Sistema **recalcula** cada verba (secao 6/RN e secao 15) e compara "Contabilidade vs Calculado", sinalizando divergencias (tolerancia R$ 1,00).
4. Salva a auditoria (localStorage), exporta CSV / resumo imprimivel.

### 3.8 Fluxo de Movimentacao (`fluxo_funcionarios.html`)

Quadro **Kanban** com drag & drop, em 2 visoes (por Status / por Setor). Mover um card confirma e faz PATCH em `rh_funcionarios` (status ou setor_id). Ver secao 14.4.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Descricao | Acesso ao modulo |
|--------|-----------|------------------|
| `administrador` | Administrador RH | Total: registrar, abrir/executar desligamento, auditar, importar |
| `rh` | Analista de RH | Registrar/abrir desligamento (unico alem de admin autorizado no workflow), auditar, dashboards |
| `gestor` | Gestor de area | Consulta de dashboards e Kanban (conforme Shell) |
| `diretoria` | Diretoria | Consulta de indicadores |

### 4.2 Permissoes por Tela

| Tela | Controle |
|------|----------|
| `desligamentos_turnover.html` | `Shell.init` + `API.setPerfil(user.perfil)` (sem role granular no HTML) |
| `auditoria_rescisao.html` | `RHAuth.init()` (aborta se `!user`) |
| `fluxo_funcionarios.html` | `Shell.init` + `API.setPerfil` |

### 4.3 Permissao no Backend (workflow)

`rh_abrir_desligamento` valida `perfil IN ('administrador','rh')` (via `rh_usuarios.perfil`, capturado do JWT `request.jwt.claim.sub` dentro de SECURITY DEFINER). Outros perfis recebem erro "Apenas perfis administrador ou rh podem abrir desligamento".

### 4.4 RLS

As 4 tabelas novas (`rh_desligamento_taxonomia/categorias`, `rh_entrevista_perguntas`, `rh_desligamento_entrevista`) tem RLS **permissivo**: policy `FOR ALL TO authenticated USING(true) WITH CHECK(true)` **e** policy equivalente para `anon`. Ver secao 18 e SIGA-04.

---

## 5. Fluxograma

```
REGISTRO / WORKFLOW                          ANALISE / TURNOVER
==================                           ==================

Selecionar colaborador ativo                 loadData():
   |                                           rh_colaboradores (ativos)
   v                                           rh_desligamentos (todos)
Causa 3 niveis                                 rh_ocorrencias (atestados)
  tipo > categoria > descricao                 rh_funcionarios (desligados)
   |                                              |
   v                                              v
Entrevista saida (p1-p5)                      Dedup (rh_desligamentos x rh_funcionarios)
   |                                              |
   v                                              v
[FRONTEND ATUAL]                              9 KPIs + 6 graficos
POST rh_desligamentos                          turnover = deslig / ativos * 100
PATCH rh_funcionarios status=desligado         turnover<90d, tempo medio
   |   (BLOQUEADO por trigger se               por causa / tipo / tempo / setor
   |    workflow deployado)                       |
   |                                              v
[WORKFLOW BACKEND]                            Inteligencia RH
rh_abrir_desligamento()                        custo = 3x salario * n
  rh_processo (pai) + rh_proc_desligamento      cruza score/ASO/absenteismo
  rascunho->pendente->aprovado->em_execucao      perfil de risco (ativos)
   |                                              |
   v                                              v
Checklist rescisorio (PATCH)                  Analise Cruzada
  exame demissional, EPIs,                     faixa tempo, produtividade,
  patrimonio, acessos                          absenteismo pre-deslig
   |                                              |
   v                                              v
rh_executar_desligamento()                    Importar Convenia
  valida checklist (4 itens)                    API v3 ou CSV
  status=desligado (autorizado)                 classificarTipo/Causa
  processo concluido                            dedup nome|data

AUDITORIA RESCISAO                            FLUXO (KANBAN)
=================                             ==============
Selecionar colaborador                        vw_funcionario_base
Digitar TRCT contabilidade                    Colunas: experiencia/ativo/
   |                                            ferias/afastado/desligado
   v                                            (ou por setor)
Recalcular verbas (JS)                            |
  saldo, 13o, ferias, aviso,                      v drag & drop
  FGTS, multa 40/20%                           PATCH rh_funcionarios
   |                                            (status ou setor_id)
   v
Comparar (tolerancia R$1)
  CONFORME / DIVERGENTE
   |
   v
Salvar (localStorage) + CSV/PDF
```

---

## 6. Regras de Negocio

### RN-01 — Taxonomia de Causas (3 niveis)

`rh_desligamento_taxonomia` organiza causas em `tipo > categoria > descricao` (UNIQUE nos 3). Seed com 29 descricoes. Tipos: pedido_demissao, demissao_empresa, termino_contrato, justa_causa, acordo_mutuo. Categorias: admissao, operacional, financeiro, pessoal, disciplinar.

### RN-02 — Tipo de Desligamento (CHECK)

`rh_desligamentos.tipo_desligamento IN (pedido_demissao, demissao_empresa, termino_contrato, justa_causa, acordo_mutuo)`. O frontend tambem manipula `morte` e `outros`, remapeados antes de gravar: `morte → demissao_empresa`, `outros/vazio → demissao_empresa`.

### RN-03 — Categoria de Causa (CHECK)

`rh_desligamentos.categoria_causa IN (admissao, operacional, financeiro, pessoal)`. Valor invalido remapeado para `pessoal`. **No formulario, a categoria e embutida como prefixo `[CATEGORIA]` no campo `motivo`**, nao em coluna propria (o dashboard extrai via regex `/^\[([A-Z]+)\]/`).

### RN-04 — Turnover Geral (frontend)

`turnover_geral = (total_desligados / total_ativos) * 100`, onde `total_ativos` = colaboradores com status='ativo' agora, `total_desligados` = `rh_desligamentos.length` no periodo. **Nao usa admissoes nem headcount medio.**

### RN-05 — Turnover Geral (backend RPC — formula distinta)

`indicadores_turnover`: `turnover = desligados / ((ativos + (ativos + desligados)) / 2) * 100` (headcount medio aproximado). **Divergente da formula do frontend (RN-04).**

### RN-06 — Turnover de Curto Prazo (<90 dias)

Frontend: `(deslig_curto / total_desligados) * 100`, `deslig_curto` = desligados com `tempo_empresa_dias < 90`. Mede qualidade da contratacao.

### RN-07 — Tempo de Empresa

`tempo_empresa_dias = ceil((data_desligamento - data_admissao) / 86400000)` (frontend) / `(data_desligamento - data_admissao)` em dias (backend). Faixas na analise: 0-30, 31-90, 91-180, 181-365, 365+ dias.

### RN-08 — Classificacao de Nivel de Turnover

`> 40%` = CRITICO; `> 20%` = ALTO; `> 10%` = MODERADO; senao SAUDAVEL. Benchmark de qualidade de contratacao (pct <90d): `>40` RUIM, `>20` REGULAR, `>10` BOA, senao EXCELENTE (<10%).

### RN-09 — Situacao por Setor (badge)

Taxa de turnover do setor: `> 30%` = Critico (danger); `> 15%` = Atencao (warning); senao Normal (success).

### RN-10 — Custo do Turnover

`custo_por_deslig = 3x salario` (recrutamento 0.5x + treinamento 1.0x + produtividade 1.5x). `custo_total = custo_por_deslig * total_desligados`. Salario default R$ 2000 quando ausente.

### RN-11 — Classificacao Automatica de Tipo (Convenia)

`classificarTipo(texto)`: "pedido do empregado"/"antecipado pelo empregado"/"quebra empregado" → pedido_demissao; "pedido da empresa"/"antecipado empregador"/"quebra empresa/estagio" → demissao_empresa; "justa causa"+"empresa" → justa_causa; "acordo" → acordo_mutuo; "morte" → morte; "termino/tempo determinado" → termino_contrato; senao outros.

### RN-12 — Classificacao Automatica de Causa

`classificarCausa(motivo, obs, tipo, tempo_dias)`: `tempo <= 90` → admissao; regex salari/financ/proposta/benefic → financeiro; lider/gestor/ambient/carga/conflito → operacional; famil/pessoal/mudanc/saude/cidade → pessoal; adapt/expectativa → admissao; `tipo=termino_contrato` → admissao; senao null.

### RN-13 — Entrevista de Saida (perguntas)

`rh_entrevista_perguntas` com 8 perguntas seed (p1-p8), tipo_campo ∈ (texto, escala_1_5, sim_nao, multipla_escolha). O formulario usa p1-p5. Uma resposta por pergunta por desligamento (UNIQUE `desligamento_id, pergunta_id`).

### RN-14 — Registro Completo (RPC transacional)

`registrar_desligamento_completo` cria desligamento + categorias (multicausal, peso 0-1) + entrevista + audit log, e atualiza `rh_funcionarios.status='inativo'` (**nao 'desligado'** — divergencia RN, ver secao 24).

### RN-15 — Bloqueio de Desligamento Direto

Trigger `fn_bloquear_desligamento_direto` (BEFORE UPDATE em `rh_funcionarios`): impede mudar status para 'desligado' exceto quando `app.desligamento_autorizado='true'` (setado apenas por `rh_executar_desligamento`).

### RN-16 — Checklist Rescisorio Obrigatorio

`rh_executar_desligamento` exige `exame_demissional_ok AND devolucao_epis_ok AND devolucao_patrimonio_ok AND bloqueio_acessos_ok`. Incompleto → excecao com o estado de cada item. (`calculo_rescisao_ok` existe na tabela mas nao entra na validacao.)

### RN-17 — Um Processo Aberto por Funcionario

`rh_abrir_desligamento` recusa se ja existir `rh_processo` tipo desligamento com status NOT IN (concluido, cancelado), ou se o funcionario ja estiver desligado.

### RN-18 — Calculo de Rescisao: Saldo de Salario

`saldo_salario = (salario / 30) * dias_saldo`. Aplica-se a todos os tipos.

### RN-19 — Calculo de Rescisao: 13o e Ferias Proporcionais

Exceto justa_causa: `13o = (salario / 12) * avos_13`; `ferias_prop = (salario / 12) * avos_ferias`; `terco_ferias_prop = ferias_prop / 3`. Regra "15+ dias = mes cheio" (avos informados).

### RN-20 — Calculo de Rescisao: Ferias Vencidas

Se valor contabil > 0: `ferias_venc = salario` (um salario integral por periodo vencido); `terco_venc = ferias_venc / 3`. **Assume exatamente 1 salario, sem multiplos periodos.**

### RN-21 — Calculo de Rescisao: Aviso Previo

`dias_aviso = min(30 + 3*anos_servico, 90)`. `sem_justa_causa` indenizado: `(salario/30)*dias_aviso`. `acordo_484a`: **50%** desse valor. `trabalhado`: 0 (ja no saldo). `pedido_demissao`/`justa_causa`/`fim_experiencia`: sem aviso indenizado.

### RN-22 — Calculo de Rescisao: FGTS e Multa

`fgts_rescisao = (saldo_salario + 13o + aviso) * 0.08` (base parcial, **sem ferias**). Multa: `sem_justa_causa` = `saldo_fgts * 0.40`; `acordo_484a` = `saldo_fgts * 0.20`; demais = 0.

### RN-23 — INSS/IRRF Nao Recalculados

INSS e IRRF sao apenas **informados** pela contabilidade (coluna Calculado = `--`, status "Informado"). Liquido calculado = `total_bruto_calc - total_descontos_contabilidade`.

### RN-24 — Deteccao de Divergencia (auditoria)

Por verba: `diff = round2(contabilidade - calculado)`; match se `|diff| <= 1.00` (tolerancia R$ 1,00). Status global "ok" so se `divergencias=0 AND match_bruto AND match_liquido`.

### RN-25 — Ajuste Automatico de Aviso por Tipo

`onTipoChanged`: sem_justa_causa → aviso indenizado; pedido_demissao/justa_causa/fim_experiencia → nenhum; acordo_484a → indenizado.

### RN-26 — Deduplicacao de Desligados

`loadData`: funcionarios de `rh_funcionarios` (status=desligado) so entram se nao existirem em `rh_desligamentos` (por id ou chave `nome|data`). Import CSV/Convenia dedup por `nome|data` (uppercase). `_origem` marca a fonte.

### RN-27 — Sincronizacao de Extras

`sincronizarFuncionariosExtras` cria registros faltantes: tipo `termino_contrato` se tempo<=90d senao `demissao_empresa`; causa `admissao` se <=90d senao `pessoal`. Payload filtrado pelas colunas existentes no schema detectado.

### RN-28 — Kanban: Regras de Movimentacao

Bloqueia mover para o mesmo status/setor; ao mudar status para != experiencia, forca `em_experiencia=false`; transferencia de setor resolve `setor_id` por nome em `rh_setores` (payload vazio se nao achar); toda movimentacao exige confirmacao. Visao Setor exclui desligados.

---

## 7. Campos

### 7.1 Registro de Desligamento (`desligamentos_turnover.html` — `#form-deslig`)

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Colaborador | `des-colaborador` | select | Sim | Somente status='ativo' |
| Data Desligamento | `des-data` | date | Sim | — |
| Tipo (nivel 1) | `des-tipo` | select | Sim | pedido_demissao / demissao_empresa / termino_contrato |
| Categoria (nivel 2) | `des-categoria` | select | Sim | admissao / operacional / financeiro / pessoal |
| Descricao (nivel 3) | `des-descricao` | textarea | Sim | Motivo detalhado |
| Entrevista P1 | `des-p1` | textarea | Nao | Trabalho diferente do apresentado? |
| Entrevista P2 | `des-p2` | textarea | Nao | O que mais incomodava? |
| Entrevista P3 | `des-p3` | textarea | Nao | Avaliacao da lideranca |
| Entrevista P4 | `des-p4` | textarea | Nao | Voltaria a trabalhar? |
| Entrevista P5 | `des-p5` | textarea | Nao | O que faria permanecer? |

### 7.2 Filtros e Periodo (Dashboard)

`filtro-tipo` (select: todos/pedido/empresa/termino), `filtro-setor` (dinamico). Periodo: `.period-btn` data-meses 3/6/12/24 (default 12).

### 7.3 Auditoria de Rescisao (`auditoria_rescisao.html`)

**Selecao/Base:** `emp-search`, `tipo-rescisao` (sem_justa_causa/pedido_demissao/justa_causa/acordo_484a/fim_experiencia), `dt-admissao` (readonly), `dt-desligamento`, `salario-base` (readonly), `ultimo-salario`.

**Verbas TRCT (contabilidade):** `ct-saldo-dias`/`ct-saldo-valor`, `ct-13-avos`/`ct-13-valor`, `ct-ferias-prop-avos`/`ct-ferias-prop-valor`, `ct-terco-ferias`, `ct-ferias-venc`, `ct-terco-ferias-venc`, `ct-aviso-tipo`/`ct-aviso-dias`/`ct-aviso-valor`, `ct-fgts-rescisao`, `ct-multa-fgts`, `saldo-fgts-estimado`.

**Descontos:** `ct-desc-inss`, `ct-desc-irrf`, `ct-desc-aviso`, `ct-desc-outros`.

**Totais:** `ct-total-bruto`, `ct-total-descontos`, `ct-total-liquido`.

### 7.4 Kanban (`fluxo_funcionarios.html`)

`searchInput` (busca por nome), toggles `tabStatus`/`tabSetor`, contador `totalCount`.

### 7.5 Checklist Rescisorio (backend `rh_proc_desligamento`)

`exame_demissional_ok`, `devolucao_epis_ok`, `devolucao_patrimonio_ok`, `bloqueio_acessos_ok`, `calculo_rescisao_ok`, `aviso_previo_trabalhado/indenizado`, `dias_aviso_previo`, `data_aviso_previo`.

---

## 8. Tabelas

### 8.1 Desligamento Analitico

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rh_desligamentos` | Registro do desligamento | id, funcionario_id (FK rh_funcionarios), data_desligamento, tipo_desligamento (CHECK 5), categoria_causa (CHECK 4), descricao, entrevista_p1..p5, created_at, updated_at. **Colunas extras usadas pelo frontend em prod:** colaborador_nome, motivo, data_admissao, cargo, salario, setor, tempo_empresa_dias, nome |
| `rh_desligamento_taxonomia` | Causas 3 niveis | id, tipo, categoria, descricao, ativo, ordem, UNIQUE(tipo,categoria,descricao) |
| `rh_desligamento_categorias` | Multicausa por desligamento | id, desligamento_id (FK CASCADE), taxonomia_id (FK), nivel (CHECK tipo/categoria/descricao), categoria, subcategoria, peso (0-1), observacao |
| `rh_entrevista_perguntas` | Banco de perguntas | id, codigo (UNIQUE p1..p8), texto, tipo_campo (CHECK), obrigatoria, ordem, ativo |
| `rh_desligamento_entrevista` | Respostas normalizadas | id, desligamento_id (FK CASCADE), pergunta_id (FK), pergunta_texto, resposta, UNIQUE(desligamento_id,pergunta_id) |

### 8.2 Workflow de Processo

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rh_processo` | Processo pai (generico) | id, funcionario_id, tipo (varchar), status (default rascunho), data_solicitacao, data_efetivacao, solicitado_por, aprovado_por/em, executado_por/em, cancelado_por/em, motivo_cancelamento, observacoes |
| `rh_proc_desligamento` | Detalhe do desligamento | id, processo_id, tipo_desligamento, data_desligamento, data_aviso_previo, aviso_previo_trabalhado/indenizado, dias_aviso_previo, categoria_causa, motivo_detalhado, entrevista_realizada, entrevista_p1..p5, exame_demissional_ok, devolucao_epis_ok, devolucao_patrimonio_ok, bloqueio_acessos_ok, calculo_rescisao_ok, status_anterior |

Estados de `rh_processo` (workflow): `rascunho → pendente_aprovacao → aprovado → em_execucao → concluido` | `cancelado`.

### 8.3 Auditoria de Rescisao

Sem tabela dedicada — persistencia em **localStorage** (`rh_auditoria_rescisao_v1`). Ver SIGA-03.

---

## 9. RPCs

### RPC-01 — `registrar_desligamento_completo(p_funcionario_id, p_data, p_tipo, p_categoria, p_descricao, p_categorias jsonb, p_entrevista jsonb)`

**Arquivo:** `MODULO_DESLIGAMENTOS_TURNOVER.sql` | **Modo:** SECURITY INVOKER. Transacional: cria desligamento + categorias (multicausal) + respostas de entrevista + audit log; atualiza `rh_funcionarios.status='inativo'`. Retorna UUID do desligamento. **Nao chamada pelo frontend atual.**

### RPC-02 — `indicadores_turnover(p_meses INT=12)`

**Modo:** STABLE SECURITY INVOKER. Retorna JSONB: total_ativos, total_desligados, desligados_curto_prazo, turnover_geral_pct (headcount medio), turnover_curto_prazo_pct, tempo_medio_empresa_dias, por_causa, por_tipo, por_setor, evolucao_mensal. **Nao chamada pelo frontend (que calcula tudo no cliente).**

### RPC-03 — `analise_cruzada_desligamentos(p_meses INT=12)`

**Modo:** STABLE SECURITY INVOKER. Retorna JSONB: faixa_tempo_empresa, produtividade_desligados (vs ativos), absenteismo_pre_deslig (90d), setor_x_causa, perfil_risco_ativos_top20. **Nao chamada pelo frontend.**

### RPC-04 — `rh_abrir_desligamento(p_funcionario_id, p_tipo, p_data, p_categoria, p_motivo, p_entrevista_p1..p5)`

**Arquivo:** `migration_workflow_desligamento.sql` | **Modo:** SECURITY DEFINER. Valida perfil (administrador/rh), funcionario, unicidade de processo, campos obrigatorios. Cria `rh_processo` + `rh_proc_desligamento` e avanca ate `em_execucao`. Retorna `{ok, processo_id, detalhe_id, status, error, mensagem}`.

### RPC-05 — `rh_executar_desligamento(p_processo_id uuid)`

**Modo:** SECURITY DEFINER. Valida status `em_execucao` e checklist (4 itens), autoriza (session var) e muda `rh_funcionarios.status='desligado'`, conclui processo. RETURNS void (excecao em erro).

---

## 10. Functions (Backend SQL)

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| `registrar_desligamento_completo` | SECURITY INVOKER | Registro transacional completo (RN-14) |
| `indicadores_turnover` | STABLE INVOKER | Indicadores gerais de turnover |
| `analise_cruzada_desligamentos` | STABLE INVOKER | Correlacoes (tempo, produtividade, absenteismo, risco) |
| `rh_abrir_desligamento` | SECURITY DEFINER | Abre processo de desligamento |
| `rh_executar_desligamento` | SECURITY DEFINER | Efetiva desligamento com checklist |
| `fn_bloquear_desligamento_direto` | TRIGGER | Impede UPDATE direto status=desligado (RN-15) |
| `rh_set_updated_at` | TRIGGER | Atualiza updated_at |
| `pg_temp.classificar_tipo` / `classificar_causa` | FUNCTION (temp) | Seed Convenia (classificacao) |

---

## 11. Triggers

| Trigger | Tabela | Evento | Funcao |
|---------|--------|--------|--------|
| `trg_bloquear_desligamento_direto` | `rh_funcionarios` | BEFORE UPDATE | `fn_bloquear_desligamento_direto()` |
| `trg_rh_desligamento_categorias_updated` | `rh_desligamento_categorias` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rh_desligamento_entrevista_updated` | `rh_desligamento_entrevista` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rh_desligamento_taxonomia_updated` | `rh_desligamento_taxonomia` | BEFORE UPDATE | `rh_set_updated_at()` |
| `trg_rh_entrevista_perguntas_updated` | `rh_entrevista_perguntas` | BEFORE UPDATE | `rh_set_updated_at()` |

---

## 12. APIs (Chamadas Frontend)

### 12.1 Supabase REST (PostgREST) — `desligamentos_turnover.html`

**Leitura (GET):**
- `rh_colaboradores?select=id,nome,cargo,setor,data_admissao,status,salario_base`
- `rh_desligamentos?select=*&order=data_desligamento.desc`
- `rh_ocorrencias?select=colaborador_id,tipo,dias,data_inicio&tipo=eq.atestado`
- `rh_funcionarios?status=eq.desligado&data_desligamento=gte.{iso}&select=...&order=data_desligamento.desc`
- `motor_score?select=funcionario_id,score_final,classificacao&order=periodo_referencia.desc&limit=500`
- `rh_asos?select=colaborador_id,data_vencimento,resultado&order=data_vencimento.desc`
- `rh_treinamentos?select=colaborador_id,data_vencimento&order=data_vencimento.desc`

**Escrita:** POST `rh_desligamentos` (return=representation / minimal no import); PATCH `rh_funcionarios?id=eq.{id}` (status=desligado); PATCH `rh_desligamentos?id=eq.{id}` (classificar auto); DELETE `rh_desligamentos?id=eq.{id}` (limpar duplicatas).

**Nenhuma RPC chamada** — turnover e analytics calculados no cliente.

### 12.2 `auditoria_rescisao.html`

GET `rh_colaboradores?select=id,nome,cpf,cargo,salario_base,data_admissao,data_desligamento,status,categoria&order=nome.asc`. Sem RPC, sem gravacao Supabase (localStorage).

### 12.3 `fluxo_funcionarios.html`

GET `vw_funcionario_base?select=id,nome,status,setor,setor_id,cargo,data_admissao,dias_empresa,em_experiencia&order=nome`; GET `rh_setores?nome=eq.{nome}&select=id&limit=1`; PATCH `rh_funcionarios?id=eq.{id}` (status ou setor_id).

### 12.4 API Externa — Convenia

`GET https://public-api.convenia.com.br/api/v3/employees?page=&per_page=100`, header `api-token` (localStorage `convenia_config`).

### 12.5 Bibliotecas Externas

Chart.js v4 (CDN jsdelivr) — `desligamentos_turnover.html`.

---

## 13. Integracoes

### 13.1 Convenia (Sistema de Folha/RH)

- **Tipo:** REST API v3 + import CSV
- **Endpoint:** `public-api.convenia.com.br/api/v3/employees`
- **Uso:** importar historico de desligamentos, classificar tipo/causa
- **Seed:** `SEED_DESLIGAMENTOS_CONVENIA.sql` mapeia tipos Convenia → sistema (Pedido do Empregado→pedido_demissao, etc.), inclui `morte` e `outros`

### 13.2 Modulos RH Internos (Inteligencia)

- **motor_score** (F Motor): score_final, classificacao dos desligados
- **rh_asos / rh_treinamentos** (F-17/SST): conformidade SST no desligamento
- **rh_ocorrencias** (F-14 Absenteismo): dias de atestado pre-desligamento
- **rh_producao**: produtividade dos desligados vs ativos

### 13.3 Contabilidade (TRCT)

Conferencia manual do TRCT (Termo de Rescisao) — entrada manual dos valores da contabilidade, recalculo independente no cliente.

---

## 14. Dashboards

### 14.1 Dashboard de Turnover (`desligamentos_turnover.html` aba Dashboard)

**9 KPIs:** Desligamentos (`kpi-deslig`), Pediu demissao (`kpi-pedido`), Empresa demitiu (`kpi-empresa`, inclui justa_causa), Fim contrato (`kpi-termino`), Sem classificacao (`kpi-semtipo`, oculto se 0), Turnover Geral (`kpi-turnover`), Turnover <90d (`kpi-curto`), Tempo Medio dias (`kpi-tempo`), Ativos Hoje (`kpi-ativos`).

**Alerta de dados:** se ha desligamentos sem tipo, botao "Classificar automaticamente" (PATCH em lote via `classificarTipo`/`classificarCausa`).

**6 Graficos (Chart.js):** Turnover Mensal (bar), Distribuicao por Causa (doughnut), Tipo de Desligamento (doughnut), Distribuicao por Tempo de Casa (doughnut, 5 faixas), Turnover por Setor (bar horizontal, top 8), Pedido vs Demissao por Mes (bar empilhado).

**Insights:** Ciclo de demissoes (Fase/Qtd/Indica/Verificar), Plano de Acao (Area/Prazo/Acao/Impacto), Rotatividade por Setor (Setor/Desligados/Pediram/Demitidos/Ativos/Taxa/Situacao).

### 14.2 Inteligencia RH (aba)

Custo do turnover (3x salario), perfil do desligado (tempo de casa, setores/cargos que mais perdem), cruzamento com score/ASO/entrevista/absenteismo, qualidade da contratacao.

### 14.3 Analise Cruzada (aba)

Desligados <30 dias (Nome/Setor/Dias/Tipo), produtivos que sairam (Nome/Setor/Tempo/Voltaria?), setores alto turnover (Setor/Desligados/Total/Taxa), absenteismo (Nome/Setor/Dias Ausencia/Tempo).

### 14.4 Kanban de Movimentacao (`fluxo_funcionarios.html`)

**Visao Status** (colunas fixas): Experiencia, Ativo, Ferias, Afastado, Desligado. **Visao Setor** (colunas dinamicas, exclui desligados). Card: badge EXP, nome, cargo·setor, tempo·Adm, link "Ver ficha" → `ficha.html?id=X`. Drag & drop → PATCH. Contador total e por coluna.

---

## 15. Relatorios

### 15.1 Comparativo de Rescisao (`auditoria_rescisao.html`)

Tabela `Verba | Contabilidade | Calculado | Diferenca | Status` para 9 verbas + descontos + totais. Status por linha: OK / DIVERGENTE (tolerancia R$ 1,00). Rotulo final CONFORME / DIVERGENTE (N itens).

### 15.2 Historico de Auditorias

Tabela `Data | Colaborador | Tipo | Liq. Contab. | Liq. Calculado | Diferenca | Status | Acoes` (Ver/Excluir). Persistido em localStorage.

### 15.3 Exportacoes (auditoria)

CSV (separador `;`, BOM UTF-8, `auditoria_rescisao_<nome>.csv`) e Resumo HTML imprimivel (`window.print()`).

### 15.4 Preview de Importacao Convenia

Tabela `Nome | Data | Tipo Convenia | Tipo Classificado | Causa | Tempo`.

---

## 16. Documentos

### 16.1 TRCT (Termo de Rescisao do Contrato de Trabalho)

Documento da contabilidade conferido pela tela de auditoria. Verbas: saldo de salario, 13o proporcional, ferias proporcionais + 1/3, ferias vencidas + 1/3, aviso previo, FGTS sobre rescisao, multa FGTS; descontos: INSS, IRRF, aviso, outros.

### 16.2 Regras por Tipo de Rescisao (aba Regras)

Tabela de referencia do que cada tipo paga/desconta (sem justa causa, pedido, justa causa, acordo 484-A, termino experiencia) — ver secao 6 (RN-18 a RN-25).

---

## 17. Evidencias

### 17.1 Entrevista de Saida

Respostas p1-p5 gravadas em `rh_desligamentos` (denormalizado) e/ou `rh_desligamento_entrevista` (normalizado). Views `vw_entrevista_consolidada` e `vw_resumo_entrevistas` agregam.

### 17.2 Audit Log

`registrar_desligamento_completo` grava em `rh_audit_log` (acao 'desligamento_completo', funcionario, cargo, setor, tipo, causa, tempo_empresa_dias).

### 17.3 Checklist Rescisorio

Flags booleanas em `rh_proc_desligamento` (exame, EPIs, patrimonio, acessos) sao a evidencia de conformidade do processo.

---

## 18. Permissoes

### 18.1 RLS — Tabelas do Modulo

As 4 tabelas novas: policy `FOR ALL TO authenticated USING(true) WITH CHECK(true)` + policy identica para `anon`. **Permissivo** (inconsistente com hardening — ver SIGA-04). `rh_desligamentos`, `rh_processo`, `rh_proc_desligamento` seguem o RLS do core RH.

### 18.2 Autorizacao de Negocio

- Abrir desligamento (workflow): apenas `administrador`/`rh` (validado na RPC)
- Efetivar desligamento: via `rh_executar_desligamento` (unico caminho autorizado pelo trigger)
- Telas: gate por `Shell.init`/`RHAuth.init`

---

## 19. Auditoria

### 19.1 Audit Log Central

`rh_audit_log` (usuario_id via auth.uid(), acao, tabela, registro_id, dados_depois JSONB) alimentado por `registrar_desligamento_completo`.

### 19.2 Rastreabilidade do Processo

`rh_processo` registra solicitado_por, aprovado_por/em, executado_por/em, cancelado_por/em. `rh_proc_desligamento.status_anterior` guarda o status do funcionario antes da efetivacao.

### 19.3 Log de Sessao Frontend

`RHAuth.log('auditoria_rescisao_salva', {...})` na auditoria de rescisao.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Registrar desligamento (frontend) | POST `rh_desligamentos` + PATCH `rh_funcionarios` |
| Abrir processo | RPC `rh_abrir_desligamento` |
| Preencher checklist | PATCH `rh_proc_desligamento` |
| Efetivar desligamento | RPC `rh_executar_desligamento` (trigger autoriza status) |
| Trocar periodo | `.period-btn` → `loadData()` |
| Classificar automaticamente | PATCH em lote (`classificarTipo`/`classificarCausa`) |
| Importar Convenia | API v3 / CSV → POST `rh_desligamentos` (dedup) |
| Comparar rescisao | `runComparison()` → painel + status |
| Salvar auditoria | `saveAudit()` → localStorage + `RHAuth.log` |
| Mover card (Kanban) | `confirmMove` → PATCH `rh_funcionarios` |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico | Detalhe |
|------------|---------|---------|
| Banco | Supabase PostgreSQL | `muiqmtnfvyffborgiwdw.supabase.co` (RH) |
| Auth | Supabase Auth | JWT, perfis em `rh_usuarios` |
| Frontend | Vercel (auto-deploy main) | `app.classiccouros.com.br` |
| API externa | Convenia v3 | Import de desligamentos |

### 21.2 Bibliotecas Externas

Chart.js v4.

### 21.3 Bibliotecas Internas

config.js, rh_auth.js, api.js, shell.js, ui_components.js, page_bootstrap.js, ds.css.

### 21.4 Dependencias de Dados

`rh_funcionarios`/`rh_colaboradores`, `rh_setores`, `rh_cargos`, `rh_producao`, `rh_ausencias`/`rh_ocorrencias`, `motor_score`, `rh_asos`, `rh_treinamentos`, `vw_funcionario_base`, `rh_audit_log`, `rh_usuarios`.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Gestao de Colaboradores | F-10 RH | Fonte de funcionarios; status alterado no desligamento |
| Absenteismo | F-14 RH | Absenteismo pre-desligamento (correlacao) |
| Gestao de EPIs | F-17 SST | Devolucao de EPIs no checklist |
| ASO / Treinamentos | SST | Exame demissional + conformidade no desligamento |
| Motor Score | Gestao | Score dos desligados (perdendo bons?) |
| Recrutamento e Selecao | F-11 RH | Ciclo: desligamento → nova vaga → admissao |
| Processos RH (admissao/afastamento) | RH | `rh_processo` compartilhado (pai generico) |

---

## 23. Melhorias SIGA

### SIGA-01 — Unificacao dos 3 Subsistemas de Desligamento

Consolidar `rh_desligamentos` (analitico), `rh_processo`/`rh_proc_desligamento` (workflow) e `rh_funcionarios.status` em um fluxo unico, com o frontend chamando `rh_abrir_desligamento`/`rh_executar_desligamento` em vez de POST direto + PATCH bloqueado.

### SIGA-02 — Frontend Consumir as RPCs de Analytics

`desligamentos_turnover.html` recalcula turnover no cliente e ignora `indicadores_turnover`/`analise_cruzada_desligamentos` e as 6 views. Migrar para consumir o backend (fonte unica de verdade, formulas consistentes).

### SIGA-03 — Persistir Auditoria de Rescisao no Banco

Criar tabela `rh_auditoria_rescisao` (hoje em localStorage) com verbas calculadas, divergencias e vinculo ao processo/desligamento.

### SIGA-04 — Hardening RLS

Remover policies `anon` e `USING(true)` permissivas das tabelas de desligamento/entrevista, aplicando RBAC por perfil (rh/administrador).

### SIGA-05 — Padronizar Status de Saida

Alinhar `status` de saida: `registrar_desligamento_completo` usa `inativo`, workflow/frontend usam `desligado`. Definir um valor canonico.

### SIGA-06 — Gravar Causa em Coluna Propria

Parar de embutir `[CATEGORIA]` no campo `motivo`; gravar `categoria_causa` e vincular `rh_desligamento_categorias` (multicausa com peso).

### SIGA-07 — Calculadora de Rescisao com INSS/IRRF

Implementar tabelas progressivas de INSS/IRRF no motor de auditoria (hoje apenas "Informado").

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas (para correcao no SIGA)

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Frontend faz PATCH direto `status='desligado'`, bloqueado pelo trigger `fn_bloquear_desligamento_direto` | **Bloqueante** se workflow deployado — registro pode falhar (TODO no codigo confirma) |
| D-02 | `registrar_desligamento_completo` seta `status='inativo'`; workflow/frontend usam `'desligado'` | Inconsistencia de estado |
| D-03 | Frontend nao usa RPCs `indicadores_turnover`/`analise_cruzada_desligamentos` nem as 6 views | Formulas de turnover divergentes (RN-04 vs RN-05) |
| D-04 | Causa embutida como `[CATEGORIA]` no `motivo` em vez de `categoria_causa` | Agregacao fragil (regex) |
| D-05 | Auditoria de rescisao so em localStorage | Sem trilha auditavel no banco |
| D-06 | RLS permissivo com acesso `anon` | Exposicao de dados sensiveis de RH |
| D-07 | `rh_desligamentos` deployado em prod, taxonomia/entrevista nao (DRIFT_MAP) | Multicausa/entrevista normalizada indisponivel |

### 24.2 Multiempresa

1. Coluna `empresa_id` em `rh_desligamentos`, `rh_processo`, `rh_proc_desligamento`
2. RLS por empresa (`empresa_id = get_empresa_usuario()`)
3. Turnover e indicadores filtrados por empresa/estabelecimento
4. Regras de rescisao parametrizaveis (convencao coletiva por empresa)
5. Taxonomia de causas compartilhada ou por empresa
6. Dashboards com seletor de empresa no topbar

---

## 25. Criterios de Aceite

### CA-01 — Registrar Desligamento

Registrar com colaborador ativo, tipo, categoria, descricao e entrevista → grava desligamento e atualiza status. (Via workflow: cria `rh_processo` + detalhe.)

### CA-02 — Checklist Obrigatorio

Efetivar so com os 4 itens do checklist marcados; incompleto → erro identificando os pendentes.

### CA-03 — Bloqueio de Bypass

UPDATE direto de `status='desligado'` fora do fluxo → excecao do trigger.

### CA-04 — Turnover

9 KPIs corretos; turnover geral, curto prazo e tempo medio conforme RN-04/06/07. Alerta de sem classificacao quando aplicavel.

### CA-05 — Graficos e Insights

6 graficos renderizados; rotatividade por setor com situacao (RN-09); classificacao de nivel (RN-08).

### CA-06 — Importacao Convenia

Import via API/CSV, classificacao automatica, dedup por nome|data, so linhas concluidas.

### CA-07 — Auditoria de Rescisao

Recalculo das verbas conforme tipo; comparacao com tolerancia R$ 1,00; CONFORME/DIVERGENTE; export CSV/resumo.

### CA-08 — Kanban

Drag & drop move status/setor com confirmacao e PATCH; visao setor exclui desligados; bloqueios de RN-28.

### CA-09 — Inteligencia RH

Custo do turnover (3x salario), perfil de risco, cruzamentos score/ASO/absenteismo.

### CA-10 — Seguranca

Abrir desligamento so por administrador/rh; efetivacao so via RPC autorizada.

---

## 26. Casos de Teste

### CT-01 — Registro Completo (Workflow)

Abrir desligamento (pedido_demissao, causa financeiro) → `rh_processo` em_execucao + detalhe. Preencher checklist. Executar → status desligado, processo concluido.

### CT-02 — Checklist Incompleto

Executar sem devolucao de EPIs → excecao "Checklist incompleto: ... epis(false) ...". Status inalterado.

### CT-03 — Processo Duplicado

Abrir 2o desligamento para funcionario com processo aberto → erro "Ja existe processo em aberto".

### CT-04 — Turnover Geral

10 desligados, 100 ativos → turnover 10.0%; nivel MODERADO.

### CT-05 — Turnover Curto Prazo

4 dos 10 com <90 dias → turnover<90d = 40% (qualidade RUIM).

### CT-06 — Classificacao Automatica

Desligamento sem tipo com motivo "proposta melhor" → classificarTipo/Causa preenche pedido_demissao/financeiro (PATCH).

### CT-07 — Import Convenia CSV

CSV com "Pedido do Empregado", status Concluida → importa como pedido_demissao. Linha "Não informado" na data → ignorada.

### CT-08 — Rescisao Sem Justa Causa

salario 3000, 20 dias saldo, 6 avos, aviso indenizado 5 anos → saldo 2000, 13o 1500, ferias 1500 + 500, aviso (30+15)/30*3000=4500, multa 40% do FGTS. Comparar com contabilidade.

### CT-09 — Rescisao Acordo 484-A

Aviso a 50%, multa 20% FGTS. Verbas conferidas com tolerancia R$ 1,00.

### CT-10 — Kanban Mover para Desligado

Arrastar card ativo → coluna Desligado → confirmar → PATCH status. (Sujeito ao trigger de bloqueio — ver D-01.)

---

## 27. Casos Extremos

### CE-01 — Funcionario Ja Desligado

`rh_abrir_desligamento` → erro "Funcionario ja esta desligado".

### CE-02 — Perfil Sem Permissao

`rh_abrir_desligamento` com perfil gestor → erro "Apenas perfis administrador ou rh...".

### CE-03 — Zero Ativos (divisao)

Turnover com `total_ativos=0` → tratado (0), sem NaN.

### CE-04 — Desligamento Sem Tipo

Card `kpi-semtipo` e alerta exibidos; grafico usa "sem_classificacao".

### CE-05 — Ferias Vencidas Multiplas

Motor assume 1 salario integral (RN-20) — periodos multiplos podem divergir da contabilidade (alerta esperado).

### CE-06 — Justa Causa

13o, ferias proporcionais, 1/3 e multa FGTS zerados; so saldo + ferias vencidas + 1/3 vencidas.

### CE-07 — Salario Ausente

Custo do turnover usa default R$ 2000.

### CE-08 — Duplicata na Importacao

Chave `nome|data` ja existente → linha ignorada (dedup).

### CE-09 — Setor Inexistente (Kanban)

Transferencia para setor sem match em `rh_setores` → PATCH com payload vazio `{}`.

### CE-10 — Divergencia de Centavos

Diferenca <= R$ 1,00 → status OK (tolerancia).

---

## 28. Melhorias Futuras

### MF-01 — Predicao de Turnover (ML)

Modelo preditivo de risco de saida a partir de tempo, produtividade, absenteismo e score (perfil de risco ja calculado em `analise_cruzada_desligamentos`).

### MF-02 — Entrevista de Saida Digital

Formulario enviado ao colaborador (link/QR) com respostas gravadas direto em `rh_desligamento_entrevista`.

### MF-03 — Integracao eSocial (S-2299/S-2399)

Gerar eventos de desligamento para o eSocial a partir do processo.

### MF-04 — Calculadora de Rescisao Completa

INSS/IRRF progressivos, pensao alimenticia, descontos de aviso, homologacao — motor completo com base legal parametrizada.

### MF-05 — Alerta de Retencao

Disparar alerta quando um colaborador de alto score entra em perfil de risco, antes do pedido de demissao.

### MF-06 — Dashboard de Custo Acumulado

Custo de turnover por periodo/setor com tendencia e meta.

### MF-07 — Fluxo de Aprovacao Real

Reativar as etapas `pendente_aprovacao`/`aprovado` (hoje puladas) com aprovadores e SLA.

### MF-08 — Comparativo com Benchmark de Mercado

Turnover da Classic vs media do setor de couros/frigorificos.

---

*Documento gerado em 28/07/2026 — Classic IA — Desligamentos e Turnover*
*Versao 1.0 — Especificacao Funcional Completa F-38*
