# F-11 — Recrutamento e Selecao

## Especificacao Funcional Completa para Rebuild no SIGA ERP

---

## 1. Identificacao

| Atributo | Valor |
|---|---|
| **Codigo** | F-11 |
| **Nome** | Recrutamento e Selecao |
| **Modulo** | RH |
| **Submodulo** | Pessoas / Recrutamento |
| **Pagina legada** | `rh/recrutamento_selecao.html` |
| **Schema SQL** | `migrations/rh/schema_recrutamento_selecao.sql` |
| **Migration** | `migration_recrutamento_selecao.sql` (aplicada 2026-04-09) |
| **Tabelas** | `rs_vagas`, `rs_vagas_distribuicao`, `rs_candidatos`, `rs_pipeline`, `rs_entrevistas`, `rs_avaliacoes`, `rs_banco_talentos` |
| **API layer** | `rh/rh_api.js` (wrapper PostgREST direto, sem contrato padrao) |
| **Rota Shell** | `{ group: 'Pessoas', id: 'admissoes', label: 'Admissoes', href: 'recrutamento_selecao.html', icon: 'user-plus', perfis: ['administrador','rh'] }` |
| **Objetivo** | Gerenciar o ciclo completo de recrutamento e selecao da Classic Couros: abertura de vagas, cadastro de candidatos, pipeline de selecao em formato Kanban (triagem ate aprovacao), agenda de entrevistas, fichas de avaliacao padronizadas, e banco de talentos para reaproveitamento futuro de candidatos. |

---

## 2. Problema que resolve

### Contexto operacional
A Classic Couros (Jardim Alegre/PR) opera com aproximadamente 133 funcionarios ativos em setores industriais (Carga e Descarga, Miudos, Descongelar, Sebo, Classificacao, Couro, entre outros). O turnover na industria de couros e relevante, gerando demanda constante de novas contratacoes, especialmente para posicoes operacionais como "Auxiliar de Producao".

### Dores resolvidas

1. **Falta de visibilidade sobre vagas abertas**: Antes do sistema, o controle de vagas era feito por planilhas ou comunicacao verbal. O RH nao tinha visao consolidada de quantas posicoes estavam abertas, por setor, turno e sexo preferencial.

2. **Perda de candidatos por falta de acompanhamento**: Candidatos que compareciam presencialmente ou eram indicados nao tinham registro sistematizado. Muitos eram perdidos por falta de follow-up.

3. **Processo seletivo nao padronizado**: Sem roteiro de entrevista ou ficha de avaliacao, cada recrutador conduzia o processo de forma diferente, dificultando comparacoes e decisoes objetivas.

4. **Ausencia de pipeline visual**: Nao havia forma de acompanhar em qual etapa cada candidato se encontrava (triagem, entrevista RH, teste pratico, entrevista gestor, exame admissional, documentacao).

5. **Agenda de entrevistas desorganizada**: Entrevistas eram marcadas de forma informal, sem controle de conflitos de horario ou registro de comparecimento.

6. **Banco de talentos inexistente**: Candidatos que nao eram aprovados para uma vaga especifica, mas tinham potencial, nao eram catalogados para vagas futuras.

7. **Distribuicao de vagas por setor**: Vagas grandes (ex: 30 posicoes de Auxiliar de Producao) precisam ser distribuidas entre setores com quantidades, turnos e sexo preferencial distintos. Sem sistema, essa distribuicao era controlada manualmente.

---

## 3. Processo operacional completo

### Fluxo passo a passo

#### Fase 1 — Abertura da vaga
1. **Solicitacao**: Lider de setor ou RH identifica necessidade de contratacao (substituicao, aumento de quadro, nova funcao, temporario).
2. **Cadastro da vaga**: RH acessa a aba "Vagas" e clica em "+ Nova Vaga", preenchendo titulo, cargo, setor, tipo de contrato, turno, quantidade, faixa salarial, prioridade, motivo de abertura, data limite, responsavel, requisitos, descricao e observacoes.
3. **Distribuicao** (opcional): Para vagas com multiplas posicoes, o RH cria registros em `rs_vagas_distribuicao` indicando quantas vagas por setor, sexo preferencial, turno e observacoes.
4. **Status inicial**: A vaga e criada com status "aberta".

#### Fase 2 — Captacao de candidatos
5. **Cadastro de candidatos**: Candidatos sao cadastrados na aba "Candidatos" com dados pessoais (nome, CPF, telefone, email, cidade, UF, escolaridade), disponibilidade, fonte (presencial, indicacao, site, agencia, LinkedIn, WhatsApp), indicado por, pretensao salarial, experiencia e observacoes.
6. **Fontes de captacao**: Presencial (portaria), indicacao de funcionarios, site institucional, agencias de emprego, LinkedIn, WhatsApp, outros.

#### Fase 3 — Vinculacao ao pipeline
7. **Vincular candidato a vaga**: Na aba "Pipeline", o RH clica em "+ Vincular Candidato", seleciona a vaga (apenas vagas com status "aberta" ou "em_selecao") e o candidato (apenas candidatos com status "ativo"), adicionando observacoes opcionais.
8. **Validacao de duplicata**: O sistema verifica se o candidato ja esta vinculado aquela vaga. Se sim, bloqueia com alerta.
9. **Etapa inicial**: O candidato entra na etapa "Triagem" do Kanban.

#### Fase 4 — Processo seletivo (Kanban)
10. **Triagem**: RH analisa perfil do candidato. Atribui nota de triagem (1-5).
11. **Entrevista RH**: RH realiza entrevista. Atribui nota de entrevista (1-5). Registra parecer RH.
12. **Teste pratico**: Candidato realiza teste na area de atuacao. Atribui nota de teste (1-5).
13. **Entrevista Gestor**: Gestor do setor entrevista o candidato. Atribui nota de gestor (1-5). Registra parecer do gestor.
14. **Exame admissional**: Candidato e encaminhado para ASO admissional.
15. **Documentacao**: Candidato entrega documentos para admissao.
16. **Movimentacao entre etapas**: A cada movimentacao, o RH abre o modal "Mover Candidato", seleciona a nova etapa, preenche notas (quando aplicavel), pareceres RH e gestor, e observacoes.

#### Fase 5 — Decisao
17. **Aprovacao**: Candidato e movido para "Aprovado". O sistema automaticamente altera o status do candidato para "contratado" na tabela `rs_candidatos`.
18. **Reprovacao**: Candidato e movido para "Reprovado". O campo "Motivo da Reprovacao" se torna visivel e obrigatoriamente preenchivel.
19. **Desistencia**: Candidato e movido para "Desistiu".

#### Fase 6 — Agenda de entrevistas
20. **Agendamento**: O RH agenda entrevistas na aba "Agenda de Entrevistas" ou diretamente a partir do card Kanban (botao calendario). Informa candidato (pipeline ativo), tipo, data/hora, duracao, local, entrevistador e observacoes.
21. **Acompanhamento**: Entrevistas sao agrupadas por dia, com filtros por periodo (hoje, amanha, semana, mes, todas) e status (agendada, confirmada, realizada, cancelada, nao compareceu).
22. **Alteracao de status**: O RH pode alterar o status da entrevista. Se marcada como "realizada", solicita o resultado (aprovado/reprovado/pendente).

#### Fase 7 — Avaliacao
23. **Ficha de avaliacao**: A qualquer momento do pipeline, o RH pode abrir a ficha de avaliacao de um candidato, preenchendo: avaliador, 6 criterios (1-5: apresentacao, comunicacao, experiencia, motivacao, adequacao a vaga, trabalho em equipe), roteiro de 5 perguntas padronizadas, parecer final (recomendado/recomendado com restricao/nao recomendado), nota geral (0-10) e justificativa.

#### Fase 8 — Banco de talentos
24. **Inclusao**: Candidatos que nao foram aprovados mas tem potencial podem ser adicionados ao Banco de Talentos pela aba "Banco de Talentos" com area de interesse, tags (separadas por virgula), proxima acao e observacoes.
25. **Status automatico**: Ao incluir no banco, o status do candidato muda para "banco_talentos".
26. **Remocao**: Ao remover do banco, o status do candidato volta para "ativo".
27. **Busca**: Busca por nome, tags e area de interesse.

#### Fase 9 — Fechamento da vaga
28. **Alteracao de status**: O RH altera o status da vaga para "fechada" quando todas as posicoes sao preenchidas. O sistema preenche automaticamente a `data_fechamento` com a data corrente.
29. **Suspensao**: A vaga pode ser suspensa temporariamente (status "suspensa").
30. **Cancelamento**: A vaga pode ser cancelada (status "cancelada").

---

## 4. Usuarios

| Perfil | Consulta | Cadastra/Edita | Altera Status | Aprova/Reprova | Avalia | Agenda | Cancela | Exporta | Audita |
|---|---|---|---|---|---|---|---|---|---|
| **administrador** | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim |
| **rh** | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim |
| **gestor** | Nao (sem acesso na rota shell) | Nao | Nao | Nao | Nao | Nao | Nao | Nao | Nao |
| **gestor_confianca** | Nao (sem acesso na rota shell) | Nao | Nao | Nao | Nao | Nao | Nao | Nao | Nao |
| **sst** | Nao (sem acesso na rota shell) | Nao | Nao | Nao | Nao | Nao | Nao | Nao | Nao |

**Observacao**: No legado, a rota `admissoes` no `shell.js` restringe o acesso a `['administrador','rh']`. Usuarios com outros perfis nao veem o link na sidebar e nao tem acesso a pagina. Porem, as policies RLS atuais sao abertas (`USING (true) WITH CHECK (true)`) tanto para `authenticated` quanto para `anon`, o que e uma falha de seguranca.

---

## 5. Fluxograma

```
INICIO
  |
  v
[RH abre nova vaga] → Preenche dados da vaga → Salva (status: aberta)
  |
  v
[Opcional: cria distribuicao por setor] → Define qtd/setor/turno/sexo
  |
  v
[Candidatos chegam] → [?] Fonte
  |--- Presencial → Cadastro na recepcao
  |--- Indicacao → Cadastro com "indicado por"
  |--- Site/LinkedIn/WhatsApp → Cadastro remoto
  |--- Agencia → Cadastro via parceiro
  v
[Cadastra candidato] → Status: ativo
  |
  v
[RH vincula candidato a vaga no Pipeline] → [?] Ja vinculado?
  |--- SIM → Bloqueia (alerta: "Candidato ja vinculado a esta vaga")
  |--- NAO → Cria rs_pipeline com etapa: triagem
  v
◆ KANBAN — Etapas do processo seletivo
  |
  v
[TRIAGEM] → RH analisa perfil → Nota triagem (1-5)
  |
  [?] Aprovado na triagem?
  |--- NAO → Mover para REPROVADO (motivo obrigatorio) → FIM candidato
  |--- SIM → Mover para ENTREVISTA RH
  v
[ENTREVISTA RH] → Agendar entrevista → Realizar → Nota entrevista (1-5) + Parecer RH
  |
  [?] Aprovado na entrevista RH?
  |--- NAO → Mover para REPROVADO → FIM candidato
  |--- SIM → Mover para TESTE PRATICO
  v
[TESTE PRATICO] → Candidato realiza teste na area → Nota teste (1-5)
  |
  [?] Aprovado no teste?
  |--- NAO → Mover para REPROVADO → FIM candidato
  |--- SIM → Mover para ENTREVISTA GESTOR
  v
[ENTREVISTA GESTOR] → Gestor do setor entrevista → Nota gestor (1-5) + Parecer gestor
  |
  [?] Gestor aprova?
  |--- NAO → Mover para REPROVADO → FIM candidato
  |--- SIM → Mover para EXAME ADMISSIONAL
  v
[EXAME ADMISSIONAL] → Candidato faz ASO admissional
  |
  [?] Apto?
  |--- NAO → Mover para REPROVADO → FIM candidato
  |--- SIM → Mover para DOCUMENTACAO
  v
[DOCUMENTACAO] → Candidato entrega documentos
  |
  [?] Documentacao completa?
  |--- NAO → Aguarda / Reprovado
  |--- SIM → Mover para APROVADO
  v
[APROVADO] → Sistema muda status do candidato para "contratado"
  |
  v
[?] Candidato nao aprovado mas com potencial?
  |--- SIM → Adicionar ao BANCO DE TALENTOS (status: banco_talentos)
  |--- NAO → Candidato fica com status ativo ou desistiu
  v
[RH fecha a vaga] → Status: fechada → data_fechamento = CURRENT_DATE
  |
  v
FIM

--- Fluxo paralelo: DESISTENCIA ---
  [A qualquer etapa do Kanban]
  |
  [?] Candidato desiste?
  |--- SIM → Mover para DESISTIU
  v
FIM candidato

--- Fluxo paralelo: AGENDA ---
  [A qualquer etapa do Kanban]
  |
  v
  [RH agenda entrevista] → tipo + data/hora + local + entrevistador
  |
  v
  [Dia da entrevista] → [?] Compareceu?
  |--- SIM → Status: realizada → Resultado (aprovado/reprovado/pendente)
  |--- NAO → Status: nao_compareceu
  |--- Reagendada → Status: reagendada → Nova data
  |--- Cancelada → Status: cancelada
```

---

## 6. Regras de negocio

### Vagas

**RN-01** — O titulo da vaga e obrigatorio. Se nao preenchido, o sistema exibe alerta "Preencha titulo, setor e cargo." e impede o salvamento.

**RN-02** — O setor da vaga e obrigatorio. Mesma validacao da RN-01.

**RN-03** — O cargo da vaga e obrigatorio. Mesma validacao da RN-01.

**RN-04** — O tipo de contrato deve ser um dos valores: `CLT`, `PJ`, `temporario`, `estagio`, `jovem_aprendiz`. Valor padrao: `CLT`.

**RN-05** — O turno deve ser um dos valores: `diurno`, `noturno`, `misto`, `comercial`. Valor padrao: `diurno`.

**RN-06** — A quantidade de vagas deve ser um inteiro >= 1. Valor padrao: 1.

**RN-07** — A prioridade deve ser um dos valores: `baixa`, `normal`, `alta`, `urgente`. Valor padrao: `normal`.

**RN-08** — O status da vaga deve ser um dos valores: `aberta`, `em_selecao`, `suspensa`, `fechada`, `cancelada`. Valor padrao: `aberta`.

**RN-09** — O motivo de abertura deve ser um dos valores: `substituicao`, `aumento_quadro`, `nova_funcao`, `temporario`, `outro`. Valor padrao: `substituicao`.

**RN-10** — A data de abertura e automaticamente preenchida com `CURRENT_DATE` no banco.

**RN-11** — Ao alterar o status para "fechada", o sistema preenche automaticamente `data_fechamento` com a data corrente (`new Date().toISOString().slice(0,10)`).

**RN-12** — A alteracao de status e feita via `prompt()`, aceitando apenas valores validos: `aberta`, `em_selecao`, `suspensa`, `fechada`, `cancelada`. Se o usuario informar valor invalido ou igual ao atual, a operacao e cancelada.

**RN-13** — A data limite e opcional. Quando informada, deve ser uma data futura (nao ha validacao no legado, mas e recomendado).

**RN-14** — O salario e um campo de texto livre (faixa salarial), nao numerico. Ex: "R$ 1.800 - 2.200".

### Candidatos

**RN-15** — O nome completo do candidato e obrigatorio. Se vazio, o sistema exibe alerta "Nome e obrigatorio." e impede o salvamento.

**RN-16** — O CPF e opcional, aceita somente numeros, maxlength=11 caracteres (sem mascara de entrada).

**RN-17** — O status do candidato deve ser um dos valores: `ativo`, `contratado`, `desistiu`, `banco_talentos`, `bloqueado`. Valor padrao: `ativo`.

**RN-18** — A escolaridade deve ser um dos valores: `fundamental`, `medio`, `tecnico`, `superior`, `pos_graduacao` ou vazio.

**RN-19** — A disponibilidade deve ser um dos valores: `imediata`, `15_dias`, `30_dias`, `a_combinar`. Valor padrao: `imediata`.

**RN-20** — A fonte deve ser um dos valores: `indicacao`, `site`, `agencia`, `linkedin`, `whatsapp`, `presencial`, `outro`. Valor padrao: `presencial`.

**RN-21** — A pretensao salarial e um valor numerico com 2 casas decimais (NUMERIC(12,2)). No frontend, usa `type="number" step="0.01"`.

**RN-22** — A UF aceita maximo 2 caracteres e e convertida para maiusculas no salvamento (`toUpperCase()`).

**RN-23** — O campo "indicado por" e texto livre, relevante quando a fonte e "indicacao".

### Pipeline

**RN-24** — Para vincular um candidato a uma vaga: a vaga deve ter status "aberta" ou "em_selecao" (o select so mostra essas), e o candidato deve ter status "ativo" (o select so mostra ativos).

**RN-25** — Nao e permitido vincular o mesmo candidato a mesma vaga duas vezes. O sistema valida no frontend: `pipelines.find(p => p.vaga_id === vaga_id && p.candidato_id === candidato_id)`. Se encontrado, exibe "Candidato ja vinculado a esta vaga." e bloqueia.

**RN-26** — Ao vincular, a etapa inicial e sempre "triagem" e a `data_entrada` e `CURRENT_DATE`.

**RN-27** — As etapas validas do pipeline sao: `triagem`, `entrevista_rh`, `teste_pratico`, `entrevista_gestor`, `exame_admissional`, `documentacao`, `aprovado`, `reprovado`, `desistiu`. Definidas por CHECK constraint no banco.

**RN-28** — As notas de triagem, entrevista, teste e gestor sao inteiros entre 1 e 5 (CHECK constraint no banco).

**RN-29** — Ao mover um candidato para a etapa "reprovado", o campo "Motivo da Reprovacao" se torna visivel. O motivo e salvo no campo `motivo_reprovacao`. Para outras etapas, este campo e `null`.

**RN-30** — Ao mover um candidato para a etapa "aprovado", o sistema automaticamente altera o status do candidato na tabela `rs_candidatos` para "contratado" via PATCH separado.

**RN-31** — A cada movimentacao de etapa, o campo `data_ultima_etapa` e atualizado para a data corrente.

**RN-32** — O Kanban exibe a media das notas preenchidas (triagem, entrevista, teste, gestor) e estrelas correspondentes (arredondamento para inteiro mais proximo).

**RN-33** — As etapas finais (`aprovado`, `reprovado`, `desistiu`) sao excluidas do select de candidatos para agendamento de entrevistas.

### Entrevistas

**RN-34** — Para agendar uma entrevista, o candidato (pipeline) e a data/hora sao obrigatorios. Se faltarem, o sistema exibe "Selecione candidato e data/hora."

**RN-35** — O tipo da entrevista deve ser: `entrevista_rh`, `teste_pratico`, `entrevista_gestor`, `exame_admissional`, `outro`.

**RN-36** — A duracao padrao e 30 minutos.

**RN-37** — Os status possiveis da entrevista sao: `agendada` (padrao), `confirmada`, `realizada`, `cancelada`, `reagendada`, `nao_compareceu`.

**RN-38** — Ao marcar uma entrevista como "realizada", o sistema solicita o resultado via `prompt()`: `aprovado`, `reprovado` ou `pendente`. Se nao informado, assume "pendente".

**RN-39** — A data/hora e armazenada em formato ISO (TIMESTAMPTZ). No frontend, e convertida de `datetime-local` para ISO.

**RN-40** — O resultado da entrevista deve ser: `aprovado`, `reprovado`, `pendente` (CHECK constraint).

### Avaliacoes

**RN-41** — O avaliador e obrigatorio. Se nao preenchido, exibe "Informe o avaliador."

**RN-42** — Os 6 criterios de avaliacao (apresentacao, comunicacao, experiencia, motivacao, adequacao_vaga, trabalho_equipe) sao inteiros de 1 a 5 (CHECK constraint).

**RN-43** — O parecer e obrigatorio e deve ser: `recomendado`, `recomendado_restricao`, `nao_recomendado`.

**RN-44** — A nota geral e um decimal de 0 a 10 com passo de 0.5. No banco: NUMERIC(3,1).

**RN-45** — O roteiro de entrevista possui 5 perguntas padronizadas fixas:
1. "Conte sobre sua experiencia anterior"
2. "Por que quer trabalhar aqui?"
3. "Maior desafio profissional"
4. "Expectativas para a vaga"
5. "Disponibilidade e restricoes"

**RN-46** — Uma avaliacao so pode ser criada para um candidato que esteja em pipeline ativo (nao em `aprovado`, `reprovado`, `desistiu`). Se o candidato nao esta em processo ativo, exibe "Candidato nao esta em nenhum processo ativo. Vincule-o a uma vaga primeiro."

**RN-47** — Multiplas avaliacoes podem ser registradas para o mesmo pipeline (nao ha unicidade). Cada avaliador pode gerar sua propria ficha.

### Banco de talentos

**RN-48** — Nao e permitido adicionar o mesmo candidato ao banco de talentos duas vezes. Validacao no frontend: `bancoTalentos.find(b => b.candidato_id === candidato_id)`. Se encontrado, exibe "Candidato ja esta no banco de talentos."

**RN-49** — Ao adicionar ao banco, o status do candidato e automaticamente alterado para "banco_talentos".

**RN-50** — Ao remover do banco (com confirmacao via `confirm()`), o status do candidato volta para "ativo".

**RN-51** — Tags sao armazenadas como texto simples separado por virgula. Ex: "solda,empilhadeira,noturno".

**RN-52** — A busca no banco filtra por nome do candidato, tags e area de interesse (busca case-insensitive em texto concatenado).

### Distribuicao de vagas

**RN-53** — Cada vaga pode ter multiplas distribuicoes por setor, cada uma com quantidade, sexo preferencial (M/F ou null), turno e observacoes.

**RN-54** — O campo `preenchidas` na distribuicao registra quantas posicoes ja foram preenchidas naquele setor. O progresso e exibido como barra visual com percentual.

**RN-55** — A criticidade visual das vagas abertas por setor e: vermelho (>5 posicoes abertas), amarelo (2-5), verde (<2).

### KPIs

**RN-56** — "Vagas Abertas": conta vagas com status `aberta` ou `em_selecao`. Se houver distribuicoes, exibe tambem o total de posicoes nao preenchidas.

**RN-57** — "Candidatos Ativos": conta candidatos com status `ativo`.

**RN-58** — "Em Selecao": conta pipelines em etapas diferentes de `aprovado`, `reprovado`, `desistiu`.

**RN-59** — "Entrevistas Semana": conta entrevistas na semana corrente (domingo a sabado) com status diferente de `cancelada`.

**RN-60** — "Contratados (mes)": conta pipelines com etapa `aprovado` cuja `data_ultima_etapa` e >= primeiro dia do mes corrente.

**RN-61** — "Banco Talentos": conta total de registros em `rs_banco_talentos`.

---

## 7. Campos

### 7.1. Tabela `rs_vagas`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `titulo` | TEXT | Sim | Formulario | NOT NULL | — | — | Titulo da vaga. Ex: "Auxiliar de Producao" |
| 3 | `setor` | TEXT | Sim | Formulario | NOT NULL | — | — | Nome do setor. Campo texto livre (nao FK) |
| 4 | `cargo` | TEXT | Sim | Formulario | NOT NULL | — | — | Nome do cargo. Campo texto livre (nao FK) |
| 5 | `tipo_contrato` | VARCHAR(30) | Nao | Formulario (select) | CHECK IN ('CLT','PJ','temporario','estagio','jovem_aprendiz') | `'CLT'` | — | Tipo de vinculo |
| 6 | `turno` | VARCHAR(20) | Nao | Formulario (select) | CHECK IN ('diurno','noturno','misto','comercial') | `'diurno'` (frontend) | — | Turno de trabalho |
| 7 | `quantidade` | INTEGER | Nao | Formulario (number) | min=1 | `1` | — | Numero de posicoes abertas |
| 8 | `salario_faixa` | TEXT | Nao | Formulario | — | null | — | Texto livre. Ex: "R$ 1.800 - 2.200" |
| 9 | `requisitos` | TEXT | Nao | Formulario (textarea) | — | null | — | Experiencia, habilidades, certificacoes |
| 10 | `descricao` | TEXT | Nao | Formulario (textarea) | — | null | — | Atividades, responsabilidades |
| 11 | `prioridade` | VARCHAR(20) | Nao | Formulario (select) | CHECK IN ('baixa','normal','alta','urgente') | `'normal'` | — | Nivel de prioridade |
| 12 | `status` | VARCHAR(20) | Nao | Sistema/Formulario | CHECK IN ('aberta','em_selecao','suspensa','fechada','cancelada') | `'aberta'` | — | Status corrente da vaga |
| 13 | `data_abertura` | DATE | Sim (auto) | Sistema | NOT NULL | `CURRENT_DATE` | DD/MM/YYYY (display) | Data de criacao da vaga |
| 14 | `data_limite` | DATE | Nao | Formulario (date) | — | null | YYYY-MM-DD (input) | Prazo para preenchimento |
| 15 | `data_fechamento` | DATE | Nao | Sistema | — | null | — | Preenchido auto ao fechar |
| 16 | `responsavel` | TEXT | Nao | Formulario | — | null | — | Nome do responsavel pela vaga |
| 17 | `motivo_abertura` | VARCHAR(30) | Nao | Formulario (select) | CHECK IN ('substituicao','aumento_quadro','nova_funcao','temporario','outro') | `'substituicao'` | — | Razao da abertura |
| 18 | `observacoes` | TEXT | Nao | Formulario (textarea) | — | null | — | Notas adicionais |
| 19 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Data de criacao |
| 20 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Atualizado via trigger `trg_rs_vagas_updated` |

**Indices**: `idx_vagas_status` (status), `idx_vagas_setor` (setor)

### 7.2. Tabela `rs_vagas_distribuicao`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `vaga_id` | UUID | Sim | Sistema/Formulario | NOT NULL, FK → rs_vagas(id) | — | — | Referencia a vaga pai |
| 3 | `setor` | TEXT | Sim | Formulario | NOT NULL | — | — | Nome do setor na distribuicao |
| 4 | `quantidade` | INTEGER | Sim | Formulario | NOT NULL | `1` | — | Qtd de posicoes neste setor |
| 5 | `sexo_preferencial` | TEXT | Nao | Formulario | — | null | — | 'M' ou 'F' ou null |
| 6 | `turno` | TEXT | Nao | Formulario | — | null | — | Turno especifico |
| 7 | `observacoes` | TEXT | Nao | Formulario | — | null | — | Notas adicionais |
| 8 | `preenchidas` | INTEGER | Nao | Sistema | — | `0` | — | Posicoes ja preenchidas |
| 9 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Data de criacao |

**Observacao**: Esta tabela NAO possui trigger de `updated_at` nem RLS propria no schema de recrutamento (definida em TABELAS_ETL.sql).

### 7.3. Tabela `rs_candidatos`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `nome_completo` | TEXT | Sim | Formulario | NOT NULL | — | — | Nome completo do candidato |
| 3 | `cpf` | VARCHAR(11) | Nao | Formulario | maxlength=11 | null | Somente numeros | Sem mascara visual no input |
| 4 | `telefone` | TEXT | Nao | Formulario | — | null | — | Telefone de contato |
| 5 | `email` | TEXT | Nao | Formulario (type=email) | — | null | — | Email de contato |
| 6 | `cidade` | TEXT | Nao | Formulario | — | null | — | Cidade de residencia |
| 7 | `uf` | VARCHAR(2) | Nao | Formulario | maxlength=2, toUpperCase | null | XX | UF do estado |
| 8 | `escolaridade` | VARCHAR(30) | Nao | Formulario (select) | CHECK IN ('fundamental','medio','tecnico','superior','pos_graduacao') | null | — | Nivel de escolaridade |
| 9 | `experiencia_resumo` | TEXT | Nao | Formulario (textarea) | — | null | — | Resumo de experiencia |
| 10 | `pretensao_salarial` | NUMERIC(12,2) | Nao | Formulario (number) | step=0.01 | null | — | Valor pretendido |
| 11 | `disponibilidade` | VARCHAR(20) | Nao | Formulario (select) | CHECK IN ('imediata','15_dias','30_dias','a_combinar') | `'imediata'` (frontend) | — | Prazo para iniciar |
| 12 | `fonte` | VARCHAR(30) | Nao | Formulario (select) | CHECK IN ('indicacao','site','agencia','linkedin','whatsapp','presencial','outro') | `'presencial'` (frontend) | — | Canal de captacao |
| 13 | `indicado_por` | TEXT | Nao | Formulario | — | null | — | Nome de quem indicou |
| 14 | `observacoes` | TEXT | Nao | Formulario (textarea) | — | null | — | Notas |
| 15 | `status` | VARCHAR(20) | Nao | Sistema | CHECK IN ('ativo','contratado','desistiu','banco_talentos','bloqueado') | `'ativo'` | — | Status corrente |
| 16 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Criacao |
| 17 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Trigger |

**Indices**: `idx_cand_status` (status), `idx_cand_nome` (nome_completo)

### 7.4. Tabela `rs_pipeline`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `vaga_id` | UUID | Sim | Formulario (select) | NOT NULL, FK → rs_vagas(id) | — | — | Vaga associada |
| 3 | `candidato_id` | UUID | Sim | Formulario (select) | NOT NULL, FK → rs_candidatos(id) | — | — | Candidato associado |
| 4 | `etapa` | VARCHAR(30) | Sim | Formulario (select) | NOT NULL, CHECK IN ('triagem','entrevista_rh','teste_pratico','entrevista_gestor','exame_admissional','documentacao','aprovado','reprovado','desistiu') | `'triagem'` | — | Etapa corrente no Kanban |
| 5 | `nota_triagem` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Nota da fase de triagem |
| 6 | `nota_entrevista` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Nota da entrevista RH |
| 7 | `nota_teste` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Nota do teste pratico |
| 8 | `nota_gestor` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Nota da entrevista com gestor |
| 9 | `parecer_rh` | TEXT | Nao | Formulario (textarea) | — | null | — | Parecer textual do RH |
| 10 | `parecer_gestor` | TEXT | Nao | Formulario (textarea) | — | null | — | Parecer textual do gestor |
| 11 | `data_entrada` | DATE | Nao | Sistema | — | `CURRENT_DATE` | — | Data de entrada no pipeline |
| 12 | `data_ultima_etapa` | DATE | Nao | Sistema | — | `CURRENT_DATE` | — | Data da ultima movimentacao |
| 13 | `motivo_reprovacao` | TEXT | Nao | Formulario (textarea) | — | null | — | Preenchido quando etapa=reprovado |
| 14 | `observacoes` | TEXT | Nao | Formulario (textarea) | — | null | — | Notas |
| 15 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Criacao |
| 16 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Trigger |

**Indices**: `idx_pipe_vaga` (vaga_id), `idx_pipe_cand` (candidato_id), `idx_pipe_etapa` (etapa)

### 7.5. Tabela `rs_entrevistas`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `pipeline_id` | UUID | Sim | Formulario (select) | NOT NULL, FK → rs_pipeline(id) | — | — | Pipeline associado |
| 3 | `tipo` | VARCHAR(30) | Sim | Formulario (select) | NOT NULL, CHECK IN ('entrevista_rh','teste_pratico','entrevista_gestor','exame_admissional','outro') | `'entrevista_rh'` (frontend) | — | Tipo da entrevista |
| 4 | `data_hora` | TIMESTAMPTZ | Sim | Formulario (datetime-local) | NOT NULL | — | ISO | Data e hora da entrevista |
| 5 | `duracao_min` | INTEGER | Nao | Formulario (number) | — | `30` | — | Duracao em minutos |
| 6 | `local` | TEXT | Nao | Formulario | — | null | — | Sala, endereco |
| 7 | `entrevistador` | TEXT | Nao | Formulario | — | null | — | Nome do entrevistador |
| 8 | `status` | VARCHAR(20) | Nao | Sistema | CHECK IN ('agendada','confirmada','realizada','cancelada','reagendada','nao_compareceu') | `'agendada'` | — | Status da entrevista |
| 9 | `resultado` | VARCHAR(20) | Nao | Formulario (prompt) | CHECK IN ('aprovado','reprovado','pendente') | null | — | Preenchido ao marcar como realizada |
| 10 | `observacoes` | TEXT | Nao | Formulario (textarea) | — | null | — | Notas |
| 11 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Criacao |
| 12 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Trigger |

**Indices**: `idx_entrev_data` (data_hora), `idx_entrev_status` (status)

### 7.6. Tabela `rs_avaliacoes`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `pipeline_id` | UUID | Sim | Sistema (hidden) | NOT NULL, FK → rs_pipeline(id) | — | — | Pipeline associado |
| 3 | `avaliador` | TEXT | Sim | Formulario | NOT NULL | — | — | Nome do avaliador |
| 4 | `data_avaliacao` | DATE | Nao | Sistema | — | `CURRENT_DATE` | — | Data da avaliacao |
| 5 | `apresentacao` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: apresentacao pessoal |
| 6 | `comunicacao` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: comunicacao |
| 7 | `experiencia` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: experiencia profissional |
| 8 | `motivacao` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: motivacao |
| 9 | `adequacao_vaga` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: adequacao a vaga |
| 10 | `trabalho_equipe` | INTEGER | Nao | Formulario (number) | CHECK BETWEEN 1 AND 5 | null | — | Criterio: trabalho em equipe |
| 11 | `p1_experiencia` | TEXT | Nao | Formulario (textarea) | — | null | — | Resposta pergunta 1 |
| 12 | `p2_motivo` | TEXT | Nao | Formulario (textarea) | — | null | — | Resposta pergunta 2 |
| 13 | `p3_desafio` | TEXT | Nao | Formulario (textarea) | — | null | — | Resposta pergunta 3 |
| 14 | `p4_expectativa` | TEXT | Nao | Formulario (textarea) | — | null | — | Resposta pergunta 4 |
| 15 | `p5_disponibilidade` | TEXT | Nao | Formulario (textarea) | — | null | — | Resposta pergunta 5 |
| 16 | `nota_geral` | NUMERIC(3,1) | Nao | Formulario (number) | min=0, max=10, step=0.5 | null | — | Nota consolidada 0-10 |
| 17 | `parecer` | TEXT | Sim | Formulario (select) | NOT NULL, CHECK IN ('recomendado','recomendado_restricao','nao_recomendado') | `'recomendado'` (frontend) | — | Parecer final |
| 18 | `justificativa` | TEXT | Nao | Formulario (textarea) | — | null | — | Justificativa do parecer |
| 19 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Criacao |
| 20 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Trigger |

**Indice**: `idx_aval_pipe` (pipeline_id)

### 7.7. Tabela `rs_banco_talentos`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra/Default | Mascara | Observacoes |
|---|---|---|---|---|---|---|---|---|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | `gen_random_uuid()` | — | Chave primaria |
| 2 | `candidato_id` | UUID | Sim | Formulario (select) | NOT NULL, FK → rs_candidatos(id) | — | — | Candidato associado |
| 3 | `tags` | TEXT | Nao | Formulario | — | null | — | Tags separadas por virgula |
| 4 | `area_interesse` | TEXT | Nao | Formulario | — | null | — | Area de interesse do candidato |
| 5 | `ultima_interacao` | DATE | Nao | Sistema | — | `CURRENT_DATE` | DD/MM/YYYY (display) | Data da ultima interacao |
| 6 | `proxima_acao` | TEXT | Nao | Formulario | — | null | — | Descricao da proxima acao |
| 7 | `observacoes` | TEXT | Nao | Formulario (textarea) | — | null | — | Notas |
| 8 | `created_at` | TIMESTAMPTZ | Sim (auto) | Sistema | — | `NOW()` | — | Criacao |
| 9 | `updated_at` | TIMESTAMPTZ | Sim (auto) | Trigger | — | `NOW()` | — | Trigger |

**Indice**: `idx_banco_cand` (candidato_id)

---

## 8. Tabelas

### 8.1. Relacionamentos (ERD textual)

```
rs_vagas (1) ──────── (N) rs_vagas_distribuicao
    |                         vaga_id → rs_vagas.id
    |
    └── (1) ──────── (N) rs_pipeline
                              vaga_id → rs_vagas.id
                              candidato_id → rs_candidatos.id
                              |
                              ├── (1) ── (N) rs_entrevistas
                              |               pipeline_id → rs_pipeline.id
                              |
                              └── (1) ── (N) rs_avaliacoes
                                              pipeline_id → rs_pipeline.id

rs_candidatos (1) ── (N) rs_pipeline
    |                      candidato_id → rs_candidatos.id
    |
    └── (1) ──────── (0..1) rs_banco_talentos
                              candidato_id → rs_candidatos.id
```

### 8.2. Tabelas utilizadas diretamente

| Tabela | Operacoes | Finalidade |
|---|---|---|
| `rs_vagas` | SELECT, INSERT, PATCH | Cadastro e gestao de vagas |
| `rs_vagas_distribuicao` | SELECT | Distribuicao de vagas por setor (somente leitura no frontend) |
| `rs_candidatos` | SELECT, INSERT, PATCH | Cadastro de candidatos + atualizacao de status |
| `rs_pipeline` | SELECT, INSERT, PATCH | Pipeline de selecao (Kanban) |
| `rs_entrevistas` | SELECT, INSERT, PATCH | Agenda de entrevistas |
| `rs_avaliacoes` | SELECT, INSERT | Fichas de avaliacao |
| `rs_banco_talentos` | SELECT, INSERT, DELETE | Banco de talentos |

---

## 9. Procedures/RPCs

**Nao ha RPCs dedicadas ao modulo de Recrutamento e Selecao.** Todas as operacoes sao realizadas via chamadas REST diretas ao PostgREST (GET, POST, PATCH, DELETE) sem uso de `RH_API.rpc()` ou `API.rpc()`.

O modulo opera exclusivamente com CRUD via PostgREST. Nenhuma das 18 funcoes em `FUNCOES_APLICACAO.sql` referencia tabelas `rs_*`.

---

## 10. Functions

### Functions SQL utilizadas indiretamente

| Funcao | Tabelas rs_* referenciadas | Finalidade |
|---|---|---|
| `rh_set_updated_at()` | Todas as 6 tabelas rs_* | Trigger function que atualiza o campo `updated_at` para `NOW()` antes de cada UPDATE |

**Definicao da funcao (extraida de `schema_rh_sst_completo.sql`):**
```sql
CREATE OR REPLACE FUNCTION rh_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Nao ha functions de calculo, validacao ou transformacao especificas do modulo de recrutamento.

---

## 11. Triggers

| Trigger | Tabela | Evento | Timing | Funcao |
|---|---|---|---|---|
| `trg_rs_vagas_updated` | `rs_vagas` | UPDATE | BEFORE | `rh_set_updated_at()` |
| `trg_rs_candidatos_updated` | `rs_candidatos` | UPDATE | BEFORE | `rh_set_updated_at()` |
| `trg_rs_pipeline_updated` | `rs_pipeline` | UPDATE | BEFORE | `rh_set_updated_at()` |
| `trg_rs_entrevistas_updated` | `rs_entrevistas` | UPDATE | BEFORE | `rh_set_updated_at()` |
| `trg_rs_avaliacoes_updated` | `rs_avaliacoes` | UPDATE | BEFORE | `rh_set_updated_at()` |
| `trg_rs_banco_talentos_updated` | `rs_banco_talentos` | UPDATE | BEFORE | `rh_set_updated_at()` |

**Observacao**: A tabela `rs_vagas_distribuicao` NAO possui trigger de `updated_at` (nao esta no array do bloco DO no schema de recrutamento, e nao tem coluna `updated_at`).

---

## 12. APIs

### 12.1. Chamadas PostgREST extraidas do codigo

Todas as chamadas usam o wrapper `RH_API` (`rh_api.js`) que constroi URLs no formato `${SB_URL}/rest/v1/${endpoint}` com headers de autenticacao JWT.

#### GET (leitura)

| # | Endpoint PostgREST | Funcao JS | Finalidade |
|---|---|---|---|
| 1 | `rs_vagas?order=created_at.desc` | `carregarVagas()` | Lista todas as vagas ordenadas por criacao descendente |
| 2 | `rs_vagas_distribuicao?order=setor.asc` | `carregarVagas()` | Lista todas as distribuicoes ordenadas por setor |
| 3 | `rs_candidatos?order=created_at.desc` | `carregarCandidatos()` | Lista todos os candidatos |
| 4 | `rs_pipeline?order=created_at.desc` | `carregarPipeline()` | Lista todo o pipeline |
| 5 | `rs_entrevistas?order=data_hora.asc` | `carregarAgenda()` | Lista todas as entrevistas ordenadas por data |
| 6 | `rs_banco_talentos?order=created_at.desc` | `carregarBancoTalentos()` | Lista banco de talentos |

#### POST (criacao)

| # | Endpoint PostgREST | Funcao JS | Payload | Finalidade |
|---|---|---|---|---|
| 1 | `rs_vagas` | `salvarVaga()` | `{titulo, setor, cargo, tipo_contrato, turno, quantidade, salario_faixa, prioridade, motivo_abertura, data_limite, responsavel, requisitos, descricao, observacoes}` | Cria nova vaga |
| 2 | `rs_candidatos` | `salvarCandidato()` | `{nome_completo, cpf, telefone, email, cidade, uf, escolaridade, disponibilidade, fonte, indicado_por, pretensao_salarial, experiencia_resumo, observacoes}` | Cadastra novo candidato |
| 3 | `rs_pipeline` | `salvarPipeline()` | `{vaga_id, candidato_id, etapa:'triagem', observacoes}` | Vincula candidato a vaga |
| 4 | `rs_entrevistas` | `salvarEntrevista()` | `{pipeline_id, tipo, data_hora, duracao_min, local, entrevistador, observacoes}` | Agenda entrevista |
| 5 | `rs_avaliacoes` | `salvarAvaliacao()` | `{pipeline_id, avaliador, parecer, apresentacao, comunicacao, experiencia, motivacao, adequacao_vaga, trabalho_equipe, p1_experiencia, p2_motivo, p3_desafio, p4_expectativa, p5_disponibilidade, nota_geral, justificativa}` | Salva ficha de avaliacao |
| 6 | `rs_banco_talentos` | `salvarBancoTalentos()` | `{candidato_id, area_interesse, tags, proxima_acao, observacoes}` | Adiciona ao banco de talentos |

#### PATCH (atualizacao)

| # | Endpoint PostgREST | Funcao JS | Payload | Finalidade |
|---|---|---|---|---|
| 1 | `rs_vagas?id=eq.{id}` | `salvarVaga()` | Mesmo payload do POST | Edita vaga existente |
| 2 | `rs_vagas?id=eq.{id}` | `alterarStatusVaga()` | `{status}` ou `{status, data_fechamento}` | Altera status da vaga |
| 3 | `rs_candidatos?id=eq.{id}` | `salvarCandidato()` | Mesmo payload do POST | Edita candidato existente |
| 4 | `rs_candidatos?id=eq.{candidato_id}` | `salvarMoverEtapa()` | `{status:'contratado'}` | Marca candidato como contratado (ao aprovar) |
| 5 | `rs_candidatos?id=eq.{candidato_id}` | `salvarBancoTalentos()` | `{status:'banco_talentos'}` | Marca candidato como banco_talentos |
| 6 | `rs_candidatos?id=eq.{candidato_id}` | `removerDoBanco()` | `{status:'ativo'}` | Reverte status ao remover do banco |
| 7 | `rs_pipeline?id=eq.{id}` | `salvarMoverEtapa()` | `{etapa, data_ultima_etapa, nota_triagem, nota_entrevista, nota_teste, nota_gestor, parecer_rh, parecer_gestor, motivo_reprovacao, observacoes}` | Move candidato de etapa no Kanban |
| 8 | `rs_entrevistas?id=eq.{id}` | `salvarEntrevista()` | Mesmo payload do POST | Edita entrevista |
| 9 | `rs_entrevistas?id=eq.{id}` | `alterarStatusEntrevista()` | `{status}` ou `{status, resultado}` | Altera status da entrevista |

#### DELETE (exclusao)

| # | Endpoint PostgREST | Funcao JS | Finalidade |
|---|---|---|---|
| 1 | `rs_banco_talentos?id=eq.{id}` | `removerDoBanco()` | Remove candidato do banco de talentos |

### 12.2. Contrato da API

O modulo usa `RH_API` (`rh_api.js`), NAO o `API` padrao (`api.js`). Diferenca critica:

- **`RH_API.get()`**: Retorna array direto (ou `[]` em erro) — SEM wrapping `{ok, data, error, status}`
- **`RH_API.post()`**: Retorna o `Response` object bruto do fetch
- **`RH_API.fetch()`**: Retorna o `Response` object bruto (usado para PATCH e DELETE)

Isso significa que o modulo nao segue o contrato padrao `{ok, data, error, status}` definido em `api.js`. O tratamento de erros e feito via try/catch com `alert('Erro: ' + e.message)`.

---

## 13. Integracoes

### 13.1. Integracoes atuais

**Nenhuma integracao externa esta implementada** no modulo de Recrutamento e Selecao. O modulo opera exclusivamente com dados locais no Supabase.

### 13.2. Integracoes potenciais identificadas

| Sistema | Tipo | Status | Observacao |
|---|---|---|---|
| Convenia | REST API | Nao integrado | Convenia gerencia dados de funcionarios, mas nao ha sync de candidatos/vagas |
| Portais de emprego | N/A | Nao integrado | Nenhuma integracao com Indeed, Catho, InfoJobs, SINE |
| WhatsApp Business | N/A | Nao integrado | A fonte "WhatsApp" e apenas um rotulo; nao ha integracao real |
| LinkedIn | N/A | Nao integrado | A fonte "LinkedIn" e apenas um rotulo |
| eSocial | N/A | Nao integrado | Evento S-2200 (admissao) nao e disparado pelo recrutamento |

---

## 14. Dashboards

### 14.1. KPIs exibidos na tela (KPI Strip)

| # | KPI | ID HTML | Calculo | Fonte de dados |
|---|---|---|---|---|
| 1 | **Vagas Abertas** | `kpiVagasAbertas` | `vagas.filter(status IN ('aberta','em_selecao')).length`. Se houver distribuicoes, exibe tambem `(totalPosicoes - totalPreenchidas)` entre parenteses. | `rs_vagas` + `rs_vagas_distribuicao` |
| 2 | **Candidatos Ativos** | `kpiCandidatos` | `candidatos.filter(status === 'ativo').length` | `rs_candidatos` |
| 3 | **Em Selecao** | `kpiEmSelecao` | `pipelines.filter(etapa NOT IN ('aprovado','reprovado','desistiu')).length` | `rs_pipeline` |
| 4 | **Entrevistas Semana** | `kpiEntrevistas` | Count de entrevistas na semana corrente (dom-sab) com status != 'cancelada' | `rs_entrevistas` |
| 5 | **Contratados (mes)** | `kpiContratados` | Count de pipelines com etapa='aprovado' e data_ultima_etapa >= primeiro dia do mes | `rs_pipeline` |
| 6 | **Banco Talentos** | `kpiBancoTalentos` | `bancoTalentos.length` (total de registros) | `rs_banco_talentos` |

### 14.2. Painel "Vagas Abertas por Setor" (Pipeline tab)

Exibido na aba Pipeline, mostra um grid de chips por setor com:
- Quantidade de posicoes abertas (total - preenchidas)
- Total de posicoes
- Candidatos em processo
- Criticidade visual:
  - Vermelho (`crit-red`): mais de 5 posicoes abertas
  - Amarelo (`crit-yellow`): 2 a 5 posicoes abertas
  - Verde (`crit-green`): menos de 2 posicoes abertas

### 14.3. Badge no header Triagem

O header da coluna "Triagem" no Kanban exibe um badge com o total de vagas abertas (posicoes nao preenchidas).

---

## 15. Relatorios

**O modulo legado NAO implementa nenhum relatorio.** Nao ha funcionalidade de geracao de relatorios, exportacao de dados agregados ou visualizacoes analiticas alem dos KPIs ja descritos.

### Relatorios recomendados para o SIGA (ver secao 23)

- Relatorio de vagas (status, tempo aberto, candidatos por vaga)
- Relatorio de candidatos por fonte de captacao
- Relatorio de eficiencia do pipeline (taxa de conversao por etapa)
- Relatorio de tempo medio de contratacao (SLA)
- Relatorio de avaliacoes consolidadas

---

## 16. Documentos

**O modulo legado NAO implementa geracao de documentos.**

Nao ha funcionalidade de:
- Exportacao para PDF
- Exportacao para Excel/CSV
- Impressao de fichas
- Geracao de contratos
- Geracao de checklists de documentacao

### Documentos recomendados para o SIGA (ver secao 23)

- Ficha de avaliacao em PDF
- Checklist de documentacao admissional
- Carta proposta
- Relatorio do processo seletivo

---

## 17. Evidencias

**O modulo legado NAO implementa gestao de evidencias.**

Nao ha funcionalidade de:
- Upload de curriculos
- Upload de fotos do candidato
- Anexar documentos (RG, CPF, comprovante de residencia)
- Assinaturas digitais
- Armazenamento de certificados/diplomas

### Evidencias recomendadas para o SIGA (ver secao 23)

- Curriculo (PDF/DOC)
- Foto do candidato
- Documentos digitalizados
- Ficha de avaliacao assinada
- Resultado de exame admissional

---

## 18. Permissoes

### 18.1. Permissoes no frontend (shell.js)

A rota esta configurada com `perfis: ['administrador','rh']`, ou seja, apenas usuarios com perfil `administrador` ou `rh` veem o link na sidebar e acessam a pagina.

### 18.2. Permissoes no banco (RLS)

**ATENCAO — Falha de seguranca identificada:**

As policies RLS atuais sao completamente abertas:

```sql
CREATE POLICY rs_vagas_all ON rs_vagas FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY rs_vagas_anon ON rs_vagas FOR ALL TO anon USING (true) WITH CHECK (true);
```

O mesmo padrao se aplica a TODAS as 6 tabelas `rs_*`. Isso significa:
- Qualquer usuario autenticado pode ler, inserir, atualizar e deletar dados
- Ate usuarios anonimos (sem login) podem fazer tudo
- Nao ha restricao por perfil no nivel do banco

### 18.3. Matriz perfil x acao (legado)

| Acao | administrador | rh | gestor | gestor_confianca | sst | anon |
|---|---|---|---|---|---|---|
| Visualizar pagina | Sim (shell) | Sim (shell) | Nao (shell) | Nao (shell) | Nao (shell) | Nao (shell) |
| Consultar vagas | Sim (RLS) | Sim (RLS) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Cadastrar vaga | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Editar vaga | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Alterar status vaga | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Cadastrar candidato | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Vincular ao pipeline | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Mover etapa | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Aprovar/Reprovar | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Avaliar candidato | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Agendar entrevista | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |
| Excluir do banco | Sim | Sim | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) | Sim (RLS*) |

(*) Marcados com "RLS*" indicam que a policy permite mas o frontend nao expoe — seguranca apenas por obscuridade.

### 18.4. Matriz recomendada para o SIGA

| Acao | admin | rh_analista | rh_coordenador | gestor_setor | visualizador |
|---|---|---|---|---|---|
| Visualizar vagas do seu setor | Sim | Sim | Sim | Sim | Sim |
| Visualizar todas as vagas | Sim | Sim | Sim | Nao | Nao |
| Cadastrar vaga | Sim | Sim | Sim | Solicitar | Nao |
| Editar vaga | Sim | Sim | Sim | Nao | Nao |
| Aprovar abertura de vaga | Sim | Nao | Sim | Nao | Nao |
| Cadastrar candidato | Sim | Sim | Sim | Nao | Nao |
| Mover etapa pipeline | Sim | Sim | Sim | Nao | Nao |
| Aprovar/Reprovar candidato | Sim | Sim (propor) | Sim | Nao | Nao |
| Registrar parecer gestor | Sim | Nao | Nao | Sim | Nao |
| Avaliar candidato | Sim | Sim | Sim | Sim | Nao |
| Cancelar/Excluir | Sim | Nao | Sim | Nao | Nao |
| Exportar relatorios | Sim | Sim | Sim | Nao | Nao |

---

## 19. Auditoria

### 19.1. Auditoria no legado

**O modulo legado NAO implementa auditoria.** Os unicos campos de rastreabilidade sao `created_at` e `updated_at`, que registram apenas a data/hora de criacao e ultima modificacao, sem identificar o usuario responsavel.

Nao ha:
- Registro de quem criou/editou cada registro
- Historico de movimentacoes do pipeline (apenas a etapa atual)
- Log de alteracoes de status
- Registro de quem agendou/cancelou entrevistas
- Trilha de auditoria para avaliacoes

### 19.2. Auditoria recomendada para o SIGA

| Evento | Dados a registrar |
|---|---|
| Vaga criada | usuario, data/hora, dados da vaga |
| Vaga editada | usuario, data/hora, campos alterados (before/after) |
| Status da vaga alterado | usuario, data/hora, status anterior, status novo, motivo |
| Candidato cadastrado | usuario, data/hora, dados do candidato |
| Candidato editado | usuario, data/hora, campos alterados |
| Candidato vinculado a vaga | usuario, data/hora, vaga_id, candidato_id |
| Etapa alterada no pipeline | usuario, data/hora, etapa anterior, etapa nova, notas, pareceres |
| Entrevista agendada | usuario, data/hora, pipeline_id, tipo, data/hora agendada |
| Status da entrevista alterado | usuario, data/hora, status anterior, novo, resultado |
| Avaliacao registrada | usuario, data/hora, pipeline_id, criterios, parecer, nota |
| Candidato adicionado ao banco | usuario, data/hora, candidato_id, area, tags |
| Candidato removido do banco | usuario, data/hora, candidato_id |

---

## 20. Eventos

### 20.1. Eventos no legado

**Nao ha sistema de eventos.** O modulo nao publica nem consome eventos de um barramento. As transicoes de estado sao aplicadas diretamente via PATCH.

### 20.2. Eventos recomendados para o SIGA (Event Bus)

| Evento | Publisher | Consumer(s) | Payload |
|---|---|---|---|
| `vaga.criada` | Modulo R&S | Dashboard RH, Notificacoes | `{vaga_id, titulo, setor, prioridade}` |
| `vaga.status_alterado` | Modulo R&S | Dashboard RH, Notificacoes | `{vaga_id, status_anterior, status_novo}` |
| `vaga.fechada` | Modulo R&S | Dashboard RH, KPIs, Notificacoes | `{vaga_id, data_fechamento}` |
| `candidato.cadastrado` | Modulo R&S | — | `{candidato_id, nome, fonte}` |
| `pipeline.etapa_alterada` | Modulo R&S | Notificacoes, Dashboard RH | `{pipeline_id, etapa_anterior, etapa_nova, candidato_id, vaga_id}` |
| `pipeline.aprovado` | Modulo R&S | Admissao, eSocial (S-2200), Notificacoes | `{pipeline_id, candidato_id, vaga_id}` |
| `pipeline.reprovado` | Modulo R&S | Notificacoes | `{pipeline_id, candidato_id, motivo}` |
| `entrevista.agendada` | Modulo R&S | Notificacoes, Agenda corporativa | `{entrevista_id, data_hora, tipo, candidato}` |
| `entrevista.realizada` | Modulo R&S | Pipeline | `{entrevista_id, resultado}` |
| `avaliacao.registrada` | Modulo R&S | Pipeline | `{avaliacao_id, pipeline_id, parecer, nota}` |
| `banco_talentos.adicionado` | Modulo R&S | — | `{candidato_id, area, tags}` |

---

## 21. Dependencias

### 21.1. Dependencias do legado

| Dependencia | Tipo | Obrigatoria | Observacao |
|---|---|---|---|
| Supabase (PostgREST) | Infraestrutura | Sim | Backend de dados |
| `config.js` | Configuracao | Sim | URL e chave do Supabase |
| `rh_api.js` | API layer | Sim | Wrapper de chamadas HTTP |
| `rh_auth.js` | Autenticacao | Sim | JWT e controle de sessao |
| `ds.css` | Design system | Sim | Estilos visuais |
| Funcao `rh_set_updated_at()` | SQL | Sim | Trigger de updated_at — definida em modulo FUNDACAO |

### 21.2. Dados pre-existentes necessarios

| Dado | Tabela/Origem | Necessidade |
|---|---|---|
| Setores | Texto livre (NAO referencia `rh_setores`) | Nao depende. Setor e digitado manualmente |
| Cargos | Texto livre (NAO referencia `rh_cargos`) | Nao depende. Cargo e digitado manualmente |
| Usuarios RH | `rh_usuarios` (via RHAuth) | Sim. Autenticacao e controle de acesso |

**Observacao importante**: No legado, os campos `setor` e `cargo` em `rs_vagas` sao TEXT livre, nao FK para `rh_setores` ou `rh_cargos`. Isso causa inconsistencia de nomenclatura e impede cruzamento automatico com outros modulos.

---

## 22. Funcionalidades relacionadas

### 22.1. Modulos que dependem ou se beneficiam do R&S

| Modulo | Relacao | Tipo | Observacao |
|---|---|---|---|
| **Admissao / Colaboradores** (`colaboradores.html`) | Pipeline aprovado → admissao | Downstream | Quando candidato e aprovado, deveria alimentar automaticamente o cadastro de funcionarios |
| **ASOs** (`asos.html`) | Exame admissional | Downstream | A etapa "exame_admissional" deveria criar ASO tipo admissional no modulo SST |
| **Desligamentos / Turnover** (`desligamentos_turnover.html`) | Vaga de substituicao | Upstream | Desligamento gera necessidade de nova vaga (motivo: substituicao) |
| **Dashboard RH** (`dashboard_rh.html`) | KPIs de recrutamento | Downstream | Vagas abertas, tempo de preenchimento, taxa de conversao |
| **Motor de Decisao** (`motor_decisoes.html`) | Dados de recrutamento | Referencia | Motor poderia considerar dificuldade de reposicao no score |
| **Alertas/Pendencias** (`alertas_rh.html`) | Vagas urgentes, entrevistas | Downstream | Vagas com prioridade "urgente" ou acima do prazo deveriam gerar alertas |
| **Notificacoes** (`notificacoes.html`) | Eventos de recrutamento | Downstream | Aprovacoes, entrevistas agendadas deveriam gerar notificacoes |
| **eSocial** (`monitor-esocial.html`) | Evento S-2200 | Downstream | Admissao precisa gerar evento eSocial |
| **Beneficios** (`beneficios.html`) | Pacote beneficios | Downstream | Candidato aprovado precisa ser vinculado a beneficios |
| **Ferias** (`ferias.html`) | Periodo aquisitivo | Downstream | Data de admissao inicia contagem de ferias |

### 22.2. Modulos que alimentam o R&S

| Modulo | Dado fornecido | Observacao |
|---|---|---|
| **Organograma** | Estrutura de setores e cargos | No legado, nao e utilizado (campos texto livre) |
| **Desligamentos** | Necessidade de substituicao | No legado, nao ha integracao automatica |

---

## 23. Melhorias para o SIGA

**IMPORTANTE: Nao copiar o legado. As melhorias abaixo devem ser implementadas no SIGA.**

### M-01: Setor e Cargo como FK

No legado, `setor` e `cargo` sao TEXT livre. No SIGA, devem ser FK para as tabelas de setores e cargos do cadastro corporativo, garantindo consistencia e cruzamento com outros modulos.

### M-02: Workflow de aprovacao de vagas

Implementar fluxo de aprovacao: Gestor solicita → RH valida → Diretoria aprova (para aumento de quadro). Vagas de substituicao podem ter aprovacao automatica.

### M-03: SLA de vaga

Calcular e exibir o tempo desde a abertura da vaga. Gerar alertas para vagas que ultrapassem o SLA definido (ex: 30 dias para urgente, 60 dias para normal).

### M-04: Historico de movimentacoes do pipeline

No legado, so se registra a etapa atual. No SIGA, criar tabela `rs_pipeline_historico` para registrar cada transicao com data, usuario, etapa anterior, etapa nova e observacoes.

### M-05: Upload de curriculos e documentos

Implementar upload de arquivos (Supabase Storage ou S3) para curriculos, documentos e fotos dos candidatos.

### M-06: Duplicidade de CPF

Validar CPF unico no cadastro de candidatos. No legado, nao ha validacao de duplicidade por CPF (apenas por vaga+candidato no pipeline).

### M-07: Validacao de CPF

Implementar validacao de digitos verificadores do CPF (algoritmo modulo 11).

### M-08: Mascara de telefone e CPF

No legado, CPF e digitado sem mascara e telefone e texto livre. Implementar mascaras: (XX) XXXXX-XXXX para telefone e XXX.XXX.XXX-XX para CPF.

### M-09: Exportacao de dados

Implementar exportacao para Excel e PDF de vagas, candidatos, pipeline e avaliacoes.

### M-10: Geracao de ficha de avaliacao em PDF

Permitir gerar PDF da ficha de avaliacao para impressao e assinatura.

### M-11: Integracao com eSocial

Ao aprovar candidato, gerar automaticamente o evento S-2200 (Cadastramento Inicial do Vinculo) ou preparar os dados para envio.

### M-12: Notificacoes automaticas

Gerar notificacoes para: vaga proxima do prazo, entrevista agendada (lembrete), candidato aprovado, candidato que nao compareceu a entrevista.

### M-13: Painel de metricas de recrutamento

Dashboard analitico com: taxa de conversao por etapa do funil, tempo medio de contratacao, eficiencia por fonte de captacao, custo por contratacao (se integrado com custos).

### M-14: Drag-and-drop no Kanban

No legado, a movimentacao e por modal (clique no card → seleciona etapa). Implementar drag-and-drop real para arrastar cards entre colunas.

### M-15: Vinculacao automatica com admissao

Ao aprovar no pipeline, pre-popular o formulario de admissao com dados do candidato (nome, CPF, telefone, etc.), evitando redigitacao.

### M-16: Banco de talentos inteligente

Match automatico de tags do banco de talentos com requisitos de novas vagas. Sugerir candidatos do banco ao abrir vaga similar.

### M-17: Campo "created_by" em todas as tabelas

Registrar o usuario que criou cada registro para auditoria completa.

### M-18: Painel de entrevistas com calendario visual

Substituir a agenda de lista por um calendario visual (semanal/mensal) com visualizacao de conflitos.

### M-19: Status "desistiu" deve registrar motivo

No legado, "desistiu" e apenas um status. No SIGA, registrar motivo da desistencia para analise.

### M-20: Fechamento automatico de vaga

Quando todas as posicoes da distribuicao estao preenchidas, sugerir ou efetuar automaticamente o fechamento da vaga.

### M-21: Restricao de edicao por status

No legado, vagas fechadas/canceladas podem ser editadas. No SIGA, bloquear edicao de vagas com status terminal (fechada/cancelada).

### M-22: LGPD — Consentimento e retencao

Registrar consentimento do candidato para armazenamento de dados pessoais. Implementar politica de retencao/exclusao de dados de candidatos nao contratados apos periodo definido.

---

## 24. Adequacao ao SIGA

### 24.1. Multiempresa

No legado, nao ha separacao por empresa (a Classic Couros e empresa unica). No SIGA:
- Tabela `rs_vagas` deve ter coluna `empresa_id` (FK para cadastro de empresas)
- Todas as queries devem filtrar por empresa do usuario logado
- RLS deve considerar `empresa_id`

### 24.2. Multiestabelecimento

No legado, nao ha separacao por filial. No SIGA:
- Tabela `rs_vagas` deve ter coluna `estabelecimento_id` (FK)
- Vagas sao abertas para um estabelecimento especifico
- Distribuicao de vagas pode cruzar estabelecimentos (se a empresa permitir)

### 24.3. RBAC (Role-Based Access Control)

No legado, controle de acesso e apenas no frontend (shell.js). No SIGA:
- Policies RLS devem verificar perfil do usuario via JWT claims
- Implementar permissoes granulares (ver secao 18.4)
- Separar permissoes por acao (visualizar, cadastrar, editar, aprovar, excluir)
- Suportar perfis customizaveis por empresa

### 24.4. Workflow engine

No legado, a movimentacao entre etapas e livre (pode pular etapas). No SIGA:
- Definir transicoes validas entre etapas (state machine)
- Implementar aprovacoes obrigatorias (ex: aprovacao do gestor antes de "entrevista_gestor")
- Permitir configuracao de etapas customizadas por empresa
- Registrar cada transicao com usuario, timestamp e observacoes

### 24.5. Auditoria

No legado, apenas `created_at` e `updated_at`. No SIGA:
- Implementar tabela de auditoria generica (`audit_log`)
- Registrar: tabela, operacao (INSERT/UPDATE/DELETE), usuario, timestamp, dados anteriores, dados novos
- Rastrear alteracoes em todos os campos sensiveis
- Manter historico completo do pipeline (cada movimentacao)

### 24.6. Event bus

No legado, nao ha eventos. No SIGA:
- Publicar eventos para cada transicao de estado (ver secao 20.2)
- Consumers podem reagir: notificacoes, integracao com eSocial, atualizacao de dashboards, gatilhos de workflow

### 24.7. APIs

No legado, chamadas diretas ao PostgREST. No SIGA:
- Implementar camada de API propria (REST ou GraphQL)
- Validacoes de negocio server-side (nao apenas no frontend)
- Paginacao server-side (no legado, carrega TUDO com `order=created_at.desc` sem limit)
- Rate limiting e throttling

### 24.8. LGPD

No legado, nao ha tratamento LGPD. No SIGA:
- Registrar base legal para tratamento de dados pessoais de candidatos
- Implementar termo de consentimento
- Direito de exclusao (anonimizacao de dados de candidatos)
- Politica de retencao (ex: dados de candidatos nao contratados por 2 anos)
- Mascaramento de CPF e dados sensiveis para perfis nao autorizados
- Log de acesso a dados pessoais

### 24.9. Rastreabilidade

No legado, nao ha rastreabilidade alem de timestamps. No SIGA:
- Cada registro deve ter `created_by`, `updated_by` (UUID do usuario)
- Historico completo de cada pipeline (nao apenas etapa atual)
- Trilha de auditoria completa para compliance trabalhista
- Vinculacao do processo seletivo com a ficha do funcionario (apos contratacao)

---

## 25. Criterios de aceite

### Vagas

**CA-01** — O usuario com perfil `rh` ou `administrador` consegue criar uma nova vaga informando titulo, setor e cargo (obrigatorios), e os demais campos opcionais.

**CA-02** — A vaga e criada com status "aberta" e data de abertura = data corrente.

**CA-03** — O usuario consegue editar todos os campos de uma vaga existente.

**CA-04** — O usuario consegue alterar o status da vaga para qualquer valor valido (aberta, em_selecao, suspensa, fechada, cancelada).

**CA-05** — Ao alterar status para "fechada", o campo `data_fechamento` e preenchido automaticamente com a data corrente.

**CA-06** — Vagas sao listadas ordenadas por data de criacao descendente.

**CA-07** — O filtro por status filtra corretamente as vagas exibidas.

**CA-08** — O filtro por prioridade filtra corretamente as vagas exibidas.

**CA-09** — A busca por texto filtra por titulo, setor e cargo (case-insensitive).

**CA-10** — A contagem de candidatos por vaga e exibida corretamente na coluna "Candidatos" da tabela.

**CA-11** — Se a vaga tem distribuicoes, as sub-linhas expansiveis mostram setor, sexo, turno, quantidade e preenchidas com barra de progresso.

### Candidatos

**CA-12** — O usuario consegue cadastrar um candidato informando apenas o nome completo (obrigatorio).

**CA-13** — O candidato e criado com status "ativo".

**CA-14** — O usuario consegue editar todos os campos de um candidato existente.

**CA-15** — O filtro por status filtra corretamente os candidatos.

**CA-16** — O filtro por fonte filtra corretamente os candidatos.

**CA-17** — A busca por texto filtra por nome, cidade e email (case-insensitive).

**CA-18** — O campo UF e convertido para maiusculas ao salvar.

### Pipeline

**CA-19** — O usuario consegue vincular um candidato ativo a uma vaga aberta/em_selecao.

**CA-20** — O sistema impede vincular o mesmo candidato a mesma vaga duas vezes, exibindo mensagem de erro.

**CA-21** — O candidato vinculado aparece na coluna "Triagem" do Kanban.

**CA-22** — O usuario consegue mover um candidato para qualquer etapa do pipeline.

**CA-23** — As notas (1-5) sao preservadas ao mover entre etapas.

**CA-24** — O campo "Motivo da Reprovacao" so aparece quando a etapa selecionada e "reprovado".

**CA-25** — Ao mover para "aprovado", o status do candidato em `rs_candidatos` e alterado para "contratado".

**CA-26** — A media das notas e exibida nos cards do Kanban com estrelas visuais.

**CA-27** — O filtro de vaga no pipeline filtra corretamente os cards exibidos no Kanban.

**CA-28** — O painel "Vagas Abertas por Setor" exibe corretamente as posicoes abertas com criticidade visual.

### Entrevistas

**CA-29** — O usuario consegue agendar uma entrevista informando candidato (pipeline) e data/hora (obrigatorios).

**CA-30** — Entrevistas sao agrupadas por dia na visualizacao.

**CA-31** — O filtro por periodo (hoje, amanha, semana, mes, todas) funciona corretamente.

**CA-32** — O filtro por status funciona corretamente.

**CA-33** — O usuario consegue alterar o status da entrevista.

**CA-34** — Ao marcar como "realizada", o sistema solicita e registra o resultado.

**CA-35** — O usuario consegue editar todos os campos de uma entrevista existente.

### Avaliacoes

**CA-36** — O usuario consegue abrir a ficha de avaliacao a partir do card no Kanban ou da lista de candidatos.

**CA-37** — Ao abrir a ficha de avaliacao a partir da lista de candidatos, o sistema encontra o pipeline ativo do candidato. Se nao houver, exibe mensagem de erro.

**CA-38** — O usuario consegue preencher os 6 criterios, 5 perguntas, parecer, nota geral e justificativa.

**CA-39** — O avaliador e obrigatorio para salvar a avaliacao.

**CA-40** — Multiplas avaliacoes podem ser salvas para o mesmo candidato/pipeline.

### Banco de talentos

**CA-41** — O usuario consegue adicionar um candidato ativo ao banco de talentos.

**CA-42** — O sistema impede adicionar o mesmo candidato ao banco duas vezes.

**CA-43** — Ao adicionar, o status do candidato muda para "banco_talentos".

**CA-44** — O usuario consegue remover um candidato do banco (com confirmacao).

**CA-45** — Ao remover, o status do candidato volta para "ativo".

**CA-46** — A busca no banco filtra por nome, tags e area de interesse.

### KPIs

**CA-47** — Todos os 6 KPIs sao calculados e exibidos corretamente na KPI strip.

**CA-48** — Os KPIs sao atualizados apos cada operacao (salvar, mover, vincular, etc.).

### Geral

**CA-49** — A pagina requer autenticacao via RHAuth. Usuarios nao autenticados sao redirecionados para login.

**CA-50** — As abas (Vagas, Candidatos, Pipeline, Agenda, Banco) alternam corretamente o conteudo visivel.

**CA-51** — Empty states sao exibidos quando nao ha dados em cada aba.

**CA-52** — Erros de API sao tratados com `alert()` mostrando a mensagem de erro.

---

## 26. Casos de teste

### Fluxo feliz

**CT-01** — Criar vaga, vincular candidato, mover por todas as etapas ate aprovacao, verificar que candidato ficou como "contratado".
- Pre-condicao: Usuario autenticado como `rh`.
- Passos: Criar vaga "Auxiliar de Producao" / Setor "Producao" / Cargo "Auxiliar" → Cadastrar candidato "Joao Silva" → Aba Pipeline → Vincular Joao a vaga → Mover: Triagem (nota 4) → Entrevista RH (nota 4) → Teste Pratico (nota 3) → Entrevista Gestor (nota 5) → Exame Admissional → Documentacao → Aprovado.
- Resultado esperado: Pipeline na coluna "Aprovado". Candidato com status "contratado" na aba Candidatos. KPI "Contratados (mes)" incrementado.

**CT-02** — Cadastrar candidato com todos os campos preenchidos e verificar persistencia.
- Pre-condicao: Usuario autenticado.
- Passos: Preencher todos os campos do formulario de candidato (nome, CPF, telefone, email, cidade, UF, escolaridade, disponibilidade, fonte, indicado por, pretensao salarial, experiencia, observacoes) → Salvar → Editar candidato.
- Resultado esperado: Todos os campos preenchidos aparecem corretamente no formulario de edicao.

**CT-03** — Agendar entrevista e marcar como realizada com resultado.
- Pre-condicao: Candidato vinculado a vaga (pipeline ativo).
- Passos: Aba Agenda → Agendar Entrevista (tipo: Entrevista RH, data: hoje, duracao: 30min, local: Sala 1, entrevistador: Maria) → Alterar status para "realizada" → Informar resultado "aprovado".
- Resultado esperado: Entrevista com status "realizada" e resultado "aprovado" na agenda.

**CT-04** — Preencher ficha de avaliacao completa.
- Pre-condicao: Candidato em pipeline ativo.
- Passos: No card Kanban, clicar no botao de avaliacao → Preencher avaliador, 6 criterios (1-5), 5 perguntas, parecer "recomendado", nota geral 8.5, justificativa → Salvar.
- Resultado esperado: Mensagem "Avaliacao salva com sucesso!".

**CT-05** — Adicionar candidato ao banco de talentos.
- Pre-condicao: Candidato com status "ativo".
- Passos: Aba Banco → Adicionar ao Banco → Selecionar candidato → Area: "Producao", Tags: "solda,empilhadeira", Proxima acao: "Ligar em agosto" → Salvar.
- Resultado esperado: Candidato aparece no banco. Status do candidato = "banco_talentos". KPI "Banco Talentos" incrementado.

### Fluxos alternativos

**CT-06** — Reprovar candidato e verificar que motivo de reprovacao e exigido visualmente.
- Pre-condicao: Candidato na etapa "Entrevista RH".
- Passos: Clicar no card → Mover para "Reprovado".
- Resultado esperado: Campo "Motivo da Reprovacao" aparece no modal. O registro e salvo com o motivo informado.

**CT-07** — Candidato desiste durante o processo.
- Pre-condicao: Candidato na etapa "Teste Pratico".
- Passos: Clicar no card → Mover para "Desistiu".
- Resultado esperado: Card aparece na coluna "Reprovado" (nota: no legado nao ha coluna "Desistiu" no Kanban, o candidato sai das colunas ativas). O candidato nao aparece mais no select de pipelines ativos para entrevistas.

**CT-08** — Suspender e reabrir vaga.
- Pre-condicao: Vaga com status "aberta".
- Passos: Alterar status para "suspensa" → Verificar que vaga nao aparece no select de vagas do pipeline → Alterar status para "aberta" → Verificar que vaga volta a aparecer.
- Resultado esperado: Vaga com status "suspensa" nao aparece no select (so "aberta" e "em_selecao").

**CT-09** — Fechar vaga e verificar data de fechamento.
- Pre-condicao: Vaga com status "aberta".
- Passos: Alterar status para "fechada".
- Resultado esperado: `data_fechamento` preenchido com a data corrente. Vaga aparece com badge "Fechada".

**CT-10** — Remover candidato do banco de talentos.
- Pre-condicao: Candidato no banco de talentos.
- Passos: Na aba Banco, clicar em "Remover" (lixeira) → Confirmar.
- Resultado esperado: Candidato removido do banco. Status volta para "ativo".

### Erros e validacoes

**CT-11** — Tentar criar vaga sem titulo.
- Passos: Abrir modal de vaga → Deixar titulo vazio → Clicar Salvar.
- Resultado esperado: Alert "Preencha titulo, setor e cargo." Vaga nao e salva.

**CT-12** — Tentar criar vaga sem setor.
- Resultado esperado: Mesmo alert da CT-11.

**CT-13** — Tentar criar vaga sem cargo.
- Resultado esperado: Mesmo alert da CT-11.

**CT-14** — Tentar cadastrar candidato sem nome.
- Passos: Abrir modal de candidato → Deixar nome vazio → Clicar Salvar.
- Resultado esperado: Alert "Nome e obrigatorio." Candidato nao e salvo.

**CT-15** — Tentar vincular candidato a vaga duplicadamente.
- Pre-condicao: Candidato ja vinculado a vaga X.
- Passos: Pipeline → Vincular Candidato → Selecionar mesma vaga X e mesmo candidato.
- Resultado esperado: Alert "Candidato ja vinculado a esta vaga." Pipeline nao e criado.

**CT-16** — Tentar adicionar candidato ja existente no banco de talentos.
- Pre-condicao: Candidato ja no banco.
- Passos: Aba Banco → Adicionar ao Banco → Selecionar mesmo candidato.
- Resultado esperado: Alert "Candidato ja esta no banco de talentos."

**CT-17** — Tentar agendar entrevista sem pipeline.
- Passos: Abrir modal de entrevista → Nao selecionar candidato → Clicar Salvar.
- Resultado esperado: Alert "Selecione candidato e data/hora."

**CT-18** — Tentar agendar entrevista sem data/hora.
- Resultado esperado: Mesmo alert da CT-17.

**CT-19** — Tentar salvar avaliacao sem avaliador.
- Passos: Abrir ficha de avaliacao → Deixar avaliador vazio → Salvar.
- Resultado esperado: Alert "Informe o avaliador."

**CT-20** — Tentar avaliar candidato sem pipeline ativo.
- Pre-condicao: Candidato com status "ativo" mas sem vinculo a vaga.
- Passos: Na lista de candidatos, clicar no botao de avaliacao.
- Resultado esperado: Alert "Candidato nao esta em nenhum processo ativo. Vincule-o a uma vaga primeiro."

### Permissoes

**CT-21** — Usuario com perfil "gestor" nao deve ver o link "Admissoes" na sidebar.
- Resultado esperado: Link ausente no menu lateral.

**CT-22** — Usuario nao autenticado tenta acessar a pagina.
- Resultado esperado: Redirecionamento para `login_rh.html`.

### Concorrencia

**CT-23** — Dois usuarios editam a mesma vaga simultaneamente.
- Resultado esperado: No legado, nao ha controle de concorrencia. O ultimo PATCH sobrescreve o primeiro. No SIGA, implementar optimistic locking com versioning.

**CT-24** — Dois usuarios movem o mesmo candidato no pipeline simultaneamente.
- Resultado esperado: No legado, o ultimo PATCH vence. No SIGA, verificar se a etapa nao mudou desde a abertura do modal.

### Integracao

**CT-25** — Verificar que `data_hora` da entrevista e armazenada em UTC/ISO.
- Passos: Agendar entrevista para 2026-07-28 14:00 → Verificar valor salvo no banco.
- Resultado esperado: Valor em formato ISO com timezone (`2026-07-28T17:00:00.000Z` para BRT).

---

## 27. Casos extremos

**CE-01** — Vaga com 0 posicoes na distribuicao (campo `quantidade` = 0 ou negativo).
- Impacto: Divisao por zero no calculo de percentual de preenchimento.
- Mitigacao no legado: `d.quantidade > 0 ? Math.round(...) : 0`.
- Recomendacao SIGA: CHECK constraint `quantidade > 0`.

**CE-02** — Candidato vinculado a multiplas vagas simultaneamente.
- Comportamento legado: Permitido. Um candidato pode estar em pipeline de vagas diferentes ao mesmo tempo.
- Impacto: Nenhum problema tecnico. Ao clicar em "avaliar" na lista de candidatos, encontra o primeiro pipeline ativo (`pipelines.find`), nao necessariamente o desejado.
- Recomendacao SIGA: Mostrar lista de pipelines ativos do candidato para selecao.

**CE-03** — Candidato aprovado em uma vaga e ainda em processo em outra.
- Comportamento legado: O status do candidato muda para "contratado" ao ser aprovado em qualquer vaga. Isso pode afetar o pipeline da outra vaga (candidato nao aparece mais nos selects de candidatos ativos).
- Recomendacao SIGA: Ao aprovar em uma vaga, encerrar automaticamente os pipelines das outras vagas com status "cancelado por contratacao em outra vaga".

**CE-04** — Vaga com distribuicao por setor, onde a soma das quantidades na distribuicao excede a `quantidade` da vaga pai.
- Comportamento legado: Nao ha validacao de consistencia entre `rs_vagas.quantidade` e `SUM(rs_vagas_distribuicao.quantidade)`.
- Recomendacao SIGA: Validar que a soma das distribuicoes nao exceda a quantidade total da vaga.

**CE-05** — CPF duplicado em candidatos diferentes.
- Comportamento legado: Nao ha validacao de unicidade do CPF. Dois candidatos podem ter o mesmo CPF.
- Recomendacao SIGA: Unique index parcial em `cpf` (WHERE `cpf IS NOT NULL AND cpf != ''`).

**CE-06** — Nome do candidato com caracteres especiais ou muito longo.
- Comportamento legado: A funcao `esc()` faz XSS escape (`textContent` → `innerHTML`). Nao ha limite de tamanho alem do tipo TEXT do PostgreSQL.
- Recomendacao SIGA: Validar tamanho maximo (ex: 200 caracteres) e sanitizar entrada.

**CE-07** — Entrevista agendada para data passada.
- Comportamento legado: Permitido. Nao ha validacao de que `data_hora` seja futura.
- Recomendacao SIGA: Alertar (nao bloquear) quando a data/hora for no passado.

**CE-08** — Milhares de candidatos e pipelines carregados de uma vez.
- Comportamento legado: `carregarTudo()` carrega TODOS os registros das 6 tabelas sem paginacao (`?order=...` sem `&limit=`). Com milhares de registros, pode causar lentidao no navegador.
- Recomendacao SIGA: Implementar paginacao server-side com `limit` e `offset`, e carregamento sob demanda (lazy loading).

**CE-09** — Candidato removido do banco de talentos e depois adicionado novamente.
- Comportamento legado: Permitido. O status volta para "ativo" e pode ser adicionado novamente ao banco.
- Recomendacao SIGA: Manter historico de inclusao/remocao do banco.

**CE-10** — Modal de mover etapa aberto para candidato que ja foi movido por outro usuario.
- Comportamento legado: O modal mostra a etapa que estava no cache local. Ao salvar, sobrescreve com a etapa selecionada, ignorando que o candidato ja foi movido.
- Recomendacao SIGA: Verificar etapa atual no servidor antes de salvar (optimistic locking).

**CE-11** — Avaliacao salva sem nenhum criterio preenchido (apenas avaliador e parecer).
- Comportamento legado: Permitido. Os criterios (apresentacao, comunicacao, etc.) aceitam null.
- Recomendacao SIGA: Exigir preenchimento de pelo menos 3 criterios para calcular media significativa.

**CE-12** — Entrevista do tipo "exame_admissional" nao dispara criacao de ASO.
- Comportamento legado: Nenhuma integracao entre modulos. A entrevista e apenas um registro de agenda.
- Recomendacao SIGA: Integrar com modulo SST para criacao automatica de ASO admissional.

---

## 28. Melhorias futuras

**MF-01** — **Integracao com portais de emprego**: Publicar vagas automaticamente em portais como Indeed, InfoJobs, SINE, Catho via APIs desses portais. Receber candidaturas diretamente no pipeline.

**MF-02** — **Formulario de candidatura online**: Landing page publica onde candidatos podem se inscrever para vagas abertas sem necessidade de comparecer presencialmente. Dados alimentam automaticamente `rs_candidatos`.

**MF-03** — **Match automatico candidato-vaga**: Algoritmo de compatibilidade que cruza requisitos da vaga (skills, escolaridade, experiencia) com perfil do candidato para sugerir matches e priorizar triagem.

**MF-04** — **Comunicacao por WhatsApp/Email**: Envio automatico de mensagens para candidatos (confirmacao de recebimento de curriculo, convite para entrevista, resultado do processo).

**MF-05** — **Video-entrevista**: Integracao com plataforma de video (Google Meet, Teams) para entrevistas remotas, com link automatico no agendamento.

**MF-06** — **Testes online**: Modulo de testes tecnicos/comportamentais online que os candidatos podem realizar antes da entrevista presencial, com nota automatica.

**MF-07** — **Analytics avancado**: Funil de conversao com taxa por etapa, comparativo mensal, analise de gargalos, previsao de tempo de preenchimento baseado em historico.

**MF-08** — **Assinatura digital**: Assinatura digital na ficha de avaliacao, carta proposta e contrato, eliminando papel.

**MF-09** — **Integracao com e-Social**: Geracao automatica do evento S-2200 (admissao) e S-2206 (alteracao contratual) a partir dos dados do candidato aprovado.

**MF-10** — **Indicacao com gamificacao**: Programa de indicacao de funcionarios com rastreamento (quem indicou quem), bonificacao automatica apos periodo de experiencia, e ranking de indicadores.

**MF-11** — **Onboarding digital**: Ao aprovar candidato, iniciar automaticamente checklist de onboarding: documentos, treinamentos obrigatorios, entrega de EPI, ASO admissional, ordem de servico.

**MF-12** — **Compliance trabalhista**: Validacao automatica de requisitos legais para contratacao (ex: idade minima para jovem aprendiz, limite de carga horaria para estagiario, cota PCD).

**MF-13** — **Diversidade e inclusao**: Dashboard de diversidade no recrutamento (genero, PCD, etnia), com metas e acompanhamento.

**MF-14** — **Banco de talentos com IA**: Utilizacao de IA para classificar candidatos do banco de talentos por fit com novas vagas, baseado em historico de avaliacoes e contratacoes anteriores.

**MF-15** — **Requisicao de vaga com orcamento**: Vincular abertura de vaga com orcamento do setor (headcount planejado vs. realizado), exigindo aprovacao financeira para vagas nao previstas.

**MF-16** — **Mobile-first para gestores**: App ou PWA para gestores avaliarem candidatos, registrarem parecer e aprovarem/reprovarem direto do celular durante a entrevista.

**MF-17** — **Timeline do candidato**: Visualizacao cronologica de todas as interacoes com o candidato (cadastro, vinculacoes, entrevistas, avaliacoes, movimentacoes, banco de talentos).

**MF-18** — **Exportacao para BI**: Disponibilizar dados de recrutamento em formato consumivel por ferramentas de BI (Power BI, Metabase) para analises corporativas.

---

*Documento gerado em 2026-07-27 para rebuild do modulo F-11 no SIGA ERP.*
*Fonte: analise exaustiva do codigo legado Classic RH (recrutamento_selecao.html, schema_recrutamento_selecao.sql, rh_api.js, shell.js, FUNCOES_APLICACAO.sql, TABELAS_ETL.sql, migration_recrutamento_selecao.sql).*
