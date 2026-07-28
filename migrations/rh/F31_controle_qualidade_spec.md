# F-31 — Controle de Qualidade (CQ)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-31 |
| Titulo | Controle de Qualidade de Materia-Prima |
| Modulo | CQ (Controle de Qualidade) |
| Sistema | Classic IA — Classic Importacao e Exportacao de Couros EIRELI |
| CNPJ | 08.849.964/0001-10 |
| Sede | Jardim Alegre - PR |
| Versao | 1.0 |
| Data | 28/07/2026 |
| Autor | Equipe Classic IA |
| Supabase CQ | nvqxsulntpftcwtkjedu.supabase.co |
| Dominio | app.classiccouros.com.br |

---

## 1. Identificacao

**F-31 — Controle de Qualidade** e o modulo responsavel por todo o ciclo de vida da inspecao de materia-prima recebida pela Classic Couros, abrangendo desde o recebimento fisico de lotes de subprodutos bovinos (omaso/bucho, aorta, tendao, rumen, vergalho) ate a decisao final de aceite, geracao de nao-conformidades ao fornecedor e calculo do score de confiabilidade (IQF).

O modulo compreende **8 telas principais**, **28 arquivos SQL** de migracao, **5 bibliotecas JS compartilhadas**, e opera sobre o projeto Supabase CQ (`nvqxsulntpftcwtkjedu.supabase.co`).

**Telas do modulo:**

| # | Arquivo | Funcao |
|---|---------|--------|
| 1 | `inspecao_qualidade.html` | Formulario de inspecao (2913 linhas) |
| 2 | `fila_inspecoes.html` | Fila/lista de inspecoes |
| 3 | `formulario_cq.html` | Formulario de entrada de dados CQ (legado) |
| 4 | `dashboard_cq.html` | Dashboard analitico CQ |
| 5 | `rastreabilidade_lote.html` | Rastreabilidade do lote |
| 6 | `recebimento_lote.html` | Registro de recebimento |
| 7 | `gestao_fornecedores.html` | Gestao de fornecedores |
| 8 | `dashboard_supplier_score.html` | Score de confiabilidade |

**Bibliotecas compartilhadas:**

| Arquivo | Funcao |
|---------|--------|
| `cq_regras.js` | Motor de regras de negocio (classificacao, decisao, score, IQF) |
| `cq_workflow.js` | Maquina de estados do workflow (8 estados, transicoes, RBAC) |
| `cq_auth.js` | Autenticacao, sessao, geolocation, audit trail |
| `cq_api.js` | Camada de API (fetch Supabase com JWT) |
| `cq_fornecedores.js` | Componente de busca/selecao de fornecedores (381 cadastros embutidos) |

---

## 2. Problema

A Classic Couros e uma industria de importacao e exportacao de couros e subprodutos bovinos (especialmente omaso/bucho) que recebe cargas de **381+ frigorificos/fornecedores** de todo o Brasil. Cada carga contem lotes de pecas que precisam ser inspecionadas no chao de fabrica para:

1. **Garantir qualidade da materia-prima** — Identificar e quantificar defeitos (fermentacao, sem folhas, folhas arrebentadas, contaminacao etc.) antes do processamento.
2. **Classificar lotes (A/B/C)** — Determinar se o lote e aprovado (A: 0-5% defeitos), aprovado com ressalva (B: 5-15%) ou bloqueado (C: >15%).
3. **Decidir aceite/rejeicao** — Decisao automatica via motor de decisao baseado em severidade (CRITICO/MAIOR/MENOR).
4. **Rastrear historico** — Manter rastreabilidade completa do lote desde o recebimento ate o destino final.
5. **Avaliar fornecedores** — Calcular IQF (Indice de Qualidade do Fornecedor) e score de confiabilidade para decisoes comerciais.
6. **Gerar nao-conformidades** — Emitir RNC (Registro de Nao Conformidade) ao fornecedor com prazo de resposta.
7. **Operar offline** — Permitir inspecoes no chao de fabrica sem conexao, com sincronizacao posterior.

**Desafios anteriores ao sistema:**
- Inspecoes em papel, sem padronizacao
- Impossibilidade de cruzar dados historicos de fornecedores
- Demora na comunicacao de problemas de qualidade
- Ausencia de evidencias fotograficas/video
- Sem controle de temperatura no recebimento
- Decisoes de aceite/rejeicao sem criterios objetivos

---

## 3. Processo Operacional

O fluxo completo do Controle de Qualidade segue 7 etapas sequenciais:

### 3.1 Recebimento (`recebimento_lote.html`)

1. Caminhao chega a planta com carga de materia-prima
2. Operador registra o recebimento:
   - Seleciona fornecedor/frigorifico (componente `CQFornecedores` com busca em 381 cadastros + 192 aliases de normalizacao)
   - Seleciona produto (Omaso Salgado/Fresco/Congelado, Aorta, Tendao, Rumen, Vergalho, Outro)
   - Informa lote ATAK, numero da etiqueta (com OCR via Tesseract ou scanner de codigo de barras)
   - Registra quantidade de pecas, peso total (kg)
   - Mede temperatura de chegada (oC) com validacao automatica por tipo de produto
   - Identifica transporte (placa, motorista, transportadora)
   - Fotografa etiqueta e carga (upload para Supabase Storage `cq-fotos/`)
3. Sistema gera ID unico `REC-XXX` e salva em `cq_recebimentos_v2`
4. Status do recebimento: `recebido` -> `em_inspecao` -> `inspecionado` -> `cancelado`

### 3.2 Inspecao (`inspecao_qualidade.html`)

1. Inspetor abre o formulario de inspecao (mobile-first, offline-capable)
2. Pode vincular a um recebimento existente via busca de PCR (Pedido de Compra/Recebimento)
3. Preenche campos obrigatorios:
   - Data, hora, inspetor (auto-preenchido via CQAuth — readonly)
   - Fornecedor, produto, lote ATAK
   - Quantidade analisada (amostra)
   - Tipo de amostragem (aleatoria simples, 100% do lote, sistematica, por palete, conveniente)
4. Registra temperatura de recebimento com alerta automatico por limites
5. Identifica transporte (placa, motorista) com autocomplete de dados historicos

### 3.3 Classificacao de Defeitos

1. Para cada um dos **11 tipos de defeito**, inspetor informa a quantidade de pecas afetadas usando stepper (+/-) ou input numerico:

| # | Codigo | Label | Categoria | Criticidade |
|---|--------|-------|-----------|-------------|
| 1 | `fermentacao` | Fermentacao | Desconto financeiro | alta |
| 2 | `sem_folhas` | Sem Folhas | Desconto financeiro | media |
| 3 | `folhas_arrebentadas` | Folhas Arrebentadas | Desconto financeiro | media |
| 4 | `pedaco` | Pedaco | Desconto financeiro | alta |
| 5 | `sujo` | Sujo / Despojo | Acao corretiva | media |
| 6 | `rasgado` | Rasgado | Acao corretiva | alta |
| 7 | `corte_irregular` | Corte Irregular | Acao corretiva | media |
| 8 | `furado` | Furado | Acao corretiva | alta |
| 9 | `mau_cheiro` | Mau Cheiro | Acao corretiva | critica |
| 10 | `contaminacao` | Contaminacao | Acao corretiva | critica |
| 11 | `outro_defeito` | Outro Defeito | Acao corretiva | media |

2. Sistema calcula em tempo real:
   - Total de defeitos (soma de todas as categorias)
   - Percentual de defeitos (total / quantidade analisada * 100)
   - Percentual por tipo individual
   - Subtotal por categoria (desconto financeiro vs. acao corretiva)

### 3.4 Classificacao ABC e Decisao

O **motor de decisao** opera em dois niveis:

**Nivel 1 — Classificacao ABC (por percentual total):**

| Classe | Faixa | Status |
|--------|-------|--------|
| A | 0 - 5% | Aprovado |
| B | 5 - 15% | Aprovado com Ressalva |
| C | > 15% | Bloqueado |

**Nivel 2 — Motor de Decisao por Severidade (`fn_motor_decisao_inspecao`):**

Os defeitos sao mapeados para severidades via `cq_tipos_defeito.severidade`:
- `CRITICO` — defeitos criticos (ex: contaminacao, mau cheiro)
- `MAIOR` — defeitos maiores (ex: fermentacao, pedaco, rasgado, furado)
- `MENOR` — defeitos menores (ex: sem folhas, sujo, corte irregular)

Regras de decisao (avaliadas em ordem):

| Regra | Condicao | Decisao | Score |
|-------|----------|---------|-------|
| R1 | % criticos >= 5% | BLOQUEADO | 0 |
| R2 | % criticos > 0% e < 5% | EM_ANALISE_CRITICA (requer reamostragem) | 100 - (crit*25) - (maior*8) - (menor*2) |
| R3 | % maiores > 10% | BLOQUEADO | 100 - (maior*8) - (menor*2) |
| R4 | % maiores > 5% | RESSALVA | MIN(79, 100 - (maior*8) - (menor*2)) |
| R5 | % total > 10% | RESSALVA | MIN(79, 100 - (maior*8) - (menor*2)) |
| R6 | Caso contrario | APROVADO | 100 - (maior*8) - (menor*2) |

**Alerta critico:** Lotes com % defeito > 30% recebem alerta visual: "LOTE CRITICO — Percentual acima de 30%! Requer negociacao ou rejeicao imediata."

### 3.5 Score do Fornecedor (IQF)

O **IQF (Indice de Qualidade do Fornecedor)** e calculado por multiplas formulas conforme o contexto:

**Formula principal (cq_fornecedor_score):**
```
Score = MAX(0, 100 - (pctC * 2) - (divergencias_abertas * 5))
```

**Formula do score CQ por lote (CQRegras.scoreLoteLocal):**
```
Score = MAX(0, MIN(100, 100 - (pctC * 0.50) - (pctB * 0.20) + (pctA * 0.20)))
```

**Formula de ranking (vw_ranking_fornecedor_produto):**
```
Score = MAX(0, MIN(100, 100 - (pctC * 2) - (pct_defeitos)))
```

**Formula 4 dimensoes (vw_qualidade_fornecedor):**
```
Score = (pecas_A * 100 + pecas_B * 60) / total_pecas
```

**Classificacao de confiabilidade (dashboard_supplier_score):**
- `>= 80` — Confiavel
- `60-79` — Regular
- `40-59` — Atencao
- `< 40` — Critico

**Status do fornecedor (cq_fornecedor_score):**
- `bloqueado` — pctC > limite * 1.5 OU score < 30
- `critico` — pctC > limite (default 15%)
- `atencao` — pctC > limite * 0.7 OU score < 60
- `normal` — demais

### 3.6 Nao-Conformidade (NC/RNC)

Quando o lote e bloqueado ou apresenta defeitos sistematicos, o sistema permite:
1. Registrar NC em `cq_nao_conformidades` com codigo `NC-YYYYMMDD-NNN` (sequencial por dia)
2. Emitir RNC ao fornecedor (tipo `fornecedor`) com:
   - Descricao do problema
   - Causa raiz
   - Acao corretiva/preventiva
   - Prazo de resposta
   - Evidencia (URL da foto/video)
3. Acompanhar status: `aberta` -> `em_tratamento` -> `concluida` / `cancelada`
4. Registrar resposta do fornecedor e validar eficacia (`eficaz`, `ineficaz`, `pendente`)

### 3.7 Plano de Acao (`gestao_fornecedores.html`)

1. Criar tratativa/cobranca vinculada a fornecedor e lote
2. Campos: lote referencia, data, problema identificado, recomendacao tecnica
3. Status: `aberto` -> `comunicado` -> `em_tratativa` -> `resolvido`
4. Historico completo de tratativas por fornecedor

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Descricao | Permissoes Principais |
|--------|-----------|----------------------|
| `administrador` | Administrador do sistema CQ | Acesso total: CRUD em todas as telas, overrides, configuracoes, cancelar/reabrir |
| `qualidade` | Inspetor/analista de qualidade | Criar/editar inspecoes, ver dashboards, registrar NC, revisar (se nao criador) |
| `diretoria` | Diretoria/gerencia | Visualizar dashboards, aprovar revisoes, ver scores |
| `financeiro` | Setor financeiro | Visualizar dashboards, ver custos, exportar CSV |
| `comercial` | Setor comercial | Visualizar fornecedores, rastreabilidade, scores |
| `logistica` | Setor de logistica | Registrar recebimentos, ver fila de inspecoes |
| `industrial` | Setor industrial/producao | Visualizar rastreabilidade |
| `gestor` | Gestor operacional | Gerenciar divergencias, configurar notificacoes |

### 4.2 Inspetores Cadastrados

Lista fixa no frontend: Janaina, Cristiano, Nelson, Leandro, Cidinha, Adriano, Valdecir, Edson, Outro.

### 4.3 Responsaveis por Decisao

Lista fixa: Janaina, Cristiano, Gerencia, Outro.

### 4.4 Permissoes por Tela

| Tela | Perfis Permitidos |
|------|-------------------|
| `inspecao_qualidade.html` | administrador, qualidade |
| `fila_inspecoes.html` | administrador, qualidade, diretoria, financeiro |
| `recebimento_lote.html` | administrador, qualidade, logistica |
| `dashboard_cq.html` | administrador, qualidade, diretoria, financeiro |
| `rastreabilidade_lote.html` | administrador, qualidade, comercial, financeiro, logistica, industrial, diretoria |
| `gestao_fornecedores.html` | administrador, qualidade, comercial, financeiro, diretoria |
| `dashboard_supplier_score.html` | administrador, qualidade, diretoria, financeiro |
| `formulario_cq.html` | administrador, qualidade |

### 4.5 Regras RBAC de Workflow (CQWorkflow)

- **Rascunho:** criador ou admin pode editar e enviar para analise
- **Em analise:** criador ou admin pode editar e enviar para revisao
- **Aguardando revisao:** qualidade/admin/diretoria pode revisar (exceto criador)
- **Revisada:** admin/diretoria pode aprovar (exceto criador)
- **Reprovada:** qualidade/admin pode abrir NC; admin pode reabrir
- **Aprovada:** admin pode encerrar
- **Cancelar:** admin pode cancelar qualquer status (exceto encerrada/cancelada)

---

## 5. Fluxograma

```
RECEBIMENTO                 INSPECAO                    DECISAO
============               ==========                  ========

Caminhao chega              Inspetor abre form          Motor calcula
     |                           |                         |
     v                           v                         v
Registrar Recebimento  -->  Vincular PCR/Lote  -->  Classificar ABC
 (fornecedor, produto,      (lote ATAK,              (A: 0-5%,
  qtd, temperatura,          quantidade,               B: 5-15%,
  placa, fotos)              amostragem)               C: >15%)
     |                           |                         |
     v                           v                         v
Status: recebido            Registrar Defeitos      Motor Severidade
     |                      (11 tipos, qtd cada)    (CRITICO/MAIOR/MENOR)
     v                           |                         |
Salvar cq_recebimentos_v2       v                         v
     |                      Calcular %              Decisao Automatica
     v                      (real-time preview)     (APROVADO/RESSALVA/
Ir p/ Inspecao                   |                   BLOQUEADO/ANALISE)
                                 v                         |
                            Evidencias                     v
                            (3 fotos + video        IQF Fornecedor
                             + observacoes)         (score 0-100)
                                 |                         |
                                 v                         v
                            Salvar (online/offline)  NC se bloqueado
                                 |                   (RNC ao fornecedor)
                                 v                         |
                            Backend Valida                 v
                            (cq_submeter_inspecao)   Plano de Acao
                                 |                   (tratativa/cobranca)
                                 v
                            Workflow (8 estados):
                            rascunho -> em_analise ->
                            aguardando_revisao -> revisada ->
                            aprovada -> encerrada
                            (reprovada / cancelada)

FILA DE INSPECOES                    DASHBOARDS
=================                    ==========
Lista paginada (20/pg)               KPIs agregados (8)
Filtros: status, fornecedor,         10 tabs de analise
  produto, lote, etiqueta,           Graficos Chart.js
  periodo                            Exportacao CSV
Contadores por status (7)            Alertas Classe C
Selecao multipla + WhatsApp/Email    Tendencia semanal (12 sem)

RASTREABILIDADE                      SCORE FORNECEDOR
===============                      ================
Busca por numero de lote             vw_supplier_score
Timeline de movimentacoes            Ranking de confiabilidade
9 etapas (ROR->RCL->CQ)             4 classificacoes: Confiavel/
Historico do fornecedor                Regular/Atencao/Critico
Relatorio PDF tecnico/fornecedor     Top 10 piores
```

---

## 6. Regras de Negocio

### RN-01 — Classificacao ABC por Percentual de Defeitos

```
SE percentual_defeitos <= 5%    ENTAO classificacao = 'A' (Aprovado)
SE percentual_defeitos <= 15%   ENTAO classificacao = 'B' (Aprovado com Ressalva)
SE percentual_defeitos > 15%    ENTAO classificacao = 'C' (Bloqueado)
```

Limites configuraveis por produto em `cq_parametros_produto` (default: alerta=5%, reprovacao=15%).

### RN-02 — 11 Tipos de Defeito

O sistema reconhece exatamente 11 tipos de defeito, divididos em duas categorias:
- **Desconto financeiro (4):** fermentacao, sem_folhas, folhas_arrebentadas, pedaco
- **Acao corretiva (7):** sujo, rasgado, corte_irregular, furado, mau_cheiro, contaminacao, outro_defeito

A classificacao ABC no relatorio PDF e baseada no percentual de defeitos de **desconto financeiro** (nao no total geral). O motor de decisao backend usa o total com severidade.

### RN-03 — Limites de Temperatura por Tipo de Produto

| Tipo de Produto | Limite Maximo | Acao se Excedido |
|-----------------|---------------|------------------|
| Salgado | <= 4oC | Alerta visual (vermelho): "Acima do limite (4oC)" |
| Congelado | <= -12oC | Alerta visual (vermelho): "Acima do limite (-12oC)" |
| Fresco | <= 2oC | Alerta visual (vermelho): "Acima do limite (2oC)" |

Implementado em `checkTemperatura()`: exibe badge verde "Dentro do limite" ou vermelho "Acima do limite".

### RN-04 — Calculo de Percentual de Defeitos

```
percentual_defeitos = (total_defeitos / quantidade_analisada) * 100
```

- Se `quantidade_analisada = 0`: nao calcular, exibir "Aguardando dados"
- Se `total_defeitos = 0` e `quantidade_analisada > 0`: exibir "0,0% — Sem defeitos"
- Arredondamento: 4 casas no backend (`numeric(10,4)`), 1-2 casas no frontend

### RN-05 — Campos Obrigatorios para Salvar Inspecao

| Campo | ID | Obrigatorio |
|-------|----|-------------|
| Data | `f-data` | Sim |
| Hora | `f-hora` | Sim |
| Inspetor | `f-inspetor` | Sim (readonly, auto CQAuth) |
| Fornecedor | `f-fornecedor` | Sim |
| Produto | `f-produto` | Sim |
| Lote ATAK | `f-lote-atak` | Sim |
| Qtd Analisada | `f-qtd-analisada` | Sim |

Barra inferior exibe: "Falta: [lista dos campos vazios]" ou "Pronto para salvar" (verde).

### RN-06 — Motor de Decisao por Severidade

Defeitos sao classificados por severidade via tabela `cq_tipos_defeito`. O trigger `fn_motor_decisao_inspecao` aplica as regras R1-R6 (secao 3.4) e grava:
- `pct_defeitos_criticos`, `pct_defeitos_maiores`, `pct_defeitos_menores`
- `decisao_motor` (APROVADO/RESSALVA/BLOQUEADO/EM_ANALISE_CRITICA)
- `score_lote` (0-100)
- `requer_reamostragem` (boolean)

### RN-07 — Alerta Critico (>30%)

Lotes com percentual de defeitos acima de 30% recebem alerta critico: "LOTE CRITICO — Requer negociacao ou rejeicao imediata." Regra: `pct > 30` -> `cls = 'reprovado'`, `label = 'Reprovado — Negociar'`.

### RN-08 — Classificacao A+B+C Compulsoria

A soma `qtd_a + qtd_b + qtd_c` DEVE ser igual a `quantidade_recebida`. Enforced pelo trigger `fn_validar_classificacao_recebimento()` na tabela `cq_classificacoes_recebimento`.

### RN-09 — Unicidade de Lote

Cada combinacao (numero_lote, fornecedor_id, produto_id) e unica. Enforced pela constraint `uq_cq_lote_negocio` e pelo hash de integridade `fn_hash_lote()` (MD5 da concatenacao).

### RN-10 — Score CQ do Fornecedor

Varias formulas conforme contexto (ver secao 3.5). Formula principal do `cq_fornecedor_score`:
```
Score = MAX(0, 100 - (pctC * 2) - (divergencias_abertas * 5))
```

### RN-11 — IQF (Indice de Qualidade do Fornecedor)

Calculado por `fn_calcular_iqf_fornecedor()`: media ponderada dos `score_lote` dos ultimos 90 dias (janela configuravel), com peso decrescente por recencia. Chamado via RPC `cq_calcular_iqf`.

### RN-12 — Codigo RNC Sequencial

Formato: `RNC-YYYYMMDD-NNN` onde NNN e sequencial por dia. Gerado por `fn_proximo_codigo_rnc()`.

### RN-13 — Override com Justificativa Minima

Overrides de classificacao (cq_overrides_v2) exigem justificativa com no minimo 10 caracteres (`ck_cq_overrides_v2_justificativa_min`). Campo restrito a `status_final` ou `classificacao`.

### RN-14 — Quantidade Analisada <= Quantidade Recebida

Enforced pela RPC `rpc_criar_inspecao_completa`: `p_quantidade_analisada <= p_quantidade_recebida`.

### RN-15 — Total de Defeitos <= Quantidade Analisada

Constraint na tabela `cq_inspecoes`: `ck_cq_inspecoes_total_defeitos_lte_qtd`.

### RN-16 — Classificacao pelo Backend

O frontend exibe um **preview local** da classificacao (via `CQRegras.motorDecisaoLocal()`), mas a decisao REAL e feita exclusivamente pelo backend via `CQRegras.submeterInspecao()` -> RPC `cq_submeter_inspecao()`. O frontend NAO decide classificacao, status_final ou workflow.

### RN-17 — Reamostragem

Quando o motor de decisao detecta defeitos criticos (> 0% e < 5%), a inspecao e marcada como `EM_ANALISE_CRITICA` com `requer_reamostragem = true`. O fluxo de reamostragem e controlado pela tabela `cq_reamostragem`, vinculando inspecao original a reinspecao.

### RN-18 — Anti-Duplicidade

Antes de salvar, o frontend verifica via n8n webhook (`/webhook/atak-cq/check-duplicidade`) se o lote/etiqueta ja foi cadastrado. Se retornar HTTP 409, exibe "Duplicidade detectada".

### RN-19 — Alertas de Qualidade Automaticos

Alertas sao gerados automaticamente em `cq_alertas_qualidade` quando fornecedores atingem %C acima do limiar configurado. Gravidade: `critica`, `atencao`, `informativa`. Tipos de alerta (vw_ranking_alertas): defeito > 5%, defeito recorrente (>=3 lotes), inconsistencia de dados, fornecedor piorando.

### RN-20 — Workflow de 8 Estados

Status workflow: `rascunho` -> `em_analise` -> `aguardando_revisao` -> `revisada` -> `aprovada` -> `encerrada` | `reprovada` | `cancelada`

Transicoes permitidas:
- `rascunho` -> `em_analise`
- `em_analise` -> `aguardando_revisao`
- `aguardando_revisao` -> `revisada`, `reprovada`, `rascunho` (devolucao)
- `revisada` -> `aprovada`
- `reprovada` -> `rascunho` (reabertura, so admin)
- `aprovada` -> `encerrada`
- `encerrada` -> (terminal)
- `cancelada` -> (terminal)

### RN-21 — Fermentacao >= 5% = Classe C

Regra especial no formulario legado (`formulario_cq.html`): se o percentual de fermentacao >= 5%, a classificacao e automaticamente C, independente do percentual total.

### RN-22 — Sessao Unica e Geolocation

- Sessao demo rejeitada (token com prefixo `demo-` bloqueado)
- JWT obrigatorio — sessao legada sem JWT gera warning
- Sessao unica por usuario — ao abrir nova, invalida as anteriores
- Timeout de inatividade: 1 hora (aviso 5min antes)
- Heartbeat a cada 1 minuto
- Geolocation: aviso se fora da planta (>500m de -24.1820, -51.6920)

### RN-23 — Scan com Alteracao Auditada

Quando o inspetor altera dados que foram preenchidos automaticamente por scan de etiqueta, deve informar justificativa obrigatoria (registrada em `cq_scan_alteracao_log`).

---

## 7. Campos

### 7.1 Formulario de Inspecao (`inspecao_qualidade.html`)

**Secao 01 — Identificacao:**

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Data | `f-data` | date | Sim | Data da inspecao (auto-preenchido com hoje) |
| Hora | `f-hora` | time | Sim | Hora da inspecao (auto-preenchido com agora) |
| Inspetor | `f-inspetor` | text (readonly) | Sim | Nome do inspetor logado (via CQAuth) |
| Fornecedor | `f-fornecedor` | hidden | Sim | Codigo do fornecedor (via componente CQFornecedores) |
| Fornecedor Nome | `f-fornecedor-nome` | hidden | — | Nome do fornecedor |
| Fornecedor UF | `f-fornecedor-uf` | hidden | — | Estado do fornecedor |
| Fornecedor Cidade | `f-fornecedor-cidade` | hidden | — | Cidade do fornecedor |
| Produto | `f-produto` | select | Sim | Omaso Salgado/Fresco/Congelado, Aorta, Tendao, Rumen, Vergalho, Outro |
| Lote ATAK | `f-lote-atak` | text (scannable) | Sim | Numero do lote (uppercase, scanner e OCR compativeis) |
| N. Etiqueta | `f-etiqueta` | text (scannable) | Nao | Numero da etiqueta |
| Temperatura | `f-temperatura` | number (step 0.1) | Nao | Temperatura de recebimento em oC |
| Placa Caminhao | `f-placa` | text (uppercase) | Nao | Placa do veiculo (autocomplete historico) |
| Motorista | `f-motorista` | text | Nao | Nome do motorista (autocomplete historico) |

**Secao 02 — Amostragem:**

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Qtd do Lote (pcs) | `f-qtd-lote` | number | Nao | Quantidade total de pecas no lote |
| Qtd Analisada | `f-qtd-analisada` | number | Sim | Quantidade de pecas inspecionadas |
| Tipo Amostragem | `f-tipo-amostragem` | select | Nao | Aleatoria simples, 100% do lote, Sistematica, Por palete, Conveniente |

**Secao 03 — Defeitos (11 campos):**

| Campo | ID | Tipo | Default |
|-------|----|------|---------|
| Fermentacao | `df-fermentacao` | number (stepper +/-) | 0 |
| Sem Folhas | `df-sem_folhas` | number (stepper +/-) | 0 |
| Folhas Arrebentadas | `df-folhas_arrebentadas` | number (stepper +/-) | 0 |
| Pedaco | `df-pedaco` | number (stepper +/-) | 0 |
| Sujo | `df-sujo` | number (stepper +/-) | 0 |
| Rasgado | `df-rasgado` | number (stepper +/-) | 0 |
| Corte Irregular | `df-corte_irregular` | number (stepper +/-) | 0 |
| Furado | `df-furado` | number (stepper +/-) | 0 |
| Mau Cheiro | `df-mau_cheiro` | number (stepper +/-) | 0 |
| Contaminacao | `df-contaminacao` | number (stepper +/-) | 0 |
| Outro Defeito | `df-outro_defeito` | number (stepper +/-) | 0 |
| Tipo Outro Defeito | `f-outro-defeito-tipo` | text | — (visivel quando outro_defeito > 0) |

**Secao 04 — Resultado Automatico (calculado):**

| Campo | ID | Tipo | Descricao |
|-------|----|------|-----------|
| % Defeito | `resultPct` | display | Calculado automaticamente |
| Label | `resultLabel` | display | "Aprovado", "Aprovado com Ressalva", "Bloqueado" |
| Total Defeitos | `rTotalDef` | display | Soma de todos os defeitos |
| Pecas Analisadas | `rAnalisadas` | display | Espelho do campo qtd analisada |
| % Calculado | `rPct` | display | Percentual formatado |
| Barra Progresso | `progressFill` | display | Visual 0-100% (cap em 30%) |
| Alerta Critico | `criticoAlert` | display | Visivel se % > 30% |

**Secao 05 — Evidencias:**

| Campo | ID | Tipo | Descricao |
|-------|----|------|-----------|
| Observacoes | `f-obs` | textarea | Condicoes gerais do lote |
| Foto Etiqueta | `fotoEtiqueta`/`fotoCamEtiqueta` | file (image) | Foto via galeria ou camera |
| Foto Produto | `fotoProduto`/`fotoCamProduto` | file (image) | Foto do produto |
| Foto Defeito | `fotoDefeito`/`fotoCamDefeito` | file (image) | Foto do defeito encontrado |
| Video Evidencia | `videoInputFile`/`videoInputCam` | file (video) | Video de evidencia (max 50MB) |
| Obs Reprovado | `f-obs-reprovado` | textarea | Obrigatorio quando status = bloqueado |

**Campos de Auditoria (hidden, auto-preenchidos por CQAuth):**

| Campo | ID | Preenchimento |
|-------|----|--------------|
| Usuario ID | `f-usuario-id` | `CQAuth.dadosRegistro().usuario_id` |
| Usuario Nome | `f-usuario-nome` | `CQAuth.dadosRegistro().usuario_nome` |
| Latitude | `f-geo-lat` | Geolocation API |
| Longitude | `f-geo-lng` | Geolocation API |
| Dentro da Planta | `f-geo-dentro` | Geofence check (raio 500m) |

### 7.2 Formulario de Recebimento (`recebimento_lote.html`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Fornecedor | `f-forn-cod`/`f-forn-nome` | hidden (componente) | Sim |
| Produto | `f-produto` | select | Sim |
| Lote ATAK | `f-lote` | text (uppercase, OCR) | Sim |
| N. Etiqueta | `f-etiqueta` | text (OCR) | Sim |
| Quantidade Pecas | `f-qtd` | number | Sim |
| Peso Total (kg) | `f-peso` | number (step 0.1) | Nao |
| Temperatura (oC) | `f-temperatura` | number (step 0.1) | Nao |
| Placa Caminhao | `f-placa` | text (uppercase, autocomplete) | Nao |
| Motorista | `f-motorista` | text (autocomplete) | Nao |
| Transportadora | `f-transportadora` | text | Nao |
| Foto Etiqueta | file (camera) | image | Nao |
| Foto Carga | file (camera) | image | Nao |
| Observacoes | `f-obs` | textarea | Nao |

### 7.3 Formulario CQ Legado (`formulario_cq.html`)

| Campo | ID | Tipo | Descricao |
|-------|----|------|-----------|
| Lote Atak | `loteAtak` | text | Numero do lote |
| No Requisicao | `nRequisicao` | text | Numero da requisicao |
| Fornecedor | `fornecedor` | text | Nome do fornecedor (obrigatorio) |
| Data Entrada | `dataEntrada` | date | Default: hoje |
| Qtd NF Pecas | `qtdNF` | number | Quantidade conforme NF |
| Qtd Total Lote | `qtdTotalLote` | number | Quantidade total |
| Qtd Amostra | `qtdAmostra` | number | Quantidade analisada (obrigatorio) |
| Lancamento Parcial | `toggleParcial` | toggle | Se e lancamento parcial |
| Sequencia Parcial | `seqParcial` | number | Numero da sequencia |
| Defeito 1-3 Tipo | `def1tipo`-`def3tipo` | select | Tipo de defeito |
| Defeito 1-3 Qtd | `def1`-`def3` | number | Quantidade |
| Observacoes | textarea | text | Observacoes gerais |
| Liberar Producao | toggle | boolean | Gera lote base BCH-DDMMYYYY |

---

## 8. Tabelas

### 8.1 Tabelas do Modelo Normalizado (v2) — 01_migration_novo_modelo_cq.sql

| Tabela | Descricao | RLS |
|--------|-----------|-----|
| `cq_fornecedores` | Cadastro de fornecedores (381+). Campos: id, codigo_atak, nome, nome_normalizado, documento, ativo, cidade, estado, email, contato, telefone, observacoes | Sim (endurecido) |
| `cq_produtos_v2` | Catalogo de produtos (31). Campos: id, codigo_atak, codigo_interno, nome, nome_normalizado, categoria, unidade_medida, ativo | Sim (endurecido) |
| `cq_defeitos` | Tipos de defeito (11). Campos: id, codigo, nome, nome_normalizado, descricao, tipo, criticidade_padrao (baixa/media/alta/critica), ativo, ordem | Sim (endurecido) |
| `cq_parametros_produto` | Limites por produto. Campos: id, produto_id (FK), perc_alerta, perc_reprovacao, exige_abc, ativo | Sim |
| `cq_lotes` | Entidade central do lote. Campos: id, numero_lote, fornecedor_id (FK), produto_id (FK), data_lote, origem (manual/atak/planilha/api), hash_integridade (UNIQUE) | Sim (endurecido) |
| `cq_recebimentos_v2` | Recebimento fisico. Campos: id, lote_id (FK), data_recebimento, quantidade_recebida, peso_total, nota_fiscal, romaneio, temperatura, placa_caminhao, motorista, transportadora, status_recebimento, fotos | Sim (endurecido) |
| `cq_inspecoes` | Inspecao do lote (1:1 com recebimento). Campos: id, recebimento_id (FK), quantidade_analisada, total_defeitos, perc_defeitos, classificacao_abc (A/B/C), status_final, score_fornecedor, override_ativo, fotos, video | Sim (endurecido) |
| `cq_inspecao_defeitos` | Defeitos por inspecao (N:1). Campos: id, inspecao_id (FK), defeito_id (FK), quantidade, percentual, criticidade | Sim (endurecido) |
| `cq_classificacoes_recebimento` | A+B+C por recebimento. Campos: id, recebimento_id (FK UNIQUE), qtd_a, qtd_b, qtd_c | Sim (endurecido) |
| `cq_overrides_v2` | Override de classificacao. Campos: id, inspecao_id (FK), classificacao_original, classificacao_nova, status_original, status_novo, justificativa (min 10 chars), realizado_por | Sim (endurecido) |
| `cq_audit_trail_v2` | Audit trail. Campos: id, tabela, registro_id, operacao (INSERT/UPDATE/DELETE), antes (jsonb), depois (jsonb), realizado_por, origem | Sim |
| `cq_nao_conformidades` | Registro de NC. Campos: id, codigo (UNIQUE), tipo, gravidade, status, inspecao_id, lote_id, fornecedor_codigo, descricao, causa_raiz, acoes, prazos, validacao, resposta_fornecedor | Sim (endurecido) |

### 8.2 Tabelas da Integracao ATAK

| Tabela | Descricao |
|--------|-----------|
| `atak_sync_config` | Configuracao de 5 endpoints ATAK (fornecedores, cargas, producao, embarques, expedicao) |
| `atak_sync_log` | Log de execucoes de sincronizacao |
| `atak_cargas_raw` | Cargas brutas importadas do ATAK com classificacao A/B/C e hash dedup |
| `atak_embarques_raw` | Embarques brutos |
| `atak_producao_raw` | Producao bruta (setor, turno, qtd) |
| `atak_expedicao_raw` | Expedicao bruta |
| `registros_cq_inspecao` | Tabela principal de inspecoes (frontend direto, com motor de decisao) |
| `cq_lote_inspecao` | Header do lote com totais e classificacao (formato LOT-YYYY-MM-DD-NNN) |
| `cq_lote_produto` | Detalhe por produto no lote com 9 defeitos e classificacao |
| `cq_tipos_defeito` | Tipos de defeito com severidade (CRITICO/MAIOR/MENOR) e gravidade |
| `cq_ranking_produtos` | Catalogo de 31 produtos para ranking |
| `cq_ranking_defeitos` | Catalogo de 9 defeitos para ranking |
| `cq_reamostragem` | Vinculo inspecao original <-> reinspecao |
| `cq_validacao_divergencias` | Divergencias detectadas (8 tipos) |
| `cq_fornecedor_score` | Score continuo do fornecedor (0-100) |
| `cq_fornecedor_score_historico` | Historico temporal de scores |
| `cq_alertas_qualidade` | Alertas automaticos de qualidade |
| `cq_usuarios` | Usuarios do sistema CQ com perfil e auth_uid |
| `cq_scan_log` | Log de leituras de scanner/OCR |
| `cq_scan_alteracao_log` | Log de alteracoes pos-scan com justificativa |

### 8.3 Tabelas Complementares

| Tabela | Descricao |
|--------|-----------|
| `cq_sal_tipos` | Tipos de sal (Fino, Grosso, Usado) |
| `cq_sal_mov` | Movimentacoes de sal (entrada/saida/consumo/ajuste/transferencia) |
| `cq_ficha_producao` | Ficha de producao (setor, turno, colaboradores, paletes em JSONB) |
| `cq_clientes` | Cadastro de clientes |
| `cq_unidades` | Unidades operacionais |
| `cq_motivos_divergencia` | Motivos padronizados |
| `cq_status_config` | Configuracao de status workflow |
| `cq_canais_notificacao` | Canais (email, WhatsApp) |
| `cq_regras_notificacao` | Regras de disparo |
| `cq_fila_notificacao` | Fila de notificacoes pendentes |
| `cq_notificacao_log` | Log de notificacoes enviadas |
| `cq_backup_log` / `cq_restore_points` / `cq_changelog` | Backup e recuperacao |
| `cq_dicionario_dados` | Dicionario de dados do sistema |
| `cq_cadastro_alias` / `cq_cadastro_merge_log` | Merge/dedup de cadastros |
| `n8n_webhook_tokens` | Tokens de autenticacao de webhooks n8n |

---

## 9. RPCs

### RPC-01 — `rpc_criar_inspecao_completa`

**Arquivo:** `05_rpc_criar_inspecao_completa.sql` | **Modo:** SECURITY DEFINER

Transacao atomica que cria lote + recebimento + inspecao + defeitos em uma unica chamada.

**Parametros obrigatorios:** `p_fornecedor_id` (uuid), `p_produto_id` (uuid), `p_numero_lote` (text), `p_quantidade_recebida` (numeric), `p_quantidade_analisada` (numeric)

**Parametros opcionais:** `p_data_lote`, `p_temperatura`, `p_placa_caminhao`, `p_motorista`, `p_transportadora`, `p_nota_fiscal`, `p_observacoes`, `p_defeitos` (jsonb array `[{defeito_id, quantidade}]`), `p_qtd_a/b/c`, `p_foto_*_url`, `p_video_evidencia_url`, `p_origem`

**Retorno (jsonb):** `{sucesso, inspecao_id, lote_id, recebimento_id, fornecedor_nome, produto_nome, total_defeitos, perc_defeitos, classificacao_abc, status_final, defeitos_inseridos}`

**Passos:** 1) Validacoes 2) UPSERT lote 3) Criar recebimento 4) Criar inspecao 5) Inserir defeitos 6) Classificacao A+B+C 7) Atualizar status 8) Ler resultado

### RPC-02 — `cq_submeter_inspecao(p_inspecao_id)`

Backend valida, classifica via motor de decisao e transiciona workflow. Chamado por `CQRegras.submeterInspecao()`.

### RPC-03 — `cq_motor_decisao(p_percentual_defeitos, p_produto_nome)`

Motor de decisao backend. Retorna: `{classificacao, status_final, limite_aprovacao, limite_ressalva, requer_analise_critica}`.

### RPC-04 — `cq_calcular_score_lote(p_class_a, p_class_b, p_class_c)`

Calcula score do lote. Retorna: `{score, classificacao_lote, pct_a, pct_b, pct_c}`.

### RPC-05 — `cq_calcular_iqf(p_fornecedor_codigo, p_janela_dias)`

Calcula IQF do fornecedor. Retorna: `{iqf_score, classificacao, tendencia}`.

### RPC-06 — `cq_validar_inspecao(p_dados)`

Valida dados da inspecao. Retorna: `{valido, erros[], total_erros, total_criticos}`.

### RPC-07 — `cq_validar_carga(p_dados)`

Valida dados da carga/recebimento. Retorna: `{valido, divergencias[], total_criticas, total_atencao}`.

### RPC-08 — `cq_avaliar_fornecedor_critico(p_fornecedor_codigo, p_pct_c)`

Avalia se fornecedor e critico. Retorna: `{critico, status_sugerido, gravidade, mensagem}`.

### RPC-09 — `cq_acoes_permitidas(p_inspecao_id)`

Lista acoes permitidas para o usuario atual. Retorna: `[{acao, label, confirm}]`.

### RPC-10 — `cq_criar_revisao(p_inspecao_id, p_decisao, p_comentario)`

Cria revisao atomica. Retorna: `{ok, decisao, de, para, revisor}`.

### RPC-11 — `cq_registrar_override(p_inspecao_id, p_campo, p_valor, p_justificativa, p_origem)`

Registra override. Retorna: `{ok, campo, valor_sistema, valor_override, decisao_original}`.

### RPC-12 — `cq_consultar_auditoria(filtros...)`

Consulta audit trail. Retorna: `{ok, registros[], total}`.

### RPC-13 — `cq_transicao_status(p_inspecao_id, p_novo_status)`

Transiciona status do workflow via backend.

### RPC-14 — `cq_proximo_codigo_rnc()`

Gera proximo codigo RNC: `RNC-YYYYMMDD-NNN`.

### RPC-15 — `cq_get_parametros(p_produto)`

Retorna parametros de inspecao por produto (limites de alerta/reprovacao).

### RPC-16 — `fn_buscar_etiqueta(p_codigo)`

Busca unificada por etiqueta em 3 fontes (atak_cargas_raw, atak_movimentacoes, registros_cq_inspecao). Retorna dados de auto-preenchimento.

---

## 10. Functions (Backend SQL)

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| `fn_normalizar_texto(p_texto)` | IMMUTABLE | Remove acentos e normaliza texto |
| `fn_set_atualizado_em()` | TRIGGER | Atualiza campo `atualizado_em` |
| `fn_hash_lote(numero, forn_id, prod_id)` | IMMUTABLE | MD5 para integridade do lote |
| `fn_cq_lotes_preparar()` | TRIGGER | Calcula hash antes de INSERT/UPDATE |
| `fn_validar_classificacao_recebimento()` | TRIGGER | Valida A+B+C = quantidade_recebida |
| `fn_recalcular_inspecao(p_id)` | PROCEDURE | Recalcula total/%, classificacao, status |
| `fn_trigger_recalcular_inspecao()` | TRIGGER | Dispara recalculo apos alteracao de defeitos |
| `fn_motor_decisao_inspecao()` | TRIGGER | Motor de decisao por severidade (6 regras) |
| `fn_calcular_iqf_fornecedor(p_codigo)` | FUNCTION | IQF com media ponderada por recencia |
| `fn_atualizar_score_fornecedor(p_codigo)` | FUNCTION | Atualiza score no cq_fornecedor_score |
| `fn_validar_abc_total()` | TRIGGER | Valida A+B+C em cargas raw do ATAK |
| `fn_validar_contagem_frigo()` | TRIGGER | Compara contagem frigorifico vs Classic |
| `fn_auto_calcular_lote_produto()` | TRIGGER | Calcula percentuais e classificacao por produto |
| `fn_consolidar_lote()` | TRIGGER | Consolida totais do lote a partir de produtos |
| `fn_gerar_id_lote()` | TRIGGER | Gera ID automatico LOT-YYYY-MM-DD-NNN |
| `fn_proximo_codigo_rnc()` | FUNCTION | Gera codigo RNC sequencial por dia |
| `fn_cq_sal_tipos_norm()` | TRIGGER | Normaliza nome de tipo de sal |
| `cq_is_authenticated()` | SECURITY DEFINER | Verifica usuario CQ ativo |

---

## 11. Triggers

| Trigger | Tabela | Evento | Funcao |
|---------|--------|--------|--------|
| `trg_cq_fornecedores_atualizado` | `cq_fornecedores` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_produtos_v2_atualizado` | `cq_produtos_v2` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_defeitos_atualizado` | `cq_defeitos` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_parametros_produto_atualizado` | `cq_parametros_produto` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_lotes_preparar` | `cq_lotes` | BEFORE INSERT/UPDATE | `fn_cq_lotes_preparar()` |
| `trg_cq_lotes_atualizado` | `cq_lotes` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_recebimentos_v2_atualizado` | `cq_recebimentos_v2` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_inspecoes_atualizado` | `cq_inspecoes` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_inspecao_defeitos_atualizado` | `cq_inspecao_defeitos` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_validar_classificacao_recebimento` | `cq_classificacoes_recebimento` | BEFORE INSERT/UPDATE | `fn_validar_classificacao_recebimento()` |
| `trg_recalcular_inspecao_after_defeitos` | `cq_inspecao_defeitos` | AFTER INSERT/UPDATE/DELETE | `fn_trigger_recalcular_inspecao()` |
| `trg_motor_decisao` | `registros_cq_inspecao` | BEFORE INSERT/UPDATE (qtd>0) | `fn_motor_decisao_inspecao()` |
| `trg_guard_reamostragem` | `registros_cq_inspecao` | BEFORE UPDATE | Protege inspecoes em reamostragem |
| `trg_auto_calcular_lote_produto` | `cq_lote_produto` | BEFORE INSERT/UPDATE | `fn_auto_calcular_lote_produto()` |
| `trg_consolidar_lote` | `cq_lote_produto` | AFTER INSERT/UPDATE/DELETE | `fn_consolidar_lote()` |
| `trg_gerar_id_lote` | `cq_lote_inspecao` | BEFORE INSERT | `fn_gerar_id_lote()` |
| `trg_validar_abc_total` | `atak_cargas_raw` | BEFORE INSERT/UPDATE | `fn_validar_abc_total()` |
| `trg_validar_contagem_frigo` | `atak_cargas_raw` | AFTER INSERT | `fn_validar_contagem_frigo()` |

---

## 12. APIs (Chamadas Frontend)

### 12.1 Supabase REST (PostgREST)

**Leitura (GET):**

| Endpoint | Tela | Descricao |
|----------|------|-----------|
| `registros_cq_inspecao?select=*&order=criado_em.desc&limit=500` | Fila | Lista inspecoes |
| `cq_recebimentos?select=*&order=criado_em.desc&limit=500` | Fila | Recebimentos PCR |
| `registros_cq_inspecao?lote_atak=eq.{lote}` | Rastreabilidade | Busca por lote |
| `registros_cq?lote_atak=eq.{lote}` | Rastreabilidade | Registros legados |
| `registros_cq_inspecao?fornecedor=eq.{nome}&limit=10` | Rastreabilidade | Historico fornecedor |
| `registros_cq_inspecao?order=data_inspecao.desc&limit=2000` | Gestao Fornecedores | Todas inspecoes |
| `registros_cq?order=created_at.desc&limit=500` | Gestao Fornecedores | Legados |
| `vw_supplier_score?select=*&order=score_confiabilidade.desc` | Score | View de score |
| `vw_qualidade_por_estado` | Dashboard | Qualidade por UF |
| `vw_qualidade_fornecedor_estado` | Dashboard | Por fornecedor x UF |
| `vw_qualidade_produto_estado` | Dashboard | Por produto x UF |
| `vw_cq_recebimentos_completos` | — | View operacional |
| `vw_inspecoes_workflow` | Workflow | Inspecoes com dados de workflow |
| `cq_revisoes?inspecao_id=eq.{id}` | Workflow | Revisoes de uma inspecao |
| `cq_nao_conformidades?inspecao_id=eq.{id}` | Workflow | NCs de uma inspecao |
| `cq_anexos?inspecao_id=eq.{id}&ativo=eq.true` | Workflow | Anexos |
| `cq_usuarios?ativo=eq.true` | Workflow | Usuarios ativos |

**Escrita (POST):**

| Endpoint | Tela | Descricao |
|----------|------|-----------|
| `registros_cq_inspecao` | Inspecao | INSERT como rascunho (Prefer: return=representation) |
| `registros_cq` | Formulario CQ | INSERT legado (Prefer: return=minimal) |
| `cq_nao_conformidades` | Workflow | Criar NC |

**RPCs (POST /rest/v1/rpc/):** Ver secao 9.

### 12.2 Supabase Storage

| Operacao | Path | Descricao |
|----------|------|-----------|
| POST `storage/v1/object/cq-fotos/inspecao/{id}/{tipo}_{ts}.jpg` | Upload foto | x-upsert: true |
| POST `storage/v1/object/cq-fotos/inspecao/{id}/video_{ts}.{ext}` | Upload video | Max 50MB |
| GET `storage/v1/object/public/cq-fotos/{path}` | Leitura | URL publica |

### 12.3 n8n Webhooks

| Endpoint | Metodo | Descricao |
|----------|--------|-----------|
| `{n8n_url}/webhook/atak-cq/check-duplicidade` | POST | Anti-duplicidade (409 se duplicado) |
| `{n8n_url}/healthz` | GET | Health check do n8n |

---

## 13. Integracoes

### 13.1 ATAK (Sistema de Gestao de Abate)

- **Tipo:** REST API via n8n workflows
- **Direcao:** ATAK -> CQ (importacao)
- **5 endpoints configurados:** fornecedores (diario), cargas (30min), producao (60min), embarques (30min), expedicao (60min)
- **Dados importados:** Fornecedores (codigo, nome, UF), cargas (PCR, classificacao A/B/C), producao, embarques
- **Tabelas:** `atak_sync_config`, `atak_sync_log`, `atak_cargas_raw`, `atak_embarques_raw`, `atak_producao_raw`, `atak_expedicao_raw`

### 13.2 n8n (Automacao de Workflows)

- **Tipo:** Docker container local / cloud
- **Endpoints:** anti-duplicidade, health check
- **Configuracao:** URL base em `localStorage('cq_n8n_webhook_url')`
- **Autenticacao:** Tokens em `n8n_webhook_tokens`

### 13.3 Tesseract OCR (Reconhecimento de Texto)

- **Tipo:** JavaScript (CDN: `tesseract.js@5`)
- **Uso:** Identificacao automatica de numeros em etiquetas/lotes
- **Padroes reconhecidos:**
  - Etiqueta: `NNN-NNN-NNNNN` (ex: 101-001-12345)
  - Lote: `REQUISICAO NNNNN`, `REQ NNNNN`, `LOTE NNNNN`, `N. NNNNN`
- **Idioma:** `por` (portugues)

### 13.4 Scanner de Codigo de Barras (USB/Bluetooth)

- **Tipo:** Deteccao de input rapido em campos `.scannable`
- **Parametros:** `SCAN_THRESHOLD = 50ms`, `MIN_LENGTH = 3`
- **Fluxo:** Buffer acumula -> Enter finaliza -> auto-preenchimento + auto-tab
- **Origens registradas:** `camera`, `scanner`, `manual`, `api` (em `cq_scan_log`)

### 13.5 WhatsApp e Email (Envio de Relatorios)

- Barra de selecao multipla na fila de inspecoes
- WhatsApp: link `wa.me/` com mensagem pre-formatada
- Email: `mailto:` com subject e body pre-formatados

---

## 14. Dashboards

### 14.1 Dashboard CQ (`dashboard_cq.html`)

**8 KPIs:** Total Lotes, Classe A, Classe B, Classe C, % Medio Defeito, Liberados, Fornecedores Distintos, Top 3 Defeitos

**Filtros:** Periodo, Fornecedor, Classe (A/B/C), Analisador, Defeito

**10 Tabs:** Visao Geral (pizza + linha + barras), Por Fornecedor (ranking + score), Volume Recebido (top 10 + composicao A/B/C), Indice de Defeitos (grid + heat + recorrencia), Tendencia Semanal (12 semanas), Fornecedor/Tempo (timeline), Por Analisador, Comparativo, Registros (tabela completa), Workflow

**Graficos (Chart.js 4 + datalabels):** pizza, linha, barras horizontais, barras empilhadas, barras agrupadas

### 14.2 Score de Confiabilidade (`dashboard_supplier_score.html`)

**4 KPIs:** Total Fornecedores, Score Medio, % Confiaveis, Criticos

**Ranking:** Tabela `vw_supplier_score` com: Fornecedor, UF, Score (barra visual), Classificacao (4 badges), Inspecoes, Bloqueios, NCs, Media Defeitos

**Graficos:** Distribuicao de Classificacao (pizza), Top 10 Piores (barras)

**Filtro:** 30, 90, 180 dias, 1 ano, todos

---

## 15. Relatorios

### 15.1 Relatorio de Inspecao (PDF via jsPDF + html2canvas)

Gerado por `gerarPDFInspecao()`. Secoes: 1) Identificacao 2) Defeitos (desconto financeiro + acao corretiva, com subtotais) 3) Classificacao (badge colorido) 4) Resultado Tecnico (decisao + observacoes + assinaturas)

### 15.2 Relatorio Tecnico (PDF — Rastreabilidade)

Gerado por `gerarRelatorio('tecnico')`. Detalhamento completo do lote com timeline.

### 15.3 Relatorio ao Fornecedor (PDF — Rastreabilidade)

Gerado por `gerarRelatorio('fornecedor')`. Versao simplificada para envio ao fornecedor.

### 15.4 Exportacao CSV

Dashboard CQ: `exportarCSV()` com dados filtrados.
Score: `exportCSV()` com ranking completo.

---

## 16. Documentos

### 16.1 PDF de Inspecao com Classificacao

**Titulo:** "RELATORIO DE INSPECAO DE QUALIDADE — Classic Couros"

**Tabela de classificacao no PDF:**

| Intervalo | Classificacao |
|-----------|---------------|
| 0 - 5% | Aprovado |
| 5 - 15% | Aprovado com Ressalva |
| 15 - 30% | Sujeito a Negociacao |
| > 30% | Reprovado |

**Separacao de defeitos no PDF:**
- Secao amarela: DESCONTO FINANCEIRO (4 defeitos, subtotal em %)
- Secao azul: ACAO CORRETIVA (7 defeitos, subtotal em %)
- Rodape escuro: TOTAL GERAL

---

## 17. Evidencias

### 17.1 Fotos (3 por inspecao)

| Foto | Storage Path | Coluna | Fonte |
|------|-------------|--------|-------|
| Etiqueta | `inspecao/{id}/etiqueta_{ts}.jpg` | `foto_etiqueta_url` | Camera ou galeria |
| Produto | `inspecao/{id}/produto_{ts}.jpg` | `foto_produto_url` | Camera ou galeria |
| Defeito | `inspecao/{id}/defeito_{ts}.jpg` | `foto_defeito_url` | Camera ou galeria |

Popup bottom-sheet oferece "Tirar Foto" ou "Galeria / Arquivo".

### 17.2 Video de Evidencia

- Limite: 50 MB maximo
- Path: `inspecao/{id}/video_{ts}.{ext}`
- Coluna: `video_evidencia_url`

### 17.3 OCR (Tesseract.js v5)

Foto -> OCR -> Resultado editavel -> Confirmacao -> Preenchimento automatico

### 17.4 Offline (IndexedDB + localStorage)

- Fotos: IndexedDB `cq_fotos_offline` (store `fotos`)
- Dados: `localStorage('cq_pendentes')` como JSON array
- Sync: `sincronizarPendentes()` ao reconectar (upload fotos + POST dados)
- Retry: ate 10 tentativas com remocao de colunas inexistentes

---

## 18. Permissoes

### 18.1 RLS (Row Level Security) — Hardening (`12_hardening_rls_cq.sql`)

**10 tabelas com RLS endurecido:** cq_fornecedores, cq_produtos_v2, cq_lotes, cq_recebimentos_v2, cq_inspecoes, cq_inspecao_defeitos, cq_defeitos, cq_nao_conformidade, cq_overrides_v2, cq_classificacoes_recebimento

**Policies:** SELECT/INSERT/UPDATE via `cq_is_authenticated()`. DELETE bloqueado (`USING(false)`).

**Acesso anon:** Revogado em todas as tabelas `cq_*`.

### 18.2 Permissoes por Perfil (Frontend)

`CQAuth.init({ perfisPermitidos })` e `CQPermissoes.verificar(tela, acao)`.

---

## 19. Auditoria

### 19.1 Audit Trail Backend (`cq_audit_trail_v2`)

Campos: tabela, registro_id, operacao (INSERT/UPDATE/DELETE), antes (jsonb), depois (jsonb), realizado_por (uuid), origem, criado_em.

### 19.2 Log de Sessao Frontend (`CQAuth.log()`)

Registra acoes complementares: `create_inspecao`, com inspecao_id, lote_atak, fornecedor, backend_result.

### 19.3 Dados de Auditoria por Inspecao

Automatico: usuario_id, usuario_email, criador_id, latitude, longitude, criado_em, offline (boolean).

### 19.4 Scan Audit (`cq_scan_log` + `cq_scan_alteracao_log`)

Log de leituras (scanner/camera/manual) e alteracoes pos-scan com justificativa obrigatoria.

### 19.5 Changelog e Backup

`cq_changelog`, `cq_backup_log`, `cq_restore_points` para governanca de schema e dados.

---

## 20. Eventos

### 20.1 Eventos do Ciclo de Vida

| Evento | Trigger/Handler |
|--------|----------------|
| Recebimento criado | INSERT em `cq_recebimentos_v2` |
| Inspecao rascunho | INSERT em `registros_cq_inspecao` (status_workflow='rascunho') |
| Inspecao submetida | RPC `cq_submeter_inspecao` |
| Defeitos alterados | TRIGGER `trg_recalcular_inspecao_after_defeitos` |
| Motor decisao | TRIGGER `trg_motor_decisao` |
| Transicao workflow | RPC `cq_transicao_status` |
| Override aplicado | INSERT em `cq_overrides_v2` |
| NC registrada | INSERT em `cq_nao_conformidades` |
| Alerta gerado | INSERT em `cq_alertas_qualidade` |
| Reamostragem solicitada | INSERT em `cq_reamostragem` |
| Score atualizado | `fn_atualizar_score_fornecedor()` |

### 20.2 Eventos de Conectividade

| Evento | Handler |
|--------|---------|
| `online` | `sincronizarPendentes()` |
| `offline` | Atualiza flag `isOnline` |

### 20.3 Eventos de UI

| Evento | Handler |
|--------|---------|
| Defeito alterado | `onDefectInput()` -> `calcResultado()` |
| Qtd analisada | `calcResultado()` |
| Temperatura | `checkTemperatura()` |
| Scanner Enter | Scanner detection + auto-tab |
| Filtro alterado | `carregarLista()` |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico | Detalhe |
|------------|---------|---------|
| Banco de dados | Supabase PostgreSQL | `nvqxsulntpftcwtkjedu.supabase.co` |
| Autenticacao | Supabase Auth | JWT, perfis em `cq_usuarios` |
| Storage | Supabase Storage | Bucket `cq-fotos/` |
| Frontend | Vercel (auto-deploy main) | `app.classiccouros.com.br` |
| Automacao | n8n (Docker) | Configuravel via localStorage |

### 21.2 Bibliotecas Externas

Chart.js 4, chartjs-plugin-datalabels 2, jsPDF 2.5.1, html2canvas 1.4.1, Tesseract.js 5

### 21.3 Bibliotecas Internas

config.js, cq_api.js, cq_auth.js, cq_regras.js, cq_workflow.js, cq_fornecedores.js, ds.css

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Lancamento de Cargas | CQ | Decisao financeira (desconto/liberacao/bloqueio) |
| Controle de Sal | CQ | cq_sal_tipos, cq_sal_mov (entrada/saida/consumo) |
| Ficha de Producao | CQ | cq_ficha_producao (setor, turno, colaboradores) |
| Dashboard Omaso | CQ | Alertas especificos para Omaso |
| Gestao RH/SST | RH | Cadastro de funcionarios (inspetores) |
| ETL Pipeline | ETL | Importacao de dados externos |
| n8n Workflows | Automacao | Sincronizacao ATAK, notificacoes |

---

## 23. Melhorias SIGA

### SIGA-01 — Padronizacao de Inspetores

Substituir lista fixa (`const INSPETORES`) por cadastro dinamico via `cq_usuarios`.

### SIGA-02 — Parametros de Produto Dinamicos

Migrar limites hardcoded de temperatura para `cq_parametros_produto` com tela administrativa.

### SIGA-03 — Workflow com Notificacao Automatica

Implementar disparos automaticos nas transicoes de workflow usando `cq_regras_notificacao` e `cq_fila_notificacao`.

### SIGA-04 — Dashboard de NC

Dashboard dedicado para NC (abertas, atrasadas, eficacia).

### SIGA-05 — Integracao Completa ATAK

Importacao de PCRs via API ATAK em tempo real.

### SIGA-06 — Unificacao de Formulas de Score

Atualmente existem 4 formulas diferentes de score conforme o contexto. Unificar em uma unica formula parametrizada.

---

## 24. Adequacao SIGA (Multiempresa)

### 24.1 Cenario Atual

Sistema CQ opera exclusivamente para a Classic Couros (CNPJ 08.849.964/0001-10) com foco em subprodutos bovinos (omaso/bucho).

### 24.2 Cenario Multiempresa

| Empresa | Produto Principal | Adequacoes |
|---------|-------------------|------------|
| Classic Couros | Couros e subprodutos bovinos (omaso) | Base atual |
| J.A. | Carne bovina | Tipos de defeito para carne, temperaturas diferentes, classificacao por corte |

**Alteracoes necessarias:**

1. Tabela `cq_empresas` com CNPJ e configuracoes
2. Coluna `empresa_id` em todas as tabelas CQ
3. RLS por empresa: `empresa_id = get_empresa_usuario()`
4. Tipos de defeito por empresa/produto
5. Parametros (limites ABC, temperatura) por empresa
6. Catalogo de produtos ampliado (cortes bovinos)
7. Inspetores separados por unidade
8. Dashboards com filtro por empresa

---

## 25. Criterios de Aceite

### CA-01 — Recebimento de Lote

Registrar recebimento com fornecedor, produto, quantidade, temperatura e fotos. ID `REC-XXX` gerado automaticamente. Temperatura validada por tipo de produto.

### CA-02 — Inspecao Completa

Preencher 11 defeitos com stepper. Calculo de % em tempo real. Classificacao ABC conforme RN-01. Salvar como rascunho. RPC `cq_submeter_inspecao` chamada para classificacao backend.

### CA-03 — Classificacao ABC Correta

A: <=5%, B: 5-15%, C: >15%. Soma A+B+C = quantidade recebida.

### CA-04 — Motor de Decisao por Severidade

Regras R1-R6 aplicadas corretamente. Score do lote calculado. Reamostragem solicitada quando necessario.

### CA-05 — Funcionamento Offline

Inspecoes salvas em localStorage/IndexedDB. Sincronizacao automatica ao reconectar. Toast de confirmacao.

### CA-06 — Score do Fornecedor

Score calculado conforme formulas. IQF com janela de 90 dias. Classificacao: Confiavel/Regular/Atencao/Critico.

### CA-07 — Nao Conformidade

Codigo RNC sequencial. Status transicionavel. Prazo de resposta. Validacao de eficacia.

### CA-08 — Rastreabilidade

Busca por lote. Timeline de 9 etapas. PDFs tecnico e fornecedor.

### CA-09 — Dashboard

8 KPIs, 10 tabs, graficos Chart.js, exportacao CSV, alertas Classe C.

### CA-10 — Seguranca

RLS habilitado. Anon revogado. DELETE bloqueado. Override com justificativa >= 10 chars.

---

## 26. Casos de Teste

### CT-01 — Criar Inspecao Online (Happy Path)

Selecionar fornecedor, produto "Omaso Salgado", lote "0050", qtd 200, defeitos fermentacao=5 sujo=3. Resultado: 4.0%, classe A, aprovado.

### CT-02 — Criar Inspecao Classe C

Qtd 100, fermentacao=20 sem_folhas=5. Resultado: 25%, classe C, bloqueado.

### CT-03 — Lote Critico (>30%)

Qtd 100, fermentacao=25 pedaco=10. Resultado: 35%, alerta "LOTE CRITICO".

### CT-04 — Inspecao Offline

Sem rede: salvar -> localStorage + IndexedDB. Reconectar -> sincronizar -> toast.

### CT-05 — Temperatura Fora do Limite

Produto "Congelado", temperatura=-5oC. Alerta: "Acima do limite (-12oC)".

### CT-06 — Scanner de Codigo de Barras

Bipar "0046" -> campo preenchido, toast, foco avanca.

### CT-07 — Anti-Duplicidade

Lote "0046" existente -> toast "Duplicidade detectada", nao salva.

### CT-08 — OCR de Etiqueta

Foto "REQUISICAO 12345" -> campo preenchido com "12345".

### CT-09 — Override de Classificacao

C -> B com justificativa >= 10 chars. Registro em cq_overrides_v2.

### CT-10 — Score do Fornecedor

Fornecedor com 5+ lotes: score calculado, barra colorida (verde>=70, amarelo>=40, vermelho<40).

### CT-11 — Rastreabilidade de Lote

Buscar lote, verificar header, KPIs, timeline, inspecao, historico fornecedor.

### CT-12 — Geracao de PDF

PDF com 4 secoes, defeitos desconto/corretiva separados, classificacao.

---

## 27. Casos Extremos

### CE-01 — Quantidade Analisada Zero

Resultado: "Aguardando dados", botao desabilitado.

### CE-02 — Todos os Defeitos Zero

Resultado: 0.0%, classe A, "Sem defeitos".

### CE-03 — Defeitos > Quantidade Analisada

Backend rejeita via constraint `ck_cq_inspecoes_total_defeitos_lte_qtd`.

### CE-04 — Fornecedor Inexistente

Componente exibe "Nenhum resultado", campo fica vazio.

### CE-05 — Lote Duplicado

RPC faz UPSERT, reutiliza lote existente.

### CE-06 — Perda de Conexao Durante Save

Registro salvo offline, modal "Salvo Localmente", sync posterior.

### CE-07 — Foto Muito Grande

IndexedDB (sem limite 5MB do localStorage). Upload normal.

### CE-08 — Video > 50MB

Toast "Video muito grande (max 50MB)", rejeitado.

### CE-09 — Coluna Inexistente no Banco

Retry automatico: regex detecta coluna, remove do payload, tenta novamente (ate 10x).

### CE-10 — A+B+C Inconsistente

Trigger lanca excecao: "Classificacao inconsistente: A+B+C != quantidade recebida".

### CE-11 — Override sem Justificativa Adequada

Constraint rejeita justificativa < 10 caracteres.

### CE-12 — Multiplos Navegadores

Ambos salvam (IDs unicos). Lote reutilizado via UPSERT.

---

## 28. Melhorias Futuras

### MF-01 — Modo Offline Completo (PWA)

PWA com Service Worker para cache de assets e sincronizacao em background.

### MF-02 — Assinatura Digital

Captura de assinatura touch no dispositivo movel para relatorio PDF.

### MF-03 — IA para Deteccao de Defeitos

Visao computacional para deteccao automatica de defeitos a partir de fotos.

### MF-04 — Balanca IoT

Integracao com balanca digital via Bluetooth/USB para peso automatico.

### MF-05 — Termometro IoT

Sensor de temperatura via Bluetooth para leitura automatica.

### MF-06 — Dashboard em Tempo Real

Supabase Realtime (WebSocket) para atualizacao automatica.

### MF-07 — Relatorio Gerencial Automatico

Relatorio semanal/mensal via n8n com envio por email.

### MF-08 — Integracao com ERP

NF, romaneio, pedidos de compra e custos de materia-prima.

### MF-09 — Mapa Geografico de Fornecedores

Heatmap de qualidade por estado com drill-down para municipio.

### MF-10 — Predicao de Qualidade

Historico para prever qualidade de lotes futuros por fornecedor.

### MF-11 — QR Code por Lote

QR Code unico para consulta rapida de rastreabilidade.

### MF-12 — Reamostragem Automatizada

Workflow completo com disparos automaticos e consolidacao.

### MF-13 — Multilinguagem

Suporte a ingles/espanhol para operacoes internacionais.

### MF-14 — API Publica para Fornecedores

Portal do fornecedor com consulta de scores, NCs e upload de documentos.

### MF-15 — Unificacao de Formulas de Score

Consolidar as 4 formulas de score em uma unica parametrizada.

---

*Documento gerado em 28/07/2026 — Classic IA — Controle de Qualidade*
*Versao 1.0 — Especificacao Funcional Completa F-31*
