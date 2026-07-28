# F-44 — Treinamentos NR

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-44 |
| Titulo | Treinamentos NR — Controle de Certificacoes e Validades |
| Modulo | SST (Saude e Seguranca do Trabalho) |
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

**F-44 — Treinamentos NR** e o modulo de controle dos treinamentos obrigatorios de seguranca do trabalho (Normas Regulamentadoras — NR-06, NR-10, NR-11, NR-12, NR-33, NR-35, CIPA, Brigada, Integracao) exigidos por lei: cadastra os treinamentos realizados por colaborador, mantem o catalogo de tipos com validade por NR, calcula automaticamente o vencimento e sinaliza reciclagens pendentes.

O modulo compreende **1 tela principal**, alimentada por **tabelas SST canonicas** (`sst_tipo_treinamento`, `sst_cargo_treinamento`, `sst_treinamento`), tabelas RH (`rh_treinamentos`, `rh_treinamentos_tipos` — drift) e a RPC `sst_dashboard_completo` (bloco treinamentos).

**Tela do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/treinamentos.html` | Painel de vencimentos, cadastro, tipos e matriz por colaborador (4 abas) | 1003 |

**Bibliotecas compartilhadas (RH):** config.js, rh_auth.js (`RHAuth.init`), api.js (`API.get/post/patch`), ui_components.js, ds.css. **Sem Chart.js, sem Shell.init.**

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `MODULO_SST_v2.sql` | `sst_tipo_treinamento`, `sst_cargo_treinamento` (matriz), `sst_treinamento` (**gravada** pela tela) |
| `SEGURANCA_SUPABASE.sql` / `schema_rh_sst_completo.sql` | `rh_treinamentos` (versionada, denormalizada) |
| `FUNCOES_APLICACAO.sql` | RPC `sst_dashboard_completo()` (bloco treinamentos) |
| (drift, nao versionada) | `rh_treinamentos_tipos` (catalogo de tipos — **lida** pela tela) |

> **Nota critica (mesmo padrao dos ASOs):** a tela **le de `rh_treinamentos`** (join `rh_treinamentos_tipos` + `rh_colaboradores`) mas **grava em `sst_treinamento`**, com colunas divergentes — o treinamento cadastrado nao reaparece no painel. Ver secao 24 (D-01).

---

## 2. Problema

As NRs (Portaria 3.214/78 e atualizacoes) exigem treinamentos periodicos com reciclagem em prazos definidos, sob pena de autuacao e responsabilizacao em acidentes. Antes do sistema:

1. **Controle de validade manual** — Sem alerta de treinamentos vencidos ou a vencer, com risco de colaborador operar equipamento (NR-12) ou trabalhar em altura (NR-35) sem certificacao valida.
2. **Prazos por NR diferentes** — Cada NR tem validade propria (NR-33 12 meses, NR-35 24 meses, etc.), dificil de controlar em planilha.
3. **Reciclagem esquecida** — A reciclagem periodica nao era agendada com antecedencia.
4. **Sem historico** — Certificados em papel, sem registro de instrutor, carga horaria, numero de certificado.

**Solucao:** modulo que (a) cadastra treinamentos com vencimento auto-calculado por tipo/NR, (b) exibe painel de vigentes/vencidos/vencendo (30/60 dias), (c) mantem catalogo de tipos configuravel, (d) permite cadastro em lote e visao por colaborador.

---

## 3. Processo Operacional

### 3.1 Cadastro de Treinamento (`treinamentos.html` aba Cadastrar)

1. Seleciona colaborador (autocomplete) — ou ativa o **modo lote** (varios colaboradores, mesmo treinamento).
2. Seleciona o tipo de treinamento (NR) → `validade_meses` auto-preenchida.
3. Informa data de realizacao, carga horaria, instituicao/instrutor, numero do certificado, observacoes.
4. A data de vencimento e calculada (readonly) = realizacao + validade (meses).
5. Salva → POST em `sst_treinamento` (1 registro por colaborador no modo lote).

### 3.2 Catalogo de Tipos (`treinamentos.html` aba Tipos)

Modal de tipo: nome, NR (opcional), validade (meses, default 24; 0 = nao vence), obrigatorio (sim/nao), descricao. CRUD via `rh_treinamentos_tipos` (POST/PATCH; desativacao soft via `ativo`).

### 3.3 Painel de Vencimentos (`treinamentos.html` aba Painel)

Lista de treinamentos ordenada por vencimento. 4 KPIs + 2 alertas (vencidos, vencendo 30d). Filtros por status e tipo.

### 3.4 Por Colaborador / Matriz (`treinamentos.html` aba Por Colaborador)

Busca um colaborador → historico de treinamentos + **matriz** (colaborador × tipos ativos) com status colorido por celula (verde vigente, amarelo vencendo, vermelho vencido, azul sem vencimento, cinza nao realizado).

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Acesso |
|--------|--------|
| administrador / rh / sst | Cadastro, tipos, matriz |
| demais autenticados | Acesso (sem gating de perfil no cliente) |

Gate de autenticacao apenas (`RHAuth.init`), **sem checagem de perfil** — todas as acoes (criar/editar/desativar tipos, cadastrar) disponiveis a qualquer autenticado. RLS aberta no schema (`FOR ALL USING(true)`).

### 4.2 Auditoria

Sem `RHAuth.log` especifico neste modulo (diferente do F-43). Trilha via timestamps das tabelas.

---

## 5. Fluxograma

```
CADASTRO DE TREINAMENTO                   CATALOGO DE TIPOS (NR)
======================                    ======================
Selecionar colaborador (ou lote)          rh_treinamentos_tipos:
   |                                         nome, nr, validade_meses,
   v                                         obrigatorio, ativo
Selecionar tipo (NR)                         |
   | validade_meses auto                     v
   v                                       Tipos padrao:
Data realizacao + carga + instrutor          NR-06 24m | NR-10 24m | NR-11 24m
   |                                         NR-12 24m | NR-33 12m | NR-35 24m
   v                                         Integracao 0 (nao vence)
Vencimento = realizacao + validade           Brigada 12m | CIPA 12m
   | (0 = nao vence)                          Direcao Defensiva 60m (nao obrig.)
   v
POST sst_treinamento  <-- (painel le rh_treinamentos!)
   | (lote: N registros)
   v
PAINEL (ordenado por vencimento)          MATRIZ (por colaborador)
  status: vigente/vencendo(30)/            colaborador x tipos ativos
    atencao(60)/vencido/sem                 celula colorida por status
  4 KPIs + alertas                          (nao usa obrigatorio)

DASHBOARD SST (sst_dashboard_completo)
======================================
treinamentos: total | validos | vencidos | tipos
  (sst_treinamento, funcionarios ativos)
```

---

## 6. Regras de Negocio

### RN-01 — Tipos de Treinamento (NR)

Catalogo em `rh_treinamentos_tipos` (nome, nr, validade_meses, obrigatorio, ativo). Tipos padrao com validades por NR:

| Tipo | Validade | Obrigatorio |
|------|----------|-------------|
| NR-06 (EPI) | 24 meses | Sim |
| NR-10 (Seguranca Eletrica) | 24 meses | Sim |
| NR-11 (Transporte e Movimentacao) | 24 meses | Sim |
| NR-12 (Maquinas e Equipamentos) | 24 meses | Sim |
| NR-33 (Espacos Confinados) | 12 meses | Sim |
| NR-35 (Trabalho em Altura) | 24 meses | Sim |
| Integracao de Seguranca | 0 (nao vence) | Sim |
| Brigada de Incendio | 12 meses | Sim |
| CIPA | 12 meses | Sim |
| Direcao Defensiva | 60 meses | Nao |

### RN-02 — Data de Vencimento

`data_vencimento = data_realizacao + validade_meses`. Se validade = 0 (ou nula) → sem vencimento ("nao vence"). Campo readonly, auto-calculado no cliente (`calcDataVencimento`).

### RN-03 — Status do Treinamento (janelas 30/60)

`calcStatus(dias_ate_vencer)`: validade 0/nula → **Sem vencimento**; sem data → **Sem data**; `diff < 0` → **Vencido**; `<= 30` → **Vencendo**; `<= 60` → **Atencao**; `> 60` → **Vigente**. Cores: vigente verde, vencendo amarelo, atencao accent, vencido vermelho, sem vencimento azul.

### RN-04 — Cadastro em Lote

Modo lote aplica o mesmo treinamento a N colaboradores selecionados (chips), gerando N registros no POST. Util para turmas.

### RN-05 — Sst_treinamento (gravacao)

Payload gravado em `sst_treinamento`: `funcionario_id`, `tipo_treinamento_id`, `data_realizacao`, `carga_horaria`, `instrutor`, `numero_certificado`, `data_validade`, `observacoes`. Constraint `chk_trein_validade` (data_validade >= data_realizacao).

### RN-06 — Rh_treinamentos (leitura)

Leitura de `rh_treinamentos` com join `rh_treinamentos_tipos(nome,nr,validade_meses)` + `rh_colaboradores(nome)`, filtrando por `colaborador_id`/`tipo_id`/`data_vencimento`. **Colunas divergentes da gravacao** (ver D-01/D-02).

### RN-07 — Matriz por Colaborador

Matriz = 1 colaborador × todos os tipos ativos; celula pega o treinamento mais recente daquele tipo (`data_realizacao desc`). **Nao usa o campo `obrigatorio`** nem detecta pendencia por cargo. "Nao realizado" = celula cinza (sem alerta de obrigatorio).

### RN-08 — Matriz Cargo × Treinamento (backend, nao usada pela tela)

`sst_cargo_treinamento` mapeia cargo_id × tipo_treinamento_id (obrigatorio) — a matriz de elegibilidade real. **O frontend nao consome essa tabela** (a "matriz" da tela e por colaborador, sem obrigatoriedade por cargo). Ver D-06.

### RN-09 — Dashboard SST: Treinamentos

`sst_dashboard_completo` bloco treinamentos: total (sst_treinamento), validos (data_validade nula ou futura), vencidos (funcionarios ativos com data_validade < hoje), tipos (sst_tipo_treinamento ativos).

### RN-10 — Desativacao Soft de Tipo

Tipos nao sao excluidos — desativados via PATCH `ativo = false`. Tipos inativos somem dos dropdowns.

### RN-11 — KPI "Ativos" = Total

O KPI "Treinamentos Ativos" conta **todos** os registros carregados, nao apenas os vigentes (nao distingue vigente/vencido) — ver D-05.

---

## 7. Campos

### 7.1 Formulario de Treinamento (`treinamentos.html`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Colaborador (individual) | `busca-colab`/`colab-id` | autocomplete | Sim (se nao-lote) |
| Colaboradores (lote) | `busca-colab-bulk`/`chip-colabs` | chips | Sim (se lote) |
| Tipo de Treinamento | `cad-tipo` | select | Sim |
| Data de Realizacao | `cad-data` | date | Sim |
| Carga Horaria (h) | `cad-carga` | number (step 0.5) | Nao |
| Instituicao / Instrutor | `cad-instituicao` | text | Nao |
| Numero do Certificado | `cad-certificado` | text | Nao |
| Validade (meses) | `cad-validade` | number | Auto (0 = nao vence) |
| Data de Vencimento | `cad-vencimento` | date (readonly) | Calculado |
| Observacoes | `cad-obs` | textarea | Nao |

Toggle `modo-bulk`. **Sem campo aprovado/reprovado** (apesar de `sst_treinamento.aprovado` existir) e **sem upload/URL de certificado** (so numero).

### 7.2 Modal de Tipo (`rh_treinamentos_tipos`)

| Campo | ID | Tipo | Default |
|-------|----|------|---------|
| Nome | `tipo-nome` | text | — |
| NR (opcional) | `tipo-nr` | text | — |
| Validade (meses) | `tipo-validade` | number | 24 (0 = nao vence) |
| Obrigatorio | `tipo-obrigatorio` | select | Sim |
| Descricao | `tipo-descricao` | textarea | — |

**Sem campo carga horaria minima** (apesar de `sst_tipo_treinamento.carga_horaria_minima` existir).

### 7.3 Filtros do Painel

`filtro-status` (Vencido/Vencendo/Atencao/Vigente/Sem vencimento), `filtro-tipo` (tipos ativos por id).

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `sst_tipo_treinamento` | Catalogo canonico SST | nome, norma_regulamentadora, carga_horaria_minima, validade_meses, descricao, ativo, UNIQUE(nome, nr) |
| `sst_cargo_treinamento` | Matriz cargo × treinamento (obrigatoriedade) | cargo_id (FK), tipo_treinamento_id (FK), obrigatorio, UNIQUE(cargo,tipo) |
| `sst_treinamento` | Treinamento realizado — **gravada** pela tela | funcionario_id, tipo_treinamento_id (FK), data_realizacao, data_validade, carga_horaria, instrutor, entidade, certificado_numero, aprovado, CHECK(data_validade>=data_realizacao) |
| `rh_treinamentos` (versionada) | Treinamento (denormalizado) | colaborador_id, treinamento (texto), nr_referencia, data_realizacao, data_vencimento, carga_horaria_horas, instrutor, certificado_url, status (valido/vencido/cancelado) |
| `rh_treinamentos` (prod/drift) | Treinamento (normalizado — **lida** pela tela) | colaborador_id, tipo_id (FK rh_treinamentos_tipos), data_realizacao, data_vencimento, carga_horaria, instituicao, numero_certificado, observacoes |
| `rh_treinamentos_tipos` (drift) | Catalogo de tipos — **lida** pela tela | nome, nr, validade_meses, obrigatorio, ativo, descricao |

---

## 9. RPCs

### RPC-01 — `sst_dashboard_completo()`

**SECURITY DEFINER.** Bloco `treinamentos`: total, validos, vencidos (funcionarios ativos), tipos. Usado pelo sst_dashboard.html (nao diretamente por treinamentos.html).

> **Nota:** `treinamentos.html` **nao chama RPCs** — opera por PostgREST direto (GET/POST/PATCH).

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `sst_dashboard_completo()` | KPIs SST (inclui bloco treinamentos) |
| Triggers de `updated_at` | Padrao SST |

Nao ha function de calculo de vencimento/status — feito no frontend (`calcDataVencimento`, `calcStatus`).

---

## 11. Triggers

Triggers de `updated_at` em sst_treinamento, sst_tipo_treinamento. CHECK `chk_trein_validade`. **Sem trigger de vencimento automatico** nem de reciclagem.

---

## 12. APIs (Chamadas Frontend)

### 12.1 treinamentos.html (`API`)

- **GET:** `rh_treinamentos_tipos` (ativos), **rh_treinamentos** (`select=*,rh_treinamentos_tipos(nome,nr,validade_meses),rh_colaboradores(nome)&order=data_vencimento.asc`), rh_colaboradores (ativos)
- **POST:** **sst_treinamento** (novo treinamento), rh_treinamentos_tipos (novo tipo)
- **PATCH:** rh_treinamentos_tipos (editar / ativar-desativar)

Nenhuma view, nenhuma RPC, nenhum Chart.js.

---

## 13. Integracoes

### 13.1 eSocial (S-2220 — nao integrado aqui)

Treinamentos NR compoem o Monitoramento da Saude/Seguranca no eSocial, mas a tela **nao gera eventos** (diferente de asos/monitor-esocial).

### 13.2 Motor Score (F-40)

`sst_treinamento` alimenta a dimensao de treinamento do motor (validos/total). Treinamento vencido reduz o score.

### 13.3 Matriz de Elegibilidade

`sst_cargo_treinamento` (backend) define os treinamentos obrigatorios por cargo — base para calcular pendencia (nao usada pela tela atual).

---

## 14. Dashboards

### 14.1 Painel de Treinamentos (`treinamentos.html`)

**4 KPIs:** Treinamentos Ativos (total), Vencidos (err), Vencendo em 30 dias (warn), Vencendo em 60 dias (attn). **2 alertas:** vencidos, vencendo 30d. Tabela ordenavel por vencimento.

### 14.2 Matriz por Colaborador

Matriz colaborador × tipos ativos, celula colorida por status. Legenda: verde/amarelo/vermelho/azul/cinza.

### 14.3 Dashboard SST (`sst_dashboard.html` — via RPC)

Bloco treinamentos: total, validos, vencidos, tipos.

---

## 15. Relatorios

### 15.1 Historico por Colaborador

Tabela por colaborador (Treinamento, Data, Carga, Instituicao, Certificado, Vencimento, Status, Dias).

> Sem exportacao CSV/PDF, sem % de conformidade agregada.

---

## 16. Documentos

### 16.1 Certificado de Treinamento

O sistema registra os metadados (tipo, data, carga, instrutor, numero do certificado), **nao o PDF** (so numero; nao ha upload — apesar de `certificado_url` existir na versao versionada).

---

## 17. Evidencias

### 17.1 Metadados do Treinamento

Numero do certificado, instrutor/instituicao, carga horaria e data sao a evidencia da certificacao.

### 17.2 Aprovacao

`sst_treinamento.aprovado` (boolean) existe mas **nao e preenchido pela tela** (sem campo no formulario).

---

## 18. Permissoes

### 18.1 Frontend

Gate de autenticacao apenas, **sem checagem de perfil** (D-07).

### 18.2 RLS

sst_treinamento segue o RLS SST; rh_treinamentos/rh_treinamentos_tipos sao drift com RLS aberta (`FOR ALL USING(true)`).

---

## 19. Auditoria

### 19.1 Timestamps

created_at/updated_at (+ created_by em sst_treinamento). Sem `RHAuth.log` especifico.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Selecionar tipo | `onTipoChange` (preenche validade + vencimento) |
| Alterar data/validade | `recalcVencimento` |
| Salvar treinamento | POST sst_treinamento (lote: N registros) |
| Novo/editar tipo | POST/PATCH rh_treinamentos_tipos |
| Desativar tipo | PATCH ativo=false |
| Buscar colaborador | render historico + matriz |
| Filtrar | client-side (status/tipo) |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (RHAuth) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_auth.js, api.js, ui_components.js, ds.css. Externas: nenhuma.

### 21.3 Dependencias de Dados

rh_colaboradores/rh_funcionarios, sst_treinamento (+ rh_treinamentos), rh_treinamentos_tipos/sst_tipo_treinamento, sst_cargo_treinamento (matriz, nao usada), rh_cargos.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| ASOs | F-43 SST | Mesmo dashboard SST + mesmo padrao read/write |
| EPIs | F-17 SST | NR-06 (uso de EPI) + matriz de cargo |
| CIPA | SST | Treinamento CIPA (12m) |
| Motor Score | F-40 | Dimensao treinamento (validos/total) |
| Acidentes | SST | Treinamento vencido como fator de risco |
| Gestao de Colaboradores | F-10 RH | Cargo (para matriz de obrigatoriedade) |

---

## 23. Melhorias SIGA

### SIGA-01 — Corrigir Read/Write

`treinamentos.html` deve ler e gravar na mesma tabela (hoje le rh_treinamentos, grava sst_treinamento, com colunas distintas) — o cadastro nao reaparece no painel.

### SIGA-02 — Unificar Tabelas de Treinamento

Consolidar sst_treinamento / rh_treinamentos (versionada e prod) / rh_treinamentos_tipos / sst_tipo_treinamento em um modelo canonico.

### SIGA-03 — Usar a Matriz Cargo × Treinamento

Consumir `sst_cargo_treinamento` para detectar pendencia de treinamento obrigatorio por cargo e alertar.

### SIGA-04 — % de Conformidade

Calcular indice de conformidade (treinamentos obrigatorios validos / exigidos) por colaborador/setor.

### SIGA-05 — Corrigir KPI "Ativos"

Contar vigentes (nao o total).

### SIGA-06 — Calculo de Status no Backend

Mover vencimento/status para function/trigger (consistencia com dashboard).

### SIGA-07 — Upload de Certificado + Aprovado

Anexar o PDF (Storage) e preencher `aprovado`.

### SIGA-08 — Restricao de Perfil e Auditoria

Restringir a sst/rh/administrador e registrar log de cadastro.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Tela **le de `rh_treinamentos` mas grava em `sst_treinamento`** | **Grave:** treinamento cadastrado nao reaparece no painel |
| D-02 | Colunas divergem entre leitura/gravacao (colaborador_id/tipo_id/data_vencimento/instituicao vs funcionario_id/tipo_treinamento_id/data_validade/instrutor) | Round-trip quebrado |
| D-03 | `rh_treinamentos` versionada (texto denormalizado) x prod (FK normalizado a rh_treinamentos_tipos) | Schema drift |
| D-04 | `rh_treinamentos_tipos` e drift (nao versionada) | Catalogo fora do controle |
| D-05 | KPI "Ativos" conta o total, nao os vigentes | Metrica enganosa |
| D-06 | `sst_cargo_treinamento` (matriz de obrigatoriedade) nao usada; matriz da tela ignora `obrigatorio` | Sem deteccao de pendencia obrigatoria por cargo |
| D-07 | Sem checagem de perfil na tela | Cadastro/edicao por qualquer autenticado |
| D-08 | `aprovado`, `carga_horaria_minima`, `certificado_url` existem no schema mas sem UI | Campos ociosos |

### 24.2 Multiempresa

1. `empresa_id`/`estabelecimento_id` em sst_treinamento, rh_treinamentos_tipos, sst_cargo_treinamento
2. RLS por empresa
3. Matriz de obrigatoriedade por empresa (cargos/riscos distintos)
4. Catalogo de tipos por empresa (validades conforme convencao/risco)
5. Dashboards e painel filtrados por empresa

---

## 25. Criterios de Aceite

### CA-01 — Cadastrar Treinamento

Cadastrar NR-35 com data → validade 24m auto, vencimento readonly, gravado.

### CA-02 — Vencimento por Tipo

Selecionar tipo altera validade (NR-33 12m, NR-35 24m, Integracao 0/nao vence).

### CA-03 — Painel

Status vigente/vencendo(30)/atencao(60)/vencido/sem; 4 KPIs; alertas.

### CA-04 — Cadastro em Lote

Modo lote com 5 colaboradores → 5 registros do mesmo treinamento.

### CA-05 — Tipos

Criar/editar/desativar tipo; validade 0 = nao vence; obrigatorio.

### CA-06 — Matriz

Por colaborador, celulas coloridas por status do ultimo treinamento de cada tipo.

### CA-07 — Filtros

Filtrar por status e tipo funciona.

### CA-08 — Dashboard SST

sst_dashboard_completo retorna treinamentos total/validos/vencidos/tipos.

---

## 26. Casos de Teste

### CT-01 — NR-35 Vigente

Realizado hoje → vencimento +24m, status vigente.

### CT-02 — NR-33 Vencido

Realizado ha 13 meses (validade 12) → status vencido, alerta vermelho.

### CT-03 — Vencendo 30 dias

Vencimento em 25 dias → status vencendo, alerta.

### CT-04 — Atencao 60 dias

Vencimento em 50 dias → status atencao.

### CT-05 — Integracao (nao vence)

Validade 0 → status sem vencimento, azul.

### CT-06 — Divergencia Tabela

Cadastrar (grava sst_treinamento) → recarregar painel (le rh_treinamentos) → nao aparece (D-01).

### CT-07 — Lote

Turma de brigada, 8 pessoas → 8 registros.

### CT-08 — Matriz

Colaborador sem NR-35 → celula cinza "nao realizado".

### CT-09 — Desativar Tipo

Desativar tipo → some dos dropdowns.

### CT-10 — Filtro por Tipo

Filtrar por NR-12 → so treinamentos daquele tipo.

---

## 27. Casos Extremos

### CE-01 — Validade Zero

Nao gera vencimento; status sem vencimento.

### CE-02 — Sem Data de Realizacao

Validacao exige data.

### CE-03 — KPI Ativos Enganoso

Todos vencidos ainda contam em "Ativos" (D-05).

### CE-04 — Matriz Ignora Obrigatorio

Tipo obrigatorio nao realizado nao gera alerta de pendencia (D-06).

### CE-05 — Aprovado Nao Setado

Registro sempre com aprovado default (sem UI — D-08).

### CE-06 — Carga Horaria Livre

Carga abaixo do minimo da NR nao e validada.

### CE-07 — Colaborador Inativo

Painel usa rh_colaboradores ativos; historico por colaborador filtra por id.

### CE-08 — Tipo sem Validade

validade_meses nula → tratado como nao vence.

---

## 28. Melhorias Futuras

### MF-01 — Agendamento de Reciclagem

Calendario de reciclagem com turmas e notificacao 60/30 dias antes.

### MF-02 — Matriz de Conformidade por Cargo

Usar sst_cargo_treinamento para mostrar quais obrigatorios faltam por colaborador/cargo.

### MF-03 — Integracao com Instituicoes

Importar certificados de escolas/instrutores (API/portal).

### MF-04 — eSocial

Vincular treinamentos ao S-2220 (Monitoramento) quando aplicavel.

### MF-05 — Painel de Conformidade SST

Indice consolidado (ASO + treinamento + EPI) por colaborador/setor.

### MF-06 — Upload e Assinatura

Anexar certificado (Storage) e lista de presenca assinada.

### MF-07 — Alerta Automatico

Notificacao de treinamentos vencidos/vencendo ao SST/gestor.

### MF-08 — Provisao de Treinamentos

Planejamento anual de treinamentos por NR e custo.

---

*Documento gerado em 28/07/2026 — Classic IA — Treinamentos NR*
*Versao 1.0 — Especificacao Funcional Completa F-44*
