# F-45 — Acidentes e Incidentes (CAT)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-45 |
| Titulo | Acidentes e Incidentes — Registro, Investigacao (5 Porques) e Indicadores NBR 14280 |
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

**F-45 — Acidentes e Incidentes** e o modulo de registro e gestao de acidentes de trabalho da Classic Couros: registra eventos (acidente com/sem afastamento, trajeto, incidente/quase-acidente, doenca ocupacional), controla a emissao de CAT, conduz a investigacao de causa raiz (metodo **5 Porques** + acoes corretivas) e calcula os indicadores legais de seguranca (**Taxa de Frequencia** e **Taxa de Gravidade**, NBR 14280), alem do painel "dias sem acidente".

O modulo compreende **1 tela principal** (4 abas), **2 tabelas de acidente** (com naming divergente — drift), e alimenta o dashboard SST, a ficha 360 do colaborador e o monitor eSocial (S-2210, F-43).

**Tela do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/acidentes.html` | Registrar, Historico, Indicadores, Investigacao (4 abas) | 1068 |

**Bibliotecas compartilhadas (RH):** config.js, rh_auth.js (`RHAuth.init/log`), api.js (`API.get/post/patch`), ui_components.js, page_bootstrap.js, shell.js (incluido, nao inicializado), ds.css + **Chart.js 4.4.4** (CDN).

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `MODULO_SST_v2.sql` | `sst_acidente` (canonica SST — **destino do POST/PATCH**), `sst_nao_conformidade` |
| `schema_rh_sst_completo.sql` | `rh_acidentes` (versionada, enxuta) |
| (drift — DDL manual em prod) | `rh_acidentes` prod (rica: 5 porques, acoes corretivas JSONB — **lida** pela tela) |
| `FUNCOES_APLICACAO.sql` | `sst_dashboard_completo()` (bloco acidentes), ficha 360 (bloco B13 + timeline) |

> **Nota critica (pior caso do padrao rh_*/sst_*):** a tela **le de `rh_acidentes`** mas **grava em `sst_acidente`** — e o payload usa **valores que violam os CHECKs** da tabela de destino (`tipo_acidente='com_afastamento'` vs CHECK `tipico/trajeto/doenca_ocupacional`; `status='registrado'` vs CHECK `aberto/investigacao/encerrado`) e **colunas que nao existem** nela (`numero_cat`, `dias_afastados`, `testemunhas`, `medidas_imediatas`, `investigacao_5porques`, `acoes_corretivas`, `licoes_aprendidas`). **O INSERT/UPDATE provavelmente falha por completo em producao.** Ver secao 24 (D-01/D-02).

---

## 2. Problema

A industria de couros tem riscos operacionais relevantes (maquinas, produtos quimicos, material cortante, esforco repetitivo). A legislacao exige registro de acidentes, emissao de CAT (1 dia util; imediata em obito), investigacao e indicadores. Antes do sistema:

1. **Registro informal** — Acidentes e quase-acidentes sem registro estruturado (tipo, gravidade, parte do corpo, agente causador, testemunhas).
2. **CAT sem controle** — Sem rastreio de quais acidentes tiveram CAT emitida e numero.
3. **Sem investigacao padronizada** — Causa raiz e acoes corretivas nao documentadas; acidentes se repetiam.
4. **Sem indicadores** — Taxa de Frequencia/Gravidade (NBR 14280) e "dias sem acidente" nao calculados.

**Solucao:** modulo que (a) registra eventos em 5 tipos com gravidade e CAT, (b) conduz investigacao 5 Porques com acoes corretivas e encerramento automatico, (c) calcula TF/TG por ano com HHT parametrizavel, (d) exibe painel com semaforo de dias sem acidente e 4 graficos.

---

## 3. Processo Operacional

### 3.1 Registro (aba Registrar)

1. Seleciona tipo do evento (com_afastamento / sem_afastamento / trajeto / incidente / doenca_ocupacional) e gravidade (leve/moderado/grave/fatal).
2. Busca o colaborador (autocomplete, ativos), informa data/hora, local/setor, parte do corpo, agente causador, descricao.
3. CAT emitida (sim/nao) — se sim, exibe campo do numero da CAT (nao obrigatorio).
4. Dias afastados, testemunhas, medidas imediatas.
5. Salva → POST `sst_acidente` com `status='registrado'` + `RHAuth.log('acidente_registrado')`.

### 3.2 Historico (aba Historico)

Lista de eventos com filtros (ano, tipo, setor, gravidade — client-side). Clique na linha abre modal de detalhe completo (16 campos, causa raiz e licoes se preenchidas).

### 3.3 Indicadores (aba Indicadores)

Parametros: ano + HHT mensal (default 50.000). Calcula:
- **Taxa de Frequencia** = (acidentes com afastamento × 1.000.000) / HHT anual
- **Taxa de Gravidade** = (dias perdidos × 1.000.000) / HHT anual
- Acidentes c/ afastamento e total de dias perdidos do ano
- 4 graficos Chart.js: por mes (bar), por setor (doughnut), por tipo (bar), por parte do corpo (bar)

### 3.4 Investigacao (aba Investigacao)

1. Seleciona o acidente (exclui incidentes).
2. **5 Porques:** 5 campos encadeados ate a causa raiz.
3. Causa raiz identificada + **acoes corretivas** (lista dinamica: acao, responsavel, prazo, status pendente/em_andamento/concluida) + licoes aprendidas.
4. Salva → PATCH com JSON (`investigacao_5porques`, `acoes_corretivas`) + `RHAuth.log('investigacao_salva')`.
5. **Encerramento automatico:** status vira `concluido` quando ha causa raiz E >= 1 acao corretiva; senao `investigando`.

### 3.5 Consumo pelos Demais Modulos

- **Dashboard SST:** total de acidentes + dias sem acidente (`sst_acidente`).
- **Ficha 360:** bloco B13 (lista de acidentes do colaborador) + timeline.
- **Monitor eSocial (F-43):** acidentes com `cat_emitida=true` geram evento S-2210 (prazo +1 dia).

---

## 4. Usuarios

### 4.1 Perfis de Acesso

Gate de autenticacao apenas (`RHAuth.init`) — **sem checagem de perfil**: qualquer autenticado registra acidente e salva investigacao. RLS documentada permissiva (`FOR ALL USING(true)`).

### 4.2 Auditoria

`RHAuth.log('acidente_registrado', {tipo, colaborador})` e `RHAuth.log('investigacao_salva', {acidente_id})`.

---

## 5. Fluxograma

```
REGISTRO                                   INVESTIGACAO (5 PORQUES)
========                                   ========================
Tipo do evento (5):                        Selecionar acidente (exclui incidente)
  com_afastamento | sem_afastamento           |
  trajeto | incidente | doenca_ocup.          v
   |                                       5 Porques (pq1..pq5 -> causa raiz)
   v                                          |
Gravidade (leve/moderado/grave/fatal)          v
Colaborador + data/hora + local            Causa raiz + Acoes corretivas
Parte do corpo + agente causador             (acao, responsavel, prazo, status)
CAT emitida? -> numero da CAT              Licoes aprendidas
Dias afastados + testemunhas                  |
Medidas imediatas                             v
   |                                       PATCH sst_acidente
   v                                         status: registrado -> investigando
POST sst_acidente (status=registrado)          -> concluido (auto: causa raiz
   ^^^ payload viola CHECKs/colunas               + >=1 acao corretiva)
   da tabela destino (D-01/D-02)
   | painel le rh_acidentes!
   v
HISTORICO + KPIs                           INDICADORES (NBR 14280)
================                           =======================
Total acidentes ano (exclui incidente)     TF = (c/ afastamento x 1M) / HHT
Dias sem acidente (ultimo c/ afastamento)  TG = (dias perdidos x 1M) / HHT
  semaforo: >=90 ok | >=30 warn | err      HHT = mensal (50.000 default) x 12
Filtros ano/tipo/setor/gravidade           4 graficos: mes | setor | tipo | corpo

CONSUMIDORES: dashboard SST | ficha 360 (B13/timeline) | eSocial S-2210 (F-43)
```

---

## 6. Regras de Negocio

### RN-01 — Tipos de Evento (taxonomia da tela)

`com_afastamento`, `sem_afastamento`, `trajeto`, `incidente` (quase-acidente), `doenca_ocupacional` — CHECK da `rh_acidentes` prod. **Divergente** da `sst_acidente` (CHECK `tipico/trajeto/doenca_ocupacional`) — ver D-02.

### RN-02 — Gravidade

leve / moderado / grave / fatal. Badges: leve info, moderado warn, grave e fatal err (fatal sem cor propria — D-07).

### RN-03 — Incidente Excluido de KPIs

`incidente` (quase-acidente) e excluido do KPI "Total acidentes ano" e da lista de investigacao. Reconhece a distincao acidente/incidente, mas **sem piramide de Bird** implementada.

### RN-04 — Dias sem Acidente

Dias desde o ultimo evento `com_afastamento` (todos os anos). Sem registros → fallback **365**. Semaforo: `>= 90` ok (verde), `>= 30` warn, senao err.

### RN-05 — Taxa de Frequencia (NBR 14280)

`TF = (n_acidentes_com_afastamento × 1.000.000) / HHT_anual`. KPI do topo usa HHT hardcoded `50.000 × 12`; aba Indicadores usa o HHT informado (`#ind-hht × 12`).

### RN-06 — Taxa de Gravidade (NBR 14280)

`TG = (dias_perdidos × 1.000.000) / HHT_anual`. Dias perdidos = soma de `dias_afastados` do ano (inclui incidentes — inconsistencia menor).

### RN-07 — CAT Condicional

Campo "Numero da CAT" so aparece quando CAT emitida = Sim — mas **nao e obrigatorio** nem ha validacao de prazo legal (1 dia util / imediato em obito) ou de obrigatoriedade para com_afastamento/fatal (D-08).

### RN-08 — Status do Evento

`registrado` → `investigando` → `concluido` (CHECK da rh_acidentes prod). Transicao automatica: salvar investigacao seta `investigando`; com causa raiz + >= 1 acao corretiva → `concluido`. Sem botao explicito de encerrar. (`sst_acidente` usa `aberto/investigacao/encerrado` — divergente.)

### RN-09 — Investigacao 5 Porques

5 campos encadeados gravados como JSON `{pq1..pq5}` em `investigacao_5porques`. Acoes corretivas como array JSON (`acao`, `responsavel`, `prazo`, `status`) — so linhas com acao preenchida. Prazo das acoes **nao e monitorado** (sem follow-up).

### RN-10 — Validacao de Registro

Obrigatorios: tipo, colaborador, data/hora, gravidade. Demais campos opcionais.

### RN-11 — Ano e Filtros

Selects de ano limitados aos ultimos 6 (atual −5). Filtros do historico sao client-side (ano, tipo, setor por `includes`, gravidade).

### RN-12 — Geracao de S-2210 (externa)

Acidentes com `cat_emitida=true` em `sst_acidente` geram evento eSocial S-2210 com prazo `data_ocorrencia + 1 dia` — feito pelo monitor eSocial (F-43), nao por esta tela.

---

## 7. Campos

### 7.1 Formulario de Registro

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Tipo do Evento | `reg-tipo` | select (5) | Sim |
| Gravidade | `reg-gravidade` | select (4) | Sim |
| Colaborador | `reg-colab-search`/`reg-colab-id` | autocomplete (ativos) | Sim |
| Data/Hora do Evento | `reg-data-hora` | datetime-local | Sim |
| Local / Setor | `reg-local` | text | Nao |
| Parte do Corpo | `reg-parte-corpo` | select (maos/bracos/pernas/pes/cabeca/tronco/olhos/coluna/multiplas) | Nao |
| Agente Causador | `reg-agente` | select (maquina/ferramenta/produto_quimico/queda/esforco_repetitivo/material_cortante/outro) | Nao |
| Descricao | `reg-descricao` | textarea | Nao |
| CAT Emitida | `reg-cat-emitida` | select (Nao default) | Nao |
| Numero da CAT | `reg-numero-cat` | text (condicional) | Nao |
| Dias Afastados | `reg-dias-afastados` | number (min 0) | Nao |
| Testemunhas | `reg-testemunhas` | text (CSV) | Nao |
| Medidas Imediatas | `reg-medidas` | textarea | Nao |

### 7.2 Investigacao

`inv-acidente` (select), `inv-pq1..pq5` (5 Porques), `inv-causa-raiz` (textarea), lista `inv-acoes-list` (acao/responsavel/prazo/status), `inv-licoes` (textarea).

### 7.3 Filtros e Parametros

Historico: `hist-ano`, `hist-tipo`, `hist-setor`, `hist-gravidade`. Indicadores: `ind-ano`, `ind-hht` (number, default 50000).

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `sst_acidente` (canonica SST — **destino do POST/PATCH**) | Acidente | funcionario_id, data_ocorrencia, hora_ocorrencia, tipo_acidente (CHECK tipico/trajeto/doenca_ocupacional), gravidade (CHECK 4), parte_corpo, agente_causador, descricao (NOT NULL), local_ocorrencia, setor_id (FK), testemunha_1/2, cat_emitida, cat_numero, dias_afastamento, data_retorno, acao_imediata, causa_raiz, acao_corretiva, status (CHECK aberto/investigacao/encerrado) |
| `rh_acidentes` (prod/drift — **lida** pela tela) | Acidente (rica) | colaborador_id (FK rh_colaboradores), tipo (CHECK 5 valores da tela), data_hora (timestamptz), local_setor, descricao, parte_corpo, agente_causador, gravidade, cat_emitida, numero_cat, dias_afastados, testemunhas, medidas_imediatas, investigacao_5porques (JSONB), causa_raiz, acoes_corretivas (JSONB), licoes_aprendidas, status (CHECK registrado/investigando/concluido) |
| `rh_acidentes` (versionada) | Acidente (enxuta) | funcionario_id, data_ocorrencia, tipo_acidente (CHECK 3), gravidade, parte_corpo, descricao, cat_emitida, dias_afastamento, acao_corretiva, setor (texto) |
| `sst_nao_conformidade` | NC de SST (relacionada) | tipo (CHECK 9), gravidade, acao_imediata/corretiva, prazo_correcao, status (CHECK 4), CHECK encerrada exige data_encerramento |

---

## 9. RPCs

**Nenhuma RPC chamada pela tela** (PostgREST direto). RPCs consumidoras de `sst_acidente`:

### RPC-01 — `sst_dashboard_completo()`

Bloco `acidentes`: total + `dias_sem_acidente` (= hoje − MAX(data_ocorrencia); fallback 365).

### RPC-02 — Ficha 360 (bloco B13)

Total + lista de acidentes do colaborador (tipo, data, gravidade, dias, CAT) + entrada na timeline.

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `sst_dashboard_completo()` | KPIs SST (bloco acidentes) |
| Ficha 360 | Acidentes por colaborador (B13/timeline) |
| Triggers de `updated_at` | Padrao SST |

Sem function de TF/TG (calculadas no frontend) e sem trigger de status/CAT.

---

## 11. Triggers

Triggers de `updated_at` em sst_acidente (padrao). CHECKs de tipo/gravidade/status nas duas tabelas (divergentes entre si). Sem trigger de prazo de CAT nem de acoes corretivas.

---

## 12. APIs (Chamadas Frontend)

### 12.1 acidentes.html (`API`)

- **GET:** rh_colaboradores (ativos, id/nome/setor/cargo), **rh_acidentes** (`select=*,rh_colaboradores(nome,setor,cargo)&order=data_hora.desc`)
- **POST:** **sst_acidente** (novo registro, status='registrado')
- **PATCH:** **sst_acidente**`?id=eq.{id}` (investigacao: 5porques/causa_raiz/acoes/licoes/status)

Nenhuma RPC, nenhuma view.

### 12.2 Bibliotecas Externas

Chart.js 4.4.4 (CDN jsdelivr) — 4 graficos na aba Indicadores.

---

## 13. Integracoes

### 13.1 eSocial S-2210 (via F-43)

Monitor eSocial gera S-2210 de `sst_acidente` com `cat_emitida=true` (prazo +1 dia). Esta tela nao gera eventos.

### 13.2 Motor de Decisoes (F-40)

Afastamentos por acidente de trabalho aparecem no motor (`tipo_afastamento='acidente_trabalho'` — exclusao de contexto no absenteismo).

### 13.3 Ficha 360 / Dashboard SST

Consomem `sst_acidente` (B13, timeline, KPIs).

### 13.4 Nao Conformidades SST

`sst_nao_conformidade` (atos/condicoes inseguras) complementa a prevencao — sem tela dedicada mapeada.

---

## 14. Dashboards

### 14.1 KPIs do Topo (`acidentes.html`)

**4 KPIs:** Total acidentes ano (exclui incidentes), Taxa de frequencia (HHT hardcoded 50.000×12), Taxa de gravidade, Dias sem acidente (semaforo 90/30, fallback 365).

### 14.2 Indicadores (aba)

TF/TG com HHT parametrizavel + acidentes c/ afastamento + dias perdidos. **4 graficos Chart.js:** Acidentes por Mes (bar), por Setor (doughnut), por Tipo (bar), por Parte do Corpo (bar).

### 14.3 Dashboard SST (via RPC)

total + dias_sem_acidente.

---

## 15. Relatorios

### 15.1 Historico Filtravel

Tabela (Data, Colaborador, Tipo, Setor, Gravidade, Dias Afast., CAT, Status) + modal de detalhe (16 campos).

> Sem exportacao CSV/PDF e sem impressao de CAT.

---

## 16. Documentos

### 16.1 CAT (Comunicacao de Acidente de Trabalho)

Registrada apenas como flag + numero — o documento e emitido externamente (gov.br). Sem geracao/impressao pelo sistema.

### 16.2 Relatorio de Investigacao

5 Porques + causa raiz + acoes + licoes ficam no registro (JSON) — sem documento formal gerado.

---

## 17. Evidencias

### 17.1 Registro do Evento

Data/hora, local, testemunhas, medidas imediatas, descricao — evidencia primaria do acidente.

### 17.2 Investigacao

JSON dos 5 Porques + acoes corretivas com responsavel/prazo — evidencia da tratativa.

### 17.3 Auditoria

`RHAuth.log` (registro e investigacao) + timestamps.

---

## 18. Permissoes

### 18.1 Frontend

Gate de autenticacao apenas; **sem checagem de perfil** (D-06).

### 18.2 RLS

`rh_acidentes` prod: policy permissiva total (`USING(true) WITH CHECK(true)`). `sst_acidente` segue RLS SST.

---

## 19. Auditoria

`RHAuth.log('acidente_registrado')` e `('investigacao_salva')`; created_by em sst_acidente; timestamps.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Registrar | POST sst_acidente + log |
| CAT = Sim | Exibe campo numero da CAT |
| Trocar aba | loadHistorico / calcularIndicadores / loadInvestigacaoSelect |
| Filtrar historico | client-side |
| Calcular indicadores | TF/TG + 4 graficos (destroy + recreate) |
| Carregar investigacao | Preenche 5 Porques/acoes existentes |
| Salvar investigacao | PATCH + status auto (investigando/concluido) + log |
| Abrir detalhe | Modal com 16 campos |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (RHAuth) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_auth.js, api.js, ui_components.js, page_bootstrap.js, ds.css. Externas: Chart.js 4.4.4.

### 21.3 Dependencias de Dados

rh_colaboradores, rh_acidentes (leitura), sst_acidente (escrita + consumidores), rh_setores.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Monitor eSocial | F-43 SST | S-2210 gerado de sst_acidente (cat_emitida) |
| ASOs | F-43 SST | ASO de retorno pos-afastamento |
| Absenteismo | F-14 RH | Afastamento por acidente (INSS) |
| Motor de Decisoes | F-40 | Exclusao de contexto acidente_trabalho |
| EPIs / Treinamentos | F-17/F-44 SST | Prevencao (NC de uso de EPI, treinamento vencido) |
| Ficha 360 | F-10 RH | Bloco B13 + timeline |
| CIPA | SST | Investigacao e acoes preventivas |

---

## 23. Melhorias SIGA

### SIGA-01 — Corrigir Read/Write e Payload

Ler e gravar na mesma tabela; alinhar valores de tipo/status e nomes de coluna — hoje o INSERT em `sst_acidente` provavelmente **falha** (CHECK violado + colunas inexistentes).

### SIGA-02 — Unificar Tabelas de Acidente

Consolidar rh_acidentes (prod rica) e sst_acidente (canonica) em modelo unico com 5 tipos + investigacao JSONB.

### SIGA-03 — Controle Legal da CAT

Validar CAT obrigatoria para com_afastamento/fatal, prazo de 1 dia util (imediato em obito), KPI de CATs pendentes e alerta.

### SIGA-04 — Follow-up de Acoes Corretivas

Monitorar prazos das acoes (pendente/em_andamento/concluida) com alertas e vinculo a plano de acao (acao_plano/alertas_rh).

### SIGA-05 — Piramide de Bird

Indicador incidente:acidente para prevencao proativa (quase-acidentes ja sao registrados).

### SIGA-06 — HHT Real

Substituir HHT hardcoded (50.000) por calculo a partir do ponto (rh_ponto) ou parametro por empresa.

### SIGA-07 — Data de Retorno e Integracao com Afastamento

Usar `data_retorno` (existe em sst_acidente, sem UI) e integrar com o processo de afastamento/retorno (rh_proc_afastamento).

### SIGA-08 — Perfil, Exportacao e CAT Impressa

Restringir a sst/rh/admin; exportar CSV/PDF; pre-preencher formulario CAT.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Tela **le de `rh_acidentes` e grava em `sst_acidente`** | Registro nunca aparece na lista (mesmo padrao F-43/F-44) |
| D-02 | Payload **viola CHECKs** de sst_acidente (`tipo_acidente='com_afastamento'` ∉ tipico/trajeto/doenca; `status='registrado'` ∉ aberto/investigacao/encerrado) e usa **colunas inexistentes** (numero_cat, dias_afastados, testemunhas, medidas_imediatas, investigacao_5porques, acoes_corretivas, licoes_aprendidas) | **Gravissimo:** INSERT/UPDATE provavelmente falham por completo — registro de acidentes possivelmente inoperante |
| D-03 | 3 versoes de rh_acidentes/sst_acidente (versionada enxuta, prod rica drift, sst canonica) | Fragmentacao de schema |
| D-04 | HHT hardcoded (50.000×12) no KPI do topo | TF/TG do painel irreais se HHT real difere |
| D-05 | Dias perdidos somam incidentes; dias sem acidente so considera com_afastamento | Inconsistencia entre metricas |
| D-06 | Sem checagem de perfil; RLS permissiva total | Qualquer autenticado registra/edita |
| D-07 | Gravidade fatal sem destaque proprio (mesma cor de grave); CAT nao obrigatoria nem com prazo | Controle legal fraco |
| D-08 | Sem eSocial S-2210 na tela, sem exportacao, sem data_retorno na UI, prazos de acoes nao monitorados | Lacunas funcionais |

### 24.2 Multiempresa

1. `empresa_id`/`estabelecimento_id` em sst_acidente
2. RLS por empresa
3. HHT e indicadores por empresa/estabelecimento
4. CAT por CNPJ (empregador correto no eSocial)
5. Dashboards filtrados por empresa

---

## 25. Criterios de Aceite

### CA-01 — Registrar Acidente

Tipo + colaborador + data/hora + gravidade obrigatorios; registro gravado e visivel no historico (requer SIGA-01).

### CA-02 — CAT Condicional

CAT=Sim exibe campo do numero.

### CA-03 — KPIs

Total do ano exclui incidentes; dias sem acidente com semaforo 90/30; TF/TG conforme NBR 14280.

### CA-04 — Historico e Filtros

Filtros ano/tipo/setor/gravidade; modal de detalhe completo.

### CA-05 — Indicadores

HHT parametrizavel; TF/TG recalculadas; 4 graficos renderizados.

### CA-06 — Investigacao

5 Porques + causa raiz + acoes (responsavel/prazo/status) + licoes; PATCH persistido.

### CA-07 — Encerramento Automatico

Causa raiz + >=1 acao → status concluido; senao investigando.

### CA-08 — Consumidores

Dashboard SST, ficha 360 e monitor eSocial leem os acidentes gravados.

---

## 26. Casos de Teste

### CT-01 — Acidente com Afastamento

Registrar com_afastamento grave, 5 dias → KPIs atualizam; dias sem acidente zera.

### CT-02 — Incidente

Registrar incidente → nao conta no total do ano nem aparece na investigacao.

### CT-03 — CAT

CAT=Sim + numero → badge Sim no historico; gera S-2210 no monitor (F-43).

### CT-04 — TF/TG

Ano com 2 c/ afastamento e 30 dias perdidos, HHT 50.000 → TF=3.33, TG=50.00.

### CT-05 — Semaforo

Ultimo acidente ha 100 dias → card ok; ha 45 → warn; ha 10 → err.

### CT-06 — Investigacao Completa

5 Porques + causa + 2 acoes → status concluido automatico.

### CT-07 — Investigacao Parcial

So 5 Porques sem acoes → status investigando.

### CT-08 — Divergencia (bug atual)

Registrar acidente → POST sst_acidente falha (CHECK/coluna) ou grava invisivel; historico (rh_acidentes) nao mostra (D-01/D-02).

### CT-09 — Filtro Setor

Filtrar "Ribeira" → so eventos daquele local_setor.

### CT-10 — Sem Registros

Base vazia → dias sem acidente = 365 (fallback).

---

## 27. Casos Extremos

### CE-01 — Fatal

Gravidade fatal → badge igual a grave (sem destaque — D-07); CAT nao exigida.

### CE-02 — CAT sem Numero

CAT=Sim sem numero → aceito (sem validacao).

### CE-03 — HHT Divergente

KPI topo (50.000 fixo) vs Indicadores (HHT informado) → TF/TG diferentes na mesma tela.

### CE-04 — Acoes sem Descricao

Linhas de acao vazias sao descartadas ao salvar.

### CE-05 — Prazo de Acao Vencido

Nao ha alerta (prazo gravado, nunca monitorado).

### CE-06 — Ano Antigo

Evento com mais de 6 anos → fora dos selects de ano.

### CE-07 — Colaborador Inativo

Nao aparece no autocomplete (so ativos); acidentes historicos permanecem.

### CE-08 — Dias Perdidos de Incidente

Somados na TG (inconsistencia D-05).

---

## 28. Melhorias Futuras

### MF-01 — CAT Digital

Pre-preenchimento e emissao da CAT (integracao gov.br/eSocial S-2210 transacional).

### MF-02 — Piramide de Bird

Indicador quase-acidente:acidente e campanhas de registro de incidentes.

### MF-03 — Analise de Tendencia

TF/TG mensais com meta e comparativo YoY; heatmap setor × parte do corpo.

### MF-04 — Workflow de Investigacao

Prazos por gravidade, aprovacao do SESMT, follow-up de acoes com notificacao.

### MF-05 — Arvore de Causas

Metodos adicionais (Ishikawa, arvore de falhas) alem dos 5 Porques.

### MF-06 — Integracao Afastamento

Acidente com afastamento abre processo INSS (rh_proc_afastamento) e controla retorno + ASO de retorno.

### MF-07 — Custo do Acidente

Estimativa de custo (dias perdidos, FAP/SAT, indenizacoes) por evento/ano.

### MF-08 — App de Registro Rapido

Registro mobile no chao de fabrica com foto do local.

---

*Documento gerado em 28/07/2026 — Classic IA — Acidentes e Incidentes*
*Versao 1.0 — Especificacao Funcional Completa F-45*
