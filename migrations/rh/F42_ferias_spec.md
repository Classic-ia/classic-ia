# F-42 — Ferias

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-42 |
| Titulo | Controle de Ferias |
| Modulo | RH (Recursos Humanos — Pessoas) |
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

**F-42 — Ferias** e o modulo de controle de ferias dos colaboradores da Classic Couros: acompanha o periodo aquisitivo de cada funcionario ativo e sinaliza ferias **vencidas** (que ultrapassaram o periodo concessivo, gerando risco de dobra), **pendentes** e **proximas a vencer** (60 dias). No estado atual, e um **dashboard exclusivamente de leitura**, dirigido por uma unica RPC (`ferias_dashboard`), sobre a tabela `rh_ferias`.

O modulo compreende **1 tela**, **1 tabela** (`rh_ferias`), **1 staging** (`stg_convenia_ferias`) e **1 RPC** (`ferias_dashboard`), no projeto Supabase RH.

**Tela do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/ferias.html` | Dashboard de ferias vencidas, pendentes e proximas a vencer (read-only) | 479 |

**Bibliotecas compartilhadas (RH):** config.js, rh_auth.js, api.js (`API.rpc/fmtData/esc`), ui_components.js, page_bootstrap.js, shell.js (incluido mas nao inicializado), ds.css. **Sem Chart.js.**

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `TABELAS_RH_CORE.sql` | `rh_ferias`, `rh_proc_afastamento` (workflow tipo=ferias) |
| `TABELAS_ETL.sql` | `stg_convenia_ferias` (staging Convenia) |
| `FUNCOES_APLICACAO.sql` | RPC `ferias_dashboard()` |

> **Nota de escopo:** a tabela `rh_ferias` tem estrutura completa (periodo aquisitivo, dias de direito/gozados/vendidos, abono pecuniario, datas de gozo/retorno), mas **nao ha frontend para criar/programar/aprovar ferias** — a tela so consome o dashboard. A alimentacao vem de importacao (Convenia) ou processo de afastamento. Ver secao 24.

---

## 2. Problema

A legislacao trabalhista (CLT art. 129-149) exige que ferias sejam concedidas dentro do **periodo concessivo** de 12 meses apos o fechamento do periodo aquisitivo; o descumprimento gera **pagamento em dobro**. Com ~133 funcionarios ativos e alta rotatividade, a Classic Couros precisava de:

1. **Visibilidade de vencimento** — Saber quem esta com ferias vencidas (risco de dobra) e quem esta proximo do limite.
2. **Priorizacao** — Ordenar por urgencia (dias de atraso / dias restantes) para agendar a escala de ferias.
3. **Controle de gozo** — Acompanhar dias gozados vs direito, vendas (abono) e status (programada, em gozo, gozada).

**Solucao (estado atual):** dashboard que classifica cada colaborador ativo em 5 status e destaca vencidas (acao imediata) e proximas a vencer (60 dias). O agendamento/aprovacao ainda ocorre fora do sistema.

---

## 3. Processo Operacional

### 3.1 Ciclo de Ferias (conceitual)

```
Admissao → Periodo Aquisitivo (12 meses) → adquire 30 dias de direito
        → Periodo Concessivo (12 meses seguintes) → deve conceder as ferias
        → se nao concedidas ao fim do concessivo → VENCIDA (dobra)
```

`rh_ferias` guarda o periodo aquisitivo (inicio/fim), os dias de direito (30), dias gozados, dias vendidos (abono pecuniario ate 1/3), as datas de gozo (inicio/fim/retorno) e o status.

### 3.2 Dashboard (`ferias.html`)

1. Ao carregar, chama `API.rpc('ferias_dashboard', {})`.
2. A RPC retorna: resumo (contagem por status), lista de **vencidas** (com dias de atraso) e lista de **proximas_vencer** (pendentes com periodo_aquisitivo_fim ate hoje+60 dias) — sempre so de funcionarios ativos.
3. Renderiza 5 KPIs, a secao "Ferias Vencidas — Acao Imediata" (oculta se vazia) e a secao "Proximas a Vencer (60 dias)".
4. Botao Imprimir (`window.print()`).

### 3.3 Alimentacao de `rh_ferias` (fora da tela)

- **Importacao Convenia:** `stg_convenia_ferias` (staging) → ETL → `rh_ferias` (endpoint existe mas sync pendente — ver Pendencias do projeto).
- **Processo de afastamento:** `rh_proc_afastamento` (tipo=ferias) via workflow muda `rh_funcionarios.status='ferias'` (`rh_executar_afastamento`).
- **Manual/SQL:** insercao direta enquanto nao ha tela de programacao.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Acesso |
|--------|--------|
| `administrador` | Dashboard completo |
| `rh` | Dashboard completo |
| `gestor` | Dashboard completo |
| `visualizador` | Dashboard completo |

`RHAuth.init({perfisPermitidos: ['administrador','rh','gestor','visualizador']})` — todos os perfis autenticados veem o dashboard (read-only). **Sem gating de UI por perfil** e sem `Shell.init`/`API.setPerfil` nesta tela.

### 4.2 LGPD

Aviso na tela: dados pessoais protegidos pela LGPD (Lei 13.709/2018), uso restrito ao RH, acesso registrado em log de auditoria.

---

## 5. Fluxograma

```
CARREGAR DASHBOARD
==================
ferias.html
   | API.rpc('ferias_dashboard', {})
   v
RPC (SECURITY DEFINER) — so funcionarios ativos:
   resumo: vencidas | pendentes | programadas | em_gozo | gozadas
   vencidas[]:        status='vencida'  (dias_atraso = hoje - periodo_fim)
   proximas_vencer[]: status='pendente' AND periodo_fim <= hoje+60
   v
RENDER
   5 KPIs (por status)
   Secao Vencidas (row-danger, badge dias_atraso)  <- oculta se vazia
   Secao Proximas (row-warning, badge dias_restantes)
      <=15 danger | <=30 warning | >30 info
   Imprimir

ALIMENTACAO (fora da tela)
==========================
Convenia -> stg_convenia_ferias -> [ETL pendente] -> rh_ferias
Afastamento (rh_proc_afastamento tipo=ferias) -> rh_funcionarios.status='ferias'
Manual/SQL -> rh_ferias
```

---

## 6. Regras de Negocio

### RN-01 — Periodo Aquisitivo e Direito

Cada colaborador acumula um periodo aquisitivo de 12 meses (`periodo_aquisitivo_inicio` → `periodo_aquisitivo_fim`) que gera `dias_direito` (default 30).

### RN-02 — Status de Ferias (5 valores)

`vencida`, `pendente`, `programada`, `em_gozo`, `gozada` (usados pela RPC). **A tabela `rh_ferias.status` nao tem CHECK constraint** (default `pendente`) — risco de integridade (D-02).

### RN-03 — Vencida (periodo concessivo estourado)

Status `vencida` = ferias que ultrapassaram o periodo concessivo (12 meses apos o periodo aquisitivo) sem serem gozadas. `dias_atraso = CURRENT_DATE - periodo_aquisitivo_fim`. Risco de pagamento em dobro (CLT art. 137). A marcacao do status e feita fora da RPC (a RPC apenas filtra por `status='vencida'`).

### RN-04 — Proximas a Vencer (60 dias)

Ferias `pendente` com `periodo_aquisitivo_fim <= CURRENT_DATE + 60` entram na lista de proximas a vencer. `dias_restantes = periodo_aquisitivo_fim - CURRENT_DATE`.

### RN-05 — Escala de Urgencia (badge dias restantes)

`dias_restantes <= 15` → danger; `<= 30` → warning; `> 30` → info. Unica regra calculada no frontend.

### RN-06 — Escopo: Somente Ativos

Todas as contagens e listas do dashboard consideram apenas `rh_funcionarios.status = 'ativo'`. Desligados/afastados nao aparecem.

### RN-07 — Dias Gozados vs Direito

`dias_gozados` acumula o gozo. `dias_gozados < dias_direito` indica saldo de ferias. (Exibido no dashboard; nao calculado no front.)

### RN-08 — Abono Pecuniario (venda de ate 1/3)

`abono_pecuniario` (boolean) e `dias_vendidos` (int) registram a conversao de ate 1/3 das ferias em dinheiro (CLT art. 143). **Nao exibidos no dashboard atual** — campos existem na tabela mas nao ha UI. (D-04)

### RN-09 — Datas de Gozo

`data_inicio`, `data_fim`, `data_retorno` registram o gozo efetivo. Alimentadas externamente (Convenia/afastamento/manual).

### RN-10 — Integracao com Status do Funcionario

Quando o colaborador entra em ferias, o processo `rh_proc_afastamento` (tipo=ferias) via `rh_executar_afastamento` muda `rh_funcionarios.status='ferias'`. `rh_ferias` e `rh_proc_afastamento` sao estruturas separadas (potencial duplicidade — D-05).

---

## 7. Campos

### 7.1 Dashboard (`ferias.html`) — somente exibicao

| KPI | ID | Fonte |
|-----|----|----|
| Vencidas | `kpi-vencidas` | resumo.vencidas |
| Pendentes | `kpi-pendentes` | resumo.pendentes |
| Programadas | `kpi-programadas` | resumo.programadas |
| Em Gozo | `kpi-emgozo` | resumo.em_gozo |
| Gozadas | `kpi-gozadas` | resumo.gozadas |

Campos de linha (tabelas): `nome`, `setor`, `periodo_fim`, `dias_atraso` (vencidas), `dias_restantes` (proximas), `dias_gozados`.

### 7.2 Tabela `rh_ferias` (backend)

| Campo | Tipo | Descricao |
|-------|------|-----------|
| funcionario_id | uuid | FK funcionario |
| periodo_aquisitivo_inicio/fim | date | Periodo aquisitivo |
| dias_direito | int (30) | Dias de direito |
| dias_gozados | int | Dias ja gozados |
| dias_vendidos | int | Abono (venda ate 1/3) |
| data_inicio/fim/retorno | date | Gozo efetivo |
| status | text (pendente) | vencida/pendente/programada/em_gozo/gozada |
| abono_pecuniario | boolean | Vendeu ferias? |
| observacoes | text | Livre |

**Sem formulario de programacao no frontend** — os campos acima nao tem UI de edicao.

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rh_ferias` | Controle de ferias por colaborador/periodo | funcionario_id, periodo_aquisitivo_inicio/fim, dias_direito (30), dias_gozados, dias_vendidos, data_inicio/fim/retorno, status, abono_pecuniario, observacoes |
| `rh_proc_afastamento` | Detalhe de afastamento (inclui tipo=ferias) | processo_id, tipo_afastamento, data_inicio, data_previsao_retorno, data_retorno_efetivo, dias_previstos, observacoes |
| `stg_convenia_ferias` | Staging de ferias do Convenia | id_origem, origem_sistema (convenia), payload_json, hash_registro, importacao_id, status_processamento |

---

## 9. RPCs

### RPC-01 — `ferias_dashboard()`

**Arquivo:** `FUNCOES_APLICACAO.sql` | **Modo:** SECURITY DEFINER. Sem parametros. Retorna JSONB:
- `resumo`: contagem por status (vencidas, pendentes, programadas, em_gozo, gozadas) — join `rh_funcionarios` status=ativo
- `vencidas[]`: `{nome, setor, periodo_fim, dias_atraso, dias_gozados}` — status=vencida, ativos, ordenado por periodo_aquisitivo_fim
- `proximas_vencer[]`: `{nome, setor, periodo_fim, dias_restantes, dias_gozados}` — status=pendente, ativos, periodo_aquisitivo_fim <= hoje+60, ordenado

**Unica fonte de dados da tela.** Nao ha GET/POST/PATCH/DELETE direto.

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `ferias_dashboard()` | Monta o dashboard (resumo + vencidas + proximas) |
| `rh_executar_afastamento` | Muda status para ferias (workflow, tipo=ferias) |

Nao ha function de calculo de periodo aquisitivo/vencimento no repo — o status `vencida` e definido externamente (importacao ou processo), nao por um job de vencimento versionado (D-03).

---

## 11. Triggers

Trigger de `updated_at` em `rh_ferias` (padrao RH). **Sem trigger de vencimento automatico** (nao ha job que promova `pendente → vencida` ao passar do concessivo).

---

## 12. APIs (Chamadas Frontend)

### 12.1 RPC

`API.rpc('ferias_dashboard', {})` — desembrulha `{ok, data, error}` (array[0]).

### 12.2 Sem REST direto / sem bibliotecas externas

Nenhum endpoint PostgREST direto, nenhuma view, nenhum Chart.js.

---

## 13. Integracoes

### 13.1 Convenia (Folha/RH)

`stg_convenia_ferias` recebe ferias do Convenia (payload_json + hash). **ETL para `rh_ferias` pendente** (Pendencia do projeto: "Convenia ferias/afastamentos — endpoints existem mas ETL nao sync").

### 13.2 Processo de Afastamento

`rh_proc_afastamento` (tipo=ferias) → `rh_executar_afastamento` muda status do funcionario para `ferias`.

### 13.3 Beneficios (F-41)

Ferias afetam a apuracao: VA nao e pago em ferias; deslocamento suspenso; assiduidade proporcional. `rh_apuracao_mensal` usa `em_ferias`/`dias_ferias`.

---

## 14. Dashboards

### 14.1 Dashboard de Ferias (`ferias.html`)

**5 KPIs:** Vencidas (red), Pendentes (yellow), Programadas (blue), Em Gozo (green), Gozadas (gray).

**Secao "Ferias Vencidas — Acao Imediata"** (`row-danger`): Nome (link ficha), Setor, Periodo Fim, Dias Atraso (badge danger), Dias Gozados. Oculta se vazia ("Tudo em dia!").

**Secao "Proximas a Vencer (60 dias)"** (`row-warning`): Nome, Setor, Periodo Fim, Dias Restantes (badge escalonado), Dias Gozados.

Botao Imprimir. Sem graficos, sem filtros, sem abas.

---

## 15. Relatorios

### 15.1 Impressao do Dashboard

`window.print()` — imprime o dashboard atual (KPIs + 2 secoes).

> **Lacuna:** nao ha exportacao CSV nem escala de ferias formal (aviso de ferias, recibo). Ver melhorias.

---

## 16. Documentos

### 16.1 Aviso de Ferias / Escala (nao implementado)

O modulo nao gera aviso de ferias (30 dias de antecedencia), recibo de ferias nem escala anual — apenas o painel de acompanhamento.

---

## 17. Evidencias

### 17.1 Log de Auditoria

Aviso LGPD indica que o acesso e registrado em log. `rh_ferias` guarda datas efetivas de gozo/retorno como evidencia do cumprimento.

---

## 18. Permissoes

### 18.1 Frontend

Todos os perfis autenticados (administrador, rh, gestor, visualizador) veem o dashboard. Sem escrita (read-only), sem gating por perfil.

### 18.2 RLS

`rh_ferias` segue o RLS do core RH. RPC `ferias_dashboard` e SECURITY DEFINER (dados de todos os ativos).

---

## 19. Auditoria

### 19.1 Timestamps

`rh_ferias.created_at/updated_at`. `rh_proc_afastamento` registra data_retorno_efetivo.

### 19.2 Staging

`stg_convenia_ferias` (status_processamento, erro_detalhe, hash) — trilha da importacao.

> **Lacuna:** o dashboard nao grava log de acesso proprio (apesar do aviso LGPD).

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Carregar tela | `loadFerias()` → `API.rpc('ferias_dashboard')` |
| Erro | `showErrorAll()` (KPIs = '--') |
| Vazio | `showEmptyAll()` (KPIs = 0) |
| Imprimir | `window.print()` |
| Entrar em ferias (externo) | `rh_executar_afastamento` (tipo=ferias) |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (RHAuth) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_auth.js, api.js, ui_components.js, page_bootstrap.js, shell.js, ds.css. Externas: nenhuma.

### 21.3 Dependencias de Dados

rh_ferias, rh_funcionarios (status ativo), rh_setores, stg_convenia_ferias, rh_proc_afastamento.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Processos RH (afastamento) | RH | Ferias como tipo de afastamento (rh_proc_afastamento) |
| Beneficios | F-41 RH | VA/deslocamento/assiduidade afetados por ferias |
| Absenteismo | F-14 RH | Ferias distintas de faltas/atestados |
| Gestao de Colaboradores | F-10 RH | Cadastro e status do funcionario |
| Dashboard do Gestor | RH | gestor_dashboard tambem exibe ferias da equipe |
| ETL Convenia | ETL | stg_convenia_ferias → rh_ferias |

---

## 23. Melhorias SIGA

### SIGA-01 — Tela de Programacao de Ferias

Criar UI de agendamento/aprovacao (formulario sobre `rh_ferias`): periodo aquisitivo, data de inicio, dias, abono, aprovacao — hoje inexistente.

### SIGA-02 — Job de Vencimento Automatico

Function/cron que promove `pendente → vencida` ao ultrapassar o periodo concessivo, em vez de depender de marcacao externa.

### SIGA-03 — CHECK Constraint no Status

Adicionar `CHECK (status IN ('vencida','pendente','programada','em_gozo','gozada'))` em `rh_ferias`.

### SIGA-04 — Wire do ETL Convenia

Ativar o sync `stg_convenia_ferias → rh_ferias` (hoje pendente).

### SIGA-05 — Superficie de Abono e Dias Vendidos

Exibir/gerir `abono_pecuniario`/`dias_vendidos` no dashboard e no relatorio.

### SIGA-06 — Unificar com Afastamento

Consolidar `rh_ferias` e `rh_proc_afastamento` (tipo=ferias) para evitar duplicidade de estado.

### SIGA-07 — Documentos Legais

Gerar aviso de ferias (30 dias), recibo e escala anual.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Modulo read-only: nao ha UI para criar/programar/aprovar ferias, apesar de `rh_ferias` completa | Gestao de ferias fora do sistema (manual/importacao) |
| D-02 | `rh_ferias.status` sem CHECK constraint | Risco de status invalido/inconsistente |
| D-03 | Status `vencida` definido externamente (sem job de vencimento versionado) | Vencidas podem nao ser detectadas automaticamente |
| D-04 | `abono_pecuniario`/`dias_vendidos` na tabela mas sem UI | Venda de ferias nao gerenciavel pela tela |
| D-05 | `rh_ferias` e `rh_proc_afastamento` (tipo=ferias) coexistem | Estado de ferias potencialmente duplicado |
| D-06 | ETL Convenia → rh_ferias pendente (staging existe, sync nao) | Dados de ferias podem estar desatualizados |
| D-07 | Sem log de acesso proprio apesar do aviso LGPD | Rastreabilidade incompleta |

### 24.2 Multiempresa

1. `empresa_id` em `rh_ferias`
2. RLS por empresa
3. Dashboard filtrado por empresa/estabelecimento
4. Calendario/escala de ferias por empresa (feriados, convencao)
5. Regras de concessivo/abono conforme convencao coletiva da empresa

---

## 25. Criterios de Aceite

### CA-01 — Dashboard Carrega

`ferias_dashboard` retorna resumo + vencidas + proximas; 5 KPIs preenchidos.

### CA-02 — Vencidas

Colaborador ativo com status=vencida aparece na secao de acao imediata com dias de atraso.

### CA-03 — Proximas a Vencer

Pendente com periodo_aquisitivo_fim <= hoje+60 aparece em proximas, com badge escalonado.

### CA-04 — Escopo Ativos

Apenas funcionarios ativos sao contados/listados.

### CA-05 — Secao Vazia

Sem vencidas → secao oculta com "Tudo em dia!".

### CA-06 — Escala de Cor

dias_restantes <=15 danger, <=30 warning, >30 info.

### CA-07 — Impressao

Botao Imprimir gera visao imprimivel.

### CA-08 — Acesso por Perfil

Todos os perfis autenticados acessam; nao autenticado e barrado por RHAuth.

---

## 26. Casos de Teste

### CT-01 — Ferias Vencida

status=vencida, periodo_fim ha 40 dias → aparece em vencidas, dias_atraso=40, badge danger.

### CT-02 — Proxima a Vencer (15 dias)

pendente, periodo_fim em 15 dias → proximas, dias_restantes=15, badge danger.

### CT-03 — Proxima a Vencer (45 dias)

pendente, periodo_fim em 45 dias → proximas, badge info.

### CT-04 — Fora da Janela

pendente, periodo_fim em 90 dias → nao aparece em proximas.

### CT-05 — Colaborador Desligado

Desligado com ferias vencidas → nao aparece (so ativos).

### CT-06 — Em Gozo

status=em_gozo → conta no KPI Em Gozo; nao aparece nas listas de vencidas/proximas.

### CT-07 — Nenhuma Vencida

Todas em dia → secao vencidas oculta.

### CT-08 — RPC Erro

Falha da RPC → KPIs exibem '--'.

---

## 27. Casos Extremos

### CE-01 — rh_ferias Vazia

Dashboard mostra 0 em todos os KPIs, secoes vazias.

### CE-02 — Status Invalido

status fora dos 5 valores (sem CHECK) → nao entra em nenhuma contagem/lista.

### CE-03 — periodo_fim Nulo

Registro sem periodo_aquisitivo_fim → nao entra na janela de 60 dias.

### CE-04 — Funcionario sem Setor

setor nulo → coluna Setor vazia (LEFT JOIN).

### CE-05 — Dias Atraso Negativo

Se status=vencida mas periodo_fim futuro → dias_atraso negativo (dado inconsistente, sem job de vencimento).

### CE-06 — Import Convenia Duplicado

hash_registro repetido em staging → dedup no ETL (quando ativo).

### CE-07 — Ferias e Afastamento Simultaneos

Registro em rh_ferias + rh_proc_afastamento tipo=ferias → estado duplicado (D-05).

### CE-08 — Muitos Vencidos

Lista longa de vencidas → renderiza tudo (sem paginacao).

---

## 28. Melhorias Futuras

### MF-01 — Escala Anual de Ferias

Planejamento visual (calendario) da escala por setor, evitando concentracao e cobrindo o concessivo.

### MF-02 — Aviso Automatico

Notificacao (email/WhatsApp) 60/30 dias antes do vencimento ao RH e ao gestor.

### MF-03 — Solicitacao pelo Colaborador

Portal para o colaborador solicitar ferias, com aprovacao do gestor.

### MF-04 — Calculo Automatico de Periodo

Function que deriva periodo aquisitivo/concessivo a partir da data de admissao e mantem o status atualizado.

### MF-05 — Integracao eSocial (S-2230)

Eventos de afastamento por ferias no eSocial.

### MF-06 — Recibo e Documentos

Geracao de aviso, recibo de ferias e calculo do valor (1/3 constitucional + abono).

### MF-07 — Dashboard de Provisao

Provisao contabil de ferias (passivo) por competencia.

### MF-08 — Alerta de Dobra

Destaque de risco financeiro (valor da dobra) para ferias ja vencidas.

---

*Documento gerado em 28/07/2026 — Classic IA — Ferias*
*Versao 1.0 — Especificacao Funcional Completa F-42*
