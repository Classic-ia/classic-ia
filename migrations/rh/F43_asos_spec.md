# F-43 — ASOs (Atestados de Saude Ocupacional)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-43 |
| Titulo | ASOs — Controle de Exames Ocupacionais (NR-07/PCMSO) e Monitor eSocial |
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

**F-43 — ASOs** e o modulo de gestao dos Atestados de Saude Ocupacional (exames medicos ocupacionais) exigidos pela **NR-07/PCMSO**: cadastra os ASOs de cada colaborador (admissional, periodico, retorno, mudanca de funcao, demissional), calcula automaticamente a validade por idade e risco do cargo, sinaliza vencimentos e monitora os eventos eSocial correspondentes (S-2220 ASO, S-2210 CAT).

O modulo compreende **2 telas principais**, **3 tabelas de ASO** (com naming divergente — drift), tabelas de eSocial e de configuracao, e a RPC `sst_dashboard_completo`, no projeto Supabase RH.

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/asos.html` | Cadastro e controle de ASOs (4 abas: Painel, Cadastrar, Por Colaborador, Config) | 1079 |
| 2 | `rh/monitor-esocial.html` | Monitor de eventos eSocial SST (S-2220, S-2210, S-2240) | 369 |

**Bibliotecas compartilhadas (RH):** config.js, rh_auth.js, api.js (`API.get/post/patch/del`), shell.js (`Shell.init` em monitor-esocial; asos usa `RHAuth`), ui_components.js, page_bootstrap.js, ds.css. **Sem Chart.js.**

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `SEGURANCA_SUPABASE.sql` | `rh_asos` (plural — lida por asos.html) |
| `schema_rh_sst_completo.sql` | `rh_aso` (singular) |
| `MODULO_SST_v2.sql` | `sst_aso` (canonica SST — gravada por asos.html, lida por dashboard/monitor) |
| `TABELAS_SST.sql` | `esocial_eventos`, `esocial_s2240` |
| `FUNCOES_APLICACAO.sql` | RPC `sst_dashboard_completo()` (bloco asos) |
| (drift, nao versionada) | `rh_asos_config` (periodicidade/risco por cargo) |

> **Nota critica (3 tabelas ASO):** existem **tres** tabelas de ASO com colunas divergentes — `rh_asos` (plural: colaborador_id, tipo_aso, data_realizacao, data_vencimento, medico_nome), `rh_aso` (singular: funcionario_id, tipo_exame, data_exame, data_validade, medico_responsavel) e `sst_aso` (canonica SST: funcionario_id, tipo_exame, restricoes, clinica). A tela `asos.html` **le de `rh_asos` mas grava em `sst_aso`** — ver secao 24 (D-01).

---

## 2. Problema

A NR-07 (PCMSO) obriga a empresa a manter exames medicos ocupacionais validos para todo colaborador, sob pena de autuacao e responsabilizacao em caso de acidente/doenca ocupacional. Antes do sistema:

1. **Controle de vencimento manual** — Sem alerta de ASOs vencidos ou a vencer, com risco de manter colaborador em atividade sem exame valido.
2. **Periodicidade complexa** — A NR-07 varia a periodicidade por idade (menor de 18, 18-45, maior de 45) e risco do cargo; calcular manualmente era erro-prone.
3. **eSocial nao reportado** — Os eventos S-2220 (ASO) e S-2210 (CAT) precisam ser enviados em prazos legais; sem monitor, havia risco de atraso e multa.
4. **Dados dispersos** — ASOs em papel/planilha, sem historico por colaborador nem rastreabilidade de restricoes.

**Solucao:** modulo que (a) cadastra ASOs com validade auto-calculada (NR-07), (b) exibe painel de vigentes/vencidos/vencendo/sem ASO, (c) mantem historico por colaborador, (d) gera e monitora eventos eSocial com prazos.

---

## 3. Processo Operacional

### 3.1 Cadastro de ASO (`asos.html` aba Cadastrar)

1. Busca o colaborador (autocomplete) — exibe idade, cargo, risco do cargo e validade sugerida.
2. Seleciona tipo (admissional/periodico/retorno/mudanca_funcao/demissional), data do exame, resultado (apto/inapto/apto_restricao).
3. Se `apto_restricao`, informa as restricoes (gravadas em `riscos_avaliados`).
4. Informa medico, CRM, validade (meses, auto-preenchida), e a data de vencimento e calculada automaticamente (readonly).
5. Marca exames complementares realizados (concatenados no campo observacoes — sem coluna propria).
6. Salva → POST em `sst_aso` + `RHAuth.log('aso_cadastrado')`.

### 3.2 Calculo de Validade (NR-07)

`calcValidadeMeses(idade, risco)`:
- idade < 18 OU idade > 45 → **12 meses**
- 18-45 anos, risco **alto** → 12 meses
- 18-45 anos, risco **baixo** → 24 meses

Data de vencimento = data do exame + validade (meses). Proximo exame sugerido = vencimento − 30 dias.

### 3.3 Painel de Vencimentos (`asos.html` aba Painel)

Uma linha por colaborador ativo (com o ultimo ASO). Status derivado: `sem` (sem vencimento), `vencido` (dias < 0), `vencendo` (<= 30 dias), `vigente`. 4 KPIs + 2 alertas (vencidos = bloqueio operacional; vencendo = agendar).

### 3.4 Historico por Colaborador (`asos.html` aba Por Colaborador)

Timeline de ASOs do colaborador (tipo, data, status, resultado, medico/CRM, vencimento, restricoes) + card de status atual e info (idade, admissao, risco, periodicidade).

### 3.5 Configuracao (`asos.html` aba Config)

- Periodicidade padrao NR-07 (tabela estatica de referencia).
- Nivel de risco por cargo (`rh_asos_config`): cargo + CBO + risco (baixo/alto). Cargos nao configurados = baixo.
- Exames padrao por tipo de ASO (UI local — nao persistida).

### 3.6 Monitor eSocial (`monitor-esocial.html`)

1. Lista eventos de `esocial_eventos` (S-2210 CAT, S-2220 ASO, S-2240) ordenados por prazo.
2. **Gerar Automaticos:** varre `sst_aso` (S-2220) e `sst_acidente` com CAT emitida (S-2210), cria eventos faltantes (dedup por `tipo|funcionario_id|data_fato`), com prazo calculado.
3. **Enviar:** marca evento como enviado (exige protocolo) — PATCH status='enviado', grava protocolo e enviado_em.
4. Status derivado do prazo: atrasado, urgente, atencao, pendente, enviado, cancelado.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | asos.html | monitor-esocial.html |
|--------|-----------|----------------------|
| administrador / rh / sst | Cadastro + config + monitor | Gerar + enviar eventos |
| demais autenticados | Acesso (sem gating de perfil no cliente) | Acesso (sem gating) |

Ambas as telas fazem apenas gate de autenticacao (`RHAuth.init` / `Shell.init`), **sem checagem de perfil** no cliente. Controle delegado a RLS/rh_auth.js.

### 4.2 Auditoria

`RHAuth.log('aso_cadastrado', {colaborador_id, tipo, resultado})` no cadastro. Monitor grava `enviado_por`/`enviado_em` no evento.

---

## 5. Fluxograma

```
CADASTRO DE ASO                          MONITOR eSOCIAL
===============                          ===============
Buscar colaborador                       esocial_eventos (S-2210/S-2220/S-2240)
  idade, cargo, risco                       ordenado por prazo_envio
   |                                         |
   v                                         v
Validade NR-07 (auto):                   Gerar Automaticos:
  <18 | >45     -> 12m                      sst_aso -> S-2220 (por tipo_exame valido)
  18-45 baixo   -> 24m                      sst_acidente (cat_emitida) -> S-2210
  18-45 alto    -> 12m                      dedup: tipo|funcionario_id|data_fato
   |                                         prazo:
   v                                           admissional: admissao +15d
Vencimento = exame + validade                  periodico: dia 15 mes seguinte
   | resultado (apto/inapto/                    retorno: mesmo dia
   |   apto_restricao -> restricoes)            demissional: exame +15d
   v                                           CAT: ocorrencia +1d
POST sst_aso  <-- (painel le rh_asos!)         |
   | RHAuth.log                                v
   v                                       status por prazo:
PAINEL (por colaborador, ultimo ASO)       atrasado | urgente | atencao |
  status: vigente/vencendo/vencido/sem       pendente | enviado | cancelado
  KPIs + alertas                             |
                                             v
CONFIG (rh_asos_config)                   Enviar (protocolo obrigatorio)
  risco por cargo/CBO                       PATCH status=enviado + protocolo

DASHBOARD SST (sst_dashboard_completo)
======================================
asos: total | vencidos | vencendo_30d | sem_aso
  (DISTINCT ON ultimo ASO por funcionario ativo, via sst_aso)
```

---

## 6. Regras de Negocio

### RN-01 — Tipos de ASO (NR-07)

admissional, periodico, retorno (ao trabalho), mudanca_funcao, demissional. `rh_asos` usa `tipo_aso` (retorno/mudanca_funcao); `rh_aso`/`sst_aso` usam `tipo_exame` (retorno_trabalho/mudanca_funcao) — naming divergente.

### RN-02 — Resultado do ASO

apto, inapto, apto_restricao. Restricoes obrigatorias quando `apto_restricao` (gravadas em `riscos_avaliados` no payload / `restricoes` em sst_aso).

### RN-03 — Periodicidade por Idade e Risco (NR-07)

| Faixa | Risco Baixo | Risco Alto |
|-------|-------------|------------|
| < 18 anos | 12 meses | 12 meses |
| 18-45 anos | 24 meses | 12 meses |
| > 45 anos | 12 meses | 12 meses |

### RN-04 — Data de Vencimento

`data_vencimento = data_realizacao + validade_meses` (readonly, auto-calculado). Coluna "Validade" no painel = meses aproximados `(venc - real)/30d`.

### RN-05 — Status do ASO (painel)

Sem `data_vencimento` → `sem`; `dias_ate_vencer < 0` → `vencido`; `<= 30` → `vencendo`; senao `vigente`. Ordenacao: vencido(0), vencendo(1), vigente(2), sem(3), depois por vencimento.

### RN-06 — ASO Vencido = Bloqueio Operacional

Alerta: "obrigatorio realizar novo exame antes de manter o colaborador em atividade". ASO admissional deve ser realizado antes do inicio das atividades.

### RN-07 — Risco por Cargo

`rh_asos_config` mapeia cargo/CBO → risco (baixo/alto). Cargos nao configurados = baixo. Usado no calculo de validade.

### RN-08 — Exames Complementares

Lista fixa (`EXAMES_DISPONIVEIS`: Audiometria, Espirometria, Hemograma, EAS, Glicemia, Acuidade Visual, Raio-X, ECG, Toxicologico, Hepatograma, Creatinina, PSA, Colesterol). Selecionados sao **concatenados no campo observacoes** (sem coluna propria — D-05).

### RN-09 — Dashboard SST: Vencidos (ultimo ASO)

`sst_dashboard_completo` calcula vencidos/vencendo via `DISTINCT ON (funcionario_id) ... ORDER BY data_exame DESC` — considera **apenas o ultimo ASO** de cada funcionario com status IN (ativo, experiencia, ferias, afastado). `sem_aso` = ativos/experiencia sem nenhum ASO.

### RN-10 — Evento eSocial S-2220 (ASO)

Gerado de `sst_aso` para `tipo_exame IN (admissional, periodico, retorno, demissional)`. `data_fato = data_exame`, `subtipo = tipo_exame`, `descricao = 'ASO - <tipo>'`.

### RN-11 — Evento eSocial S-2210 (CAT)

Gerado de `sst_acidente` com `cat_emitida = true`. `data_fato = data_ocorrencia`, `prazo_envio = data_ocorrencia + 1 dia`, `subtipo = tipo_acidente`.

### RN-12 — Prazos de Envio (S-2220)

`calcPrazoASO`: admissional = `data_admissao + 15d`; periodico = **dia 15 do mes seguinte** ao exame; retorno = mesmo dia; demissional = `data_exame + 15d`; default = `+15d`.

### RN-13 — Status do Evento eSocial (por prazo)

`prazoStatus(prazo)`: `< hoje` → atrasado; `= hoje` → urgente; diferenca <= 3 dias → atencao; senao → pendente. Alem de enviado e cancelado. **O status e persistido na geracao e nao recalculado na leitura** (pode ficar defasado — D-06).

### RN-14 — Geracao Idempotente

Dedup por chave `tipo|funcionario_id|data_fato` contra `esocial_eventos` existentes. Nao duplica.

### RN-15 — Envio Exige Protocolo

Marcar evento como enviado exige protocolo nao vazio; grava `status='enviado'`, `protocolo`, `enviado_em`. Botao Enviar oculto para eventos ja enviados/cancelados.

---

## 7. Campos

### 7.1 Formulario de ASO (`asos.html`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Colaborador | `cad-colab-search`/`cad-colab-id` | search autocomplete | Sim |
| Tipo do ASO | `cad-tipo` | select | Sim |
| Data do Exame | `cad-data-exame` | date | Sim |
| Resultado | `cad-resultado` | select | Sim |
| Restricoes | `cad-restricoes` | textarea | Se apto_restricao |
| Medico Responsavel | `cad-medico` | text | Sim |
| CRM | `cad-crm` | text | Sim |
| Validade (meses) | `cad-validade` | number (1-48, default 12) | Auto |
| Data de Vencimento | `cad-vencimento` | date (readonly) | Sim (validado) |
| Exames Complementares | `cad-exames-group` | checkboxes | Nao |
| Observacoes | `cad-observacoes` | textarea | Nao |

Filtros do painel: `painel-search`, `painel-filter-status`, `painel-filter-tipo`.

### 7.2 Config de Risco (`rh_asos_config`)

`cfg-cargo`, `cfg-cbo`, `cfg-risco` (baixo/alto). Schema inclui `exames_admissional/periodico/retorno/mudanca_funcao/demissional` — **lidos mas nunca gravados pela UI** (D-04).

### 7.3 Monitor eSocial (`monitor-esocial.html`)

Filtros: `fTipo` (S-2210/S-2220/S-2240), `fStatus`, `fDe`/`fAte` (data_fato), `fBusca`. 5 KPIs: Eventos Pendentes, Atrasados, Urgentes, Em Atencao, Enviados no Mes.

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rh_asos` (plural) | ASO — **lida** por asos.html | colaborador_id, tipo_aso (CHECK 5), data_realizacao, data_vencimento, resultado (apto/inapto/apto_restricao), medico_nome, medico_crm, riscos_avaliados, status (valido/vencido/cancelado) |
| `rh_aso` (singular) | ASO — lida por versoes legadas | funcionario_id, tipo_exame, data_exame, data_validade, resultado, medico_responsavel, crm |
| `sst_aso` (canonica SST) | ASO — **gravada** por asos.html + lida por monitor/dashboard | funcionario_id, tipo_exame (CHECK 5), data_exame, data_validade, resultado, restricoes, medico_responsavel (NOT NULL), crm, clinica, CHECK(data_validade>=data_exame) |
| `rh_asos_config` (drift) | Risco/periodicidade por cargo | cargo, cbo, risco (baixo/alto), exames_* por tipo |
| `esocial_eventos` | Eventos eSocial SST | tipo (S-2210/2220/2240), funcionario_id, empresa_id, data_fato, prazo_envio, subtipo, descricao, status, protocolo, enviado_por/em |
| `esocial_s2240` | Condicoes ambientais (S-2240) | setor, ... |
| `sst_acidente` | Acidentes (fonte S-2210) | funcionario_id, data_ocorrencia, tipo_acidente, cat_emitida |

---

## 9. RPCs

### RPC-01 — `sst_dashboard_completo()`

**SECURITY DEFINER.** Bloco `asos`: total (sst_aso), vencidos e vencendo_30d (via DISTINCT ON ultimo ASO por funcionario ativo), sem_aso (ativos/experiencia sem ASO). Tambem retorna epis, treinamentos, os, acidentes. Usado pelo sst_dashboard.html (nao diretamente por asos.html).

> **Nota:** `asos.html` e `monitor-esocial.html` **nao chamam RPCs** — operam por PostgREST direto (GET/POST/PATCH/DELETE).

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `sst_dashboard_completo()` | KPIs consolidados SST (inclui bloco asos) |
| Triggers de `updated_at` | Padrao SST |

Nao ha function de calculo de validade/periodicidade no backend — a periodicidade NR-07 e calculada no frontend (`calcValidadeMeses`), e o status `vencido`/`vigente` idem.

---

## 11. Triggers

Triggers de `updated_at` em sst_aso, esocial_eventos (padrao). CHECK constraint `chk_aso_datas` (data_validade >= data_exame) em sst_aso. **Sem trigger de vencimento automatico** nem de geracao automatica de evento eSocial (geracao e manual via botao).

---

## 12. APIs (Chamadas Frontend)

### 12.1 asos.html (`API`)

- **GET:** rh_colaboradores (ativos), **rh_asos** (`select=*&order=data_realizacao.desc`), rh_asos_config
- **POST:** **sst_aso** (novo ASO), rh_asos_config (risco)
- **DELETE:** rh_asos_config (`id=eq.{id}`)

### 12.2 monitor-esocial.html (`API`)

- **GET:** rh_funcionarios (limit 2000), rh_setores, esocial_eventos (order prazo_envio + filtros), sst_acidente, sst_aso
- **POST:** esocial_eventos (geracao)
- **PATCH:** esocial_eventos (enviar / observacoes)

Nenhuma view, nenhuma RPC, nenhum Chart.js.

---

## 13. Integracoes

### 13.1 eSocial (S-2220 / S-2210 / S-2240)

- **S-2220** (Monitoramento da Saude/ASO): gerado de sst_aso
- **S-2210** (CAT): gerado de sst_acidente (cat_emitida)
- **S-2240** (Condicoes Ambientais): filtro/classe presentes, **sem geracao** (esocial_s2240 existe)
- **S-2230** (Afastamento): nao coberto por este monitor
- Envio manual (marca protocolo) — **sem transmissao automatizada** ao eSocial

### 13.2 Acidentes (F Acidentes)

`sst_acidente` alimenta os eventos S-2210 (CAT).

### 13.3 Restricoes Medicas

ASO `apto_restricao` gera restricao (campo restricoes/riscos_avaliados) — relacionado a `restricoes_medicas.html` (modulo separado).

### 13.4 Motor Score (F-40)

Treinamentos/ASO alimentam a dimensao de conformidade SST; ASO faltante e passivo apontado pelo motor.

---

## 14. Dashboards

### 14.1 Painel de ASOs (`asos.html`)

**4 KPIs:** ASOs Vigentes, Vencidos (red se >0), Vencendo em 30 dias (orange se >0), Sem ASO Cadastrado. **2 alertas:** vencidos (bloqueio), vencendo (agendar). Tabela por colaborador (ultimo ASO).

### 14.2 Monitor eSocial (`monitor-esocial.html`)

**5 KPIs:** Eventos Pendentes, Atrasados, Urgentes, Em Atencao, Enviados no Mes. Tabela de eventos (Tipo/Funcionario/Data Fato/Prazo/Subtipo/Status/Acoes) com linha expansivel (descricao, protocolo, observacoes, enviado_em).

### 14.3 Dashboard SST (`sst_dashboard.html` — via RPC)

Bloco asos: total, vencidos, vencendo_30d, sem_aso (fonte sst_aso).

---

## 15. Relatorios

### 15.1 Historico por Colaborador

Timeline de ASOs (`asos.html` aba Por Colaborador) com status atual, proximo exame e info do colaborador.

### 15.2 Lista de Eventos eSocial

Tabela filtravel por tipo/status/data para acompanhamento de prazos.

> Sem exportacao CSV/PDF nas duas telas.

---

## 16. Documentos

### 16.1 ASO (Atestado de Saude Ocupacional)

Documento medico (NR-07) — o sistema registra os metadados (tipo, resultado, medico, CRM, validade, restricoes), nao o PDF do atestado (sem upload de arquivo).

### 16.2 CAT / Eventos eSocial

Eventos S-2210/S-2220 representam os documentos legais transmitidos ao eSocial.

---

## 17. Evidencias

### 17.1 Metadados do ASO

Medico, CRM, clinica, data, resultado e restricoes sao a evidencia do exame. Exames complementares no texto de observacoes.

### 17.2 Protocolo eSocial

`esocial_eventos.protocolo` + `enviado_em`/`enviado_por` sao a evidencia da transmissao.

### 17.3 Auditoria

`RHAuth.log('aso_cadastrado')`; timestamps nas tabelas.

---

## 18. Permissoes

### 18.1 Frontend

Ambas as telas: gate de autenticacao apenas, **sem checagem de perfil** no cliente (D-07).

### 18.2 RLS

sst_aso, esocial_eventos seguem o RLS SST. rh_asos/rh_asos_config sao drift (aplicadas manualmente — RLS pode divergir).

---

## 19. Auditoria

### 19.1 Log de Cadastro

`RHAuth.log('aso_cadastrado', {colaborador_id, tipo, resultado})`.

### 19.2 Trilha eSocial

Evento guarda enviado_por/enviado_em/protocolo. `created_by` em sst_aso.

### 19.3 Timestamps

created_at/updated_at em todas as tabelas.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Cadastrar ASO | POST sst_aso + RHAuth.log |
| Calcular vencimento | `calcVencimento` (data + validade) |
| Adicionar risco de cargo | POST rh_asos_config |
| Remover risco | DELETE rh_asos_config |
| Gerar eventos eSocial | POST esocial_eventos (loop, dedup) |
| Enviar evento | PATCH status=enviado + protocolo |
| Salvar observacoes | PATCH esocial_eventos |
| Filtrar | server-side (tipo/status/data) + client-side (busca) |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (RHAuth) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_auth.js, api.js, shell.js, ui_components.js, page_bootstrap.js, ds.css. Externas: nenhuma.

### 21.3 Dependencias de Dados

rh_colaboradores/rh_funcionarios, sst_aso (+ rh_asos/rh_aso), rh_asos_config, esocial_eventos, sst_acidente, rh_setores.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Acidentes | SST | sst_acidente → CAT (S-2210) |
| Restricoes Medicas | SST | ASO apto_restricao → restricao |
| Treinamentos NR | SST | Conformidade SST (mesmo dashboard) |
| EPIs | F-17 SST | sst_dashboard_completo compartilhado |
| Gestao de Colaboradores | F-10 RH | Cadastro (idade, admissao, cargo) |
| Motor Score | F-40 | ASO faltante como passivo |
| Desligamentos | F-38 | ASO demissional no checklist |

---

## 23. Melhorias SIGA

### SIGA-01 — Unificar as 3 Tabelas de ASO

Consolidar rh_asos / rh_aso / sst_aso em uma unica tabela canonica com naming padronizado.

### SIGA-02 — Corrigir Leitura/Gravacao

`asos.html` deve ler e gravar na mesma tabela (hoje le rh_asos, grava sst_aso) — o cadastro nao reaparece no painel.

### SIGA-03 — Calculo de Validade no Backend

Mover `calcValidadeMeses` e o status vencido/vigente para function/trigger, garantindo consistencia entre telas e dashboard.

### SIGA-04 — Recalcular Status eSocial na Leitura

O status por prazo e persistido e defasa; recalcular no dashboard/consulta (view).

### SIGA-05 — Coluna de Exames Complementares

Criar coluna/tabela para exames complementares em vez de concatenar em observacoes.

### SIGA-06 — Transmissao Automatizada eSocial

Integrar com o eSocial (geracao de XML + transmissao) em vez de marcar protocolo manualmente.

### SIGA-07 — Upload do ASO (PDF)

Anexar o documento do ASO (Storage) ao registro.

### SIGA-08 — Restricao de Perfil

Restringir cadastro/envio a perfis sst/rh/administrador.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | `asos.html` **le de `rh_asos` mas grava em `sst_aso`** | **Grave:** ASO cadastrado nao reaparece no painel (le tabela diferente da que gravou) |
| D-02 | 3 tabelas ASO (rh_asos/rh_aso/sst_aso) com colunas divergentes | Fragmentacao; dashboard usa sst_aso, painel usa rh_asos |
| D-03 | rh_asos_config e drift (nao versionada) | Config de risco/periodicidade fora do controle |
| D-04 | `exames_*` do rh_asos_config lidos mas nunca gravados pela UI | Exames padrao por tipo nao configuraveis |
| D-05 | Exames complementares gravados dentro de observacoes | Sem estrutura/consulta |
| D-06 | Status eSocial persistido (prazoStatus) e nao recalculado na leitura | KPIs/filtros defasam com o tempo |
| D-07 | Sem checagem de perfil nas telas | Cadastro/envio por qualquer autenticado |
| D-08 | S-2240 sem geracao; S-2230 nao coberto | Cobertura eSocial parcial |

### 24.2 Multiempresa

1. `empresa_id`/`estabelecimento_id` em sst_aso, esocial_eventos, rh_asos_config
2. RLS por empresa
3. PCMSO/periodicidade por empresa (risco por cargo distinto)
4. eSocial por CNPJ (empresa) — cada empresa transmite seus eventos
5. Dashboards e monitor filtrados por empresa

---

## 25. Criterios de Aceite

### CA-01 — Cadastrar ASO

Cadastrar ASO admissional apto → validade calculada por idade/risco; vencimento readonly; gravado.

### CA-02 — Validade NR-07

Colaborador 30 anos, risco baixo → 24 meses; risco alto → 12; >45 → 12.

### CA-03 — Painel

Ultimo ASO por colaborador; status vigente/vencendo/vencido/sem; KPIs e alertas corretos.

### CA-04 — Restricao

Resultado apto_restricao → campo restricoes obrigatorio; gravado em riscos_avaliados/restricoes.

### CA-05 — Config de Risco

Adicionar cargo/CBO/risco → usado no calculo de validade; remover funciona.

### CA-06 — Historico

Timeline por colaborador com todos os ASOs e status atual.

### CA-07 — Gerar Evento eSocial

Gerar automaticos → cria S-2220 (ASO) e S-2210 (CAT) faltantes, sem duplicar.

### CA-08 — Prazo eSocial

Prazos por tipo (admissional +15, periodico dia 15 mes seguinte, CAT +1); status por prazo.

### CA-09 — Enviar Evento

Marcar enviado exige protocolo; grava protocolo/enviado_em.

### CA-10 — Dashboard SST

sst_dashboard_completo retorna asos vencidos/vencendo/sem (ultimo ASO).

---

## 26. Casos de Teste

### CT-01 — ASO Periodico 24m

30 anos, risco baixo, exame hoje → vencimento +24 meses, status vigente.

### CT-02 — ASO Vencido

Vencimento ha 10 dias → status vencido, alerta vermelho, bloqueio.

### CT-03 — Vencendo em 20 dias

Vencimento em 20 dias → status vencendo, alerta laranja.

### CT-04 — Sem ASO

Colaborador ativo sem ASO → KPI Sem ASO, mensagem admissional.

### CT-05 — Apto com Restricao

Resultado apto_restricao sem restricoes → validacao exige o campo.

### CT-06 — Divergencia Tabela

Cadastrar ASO (grava sst_aso) → recarregar painel (le rh_asos) → ASO nao aparece (D-01).

### CT-07 — Gerar S-2220

ASO periodico sem evento → gera S-2220, prazo dia 15 do mes seguinte.

### CT-08 — Gerar S-2210

Acidente com CAT emitida → gera S-2210, prazo +1 dia.

### CT-09 — Dedup

Rodar geracao 2x → nao duplica (chave tipo|func|data).

### CT-10 — Enviar sem Protocolo

Marcar enviado sem protocolo → alerta "Informe o protocolo".

---

## 27. Casos Extremos

### CE-01 — Menor de 18

Idade < 18 → 12 meses (independente de risco).

### CE-02 — Cargo sem Config

Cargo nao configurado → risco baixo.

### CE-03 — Multiplos ASOs

Colaborador com varios ASOs → painel usa o ultimo (data_realizacao desc).

### CE-04 — Status eSocial Defasado

Evento gerado como pendente ha semanas → continua pendente na leitura mesmo apos vencer (D-06).

### CE-05 — ASO sem Vencimento

data_vencimento nula → status sem.

### CE-06 — Exames em Observacoes

Exames complementares concatenados → sem consulta estruturada (D-05).

### CE-07 — S-2240 Filtro sem Dados

Filtrar S-2240 → nenhum evento (nao ha geracao).

### CE-08 — Funcionario > 2000

funcMap limitado a 2000 → nomes alem disso nao resolvem no monitor.

---

## 28. Melhorias Futuras

### MF-01 — Agendamento de Exames

Calendario de agendamento com clinica/medico e notificacao 30 dias antes.

### MF-02 — Integracao com Clinica

Recebimento eletronico do ASO da clinica (API/portal).

### MF-03 — PCMSO Digital

Vinculo ASO ↔ riscos do PGR/PCMSO por cargo/setor.

### MF-04 — Alerta Automatico

Notificacao (email/WhatsApp) de ASOs vencidos/vencendo ao RH/SST e gestor.

### MF-05 — eSocial Completo

S-2220/S-2210/S-2240/S-2230 com geracao de XML e transmissao automatizada.

### MF-06 — Painel de Conformidade SST

Indice de conformidade (ASO + treinamento + EPI) por colaborador/setor.

### MF-07 — Histórico de Restricoes

Timeline de restricoes medicas ligada ao ASO (integra restricoes_medicas.html).

### MF-08 — Provisao de Exames

Projecao de exames a realizar por mes/custo (planejamento PCMSO).

---

*Documento gerado em 28/07/2026 — Classic IA — ASOs*
*Versao 1.0 — Especificacao Funcional Completa F-43*
