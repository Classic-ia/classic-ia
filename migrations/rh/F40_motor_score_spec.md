# F-40 — Motor Score v2 (Inteligencia de RH & SST)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-40 |
| Titulo | Motor Score v2 — Scoring de Colaboradores, Auditoria de Dados e Motor de Decisoes |
| Modulo | RH / Gestao (Inteligencia — Motor de Decisao) |
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

**F-40 — Motor Score v2** e o nucleo de inteligencia do RH: calcula um **score consolidado** (0-100) para cada colaborador ativo (133 funcionarios) a partir de 6 dimensoes operacionais, classifica cada score com base na **confiabilidade dos dados** (nao apenas no valor), audita a qualidade das fontes e converte problemas detectados (absenteismo, afastamento, setor critico, SST/EPI) em **planos de acao** com responsavel e prazo. E o modulo transversal referenciado por F-14 (Absenteismo) e F-38 (Turnover).

O modulo compreende **3 telas principais**, alimentadas por **5 tabelas**, **8 RPCs** e **12 views** (`vw_motor_*`), no projeto Supabase RH.

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/motor_v2.html` | Dashboard executivo do motor (scores, evolucao, confiabilidade, alertas, padroes) | 689 |
| 2 | `rh/motor_v2_auditoria.html` | Auditoria de scores nao-consistentes e diagnostico de causas | 571 |
| 3 | `rh/motor_decisoes.html` | Deteccao automatica de problemas + plano de acao | 109 |

**Bibliotecas compartilhadas (RH):** config.js, rh_auth.js, api.js (`API.rpc/setPerfil/esc`), shell.js (`Shell.init`), ui_components.js, page_bootstrap.js, ds.css. **Sem Chart.js** — todas as visualizacoes sao CSS/SVG manuais.

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `TABELAS_GESTOR.sql` | 5 tabelas (motor_execucao, motor_score, motor_alerta, motor_indicador, motor_padrao) |
| `FUNCOES_APLICACAO.sql` | RPCs motor_v2_executar, motor_v2_calcular_scores, motor_v2_calcular_indicadores, motor_v2_detectar_padroes, motor_v2_gerar_alertas, motor_v2_dashboard_executivo, motor_v2_auditoria, motor_decisoes |
| `VIEWS_APLICACAO.sql` | 12 views `vw_motor_*` |

---

## 2. Problema

A Classic Couros gera dados operacionais em multiplas fontes (ponto Secullum, producao ATAK, treinamentos SST, EPIs BuscaEPI) que ficavam isolados. Antes do motor:

1. **Sem visao consolidada do colaborador** — Impossivel ter um indicador unico de "saude operacional" por funcionario.
2. **Dados de qualidade desigual** — Algumas fontes cobrem 100% (treinamento), outras 40% (EPI, por carga incompleta). Um score sem medir a **confiabilidade** enganaria (baixo score por falta de dado, nao por baixo desempenho).
3. **Problemas nao acionados** — Absenteismo recorrente, afastamentos prolongados e setores criticos eram detectados tarde, sem plano de acao.
4. **Risco LGPD** — Tentar usar dados de saude (CID, ASO) para pontuar pessoas seria ilegal e injusto.

**Solucao:** motor que (a) pontua com 6 dimensoes operacionais **ponderadas apenas quando ha dado**, (b) classifica cruzando score x confiabilidade, (c) audita a cobertura das fontes por setor, (d) converte problemas em planos de acao — tudo **sem dados de saude** (LGPD).

---

## 3. Processo Operacional

### 3.1 Execucao do Motor (`motor_v2.html` → RPC `motor_v2_executar`)

1. Admin/RH seleciona periodo (mes/ano) e clica **Recalcular**.
2. `motor_v2_executar(p_periodo)` cria `motor_execucao` (status=executando) e roda em sequencia:
   - `motor_v2_calcular_scores` — pontua cada funcionario ativo (secao 6)
   - `motor_v2_calcular_indicadores` — KPIs executivos
   - `motor_v2_detectar_padroes` — padroes (motor_padrao)
   - `motor_v2_gerar_alertas` — alertas (motor_alerta)
3. Atualiza `motor_execucao` (status=concluido, registros_processados, detalhes) ou marca `erro`.

### 3.2 Calculo do Score (engine `motor_v2_calcular_scores`)

Para cada funcionario ativo, calcula **6 dimensoes** (0-100), pondera **apenas as com dado** (normaliza pelo peso total), determina **confiabilidade** (nº de fontes ativas) e **classificacao**. Grava em `motor_score` com fatores textuais e cobertura por fonte. Ver secao 6 (regras completas).

### 3.3 Dashboard Executivo (`motor_v2.html` → `motor_v2_dashboard_executivo`)

RPC perfil-consciente: base para todos (KPIs, resumo, confiabilidade, setores de risco, contadores de alerta, evolucao); **gestor** ganha alertas de setor; **admin/rh** ganham scores individuais detalhados + alertas completos + padroes. Ver secao 14.1.

### 3.4 Auditoria (`motor_v2_auditoria.html` → `motor_v2_auditoria`)

Restrita a admin/rh. Diagnostica scores nao-consistentes: resumo por classificacao, ranking de causas de perda, analise por setor (sem_ponto/sem_treinamento/sem_epi) e diagnostico individual (6 dimensoes + tipo de problema + causa). Ver secao 14.2.

### 3.5 Motor de Decisoes (`motor_decisoes.html` → `motor_decisoes`)

Detecta 4 categorias de problema (recorrencia, afastamento prolongado, setor critico, SST/EPI), exclui contextos legitimos (licenca maternidade, INSS), gera diagnostico + causa provavel + plano de acao (responsavel/prazo) + impacto financeiro. Ordena por prioridade e impacto. Ver secao 6 (RN-14 a RN-18) e 14.3.

---

## 4. Usuarios

### 4.1 Perfis e Escopo

| Perfil | motor_v2 (dashboard) | motor_v2_auditoria | motor_decisoes | Executar |
|--------|----------------------|--------------------|----------------|----------|
| `administrador` | Completo (scores individuais, alertas, padroes) | Sim | Sim | Sim |
| `rh` | Completo | Sim | Sim | Sim |
| `gestor` | Base + alertas de setor | Bloqueado (tela 🔒) | Sim (sem restricao no cliente) | Nao |
| demais autenticados | Base (KPIs, resumo, setores) | Bloqueado | Sim (sem restricao no cliente) | Nao |

### 4.2 Enforcement

- **Backend:** `motor_v2_dashboard_executivo` monta o payload conforme `rh_perfil_atual()`; `motor_v2_executar` e `motor_v2_auditoria` recusam perfis fora de (administrador, rh).
- **Frontend:** `motor_v2.html` oculta Recalcular e Scores Detalhados fora de admin/rh; `motor_v2_auditoria.html` mostra tela "Acesso Restrito" para nao-admin/rh; **`motor_decisoes.html` nao tem restricao explicita de perfil** (so `Shell.init`) — ver D-02.

---

## 5. Fluxograma

```
EXECUCAO                                    SCORING (por funcionario ativo)
========                                    ===============================
Selecionar periodo (mes/ano)                Presenca (0.20)  <- rh_ponto | producao_rateada
   | Recalcular (admin/rh)                    (cargo_confianca = 100)
   v                                         Producao (0.20)  <- producao_rateada (indiv/media setor)
motor_v2_executar(p_periodo)                Treinamento (0.20) <- sst_treinamento (valido/total)
  cria motor_execucao                       Estabilidade (0.10) <- meses de empresa
   |                                         EPI (0.10)        <- sst_epi_entrega (365d)
   +-> calcular_scores  ---------------->   Qualidade (0.20)  <- fontes_ativas/5 * 100
   +-> calcular_indicadores                    |
   +-> detectar_padroes                        v
   +-> gerar_alertas                        score_final = soma_ponderada / peso_total
   |                                         (so pondera dimensao COM dado)
   v                                            |
motor_execucao: concluido                       v
                                            confiabilidade (fontes_ativas): >=4 alta, >=2 media,
DASHBOARD (perfil-based)                       >=1 baixa, else indeterminada
========================                        |
motor_v2_dashboard_executivo                    v
  base: kpis, resumo, confiab,              classificacao:
        setores, evolucao, alertas            baixa/indet -> dado_insuficiente
  gestor: + alertas de setor                  >=75 -> consistente
  rh/admin: + scores_detalhados               >=55 -> (alta: regular | else monitorar)
            + alertas + padroes               <55  -> atencao_operacional
                                                |
AUDITORIA (admin/rh)                            v
====================                        motor_score (6 scores + cobertura + fatores)
motor_v2_auditoria
  resumo por classificacao                 MOTOR DE DECISOES
  causas de perda                          =================
  por_setor (sem_ponto/trein/epi)          motor_decisoes (4 categorias):
  diagnostico individual                     1 recorrencia (absenteismo)
                                             2 afastamento_prolongado
                                             3 setor_critico
                                             4 sst_epi
                                           exclui: licenca maternidade, INSS
                                           -> diagnostico + causa + plano_acao (resp/prazo)
```

---

## 6. Regras de Negocio

### RN-01 — Score de Presenca (peso 0.20)

- `cargo_confianca = true` → presenca = 100 (isento de ponto), cobertura 100, +1 fonte.
- Se ha `rh_ponto` no periodo: `presenca = min(100, presente / base * 100)`, onde `presente` = dias com registro sem justificativa de falta, `base = max(1, dias_uteis - dias_ferias)`; cobertura = `ponto_total / dias_uteis * 100`. Fatores: nº de faltas, dias de ferias.
- Sem ponto: usa `producao_rateada` como proxy — cobertura = `dias_com_registro / dias_uteis`; se >=50% presenca = cobertura; se >0 presenca = cobertura*1.2 (parcial); senao "Sem registro de presenca".

### RN-02 — Score de Producao (peso 0.20)

So calcula se cobertura de producao >= 30%. `producao = min(100, (media_individual / media_setor) * 100)` a partir de `producao_rateada` (comparacao com a media do setor). +1 fonte quando calculado.

### RN-03 — Score de Treinamento (peso 0.20)

`treinamento = treinamentos_validos / treinamentos_totais * 100` (de `sst_treinamento`; valido = data_validade nula ou futura). Cobertura 100 se ha registro, 0 caso contrario. Fator: nº de treinamentos vencidos.

### RN-04 — Score de Estabilidade (peso 0.10)

Por meses de empresa (`data_admissao`): `<3m` → 30-60 (linear); `3-12m` → 60-80; `12-36m` → 80-100; `>=36m` → 100. Sempre conta como fonte ativa. Fator "Periodo de experiencia" se <3m.

### RN-05 — Score de EPI (peso 0.10)

`sst_epi_entrega` nos ultimos 365 dias: `>=3 entregas` → 100; senao `50 + (count/3)*50`. Cobertura = 100 se ha entrega nos ultimos 180 dias, senao 50. Fator "EPI: dado > 180 dias" ou "Sem registro de EPI".

### RN-06 — Score de Qualidade (peso 0.20)

`qualidade = fontes_ativas / 5 * 100` — mede quantas das 5 fontes operacionais (presenca, producao, treinamento, estabilidade, EPI) tem dado. **Meta-dimensao de confiabilidade**, sempre ponderada.

### RN-07 — Score Final (ponderacao normalizada)

Pesos: presenca 0.20, producao 0.20, treinamento 0.20, estabilidade 0.10, EPI 0.10, qualidade 0.20. **Uma dimensao so entra no somatorio se tiver dado** (presenca/producao/treinamento/EPI podem ser NULL); estabilidade e qualidade sempre entram. `score_final = soma_ponderada / peso_total` (normaliza para os pesos efetivamente presentes).

### RN-08 — Confiabilidade do Dado

Por `fontes_ativas`: `>=4` → alta; `>=2` → media; `>=1` → baixa; `0` → indeterminada.

### RN-09 — Classificacao (5 niveis)

- confiabilidade `baixa`/`indeterminada` → **dado_insuficiente**
- score_final `>=75` → **consistente**
- score_final `>=55` → **regular** (se confiabilidade alta) ou **monitorar** (senao)
- score_final `<55` → **atencao_operacional**

Cores: consistente verde, regular azul, monitorar amarelo, atencao_operacional vermelho, dado_insuficiente cinza.

### RN-10 — Recalculo por Periodo (idempotencia)

`motor_v2_calcular_scores` faz `DELETE FROM motor_score WHERE periodo_referencia = p_periodo` antes de recalcular (FIX B1: por periodo, nao por execucao_id, que e sempre novo). Reexecutar o mesmo mes sobrescreve.

### RN-11 — Dias Uteis do Periodo

Conta dias uteis (seg-sex) de `periodo_inicio` ate `min(periodo_fim, hoje)`; se <1, assume 22.

### RN-12 — Score Zero/Null na Exibicao

Score 0 ou null e exibido como "—" (travessao). Cores de score (frontend): `>=80` verde, `>=60/>=50` azul/amarelo, `>=40/<50` amarelo/vermelho, `>=20` vermelho, senao cinza.

### RN-13 — LGPD (sem dados de saude)

O motor usa **exclusivamente** dados operacionais agregados (presenca via producao, producao KG, treinamentos, tempo de empresa, EPI). **Nao usa** CID, ASO, restricoes medicas ou diagnosticos. `dado_insuficiente` = falta de registro, **nao** baixa performance. Score nao e julgamento de valor e nao deve embasar decisao de gestao sem validacao humana.

### RN-14 — Motor de Decisoes: Recorrencia de Absenteismo

De `vw_absenteismo_recorrencia` (classificacao RECORRENTE/RECORRENTE CRITICO, top 15 por custo). **Exclui** licenca maternidade/paternidade e afastamento INSS (doenca/acidente) — registrados em `excluidos_por_contexto`. Risco: `>=3 atestados/mes` alto, `>=2` medio, senao baixo. Plano de acao: conversa RH+lideranca, revisao de historico/CID, avaliacao medica ocupacional, acompanhamento 3 meses.

### RN-15 — Motor de Decisoes: Afastamento Prolongado

De `vw_absenteismo_recorrencia` (AFASTAMENTO PROLONGADO, top 5 por dias). Exclui licenca maternidade. Risco: `>=90 dias` alto. Plano: ASO de retorno, retorno com restricoes, avaliar posto, acompanhamento semanal.

### RN-16 — Motor de Decisoes: Setor Critico

De `vw_absenteismo_por_setor` (total_dias > 50). Prioridade 1 se `>=500 dias`. Causa: `>50% do setor afetado` → problema ambiental/gestao; senao casos individuais. Plano: reuniao lideranca, AET, revisar condicoes.

### RN-17 — Motor de Decisoes: SST/EPI (com carga incompleta)

So alerta como problema real se a carga do BuscaEPI `>= 80%` (`carregadas / 41787`) **e** `sem_epi > 5`. Impacto = `sem_epi * 3000` (estimativa realista, nao multa maxima). Carga incompleta → registrado como `excluido` (informativo). Considera so funcionarios `elegivel_sst` sem entrega.

### RN-18 — Motor de Decisoes: Ordenacao e Resumo

Problemas ordenados por prioridade (1-3) e impacto financeiro desc. Resumo: impacto_total, alto_risco (count), acoes_total (soma dos planos), custo_excluido. Plano de acao no frontend tem checkbox **local (sem persistencia)** — ver D-03.

### RN-19 — Alertas e Padroes

`motor_alerta` (aberto/em_andamento, prioridade critica/alta/media/baixa, dedup por `chave_dedup`) e `motor_padrao` (severidade alta/media/baixa, ultimos 90 dias). Badge de alertas no dashboard exibe abertos e criticos.

---

## 7. Campos

### 7.1 Dimensoes do Score (`motor_score`)

| Dimensao | Coluna | Peso | Fonte |
|----------|--------|------|-------|
| Presenca | `score_presenca` | 0.20 | rh_ponto / producao_rateada |
| Producao | `score_producao` | 0.20 | producao_rateada (indiv/setor) |
| Treinamento | `score_treinamento` | 0.20 | sst_treinamento |
| Estabilidade | `score_estabilidade` | 0.10 | data_admissao (meses) |
| EPI | `score_epi` | 0.10 | sst_epi_entrega |
| Qualidade | `score_qualidade` | 0.20 | fontes_ativas/5 |
| **Final** | `score_final` | — | ponderacao normalizada |

Metadados: `classificacao`, `confiabilidade`, `cobertura_presenca/producao/treinamento/epi`, `fontes_ativas`, `fatores` (text[]), `periodo_referencia`.

### 7.2 Controles de Tela

- **motor_v2.html:** `#sel-month`, `#sel-year` (periodo), `#btn-exec` (Recalcular, admin/rh), `#badge-alertas`.
- **motor_v2_auditoria.html:** filtros `#f-classif`, `#f-setor`, `#f-tipo`, `#f-search`; `#btn-refresh`.
- **motor_decisoes.html:** sem filtros; KPIs com deep-links.

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `motor_execucao` | Execucao do motor | id, tipo, inicio, fim, status (executando/concluido/erro), registros_processados, detalhes (jsonb), erro |
| `motor_score` | Score por funcionario/periodo | id, execucao_id, funcionario_id, funcionario_nome, setor_nome, cargo_nome, 6 scores, classificacao, fatores (text[]), periodo_referencia, confiabilidade, cobertura_presenca/producao/treinamento/epi, fontes_ativas |
| `motor_alerta` | Alertas gerados | id, execucao_id, tipo, prioridade, titulo, descricao, referencia_tipo/id/nome, dados (jsonb), status, responsavel, resolucao, chave_dedup |
| `motor_indicador` | KPIs/indicadores | id, execucao_id, categoria, indicador, periodo_inicio/fim, valor, unidade, referencia_tipo/id/nome, meta, tendencia |
| `motor_padrao` | Padroes detectados | id, execucao_id, tipo, referencia_tipo/id/nome, periodo, descricao, dados (jsonb), severidade |

Fontes de dados: `rh_funcionarios`, `rh_setores`, `rh_cargos`, `rh_ponto`, `producao_rateada`, `sst_treinamento`, `sst_epi_entrega`, `vw_absenteismo_recorrencia`, `vw_absenteismo_por_setor`, `stg_buscaepi_entregas`.

---

## 9. RPCs

### RPC-01 — `motor_v2_executar(p_periodo)`

**SECURITY DEFINER.** Orquestrador. Recusa perfis fora de (administrador, rh). Cria execucao e roda calcular_scores → calcular_indicadores → detectar_padroes → gerar_alertas. Retorna `{execucao_id, status, scores, indicadores, padroes, alertas, periodo}`.

### RPC-02 — `motor_v2_calcular_scores(p_execucao_id, p_periodo)` → integer

Engine de scoring (secao 6). DELETE por periodo + INSERT por funcionario ativo. Retorna nº de scores.

### RPC-03 — `motor_v2_calcular_indicadores(p_execucao_id, p_periodo)`

Gera KPIs executivos em `motor_indicador`.

### RPC-04 — `motor_v2_detectar_padroes(p_execucao_id, p_periodo)`

Detecta padroes em `motor_padrao`.

### RPC-05 — `motor_v2_gerar_alertas(p_execucao_id, p_periodo)`

Gera alertas em `motor_alerta` (dedup por chave).

### RPC-06 — `motor_v2_dashboard_executivo()`

**SECURITY DEFINER, perfil-based.** Base (kpis, resumo_score, confiabilidade, setores, alertas_abertos/criticos, evolucao) + gestor (alertas de setor) + rh/admin (scores_detalhados classificacao IN atencao/monitorar LIMIT 50, alertas completos, padroes). Guard: `_sem_acesso` → erro.

### RPC-07 — `motor_v2_auditoria()`

**SECURITY DEFINER,** admin/rh. Retorna resumo (por classificacao), causas (`vw_motor_causas_perda`), por_setor (`vw_motor_setor_nao_consistente`), detalhes (`vw_motor_auditoria_score`).

### RPC-08 — `motor_decisoes()`

**STABLE SECURITY DEFINER.** 4 categorias de problema com exclusoes de contexto, plano de acao, impacto financeiro. Retorna `{total_problemas, problemas[], excluidos_por_contexto[], total_excluidos, resumo, gerado_em}`.

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `motor_v2_executar` | Orquestra a execucao completa |
| `motor_v2_calcular_scores` | Engine de scoring (6 dimensoes) |
| `motor_v2_calcular_indicadores` | KPIs |
| `motor_v2_detectar_padroes` | Padroes |
| `motor_v2_gerar_alertas` | Alertas |
| `motor_v2_dashboard_executivo` | Payload do dashboard (perfil) |
| `motor_v2_auditoria` | Diagnostico de qualidade |
| `motor_decisoes` | Deteccao + plano de acao |
| `rh_perfil_atual()` | Perfil do usuario (guard) |

---

## 11. Triggers

O modulo nao define triggers proprios de negocio — a orquestracao e sequencial via `motor_v2_executar`. `motor_alerta` tem `updated_at` (via padrao RH). Nenhum trigger de recalculo automatico (o motor e disparado manualmente ou por rotina externa).

---

## 12. APIs (Chamadas Frontend)

### 12.1 RPCs (via `API.rpc` ou fetch direto)

| Tela | Chamada | Parametros |
|------|---------|-----------|
| motor_v2.html | `API.rpc('motor_v2_dashboard_executivo', {})` | — |
| motor_v2.html | `API.rpc('motor_v2_executar', {p_periodo})` | `AAAA-MM-01` |
| motor_v2_auditoria.html | `fetch(SB_URL + '/rest/v1/rpc/motor_v2_auditoria')` | — (token `rh_sess_v2`) |
| motor_decisoes.html | `API.rpc('motor_decisoes', {})` | — |

**Nenhuma tela consulta views `vw_motor_*` ou tabelas diretamente** — toda a logica esta encapsulada nas RPCs. `motor_v2_auditoria.html` usa `fetch` direto (SB_URL/SB_KEY + token) em vez do wrapper `API.rpc` — inconsistente (ver D-01).

### 12.2 Views (consumidas pelas RPCs)

12 views `vw_motor_*`: alertas_abertos, gestor_alertas, padroes, evolucao_mensal, kpis_executivos, score_resumo, confiabilidade_dado, setor_risco, scores_detalhado, setor_nao_consistente, causas_perda, auditoria_score.

---

## 13. Integracoes

### 13.1 Fontes de Dados Operacionais

| Fonte | Sistema | Alimenta |
|-------|---------|----------|
| Ponto | Secullum (importar) → `rh_ponto` | Presenca |
| Producao | ATAK WRPRD600 → `producao_rateada` | Presenca (fallback) + Producao |
| Treinamentos | SST → `sst_treinamento` | Treinamento |
| EPIs | BuscaEPI → `sst_epi_entrega` / `stg_buscaepi_entregas` | EPI + SST/decisoes |
| Cadastro | `rh_funcionarios` | Estabilidade (admissao), cargo_confianca |
| Absenteismo | `vw_absenteismo_*` (F-14) | Motor de decisoes |

### 13.2 Consumidores do Motor

- **F-38 Turnover:** `motor_score` (score dos desligados — "perdendo bons?")
- **F-14 Absenteismo:** compartilha `vw_absenteismo_recorrencia`/`por_setor`
- **Alertas RH** (`alertas_rh.html`): deep-link do motor_decisoes
- **Relatorio de Atestados** (`relatorio_atestados.html`): deep-link

---

## 14. Dashboards

### 14.1 Dashboard Executivo (`motor_v2.html`)

**KPIs dinamicos** (do backend, `vw_motor_kpis_executivos`): label = indicador, valor + unidade (%/R$/KG/dias), referencia. Icones por categoria (headcount, turnover, producao, dias_sem_acidente, estabilidade).

**Secoes (single-view, CSS/SVG):**
- **Distribuicao de Classificacoes:** barra empilhada por classificacao + score medio por classe
- **Evolucao Mensal:** mini-barras verticais (score medio por periodo) + tendencia ▲/▼/▶
- **Qualidade dos Dados:** badge de confiabilidade geral + barra empilhada Alta/Media/Baixa + 4 barras de cobertura (Presenca/Producao/Treinamento/EPI) + aviso se baixa/<50%
- **Ranking por Setor** (`#tbl-setores`): Setor, Func., Score Medio (barra), Distribuicao (miniDist), Fontes, Qualidade
- **Confiabilidade por Setor** (`#tbl-conf`): Setor, Total, Alta, Media, Baixa, cobertura Presenca/Producao/Treinamento/EPI
- **Alertas** (`#tbl-alertas`) + **Padroes** (`#tbl-padroes`, oculto se vazio)
- **Scores Individuais Detalhados** (`#tbl-detalhado`, admin/rh): Funcionario, Setor, Score, Classificacao, Confiabilidade, Fontes, Qualidade

### 14.2 Auditoria (`motor_v2_auditoria.html`)

**5 KPIs:** Total Avaliados, Consistentes, Monitorar, Atencao Operacional, Dado Insuficiente.

**Secoes:** Consistency Overview (donut SVG % consistentes), Ranking de Causas (barras por causa + tipo + setores afetados), Analise por Setor (`#tbl-setores`: Nao Consist., Atencao Op., Monitorar, Dado Insuf., Sem Ponto, Sem Trein., Sem EPI, Score Medio, Fontes Media), Diagnostico Individual (`#tbl-detalhes`, 13 colunas: 6 dimensoes + classificacao + tipo_problema + causa, linha expansivel com fatores), Explicabilidade (guia dado_ausente/dado_parcial/operacional/misto).

### 14.3 Motor de Decisoes (`motor_decisoes.html`)

**4 KPIs (com deep-links):** Problemas Detectados (→#problems), Alto Risco (→alertas_rh.html), Acoes Necessarias (→alertas_rh.html), Impacto Total R$ (→relatorio_atestados.html).

**Cards de problema** (`recorrencia` 🔁 / `afastamento_prolongado` 🏥 / `setor_critico` 🏭 / `sst_epi` 🛡️): titulo (funcionario ou setor), risco (tag), diagnostico, causa provavel, impacto financeiro, plano de acao (checkbox local + acao + responsavel + prazo).

---

## 15. Relatorios

### 15.1 Evolucao Mensal

`vw_motor_evolucao_mensal`: score_medio, qualidade_media, fontes_media, contagens por classificacao por periodo — base do grafico de tendencia.

### 15.2 Ranking por Setor

`vw_motor_setor_risco` (dashboard) e `vw_motor_setor_nao_consistente` (auditoria).

### 15.3 Diagnostico Individual (auditoria)

Lista exportavel visualmente (13 colunas) com fatores expansiveis por funcionario.

### 15.4 Plano de Acao (decisoes)

Lista de acoes por problema com responsavel (RH/SST/Gestor) e prazo — **nao persistida** (checkbox local).

---

## 16. Documentos

### 16.1 Aviso LGPD (rodape das 3 telas)

Texto fixo: o motor usa so dados operacionais; nao usa dados de saude; `dado_insuficiente` != baixa performance; score nao e julgamento e exige validacao humana.

### 16.2 Guia de Explicabilidade (auditoria)

Interpretacao dos tipos de problema: `dado_ausente` (sem registros — nao e desempenho), `dado_parcial` (falta EPI/treinamento — falha de cadastro/integracao), `operacional` (score baixo com dados confiaveis), `misto` (caso a caso).

---

## 17. Evidencias

### 17.1 Fatores do Score

`motor_score.fatores` (text[]) explica cada score: "N falta(s)", "N treinamento(s) vencido(s)", "Periodo de experiencia", "Sem registro de EPI", "Cargo de confianca (isento de ponto)", etc.

### 17.2 Cobertura por Fonte

`cobertura_presenca/producao/treinamento/epi` + `fontes_ativas` sao a evidencia da confiabilidade do score.

### 17.3 Trilha de Execucao

`motor_execucao` (inicio/fim/status/registros_processados/detalhes) registra cada rodada do motor.

---

## 18. Permissoes

### 18.1 Backend

`motor_v2_executar` e `motor_v2_auditoria` recusam perfis fora de (administrador, rh). `motor_v2_dashboard_executivo` filtra o payload por perfil. `motor_decisoes` e STABLE SECURITY DEFINER (sem guard de perfil interno).

### 18.2 Frontend

motor_v2 oculta recalculo/scores detalhados fora de admin/rh; auditoria bloqueia com tela "Acesso Restrito"; **motor_decisoes nao restringe perfil no cliente** (D-02).

---

## 19. Auditoria

### 19.1 Execucoes

`motor_execucao` — historico completo de rodadas (status, erro, detalhes).

### 19.2 Auditoria de Qualidade

Tela `motor_v2_auditoria.html` + RPC `motor_v2_auditoria` sao a auditoria de confiabilidade dos scores (nao de "quem fez").

> **Lacuna:** o motor nao grava em `rh_audit_log` quem executou/consultou. A rastreabilidade e por `motor_execucao` apenas.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Recalcular | `API.rpc('motor_v2_executar', {p_periodo})` → cria motor_execucao |
| Carregar dashboard | `API.rpc('motor_v2_dashboard_executivo')` |
| Atualizar auditoria | fetch `motor_v2_auditoria` |
| Carregar decisoes | `API.rpc('motor_decisoes')` |
| Trocar periodo | selects month/year → `getSelectedPeriod()` |
| Filtrar auditoria | client-side (`applyFilters`) |
| Marcar acao (decisoes) | checkbox local (sem persistencia) |
| Expandir diagnostico | `toggleDetail(idx)` |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (JWT, `rh_perfil_atual()`) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_api.js/api.js, rh_auth.js, shell.js, ui_components.js, page_bootstrap.js, ds.css. Externas: nenhuma (viz CSS/SVG).

### 21.3 Dependencias de Dados

rh_funcionarios, rh_setores, rh_cargos, rh_ponto, producao_rateada, sst_treinamento, sst_epi_entrega, stg_buscaepi_entregas, vw_absenteismo_recorrencia, vw_absenteismo_por_setor.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Absenteismo | F-14 RH | Fonte do motor_decisoes (vw_absenteismo_*) |
| Turnover | F-38 RH | Consome motor_score (score dos desligados) |
| Producao | F-37 CQ | producao_rateada alimenta presenca/producao |
| EPIs | F-17 SST | sst_epi_entrega alimenta EPI + decisoes SST |
| Ponto Secullum | RH | rh_ponto alimenta presenca |
| Alertas RH | RH | Deep-link do motor_decisoes |

---

## 23. Melhorias SIGA

### SIGA-01 — Padronizar Cliente de API

`motor_v2_auditoria.html` usa `fetch` direto; migrar para `API.rpc` como as demais telas.

### SIGA-02 — Restricao de Perfil no Motor de Decisoes

`motor_decisoes.html` expoe dados individuais + impacto financeiro sem restricao de perfil no cliente e sem guard na RPC — aplicar admin/rh/gestor.

### SIGA-03 — Persistir Planos de Acao

Os checkboxes de plano de acao sao locais; gravar as acoes/decisoes (com responsavel, prazo, status) para acompanhamento — ligar a `alertas_rh.html` / `acao_plano`.

### SIGA-04 — Pesos Configuraveis

Migrar os pesos das 6 dimensoes (hardcoded na RPC) para tabela de configuracao por empresa/cargo.

### SIGA-05 — Auditoria de Uso

Registrar em `rh_audit_log` quem executou o motor e consultou scores individuais.

### SIGA-06 — Carga BuscaEPI Dinamica

O total de entregas do BuscaEPI esta hardcoded (41787) em `motor_decisoes`; obter dinamicamente da API/staging.

### SIGA-07 — Agendamento do Motor

Executar o motor automaticamente (cron/n8n) no fechamento mensal, em vez de recalculo manual.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | `motor_v2_auditoria.html` chama via `fetch` direto (SB_URL/SB_KEY + rh_sess_v2), nao `API.rpc` | Inconsistencia de stack; token exposto |
| D-02 | `motor_decisoes.html` sem restricao de perfil (cliente) e `motor_decisoes()` sem guard interno | Dados individuais + financeiros a qualquer autenticado |
| D-03 | Plano de acao (checkbox) sem persistencia | Acompanhamento nao rastreado |
| D-04 | Pesos e limiares (0.20/0.10, 75/55, 41787) hardcoded na RPC | Nao parametrizavel por empresa |
| D-05 | Sem `rh_audit_log` de execucao/consulta | Sem trilha de "quem fez" |
| D-06 | Motor disparado manualmente (sem agendamento) | Scores podem ficar defasados |

### 24.2 Multiempresa

1. Coluna `empresa_id` em `motor_score`, `motor_execucao`, `motor_alerta`, `motor_indicador`, `motor_padrao`
2. Score calculado e filtrado por empresa/estabelecimento
3. Pesos e limiares por empresa (config)
4. Dashboards e auditoria com seletor de empresa
5. Fontes de dado por empresa (ponto/producao/EPI distintos)

---

## 25. Criterios de Aceite

### CA-01 — Executar Motor

Admin/RH recalcula um periodo → `motor_execucao` concluido; `motor_score` populado para todos os ativos; reexecutar sobrescreve o periodo.

### CA-02 — Ponderacao Normalizada

Funcionario sem dado de EPI/producao → score_final calculado so com as dimensoes disponiveis (peso_total < 1.0).

### CA-03 — Confiabilidade e Classificacao

fontes_ativas < 2 → dado_insuficiente independente do score; >=75 com fontes >=4 → consistente.

### CA-04 — Dashboard por Perfil

Gestor ve base + alertas de setor; admin/rh veem scores individuais + padroes; visualizador ve so base.

### CA-05 — Auditoria Restrita

Nao-admin/rh → tela "Acesso Restrito"; admin/rh veem resumo, causas, por setor e diagnostico individual.

### CA-06 — Motor de Decisoes

Detecta 4 categorias; exclui licenca maternidade/INSS; gera plano de acao com responsavel/prazo; ordena por prioridade/impacto.

### CA-07 — LGPD

Nenhuma dimensao usa dado de saude; rodape LGPD presente nas 3 telas.

### CA-08 — Idempotencia

Recalcular o mesmo periodo nao duplica scores (DELETE por periodo).

### CA-09 — Cobertura por Fonte

Auditoria mostra sem_ponto/sem_treinamento/sem_epi por setor; dashboard mostra cobertura media por fonte.

### CA-10 — Alertas e Padroes

Badge de alertas com abertos/criticos; secao de padroes oculta se vazia.

---

## 26. Casos de Teste

### CT-01 — Cargo de Confianca

Funcionario com `cargo_confianca=true` → presenca 100, fator "isento de ponto".

### CT-02 — Presenca via Producao

Sem ponto, com producao cobrindo 60% → presenca = cobertura; fonte ativa contada.

### CT-03 — Treinamento Vencido

2 de 5 treinamentos vencidos → treinamento 60%, fator "3 treinamento(s) vencido(s)".

### CT-04 — Estabilidade Novato

Admissao ha 1 mes → estabilidade ~40, fator "Periodo de experiencia".

### CT-05 — Dado Insuficiente

Funcionario so com estabilidade (1 fonte) → confiabilidade baixa → classificacao dado_insuficiente.

### CT-06 — Consistente

Score 80 com 5 fontes → confiabilidade alta → consistente.

### CT-07 — Motor de Decisoes: Recorrente

Funcionario RECORRENTE CRITICO nao-licenca → problema com risco alto e plano de 4 acoes.

### CT-08 — Exclusao Maternidade

Funcionaria RECORRENTE em licenca maternidade → vai para excluidos_por_contexto.

### CT-09 — SST/EPI Carga Incompleta

Carga BuscaEPI 40% → sem_epi vira excluido (informativo), nao problema.

### CT-10 — Recalculo Idempotente

Executar mes 2026-06 duas vezes → uma linha por funcionario (DELETE por periodo).

---

## 27. Casos Extremos

### CE-01 — Zero Dias Uteis

Periodo sem dias uteis ate hoje → assume 22 dias.

### CE-02 — Media de Setor Zero

Setor sem producao → score de producao nao calculado (evita divisao por zero).

### CE-03 — Nenhuma Fonte

Funcionario sem nenhum registro → fontes_ativas so estabilidade; qualidade baixa; dado_insuficiente.

### CE-04 — Perfil Sem Acesso

`rh_perfil_atual()='_sem_acesso'` → dashboard retorna `{erro}`.

### CE-05 — Execucao com Erro

Excecao no meio → `motor_execucao.status='erro'`, erro gravado, RAISE propaga.

### CE-06 — Score Null na Tela

Dimensao sem dado → exibida como "—".

### CE-07 — Auditoria por Nao-Admin

Acesso negado com tela "Acesso Restrito"; nenhum dado carregado.

### CE-08 — Decisoes Vazias

Nenhum problema → "Nenhum problema detectado".

### CE-09 — Checkbox de Acao

Marcar acao no motor_decisoes → visual apenas; recarregar perde o estado (sem persistencia).

### CE-10 — Alto Risco Zero

`alto_risco=0` → KPI em verde (em vez de vermelho).

---

## 28. Melhorias Futuras

### MF-01 — Score Preditivo

Usar historico de `motor_score` + desligamentos (F-38) para prever risco de saida.

### MF-02 — Drill-down por Funcionario

Ficha individual do score com evolucao temporal e detalhamento por dimensao.

### MF-03 — Recomendacoes Automaticas

Sugerir acoes por padrao detectado (treinamento vencido → agendar; EPI faltante → entregar).

### MF-04 — Integracao com Lideranca

Ligar score a avaliacoes e metas (`lideranca.html`).

### MF-05 — Realtime de Alertas

Supabase Realtime para notificar novos alertas criticos.

### MF-06 — Explicabilidade por Funcionario

Mostrar a decomposicao do score_final (contribuicao de cada dimensao) na ficha.

### MF-07 — Metas por Dimensao

Definir metas de cobertura por fonte/setor e acompanhar o fechamento da lacuna de dados.

### MF-08 — Auto-execucao Mensal

Rotina n8n/cron que executa o motor no fechamento e notifica o resultado.

---

*Documento gerado em 28/07/2026 — Classic IA — Motor Score v2*
*Versao 1.0 — Especificacao Funcional Completa F-40*
