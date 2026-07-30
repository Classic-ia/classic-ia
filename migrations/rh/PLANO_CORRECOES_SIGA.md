# Plano de Correcoes — Auditoria Funcional SIGA (F-37 a F-46)

**Data:** 2026-07-30
**Base:** Divergencias documentadas nas specs `F37..F46_*_spec.md` (secao 24 de cada uma), `DRIFT_MAP.md` e `PLANO_MIGRACAO_SIGA.md`
**Objetivo:** roteiro executavel para eliminar os defeitos encontrados e entregar os modulos funcionando corretamente no SIGA.

---

## 0. Principio orientador

O `PLANO_MIGRACAO_SIGA.md` preve que a Fase 3 **reescreve todo o acesso a dados** (PostgREST direto → API Node v1). Portanto:

- **Correcoes de BANCO** (unificacao de tabelas, RLS, constraints, jobs, RPCs) tem prioridade — sobrevivem a migracao e beneficiam o SIGA diretamente.
- **Correcoes de FRONTEND** devem ser cirurgicas — apenas o minimo para destravar fluxos **quebrados hoje**, porque essas chamadas serao reescritas na Fase 3 de qualquer forma.
- Cada correcao de banco deve nascer como **migration versionada** em `migrations/rh/` (numerada na ORDEM_REBUILD), nunca como DDL manual — o drift atual e consequencia direta de DDL manual.

---

## 1. Sumario dos problemas por severidade

| Severidade | Problema | Modulos | Status hoje |
|------------|----------|---------|-------------|
| **P0 — Quebrado** | Telas SST leem `rh_*` e gravam `sst_*` (colunas/CHECKs divergentes) | F-43, F-44, F-45 | Cadastro invisivel; no F-45 o INSERT provavelmente **falha** |
| **P0 — Quebrado** | Registro de desligamento faz PATCH direto bloqueado por trigger | F-38 | Fluxo principal pode falhar em prod |
| **P0 — Quebrado** | Colunas divergentes dentro de cipa.html (proxima/ata/presencas/mandato_id) | F-46 | KPIs e ata sempre vazios; presencas perdidas |
| **P0 — Quebrado** | Tela de pagamentos usa tabelas nao deployadas (`rh_dados_bancarios`, `rh_lotes_pagamento`) | F-41 | Tela fantasma (carrega vazia, nada persiste) |
| **P1 — Incompleto** | Aprovacao no pipeline nao cria funcionario (RPC de admissao ociosa) | F-39 | Ciclo candidato→colaborador interrompido |
| **P1 — Incompleto** | Ferias sem UI de programacao e sem job de vencimento | F-42 | Vencidas dependem de marcacao manual |
| **P1 — Incompleto** | Denuncias CIPA write-only (sem triagem); empresa descartada no POST | F-46 | Canal existe, gestao nao |
| **P2 — Seguranca/LGPD** | RLS permissivo (`USING(true)` + policies `anon`) em dezenas de tabelas | F-37..F-46 | Dados de RH/candidatos/salarios expostos a anon |
| **P2 — Seguranca/LGPD** | Telas sem checagem de perfil; RPC `motor_decisoes` sem guard | F-39..F-46, F-40 | Qualquer autenticado edita tudo |
| **P3 — Consistencia** | Frontend ignora RPCs/views prontas; formulas divergentes (turnover) | F-38, F-40 | Numeros diferentes entre telas |
| **P3 — Consistencia** | Parametros hardcoded (pesos motor, HHT, valores de beneficio, cidades, 41787) | F-40, F-41, F-45 | Nao parametrizavel por empresa |
| **P3 — Consistencia** | Stack de API inconsistente (rh_api.js / fetch direto vs api.js) | F-39, F-40, F-41 | Manutencao fragmentada |
| **P4 — Conformidade** | CAT sem prazo/obrigatoriedade; NR-05 sem dimensionamento/eleicao; eSocial manual | F-43, F-45, F-46 | Risco legal |

---

## 2. P0 — Destravar o que esta quebrado (Sprint 1, ~1 semana)

### 2.1 Unificacao SST (F-43 ASOs, F-44 Treinamentos, F-45 Acidentes)

**Decisao:** a familia `sst_*` e a canonica — ja e consumida por `sst_dashboard_completo`, ficha 360 e monitor eSocial. As `rh_*` (rh_asos, rh_aso, rh_treinamentos, rh_treinamentos_tipos, rh_acidentes, rh_asos_config) viram legado.

**Migration `UNIFICACAO_SST.sql` (nova, versionada):**

1. **Ampliar `sst_acidente`** para cobrir a taxonomia real da operacao:
   - `ALTER ... DROP CONSTRAINT` do CHECK de `tipo_acidente` e recriar com os 5 tipos da tela (`com_afastamento`, `sem_afastamento`, `trajeto`, `incidente`, `doenca_ocupacional`);
   - idem para `status` (`registrado`, `investigando`, `concluido`);
   - `ADD COLUMN` faltantes: `investigacao_5porques JSONB`, `acoes_corretivas JSONB`, `licoes_aprendidas TEXT`, `testemunhas TEXT`, `medidas_imediatas TEXT` (as demais ja existem com outro nome — ver item 3).
2. **Adicionar em `sst_aso`**: coluna `exames_complementares TEXT[]` (tirar do texto de observacoes).
3. **Migrar dados** de `rh_asos`/`rh_aso` → `sst_aso`, `rh_treinamentos` → `sst_treinamento`, `rh_acidentes` → `sst_acidente` (INSERT..SELECT com mapeamento de colunas e dedup por funcionario+data+tipo).
4. **Congelar as `rh_*`**: `REVOKE INSERT/UPDATE/DELETE` + comentario de deprecacao (drop so apos homologacao).
5. **Versionar** `rh_asos_config` → nova `sst_aso_config` (risco/periodicidade por cargo) e `rh_treinamentos_tipos` → usar `sst_tipo_treinamento` (ja existe, com carga_horaria_minima).

**Frontend (correcao cirurgica, 3 arquivos):**

- `asos.html`: trocar `API.get('rh_asos...')` → `sst_aso` e alinhar nomes (`tipo_aso`→`tipo_exame`, `data_realizacao`→`data_exame`, `data_vencimento`→`data_validade`, `medico_nome`→`medico_responsavel`, `medico_crm`→`crm`); config → `sst_aso_config`.
- `treinamentos.html`: GET → `sst_treinamento` (embed `sst_tipo_treinamento`, `rh_funcionarios`); alinhar `colaborador_id`→`funcionario_id`, `tipo_id`→`tipo_treinamento_id`, `data_vencimento`→`data_validade`, `instituicao`→`instrutor`; tipos → `sst_tipo_treinamento`.
- `acidentes.html`: GET → `sst_acidente`; alinhar `tipo`→`tipo_acidente`, `data_hora`→`data_ocorrencia`+`hora_ocorrencia`, `local_setor`→`local_ocorrencia`, `numero_cat`→`cat_numero`, `dias_afastados`→`dias_afastamento`.

**Criterio de aceite:** cadastrar ASO/treinamento/acidente → registro reaparece imediatamente no painel; dashboard SST, ficha 360 e monitor eSocial veem o mesmo dado.

### 2.2 Desligamento via workflow (F-38)

O backend ja existe e esta aplicado (`rh_abrir_desligamento` + `rh_executar_desligamento` + trigger de bloqueio). Correcao so no frontend:

- `desligamentos_turnover.html` (`salvarDesligamento`): substituir `POST rh_desligamentos` + `PATCH rh_funcionarios` por `API.rpc('rh_abrir_desligamento', {...})`, exibir o checklist rescisorio (PATCH `rh_proc_desligamento`) e botao "Efetivar" → `API.rpc('rh_executar_desligamento', {p_processo_id})`.
- Manter o INSERT em `rh_desligamentos` **dentro** de `rh_executar_desligamento` (ajustar a RPC para gravar tambem o registro analitico + entrevista) — assim o dashboard de turnover continua funcionando sem mudanca.
- Padronizar status de saida: `'desligado'` em todo lugar (corrigir `registrar_desligamento_completo` que usa `'inativo'`).

### 2.3 CIPA — colunas e presencas (F-46)

Correcao em 1 arquivo (`cipa.html`), sem migration:

- Ler `proxima_data` (nao `proxima_reuniao`) e `ata_texto` (nao `ata`); gravar `deliberacoes` em coluna propria (`ALTER TABLE cipa_reunioes ADD COLUMN deliberacoes TEXT` — unica DDL);
- dropdowns/edicao usarem `funcionario_id` (nao `colaborador_id`);
- gravar `mandato_id` em `cipa_acoes` e filtrar acoes pelo mandato corrente;
- persistir presencas: apos POST da reuniao, batch INSERT em `cipa_presencas` com os `selectedPresentes`;
- canal de denuncia: incluir `empresa_id` no payload (mapear slug→uuid de `rh_empresas`).

### 2.4 Pagamentos (F-41)

**Decisao:** nao deployar as `rh_*` fantasma — **migrar a tela para a familia `fp_*`** (`fp_lote_pagamento`, `fp_pagamento_funcionario`), que e o modelo que a folha ja usa. Dados bancarios: deployar apenas `rh_dados_bancarios` (unica sem equivalente em `fp_*`), via migration versionada com RLS restrita (rh/admin/financeiro).

---

## 3. P1 — Completar fluxos interrompidos (Sprint 2, ~1 semana)

### 3.1 Admissao a partir do pipeline (F-39)

Em `recrutamento_selecao.html`, ao mover para `aprovado`:
1. Abrir modal de admissao (cargo_id, setor_id, empresa/filial, salario proposto, data prevista — os campos de `rh_proc_admissao`);
2. `INSERT rh_processo (tipo='admissao')` + `rh_proc_admissao`; avancar status conforme a maquina de estados; `API.rpc('rh_executar_admissao', {p_processo_id})` cria o funcionario;
3. Gravar `rs_candidatos.funcionario_criado_id` para rastreabilidade candidato→colaborador.

### 3.2 Ferias operacional (F-42)

1. **Migration:** `CHECK (status IN ('vencida','pendente','programada','em_gozo','gozada'))` em `rh_ferias` + function `ferias_atualizar_vencidas()` (promove `pendente`→`vencida` quando `periodo_aquisitivo_fim + interval '12 months' < CURRENT_DATE`) agendada via pg_cron/n8n diario;
2. **Tela:** aba "Programar" em `ferias.html` (form sobre rh_ferias: periodo, data inicio, dias, abono/dias vendidos) — CRUD simples;
3. **ETL:** ativar o sync `stg_convenia_ferias` → `rh_ferias` (mapeamento payload_json→colunas, dedup por hash).

### 3.3 Triagem de denuncias (F-46)

Nova aba "Denuncias" em `cipa.html` (admin/rh/sst): lista `cipa_denuncias` por status, resposta + `respondido_por/em`, transicao `aberta → em_analise → respondida`. Consulta publica opcional por protocolo (SELECT anon por id exato).

---

## 4. P2 — Seguranca e LGPD (Sprint 3, ~1 semana — pode rodar em paralelo)

### 4.1 Hardening RLS (migration `HARDENING_RLS_MODULOS.sql`)

Aplicar o **padrao do `cipa_acoes`** (o melhor do sistema: SELECT autenticado; escrita por perfil via `rh_usuarios`) a todas as tabelas com policy permissiva:

| Grupo | Tabelas | Escrita permitida |
|-------|---------|-------------------|
| Desligamentos | rh_desligamento_taxonomia/categorias, rh_entrevista_perguntas, rh_desligamento_entrevista | administrador, rh |
| Recrutamento | rs_vagas, rs_candidatos, rs_pipeline, rs_entrevistas, rs_avaliacoes, rs_banco_talentos | administrador, rh, gestor |
| Beneficios/Pagamentos | rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca, rh_dados_bancarios | administrador, rh (+financeiro p/ dados bancarios) |
| SST | sst_aso, sst_treinamento, sst_acidente, sst_aso_config, esocial_eventos | administrador, rh, sst |
| CIPA | cipa_mandato/membros/reunioes/presencas | administrador, rh (denuncias: INSERT anon mantido, SELECT restrito) |
| CQ (projeto CQ) | cq_sal_*, cq_ficha_producao | remover SELECT anon; escrita producao/qualidade/admin |

**Remover TODAS as policies `anon`** exceto `cipa_denuncias INSERT` (canal publico por design).

### 4.2 Guards de perfil

- RPC `motor_decisoes()`: adicionar guard `rh_perfil_atual() IN ('administrador','rh','gestor')`;
- Frontend: gating por perfil nas telas de escrita (padrao `isReadOnly` do beneficios.html, ja pronto para copiar) — exportar_pagamentos, relatorio_beneficios, asos, treinamentos, acidentes, cipa, monitor-esocial;
- Padronizar cliente: substituir `rh_api.js`/fetch direto por `api.js` em recrutamento_selecao, exportar_pagamentos e motor_v2_auditoria (3 arquivos).

### 4.3 Auditoria

`RHAuth.log` nas acoes hoje sem trilha (recrutamento: mover etapa/aprovar; treinamentos: cadastrar; cipa: mandato/membro/reuniao) e INSERT em `rh_audit_log` nas RPCs de execucao (motor, admissao, desligamento — desligamento ja tem).

---

## 5. P3 — Consistencia de dados e parametros (Sprint 4, ~1 semana)

1. **Fonte unica de indicadores:** `desligamentos_turnover.html` passa a consumir `indicadores_turnover`/`analise_cruzada_desligamentos` (formula canonica = headcount medio da RPC); eliminar o calculo client-side divergente. Mesmo para o status eSocial: view `vw_esocial_eventos` com status recalculado por prazo (o trigger `esocial_calcular_status` ja cobre o write; a view cobre a leitura).
2. **Tabela de parametros** (`rh_parametros` chave/valor por empresa): pesos e limiares do motor (0.20/0.10, 75/55), HHT mensal, valores de beneficio (VA 300, desconto 8%, assiduidade 150), total BuscaEPI (41787), cidades com transporte. RPCs e telas leem da tabela.
3. **Catalogo de produtos CQ:** resolver a colisao `45084` (Bucho Congelado × Classe A) com tabela unica de produtos MP × acabado (F-37 SIGA-05).
4. **Persistir plano de acao do motor (F-40):** gravar as acoes geradas por `motor_decisoes` em `acao_plano`/`motor_alerta` com responsavel/prazo/status — o checkbox passa a fazer PATCH.
5. **Dump do drift:** `pg_dump --schema-only` das 91 views + tabelas de prod nao versionadas → repo (fecha o item 1 do DRIFT_MAP).

---

## 6. P4 — Conformidade legal (Sprint 5+, continuo)

1. **CAT (F-45):** obrigatoria para `com_afastamento`/`fatal`; KPI "CATs pendentes" com prazo de 1 dia util; bloqueio de encerramento sem CAT quando exigivel.
2. **eSocial (F-43):** cobrir S-2240 (geracao de `esocial_s2240`) e S-2230 (afastamentos); na Fase 3 do SIGA, transmissao real (XML + webservice gov) substituindo o protocolo manual.
3. **NR-05 (F-46):** dimensionamento (Quadro I por grau de risco/CNAE — colunas ja existem em cipa_mandato), alerta de eleicao 60 dias antes do fim do mandato, verificacao de reuniao mensal, vinculo treinamento CIPA 20h ↔ sst_treinamento.
4. **NR-07 (F-43):** mover calculo de validade/status do ASO para function no banco (fonte unica).
5. **Rescisao (F-38):** persistir auditorias de TRCT em tabela (`rh_auditoria_rescisao`) e implementar INSS/IRRF progressivos no motor de conferencia.
6. **Ferias (F-42):** aviso de ferias (30 dias) e recibo; provisao contabil.

---

## 7. Sequenciamento e esforco

| Sprint | Foco | Entregas | Esforco estimado |
|--------|------|----------|------------------|
| S1 (P0) | Destravar quebrados | UNIFICACAO_SST.sql + 3 telas SST corrigidas + workflow desligamento no front + cipa.html colunas + decisao fp_* | 4-6 dias |
| S2 (P1) | Completar fluxos | Admissao no pipeline + ferias (job+aba+ETL) + triagem denuncias | 4-5 dias |
| S3 (P2) | Seguranca | HARDENING_RLS_MODULOS.sql + guards + padronizacao api.js + auditoria | 3-4 dias |
| S4 (P3) | Consistencia | RPCs como fonte unica + rh_parametros + catalogo CQ + dump drift | 4-5 dias |
| S5+ (P4) | Conformidade | CAT/eSocial/NR-05/NR-07/rescisao/ferias legais | continuo |

**Regras de execucao:**
- Toda DDL entra como migration numerada em `migrations/rh/` e na `ORDEM_REBUILD.sql`;
- Cada correcao de tela fecha com o teste do criterio de aceite da spec correspondente (secao 25 de cada F-XX);
- Apos S1, rodar smoke test dos 3 fluxos SST (cadastrar → reaparece) e do desligamento completo (abrir → checklist → efetivar) em staging antes de prod;
- As correcoes P0/P1 de frontend sao deliberadamente minimas — a Fase 3 do SIGA (API v1) reescreve essas chamadas; o que fica de duradouro e o banco arrumado.

---

## 8. Mapa problema → correcao (rastreabilidade)

| Spec | Divergencia | Correcao neste plano |
|------|-------------|----------------------|
| F-37 D-01/D-02 | RLS permissivo sal/ficha; colisao 45084 | 4.1 / 5.3 |
| F-38 D-01..D-07 | PATCH bloqueado; inativo/desligado; RPCs ignoradas; [CATEGORIA]; localStorage; RLS anon | 2.2 / 5.1 / 6.5 / 4.1 |
| F-39 D-01..D-06 | Admissao incompleta; RLS anon; rh_api.js; sem perfil; sem audit; sem validacao CPF | 3.1 / 4.1 / 4.2 / 4.3 |
| F-40 D-01..D-06 | fetch direto; motor_decisoes sem guard; plano nao persistido; hardcoded; sem audit; sem agenda | 4.2 / 5.2 / 5.4 |
| F-41 D-01..D-08 | Tabelas fantasma; rh_* vs fp_*; sem CNAB; sem perfil; custo superestimado; hardcoded | 2.4 / 4.2 / 5.2 |
| F-42 D-01..D-07 | Read-only; sem CHECK; sem job de vencimento; ETL pendente | 3.2 |
| F-43 D-01..D-08 | Le rh_asos/grava sst_aso; 3 tabelas; config drift; status eSocial defasado | 2.1 / 5.1 / 6.2 / 6.4 |
| F-44 D-01..D-08 | Le rh_treinamentos/grava sst_treinamento; matriz nao usada; KPI errado | 2.1 / 6.3 (matriz na fase NR) |
| F-45 D-01..D-08 | Payload viola CHECKs; HHT hardcoded; CAT sem prazo | 2.1 / 5.2 / 6.1 |
| F-46 D-01..D-10 | Colunas divergentes; presencas perdidas; denuncia sem empresa/triagem; NR-05 parcial | 2.3 / 3.3 / 6.3 |

---

*Documento gerado em 30/07/2026 — Classic IA — Plano de Correcoes da Auditoria SIGA*
