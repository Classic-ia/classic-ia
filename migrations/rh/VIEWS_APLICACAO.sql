-- ============================================================================
-- VIEWS_APLICACAO.sql
-- Views de aplicacao exportadas do Supabase producao (muiqmtnfvyffborgiwdw)
-- Data de exportacao: 2026-04-13
--
-- Proposito: Definicoes CREATE OR REPLACE VIEW para as 16 views que sao
-- dependencias das RPCs definidas em FUNCOES_APLICACAO.sql mas que nunca
-- foram versionadas no repositorio.
--
-- Cadeia de dependencia:
--   Tabelas base -> Views (este arquivo) -> RPCs (FUNCOES_APLICACAO.sql)
--
-- Ordem de criacao:
--   Grupo 1: Views base (dependem apenas de tabelas)
--   Grupo 2: Views que dependem de motor_score com CTE ultimo_periodo
--   Grupo 3: Views dependentes de outras views (vw_absenteismo_recorrencia)
--
-- IMPORTANTE: Executar na ordem listada. O Grupo 3 depende do Grupo 1.
-- ============================================================================

-- ============================================================================
-- GRUPO 1: Views base (sem dependencia de outras views)
-- ============================================================================

-- --------------------------------------------------------------------------
-- vw_absenteismo_real
-- Dependencia de: vw_absenteismo_recorrencia (Grupo 3)
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_absenteismo_real AS
SELECT f.id AS funcionario_id, f.nome_completo, c.nome AS cargo, s.nome AS setor,
  f.salario_base, round(f.salario_base / 22, 2) AS salario_dia,
  (d.payload_json ->> 'data_inicio')::date AS data_inicio,
  (d.payload_json ->> 'data_fim')::date AS data_fim,
  (d.payload_json ->> 'dias')::integer AS dias,
  d.payload_json ->> 'cid' AS cid,
  d.payload_json ->> 'tipo' AS tipo_original,
  CASE WHEN (d.payload_json ->> 'dias')::integer <= 3 THEN 'curto'
    WHEN (d.payload_json ->> 'dias')::integer <= 14 THEN 'medio'
    ELSE 'longo_inss' END AS faixa,
  CASE WHEN (d.payload_json ->> 'dias')::integer <= 14 THEN (d.payload_json ->> 'dias')::integer ELSE 14 END AS dias_empresa,
  CASE WHEN (d.payload_json ->> 'dias')::integer > 14 THEN (d.payload_json ->> 'dias')::integer - 14 ELSE 0 END AS dias_inss,
  round(LEAST((d.payload_json ->> 'dias')::integer, 14)::numeric * (f.salario_base / 22) *
    CASE WHEN (d.payload_json ->> 'dias')::integer <= 3 THEN 1.5 ELSE 1.3 END, 2) AS custo_empresa_real,
  round(LEAST((d.payload_json ->> 'dias')::integer, 14)::numeric * (f.salario_base / 22), 2) AS custo_empresa_bruto
FROM stg_convenia_documentos d
JOIN rh_funcionarios f ON f.id = (d.payload_json ->> 'funcionario_id')::uuid
JOIN rh_cargos c ON c.id = f.cargo_id
JOIN rh_setores s ON s.id = f.setor_id
WHERE d.origem_sistema = 'manual';

-- --------------------------------------------------------------------------
-- vw_absenteismo_por_setor
-- Usada por: motor_decisoes()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_absenteismo_por_setor AS
SELECT s.nome AS setor,
  count(DISTINCT f.id) AS funcionarios_afetados,
  (SELECT count(*) FROM rh_funcionarios ff WHERE ff.setor_id = s.id AND ff.status IN ('ativo','ferias')) AS total_setor,
  count(*) AS atestados,
  sum((d.payload_json ->> 'dias')::integer) AS total_dias,
  round(sum((d.payload_json ->> 'dias')::integer)::numeric * avg(f.salario_base) / 22, 2) AS custo_estimado
FROM stg_convenia_documentos d
JOIN rh_funcionarios f ON f.id = (d.payload_json ->> 'funcionario_id')::uuid
JOIN rh_setores s ON s.id = f.setor_id
WHERE d.origem_sistema = 'manual'
GROUP BY s.id, s.nome
ORDER BY sum((d.payload_json ->> 'dias')::integer) DESC;

-- --------------------------------------------------------------------------
-- vw_atestados_analise
-- Usada por: relatorio_atestados_dashboard()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_atestados_analise AS
SELECT oc.id, f.nome_completo AS funcionario, s.nome AS setor,
  oc.data_inicio, oc.data_fim, oc.dias, oc.competencia,
  oc.observacoes AS categoria, oc.arquivo_nome,
  NULL::text AS cid, NULL::text AS grupo_cid,
  NULL::text AS medico_crm, NULL::text AS local_atendimento,
  EXTRACT(year FROM oc.data_inicio)::integer AS ano,
  EXTRACT(month FROM oc.data_inicio)::integer AS mes
FROM rh_ocorrencias oc
JOIN rh_funcionarios f ON f.id = oc.colaborador_id
LEFT JOIN rh_setores s ON s.id = f.setor_id
WHERE oc.tipo = 'atestado'
UNION ALL
SELECT o.id, COALESCE(f.nome_completo, o.funcionario_nome) AS funcionario,
  s.nome AS setor, o.data_documento AS data_inicio, NULL::date AS data_fim,
  o.dias_afastamento AS dias, NULL::text AS competencia,
  o.gravidade AS categoria, o.arquivo_nome, o.cid,
  CASE WHEN o.cid LIKE 'M%' THEN 'Musculoesqueletico'
    WHEN o.cid LIKE 'K%' THEN 'Digestivo'
    WHEN o.cid LIKE 'J%' THEN 'Respiratorio'
    WHEN o.cid LIKE 'R%' THEN 'Sintomas gerais'
    WHEN o.cid LIKE 'S%' OR o.cid LIKE 'T%' THEN 'Trauma/Lesao'
    WHEN o.cid LIKE 'F%' THEN 'Saude mental'
    WHEN o.cid LIKE 'G%' THEN 'Neurologico'
    WHEN o.cid LIKE 'H%' THEN 'Olhos/Ouvido'
    WHEN o.cid LIKE 'N%' THEN 'Geniturinario'
    WHEN o.cid LIKE 'Z%' THEN 'Fatores saude'
    ELSE 'Outro' END AS grupo_cid,
  o.crm AS medico_crm,
  CASE WHEN o.texto_extraido ILIKE '%santa casa%' THEN 'Santa Casa'
    WHEN o.texto_extraido ILIKE '%hospital municipal%' THEN 'Hospital Municipal'
    WHEN o.texto_extraido ILIKE '%UBS%' OR o.texto_extraido ILIKE '%unidade basica%' THEN 'UBS'
    WHEN o.texto_extraido ILIKE '%UPA%' THEN 'UPA'
    WHEN o.texto_extraido ILIKE '%clinica%' THEN 'Clinica'
    WHEN o.texto_extraido ILIKE '%hospital%' THEN 'Hospital'
    WHEN o.texto_extraido ILIKE '%jardim alegre%' THEN 'Hospital Jardim Alegre'
    ELSE NULL END AS local_atendimento,
  o.ano_referencia AS ano, o.mes_referencia AS mes
FROM rh_documento_ocr o
LEFT JOIN rh_funcionarios f ON f.id = o.funcionario_id
LEFT JOIN rh_setores s ON s.id = f.setor_id
WHERE o.tipo_documento = 'atestado';

-- --------------------------------------------------------------------------
-- vw_motor_alertas_abertos
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_alertas_abertos AS
SELECT id, tipo, prioridade, titulo, descricao, referencia_tipo, referencia_nome, dados, status, responsavel, created_at
FROM motor_alerta
WHERE status IN ('aberto','em_andamento')
ORDER BY CASE prioridade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2 WHEN 'media' THEN 3 WHEN 'baixa' THEN 4 END, created_at DESC;

-- --------------------------------------------------------------------------
-- vw_motor_gestor_alertas
-- Usada por: motor_v2_dashboard_executivo() (escopo gestor)
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_gestor_alertas AS
SELECT id, tipo, prioridade, titulo, descricao, referencia_nome, dados, status, created_at
FROM motor_alerta
WHERE status IN ('aberto','em_andamento') AND referencia_tipo = 'setor'
ORDER BY CASE prioridade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2 WHEN 'media' THEN 3 WHEN 'baixa' THEN 4 END;

-- --------------------------------------------------------------------------
-- vw_motor_padroes
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_padroes AS
SELECT tipo, referencia_tipo, referencia_nome, periodo, descricao, severidade, dados, created_at
FROM motor_padrao WHERE created_at >= CURRENT_DATE - 90
ORDER BY CASE severidade WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'baixa' THEN 3 END;

-- --------------------------------------------------------------------------
-- vw_motor_evolucao_mensal
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_evolucao_mensal AS
SELECT periodo_referencia, count(*) AS total, round(avg(score_final),1) AS score_medio,
  round(avg(score_qualidade),1) AS qualidade_media, round(avg(fontes_ativas),1) AS fontes_media,
  count(*) FILTER (WHERE classificacao = 'consistente') AS consistentes,
  count(*) FILTER (WHERE classificacao = 'regular') AS regulares,
  count(*) FILTER (WHERE classificacao = 'dado_insuficiente') AS dado_insuf
FROM motor_score GROUP BY periodo_referencia ORDER BY periodo_referencia;

-- ============================================================================
-- GRUPO 2: Views dependentes de motor_score com CTE ultimo_periodo
-- ============================================================================

-- --------------------------------------------------------------------------
-- vw_motor_kpis_executivos
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_kpis_executivos AS
WITH ultima_exec AS (
  SELECT id FROM motor_execucao
  WHERE status = 'concluido' AND tipo IN ('completo','indicadores')
  ORDER BY created_at DESC LIMIT 1
)
SELECT i.categoria, i.indicador, i.valor, i.unidade, i.referencia_tipo, i.referencia_nome, i.periodo_inicio, i.periodo_fim
FROM motor_indicador i JOIN ultima_exec e ON i.execucao_id = e.id
ORDER BY i.categoria, i.indicador;

-- --------------------------------------------------------------------------
-- vw_motor_score_resumo
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_score_resumo AS
WITH ultimo_periodo AS (SELECT max(periodo_referencia) AS periodo FROM motor_score)
SELECT s.classificacao, s.confiabilidade, count(*) AS total, round(avg(s.score_final),1) AS score_medio,
  round(avg(s.fontes_ativas),1) AS fontes_media, round(avg(s.score_qualidade),1) AS qualidade_media
FROM motor_score s JOIN ultimo_periodo p ON s.periodo_referencia = p.periodo
GROUP BY s.classificacao, s.confiabilidade
ORDER BY CASE s.classificacao WHEN 'consistente' THEN 1 WHEN 'regular' THEN 2 WHEN 'monitorar' THEN 3 WHEN 'atencao_operacional' THEN 4 WHEN 'dado_insuficiente' THEN 5 END;

-- --------------------------------------------------------------------------
-- vw_motor_confiabilidade_dado
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_confiabilidade_dado AS
WITH ultimo_periodo AS (SELECT max(periodo_referencia) AS periodo FROM motor_score)
SELECT s.setor_nome, count(*) AS total,
  count(*) FILTER (WHERE s.confiabilidade = 'alta') AS conf_alta,
  count(*) FILTER (WHERE s.confiabilidade = 'media') AS conf_media,
  count(*) FILTER (WHERE s.confiabilidade = 'baixa') AS conf_baixa,
  count(*) FILTER (WHERE s.confiabilidade = 'indeterminada') AS conf_indet,
  round(avg(s.fontes_ativas),1) AS fontes_media,
  round(avg(s.cobertura_presenca),1) AS cob_presenca,
  round(avg(s.cobertura_producao),1) AS cob_producao,
  round(avg(s.cobertura_treinamento),1) AS cob_treinamento,
  round(avg(s.cobertura_epi),1) AS cob_epi
FROM motor_score s JOIN ultimo_periodo p ON s.periodo_referencia = p.periodo
GROUP BY s.setor_nome ORDER BY round(avg(s.fontes_ativas),1) DESC;

-- --------------------------------------------------------------------------
-- vw_motor_setor_risco
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_setor_risco AS
WITH ultimo_periodo AS (SELECT max(periodo_referencia) AS periodo FROM motor_score)
SELECT s.setor_nome, count(*) AS total_funcionarios,
  round(avg(s.score_final),1) AS score_medio, min(s.score_final) AS score_minimo, max(s.score_final) AS score_maximo,
  count(*) FILTER (WHERE s.classificacao = 'consistente') AS consistentes,
  count(*) FILTER (WHERE s.classificacao = 'regular') AS regulares,
  count(*) FILTER (WHERE s.classificacao = 'monitorar') AS monitorar,
  count(*) FILTER (WHERE s.classificacao = 'atencao_operacional') AS atencao_op,
  count(*) FILTER (WHERE s.classificacao = 'dado_insuficiente') AS dado_insuf,
  round(avg(s.fontes_ativas),1) AS fontes_media, round(avg(s.score_qualidade),1) AS qualidade_media
FROM motor_score s JOIN ultimo_periodo p ON s.periodo_referencia = p.periodo
GROUP BY s.setor_nome ORDER BY round(avg(s.score_final),1);

-- --------------------------------------------------------------------------
-- vw_motor_scores_detalhado
-- Usada por: motor_v2_dashboard_executivo()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_scores_detalhado AS
WITH ultimo_periodo AS (SELECT max(periodo_referencia) AS periodo FROM motor_score)
SELECT s.funcionario_id, s.funcionario_nome, s.setor_nome, s.cargo_nome,
  s.score_presenca, s.score_producao, s.score_treinamento, s.score_estabilidade,
  s.score_epi, s.score_qualidade, s.score_final, s.classificacao, s.confiabilidade,
  s.cobertura_presenca, s.cobertura_producao, s.cobertura_treinamento, s.cobertura_epi,
  s.fontes_ativas, s.fatores, s.periodo_referencia
FROM motor_score s JOIN ultimo_periodo p ON s.periodo_referencia = p.periodo
ORDER BY s.score_final;

-- --------------------------------------------------------------------------
-- vw_motor_setor_nao_consistente
-- Usada por: motor_v2_auditoria()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_setor_nao_consistente AS
WITH ultimo AS (SELECT max(periodo_referencia) AS p FROM motor_score)
SELECT s.setor_nome, count(*) AS total_nao_consistente,
  count(*) FILTER (WHERE s.classificacao = 'atencao_operacional') AS atencao_op,
  count(*) FILTER (WHERE s.classificacao = 'monitorar') AS monitorar,
  count(*) FILTER (WHERE s.classificacao = 'dado_insuficiente') AS dado_insuf,
  count(*) FILTER (WHERE s.cobertura_presenca = 0) AS sem_ponto,
  count(*) FILTER (WHERE s.cobertura_treinamento = 0) AS sem_treinamento,
  count(*) FILTER (WHERE s.cobertura_epi = 0) AS sem_epi,
  round(avg(s.score_final),1) AS score_medio, round(avg(s.fontes_ativas),1) AS fontes_media
FROM motor_score s JOIN ultimo u ON s.periodo_referencia = u.p
WHERE s.classificacao != 'consistente'
GROUP BY s.setor_nome ORDER BY count(*) DESC;

-- --------------------------------------------------------------------------
-- vw_motor_causas_perda
-- Usada por: motor_v2_auditoria()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_causas_perda AS
WITH ultimo AS (SELECT max(periodo_referencia) AS p FROM motor_score),
causas AS (
  SELECT CASE
    WHEN s.cobertura_treinamento = 0 AND s.cobertura_epi = 0 AND s.cobertura_presenca = 0 THEN 'Sem nenhum registro (ponto/EPI/treinamento)'
    WHEN s.cobertura_treinamento = 0 AND s.cobertura_epi = 0 THEN 'Sem treinamento + sem EPI'
    WHEN s.cobertura_treinamento = 0 THEN 'Sem registro de treinamento'
    WHEN s.cobertura_epi = 0 THEN 'Sem registro de EPI'
    WHEN s.cobertura_presenca = 0 THEN 'Sem registro de ponto'
    WHEN s.score_presenca < 50 THEN 'Presenca baixa (operacional)'
    ELSE 'Outro' END AS causa,
  CASE
    WHEN s.cobertura_presenca = 0 OR s.cobertura_epi = 0 OR s.cobertura_treinamento = 0 THEN 'qualidade_dado'
    WHEN s.score_presenca < 50 THEN 'operacional'
    ELSE 'misto' END AS tipo,
  s.classificacao, s.setor_nome
  FROM motor_score s JOIN ultimo u ON s.periodo_referencia = u.p
  WHERE s.classificacao != 'consistente'
)
SELECT causa, tipo, count(*) AS funcionarios,
  string_agg(DISTINCT setor_nome, ', ' ORDER BY setor_nome) AS setores_afetados
FROM causas GROUP BY causa, tipo ORDER BY count(*) DESC;

-- --------------------------------------------------------------------------
-- vw_motor_auditoria_score
-- Usada por: motor_v2_auditoria()
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_motor_auditoria_score AS
WITH ultimo AS (SELECT max(periodo_referencia) AS p FROM motor_score)
SELECT s.funcionario_nome, s.setor_nome, s.cargo_nome, s.classificacao, s.confiabilidade,
  s.score_final, s.score_presenca, s.score_producao, s.score_treinamento, s.score_estabilidade,
  s.score_epi, s.score_qualidade, s.fontes_ativas, s.cobertura_presenca, s.cobertura_producao,
  s.cobertura_treinamento, s.cobertura_epi,
  CASE
    WHEN s.fontes_ativas <= 1 THEN 'dado_ausente'
    WHEN s.cobertura_presenca = 0 AND s.cobertura_epi = 0 AND s.cobertura_treinamento = 0 THEN 'dado_ausente'
    WHEN s.score_presenca < 50 AND s.cobertura_presenca > 50 THEN 'operacional'
    WHEN s.cobertura_treinamento = 0 AND s.cobertura_epi = 0 THEN 'dado_parcial'
    WHEN s.score_presenca >= 80 AND s.score_final < 75 THEN 'dado_parcial'
    ELSE 'misto' END AS tipo_problema,
  CASE
    WHEN s.cobertura_presenca = 0 AND s.cobertura_epi = 0 THEN 'Sem ponto e sem EPI registrado'
    WHEN s.cobertura_treinamento = 0 AND s.cobertura_epi = 0 THEN 'Sem treinamento e sem EPI'
    WHEN s.cobertura_treinamento = 0 THEN 'Sem registro de treinamento'
    WHEN s.cobertura_epi = 0 THEN 'Sem registro de EPI'
    WHEN s.score_presenca < 50 AND s.cobertura_presenca > 0 THEN 'Baixa presenca'
    WHEN s.cobertura_presenca = 0 THEN 'Sem registro de ponto'
    ELSE 'Multiplos fatores' END AS causa_principal,
  s.fatores, s.periodo_referencia
FROM motor_score s JOIN ultimo u ON s.periodo_referencia = u.p
WHERE s.classificacao != 'consistente'
ORDER BY CASE s.classificacao WHEN 'atencao_operacional' THEN 1 WHEN 'dado_insuficiente' THEN 2 WHEN 'monitorar' THEN 3 END, s.score_final;

-- ============================================================================
-- GRUPO 3: Views dependentes de outras views
-- ============================================================================

-- --------------------------------------------------------------------------
-- vw_absenteismo_recorrencia
-- Usada por: motor_decisoes()
-- DEPENDE DE: vw_absenteismo_real (Grupo 1)
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_absenteismo_recorrencia AS
SELECT funcionario_id, nome_completo, cargo, setor, salario_base,
  count(*) AS total_atestados,
  count(*) FILTER (WHERE faixa = 'curto') AS atestados_curtos,
  count(*) FILTER (WHERE faixa = 'medio') AS atestados_medios,
  count(*) FILTER (WHERE faixa = 'longo_inss') AS afastamentos_inss,
  sum(dias) AS total_dias, sum(dias_empresa) AS dias_pagos_empresa,
  sum(dias_inss) AS dias_pagos_inss, sum(custo_empresa_real) AS custo_empresa_total,
  round(count(*)::numeric / GREATEST(EXTRACT(month FROM age(max(data_inicio), min(data_inicio))) + 1, 1), 1) AS atestados_por_mes,
  min(data_inicio) AS primeiro, max(data_inicio) AS ultimo,
  CASE
    WHEN count(*) >= 10 AND count(*) FILTER (WHERE faixa = 'curto') >= 8 THEN 'RECORRENTE CRITICO'
    WHEN count(*) >= 5 THEN 'RECORRENTE'
    WHEN sum(dias) >= 30 THEN 'AFASTAMENTO PROLONGADO'
    ELSE 'NORMAL' END AS classificacao
FROM vw_absenteismo_real
GROUP BY funcionario_id, nome_completo, cargo, setor, salario_base
ORDER BY sum(custo_empresa_real) DESC;

-- ============================================================================
-- GRUPO 4: Views utilitarias de uso geral
-- ============================================================================

-- --------------------------------------------------------------------------
-- vw_funcionario_base
-- View base usada por multiplas paginas (armarios, shell search, selects)
-- Campos: id, nome (alias de nome_completo), cargo, setor, status
-- Dependencia: rh_funcionarios, rh_cargos, rh_setores
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_funcionario_base AS
SELECT
  f.id,
  f.nome_completo AS nome,
  c.nome AS cargo,
  s.nome AS setor,
  f.status,
  f.setor_id,
  f.cargo_id,
  f.data_admissao,
  f.sexo
FROM rh_funcionarios f
  LEFT JOIN rh_cargos c ON c.id = f.cargo_id
  LEFT JOIN rh_setores s ON s.id = f.setor_id;
