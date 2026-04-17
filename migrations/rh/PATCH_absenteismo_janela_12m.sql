-- ============================================================================
-- PATCH_absenteismo_janela_12m.sql
-- Data: 2026-04-17
-- Escopo: Motor de Decisoes — restringir analise de atestados aos ultimos 12 meses
--
-- Motivacao:
--   As views vw_absenteismo_real e vw_absenteismo_por_setor liam TODO o historico
--   de stg_convenia_documentos (desde 2023+), inflando contagens como "13 atestados"
--   sem contexto temporal e prejudicando quem tem muito tempo de casa.
--
-- Solucao (Opcao 1 — rolling window):
--   Filtrar d.payload_json ->> 'data_inicio' >= CURRENT_DATE - INTERVAL '12 months'.
--   vw_absenteismo_recorrencia herda o filtro via dependencia de vw_absenteismo_real.
--
-- Como aplicar em producao (Supabase SQL Editor):
--   Executar este arquivo na integra. Seguro: apenas CREATE OR REPLACE de views
--   + CREATE OR REPLACE de uma funcao. Sem DROP, sem alteracao de dados.
-- ============================================================================

-- ─── 1. vw_absenteismo_real ──────────────────────────────────────────────────
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
WHERE d.origem_sistema = 'manual'
  AND (d.payload_json ->> 'data_inicio')::date >= CURRENT_DATE - INTERVAL '12 months';

-- ─── 2. vw_absenteismo_por_setor ────────────────────────────────────────────
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
  AND (d.payload_json ->> 'data_inicio')::date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY s.id, s.nome
ORDER BY sum((d.payload_json ->> 'dias')::integer) DESC;

-- ─── 3. motor_decisoes() — atualizar textos de diagnostico ──────────────────
CREATE OR REPLACE FUNCTION public.motor_decisoes()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_problemas JSONB := '[]'::JSONB;
  v_excluidos JSONB := '[]'::JSONB;
  r RECORD;
BEGIN
  -- ═══ 1. RECORRENTES (exclui licencas) ═══
  FOR r IN
    SELECT rc.*, f.status AS f_status, f.tipo_afastamento
    FROM vw_absenteismo_recorrencia rc
      INNER JOIN rh_funcionarios f ON f.id = rc.funcionario_id
    WHERE rc.classificacao IN ('RECORRENTE CRITICO','RECORRENTE')
    ORDER BY rc.custo_empresa_total DESC LIMIT 15
  LOOP
    IF r.tipo_afastamento IN ('licenca_maternidade','licenca_paternidade') THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Licenca maternidade',
        'atestados', r.total_atestados, 'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;
    IF r.f_status = 'afastado' AND r.tipo_afastamento IN ('inss_doenca','acidente_trabalho') THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Afastamento INSS',
        'atestados', r.total_atestados, 'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;

    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'recorrencia',
      'prioridade', CASE r.classificacao WHEN 'RECORRENTE CRITICO' THEN 1 ELSE 2 END,
      'risco', CASE WHEN r.atestados_por_mes >= 3 THEN 'alto' WHEN r.atestados_por_mes >= 2 THEN 'medio' ELSE 'baixo' END,
      'funcionario', r.nome_completo, 'cargo', r.cargo, 'setor', r.setor,
      'impacto_financeiro', r.custo_empresa_total,
      'diagnostico', r.total_atestados || ' atestados nos ultimos 12 meses (' || r.atestados_curtos || ' curtos), ' ||
        ROUND(r.atestados_por_mes, 1) || '/mes. Custo: R$ ' || r.custo_empresa_total,
      'causa_provavel', CASE
        WHEN r.atestados_curtos >= r.total_atestados * 0.8 THEN 'Padrao de faltas curtas recorrentes — possivel desmotivacao, problema pessoal ou condicao cronica'
        ELSE 'Faltas curtas + afastamentos — possivel problema de saude em agravamento'
      END,
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'Conversa individual com RH e lideranca', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Revisar historico de atestados e CIDs', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Encaminhar para avaliacao medica ocupacional', 'responsavel', 'sst', 'prazo_dias', 14),
        jsonb_build_object('acao', 'Acompanhamento mensal por 3 meses', 'responsavel', 'gestor', 'prazo_dias', 30)
      )
    );
  END LOOP;

  -- ═══ 2. AFASTAMENTOS PROLONGADOS (exclui licencas) ═══
  FOR r IN
    SELECT rc.*, f.tipo_afastamento
    FROM vw_absenteismo_recorrencia rc
      INNER JOIN rh_funcionarios f ON f.id = rc.funcionario_id
    WHERE rc.classificacao = 'AFASTAMENTO PROLONGADO'
    ORDER BY rc.total_dias DESC LIMIT 5
  LOOP
    IF r.tipo_afastamento = 'licenca_maternidade' THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Licenca maternidade',
        'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;
    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'afastamento_prolongado', 'prioridade', 3,
      'risco', CASE WHEN r.total_dias >= 90 THEN 'alto' ELSE 'medio' END,
      'funcionario', r.nome_completo, 'cargo', r.cargo, 'setor', r.setor,
      'impacto_financeiro', r.custo_empresa_total,
      'diagnostico', r.total_dias || ' dias nos ultimos 12 meses (' || r.dias_pagos_empresa || ' empresa + ' || r.dias_pagos_inss || ' INSS)',
      'causa_provavel', 'Lesao ou doenca grave — verificar componente ergonomico ou ocupacional',
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'ASO de retorno obrigatorio', 'responsavel', 'sst', 'prazo_dias', 1),
        jsonb_build_object('acao', 'Planejar retorno com restricoes', 'responsavel', 'sst', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Avaliar posto de trabalho', 'responsavel', 'sst', 'prazo_dias', 14),
        jsonb_build_object('acao', 'Acompanhamento semanal no 1o mes', 'responsavel', 'gestor', 'prazo_dias', 30)
      )
    );
  END LOOP;

  -- ═══ 3. SETORES CRITICOS ═══
  FOR r IN
    SELECT * FROM vw_absenteismo_por_setor WHERE total_dias > 50
  LOOP
    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'setor_critico', 'prioridade', CASE WHEN r.total_dias >= 500 THEN 1 ELSE 3 END,
      'risco', CASE WHEN r.total_dias >= 500 THEN 'alto' ELSE 'medio' END,
      'setor', r.setor, 'impacto_financeiro', r.custo_estimado,
      'diagnostico', r.setor || ': ' || r.funcionarios_afetados || '/' || r.total_setor ||
        ' afetados, ' || r.atestados || ' atestados, ' || r.total_dias || ' dias (ultimos 12 meses)',
      'causa_provavel', CASE
        WHEN r.funcionarios_afetados::NUMERIC / NULLIF(r.total_setor, 0) > 0.5 THEN 'Mais de 50% afetado — possivel problema ambiental ou de gestao'
        ELSE 'Concentrado em poucos — verificar casos individuais'
      END,
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'Reuniao com lideranca sobre absenteismo', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Analise ergonomica (AET)', 'responsavel', 'sst', 'prazo_dias', 30),
        jsonb_build_object('acao', 'Revisar condicoes de trabalho', 'responsavel', 'sst', 'prazo_dias', 14)
      )
    );
  END LOOP;

  -- ═══ 4. SST — considerar carga incompleta ═══
  DECLARE
    v_sem_epi INTEGER;
    v_pct_carregado NUMERIC;
    v_total_entregas_api INTEGER := 41787;
    v_carregadas INTEGER;
  BEGIN
    SELECT COUNT(*) INTO v_carregadas FROM stg_buscaepi_entregas;
    v_pct_carregado := ROUND(v_carregadas::NUMERIC / GREATEST(v_total_entregas_api, 1) * 100, 1);

    SELECT COUNT(*) INTO v_sem_epi FROM rh_funcionarios f
    WHERE f.status IN ('ativo','ferias') AND f.elegivel_sst = TRUE
      AND NOT EXISTS (SELECT 1 FROM sst_epi_entrega e WHERE e.funcionario_id = f.id);

    IF v_pct_carregado >= 80 AND v_sem_epi > 5 THEN
      v_problemas := v_problemas || jsonb_build_object(
        'tipo', 'sst_epi', 'prioridade', 2, 'risco', 'alto',
        'impacto_financeiro', v_sem_epi * 3000,
        'diagnostico', v_sem_epi || ' funcionarios sem EPI registrado (carga ' || v_pct_carregado || '% concluida)',
        'causa_provavel', 'Funcionarios que realmente nao receberam EPI — requer acao imediata',
        'plano_acao', jsonb_build_array(
          jsonb_build_object('acao', 'Verificar quais funcionarios nunca receberam EPI', 'responsavel', 'sst', 'prazo_dias', 7),
          jsonb_build_object('acao', 'Providenciar entrega imediata', 'responsavel', 'sst', 'prazo_dias', 14)
        )
      );
    ELSE
      v_excluidos := v_excluidos || jsonb_build_object(
        'motivo_exclusao', 'Carga BuscaEPI incompleta (' || v_pct_carregado || '%). ' ||
          v_sem_epi || ' sem EPI no sistema, mas maioria recebe fisicamente. Aguardar carga automatica.',
        'custo_removido', 0,
        'dados', jsonb_build_object(
          'entregas_carregadas', v_carregadas,
          'entregas_total_api', v_total_entregas_api,
          'pct_carregado', v_pct_carregado,
          'sem_epi_sistema', v_sem_epi
        )
      );
    END IF;
  END;

  -- Ordenar
  SELECT COALESCE(jsonb_agg(elem ORDER BY (elem->>'prioridade')::INTEGER, (elem->>'impacto_financeiro')::NUMERIC DESC), '[]'::JSONB)
  INTO v_problemas FROM jsonb_array_elements(v_problemas) AS elem;

  RETURN jsonb_build_object(
    'total_problemas', jsonb_array_length(v_problemas),
    'problemas', v_problemas,
    'excluidos_por_contexto', v_excluidos,
    'total_excluidos', jsonb_array_length(v_excluidos),
    'resumo', jsonb_build_object(
      'impacto_total', (SELECT COALESCE(SUM((e->>'impacto_financeiro')::NUMERIC), 0) FROM jsonb_array_elements(v_problemas) e),
      'alto_risco', (SELECT COUNT(*) FROM jsonb_array_elements(v_problemas) e WHERE e->>'risco' = 'alto'),
      'acoes_total', (SELECT COALESCE(SUM(jsonb_array_length(e->'plano_acao')), 0) FROM jsonb_array_elements(v_problemas) e),
      'custo_excluido', (SELECT COALESCE(SUM((e->>'custo_removido')::NUMERIC), 0) FROM jsonb_array_elements(v_excluidos) e)
    ),
    'janela_analise', '12 meses (rolling window)',
    'gerado_em', NOW()
  );
END;
$function$;
