-- ══════════════════════════════════════════════════════════════════════════════
-- P1 — CENTRALIZAÇÃO DE REGRAS DE NEGÓCIO
-- Migration: p1_centralizar_regras_negocio.sql
-- Data: 2026-03-18
-- Objetivo: Eliminar lógica duplicada no frontend e criar fonte única da verdade
--
-- Funções criadas:
--   1. cq_motor_decisao()          — classificação + status_final a partir de %defeitos
--   2. cq_calcular_score_lote()    — score de qualidade por lote
--   3. cq_calcular_iqf()           — Índice de Qualidade do Fornecedor (90 dias)
--   4. cq_validar_inspecao()       — validações obrigatórias antes de salvar
--   5. cq_validar_carga()          — validações de carga/importação
--   6. cq_submeter_inspecao()      — submissão atômica (validar + classificar + salvar)
--   7. cq_acoes_permitidas()       — ações de workflow permitidas para o usuário
--   8. cq_criar_revisao()          — revisão atômica (inserir revisão + transicionar)
--
-- IMPORTANTE: Executar APÓS todas as migrations anteriores (fase1, fase2, etc.)
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. MOTOR DE DECISÃO
-- Fonte única da verdade para classificação (A/B/C) e status final
-- (aprovado/ressalva/bloqueado)
--
-- Usa cq_parametros_inspecao para obter limites por produto.
-- Resolve inconsistência: frontend usava 5%/12% em dashboards
-- e 5%/15% no formulário. Agora usa sempre os limites do banco.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_motor_decisao(
  p_percentual_defeitos NUMERIC,
  p_produto_nome TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_limite_aprov NUMERIC;
  v_limite_ress  NUMERIC;
  v_classificacao TEXT;
  v_status_final  TEXT;
  v_requer_analise_critica BOOLEAN := FALSE;
BEGIN
  -- Buscar limites do produto (ou fallback para padrão geral)
  SELECT p.limite_aprovacao, p.limite_ressalva
  INTO v_limite_aprov, v_limite_ress
  FROM cq_parametros_inspecao p
  LEFT JOIN cq_produtos prod ON p.produto_id = prod.id
  WHERE (
    (p_produto_nome IS NOT NULL AND (prod.nome = p_produto_nome OR p.nome_produto = p_produto_nome))
    OR (p_produto_nome IS NULL AND p.produto_id IS NULL)
  )
    AND p.ativo = TRUE
  ORDER BY
    CASE WHEN p.produto_id IS NOT NULL THEN 0 ELSE 1 END  -- produto específico primeiro
  LIMIT 1;

  -- Fallback se não encontrou nenhum parâmetro
  IF v_limite_aprov IS NULL THEN
    v_limite_aprov := 5.0;
    v_limite_ress  := 15.0;
  END IF;

  -- Aplicar regras de classificação
  IF p_percentual_defeitos <= v_limite_aprov THEN
    v_classificacao := 'A';
    v_status_final  := 'aprovado';
  ELSIF p_percentual_defeitos <= v_limite_ress THEN
    v_classificacao := 'B';
    v_status_final  := 'ressalva';
  ELSE
    v_classificacao := 'C';
    v_status_final  := 'bloqueado';
  END IF;

  -- Regra especial: análise crítica quando > 30%
  IF p_percentual_defeitos > 30.0 THEN
    v_requer_analise_critica := TRUE;
  END IF;

  RETURN jsonb_build_object(
    'classificacao', v_classificacao,
    'status_final', v_status_final,
    'percentual_defeitos', ROUND(p_percentual_defeitos, 2),
    'limite_aprovacao', v_limite_aprov,
    'limite_ressalva', v_limite_ress,
    'requer_analise_critica', v_requer_analise_critica
  );
END;
$$;

COMMENT ON FUNCTION cq_motor_decisao IS
  'Motor de decisão centralizado: classifica inspeção (A/B/C) e define status final (aprovado/ressalva/bloqueado) com base nos parâmetros configurados por produto.';


-- ════════════════════════════════════════════════════════════════════════════
-- 2. CÁLCULO DE SCORE POR LOTE
-- Fórmula unificada: 100 - (%C × 50) - (%B × 20) + (%A × 20)
-- Clamped em [0, 100]
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_calcular_score_lote(
  p_class_a INTEGER,
  p_class_b INTEGER,
  p_class_c INTEGER
)
RETURNS JSONB
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
  v_total INTEGER;
  v_pct_a NUMERIC;
  v_pct_b NUMERIC;
  v_pct_c NUMERIC;
  v_score NUMERIC;
  v_classificacao_lote TEXT;
BEGIN
  v_total := COALESCE(p_class_a, 0) + COALESCE(p_class_b, 0) + COALESCE(p_class_c, 0);

  IF v_total = 0 THEN
    RETURN jsonb_build_object(
      'score', 0,
      'classificacao_lote', NULL,
      'pct_a', 0, 'pct_b', 0, 'pct_c', 0,
      'total', 0,
      'erro', 'Nenhuma peça classificada'
    );
  END IF;

  v_pct_a := (COALESCE(p_class_a, 0)::NUMERIC / v_total) * 100;
  v_pct_b := (COALESCE(p_class_b, 0)::NUMERIC / v_total) * 100;
  v_pct_c := (COALESCE(p_class_c, 0)::NUMERIC / v_total) * 100;

  -- Fórmula centralizada (era duplicada em 4+ HTML files)
  v_score := GREATEST(0, LEAST(100,
    100 - (v_pct_c * 0.50) - (v_pct_b * 0.20) + (v_pct_a * 0.20)
  ));

  -- Classificação do lote baseada no score
  IF v_score >= 80 THEN
    v_classificacao_lote := 'A';
  ELSIF v_score >= 50 THEN
    v_classificacao_lote := 'B';
  ELSE
    v_classificacao_lote := 'C';
  END IF;

  RETURN jsonb_build_object(
    'score', ROUND(v_score, 1),
    'classificacao_lote', v_classificacao_lote,
    'pct_a', ROUND(v_pct_a, 1),
    'pct_b', ROUND(v_pct_b, 1),
    'pct_c', ROUND(v_pct_c, 1),
    'total', v_total
  );
END;
$$;

COMMENT ON FUNCTION cq_calcular_score_lote IS
  'Calcula score de qualidade do lote: 100 - (%C×50) - (%B×20) + (%A×20), clamped [0,100]. Fonte única da verdade para a fórmula de score.';


-- ════════════════════════════════════════════════════════════════════════════
-- 3. ÍNDICE DE QUALIDADE DO FORNECEDOR (IQF)
-- Janela de 90 dias. Penalidades: bloqueio -15, ressalva -5,
-- NC crítica -20, NC maior -10, NC menor -3
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_calcular_iqf(
  p_fornecedor_codigo TEXT,
  p_janela_dias INTEGER DEFAULT 90
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_data_inicio DATE;
  v_total_inspecoes INTEGER := 0;
  v_total_aprovadas INTEGER := 0;
  v_total_ressalvas INTEGER := 0;
  v_total_bloqueadas INTEGER := 0;
  v_nc_criticas INTEGER := 0;
  v_nc_maiores INTEGER := 0;
  v_nc_menores INTEGER := 0;
  v_score NUMERIC := 100;
  v_classificacao TEXT;
  v_media_defeitos NUMERIC := 0;
  v_tendencia TEXT := 'estavel';
  v_score_30d NUMERIC;
  v_score_total NUMERIC;
BEGIN
  v_data_inicio := CURRENT_DATE - p_janela_dias;

  -- Contagem de inspeções por status final na janela
  SELECT
    COUNT(*),
    COUNT(*) FILTER (WHERE status_final = 'aprovado'),
    COUNT(*) FILTER (WHERE status_final = 'ressalva'),
    COUNT(*) FILTER (WHERE status_final = 'bloqueado'),
    COALESCE(AVG(percentual_defeitos), 0)
  INTO v_total_inspecoes, v_total_aprovadas, v_total_ressalvas, v_total_bloqueadas, v_media_defeitos
  FROM registros_cq_inspecao
  WHERE fornecedor ILIKE '%' || p_fornecedor_codigo || '%'
    AND data_inspecao >= v_data_inicio
    AND status_workflow NOT IN ('cancelada', 'rascunho');

  -- Contagem de NCs na janela
  SELECT
    COUNT(*) FILTER (WHERE nc.gravidade = 'critica'),
    COUNT(*) FILTER (WHERE nc.gravidade = 'maior'),
    COUNT(*) FILTER (WHERE nc.gravidade = 'menor')
  INTO v_nc_criticas, v_nc_maiores, v_nc_menores
  FROM cq_nao_conformidades nc
  JOIN registros_cq_inspecao i ON nc.inspecao_id = i.id
  WHERE i.fornecedor ILIKE '%' || p_fornecedor_codigo || '%'
    AND nc.criado_em >= v_data_inicio;

  -- Cálculo do IQF (fórmula centralizada)
  v_score := 100
    - (v_total_bloqueadas * 15)
    - (v_total_ressalvas * 5)
    - (v_nc_criticas * 20)
    - (v_nc_maiores * 10)
    - (v_nc_menores * 3);
  v_score := GREATEST(0, LEAST(100, v_score));
  v_score_total := v_score;

  -- Classificação de confiabilidade
  IF v_score >= 85 THEN
    v_classificacao := 'confiavel';
  ELSIF v_score >= 70 THEN
    v_classificacao := 'regular';
  ELSIF v_score >= 50 THEN
    v_classificacao := 'atencao';
  ELSE
    v_classificacao := 'critico';
  END IF;

  -- Tendência: comparar últimos 30 dias vs janela total
  SELECT GREATEST(0, LEAST(100,
    100
    - (COUNT(*) FILTER (WHERE status_final = 'bloqueado') * 15)
    - (COUNT(*) FILTER (WHERE status_final = 'ressalva') * 5)
  ))
  INTO v_score_30d
  FROM registros_cq_inspecao
  WHERE fornecedor ILIKE '%' || p_fornecedor_codigo || '%'
    AND data_inspecao >= (CURRENT_DATE - 30)
    AND status_workflow NOT IN ('cancelada', 'rascunho');

  IF v_score_30d IS NOT NULL AND v_score_total > 0 THEN
    IF v_score_30d > v_score_total + 5 THEN
      v_tendencia := 'melhorando';
    ELSIF v_score_30d < v_score_total - 5 THEN
      v_tendencia := 'piorando';
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'fornecedor_codigo', p_fornecedor_codigo,
    'janela_dias', p_janela_dias,
    'iqf_score', ROUND(v_score_total, 1),
    'classificacao', v_classificacao,
    'tendencia', v_tendencia,
    'score_30d', ROUND(COALESCE(v_score_30d, 0), 1),
    'total_inspecoes', v_total_inspecoes,
    'aprovadas', v_total_aprovadas,
    'ressalvas', v_total_ressalvas,
    'bloqueadas', v_total_bloqueadas,
    'nc_criticas', v_nc_criticas,
    'nc_maiores', v_nc_maiores,
    'nc_menores', v_nc_menores,
    'media_defeitos_pct', ROUND(v_media_defeitos, 2)
  );
END;
$$;

COMMENT ON FUNCTION cq_calcular_iqf IS
  'Calcula o Índice de Qualidade do Fornecedor (IQF) em janela de N dias. Penalidades: bloqueio -15, ressalva -5, NC crítica -20, NC maior -10, NC menor -3. Score [0,100].';


-- ════════════════════════════════════════════════════════════════════════════
-- 4. VALIDAÇÃO DE INSPEÇÃO
-- Validações obrigatórias antes de salvar/submeter uma inspeção
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_validar_inspecao(p_dados JSONB)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_erros JSONB := '[]'::JSONB;
  v_qtd_analisada INTEGER;
  v_total_defeitos INTEGER;
  v_pct NUMERIC;
  v_status_final TEXT;
  v_obs TEXT;
BEGIN
  -- 1. Fornecedor obrigatório
  IF (p_dados->>'fornecedor') IS NULL OR TRIM(p_dados->>'fornecedor') = '' THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'campo', 'fornecedor', 'tipo', 'obrigatorio', 'gravidade', 'critica',
      'mensagem', 'Fornecedor é obrigatório'
    ));
  END IF;

  -- 2. Produto obrigatório
  IF (p_dados->>'produto') IS NULL OR TRIM(p_dados->>'produto') = '' THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'campo', 'produto', 'tipo', 'obrigatorio', 'gravidade', 'critica',
      'mensagem', 'Produto é obrigatório'
    ));
  END IF;

  -- 3. Quantidade analisada > 0
  v_qtd_analisada := COALESCE((p_dados->>'quantidade_analisada')::INTEGER, 0);
  IF v_qtd_analisada <= 0 THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'campo', 'quantidade_analisada', 'tipo', 'obrigatorio', 'gravidade', 'critica',
      'mensagem', 'Quantidade analisada deve ser maior que zero'
    ));
  END IF;

  -- 4. Defeitos não podem ser negativos
  DECLARE
    v_campo TEXT;
    v_campos_defeito TEXT[] := ARRAY[
      'fermentacao', 'sem_folhas', 'folhas_arrebentadas', 'pedaco', 'sujo',
      'rasgado', 'corte_irregular', 'furado', 'mau_cheiro', 'contaminacao', 'outro_defeito'
    ];
  BEGIN
    FOREACH v_campo IN ARRAY v_campos_defeito LOOP
      IF COALESCE((p_dados->>v_campo)::INTEGER, 0) < 0 THEN
        v_erros := v_erros || jsonb_build_array(jsonb_build_object(
          'campo', v_campo, 'tipo', 'valor_negativo', 'gravidade', 'critica',
          'mensagem', format('Defeito %s não pode ser negativo', v_campo)
        ));
      END IF;
    END LOOP;
  END;

  -- 5. Total de defeitos não pode exceder quantidade analisada
  v_total_defeitos := COALESCE((p_dados->>'total_defeitos')::INTEGER, 0);
  IF v_qtd_analisada > 0 AND v_total_defeitos > v_qtd_analisada THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'campo', 'total_defeitos', 'tipo', 'consistencia', 'gravidade', 'critica',
      'mensagem', format('Total de defeitos (%s) excede quantidade analisada (%s)', v_total_defeitos, v_qtd_analisada)
    ));
  END IF;

  -- 6. Bloqueio sem observação
  v_status_final := p_dados->>'status_final';
  v_obs := p_dados->>'observacoes';
  IF v_status_final = 'bloqueado' AND (v_obs IS NULL OR TRIM(v_obs) = '') THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'campo', 'observacoes', 'tipo', 'obrigatorio_condicional', 'gravidade', 'critica',
      'mensagem', 'Observação obrigatória quando status é BLOQUEADO'
    ));
  END IF;

  -- 7. Data de inspeção não pode ser futura
  IF (p_dados->>'data_inspecao') IS NOT NULL THEN
    IF (p_dados->>'data_inspecao')::DATE > CURRENT_DATE THEN
      v_erros := v_erros || jsonb_build_array(jsonb_build_object(
        'campo', 'data_inspecao', 'tipo', 'data_futura', 'gravidade', 'atencao',
        'mensagem', 'Data de inspeção está no futuro'
      ));
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'valido', (jsonb_array_length(v_erros) = 0),
    'erros', v_erros,
    'total_erros', jsonb_array_length(v_erros),
    'total_criticos', (
      SELECT COUNT(*)::INTEGER FROM jsonb_array_elements(v_erros) e
      WHERE e->>'gravidade' = 'critica'
    )
  );
END;
$$;

COMMENT ON FUNCTION cq_validar_inspecao IS
  'Valida dados de inspeção antes de salvar. Retorna lista de erros com gravidade. Bloqueio sem observação é erro crítico.';


-- ════════════════════════════════════════════════════════════════════════════
-- 5. VALIDAÇÃO DE CARGA
-- Regras de validação para cargas/importações (antes em regras_validacao.js)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_validar_carga(p_dados JSONB)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_erros JSONB := '[]'::JSONB;
  v_class_a INTEGER;
  v_class_b INTEGER;
  v_class_c INTEGER;
  v_total_classificado INTEGER;
  v_soma INTEGER;
  v_qtd_frigo INTEGER;
  v_qtd_classic INTEGER;
  v_diferenca INTEGER;
  v_gravidade TEXT;
  v_campo TEXT;
  v_campos_numericos TEXT[] := ARRAY['qtd_frigo', 'class_a', 'class_b', 'class_c', 'total_classificado'];
  -- Limites configuráveis (mesmos de REGRAS_VALIDACAO.LIMITES)
  c_diferenca_atencao CONSTANT INTEGER := 5;
  c_diferenca_critica CONSTANT INTEGER := 10;
BEGIN
  -- REGRA 1: A + B + C = Total Classificado
  v_class_a := COALESCE((p_dados->>'class_a')::INTEGER, 0);
  v_class_b := COALESCE((p_dados->>'class_b')::INTEGER, 0);
  v_class_c := COALESCE((p_dados->>'class_c')::INTEGER, 0);
  v_total_classificado := COALESCE((p_dados->>'total_classificado')::INTEGER, 0);
  v_soma := v_class_a + v_class_b + v_class_c;

  IF v_total_classificado > 0 AND v_soma != v_total_classificado THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'tipo', 'abc_total_mismatch',
      'gravidade', 'critica',
      'campo', 'class_a+class_b+class_c',
      'esperado', v_total_classificado,
      'encontrado', v_soma,
      'diferenca', ABS(v_total_classificado - v_soma),
      'mensagem', format('A(%s) + B(%s) + C(%s) = %s ≠ Total classificado(%s)',
        v_class_a, v_class_b, v_class_c, v_soma, v_total_classificado)
    ));
  END IF;

  -- REGRA 2: Divergência contagem frigorífico vs contagem interna
  v_qtd_frigo := COALESCE((p_dados->>'qtd_frigo')::INTEGER, 0);
  v_qtd_classic := COALESCE((p_dados->>'qtd_classic')::INTEGER, 0);

  IF (v_qtd_frigo > 0 OR v_qtd_classic > 0) AND v_qtd_frigo != v_qtd_classic THEN
    v_diferenca := ABS(v_qtd_frigo - v_qtd_classic);
    IF v_diferenca > c_diferenca_critica THEN
      v_gravidade := 'critica';
    ELSIF v_diferenca > c_diferenca_atencao THEN
      v_gravidade := 'atencao';
    ELSE
      v_gravidade := 'informativa';
    END IF;

    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'tipo', 'contagem_frigo_vs_classic',
      'gravidade', v_gravidade,
      'campo', 'qtd_frigo vs qtd_classic',
      'esperado', v_qtd_classic,
      'encontrado', v_qtd_frigo,
      'diferenca', v_diferenca,
      'mensagem', format('Contagem frigorífico(%s) ≠ Contagem interna(%s), diferença: %s',
        v_qtd_frigo, v_qtd_classic, v_diferenca)
    ));
  END IF;

  -- REGRA 3: Fornecedor obrigatório
  IF (p_dados->>'fornecedor_codigo') IS NULL
    AND (p_dados->>'fornecedor_nome') IS NULL
    AND (p_dados->>'frigorifico') IS NULL THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'tipo', 'documento_ausente', 'gravidade', 'critica',
      'campo', 'fornecedor',
      'mensagem', 'Fornecedor não identificado na carga'
    ));
  END IF;

  -- REGRA 4: Data obrigatória
  IF (p_dados->>'data_coleta') IS NULL AND (p_dados->>'data_chegada') IS NULL THEN
    v_erros := v_erros || jsonb_build_array(jsonb_build_object(
      'tipo', 'data_inconsistente', 'gravidade', 'atencao',
      'campo', 'data_coleta/data_chegada',
      'mensagem', 'Nenhuma data de coleta ou chegada informada'
    ));
  END IF;

  -- REGRA 5: Data futura
  IF (p_dados->>'data_coleta') IS NOT NULL THEN
    BEGIN
      IF (p_dados->>'data_coleta')::DATE > CURRENT_DATE THEN
        v_erros := v_erros || jsonb_build_array(jsonb_build_object(
          'tipo', 'data_inconsistente', 'gravidade', 'atencao',
          'campo', 'data_coleta',
          'mensagem', format('Data futura detectada: %s', p_dados->>'data_coleta')
        ));
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Data inválida, ignora silenciosamente
      NULL;
    END;
  END IF;

  -- REGRA 6: Valores negativos
  FOREACH v_campo IN ARRAY v_campos_numericos LOOP
    IF (p_dados->>v_campo) IS NOT NULL AND (p_dados->>v_campo)::INTEGER < 0 THEN
      v_erros := v_erros || jsonb_build_array(jsonb_build_object(
        'tipo', 'quantidade_negativa', 'gravidade', 'critica',
        'campo', v_campo,
        'mensagem', format('Valor negativo em %s: %s', v_campo, p_dados->>v_campo)
      ));
    END IF;
  END LOOP;

  -- REGRA 7: Duplicidade PCR
  IF (p_dados->>'numero_pcr') IS NOT NULL THEN
    DECLARE
      v_dup_id UUID;
      v_dup_data TIMESTAMPTZ;
    BEGIN
      SELECT id, criado_em INTO v_dup_id, v_dup_data
      FROM atak_cargas_raw
      WHERE numero_pcr = p_dados->>'numero_pcr'
      LIMIT 1;

      IF v_dup_id IS NOT NULL THEN
        v_erros := v_erros || jsonb_build_array(jsonb_build_object(
          'tipo', 'duplicidade_carga', 'gravidade', 'atencao',
          'campo', 'numero_pcr',
          'esperado', 'Registro único',
          'encontrado', format('Duplicata de %s', v_dup_id),
          'mensagem', format('PCR %s já existe (ID: %s, criado: %s)',
            p_dados->>'numero_pcr', v_dup_id, v_dup_data)
        ));
      END IF;
    END;
  END IF;

  RETURN jsonb_build_object(
    'valido', (jsonb_array_length(v_erros) = 0),
    'divergencias', v_erros,
    'total_criticas', (
      SELECT COUNT(*)::INTEGER FROM jsonb_array_elements(v_erros) e
      WHERE e->>'gravidade' = 'critica'
    ),
    'total_atencao', (
      SELECT COUNT(*)::INTEGER FROM jsonb_array_elements(v_erros) e
      WHERE e->>'gravidade' = 'atencao'
    ),
    'total_informativas', (
      SELECT COUNT(*)::INTEGER FROM jsonb_array_elements(v_erros) e
      WHERE e->>'gravidade' = 'informativa'
    )
  );
END;
$$;

COMMENT ON FUNCTION cq_validar_carga IS
  'Valida dados de carga/importação. Centraliza regras que estavam em regras_validacao.js (ABC total, contagem frigo vs classic, duplicidade PCR, dados básicos).';


-- ════════════════════════════════════════════════════════════════════════════
-- 6. SUBMISSÃO ATÔMICA DE INSPEÇÃO
-- Valida → calcula classificação → atualiza registro em transação única
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_submeter_inspecao(p_inspecao_id UUID)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_insp RECORD;
  v_total_defeitos INTEGER;
  v_pct NUMERIC;
  v_decisao JSONB;
  v_validacao JSONB;
  v_dados_validacao JSONB;
  v_usuario_id UUID;
  v_perfil TEXT;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- Carregar inspeção
  SELECT * INTO v_insp
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_insp IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  -- Só pode submeter se estiver em rascunho ou em_analise
  IF v_insp.status_workflow NOT IN ('rascunho', 'em_analise') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Inspeção em status "%s" não pode ser submetida', v_insp.status_workflow));
  END IF;

  -- Só criador ou admin pode submeter
  IF v_usuario_id != v_insp.criador_id AND v_perfil != 'administrador' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas o criador ou administrador pode submeter');
  END IF;

  -- Calcular total de defeitos
  v_total_defeitos := COALESCE(v_insp.fermentacao, 0) + COALESCE(v_insp.sem_folhas, 0)
    + COALESCE(v_insp.folhas_arrebentadas, 0) + COALESCE(v_insp.pedaco, 0)
    + COALESCE(v_insp.sujo, 0) + COALESCE(v_insp.rasgado, 0)
    + COALESCE(v_insp.corte_irregular, 0) + COALESCE(v_insp.furado, 0)
    + COALESCE(v_insp.mau_cheiro, 0) + COALESCE(v_insp.contaminacao, 0)
    + COALESCE(v_insp.outro_defeito, 0);

  -- Calcular percentual
  IF COALESCE(v_insp.quantidade_analisada, 0) > 0 THEN
    v_pct := (v_total_defeitos::NUMERIC / v_insp.quantidade_analisada) * 100;
  ELSE
    v_pct := 0;
  END IF;

  -- Montar dados para validação
  v_dados_validacao := jsonb_build_object(
    'fornecedor', v_insp.fornecedor,
    'produto', v_insp.produto,
    'quantidade_analisada', v_insp.quantidade_analisada,
    'total_defeitos', v_total_defeitos,
    'data_inspecao', v_insp.data_inspecao,
    'status_final', CASE WHEN v_pct > 15 THEN 'bloqueado' ELSE NULL END,
    'observacoes', v_insp.observacoes
  );

  -- Validar
  v_validacao := cq_validar_inspecao(v_dados_validacao);
  IF NOT (v_validacao->>'valido')::BOOLEAN THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'erro', 'Validação falhou',
      'validacao', v_validacao
    );
  END IF;

  -- Calcular decisão via motor
  v_decisao := cq_motor_decisao(v_pct, v_insp.produto);

  -- Atualizar registro atomicamente
  UPDATE registros_cq_inspecao
  SET
    total_defeitos = v_total_defeitos,
    percentual_defeitos = ROUND(v_pct, 2),
    classificacao = v_decisao->>'classificacao',
    status_final = v_decisao->>'status_final',
    status_workflow = 'aguardando_revisao',
    atualizado_em = NOW()
  WHERE id = p_inspecao_id;

  RETURN jsonb_build_object(
    'ok', TRUE,
    'inspecao_id', p_inspecao_id,
    'total_defeitos', v_total_defeitos,
    'percentual_defeitos', ROUND(v_pct, 2),
    'decisao', v_decisao,
    'status_workflow', 'aguardando_revisao'
  );
END;
$$;

COMMENT ON FUNCTION cq_submeter_inspecao IS
  'Submissão atômica de inspeção: valida dados → calcula %defeitos → aplica motor de decisão → transiciona para aguardando_revisao. Frontend não decide nada.';


-- ════════════════════════════════════════════════════════════════════════════
-- 7. AÇÕES PERMITIDAS PARA WORKFLOW
-- Centraliza a lógica de getAcoesPermitidas() que estava no frontend
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_acoes_permitidas(p_inspecao_id UUID)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_status TEXT;
  v_criador_id UUID;
  v_usuario_id UUID;
  v_perfil TEXT;
  v_eh_criador BOOLEAN;
  v_acoes JSONB := '[]'::JSONB;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN '[]'::JSONB;
  END IF;

  SELECT status_workflow, criador_id
  INTO v_status, v_criador_id
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_status IS NULL THEN
    RETURN '[]'::JSONB;
  END IF;

  v_eh_criador := (v_usuario_id = v_criador_id);

  -- Rascunho
  IF v_status = 'rascunho' THEN
    IF v_eh_criador OR v_perfil = 'administrador' THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'editar', 'label', 'Editar', 'confirm', FALSE),
        jsonb_build_object('acao', 'submeter', 'label', 'Submeter para Revisão', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Em análise
  IF v_status = 'em_analise' THEN
    IF v_eh_criador OR v_perfil = 'administrador' THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'editar', 'label', 'Editar', 'confirm', FALSE),
        jsonb_build_object('acao', 'submeter', 'label', 'Submeter para Revisão', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Aguardando revisão
  IF v_status = 'aguardando_revisao' THEN
    IF NOT v_eh_criador AND v_perfil IN ('administrador', 'qualidade', 'diretoria') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'revisar', 'label', 'Revisar', 'confirm', FALSE)
      );
    END IF;
  END IF;

  -- Revisada
  IF v_status = 'revisada' THEN
    IF NOT v_eh_criador AND v_perfil IN ('administrador', 'diretoria') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'aprovada', 'label', 'Aprovar', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Reprovada
  IF v_status = 'reprovada' THEN
    IF v_perfil IN ('administrador', 'qualidade') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'abrir_nc', 'label', 'Abrir NC', 'confirm', FALSE)
      );
    END IF;
    IF v_perfil = 'administrador' THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'rascunho', 'label', 'Reabrir', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Aprovada
  IF v_status = 'aprovada' THEN
    IF v_perfil = 'administrador' THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'encerrada', 'label', 'Encerrar', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Cancelar: admin em qualquer status não-final
  IF v_perfil = 'administrador' AND v_status NOT IN ('encerrada', 'cancelada') THEN
    v_acoes := v_acoes || jsonb_build_array(
      jsonb_build_object('acao', 'cancelada', 'label', 'Cancelar', 'confirm', TRUE)
    );
  END IF;

  RETURN v_acoes;
END;
$$;

COMMENT ON FUNCTION cq_acoes_permitidas IS
  'Retorna ações de workflow permitidas para o usuário atual na inspeção. Substitui getAcoesPermitidas() do frontend.';


-- ════════════════════════════════════════════════════════════════════════════
-- 8. REVISÃO ATÔMICA
-- Centraliza criarRevisao() — insere revisão + transiciona status em uma
-- transação. Antes era feito em 2 chamadas separadas no frontend.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_criar_revisao(
  p_inspecao_id UUID,
  p_decisao TEXT,           -- 'aprovada', 'reprovada', 'devolvida'
  p_comentario TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_usuario_id UUID;
  v_perfil TEXT;
  v_user_email TEXT;
  v_user_nome TEXT;
  v_criador_id UUID;
  v_status_atual TEXT;
  v_novo_status TEXT;
  v_resultado JSONB;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- Validar decisão
  IF p_decisao NOT IN ('aprovada', 'reprovada', 'devolvida') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Decisão inválida: %s', p_decisao));
  END IF;

  -- Buscar inspeção
  SELECT status_workflow, criador_id
  INTO v_status_atual, v_criador_id
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_status_atual IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  -- Só pode revisar em status aguardando_revisao
  IF v_status_atual != 'aguardando_revisao' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Inspeção em status "%s" não pode ser revisada', v_status_atual));
  END IF;

  -- Criador não pode revisar própria inspeção
  IF v_usuario_id = v_criador_id THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Criador não pode revisar própria inspeção');
  END IF;

  -- Verificar perfil
  IF v_perfil NOT IN ('administrador', 'qualidade', 'diretoria') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Perfil sem permissão para revisar');
  END IF;

  -- Buscar dados do usuário
  SELECT email, nome INTO v_user_email, v_user_nome
  FROM cq_usuarios
  WHERE id = v_usuario_id;

  -- Mapear decisão → novo status
  CASE p_decisao
    WHEN 'aprovada'  THEN v_novo_status := 'revisada';
    WHEN 'reprovada' THEN v_novo_status := 'reprovada';
    WHEN 'devolvida' THEN v_novo_status := 'rascunho';
  END CASE;

  -- Inserir revisão
  INSERT INTO cq_revisoes (inspecao_id, revisor_id, revisor_email, revisor_nome, decisao, comentario)
  VALUES (p_inspecao_id, v_usuario_id, v_user_email, v_user_nome, p_decisao, p_comentario);

  -- Transicionar status
  UPDATE registros_cq_inspecao
  SET status_workflow = v_novo_status, atualizado_em = NOW()
  WHERE id = p_inspecao_id;

  RETURN jsonb_build_object(
    'ok', TRUE,
    'decisao', p_decisao,
    'de', v_status_atual,
    'para', v_novo_status,
    'revisor', v_user_nome
  );
END;
$$;

COMMENT ON FUNCTION cq_criar_revisao IS
  'Revisão atômica: insere registro de revisão e transiciona status em uma única transação. Garante regras de separação de funções (criador ≠ revisor).';


-- ════════════════════════════════════════════════════════════════════════════
-- 9. AVALIAR FORNECEDOR CRÍTICO
-- Centraliza regra 4 de regras_validacao.js
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_avaliar_fornecedor_critico(
  p_fornecedor_codigo TEXT,
  p_pct_c NUMERIC DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_pct_c NUMERIC;
  v_limite_c NUMERIC := 15.0;   -- PCT_C_CRITICO default
  v_limite_bloqueio NUMERIC := 25.0;  -- PCT_C_BLOQUEIO
  v_status TEXT;
BEGIN
  -- Se %C não foi passado, calcular dos últimos 90 dias
  IF p_pct_c IS NULL THEN
    SELECT COALESCE(AVG(
      CASE WHEN classificacao = 'C' THEN 100.0 ELSE 0.0 END
    ), 0)
    INTO v_pct_c
    FROM registros_cq_inspecao
    WHERE fornecedor ILIKE '%' || p_fornecedor_codigo || '%'
      AND data_inspecao >= (CURRENT_DATE - 90)
      AND status_workflow NOT IN ('cancelada', 'rascunho');
  ELSE
    v_pct_c := p_pct_c;
  END IF;

  IF v_pct_c > v_limite_bloqueio THEN
    v_status := 'bloqueado';
    RETURN jsonb_build_object(
      'critico', TRUE,
      'status_sugerido', 'bloqueado',
      'gravidade', 'critica',
      'pct_c', ROUND(v_pct_c, 2),
      'limite', v_limite_bloqueio,
      'mensagem', format('%%C = %.1f%% EXCEDE %.0f%% → BLOQUEIO RECOMENDADO', v_pct_c, v_limite_bloqueio)
    );
  ELSIF v_pct_c > v_limite_c THEN
    RETURN jsonb_build_object(
      'critico', TRUE,
      'status_sugerido', 'critico',
      'gravidade', 'atencao',
      'pct_c', ROUND(v_pct_c, 2),
      'limite', v_limite_c,
      'mensagem', format('%%C = %.1f%% acima do limite de %.0f%% → FORNECEDOR CRÍTICO', v_pct_c, v_limite_c)
    );
  ELSE
    RETURN jsonb_build_object(
      'critico', FALSE,
      'status_sugerido', 'normal',
      'gravidade', NULL,
      'pct_c', ROUND(v_pct_c, 2),
      'limite', v_limite_c,
      'mensagem', NULL
    );
  END IF;
END;
$$;

COMMENT ON FUNCTION cq_avaliar_fornecedor_critico IS
  'Avalia se fornecedor é crítico com base no %C (último 90 dias). Limites: >15% = crítico, >25% = bloqueio recomendado.';


-- ════════════════════════════════════════════════════════════════════════════
-- GRANT: Permitir chamadas RPC para usuários autenticados
-- ════════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION cq_motor_decisao TO authenticated;
GRANT EXECUTE ON FUNCTION cq_calcular_score_lote TO authenticated;
GRANT EXECUTE ON FUNCTION cq_calcular_iqf TO authenticated;
GRANT EXECUTE ON FUNCTION cq_validar_inspecao TO authenticated;
GRANT EXECUTE ON FUNCTION cq_validar_carga TO authenticated;
GRANT EXECUTE ON FUNCTION cq_submeter_inspecao TO authenticated;
GRANT EXECUTE ON FUNCTION cq_acoes_permitidas TO authenticated;
GRANT EXECUTE ON FUNCTION cq_criar_revisao TO authenticated;
GRANT EXECUTE ON FUNCTION cq_avaliar_fornecedor_critico TO authenticated;

COMMIT;


-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO PÓS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════════

-- V1: Confirmar funções criadas
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name IN (
    'cq_motor_decisao',
    'cq_calcular_score_lote',
    'cq_calcular_iqf',
    'cq_validar_inspecao',
    'cq_validar_carga',
    'cq_submeter_inspecao',
    'cq_acoes_permitidas',
    'cq_criar_revisao',
    'cq_avaliar_fornecedor_critico'
  )
ORDER BY routine_name;

-- V2: Testar motor de decisão
SELECT cq_motor_decisao(3.0, NULL);   -- Espera: A / aprovado
SELECT cq_motor_decisao(10.0, NULL);  -- Espera: B / ressalva
SELECT cq_motor_decisao(20.0, NULL);  -- Espera: C / bloqueado
SELECT cq_motor_decisao(35.0, NULL);  -- Espera: C / bloqueado / requer_analise_critica = true

-- V3: Testar score do lote
SELECT cq_calcular_score_lote(80, 15, 5);    -- Espera: score alto, lote A
SELECT cq_calcular_score_lote(20, 30, 50);   -- Espera: score baixo, lote C
SELECT cq_calcular_score_lote(0, 0, 0);      -- Espera: erro, nenhuma peça

-- V4: Testar validação de carga
SELECT cq_validar_carga('{"class_a": 10, "class_b": 5, "class_c": 3, "total_classificado": 20}'::JSONB);
-- Espera: ABC mismatch (10+5+3=18 ≠ 20)

-- V5: Testar validação de inspeção
SELECT cq_validar_inspecao('{"fornecedor": "", "produto": "Omaso", "quantidade_analisada": 0}'::JSONB);
-- Espera: erros em fornecedor e quantidade_analisada
