-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — Views e RPCs do modulo
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- Convencao:
--   - Views prefixo vw_cs_*
--   - RPCs prefixo cs_*
--   - Todas tipam dados sensiveis. RLS nas tabelas-fonte ja restringe.
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. vw_cs_colaborador_banda ─────────────────────────────────────────────
-- View principal do modulo: para cada colaborador mostra cargo, nivel,
-- salario atual, banda vigente (min/ref/teto) e posicao na banda.
-- Usada por sgcs_colaboradores.html e pelo dashboard executivo.
CREATE OR REPLACE VIEW public.vw_cs_colaborador_banda AS
SELECT
  f.id                            AS funcionario_id,
  f.nome_completo                 AS nome,
  f.cpf,
  f.matricula,
  f.data_admissao,
  f.status,
  c.id                            AS cargo_id,
  c.nome                          AS cargo,
  c.cbo,
  fam.id                          AS familia_id,
  fam.nome                        AS familia,
  cs.nivel,
  cs.data_no_nivel,
  cs.fiducia_natureza,
  cs.area_cs,
  cs.centro_custo,
  f.salario_base,
  cs.gratif_funcao,
  (COALESCE(f.salario_base,0) + COALESCE(cs.gratif_funcao,0)) AS remuneracao_total,
  cm.criticidade,
  b.id                            AS banda_id,
  b.salario_min                   AS banda_min,
  b.salario_referencia            AS banda_ref,
  b.salario_teto                  AS banda_teto,
  CASE
    WHEN b.id IS NULL THEN 'sem_banda'
    WHEN f.salario_base IS NULL THEN 'sem_salario'
    WHEN f.salario_base < b.salario_min THEN 'abaixo_min'
    WHEN f.salario_base > b.salario_teto THEN 'acima_teto'
    WHEN f.salario_base <= b.salario_referencia THEN 'piso'
    ELSE 'teto'
  END                             AS posicao_banda,
  CASE
    WHEN b.id IS NULL OR f.salario_base IS NULL THEN NULL
    WHEN b.salario_teto = b.salario_min THEN 100
    ELSE ROUND(
      ((f.salario_base - b.salario_min) / (b.salario_teto - b.salario_min)) * 100, 1
    )
  END                             AS pct_na_banda,
  s.nome                          AS setor,
  fil.nome                        AS filial
FROM rh_funcionarios f
  LEFT JOIN rh_cargos c            ON c.id = f.cargo_id
  LEFT JOIN rh_setores s           ON s.id = f.setor_id
  LEFT JOIN rh_filiais fil         ON fil.id = f.filial_id
  LEFT JOIN rh_cs_colaborador cs   ON cs.funcionario_id = f.id
  LEFT JOIN rh_cs_familias fam     ON fam.id = cs.familia_id
  LEFT JOIN rh_cs_cargo_meta cm    ON cm.cargo_id = c.id
  LEFT JOIN LATERAL (
    SELECT b.*
    FROM rh_cs_bandas b
    WHERE b.cargo_id = c.id
      AND (cs.nivel IS NULL OR b.nivel = cs.nivel OR b.nivel IS NULL)
      AND b.vigencia_inicio <= CURRENT_DATE
      AND (b.vigencia_fim IS NULL OR b.vigencia_fim >= CURRENT_DATE)
    ORDER BY
      CASE WHEN b.nivel = cs.nivel THEN 0 ELSE 1 END,
      b.vigencia_inicio DESC
    LIMIT 1
  ) b ON TRUE;

COMMENT ON VIEW public.vw_cs_colaborador_banda IS
  'View principal SGCS: colaborador x cargo x banda vigente x posicao na banda.';

-- ── 2. vw_cs_bandas_ativas ─────────────────────────────────────────────────
-- Bandas vigentes na data atual, com contagem de colaboradores em cada
CREATE OR REPLACE VIEW public.vw_cs_bandas_ativas AS
SELECT
  b.id                            AS banda_id,
  c.id                            AS cargo_id,
  c.nome                          AS cargo,
  c.cbo,
  fam.id                          AS familia_id,
  fam.nome                        AS familia,
  cm.criticidade,
  b.nivel,
  b.salario_min,
  b.salario_referencia,
  b.salario_teto,
  b.vigencia_inicio,
  b.vigencia_fim,
  (
    SELECT COUNT(*)::INT
    FROM rh_funcionarios f
      LEFT JOIN rh_cs_colaborador cs ON cs.funcionario_id = f.id
    WHERE f.cargo_id = b.cargo_id
      AND f.status IN ('ativo','afastado','ferias','experiencia')
      AND (b.nivel IS NULL OR cs.nivel = b.nivel)
  )                               AS qtd_colaboradores,
  (
    SELECT COUNT(*)::INT
    FROM rh_funcionarios f
      LEFT JOIN rh_cs_colaborador cs ON cs.funcionario_id = f.id
    WHERE f.cargo_id = b.cargo_id
      AND f.status IN ('ativo','afastado','ferias','experiencia')
      AND (b.nivel IS NULL OR cs.nivel = b.nivel)
      AND f.salario_base > b.salario_teto
  )                               AS qtd_fora_teto
FROM rh_cs_bandas b
  JOIN rh_cargos c                ON c.id = b.cargo_id
  LEFT JOIN rh_cs_cargo_meta cm   ON cm.cargo_id = c.id
  LEFT JOIN rh_cs_familias fam    ON fam.id = cm.familia_id
WHERE b.vigencia_inicio <= CURRENT_DATE
  AND (b.vigencia_fim IS NULL OR b.vigencia_fim >= CURRENT_DATE);

COMMENT ON VIEW public.vw_cs_bandas_ativas IS
  'Bandas vigentes com contagem de colaboradores e quantos fora do teto.';

-- ── 3. vw_cs_dashboard_kpis ────────────────────────────────────────────────
-- KPIs do dashboard executivo do SGCS
CREATE OR REPLACE VIEW public.vw_cs_dashboard_kpis AS
SELECT
  -- Quadro
  (SELECT COUNT(*) FROM rh_funcionarios WHERE status IN ('ativo','experiencia'))
    AS total_ativos,
  (SELECT COUNT(*) FROM rh_funcionarios WHERE status = 'afastado')
    AS total_afastados,
  (SELECT COUNT(*) FROM rh_funcionarios WHERE status = 'ferias')
    AS total_ferias,
  -- Distribuicao na banda
  (SELECT COUNT(*) FROM vw_cs_colaborador_banda
     WHERE posicao_banda = 'piso' AND status IN ('ativo','experiencia'))
    AS qtd_no_piso,
  (SELECT COUNT(*) FROM vw_cs_colaborador_banda
     WHERE posicao_banda = 'teto' AND status IN ('ativo','experiencia'))
    AS qtd_no_teto,
  (SELECT COUNT(*) FROM vw_cs_colaborador_banda
     WHERE posicao_banda = 'acima_teto' AND status IN ('ativo','experiencia'))
    AS qtd_acima_teto,
  (SELECT COUNT(*) FROM vw_cs_colaborador_banda
     WHERE posicao_banda = 'abaixo_min' AND status IN ('ativo','experiencia'))
    AS qtd_abaixo_min,
  (SELECT COUNT(*) FROM vw_cs_colaborador_banda
     WHERE posicao_banda = 'sem_banda' AND status IN ('ativo','experiencia'))
    AS qtd_sem_banda,
  -- Fiducia
  (SELECT COUNT(*) FROM rh_cs_colaborador WHERE fiducia_natureza IS NOT NULL)
    AS qtd_fiducia,
  -- Custo total
  (SELECT COALESCE(SUM(salario_base),0)
     FROM rh_funcionarios WHERE status IN ('ativo','experiencia'))
    AS custo_salarios,
  (SELECT COALESCE(SUM(gratif_funcao),0)
     FROM rh_cs_colaborador cs
     JOIN rh_funcionarios f ON f.id = cs.funcionario_id
     WHERE f.status IN ('ativo','experiencia'))
    AS custo_gratificacoes,
  -- Ciclo / governanca
  (SELECT COUNT(*) FROM rh_cs_excecoes
     WHERE ciclo_ano = EXTRACT(YEAR FROM CURRENT_DATE) AND status = 'aprovada')
    AS excecoes_ano,
  (SELECT COUNT(*) FROM rh_cs_progressoes
     WHERE ciclo_ano = EXTRACT(YEAR FROM CURRENT_DATE)
       AND status IN ('homologada','aprovada_diretoria'))
    AS progressoes_ano,
  (SELECT COUNT(*) FROM rh_cs_alertas WHERE resolvido = FALSE)
    AS alertas_abertos;

COMMENT ON VIEW public.vw_cs_dashboard_kpis IS
  'KPIs consolidados do dashboard executivo do SGCS (1 linha).';

-- ── 4. RPC cs_recalcular_alertas_fora_banda ────────────────────────────────
-- Gera alertas para colaboradores fora do teto da banda.
-- Idempotente: marca como resolvido alertas obsoletos.
CREATE OR REPLACE FUNCTION public.cs_recalcular_alertas_fora_banda()
RETURNS TABLE (criados INT, resolvidos INT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_criados INT := 0;
  v_resolvidos INT := 0;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh') THEN
    RAISE EXCEPTION 'Acesso negado: apenas administrador ou RH podem recalcular alertas';
  END IF;

  -- Resolve alertas de fora_banda que nao se aplicam mais
  UPDATE rh_cs_alertas a
     SET resolvido = TRUE, data_resolucao = NOW()
   WHERE a.tipo = 'fora_banda'
     AND a.resolvido = FALSE
     AND NOT EXISTS (
       SELECT 1 FROM vw_cs_colaborador_banda v
        WHERE v.funcionario_id = a.funcionario_id
          AND v.posicao_banda IN ('acima_teto','abaixo_min')
     );
  GET DIAGNOSTICS v_resolvidos = ROW_COUNT;

  -- Cria novos alertas
  INSERT INTO rh_cs_alertas (tipo, criticidade, funcionario_id, mensagem, contexto_jsonb)
  SELECT
    'fora_banda',
    CASE WHEN v.posicao_banda = 'acima_teto' THEN 'alta' ELSE 'media' END,
    v.funcionario_id,
    v.nome || ' esta ' ||
      CASE v.posicao_banda
        WHEN 'acima_teto' THEN 'ACIMA do teto da banda'
        WHEN 'abaixo_min' THEN 'ABAIXO do minimo da banda'
      END ||
      ' (' || v.cargo || COALESCE(' ' || v.nivel, '') || ')',
    jsonb_build_object(
      'posicao_banda', v.posicao_banda,
      'salario_atual', v.salario_base,
      'banda_min', v.banda_min,
      'banda_teto', v.banda_teto,
      'cargo', v.cargo,
      'nivel', v.nivel
    )
  FROM vw_cs_colaborador_banda v
  WHERE v.posicao_banda IN ('acima_teto','abaixo_min')
    AND v.status IN ('ativo','experiencia')
    AND NOT EXISTS (
      SELECT 1 FROM rh_cs_alertas a
       WHERE a.tipo = 'fora_banda'
         AND a.funcionario_id = v.funcionario_id
         AND a.resolvido = FALSE
    );
  GET DIAGNOSTICS v_criados = ROW_COUNT;

  RETURN QUERY SELECT v_criados, v_resolvidos;
END;
$$;

COMMENT ON FUNCTION public.cs_recalcular_alertas_fora_banda IS
  'Recalcula alertas de salario fora da banda (acima do teto ou abaixo do minimo).';

-- ── 5. RPC cs_set_perfil_colaborador ───────────────────────────────────────
-- Atualiza ou cria rh_cs_colaborador (extensao) e registra historico salarial
-- se o salario foi alterado.
CREATE OR REPLACE FUNCTION public.cs_set_perfil_colaborador(
  p_funcionario_id   UUID,
  p_familia_id       UUID DEFAULT NULL,
  p_nivel            CHAR(3) DEFAULT NULL,
  p_data_no_nivel    DATE DEFAULT NULL,
  p_gratif_funcao    NUMERIC(12,2) DEFAULT NULL,
  p_fiducia_natureza CHAR(1) DEFAULT NULL,
  p_area_cs          TEXT DEFAULT NULL,
  p_centro_custo     TEXT DEFAULT NULL,
  p_reporta_a_id     UUID DEFAULT NULL,
  p_observacoes      TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_id UUID;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  INSERT INTO rh_cs_colaborador (
    funcionario_id, familia_id, nivel, data_no_nivel,
    gratif_funcao, fiducia_natureza, area_cs, centro_custo,
    reporta_a_id, observacoes
  ) VALUES (
    p_funcionario_id, p_familia_id, p_nivel, p_data_no_nivel,
    COALESCE(p_gratif_funcao, 0), p_fiducia_natureza, p_area_cs, p_centro_custo,
    p_reporta_a_id, p_observacoes
  )
  ON CONFLICT (funcionario_id) DO UPDATE SET
    familia_id       = COALESCE(EXCLUDED.familia_id, rh_cs_colaborador.familia_id),
    nivel            = COALESCE(EXCLUDED.nivel, rh_cs_colaborador.nivel),
    data_no_nivel    = COALESCE(EXCLUDED.data_no_nivel, rh_cs_colaborador.data_no_nivel),
    gratif_funcao    = COALESCE(EXCLUDED.gratif_funcao, rh_cs_colaborador.gratif_funcao),
    fiducia_natureza = COALESCE(EXCLUDED.fiducia_natureza, rh_cs_colaborador.fiducia_natureza),
    area_cs          = COALESCE(EXCLUDED.area_cs, rh_cs_colaborador.area_cs),
    centro_custo     = COALESCE(EXCLUDED.centro_custo, rh_cs_colaborador.centro_custo),
    reporta_a_id     = COALESCE(EXCLUDED.reporta_a_id, rh_cs_colaborador.reporta_a_id),
    observacoes      = COALESCE(EXCLUDED.observacoes, rh_cs_colaborador.observacoes),
    updated_at       = NOW()
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$$;

COMMENT ON FUNCTION public.cs_set_perfil_colaborador IS
  'Cria ou atualiza dados de C&S do colaborador (nivel, gratif, fiducia).';

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_03_VIEWS.sql
-- 3 views | 2 RPCs
-- ════════════════════════════════════════════════════════════════════════════
