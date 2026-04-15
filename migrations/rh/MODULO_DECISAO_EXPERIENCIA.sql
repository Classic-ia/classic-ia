-- ═══════════════════════════════════════════════════════════════════════════
-- MODULO_DECISAO_EXPERIENCIA.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Proposito: persistir decisoes executivas sobre contratos de experiencia
--            (1o periodo 45 dias e 2o periodo 90 dias) tomadas via o painel
--            alertas_rh.html. Ate aqui o CTA "Registrar decisao" era
--            cosmetico — esta migration o torna funcional.
--
-- Componentes:
--   1. Tabela `rh_decisao_experiencia`            (1 decisao por funcionario+periodo)
--   2. Trigger updated_at + trigger auditoria
--   3. RLS: rh/admin escrevem; autenticados leem
--   4. RPC `rh_registrar_decisao_experiencia`     (UPSERT seguro)
--   5. RPC `rh_listar_decisoes_experiencia`       (devolve mapa pro frontend)
--   6. View `vw_decisao_experiencia`              (decisao + dados do funcionario)
--
-- Idempotente.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. TABELA ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.rh_decisao_experiencia (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  funcionario_id  UUID NOT NULL REFERENCES public.rh_funcionarios(id) ON DELETE CASCADE,
  periodo         SMALLINT NOT NULL CHECK (periodo IN (1, 2)),
  decisao         VARCHAR(20) NOT NULL
                  CHECK (decisao IN ('renovar', 'avaliar', 'ressalva', 'nao-renovar')),
  justificativa   TEXT,
  data_admissao   DATE NOT NULL,
  data_vencimento DATE NOT NULL,
  decidido_por    UUID REFERENCES auth.users(id),
  decidido_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Justificativa obrigatoria para decisoes terminais
  CONSTRAINT chk_just_obrigatoria CHECK (
    decisao NOT IN ('nao-renovar', 'ressalva')
    OR (justificativa IS NOT NULL AND length(trim(justificativa)) >= 10)
  ),

  -- Uma decisao ativa por funcionario+periodo (re-decisao SOBRESCREVE)
  CONSTRAINT uq_decisao_funcionario_periodo UNIQUE (funcionario_id, periodo)
);

CREATE INDEX IF NOT EXISTS idx_decisao_funcionario ON public.rh_decisao_experiencia(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_decisao_data        ON public.rh_decisao_experiencia(decidido_em DESC);
CREATE INDEX IF NOT EXISTS idx_decisao_decisao     ON public.rh_decisao_experiencia(decisao);

COMMENT ON TABLE public.rh_decisao_experiencia IS
  'Decisoes registradas via painel de pendencias executivas (alertas_rh.html). '
  '1 decisao ativa por funcionario+periodo. Re-decisao sobrescreve via UPSERT no RPC.';

-- ── 2. Trigger updated_at ──────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.fn_decisao_exp_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_decisao_exp_updated_at ON public.rh_decisao_experiencia;
CREATE TRIGGER trg_decisao_exp_updated_at
  BEFORE UPDATE ON public.rh_decisao_experiencia
  FOR EACH ROW EXECUTE FUNCTION public.fn_decisao_exp_updated_at();

-- ── 3. RLS ─────────────────────────────────────────────────────────────────
ALTER TABLE public.rh_decisao_experiencia ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_decisao_exp_read   ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_write  ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_update ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_delete ON public.rh_decisao_experiencia;

-- READ: todos autenticados
CREATE POLICY p_decisao_exp_read ON public.rh_decisao_experiencia
  FOR SELECT TO authenticated USING (true);

-- WRITE: rh + admin
CREATE POLICY p_decisao_exp_write ON public.rh_decisao_experiencia
  FOR INSERT TO authenticated
  WITH CHECK (public.has_perfil('rh') OR public.has_perfil('admin'));

CREATE POLICY p_decisao_exp_update ON public.rh_decisao_experiencia
  FOR UPDATE TO authenticated
  USING      (public.has_perfil('rh') OR public.has_perfil('admin'))
  WITH CHECK (public.has_perfil('rh') OR public.has_perfil('admin'));

-- DELETE: bloqueado (decisao e' historico imutavel; UPDATE sobrescreve)
CREATE POLICY p_decisao_exp_delete ON public.rh_decisao_experiencia
  FOR DELETE TO authenticated USING (false);

-- ── 4. RPC: registrar decisao (UPSERT) ─────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rh_registrar_decisao_experiencia(
  p_funcionario_id  UUID,
  p_periodo         SMALLINT,
  p_decisao         TEXT,
  p_justificativa   TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_funcionario  RECORD;
  v_decisao_id   UUID;
  v_was_update   BOOLEAN := false;
  v_uid          UUID := auth.uid();
BEGIN
  -- Permissao
  IF v_uid IS NULL THEN
    RAISE EXCEPTION 'Sessao nao autenticada' USING ERRCODE = '28000';
  END IF;
  IF NOT (public.has_perfil('rh') OR public.has_perfil('admin')) THEN
    RAISE EXCEPTION 'Apenas perfis rh/admin podem registrar decisoes' USING ERRCODE = '42501';
  END IF;

  -- Validacao de entrada
  IF p_periodo NOT IN (1, 2) THEN
    RAISE EXCEPTION 'periodo deve ser 1 (45 dias) ou 2 (90 dias), recebido: %', p_periodo
      USING ERRCODE = '22023';
  END IF;
  IF p_decisao NOT IN ('renovar','avaliar','ressalva','nao-renovar') THEN
    RAISE EXCEPTION 'decisao invalida: %', p_decisao USING ERRCODE = '22023';
  END IF;
  IF p_decisao IN ('nao-renovar','ressalva')
     AND (p_justificativa IS NULL OR length(trim(p_justificativa)) < 10) THEN
    RAISE EXCEPTION 'justificativa de pelo menos 10 caracteres e obrigatoria para decisao "%"', p_decisao
      USING ERRCODE = '22023';
  END IF;

  -- Buscar funcionario + validar que esta em janela de experiencia
  SELECT id, nome_completo, data_admissao, status
    INTO v_funcionario
    FROM public.rh_funcionarios
   WHERE id = p_funcionario_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'funcionario nao encontrado: %', p_funcionario_id USING ERRCODE = '23503';
  END IF;
  IF v_funcionario.status = 'desligado' THEN
    RAISE EXCEPTION 'funcionario % ja foi desligado, decisao nao se aplica', v_funcionario.nome_completo
      USING ERRCODE = '22023';
  END IF;

  -- UPSERT
  INSERT INTO public.rh_decisao_experiencia (
    funcionario_id, periodo, decisao, justificativa,
    data_admissao, data_vencimento, decidido_por
  ) VALUES (
    p_funcionario_id, p_periodo, p_decisao, NULLIF(trim(p_justificativa), ''),
    v_funcionario.data_admissao,
    v_funcionario.data_admissao + (CASE p_periodo WHEN 1 THEN 45 ELSE 90 END),
    v_uid
  )
  ON CONFLICT (funcionario_id, periodo) DO UPDATE
    SET decisao        = EXCLUDED.decisao,
        justificativa  = EXCLUDED.justificativa,
        decidido_por   = EXCLUDED.decidido_por,
        decidido_em    = now(),
        updated_at     = now()
  RETURNING id, (xmax::text::int > 0) INTO v_decisao_id, v_was_update;

  -- Log
  INSERT INTO public.rh_audit_log (tabela, registro_id, acao, dados_novos, usuario_id)
  VALUES (
    'rh_decisao_experiencia',
    v_decisao_id::text,
    CASE WHEN v_was_update THEN 'UPDATE' ELSE 'INSERT' END,
    jsonb_build_object(
      'funcionario_id', p_funcionario_id,
      'funcionario_nome', v_funcionario.nome_completo,
      'periodo', p_periodo,
      'decisao', p_decisao,
      'justificativa', p_justificativa
    ),
    v_uid
  );

  RETURN jsonb_build_object(
    'ok', true,
    'decisao_id', v_decisao_id,
    'foi_atualizacao', v_was_update,
    'funcionario_nome', v_funcionario.nome_completo,
    'periodo', p_periodo,
    'decisao', p_decisao
  );
END;
$$;

COMMENT ON FUNCTION public.rh_registrar_decisao_experiencia IS
  'UPSERT de decisao executiva sobre contrato de experiencia. '
  'Permissao: perfil rh ou admin. '
  'Justificativa >=10 chars obrigatoria para decisoes "nao-renovar" e "ressalva".';

GRANT EXECUTE ON FUNCTION public.rh_registrar_decisao_experiencia
  TO authenticated;

-- ── 5. RPC: listar decisoes do mes corrente ────────────────────────────────
-- Devolve mapa que o frontend usa para renderizar badges nos cards
CREATE OR REPLACE FUNCTION public.rh_listar_decisoes_experiencia()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'funcionario_id', d.funcionario_id,
    'periodo',        d.periodo,
    'decisao',        d.decisao,
    'justificativa',  d.justificativa,
    'decidido_em',    d.decidido_em,
    'decidido_por',   d.decidido_por,
    'data_vencimento', d.data_vencimento
  )), '[]'::jsonb)
  FROM public.rh_decisao_experiencia d
  WHERE d.data_vencimento >= CURRENT_DATE - INTERVAL '180 days';
$$;

COMMENT ON FUNCTION public.rh_listar_decisoes_experiencia IS
  'Lista decisoes dos ultimos 180 dias para popular o painel alertas_rh.html.';

GRANT EXECUTE ON FUNCTION public.rh_listar_decisoes_experiencia TO authenticated;

-- ── 6. VIEW: decisao + funcionario ─────────────────────────────────────────
DROP VIEW IF EXISTS public.vw_decisao_experiencia CASCADE;
CREATE VIEW public.vw_decisao_experiencia AS
SELECT
  d.id,
  d.funcionario_id,
  f.nome_completo                AS funcionario_nome,
  d.periodo,
  d.decisao,
  d.justificativa,
  d.data_admissao,
  d.data_vencimento,
  (d.data_vencimento - CURRENT_DATE)::INTEGER  AS dias_para_vencer,
  d.decidido_por,
  d.decidido_em,
  d.created_at,
  d.updated_at
FROM public.rh_decisao_experiencia d
JOIN public.rh_funcionarios f ON f.id = d.funcionario_id;

COMMENT ON VIEW public.vw_decisao_experiencia IS
  'Decisoes com nome do funcionario e dias para vencer (consumo direto pelo frontend opcional).';

GRANT SELECT ON public.vw_decisao_experiencia TO authenticated;

-- ── 7. Validacao final ─────────────────────────────────────────────────────
DO $verify$
BEGIN
  PERFORM id FROM public.rh_decisao_experiencia LIMIT 1;
  PERFORM id FROM public.vw_decisao_experiencia LIMIT 1;
  RAISE NOTICE '[OK] tabela rh_decisao_experiencia e view vw_decisao_experiencia criadas.';
END
$verify$;

COMMIT;
