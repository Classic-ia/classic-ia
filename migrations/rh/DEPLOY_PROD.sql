-- ═══════════════════════════════════════════════════════════════════════════
-- DEPLOY_PROD.sql — MIGRATION CONSOLIDADA PARA SUPABASE SQL EDITOR
-- ═══════════════════════════════════════════════════════════════════════════
-- Copie TODO este arquivo e cole num unico run no SQL Editor do Supabase.
-- NAO usa meta-comandos psql (\i, \echo). Idempotente — pode rodar varias vezes.
--
-- Conteudo (3 migrations em ordem):
--   1. VIEWS_FRONTEND_COMPATIBILIDADE.sql  (vw_funcionario_base, rh_colaboradores)
--   2. MODULO_DECISAO_EXPERIENCIA.sql      (decisao de contrato de experiencia)
--   3. MODULO_CIPA_ACOES.sql               (backend de acoes em cipa.html)
--
-- Pre-requisitos ja existentes em producao:
--   rh_funcionarios, rh_setores, rh_cargos, rh_filiais, rh_usuarios,
--   rh_audit_log, cipa_mandato, cipa_reunioes, auth.users
--
-- Tempo estimado: <2s.
-- ═══════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════
-- PARTE 1/3: VIEWS DE COMPATIBILIDADE (vw_funcionario_base + rh_colaboradores)
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- Safety check: rh_colaboradores ja' e' view ou nao existe
DO $check$
DECLARE
  rel_kind  CHAR(1);
  row_count BIGINT;
BEGIN
  SELECT relkind INTO rel_kind
    FROM pg_class
   WHERE relname = 'rh_colaboradores'
     AND relnamespace = 'public'::regnamespace;

  IF rel_kind = 'r' THEN
    EXECUTE 'SELECT count(*) FROM public.rh_colaboradores' INTO row_count;
    IF row_count > 0 THEN
      RAISE EXCEPTION
        'rh_colaboradores existe como TABELA com % registros. Esta migration assume que rh_colaboradores e'' uma VIEW. Migre os dados para rh_funcionarios e DROP TABLE antes de aplicar.', row_count;
    ELSE
      RAISE NOTICE 'rh_colaboradores existe como TABELA vazia — sera convertida em VIEW.';
      DROP TABLE public.rh_colaboradores CASCADE;
    END IF;
  ELSIF rel_kind = 'v' THEN
    RAISE NOTICE 'rh_colaboradores ja existe como VIEW — sera substituida.';
  ELSIF rel_kind IS NULL THEN
    RAISE NOTICE 'rh_colaboradores nao existe — sera criada como VIEW.';
  END IF;
END
$check$;

DROP VIEW IF EXISTS public.vw_funcionario_base CASCADE;

CREATE VIEW public.vw_funcionario_base AS
SELECT
  f.id,
  f.nome_completo                                               AS nome,
  f.nome_completo,
  f.cpf,
  f.matricula,
  f.data_admissao,
  f.data_nascimento,
  f.data_desligamento,
  f.status,
  f.salario_base,
  f.turno,
  f.tipo_vinculo,
  f.modalidade_contrato,
  c.nome                                                        AS cargo,
  s.nome                                                        AS setor,
  f.setor_id,
  f.cargo_id,
  f.filial_id,
  fl.nome_fantasia                                              AS unidade,
  (CURRENT_DATE - f.data_admissao)::INTEGER                     AS dias_empresa,
  (
    f.status = 'experiencia'
    OR (CURRENT_DATE - f.data_admissao) <= 90
  )                                                             AS em_experiencia
FROM public.rh_funcionarios f
LEFT JOIN public.rh_cargos   c  ON c.id  = f.cargo_id
LEFT JOIN public.rh_setores  s  ON s.id  = f.setor_id
LEFT JOIN public.rh_filiais  fl ON fl.id = f.filial_id;

COMMENT ON VIEW public.vw_funcionario_base IS
  'Compatibilidade com o frontend (consumido por shell.js, app.html, organograma.html, fluxo_funcionarios.html, page_bootstrap.js, api.js).';

DROP VIEW IF EXISTS public.rh_colaboradores CASCADE;

CREATE VIEW public.rh_colaboradores AS
SELECT
  f.id,
  f.nome_completo                                               AS nome,
  f.cpf,
  f.matricula,
  c.nome                                                        AS cargo,
  c.cbo,
  f.salario_base,
  f.data_admissao,
  f.data_desligamento,
  f.data_nascimento,
  COALESCE(f.endereco->>'cidade', '')                           AS cidade,
  COALESCE(f.endereco->>'uf', 'PR')                             AS estado,
  s.nome                                                        AS setor,
  f.setor_id,
  CASE
    WHEN f.modalidade_contrato = 'Experiencia' THEN '103'
    WHEN f.tipo_vinculo = 'CLT'                THEN '101'
    WHEN f.tipo_vinculo = 'APAC'               THEN '102'
    ELSE '999'
  END                                                           AS categoria,
  fl.nome_fantasia                                              AS estabelecimento,
  f.turno,
  f.status,
  CASE
    WHEN fl.nome_fantasia ILIKE '%apac%' THEN 'apac'
    ELSE 'classic'
  END                                                           AS unidade,
  f.created_at                                                  AS criado_em,
  f.updated_at                                                  AS atualizado_em
FROM public.rh_funcionarios f
LEFT JOIN public.rh_cargos   c  ON c.id  = f.cargo_id
LEFT JOIN public.rh_setores  s  ON s.id  = f.setor_id
LEFT JOIN public.rh_filiais  fl ON fl.id = f.filial_id;

COMMENT ON VIEW public.rh_colaboradores IS
  'View de compatibilidade. NAO inserir/atualizar diretamente — escreva em rh_funcionarios. Consumida por 25+ telas RH legadas.';

GRANT SELECT ON public.vw_funcionario_base TO authenticated;
GRANT SELECT ON public.rh_colaboradores    TO authenticated;

DO $verify$
BEGIN
  PERFORM id, nome, cargo, setor, status, data_admissao, data_nascimento,
          dias_empresa, em_experiencia
    FROM public.vw_funcionario_base LIMIT 1;

  PERFORM id, nome, cpf, cargo, setor, status, data_admissao, data_nascimento,
          salario_base, cidade, categoria, unidade
    FROM public.rh_colaboradores LIMIT 1;

  RAISE NOTICE '[PARTE 1/3 OK] vw_funcionario_base e rh_colaboradores criadas.';
END
$verify$;

COMMIT;


-- ═══════════════════════════════════════════════════════════════════════════
-- PARTE 2/3: MODULO DECISAO EXPERIENCIA (tabela + RPC + view)
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

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

  CONSTRAINT chk_just_obrigatoria CHECK (
    decisao NOT IN ('nao-renovar', 'ressalva')
    OR (justificativa IS NOT NULL AND length(trim(justificativa)) >= 10)
  ),
  CONSTRAINT uq_decisao_funcionario_periodo UNIQUE (funcionario_id, periodo)
);

CREATE INDEX IF NOT EXISTS idx_decisao_funcionario ON public.rh_decisao_experiencia(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_decisao_data        ON public.rh_decisao_experiencia(decidido_em DESC);
CREATE INDEX IF NOT EXISTS idx_decisao_decisao     ON public.rh_decisao_experiencia(decisao);

COMMENT ON TABLE public.rh_decisao_experiencia IS
  'Decisoes registradas via painel de pendencias executivas (alertas_rh.html). '
  '1 decisao ativa por funcionario+periodo. Re-decisao sobrescreve via UPSERT no RPC.';

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

ALTER TABLE public.rh_decisao_experiencia ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_decisao_exp_read   ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_write  ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_update ON public.rh_decisao_experiencia;
DROP POLICY IF EXISTS p_decisao_exp_delete ON public.rh_decisao_experiencia;

CREATE OR REPLACE FUNCTION public.fn_decisao_exp_pode_gravar()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador', 'rh')
       AND ativo = true
  );
$$;

CREATE POLICY p_decisao_exp_read ON public.rh_decisao_experiencia
  FOR SELECT TO authenticated USING (true);

CREATE POLICY p_decisao_exp_write ON public.rh_decisao_experiencia
  FOR INSERT TO authenticated
  WITH CHECK (public.fn_decisao_exp_pode_gravar());

CREATE POLICY p_decisao_exp_update ON public.rh_decisao_experiencia
  FOR UPDATE TO authenticated
  USING      (public.fn_decisao_exp_pode_gravar())
  WITH CHECK (public.fn_decisao_exp_pode_gravar());

CREATE POLICY p_decisao_exp_delete ON public.rh_decisao_experiencia
  FOR DELETE TO authenticated USING (false);

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
  IF v_uid IS NULL THEN
    RAISE EXCEPTION 'Sessao nao autenticada' USING ERRCODE = '28000';
  END IF;
  IF NOT public.fn_decisao_exp_pode_gravar() THEN
    RAISE EXCEPTION 'Apenas perfis administrador/rh podem registrar decisoes' USING ERRCODE = '42501';
  END IF;

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
  'Permissao: perfil rh ou administrador em rh_usuarios. '
  'Justificativa >=10 chars obrigatoria para decisoes "nao-renovar" e "ressalva".';

GRANT EXECUTE ON FUNCTION public.rh_registrar_decisao_experiencia TO authenticated;

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

DO $verify$
BEGIN
  PERFORM id FROM public.rh_decisao_experiencia LIMIT 1;
  PERFORM id FROM public.vw_decisao_experiencia LIMIT 1;
  RAISE NOTICE '[PARTE 2/3 OK] rh_decisao_experiencia + RPC + view criadas.';
END
$verify$;

COMMIT;


-- ═══════════════════════════════════════════════════════════════════════════
-- PARTE 3/3: MODULO CIPA ACOES (backend da aba Acoes em cipa.html)
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

CREATE TABLE IF NOT EXISTS public.cipa_acoes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reuniao_id      UUID,
  mandato_id      UUID,
  acao            TEXT NOT NULL,
  descricao       TEXT,
  responsavel     TEXT NOT NULL,
  responsavel_id  UUID,
  prazo           DATE,
  status          VARCHAR(20) NOT NULL DEFAULT 'aberto'
                  CHECK (status IN ('aberto','andamento','concluido','cancelado')),
  prioridade      VARCHAR(10) DEFAULT 'media'
                  CHECK (prioridade IN ('baixa','media','alta','critica')),
  data_conclusao  DATE,
  observacoes     TEXT,
  criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
  atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
  criado_por      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_cipa_acoes_reuniao   ON public.cipa_acoes(reuniao_id);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_mandato   ON public.cipa_acoes(mandato_id);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_status    ON public.cipa_acoes(status);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_prazo     ON public.cipa_acoes(prazo);

COMMENT ON TABLE public.cipa_acoes IS
  'Acoes/deliberacoes registradas em reunioes de CIPA com tracking de status e prazo.';

CREATE OR REPLACE FUNCTION public.fn_cipa_acoes_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.atualizado_em := now(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS trg_cipa_acoes_updated_at ON public.cipa_acoes;
CREATE TRIGGER trg_cipa_acoes_updated_at
  BEFORE UPDATE ON public.cipa_acoes
  FOR EACH ROW EXECUTE FUNCTION public.fn_cipa_acoes_updated_at();

ALTER TABLE public.cipa_acoes ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_cipa_acoes_read   ON public.cipa_acoes;
DROP POLICY IF EXISTS p_cipa_acoes_write  ON public.cipa_acoes;
DROP POLICY IF EXISTS p_cipa_acoes_update ON public.cipa_acoes;

CREATE POLICY p_cipa_acoes_read ON public.cipa_acoes
  FOR SELECT TO authenticated USING (true);

CREATE POLICY p_cipa_acoes_write ON public.cipa_acoes
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh')
       AND ativo = true
  ));

CREATE POLICY p_cipa_acoes_update ON public.cipa_acoes
  FOR UPDATE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh')
       AND ativo = true
  ));

GRANT SELECT, INSERT, UPDATE ON public.cipa_acoes TO authenticated;

DO $verify$
BEGIN
  PERFORM id FROM public.cipa_acoes LIMIT 1;
  RAISE NOTICE '[PARTE 3/3 OK] cipa_acoes + RLS criadas.';
  RAISE NOTICE '═══════════════════════════════════════════';
  RAISE NOTICE 'DEPLOY COMPLETO. Todas as 3 partes aplicadas.';
  RAISE NOTICE '═══════════════════════════════════════════';
END
$verify$;

COMMIT;
