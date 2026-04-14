-- ═══════════════════════════════════════════════════════════════════════════
-- VIEWS_FRONTEND_COMPATIBILIDADE.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Proposito: garantir que `vw_funcionario_base` e `rh_colaboradores` existam
--            como views sobre `rh_funcionarios` (tabela canonica v2) com as
--            colunas exatas que o frontend RH espera consumir.
--
-- Origem do problema (auditoria 2026-04-14):
--   - 7 telas referenciam `vw_funcionario_base` mas nao havia definicao no repo
--   - 25+ telas referenciam `rh_colaboradores` que em producao e' view sobre
--     rh_funcionarios, mas o repo so' tinha a definicao legacy como TABELA em
--     `00_schema_supabase.sql` (sem coluna `data_nascimento`)
--
-- Idempotente: pode rodar varias vezes. Detecta se rh_colaboradores existe
-- como TABELA com dados reais e nesse caso aborta com erro instrutivo.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. SAFETY CHECK: rh_colaboradores ja' e' view ou nao existe ─────────────
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
    -- e' uma TABELA fisica — verificar se tem dados
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


-- ── 2. vw_funcionario_base ──────────────────────────────────────────────────
-- View enxuta para listagens e busca global.
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


-- ── 3. rh_colaboradores (view de compatibilidade) ──────────────────────────
-- Mantem a interface esperada por 25+ telas legadas que ainda referenciam
-- `rh_colaboradores`. Mapeia diretamente sobre rh_funcionarios.
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
  -- categoria: mapeamento legado (101=CLT, 103=Experiencia)
  CASE
    WHEN f.modalidade_contrato = 'Experiencia' THEN '103'
    WHEN f.tipo_vinculo = 'CLT'                THEN '101'
    WHEN f.tipo_vinculo = 'APAC'               THEN '102'
    ELSE '999'
  END                                                           AS categoria,
  fl.nome_fantasia                                              AS estabelecimento,
  f.turno,
  f.status,
  -- unidade: derivada de filial.nome_fantasia (compat: 'classic' | 'apac')
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


-- ── 4. Permissoes (consumida por anon e authenticated via PostgREST) ───────
GRANT SELECT ON public.vw_funcionario_base TO anon, authenticated;
GRANT SELECT ON public.rh_colaboradores    TO anon, authenticated;


-- ── 5. Validacao final — campos do contrato com o frontend ─────────────────
DO $verify$
BEGIN
  -- Estes SELECTs falham se a view nao expoe a coluna esperada
  PERFORM id, nome, cargo, setor, status, data_admissao, data_nascimento,
          dias_empresa, em_experiencia
    FROM public.vw_funcionario_base LIMIT 1;

  PERFORM id, nome, cpf, cargo, setor, status, data_admissao, data_nascimento,
          salario_base, cidade, categoria, unidade
    FROM public.rh_colaboradores LIMIT 1;

  RAISE NOTICE '[OK] vw_funcionario_base e rh_colaboradores expoem todas as colunas exigidas pelo frontend.';
END
$verify$;

COMMIT;
