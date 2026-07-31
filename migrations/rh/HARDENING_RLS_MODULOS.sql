-- ============================================================================
-- HARDENING_RLS_MODULOS.sql
-- Sprint 3 (P2) do PLANO_CORRECOES_SIGA.md — itens 4.1 e 4.2 (parte DB)
--
-- CONTEXTO:
--   Tres migrations antigas (schema_rh_sst_completo, MODULO_DESLIGAMENTOS_
--   TURNOVER, schema_recrutamento_selecao) criaram policies
--   `FOR ALL TO anon USING (true)` — ou seja, QUALQUER pessoa com a anon
--   key lia e escrevia funcionarios, desligamentos, saude mental, vagas,
--   candidatos etc. Alem disso a familia sst_* canonica (MODULO_SST_v2) e
--   as tabelas CIPA/eSocial (TABELAS_SST) nao tinham RLS alguma, e
--   beneficios (00_schema_supabase) aceitava escrita de qualquer
--   autenticado.
--
-- O QUE ESTE SCRIPT FAZ (script 29 da ORDEM_REBUILD):
--   1. Remove TODAS as policies exclusivas de anon do schema public
--      (o unico fluxo anon legitimo — canal de denuncias — ja opera via
--      RPC SECURITY DEFINER desde TRIAGEM_DENUNCIAS; nenhuma policy anon
--      e necessaria)
--   2. Aplica o padrao RBAC do cipa_acoes (SELECT autenticado; escrita
--      por perfil via rh_usuarios) aos grupos do plano:
--      - Desligamentos (taxonomia/categorias/perguntas/entrevista):
--        escrita administrador, rh
--      - Recrutamento (rs_*): escrita administrador, rh, gestor
--      - Beneficios (vale_alimentacao/auxilio_deslocamento/plano_alianca):
--        escrita administrador, rh
--      - SST (sst_aso, sst_treinamento, sst_tipo_treinamento, sst_acidente,
--        esocial_eventos): escrita administrador, rh, sst
--      - CIPA (mandato/membros/reunioes/presencas): escrita administrador,
--        rh, sst (o plano listava so admin/rh; sst incluido porque a CIPA
--        e operada pela equipe SST — mesma regra da triagem de denuncias)
--   3. Guard de perfil no motor_decisoes() (item 4.2): so administrador/
--      rh/gestor — via wrapper que preserva o corpo original
--      (motor_decisoes_core), sem duplicar 200 linhas de logica
--
-- FORA DE ESCOPO (deliberado):
--   - Projeto CQ (cq_sal_*, cq_ficha_producao): banco Supabase separado
--     (nvqxsulntpftcwtkjedu) sem scripts de rebuild versionados no repo —
--     hardening exige snapshot do schema de producao. Pendencia registrada
--     no PLANO_CORRECOES_SIGA 4.1.
--   - Policies `FOR ALL TO authenticated USING(true)` de tabelas core
--     (rh_funcionarios etc.): SEGURANCA_SUPABASE ja adiciona policies por
--     perfil; a consolidacao total (drop das permissivas) e a Fase 3.
--
-- ATENCAO (ordem): este script deve rodar DEPOIS de FUNCOES_APLICACAO
--   (que cria motor_decisoes) e de TRIAGEM_DENUNCIAS. Se FUNCOES_APLICACAO
--   for reaplicada isolada em producao, reaplicar este script na sequencia
--   (o wrapper do motor_decisoes e recriado).
--
-- Idempotente.
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 0. Colunas de afastamento ausentes no rebuild (descoberto no E2E)
-- ════════════════════════════════════════════════════════════════════════
-- FUNCOES_APLICACAO (motor_decisoes, ficha 360) referencia colunas de
-- afastamento de rh_funcionarios que existem em producao mas nunca foram
-- versionadas — em rebuild limpo essas RPCs falham em runtime. Em
-- producao os ADD COLUMN IF NOT EXISTS sao no-op.

ALTER TABLE public.rh_funcionarios ADD COLUMN IF NOT EXISTS tipo_afastamento TEXT;
ALTER TABLE public.rh_funcionarios ADD COLUMN IF NOT EXISTS afastamento_inicio DATE;
ALTER TABLE public.rh_funcionarios ADD COLUMN IF NOT EXISTS afastamento_previsao_retorno DATE;
ALTER TABLE public.rh_funcionarios ADD COLUMN IF NOT EXISTS afastamento_origem TEXT;

-- ════════════════════════════════════════════════════════════════════════
-- 1. Remover TODAS as policies exclusivas de anon
-- ════════════════════════════════════════════════════════════════════════

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT schemaname, tablename, policyname, roles
    FROM pg_policies
    WHERE schemaname = 'public' AND 'anon' = ANY(roles)
  LOOP
    IF r.roles = '{anon}' THEN
      EXECUTE format('DROP POLICY %I ON %I.%I', r.policyname, r.schemaname, r.tablename);
      RAISE NOTICE 'Policy anon removida: %.%', r.tablename, r.policyname;
    ELSE
      -- policy mista (anon + outros roles): nao dropar as cegas — revisar
      RAISE WARNING 'Policy mista com anon NAO removida (revisar manualmente): %.% roles=%',
        r.tablename, r.policyname, r.roles;
    END IF;
  END LOOP;
END $$;

-- ════════════════════════════════════════════════════════════════════════
-- 2. Padrao RBAC por grupo (drop de todas as policies + recriacao)
-- ════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  grupo RECORD;
  tbl TEXT;
  pol RECORD;
  v_perfis TEXT;
BEGIN
  FOR grupo IN
    SELECT * FROM (VALUES
      (ARRAY['rh_desligamento_taxonomia','rh_desligamento_categorias',
             'rh_entrevista_perguntas','rh_desligamento_entrevista'],
       ARRAY['administrador','rh']),
      (ARRAY['rs_vagas','rs_candidatos','rs_pipeline','rs_entrevistas',
             'rs_avaliacoes','rs_banco_talentos'],
       ARRAY['administrador','rh','gestor']),
      (ARRAY['rh_vale_alimentacao','rh_auxilio_deslocamento','rh_plano_alianca'],
       ARRAY['administrador','rh']),
      (ARRAY['sst_aso','sst_treinamento','sst_tipo_treinamento','sst_acidente',
             'esocial_eventos'],
       ARRAY['administrador','rh','sst']),
      (ARRAY['cipa_mandato','cipa_membros','cipa_reunioes','cipa_presencas'],
       ARRAY['administrador','rh','sst'])
    ) AS g(tabelas, perfis_escrita)
  LOOP
    v_perfis := (SELECT string_agg(quote_literal(p), ',') FROM unnest(grupo.perfis_escrita) p);

    FOREACH tbl IN ARRAY grupo.tabelas LOOP
      IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = tbl) THEN
        RAISE WARNING 'Tabela % nao existe — pulando', tbl;
        CONTINUE;
      END IF;

      EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', tbl);

      FOR pol IN
        SELECT policyname FROM pg_policies
        WHERE schemaname = 'public' AND tablename = tbl
      LOOP
        EXECUTE format('DROP POLICY %I ON public.%I', pol.policyname, tbl);
      END LOOP;

      -- SELECT: qualquer autenticado (dado operacional; telas de leitura
      -- usam perfis visualizador/gestor)
      EXECUTE format(
        'CREATE POLICY p_%s_select ON public.%I FOR SELECT TO authenticated USING (true)',
        tbl, tbl);

      -- Escrita: somente perfis do grupo (padrao cipa_acoes)
      EXECUTE format(
        'CREATE POLICY p_%s_insert ON public.%I FOR INSERT TO authenticated
         WITH CHECK (EXISTS (SELECT 1 FROM rh_usuarios
           WHERE auth_uid = auth.uid() AND perfil IN (%s) AND ativo = true))',
        tbl, tbl, v_perfis);
      EXECUTE format(
        'CREATE POLICY p_%s_update ON public.%I FOR UPDATE TO authenticated
         USING (EXISTS (SELECT 1 FROM rh_usuarios
           WHERE auth_uid = auth.uid() AND perfil IN (%s) AND ativo = true))',
        tbl, tbl, v_perfis);
      EXECUTE format(
        'CREATE POLICY p_%s_delete ON public.%I FOR DELETE TO authenticated
         USING (EXISTS (SELECT 1 FROM rh_usuarios
           WHERE auth_uid = auth.uid() AND perfil IN (%s) AND ativo = true))',
        tbl, tbl, v_perfis);

      EXECUTE format('REVOKE ALL ON public.%I FROM anon', tbl);
      EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO authenticated', tbl);
    END LOOP;
  END LOOP;
END $$;

-- O padrao EXISTS(rh_usuarios ...) exige que authenticated possa ler a
-- propria linha em rh_usuarios (no Supabase o grant existe por default;
-- explicito aqui para ambientes rebuildados do zero)
GRANT SELECT ON public.rh_usuarios TO authenticated;
REVOKE ALL ON public.rh_usuarios FROM anon;

-- ════════════════════════════════════════════════════════════════════════
-- 3. Guard de perfil no motor_decisoes() (wrapper sobre o corpo original)
-- ════════════════════════════════════════════════════════════════════════

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = 'public' AND p.proname = 'motor_decisoes_core') THEN
    ALTER FUNCTION public.motor_decisoes() RENAME TO motor_decisoes_core;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION public.motor_decisoes()
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_uid UUID;
  v_perfil TEXT;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  SELECT perfil INTO v_perfil FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh','gestor') THEN
    RETURN jsonb_build_object('ok', false, 'problemas', '[]'::jsonb, 'excluidos', '[]'::jsonb,
      'error', 'Apenas perfis administrador, rh ou gestor podem consultar o motor de decisoes. Perfil atual: ' || v_perfil);
  END IF;
  RETURN public.motor_decisoes_core();
END; $fn$;

-- core so e chamavel pelo wrapper (SECURITY DEFINER roda como owner)
REVOKE ALL ON FUNCTION public.motor_decisoes_core() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.motor_decisoes() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.motor_decisoes() TO authenticated;

COMMENT ON FUNCTION public.motor_decisoes() IS
  'Wrapper com guard de perfil (administrador/rh/gestor) sobre motor_decisoes_core(). Se FUNCOES_APLICACAO for reaplicada isolada, reaplicar HARDENING_RLS_MODULOS.';

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

-- deve retornar 0 linhas
SELECT tablename, policyname FROM pg_policies
WHERE schemaname = 'public' AND roles = '{anon}';

SELECT tablename, COUNT(*) AS policies FROM pg_policies
WHERE tablename IN ('rs_vagas','sst_aso','cipa_mandato','rh_vale_alimentacao',
                    'rh_desligamento_taxonomia','esocial_eventos')
GROUP BY tablename ORDER BY tablename;
