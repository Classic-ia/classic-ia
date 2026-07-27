-- ═══════════════════════════════════════════════════════════════════════════
-- 12_hardening_rls_cq.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Auditoria C-02/C-03: endurecer RLS do modulo CQ.
-- Remove acesso anon, substitui USING(true) por RBAC via cq_usuarios.
--
-- Pre-requisito: cq_e_autorizado() e cq_e_gestor() devem existir
-- (criadas em seg_sal_ficha).
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── Helper: usuario CQ autenticado e ativo ──────────────────────────────

CREATE OR REPLACE FUNCTION public.cq_is_authenticated()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM cq_usuarios
    WHERE auth_uid = auth.uid()
      AND ativo = true
  );
$$;

GRANT EXECUTE ON FUNCTION public.cq_is_authenticated TO authenticated;

-- ── Dropar policies anon do CQ ──────────────────────────────────────────

DO $revoke_cq$
DECLARE
  pol RECORD;
BEGIN
  FOR pol IN
    SELECT policyname, tablename
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename LIKE 'cq_%'
      AND (roles @> ARRAY['anon'] OR roles = ARRAY['anon'])
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', pol.policyname, pol.tablename);
    RAISE NOTICE 'Dropped CQ anon policy: % on %', pol.policyname, pol.tablename;
  END LOOP;

  -- Revogar grants
  FOR pol IN
    SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'cq_%'
  LOOP
    EXECUTE format('REVOKE ALL ON public.%I FROM anon', pol.tablename);
  END LOOP;

  RAISE NOTICE '[OK] Acesso anon revogado em todas as tabelas cq_*.';
END
$revoke_cq$;

-- ── Endurecer tabelas CQ criticas ───────────────────────────────────────

DO $harden_cq$
DECLARE
  tbl TEXT;
  pol RECORD;
BEGIN
  FOREACH tbl IN ARRAY ARRAY[
    'cq_fornecedores',
    'cq_produtos_v2',
    'cq_lotes',
    'cq_recebimentos_v2',
    'cq_inspecoes',
    'cq_inspecao_defeitos',
    'cq_defeitos',
    'cq_nao_conformidade',
    'cq_overrides_v2',
    'cq_classificacoes_recebimento'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=tbl) THEN
      RAISE NOTICE 'Tabela % nao existe, pulando.', tbl;
      CONTINUE;
    END IF;

    -- Dropar policies USING(true)
    FOR pol IN
      SELECT policyname FROM pg_policies
      WHERE schemaname='public' AND tablename=tbl AND qual = 'true'
    LOOP
      EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', pol.policyname, tbl);
    END LOOP;

    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', tbl);

    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR SELECT TO authenticated USING (public.cq_is_authenticated())',
      tbl || '_sel_auth', tbl
    );
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR INSERT TO authenticated WITH CHECK (public.cq_is_authenticated())',
      tbl || '_ins_auth', tbl
    );
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR UPDATE TO authenticated USING (public.cq_is_authenticated()) WITH CHECK (public.cq_is_authenticated())',
      tbl || '_upd_auth', tbl
    );
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR DELETE TO authenticated USING (false)',
      tbl || '_del_deny', tbl
    );

    RAISE NOTICE '[OK] CQ policies endurecidas para %', tbl;
  END LOOP;
END
$harden_cq$;

COMMIT;
