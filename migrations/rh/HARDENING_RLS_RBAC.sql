-- ═══════════════════════════════════════════════════════════════════════════
-- HARDENING_RLS_RBAC.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Auditoria C-02: substituir USING(true) / WITH CHECK(true) por RBAC real.
--
-- Estrategia: usar rh_usuarios como backbone RBAC (schema de producao).
-- Helper reutilizavel `is_authenticated_user()` que verifica auth.uid()
-- esta ativo em rh_usuarios. Policies de ESCRITA exigem perfil especifico.
--
-- APLICAR DEPOIS de HARDENING_REVOKE_ANON.sql.
-- Idempotente. Roda no SQL Editor do Supabase.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── Helper: usuario autenticado e ativo ─────────────────────────────────

CREATE OR REPLACE FUNCTION public.is_authenticated_user()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid()
      AND ativo = true
  );
$$;

CREATE OR REPLACE FUNCTION public.is_rh_or_admin()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid()
      AND perfil IN ('administrador', 'rh')
      AND ativo = true
  );
$$;

CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid()
      AND perfil = 'administrador'
      AND ativo = true
  );
$$;

GRANT EXECUTE ON FUNCTION public.is_authenticated_user TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_rh_or_admin TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_admin TO authenticated;

-- ── Endurecer tabelas CRITICAS (dados pessoais e sensiveis) ─────────────

-- rh_funcionarios: SELECT para autenticados, WRITE para rh/admin
DO $harden$
DECLARE
  tbl TEXT;
  pol RECORD;
BEGIN
  -- Lista de tabelas criticas para endurecer
  FOREACH tbl IN ARRAY ARRAY[
    'rh_funcionarios',
    'rh_asos',
    'rh_treinamentos',
    'rh_ocorrencias',
    'rh_apuracao_mensal',
    'rh_saude_mental',
    'rh_decisao_experiencia',
    'rh_audit_log',
    'rh_usuarios'
  ] LOOP
    -- Verificar se tabela existe
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=tbl) THEN
      RAISE NOTICE 'Tabela % nao existe, pulando.', tbl;
      CONTINUE;
    END IF;

    -- Dropar policies USING(true) existentes
    FOR pol IN
      SELECT policyname FROM pg_policies
      WHERE schemaname='public' AND tablename=tbl
        AND qual = 'true'
    LOOP
      EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', pol.policyname, tbl);
      RAISE NOTICE 'Dropped permissive policy: % on %', pol.policyname, tbl;
    END LOOP;

    -- RLS ativa
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', tbl);

    -- SELECT: apenas usuarios autenticados ativos
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR SELECT TO authenticated USING (public.is_authenticated_user())',
      tbl || '_sel_auth', tbl
    );

    -- INSERT: apenas rh/admin
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR INSERT TO authenticated WITH CHECK (public.is_rh_or_admin())',
      tbl || '_ins_rh', tbl
    );

    -- UPDATE: apenas rh/admin
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR UPDATE TO authenticated USING (public.is_rh_or_admin()) WITH CHECK (public.is_rh_or_admin())',
      tbl || '_upd_rh', tbl
    );

    -- DELETE: bloqueado (registros auditaveis nao devem ser excluidos)
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR DELETE TO authenticated USING (false)',
      tbl || '_del_deny', tbl
    );

    RAISE NOTICE '[OK] Policies endurecidas para %', tbl;
  END LOOP;
END
$harden$;

-- ── Auditoria: tornar imutavel via RULE (A-03) ─────────────────────────

DO $audit_immutable$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='rh_audit_log') THEN
    -- Bloquear UPDATE via rule (superuser-safe, diferente de RLS)
    DROP RULE IF EXISTS audit_no_update ON public.rh_audit_log;
    CREATE RULE audit_no_update AS ON UPDATE TO public.rh_audit_log DO INSTEAD NOTHING;

    -- Bloquear DELETE via rule
    DROP RULE IF EXISTS audit_no_delete ON public.rh_audit_log;
    CREATE RULE audit_no_delete AS ON DELETE TO public.rh_audit_log DO INSTEAD NOTHING;

    RAISE NOTICE '[OK] rh_audit_log agora imutavel via RULE (nao bypassavel por RLS).';
  ELSE
    RAISE NOTICE 'rh_audit_log nao existe, pulando.';
  END IF;
END
$audit_immutable$;

-- ── Verificacao final ───────────────────────────────────────────────────

DO $verify$
DECLARE
  remaining INT;
BEGIN
  SELECT count(*) INTO remaining
  FROM pg_policies
  WHERE schemaname = 'public'
    AND qual = 'true'
    AND tablename IN (
      'rh_funcionarios', 'rh_asos', 'rh_treinamentos',
      'rh_ocorrencias', 'rh_apuracao_mensal', 'rh_saude_mental',
      'rh_decisao_experiencia', 'rh_audit_log', 'rh_usuarios'
    );

  IF remaining > 0 THEN
    RAISE WARNING '[ATENCAO] % policies USING(true) ainda restam nas tabelas criticas.', remaining;
  ELSE
    RAISE NOTICE '[OK] Zero policies USING(true) nas tabelas criticas.';
  END IF;
END
$verify$;

COMMIT;
