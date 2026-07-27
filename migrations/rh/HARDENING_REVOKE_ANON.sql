-- ═══════════════════════════════════════════════════════════════════════════
-- HARDENING_REVOKE_ANON.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Auditoria C-03: revogar acesso anon em TODAS as tabelas operacionais.
-- O papel anon NAO deve ler dados de RH, SST, producao ou qualidade.
--
-- Idempotente. Aplicar no Supabase SQL Editor (RH e CQ separadamente).
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. Dropar TODAS as policies anon existentes ─────────────────────────

DO $revoke$
DECLARE
  pol RECORD;
BEGIN
  FOR pol IN
    SELECT policyname, tablename
    FROM pg_policies
    WHERE schemaname = 'public'
      AND (roles @> ARRAY['anon'] OR roles = ARRAY['anon'])
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', pol.policyname, pol.tablename);
    RAISE NOTICE 'Dropped anon policy: % on %', pol.policyname, pol.tablename;
  END LOOP;
END
$revoke$;

-- ── 2. Revogar GRANTS de anon em todas as tabelas do schema public ──────

DO $grants$
DECLARE
  tbl RECORD;
BEGIN
  FOR tbl IN
    SELECT tablename FROM pg_tables WHERE schemaname = 'public'
  LOOP
    EXECUTE format('REVOKE ALL ON public.%I FROM anon', tbl.tablename);
  END LOOP;

  -- Views tambem
  FOR tbl IN
    SELECT viewname AS tablename FROM pg_views WHERE schemaname = 'public'
  LOOP
    EXECUTE format('REVOKE ALL ON public.%I FROM anon', tbl.tablename);
  END LOOP;

  -- Functions
  FOR tbl IN
    SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'public'
  LOOP
    BEGIN
      EXECUTE format('REVOKE EXECUTE ON FUNCTION public.%I(%s) FROM anon', tbl.proname, tbl.args);
    EXCEPTION WHEN OTHERS THEN
      NULL; -- ignorar se funcao nao tem grant
    END;
  END LOOP;

  RAISE NOTICE '[OK] Todos os GRANT anon revogados em tabelas, views e functions do schema public.';
END
$grants$;

-- ── 3. Verificacao ──────────────────────────────────────────────────────

DO $verify$
DECLARE
  remaining INT;
BEGIN
  SELECT count(*) INTO remaining
  FROM pg_policies
  WHERE schemaname = 'public'
    AND (roles @> ARRAY['anon'] OR roles = ARRAY['anon']);

  IF remaining > 0 THEN
    RAISE WARNING '[ATENCAO] % policies anon ainda restam. Verifique manualmente.', remaining;
  ELSE
    RAISE NOTICE '[OK] Zero policies anon restantes no schema public.';
  END IF;
END
$verify$;

COMMIT;
