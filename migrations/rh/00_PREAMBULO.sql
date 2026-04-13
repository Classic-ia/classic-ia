-- ════════════════════════════════════════════════════════════════════════════
-- 00_PREAMBULO.sql — Pre-requisitos para rebuild em ambiente limpo
-- DEVE SER O PRIMEIRO SCRIPT EXECUTADO
-- ════════════════════════════════════════════════════════════════════════════
-- Cria stubs para dependencias do Supabase que nao existem em PostgreSQL
-- vanilla. Em producao Supabase estes objetos ja existem nativamente.
--
-- Dependencias cobertas:
--   - auth schema + auth.uid() + auth.jwt()
--   - auth.users (tabela referenciada por FKs)
--   - Role 'authenticated' (usada em GRANT/POLICY)
--   - Extensions: fuzzystrmatch, unaccent
-- ════════════════════════════════════════════════════════════════════════════

-- Extensions obrigatorias
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Extension http (opcional — usada por integ_chamar_edge_function)
-- Nao disponivel em PostgreSQL vanilla; falha silenciosa e aceitavel.
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS http;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Extensao http nao disponivel (esperado fora do Supabase)';
END $$;

-- Schema auth (Supabase Auth)
CREATE SCHEMA IF NOT EXISTS auth;

-- Stub: auth.users (referenciada por FKs em varias tabelas)
CREATE TABLE IF NOT EXISTS auth.users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text,
  created_at timestamptz DEFAULT now()
);

-- Stub: auth.uid() — retorna UUID do usuario autenticado
CREATE OR REPLACE FUNCTION auth.uid()
RETURNS uuid LANGUAGE sql STABLE
AS $$ SELECT COALESCE(current_setting('request.jwt.claim.sub', true)::uuid, '00000000-0000-0000-0000-000000000000'::uuid); $$;

-- Stub: auth.jwt() — retorna claims JWT
CREATE OR REPLACE FUNCTION auth.jwt()
RETURNS jsonb LANGUAGE sql STABLE
AS $$ SELECT COALESCE(current_setting('request.jwt.claims', true)::jsonb, '{"role":"authenticated"}'::jsonb); $$;

-- Role authenticated (usada em GRANT e CREATE POLICY ... TO authenticated)
DO $$ BEGIN
  CREATE ROLE authenticated NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- Role anon (usada em alguns GRANTs)
DO $$ BEGIN
  CREATE ROLE anon NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- Role service_role (usada em SECURITY DEFINER context)
DO $$ BEGIN
  CREATE ROLE service_role NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- Stub: auth.role() — retorna role do usuario autenticado
-- Em Supabase producao retorna 'authenticated', 'anon' ou 'service_role'
CREATE OR REPLACE FUNCTION auth.role()
RETURNS text LANGUAGE sql STABLE
AS $$ SELECT COALESCE(current_setting('request.jwt.claim.role', true), 'authenticated'); $$;

-- Stub: rh_perfil_atual() — sera redefinida por SEGURANCA_SUPABASE.sql
-- Precisa existir antes de FUNDACAO criar policies que a referenciam
CREATE OR REPLACE FUNCTION public.rh_perfil_atual()
RETURNS text LANGUAGE sql STABLE
SET search_path TO 'public'
AS $$ SELECT 'administrador'::text; $$;
