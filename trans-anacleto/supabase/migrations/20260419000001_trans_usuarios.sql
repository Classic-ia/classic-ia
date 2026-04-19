-- ============================================================
-- Migration: trans_usuarios + função trans_meu_perfil + RLS base
-- Fase 1 do plano Trans Anacleto.
-- ============================================================

-- Extensões base
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- TABELA: trans_usuarios
-- RBAC do módulo Trans Anacleto.
-- auth_user_id referencia auth.users (Supabase Auth).
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_usuarios (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  auth_user_id   UUID UNIQUE REFERENCES auth.users(id) ON DELETE SET NULL,
  email          TEXT NOT NULL UNIQUE,
  nome           TEXT NOT NULL,
  perfil         TEXT NOT NULL CHECK (perfil IN ('admin', 'gestor', 'almoxarife', 'motorista')),
  ativo          BOOLEAN NOT NULL DEFAULT TRUE,
  telefone       TEXT,
  observacao     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_usuarios_auth_user_id ON public.trans_usuarios (auth_user_id);
CREATE INDEX IF NOT EXISTS idx_trans_usuarios_email        ON public.trans_usuarios (email);
CREATE INDEX IF NOT EXISTS idx_trans_usuarios_perfil_ativo ON public.trans_usuarios (perfil) WHERE ativo = TRUE;

-- ============================================================
-- TRIGGER genérico de updated_at / updated_by
-- Reutilizável em qualquer tabela do módulo.
-- ============================================================
CREATE OR REPLACE FUNCTION public.trans_tg_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := NOW();
  IF TG_OP = 'INSERT' THEN
    NEW.created_at := COALESCE(NEW.created_at, NOW());
    NEW.created_by := COALESCE(NEW.created_by, auth.uid());
  END IF;
  NEW.updated_by := auth.uid();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_trans_usuarios_set_updated_at ON public.trans_usuarios;
CREATE TRIGGER tg_trans_usuarios_set_updated_at
BEFORE INSERT OR UPDATE ON public.trans_usuarios
FOR EACH ROW
EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- HELPER: trans_current_perfil()
-- Retorna o perfil do usuário autenticado no módulo Trans
-- ou NULL se não estiver cadastrado/ativo.
-- STABLE + SECURITY DEFINER para ser usada dentro de policies.
-- ============================================================
CREATE OR REPLACE FUNCTION public.trans_current_perfil()
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT perfil
    FROM public.trans_usuarios
   WHERE auth_user_id = auth.uid()
     AND ativo = TRUE
   LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.trans_current_perfil() TO anon, authenticated;

-- ============================================================
-- HELPER: trans_is_admin()
-- Atalho booleano.
-- ============================================================
CREATE OR REPLACE FUNCTION public.trans_is_admin()
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.trans_usuarios
     WHERE auth_user_id = auth.uid()
       AND perfil = 'admin'
       AND ativo = TRUE
  );
$$;

GRANT EXECUTE ON FUNCTION public.trans_is_admin() TO anon, authenticated;

-- ============================================================
-- RPC: trans_meu_perfil()
-- Chamado pelo front-end logo após o login para obter dados
-- do usuário cadastrado na tabela trans_usuarios.
-- ============================================================
CREATE OR REPLACE FUNCTION public.trans_meu_perfil()
RETURNS TABLE (
  id       UUID,
  email    TEXT,
  nome     TEXT,
  perfil   TEXT,
  ativo    BOOLEAN
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT id, email, nome, perfil, ativo
    FROM public.trans_usuarios
   WHERE auth_user_id = auth.uid()
     AND ativo = TRUE
   LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.trans_meu_perfil() TO authenticated;

-- ============================================================
-- RLS em trans_usuarios
-- - Usuário autenticado lê o próprio cadastro.
-- - admin gerencia todos os usuários.
-- ============================================================
ALTER TABLE public.trans_usuarios ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_usuarios_self_select ON public.trans_usuarios;
CREATE POLICY trans_usuarios_self_select
  ON public.trans_usuarios
  FOR SELECT
  TO authenticated
  USING (auth_user_id = auth.uid() OR public.trans_is_admin());

DROP POLICY IF EXISTS trans_usuarios_admin_insert ON public.trans_usuarios;
CREATE POLICY trans_usuarios_admin_insert
  ON public.trans_usuarios
  FOR INSERT
  TO authenticated
  WITH CHECK (public.trans_is_admin());

DROP POLICY IF EXISTS trans_usuarios_admin_update ON public.trans_usuarios;
CREATE POLICY trans_usuarios_admin_update
  ON public.trans_usuarios
  FOR UPDATE
  TO authenticated
  USING (public.trans_is_admin())
  WITH CHECK (public.trans_is_admin());

DROP POLICY IF EXISTS trans_usuarios_admin_delete ON public.trans_usuarios;
CREATE POLICY trans_usuarios_admin_delete
  ON public.trans_usuarios
  FOR DELETE
  TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- Comentários
-- ============================================================
COMMENT ON TABLE  public.trans_usuarios           IS 'Usuários do módulo Trans Anacleto. Mapeia auth.users → perfil (admin/gestor/almoxarife/motorista).';
COMMENT ON FUNCTION public.trans_current_perfil() IS 'Retorna perfil do auth.uid() atual no módulo Trans. NULL se não cadastrado ou inativo.';
COMMENT ON FUNCTION public.trans_is_admin()       IS 'Atalho: usuário atual é admin no módulo Trans?';
COMMENT ON FUNCTION public.trans_meu_perfil()     IS 'Front-end chama após login para hidratar sessão com dados de trans_usuarios.';
