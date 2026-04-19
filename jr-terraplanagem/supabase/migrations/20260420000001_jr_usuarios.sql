-- ============================================================
-- Migration: jr_usuarios + helpers + RLS base (Modulo JR)
-- Fase J1 do plano JR Terraplanagem.
-- Compartilha o mesmo projeto Supabase do Trans Anacleto.
-- Isolamento por prefixo jr_*.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- TABELA: jr_usuarios
-- RBAC do modulo JR Terraplanagem.
-- Perfis:
--   admin       - full CRUD
--   gestor      - SELECT tudo + INSERT/UPDATE operacional
--   operador    - apontamento OS/abastecimento/manutencao no campo
--   financeiro  - pagamentos, despesas, relatorios
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_usuarios (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  auth_user_id   UUID UNIQUE REFERENCES auth.users(id) ON DELETE SET NULL,
  email          TEXT NOT NULL UNIQUE,
  nome           TEXT NOT NULL,
  perfil         TEXT NOT NULL CHECK (perfil IN ('admin', 'gestor', 'operador', 'financeiro')),
  ativo          BOOLEAN NOT NULL DEFAULT TRUE,
  telefone       TEXT,
  observacao     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_usuarios_auth_user_id ON public.jr_usuarios (auth_user_id);
CREATE INDEX IF NOT EXISTS idx_jr_usuarios_email        ON public.jr_usuarios (email);
CREATE INDEX IF NOT EXISTS idx_jr_usuarios_perfil_ativo ON public.jr_usuarios (perfil) WHERE ativo = TRUE;

-- ============================================================
-- TRIGGER genérico de updated_at / updated_by
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_set_updated_at()
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

DROP TRIGGER IF EXISTS tg_jr_usuarios_set_updated_at ON public.jr_usuarios;
CREATE TRIGGER tg_jr_usuarios_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_usuarios
FOR EACH ROW
EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- HELPER: jr_current_perfil()
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_current_perfil()
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT perfil
    FROM public.jr_usuarios
   WHERE auth_user_id = auth.uid()
     AND ativo = TRUE
   LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.jr_current_perfil() TO anon, authenticated;

-- ============================================================
-- HELPER: jr_is_admin()
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_is_admin()
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.jr_usuarios
     WHERE auth_user_id = auth.uid()
       AND perfil = 'admin'
       AND ativo = TRUE
  );
$$;

GRANT EXECUTE ON FUNCTION public.jr_is_admin() TO anon, authenticated;

-- ============================================================
-- RPC: jr_meu_perfil()
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_meu_perfil()
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
    FROM public.jr_usuarios
   WHERE auth_user_id = auth.uid()
     AND ativo = TRUE
   LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.jr_meu_perfil() TO authenticated;

-- ============================================================
-- RLS em jr_usuarios
-- ============================================================
ALTER TABLE public.jr_usuarios ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS jr_usuarios_self_select ON public.jr_usuarios;
CREATE POLICY jr_usuarios_self_select
  ON public.jr_usuarios
  FOR SELECT
  TO authenticated
  USING (auth_user_id = auth.uid() OR public.jr_is_admin());

DROP POLICY IF EXISTS jr_usuarios_admin_insert ON public.jr_usuarios;
CREATE POLICY jr_usuarios_admin_insert
  ON public.jr_usuarios
  FOR INSERT
  TO authenticated
  WITH CHECK (public.jr_is_admin());

DROP POLICY IF EXISTS jr_usuarios_admin_update ON public.jr_usuarios;
CREATE POLICY jr_usuarios_admin_update
  ON public.jr_usuarios
  FOR UPDATE
  TO authenticated
  USING (public.jr_is_admin())
  WITH CHECK (public.jr_is_admin());

DROP POLICY IF EXISTS jr_usuarios_admin_delete ON public.jr_usuarios;
CREATE POLICY jr_usuarios_admin_delete
  ON public.jr_usuarios
  FOR DELETE
  TO authenticated
  USING (public.jr_is_admin());

COMMENT ON TABLE  public.jr_usuarios           IS 'Usuarios do modulo JR Terraplanagem. Isolado de trans_usuarios.';
COMMENT ON FUNCTION public.jr_current_perfil() IS 'Retorna perfil JR do auth.uid() atual.';
COMMENT ON FUNCTION public.jr_is_admin()       IS 'Atalho: usuario atual e admin JR?';
COMMENT ON FUNCTION public.jr_meu_perfil()     IS 'Front-end chama apos login para hidratar sessao JR.';
