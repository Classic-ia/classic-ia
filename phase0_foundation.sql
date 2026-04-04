-- ═══════════════════════════════════════════════════════════════
-- FASE 0 — FUNDAÇÃO: Tabelas Faltantes, RPCs, RLS Real
-- Classic RH & SST · Supabase (PostgreSQL)
-- Executar no SQL Editor do Supabase (projeto muiqmtnfvyffborgiwdw)
--
-- Padrão espelhado do CQ: fase1_hardening_rls.sql + fase2_governanca.sql
-- ═══════════════════════════════════════════════════════════════

-- ══════════════════════════════════════════════════════════
-- 1. TABELA rh_usuarios (espelho de cq_usuarios)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_usuarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  auth_user_id UUID UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  email TEXT NOT NULL,
  perfil TEXT NOT NULL CHECK (perfil IN (
    'administrador','rh','sst','lideranca','financeiro','operacional','visualizador'
  )),
  empresa_id UUID REFERENCES rh_empresas(id),
  filial_id UUID REFERENCES rh_filiais(id),
  ativo BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rh_usuarios_auth ON rh_usuarios(auth_user_id);
CREATE INDEX IF NOT EXISTS idx_rh_usuarios_email ON rh_usuarios(email);
CREATE INDEX IF NOT EXISTS idx_rh_usuarios_empresa ON rh_usuarios(empresa_id);

-- ══════════════════════════════════════════════════════════
-- 2. TABELA rh_sessoes (referenciada em rh_auth.js)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_sessoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID REFERENCES rh_usuarios(id),
  usuario_email TEXT,
  session_token TEXT NOT NULL,
  dispositivo TEXT,
  ativa BOOLEAN DEFAULT TRUE,
  expira_em TIMESTAMPTZ,
  encerrada_em TIMESTAMPTZ,
  ultimo_acesso TIMESTAMPTZ DEFAULT NOW(),
  criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rh_sessoes_token ON rh_sessoes(session_token);
CREATE INDEX IF NOT EXISTS idx_rh_sessoes_usuario ON rh_sessoes(usuario_id);
CREATE INDEX IF NOT EXISTS idx_rh_sessoes_ativa ON rh_sessoes(ativa) WHERE ativa = TRUE;

-- ══════════════════════════════════════════════════════════
-- 3. TABELA rh_apuracao_mensal
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_apuracao_mensal (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  competencia VARCHAR(7) NOT NULL,  -- YYYY-MM
  empresa_id UUID REFERENCES rh_empresas(id),
  total_funcionarios INTEGER DEFAULT 0,
  admissoes INTEGER DEFAULT 0,
  desligamentos INTEGER DEFAULT 0,
  faltas INTEGER DEFAULT 0,
  dias_uteis INTEGER DEFAULT 22,
  taxa_absenteismo NUMERIC(5,2) DEFAULT 0,
  taxa_turnover NUMERIC(5,2) DEFAULT 0,
  status TEXT DEFAULT 'aberto' CHECK (status IN ('aberto','fechado','revisao')),
  fechado_por UUID REFERENCES rh_usuarios(id),
  fechado_em TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(competencia, empresa_id)
);

CREATE INDEX IF NOT EXISTS idx_apuracao_competencia ON rh_apuracao_mensal(competencia);
CREATE INDEX IF NOT EXISTS idx_apuracao_empresa ON rh_apuracao_mensal(empresa_id);

-- ══════════════════════════════════════════════════════════
-- 4. TRIGGERS updated_at para novas tabelas
-- ══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['rh_usuarios','rh_apuracao_mensal'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated ON %I', t, t);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_updated BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at()',
      t, t
    );
  END LOOP;
END;
$$;

-- ══════════════════════════════════════════════════════════
-- 5. RPCs SECURITY DEFINER
--    Espelho exato de: cq_meu_perfil, cq_get_perfil,
--    cq_user_id, cq_empresa_do_usuario
--    (fonte: fase1_hardening_rls.sql)
-- ══════════════════════════════════════════════════════════

-- Retorna perfil completo do usuário autenticado
CREATE OR REPLACE FUNCTION public.rh_meu_perfil()
RETURNS TABLE(id UUID, email TEXT, nome TEXT, perfil TEXT, ativo BOOLEAN)
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT u.id, u.email, u.nome, u.perfil, u.ativo
  FROM public.rh_usuarios u
  WHERE u.auth_user_id = auth.uid() AND u.ativo = TRUE
  LIMIT 1;
$$;

-- Retorna empresa_id do usuário atual (usado no RLS)
CREATE OR REPLACE FUNCTION public.rh_empresa_do_usuario()
RETURNS UUID
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT empresa_id
  FROM public.rh_usuarios
  WHERE auth_user_id = auth.uid() AND ativo = TRUE
  LIMIT 1;
$$;

-- Retorna id interno do rh_usuarios para o auth.uid() atual
CREATE OR REPLACE FUNCTION public.rh_user_id()
RETURNS UUID
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT id
  FROM public.rh_usuarios
  WHERE auth_user_id = auth.uid() AND ativo = TRUE
  LIMIT 1;
$$;

-- Retorna string do perfil (usado nas policies RLS)
CREATE OR REPLACE FUNCTION public.rh_get_perfil()
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT perfil
  FROM public.rh_usuarios
  WHERE auth_user_id = auth.uid() AND ativo = TRUE
  LIMIT 1;
$$;

-- Helper: verifica se é admin ou RH
CREATE OR REPLACE FUNCTION public.rh_is_admin_or_rh()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.rh_usuarios
    WHERE auth_user_id = auth.uid()
      AND ativo = TRUE
      AND perfil IN ('administrador','rh')
  );
$$;

-- Helper: verifica se é gestor (admin, rh, sst ou liderança)
CREATE OR REPLACE FUNCTION public.rh_is_gestor()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.rh_usuarios
    WHERE auth_user_id = auth.uid()
      AND ativo = TRUE
      AND perfil IN ('administrador','rh','sst','lideranca')
  );
$$;

-- ══════════════════════════════════════════════════════════
-- 6. RLS REAL — Remover USING(true) e aplicar políticas
--    baseadas em perfil (padrão CQ)
-- ══════════════════════════════════════════════════════════

-- 6a. Habilitar RLS nas novas tabelas
ALTER TABLE rh_usuarios ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_sessoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_apuracao_mensal ENABLE ROW LEVEL SECURITY;

-- ═══════════════════════════════════════════════════════════
-- 6b. REMOVER todas as policies permissivas existentes
--     (as que usam USING(true) criadas no schema original)
-- ═══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos','rh_saude_mental',
    'rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao','rh_audit_log'
  ] LOOP
    -- Remover policy _all (authenticated USING true)
    EXECUTE format('DROP POLICY IF EXISTS %s_all ON %I', t, t);
    -- Remover policy _anon (anon USING true)
    EXECUTE format('DROP POLICY IF EXISTS %s_anon ON %I', t, t);
  END LOOP;
END;
$$;

-- ═══════════════════════════════════════════════════════════
-- 6c. POLICIES PARA TABELAS ORGANIZACIONAIS
--     SELECT: qualquer authenticated cadastrado
--     INSERT/UPDATE: admin, rh, sst
--     DELETE: admin only
-- ═══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos',
    'rh_lotes_pagamento','rh_log_exportacao','rh_apuracao_mensal'
  ] LOOP
    -- SELECT: qualquer usuário cadastrado e ativo
    EXECUTE format(
      'CREATE POLICY %s_select ON %I FOR SELECT TO authenticated USING (rh_get_perfil() IS NOT NULL)',
      t, t
    );
    -- INSERT: admin, rh ou sst
    EXECUTE format(
      'CREATE POLICY %s_insert ON %I FOR INSERT TO authenticated WITH CHECK (rh_get_perfil() IN (''administrador'',''rh'',''sst''))',
      t, t
    );
    -- UPDATE: admin, rh ou sst
    EXECUTE format(
      'CREATE POLICY %s_update ON %I FOR UPDATE TO authenticated USING (rh_get_perfil() IN (''administrador'',''rh'',''sst'')) WITH CHECK (rh_get_perfil() IN (''administrador'',''rh'',''sst''))',
      t, t
    );
    -- DELETE: somente admin
    EXECUTE format(
      'CREATE POLICY %s_delete ON %I FOR DELETE TO authenticated USING (rh_get_perfil() = ''administrador'')',
      t, t
    );
  END LOOP;
END;
$$;

-- ═══════════════════════════════════════════════════════════
-- 6d. POLICIES PARA TABELAS SENSÍVEIS
--     rh_saude_mental, rh_dados_bancarios
--     Apenas admin e RH podem ver/modificar
--     DELETE bloqueado (sem policy)
-- ═══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['rh_saude_mental','rh_dados_bancarios'] LOOP
    -- SELECT: somente admin e RH
    EXECUTE format(
      'CREATE POLICY %s_select ON %I FOR SELECT TO authenticated USING (rh_get_perfil() IN (''administrador'',''rh''))',
      t, t
    );
    -- INSERT: somente admin e RH
    EXECUTE format(
      'CREATE POLICY %s_insert ON %I FOR INSERT TO authenticated WITH CHECK (rh_get_perfil() IN (''administrador'',''rh''))',
      t, t
    );
    -- UPDATE: somente admin e RH
    EXECUTE format(
      'CREATE POLICY %s_update ON %I FOR UPDATE TO authenticated USING (rh_get_perfil() IN (''administrador'',''rh'')) WITH CHECK (rh_get_perfil() IN (''administrador'',''rh''))',
      t, t
    );
    -- DELETE: bloqueado (nenhuma policy = denied by default)
  END LOOP;
END;
$$;

-- ═══════════════════════════════════════════════════════════
-- 6e. POLICIES PARA rh_audit_log
--     INSERT: qualquer authenticated cadastrado
--     SELECT: somente admin
--     UPDATE/DELETE: bloqueado (sem policy)
-- ═══════════════════════════════════════════════════════════

CREATE POLICY rh_audit_log_insert ON rh_audit_log
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IS NOT NULL);

CREATE POLICY rh_audit_log_select ON rh_audit_log
  FOR SELECT TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- UPDATE e DELETE: sem policy = bloqueado por RLS

-- ═══════════════════════════════════════════════════════════
-- 6f. POLICIES PARA rh_usuarios
--     SELECT: próprio registro ou admin
--     INSERT: somente admin
--     UPDATE: somente admin
--     DELETE: somente admin
-- ═══════════════════════════════════════════════════════════

CREATE POLICY rh_usuarios_select ON rh_usuarios
  FOR SELECT TO authenticated
  USING (
    auth_user_id = auth.uid()
    OR rh_get_perfil() = 'administrador'
  );

CREATE POLICY rh_usuarios_insert ON rh_usuarios
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() = 'administrador');

CREATE POLICY rh_usuarios_update ON rh_usuarios
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() = 'administrador')
  WITH CHECK (rh_get_perfil() = 'administrador');

CREATE POLICY rh_usuarios_delete ON rh_usuarios
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- ═══════════════════════════════════════════════════════════
-- 6g. POLICIES PARA rh_sessoes
--     SELECT: próprias sessões ou admin
--     INSERT: próprias sessões (via usuario_id = rh_user_id())
--     UPDATE: próprias sessões ou admin
--     DELETE: somente admin
-- ═══════════════════════════════════════════════════════════

CREATE POLICY rh_sessoes_select ON rh_sessoes
  FOR SELECT TO authenticated
  USING (
    usuario_id = rh_user_id()
    OR rh_get_perfil() = 'administrador'
  );

CREATE POLICY rh_sessoes_insert ON rh_sessoes
  FOR INSERT TO authenticated
  WITH CHECK (
    usuario_id = rh_user_id()
    OR rh_get_perfil() = 'administrador'
  );

CREATE POLICY rh_sessoes_update ON rh_sessoes
  FOR UPDATE TO authenticated
  USING (
    usuario_id = rh_user_id()
    OR rh_get_perfil() = 'administrador'
  );

CREATE POLICY rh_sessoes_delete ON rh_sessoes
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- ═══════════════════════════════════════════════════════════
-- 7. MELHORAR rh_audit_log — Campos JSONB + email
-- ═══════════════════════════════════════════════════════════

-- Converter dados_antes/dados_depois de TEXT para JSONB
DO $$
BEGIN
  -- Só altera se a coluna existir e for TEXT
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'rh_audit_log'
      AND column_name = 'dados_antes'
      AND data_type = 'text'
  ) THEN
    ALTER TABLE rh_audit_log
      ALTER COLUMN dados_antes TYPE JSONB USING CASE
        WHEN dados_antes IS NULL THEN NULL
        WHEN dados_antes = '' THEN NULL
        ELSE dados_antes::JSONB
      END;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'rh_audit_log'
      AND column_name = 'dados_depois'
      AND data_type = 'text'
  ) THEN
    ALTER TABLE rh_audit_log
      ALTER COLUMN dados_depois TYPE JSONB USING CASE
        WHEN dados_depois IS NULL THEN NULL
        WHEN dados_depois = '' THEN NULL
        ELSE dados_depois::JSONB
      END;
  END IF;
END;
$$;

ALTER TABLE rh_audit_log ADD COLUMN IF NOT EXISTS usuario_email TEXT;

-- ═══════════════════════════════════════════════════════════
-- 8. GRANT para service_role (bypass RLS — usado por RPCs)
-- ═══════════════════════════════════════════════════════════

GRANT ALL ON rh_usuarios TO service_role;
GRANT ALL ON rh_sessoes TO service_role;
GRANT ALL ON rh_apuracao_mensal TO service_role;
GRANT EXECUTE ON FUNCTION rh_meu_perfil() TO authenticated;
GRANT EXECUTE ON FUNCTION rh_empresa_do_usuario() TO authenticated;
GRANT EXECUTE ON FUNCTION rh_user_id() TO authenticated;
GRANT EXECUTE ON FUNCTION rh_get_perfil() TO authenticated;
GRANT EXECUTE ON FUNCTION rh_is_admin_or_rh() TO authenticated;
GRANT EXECUTE ON FUNCTION rh_is_gestor() TO authenticated;

-- ═══════════════════════════════════════════════════════════
-- FASE 0 COMPLETA — Executar no Supabase SQL Editor
-- Próximo: rh_auth.js (remover demo bypass)
-- ═══════════════════════════════════════════════════════════
