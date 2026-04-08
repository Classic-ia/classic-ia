-- ============================================================================
-- migration_rls_rh_sst.sql
-- RLS real por perfil para todas as tabelas RH, SST, R&S e Liderança
--
-- PRE-REQUISITOS:
--   - schema_rh_sst_completo.sql já executado
--   - schema_recrutamento_selecao.sql já executado
--   - schema_lideranca.sql já executado
--   - Supabase auth.users populado
--
-- PERFIS SUPORTADOS:
--   administrador  → CRUD total
--   diretoria      → SELECT total, INSERT/UPDATE em audit
--   rh             → CRUD em RH + SST + R&S
--   sst            → CRUD em SST, SELECT em funcionários
--   financeiro     → CRUD em financeiro, SELECT em funcionários
--   gestor         → SELECT filtrado por setor
--   qualidade      → ZERO acesso (RH/SST bloqueado)
--
-- EXECUTAR EM ORDEM: Seção 1 → 2 → 3 → 4
-- ============================================================================

BEGIN;

-- ============================================================================
-- SEÇÃO 1: INFRAESTRUTURA DE AUTH (tabelas + funções)
-- ============================================================================

-- 1.1 Tabela de usuários RH (link auth.uid() → perfil + setor)
CREATE TABLE IF NOT EXISTS rh_usuarios (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  auth_id       UUID UNIQUE REFERENCES auth.users(id),
  email         TEXT UNIQUE NOT NULL,
  nome          TEXT NOT NULL,
  perfil        TEXT NOT NULL DEFAULT 'gestor'
    CHECK (perfil IN ('administrador','diretoria','rh','sst','financeiro','gestor','qualidade')),
  setor         TEXT,            -- para filtro do perfil gestor
  ativo         BOOLEAN DEFAULT TRUE,
  criado_em     TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rh_usuarios_auth_id ON rh_usuarios(auth_id);
CREATE INDEX IF NOT EXISTS idx_rh_usuarios_perfil ON rh_usuarios(perfil);

ALTER TABLE rh_usuarios ENABLE ROW LEVEL SECURITY;

-- Usuários só veem seu próprio registro (exceto admin que vê todos)
CREATE POLICY rh_usuarios_self ON rh_usuarios
  FOR SELECT TO authenticated
  USING (
    auth_id = auth.uid()
    OR (SELECT perfil FROM rh_usuarios WHERE auth_id = auth.uid() AND ativo = TRUE LIMIT 1) = 'administrador'
  );

CREATE POLICY rh_usuarios_admin_write ON rh_usuarios
  FOR ALL TO authenticated
  USING (
    (SELECT perfil FROM rh_usuarios WHERE auth_id = auth.uid() AND ativo = TRUE LIMIT 1) = 'administrador'
  )
  WITH CHECK (
    (SELECT perfil FROM rh_usuarios WHERE auth_id = auth.uid() AND ativo = TRUE LIMIT 1) = 'administrador'
  );

-- 1.2 Tabela de sessões RH
CREATE TABLE IF NOT EXISTS rh_sessoes (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  usuario_id    UUID NOT NULL,
  usuario_email TEXT NOT NULL,
  session_token TEXT NOT NULL UNIQUE,
  dispositivo   TEXT,
  criado_em     TIMESTAMPTZ DEFAULT NOW(),
  ultimo_acesso TIMESTAMPTZ DEFAULT NOW(),
  expira_em     TIMESTAMPTZ DEFAULT NOW() + INTERVAL '1 hour',
  encerrada_em  TIMESTAMPTZ,
  ativa         BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_rh_sessoes_token ON rh_sessoes(session_token) WHERE ativa = TRUE;
CREATE INDEX IF NOT EXISTS idx_rh_sessoes_usuario ON rh_sessoes(usuario_id) WHERE ativa = TRUE;

ALTER TABLE rh_sessoes ENABLE ROW LEVEL SECURITY;

-- Sessões: cada usuário manipula apenas as suas
CREATE POLICY rh_sessoes_own ON rh_sessoes
  FOR ALL TO authenticated
  USING (
    usuario_id IN (SELECT id FROM rh_usuarios WHERE auth_id = auth.uid())
  )
  WITH CHECK (
    usuario_id IN (SELECT id FROM rh_usuarios WHERE auth_id = auth.uid())
  );

-- Anon precisa poder inserir sessão durante login
CREATE POLICY rh_sessoes_anon_insert ON rh_sessoes
  FOR INSERT TO anon
  WITH CHECK (true);

CREATE POLICY rh_sessoes_anon_select ON rh_sessoes
  FOR SELECT TO anon
  USING (true);

-- 1.3 Funções helper (SECURITY DEFINER — executam com privilégio do owner)

-- Retorna o perfil do usuário autenticado
CREATE OR REPLACE FUNCTION public.rh_get_perfil()
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT perfil FROM rh_usuarios
  WHERE auth_id = auth.uid() AND ativo = TRUE
  LIMIT 1;
$$;

-- Retorna o setor do usuário (para perfil gestor)
CREATE OR REPLACE FUNCTION public.rh_get_setor()
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT setor FROM rh_usuarios
  WHERE auth_id = auth.uid() AND ativo = TRUE
  LIMIT 1;
$$;

-- Verifica se é admin
CREATE OR REPLACE FUNCTION public.rh_is_admin()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM rh_usuarios
    WHERE auth_id = auth.uid() AND perfil = 'administrador' AND ativo = TRUE
  );
$$;

-- RPC que o frontend (rh_auth.js) chama para revalidar perfil
CREATE OR REPLACE FUNCTION public.rh_meu_perfil()
RETURNS TABLE(id UUID, nome TEXT, perfil TEXT, email TEXT)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT u.id, u.nome, u.perfil, u.email
  FROM rh_usuarios u
  WHERE u.auth_id = auth.uid() AND u.ativo = TRUE
  LIMIT 1;
$$;


-- ============================================================================
-- SEÇÃO 2: REMOÇÃO DAS POLICIES PERMISSIVAS
-- ============================================================================

-- RH SST (16 tabelas × 2 policies = 32 drops)
DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos','rh_saude_mental',
    'rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao','rh_audit_log'
  ]) LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I', t || '_all', t);
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I', t || '_anon', t);
  END LOOP;
END $$;

-- R&S (6 tabelas × 2 policies)
DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'rs_vagas','rs_candidatos','rs_pipeline',
    'rs_entrevistas','rs_avaliacoes','rs_banco_talentos'
  ]) LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I', t || '_all', t);
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I', t || '_anon', t);
  END LOOP;
END $$;

-- Liderança (4 tabelas × 1 policy)
DROP POLICY IF EXISTS rh_avaliacoes_all ON rh_avaliacoes;
DROP POLICY IF EXISTS rh_metas_all ON rh_metas;
DROP POLICY IF EXISTS rh_pulsos_all ON rh_pulsos;
DROP POLICY IF EXISTS rh_rondas_all ON rh_rondas;


-- ============================================================================
-- SEÇÃO 3: POLICIES NOVAS — POR GRUPO DE TABELAS
-- ============================================================================

-- ─────────────────────────────────────────────
-- 3.1 DADOS MASTER (rh_empresas, rh_filiais, rh_setores)
-- Regra: leitura para todos autenticados, escrita para admin/rh
-- ─────────────────────────────────────────────

-- rh_empresas
CREATE POLICY rh_empresas_select ON rh_empresas
  FOR SELECT TO authenticated
  USING (rh_get_perfil() IN ('administrador','diretoria','rh','sst','financeiro','gestor'));

CREATE POLICY rh_empresas_write ON rh_empresas
  FOR ALL TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

-- rh_filiais
CREATE POLICY rh_filiais_select ON rh_filiais
  FOR SELECT TO authenticated
  USING (rh_get_perfil() IN ('administrador','diretoria','rh','sst','financeiro','gestor'));

CREATE POLICY rh_filiais_write ON rh_filiais
  FOR ALL TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

-- rh_setores
CREATE POLICY rh_setores_select ON rh_setores
  FOR SELECT TO authenticated
  USING (rh_get_perfil() IN ('administrador','diretoria','rh','sst','financeiro','gestor'));

CREATE POLICY rh_setores_write ON rh_setores
  FOR ALL TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));


-- ─────────────────────────────────────────────
-- 3.2 FUNCIONÁRIOS
-- Regra: leitura ampla (exceto qualidade), escrita admin/rh
--        gestor só vê seu setor
-- ─────────────────────────────────────────────

CREATE POLICY rh_funcionarios_select ON rh_funcionarios
  FOR SELECT TO authenticated
  USING (
    CASE rh_get_perfil()
      WHEN 'administrador' THEN true
      WHEN 'diretoria'     THEN true
      WHEN 'rh'            THEN true
      WHEN 'sst'           THEN true
      WHEN 'financeiro'    THEN true
      WHEN 'gestor'        THEN setor = rh_get_setor()
      ELSE false  -- qualidade e outros
    END
  );

CREATE POLICY rh_funcionarios_insert ON rh_funcionarios
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_funcionarios_update ON rh_funcionarios
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_funcionarios_delete ON rh_funcionarios
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');


-- ─────────────────────────────────────────────
-- 3.3 SST (aso, treinamentos, epi, acidentes, restricoes, saude_mental)
-- Regra: CRUD para admin/rh/sst
--        SELECT para diretoria
--        gestor vê apenas seu setor (via funcionario_id → setor)
--        financeiro/qualidade: sem acesso
-- ─────────────────────────────────────────────

-- Macro: tabelas SST que têm funcionario_id
DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'rh_aso','rh_treinamentos','rh_epi_entregas',
    'rh_acidentes','rh_restricoes','rh_saude_mental'
  ]) LOOP

    -- SELECT: admin, diretoria, rh, sst veem tudo; gestor vê seu setor
    EXECUTE format(
      'CREATE POLICY %I ON %I FOR SELECT TO authenticated USING (
        CASE rh_get_perfil()
          WHEN ''administrador'' THEN true
          WHEN ''diretoria''     THEN true
          WHEN ''rh''            THEN true
          WHEN ''sst''           THEN true
          WHEN ''gestor''        THEN funcionario_id IN (
            SELECT id FROM rh_funcionarios WHERE setor = rh_get_setor()
          )
          ELSE false
        END
      )', t || '_sel', t);

    -- INSERT: admin, rh, sst
    EXECUTE format(
      'CREATE POLICY %I ON %I FOR INSERT TO authenticated WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'',''sst'')
      )', t || '_ins', t);

    -- UPDATE: admin, rh, sst
    EXECUTE format(
      'CREATE POLICY %I ON %I FOR UPDATE TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''rh'',''sst'')
      ) WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'',''sst'')
      )', t || '_upd', t);

    -- DELETE: apenas admin
    EXECUTE format(
      'CREATE POLICY %I ON %I FOR DELETE TO authenticated USING (
        rh_get_perfil() = ''administrador''
      )', t || '_del', t);

  END LOOP;
END $$;


-- ─────────────────────────────────────────────
-- 3.4 RH OPERACIONAL (ausencias, desligamentos)
-- Regra: CRUD para admin/rh
--        SELECT para diretoria/sst
--        gestor vê seu setor
-- ─────────────────────────────────────────────

DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY['rh_ausencias','rh_desligamentos']) LOOP

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR SELECT TO authenticated USING (
        CASE rh_get_perfil()
          WHEN ''administrador'' THEN true
          WHEN ''diretoria''     THEN true
          WHEN ''rh''            THEN true
          WHEN ''sst''           THEN true
          WHEN ''gestor''        THEN funcionario_id IN (
            SELECT id FROM rh_funcionarios WHERE setor = rh_get_setor()
          )
          ELSE false
        END
      )', t || '_sel', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR INSERT TO authenticated WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'')
      )', t || '_ins', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR UPDATE TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''rh'')
      ) WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'')
      )', t || '_upd', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR DELETE TO authenticated USING (
        rh_get_perfil() = ''administrador''
      )', t || '_del', t);

  END LOOP;
END $$;


-- ─────────────────────────────────────────────
-- 3.5 FINANCEIRO (dados_bancarios, lotes_pagamento, log_exportacao)
-- Regra: CRUD para admin/rh/financeiro
--        SELECT para diretoria
--        gestor/sst/qualidade: sem acesso
-- ─────────────────────────────────────────────

DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY['rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao']) LOOP

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR SELECT TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''diretoria'',''rh'',''financeiro'')
      )', t || '_sel', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR INSERT TO authenticated WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'',''financeiro'')
      )', t || '_ins', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR UPDATE TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''rh'',''financeiro'')
      ) WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'',''financeiro'')
      )', t || '_upd', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR DELETE TO authenticated USING (
        rh_get_perfil() = ''administrador''
      )', t || '_del', t);

  END LOOP;
END $$;


-- ─────────────────────────────────────────────
-- 3.6 AUDITORIA (rh_audit_log)
-- Regra: INSERT para todos autenticados (registro de ações)
--        SELECT apenas admin/rh/diretoria
--        UPDATE/DELETE: ninguém (imutável)
-- ─────────────────────────────────────────────

CREATE POLICY rh_audit_log_insert ON rh_audit_log
  FOR INSERT TO authenticated
  WITH CHECK (true);  -- qualquer autenticado pode gerar log

CREATE POLICY rh_audit_log_select ON rh_audit_log
  FOR SELECT TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh','diretoria'));

-- Sem UPDATE/DELETE — audit log é imutável


-- ─────────────────────────────────────────────
-- 3.7 RECRUTAMENTO & SELEÇÃO (rs_*)
-- Regra: CRUD para admin/rh
--        SELECT para diretoria
--        gestor/sst/financeiro/qualidade: sem acesso
-- ─────────────────────────────────────────────

DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'rs_vagas','rs_candidatos','rs_pipeline',
    'rs_entrevistas','rs_avaliacoes','rs_banco_talentos'
  ]) LOOP

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR SELECT TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''diretoria'',''rh'')
      )', t || '_sel', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR INSERT TO authenticated WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'')
      )', t || '_ins', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR UPDATE TO authenticated USING (
        rh_get_perfil() IN (''administrador'',''rh'')
      ) WITH CHECK (
        rh_get_perfil() IN (''administrador'',''rh'')
      )', t || '_upd', t);

    EXECUTE format(
      'CREATE POLICY %I ON %I FOR DELETE TO authenticated USING (
        rh_get_perfil() = ''administrador''
      )', t || '_del', t);

  END LOOP;
END $$;


-- ─────────────────────────────────────────────
-- 3.8 LIDERANÇA (rh_avaliacoes, rh_metas, rh_pulsos, rh_rondas)
-- Regra: CRUD para admin/rh
--        SELECT para diretoria/gestor
--        gestor vê apenas dados do seu setor
-- ─────────────────────────────────────────────

-- rh_avaliacoes (tem funcionario_id)
CREATE POLICY rh_avaliacoes_sel ON rh_avaliacoes
  FOR SELECT TO authenticated
  USING (
    CASE rh_get_perfil()
      WHEN 'administrador' THEN true
      WHEN 'diretoria'     THEN true
      WHEN 'rh'            THEN true
      WHEN 'gestor'        THEN funcionario_id IN (
        SELECT id FROM rh_funcionarios WHERE setor = rh_get_setor()
      )
      ELSE false
    END
  );

CREATE POLICY rh_avaliacoes_ins ON rh_avaliacoes
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IN ('administrador','rh','gestor'));

CREATE POLICY rh_avaliacoes_upd ON rh_avaliacoes
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_avaliacoes_del ON rh_avaliacoes
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- rh_metas (tem setor)
CREATE POLICY rh_metas_sel ON rh_metas
  FOR SELECT TO authenticated
  USING (
    CASE rh_get_perfil()
      WHEN 'administrador' THEN true
      WHEN 'diretoria'     THEN true
      WHEN 'rh'            THEN true
      WHEN 'gestor'        THEN setor = rh_get_setor()
      ELSE false
    END
  );

CREATE POLICY rh_metas_ins ON rh_metas
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IN ('administrador','rh','gestor'));

CREATE POLICY rh_metas_upd ON rh_metas
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_metas_del ON rh_metas
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- rh_pulsos (tem setor)
CREATE POLICY rh_pulsos_sel ON rh_pulsos
  FOR SELECT TO authenticated
  USING (
    CASE rh_get_perfil()
      WHEN 'administrador' THEN true
      WHEN 'diretoria'     THEN true
      WHEN 'rh'            THEN true
      WHEN 'gestor'        THEN setor = rh_get_setor()
      ELSE false
    END
  );

CREATE POLICY rh_pulsos_ins ON rh_pulsos
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IN ('administrador','rh','gestor'));

CREATE POLICY rh_pulsos_upd ON rh_pulsos
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_pulsos_del ON rh_pulsos
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');

-- rh_rondas (tem setor_alvo)
CREATE POLICY rh_rondas_sel ON rh_rondas
  FOR SELECT TO authenticated
  USING (
    CASE rh_get_perfil()
      WHEN 'administrador' THEN true
      WHEN 'diretoria'     THEN true
      WHEN 'rh'            THEN true
      WHEN 'gestor'        THEN setor_alvo = rh_get_setor() OR setor_alvo IS NULL
      ELSE false
    END
  );

CREATE POLICY rh_rondas_ins ON rh_rondas
  FOR INSERT TO authenticated
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_rondas_upd ON rh_rondas
  FOR UPDATE TO authenticated
  USING (rh_get_perfil() IN ('administrador','rh'))
  WITH CHECK (rh_get_perfil() IN ('administrador','rh'));

CREATE POLICY rh_rondas_del ON rh_rondas
  FOR DELETE TO authenticated
  USING (rh_get_perfil() = 'administrador');


-- ============================================================================
-- SEÇÃO 4: GARANTIA DE SEGURANÇA — BLOQUEAR ANON EM TUDO
-- ============================================================================

-- Remove qualquer policy anon remanescente e bloqueia acesso anon
-- Tabelas de dados nunca devem ser acessíveis por anon
DO $$ DECLARE t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos','rh_saude_mental',
    'rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao','rh_audit_log',
    'rs_vagas','rs_candidatos','rs_pipeline',
    'rs_entrevistas','rs_avaliacoes','rs_banco_talentos',
    'rh_avaliacoes','rh_metas','rh_pulsos','rh_rondas'
  ]) LOOP
    -- Criar policy explícita de bloqueio anon
    EXECUTE format(
      'CREATE POLICY %I ON %I FOR ALL TO anon USING (false) WITH CHECK (false)',
      t || '_anon_block', t);
  END LOOP;
END $$;


-- ============================================================================
-- SEÇÃO 5: GRANTS
-- ============================================================================

-- Garantir que authenticated pode usar as funções helper
GRANT EXECUTE ON FUNCTION public.rh_get_perfil() TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_get_setor() TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_is_admin() TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_meu_perfil() TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_meu_perfil() TO anon;  -- necessário para login


COMMIT;
