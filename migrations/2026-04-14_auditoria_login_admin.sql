-- ══════════════════════════════════════════════════════════════════════
-- BLOCO 2 — Auditoria de login administrativo
-- Migração: 2026-04-14_auditoria_login_admin.sql
-- ══════════════════════════════════════════════════════════════════════
--
-- Objetivo
--   Rastrear TODOS os acessos administrativos (login, logout, falhas).
--   Tabela canônica: rh_audit_log (existente). CLAUDE.md mencionava
--   rh_audit_trail — nome errado; esta migração alinha tudo.
--
-- Schema atual de rh_audit_log (schema_rh_sst_completo.sql:328):
--   id, usuario_id, usuario_nome, acao, tabela, registro_id,
--   dados_antes, dados_depois, created_at
--
-- Extensão necessária para auditoria de login:
--   usuario_email  TEXT       — registrar mesmo se user_id vier null
--   ip_origem      TEXT       — captura best-effort (NULL se indisponível)
--   user_agent     TEXT
--   sucesso        BOOLEAN    — distingue login ok vs falha
--   detalhes       TEXT       — JSON livre (dispositivo, motivo, etc)
--
-- Todos os campos são NULLABLE (compatível com inserts existentes).
-- Idempotente.
-- ══════════════════════════════════════════════════════════════════════

-- ── 1. Estender rh_audit_log com colunas de auditoria de acesso ─────
ALTER TABLE rh_audit_log
  ADD COLUMN IF NOT EXISTS usuario_email TEXT,
  ADD COLUMN IF NOT EXISTS ip_origem     TEXT,
  ADD COLUMN IF NOT EXISTS user_agent    TEXT,
  ADD COLUMN IF NOT EXISTS sucesso       BOOLEAN,
  ADD COLUMN IF NOT EXISTS detalhes      TEXT;

-- Índices para consultas de auditoria frequentes
CREATE INDEX IF NOT EXISTS idx_audit_acao         ON rh_audit_log(acao);
CREATE INDEX IF NOT EXISTS idx_audit_usuario      ON rh_audit_log(usuario_id);
CREATE INDEX IF NOT EXISTS idx_audit_acao_data    ON rh_audit_log(acao, created_at DESC);

-- ── 2. RPC rh_registrar_login_admin ─────────────────────────────────
-- Chamada pelo frontend APÓS autenticação bem-sucedida com perfil
-- administrador. NUNCA deve falhar silenciosamente — em caso de erro
-- o frontend loga em console.error.
--
-- Assinatura exposta via PostgREST como POST /rpc/rh_registrar_login_admin
CREATE OR REPLACE FUNCTION rh_registrar_login_admin(
  p_usuario_id    UUID,
  p_usuario_email TEXT,
  p_usuario_nome  TEXT,
  p_ip            TEXT DEFAULT NULL,
  p_user_agent    TEXT DEFAULT NULL,
  p_sucesso       BOOLEAN DEFAULT true,
  p_detalhes      TEXT DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_id UUID;
BEGIN
  -- Validação mínima — ação de login exige email ao menos
  IF p_usuario_email IS NULL OR LENGTH(TRIM(p_usuario_email)) = 0 THEN
    RAISE EXCEPTION 'email obrigatorio para auditoria de login_admin'
      USING ERRCODE = 'check_violation';
  END IF;

  INSERT INTO rh_audit_log (
    usuario_id, usuario_email, usuario_nome,
    acao, ip_origem, user_agent, sucesso, detalhes,
    created_at
  ) VALUES (
    p_usuario_id,
    LOWER(TRIM(p_usuario_email)),
    p_usuario_nome,
    'LOGIN_ADMIN',
    p_ip,
    COALESCE(p_user_agent, ''),  -- garante string vazia (não NULL)
    COALESCE(p_sucesso, true),
    p_detalhes,
    NOW()
  )
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$$;

COMMENT ON FUNCTION rh_registrar_login_admin(UUID, TEXT, TEXT, TEXT, TEXT, BOOLEAN, TEXT) IS
'Registra login administrativo em rh_audit_log com acao = LOGIN_ADMIN. '
'Usado por rh_auth.js após login bem-sucedido de perfil administrador/admin. '
'Bloco 2 — Estabilização Classic IA (2026-04-14).';

GRANT EXECUTE ON FUNCTION rh_registrar_login_admin(UUID, TEXT, TEXT, TEXT, TEXT, BOOLEAN, TEXT)
  TO authenticated;

-- ── 3. RLS — apenas admin pode consultar audit log ──────────────────
-- Se já existe política, não recriar. Se não, aplicar política restrita.
ALTER TABLE rh_audit_log ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  -- Tentar criar policy de SELECT restrita a admin (idempotente)
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'rh_audit_log' AND policyname = 'audit_log_select_admin'
  ) THEN
    CREATE POLICY audit_log_select_admin ON rh_audit_log
      FOR SELECT
      TO authenticated
      USING (
        EXISTS (
          SELECT 1 FROM rh_usuarios u
          WHERE u.id = auth.uid()
            AND u.perfil IN ('administrador', 'admin')
            AND u.ativo = true
        )
      );
  END IF;

  -- INSERT livre para authenticated (frontend pode inserir suas próprias ações)
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'rh_audit_log' AND policyname = 'audit_log_insert_auth'
  ) THEN
    CREATE POLICY audit_log_insert_auth ON rh_audit_log
      FOR INSERT
      TO authenticated
      WITH CHECK (true);
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- rh_usuarios pode não existir em algum ambiente de teste; ignora
  RAISE NOTICE 'rh_usuarios ausente — policy de admin não aplicada';
END $$;

-- ══════════════════════════════════════════════════════════════════════
-- Rollback (manual):
--   DROP POLICY IF EXISTS audit_log_select_admin ON rh_audit_log;
--   DROP POLICY IF EXISTS audit_log_insert_auth  ON rh_audit_log;
--   DROP FUNCTION IF EXISTS rh_registrar_login_admin(UUID,TEXT,TEXT,TEXT,TEXT,BOOLEAN,TEXT);
--   ALTER TABLE rh_audit_log
--     DROP COLUMN IF EXISTS usuario_email,
--     DROP COLUMN IF EXISTS ip_origem,
--     DROP COLUMN IF EXISTS user_agent,
--     DROP COLUMN IF EXISTS sucesso,
--     DROP COLUMN IF EXISTS detalhes;
-- ══════════════════════════════════════════════════════════════════════
