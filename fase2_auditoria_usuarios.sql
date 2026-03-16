-- ══════════════════════════════════════════════════════════════════
-- FASE 2 — AUDITORIA DE USUÁRIOS (executar antes da migração)
-- ══════════════════════════════════════════════════════════════════

-- ──────────────────────────────────────────────────────────────────
-- 1. Todos os usuários em auth.users
-- ──────────────────────────────────────────────────────────────────
SELECT
  id AS auth_id,
  email,
  raw_user_meta_data->>'nome' AS nome_metadata,
  raw_user_meta_data->>'perfil' AS perfil_metadata,
  created_at,
  last_sign_in_at,
  CASE
    WHEN banned_until IS NOT NULL AND banned_until > NOW() THEN 'BANIDO'
    WHEN confirmed_at IS NULL THEN 'NAO_CONFIRMADO'
    ELSE 'ATIVO'
  END AS status_auth
FROM auth.users
ORDER BY created_at;

-- ──────────────────────────────────────────────────────────────────
-- 2. Todos os usuários em cq_usuarios
-- ──────────────────────────────────────────────────────────────────
SELECT
  id,
  auth_id,
  email,
  nome,
  perfil,
  ativo,
  criado_em,
  ultimo_login
FROM cq_usuarios
ORDER BY criado_em;

-- ──────────────────────────────────────────────────────────────────
-- 3. Divergências: auth.users SEM registro em cq_usuarios
-- ──────────────────────────────────────────────────────────────────
SELECT
  a.id AS auth_id,
  a.email,
  a.raw_user_meta_data->>'nome' AS nome_metadata,
  a.raw_user_meta_data->>'perfil' AS perfil_metadata,
  a.created_at,
  'ORFAO_SEM_CQ_USUARIOS' AS situacao
FROM auth.users a
LEFT JOIN cq_usuarios u ON u.auth_id = a.id OR u.email = a.email
WHERE u.id IS NULL;

-- ──────────────────────────────────────────────────────────────────
-- 4. Divergências: cq_usuarios SEM vínculo com auth.users
-- ──────────────────────────────────────────────────────────────────
SELECT
  u.id,
  u.email,
  u.nome,
  u.perfil,
  u.auth_id,
  u.ativo,
  'SEM_AUTH_ID' AS situacao
FROM cq_usuarios u
WHERE u.auth_id IS NULL
   OR NOT EXISTS (SELECT 1 FROM auth.users a WHERE a.id = u.auth_id);

-- ──────────────────────────────────────────────────────────────────
-- 5. Divergências de perfil: metadata vs cq_usuarios
-- ──────────────────────────────────────────────────────────────────
SELECT
  a.email,
  a.raw_user_meta_data->>'perfil' AS perfil_metadata,
  u.perfil AS perfil_cq_usuarios,
  CASE
    WHEN (a.raw_user_meta_data->>'perfil') != u.perfil THEN 'DIVERGENTE'
    ELSE 'OK'
  END AS status
FROM auth.users a
JOIN cq_usuarios u ON u.auth_id = a.id OR u.email = a.email
ORDER BY a.email;

-- ──────────────────────────────────────────────────────────────────
-- 6. Emails únicos que aparecem em audit/sessões mas não em cq_usuarios
-- ──────────────────────────────────────────────────────────────────
SELECT DISTINCT usuario_email AS email, 'cq_audit_log' AS origem
FROM cq_audit_log
WHERE usuario_email NOT IN (SELECT email FROM cq_usuarios)
UNION
SELECT DISTINCT usuario_email, 'cq_sessoes'
FROM cq_sessoes
WHERE usuario_email NOT IN (SELECT email FROM cq_usuarios)
ORDER BY email;
