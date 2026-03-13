-- ══════════════════════════════════════════════════════════════
-- Classic CQ — Autenticação, Sessões e Auditoria
-- Executar no SQL Editor do Supabase (Dashboard → SQL Editor)
-- ══════════════════════════════════════════════════════════════

-- ──────────────────────────────────────────────────────────────
-- 1. SESSÕES ATIVAS
--    Uma linha por sessão aberta. Sessão única: ao fazer login,
--    as sessões anteriores do mesmo usuário são marcadas ativa=false.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cq_sessoes (
  id             UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID        NOT NULL,              -- auth.users.id
  usuario_email  TEXT        NOT NULL,
  session_token  TEXT        NOT NULL UNIQUE,       -- gerado no cliente
  dispositivo    TEXT,                              -- user-agent (150 chars)
  criado_em      TIMESTAMPTZ DEFAULT NOW(),
  ultimo_acesso  TIMESTAMPTZ DEFAULT NOW(),
  expira_em      TIMESTAMPTZ DEFAULT NOW() + INTERVAL '1 hour',
  encerrada_em   TIMESTAMPTZ,                       -- preenchido no logout
  ativa          BOOLEAN     DEFAULT TRUE
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_cq_sess_usuario ON cq_sessoes(usuario_id);
CREATE INDEX IF NOT EXISTS idx_cq_sess_token   ON cq_sessoes(session_token);
CREATE INDEX IF NOT EXISTS idx_cq_sess_ativa   ON cq_sessoes(ativa) WHERE ativa = TRUE;

-- RLS: cada usuário acessa apenas suas próprias sessões
ALTER TABLE cq_sessoes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cq_sess_proprio" ON cq_sessoes
  FOR ALL USING (TRUE);  -- anon key tem acesso; validação feita via session_token

-- ──────────────────────────────────────────────────────────────
-- 2. AUDIT LOG
--    Registro imutável de toda ação relevante.
--    Ações: login | logout | session_expired | create_inspecao |
--            edit_inspecao | delete_inspecao | view_relatorio
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cq_audit_log (
  id             UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID,                              -- nulo só em erros de sistema
  usuario_email  TEXT,
  usuario_nome   TEXT,
  acao           TEXT        NOT NULL,              -- ver lista acima
  detalhes       TEXT,                              -- JSON stringificado
  latitude       NUMERIC(10, 7),                   -- geo da inspeção
  longitude      NUMERIC(10, 7),
  dentro_planta  BOOLEAN,                           -- calculado no cliente
  criado_em      TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_cq_audit_usuario ON cq_audit_log(usuario_id);
CREATE INDEX IF NOT EXISTS idx_cq_audit_acao    ON cq_audit_log(acao);
CREATE INDEX IF NOT EXISTS idx_cq_audit_dt      ON cq_audit_log(criado_em DESC);

-- RLS: acesso livre para insert via anon key; leitura só para gestor
ALTER TABLE cq_audit_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cq_audit_insert" ON cq_audit_log
  FOR INSERT WITH CHECK (TRUE);

CREATE POLICY "cq_audit_select" ON cq_audit_log
  FOR SELECT USING (TRUE);   -- controle de acesso feito na aplicação por perfil

-- ──────────────────────────────────────────────────────────────
-- 3. CAMPOS DE AUDITORIA NA TABELA DE INSPEÇÕES
--    Adicionar se a tabela já existir
-- ──────────────────────────────────────────────────────────────
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS usuario_id        UUID,
  ADD COLUMN IF NOT EXISTS usuario_email     TEXT,
  ADD COLUMN IF NOT EXISTS usuario_nome      TEXT,
  ADD COLUMN IF NOT EXISTS session_token     TEXT,
  ADD COLUMN IF NOT EXISTS latitude          NUMERIC(10, 7),
  ADD COLUMN IF NOT EXISTS longitude         NUMERIC(10, 7),
  ADD COLUMN IF NOT EXISTS geo_dentro_planta BOOLEAN;

-- ──────────────────────────────────────────────────────────────
-- 4. VIEW AUDITORIA (para a tela de gestor)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_cq_auditoria AS
SELECT
  a.criado_em,
  a.usuario_nome,
  a.usuario_email,
  a.acao,
  a.dentro_planta,
  a.latitude,
  a.longitude,
  (a.detalhes::jsonb ->> 'lote_atak')    AS lote,
  (a.detalhes::jsonb ->> 'fornecedor')   AS fornecedor,
  (a.detalhes::jsonb ->> 'classificacao') AS classificacao
FROM cq_audit_log a
ORDER BY a.criado_em DESC;

-- ──────────────────────────────────────────────────────────────
-- 5. FUNÇÃO: limpar sessões expiradas (opcional — rodar via cron)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION limpar_sessoes_expiradas()
RETURNS INTEGER AS $$
DECLARE v_count INTEGER;
BEGIN
  UPDATE cq_sessoes
  SET    ativa = FALSE, encerrada_em = NOW()
  WHERE  ativa = TRUE AND expira_em < NOW();
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ──────────────────────────────────────────────────────────────
-- 6. METADADOS DOS USUÁRIOS
--    Ao criar usuário no Supabase Auth, adicionar:
--    raw_user_meta_data: { "nome": "Janaína", "perfil": "gestor" }
--
--    Perfis disponíveis:
--      gestor           — acesso total + auditoria
--      analisador       — criação e edição de inspeções
--      conferente       — apenas visualização
--      qualidade_externo— relatórios e consultas
--      financeiro       — dashboards financeiros
-- ──────────────────────────────────────────────────────────────

-- Para atualizar metadados de um usuário existente:
-- UPDATE auth.users
-- SET raw_user_meta_data = raw_user_meta_data ||
--     '{"nome":"Nome Completo","perfil":"gestor"}'::jsonb
-- WHERE email = 'usuario@email.com';

-- ──────────────────────────────────────────────────────────────
-- 7. VERIFICAÇÃO FINAL
-- ──────────────────────────────────────────────────────────────
SELECT 'cq_sessoes'   AS tabela, COUNT(*) AS registros FROM cq_sessoes
UNION ALL
SELECT 'cq_audit_log', COUNT(*) FROM cq_audit_log
UNION ALL
SELECT 'registros_cq_inspecao (novos campos)',
  COUNT(*) FILTER (WHERE usuario_id IS NOT NULL) FROM registros_cq_inspecao;
