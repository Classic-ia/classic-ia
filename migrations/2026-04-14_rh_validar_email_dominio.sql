-- ══════════════════════════════════════════════════════════════════════
-- BLOCO 1 — Validação server-side de domínio de email
-- Migração: 2026-04-14_rh_validar_email_dominio.sql
-- ══════════════════════════════════════════════════════════════════════
--
-- Objetivo
--   Garantir que apenas emails de domínios autorizados possam ser
--   cadastrados/bootstrap no sistema. Validação NÃO pode depender do
--   frontend (facilmente bypassada).
--
-- Estratégia
--   1. Tabela `rh_dominios_permitidos` — whitelist configurável
--   2. Função `rh_validar_email_dominio(TEXT) RETURNS BOOLEAN` com
--      SECURITY DEFINER, para ser chamada via PostgREST (RPC).
--   3. Trigger de validação no insert/update de `usuarios` e `rh_usuarios`
--      (defesa em profundidade — bloqueia mesmo que a API seja chamada
--      diretamente bypass-ando o RPC).
--
-- Idempotente: pode ser rodada múltiplas vezes sem efeito colateral.
-- ══════════════════════════════════════════════════════════════════════

-- ── 1. Tabela whitelist de domínios ─────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_dominios_permitidos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  dominio TEXT NOT NULL UNIQUE,
  descricao TEXT,
  ativo BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dominios_permitidos_ativo
  ON rh_dominios_permitidos(ativo) WHERE ativo = true;

-- Seed: domínios oficiais da Classic (lowercase, sem @)
INSERT INTO rh_dominios_permitidos (dominio, descricao, ativo) VALUES
  ('classiccouros.com.br', 'Domínio oficial da Classic Couros', true),
  ('classic.com.br',       'Domínio legacy (transição — desativar quando migrado)', true)
ON CONFLICT (dominio) DO NOTHING;

-- ── 2. RPC: rh_validar_email_dominio ────────────────────────────────
-- Entrada : email TEXT
-- Saída   : BOOLEAN (true = domínio permitido, false = bloqueado)
-- Segurança: SECURITY DEFINER — executa com privilégios do dono.
--            Exposto via PostgREST como POST /rpc/rh_validar_email_dominio
-- Chamador : frontend (primeiro_acesso.html), triggers, outras RPCs.
CREATE OR REPLACE FUNCTION rh_validar_email_dominio(p_email TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_dominio TEXT;
  v_permitido BOOLEAN;
BEGIN
  -- Validação básica de formato
  IF p_email IS NULL OR LENGTH(TRIM(p_email)) = 0 THEN
    RETURN false;
  END IF;

  -- Normaliza: lowercase + trim
  p_email := LOWER(TRIM(p_email));

  -- Deve ter exatamente um @ e não estar vazio antes/depois
  IF p_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$' THEN
    RETURN false;
  END IF;

  -- Extrai domínio (tudo após o @)
  v_dominio := SPLIT_PART(p_email, '@', 2);

  IF v_dominio IS NULL OR LENGTH(v_dominio) = 0 THEN
    RETURN false;
  END IF;

  -- Consulta whitelist (case-insensitive por conta do LOWER acima)
  SELECT TRUE INTO v_permitido
  FROM rh_dominios_permitidos
  WHERE ativo = true
    AND LOWER(dominio) = v_dominio
  LIMIT 1;

  RETURN COALESCE(v_permitido, false);
END;
$$;

COMMENT ON FUNCTION rh_validar_email_dominio(TEXT) IS
'Valida se o domínio de um email está na whitelist rh_dominios_permitidos. '
'Retorna BOOLEAN. SECURITY DEFINER — não confiar em validação do frontend. '
'Bloco 1 — Estabilização Classic IA (2026-04-14).';

-- Permitir chamada via PostgREST por qualquer usuário autenticado E anônimo
-- (o bootstrap/primeiro_acesso precisa validar antes de ter sessão)
GRANT EXECUTE ON FUNCTION rh_validar_email_dominio(TEXT) TO anon, authenticated;

-- ── 3. Trigger de defesa em profundidade ────────────────────────────
-- Garante que mesmo chamadas diretas ao PostgREST (bypass do RPC)
-- sejam bloqueadas se o email tiver domínio não autorizado.

CREATE OR REPLACE FUNCTION fn_validar_email_dominio_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.email IS NOT NULL AND NOT rh_validar_email_dominio(NEW.email) THEN
    RAISE EXCEPTION 'Dominio de email nao autorizado: %. Contate o administrador.', NEW.email
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

-- Aplica em `usuarios` (CQ) se a tabela existir
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'usuarios') THEN
    DROP TRIGGER IF EXISTS trg_validar_email_dominio ON usuarios;
    CREATE TRIGGER trg_validar_email_dominio
      BEFORE INSERT OR UPDATE OF email ON usuarios
      FOR EACH ROW EXECUTE FUNCTION fn_validar_email_dominio_trigger();
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rh_usuarios') THEN
    DROP TRIGGER IF EXISTS trg_validar_email_dominio ON rh_usuarios;
    CREATE TRIGGER trg_validar_email_dominio
      BEFORE INSERT OR UPDATE OF email ON rh_usuarios
      FOR EACH ROW EXECUTE FUNCTION fn_validar_email_dominio_trigger();
  END IF;
END $$;

-- ══════════════════════════════════════════════════════════════════════
-- Rollback (não automatizado — rodar manualmente se necessário):
--   DROP TRIGGER IF EXISTS trg_validar_email_dominio ON usuarios;
--   DROP TRIGGER IF EXISTS trg_validar_email_dominio ON rh_usuarios;
--   DROP FUNCTION IF EXISTS fn_validar_email_dominio_trigger();
--   DROP FUNCTION IF EXISTS rh_validar_email_dominio(TEXT);
--   DROP TABLE IF EXISTS rh_dominios_permitidos;
-- ══════════════════════════════════════════════════════════════════════
