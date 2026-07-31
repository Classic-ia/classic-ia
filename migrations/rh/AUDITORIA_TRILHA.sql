-- ============================================================================
-- AUDITORIA_TRILHA.sql
-- Sprint 3 (P2) do PLANO_CORRECOES_SIGA.md — item 4.3
--
-- CONTEXTO:
--   rh_audit_log tem DUAS definicoes divergentes no repo:
--   - FUNDACAO_BANCO_v2 (script 2, a que vale no rebuild): (tabela,
--     registro_id, acao CHECK IN ('INSERT','UPDATE','DELETE_BLOCKED'),
--     dados_*, usuario_id) — shape do fn_audit_trigger
--   - 00_schema_supabase (script 3, no-op no rebuild): (usuario_id,
--     usuario_email, usuario_nome, acao, detalhes, criado_em) — shape que
--     o RHAuth.log do frontend envia
--   Resultado: TODO RHAuth.log falha silenciosamente (catch + console.warn)
--   em ambiente rebuildado — colunas inexistentes, NOT NULL de tabela/
--   registro_id e CHECK de acao violados. Alem disso a tabela tinha policy
--   permissiva FOR ALL (authenticated podia alterar/apagar a trilha).
--
-- O QUE ESTE SCRIPT FAZ (script 30 da ORDEM_REBUILD):
--   1. Unifica o shape: adiciona as colunas do RHAuth.log, remove os
--      NOT NULL de tabela/registro_id e o CHECK de acao (acoes de negocio
--      sao livres: 'login', 'rs_mover_etapa', 'ferias_programar'...)
--   2. Imutabilidade: INSERT autenticado; SELECT so administrador/rh;
--      sem policy nem GRANT de UPDATE/DELETE (trilha append-only;
--      fn_audit_trigger segue funcionando por ser SECURITY DEFINER)
--
-- RPCs de execucao com INSERT em rh_audit_log (mesmo commit):
--   - motor_v2_executar (FUNCOES_APLICACAO.sql)
--   - rh_executar_admissao (WORKFLOW_ADMISSAO_V2.sql)
--   - desligamento ja tem trilha propria (rh_processo: status/executado_por)
--   Em PRODUCAO aplicar este script ANTES de reaplicar as funcoes editadas.
--
-- Idempotente.
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 1. Unificacao do shape
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS usuario_email TEXT;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS usuario_nome  TEXT;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS detalhes      TEXT;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS criado_em     TIMESTAMPTZ DEFAULT now();
-- shape FUNDACAO pode faltar em prod (se la vale o shape 00_schema)
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS tabela            TEXT;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS registro_id       TEXT;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS dados_antigos     JSONB;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS dados_novos       JSONB;
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS campos_alterados  TEXT[];
ALTER TABLE public.rh_audit_log ADD COLUMN IF NOT EXISTS usuario_id        UUID;

ALTER TABLE public.rh_audit_log ALTER COLUMN tabela      DROP NOT NULL;
ALTER TABLE public.rh_audit_log ALTER COLUMN registro_id DROP NOT NULL;

-- CHECK de acao restrito a INSERT/UPDATE/DELETE_BLOCKED impede acoes de
-- negocio (login, mover etapa, programar ferias) — remover
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = 'public.rh_audit_log'::regclass AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%acao%'
  LOOP
    EXECUTE format('ALTER TABLE public.rh_audit_log DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

-- ════════════════════════════════════════════════════════════════════════
-- 2. Imutabilidade (append-only)
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE public.rh_audit_log ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT policyname FROM pg_policies
    WHERE schemaname = 'public' AND tablename = 'rh_audit_log'
  LOOP
    EXECUTE format('DROP POLICY %I ON public.rh_audit_log', r.policyname);
  END LOOP;
END $$;

CREATE POLICY p_audit_insert ON public.rh_audit_log
  FOR INSERT TO authenticated WITH CHECK (true);

CREATE POLICY p_audit_select ON public.rh_audit_log
  FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

REVOKE ALL ON public.rh_audit_log FROM anon;
REVOKE UPDATE, DELETE ON public.rh_audit_log FROM authenticated;
GRANT SELECT, INSERT ON public.rh_audit_log TO authenticated;

COMMENT ON TABLE public.rh_audit_log IS
  'Trilha de auditoria unificada (triggers fn_audit_trigger + RHAuth.log + RPCs de execucao). Append-only: sem UPDATE/DELETE para authenticated.';

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

SELECT policyname, cmd FROM pg_policies WHERE tablename = 'rh_audit_log' ORDER BY policyname;
SELECT has_table_privilege('authenticated','rh_audit_log','UPDATE') AS auth_update_deve_ser_f,
       has_table_privilege('authenticated','rh_audit_log','INSERT') AS auth_insert_deve_ser_t;
