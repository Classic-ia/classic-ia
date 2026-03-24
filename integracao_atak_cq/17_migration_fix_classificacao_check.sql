-- ════════════════════════════════════════════════════════════════════════════
-- Migration 17: Fix classificacao CHECK constraint to allow NULL (drafts)
-- ════════════════════════════════════════════════════════════════════════════
-- Problem: The CHECK constraint on registros_cq_inspecao.classificacao only
-- allows 'A','B','C' but does NOT allow NULL. When the frontend saves a
-- draft (rascunho) without classification (backend calculates it later),
-- the INSERT fails with:
--   "violates check constraint registros_cq_inspecao_classificacao_check"
--
-- Fix: Drop and recreate constraint to allow NULL values.
-- ════════════════════════════════════════════════════════════════════════════

-- Drop existing constraints (may have different names)
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_classificacao_check;

ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS chk_insp_classificacao;

-- Recreate allowing NULL (drafts that haven't been classified yet)
ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_classificacao_check
  CHECK (classificacao IS NULL OR classificacao IN ('A','B','C'));

-- Same fix for status_final (may also fail for drafts)
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_status_final_check;

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_status_final_check
  CHECK (status_final IS NULL OR status_final IN ('aprovado','ressalva','bloqueado'));
