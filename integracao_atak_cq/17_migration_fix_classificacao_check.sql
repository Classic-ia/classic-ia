-- ════════════════════════════════════════════════════════════════════════════
-- Migration 17: Fix ALL CHECK constraints on registros_cq_inspecao
-- ════════════════════════════════════════════════════════════════════════════
-- Multiple CHECK constraints are too restrictive for the draft workflow:
--   - classificacao: only A/B/C, no NULL (drafts need NULL)
--   - status_final: only specific values, no NULL
--   - tipo_amostragem: restricted values don't match frontend options
--   - quantidade_analisada: CHECK > 0 fails for drafts with 0
--
-- Fix: Make all constraints permissive for the draft-then-validate workflow.
-- ════════════════════════════════════════════════════════════════════════════

-- ── classificacao ──────────────────────────────────────────────────────────
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_classificacao_check;
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS chk_insp_classificacao;

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_classificacao_check
  CHECK (classificacao IS NULL OR classificacao IN ('A','B','C'));

-- ── status_final ───────────────────────────────────────────────────────────
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_status_final_check;

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_status_final_check
  CHECK (status_final IS NULL OR status_final IN ('aprovado','ressalva','bloqueado'));

-- ── tipo_amostragem ────────────────────────────────────────────────────────
-- Frontend sends: "Aleatória simples", "100% do lote", "Sistemática",
--                 "Por palete", "Conveniente"
-- Drop restrictive constraint — allow any text value (free-form field)
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_tipo_amostragem_check;

-- ── quantidade_analisada ───────────────────────────────────────────────────
-- Drafts may start with 0; backend validates before submission
ALTER TABLE registros_cq_inspecao
  DROP CONSTRAINT IF EXISTS registros_cq_inspecao_quantidade_analisada_check;

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_quantidade_analisada_check
  CHECK (quantidade_analisada IS NULL OR quantidade_analisada >= 0);

-- ── Ensure columns needed by frontend exist ────────────────────────────────
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS status_workflow TEXT DEFAULT 'rascunho';
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS criador_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS video_evidencia_url TEXT;
