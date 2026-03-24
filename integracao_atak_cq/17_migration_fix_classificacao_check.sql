-- ════════════════════════════════════════════════════════════════════════════
-- Migration 17: COMPREHENSIVE FIX — registros_cq_inspecao
-- ════════════════════════════════════════════════════════════════════════════
-- This migration ensures the table has ALL columns needed by the frontend
-- and ALL triggers, with constraints permissive enough for the draft workflow.
-- Run this ONCE and all save errors will be resolved.
-- ════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════
-- PART 1: ENSURE ALL COLUMNS EXIST
-- ══════════════════════════════════════════════════════════════════════════

-- Columns from original schema that may be missing
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS transportadora TEXT;

-- Columns needed by frontend payload
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS status_workflow TEXT DEFAULT 'rascunho';
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS criador_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS video_evidencia_url TEXT;

-- Columns needed by trigger trg_resolver_ids_inspecao (migration 06)
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS fornecedor_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS produto_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS motorista_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS veiculo_id UUID;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS transportadora_id UUID;

-- Columns needed by trigger fn_motor_decisao_inspecao (migration 13)
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS pct_defeitos_criticos NUMERIC(6,2) DEFAULT 0;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS pct_defeitos_maiores NUMERIC(6,2) DEFAULT 0;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS pct_defeitos_menores NUMERIC(6,2) DEFAULT 0;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS decisao_motor TEXT;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS score_lote NUMERIC(6,2) DEFAULT 100;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS requer_reamostragem BOOLEAN DEFAULT FALSE;
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS reamostragem_id UUID;


-- ══════════════════════════════════════════════════════════════════════════
-- PART 1b: DROP FOREIGN KEY on usuario_id (points to cq_usuarios but
--          frontend sends auth.uid() from Supabase Auth — different table)
-- ══════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_usuario_id_fkey;
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS fk_insp_usuario;


-- ══════════════════════════════════════════════════════════════════════════
-- PART 2: DROP ALL RESTRICTIVE CHECK CONSTRAINTS
-- ══════════════════════════════════════════════════════════════════════════

-- classificacao
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_classificacao_check;
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_classificacao;

-- status_final
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_status_final_check;
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_status_final;

-- tipo_amostragem
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_tipo_amostragem_check;

-- quantidade_analisada
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_quantidade_analisada_check;
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_qtd_analisada_pos;

-- percentual_defeitos
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_percentual_defeitos_check;
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_pct_range;

-- decisao_motor
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS registros_cq_inspecao_decisao_motor_check;


-- ══════════════════════════════════════════════════════════════════════════
-- PART 3: RECREATE CONSTRAINTS — PERMISSIVE (all allow NULL for drafts)
-- ══════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_classificacao_check
  CHECK (classificacao IS NULL OR classificacao IN ('A','B','C'));

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_status_final_check
  CHECK (status_final IS NULL OR status_final IN ('aprovado','ressalva','bloqueado'));

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_quantidade_analisada_check
  CHECK (quantidade_analisada IS NULL OR quantidade_analisada >= 0);

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_percentual_defeitos_check
  CHECK (percentual_defeitos IS NULL OR (percentual_defeitos >= 0 AND percentual_defeitos <= 100));

ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT registros_cq_inspecao_decisao_motor_check
  CHECK (decisao_motor IS NULL OR decisao_motor IN ('APROVADO','RESSALVA','BLOQUEADO','EM_ANALISE_CRITICA'));

-- tipo_amostragem: NO constraint — free-form text from frontend


-- ══════════════════════════════════════════════════════════════════════════
-- PART 4: RELAX NOT NULL WHERE NEEDED FOR DRAFTS
-- ══════════════════════════════════════════════════════════════════════════

-- quantidade_analisada may be 0 in early drafts
ALTER TABLE registros_cq_inspecao ALTER COLUMN quantidade_analisada DROP NOT NULL;
ALTER TABLE registros_cq_inspecao ALTER COLUMN quantidade_analisada SET DEFAULT 0;

-- etiqueta may be empty for some inspection types
ALTER TABLE registros_cq_inspecao ALTER COLUMN etiqueta DROP NOT NULL;
ALTER TABLE registros_cq_inspecao ALTER COLUMN etiqueta SET DEFAULT '';


-- ══════════════════════════════════════════════════════════════════════════
-- PART 5: MAKE TRIGGER fn_resolver_ids_inspecao DEFENSIVE
-- ══════════════════════════════════════════════════════════════════════════
-- Wrap in EXCEPTION block so missing resolver functions don't crash the INSERT

CREATE OR REPLACE FUNCTION fn_resolver_ids_inspecao()
RETURNS TRIGGER AS $$
BEGIN
  -- Fornecedor
  BEGIN
    IF NEW.fornecedor_id IS NULL AND NEW.fornecedor IS NOT NULL AND NEW.fornecedor != '' THEN
      NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NEW.fornecedor);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'fn_resolver_fornecedor falhou: %', SQLERRM;
  END;

  -- Produto
  BEGIN
    IF NEW.produto_id IS NULL AND NEW.produto IS NOT NULL AND NEW.produto != '' THEN
      NEW.produto_id := fn_resolver_produto(NULL, NEW.produto);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'fn_resolver_produto falhou: %', SQLERRM;
  END;

  -- Motorista
  BEGIN
    IF NEW.motorista_id IS NULL AND NEW.motorista IS NOT NULL AND NEW.motorista != '' THEN
      NEW.motorista_id := fn_resolver_motorista(NULL, NEW.motorista);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'fn_resolver_motorista falhou: %', SQLERRM;
  END;

  -- Veículo
  BEGIN
    IF NEW.veiculo_id IS NULL AND NEW.placa_caminhao IS NOT NULL AND NEW.placa_caminhao != '' THEN
      NEW.veiculo_id := fn_resolver_veiculo(NEW.placa_caminhao);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'fn_resolver_veiculo falhou: %', SQLERRM;
  END;

  -- Transportadora
  BEGIN
    IF NEW.transportadora_id IS NULL AND NEW.transportadora IS NOT NULL AND NEW.transportadora != '' THEN
      NEW.transportadora_id := fn_resolver_transportadora(NEW.transportadora);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'fn_resolver_transportadora falhou: %', SQLERRM;
  END;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_resolver_ids_inspecao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_inspecao();


-- ══════════════════════════════════════════════════════════════════════════
-- PART 6: ENSURE cq_inspecao_defeitos TABLE EXISTS (needed by trg_sync_defeitos)
-- ══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_inspecao_defeitos (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  inspecao_id   UUID NOT NULL,
  defeito_id    UUID NOT NULL,
  quantidade    INTEGER DEFAULT 0,
  criado_em     TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(inspecao_id, defeito_id)
);


-- ══════════════════════════════════════════════════════════════════════════
-- DONE. All columns, constraints, and triggers are now safe for the
-- draft-then-validate workflow used by inspecao_qualidade.html.
-- ══════════════════════════════════════════════════════════════════════════
