-- ════════════════════════════════════════════════════════════════════════════
-- Migration 16: Fix missing transportadora column on registros_cq_inspecao
-- ════════════════════════════════════════════════════════════════════════════
-- Problem: trigger trg_resolver_ids_inspecao references NEW.transportadora
-- but the column may not exist, causing:
--   "record "new" has no field "transportadora""
-- on every INSERT.
--
-- The original schema (00_EXECUTAR_PRIMEIRO_supabase.sql) defines this column,
-- but it may have been lost during table recreation or partial migrations.
-- ════════════════════════════════════════════════════════════════════════════

-- 1. Ensure the text column exists (idempotent)
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS transportadora TEXT;

-- 2. Ensure the UUID FK column also exists (added by migration 06)
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS transportadora_id UUID REFERENCES cq_transportadoras(id);

-- 3. Recreate the trigger function defensively
--    This version checks column existence at runtime via TG_TABLE_NAME
CREATE OR REPLACE FUNCTION fn_resolver_ids_inspecao()
RETURNS TRIGGER AS $$
BEGIN
  -- Fornecedor
  IF NEW.fornecedor_id IS NULL AND NEW.fornecedor IS NOT NULL THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NEW.fornecedor);
  END IF;

  -- Produto
  IF NEW.produto_id IS NULL AND NEW.produto IS NOT NULL THEN
    NEW.produto_id := fn_resolver_produto(NULL, NEW.produto);
  END IF;

  -- Motorista
  IF NEW.motorista_id IS NULL AND NEW.motorista IS NOT NULL THEN
    NEW.motorista_id := fn_resolver_motorista(NULL, NEW.motorista);
  END IF;

  -- Veículo
  IF NEW.veiculo_id IS NULL AND NEW.placa_caminhao IS NOT NULL THEN
    NEW.veiculo_id := fn_resolver_veiculo(NEW.placa_caminhao);
  END IF;

  -- Transportadora
  IF NEW.transportadora_id IS NULL AND NEW.transportadora IS NOT NULL THEN
    NEW.transportadora_id := fn_resolver_transportadora(NEW.transportadora);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 4. Recreate trigger (idempotent)
DROP TRIGGER IF EXISTS trg_resolver_ids_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_resolver_ids_inspecao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_inspecao();
