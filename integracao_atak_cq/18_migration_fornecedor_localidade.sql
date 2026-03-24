-- ============================================================
-- Migration 18: Adicionar cidade e UF do fornecedor na inspeção
-- Para que os relatórios exibam a localidade do frigorífico
-- ============================================================

-- Adicionar colunas na tabela de inspeções
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS fornecedor_uf      TEXT,
  ADD COLUMN IF NOT EXISTS fornecedor_cidade   TEXT;

-- Índice para consultas por estado
CREATE INDEX IF NOT EXISTS idx_inspecao_fornecedor_uf
  ON registros_cq_inspecao (fornecedor_uf)
  WHERE fornecedor_uf IS NOT NULL;

-- Permitir acesso via RLS (segue padrão existente)
COMMENT ON COLUMN registros_cq_inspecao.fornecedor_uf     IS 'UF (estado) do frigorífico fornecedor';
COMMENT ON COLUMN registros_cq_inspecao.fornecedor_cidade  IS 'Cidade do frigorífico fornecedor';

-- Verificação
SELECT 'migration_18_fornecedor_localidade' AS migration, 'OK' AS status;
