-- Migration 19: Lógica de Desconto por Quantidade e Qualidade
-- Adicionar campos de desconto à tabela registros_cq
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS responsavel_carteira VARCHAR(20);
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS decisao_desconto VARCHAR(30);
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS valor_desconto_quantidade NUMERIC(10,2);
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS valor_desconto_qualidade NUMERIC(10,2);
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS valor_desconto_total NUMERIC(10,2);
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS obs_desconto TEXT;
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS desconto_confirmado_em TIMESTAMPTZ;
ALTER TABLE registros_cq ADD COLUMN IF NOT EXISTS desconto_executado_por VARCHAR(100);

-- Também na tabela de inspeções
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS responsavel_carteira VARCHAR(20);
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS decisao_desconto VARCHAR(30);
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS valor_desconto_quantidade NUMERIC(10,2);
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS valor_desconto_qualidade NUMERIC(10,2);
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS valor_desconto_total NUMERIC(10,2);
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS obs_desconto TEXT;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS desconto_confirmado_em TIMESTAMPTZ;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS desconto_executado_por VARCHAR(100);
