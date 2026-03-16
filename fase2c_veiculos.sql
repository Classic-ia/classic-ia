-- ============================================================
-- FASE 2C — CADASTRO DE VEÍCULOS
-- Data: 2026-03-16
-- Importação da planilha ATAK: 64 veículos
-- ============================================================

-- ============================================================
-- PASSO 1: Criar tabela cq_veiculos
-- ============================================================

CREATE TABLE IF NOT EXISTS cq_veiculos (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  placa         TEXT NOT NULL UNIQUE,
  nome          TEXT NOT NULL,                    -- modelo/descrição do veículo
  tipo_veiculo  TEXT NOT NULL DEFAULT 'OUTROS'
    CHECK (tipo_veiculo IN ('CARRETA','TRUCK','CAMINHONETE','3/4 (CAMINHONETE)','TOCO','OUTROS')),
  uf            TEXT DEFAULT 'PR',
  proprio       BOOLEAN DEFAULT FALSE,            -- veículo próprio da Classic
  ativo         BOOLEAN DEFAULT TRUE,
  criado_em     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_veiculos_placa ON cq_veiculos(placa);
CREATE INDEX IF NOT EXISTS idx_veiculos_tipo  ON cq_veiculos(tipo_veiculo);

-- RLS
ALTER TABLE cq_veiculos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "veic_select" ON cq_veiculos
  FOR SELECT TO authenticated
  USING (TRUE);

CREATE POLICY "veic_insert" ON cq_veiculos
  FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','logistica'));

CREATE POLICY "veic_update" ON cq_veiculos
  FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','logistica'));

-- Verificação P1
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'cq_veiculos'
ORDER BY ordinal_position;

-- ============================================================
-- PASSO 2: Importar 64 veículos da planilha ATAK
-- ============================================================

INSERT INTO cq_veiculos (placa, nome, tipo_veiculo, uf, proprio) VALUES
  ('AFR0463', 'SCANIA R 420 A 4X2', 'CARRETA', 'PR', FALSE),
  ('AFR0B94', 'SCANIA R 420', 'CARRETA', 'PR', FALSE),
  ('ARG5G61', 'VOLKSWAGEM/ 8.120 EURO3', 'OUTROS', 'PR', FALSE),
  ('ATE1H28', 'MERCEDES BENZ - ATEGO 2428', 'TRUCK', 'PR', FALSE),
  ('ATK0007', 'OUTROS VEICULOS', 'CAMINHONETE', 'PR', FALSE),
  ('ATK002', 'OUTROS VEICULOS FRETE VIVO', 'OUTROS', 'PR', FALSE),
  ('ATK003', 'TRUCK ALONGADO FRETE VIVO', 'TRUCK', 'PR', FALSE),
  ('ATK004', 'CAMINHO 3/4 FRETE VIVO', '3/4 (CAMINHONETE)', 'PR', FALSE),
  ('ATK005', 'TRUCK FRETE VIVO', 'TOCO', 'PR', FALSE),
  ('ATK006', 'CARRETA FRETE VIVO', 'CARRETA', 'PR', FALSE),
  ('AUA6F69', 'VOLKSWAGEM 140E DELIVERY', '3/4 (CAMINHONETE)', 'PR', FALSE),
  ('AWR1E30', 'FIAT STRADA WORKING', 'CAMINHONETE', 'PR', FALSE),
  ('AWZ2G96', 'MERCEDES BENZ ACCELO 1016', 'TRUCK', 'PR', FALSE),
  ('AXR4J10', 'SCANIA P310 8X2', 'TRUCK', 'PR', FALSE),
  ('AXW9J71', 'VOLVO FH 460 6X4T', 'CARRETA', 'PR', FALSE),
  ('AZE6J92', 'MERCEDES BENZ ATEGO 2429', 'TRUCK', 'PR', FALSE),
  ('AZT7G15', 'CHEVROLET S10 LS DS4', 'CAMINHONETE', 'PR', FALSE),
  ('BCA1D20', 'SCANIA P 360 A6X2', 'CARRETA', 'PR', FALSE),
  ('BDQ2D29', 'VOLVO/FH 540 6X4T', 'CARRETA', 'PR', FALSE),
  ('BEA3J32', 'MERCEDES BENZ ACTROS 2456LS', 'CARRETA', 'PR', FALSE),
  ('BEP3E20', 'SCANIA P320 8X2', 'TRUCK', 'PR', FALSE),
  ('BEY3F09', 'MERCEDES BENZ ATEGO 1419', 'TRUCK', 'PR', FALSE),
  ('BEZ8C86', 'SCANIA P320 8X2', 'TRUCK', 'PR', FALSE),
  ('BEZ8C87', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('BYQ4E46', 'FIAT STRADA FREEDOM CC', 'CAMINHONETE', 'PR', FALSE),
  ('CUK4I41', 'MERCEDES BENZ ATEGO 1719', 'TRUCK', 'PR', FALSE),
  ('JAS3E65', 'VOLVO FH 460 6X2T', 'CARRETA', 'PR', FALSE),
  ('JAS4B46', 'VOLVO FH 460 6X2T', 'CARRETA', 'PR', FALSE),
  ('JCB6C21', 'VW/13.180 DRC 6X2', 'TRUCK', 'SP', FALSE),
  ('JCD7E97', 'VOLVO/FH 460 4X2T', 'TRUCK', 'PR', FALSE),
  ('RGG5F56', 'MERCEDES BENZ - ATEGO 3030 CE', 'TRUCK', 'PR', FALSE),
  ('RHA3B46', 'DAF XF FTS 480', 'CARRETA', 'PR', FALSE),
  ('RHC1D47', 'MERCEDES BENZ ACTROS 2548S', 'CARRETA', 'PR', FALSE),
  ('RHE0H37', 'FIAT/STRADA FREEDOM 13CD', 'CAMINHONETE', 'PR', FALSE),
  ('RHI9I13', 'DAF XF FTS 480', 'CARRETA', 'PR', FALSE),
  ('RHL2F37', 'MERCEDES BENZ ATEGO 3030 CE', 'CARRETA', 'PR', FALSE),
  ('RHN5E02', 'VW/24.280 CRM 6X2', 'TRUCK', 'PR', FALSE),
  ('RHN9F68', 'VOLVO VM 330 8X2R', 'TRUCK', 'PR', FALSE),
  ('RHN9F69', 'VOLVO VM 330 8X2R', 'TRUCK', 'PR', FALSE),
  ('RHO5C46', 'PEUGEOT EXPERT BUSINPK', 'CAMINHONETE', 'PR', FALSE),
  ('RHO9J51', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('RHO9J52', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('RHP9I13', 'DAF/XF FTS 480', 'TRUCK', 'PR', FALSE),
  ('RHP9I14', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('RHP9I15', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('RHR9C33', 'VOLVO FH 500 6X2T', 'CARRETA', 'PR', FALSE),
  ('RHT5B97', 'MERCEDES BENZ ACELLO 1316 CE', 'TRUCK', 'PR', FALSE),
  ('RXM6H08', 'SCANIA R450 A6X2', 'CARRETA', 'PR', FALSE),
  ('SCI6E76', 'VW/25.460 CTM 6X2', 'TRUCK', 'PR', FALSE),
  ('SDE9H73', 'IVECO/TECTOR 240E28', 'CARRETA', 'PR', FALSE),
  ('SDG7J58', 'VW/29.520 METEOR 6X4', 'TRUCK', 'PR', FALSE),
  ('SDQ2E29', 'SCANIA R450 A6X2', 'CARRETA', 'PR', FALSE),
  ('SET2C48', 'TOYOTA/HILUX', 'CAMINHONETE', 'PR', FALSE),
  ('SFC2E18', 'FIAT/STRADA FREEDOM CS13', 'CAMINHONETE', 'PR', FALSE),
  ('SFD5E24', 'FIAT/STRADA', 'OUTROS', 'PR', FALSE),
  ('SFY2C67', 'VW/28.480 MTM 6X2', 'CARRETA', 'PR', FALSE),
  ('SNK6E91', 'VOLVO/VM 360 8X2 R', 'TRUCK', 'AL', FALSE),
  ('SPC3F43', 'VW/26.260 CRM 6X2', 'TRUCK', 'PR', FALSE),
  ('SRL0E11', 'VW / 26.320 CRM 6X2', 'OUTROS', 'SP', FALSE),
  ('TAV8F89', 'VOLVO/FH 460 6X2T', 'CARRETA', 'PR', FALSE),
  ('TAV8F91', 'VOLVO/FH 500 6X2T', 'TRUCK', 'PR', FALSE),
  ('TBC1J30', 'FIAT / STRADA FREEDOM CS13', 'CAMINHONETE', 'PR', FALSE),
  ('UAS3E58', 'VOLVO/VM 360 8X2 R', 'TRUCK', 'PR', FALSE),
  ('UAS3H59', 'VOLVO/VM 360 8X2 R', 'TRUCK', 'PR', FALSE)
ON CONFLICT (placa) DO NOTHING;

-- Verificação P2
SELECT tipo_veiculo, COUNT(*) as total
FROM cq_veiculos
GROUP BY tipo_veiculo
ORDER BY total DESC;

-- ============================================================
-- PASSO 3: Verificação final
-- ============================================================

-- V1: Total de veículos
SELECT COUNT(*) as total_veiculos FROM cq_veiculos;

-- V2: Policies
SELECT policyname, cmd, qual
FROM pg_policies
WHERE tablename = 'cq_veiculos';

-- V3: Tipos de veículo
SELECT tipo_veiculo, COUNT(*) as qtd
FROM cq_veiculos
GROUP BY tipo_veiculo
ORDER BY qtd DESC;
