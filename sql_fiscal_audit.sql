-- ============================================================
-- Classic CQ — Módulo Auditor Fiscal (Lucro Real + Exportação)
-- Execute este SQL no Supabase SQL Editor
-- ============================================================

-- 1. Adicionar campos fiscais na tabela de movimentações
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS nat_operacao TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS cst_icms TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS aliq_icms NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS bc_icms NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS icms_st NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS fcp NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS orig TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS cst_pis TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS aliq_pis NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS bc_pis NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS cst_cofins TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS aliq_cofins NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS bc_cofins NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS ipi NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS cst_ipi TEXT;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS aliq_ipi NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS ii NUMERIC DEFAULT 0;
ALTER TABLE estoque_movimentacoes ADD COLUMN IF NOT EXISTS filial TEXT DEFAULT 'MATRIZ';

-- 2. Campo estoque_minimo na tabela de categorias (se ainda não existe)
ALTER TABLE estoque_categorias ADD COLUMN IF NOT EXISTS estoque_minimo NUMERIC DEFAULT 0;

-- 3. Tabela de contratos de exportação
CREATE TABLE IF NOT EXISTS exportacao_contratos (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  numero_contrato TEXT NOT NULL,
  cliente         TEXT NOT NULL,
  cliente_cnpj    TEXT,
  pais_destino    TEXT,
  incoterm        TEXT CHECK (incoterm IN ('FOB','CIF','CFR','EXW','FCA','CPT','CIP','DAP','DPU','DDP')),
  moeda           TEXT DEFAULT 'USD',
  valor_contrato  NUMERIC DEFAULT 0,
  taxa_cambio     NUMERIC DEFAULT 0,
  valor_brl       NUMERIC DEFAULT 0,
  data_contrato   DATE,
  data_embarque_prev DATE,
  data_embarque_real DATE,
  status          TEXT DEFAULT 'ativo' CHECK (status IN ('ativo','embarcado','finalizado','cancelado')),
  due_numero      TEXT,
  bl_numero       TEXT,
  ncm_principal   TEXT,
  peso_liquido_kg NUMERIC DEFAULT 0,
  observacoes     TEXT,
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now()
);

-- 4. Tabela de contratos de câmbio (ACC/ACE)
CREATE TABLE IF NOT EXISTS exportacao_cambio (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  contrato_id     UUID REFERENCES exportacao_contratos(id),
  tipo            TEXT CHECK (tipo IN ('ACC','ACE','PRONTO')),
  banco           TEXT,
  numero_cambio   TEXT,
  moeda           TEXT DEFAULT 'USD',
  valor_moeda     NUMERIC DEFAULT 0,
  taxa_contratada NUMERIC DEFAULT 0,
  valor_brl       NUMERIC DEFAULT 0,
  data_contratacao DATE,
  data_vencimento  DATE,
  data_liquidacao  DATE,
  status          TEXT DEFAULT 'ativo' CHECK (status IN ('ativo','liquidado','vencido','cancelado')),
  criado_em       TIMESTAMPTZ DEFAULT now()
);

-- 5. Tabela de Drawback
CREATE TABLE IF NOT EXISTS exportacao_drawback (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  numero_ato      TEXT NOT NULL,
  modalidade      TEXT CHECK (modalidade IN ('suspensao','isencao','restituicao')),
  data_concessao  DATE,
  data_vencimento DATE,
  valor_importacao NUMERIC DEFAULT 0,
  valor_exportacao_compromisso NUMERIC DEFAULT 0,
  valor_exportado NUMERIC DEFAULT 0,
  status          TEXT DEFAULT 'ativo' CHECK (status IN ('ativo','adimplido','inadimplido','vencido')),
  observacoes     TEXT,
  criado_em       TIMESTAMPTZ DEFAULT now()
);

-- 6. View de apuração fiscal mensal detalhada
CREATE OR REPLACE VIEW vw_apuracao_fiscal AS
SELECT
  date_trunc('month', m.data_emissao)::date AS competencia,
  m.tipo,
  m.cfop,
  m.cst_icms,
  m.cst_pis,
  m.cst_cofins,
  COUNT(*) AS qtd_itens,
  COUNT(DISTINCT m.chave_nfe) AS qtd_nfs,
  COALESCE(SUM(m.valor_total), 0) AS valor_total,
  COALESCE(SUM(m.bc_icms), 0) AS base_icms,
  COALESCE(SUM(m.icms), 0) AS icms_total,
  COALESCE(SUM(m.icms_st), 0) AS icms_st_total,
  COALESCE(SUM(m.bc_pis), 0) AS base_pis,
  COALESCE(SUM(m.pis), 0) AS pis_total,
  COALESCE(SUM(m.bc_cofins), 0) AS base_cofins,
  COALESCE(SUM(m.cofins), 0) AS cofins_total,
  COALESCE(SUM(m.ipi), 0) AS ipi_total,
  COALESCE(SUM(m.ii), 0) AS ii_total
FROM estoque_movimentacoes m
GROUP BY competencia, m.tipo, m.cfop, m.cst_icms, m.cst_pis, m.cst_cofins
ORDER BY competencia DESC, m.tipo;

-- 7. View de créditos acumulados por mês
CREATE OR REPLACE VIEW vw_creditos_acumulados AS
SELECT
  date_trunc('month', m.data_emissao)::date AS competencia,
  -- PIS
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.pis ELSE 0 END), 0) AS pis_credito,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.pis ELSE 0 END), 0) AS pis_debito,
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.pis ELSE 0 END), 0)
    - COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.pis ELSE 0 END), 0) AS pis_saldo,
  -- COFINS
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.cofins ELSE 0 END), 0) AS cofins_credito,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.cofins ELSE 0 END), 0) AS cofins_debito,
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.cofins ELSE 0 END), 0)
    - COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.cofins ELSE 0 END), 0) AS cofins_saldo,
  -- ICMS
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.icms ELSE 0 END), 0) AS icms_credito,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.icms ELSE 0 END), 0) AS icms_debito,
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.icms ELSE 0 END), 0)
    - COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.icms ELSE 0 END), 0) AS icms_saldo,
  -- IPI
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.ipi ELSE 0 END), 0) AS ipi_credito,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.ipi ELSE 0 END), 0) AS ipi_debito,
  -- Totais
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.valor_total ELSE 0 END), 0) AS valor_compras,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida' THEN m.valor_total ELSE 0 END), 0) AS valor_vendas,
  -- Exportações (CFOP 7xxx)
  COALESCE(SUM(CASE WHEN m.cfop LIKE '7%' THEN m.valor_total ELSE 0 END), 0) AS valor_exportacoes
FROM estoque_movimentacoes m
GROUP BY competencia
ORDER BY competencia DESC;

-- 8. RLS para novas tabelas
ALTER TABLE exportacao_contratos ENABLE ROW LEVEL SECURITY;
ALTER TABLE exportacao_cambio    ENABLE ROW LEVEL SECURITY;
ALTER TABLE exportacao_drawback  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "exp_cont_select" ON exportacao_contratos FOR SELECT TO authenticated USING (true);
CREATE POLICY "exp_cont_insert" ON exportacao_contratos FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "exp_cont_update" ON exportacao_contratos FOR UPDATE TO authenticated USING (true);
CREATE POLICY "exp_cont_delete" ON exportacao_contratos FOR DELETE TO authenticated USING (true);

CREATE POLICY "exp_camb_select" ON exportacao_cambio FOR SELECT TO authenticated USING (true);
CREATE POLICY "exp_camb_insert" ON exportacao_cambio FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "exp_camb_update" ON exportacao_cambio FOR UPDATE TO authenticated USING (true);
CREATE POLICY "exp_camb_delete" ON exportacao_cambio FOR DELETE TO authenticated USING (true);

CREATE POLICY "exp_draw_select" ON exportacao_drawback FOR SELECT TO authenticated USING (true);
CREATE POLICY "exp_draw_insert" ON exportacao_drawback FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "exp_draw_update" ON exportacao_drawback FOR UPDATE TO authenticated USING (true);
CREATE POLICY "exp_draw_delete" ON exportacao_drawback FOR DELETE TO authenticated USING (true);

-- Anon read
CREATE POLICY "exp_cont_anon" ON exportacao_contratos FOR SELECT TO anon USING (true);
CREATE POLICY "exp_camb_anon" ON exportacao_cambio FOR SELECT TO anon USING (true);
CREATE POLICY "exp_draw_anon" ON exportacao_drawback FOR SELECT TO anon USING (true);

-- 9. Índices para performance
CREATE INDEX IF NOT EXISTS idx_mov_cfop ON estoque_movimentacoes(cfop);
CREATE INDEX IF NOT EXISTS idx_mov_cst_pis ON estoque_movimentacoes(cst_pis);
CREATE INDEX IF NOT EXISTS idx_mov_filial ON estoque_movimentacoes(filial);
CREATE INDEX IF NOT EXISTS idx_exp_cont_status ON exportacao_contratos(status);
CREATE INDEX IF NOT EXISTS idx_exp_camb_venc ON exportacao_cambio(data_vencimento);
