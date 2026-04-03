-- ============================================================
-- Classic CQ — Módulo Controle de Estoque via XML NFe
-- Execute este SQL no Supabase SQL Editor
-- ============================================================

-- 1. Categorias de produto (equivale às abas da planilha)
CREATE TABLE IF NOT EXISTS estoque_categorias (
  id         UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome       TEXT NOT NULL UNIQUE,
  unidade    TEXT DEFAULT 'KG',
  saldo_inicial NUMERIC DEFAULT 0,
  ativo      BOOLEAN DEFAULT true,
  criado_em  TIMESTAMPTZ DEFAULT now()
);

-- 2. Mapeamento produto NFe → categoria
CREATE TABLE IF NOT EXISTS estoque_mapeamento (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  chave         TEXT NOT NULL,
  tipo_chave    TEXT DEFAULT 'descricao' CHECK (tipo_chave IN ('descricao','ncm')),
  categoria_id  UUID NOT NULL REFERENCES estoque_categorias(id) ON DELETE CASCADE,
  criado_em     TIMESTAMPTZ DEFAULT now(),
  UNIQUE(chave, tipo_chave)
);

-- 3. Movimentações (cada item de cada NFe importada)
CREATE TABLE IF NOT EXISTS estoque_movimentacoes (
  id                UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  chave_nfe         TEXT NOT NULL,
  numero_nf         TEXT NOT NULL,
  data_emissao      DATE NOT NULL,
  tipo              TEXT NOT NULL CHECK (tipo IN ('entrada','saida')),
  fornecedor_cliente TEXT NOT NULL,
  cnpj              TEXT,
  categoria_id      UUID REFERENCES estoque_categorias(id),
  produto_xml       TEXT NOT NULL,
  ncm               TEXT,
  cfop              TEXT,
  unidade           TEXT,
  quantidade        NUMERIC NOT NULL DEFAULT 0,
  valor_unitario    NUMERIC DEFAULT 0,
  valor_total       NUMERIC DEFAULT 0,
  icms              NUMERIC DEFAULT 0,
  pis               NUMERIC DEFAULT 0,
  cofins            NUMERIC DEFAULT 0,
  compra_liquida    NUMERIC DEFAULT 0,
  importado_em      TIMESTAMPTZ DEFAULT now(),
  importado_por     UUID,
  UNIQUE(chave_nfe, produto_xml)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_mov_data ON estoque_movimentacoes(data_emissao);
CREATE INDEX IF NOT EXISTS idx_mov_tipo ON estoque_movimentacoes(tipo);
CREATE INDEX IF NOT EXISTS idx_mov_cat  ON estoque_movimentacoes(categoria_id);
CREATE INDEX IF NOT EXISTS idx_mov_nfe  ON estoque_movimentacoes(chave_nfe);
CREATE INDEX IF NOT EXISTS idx_map_cat  ON estoque_mapeamento(categoria_id);

-- 4. View de saldo por categoria
CREATE OR REPLACE VIEW vw_estoque_saldo AS
SELECT
  c.id,
  c.nome,
  c.unidade,
  c.saldo_inicial,
  c.estoque_minimo,
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.quantidade ELSE 0 END), 0) AS total_entrada,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida'   THEN m.quantidade ELSE 0 END), 0) AS total_saida,
  c.saldo_inicial
    + COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.quantidade ELSE 0 END), 0)
    - COALESCE(SUM(CASE WHEN m.tipo = 'saida'   THEN m.quantidade ELSE 0 END), 0) AS saldo_atual,
  COALESCE(SUM(CASE WHEN m.tipo = 'entrada' THEN m.valor_total ELSE 0 END), 0) AS valor_entradas,
  COALESCE(SUM(CASE WHEN m.tipo = 'saida'   THEN m.valor_total ELSE 0 END), 0) AS valor_saidas,
  COUNT(DISTINCT m.chave_nfe) AS total_nfs
FROM estoque_categorias c
LEFT JOIN estoque_movimentacoes m ON m.categoria_id = c.id
WHERE c.ativo = true
GROUP BY c.id, c.nome, c.unidade, c.saldo_inicial, c.estoque_minimo
ORDER BY c.nome;

-- 5. View de evolução mensal
CREATE OR REPLACE VIEW vw_estoque_mensal AS
SELECT
  date_trunc('month', m.data_emissao)::date AS mes,
  m.tipo,
  COALESCE(SUM(m.quantidade), 0) AS qtd_total,
  COALESCE(SUM(m.valor_total), 0) AS valor_total,
  COUNT(*) AS num_itens
FROM estoque_movimentacoes m
GROUP BY date_trunc('month', m.data_emissao), m.tipo
ORDER BY mes;

-- 6. RLS (Row Level Security)
ALTER TABLE estoque_categorias     ENABLE ROW LEVEL SECURITY;
ALTER TABLE estoque_mapeamento     ENABLE ROW LEVEL SECURITY;
ALTER TABLE estoque_movimentacoes  ENABLE ROW LEVEL SECURITY;

-- Políticas: permitir leitura para autenticados, escrita para autenticados
CREATE POLICY "estoque_cat_select" ON estoque_categorias FOR SELECT TO authenticated USING (true);
CREATE POLICY "estoque_cat_insert" ON estoque_categorias FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "estoque_cat_update" ON estoque_categorias FOR UPDATE TO authenticated USING (true);
CREATE POLICY "estoque_cat_delete" ON estoque_categorias FOR DELETE TO authenticated USING (true);

CREATE POLICY "estoque_map_select" ON estoque_mapeamento FOR SELECT TO authenticated USING (true);
CREATE POLICY "estoque_map_insert" ON estoque_mapeamento FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "estoque_map_update" ON estoque_mapeamento FOR UPDATE TO authenticated USING (true);
CREATE POLICY "estoque_map_delete" ON estoque_mapeamento FOR DELETE TO authenticated USING (true);

CREATE POLICY "estoque_mov_select" ON estoque_movimentacoes FOR SELECT TO authenticated USING (true);
CREATE POLICY "estoque_mov_insert" ON estoque_movimentacoes FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "estoque_mov_update" ON estoque_movimentacoes FOR UPDATE TO authenticated USING (true);
CREATE POLICY "estoque_mov_delete" ON estoque_movimentacoes FOR DELETE TO authenticated USING (true);

-- Anon também pode ler (para views públicas se necessário)
CREATE POLICY "estoque_cat_anon_select" ON estoque_categorias FOR SELECT TO anon USING (true);
CREATE POLICY "estoque_map_anon_select" ON estoque_mapeamento FOR SELECT TO anon USING (true);
CREATE POLICY "estoque_mov_anon_select" ON estoque_movimentacoes FOR SELECT TO anon USING (true);

-- 7. Pré-população das categorias (abas da planilha)
INSERT INTO estoque_categorias (nome, unidade) VALUES
  ('CARNE INDUSTRIAL', 'KG'),
  ('Couro Equideo', 'PC'),
  ('SAPATOS', 'PRS'),
  ('CORACAO', 'KG'),
  ('COURO SUINO', 'KG'),
  ('CULATRA', 'KG'),
  ('PULMAO', 'KG'),
  ('GLANDULA MAMARIA', 'KG'),
  ('GLOTE', 'KG'),
  ('BACO', 'KG'),
  ('UBERE', 'KG'),
  ('Ligamento', 'KG'),
  ('CALCULO BILIAR', 'KG'),
  ('ORELHA SUINA', 'KG'),
  ('MEMBRANA', 'KG'),
  ('BUCHAO', 'KG'),
  ('Bucho suino', 'KG'),
  ('DESPOJO', 'KG'),
  ('COLMEIA', 'KG'),
  ('Traqueia', 'KG'),
  ('CAPA DE VERGALHO', 'KG'),
  ('SEBO', 'KG'),
  ('Vergalho Suino', 'KG'),
  ('Vergalho Bovino', 'KG'),
  ('Bexiga', 'KG'),
  ('AORTA', 'KG'),
  ('Esofago', 'KG'),
  ('Tendao', 'KG'),
  ('Orelha', 'KG'),
  ('Bucho', 'KG'),
  ('Couro verde', 'PC'),
  ('Semi', 'PC'),
  ('Wet Blue', 'PC'),
  ('Acabado', 'PC'),
  ('Raspa', 'KG'),
  ('SAL', 'KG'),
  ('EMBALAGEM', 'UN'),
  ('Fretes Vendas', 'UN'),
  ('Servicos', 'UN')
ON CONFLICT (nome) DO NOTHING;
