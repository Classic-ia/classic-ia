-- ══════════════════════════════════════════════════════════════
-- Tabela: cq_cargas
-- Importação das planilhas de cargas dos motoristas (aba FECHAMENTO)
-- Rodar no SQL Editor do Supabase
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_cargas (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,

  -- Identificação da viagem
  motorista       TEXT,
  data_viagem     DATE,
  data_coleta     DATE NOT NULL,
  cidade          TEXT,
  frigorifico     TEXT,
  tipo_conservacao TEXT,          -- CONGELADO / SALGADO

  -- Contagem
  qtd_frigo       INTEGER DEFAULT 0,
  qtd_classic     INTEGER DEFAULT 0,
  diferenca       INTEGER DEFAULT 0,

  -- Defeitos (condições do produto)
  rasgados        INTEGER DEFAULT 0,
  furados         INTEGER DEFAULT 0,
  pelando         INTEGER DEFAULT 0,
  sem_folha       INTEGER DEFAULT 0,
  sujos           INTEGER DEFAULT 0,
  pedacos         INTEGER DEFAULT 0,
  corte_irregular INTEGER DEFAULT 0,
  folha_extra     INTEGER DEFAULT 0,
  total_problemas INTEGER DEFAULT 0,

  -- Classificação
  porcentagem     NUMERIC(8,4) DEFAULT 0,
  classificacao   TEXT,          -- A / B / C

  -- Status
  situacao        TEXT,          -- CONCLUIDO, etc.
  observacoes     TEXT,
  numero_pcr      TEXT UNIQUE,   -- PCR do lote (evita duplicatas)

  -- Metadata
  importado_por   UUID REFERENCES cq_usuarios(id),
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_cq_cargas_motorista ON cq_cargas(motorista);
CREATE INDEX IF NOT EXISTS idx_cq_cargas_frigorifico ON cq_cargas(frigorifico);
CREATE INDEX IF NOT EXISTS idx_cq_cargas_data_coleta ON cq_cargas(data_coleta);
CREATE INDEX IF NOT EXISTS idx_cq_cargas_pcr ON cq_cargas(numero_pcr);
CREATE INDEX IF NOT EXISTS idx_cq_cargas_classificacao ON cq_cargas(classificacao);

-- RLS
ALTER TABLE cq_cargas ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cq_cargas_select" ON cq_cargas
  FOR SELECT USING (true);

CREATE POLICY "cq_cargas_insert" ON cq_cargas
  FOR INSERT WITH CHECK (true);

CREATE POLICY "cq_cargas_update" ON cq_cargas
  FOR UPDATE USING (true);

-- ── VERIFICAÇÃO ──────────────────────────────────────────────
SELECT 'cq_cargas' AS tabela, COUNT(*) AS rows FROM cq_cargas;
