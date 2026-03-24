-- ══════════════════════════════════════════════════════════════
-- CLASSIC CQ — SQL COMPLEMENTAR (executar após o principal)
-- Supabase → SQL Editor → New Query → Run
-- ══════════════════════════════════════════════════════════════

-- ── 1. RECEBIMENTOS ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cq_recebimentos (
  id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  id_recebimento  TEXT    NOT NULL UNIQUE,    -- REC-AAMMDD-HHMM-XXX
  fornecedor      TEXT    NOT NULL,
  fornecedor_cod  TEXT,
  produto         TEXT    NOT NULL,
  lote_atak       TEXT    NOT NULL,
  etiqueta        TEXT    NOT NULL,
  quantidade_lote INTEGER NOT NULL,
  peso_kg         NUMERIC(10,2),
  temperatura     NUMERIC(5,1),
  placa_caminhao  TEXT,
  motorista       TEXT,
  transportadora  TEXT,
  foto_etiqueta_url TEXT,
  foto_carga_url  TEXT,
  observacoes     TEXT,
  status          TEXT    DEFAULT 'pendente_inspecao'
                    CHECK (status IN ('pendente_inspecao','inspecionado','cancelado')),
  usuario_nome    TEXT,
  usuario_email   TEXT,
  latitude        NUMERIC(10,7),
  longitude       NUMERIC(10,7),
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rec_lote   ON cq_recebimentos(lote_atak);
CREATE INDEX IF NOT EXISTS idx_rec_forn   ON cq_recebimentos(fornecedor);
CREATE INDEX IF NOT EXISTS idx_rec_status ON cq_recebimentos(status);
CREATE INDEX IF NOT EXISTS idx_rec_dt     ON cq_recebimentos(criado_em DESC);

ALTER TABLE cq_recebimentos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "rec_all" ON cq_recebimentos;
CREATE POLICY "rec_all" ON cq_recebimentos FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- ── 2. PLANOS DE AÇÃO / COBRANÇA ─────────────────────────────
CREATE TABLE IF NOT EXISTS cq_planos_acao (
  id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  fornecedor      TEXT    NOT NULL,
  fornecedor_cod  TEXT,
  lote_atak       TEXT,
  data_acao       DATE,
  problema        TEXT    NOT NULL,
  recomendacao    TEXT    NOT NULL,
  status          TEXT    DEFAULT 'aberto'
                    CHECK (status IN ('aberto','comunicado','em_tratativa','resolvido')),
  retorno_fornecedor TEXT,
  usuario_nome    TEXT,
  usuario_email   TEXT,
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plano_forn   ON cq_planos_acao(fornecedor);
CREATE INDEX IF NOT EXISTS idx_plano_status ON cq_planos_acao(status);
CREATE INDEX IF NOT EXISTS idx_plano_dt     ON cq_planos_acao(criado_em DESC);

ALTER TABLE cq_planos_acao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "plano_all" ON cq_planos_acao;
CREATE POLICY "plano_all" ON cq_planos_acao FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- ── 3. CAMPOS ADICIONAIS NA INSPEÇÃO ────────────────────────
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS foto_defeito_url TEXT;

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS video_evidencia_url TEXT;

-- ── 4. VIEW: RECEBIMENTOS PENDENTES ──────────────────────────
CREATE OR REPLACE VIEW vw_recebimentos_pendentes AS
SELECT
  r.id_recebimento, r.fornecedor, r.produto,
  r.lote_atak, r.etiqueta, r.quantidade_lote,
  r.temperatura, r.placa_caminhao, r.motorista,
  r.criado_em,
  EXTRACT(EPOCH FROM (NOW() - r.criado_em))/3600 AS horas_aguardando
FROM cq_recebimentos r
WHERE r.status = 'pendente_inspecao'
ORDER BY r.criado_em DESC;

-- ── 5. VIEW: FORNECEDORES COM PLANOS ABERTOS ─────────────────
CREATE OR REPLACE VIEW vw_fornecedores_criticos AS
SELECT
  p.fornecedor,
  COUNT(*) FILTER (WHERE p.status = 'aberto')       AS planos_abertos,
  COUNT(*) FILTER (WHERE p.status = 'em_tratativa')  AS em_tratativa,
  COUNT(*) FILTER (WHERE p.status = 'resolvido')     AS resolvidos,
  MAX(p.criado_em)                                    AS ultimo_plano
FROM cq_planos_acao p
GROUP BY p.fornecedor
HAVING COUNT(*) FILTER (WHERE p.status IN ('aberto','em_tratativa')) > 0
ORDER BY planos_abertos DESC;

-- ── VERIFICAÇÃO ───────────────────────────────────────────────
SELECT tablename, 'tabela' tipo FROM pg_tables
WHERE schemaname='public' AND tablename IN ('cq_recebimentos','cq_planos_acao')
UNION ALL
SELECT viewname, 'view' FROM pg_views
WHERE schemaname='public' AND viewname IN ('vw_recebimentos_pendentes','vw_fornecedores_criticos');
