-- ══════════════════════════════════════════════════════════
-- cq_alertas_qualidade — Alertas de qualidade (Omaso → Fila CQ)
-- Gerados automaticamente pelo Dashboard Omaso quando
-- fornecedores atingem %C > limiar configurado
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_alertas_qualidade (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo            TEXT NOT NULL DEFAULT 'fornecedor_critico',
  gravidade       TEXT NOT NULL CHECK (gravidade IN ('critica','atencao','informativa')),
  status          TEXT NOT NULL DEFAULT 'aberto' CHECK (status IN ('aberto','visto','em_tratamento','resolvido')),

  -- Dados do fornecedor
  frigorifico     TEXT NOT NULL,
  numero_pedido   TEXT,
  numero_carga    TEXT,

  -- Métricas no momento do alerta
  total_pecas     INTEGER DEFAULT 0,
  pcs_a           INTEGER DEFAULT 0,
  pcs_b           INTEGER DEFAULT 0,
  pcs_c           INTEGER DEFAULT 0,
  pct_a           NUMERIC(5,2) DEFAULT 0,
  pct_b           NUMERIC(5,2) DEFAULT 0,
  pct_c           NUMERIC(5,2) DEFAULT 0,

  -- Detalhes
  mensagem        TEXT NOT NULL,
  acao_sugerida   TEXT,
  produto         TEXT DEFAULT 'Omaso Bovino',
  origem          TEXT DEFAULT 'dashboard_omaso',

  -- Quem criou / resolveu
  criado_por      TEXT,
  resolvido_por   TEXT,
  data_resolucao  TIMESTAMPTZ,
  obs_resolucao   TEXT,

  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_alertas_qual_status ON cq_alertas_qualidade(status);
CREATE INDEX IF NOT EXISTS idx_alertas_qual_frigo ON cq_alertas_qualidade(frigorifico);
CREATE INDEX IF NOT EXISTS idx_alertas_qual_criado ON cq_alertas_qualidade(criado_em DESC);

-- RLS
ALTER TABLE cq_alertas_qualidade ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth read alertas" ON cq_alertas_qualidade FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth insert alertas" ON cq_alertas_qualidade FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "auth update alertas" ON cq_alertas_qualidade FOR UPDATE TO authenticated USING (true);
