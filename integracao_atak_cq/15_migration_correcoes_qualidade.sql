-- ═══════════════════════════════════════════════════════════════════════════
-- Migration 15 — Correções de qualidade: indexes, constraints, enum fix
-- ═══════════════════════════════════════════════════════════════════════════

-- ── INDEXES FALTANTES EM FK e COLUNAS FREQUENTES ─────────────────────────

-- atak_cargas_raw: composite para filtros de fornecedor + status
CREATE INDEX IF NOT EXISTS idx_atak_cargas_raw_forn_status
  ON atak_cargas_raw (fornecedor_codigo, status_validacao);

-- atak_embarques_raw: status para tracking
CREATE INDEX IF NOT EXISTS idx_atak_embarques_status
  ON atak_embarques_raw (status_embarque);

-- atak_producao_raw: compound setor + data para dashboard
CREATE INDEX IF NOT EXISTS idx_atak_producao_setor_data
  ON atak_producao_raw (setor, data_producao);

-- cq_fila_notificacao: para scheduler e event queries
CREATE INDEX IF NOT EXISTS idx_cq_fila_notif_evento_status
  ON cq_fila_notificacao (evento, status);

CREATE INDEX IF NOT EXISTS idx_cq_fila_notif_agendado_status
  ON cq_fila_notificacao (agendado_para, status);

-- cq_fornecedor_score_historico: status filtering
CREATE INDEX IF NOT EXISTS idx_cq_forn_score_hist_status
  ON cq_fornecedor_score_historico (status_fornecedor);

-- cq_lote_inspecao: supplier timeline queries
CREATE INDEX IF NOT EXISTS idx_cq_lote_insp_forn_data
  ON cq_lote_inspecao (fornecedor_codigo, data_inspecao);

-- cq_lote_produto: quality grade filter
CREATE INDEX IF NOT EXISTS idx_cq_lote_prod_class
  ON cq_lote_produto (classificacao);

-- cq_planos_acao: FK index
CREATE INDEX IF NOT EXISTS idx_cq_planos_acao_forn
  ON cq_planos_acao (fornecedor_id);

-- cq_validacao_divergencias: FK index
CREATE INDEX IF NOT EXISTS idx_cq_valid_diverg_forn
  ON cq_validacao_divergencias (fornecedor_id);


-- ── NOT NULL EM COLUNAS CRÍTICAS ─────────────────────────────────────────

-- Contadores em sync_log devem ser NOT NULL (default 0 já existe)
ALTER TABLE atak_sync_log ALTER COLUMN novos SET NOT NULL;
ALTER TABLE atak_sync_log ALTER COLUMN atualizados SET NOT NULL;
ALTER TABLE atak_sync_log ALTER COLUMN ignorados SET NOT NULL;
ALTER TABLE atak_sync_log ALTER COLUMN erros SET NOT NULL;

-- Fornecedor código no lote inspeção é obrigatório
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'cq_lote_inspecao' AND column_name = 'fornecedor_codigo'
    AND is_nullable = 'YES'
  ) THEN
    ALTER TABLE cq_lote_inspecao ALTER COLUMN fornecedor_codigo SET NOT NULL;
  END IF;
END $$;


-- ── NORMALIZAÇÃO DE ENUMS (decisao_motor: UPPER → lower) ────────────────

-- Adicionar coluna com valores lowercase se ainda em uppercase
DO $$
BEGIN
  -- Normalizar decisao_motor em registros_cq_inspecao
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'registros_cq_inspecao' AND column_name = 'decisao_motor'
  ) THEN
    UPDATE registros_cq_inspecao
    SET decisao_motor = LOWER(decisao_motor)
    WHERE decisao_motor <> LOWER(decisao_motor);
  END IF;

  -- Normalizar decisao_motor em cq_lote_inspecao
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'cq_lote_inspecao' AND column_name = 'decisao_motor'
  ) THEN
    UPDATE cq_lote_inspecao
    SET decisao_motor = LOWER(decisao_motor)
    WHERE decisao_motor <> LOWER(decisao_motor);
  END IF;
END $$;


-- ── FUNÇÃO UTILITÁRIA set_updated_at (referenciada mas nunca criada) ─────

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ── FIX: Motor decisão usar lowercase ────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_motor_decisao_inspecao()
RETURNS TRIGGER AS $$
DECLARE
  v_pct_criticos NUMERIC;
  v_pct_maiores  NUMERIC;
  v_pct_total    NUMERIC;
  v_decisao      TEXT;
  v_score        INTEGER;
  v_precisa_ream BOOLEAN := FALSE;
BEGIN
  -- Calcular percentuais (simplificado - depende do schema real)
  v_pct_criticos := COALESCE(NEW.pct_defeitos_criticos, 0);
  v_pct_maiores  := COALESCE(NEW.pct_defeitos_maiores, 0);
  v_pct_total    := COALESCE(NEW.pct_defeitos_total, 0);

  -- Motor de decisão
  IF v_pct_criticos >= 5 THEN
    v_decisao := 'bloqueado';
    v_score   := 0;
  ELSIF v_pct_criticos > 0 THEN
    v_decisao := 'em_analise_critica';
    v_score   := GREATEST(0, 100 - ROUND(v_pct_criticos * 25 + v_pct_maiores * 8));
    v_precisa_ream := TRUE;
  ELSIF v_pct_maiores > 10 THEN
    v_decisao := 'bloqueado';
    v_score   := 0;
  ELSIF v_pct_maiores > 5 THEN
    v_decisao := 'ressalva';
    v_score   := LEAST(79, GREATEST(0, 100 - ROUND(v_pct_maiores * 8)));
  ELSIF v_pct_total > 10 THEN
    v_decisao := 'ressalva';
    v_score   := GREATEST(0, 100 - ROUND(v_pct_total * 5));
  ELSE
    v_decisao := 'aprovado';
    v_score   := GREATEST(0, 100 - ROUND(v_pct_criticos * 25 + v_pct_maiores * 8 + v_pct_total * 2));
  END IF;

  NEW.decisao_motor := v_decisao;
  NEW.score_lote    := v_score;

  -- Auto-criar reamostragem se crítico
  IF v_precisa_ream AND EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_name = 'cq_reamostragem'
  ) THEN
    INSERT INTO cq_reamostragem (inspecao_id, motivo, status)
    VALUES (NEW.id, 'defeito_critico_detectado', 'pendente')
    ON CONFLICT DO NOTHING;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
