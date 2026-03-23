-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 14: Leitura de Etiqueta / Scanner + Auto-preenchimento
--
-- Estrutura para scan de etiqueta (câmera mobile / scanner desktop):
--   1. Campo de busca: codigo_etiqueta como chave
--   2. Log de leituras (auditoria)
--   3. Log de alterações manuais (quando operador muda dado pós-leitura)
--   4. View de busca unificada (lotes recebidos + cargas + movimentações)
--
-- Rodar APÓS 13_migration_motor_decisao_iqf.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. EVOLUÇÃO: registros_cq_inspecao — campo de etiqueta escaneada
-- O campo 'etiqueta' já existe. Adicionamos metadados de scan.
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS scan_origem       TEXT CHECK (scan_origem IN ('camera','scanner','manual','api')),
  ADD COLUMN IF NOT EXISTS scan_codigo_raw   TEXT,
  ADD COLUMN IF NOT EXISTS scan_timestamp    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS scan_auto_preenchido BOOLEAN DEFAULT FALSE;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TABELA: Log de leituras de etiqueta (auditoria)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_scan_log (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  usuario_email    TEXT NOT NULL,
  usuario_nome     TEXT,

  -- O que foi lido
  codigo_lido      TEXT NOT NULL,
  tipo_codigo      TEXT CHECK (tipo_codigo IN ('barcode','qrcode','manual','rfid')),
  scan_origem      TEXT CHECK (scan_origem IN ('camera','scanner','manual')),

  -- Resultado
  encontrado       BOOLEAN NOT NULL DEFAULT FALSE,
  tabela_origem    TEXT,    -- ex: 'atak_cargas_raw', 'atak_movimentacoes'
  registro_id      TEXT,    -- ID do registro encontrado

  -- Dados auto-preenchidos (snapshot do que foi preenchido)
  dados_preenchidos JSONB,

  -- Vínculo
  inspecao_id      UUID REFERENCES registros_cq_inspecao(id),

  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scan_log_codigo ON cq_scan_log(codigo_lido);
CREATE INDEX IF NOT EXISTS idx_scan_log_usuario ON cq_scan_log(usuario_email);
CREATE INDEX IF NOT EXISTS idx_scan_log_criado ON cq_scan_log(criado_em DESC);


-- ════════════════════════════════════════════════════════════════════════════
-- 3. TABELA: Log de alterações pós-scan (justificativa obrigatória)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_scan_alteracao_log (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inspecao_id      UUID NOT NULL REFERENCES registros_cq_inspecao(id),
  scan_log_id      UUID REFERENCES cq_scan_log(id),
  usuario_email    TEXT NOT NULL,

  campo_alterado   TEXT NOT NULL,    -- ex: 'fornecedor', 'produto', 'lote_atak'
  valor_original   TEXT,             -- valor do auto-preenchimento
  valor_novo       TEXT NOT NULL,    -- valor digitado pelo operador
  justificativa    TEXT NOT NULL,    -- obrigatória

  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scan_alt_inspecao ON cq_scan_alteracao_log(inspecao_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 4. VIEW: Busca unificada por etiqueta/documento
-- Consolida todas as fontes de dados para auto-preenchimento
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_busca_etiqueta AS

-- Fonte 1: atak_cargas_raw (cargas recebidas via API)
SELECT
  COALESCE(c.numero_documento, c.numero_pcr) AS codigo_busca,
  'atak_cargas_raw' AS tabela_origem,
  c.id::TEXT AS registro_id,
  c.fornecedor_nome AS fornecedor,
  c.fornecedor_codigo,
  c.produto,
  c.qtd_frigo AS quantidade,
  c.data_chegada AS data_recebimento,
  c.numero_carga AS pedido_carga,
  c.motorista,
  c.placa_veiculo AS placa,
  c.transportadora
FROM atak_cargas_raw c
WHERE c.numero_documento IS NOT NULL OR c.numero_pcr IS NOT NULL

UNION ALL

-- Fonte 2: atak_movimentacoes (planilha importada)
SELECT
  m.documento AS codigo_busca,
  'atak_movimentacoes' AS tabela_origem,
  m.id::TEXT AS registro_id,
  m.participante_nome AS fornecedor,
  m.participante_codigo AS fornecedor_codigo,
  COALESCE(p.nome, m.id_atak_produto) AS produto,
  m.quantidade::INTEGER AS quantidade,
  m.data_movimentacao AS data_recebimento,
  NULL AS pedido_carga,
  NULL AS motorista,
  NULL AS placa,
  NULL AS transportadora
FROM atak_movimentacoes m
LEFT JOIN cq_produtos p ON p.id = m.produto_id
WHERE m.tipo_movimentacao = 'Entrada'

UNION ALL

-- Fonte 3: registros_cq_inspecao existentes (para lotes já inspecionados)
SELECT
  i.etiqueta AS codigo_busca,
  'registros_cq_inspecao' AS tabela_origem,
  i.id::TEXT AS registro_id,
  i.fornecedor,
  i.fornecedor_codigo,
  i.produto,
  i.quantidade_lote AS quantidade,
  i.data_inspecao AS data_recebimento,
  i.lote_atak AS pedido_carga,
  i.motorista,
  i.placa_caminhao AS placa,
  i.transportadora
FROM registros_cq_inspecao i
WHERE i.etiqueta IS NOT NULL;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. FUNCTION: Buscar por etiqueta (chamada via RPC do frontend)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_buscar_etiqueta(p_codigo TEXT)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
  v_result JSONB;
BEGIN
  -- Buscar match exato primeiro
  SELECT jsonb_build_object(
    'encontrado', true,
    'tabela_origem', b.tabela_origem,
    'registro_id', b.registro_id,
    'fornecedor', b.fornecedor,
    'fornecedor_codigo', b.fornecedor_codigo,
    'produto', b.produto,
    'quantidade', b.quantidade,
    'data_recebimento', b.data_recebimento,
    'pedido_carga', b.pedido_carga,
    'motorista', b.motorista,
    'placa', b.placa,
    'transportadora', b.transportadora
  ) INTO v_result
  FROM vw_busca_etiqueta b
  WHERE b.codigo_busca = p_codigo
  LIMIT 1;

  IF v_result IS NOT NULL THEN
    RETURN v_result;
  END IF;

  -- Buscar match parcial (LIKE)
  SELECT jsonb_build_object(
    'encontrado', true,
    'match_parcial', true,
    'tabela_origem', b.tabela_origem,
    'registro_id', b.registro_id,
    'fornecedor', b.fornecedor,
    'fornecedor_codigo', b.fornecedor_codigo,
    'produto', b.produto,
    'quantidade', b.quantidade,
    'data_recebimento', b.data_recebimento,
    'pedido_carga', b.pedido_carga,
    'motorista', b.motorista,
    'placa', b.placa,
    'transportadora', b.transportadora
  ) INTO v_result
  FROM vw_busca_etiqueta b
  WHERE b.codigo_busca ILIKE '%' || p_codigo || '%'
  LIMIT 1;

  RETURN COALESCE(v_result, jsonb_build_object('encontrado', false, 'codigo', p_codigo));
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_scan_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_scan_alteracao_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_scan_log" ON cq_scan_log FOR SELECT USING (true);
CREATE POLICY "all_scan_log" ON cq_scan_log FOR ALL USING (true);
CREATE POLICY "read_scan_alt" ON cq_scan_alteracao_log FOR SELECT USING (true);
CREATE POLICY "all_scan_alt" ON cq_scan_alteracao_log FOR ALL USING (true);


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRACAO 14 — Scan de Etiqueta + Auto-preenchimento';
  RAISE NOTICE '  ';
  RAISE NOTICE '  TABELAS:';
  RAISE NOTICE '    ✓ cq_scan_log (log de leituras)';
  RAISE NOTICE '    ✓ cq_scan_alteracao_log (alteracoes pos-scan)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS:';
  RAISE NOTICE '    ✓ vw_busca_etiqueta (3 fontes unificadas)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  FUNCTIONS:';
  RAISE NOTICE '    ✓ fn_buscar_etiqueta (RPC para frontend)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
