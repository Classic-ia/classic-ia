-- =====================================================================
-- Fase 4: Pneus - schema, views, trigger (parte 1/2)
-- =====================================================================
-- Pneus sao entidades individuais (nao passam pelas 3 camadas de estoque).
-- Cada pneu tem codigo_interno unico, etiqueta QR/barcode, status e
-- localizacao_atual. Historico completo de posicoes, movimentacoes,
-- recapagens e fotos.
-- =====================================================================

-- -------------------------------------------------------------
-- trans_pneus (ficha individual)
-- -------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS trans_pneu_codigo_seq START 1;

CREATE OR REPLACE FUNCTION trans_gerar_codigo_pneu()
RETURNS text
LANGUAGE sql
AS $$
  SELECT 'P-' || LPAD(nextval('trans_pneu_codigo_seq')::text, 5, '0');
$$;

CREATE TABLE IF NOT EXISTS trans_pneus (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo_interno text UNIQUE NOT NULL,
  marca text,
  modelo text,
  medida text,             -- ex: 295/80R22.5
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  dot text,                -- marca data DOT do pneu
  data_fabricacao date,
  data_entrada date NOT NULL DEFAULT CURRENT_DATE,
  km_inicial numeric(14,2) DEFAULT 0,
  vida_util_km numeric(14,2),
  sulco_inicial_mm numeric(6,2),
  sulco_atual_mm numeric(6,2),
  custo_aquisicao numeric(14,2),
  status text NOT NULL DEFAULT 'estoque'
    CHECK (status IN ('estoque','instalado','recapagem','sucata','reserva')),
  localizacao_atual text NOT NULL DEFAULT 'almoxarifado',
  km_acumulado numeric(14,2) NOT NULL DEFAULT 0,
  numero_recapagens int NOT NULL DEFAULT 0,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id)
);

CREATE INDEX idx_trans_pneus_status ON trans_pneus(status);
CREATE INDEX idx_trans_pneus_medida ON trans_pneus(medida);
CREATE INDEX idx_trans_pneus_fornecedor ON trans_pneus(fornecedor_id) WHERE fornecedor_id IS NOT NULL;

CREATE TRIGGER trans_pneus_updated
BEFORE UPDATE ON trans_pneus
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- -------------------------------------------------------------
-- trans_pneus_posicoes (historico + posicao atual)
-- removido_em IS NULL = posicao ativa
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_pneus_posicoes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pneu_id uuid NOT NULL REFERENCES trans_pneus(id) ON DELETE RESTRICT,
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id) ON DELETE RESTRICT,
  posicao text NOT NULL
    CHECK (posicao IN (
      'DE','DD',                          -- eixo 1 (direcional)
      'TE','TD',                          -- eixo 2 (truck)
      '3EE','3EI','3DE','3DI',            -- eixo 3
      '4EE','4EI','4DE','4DI',            -- eixo 4
      'estepe'
    )),
  km_instalado numeric(14,2) NOT NULL,
  km_removido numeric(14,2),
  motivo_remocao text,
  instalado_em timestamptz NOT NULL DEFAULT now(),
  removido_em timestamptz,
  instalado_por uuid REFERENCES auth.users(id),
  removido_por uuid REFERENCES auth.users(id),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_trans_pneus_pos_pneu ON trans_pneus_posicoes(pneu_id);
CREATE INDEX idx_trans_pneus_pos_veiculo ON trans_pneus_posicoes(veiculo_id);

-- UNIQUE parcial: apenas 1 posicao ativa por pneu
CREATE UNIQUE INDEX ux_trans_pneus_pos_ativa_pneu
  ON trans_pneus_posicoes(pneu_id) WHERE removido_em IS NULL;

-- UNIQUE parcial: apenas 1 pneu por veiculo+posicao ativa
CREATE UNIQUE INDEX ux_trans_pneus_pos_ativa_slot
  ON trans_pneus_posicoes(veiculo_id, posicao) WHERE removido_em IS NULL;

-- -------------------------------------------------------------
-- trans_pneus_recapagem (ciclos de recapagem)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_pneus_recapagem (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pneu_id uuid NOT NULL REFERENCES trans_pneus(id) ON DELETE RESTRICT,
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  data_envio timestamptz NOT NULL DEFAULT now(),
  data_retorno timestamptz,
  sulco_pre_mm numeric(6,2),
  sulco_pos_mm numeric(6,2),
  custo numeric(14,2),
  km_rodado_antes numeric(14,2),
  numero_recapagem int NOT NULL,
  status text NOT NULL DEFAULT 'enviado'
    CHECK (status IN ('enviado','retornado','reprovado')),
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id)
);

CREATE INDEX idx_trans_recap_pneu ON trans_pneus_recapagem(pneu_id);
CREATE INDEX idx_trans_recap_status ON trans_pneus_recapagem(status);

-- -------------------------------------------------------------
-- trans_pneus_movimentacoes (ledger imutavel)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_pneus_movimentacoes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pneu_id uuid NOT NULL REFERENCES trans_pneus(id) ON DELETE RESTRICT,
  tipo text NOT NULL
    CHECK (tipo IN (
      'entrada','instalacao','retirada',
      'envio_recap','retorno_recap','sucata',
      'ajuste','inventario','reserva','transferencia'
    )),
  usuario_id uuid NOT NULL REFERENCES auth.users(id),
  data_hora timestamptz NOT NULL DEFAULT now(),
  km_veiculo numeric(14,2),
  veiculo_id uuid REFERENCES trans_veiculos(id),
  posicao text,
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  recapagem_id uuid REFERENCES trans_pneus_recapagem(id),
  destino text,        -- usado em retirada: estoque/recapagem/sucata/troca_direta
  origem text,         -- usado em entrada
  motivo text,
  observacao text,
  latitude numeric(10,7),
  longitude numeric(10,7),
  foto_url text,
  status_anterior text,
  status_novo text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_trans_pneu_mov_pneu ON trans_pneus_movimentacoes(pneu_id, data_hora DESC);
CREATE INDEX idx_trans_pneu_mov_tipo ON trans_pneus_movimentacoes(tipo);
CREATE INDEX idx_trans_pneu_mov_veiculo ON trans_pneus_movimentacoes(veiculo_id) WHERE veiculo_id IS NOT NULL;

-- -------------------------------------------------------------
-- trans_pneus_fotos (pode existir sem movimentacao)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_pneus_fotos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pneu_id uuid NOT NULL REFERENCES trans_pneus(id) ON DELETE CASCADE,
  movimentacao_id uuid REFERENCES trans_pneus_movimentacoes(id) ON DELETE SET NULL,
  url text NOT NULL,
  tipo text CHECK (tipo IN ('instalacao','retirada','sucata','avaria','outro')),
  capturada_em timestamptz NOT NULL DEFAULT now(),
  usuario_id uuid REFERENCES auth.users(id)
);

CREATE INDEX idx_trans_pneus_fotos_pneu ON trans_pneus_fotos(pneu_id);

-- -------------------------------------------------------------
-- Views: km rodado + pneu full
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_pneus_km_rodado AS
SELECT
  p.id AS pneu_id,
  p.codigo_interno,
  -- soma das posicoes fechadas
  COALESCE((
    SELECT SUM(km_removido - km_instalado)
    FROM trans_pneus_posicoes
    WHERE pneu_id = p.id AND removido_em IS NOT NULL AND km_removido IS NOT NULL
  ), 0)
  -- + posicao ativa (ate o km_atual do veiculo)
  + COALESCE((
    SELECT COALESCE(v.km_atual, pos.km_instalado) - pos.km_instalado
    FROM trans_pneus_posicoes pos
    JOIN trans_veiculos v ON v.id = pos.veiculo_id
    WHERE pos.pneu_id = p.id AND pos.removido_em IS NULL
    LIMIT 1
  ), 0) AS km_rodado
FROM trans_pneus p;

CREATE OR REPLACE VIEW v_trans_pneus_full AS
SELECT
  p.*,
  f.razao_social AS fornecedor_razao,
  kr.km_rodado AS km_rodado_calc,
  pos.veiculo_id AS veiculo_atual_id,
  pos.posicao AS posicao_atual,
  pos.km_instalado AS km_instalado_atual,
  v.placa AS veiculo_placa,
  v.km_atual AS veiculo_km_atual,
  (SELECT COUNT(*) FROM trans_pneus_recapagem WHERE pneu_id = p.id) AS total_recapagens_hist,
  (SELECT MAX(data_hora) FROM trans_pneus_movimentacoes WHERE pneu_id = p.id) AS ultima_mov
FROM trans_pneus p
LEFT JOIN trans_fornecedores f ON f.id = p.fornecedor_id
LEFT JOIN v_trans_pneus_km_rodado kr ON kr.pneu_id = p.id
LEFT JOIN trans_pneus_posicoes pos ON pos.pneu_id = p.id AND pos.removido_em IS NULL
LEFT JOIN trans_veiculos v ON v.id = pos.veiculo_id;

-- -------------------------------------------------------------
-- RLS
-- -------------------------------------------------------------
ALTER TABLE trans_pneus ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_pneus_posicoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_pneus_recapagem ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_pneus_movimentacoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_pneus_fotos ENABLE ROW LEVEL SECURITY;

CREATE POLICY trans_pneus_sel ON trans_pneus FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pneus_ins ON trans_pneus FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_pneus_upd ON trans_pneus FOR UPDATE TO authenticated USING (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
) WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_pneus_del ON trans_pneus FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_pneus_pos_sel ON trans_pneus_posicoes FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pneus_pos_admin ON trans_pneus_posicoes FOR ALL TO authenticated
  USING (trans_is_admin()) WITH CHECK (trans_is_admin());

CREATE POLICY trans_recap_sel ON trans_pneus_recapagem FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_recap_admin ON trans_pneus_recapagem FOR ALL TO authenticated
  USING (trans_is_admin()) WITH CHECK (trans_is_admin());

CREATE POLICY trans_pneu_mov_sel ON trans_pneus_movimentacoes FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pneu_mov_ins ON trans_pneus_movimentacoes FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_pneu_mov_del ON trans_pneus_movimentacoes FOR DELETE TO authenticated USING (false);

CREATE POLICY trans_pneus_fotos_sel ON trans_pneus_fotos FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pneus_fotos_ins ON trans_pneus_fotos FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife','motorista')
);
CREATE POLICY trans_pneus_fotos_del ON trans_pneus_fotos FOR DELETE TO authenticated USING (trans_is_admin());
