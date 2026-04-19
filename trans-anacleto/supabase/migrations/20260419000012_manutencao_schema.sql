-- =====================================================================
-- Fase 5: Manutencao da frota - schema + views
-- =====================================================================
-- Tipos:
--   preventiva: controlada por plano (intervalo_km/intervalo_dias)
--   corretiva: por ocorrencia (vazamento, quebra, etc.)
-- Integracao:
--   itens consumidos geram baixa real no estoque (novo lote / usado condicao)
--   acoes em pneus (instalar/retirar/recap/sucata) chamam RPCs de pneus
-- =====================================================================

-- -------------------------------------------------------------
-- trans_planos_manutencao (templates reutilizaveis)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_planos_manutencao (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text UNIQUE NOT NULL,
  tipo_servico text NOT NULL
    CHECK (tipo_servico IN (
      'troca_oleo','filtro_oleo','filtro_comb','filtro_ar',
      'lubrificacao','revisao','alinhamento','balanceamento',
      'correia','freio','suspensao','eletrica','outro'
    )),
  intervalo_km numeric(14,2),
  intervalo_dias int,
  ativo boolean NOT NULL DEFAULT true,
  veiculo_tipo_aplicavel text[] NOT NULL DEFAULT '{}', -- ['cavalo','truck','carreta'...]
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id),
  CONSTRAINT ck_intervalo_pelo_menos_um CHECK (
    intervalo_km IS NOT NULL OR intervalo_dias IS NOT NULL
  )
);

CREATE INDEX IF NOT EXISTS idx_trans_planos_ativo ON trans_planos_manutencao(ativo);

DROP TRIGGER IF EXISTS tg_trans_planos_manutencao_upd ON trans_planos_manutencao;
CREATE TRIGGER tg_trans_planos_manutencao_upd
BEFORE UPDATE ON trans_planos_manutencao
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- -------------------------------------------------------------
-- trans_planos_veiculo (plano x veiculo: ultima execucao)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_planos_veiculo (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  plano_id uuid NOT NULL REFERENCES trans_planos_manutencao(id) ON DELETE CASCADE,
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id) ON DELETE CASCADE,
  ultima_execucao_km numeric(14,2),
  ultima_execucao_data date,
  ativo boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id),
  UNIQUE (plano_id, veiculo_id)
);

CREATE INDEX IF NOT EXISTS idx_trans_planos_veic_veic ON trans_planos_veiculo(veiculo_id);

DROP TRIGGER IF EXISTS tg_trans_planos_veiculo_upd ON trans_planos_veiculo;
CREATE TRIGGER tg_trans_planos_veiculo_upd
BEFORE UPDATE ON trans_planos_veiculo
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- -------------------------------------------------------------
-- trans_manutencoes (ordens de servico)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_manutencoes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id),
  tipo text NOT NULL CHECK (tipo IN ('preventiva','corretiva')),
  servico text NOT NULL,
  status text NOT NULL DEFAULT 'aberta'
    CHECK (status IN ('aberta','em_andamento','concluida','cancelada')),
  data_abertura timestamptz NOT NULL DEFAULT now(),
  data_execucao timestamptz,
  data_encerramento timestamptz,
  km_abertura numeric(14,2) NOT NULL,
  km_execucao numeric(14,2),
  oficina text,
  executado_por text,
  observacao text,
  nota_fiscal_id uuid, -- FK futura para trans_notas_fiscais (servicos)
  proximo_km numeric(14,2),
  proxima_data date,
  custo_total_calc numeric(14,2) NOT NULL DEFAULT 0, -- recalculado via trigger dos itens
  motivo_cancelamento text,
  cancelada_em timestamptz,
  cancelada_por uuid REFERENCES auth.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_trans_manut_veic ON trans_manutencoes(veiculo_id);
CREATE INDEX IF NOT EXISTS idx_trans_manut_status ON trans_manutencoes(status);
CREATE INDEX IF NOT EXISTS idx_trans_manut_data ON trans_manutencoes(data_abertura);
CREATE INDEX IF NOT EXISTS idx_trans_manut_tipo ON trans_manutencoes(tipo);

DROP TRIGGER IF EXISTS tg_trans_manutencoes_upd ON trans_manutencoes;
CREATE TRIGGER tg_trans_manutencoes_upd
BEFORE UPDATE ON trans_manutencoes
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- -------------------------------------------------------------
-- trans_manutencao_itens (peças/insumos consumidos)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_manutencao_itens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  manutencao_id uuid NOT NULL REFERENCES trans_manutencoes(id) ON DELETE CASCADE,
  produto_id uuid NOT NULL REFERENCES trans_produtos(id),
  modalidade text NOT NULL CHECK (modalidade IN ('novo','usado')),
  lote_id uuid REFERENCES trans_lotes(id),
  condicao text CHECK (condicao IN ('A','B','C')),
  quantidade numeric(14,3) NOT NULL CHECK (quantidade > 0),
  custo_unitario numeric(14,4),
  custo_total numeric(14,2) GENERATED ALWAYS AS (quantidade * COALESCE(custo_unitario,0)) STORED,
  movimentacao_estoque_id uuid REFERENCES trans_estoque_movimentacao(id),
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  CONSTRAINT ck_mi_mut CHECK (
    (modalidade = 'novo' AND lote_id IS NOT NULL AND condicao IS NULL) OR
    (modalidade = 'usado' AND condicao IS NOT NULL AND lote_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_trans_mi_manut ON trans_manutencao_itens(manutencao_id);
CREATE INDEX IF NOT EXISTS idx_trans_mi_prod ON trans_manutencao_itens(produto_id);

-- -------------------------------------------------------------
-- trans_manutencao_pneus (acoes em pneus dentro da manutencao)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_manutencao_pneus (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  manutencao_id uuid NOT NULL REFERENCES trans_manutencoes(id) ON DELETE CASCADE,
  pneu_id uuid NOT NULL REFERENCES trans_pneus(id),
  acao text NOT NULL
    CHECK (acao IN ('instalar','retirar','rodizio','enviar_recap','retornar_recap','sucata')),
  movimentacao_pneu_id uuid REFERENCES trans_pneus_movimentacoes(id),
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_trans_mp_manut ON trans_manutencao_pneus(manutencao_id);
CREATE INDEX IF NOT EXISTS idx_trans_mp_pneu ON trans_manutencao_pneus(pneu_id);

-- -------------------------------------------------------------
-- trans_manutencao_anexos (fotos, NFS-e, laudos)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_manutencao_anexos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  manutencao_id uuid NOT NULL REFERENCES trans_manutencoes(id) ON DELETE CASCADE,
  url text NOT NULL,
  tipo text NOT NULL CHECK (tipo IN ('foto','nota','laudo','comprovante','outro')),
  descricao text,
  capturada_em timestamptz NOT NULL DEFAULT now(),
  usuario_id uuid REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_trans_ma_manut ON trans_manutencao_anexos(manutencao_id);

-- -------------------------------------------------------------
-- Trigger: recalcular custo_total_calc ao inserir/atualizar itens
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_tg_recalc_custo_manutencao()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE v_id uuid;
BEGIN
  v_id := COALESCE(NEW.manutencao_id, OLD.manutencao_id);
  UPDATE trans_manutencoes SET custo_total_calc = (
    SELECT COALESCE(SUM(custo_total), 0)
    FROM trans_manutencao_itens WHERE manutencao_id = v_id
  ) WHERE id = v_id;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS tg_trans_mi_recalc ON trans_manutencao_itens;
CREATE TRIGGER tg_trans_mi_recalc
AFTER INSERT OR UPDATE OR DELETE ON trans_manutencao_itens
FOR EACH ROW EXECUTE FUNCTION trans_tg_recalc_custo_manutencao();

-- -------------------------------------------------------------
-- View: planos pendentes / vencidos por veiculo
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_manutencoes_pendentes AS
SELECT
  pv.id AS plano_veiculo_id,
  pv.plano_id,
  pv.veiculo_id,
  pm.nome AS plano_nome,
  pm.tipo_servico,
  pm.intervalo_km,
  pm.intervalo_dias,
  v.placa,
  v.modelo AS veiculo_modelo,
  v.tipo AS veiculo_tipo,
  v.km_atual,
  pv.ultima_execucao_km,
  pv.ultima_execucao_data,
  -- proximo vencimento
  CASE
    WHEN pm.intervalo_km IS NOT NULL AND pv.ultima_execucao_km IS NOT NULL
      THEN pv.ultima_execucao_km + pm.intervalo_km
    WHEN pm.intervalo_km IS NOT NULL
      THEN COALESCE(v.km_atual,0) + pm.intervalo_km  -- sem historia: vence um ciclo adiante
    ELSE NULL
  END AS proximo_km,
  CASE
    WHEN pm.intervalo_dias IS NOT NULL AND pv.ultima_execucao_data IS NOT NULL
      THEN pv.ultima_execucao_data + pm.intervalo_dias
    WHEN pm.intervalo_dias IS NOT NULL
      THEN CURRENT_DATE + pm.intervalo_dias
    ELSE NULL
  END AS proxima_data,
  -- saldo em KM e dias (pode ser negativo = vencido)
  CASE
    WHEN pm.intervalo_km IS NOT NULL AND pv.ultima_execucao_km IS NOT NULL
      THEN (pv.ultima_execucao_km + pm.intervalo_km) - COALESCE(v.km_atual,0)
    ELSE NULL
  END AS saldo_km,
  CASE
    WHEN pm.intervalo_dias IS NOT NULL AND pv.ultima_execucao_data IS NOT NULL
      THEN (pv.ultima_execucao_data + pm.intervalo_dias) - CURRENT_DATE
    ELSE NULL
  END AS saldo_dias,
  -- categoria resumida
  CASE
    WHEN pm.intervalo_km IS NOT NULL AND pv.ultima_execucao_km IS NOT NULL
         AND COALESCE(v.km_atual,0) > (pv.ultima_execucao_km + pm.intervalo_km)
      THEN 'vencida'
    WHEN pm.intervalo_dias IS NOT NULL AND pv.ultima_execucao_data IS NOT NULL
         AND CURRENT_DATE > (pv.ultima_execucao_data + pm.intervalo_dias)
      THEN 'vencida'
    WHEN pm.intervalo_km IS NOT NULL AND pv.ultima_execucao_km IS NOT NULL
         AND (pv.ultima_execucao_km + pm.intervalo_km) - COALESCE(v.km_atual,0) <= 500
      THEN 'vence_em_breve'
    WHEN pm.intervalo_dias IS NOT NULL AND pv.ultima_execucao_data IS NOT NULL
         AND (pv.ultima_execucao_data + pm.intervalo_dias) - CURRENT_DATE <= 15
      THEN 'vence_em_breve'
    ELSE 'em_dia'
  END AS situacao
FROM trans_planos_veiculo pv
JOIN trans_planos_manutencao pm ON pm.id = pv.plano_id
JOIN trans_veiculos v ON v.id = pv.veiculo_id
WHERE pv.ativo = true AND pm.ativo = true;

-- -------------------------------------------------------------
-- View: ficha manutencao enriquecida
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_manutencoes_full AS
SELECT
  m.*,
  v.placa,
  v.modelo AS veiculo_modelo,
  v.tipo AS veiculo_tipo,
  (SELECT COUNT(*) FROM trans_manutencao_itens WHERE manutencao_id = m.id) AS itens_count,
  (SELECT COUNT(*) FROM trans_manutencao_pneus WHERE manutencao_id = m.id) AS pneus_count,
  (SELECT COUNT(*) FROM trans_manutencao_anexos WHERE manutencao_id = m.id) AS anexos_count
FROM trans_manutencoes m
JOIN trans_veiculos v ON v.id = m.veiculo_id;

-- -------------------------------------------------------------
-- RLS
-- -------------------------------------------------------------
ALTER TABLE trans_planos_manutencao ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_planos_veiculo ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_manutencoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_manutencao_itens ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_manutencao_pneus ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_manutencao_anexos ENABLE ROW LEVEL SECURITY;

-- Planos (gestao: admin/gestor; leitura: todos autenticados)
CREATE POLICY trans_pmanut_sel ON trans_planos_manutencao FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pmanut_ins ON trans_planos_manutencao FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor')
);
CREATE POLICY trans_pmanut_upd ON trans_planos_manutencao FOR UPDATE TO authenticated
  USING (trans_current_perfil() IN ('admin','gestor'))
  WITH CHECK (trans_current_perfil() IN ('admin','gestor'));
CREATE POLICY trans_pmanut_del ON trans_planos_manutencao FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_pveic_sel ON trans_planos_veiculo FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_pveic_ins ON trans_planos_veiculo FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor')
);
CREATE POLICY trans_pveic_upd ON trans_planos_veiculo FOR UPDATE TO authenticated
  USING (trans_current_perfil() IN ('admin','gestor'))
  WITH CHECK (trans_current_perfil() IN ('admin','gestor'));
CREATE POLICY trans_pveic_del ON trans_planos_veiculo FOR DELETE TO authenticated USING (trans_is_admin());

-- Manutencoes (CRUD por admin/gestor/almoxarife; leitura todos)
CREATE POLICY trans_manut_sel ON trans_manutencoes FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_manut_ins ON trans_manutencoes FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_manut_upd ON trans_manutencoes FOR UPDATE TO authenticated
  USING (trans_current_perfil() IN ('admin','gestor','almoxarife'))
  WITH CHECK (trans_current_perfil() IN ('admin','gestor','almoxarife'));
CREATE POLICY trans_manut_del ON trans_manutencoes FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_mi_sel ON trans_manutencao_itens FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_mi_ins ON trans_manutencao_itens FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_mi_del ON trans_manutencao_itens FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_mp_sel ON trans_manutencao_pneus FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_mp_ins ON trans_manutencao_pneus FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_mp_del ON trans_manutencao_pneus FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_ma_sel ON trans_manutencao_anexos FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_ma_ins ON trans_manutencao_anexos FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife','motorista')
);
CREATE POLICY trans_ma_del ON trans_manutencao_anexos FOR DELETE TO authenticated USING (trans_is_admin());
