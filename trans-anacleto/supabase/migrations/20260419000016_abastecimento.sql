-- =====================================================================
-- 20260419000016_abastecimento.sql
-- =====================================================================
-- Modulo Abastecimento Trans Anacleto.
--
-- - trans_tanque: tanques proprios (diesel, s10, arla, gasolina)
-- - trans_tanque_movimentacao: ledger (entrada = compra, saida = abast
--   de veiculo a partir do tanque). Trigger mantem saldo_atual_litros.
-- - trans_abastecimentos: abastecimentos (tanque proprio ou posto),
--   com km_odometro e consumo_km_l (GENERATED via view auxiliar; em
--   tabela usamos km_percorrido calculado no insert).
--
-- RPCs:
--   - trans_tanque_entrada(tanque, litros, valor_unit, fornecedor, nf)
--   - trans_abastecimento_registrar(veiculo, motorista, origem, tanque?,
--     posto?, litros, valor_total, km_odometro, data)
-- =====================================================================

CREATE TABLE IF NOT EXISTS trans_tanque (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  combustivel_tipo text NOT NULL CHECK (combustivel_tipo IN ('diesel','s10','arla','gasolina')),
  capacidade_litros numeric(12,2) NOT NULL CHECK (capacidade_litros > 0),
  saldo_atual_litros numeric(12,2) NOT NULL DEFAULT 0 CHECK (saldo_atual_litros >= 0),
  ativo boolean NOT NULL DEFAULT true,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid,
  updated_by uuid,
  UNIQUE(nome)
);

CREATE TRIGGER trans_tanque_upd BEFORE UPDATE ON trans_tanque
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

CREATE TABLE IF NOT EXISTS trans_tanque_movimentacao (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tanque_id uuid NOT NULL REFERENCES trans_tanque(id),
  tipo text NOT NULL CHECK (tipo IN ('entrada','saida','ajuste')),
  litros numeric(12,2) NOT NULL CHECK (litros > 0),
  valor_unitario numeric(12,4),
  valor_total numeric(14,2),
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  nf_numero text,
  abastecimento_id uuid,
  data_hora timestamptz NOT NULL DEFAULT now(),
  usuario_id uuid,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE INDEX idx_trans_tanque_mov_tanque ON trans_tanque_movimentacao(tanque_id, data_hora DESC);
CREATE INDEX idx_trans_tanque_mov_abast ON trans_tanque_movimentacao(abastecimento_id) WHERE abastecimento_id IS NOT NULL;

-- Trigger: atualiza saldo do tanque apos movimentacao
CREATE OR REPLACE FUNCTION trans_tg_tanque_saldo() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.tipo = 'entrada' THEN
      UPDATE trans_tanque SET saldo_atual_litros = saldo_atual_litros + NEW.litros,
        updated_at = now() WHERE id = NEW.tanque_id;
    ELSIF NEW.tipo = 'saida' THEN
      UPDATE trans_tanque SET saldo_atual_litros = saldo_atual_litros - NEW.litros,
        updated_at = now() WHERE id = NEW.tanque_id;
      IF (SELECT saldo_atual_litros FROM trans_tanque WHERE id = NEW.tanque_id) < 0 THEN
        RAISE EXCEPTION 'Saldo do tanque ficaria negativo';
      END IF;
    ELSIF NEW.tipo = 'ajuste' THEN
      UPDATE trans_tanque SET saldo_atual_litros = NEW.litros,
        updated_at = now() WHERE id = NEW.tanque_id;
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trans_tanque_mov_saldo AFTER INSERT ON trans_tanque_movimentacao
FOR EACH ROW EXECUTE FUNCTION trans_tg_tanque_saldo();

-- =====================================================================
-- trans_abastecimentos
-- =====================================================================
CREATE TABLE IF NOT EXISTS trans_abastecimentos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id),
  motorista_id uuid REFERENCES trans_motoristas(id),
  origem text NOT NULL CHECK (origem IN ('tanque_proprio','posto')),
  tanque_id uuid REFERENCES trans_tanque(id),
  posto_nome text,
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  data timestamptz NOT NULL DEFAULT now(),
  litros numeric(12,2) NOT NULL CHECK (litros > 0),
  valor_unitario numeric(12,4),
  valor_total numeric(14,2) NOT NULL CHECK (valor_total >= 0),
  km_odometro numeric NOT NULL,
  km_percorrido numeric,
  consumo_km_l numeric(10,3) GENERATED ALWAYS AS (
    CASE WHEN litros > 0 AND km_percorrido IS NOT NULL AND km_percorrido > 0
         THEN km_percorrido / litros ELSE NULL END
  ) STORED,
  nf_numero text,
  anexo_url text,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid,
  updated_by uuid,
  CHECK (
    (origem = 'tanque_proprio' AND tanque_id IS NOT NULL AND posto_nome IS NULL) OR
    (origem = 'posto' AND posto_nome IS NOT NULL AND tanque_id IS NULL)
  )
);

CREATE INDEX idx_trans_abast_veiculo_data ON trans_abastecimentos(veiculo_id, data DESC);
CREATE INDEX idx_trans_abast_motorista ON trans_abastecimentos(motorista_id) WHERE motorista_id IS NOT NULL;
CREATE INDEX idx_trans_abast_tanque ON trans_abastecimentos(tanque_id) WHERE tanque_id IS NOT NULL;

CREATE TRIGGER trans_abast_upd BEFORE UPDATE ON trans_abastecimentos
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- =====================================================================
-- RLS
-- =====================================================================
ALTER TABLE trans_tanque ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_tanque_movimentacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_abastecimentos ENABLE ROW LEVEL SECURITY;

CREATE POLICY tanque_sel ON trans_tanque FOR SELECT TO authenticated USING (true);
CREATE POLICY tanque_mod ON trans_tanque FOR ALL TO authenticated
  USING (trans_current_perfil() IN ('admin','gestor','almoxarife'))
  WITH CHECK (trans_current_perfil() IN ('admin','gestor','almoxarife'));

CREATE POLICY tanque_mov_sel ON trans_tanque_movimentacao FOR SELECT TO authenticated USING (true);
CREATE POLICY tanque_mov_ins ON trans_tanque_movimentacao FOR INSERT TO authenticated
  WITH CHECK (trans_current_perfil() IN ('admin','gestor','almoxarife'));
CREATE POLICY tanque_mov_del ON trans_tanque_movimentacao FOR DELETE TO authenticated
  USING (trans_is_admin());

CREATE POLICY abast_sel ON trans_abastecimentos FOR SELECT TO authenticated USING (true);
CREATE POLICY abast_ins ON trans_abastecimentos FOR INSERT TO authenticated
  WITH CHECK (trans_current_perfil() IN ('admin','gestor','almoxarife','motorista'));
CREATE POLICY abast_upd ON trans_abastecimentos FOR UPDATE TO authenticated
  USING (trans_current_perfil() IN ('admin','gestor','almoxarife'));
CREATE POLICY abast_del ON trans_abastecimentos FOR DELETE TO authenticated
  USING (trans_is_admin());

-- =====================================================================
-- VIEWS
-- =====================================================================
CREATE OR REPLACE VIEW v_trans_abastecimentos_full AS
SELECT
  a.*,
  v.placa,
  v.modelo AS veiculo_modelo,
  v.tipo AS veiculo_tipo,
  m.nome AS motorista_nome,
  t.nome AS tanque_nome,
  f.razao_social AS fornecedor_nome
FROM trans_abastecimentos a
JOIN trans_veiculos v ON v.id = a.veiculo_id
LEFT JOIN trans_motoristas m ON m.id = a.motorista_id
LEFT JOIN trans_tanque t ON t.id = a.tanque_id
LEFT JOIN trans_fornecedores f ON f.id = a.fornecedor_id;

GRANT SELECT ON v_trans_abastecimentos_full TO authenticated;

-- Consumo medio por veiculo (90 dias)
CREATE OR REPLACE VIEW v_trans_consumo_medio AS
SELECT
  a.veiculo_id,
  v.placa,
  v.modelo,
  COUNT(*) AS n_abast,
  SUM(a.litros) AS total_litros,
  SUM(a.km_percorrido) AS total_km,
  CASE WHEN SUM(a.litros) > 0 THEN SUM(a.km_percorrido)/SUM(a.litros) ELSE NULL END AS consumo_medio_km_l,
  AVG(a.valor_unitario) AS preco_medio_l,
  MIN(a.data) AS primeira_data,
  MAX(a.data) AS ultima_data
FROM trans_abastecimentos a
JOIN trans_veiculos v ON v.id = a.veiculo_id
WHERE a.data >= NOW() - INTERVAL '90 days'
  AND a.km_percorrido IS NOT NULL AND a.km_percorrido > 0
GROUP BY a.veiculo_id, v.placa, v.modelo;

GRANT SELECT ON v_trans_consumo_medio TO authenticated;

-- =====================================================================
-- RPC: entrada de combustivel no tanque (compra)
-- =====================================================================
CREATE OR REPLACE FUNCTION trans_tanque_entrada(
  p_tanque_id uuid,
  p_litros numeric,
  p_valor_unitario numeric DEFAULT NULL,
  p_fornecedor_id uuid DEFAULT NULL,
  p_nf_numero text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_mov_id uuid;
  v_cap numeric;
  v_saldo numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_litros IS NULL OR p_litros <= 0 THEN
    RAISE EXCEPTION 'Litros invalidos';
  END IF;

  SELECT capacidade_litros, saldo_atual_litros INTO v_cap, v_saldo
  FROM trans_tanque WHERE id = p_tanque_id FOR UPDATE;
  IF v_cap IS NULL THEN RAISE EXCEPTION 'Tanque nao encontrado'; END IF;
  IF v_saldo + p_litros > v_cap THEN
    RAISE EXCEPTION 'Entrada de % L excede capacidade (saldo % + entrada > cap %)',
      p_litros, v_saldo, v_cap;
  END IF;

  INSERT INTO trans_tanque_movimentacao (
    tanque_id, tipo, litros, valor_unitario, valor_total,
    fornecedor_id, nf_numero, usuario_id, observacao, created_by
  ) VALUES (
    p_tanque_id, 'entrada', p_litros, p_valor_unitario,
    COALESCE(p_valor_unitario,0) * p_litros,
    p_fornecedor_id, p_nf_numero, auth.uid(), p_observacao, auth.uid()
  ) RETURNING id INTO v_mov_id;

  RETURN json_build_object('ok', true, 'movimentacao_id', v_mov_id);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_tanque_entrada(uuid, numeric, numeric, uuid, text, text) TO authenticated;

-- =====================================================================
-- RPC: registrar abastecimento
-- =====================================================================
CREATE OR REPLACE FUNCTION trans_abastecimento_registrar(
  p_veiculo_id uuid,
  p_origem text,
  p_litros numeric,
  p_valor_total numeric,
  p_km_odometro numeric,
  p_motorista_id uuid DEFAULT NULL,
  p_tanque_id uuid DEFAULT NULL,
  p_posto_nome text DEFAULT NULL,
  p_fornecedor_id uuid DEFAULT NULL,
  p_valor_unitario numeric DEFAULT NULL,
  p_data timestamptz DEFAULT NULL,
  p_nf_numero text DEFAULT NULL,
  p_anexo_url text DEFAULT NULL,
  p_observacao text DEFAULT NULL,
  p_correcao boolean DEFAULT false
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_abast_id uuid;
  v_km_ant numeric;
  v_km_percorrido numeric;
  v_km_veic numeric;
  v_data timestamptz := COALESCE(p_data, now());
  v_valor_unit numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife','motorista') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  IF p_origem = 'tanque_proprio' THEN
    IF p_tanque_id IS NULL THEN RAISE EXCEPTION 'Tanque obrigatorio'; END IF;
    IF p_posto_nome IS NOT NULL THEN RAISE EXCEPTION 'Posto_nome nao aplica a tanque_proprio'; END IF;
  ELSIF p_origem = 'posto' THEN
    IF COALESCE(p_posto_nome,'') = '' THEN RAISE EXCEPTION 'Nome do posto obrigatorio'; END IF;
    IF p_tanque_id IS NOT NULL THEN RAISE EXCEPTION 'Tanque_id nao aplica a posto'; END IF;
  ELSE
    RAISE EXCEPTION 'origem invalida: %', p_origem;
  END IF;

  -- KM monotonico
  SELECT km_atual INTO v_km_veic FROM trans_veiculos WHERE id = p_veiculo_id FOR UPDATE;
  IF v_km_veic IS NULL THEN RAISE EXCEPTION 'Veiculo nao encontrado'; END IF;
  IF p_km_odometro < v_km_veic AND NOT p_correcao THEN
    RAISE EXCEPTION 'Odometro % menor que km_atual do veiculo % (use p_correcao=true para exceção)',
      p_km_odometro, v_km_veic;
  END IF;

  -- KM percorrido baseado no ultimo abastecimento do mesmo veiculo
  SELECT km_odometro INTO v_km_ant
  FROM trans_abastecimentos
  WHERE veiculo_id = p_veiculo_id AND data < v_data
  ORDER BY data DESC LIMIT 1;

  v_km_percorrido := CASE WHEN v_km_ant IS NOT NULL AND p_km_odometro > v_km_ant
                          THEN p_km_odometro - v_km_ant ELSE NULL END;

  v_valor_unit := COALESCE(p_valor_unitario,
    CASE WHEN p_litros > 0 THEN p_valor_total / p_litros ELSE NULL END);

  INSERT INTO trans_abastecimentos (
    veiculo_id, motorista_id, origem, tanque_id, posto_nome,
    fornecedor_id, data, litros, valor_unitario, valor_total,
    km_odometro, km_percorrido, nf_numero, anexo_url,
    observacao, created_by, updated_by
  ) VALUES (
    p_veiculo_id, p_motorista_id, p_origem, p_tanque_id, p_posto_nome,
    p_fornecedor_id, v_data, p_litros, v_valor_unit, p_valor_total,
    p_km_odometro, v_km_percorrido, p_nf_numero, p_anexo_url,
    p_observacao, auth.uid(), auth.uid()
  ) RETURNING id INTO v_abast_id;

  -- Saida do tanque proprio
  IF p_origem = 'tanque_proprio' THEN
    INSERT INTO trans_tanque_movimentacao (
      tanque_id, tipo, litros, valor_unitario, valor_total,
      abastecimento_id, usuario_id, observacao, created_by
    ) VALUES (
      p_tanque_id, 'saida', p_litros, v_valor_unit, p_valor_total,
      v_abast_id, auth.uid(),
      'Abastecimento ' || SUBSTRING(v_abast_id::text,1,8), auth.uid()
    );
  END IF;

  -- Atualiza km_atual do veiculo (monotonico)
  UPDATE trans_veiculos
  SET km_atual = GREATEST(COALESCE(km_atual, 0), p_km_odometro),
      updated_at = now(), updated_by = auth.uid()
  WHERE id = p_veiculo_id;

  RETURN json_build_object(
    'ok', true,
    'abastecimento_id', v_abast_id,
    'km_percorrido', v_km_percorrido,
    'consumo_km_l', CASE WHEN p_litros > 0 AND v_km_percorrido IS NOT NULL
                         THEN v_km_percorrido / p_litros ELSE NULL END
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_abastecimento_registrar(
  uuid, text, numeric, numeric, numeric, uuid, uuid, text, uuid,
  numeric, timestamptz, text, text, text, boolean
) TO authenticated;
