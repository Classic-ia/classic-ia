-- ============================================================
-- Migration: Fase J6.4 - Abastecimento (RPCs)
-- tanque_movimentar / abastecimento_registrar
-- ============================================================

-- ============================================================
-- RPC: jr_tanque_movimentar (entrada/saida/ajuste no tanque proprio)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tanque_movimentar(
  p_tanque_id      UUID,
  p_tipo           TEXT,
  p_litros         NUMERIC,
  p_valor_unitario NUMERIC DEFAULT NULL,
  p_data           DATE    DEFAULT NULL,
  p_fornecedor_id  UUID    DEFAULT NULL,
  p_nf_numero      TEXT    DEFAULT NULL,
  p_observacao     TEXT    DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para movimentar tanque';
  END IF;
  IF p_tipo NOT IN ('entrada','saida','ajuste') THEN
    RAISE EXCEPTION 'Tipo invalido';
  END IF;
  IF p_litros IS NULL OR p_litros <= 0 THEN
    RAISE EXCEPTION 'Litros invalido';
  END IF;

  INSERT INTO public.jr_tanque_movimentacao (
    tanque_id, tipo, litros, valor_unitario, data, fornecedor_id, nf_numero, observacao
  ) VALUES (
    p_tanque_id, p_tipo, p_litros, p_valor_unitario,
    COALESCE(p_data, CURRENT_DATE), p_fornecedor_id, p_nf_numero, p_observacao
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================
-- RPC: jr_abastecimento_registrar
-- Calcula consumo L/h baseado no horimetro do ultimo abastecimento.
-- Se tanque_proprio: cria saida no tanque (saldo decrementa).
-- Cria despesa tipo=combustivel (vinculada a OS se houver).
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_abastecimento_registrar(
  p_maquina_id        UUID,
  p_origem            TEXT,
  p_litros            NUMERIC,
  p_valor_total       NUMERIC,
  p_horimetro_leitura NUMERIC,
  p_data              DATE    DEFAULT NULL,
  p_tanque_id         UUID    DEFAULT NULL,
  p_posto_nome        TEXT    DEFAULT NULL,
  p_fornecedor_id     UUID    DEFAULT NULL,
  p_os_id             UUID    DEFAULT NULL,
  p_operador_nome     TEXT    DEFAULT NULL,
  p_nf_numero         TEXT    DEFAULT NULL,
  p_observacao        TEXT    DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_id UUID;
  v_horimetro_anterior NUMERIC;
  v_horas_desde NUMERIC;
  v_consumo NUMERIC;
  v_valor_unit NUMERIC;
  v_horimetro_maq NUMERIC;
  v_despesa_id UUID;
  v_descricao TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para registrar abastecimento';
  END IF;

  IF p_origem NOT IN ('tanque_proprio','posto') THEN
    RAISE EXCEPTION 'Origem invalida';
  END IF;
  IF p_litros IS NULL OR p_litros <= 0 THEN
    RAISE EXCEPTION 'Litros invalido';
  END IF;
  IF p_valor_total IS NULL OR p_valor_total < 0 THEN
    RAISE EXCEPTION 'Valor total invalido';
  END IF;
  IF p_horimetro_leitura IS NULL OR p_horimetro_leitura < 0 THEN
    RAISE EXCEPTION 'Horimetro invalido';
  END IF;

  IF p_origem = 'tanque_proprio' AND p_tanque_id IS NULL THEN
    RAISE EXCEPTION 'tanque_proprio exige tanque_id';
  END IF;
  IF p_origem = 'posto' AND p_posto_nome IS NULL AND p_fornecedor_id IS NULL THEN
    RAISE EXCEPTION 'posto exige nome do posto ou fornecedor';
  END IF;

  -- Horimetro nao retrocede
  SELECT horimetro_atual INTO v_horimetro_maq FROM public.jr_maquinas WHERE id = p_maquina_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Maquina nao encontrada'; END IF;
  IF p_horimetro_leitura < v_horimetro_maq THEN
    RAISE EXCEPTION 'Horimetro retrocedeu (atual %, leitura %)', v_horimetro_maq, p_horimetro_leitura;
  END IF;

  -- Calcula consumo a partir do ultimo abastecimento da mesma maquina
  SELECT horimetro_leitura INTO v_horimetro_anterior
    FROM public.jr_abastecimentos
   WHERE maquina_id = p_maquina_id
   ORDER BY data DESC, created_at DESC
   LIMIT 1;

  IF v_horimetro_anterior IS NOT NULL AND p_horimetro_leitura > v_horimetro_anterior THEN
    v_horas_desde := p_horimetro_leitura - v_horimetro_anterior;
    v_consumo := p_litros / NULLIF(v_horas_desde, 0);
  END IF;

  v_valor_unit := p_valor_total / NULLIF(p_litros, 0);

  -- Insere abastecimento
  INSERT INTO public.jr_abastecimentos (
    maquina_id, os_id, origem, tanque_id, posto_nome, fornecedor_id,
    data, litros, valor_unitario, valor_total, horimetro_leitura,
    horas_desde_ultimo, consumo_l_h, operador_nome, nf_numero, observacao
  ) VALUES (
    p_maquina_id, p_os_id, p_origem, p_tanque_id, p_posto_nome, p_fornecedor_id,
    COALESCE(p_data, CURRENT_DATE), p_litros, v_valor_unit, p_valor_total, p_horimetro_leitura,
    v_horas_desde, v_consumo, p_operador_nome, p_nf_numero, p_observacao
  )
  RETURNING id INTO v_id;

  -- Atualiza horimetro da maquina (monotonico)
  UPDATE public.jr_maquinas
     SET horimetro_atual = GREATEST(horimetro_atual, p_horimetro_leitura)
   WHERE id = p_maquina_id;

  -- Saida no tanque se origem = tanque_proprio
  IF p_origem = 'tanque_proprio' THEN
    INSERT INTO public.jr_tanque_movimentacao (
      tanque_id, tipo, litros, valor_unitario, data,
      abastecimento_id, observacao
    ) VALUES (
      p_tanque_id, 'saida', p_litros, v_valor_unit, COALESCE(p_data, CURRENT_DATE),
      v_id, 'Abastecimento maquina ' || p_maquina_id::TEXT
    );
  END IF;

  -- Despesa de combustivel
  v_descricao := CASE WHEN p_origem = 'tanque_proprio' THEN 'Tanque proprio' ELSE COALESCE(p_posto_nome, 'Posto externo') END;
  v_descricao := 'Combustivel ' || v_descricao || ' (' || p_litros::TEXT || 'L)';

  INSERT INTO public.jr_despesas (
    data, tipo, valor, descricao,
    maquina_id, os_id, fornecedor_id, nota_fiscal_numero
  ) VALUES (
    COALESCE(p_data, CURRENT_DATE), 'combustivel', p_valor_total, v_descricao,
    p_maquina_id, p_os_id, p_fornecedor_id, p_nf_numero
  )
  RETURNING id INTO v_despesa_id;

  UPDATE public.jr_abastecimentos SET despesa_id = v_despesa_id WHERE id = v_id;

  RETURN jsonb_build_object(
    'ok', true,
    'id', v_id,
    'consumo_l_h', v_consumo,
    'horas_desde_ultimo', v_horas_desde,
    'despesa_id', v_despesa_id
  );
END;
$$;

-- ============================================================
-- RPC: jr_abastecimento_remover (admin only)
-- Reverte o consumo do tanque e despesa associada.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_abastecimento_remover(p_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_ab RECORD;
BEGIN
  IF NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'Apenas admin pode remover abastecimento';
  END IF;

  SELECT * INTO v_ab FROM public.jr_abastecimentos WHERE id = p_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Abastecimento nao encontrado'; END IF;

  IF v_ab.origem = 'tanque_proprio' THEN
    INSERT INTO public.jr_tanque_movimentacao (
      tanque_id, tipo, litros, valor_unitario, data, observacao
    ) VALUES (
      v_ab.tanque_id, 'entrada', v_ab.litros, v_ab.valor_unitario, CURRENT_DATE,
      'Reversao abastecimento ' || p_id::TEXT
    );
  END IF;

  IF v_ab.despesa_id IS NOT NULL THEN
    DELETE FROM public.jr_despesas WHERE id = v_ab.despesa_id;
  END IF;

  DELETE FROM public.jr_abastecimentos WHERE id = p_id;

  RETURN jsonb_build_object('ok', true);
END;
$$;

GRANT EXECUTE ON FUNCTION public.jr_tanque_movimentar         TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_abastecimento_registrar   TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_abastecimento_remover     TO authenticated;
