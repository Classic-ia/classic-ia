-- =====================================================================
-- Fase 5: Manutencao - RPCs
-- =====================================================================
-- RPCs para orquestrar o ciclo de uma manutencao:
--   abrir -> adicionar_item -> adicionar_pneu -> finalizar | cancelar
-- Abordagem: itens consumidos SEMPRE passam pelos RPCs de estoque
-- (trans_saida_estoque ou trans_saida_usado) para manter saldo correto.
-- =====================================================================

-- -------------------------------------------------------------
-- Abrir manutencao
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_abrir(
  p_veiculo_id uuid,
  p_tipo text,
  p_km_atual numeric,
  p_servico text,
  p_oficina text DEFAULT NULL,
  p_executado_por text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_km_veic numeric;
  v_id uuid;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_tipo NOT IN ('preventiva','corretiva') THEN
    RAISE EXCEPTION 'Tipo invalido: %', p_tipo;
  END IF;
  IF p_servico IS NULL OR length(trim(p_servico)) = 0 THEN
    RAISE EXCEPTION 'Servico obrigatorio';
  END IF;

  SELECT COALESCE(km_atual,0) INTO v_km_veic FROM trans_veiculos WHERE id = p_veiculo_id;
  IF v_km_veic IS NULL THEN RAISE EXCEPTION 'Veiculo nao encontrado'; END IF;
  IF p_km_atual < v_km_veic THEN
    RAISE EXCEPTION 'KM nao pode retroceder (atual % / informado %)', v_km_veic, p_km_atual;
  END IF;

  INSERT INTO trans_manutencoes (
    veiculo_id, tipo, servico, status, km_abertura,
    oficina, executado_por, observacao, created_by, updated_by
  ) VALUES (
    p_veiculo_id, p_tipo, p_servico, 'aberta', p_km_atual,
    p_oficina, p_executado_por, p_observacao, auth.uid(), auth.uid()
  ) RETURNING id INTO v_id;

  -- atualiza km do veiculo (monotonico)
  UPDATE trans_veiculos
  SET km_atual = GREATEST(COALESCE(km_atual,0), p_km_atual),
      updated_by = auth.uid()
  WHERE id = p_veiculo_id;

  RETURN json_build_object('ok', true, 'manutencao_id', v_id);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_abrir(uuid,text,numeric,text,text,text,text) TO authenticated;

-- -------------------------------------------------------------
-- Adicionar item (consumo de estoque atrelado a manutencao)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_adicionar_item(
  p_manutencao_id uuid,
  p_produto_id uuid,
  p_quantidade numeric,
  p_modalidade text,
  p_lote_id uuid DEFAULT NULL,
  p_condicao text DEFAULT NULL,
  p_custo_unitario numeric DEFAULT NULL,
  p_ean_lido text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_veiculo_id uuid;
  v_km numeric;
  v_lote_id uuid := p_lote_id;
  v_saida json;
  v_mov_id uuid;
  v_sug json;
  v_item_id uuid;
  v_custo numeric := p_custo_unitario;
  v_doc text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser > 0';
  END IF;
  IF p_modalidade NOT IN ('novo','usado') THEN
    RAISE EXCEPTION 'Modalidade invalida';
  END IF;

  SELECT status, veiculo_id, km_abertura INTO v_status, v_veiculo_id, v_km
  FROM trans_manutencoes WHERE id = p_manutencao_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_status IN ('concluida','cancelada') THEN
    RAISE EXCEPTION 'Manutencao % nao aceita novos itens', v_status;
  END IF;

  v_doc := 'MANUT-' || substring(p_manutencao_id::text, 1, 8);

  IF p_modalidade = 'novo' THEN
    IF v_lote_id IS NULL THEN
      v_sug := trans_sugerir_lote(p_produto_id, p_quantidade);
      IF v_sug IS NULL OR (v_sug->>'lote_id') IS NULL THEN
        RAISE EXCEPTION 'Sem lote disponivel para FEFO';
      END IF;
      v_lote_id := (v_sug->>'lote_id')::uuid;
    END IF;
    v_saida := trans_saida_estoque(
      v_lote_id, p_quantidade, v_doc, v_veiculo_id, p_ean_lido, false, NULL, p_observacao
    );
  ELSE -- usado
    IF p_condicao IS NULL THEN
      RAISE EXCEPTION 'Condicao obrigatoria para modalidade=usado';
    END IF;
    v_saida := trans_saida_usado(
      p_produto_id, p_condicao, p_quantidade, v_doc, v_veiculo_id, p_observacao
    );
  END IF;

  -- pega a ultima movimentacao criada para este usuario+manutencao
  SELECT id INTO v_mov_id
  FROM trans_estoque_movimentacao
  WHERE documento_origem = v_doc AND produto_id = p_produto_id
  ORDER BY data_hora DESC LIMIT 1;

  -- custo unitario: se nao veio, tenta pegar do custo_medio do produto
  IF v_custo IS NULL THEN
    SELECT custo_medio INTO v_custo FROM trans_produtos WHERE id = p_produto_id;
  END IF;

  INSERT INTO trans_manutencao_itens (
    manutencao_id, produto_id, modalidade, lote_id, condicao,
    quantidade, custo_unitario, movimentacao_estoque_id, observacao, created_by
  ) VALUES (
    p_manutencao_id, p_produto_id, p_modalidade,
    CASE WHEN p_modalidade='novo' THEN v_lote_id ELSE NULL END,
    CASE WHEN p_modalidade='usado' THEN p_condicao ELSE NULL END,
    p_quantidade, v_custo, v_mov_id, p_observacao, auth.uid()
  ) RETURNING id INTO v_item_id;

  -- status em_andamento ao primeiro item
  UPDATE trans_manutencoes SET status = 'em_andamento', updated_by = auth.uid()
  WHERE id = p_manutencao_id AND status = 'aberta';

  RETURN json_build_object(
    'ok', true, 'item_id', v_item_id,
    'movimentacao_id', v_mov_id, 'lote_id', v_lote_id
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_adicionar_item(uuid,uuid,numeric,text,uuid,text,numeric,text,text) TO authenticated;

-- -------------------------------------------------------------
-- Adicionar acao em pneu (registra vinculo; chamada ao RPC de pneu
-- fica a cargo do cliente para simplicidade e flexibilidade)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_vincular_pneu(
  p_manutencao_id uuid,
  p_pneu_id uuid,
  p_acao text,
  p_movimentacao_pneu_id uuid DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_id uuid;
  v_mov uuid := p_movimentacao_pneu_id;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT status INTO v_status FROM trans_manutencoes WHERE id = p_manutencao_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_status IN ('concluida','cancelada') THEN
    RAISE EXCEPTION 'Manutencao % nao aceita vinculo de pneu', v_status;
  END IF;

  -- se nao passou mov_id, pega a ultima do pneu
  IF v_mov IS NULL THEN
    SELECT id INTO v_mov FROM trans_pneus_movimentacoes
    WHERE pneu_id = p_pneu_id
    ORDER BY data_hora DESC LIMIT 1;
  END IF;

  INSERT INTO trans_manutencao_pneus (
    manutencao_id, pneu_id, acao, movimentacao_pneu_id, observacao, created_by
  ) VALUES (
    p_manutencao_id, p_pneu_id, p_acao, v_mov, p_observacao, auth.uid()
  ) RETURNING id INTO v_id;

  UPDATE trans_manutencoes SET status = 'em_andamento', updated_by = auth.uid()
  WHERE id = p_manutencao_id AND status = 'aberta';

  RETURN json_build_object('ok', true, 'vinculo_id', v_id);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_vincular_pneu(uuid,uuid,text,uuid,text) TO authenticated;

-- -------------------------------------------------------------
-- Finalizar manutencao (calcula proximo vencimento)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_finalizar(
  p_manutencao_id uuid,
  p_km_conclusao numeric,
  p_data_execucao timestamptz DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_tipo text;
  v_servico text;
  v_veic uuid;
  v_km_abert numeric;
  v_exec timestamptz := COALESCE(p_data_execucao, now());
  v_prox_km numeric;
  v_prox_data date;
  r record;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT status, tipo, servico, veiculo_id, km_abertura
    INTO v_status, v_tipo, v_servico, v_veic, v_km_abert
  FROM trans_manutencoes WHERE id = p_manutencao_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_status IN ('concluida','cancelada') THEN
    RAISE EXCEPTION 'Manutencao ja % — nao pode finalizar', v_status;
  END IF;
  IF p_km_conclusao < v_km_abert THEN
    RAISE EXCEPTION 'KM conclusao nao pode ser menor que abertura';
  END IF;

  -- para preventivas: atualiza plano_veiculo que case pelo tipo_servico
  IF v_tipo = 'preventiva' THEN
    FOR r IN
      SELECT pv.id AS pveic_id, pm.intervalo_km, pm.intervalo_dias
      FROM trans_planos_veiculo pv
      JOIN trans_planos_manutencao pm ON pm.id = pv.plano_id
      WHERE pv.veiculo_id = v_veic AND pv.ativo = true AND pm.ativo = true
        AND (pm.tipo_servico = v_servico
             OR v_servico ILIKE '%' || pm.nome || '%'
             OR v_servico ILIKE '%' || pm.tipo_servico || '%')
    LOOP
      UPDATE trans_planos_veiculo
      SET ultima_execucao_km = p_km_conclusao,
          ultima_execucao_data = v_exec::date,
          updated_by = auth.uid()
      WHERE id = r.pveic_id;
      -- registra primeiro match como proximo vencimento da manutencao
      IF v_prox_km IS NULL AND r.intervalo_km IS NOT NULL THEN
        v_prox_km := p_km_conclusao + r.intervalo_km;
      END IF;
      IF v_prox_data IS NULL AND r.intervalo_dias IS NOT NULL THEN
        v_prox_data := (v_exec::date) + r.intervalo_dias;
      END IF;
    END LOOP;
  END IF;

  UPDATE trans_manutencoes
  SET status = 'concluida',
      data_execucao = v_exec,
      data_encerramento = now(),
      km_execucao = p_km_conclusao,
      proximo_km = v_prox_km,
      proxima_data = v_prox_data,
      updated_by = auth.uid()
  WHERE id = p_manutencao_id;

  -- km veiculo monotonico
  UPDATE trans_veiculos
  SET km_atual = GREATEST(COALESCE(km_atual,0), p_km_conclusao),
      updated_by = auth.uid()
  WHERE id = v_veic;

  RETURN json_build_object(
    'ok', true,
    'manutencao_id', p_manutencao_id,
    'proximo_km', v_prox_km,
    'proxima_data', v_prox_data
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_finalizar(uuid,numeric,timestamptz) TO authenticated;

-- -------------------------------------------------------------
-- Cancelar manutencao (reverte itens de estoque)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_cancelar(
  p_manutencao_id uuid,
  p_motivo text
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  r record;
  v_rev int := 0;
BEGIN
  IF v_perfil <> 'admin' THEN
    RAISE EXCEPTION 'Apenas admin pode cancelar manutencao';
  END IF;
  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT status INTO v_status FROM trans_manutencoes WHERE id = p_manutencao_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_status IN ('concluida','cancelada') THEN
    RAISE EXCEPTION 'Manutencao ja % — nao pode cancelar', v_status;
  END IF;

  -- reverte cada item criando movimentacao de entrada compensatoria
  FOR r IN
    SELECT mi.*, m.veiculo_id
    FROM trans_manutencao_itens mi
    JOIN trans_manutencoes m ON m.id = mi.manutencao_id
    WHERE mi.manutencao_id = p_manutencao_id
  LOOP
    INSERT INTO trans_estoque_movimentacao (
      tipo, modalidade, produto_id, lote_id, condicao,
      quantidade, usuario_id, documento_origem, veiculo_id, observacao
    ) VALUES (
      'entrada', r.modalidade, r.produto_id, r.lote_id, r.condicao,
      r.quantidade, auth.uid(),
      'CANCEL-MANUT-' || substring(p_manutencao_id::text,1,8),
      r.veiculo_id, 'Compensacao cancelamento manutencao: ' || p_motivo
    );
    v_rev := v_rev + 1;
  END LOOP;

  UPDATE trans_manutencoes
  SET status = 'cancelada',
      motivo_cancelamento = p_motivo,
      cancelada_em = now(),
      cancelada_por = auth.uid(),
      updated_by = auth.uid()
  WHERE id = p_manutencao_id;

  RETURN json_build_object('ok', true, 'itens_revertidos', v_rev);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_cancelar(uuid,text) TO authenticated;

-- -------------------------------------------------------------
-- Lista de alertas de manutencao (filtravel por veiculo)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencoes_alertas(
  p_veiculo_id uuid DEFAULT NULL,
  p_somente_pendentes boolean DEFAULT true
) RETURNS json
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = public
AS $$
DECLARE v_json json;
BEGIN
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY
    CASE r.situacao WHEN 'vencida' THEN 1 WHEN 'vence_em_breve' THEN 2 ELSE 3 END,
    COALESCE(r.saldo_km, 99999999),
    COALESCE(r.saldo_dias, 99999999)
  ), '[]'::json) INTO v_json
  FROM v_trans_manutencoes_pendentes r
  WHERE (p_veiculo_id IS NULL OR r.veiculo_id = p_veiculo_id)
    AND (NOT p_somente_pendentes OR r.situacao IN ('vencida','vence_em_breve'));

  RETURN v_json;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_manutencoes_alertas(uuid,boolean) TO authenticated;

-- -------------------------------------------------------------
-- Resumo de custo por veiculo / periodo
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_manutencao_custos(
  p_data_ini date,
  p_data_fim date,
  p_veiculo_id uuid DEFAULT NULL
) RETURNS json
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT json_build_object(
    'total_ordens', COUNT(*),
    'concluidas', COUNT(*) FILTER (WHERE status = 'concluida'),
    'canceladas', COUNT(*) FILTER (WHERE status = 'cancelada'),
    'custo_total', COALESCE(SUM(custo_total_calc) FILTER (WHERE status = 'concluida'), 0),
    'custo_preventiva', COALESCE(SUM(custo_total_calc) FILTER (WHERE status='concluida' AND tipo='preventiva'), 0),
    'custo_corretiva', COALESCE(SUM(custo_total_calc) FILTER (WHERE status='concluida' AND tipo='corretiva'), 0),
    'por_veiculo', (
      SELECT COALESCE(json_agg(row_to_json(x) ORDER BY x.custo DESC), '[]'::json)
      FROM (
        SELECT m.veiculo_id, v.placa, v.modelo,
               COUNT(*) AS ordens,
               COALESCE(SUM(m.custo_total_calc) FILTER (WHERE m.status='concluida'), 0) AS custo
        FROM trans_manutencoes m
        JOIN trans_veiculos v ON v.id = m.veiculo_id
        WHERE m.data_abertura::date BETWEEN p_data_ini AND p_data_fim
          AND (p_veiculo_id IS NULL OR m.veiculo_id = p_veiculo_id)
        GROUP BY m.veiculo_id, v.placa, v.modelo
      ) x
    )
  )
  FROM trans_manutencoes m
  WHERE m.data_abertura::date BETWEEN p_data_ini AND p_data_fim
    AND (p_veiculo_id IS NULL OR m.veiculo_id = p_veiculo_id);
$$;

GRANT EXECUTE ON FUNCTION trans_manutencao_custos(date,date,uuid) TO authenticated;
