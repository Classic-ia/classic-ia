-- =====================================================================
-- Fase 3.2: RPCs de Estoque USADO + Conversoes
-- =====================================================================

-- Entrada de estoque usado (sem lote, com condicao A/B/C)
CREATE OR REPLACE FUNCTION trans_entrada_usado(
  p_produto_id uuid,
  p_condicao text,
  p_quantidade numeric,
  p_origem text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_condicao NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao deve ser A, B ou C';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    usuario_id, documento_origem, observacao
  ) VALUES (
    'entrada', 'usado', p_produto_id, p_condicao, p_quantidade,
    auth.uid(), COALESCE(p_origem, 'ENTRADA-USADO'), p_observacao
  );

  RETURN json_build_object('ok', true, 'condicao', p_condicao, 'quantidade', p_quantidade);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_entrada_usado(uuid, text, numeric, text, text) TO authenticated;

-- Saida de estoque usado (valida saldo por SKU+condicao)
CREATE OR REPLACE FUNCTION trans_saida_usado(
  p_produto_id uuid,
  p_condicao text,
  p_quantidade numeric,
  p_documento text DEFAULT NULL,
  p_veiculo_id uuid DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_saldo numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT quantidade INTO v_saldo
  FROM trans_estoque_usado
  WHERE produto_id = p_produto_id AND condicao = p_condicao
  FOR UPDATE;

  IF v_saldo IS NULL OR v_saldo < p_quantidade THEN
    RAISE EXCEPTION 'Saldo insuficiente na condicao % (disponivel: %)', p_condicao, COALESCE(v_saldo, 0);
  END IF;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    usuario_id, documento_origem, veiculo_id, observacao
  ) VALUES (
    'saida', 'usado', p_produto_id, p_condicao, p_quantidade,
    auth.uid(), p_documento, p_veiculo_id, p_observacao
  );

  SELECT quantidade INTO v_saldo
  FROM trans_estoque_usado
  WHERE produto_id = p_produto_id AND condicao = p_condicao;

  RETURN json_build_object('ok', true, 'saldo_restante', v_saldo);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_saida_usado(uuid, text, numeric, text, uuid, text) TO authenticated;

-- Conversao novo -> usado (par de movimentacoes atomico)
CREATE OR REPLACE FUNCTION trans_converter_novo_usado(
  p_lote_id uuid,
  p_quantidade numeric,
  p_condicao_destino text,
  p_motivo text
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_produto_id uuid;
  v_saldo numeric;
  v_saida_id uuid;
BEGIN
  IF v_perfil NOT IN ('admin','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao para conversao';
  END IF;
  IF p_condicao_destino NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao destino deve ser A, B ou C';
  END IF;
  IF p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;
  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT produto_id INTO v_produto_id FROM trans_lotes WHERE id = p_lote_id;
  IF v_produto_id IS NULL THEN
    RAISE EXCEPTION 'Lote nao encontrado';
  END IF;

  SELECT quantidade INTO v_saldo FROM trans_estoque_lote WHERE lote_id = p_lote_id FOR UPDATE;
  IF v_saldo IS NULL OR v_saldo < p_quantidade THEN
    RAISE EXCEPTION 'Saldo insuficiente no lote (disponivel: %)', COALESCE(v_saldo, 0);
  END IF;

  -- Saida do lote novo (conversao_origem_id = NULL => sinal negativo)
  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, quantidade,
    usuario_id, documento_origem, observacao
  ) VALUES (
    'conversao', 'novo', v_produto_id, p_lote_id, p_quantidade,
    auth.uid(), 'CONVERSAO-NOVO-USADO',
    'CONV->usado ' || p_condicao_destino || ': ' || p_motivo
  ) RETURNING id INTO v_saida_id;

  -- Entrada em usado (conversao_origem_id = saida_id => sinal positivo)
  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    usuario_id, documento_origem, observacao, conversao_origem_id
  ) VALUES (
    'conversao', 'usado', v_produto_id, p_condicao_destino, p_quantidade,
    auth.uid(), 'CONVERSAO-NOVO-USADO',
    'CONV<-lote ' || p_lote_id::text || ': ' || p_motivo,
    v_saida_id
  );

  RETURN json_build_object('ok', true, 'saida_id', v_saida_id, 'lote_id', p_lote_id, 'condicao_destino', p_condicao_destino);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_converter_novo_usado(uuid, numeric, text, text) TO authenticated;

-- Conversao usado -> sucata (respeita trans_config.sucata_modo)
CREATE OR REPLACE FUNCTION trans_converter_usado_sucata(
  p_produto_id uuid,
  p_condicao_origem text,
  p_quantidade numeric,
  p_motivo text
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_modo text;
  v_saldo numeric;
  v_saida_id uuid;
BEGIN
  IF v_perfil NOT IN ('admin','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_condicao_origem NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao origem deve ser A, B ou C';
  END IF;
  IF p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;
  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT valor INTO v_modo FROM trans_config WHERE chave = 'sucata_modo';
  v_modo := COALESCE(v_modo, 'baixa_direta');

  SELECT quantidade INTO v_saldo
  FROM trans_estoque_usado
  WHERE produto_id = p_produto_id AND condicao = p_condicao_origem
  FOR UPDATE;

  IF v_saldo IS NULL OR v_saldo < p_quantidade THEN
    RAISE EXCEPTION 'Saldo insuficiente em % (disponivel: %)', p_condicao_origem, COALESCE(v_saldo, 0);
  END IF;

  -- Saida da condicao origem
  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    usuario_id, documento_origem, observacao
  ) VALUES (
    'conversao', 'usado', p_produto_id, p_condicao_origem, p_quantidade,
    auth.uid(), 'SUCATA',
    'SUCATA modo=' || v_modo || ': ' || p_motivo
  ) RETURNING id INTO v_saida_id;

  -- Se modo = estoque_c, entra como condicao C
  IF v_modo = 'estoque_c' AND p_condicao_origem <> 'C' THEN
    INSERT INTO trans_estoque_movimentacao (
      tipo, modalidade, produto_id, condicao, quantidade,
      usuario_id, documento_origem, observacao, conversao_origem_id
    ) VALUES (
      'conversao', 'usado', p_produto_id, 'C', p_quantidade,
      auth.uid(), 'SUCATA',
      'SUCATA->C: ' || p_motivo,
      v_saida_id
    );
  END IF;

  RETURN json_build_object('ok', true, 'modo', v_modo, 'saida_id', v_saida_id);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_converter_usado_sucata(uuid, text, numeric, text) TO authenticated;

-- Ajuste de inventario USADO (delta por SKU+condicao)
CREATE OR REPLACE FUNCTION trans_ajuste_inventario_usado(
  p_produto_id uuid,
  p_condicao text,
  p_quantidade_contada numeric,
  p_motivo text
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_saldo numeric;
  v_delta numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT COALESCE(quantidade, 0) INTO v_saldo
  FROM trans_estoque_usado
  WHERE produto_id = p_produto_id AND condicao = p_condicao;

  v_delta := p_quantidade_contada - COALESCE(v_saldo, 0);

  IF v_delta = 0 THEN
    RETURN json_build_object('ok', true, 'delta', 0, 'mensagem', 'Sem ajuste');
  END IF;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    usuario_id, documento_origem, observacao
  ) VALUES (
    CASE WHEN v_delta > 0 THEN 'ajuste' ELSE 'saida' END,
    'usado', p_produto_id, p_condicao, abs(v_delta),
    auth.uid(), 'INVENTARIO',
    'AJUSTE-INV-USADO: ' || p_motivo
  );

  RETURN json_build_object('ok', true, 'delta', v_delta);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_ajuste_inventario_usado(uuid, text, numeric, text) TO authenticated;

-- View: consolidado por produto (saldo novo total + saldos usado por condicao)
CREATE OR REPLACE VIEW v_trans_estoque_consolidado AS
SELECT
  p.id AS produto_id,
  p.sku,
  p.nome,
  p.unidade,
  p.estoque_min,
  p.localizacao,
  p.categoria_id,
  c.nome AS categoria_nome,
  c.tipo AS categoria_tipo,
  COALESCE((SELECT SUM(el.quantidade) FROM trans_estoque_lote el WHERE el.produto_id = p.id), 0) AS saldo_novo,
  COALESCE((SELECT quantidade FROM trans_estoque_usado WHERE produto_id = p.id AND condicao = 'A'), 0) AS saldo_usado_a,
  COALESCE((SELECT quantidade FROM trans_estoque_usado WHERE produto_id = p.id AND condicao = 'B'), 0) AS saldo_usado_b,
  COALESCE((SELECT quantidade FROM trans_estoque_usado WHERE produto_id = p.id AND condicao = 'C'), 0) AS saldo_usado_c,
  p.ativo
FROM trans_produtos p
LEFT JOIN trans_categorias_produto c ON c.id = p.categoria_id
WHERE p.ativo = true;
