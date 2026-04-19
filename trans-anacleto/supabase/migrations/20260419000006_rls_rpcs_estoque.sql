-- =====================================================================
-- Fase 3: RLS + RPCs de Estoque (modalidade=novo)
-- =====================================================================

-- -------------------------------------------------------------
-- RLS
-- -------------------------------------------------------------
ALTER TABLE trans_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_lotes ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_estoque_lote ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_estoque_usado ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_estoque_movimentacao ENABLE ROW LEVEL SECURITY;

-- trans_config: admin edita, demais leem
CREATE POLICY trans_config_sel ON trans_config FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_config_ins ON trans_config FOR INSERT TO authenticated WITH CHECK (trans_is_admin());
CREATE POLICY trans_config_upd ON trans_config FOR UPDATE TO authenticated USING (trans_is_admin()) WITH CHECK (trans_is_admin());
CREATE POLICY trans_config_del ON trans_config FOR DELETE TO authenticated USING (trans_is_admin());

-- trans_lotes: almoxarife/gestor/admin
CREATE POLICY trans_lotes_sel ON trans_lotes FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_lotes_ins ON trans_lotes FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_lotes_upd ON trans_lotes FOR UPDATE TO authenticated USING (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
) WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_lotes_del ON trans_lotes FOR DELETE TO authenticated USING (trans_is_admin());

-- estoque_lote (saldo): read-only via UI; saldos alterados so por trigger
CREATE POLICY trans_estoque_lote_sel ON trans_estoque_lote FOR SELECT TO authenticated USING (true);
-- sem policies de INSERT/UPDATE/DELETE — so via trigger SECURITY DEFINER ou admin direto
CREATE POLICY trans_estoque_lote_admin ON trans_estoque_lote FOR ALL TO authenticated
  USING (trans_is_admin()) WITH CHECK (trans_is_admin());

-- estoque_usado: mesmo padrao
CREATE POLICY trans_estoque_usado_sel ON trans_estoque_usado FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_estoque_usado_admin ON trans_estoque_usado FOR ALL TO authenticated
  USING (trans_is_admin()) WITH CHECK (trans_is_admin());

-- estoque_movimentacao: insert via RPC; select livre para autenticados
CREATE POLICY trans_mov_sel ON trans_estoque_movimentacao FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_mov_ins ON trans_estoque_movimentacao FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
-- ledger imutavel: nunca UPDATE/DELETE exceto admin (para ajustes criticos)
CREATE POLICY trans_mov_upd ON trans_estoque_movimentacao FOR UPDATE TO authenticated
  USING (trans_is_admin()) WITH CHECK (trans_is_admin());
CREATE POLICY trans_mov_del ON trans_estoque_movimentacao FOR DELETE TO authenticated
  USING (false);  -- nunca deletar

-- -------------------------------------------------------------
-- RPCs de consulta
-- -------------------------------------------------------------

-- Busca produto por EAN; retorna JSON com produto + lotes disponiveis FEFO + saldos usado
CREATE OR REPLACE FUNCTION trans_buscar_produto_por_ean(p_ean text)
RETURNS json
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = public, auth
AS $$
DECLARE
  v_produto_id uuid;
  v_result json;
BEGIN
  SELECT produto_id INTO v_produto_id
  FROM trans_produtos_ean
  WHERE ean = p_ean
  LIMIT 1;

  IF v_produto_id IS NULL THEN
    RETURN json_build_object('encontrado', false, 'ean', p_ean);
  END IF;

  SELECT json_build_object(
    'encontrado', true,
    'ean', p_ean,
    'produto', (
      SELECT row_to_json(p)
      FROM (
        SELECT p.id, p.sku, p.nome, p.unidade, p.estoque_min,
               p.localizacao, p.custo_medio, p.categoria_id,
               c.nome AS categoria_nome, c.tipo AS categoria_tipo
        FROM trans_produtos p
        LEFT JOIN trans_categorias_produto c ON c.id = p.categoria_id
        WHERE p.id = v_produto_id
      ) p
    ),
    'lotes_disponiveis', (
      SELECT COALESCE(json_agg(row_to_json(l) ORDER BY
        COALESCE(l.data_validade, l.data_entrada + interval '10 years'),
        l.data_entrada), '[]'::json)
      FROM (
        SELECT lt.id AS lote_id, lt.lote_interno, lt.lote_fornecedor,
               lt.data_entrada, lt.data_validade, lt.status,
               lt.custo_unitario, el.quantidade AS saldo
        FROM trans_lotes lt
        JOIN trans_estoque_lote el ON el.lote_id = lt.id
        WHERE lt.produto_id = v_produto_id
          AND el.quantidade > 0
      ) l
    ),
    'saldos_usado', (
      SELECT COALESCE(json_agg(row_to_json(u) ORDER BY u.condicao), '[]'::json)
      FROM (
        SELECT condicao, quantidade
        FROM trans_estoque_usado
        WHERE produto_id = v_produto_id AND quantidade > 0
      ) u
    )
  ) INTO v_result;

  RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_buscar_produto_por_ean(text) TO authenticated;

-- Sugerir lote (FEFO > FIFO) para saida
CREATE OR REPLACE FUNCTION trans_sugerir_lote(p_produto_id uuid, p_quantidade numeric DEFAULT 1)
RETURNS uuid
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT lt.id
  FROM trans_lotes lt
  JOIN trans_estoque_lote el ON el.lote_id = lt.id
  WHERE lt.produto_id = p_produto_id
    AND lt.status = 'liberado'
    AND el.quantidade >= p_quantidade
  ORDER BY
    COALESCE(lt.data_validade, lt.data_entrada + interval '10 years'),
    lt.data_entrada,
    lt.lote_interno
  LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION trans_sugerir_lote(uuid, numeric) TO authenticated;

-- -------------------------------------------------------------
-- RPCs de movimentacao NOVO
-- -------------------------------------------------------------

-- Entrada de estoque (novo, com lote)
CREATE OR REPLACE FUNCTION trans_entrada_estoque(
  p_produto_id uuid,
  p_quantidade numeric,
  p_lote_fornecedor text DEFAULT NULL,
  p_fornecedor_id uuid DEFAULT NULL,
  p_data_fabricacao date DEFAULT NULL,
  p_data_validade date DEFAULT NULL,
  p_custo_unitario numeric DEFAULT NULL,
  p_nota_fiscal_id uuid DEFAULT NULL,
  p_ean_lido text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_lote_id uuid;
  v_lote_interno text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Perfil sem permissao para entrada de estoque';
  END IF;

  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;

  v_lote_interno := trans_gerar_lote_interno();

  INSERT INTO trans_lotes (
    produto_id, lote_interno, lote_fornecedor, fornecedor_id,
    nota_fiscal_id, data_entrada, data_fabricacao, data_validade,
    status, custo_unitario, observacao, created_by, updated_by
  ) VALUES (
    p_produto_id, v_lote_interno, p_lote_fornecedor, p_fornecedor_id,
    p_nota_fiscal_id, CURRENT_DATE, p_data_fabricacao, p_data_validade,
    'liberado', p_custo_unitario, p_observacao, auth.uid(), auth.uid()
  ) RETURNING id INTO v_lote_id;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, ean_lido, quantidade,
    custo_unitario, usuario_id, documento_origem, observacao
  ) VALUES (
    'entrada', 'novo', p_produto_id, v_lote_id, p_ean_lido, p_quantidade,
    p_custo_unitario, auth.uid(),
    COALESCE('NF:' || p_nota_fiscal_id::text, 'ENTRADA-MANUAL'),
    p_observacao
  );

  RETURN json_build_object(
    'ok', true,
    'lote_id', v_lote_id,
    'lote_interno', v_lote_interno,
    'quantidade', p_quantidade
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_entrada_estoque(uuid, numeric, text, uuid, date, date, numeric, uuid, text, text) TO authenticated;

-- Saida de estoque (novo, com lote)
CREATE OR REPLACE FUNCTION trans_saida_estoque(
  p_lote_id uuid,
  p_quantidade numeric,
  p_documento text DEFAULT NULL,
  p_veiculo_id uuid DEFAULT NULL,
  p_ean_lido text DEFAULT NULL,
  p_override_bloqueio boolean DEFAULT false,
  p_justificativa text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_produto_id uuid;
  v_status text;
  v_saldo numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Perfil sem permissao para saida de estoque';
  END IF;

  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;

  SELECT produto_id, status INTO v_produto_id, v_status
  FROM trans_lotes WHERE id = p_lote_id FOR UPDATE;

  IF v_produto_id IS NULL THEN
    RAISE EXCEPTION 'Lote nao encontrado';
  END IF;

  IF v_status <> 'liberado' THEN
    IF NOT p_override_bloqueio THEN
      RAISE EXCEPTION 'Lote com status % — saida bloqueada. Use override com justificativa.', v_status;
    END IF;
    IF v_perfil <> 'admin' THEN
      RAISE EXCEPTION 'Apenas admin pode fazer override de lote bloqueado';
    END IF;
    IF p_justificativa IS NULL OR length(trim(p_justificativa)) = 0 THEN
      RAISE EXCEPTION 'Justificativa obrigatoria para override';
    END IF;
  END IF;

  SELECT quantidade INTO v_saldo FROM trans_estoque_lote WHERE lote_id = p_lote_id FOR UPDATE;

  IF v_saldo IS NULL OR v_saldo < p_quantidade THEN
    RAISE EXCEPTION 'Saldo insuficiente no lote (disponivel: %)', COALESCE(v_saldo, 0);
  END IF;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, ean_lido, quantidade,
    usuario_id, documento_origem, veiculo_id, observacao,
    override_bloqueio, justificativa
  ) VALUES (
    'saida', 'novo', v_produto_id, p_lote_id, p_ean_lido, p_quantidade,
    auth.uid(), p_documento, p_veiculo_id, p_observacao,
    p_override_bloqueio, p_justificativa
  );

  SELECT quantidade INTO v_saldo FROM trans_estoque_lote WHERE lote_id = p_lote_id;

  RETURN json_build_object(
    'ok', true,
    'lote_id', p_lote_id,
    'quantidade_baixada', p_quantidade,
    'saldo_restante', v_saldo
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_saida_estoque(uuid, numeric, text, uuid, text, boolean, text, text) TO authenticated;

-- Alterar status do lote (bloqueado/quarentena/reprovado/liberado) com auditoria
CREATE OR REPLACE FUNCTION trans_alterar_status_lote(
  p_lote_id uuid,
  p_novo_status text,
  p_motivo text
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

  IF p_novo_status NOT IN ('liberado','bloqueado','quarentena','reprovado') THEN
    RAISE EXCEPTION 'Status invalido: %', p_novo_status;
  END IF;

  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio para alteracao de status';
  END IF;

  UPDATE trans_lotes
  SET status = p_novo_status,
      status_alterado_em = now(),
      status_alterado_por = auth.uid(),
      status_motivo = p_motivo,
      updated_by = auth.uid()
  WHERE id = p_lote_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Lote nao encontrado';
  END IF;

  RETURN json_build_object('ok', true, 'novo_status', p_novo_status);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_alterar_status_lote(uuid, text, text) TO authenticated;

-- Ajuste de inventario (gera movimentacao tipo=ajuste ou saida conforme delta)
CREATE OR REPLACE FUNCTION trans_ajuste_inventario(
  p_lote_id uuid,
  p_quantidade_contada numeric,
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
  v_delta numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT produto_id INTO v_produto_id FROM trans_lotes WHERE id = p_lote_id;
  SELECT COALESCE(quantidade, 0) INTO v_saldo FROM trans_estoque_lote WHERE lote_id = p_lote_id;

  v_delta := p_quantidade_contada - v_saldo;

  IF v_delta = 0 THEN
    RETURN json_build_object('ok', true, 'delta', 0, 'mensagem', 'Sem ajuste');
  END IF;

  INSERT INTO trans_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, quantidade,
    usuario_id, documento_origem, observacao
  ) VALUES (
    CASE WHEN v_delta > 0 THEN 'ajuste' ELSE 'saida' END,
    'novo', v_produto_id, p_lote_id, abs(v_delta),
    auth.uid(), 'INVENTARIO',
    'AJUSTE-INV: ' || p_motivo
  );

  RETURN json_build_object('ok', true, 'delta', v_delta);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_ajuste_inventario(uuid, numeric, text) TO authenticated;
