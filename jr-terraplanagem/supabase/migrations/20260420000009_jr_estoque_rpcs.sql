-- ============================================================
-- Migration: Fase J5 - Estoque (RPCs)
-- Entrada/saida (NOVO+lote, USADO+condicao), conversoes, status lote.
-- ============================================================

-- ============================================================
-- RPC: jr_buscar_produto_por_ean
-- Retorna produto + lotes disponiveis (FEFO) + saldos usados.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_buscar_produto_por_ean(p_ean TEXT)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_produto jsonb;
  v_lotes   jsonb;
  v_usados  jsonb;
  v_pid UUID;
BEGIN
  IF public.jr_current_perfil() IS NULL THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT pe.produto_id INTO v_pid
    FROM public.jr_produtos_ean pe
   WHERE pe.ean = p_ean
   LIMIT 1;

  IF v_pid IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'EAN nao cadastrado');
  END IF;

  SELECT to_jsonb(p) INTO v_produto FROM public.jr_produtos p WHERE p.id = v_pid;

  SELECT COALESCE(jsonb_agg(x ORDER BY prioridade_dt, data_entrada), '[]'::jsonb) INTO v_lotes
    FROM (
      SELECT lote_id, lote_interno, lote_fornecedor, data_entrada, data_validade, status, saldo, prioridade_dt
        FROM public.v_jr_lotes_disponiveis
       WHERE produto_id = v_pid AND status = 'liberado'
    ) x;

  SELECT COALESCE(jsonb_object_agg(condicao, quantidade), '{}'::jsonb) INTO v_usados
    FROM public.jr_estoque_usado WHERE produto_id = v_pid;

  RETURN jsonb_build_object(
    'ok', true,
    'produto', v_produto,
    'lotes_liberados', v_lotes,
    'saldos_usado', v_usados
  );
END;
$$;

-- ============================================================
-- RPC: jr_sugerir_lote (FEFO > FIFO; status liberado)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_sugerir_lote(p_produto_id UUID, p_quantidade NUMERIC DEFAULT 1)
RETURNS UUID
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE v_lote_id UUID;
BEGIN
  SELECT lote_id INTO v_lote_id
    FROM public.v_jr_lotes_disponiveis
   WHERE produto_id = p_produto_id
     AND status = 'liberado'
     AND saldo >= p_quantidade
   ORDER BY prioridade_dt, data_entrada
   LIMIT 1;
  RETURN v_lote_id;
END;
$$;

-- ============================================================
-- RPC: jr_entrada_estoque (NOVO com lote)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_entrada_estoque(
  p_produto_id      UUID,
  p_quantidade      NUMERIC,
  p_lote_fornecedor TEXT DEFAULT NULL,
  p_fornecedor_id   UUID DEFAULT NULL,
  p_data_validade   DATE DEFAULT NULL,
  p_data_fabricacao DATE DEFAULT NULL,
  p_custo_unitario  NUMERIC DEFAULT NULL,
  p_ean_lido        TEXT DEFAULT NULL,
  p_documento       TEXT DEFAULT NULL,
  p_nota_fiscal_id  UUID DEFAULT NULL,
  p_observacao      TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_lote_id UUID;
  v_lote_interno TEXT;
  v_mov_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para entrada de estoque';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM public.jr_produtos WHERE id = p_produto_id) THEN
    RAISE EXCEPTION 'Produto nao encontrado';
  END IF;

  INSERT INTO public.jr_lotes (
    produto_id, lote_fornecedor, fornecedor_id, nota_fiscal_id,
    data_entrada, data_fabricacao, data_validade, custo_unitario, observacao
  ) VALUES (
    p_produto_id, p_lote_fornecedor, p_fornecedor_id, p_nota_fiscal_id,
    CURRENT_DATE, p_data_fabricacao, p_data_validade, COALESCE(p_custo_unitario, 0), p_observacao
  )
  RETURNING id, lote_interno INTO v_lote_id, v_lote_interno;

  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, ean_lido,
    quantidade, custo_unitario, documento_origem, observacao
  ) VALUES (
    'entrada', 'novo', p_produto_id, v_lote_id, p_ean_lido,
    p_quantidade, COALESCE(p_custo_unitario, 0), p_documento, p_observacao
  )
  RETURNING id INTO v_mov_id;

  -- Atualiza custo_medio do produto (media ponderada simples)
  IF p_custo_unitario IS NOT NULL AND p_custo_unitario > 0 THEN
    UPDATE public.jr_produtos
       SET custo_medio = (
         SELECT SUM(el.quantidade * COALESCE(l.custo_unitario,0)) / NULLIF(SUM(el.quantidade), 0)
           FROM public.jr_estoque_lote el
           JOIN public.jr_lotes l ON l.id = el.lote_id
          WHERE el.produto_id = p_produto_id AND el.quantidade > 0
       ),
       updated_at = NOW()
     WHERE id = p_produto_id;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'lote_id', v_lote_id,
    'lote_interno', v_lote_interno,
    'movimentacao_id', v_mov_id
  );
END;
$$;

-- ============================================================
-- RPC: jr_saida_estoque (NOVO com lote)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_saida_estoque(
  p_lote_id          UUID,
  p_quantidade       NUMERIC,
  p_ean_lido         TEXT DEFAULT NULL,
  p_documento        TEXT DEFAULT NULL,
  p_os_id            UUID DEFAULT NULL,
  p_maquina_id       UUID DEFAULT NULL,
  p_manutencao_id    UUID DEFAULT NULL,
  p_observacao       TEXT DEFAULT NULL,
  p_override_bloqueio BOOLEAN DEFAULT FALSE,
  p_justificativa    TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_produto_id UUID;
  v_status TEXT;
  v_saldo NUMERIC(14,3);
  v_custo NUMERIC(14,4);
  v_mov_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para saida de estoque';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade deve ser maior que zero';
  END IF;

  SELECT l.produto_id, l.status, COALESCE(l.custo_unitario,0)
    INTO v_produto_id, v_status, v_custo
    FROM public.jr_lotes l WHERE l.id = p_lote_id;
  IF v_produto_id IS NULL THEN
    RAISE EXCEPTION 'Lote nao encontrado';
  END IF;

  IF v_status <> 'liberado' THEN
    IF NOT (p_override_bloqueio AND public.jr_is_admin()) THEN
      RAISE EXCEPTION 'Lote com status % - bloqueado para saida (admin pode override)', v_status;
    END IF;
    IF p_justificativa IS NULL OR length(trim(p_justificativa)) < 3 THEN
      RAISE EXCEPTION 'Override exige justificativa (>= 3 caracteres)';
    END IF;
  END IF;

  SELECT quantidade INTO v_saldo FROM public.jr_estoque_lote WHERE lote_id = p_lote_id;
  IF COALESCE(v_saldo, 0) < p_quantidade THEN
    RAISE EXCEPTION 'Saldo insuficiente no lote (saldo=%; pedido=%)', COALESCE(v_saldo,0), p_quantidade;
  END IF;

  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, ean_lido,
    quantidade, custo_unitario, documento_origem,
    os_id, maquina_id, manutencao_id, observacao
  ) VALUES (
    'saida', 'novo', v_produto_id, p_lote_id, p_ean_lido,
    p_quantidade, v_custo, p_documento,
    p_os_id, p_maquina_id, p_manutencao_id,
    CASE WHEN p_override_bloqueio THEN COALESCE(p_observacao,'') || ' [OVERRIDE: ' || COALESCE(p_justificativa,'') || ']' ELSE p_observacao END
  )
  RETURNING id INTO v_mov_id;

  RETURN jsonb_build_object(
    'ok', true,
    'movimentacao_id', v_mov_id,
    'saldo_restante', v_saldo - p_quantidade
  );
END;
$$;

-- ============================================================
-- RPC: jr_alterar_status_lote
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_alterar_status_lote(
  p_lote_id    UUID,
  p_novo_status TEXT,
  p_motivo     TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE v_perfil TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para alterar status de lote';
  END IF;
  IF p_novo_status NOT IN ('liberado','bloqueado','quarentena','reprovado') THEN
    RAISE EXCEPTION 'Status invalido: %', p_novo_status;
  END IF;
  IF p_motivo IS NULL OR length(trim(p_motivo)) < 3 THEN
    RAISE EXCEPTION 'Motivo obrigatorio (>= 3 caracteres)';
  END IF;

  UPDATE public.jr_lotes
     SET status = p_novo_status,
         status_alterado_em = NOW(),
         observacao = COALESCE(observacao,'') || ' [' || NOW()::DATE || ' ' || p_novo_status || ': ' || p_motivo || ']',
         updated_at = NOW()
   WHERE id = p_lote_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Lote nao encontrado'; END IF;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_entrada_usado
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_entrada_usado(
  p_produto_id UUID,
  p_condicao   CHAR(1),
  p_quantidade NUMERIC,
  p_origem     TEXT DEFAULT NULL,
  p_observacao TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE v_mov_id UUID;
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor','operador') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_condicao NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao invalida';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade invalida';
  END IF;

  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    documento_origem, observacao
  ) VALUES (
    'entrada', 'usado', p_produto_id, p_condicao, p_quantidade,
    p_origem, p_observacao
  )
  RETURNING id INTO v_mov_id;

  RETURN jsonb_build_object('ok', true, 'movimentacao_id', v_mov_id);
END;
$$;

-- ============================================================
-- RPC: jr_saida_usado
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_saida_usado(
  p_produto_id  UUID,
  p_condicao    CHAR(1),
  p_quantidade  NUMERIC,
  p_documento   TEXT DEFAULT NULL,
  p_os_id       UUID DEFAULT NULL,
  p_maquina_id  UUID DEFAULT NULL,
  p_observacao  TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_saldo NUMERIC(14,3);
  v_mov_id UUID;
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor','operador') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_condicao NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao invalida';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade invalida';
  END IF;

  SELECT quantidade INTO v_saldo FROM public.jr_estoque_usado
   WHERE produto_id = p_produto_id AND condicao = p_condicao;
  IF COALESCE(v_saldo, 0) < p_quantidade THEN
    RAISE EXCEPTION 'Saldo usado % insuficiente (saldo=%; pedido=%)', p_condicao, COALESCE(v_saldo,0), p_quantidade;
  END IF;

  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    documento_origem, os_id, maquina_id, observacao
  ) VALUES (
    'saida', 'usado', p_produto_id, p_condicao, p_quantidade,
    p_documento, p_os_id, p_maquina_id, p_observacao
  )
  RETURNING id INTO v_mov_id;

  RETURN jsonb_build_object('ok', true, 'movimentacao_id', v_mov_id, 'saldo_restante', v_saldo - p_quantidade);
END;
$$;

-- ============================================================
-- RPC: jr_converter_novo_usado
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_converter_novo_usado(
  p_lote_id          UUID,
  p_quantidade       NUMERIC,
  p_condicao_destino CHAR(1),
  p_motivo           TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_produto_id UUID;
  v_saldo NUMERIC(14,3);
  v_origem_id UUID;
  v_destino_id UUID;
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para conversao';
  END IF;
  IF p_condicao_destino NOT IN ('A','B','C') THEN
    RAISE EXCEPTION 'Condicao destino invalida';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade invalida';
  END IF;
  IF p_motivo IS NULL OR length(trim(p_motivo)) < 3 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT l.produto_id INTO v_produto_id FROM public.jr_lotes l WHERE l.id = p_lote_id;
  IF v_produto_id IS NULL THEN RAISE EXCEPTION 'Lote nao encontrado'; END IF;

  SELECT quantidade INTO v_saldo FROM public.jr_estoque_lote WHERE lote_id = p_lote_id;
  IF COALESCE(v_saldo, 0) < p_quantidade THEN
    RAISE EXCEPTION 'Saldo do lote insuficiente';
  END IF;

  -- Saida do lote (origem)
  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, lote_id, quantidade, observacao
  ) VALUES (
    'conversao', 'novo', v_produto_id, p_lote_id, p_quantidade,
    'CONVERSAO->USADO ' || p_condicao_destino || ': ' || p_motivo
  )
  RETURNING id INTO v_origem_id;

  -- Entrada no usado (destino) ligado por conversao_origem_id
  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade,
    conversao_origem_id, observacao
  ) VALUES (
    'conversao', 'usado', v_produto_id, p_condicao_destino, p_quantidade,
    v_origem_id, 'CONVERSAO<-NOVO lote ' || p_lote_id::TEXT || ': ' || p_motivo
  )
  RETURNING id INTO v_destino_id;

  RETURN jsonb_build_object('ok', true, 'origem_id', v_origem_id, 'destino_id', v_destino_id);
END;
$$;

-- ============================================================
-- RPC: jr_converter_usado_sucata
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_converter_usado_sucata(
  p_produto_id     UUID,
  p_condicao_origem CHAR(1),
  p_quantidade     NUMERIC,
  p_motivo         TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_modo TEXT;
  v_saldo NUMERIC(14,3);
  v_origem_id UUID;
  v_destino_id UUID;
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  IF p_condicao_origem NOT IN ('A','B') THEN
    RAISE EXCEPTION 'Sucata so aceita origem A ou B (C ja e sucata)';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade invalida';
  END IF;

  SELECT COALESCE(valor, 'baixa_direta') INTO v_modo
    FROM public.jr_config WHERE chave = 'sucata_modo';
  IF v_modo IS NULL THEN v_modo := 'baixa_direta'; END IF;

  SELECT quantidade INTO v_saldo FROM public.jr_estoque_usado
   WHERE produto_id = p_produto_id AND condicao = p_condicao_origem;
  IF COALESCE(v_saldo, 0) < p_quantidade THEN
    RAISE EXCEPTION 'Saldo origem insuficiente';
  END IF;

  INSERT INTO public.jr_estoque_movimentacao (
    tipo, modalidade, produto_id, condicao, quantidade, observacao
  ) VALUES (
    'conversao', 'usado', p_produto_id, p_condicao_origem, p_quantidade,
    'SUCATA (' || v_modo || '): ' || p_motivo
  )
  RETURNING id INTO v_origem_id;

  IF v_modo = 'estoque_c' THEN
    INSERT INTO public.jr_estoque_movimentacao (
      tipo, modalidade, produto_id, condicao, quantidade,
      conversao_origem_id, observacao
    ) VALUES (
      'conversao', 'usado', p_produto_id, 'C', p_quantidade,
      v_origem_id, 'SUCATA destino C'
    )
    RETURNING id INTO v_destino_id;
  END IF;

  RETURN jsonb_build_object('ok', true, 'modo', v_modo, 'origem_id', v_origem_id, 'destino_id', v_destino_id);
END;
$$;

-- ============================================================
-- jr_config: garante chave sucata_modo existe (default baixa_direta)
-- ============================================================
INSERT INTO public.jr_config (chave, valor, descricao)
VALUES ('sucata_modo', 'baixa_direta', 'baixa_direta | estoque_c (sucata vira saldo C)')
ON CONFLICT (chave) DO NOTHING;

GRANT EXECUTE ON FUNCTION public.jr_buscar_produto_por_ean    TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_sugerir_lote              TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_entrada_estoque           TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_saida_estoque             TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_alterar_status_lote       TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_entrada_usado             TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_saida_usado               TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_converter_novo_usado      TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_converter_usado_sucata    TO authenticated;

COMMENT ON FUNCTION public.jr_entrada_estoque        IS 'Entrada NOVO: cria lote + movimentacao; atualiza custo_medio.';
COMMENT ON FUNCTION public.jr_saida_estoque          IS 'Saida NOVO por lote_id; valida status liberado (admin override).';
COMMENT ON FUNCTION public.jr_converter_novo_usado   IS 'Conversao novo->usado em par ligado por conversao_origem_id.';
COMMENT ON FUNCTION public.jr_converter_usado_sucata IS 'Sucata respeita jr_config.sucata_modo (baixa_direta | estoque_c).';
