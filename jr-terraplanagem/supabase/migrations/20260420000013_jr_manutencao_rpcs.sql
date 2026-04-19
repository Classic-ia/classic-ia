-- ============================================================
-- Migration: Fase J6.3 - Manutencao (RPCs)
-- abrir / adicionar_item / finalizar / cancelar / alertas
-- ============================================================

-- ============================================================
-- RPC: jr_manutencao_abrir
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencao_abrir(
  p_maquina_id     UUID,
  p_tipo           TEXT,
  p_servico        TEXT,
  p_horimetro      NUMERIC DEFAULT NULL,
  p_observacao     TEXT DEFAULT NULL,
  p_oficina        TEXT DEFAULT NULL,
  p_executado_por  TEXT DEFAULT NULL,
  p_plano_id       UUID DEFAULT NULL,
  p_os_id          UUID DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_id UUID;
  v_horimetro_atual NUMERIC;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para abrir manutencao';
  END IF;

  IF p_tipo NOT IN ('preventiva','corretiva') THEN
    RAISE EXCEPTION 'Tipo invalido: %', p_tipo;
  END IF;

  SELECT horimetro_atual INTO v_horimetro_atual
    FROM public.jr_maquinas WHERE id = p_maquina_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Maquina nao encontrada';
  END IF;

  IF p_horimetro IS NOT NULL AND p_horimetro < v_horimetro_atual THEN
    RAISE EXCEPTION 'Horimetro nao pode retroceder (atual %, informado %)', v_horimetro_atual, p_horimetro;
  END IF;

  INSERT INTO public.jr_manutencoes (
    maquina_id, plano_id, tipo, servico, horimetro_execucao,
    observacao, oficina, executado_por, os_id, status
  ) VALUES (
    p_maquina_id, p_plano_id, p_tipo, p_servico,
    COALESCE(p_horimetro, v_horimetro_atual),
    p_observacao, p_oficina, p_executado_por, p_os_id, 'aberta'
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================
-- RPC: jr_manutencao_adicionar_item
-- Roteia para jr_saida_estoque (novo) ou jr_saida_usado conforme modalidade.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencao_adicionar_item(
  p_manutencao_id  UUID,
  p_produto_id     UUID,
  p_quantidade     NUMERIC,
  p_modalidade     TEXT,
  p_lote_id        UUID DEFAULT NULL,
  p_condicao       TEXT DEFAULT NULL,
  p_observacao     TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_manut RECORD;
  v_lote_id UUID;
  v_custo_unit NUMERIC;
  v_mov_id UUID;
  v_doc TEXT;
  v_item_id UUID;
  v_saida jsonb;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT * INTO v_manut FROM public.jr_manutencoes WHERE id = p_manutencao_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_manut.status NOT IN ('aberta','em_andamento') THEN
    RAISE EXCEPTION 'Manutencao em status % nao aceita itens', v_manut.status;
  END IF;

  IF p_modalidade NOT IN ('novo','usado') THEN
    RAISE EXCEPTION 'Modalidade invalida';
  END IF;
  IF p_quantidade IS NULL OR p_quantidade <= 0 THEN
    RAISE EXCEPTION 'Quantidade invalida';
  END IF;

  v_doc := 'Manutencao ' || v_manut.id::TEXT;

  IF p_modalidade = 'novo' THEN
    v_lote_id := p_lote_id;
    IF v_lote_id IS NULL THEN
      v_lote_id := public.jr_sugerir_lote(p_produto_id, p_quantidade);
      IF v_lote_id IS NULL THEN
        RAISE EXCEPTION 'Sem lote disponivel (status=liberado) para produto %', p_produto_id;
      END IF;
    END IF;

    SELECT custo_unitario INTO v_custo_unit FROM public.jr_lotes WHERE id = v_lote_id;

    v_saida := public.jr_saida_estoque(
      p_lote_id    => v_lote_id,
      p_quantidade => p_quantidade,
      p_documento  => v_doc,
      p_maquina_id => v_manut.maquina_id,
      p_os_id      => v_manut.os_id
    );

    IF NOT (v_saida->>'ok')::BOOLEAN THEN
      RAISE EXCEPTION 'Saida de estoque falhou: %', v_saida->>'error';
    END IF;

    v_mov_id := (v_saida->>'movimentacao_id')::UUID;

    INSERT INTO public.jr_manutencao_itens (
      manutencao_id, produto_id, modalidade, lote_id, quantidade,
      custo_unitario, movimentacao_estoque_id, observacao
    ) VALUES (
      p_manutencao_id, p_produto_id, 'novo', v_lote_id, p_quantidade,
      COALESCE(v_custo_unit, 0), v_mov_id, p_observacao
    )
    RETURNING id INTO v_item_id;

  ELSE
    IF p_condicao IS NULL OR p_condicao NOT IN ('A','B','C') THEN
      RAISE EXCEPTION 'Condicao invalida (use A, B ou C)';
    END IF;

    SELECT custo_medio INTO v_custo_unit FROM public.jr_produtos WHERE id = p_produto_id;

    v_saida := public.jr_saida_usado(
      p_produto_id => p_produto_id,
      p_condicao   => p_condicao,
      p_quantidade => p_quantidade,
      p_documento  => v_doc,
      p_maquina_id => v_manut.maquina_id,
      p_os_id      => v_manut.os_id
    );

    IF NOT (v_saida->>'ok')::BOOLEAN THEN
      RAISE EXCEPTION 'Saida de usado falhou: %', v_saida->>'error';
    END IF;

    v_mov_id := (v_saida->>'movimentacao_id')::UUID;

    INSERT INTO public.jr_manutencao_itens (
      manutencao_id, produto_id, modalidade, condicao, quantidade,
      custo_unitario, movimentacao_estoque_id, observacao
    ) VALUES (
      p_manutencao_id, p_produto_id, 'usado', p_condicao, p_quantidade,
      COALESCE(v_custo_unit, 0), v_mov_id, p_observacao
    )
    RETURNING id INTO v_item_id;
  END IF;

  -- Atualiza custo da manutencao + status
  UPDATE public.jr_manutencoes
     SET custo_total_calc = (
           SELECT COALESCE(SUM(custo_total), 0)
             FROM public.jr_manutencao_itens
            WHERE manutencao_id = p_manutencao_id
         ),
         status = CASE WHEN status = 'aberta' THEN 'em_andamento' ELSE status END
   WHERE id = p_manutencao_id;

  RETURN jsonb_build_object('ok', true, 'item_id', v_item_id, 'movimentacao_id', v_mov_id);
END;
$$;

-- ============================================================
-- RPC: jr_manutencao_remover_item (reverte movimentacao via entrada compensatoria)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencao_remover_item(p_item_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_item RECORD;
  v_manut RECORD;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para remover item';
  END IF;

  SELECT * INTO v_item FROM public.jr_manutencao_itens WHERE id = p_item_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Item nao encontrado'; END IF;

  SELECT * INTO v_manut FROM public.jr_manutencoes WHERE id = v_item.manutencao_id;
  IF v_manut.status NOT IN ('aberta','em_andamento') THEN
    RAISE EXCEPTION 'Manutencao concluida/cancelada nao permite remocao direta de item';
  END IF;

  -- Compensacao: registra movimentacao oposta (entrada) com mesmo lote/condicao
  IF v_item.modalidade = 'novo' THEN
    INSERT INTO public.jr_estoque_movimentacao (
      tipo, modalidade, produto_id, lote_id, quantidade,
      custo_unitario, documento_origem, observacao
    ) VALUES (
      'entrada', 'novo', v_item.produto_id, v_item.lote_id, v_item.quantidade,
      v_item.custo_unitario, 'Reversao manutencao ' || v_manut.id::TEXT,
      'Compensacao remocao de item'
    );
  ELSE
    INSERT INTO public.jr_estoque_movimentacao (
      tipo, modalidade, produto_id, condicao, quantidade,
      custo_unitario, documento_origem, observacao
    ) VALUES (
      'entrada', 'usado', v_item.produto_id, v_item.condicao, v_item.quantidade,
      v_item.custo_unitario, 'Reversao manutencao ' || v_manut.id::TEXT,
      'Compensacao remocao de item'
    );
  END IF;

  DELETE FROM public.jr_manutencao_itens WHERE id = p_item_id;

  UPDATE public.jr_manutencoes
     SET custo_total_calc = (
       SELECT COALESCE(SUM(custo_total),0) FROM public.jr_manutencao_itens WHERE manutencao_id = v_manut.id
     )
   WHERE id = v_manut.id;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_manutencao_finalizar
-- Atualiza maquina.horimetro_atual; recalcula proximo vencimento;
-- atualiza jr_planos_maquina; cria despesa tipo=manutencao agregada.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencao_finalizar(
  p_manutencao_id   UUID,
  p_horimetro_final NUMERIC,
  p_data_execucao   DATE DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_manut RECORD;
  v_plano RECORD;
  v_horimetro_maq NUMERIC;
  v_proximo_h NUMERIC;
  v_proxima_d DATE;
  v_despesa_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para finalizar manutencao';
  END IF;

  SELECT * INTO v_manut FROM public.jr_manutencoes WHERE id = p_manutencao_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_manut.status NOT IN ('aberta','em_andamento') THEN
    RAISE EXCEPTION 'Manutencao em status % nao pode ser finalizada', v_manut.status;
  END IF;

  SELECT horimetro_atual INTO v_horimetro_maq FROM public.jr_maquinas WHERE id = v_manut.maquina_id;
  IF p_horimetro_final < v_horimetro_maq THEN
    RAISE EXCEPTION 'Horimetro final (%) < horimetro atual da maquina (%)', p_horimetro_final, v_horimetro_maq;
  END IF;

  -- Atualiza maquina
  UPDATE public.jr_maquinas
     SET horimetro_atual = GREATEST(horimetro_atual, p_horimetro_final)
   WHERE id = v_manut.maquina_id;

  -- Atualiza/Cria plano_maquina e calcula proximo vencimento
  IF v_manut.plano_id IS NOT NULL THEN
    SELECT * INTO v_plano FROM public.jr_planos_manutencao WHERE id = v_manut.plano_id;

    INSERT INTO public.jr_planos_maquina (
      plano_id, maquina_id, ultima_execucao_horimetro, ultima_execucao_data
    ) VALUES (
      v_manut.plano_id, v_manut.maquina_id, p_horimetro_final, COALESCE(p_data_execucao, CURRENT_DATE)
    )
    ON CONFLICT (plano_id, maquina_id) DO UPDATE
      SET ultima_execucao_horimetro = EXCLUDED.ultima_execucao_horimetro,
          ultima_execucao_data      = EXCLUDED.ultima_execucao_data;

    v_proximo_h := CASE WHEN v_plano.intervalo_horas IS NOT NULL
                        THEN p_horimetro_final + v_plano.intervalo_horas END;
    v_proxima_d := CASE WHEN v_plano.intervalo_dias IS NOT NULL
                        THEN COALESCE(p_data_execucao, CURRENT_DATE) + v_plano.intervalo_dias END;
  END IF;

  -- Despesa agregada da manutencao (consumos + mao de obra externa)
  IF v_manut.custo_total_calc > 0 THEN
    INSERT INTO public.jr_despesas (
      data, tipo, valor, descricao, maquina_id, os_id
    ) VALUES (
      COALESCE(p_data_execucao, CURRENT_DATE),
      'manutencao', v_manut.custo_total_calc,
      v_manut.tipo || ' - ' || v_manut.servico,
      v_manut.maquina_id, v_manut.os_id
    )
    RETURNING id INTO v_despesa_id;
  END IF;

  UPDATE public.jr_manutencoes
     SET status              = 'concluida',
         data_execucao       = COALESCE(p_data_execucao, CURRENT_DATE),
         data_encerramento   = CURRENT_DATE,
         horimetro_execucao  = p_horimetro_final,
         proximo_horimetro   = v_proximo_h,
         proxima_data        = v_proxima_d
   WHERE id = p_manutencao_id;

  RETURN jsonb_build_object(
    'ok', true,
    'horimetro_atual', p_horimetro_final,
    'proximo_horimetro', v_proximo_h,
    'proxima_data', v_proxima_d,
    'despesa_id', v_despesa_id
  );
END;
$$;

-- ============================================================
-- RPC: jr_manutencao_cancelar
-- Reverte itens consumidos via movimentacoes compensatorias.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencao_cancelar(
  p_manutencao_id UUID,
  p_motivo        TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_manut RECORD;
  v_item RECORD;
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para cancelar manutencao';
  END IF;

  IF p_motivo IS NULL OR length(trim(p_motivo)) < 3 THEN
    RAISE EXCEPTION 'Motivo do cancelamento obrigatorio (>=3 caracteres)';
  END IF;

  SELECT * INTO v_manut FROM public.jr_manutencoes WHERE id = p_manutencao_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Manutencao nao encontrada'; END IF;
  IF v_manut.status = 'cancelada' THEN
    RAISE EXCEPTION 'Manutencao ja cancelada';
  END IF;

  -- Reverte cada item via entrada compensatoria
  FOR v_item IN SELECT * FROM public.jr_manutencao_itens WHERE manutencao_id = p_manutencao_id LOOP
    IF v_item.modalidade = 'novo' THEN
      INSERT INTO public.jr_estoque_movimentacao (
        tipo, modalidade, produto_id, lote_id, quantidade,
        custo_unitario, documento_origem, observacao
      ) VALUES (
        'entrada','novo', v_item.produto_id, v_item.lote_id, v_item.quantidade,
        v_item.custo_unitario, 'Cancelamento manutencao ' || p_manutencao_id::TEXT,
        'Reversao automatica'
      );
    ELSE
      INSERT INTO public.jr_estoque_movimentacao (
        tipo, modalidade, produto_id, condicao, quantidade,
        custo_unitario, documento_origem, observacao
      ) VALUES (
        'entrada','usado', v_item.produto_id, v_item.condicao, v_item.quantidade,
        v_item.custo_unitario, 'Cancelamento manutencao ' || p_manutencao_id::TEXT,
        'Reversao automatica'
      );
    END IF;
  END LOOP;

  UPDATE public.jr_manutencoes
     SET status = 'cancelada',
         cancelada_motivo = p_motivo,
         data_encerramento = CURRENT_DATE
   WHERE id = p_manutencao_id;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_manutencoes_alertas
-- Lista planos por maquina classificados (atrasada/vence_em_breve/em_dia).
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_manutencoes_alertas(p_maquina_id UUID DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_threshold_h NUMERIC;
  v_threshold_d INT;
  v_lista jsonb;
BEGIN
  IF public.jr_current_perfil() IS NULL THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT COALESCE(NULLIF(valor,'')::NUMERIC, 20) INTO v_threshold_h
    FROM public.jr_config WHERE chave = 'alerta_manut_horas_antes';
  SELECT COALESCE(NULLIF(valor,'')::INT, 15) INTO v_threshold_d
    FROM public.jr_config WHERE chave = 'alerta_manut_dias_antes';

  SELECT COALESCE(jsonb_agg(x ORDER BY severidade_ord, COALESCE(horas_restantes, 999999)), '[]'::jsonb)
    INTO v_lista
    FROM (
      SELECT v.*,
        CASE
          WHEN (v.horas_restantes IS NOT NULL AND v.horas_restantes <= 0)
            OR (v.dias_restantes  IS NOT NULL AND v.dias_restantes  <= 0)  THEN 'atrasada'
          WHEN (v.horas_restantes IS NOT NULL AND v.horas_restantes <= v_threshold_h)
            OR (v.dias_restantes  IS NOT NULL AND v.dias_restantes  <= v_threshold_d) THEN 'vence_em_breve'
          ELSE 'em_dia'
        END AS severidade,
        CASE
          WHEN (v.horas_restantes IS NOT NULL AND v.horas_restantes <= 0)
            OR (v.dias_restantes  IS NOT NULL AND v.dias_restantes  <= 0)  THEN 0
          WHEN (v.horas_restantes IS NOT NULL AND v.horas_restantes <= v_threshold_h)
            OR (v.dias_restantes  IS NOT NULL AND v.dias_restantes  <= v_threshold_d) THEN 1
          ELSE 2
        END AS severidade_ord
      FROM public.v_jr_planos_maquina_status v
      WHERE p_maquina_id IS NULL OR v.maquina_id = p_maquina_id
    ) x;

  RETURN jsonb_build_object('ok', true, 'alertas', v_lista);
END;
$$;

-- ============================================================
-- GRANTS
-- ============================================================
GRANT EXECUTE ON FUNCTION public.jr_manutencao_abrir              TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_manutencao_adicionar_item     TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_manutencao_remover_item       TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_manutencao_finalizar          TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_manutencao_cancelar           TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_manutencoes_alertas           TO authenticated;
