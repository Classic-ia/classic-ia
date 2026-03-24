-- ============================================================
-- RPC: rpc_criar_inspecao_completa
-- Transacao atomica: lote + recebimento + inspecao + defeitos
-- Se qualquer etapa falhar → rollback total
-- Todos os calculos sao backend (triggers)
--
-- Executar no Supabase SQL Editor (projeto Classic-ia CQ)
-- ============================================================

-- Tipo para receber array de defeitos
DO $$ BEGIN
  CREATE TYPE public.defeito_entrada AS (
    defeito_id uuid,
    quantidade numeric(14,3)
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- Funcao principal
CREATE OR REPLACE FUNCTION public.rpc_criar_inspecao_completa(
  -- Obrigatorios (sem default) — devem vir primeiro
  p_fornecedor_id        uuid,
  p_produto_id           uuid,
  p_numero_lote          text,
  p_quantidade_recebida  numeric(14,3),
  p_quantidade_analisada numeric(14,3),
  -- Opcionais (com default) — devem vir depois
  p_data_lote            date DEFAULT current_date,
  p_temperatura          numeric(5,1) DEFAULT NULL,
  p_placa_caminhao       text DEFAULT NULL,
  p_motorista            text DEFAULT NULL,
  p_transportadora       text DEFAULT NULL,
  p_nota_fiscal          text DEFAULT NULL,
  p_observacoes          text DEFAULT NULL,
  p_defeitos             jsonb DEFAULT '[]'::jsonb,
  p_qtd_a                numeric(14,3) DEFAULT NULL,
  p_qtd_b                numeric(14,3) DEFAULT NULL,
  p_qtd_c                numeric(14,3) DEFAULT NULL,
  p_foto_etiqueta_url    text DEFAULT NULL,
  p_foto_produto_url     text DEFAULT NULL,
  p_foto_defeito_url     text DEFAULT NULL,
  p_video_evidencia_url  text DEFAULT NULL,
  p_origem               text DEFAULT 'manual'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_lote_id           uuid;
  v_recebimento_id    uuid;
  v_inspecao_id       uuid;
  v_defeito           jsonb;
  v_defeito_id        uuid;
  v_defeito_qtd       numeric(14,3);
  v_defeito_count     integer := 0;
  v_total_defeitos    numeric(14,3);
  v_perc_defeitos     numeric(10,4);
  v_classificacao     text;
  v_status_final      text;
  v_user_id           uuid;
  v_fornecedor_nome   text;
  v_produto_nome      text;
BEGIN

  -- ══════════════════════════════════════════════════
  -- VALIDACOES PRE-TRANSACAO
  -- ══════════════════════════════════════════════════

  -- Usuario autenticado
  v_user_id := auth.uid();

  -- Fornecedor obrigatorio e deve existir
  IF p_fornecedor_id IS NULL THEN
    RAISE EXCEPTION 'fornecedor_id e obrigatorio';
  END IF;
  SELECT nome INTO v_fornecedor_nome
    FROM public.cq_fornecedores
    WHERE id = p_fornecedor_id AND ativo = true;
  IF v_fornecedor_nome IS NULL THEN
    RAISE EXCEPTION 'Fornecedor nao encontrado ou inativo: %', p_fornecedor_id;
  END IF;

  -- Produto obrigatorio e deve existir
  IF p_produto_id IS NULL THEN
    RAISE EXCEPTION 'produto_id e obrigatorio';
  END IF;
  SELECT nome INTO v_produto_nome
    FROM public.cq_produtos_v2
    WHERE id = p_produto_id AND ativo = true;
  IF v_produto_nome IS NULL THEN
    RAISE EXCEPTION 'Produto nao encontrado ou inativo: %', p_produto_id;
  END IF;

  -- Numero do lote obrigatorio
  IF p_numero_lote IS NULL OR trim(p_numero_lote) = '' THEN
    RAISE EXCEPTION 'numero_lote e obrigatorio';
  END IF;

  -- Quantidade recebida > 0
  IF p_quantidade_recebida IS NULL OR p_quantidade_recebida <= 0 THEN
    RAISE EXCEPTION 'quantidade_recebida deve ser maior que zero';
  END IF;

  -- Quantidade analisada > 0 e <= recebida
  IF p_quantidade_analisada IS NULL OR p_quantidade_analisada <= 0 THEN
    RAISE EXCEPTION 'quantidade_analisada deve ser maior que zero';
  END IF;
  IF p_quantidade_analisada > p_quantidade_recebida THEN
    RAISE EXCEPTION 'quantidade_analisada (%) nao pode ser maior que quantidade_recebida (%)',
      p_quantidade_analisada, p_quantidade_recebida;
  END IF;

  -- Validar defeitos: cada item deve ter defeito_id valido e quantidade >= 0
  IF p_defeitos IS NOT NULL AND jsonb_array_length(p_defeitos) > 0 THEN
    FOR v_defeito IN SELECT * FROM jsonb_array_elements(p_defeitos)
    LOOP
      v_defeito_id := (v_defeito->>'defeito_id')::uuid;
      v_defeito_qtd := (v_defeito->>'quantidade')::numeric;

      IF v_defeito_id IS NULL THEN
        RAISE EXCEPTION 'defeito_id nulo no array de defeitos';
      END IF;
      IF NOT EXISTS (SELECT 1 FROM public.cq_defeitos WHERE id = v_defeito_id AND ativo = true) THEN
        RAISE EXCEPTION 'Defeito nao encontrado ou inativo: %', v_defeito_id;
      END IF;
      IF v_defeito_qtd IS NULL OR v_defeito_qtd < 0 THEN
        RAISE EXCEPTION 'Quantidade do defeito deve ser >= 0 para defeito %', v_defeito_id;
      END IF;
    END LOOP;
  END IF;

  -- Validar A+B+C se informados
  IF p_qtd_a IS NOT NULL AND p_qtd_b IS NOT NULL AND p_qtd_c IS NOT NULL THEN
    IF round(p_qtd_a + p_qtd_b + p_qtd_c, 3) <> round(p_quantidade_recebida, 3) THEN
      RAISE EXCEPTION 'Classificacao inconsistente: A(%) + B(%) + C(%) = % <> quantidade recebida (%)',
        p_qtd_a, p_qtd_b, p_qtd_c, p_qtd_a + p_qtd_b + p_qtd_c, p_quantidade_recebida;
    END IF;
  END IF;

  -- ══════════════════════════════════════════════════
  -- PASSO 1: UPSERT LOTE
  -- ══════════════════════════════════════════════════
  INSERT INTO public.cq_lotes (numero_lote, fornecedor_id, produto_id, data_lote, origem)
  VALUES (trim(p_numero_lote), p_fornecedor_id, p_produto_id, p_data_lote, p_origem)
  ON CONFLICT (numero_lote, fornecedor_id, produto_id)
    DO UPDATE SET atualizado_em = now()
  RETURNING id INTO v_lote_id;

  -- ══════════════════════════════════════════════════
  -- PASSO 2: CRIAR RECEBIMENTO
  -- ══════════════════════════════════════════════════
  INSERT INTO public.cq_recebimentos_v2 (
    lote_id, data_recebimento, quantidade_recebida, unidade_medida,
    temperatura, placa_caminhao, motorista, transportadora,
    nota_fiscal, origem, status_recebimento,
    foto_etiqueta_url, foto_carga_url,
    criado_por
  ) VALUES (
    v_lote_id, now(), p_quantidade_recebida, 'UN',
    p_temperatura, upper(trim(p_placa_caminhao)), trim(p_motorista), trim(p_transportadora),
    trim(p_nota_fiscal), p_origem, 'em_inspecao',
    p_foto_etiqueta_url, NULL,
    v_user_id
  )
  RETURNING id INTO v_recebimento_id;

  -- ══════════════════════════════════════════════════
  -- PASSO 3: CRIAR INSPECAO
  -- ══════════════════════════════════════════════════
  INSERT INTO public.cq_inspecoes (
    recebimento_id, data_inspecao, quantidade_analisada,
    observacoes,
    foto_etiqueta_url, foto_produto_url, foto_defeito_url, video_evidencia_url,
    criado_por
  ) VALUES (
    v_recebimento_id, now(), p_quantidade_analisada,
    trim(p_observacoes),
    p_foto_etiqueta_url, p_foto_produto_url, p_foto_defeito_url, p_video_evidencia_url,
    v_user_id
  )
  RETURNING id INTO v_inspecao_id;

  -- ══════════════════════════════════════════════════
  -- PASSO 4: INSERIR DEFEITOS (trigger recalcula)
  -- ══════════════════════════════════════════════════
  IF p_defeitos IS NOT NULL AND jsonb_array_length(p_defeitos) > 0 THEN
    FOR v_defeito IN SELECT * FROM jsonb_array_elements(p_defeitos)
    LOOP
      v_defeito_id := (v_defeito->>'defeito_id')::uuid;
      v_defeito_qtd := (v_defeito->>'quantidade')::numeric;

      -- So insere se quantidade > 0
      IF v_defeito_qtd > 0 THEN
        INSERT INTO public.cq_inspecao_defeitos (
          inspecao_id, defeito_id, quantidade,
          percentual, criticidade
        ) VALUES (
          v_inspecao_id,
          v_defeito_id,
          v_defeito_qtd,
          CASE WHEN p_quantidade_analisada > 0
            THEN round((v_defeito_qtd / p_quantidade_analisada) * 100, 4)
            ELSE 0
          END,
          (SELECT criticidade_padrao FROM public.cq_defeitos WHERE id = v_defeito_id)
        );
        v_defeito_count := v_defeito_count + 1;
      END IF;
    END LOOP;
  END IF;

  -- ══════════════════════════════════════════════════
  -- PASSO 5: CLASSIFICACAO A+B+C (se informada)
  -- ══════════════════════════════════════════════════
  IF p_qtd_a IS NOT NULL AND p_qtd_b IS NOT NULL AND p_qtd_c IS NOT NULL THEN
    INSERT INTO public.cq_classificacoes_recebimento (
      recebimento_id, qtd_a, qtd_b, qtd_c
    ) VALUES (
      v_recebimento_id, p_qtd_a, p_qtd_b, p_qtd_c
    );
    -- Trigger fn_validar_classificacao_recebimento valida A+B+C = qtd_recebida
  END IF;

  -- ══════════════════════════════════════════════════
  -- PASSO 6: ATUALIZAR STATUS DO RECEBIMENTO
  -- ══════════════════════════════════════════════════
  UPDATE public.cq_recebimentos_v2
    SET status_recebimento = 'inspecionado'
  WHERE id = v_recebimento_id;

  -- ══════════════════════════════════════════════════
  -- PASSO 7: LER RESULTADO (trigger ja recalculou)
  -- ══════════════════════════════════════════════════
  SELECT total_defeitos, perc_defeitos, classificacao_abc, status_final
    INTO v_total_defeitos, v_perc_defeitos, v_classificacao, v_status_final
  FROM public.cq_inspecoes
  WHERE id = v_inspecao_id;

  -- ══════════════════════════════════════════════════
  -- RETORNO
  -- ══════════════════════════════════════════════════
  RETURN jsonb_build_object(
    'sucesso',            true,
    'inspecao_id',        v_inspecao_id,
    'lote_id',            v_lote_id,
    'recebimento_id',     v_recebimento_id,
    'fornecedor_nome',    v_fornecedor_nome,
    'produto_nome',       v_produto_nome,
    'numero_lote',        p_numero_lote,
    'quantidade_recebida', p_quantidade_recebida,
    'quantidade_analisada', p_quantidade_analisada,
    'total_defeitos',     v_total_defeitos,
    'perc_defeitos',      v_perc_defeitos,
    'classificacao_abc',  v_classificacao,
    'status_final',       v_status_final,
    'defeitos_inseridos', v_defeito_count
  );

EXCEPTION WHEN OTHERS THEN
  -- Qualquer erro → rollback automatico (PLpgSQL faz isso)
  RETURN jsonb_build_object(
    'sucesso', false,
    'erro',    SQLERRM,
    'codigo',  SQLSTATE
  );
END;
$$;

-- Permissao para usuarios autenticados chamarem a RPC
GRANT EXECUTE ON FUNCTION public.rpc_criar_inspecao_completa TO authenticated;
GRANT EXECUTE ON FUNCTION public.rpc_criar_inspecao_completa TO anon;


-- ============================================================
-- TESTE DA RPC (executar depois de popular fornecedores)
-- ============================================================

-- Teste: criar inspecao completa em uma unica chamada
/*
SELECT public.rpc_criar_inspecao_completa(
  p_fornecedor_id := (SELECT id FROM cq_fornecedores WHERE codigo_atak = '60003' LIMIT 1),
  p_produto_id := (SELECT id FROM cq_produtos_v2 WHERE codigo_interno = 'bucho' LIMIT 1),
  p_numero_lote := 'TESTE-001',
  p_quantidade_recebida := 200,
  p_quantidade_analisada := 200,
  p_placa_caminhao := 'ABC1234',
  p_motorista := 'TESTE MOTORISTA',
  p_defeitos := (
    SELECT jsonb_agg(jsonb_build_object('defeito_id', id, 'quantidade', 10))
    FROM cq_defeitos WHERE codigo IN ('fermentacao', 'rasgado', 'furado')
  )
);
*/

-- Teste com A+B+C:
/*
SELECT public.rpc_criar_inspecao_completa(
  p_fornecedor_id := (SELECT id FROM cq_fornecedores WHERE codigo_atak = '60003' LIMIT 1),
  p_produto_id := (SELECT id FROM cq_produtos_v2 WHERE codigo_interno = 'omaso' LIMIT 1),
  p_numero_lote := 'TESTE-002',
  p_quantidade_recebida := 100,
  p_quantidade_analisada := 100,
  p_defeitos := '[{"defeito_id":"uuid-aqui","quantidade":5}]'::jsonb,
  p_qtd_a := 80,
  p_qtd_b := 15,
  p_qtd_c := 5
);
*/
