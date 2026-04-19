-- ============================================================
-- Migration: Fase J5 - NFe (tabelas + RPC importar)
-- ============================================================

-- ============================================================
-- TABELA: jr_notas_fiscais (NFe entrada)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_notas_fiscais (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chave_acesso    TEXT NOT NULL UNIQUE CHECK (length(chave_acesso) = 44),
  numero          TEXT,
  serie           TEXT,
  data_emissao    DATE,
  fornecedor_id   UUID REFERENCES public.jr_fornecedores(id),
  valor_total     NUMERIC(14,2),
  tipo            TEXT NOT NULL DEFAULT 'entrada' CHECK (tipo IN ('entrada','servico')),
  xml_raw         TEXT,
  status          TEXT NOT NULL DEFAULT 'pendente' CHECK (status IN ('pendente','processada','erro')),
  observacao      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_nf_data       ON public.jr_notas_fiscais (data_emissao DESC);
CREATE INDEX IF NOT EXISTS idx_jr_nf_fornecedor ON public.jr_notas_fiscais (fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_jr_nf_status     ON public.jr_notas_fiscais (status);

DROP TRIGGER IF EXISTS tg_jr_nf_updated_at ON public.jr_notas_fiscais;
CREATE TRIGGER tg_jr_nf_updated_at
BEFORE INSERT OR UPDATE ON public.jr_notas_fiscais
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_notas_fiscais_itens
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_notas_fiscais_itens (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nota_id           UUID NOT NULL REFERENCES public.jr_notas_fiscais(id) ON DELETE CASCADE,
  produto_id        UUID REFERENCES public.jr_produtos(id),
  codigo_ean        TEXT,
  descricao_nfe     TEXT,
  ncm               TEXT,
  cfop              TEXT,
  unidade_nfe       TEXT,
  quantidade        NUMERIC(14,3),
  valor_unitario    NUMERIC(14,4),
  valor_total       NUMERIC(14,2),
  lote_fornecedor   TEXT,
  data_fabricacao   DATE,
  data_validade     DATE,
  lote_id           UUID REFERENCES public.jr_lotes(id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jr_nfi_nota    ON public.jr_notas_fiscais_itens (nota_id);
CREATE INDEX IF NOT EXISTS idx_jr_nfi_produto ON public.jr_notas_fiscais_itens (produto_id);

ALTER TABLE public.jr_notas_fiscais       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_notas_fiscais_itens ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['jr_notas_fiscais','jr_notas_fiscais_itens'] LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','financeiro'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor','financeiro'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

-- ============================================================
-- RPC: jr_importar_nfe
-- p_payload: {
--   chave, numero, serie, data_emissao, valor_total,
--   fornecedor: { cnpj, razao_social },
--   itens: [
--     { produto_id, ean, descricao_nfe, ncm, cfop, unidade,
--       quantidade, valor_unitario, valor_total,
--       lote_fornecedor, data_fabricacao, data_validade }
--   ]
-- }
-- Cria fornecedor se necessario, NF, 1 lote por item, movimentacoes.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_importar_nfe(p_payload jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_chave TEXT;
  v_forn_id UUID;
  v_forn_cnpj TEXT;
  v_forn_nome TEXT;
  v_nf_id UUID;
  v_item jsonb;
  v_prod_id UUID;
  v_lote_id UUID;
  v_lote_interno TEXT;
  v_qt NUMERIC;
  v_valor_unit NUMERIC;
  v_lotes_criados jsonb := '[]'::jsonb;
  v_itens_count INT := 0;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para importar NFe';
  END IF;

  v_chave := p_payload->>'chave';
  IF v_chave IS NULL OR length(v_chave) <> 44 THEN
    RAISE EXCEPTION 'Chave de acesso invalida (44 digitos)';
  END IF;

  IF EXISTS (SELECT 1 FROM public.jr_notas_fiscais WHERE chave_acesso = v_chave) THEN
    RAISE EXCEPTION 'NFe ja importada (chave duplicada): %', v_chave;
  END IF;

  -- Fornecedor: upsert por CNPJ
  v_forn_cnpj := p_payload->'fornecedor'->>'cnpj';
  v_forn_nome := COALESCE(p_payload->'fornecedor'->>'razao_social', 'NF ' || v_chave);
  IF v_forn_cnpj IS NOT NULL THEN
    SELECT id INTO v_forn_id FROM public.jr_fornecedores WHERE cnpj = v_forn_cnpj;
    IF v_forn_id IS NULL THEN
      INSERT INTO public.jr_fornecedores (razao_social, cnpj, tipo)
        VALUES (v_forn_nome, v_forn_cnpj, 'peca')
      RETURNING id INTO v_forn_id;
    END IF;
  END IF;

  -- Cria NF
  INSERT INTO public.jr_notas_fiscais (
    chave_acesso, numero, serie, data_emissao, fornecedor_id,
    valor_total, tipo, xml_raw, status
  ) VALUES (
    v_chave,
    p_payload->>'numero',
    p_payload->>'serie',
    NULLIF(p_payload->>'data_emissao','')::DATE,
    v_forn_id,
    NULLIF(p_payload->>'valor_total','')::NUMERIC,
    'entrada',
    p_payload->>'xml_raw',
    'processada'
  ) RETURNING id INTO v_nf_id;

  -- Itens
  FOR v_item IN SELECT * FROM jsonb_array_elements(p_payload->'itens') LOOP
    v_prod_id := NULLIF(v_item->>'produto_id','')::UUID;
    IF v_prod_id IS NULL THEN
      RAISE EXCEPTION 'Item sem produto vinculado (descricao=%)', v_item->>'descricao_nfe';
    END IF;

    v_qt := COALESCE(NULLIF(v_item->>'quantidade','')::NUMERIC, 0);
    v_valor_unit := COALESCE(NULLIF(v_item->>'valor_unitario','')::NUMERIC, 0);
    IF v_qt <= 0 THEN
      RAISE EXCEPTION 'Quantidade invalida no item (descricao=%)', v_item->>'descricao_nfe';
    END IF;

    -- Cria lote
    INSERT INTO public.jr_lotes (
      produto_id, lote_fornecedor, fornecedor_id, nota_fiscal_id,
      data_entrada, data_fabricacao, data_validade, custo_unitario
    ) VALUES (
      v_prod_id,
      NULLIF(v_item->>'lote_fornecedor',''),
      v_forn_id,
      v_nf_id,
      CURRENT_DATE,
      NULLIF(v_item->>'data_fabricacao','')::DATE,
      NULLIF(v_item->>'data_validade','')::DATE,
      v_valor_unit
    ) RETURNING id, lote_interno INTO v_lote_id, v_lote_interno;

    -- Movimentacao entrada NOVO
    INSERT INTO public.jr_estoque_movimentacao (
      tipo, modalidade, produto_id, lote_id, ean_lido,
      quantidade, custo_unitario, documento_origem, observacao
    ) VALUES (
      'entrada', 'novo', v_prod_id, v_lote_id, v_item->>'ean',
      v_qt, v_valor_unit, 'NFe ' || COALESCE(p_payload->>'numero','') || '/' || COALESCE(p_payload->>'serie',''),
      'Importacao NFe ' || v_chave
    );

    -- Salva item da NF
    INSERT INTO public.jr_notas_fiscais_itens (
      nota_id, produto_id, codigo_ean, descricao_nfe, ncm, cfop,
      unidade_nfe, quantidade, valor_unitario, valor_total,
      lote_fornecedor, data_fabricacao, data_validade, lote_id
    ) VALUES (
      v_nf_id, v_prod_id,
      v_item->>'ean',
      v_item->>'descricao_nfe',
      v_item->>'ncm',
      v_item->>'cfop',
      v_item->>'unidade',
      v_qt, v_valor_unit,
      COALESCE(NULLIF(v_item->>'valor_total','')::NUMERIC, v_qt * v_valor_unit),
      NULLIF(v_item->>'lote_fornecedor',''),
      NULLIF(v_item->>'data_fabricacao','')::DATE,
      NULLIF(v_item->>'data_validade','')::DATE,
      v_lote_id
    );

    v_lotes_criados := v_lotes_criados || jsonb_build_object(
      'lote_interno', v_lote_interno,
      'lote_id', v_lote_id,
      'produto_id', v_prod_id,
      'quantidade', v_qt
    );
    v_itens_count := v_itens_count + 1;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'nota_id', v_nf_id,
    'fornecedor_id', v_forn_id,
    'itens_processados', v_itens_count,
    'lotes_criados', v_lotes_criados
  );
END;
$$;

GRANT EXECUTE ON FUNCTION public.jr_importar_nfe TO authenticated;

COMMENT ON FUNCTION public.jr_importar_nfe IS 'Importa XML NFe ja parseado: cria fornecedor + NF + 1 lote por item + movimentacoes. Idempotente por chave.';
