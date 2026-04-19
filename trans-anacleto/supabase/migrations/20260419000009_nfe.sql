-- =====================================================================
-- Fase 3.5: Entrada por XML NFe
-- =====================================================================
-- - trans_notas_fiscais        : cabecalho NFe/NFS-e (chave UQ, emit, ide)
-- - trans_notas_fiscais_itens  : itens extraidos do XML (det/prod/rastro)
-- - FK trans_lotes.nota_fiscal_id -> trans_notas_fiscais.id
-- - RPC trans_importar_nfe     : recebe chave+itens mapeados; cria 1 lote por item
-- =====================================================================

-- -------------------------------------------------------------
-- trans_notas_fiscais
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_notas_fiscais (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  chave_acesso text UNIQUE,               -- 44 digitos (NFe) ou nulo em NFS-e
  numero text NOT NULL,
  serie text,
  data_emissao timestamptz,
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  valor_total numeric(14,2),
  tipo text NOT NULL DEFAULT 'entrada'
    CHECK (tipo IN ('entrada','servico')),
  xml_raw text,
  status text NOT NULL DEFAULT 'processada'
    CHECK (status IN ('pendente','processada','erro')),
  erro_mensagem text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id)
);

CREATE INDEX idx_trans_nf_fornecedor ON trans_notas_fiscais(fornecedor_id);
CREATE INDEX idx_trans_nf_emissao ON trans_notas_fiscais(data_emissao DESC);
CREATE INDEX idx_trans_nf_tipo ON trans_notas_fiscais(tipo, status);

CREATE TRIGGER trans_nf_updated
BEFORE UPDATE ON trans_notas_fiscais
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- FK de lotes -> NF (referencia ja existia como uuid; agora constrain)
ALTER TABLE trans_lotes
  ADD CONSTRAINT fk_trans_lotes_nf
  FOREIGN KEY (nota_fiscal_id) REFERENCES trans_notas_fiscais(id);

-- -------------------------------------------------------------
-- trans_notas_fiscais_itens
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_notas_fiscais_itens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nota_id uuid NOT NULL REFERENCES trans_notas_fiscais(id) ON DELETE CASCADE,
  produto_id uuid REFERENCES trans_produtos(id),
  lote_id uuid REFERENCES trans_lotes(id),
  codigo_ean text,
  codigo_produto_nfe text,
  descricao_nfe text,
  ncm text,
  cfop text,
  quantidade numeric(14,4) NOT NULL,
  unidade text,
  valor_unitario numeric(14,4),
  valor_total numeric(14,2),
  lote_fornecedor_nfe text,
  data_fabricacao_nfe date,
  data_validade_nfe date,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_trans_nf_itens_nota ON trans_notas_fiscais_itens(nota_id);
CREATE INDEX idx_trans_nf_itens_produto ON trans_notas_fiscais_itens(produto_id) WHERE produto_id IS NOT NULL;
CREATE INDEX idx_trans_nf_itens_ean ON trans_notas_fiscais_itens(codigo_ean) WHERE codigo_ean IS NOT NULL;

-- -------------------------------------------------------------
-- RLS
-- -------------------------------------------------------------
ALTER TABLE trans_notas_fiscais ENABLE ROW LEVEL SECURITY;
ALTER TABLE trans_notas_fiscais_itens ENABLE ROW LEVEL SECURITY;

CREATE POLICY trans_nf_sel ON trans_notas_fiscais FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_nf_ins ON trans_notas_fiscais FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_nf_upd ON trans_notas_fiscais FOR UPDATE TO authenticated USING (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
) WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_nf_del ON trans_notas_fiscais FOR DELETE TO authenticated USING (trans_is_admin());

CREATE POLICY trans_nf_itens_sel ON trans_notas_fiscais_itens FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_nf_itens_ins ON trans_notas_fiscais_itens FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_nf_itens_upd ON trans_notas_fiscais_itens FOR UPDATE TO authenticated USING (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
) WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_nf_itens_del ON trans_notas_fiscais_itens FOR DELETE TO authenticated USING (trans_is_admin());

-- -------------------------------------------------------------
-- RPC: trans_importar_nfe
-- -------------------------------------------------------------
-- Payload esperado:
--   p_chave: chave de acesso (44 digitos) — UQ; rejeita duplicata
--   p_dados: jsonb com cabecalho: {numero, serie, data_emissao, fornecedor_id,
--            valor_total, xml_raw, fornecedor:{cnpj, razao_social}}
--   p_itens: jsonb array; cada item:
--            {produto_id, codigo_ean, descricao_nfe, ncm, cfop,
--             quantidade, unidade, valor_unitario, valor_total,
--             lote_fornecedor_nfe, data_fabricacao_nfe, data_validade_nfe}
-- Comportamento:
--   - Se fornecedor_id ausente mas cnpj presente: cria ou recupera fornecedor
--   - Cria 1 nota fiscal
--   - Para cada item com produto_id: gera trans_lotes + movimentacao entrada
--   - Cria linha em trans_notas_fiscais_itens vinculando ao lote criado
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_importar_nfe(
  p_chave text,
  p_dados jsonb,
  p_itens jsonb
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_fornecedor_id uuid;
  v_cnpj text;
  v_razao text;
  v_nota_id uuid;
  v_item jsonb;
  v_produto_id uuid;
  v_lote_id uuid;
  v_lote_interno text;
  v_qtd numeric;
  v_itens_criados int := 0;
  v_itens_sem_produto int := 0;
  v_lotes jsonb := '[]'::jsonb;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Perfil sem permissao para importar NFe';
  END IF;

  IF p_chave IS NOT NULL AND EXISTS (SELECT 1 FROM trans_notas_fiscais WHERE chave_acesso = p_chave) THEN
    RAISE EXCEPTION 'NFe com chave % ja importada', p_chave;
  END IF;

  -- Fornecedor: resolve por fornecedor_id explicito ou cria via cnpj
  v_fornecedor_id := NULLIF(p_dados->>'fornecedor_id','')::uuid;

  IF v_fornecedor_id IS NULL THEN
    v_cnpj := p_dados->'fornecedor'->>'cnpj';
    v_razao := p_dados->'fornecedor'->>'razao_social';

    IF v_cnpj IS NOT NULL THEN
      SELECT id INTO v_fornecedor_id FROM trans_fornecedores
       WHERE regexp_replace(cnpj,'\D','','g') = regexp_replace(v_cnpj,'\D','','g')
       LIMIT 1;

      IF v_fornecedor_id IS NULL THEN
        INSERT INTO trans_fornecedores (razao_social, cnpj, tipo, created_by, updated_by)
        VALUES (COALESCE(v_razao, v_cnpj), v_cnpj, 'peca', auth.uid(), auth.uid())
        RETURNING id INTO v_fornecedor_id;
      END IF;
    END IF;
  END IF;

  -- Cabecalho
  INSERT INTO trans_notas_fiscais (
    chave_acesso, numero, serie, data_emissao, fornecedor_id,
    valor_total, tipo, xml_raw, status, created_by, updated_by
  ) VALUES (
    p_chave,
    COALESCE(p_dados->>'numero', 'SEM-NUMERO'),
    p_dados->>'serie',
    NULLIF(p_dados->>'data_emissao','')::timestamptz,
    v_fornecedor_id,
    NULLIF(p_dados->>'valor_total','')::numeric,
    COALESCE(p_dados->>'tipo','entrada'),
    p_dados->>'xml_raw',
    'processada',
    auth.uid(),
    auth.uid()
  ) RETURNING id INTO v_nota_id;

  -- Itens
  FOR v_item IN SELECT * FROM jsonb_array_elements(p_itens)
  LOOP
    v_produto_id := NULLIF(v_item->>'produto_id','')::uuid;
    v_qtd := NULLIF(v_item->>'quantidade','')::numeric;

    IF v_produto_id IS NULL OR v_qtd IS NULL OR v_qtd <= 0 THEN
      -- Item sem produto casado: grava linha de referencia sem lote
      INSERT INTO trans_notas_fiscais_itens (
        nota_id, codigo_ean, codigo_produto_nfe, descricao_nfe, ncm, cfop,
        quantidade, unidade, valor_unitario, valor_total,
        lote_fornecedor_nfe, data_fabricacao_nfe, data_validade_nfe
      ) VALUES (
        v_nota_id,
        v_item->>'codigo_ean',
        v_item->>'codigo_produto_nfe',
        v_item->>'descricao_nfe',
        v_item->>'ncm',
        v_item->>'cfop',
        COALESCE(v_qtd, 0),
        v_item->>'unidade',
        NULLIF(v_item->>'valor_unitario','')::numeric,
        NULLIF(v_item->>'valor_total','')::numeric,
        v_item->>'lote_fornecedor_nfe',
        NULLIF(v_item->>'data_fabricacao_nfe','')::date,
        NULLIF(v_item->>'data_validade_nfe','')::date
      );
      v_itens_sem_produto := v_itens_sem_produto + 1;
      CONTINUE;
    END IF;

    -- Gera lote + movimentacao de entrada
    v_lote_interno := trans_gerar_lote_interno();

    INSERT INTO trans_lotes (
      produto_id, lote_interno, lote_fornecedor, fornecedor_id,
      nota_fiscal_id, data_entrada, data_fabricacao, data_validade,
      status, custo_unitario, observacao, created_by, updated_by
    ) VALUES (
      v_produto_id, v_lote_interno,
      v_item->>'lote_fornecedor_nfe', v_fornecedor_id,
      v_nota_id, CURRENT_DATE,
      NULLIF(v_item->>'data_fabricacao_nfe','')::date,
      NULLIF(v_item->>'data_validade_nfe','')::date,
      'liberado',
      NULLIF(v_item->>'valor_unitario','')::numeric,
      'IMPORT-NFE: ' || COALESCE(p_dados->>'numero',''),
      auth.uid(), auth.uid()
    ) RETURNING id INTO v_lote_id;

    INSERT INTO trans_estoque_movimentacao (
      tipo, modalidade, produto_id, lote_id, ean_lido, quantidade,
      custo_unitario, usuario_id, documento_origem, observacao
    ) VALUES (
      'entrada','novo', v_produto_id, v_lote_id,
      v_item->>'codigo_ean', v_qtd,
      NULLIF(v_item->>'valor_unitario','')::numeric,
      auth.uid(),
      'NFE:' || COALESCE(p_chave, v_nota_id::text),
      'Import NFe ' || COALESCE(p_dados->>'numero','')
    );

    INSERT INTO trans_notas_fiscais_itens (
      nota_id, produto_id, lote_id, codigo_ean, codigo_produto_nfe,
      descricao_nfe, ncm, cfop, quantidade, unidade,
      valor_unitario, valor_total,
      lote_fornecedor_nfe, data_fabricacao_nfe, data_validade_nfe
    ) VALUES (
      v_nota_id, v_produto_id, v_lote_id,
      v_item->>'codigo_ean', v_item->>'codigo_produto_nfe',
      v_item->>'descricao_nfe', v_item->>'ncm', v_item->>'cfop',
      v_qtd, v_item->>'unidade',
      NULLIF(v_item->>'valor_unitario','')::numeric,
      NULLIF(v_item->>'valor_total','')::numeric,
      v_item->>'lote_fornecedor_nfe',
      NULLIF(v_item->>'data_fabricacao_nfe','')::date,
      NULLIF(v_item->>'data_validade_nfe','')::date
    );

    v_itens_criados := v_itens_criados + 1;
    v_lotes := v_lotes || jsonb_build_object(
      'lote_id', v_lote_id,
      'lote_interno', v_lote_interno,
      'produto_id', v_produto_id,
      'quantidade', v_qtd
    );
  END LOOP;

  RETURN json_build_object(
    'ok', true,
    'nota_id', v_nota_id,
    'chave', p_chave,
    'fornecedor_id', v_fornecedor_id,
    'itens_criados', v_itens_criados,
    'itens_sem_produto', v_itens_sem_produto,
    'lotes', v_lotes
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_importar_nfe(text, jsonb, jsonb) TO authenticated;

-- Listagem de notas (view com join de fornecedor + contagem itens)
CREATE OR REPLACE VIEW v_trans_notas_fiscais AS
SELECT
  n.id,
  n.chave_acesso,
  n.numero,
  n.serie,
  n.data_emissao,
  n.valor_total,
  n.tipo,
  n.status,
  n.fornecedor_id,
  f.razao_social AS fornecedor_razao,
  f.cnpj AS fornecedor_cnpj,
  (SELECT COUNT(*) FROM trans_notas_fiscais_itens i WHERE i.nota_id = n.id) AS itens_count,
  (SELECT COUNT(*) FROM trans_notas_fiscais_itens i WHERE i.nota_id = n.id AND i.lote_id IS NOT NULL) AS lotes_count,
  n.created_at
FROM trans_notas_fiscais n
LEFT JOIN trans_fornecedores f ON f.id = n.fornecedor_id;
