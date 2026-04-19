-- =====================================================================
-- 20260419000015_servicos.sql
-- =====================================================================
-- Modulo Servicos Externos (NFS-e) da Trans Anacleto.
--
-- Objetivo: registrar servicos executados por oficinas externas /
-- recapadoras / borracharias, vinculando ao veiculo, opcionalmente ao
-- pneu (recapagem, borracharia) e/ou ao produto consumido (troca de
-- oleo, filtros etc), com integracao automatica:
--
--   - tipo=recapagem + pneu_id -> INSERT em trans_pneus_recapagem
--     (status=retornado, custo, sulco_pos_mm, numero_recapagem++);
--     se pneu estava em status=recapagem -> volta para estoque;
--     atualiza sulco_atual_mm e numero_recapagens.
--
--   - produto_id + quantidade_produto -> chama trans_sugerir_lote (FEFO)
--     e trans_saida_estoque vinculando ao veiculo (documento_origem
--     = SERV-{first8}).
--
-- Diferente de NFe de entrada, NFS-e nao gera lote - e consumo/servico
-- (saida). O arquivo PDF pode ser anexado via anexo_url. Caso o usuario
-- informe nota_fiscal_id, a trans_notas_fiscais deve ser tipo=servico.
-- =====================================================================

CREATE TABLE IF NOT EXISTS trans_servicos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo text NOT NULL CHECK (tipo IN (
    'recapagem','troca_oleo','borracharia','funilaria',
    'revisao','alinhamento','balanceamento','lavagem','outro'
  )),
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id),
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  data date NOT NULL DEFAULT CURRENT_DATE,
  valor numeric(12,2) NOT NULL DEFAULT 0 CHECK (valor >= 0),
  descricao text,
  km_ocorrencia numeric,

  -- integracao pneu (tipo=recapagem/borracharia)
  pneu_id uuid REFERENCES trans_pneus(id),
  sulco_pos_mm numeric,
  recapagem_id uuid REFERENCES trans_pneus_recapagem(id),

  -- integracao estoque (consumo de peca fornecida por terceiros ou
  -- baixa de peca propria enviada a oficina externa)
  produto_id uuid REFERENCES trans_produtos(id),
  quantidade_produto numeric,
  movimentacao_estoque_id uuid REFERENCES trans_estoque_movimentacao(id),

  -- vinculos opcionais
  manutencao_id uuid REFERENCES trans_manutencoes(id),
  nota_fiscal_id uuid REFERENCES trans_notas_fiscais(id),
  anexo_url text,

  status text NOT NULL DEFAULT 'processado' CHECK (status IN (
    'pendente','processado','cancelado'
  )),
  observacao text,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid,
  updated_by uuid
);

CREATE INDEX IF NOT EXISTS idx_trans_servicos_veiculo ON trans_servicos(veiculo_id, data DESC);
CREATE INDEX IF NOT EXISTS idx_trans_servicos_fornecedor ON trans_servicos(fornecedor_id) WHERE fornecedor_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trans_servicos_pneu ON trans_servicos(pneu_id) WHERE pneu_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trans_servicos_tipo_data ON trans_servicos(tipo, data DESC);
CREATE INDEX IF NOT EXISTS idx_trans_servicos_manut ON trans_servicos(manutencao_id) WHERE manutencao_id IS NOT NULL;

CREATE TRIGGER trans_servicos_updated
BEFORE UPDATE ON trans_servicos
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

ALTER TABLE trans_servicos ENABLE ROW LEVEL SECURITY;

CREATE POLICY trans_servicos_sel ON trans_servicos FOR SELECT TO authenticated USING (true);
CREATE POLICY trans_servicos_ins ON trans_servicos FOR INSERT TO authenticated WITH CHECK (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_servicos_upd ON trans_servicos FOR UPDATE TO authenticated USING (
  trans_current_perfil() IN ('admin','gestor','almoxarife')
);
CREATE POLICY trans_servicos_del ON trans_servicos FOR DELETE TO authenticated USING (trans_is_admin());

-- =====================================================================
-- VIEW completa para listagem
-- =====================================================================
CREATE OR REPLACE VIEW v_trans_servicos_full AS
SELECT
  s.*,
  v.placa,
  v.modelo AS veiculo_modelo,
  f.razao_social AS fornecedor_nome,
  p.codigo_interno AS pneu_codigo,
  prod.sku AS produto_sku,
  prod.nome AS produto_nome,
  nf.numero AS nf_numero,
  nf.chave_acesso AS nf_chave
FROM trans_servicos s
JOIN trans_veiculos v ON v.id = s.veiculo_id
LEFT JOIN trans_fornecedores f ON f.id = s.fornecedor_id
LEFT JOIN trans_pneus p ON p.id = s.pneu_id
LEFT JOIN trans_produtos prod ON prod.id = s.produto_id
LEFT JOIN trans_notas_fiscais nf ON nf.id = s.nota_fiscal_id;

GRANT SELECT ON v_trans_servicos_full TO authenticated;

-- =====================================================================
-- RPC: registrar servico com integracoes automaticas
-- =====================================================================
-- Parametros:
--   p_tipo, p_veiculo_id, p_data (default hoje), p_valor, p_descricao
--   p_fornecedor_id, p_km_ocorrencia
--   p_pneu_id, p_sulco_pos_mm  (para recapagem/borracharia)
--   p_produto_id, p_quantidade_produto (para consumo de peca)
--   p_manutencao_id, p_nota_fiscal_id, p_anexo_url
--   p_observacao
--
-- Retorna: { ok, servico_id, recapagem_id?, movimentacao_estoque_id? }
-- =====================================================================
CREATE OR REPLACE FUNCTION trans_servico_registrar(
  p_tipo text,
  p_veiculo_id uuid,
  p_valor numeric,
  p_data date DEFAULT NULL,
  p_fornecedor_id uuid DEFAULT NULL,
  p_descricao text DEFAULT NULL,
  p_km_ocorrencia numeric DEFAULT NULL,
  p_pneu_id uuid DEFAULT NULL,
  p_sulco_pos_mm numeric DEFAULT NULL,
  p_produto_id uuid DEFAULT NULL,
  p_quantidade_produto numeric DEFAULT NULL,
  p_manutencao_id uuid DEFAULT NULL,
  p_nota_fiscal_id uuid DEFAULT NULL,
  p_anexo_url text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_servico_id uuid;
  v_recap_id uuid;
  v_mov_id uuid;
  v_pneu_status text;
  v_pneu_num int;
  v_pneu_km numeric;
  v_doc text;
  v_lote_id uuid;
  v_saida_json json;
  v_data date := COALESCE(p_data, CURRENT_DATE);
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao para registrar servico';
  END IF;
  IF p_tipo IS NULL OR p_veiculo_id IS NULL THEN
    RAISE EXCEPTION 'tipo e veiculo sao obrigatorios';
  END IF;

  -- Cria registro principal primeiro para obter o id
  INSERT INTO trans_servicos (
    tipo, veiculo_id, fornecedor_id, data, valor, descricao,
    km_ocorrencia, pneu_id, sulco_pos_mm, produto_id, quantidade_produto,
    manutencao_id, nota_fiscal_id, anexo_url, observacao,
    status, created_by, updated_by
  ) VALUES (
    p_tipo, p_veiculo_id, p_fornecedor_id, v_data, p_valor, p_descricao,
    p_km_ocorrencia, p_pneu_id, p_sulco_pos_mm, p_produto_id, p_quantidade_produto,
    p_manutencao_id, p_nota_fiscal_id, p_anexo_url, p_observacao,
    'pendente', auth.uid(), auth.uid()
  ) RETURNING id INTO v_servico_id;

  v_doc := 'SERV-' || SUBSTRING(v_servico_id::text, 1, 8);

  -- Integracao RECAPAGEM
  IF p_tipo = 'recapagem' AND p_pneu_id IS NOT NULL THEN
    SELECT status, numero_recapagens, km_acumulado
      INTO v_pneu_status, v_pneu_num, v_pneu_km
    FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;

    IF v_pneu_status IS NULL THEN
      RAISE EXCEPTION 'Pneu % nao encontrado', p_pneu_id;
    END IF;

    -- Cria registro de recapagem como retornado (pos-execucao)
    INSERT INTO trans_pneus_recapagem (
      pneu_id, fornecedor_id, sulco_pre_mm, sulco_pos_mm,
      km_rodado_antes, numero_recapagem, custo, status,
      data_envio, data_retorno, observacao, created_by
    ) VALUES (
      p_pneu_id, p_fornecedor_id, NULL, p_sulco_pos_mm,
      v_pneu_km, COALESCE(v_pneu_num,0) + 1, p_valor, 'retornado',
      v_data, v_data, p_observacao, auth.uid()
    ) RETURNING id INTO v_recap_id;

    -- Atualiza pneu: incrementa recapagens, sulco, e se estava em
    -- recapagem volta a estoque
    UPDATE trans_pneus
    SET numero_recapagens = COALESCE(numero_recapagens,0) + 1,
        sulco_atual_mm = COALESCE(p_sulco_pos_mm, sulco_atual_mm),
        status = CASE WHEN status = 'recapagem' THEN 'estoque' ELSE status END,
        localizacao_atual = CASE WHEN status = 'recapagem' THEN 'almoxarifado' ELSE localizacao_atual END,
        updated_at = now(), updated_by = auth.uid()
    WHERE id = p_pneu_id;

    -- Movimentacao no ledger do pneu
    INSERT INTO trans_pneus_movimentacoes (
      pneu_id, tipo, usuario_id, data_hora,
      fornecedor_id, motivo, observacao, created_by
    ) VALUES (
      p_pneu_id, 'retorno_recap', auth.uid(), now(),
      p_fornecedor_id, 'NFS-e recapagem', 'Servico ' || v_doc, auth.uid()
    );

    UPDATE trans_servicos SET recapagem_id = v_recap_id WHERE id = v_servico_id;
  END IF;

  -- Integracao CONSUMO de peca (produto+qty) com FEFO
  IF p_produto_id IS NOT NULL AND COALESCE(p_quantidade_produto,0) > 0 THEN
    v_lote_id := trans_sugerir_lote(p_produto_id, p_quantidade_produto);
    IF v_lote_id IS NULL THEN
      RAISE EXCEPTION 'Sem lote disponivel (FEFO) para produto % qty %',
        p_produto_id, p_quantidade_produto;
    END IF;

    v_saida_json := trans_saida_estoque(
      p_lote_id := v_lote_id,
      p_quantidade := p_quantidade_produto,
      p_documento := v_doc,
      p_veiculo_id := p_veiculo_id,
      p_ean_lido := NULL,
      p_override_bloqueio := false,
      p_justificativa := NULL,
      p_observacao := 'Consumo via servico ' || v_doc
    );

    -- Recupera a movimentacao criada pelo documento
    SELECT id INTO v_mov_id
    FROM trans_estoque_movimentacao
    WHERE documento_origem = v_doc
      AND produto_id = p_produto_id
    ORDER BY data_hora DESC LIMIT 1;

    UPDATE trans_servicos
    SET movimentacao_estoque_id = v_mov_id
    WHERE id = v_servico_id;
  END IF;

  -- Finaliza como processado
  UPDATE trans_servicos SET status = 'processado' WHERE id = v_servico_id;

  RETURN json_build_object(
    'ok', true,
    'servico_id', v_servico_id,
    'recapagem_id', v_recap_id,
    'movimentacao_estoque_id', v_mov_id
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_servico_registrar(
  text, uuid, numeric, date, uuid, text, numeric,
  uuid, numeric, uuid, numeric, uuid, uuid, text, text
) TO authenticated;

-- =====================================================================
-- RPC: cancelar servico - apenas admin; marca como cancelado. NAO
-- reverte movimentacoes de estoque/recapagem automaticamente (o admin
-- deve decidir; abrir movimentacao compensatoria manualmente se preciso).
-- =====================================================================
CREATE OR REPLACE FUNCTION trans_servico_cancelar(
  p_servico_id uuid,
  p_motivo text
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT trans_is_admin() THEN
    RAISE EXCEPTION 'Apenas administradores podem cancelar servicos';
  END IF;
  IF COALESCE(p_motivo,'') = '' THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  UPDATE trans_servicos
  SET status = 'cancelado',
      observacao = COALESCE(observacao || E'\n','') || 'CANCELADO: ' || p_motivo,
      updated_at = now(), updated_by = auth.uid()
  WHERE id = p_servico_id;

  RETURN json_build_object('ok', true);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_servico_cancelar(uuid, text) TO authenticated;

-- =====================================================================
-- RPC: custo de servicos por veiculo/periodo (dashboard)
-- =====================================================================
CREATE OR REPLACE FUNCTION trans_servicos_custos(
  p_data_ini date DEFAULT NULL,
  p_data_fim date DEFAULT NULL,
  p_veiculo_id uuid DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_resultado json;
BEGIN
  SELECT json_build_object(
    'total', COUNT(*),
    'custo_total', COALESCE(SUM(valor),0),
    'por_tipo', (
      SELECT json_agg(json_build_object('tipo', tipo, 'qtd', qtd, 'custo', custo) ORDER BY custo DESC)
      FROM (
        SELECT tipo, COUNT(*) AS qtd, SUM(valor) AS custo
        FROM trans_servicos
        WHERE status != 'cancelado'
          AND (p_data_ini IS NULL OR data >= p_data_ini)
          AND (p_data_fim IS NULL OR data <= p_data_fim)
          AND (p_veiculo_id IS NULL OR veiculo_id = p_veiculo_id)
        GROUP BY tipo
      ) t
    ),
    'por_veiculo', (
      SELECT json_agg(json_build_object(
        'veiculo_id', veiculo_id, 'placa', placa, 'qtd', qtd, 'custo', custo
      ) ORDER BY custo DESC)
      FROM (
        SELECT s.veiculo_id, v.placa, COUNT(*) AS qtd, SUM(s.valor) AS custo
        FROM trans_servicos s
        JOIN trans_veiculos v ON v.id = s.veiculo_id
        WHERE s.status != 'cancelado'
          AND (p_data_ini IS NULL OR s.data >= p_data_ini)
          AND (p_data_fim IS NULL OR s.data <= p_data_fim)
          AND (p_veiculo_id IS NULL OR s.veiculo_id = p_veiculo_id)
        GROUP BY s.veiculo_id, v.placa
      ) t
    )
  ) INTO v_resultado
  FROM trans_servicos
  WHERE status != 'cancelado'
    AND (p_data_ini IS NULL OR data >= p_data_ini)
    AND (p_data_fim IS NULL OR data <= p_data_fim)
    AND (p_veiculo_id IS NULL OR veiculo_id = p_veiculo_id);

  RETURN v_resultado;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_servicos_custos(date, date, uuid) TO authenticated;
