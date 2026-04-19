-- =====================================================================
-- Fase 3: Estoque NOVO por Lote
-- =====================================================================
-- - trans_config       : chave/valor (sucata_modo, prefixos, thresholds)
-- - trans_lotes        : lotes internos (`L-000001`), status CQ
-- - trans_estoque_lote : saldo 1 linha por lote, CHECK >= 0
-- - trans_estoque_movimentacao: ledger imutavel (entrada/saida/conversao/ajuste)
-- - trigger atualiza saldo em estoque_lote / estoque_usado conforme modalidade
-- =====================================================================

-- -------------------------------------------------------------
-- trans_config
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_config (
  chave text PRIMARY KEY,
  valor text NOT NULL,
  descricao text,
  atualizado_em timestamptz NOT NULL DEFAULT now(),
  atualizado_por uuid REFERENCES auth.users(id)
);

CREATE TRIGGER trans_config_updated
BEFORE UPDATE ON trans_config
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- -------------------------------------------------------------
-- trans_lotes
-- -------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS trans_lote_interno_seq START 1;

CREATE TABLE IF NOT EXISTS trans_lotes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL REFERENCES trans_produtos(id) ON DELETE RESTRICT,
  lote_interno text UNIQUE NOT NULL,
  lote_fornecedor text,
  fornecedor_id uuid REFERENCES trans_fornecedores(id),
  nota_fiscal_id uuid, -- FK adicionada depois em Fase 3.5
  data_entrada date NOT NULL DEFAULT CURRENT_DATE,
  data_fabricacao date,
  data_validade date,
  status text NOT NULL DEFAULT 'liberado'
    CHECK (status IN ('liberado','bloqueado','quarentena','reprovado')),
  custo_unitario numeric(14,4),
  observacao text,
  status_alterado_em timestamptz,
  status_alterado_por uuid REFERENCES auth.users(id),
  status_motivo text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES auth.users(id),
  updated_by uuid REFERENCES auth.users(id)
);

CREATE INDEX idx_trans_lotes_produto ON trans_lotes(produto_id);
CREATE INDEX idx_trans_lotes_status ON trans_lotes(status);
CREATE INDEX idx_trans_lotes_validade ON trans_lotes(data_validade) WHERE data_validade IS NOT NULL;
CREATE INDEX idx_trans_lotes_fornecedor ON trans_lotes(fornecedor_id) WHERE fornecedor_id IS NOT NULL;

CREATE TRIGGER trans_lotes_updated
BEFORE UPDATE ON trans_lotes
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

-- Funcao geradora do lote_interno (L-000001)
CREATE OR REPLACE FUNCTION trans_gerar_lote_interno()
RETURNS text
LANGUAGE sql
AS $$
  SELECT 'L-' || LPAD(nextval('trans_lote_interno_seq')::text, 6, '0');
$$;

-- -------------------------------------------------------------
-- trans_estoque_lote (saldo por lote, 1 linha por lote)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_estoque_lote (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL REFERENCES trans_produtos(id) ON DELETE RESTRICT,
  lote_id uuid NOT NULL UNIQUE REFERENCES trans_lotes(id) ON DELETE RESTRICT,
  quantidade numeric(14,4) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  atualizado_em timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_trans_estoque_lote_produto ON trans_estoque_lote(produto_id);
CREATE INDEX idx_trans_estoque_lote_saldo ON trans_estoque_lote(produto_id) WHERE quantidade > 0;

-- -------------------------------------------------------------
-- trans_estoque_usado (saldo por SKU + condicao, 1 linha por par)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_estoque_usado (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL REFERENCES trans_produtos(id) ON DELETE RESTRICT,
  condicao text NOT NULL CHECK (condicao IN ('A','B','C')),
  quantidade numeric(14,4) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  atualizado_em timestamptz NOT NULL DEFAULT now(),
  UNIQUE (produto_id, condicao)
);

CREATE INDEX idx_trans_estoque_usado_produto ON trans_estoque_usado(produto_id);

-- -------------------------------------------------------------
-- trans_estoque_movimentacao (ledger)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_estoque_movimentacao (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo text NOT NULL CHECK (tipo IN ('entrada','saida','transferencia','ajuste','conversao')),
  modalidade text NOT NULL CHECK (modalidade IN ('novo','usado')),
  produto_id uuid NOT NULL REFERENCES trans_produtos(id),
  lote_id uuid REFERENCES trans_lotes(id),
  condicao text CHECK (condicao IN ('A','B','C')),
  ean_lido text,
  quantidade numeric(14,4) NOT NULL CHECK (quantidade > 0),
  custo_unitario numeric(14,4),
  data_hora timestamptz NOT NULL DEFAULT now(),
  usuario_id uuid REFERENCES auth.users(id),
  documento_origem text,
  veiculo_id uuid REFERENCES trans_veiculos(id),
  observacao text,
  conversao_origem_id uuid REFERENCES trans_estoque_movimentacao(id),
  override_bloqueio boolean NOT NULL DEFAULT false,
  justificativa text,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- mutex modalidade x campos
  CONSTRAINT ck_mov_mut_novo CHECK (
    (modalidade = 'novo' AND lote_id IS NOT NULL AND condicao IS NULL)
    OR
    (modalidade = 'usado' AND condicao IS NOT NULL AND lote_id IS NULL)
  ),
  -- saida com bloqueio exige justificativa
  CONSTRAINT ck_mov_override_just CHECK (
    NOT override_bloqueio OR justificativa IS NOT NULL
  )
);

CREATE INDEX idx_trans_mov_produto ON trans_estoque_movimentacao(produto_id, data_hora DESC);
CREATE INDEX idx_trans_mov_lote ON trans_estoque_movimentacao(lote_id) WHERE lote_id IS NOT NULL;
CREATE INDEX idx_trans_mov_tipo ON trans_estoque_movimentacao(tipo, modalidade);
CREATE INDEX idx_trans_mov_usuario ON trans_estoque_movimentacao(usuario_id);
CREATE INDEX idx_trans_mov_conversao ON trans_estoque_movimentacao(conversao_origem_id) WHERE conversao_origem_id IS NOT NULL;

-- -------------------------------------------------------------
-- Trigger: aplicar movimentacao em estoque_lote / estoque_usado
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_tg_aplicar_movimentacao()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_sinal int;
BEGIN
  IF NEW.tipo = 'entrada' THEN
    v_sinal := 1;
  ELSIF NEW.tipo IN ('saida','transferencia') THEN
    v_sinal := -1;
  ELSIF NEW.tipo = 'ajuste' THEN
    v_sinal := 1;  -- ajustes negativos devem vir como tipo='saida'
  ELSIF NEW.tipo = 'conversao' THEN
    IF NEW.conversao_origem_id IS NULL THEN
      v_sinal := -1;
    ELSE
      v_sinal := 1;
    END IF;
  ELSE
    RAISE EXCEPTION 'Tipo de movimentacao invalido: %', NEW.tipo;
  END IF;

  IF NEW.modalidade = 'novo' THEN
    INSERT INTO trans_estoque_lote (produto_id, lote_id, quantidade, atualizado_em)
    VALUES (NEW.produto_id, NEW.lote_id, v_sinal * NEW.quantidade, now())
    ON CONFLICT (lote_id) DO UPDATE
      SET quantidade = trans_estoque_lote.quantidade + v_sinal * NEW.quantidade,
          atualizado_em = now();
  ELSE
    INSERT INTO trans_estoque_usado (produto_id, condicao, quantidade, atualizado_em)
    VALUES (NEW.produto_id, NEW.condicao, v_sinal * NEW.quantidade, now())
    ON CONFLICT (produto_id, condicao) DO UPDATE
      SET quantidade = trans_estoque_usado.quantidade + v_sinal * NEW.quantidade,
          atualizado_em = now();
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER trans_mov_aplicar
AFTER INSERT ON trans_estoque_movimentacao
FOR EACH ROW EXECUTE FUNCTION trans_tg_aplicar_movimentacao();

-- -------------------------------------------------------------
-- View: lotes com saldo disponivel (FEFO > FIFO)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_lotes_disponiveis AS
SELECT
  l.id AS lote_id,
  l.produto_id,
  l.lote_interno,
  l.lote_fornecedor,
  l.fornecedor_id,
  l.data_entrada,
  l.data_validade,
  l.status,
  l.custo_unitario,
  el.quantidade AS saldo
FROM trans_lotes l
JOIN trans_estoque_lote el ON el.lote_id = l.id
WHERE el.quantidade > 0
  AND l.status = 'liberado'
ORDER BY
  COALESCE(l.data_validade, l.data_entrada + interval '10 years'),
  l.data_entrada,
  l.lote_interno;
