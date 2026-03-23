-- ══════════════════════════════════════════════════════════════════════════════
-- LOTE BASE DE PRODUÇÃO DIÁRIO
-- Migration: lote_base_producao.sql
-- Data: 2026-03-18
-- Status: INFRAESTRUTURA — preparação para implantação futura
--
-- PROBLEMA:
--   Atualmente o ATAK gera 1 lote de produção para cada lote de matéria-prima (1:1).
--   Com a ampliação da salga, múltiplos lotes de MP serão usados na mesma produção.
--
-- SOLUÇÃO:
--   1. cq_lote_base_producao — lote consolidado diário (ex: BCH-11032026)
--   2. cq_lote_base_composicao — rastreabilidade: quais lotes de MP compõem o lote base
--   3. cq_estoque_mp — saldo de matéria-prima por lote de origem
--   4. Funções SQL para criar lote base, consumir MP, consultar composição
--
-- FLUXO NOVO:
--   Estoque MP (lotes 100, 101, 102...)
--     ↓ descongelamento + classificação CQ
--     ↓ consumo parcial de múltiplos lotes
--   Lote Base BCH-11032026 (500+700+300 = 1500 peças)
--     ↓ DAQUI PRA FRENTE: fluxo normal ATAK (T650→T653→T654→T655→T656→T657→T630→T621)
--
-- IMPORTANTE:
--   - NÃO altera o fluxo existente do ATAK após a geração do lote base
--   - Romaneios, apontamentos e classificação continuam iguais
--   - Apenas a ENTRADA do processo muda (consolidação de MP)
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELA: LOTE BASE DE PRODUÇÃO
-- Representa o lote consolidado diário gerado a partir de múltiplos lotes de MP
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_lote_base_producao (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Identificação do lote base
  codigo              TEXT NOT NULL UNIQUE,          -- ex: BCH-11032026
  data_producao       DATE NOT NULL,                 -- data do lote base

  -- Totais consolidados
  total_pecas         INTEGER NOT NULL DEFAULT 0 CHECK (total_pecas >= 0),
  total_lotes_origem  INTEGER NOT NULL DEFAULT 0,    -- quantos lotes de MP foram usados

  -- Produto (quando aplicável — ex: 45083 Bucho Bovino Salgado)
  produto_codigo      TEXT,
  produto_nome        TEXT,

  -- Status do lote base
  status              TEXT NOT NULL DEFAULT 'aberto'
                      CHECK (status IN (
                        'aberto',          -- ainda recebendo consumos de MP
                        'fechado',         -- consolidação finalizada, pronto para produção
                        'em_producao',     -- já entrou no fluxo ATAK (T650+)
                        'finalizado',      -- produção concluída
                        'cancelado'        -- cancelado antes de entrar em produção
                      )),

  -- Referência ao ATAK (quando o lote base gera movimentação no ATAK)
  documento_atak      TEXT,                          -- documento de referência no ATAK
  ordem_producao_atak TEXT,                          -- OP gerada no ATAK

  -- Observações
  observacoes         TEXT,

  -- Auditoria
  criado_por          UUID REFERENCES cq_usuarios(id),
  criado_por_nome     TEXT,
  fechado_por         UUID REFERENCES cq_usuarios(id),
  fechado_em          TIMESTAMPTZ,
  criado_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lote_base_data ON cq_lote_base_producao(data_producao DESC);
CREATE INDEX IF NOT EXISTS idx_lote_base_status ON cq_lote_base_producao(status);
CREATE INDEX IF NOT EXISTS idx_lote_base_codigo ON cq_lote_base_producao(codigo);

COMMENT ON TABLE cq_lote_base_producao IS
  'Lote base de produção diário — consolida múltiplos lotes de matéria-prima para uma produção. Substitui o modelo 1:1 do ATAK para o início do processo produtivo.';


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TABELA: COMPOSIÇÃO DO LOTE BASE (rastreabilidade MP → Produção)
-- Cada linha = 1 consumo parcial de 1 lote de MP para o lote base
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_lote_base_composicao (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Vínculo com lote base
  lote_base_id        UUID NOT NULL REFERENCES cq_lote_base_producao(id) ON DELETE CASCADE,

  -- Lote de origem (matéria-prima)
  lote_origem_codigo  TEXT NOT NULL,                 -- ex: "100", "101", "102"
  lote_origem_atak    TEXT,                          -- código no ATAK (documento/etiqueta)
  fornecedor_codigo   TEXT,
  fornecedor_nome     TEXT,

  -- Quantidade consumida deste lote
  quantidade_consumida INTEGER NOT NULL CHECK (quantidade_consumida > 0),

  -- Referência à inspeção CQ do lote de origem (se houver)
  inspecao_id         UUID REFERENCES registros_cq_inspecao(id),
  classificacao_mp    TEXT,                          -- classificação CQ da MP (A/B/C)
  percentual_defeitos NUMERIC(6,2),                  -- % defeito da MP neste lote

  -- Auditoria
  criado_por          UUID REFERENCES cq_usuarios(id),
  criado_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Controle de unicidade: mesmo lote origem não pode ser adicionado 2x no mesmo lote base
  UNIQUE(lote_base_id, lote_origem_codigo)
);

CREATE INDEX IF NOT EXISTS idx_composicao_lote_base ON cq_lote_base_composicao(lote_base_id);
CREATE INDEX IF NOT EXISTS idx_composicao_lote_origem ON cq_lote_base_composicao(lote_origem_codigo);
CREATE INDEX IF NOT EXISTS idx_composicao_fornecedor ON cq_lote_base_composicao(fornecedor_codigo);

COMMENT ON TABLE cq_lote_base_composicao IS
  'Rastreabilidade: registra quais lotes de matéria-prima compõem cada lote base de produção, com quantidade consumida de cada um.';


-- ════════════════════════════════════════════════════════════════════════════
-- 3. TABELA: ESTOQUE DE MATÉRIA-PRIMA POR LOTE
-- Controla o saldo disponível de cada lote de MP para consumo em produções
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_estoque_mp (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Identificação do lote
  lote_codigo         TEXT NOT NULL UNIQUE,           -- ex: "100", "101"
  lote_atak           TEXT,                           -- referência no ATAK

  -- Fornecedor
  fornecedor_codigo   TEXT,
  fornecedor_nome     TEXT,

  -- Produto
  produto_codigo      TEXT,
  produto_nome        TEXT,

  -- Quantidades
  quantidade_entrada  INTEGER NOT NULL CHECK (quantidade_entrada > 0),    -- total recebido
  quantidade_consumida INTEGER NOT NULL DEFAULT 0 CHECK (quantidade_consumida >= 0),
  quantidade_disponivel INTEGER GENERATED ALWAYS AS (quantidade_entrada - quantidade_consumida) STORED,

  -- Status
  status              TEXT NOT NULL DEFAULT 'disponivel'
                      CHECK (status IN (
                        'aguardando_inspecao',  -- recebido, aguardando CQ
                        'disponivel',           -- liberado para produção
                        'parcialmente_consumido', -- parte já usada em produção
                        'esgotado',             -- saldo zero
                        'bloqueado',            -- bloqueado por qualidade
                        'devolvido'             -- devolvido ao fornecedor
                      )),

  -- Qualidade (referência à inspeção)
  inspecao_id         UUID REFERENCES registros_cq_inspecao(id),
  classificacao       TEXT,                           -- resultado CQ: A/B/C
  percentual_defeitos NUMERIC(6,2),
  data_liberacao      DATE,                           -- data que CQ liberou

  -- Datas
  data_entrada        DATE NOT NULL,
  data_validade       DATE,                           -- validade da MP (se aplicável)

  -- Auditoria
  criado_por          UUID REFERENCES cq_usuarios(id),
  criado_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_estoque_mp_status ON cq_estoque_mp(status);
CREATE INDEX IF NOT EXISTS idx_estoque_mp_lote ON cq_estoque_mp(lote_codigo);
CREATE INDEX IF NOT EXISTS idx_estoque_mp_fornecedor ON cq_estoque_mp(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_estoque_mp_disponivel ON cq_estoque_mp(quantidade_disponivel) WHERE quantidade_disponivel > 0;

COMMENT ON TABLE cq_estoque_mp IS
  'Estoque de matéria-prima por lote — controla saldo disponível para consumo em lotes base de produção. Atualizado automaticamente ao consumir MP.';


-- ════════════════════════════════════════════════════════════════════════════
-- 4. FUNÇÃO: CRIAR LOTE BASE DE PRODUÇÃO
-- Gera o código BCH-DDMMAAAA e cria o registro
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_criar_lote_base(
  p_data_producao DATE DEFAULT CURRENT_DATE,
  p_produto_codigo TEXT DEFAULT NULL,
  p_produto_nome TEXT DEFAULT NULL,
  p_observacoes TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_codigo TEXT;
  v_seq INTEGER;
  v_user_id UUID;
  v_user_nome TEXT;
  v_lote_id UUID;
BEGIN
  -- Usuário
  SELECT u.id, u.nome INTO v_user_id, v_user_nome
  FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.ativo = TRUE LIMIT 1;

  IF v_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário não autenticado');
  END IF;

  -- Gerar código: BCH-DDMMAAAA(-N se já existir)
  v_codigo := 'BCH-' || TO_CHAR(p_data_producao, 'DDMMYYYY');

  -- Verificar se já existe lote para esta data
  SELECT COUNT(*) INTO v_seq
  FROM cq_lote_base_producao
  WHERE data_producao = p_data_producao
    AND status != 'cancelado';

  -- Se já existe, adicionar sufixo
  IF v_seq > 0 THEN
    v_codigo := v_codigo || '-' || (v_seq + 1)::TEXT;
  END IF;

  -- Verificar unicidade (safety net)
  WHILE EXISTS (SELECT 1 FROM cq_lote_base_producao WHERE codigo = v_codigo) LOOP
    v_seq := v_seq + 1;
    v_codigo := 'BCH-' || TO_CHAR(p_data_producao, 'DDMMYYYY') || '-' || v_seq::TEXT;
  END LOOP;

  -- Criar lote base
  INSERT INTO cq_lote_base_producao (
    codigo, data_producao, produto_codigo, produto_nome,
    observacoes, criado_por, criado_por_nome
  ) VALUES (
    v_codigo, p_data_producao, p_produto_codigo, p_produto_nome,
    p_observacoes, v_user_id, v_user_nome
  ) RETURNING id INTO v_lote_id;

  -- Audit trail
  PERFORM cq_registrar_audit(
    'producao', 'criar', 'cq_lote_base_producao', v_lote_id, v_codigo,
    NULL,
    jsonb_build_object('codigo', v_codigo, 'data_producao', p_data_producao, 'status', 'aberto'),
    NULL, 'Lote base de produção criado'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'lote_base_id', v_lote_id,
    'codigo', v_codigo,
    'data_producao', p_data_producao
  );
END;
$$;

COMMENT ON FUNCTION cq_criar_lote_base IS
  'Cria um novo lote base de produção diário com código BCH-DDMMAAAA. Suporta múltiplos lotes no mesmo dia com sufixo sequencial.';


-- ════════════════════════════════════════════════════════════════════════════
-- 5. FUNÇÃO: CONSUMIR MATÉRIA-PRIMA PARA LOTE BASE
-- Debita quantidade do estoque de MP e registra na composição
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_consumir_mp_para_lote_base(
  p_lote_base_id UUID,
  p_lote_mp_codigo TEXT,
  p_quantidade INTEGER
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_lote_base RECORD;
  v_estoque RECORD;
  v_user_id UUID;
  v_lote_base_codigo TEXT;
BEGIN
  -- Usuário
  SELECT u.id INTO v_user_id
  FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.ativo = TRUE LIMIT 1;

  -- Validar quantidade
  IF p_quantidade <= 0 THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Quantidade deve ser maior que zero');
  END IF;

  -- Carregar lote base
  SELECT * INTO v_lote_base FROM cq_lote_base_producao WHERE id = p_lote_base_id;
  IF v_lote_base IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Lote base não encontrado');
  END IF;
  IF v_lote_base.status != 'aberto' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Lote base "%s" não está aberto (status: %s)', v_lote_base.codigo, v_lote_base.status));
  END IF;
  v_lote_base_codigo := v_lote_base.codigo;

  -- Carregar estoque MP
  SELECT * INTO v_estoque FROM cq_estoque_mp WHERE lote_codigo = p_lote_mp_codigo;
  IF v_estoque IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Lote de MP "%s" não encontrado no estoque', p_lote_mp_codigo));
  END IF;
  IF v_estoque.status IN ('bloqueado', 'devolvido') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Lote "%s" está %s', p_lote_mp_codigo, v_estoque.status));
  END IF;
  IF v_estoque.quantidade_disponivel < p_quantidade THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Saldo insuficiente no lote "%s": disponível %s, solicitado %s',
        p_lote_mp_codigo, v_estoque.quantidade_disponivel, p_quantidade));
  END IF;

  -- Registrar composição
  INSERT INTO cq_lote_base_composicao (
    lote_base_id, lote_origem_codigo, lote_origem_atak,
    fornecedor_codigo, fornecedor_nome,
    quantidade_consumida,
    inspecao_id, classificacao_mp, percentual_defeitos,
    criado_por
  ) VALUES (
    p_lote_base_id, p_lote_mp_codigo, v_estoque.lote_atak,
    v_estoque.fornecedor_codigo, v_estoque.fornecedor_nome,
    p_quantidade,
    v_estoque.inspecao_id, v_estoque.classificacao, v_estoque.percentual_defeitos,
    v_user_id
  )
  ON CONFLICT (lote_base_id, lote_origem_codigo) DO UPDATE SET
    quantidade_consumida = cq_lote_base_composicao.quantidade_consumida + p_quantidade,
    criado_em = NOW();

  -- Debitar do estoque
  UPDATE cq_estoque_mp SET
    quantidade_consumida = quantidade_consumida + p_quantidade,
    status = CASE
      WHEN (quantidade_entrada - (quantidade_consumida + p_quantidade)) <= 0 THEN 'esgotado'
      ELSE 'parcialmente_consumido'
    END,
    atualizado_em = NOW()
  WHERE lote_codigo = p_lote_mp_codigo;

  -- Atualizar totais do lote base
  UPDATE cq_lote_base_producao SET
    total_pecas = (
      SELECT COALESCE(SUM(quantidade_consumida), 0)
      FROM cq_lote_base_composicao WHERE lote_base_id = p_lote_base_id
    ),
    total_lotes_origem = (
      SELECT COUNT(DISTINCT lote_origem_codigo)
      FROM cq_lote_base_composicao WHERE lote_base_id = p_lote_base_id
    ),
    atualizado_em = NOW()
  WHERE id = p_lote_base_id;

  -- Audit trail
  PERFORM cq_registrar_audit(
    'producao', 'consumir_mp', 'cq_lote_base_composicao', p_lote_base_id, v_lote_base_codigo,
    jsonb_build_object('saldo_antes', v_estoque.quantidade_disponivel),
    jsonb_build_object('lote_mp', p_lote_mp_codigo, 'quantidade', p_quantidade,
      'saldo_depois', v_estoque.quantidade_disponivel - p_quantidade),
    NULL,
    format('Consumo de %s pcs do lote %s para %s', p_quantidade, p_lote_mp_codigo, v_lote_base_codigo)
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'lote_base', v_lote_base_codigo,
    'lote_mp', p_lote_mp_codigo,
    'quantidade_consumida', p_quantidade,
    'saldo_restante_mp', v_estoque.quantidade_disponivel - p_quantidade,
    'total_lote_base', (SELECT total_pecas FROM cq_lote_base_producao WHERE id = p_lote_base_id)
  );
END;
$$;

COMMENT ON FUNCTION cq_consumir_mp_para_lote_base IS
  'Consome quantidade de um lote de MP para o lote base. Valida saldo, debita estoque, registra composição e atualiza totais.';


-- ════════════════════════════════════════════════════════════════════════════
-- 6. FUNÇÃO: FECHAR LOTE BASE
-- Marca o lote base como pronto para entrar em produção
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_fechar_lote_base(
  p_lote_base_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_lote RECORD;
  v_user_id UUID;
  v_composicao JSONB;
BEGIN
  SELECT u.id INTO v_user_id
  FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.ativo = TRUE LIMIT 1;

  SELECT * INTO v_lote FROM cq_lote_base_producao WHERE id = p_lote_base_id;
  IF v_lote IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Lote base não encontrado');
  END IF;
  IF v_lote.status != 'aberto' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Lote não está aberto (status: %s)', v_lote.status));
  END IF;
  IF v_lote.total_pecas = 0 THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Lote base não tem nenhuma MP consumida');
  END IF;

  -- Fechar
  UPDATE cq_lote_base_producao SET
    status = 'fechado',
    fechado_por = v_user_id,
    fechado_em = NOW(),
    atualizado_em = NOW()
  WHERE id = p_lote_base_id;

  -- Composição para retorno
  SELECT jsonb_agg(jsonb_build_object(
    'lote_origem', c.lote_origem_codigo,
    'fornecedor', c.fornecedor_nome,
    'quantidade', c.quantidade_consumida,
    'classificacao_mp', c.classificacao_mp
  ))
  INTO v_composicao
  FROM cq_lote_base_composicao c
  WHERE c.lote_base_id = p_lote_base_id;

  -- Audit
  PERFORM cq_registrar_audit(
    'producao', 'fechar_lote_base', 'cq_lote_base_producao', p_lote_base_id, v_lote.codigo,
    jsonb_build_object('status', 'aberto'),
    jsonb_build_object('status', 'fechado', 'total_pecas', v_lote.total_pecas, 'composicao', v_composicao)
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'lote_base', v_lote.codigo,
    'total_pecas', v_lote.total_pecas,
    'total_lotes_origem', v_lote.total_lotes_origem,
    'composicao', v_composicao
  );
END;
$$;

COMMENT ON FUNCTION cq_fechar_lote_base IS
  'Fecha o lote base de produção — nenhuma MP adicional pode ser consumida. Lote fica pronto para entrar no fluxo ATAK.';


-- ════════════════════════════════════════════════════════════════════════════
-- 7. FUNÇÃO: CONSULTAR COMPOSIÇÃO DO LOTE BASE
-- Rastreabilidade completa: origem → produção
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_consultar_composicao_lote_base(
  p_lote_base_codigo TEXT DEFAULT NULL,
  p_lote_base_id UUID DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_lote RECORD;
  v_composicao JSONB;
BEGIN
  -- Buscar por código ou ID
  IF p_lote_base_id IS NOT NULL THEN
    SELECT * INTO v_lote FROM cq_lote_base_producao WHERE id = p_lote_base_id;
  ELSIF p_lote_base_codigo IS NOT NULL THEN
    SELECT * INTO v_lote FROM cq_lote_base_producao WHERE codigo = p_lote_base_codigo;
  ELSE
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Informe código ou ID do lote base');
  END IF;

  IF v_lote IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Lote base não encontrado');
  END IF;

  SELECT jsonb_agg(jsonb_build_object(
    'lote_origem', c.lote_origem_codigo,
    'lote_atak', c.lote_origem_atak,
    'fornecedor_codigo', c.fornecedor_codigo,
    'fornecedor_nome', c.fornecedor_nome,
    'quantidade_consumida', c.quantidade_consumida,
    'classificacao_mp', c.classificacao_mp,
    'percentual_defeitos', c.percentual_defeitos,
    'inspecao_id', c.inspecao_id,
    'criado_em', c.criado_em
  ) ORDER BY c.criado_em)
  INTO v_composicao
  FROM cq_lote_base_composicao c
  WHERE c.lote_base_id = v_lote.id;

  RETURN jsonb_build_object(
    'ok', TRUE,
    'lote_base', jsonb_build_object(
      'id', v_lote.id,
      'codigo', v_lote.codigo,
      'data_producao', v_lote.data_producao,
      'status', v_lote.status,
      'total_pecas', v_lote.total_pecas,
      'total_lotes_origem', v_lote.total_lotes_origem,
      'produto_codigo', v_lote.produto_codigo,
      'produto_nome', v_lote.produto_nome,
      'documento_atak', v_lote.documento_atak,
      'ordem_producao_atak', v_lote.ordem_producao_atak,
      'criado_por', v_lote.criado_por_nome,
      'criado_em', v_lote.criado_em,
      'fechado_em', v_lote.fechado_em
    ),
    'composicao', COALESCE(v_composicao, '[]'::JSONB)
  );
END;
$$;

COMMENT ON FUNCTION cq_consultar_composicao_lote_base IS
  'Consulta rastreabilidade completa de um lote base: quais lotes de MP o compõem, com quantidades, fornecedores e classificação CQ.';


-- ════════════════════════════════════════════════════════════════════════════
-- 8. FUNÇÃO: CONSULTAR ESTOQUE DE MP DISPONÍVEL
-- Lista lotes com saldo para seleção na tela de consolidação
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_listar_estoque_mp_disponivel(
  p_produto_codigo TEXT DEFAULT NULL,
  p_fornecedor_codigo TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_result JSONB;
BEGIN
  SELECT jsonb_agg(jsonb_build_object(
    'id', e.id,
    'lote_codigo', e.lote_codigo,
    'lote_atak', e.lote_atak,
    'fornecedor_codigo', e.fornecedor_codigo,
    'fornecedor_nome', e.fornecedor_nome,
    'produto_codigo', e.produto_codigo,
    'produto_nome', e.produto_nome,
    'quantidade_entrada', e.quantidade_entrada,
    'quantidade_consumida', e.quantidade_consumida,
    'quantidade_disponivel', e.quantidade_disponivel,
    'status', e.status,
    'classificacao', e.classificacao,
    'percentual_defeitos', e.percentual_defeitos,
    'data_entrada', e.data_entrada,
    'data_liberacao', e.data_liberacao
  ) ORDER BY e.data_entrada ASC)
  INTO v_result
  FROM cq_estoque_mp e
  WHERE e.quantidade_disponivel > 0
    AND e.status NOT IN ('bloqueado', 'devolvido', 'esgotado')
    AND (p_produto_codigo IS NULL OR e.produto_codigo = p_produto_codigo)
    AND (p_fornecedor_codigo IS NULL OR e.fornecedor_codigo = p_fornecedor_codigo);

  RETURN jsonb_build_object(
    'ok', TRUE,
    'estoque', COALESCE(v_result, '[]'::JSONB),
    'total_lotes', jsonb_array_length(COALESCE(v_result, '[]'::JSONB))
  );
END;
$$;

COMMENT ON FUNCTION cq_listar_estoque_mp_disponivel IS
  'Lista lotes de MP com saldo disponível para consumo em lotes base de produção. Filtrável por produto e fornecedor.';


-- ════════════════════════════════════════════════════════════════════════════
-- 9. FUNÇÃO: RASTREABILIDADE REVERSA
-- Dado um lote de MP, em quais lotes base ele foi usado?
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_rastrear_mp_em_producao(p_lote_mp_codigo TEXT)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_result JSONB;
  v_estoque RECORD;
BEGIN
  SELECT * INTO v_estoque FROM cq_estoque_mp WHERE lote_codigo = p_lote_mp_codigo;

  SELECT jsonb_agg(jsonb_build_object(
    'lote_base_codigo', lb.codigo,
    'data_producao', lb.data_producao,
    'status', lb.status,
    'quantidade_consumida', c.quantidade_consumida,
    'total_pecas_lote_base', lb.total_pecas
  ) ORDER BY lb.data_producao DESC)
  INTO v_result
  FROM cq_lote_base_composicao c
  JOIN cq_lote_base_producao lb ON lb.id = c.lote_base_id
  WHERE c.lote_origem_codigo = p_lote_mp_codigo;

  RETURN jsonb_build_object(
    'ok', TRUE,
    'lote_mp', p_lote_mp_codigo,
    'estoque', CASE WHEN v_estoque IS NOT NULL THEN jsonb_build_object(
      'quantidade_entrada', v_estoque.quantidade_entrada,
      'quantidade_consumida', v_estoque.quantidade_consumida,
      'quantidade_disponivel', v_estoque.quantidade_disponivel,
      'status', v_estoque.status
    ) ELSE NULL END,
    'usado_em', COALESCE(v_result, '[]'::JSONB)
  );
END;
$$;

COMMENT ON FUNCTION cq_rastrear_mp_em_producao IS
  'Rastreabilidade reversa: dado um lote de MP, mostra em quais lotes base de produção ele foi consumido e quantidades.';


-- ════════════════════════════════════════════════════════════════════════════
-- RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_lote_base_producao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_lote_base_composicao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_estoque_mp ENABLE ROW LEVEL SECURITY;

-- Leitura: perfis com acesso à produção
CREATE POLICY "lote_base_select" ON cq_lote_base_producao FOR SELECT TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','industrial','logistica','diretoria'));

CREATE POLICY "lote_base_insert" ON cq_lote_base_producao FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','industrial'));

CREATE POLICY "lote_base_update" ON cq_lote_base_producao FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','industrial'));

CREATE POLICY "composicao_select" ON cq_lote_base_composicao FOR SELECT TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','industrial','logistica','diretoria'));

CREATE POLICY "composicao_insert" ON cq_lote_base_composicao FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','industrial'));

CREATE POLICY "estoque_mp_select" ON cq_estoque_mp FOR SELECT TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','industrial','logistica','diretoria'));

CREATE POLICY "estoque_mp_insert" ON cq_estoque_mp FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','industrial','logistica'));

CREATE POLICY "estoque_mp_update" ON cq_estoque_mp FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','industrial'));


-- ════════════════════════════════════════════════════════════════════════════
-- AUDIT TRIGGERS
-- ════════════════════════════════════════════════════════════════════════════

DROP TRIGGER IF EXISTS trg_audit_trail_lote_base ON cq_lote_base_producao;
CREATE TRIGGER trg_audit_trail_lote_base
  AFTER INSERT OR UPDATE OR DELETE ON cq_lote_base_producao
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_estoque_mp ON cq_estoque_mp;
CREATE TRIGGER trg_audit_trail_estoque_mp
  AFTER INSERT OR UPDATE OR DELETE ON cq_estoque_mp
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();


-- ════════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ════════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION cq_criar_lote_base TO authenticated;
GRANT EXECUTE ON FUNCTION cq_consumir_mp_para_lote_base TO authenticated;
GRANT EXECUTE ON FUNCTION cq_fechar_lote_base TO authenticated;
GRANT EXECUTE ON FUNCTION cq_consultar_composicao_lote_base TO authenticated;
GRANT EXECUTE ON FUNCTION cq_listar_estoque_mp_disponivel TO authenticated;
GRANT EXECUTE ON FUNCTION cq_rastrear_mp_em_producao TO authenticated;

COMMIT;


-- ══════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO PÓS-MIGRATION
-- ══════════════════════════════════════════════════════════════════════════════

-- V1: Tabelas criadas
SELECT tablename FROM pg_tables
WHERE tablename IN ('cq_lote_base_producao', 'cq_lote_base_composicao', 'cq_estoque_mp')
ORDER BY tablename;

-- V2: Funções criadas
SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name IN (
    'cq_criar_lote_base',
    'cq_consumir_mp_para_lote_base',
    'cq_fechar_lote_base',
    'cq_consultar_composicao_lote_base',
    'cq_listar_estoque_mp_disponivel',
    'cq_rastrear_mp_em_producao'
  )
ORDER BY routine_name;

-- V3: Teste de criação (DRY RUN — não precisa executar agora)
-- SELECT cq_criar_lote_base('2026-03-11'::DATE, '45083', 'Bucho Bovino Salgado');
-- SELECT cq_listar_estoque_mp_disponivel();
