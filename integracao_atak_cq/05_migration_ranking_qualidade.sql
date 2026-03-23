-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 05: Sistema de Ranking de Qualidade por Fornecedor × Produto
-- 31 produtos, 9 defeitos obrigatórios, classificação A/B/C
-- Rodar APÓS 04_migration_observabilidade.sql
-- ══════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELA DE PRODUTOS AVALIADOS (seed obrigatório)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_ranking_produtos (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo      TEXT UNIQUE NOT NULL,
  nome        TEXT NOT NULL,
  grupo       TEXT NOT NULL DEFAULT 'subproduto',
  ativo       BOOLEAN DEFAULT true,
  ordem       INTEGER NOT NULL DEFAULT 0,
  criado_em   TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO cq_ranking_produtos (codigo, nome, grupo, ordem) VALUES
  ('despojo',          'Despojo',             'subproduto',    1),
  ('culatra',          'Culatra',             'subproduto',    2),
  ('couro_suino',      'Couro suíno',         'couro',         3),
  ('colmeia',          'Colmeia',             'subproduto',    4),
  ('glote',            'Glote',               'subproduto',    5),
  ('baco',             'Baço',                'subproduto',    6),
  ('sebo',             'Sebo',                'subproduto',    7),
  ('membrana',         'Membrana',            'subproduto',    8),
  ('orelha_suina',     'Orelha suína',        'subproduto',    9),
  ('ligamento_cerv',   'Ligamento cervical',  'subproduto',   10),
  ('calculo_biliar',   'Cálculo biliar',      'especial',     11),
  ('traqueia',         'Traqueia',            'subproduto',   12),
  ('capa_vergalho',    'Capa de vergalho',    'subproduto',   13),
  ('buchao_branco',    'Buchão branco',       'subproduto',   14),
  ('bucho_suino',      'Bucho suíno',         'subproduto',   15),
  ('vergalho_suino',   'Vergalho suíno',      'subproduto',   16),
  ('vergalho_bovino',  'Vergalho bovino',     'subproduto',   17),
  ('esofago',          'Esôfago',             'subproduto',   18),
  ('aorta',            'Aorta',               'subproduto',   19),
  ('bexiga',           'Bexiga',              'subproduto',   20),
  ('tendao',           'Tendão',              'subproduto',   21),
  ('pulmao',           'Pulmão',              'subproduto',   22),
  ('orelha_bovina',    'Orelha bovina',       'subproduto',   23),
  ('omaso',            'Omaso',               'subproduto',   24),
  ('couro_verde',      'Couro verde',         'couro',        25),
  ('couro_wet_blue',   'Couro wet blue',      'couro',        26),
  ('raspa',            'Raspa',               'couro',        27),
  ('sal',              'Sal',                 'insumo',       28),
  ('coracao',          'Coração',             'subproduto',   29),
  ('glandula_mamaria', 'Glândula mamária',    'subproduto',   30),
  ('ubere',            'Úbere',               'subproduto',   31)
ON CONFLICT (codigo) DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TABELA DE DEFEITOS AVALIADOS (seed obrigatório)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_ranking_defeitos (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo      TEXT UNIQUE NOT NULL,
  nome        TEXT NOT NULL,
  descricao   TEXT,
  gravidade   TEXT NOT NULL DEFAULT 'media' CHECK (gravidade IN ('baixa','media','alta','critica')),
  ativo       BOOLEAN DEFAULT true,
  ordem       INTEGER NOT NULL DEFAULT 0,
  criado_em   TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO cq_ranking_defeitos (codigo, nome, gravidade, ordem) VALUES
  ('rasgado',             'Rasgado',              'alta',     1),
  ('furado',              'Furado',               'alta',     2),
  ('pelando',             'Pelando',              'media',    3),
  ('folha_estourada',     'Folha estourada',      'alta',     4),
  ('despojo_sujo',        'Despojo / sujo',       'media',    5),
  ('sebo_corte_incorreto','Sebo / corte incorreto','media',   6),
  ('carne_sem_folha',     'Carne sem folha',      'alta',     7),
  ('agua',                'Água',                 'critica',  8),
  ('orelha_misturada',    'Orelha misturada',     'media',    9)
ON CONFLICT (codigo) DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. INSPEÇÃO POR LOTE (header)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_lote_inspecao (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  id_lote             TEXT UNIQUE NOT NULL,  -- ex: LOT-2026-03-18-001
  fornecedor_codigo   TEXT NOT NULL,
  fornecedor_nome     TEXT,

  data_inspecao       DATE NOT NULL DEFAULT CURRENT_DATE,
  inspetor_id         UUID,
  inspetor_nome       TEXT,
  inspetor_email      TEXT,

  -- Totais do lote (todos os produtos somados)
  total_pecas         INTEGER NOT NULL DEFAULT 0 CHECK (total_pecas >= 0),
  total_class_a       INTEGER NOT NULL DEFAULT 0 CHECK (total_class_a >= 0),
  total_class_b       INTEGER NOT NULL DEFAULT 0 CHECK (total_class_b >= 0),
  total_class_c       INTEGER NOT NULL DEFAULT 0 CHECK (total_class_c >= 0),
  total_defeitos      INTEGER NOT NULL DEFAULT 0,

  -- Percentuais calculados (trigger)
  pct_a               NUMERIC(6,2) DEFAULT 0,
  pct_b               NUMERIC(6,2) DEFAULT 0,
  pct_c               NUMERIC(6,2) DEFAULT 0,
  pct_defeitos        NUMERIC(6,2) DEFAULT 0,

  -- Classificação geral do lote
  classificacao_lote  TEXT CHECK (classificacao_lote IN ('A','B','C')),

  -- Metadata
  observacoes         TEXT,
  status              TEXT NOT NULL DEFAULT 'aberto' CHECK (status IN ('aberto','finalizado','cancelado')),
  latitude            NUMERIC(10,6),
  longitude           NUMERIC(10,6),
  geo_dentro_planta   BOOLEAN,
  session_token       TEXT,
  criado_em           TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em       TIMESTAMPTZ DEFAULT NOW(),

  -- Constraints
  CONSTRAINT chk_lote_abc_total CHECK (total_class_a + total_class_b + total_class_c = total_pecas)
);

CREATE INDEX IF NOT EXISTS idx_lote_inspecao_fornecedor ON cq_lote_inspecao(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_lote_inspecao_data ON cq_lote_inspecao(data_inspecao DESC);
CREATE INDEX IF NOT EXISTS idx_lote_inspecao_status ON cq_lote_inspecao(status);


-- ════════════════════════════════════════════════════════════════════════════
-- 4. INSPEÇÃO POR PRODUTO NO LOTE (detail)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_lote_produto (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lote_id             UUID NOT NULL REFERENCES cq_lote_inspecao(id) ON DELETE CASCADE,
  produto_codigo      TEXT NOT NULL REFERENCES cq_ranking_produtos(codigo),

  -- Classificação ABC
  class_a             INTEGER NOT NULL DEFAULT 0 CHECK (class_a >= 0),
  class_b             INTEGER NOT NULL DEFAULT 0 CHECK (class_b >= 0),
  class_c             INTEGER NOT NULL DEFAULT 0 CHECK (class_c >= 0),
  total_pecas         INTEGER NOT NULL GENERATED ALWAYS AS (class_a + class_b + class_c) STORED,

  -- Percentuais (trigger)
  pct_a               NUMERIC(6,2) DEFAULT 0,
  pct_b               NUMERIC(6,2) DEFAULT 0,
  pct_c               NUMERIC(6,2) DEFAULT 0,

  -- 9 defeitos obrigatórios (todos registrados, mesmo que zero)
  rasgado             INTEGER NOT NULL DEFAULT 0 CHECK (rasgado >= 0),
  furado              INTEGER NOT NULL DEFAULT 0 CHECK (furado >= 0),
  pelando             INTEGER NOT NULL DEFAULT 0 CHECK (pelando >= 0),
  folha_estourada     INTEGER NOT NULL DEFAULT 0 CHECK (folha_estourada >= 0),
  despojo_sujo        INTEGER NOT NULL DEFAULT 0 CHECK (despojo_sujo >= 0),
  sebo_corte_incorreto INTEGER NOT NULL DEFAULT 0 CHECK (sebo_corte_incorreto >= 0),
  carne_sem_folha     INTEGER NOT NULL DEFAULT 0 CHECK (carne_sem_folha >= 0),
  agua                INTEGER NOT NULL DEFAULT 0 CHECK (agua >= 0),
  orelha_misturada    INTEGER NOT NULL DEFAULT 0 CHECK (orelha_misturada >= 0),

  -- Totais de defeitos (trigger)
  total_defeitos      INTEGER NOT NULL DEFAULT 0,
  pct_defeitos        NUMERIC(6,2) DEFAULT 0,
  principal_defeito   TEXT,  -- código do defeito com maior contagem

  -- Classificação do produto neste lote
  classificacao       TEXT CHECK (classificacao IN ('A','B','C')),

  observacoes         TEXT,
  criado_em           TIMESTAMPTZ DEFAULT NOW(),

  -- Unique: um produto por lote
  CONSTRAINT uq_lote_produto UNIQUE (lote_id, produto_codigo)
);

CREATE INDEX IF NOT EXISTS idx_lote_produto_lote ON cq_lote_produto(lote_id);
CREATE INDEX IF NOT EXISTS idx_lote_produto_produto ON cq_lote_produto(produto_codigo);


-- ════════════════════════════════════════════════════════════════════════════
-- 5. TRIGGERS DE CÁLCULO AUTOMÁTICO
-- ════════════════════════════════════════════════════════════════════════════

-- 5.1 Auto-calcular defeitos e percentuais no produto
CREATE OR REPLACE FUNCTION fn_auto_calcular_lote_produto()
RETURNS TRIGGER AS $$
DECLARE
  v_total INTEGER;
  v_defeitos INTEGER;
  v_max_def INTEGER;
  v_max_nome TEXT;
BEGIN
  -- Total peças (já é GENERATED)
  v_total := NEW.class_a + NEW.class_b + NEW.class_c;

  -- Percentuais ABC
  IF v_total > 0 THEN
    NEW.pct_a := ROUND(NEW.class_a::NUMERIC / v_total * 100, 2);
    NEW.pct_b := ROUND(NEW.class_b::NUMERIC / v_total * 100, 2);
    NEW.pct_c := ROUND(NEW.class_c::NUMERIC / v_total * 100, 2);
  ELSE
    NEW.pct_a := 0; NEW.pct_b := 0; NEW.pct_c := 0;
  END IF;

  -- Total defeitos
  v_defeitos := NEW.rasgado + NEW.furado + NEW.pelando + NEW.folha_estourada
              + NEW.despojo_sujo + NEW.sebo_corte_incorreto + NEW.carne_sem_folha
              + NEW.agua + NEW.orelha_misturada;
  NEW.total_defeitos := v_defeitos;

  -- Percentual defeitos (sobre total de peças)
  IF v_total > 0 THEN
    NEW.pct_defeitos := ROUND(v_defeitos::NUMERIC / v_total * 100, 2);
  ELSE
    NEW.pct_defeitos := 0;
  END IF;

  -- Principal defeito
  v_max_def := 0;
  v_max_nome := NULL;
  IF NEW.rasgado > v_max_def THEN v_max_def := NEW.rasgado; v_max_nome := 'rasgado'; END IF;
  IF NEW.furado > v_max_def THEN v_max_def := NEW.furado; v_max_nome := 'furado'; END IF;
  IF NEW.pelando > v_max_def THEN v_max_def := NEW.pelando; v_max_nome := 'pelando'; END IF;
  IF NEW.folha_estourada > v_max_def THEN v_max_def := NEW.folha_estourada; v_max_nome := 'folha_estourada'; END IF;
  IF NEW.despojo_sujo > v_max_def THEN v_max_def := NEW.despojo_sujo; v_max_nome := 'despojo_sujo'; END IF;
  IF NEW.sebo_corte_incorreto > v_max_def THEN v_max_def := NEW.sebo_corte_incorreto; v_max_nome := 'sebo_corte_incorreto'; END IF;
  IF NEW.carne_sem_folha > v_max_def THEN v_max_def := NEW.carne_sem_folha; v_max_nome := 'carne_sem_folha'; END IF;
  IF NEW.agua > v_max_def THEN v_max_def := NEW.agua; v_max_nome := 'agua'; END IF;
  IF NEW.orelha_misturada > v_max_def THEN v_max_def := NEW.orelha_misturada; v_max_nome := 'orelha_misturada'; END IF;
  NEW.principal_defeito := v_max_nome;

  -- Classificação do produto
  IF NEW.pct_defeitos <= 5 THEN
    NEW.classificacao := 'A';
  ELSIF NEW.pct_defeitos <= 15 THEN
    NEW.classificacao := 'B';
  ELSE
    NEW.classificacao := 'C';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_calcular_lote_produto
  BEFORE INSERT OR UPDATE OF class_a, class_b, class_c,
    rasgado, furado, pelando, folha_estourada, despojo_sujo,
    sebo_corte_incorreto, carne_sem_folha, agua, orelha_misturada
  ON cq_lote_produto
  FOR EACH ROW EXECUTE FUNCTION fn_auto_calcular_lote_produto();


-- 5.2 Auto-consolidar totais do lote quando produto é inserido/atualizado
CREATE OR REPLACE FUNCTION fn_consolidar_lote()
RETURNS TRIGGER AS $$
DECLARE
  v_lote_id UUID;
  v_totals RECORD;
BEGIN
  v_lote_id := COALESCE(NEW.lote_id, OLD.lote_id);

  SELECT
    COALESCE(SUM(class_a + class_b + class_c), 0) AS total_pecas,
    COALESCE(SUM(class_a), 0) AS total_a,
    COALESCE(SUM(class_b), 0) AS total_b,
    COALESCE(SUM(class_c), 0) AS total_c,
    COALESCE(SUM(total_defeitos), 0) AS total_defeitos
  INTO v_totals
  FROM cq_lote_produto
  WHERE lote_id = v_lote_id;

  UPDATE cq_lote_inspecao SET
    total_pecas    = v_totals.total_pecas,
    total_class_a  = v_totals.total_a,
    total_class_b  = v_totals.total_b,
    total_class_c  = v_totals.total_c,
    total_defeitos = v_totals.total_defeitos,
    pct_a = CASE WHEN v_totals.total_pecas > 0 THEN ROUND(v_totals.total_a::NUMERIC / v_totals.total_pecas * 100, 2) ELSE 0 END,
    pct_b = CASE WHEN v_totals.total_pecas > 0 THEN ROUND(v_totals.total_b::NUMERIC / v_totals.total_pecas * 100, 2) ELSE 0 END,
    pct_c = CASE WHEN v_totals.total_pecas > 0 THEN ROUND(v_totals.total_c::NUMERIC / v_totals.total_pecas * 100, 2) ELSE 0 END,
    pct_defeitos = CASE WHEN v_totals.total_pecas > 0 THEN ROUND(v_totals.total_defeitos::NUMERIC / v_totals.total_pecas * 100, 2) ELSE 0 END,
    classificacao_lote = CASE
      WHEN v_totals.total_pecas = 0 THEN NULL
      WHEN ROUND(v_totals.total_defeitos::NUMERIC / v_totals.total_pecas * 100, 2) <= 5 THEN 'A'
      WHEN ROUND(v_totals.total_defeitos::NUMERIC / v_totals.total_pecas * 100, 2) <= 15 THEN 'B'
      ELSE 'C'
    END,
    atualizado_em = NOW()
  WHERE id = v_lote_id;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_consolidar_lote
  AFTER INSERT OR UPDATE OR DELETE ON cq_lote_produto
  FOR EACH ROW EXECUTE FUNCTION fn_consolidar_lote();


-- ════════════════════════════════════════════════════════════════════════════
-- 6. VIEW: RANKING POR FORNECEDOR × PRODUTO (últimos 90 dias)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_ranking_fornecedor_produto AS
WITH dados AS (
  SELECT
    li.fornecedor_codigo,
    li.fornecedor_nome,
    lp.produto_codigo,
    rp.nome AS produto_nome,
    rp.grupo AS produto_grupo,
    SUM(lp.class_a + lp.class_b + lp.class_c) AS total_pecas,
    SUM(lp.class_a) AS total_a,
    SUM(lp.class_b) AS total_b,
    SUM(lp.class_c) AS total_c,
    SUM(lp.total_defeitos) AS total_defeitos,
    SUM(lp.rasgado) AS total_rasgado,
    SUM(lp.furado) AS total_furado,
    SUM(lp.pelando) AS total_pelando,
    SUM(lp.folha_estourada) AS total_folha_estourada,
    SUM(lp.despojo_sujo) AS total_despojo_sujo,
    SUM(lp.sebo_corte_incorreto) AS total_sebo_corte,
    SUM(lp.carne_sem_folha) AS total_carne_sem_folha,
    SUM(lp.agua) AS total_agua,
    SUM(lp.orelha_misturada) AS total_orelha_misturada,
    COUNT(DISTINCT li.id) AS total_lotes
  FROM cq_lote_produto lp
  JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  JOIN cq_ranking_produtos rp ON rp.codigo = lp.produto_codigo
  WHERE li.status = 'finalizado'
    AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo, rp.nome, rp.grupo
)
SELECT
  d.*,
  ROUND(d.total_a::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_a,
  ROUND(d.total_b::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_b,
  ROUND(d.total_c::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_c,
  ROUND(d.total_defeitos::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_defeitos,

  -- Percentual de cada defeito
  ROUND(d.total_rasgado::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_rasgado,
  ROUND(d.total_furado::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_furado,
  ROUND(d.total_pelando::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_pelando,
  ROUND(d.total_folha_estourada::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_folha_estourada,
  ROUND(d.total_despojo_sujo::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_despojo_sujo,
  ROUND(d.total_sebo_corte::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_sebo_corte,
  ROUND(d.total_carne_sem_folha::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_carne_sem_folha,
  ROUND(d.total_agua::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_agua,
  ROUND(d.total_orelha_misturada::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2) AS pct_orelha_misturada,

  -- Score: 100 - (%C * 2 + %defeitos)  clamp 0-100
  GREATEST(0, LEAST(100,
    100
    - COALESCE(ROUND(d.total_c::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2), 0) * 2
    - COALESCE(ROUND(d.total_defeitos::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2), 0)
  )) AS score,

  -- Principal defeito
  CASE GREATEST(
    d.total_rasgado, d.total_furado, d.total_pelando, d.total_folha_estourada,
    d.total_despojo_sujo, d.total_sebo_corte, d.total_carne_sem_folha,
    d.total_agua, d.total_orelha_misturada
  )
    WHEN d.total_rasgado THEN 'Rasgado'
    WHEN d.total_furado THEN 'Furado'
    WHEN d.total_pelando THEN 'Pelando'
    WHEN d.total_folha_estourada THEN 'Folha estourada'
    WHEN d.total_despojo_sujo THEN 'Despojo / sujo'
    WHEN d.total_sebo_corte THEN 'Sebo / corte incorreto'
    WHEN d.total_carne_sem_folha THEN 'Carne sem folha'
    WHEN d.total_agua THEN 'Água'
    WHEN d.total_orelha_misturada THEN 'Orelha misturada'
    ELSE NULL
  END AS principal_defeito,

  -- Ranking por score
  ROW_NUMBER() OVER (ORDER BY
    GREATEST(0, LEAST(100,
      100
      - COALESCE(ROUND(d.total_c::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2), 0) * 2
      - COALESCE(ROUND(d.total_defeitos::NUMERIC / NULLIF(d.total_pecas, 0) * 100, 2), 0)
    )) DESC
  ) AS ranking

FROM dados d
WHERE d.total_pecas > 0
ORDER BY score DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. VIEW: RANKING GERAL POR FORNECEDOR (ponderado por volume)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_ranking_geral_fornecedor AS
WITH por_fornecedor AS (
  SELECT
    li.fornecedor_codigo,
    li.fornecedor_nome,
    SUM(lp.class_a + lp.class_b + lp.class_c) AS total_pecas,
    SUM(lp.class_a) AS total_a,
    SUM(lp.class_b) AS total_b,
    SUM(lp.class_c) AS total_c,
    SUM(lp.total_defeitos) AS total_defeitos,
    COUNT(DISTINCT li.id) AS total_lotes,
    COUNT(DISTINCT lp.produto_codigo) AS total_produtos_distintos,
    SUM(lp.rasgado) AS rasgado,
    SUM(lp.furado) AS furado,
    SUM(lp.pelando) AS pelando,
    SUM(lp.folha_estourada) AS folha_estourada,
    SUM(lp.despojo_sujo) AS despojo_sujo,
    SUM(lp.sebo_corte_incorreto) AS sebo_corte,
    SUM(lp.carne_sem_folha) AS carne_sem_folha,
    SUM(lp.agua) AS agua,
    SUM(lp.orelha_misturada) AS orelha_misturada
  FROM cq_lote_produto lp
  JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado'
    AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome
),
volume_total AS (
  SELECT SUM(total_pecas) AS total_geral FROM por_fornecedor
)
SELECT
  pf.fornecedor_codigo,
  pf.fornecedor_nome,
  ca.uf,
  pf.total_pecas,
  pf.total_lotes,
  pf.total_produtos_distintos,
  ROUND(pf.total_a::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) AS pct_a,
  ROUND(pf.total_b::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) AS pct_b,
  ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) AS pct_c,
  ROUND(pf.total_defeitos::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) AS pct_defeitos,

  -- Score ponderado: penaliza mais %C e defeitos
  GREATEST(0, LEAST(100,
    100
    - COALESCE(ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2), 0) * 2
    - COALESCE(ROUND(pf.total_defeitos::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2), 0)
  )) AS score,

  -- Peso relativo (volume do fornecedor / volume total)
  ROUND(pf.total_pecas::NUMERIC / NULLIF(vt.total_geral, 0) * 100, 1) AS peso_volume_pct,

  -- Status
  CASE
    WHEN ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) > 22.5 THEN 'bloqueado'
    WHEN ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) > 15 THEN 'critico'
    WHEN ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2) > 10.5 THEN 'atencao'
    ELSE 'normal'
  END AS status_qualidade,

  -- Principal defeito
  CASE GREATEST(pf.rasgado, pf.furado, pf.pelando, pf.folha_estourada,
    pf.despojo_sujo, pf.sebo_corte, pf.carne_sem_folha, pf.agua, pf.orelha_misturada)
    WHEN pf.rasgado THEN 'Rasgado'
    WHEN pf.furado THEN 'Furado'
    WHEN pf.pelando THEN 'Pelando'
    WHEN pf.folha_estourada THEN 'Folha estourada'
    WHEN pf.despojo_sujo THEN 'Despojo / sujo'
    WHEN pf.sebo_corte THEN 'Sebo / corte incorreto'
    WHEN pf.carne_sem_folha THEN 'Carne sem folha'
    WHEN pf.agua THEN 'Água'
    WHEN pf.orelha_misturada THEN 'Orelha misturada'
    ELSE NULL
  END AS principal_defeito,

  ROW_NUMBER() OVER (ORDER BY
    GREATEST(0, LEAST(100,
      100
      - COALESCE(ROUND(pf.total_c::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2), 0) * 2
      - COALESCE(ROUND(pf.total_defeitos::NUMERIC / NULLIF(pf.total_pecas, 0) * 100, 2), 0)
    )) DESC
  ) AS ranking

FROM por_fornecedor pf
CROSS JOIN volume_total vt
LEFT JOIN cadastros_atak ca ON ca.codigo = pf.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE pf.total_pecas > 0
ORDER BY score DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 8. VIEW: RANKING DE DEFEITOS POR FORNECEDOR
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_ranking_defeitos_fornecedor AS
WITH defeitos_unpivot AS (
  SELECT
    li.fornecedor_codigo,
    li.fornecedor_nome,
    lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c) AS total_pecas,
    'rasgado' AS defeito, SUM(lp.rasgado) AS qtd
  FROM cq_lote_produto lp
  JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'furado', SUM(lp.furado)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'pelando', SUM(lp.pelando)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'folha_estourada', SUM(lp.folha_estourada)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'despojo_sujo', SUM(lp.despojo_sujo)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'sebo_corte_incorreto', SUM(lp.sebo_corte_incorreto)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'carne_sem_folha', SUM(lp.carne_sem_folha)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'agua', SUM(lp.agua)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo

  UNION ALL
  SELECT li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c), 'orelha_misturada', SUM(lp.orelha_misturada)
  FROM cq_lote_produto lp JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado' AND li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.produto_codigo
)
SELECT
  fornecedor_codigo,
  fornecedor_nome,
  defeito,
  rd.nome AS defeito_nome,
  rd.gravidade AS defeito_gravidade,
  SUM(qtd) AS total_ocorrencias,
  SUM(total_pecas) AS total_pecas_inspecionadas,
  ROUND(SUM(qtd)::NUMERIC / NULLIF(SUM(total_pecas), 0) * 100, 2) AS pct_defeito,
  COUNT(DISTINCT produto_codigo) AS produtos_afetados,

  -- Alertas
  CASE
    WHEN ROUND(SUM(qtd)::NUMERIC / NULLIF(SUM(total_pecas), 0) * 100, 2) > 5 THEN true
    ELSE false
  END AS alerta_acima_5pct,

  ROW_NUMBER() OVER (
    PARTITION BY fornecedor_codigo
    ORDER BY SUM(qtd) DESC
  ) AS ranking_defeito

FROM defeitos_unpivot du
JOIN cq_ranking_defeitos rd ON rd.codigo = du.defeito
WHERE qtd > 0
GROUP BY fornecedor_codigo, fornecedor_nome, defeito, rd.nome, rd.gravidade
ORDER BY fornecedor_codigo, total_ocorrencias DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 9. VIEW: TENDÊNCIA 30 DIAS
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_ranking_tendencia_30d AS
WITH periodo_atual AS (
  SELECT
    li.fornecedor_codigo,
    li.fornecedor_nome,
    SUM(lp.class_a + lp.class_b + lp.class_c) AS total_pecas,
    ROUND(SUM(lp.class_a)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_a,
    ROUND(SUM(lp.class_c)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_c,
    ROUND(SUM(lp.total_defeitos)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_defeitos,
    GREATEST(0, LEAST(100,
      100
      - COALESCE(ROUND(SUM(lp.class_c)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2), 0) * 2
      - COALESCE(ROUND(SUM(lp.total_defeitos)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2), 0)
    )) AS score
  FROM cq_lote_produto lp
  JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado'
    AND li.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY li.fornecedor_codigo, li.fornecedor_nome
),
periodo_anterior AS (
  SELECT
    li.fornecedor_codigo,
    SUM(lp.class_a + lp.class_b + lp.class_c) AS total_pecas,
    ROUND(SUM(lp.class_a)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_a,
    ROUND(SUM(lp.class_c)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_c,
    ROUND(SUM(lp.total_defeitos)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2) AS pct_defeitos,
    GREATEST(0, LEAST(100,
      100
      - COALESCE(ROUND(SUM(lp.class_c)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2), 0) * 2
      - COALESCE(ROUND(SUM(lp.total_defeitos)::NUMERIC / NULLIF(SUM(lp.class_a + lp.class_b + lp.class_c), 0) * 100, 2), 0)
    )) AS score
  FROM cq_lote_produto lp
  JOIN cq_lote_inspecao li ON li.id = lp.lote_id
  WHERE li.status = 'finalizado'
    AND li.data_inspecao >= CURRENT_DATE - INTERVAL '60 days'
    AND li.data_inspecao < CURRENT_DATE - INTERVAL '30 days'
  GROUP BY li.fornecedor_codigo
)
SELECT
  pa.fornecedor_codigo,
  pa.fornecedor_nome,
  pa.total_pecas AS pecas_30d,
  pa.pct_a AS pct_a_30d,
  pa.pct_c AS pct_c_30d,
  pa.pct_defeitos AS pct_defeitos_30d,
  pa.score AS score_30d,

  ant.total_pecas AS pecas_anterior,
  ant.pct_a AS pct_a_anterior,
  ant.pct_c AS pct_c_anterior,
  ant.pct_defeitos AS pct_defeitos_anterior,
  ant.score AS score_anterior,

  -- Variações
  pa.score - COALESCE(ant.score, pa.score) AS variacao_score,
  pa.pct_a - COALESCE(ant.pct_a, pa.pct_a) AS variacao_pct_a,
  pa.pct_c - COALESCE(ant.pct_c, pa.pct_c) AS variacao_pct_c,
  pa.pct_defeitos - COALESCE(ant.pct_defeitos, pa.pct_defeitos) AS variacao_pct_defeitos,

  -- Classificação de tendência
  CASE
    WHEN pa.score - COALESCE(ant.score, pa.score) > 3 THEN 'melhorando'
    WHEN pa.score - COALESCE(ant.score, pa.score) < -3 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,

  -- Flag se fornecedor é novo (sem dados anteriores)
  CASE WHEN ant.score IS NULL THEN true ELSE false END AS fornecedor_novo

FROM periodo_atual pa
LEFT JOIN periodo_anterior ant ON ant.fornecedor_codigo = pa.fornecedor_codigo
ORDER BY pa.score DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 10. VIEW: ALERTAS AUTOMÁTICOS
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_ranking_alertas AS
-- Alerta 1: Defeito acima de 5%
SELECT
  'defeito_acima_5pct' AS tipo_alerta,
  'alta' AS severidade,
  fornecedor_codigo,
  fornecedor_nome,
  defeito_nome || ' em ' || fornecedor_nome AS titulo,
  FORMAT('%s está com %s%% de %s (limite: 5%%)', fornecedor_nome, pct_defeito, defeito_nome) AS mensagem,
  JSONB_BUILD_OBJECT(
    'defeito', defeito, 'pct', pct_defeito,
    'total_ocorrencias', total_ocorrencias, 'produtos_afetados', produtos_afetados
  ) AS dados
FROM vw_ranking_defeitos_fornecedor
WHERE pct_defeito > 5

UNION ALL

-- Alerta 2: Repetição de defeito (mesmo defeito como principal em >3 lotes consecutivos)
SELECT
  'defeito_recorrente' AS tipo_alerta,
  'media' AS severidade,
  li.fornecedor_codigo,
  li.fornecedor_nome,
  'Defeito recorrente: ' || lp.principal_defeito AS titulo,
  FORMAT('%s tem %s como principal defeito em %s lotes recentes',
    li.fornecedor_nome,
    lp.principal_defeito,
    COUNT(DISTINCT li.id)
  ) AS mensagem,
  JSONB_BUILD_OBJECT(
    'defeito', lp.principal_defeito,
    'lotes_afetados', COUNT(DISTINCT li.id)
  ) AS dados
FROM cq_lote_produto lp
JOIN cq_lote_inspecao li ON li.id = lp.lote_id
WHERE li.status = 'finalizado'
  AND li.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'
  AND lp.principal_defeito IS NOT NULL
GROUP BY li.fornecedor_codigo, li.fornecedor_nome, lp.principal_defeito
HAVING COUNT(DISTINCT li.id) >= 3

UNION ALL

-- Alerta 3: Inconsistência de dados (A+B+C = 0 em lote finalizado)
SELECT
  'inconsistencia_dados' AS tipo_alerta,
  'critica' AS severidade,
  li.fornecedor_codigo,
  li.fornecedor_nome,
  'Lote sem classificação ABC' AS titulo,
  FORMAT('Lote %s do fornecedor %s está finalizado mas sem classificação', li.id_lote, li.fornecedor_nome) AS mensagem,
  JSONB_BUILD_OBJECT('lote_id', li.id, 'id_lote', li.id_lote) AS dados
FROM cq_lote_inspecao li
WHERE li.status = 'finalizado'
  AND li.total_pecas = 0
  AND li.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

-- Alerta 4: Fornecedor piorando (tendência negativa > 3%)
SELECT
  'fornecedor_piorando' AS tipo_alerta,
  'alta' AS severidade,
  fornecedor_codigo,
  fornecedor_nome,
  'Tendência de piora: ' || fornecedor_nome AS titulo,
  FORMAT('%s piorou %s pontos nos últimos 30 dias (score: %s → %s)',
    fornecedor_nome,
    ABS(variacao_score),
    score_anterior,
    score_30d
  ) AS mensagem,
  JSONB_BUILD_OBJECT(
    'score_atual', score_30d,
    'score_anterior', score_anterior,
    'variacao', variacao_score,
    'tendencia', tendencia
  ) AS dados
FROM vw_ranking_tendencia_30d
WHERE tendencia = 'piorando';


-- ════════════════════════════════════════════════════════════════════════════
-- 11. FUNCTION: Gerar ID de lote automaticamente
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_gerar_id_lote()
RETURNS TRIGGER AS $$
DECLARE
  v_seq INTEGER;
BEGIN
  IF NEW.id_lote IS NULL OR NEW.id_lote = '' THEN
    SELECT COALESCE(MAX(
      CAST(NULLIF(SPLIT_PART(id_lote, '-', 5), '') AS INTEGER)
    ), 0) + 1
    INTO v_seq
    FROM cq_lote_inspecao
    WHERE data_inspecao = NEW.data_inspecao;

    NEW.id_lote := FORMAT('LOT-%s-%03s',
      TO_CHAR(NEW.data_inspecao, 'YYYY-MM-DD'),
      v_seq
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_gerar_id_lote
  BEFORE INSERT ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_gerar_id_lote();


-- ════════════════════════════════════════════════════════════════════════════
-- 12. RLS (Row Level Security)
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_ranking_produtos ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_ranking_defeitos ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_lote_inspecao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_lote_produto ENABLE ROW LEVEL SECURITY;

CREATE POLICY "auth_read_ranking_produtos" ON cq_ranking_produtos FOR SELECT USING (true);
CREATE POLICY "auth_read_ranking_defeitos" ON cq_ranking_defeitos FOR SELECT USING (true);
CREATE POLICY "auth_read_lote_inspecao" ON cq_lote_inspecao FOR SELECT USING (true);
CREATE POLICY "auth_all_lote_inspecao" ON cq_lote_inspecao FOR ALL USING (true);
CREATE POLICY "auth_read_lote_produto" ON cq_lote_produto FOR SELECT USING (true);
CREATE POLICY "auth_all_lote_produto" ON cq_lote_produto FOR ALL USING (true);


-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 05 CONCLUÍDA — Sistema de Ranking';
  RAISE NOTICE '  Tabelas: cq_ranking_produtos (31), cq_ranking_defeitos (9)';
  RAISE NOTICE '  Tabelas: cq_lote_inspecao, cq_lote_produto';
  RAISE NOTICE '  Views: vw_ranking_fornecedor_produto';
  RAISE NOTICE '         vw_ranking_geral_fornecedor';
  RAISE NOTICE '         vw_ranking_defeitos_fornecedor';
  RAISE NOTICE '         vw_ranking_tendencia_30d';
  RAISE NOTICE '         vw_ranking_alertas';
  RAISE NOTICE '  Triggers: auto-cálculo ABC/%/defeitos, consolidação lote';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
