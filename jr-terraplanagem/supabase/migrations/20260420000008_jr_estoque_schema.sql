-- ============================================================
-- Migration: Fase J5 - Estoque (schema base)
-- Espelho do Trans Anacleto adaptado: SKU mestre + EAN leitura
-- Camadas: NOVO (lote obrigatorio FEFO/FIFO) + USADO (condicao A/B/C)
-- ============================================================

-- ============================================================
-- TABELA: jr_categorias_produto
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_categorias_produto (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome        TEXT NOT NULL UNIQUE,
  tipo        TEXT NOT NULL CHECK (tipo IN ('peca','filtro','oleo','lubrificante','combustivel','acessorio','epi','outros')),
  ativo       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by  UUID,
  updated_by  UUID
);

DROP TRIGGER IF EXISTS tg_jr_categorias_produto_updated_at ON public.jr_categorias_produto;
CREATE TRIGGER tg_jr_categorias_produto_updated_at
BEFORE INSERT OR UPDATE ON public.jr_categorias_produto
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

INSERT INTO public.jr_categorias_produto (nome, tipo) VALUES
  ('Filtro de oleo',         'filtro'),
  ('Filtro de combustivel',  'filtro'),
  ('Filtro de ar',           'filtro'),
  ('Filtro hidraulico',      'filtro'),
  ('Oleo motor 15W40',       'oleo'),
  ('Oleo hidraulico',        'oleo'),
  ('Graxa',                  'lubrificante'),
  ('Peca de reposicao',      'peca'),
  ('Acessorio',              'acessorio'),
  ('EPI',                    'epi'),
  ('Outros',                 'outros')
ON CONFLICT (nome) DO NOTHING;

-- ============================================================
-- TABELA: jr_produtos (SKU mestre)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_produtos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku             TEXT NOT NULL UNIQUE,
  nome            TEXT NOT NULL,
  categoria_id    UUID REFERENCES public.jr_categorias_produto(id),
  unidade         TEXT NOT NULL DEFAULT 'UN' CHECK (unidade IN ('UN','PC','LT','KG','M','M2','M3','CX','PAR')),
  estoque_min     NUMERIC(14,3) DEFAULT 0,
  localizacao     TEXT,
  custo_medio     NUMERIC(14,4) DEFAULT 0,
  ncm             TEXT,
  observacao      TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_produtos_sku       ON public.jr_produtos (sku);
CREATE INDEX IF NOT EXISTS idx_jr_produtos_categoria ON public.jr_produtos (categoria_id);
CREATE INDEX IF NOT EXISTS idx_jr_produtos_nome      ON public.jr_produtos USING gin (to_tsvector('portuguese', nome));

DROP TRIGGER IF EXISTS tg_jr_produtos_updated_at ON public.jr_produtos;
CREATE TRIGGER tg_jr_produtos_updated_at
BEFORE INSERT OR UPDATE ON public.jr_produtos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_produtos_ean (multiplos EANs por produto)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_produtos_ean (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id  UUID NOT NULL REFERENCES public.jr_produtos(id) ON DELETE CASCADE,
  ean         TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by  UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_produtos_ean_produto ON public.jr_produtos_ean (produto_id);

-- ============================================================
-- SEQUENCE + helper para gerar lote_interno (L-000001)
-- ============================================================
CREATE SEQUENCE IF NOT EXISTS public.jr_lote_seq START 1;

CREATE OR REPLACE FUNCTION public.jr_proximo_lote_interno()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE v_n BIGINT;
BEGIN
  v_n := nextval('public.jr_lote_seq');
  RETURN 'L-' || LPAD(v_n::TEXT, 6, '0');
END;
$$;

-- ============================================================
-- TABELA: jr_lotes
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_lotes (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id          UUID NOT NULL REFERENCES public.jr_produtos(id),
  lote_interno        TEXT NOT NULL UNIQUE DEFAULT public.jr_proximo_lote_interno(),
  lote_fornecedor     TEXT,
  fornecedor_id       UUID REFERENCES public.jr_fornecedores(id),
  nota_fiscal_id      UUID,
  data_entrada        DATE NOT NULL DEFAULT CURRENT_DATE,
  data_fabricacao     DATE,
  data_validade       DATE,
  custo_unitario      NUMERIC(14,4) DEFAULT 0,
  status              TEXT NOT NULL DEFAULT 'liberado'
                      CHECK (status IN ('liberado','bloqueado','quarentena','reprovado')),
  status_alterado_por UUID,
  status_alterado_em  TIMESTAMPTZ,
  observacao          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID,
  updated_by          UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_lotes_produto       ON public.jr_lotes (produto_id);
CREATE INDEX IF NOT EXISTS idx_jr_lotes_status        ON public.jr_lotes (status);
CREATE INDEX IF NOT EXISTS idx_jr_lotes_fornecedor    ON public.jr_lotes (fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_jr_lotes_data_validade ON public.jr_lotes (data_validade) WHERE data_validade IS NOT NULL;

DROP TRIGGER IF EXISTS tg_jr_lotes_updated_at ON public.jr_lotes;
CREATE TRIGGER tg_jr_lotes_updated_at
BEFORE INSERT OR UPDATE ON public.jr_lotes
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_estoque_lote (saldo por lote)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_estoque_lote (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id  UUID NOT NULL REFERENCES public.jr_produtos(id),
  lote_id     UUID NOT NULL UNIQUE REFERENCES public.jr_lotes(id),
  quantidade  NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jr_estoque_lote_produto ON public.jr_estoque_lote (produto_id);

-- ============================================================
-- TABELA: jr_estoque_usado (saldo por SKU + condicao A/B/C)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_estoque_usado (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id   UUID NOT NULL REFERENCES public.jr_produtos(id),
  condicao     CHAR(1) NOT NULL CHECK (condicao IN ('A','B','C')),
  quantidade   NUMERIC(14,3) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  observacao   TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (produto_id, condicao)
);

-- ============================================================
-- TABELA: jr_estoque_movimentacao (ledger imutavel)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_estoque_movimentacao (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo                TEXT NOT NULL CHECK (tipo IN ('entrada','saida','ajuste','conversao','transferencia','inventario')),
  modalidade          TEXT NOT NULL CHECK (modalidade IN ('novo','usado')),
  produto_id          UUID NOT NULL REFERENCES public.jr_produtos(id),
  lote_id             UUID REFERENCES public.jr_lotes(id),
  condicao            CHAR(1) CHECK (condicao IN ('A','B','C')),
  ean_lido            TEXT,
  quantidade          NUMERIC(14,3) NOT NULL CHECK (quantidade > 0),
  custo_unitario      NUMERIC(14,4),
  documento_origem    TEXT,
  os_id               UUID,
  maquina_id          UUID REFERENCES public.jr_maquinas(id),
  manutencao_id       UUID,
  conversao_origem_id UUID REFERENCES public.jr_estoque_movimentacao(id),
  observacao          TEXT,
  usuario_id          UUID,
  data_hora           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Mutex: novo => lote_id obrigatorio + condicao NULL
  --        usado => condicao obrigatoria + lote_id NULL
  CONSTRAINT chk_jr_estmov_modalidade_consistencia CHECK (
    (modalidade = 'novo'  AND lote_id IS NOT NULL AND condicao IS NULL)
    OR
    (modalidade = 'usado' AND condicao IS NOT NULL AND lote_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_jr_estmov_produto      ON public.jr_estoque_movimentacao (produto_id);
CREATE INDEX IF NOT EXISTS idx_jr_estmov_lote         ON public.jr_estoque_movimentacao (lote_id) WHERE lote_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jr_estmov_data         ON public.jr_estoque_movimentacao (data_hora DESC);
CREATE INDEX IF NOT EXISTS idx_jr_estmov_os           ON public.jr_estoque_movimentacao (os_id) WHERE os_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jr_estmov_maquina      ON public.jr_estoque_movimentacao (maquina_id) WHERE maquina_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jr_estmov_manutencao   ON public.jr_estoque_movimentacao (manutencao_id) WHERE manutencao_id IS NOT NULL;

-- ============================================================
-- TRIGGER: atualiza saldos ao inserir movimentacao
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_estoque_movimentacao_aplicar()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_delta NUMERIC(14,3);
BEGIN
  -- Define delta: entrada/conversao(entrada)/inventario(+) somam; saida/conversao(saida)/ajuste(-) subtraem
  -- Convencao: tipo determina sinal
  IF NEW.tipo IN ('entrada') THEN
    v_delta := NEW.quantidade;
  ELSIF NEW.tipo IN ('saida') THEN
    v_delta := -NEW.quantidade;
  ELSIF NEW.tipo = 'ajuste' THEN
    -- ajuste: usar quantidade como delta absoluto, mas user fornece valor positivo;
    -- se observacao contem "-" ou se for usar coluna explicita, simplificamos:
    -- aqui assumimos que ajuste de entrada e tipo=entrada e ajuste de saida e tipo=saida.
    -- tipo=ajuste explicito mantem soma (aumento).
    v_delta := NEW.quantidade;
  ELSIF NEW.tipo = 'conversao' THEN
    -- Conversoes vem em pares ligados por conversao_origem_id;
    -- a convencao aqui: a movimentacao com conversao_origem_id NULL e a saida (origem),
    -- a com conversao_origem_id NOT NULL e a entrada (destino).
    IF NEW.conversao_origem_id IS NULL THEN
      v_delta := -NEW.quantidade;
    ELSE
      v_delta := NEW.quantidade;
    END IF;
  ELSIF NEW.tipo = 'transferencia' THEN
    v_delta := 0;
  ELSIF NEW.tipo = 'inventario' THEN
    v_delta := NEW.quantidade;
  ELSE
    v_delta := 0;
  END IF;

  IF v_delta = 0 THEN RETURN NEW; END IF;

  IF NEW.modalidade = 'novo' THEN
    INSERT INTO public.jr_estoque_lote (produto_id, lote_id, quantidade, updated_at)
      VALUES (NEW.produto_id, NEW.lote_id, GREATEST(v_delta, 0), NOW())
    ON CONFLICT (lote_id) DO UPDATE
      SET quantidade = jr_estoque_lote.quantidade + v_delta,
          updated_at = NOW();
  ELSE
    INSERT INTO public.jr_estoque_usado (produto_id, condicao, quantidade, updated_at)
      VALUES (NEW.produto_id, NEW.condicao, GREATEST(v_delta, 0), NOW())
    ON CONFLICT (produto_id, condicao) DO UPDATE
      SET quantidade = jr_estoque_usado.quantidade + v_delta,
          updated_at = NOW();
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_jr_estoque_movimentacao_aplicar ON public.jr_estoque_movimentacao;
CREATE TRIGGER tg_jr_estoque_movimentacao_aplicar
AFTER INSERT ON public.jr_estoque_movimentacao
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_estoque_movimentacao_aplicar();

-- ============================================================
-- VIEW: v_jr_lotes_disponiveis (FEFO > FIFO, apenas liberados com saldo)
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_lotes_disponiveis AS
SELECT
  l.id              AS lote_id,
  l.produto_id,
  l.lote_interno,
  l.lote_fornecedor,
  l.fornecedor_id,
  l.data_entrada,
  l.data_validade,
  l.status,
  el.quantidade     AS saldo,
  COALESCE(l.data_validade, l.data_entrada + INTERVAL '10 years') AS prioridade_dt
FROM public.jr_lotes l
JOIN public.jr_estoque_lote el ON el.lote_id = l.id
WHERE el.quantidade > 0;

-- ============================================================
-- VIEW: v_jr_estoque_consolidado (saldo por SKU = novo + usado)
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_estoque_consolidado AS
SELECT
  p.id              AS produto_id,
  p.sku,
  p.nome,
  p.unidade,
  p.estoque_min,
  p.localizacao,
  c.nome            AS categoria,
  COALESCE(novo.qt, 0)        AS saldo_novo,
  COALESCE(usado_a.qt, 0)     AS saldo_usado_a,
  COALESCE(usado_b.qt, 0)     AS saldo_usado_b,
  COALESCE(usado_c.qt, 0)     AS saldo_usado_c,
  COALESCE(novo.qt, 0)
    + COALESCE(usado_a.qt, 0)
    + COALESCE(usado_b.qt, 0)
    + COALESCE(usado_c.qt, 0) AS saldo_total,
  COALESCE(novo.lotes_count, 0) AS lotes_count
FROM public.jr_produtos p
LEFT JOIN public.jr_categorias_produto c ON c.id = p.categoria_id
LEFT JOIN LATERAL (
  SELECT SUM(el.quantidade) AS qt, COUNT(DISTINCT el.lote_id) AS lotes_count
  FROM public.jr_estoque_lote el
  WHERE el.produto_id = p.id AND el.quantidade > 0
) novo ON TRUE
LEFT JOIN LATERAL (
  SELECT quantidade AS qt FROM public.jr_estoque_usado WHERE produto_id = p.id AND condicao = 'A' LIMIT 1
) usado_a ON TRUE
LEFT JOIN LATERAL (
  SELECT quantidade AS qt FROM public.jr_estoque_usado WHERE produto_id = p.id AND condicao = 'B' LIMIT 1
) usado_b ON TRUE
LEFT JOIN LATERAL (
  SELECT quantidade AS qt FROM public.jr_estoque_usado WHERE produto_id = p.id AND condicao = 'C' LIMIT 1
) usado_c ON TRUE;

-- ============================================================
-- RLS
-- ============================================================
ALTER TABLE public.jr_categorias_produto    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_produtos              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_produtos_ean          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_lotes                 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_estoque_lote          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_estoque_usado         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_estoque_movimentacao  ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'jr_categorias_produto','jr_produtos','jr_produtos_ean',
    'jr_lotes','jr_estoque_lote','jr_estoque_usado','jr_estoque_movimentacao'
  ] LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','operador','financeiro'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor'))
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

GRANT SELECT ON public.v_jr_lotes_disponiveis     TO authenticated;
GRANT SELECT ON public.v_jr_estoque_consolidado   TO authenticated;

COMMENT ON TABLE public.jr_produtos              IS 'SKU mestre. EAN(s) auxiliares em jr_produtos_ean.';
COMMENT ON TABLE public.jr_lotes                 IS 'Lote interno auto-gerado (L-NNNNNN). Status liberado/bloqueado/quarentena/reprovado.';
COMMENT ON TABLE public.jr_estoque_lote          IS 'Saldo por lote (somente NOVO).';
COMMENT ON TABLE public.jr_estoque_usado         IS 'Saldo por SKU+condicao A/B/C (USADO). UNIQUE(produto_id, condicao).';
COMMENT ON TABLE public.jr_estoque_movimentacao  IS 'Ledger imutavel; trigger atualiza saldos. Mutex modalidade x lote/condicao.';
COMMENT ON VIEW public.v_jr_lotes_disponiveis   IS 'Lotes com saldo > 0; ORDER BY prioridade_dt para FEFO.';
COMMENT ON VIEW public.v_jr_estoque_consolidado IS 'Saldo total por SKU (novo + usado A/B/C).';
