-- ============================================================
-- Migration: cadastros (Fase 2)
--   trans_motoristas
--   trans_veiculos
--   trans_fornecedores
--   trans_categorias_produto
--   trans_produtos           (SKU mestre)
--   trans_produtos_ean       (N EANs por produto, unique global)
-- Todas as tabelas herdam padrão de colunas auditáveis e usam
-- o trigger trans_tg_set_updated_at criado na migration 01.
-- ============================================================

-- ============================================================
-- trans_motoristas
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_motoristas (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome            TEXT NOT NULL,
  cpf             TEXT UNIQUE,
  cnh             TEXT,
  categoria_cnh   TEXT CHECK (categoria_cnh IN ('A','B','C','D','E','AB','AC','AD','AE')),
  validade_cnh    DATE,
  telefone        TEXT,
  email           TEXT,
  usuario_id      UUID REFERENCES public.trans_usuarios(id) ON DELETE SET NULL,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  observacao      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_motoristas_nome_ativo ON public.trans_motoristas (nome) WHERE ativo = TRUE;
CREATE INDEX IF NOT EXISTS idx_trans_motoristas_cpf        ON public.trans_motoristas (cpf) WHERE cpf IS NOT NULL;

DROP TRIGGER IF EXISTS tg_trans_motoristas_updated_at ON public.trans_motoristas;
CREATE TRIGGER tg_trans_motoristas_updated_at
BEFORE INSERT OR UPDATE ON public.trans_motoristas
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- trans_veiculos
-- km_atual monotônico (trigger que rejeita retrocesso).
-- motorista_atual_id: FK para trans_motoristas (nullable).
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_veiculos (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  placa                 TEXT NOT NULL UNIQUE,
  modelo                TEXT NOT NULL,
  marca                 TEXT,
  ano                   INT,
  tipo                  TEXT NOT NULL CHECK (tipo IN ('cavalo','truck','toco','carreta','utilitario','outro')),
  km_atual              NUMERIC(12,1) NOT NULL DEFAULT 0 CHECK (km_atual >= 0),
  motorista_atual_id    UUID REFERENCES public.trans_motoristas(id) ON DELETE SET NULL,
  status                TEXT NOT NULL DEFAULT 'ativo' CHECK (status IN ('ativo','parado','manutencao','vendido','inativo')),
  capacidade_tanque_l   NUMERIC(8,1) DEFAULT 0 CHECK (capacidade_tanque_l >= 0),
  renavam               TEXT,
  chassi                TEXT,
  observacao            TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by            UUID,
  updated_by            UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_veiculos_status_placa ON public.trans_veiculos (status, placa);
CREATE INDEX IF NOT EXISTS idx_trans_veiculos_motorista   ON public.trans_veiculos (motorista_atual_id) WHERE motorista_atual_id IS NOT NULL;

-- Trigger: não permite retrocesso de km_atual (a não ser que admin setando via RPC)
CREATE OR REPLACE FUNCTION public.trans_veiculos_tg_km_monotonico()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.km_atual < OLD.km_atual THEN
    -- Admin pode corrigir (via UPDATE direto passando pelo RLS admin).
    -- Outros perfis nunca diminuem km.
    IF NOT public.trans_is_admin() THEN
      RAISE EXCEPTION 'km_atual nao pode retroceder (atual % -> novo %). Use RPC de correcao.', OLD.km_atual, NEW.km_atual
        USING ERRCODE = 'check_violation';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_trans_veiculos_km_monotonico ON public.trans_veiculos;
CREATE TRIGGER tg_trans_veiculos_km_monotonico
BEFORE UPDATE ON public.trans_veiculos
FOR EACH ROW EXECUTE FUNCTION public.trans_veiculos_tg_km_monotonico();

DROP TRIGGER IF EXISTS tg_trans_veiculos_updated_at ON public.trans_veiculos;
CREATE TRIGGER tg_trans_veiculos_updated_at
BEFORE INSERT OR UPDATE ON public.trans_veiculos
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- trans_fornecedores
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_fornecedores (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  razao_social    TEXT NOT NULL,
  nome_fantasia   TEXT,
  cnpj            TEXT UNIQUE,
  tipo            TEXT NOT NULL CHECK (tipo IN ('pneu','posto','peca','recapadora','oficina','outro')),
  contato         TEXT,
  telefone        TEXT,
  email           TEXT,
  endereco        TEXT,
  cidade          TEXT,
  uf              TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  observacao      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_fornecedores_tipo_ativo ON public.trans_fornecedores (tipo) WHERE ativo = TRUE;
CREATE INDEX IF NOT EXISTS idx_trans_fornecedores_cnpj      ON public.trans_fornecedores (cnpj) WHERE cnpj IS NOT NULL;

DROP TRIGGER IF EXISTS tg_trans_fornecedores_updated_at ON public.trans_fornecedores;
CREATE TRIGGER tg_trans_fornecedores_updated_at
BEFORE INSERT OR UPDATE ON public.trans_fornecedores
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- trans_categorias_produto
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_categorias_produto (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome            TEXT NOT NULL UNIQUE,
  tipo            TEXT NOT NULL CHECK (tipo IN ('pneu','peca','acessorio','combustivel','lubrificante','filtro','outro')),
  descricao       TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

DROP TRIGGER IF EXISTS tg_trans_categorias_produto_updated_at ON public.trans_categorias_produto;
CREATE TRIGGER tg_trans_categorias_produto_updated_at
BEFORE INSERT OR UPDATE ON public.trans_categorias_produto
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- trans_produtos (SKU mestre)
-- Nenhuma coluna codigo_barras aqui — EAN vive em trans_produtos_ean.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_produtos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku             TEXT NOT NULL UNIQUE,
  nome            TEXT NOT NULL,
  categoria_id    UUID NOT NULL REFERENCES public.trans_categorias_produto(id) ON DELETE RESTRICT,
  unidade         TEXT NOT NULL DEFAULT 'UN' CHECK (unidade IN ('UN','PC','CX','KG','L','M','PAR')),
  estoque_min     NUMERIC(12,3) DEFAULT 0 CHECK (estoque_min >= 0),
  localizacao     TEXT,
  custo_medio     NUMERIC(14,4) DEFAULT 0 CHECK (custo_medio >= 0),
  descricao       TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_produtos_categoria ON public.trans_produtos (categoria_id);
CREATE INDEX IF NOT EXISTS idx_trans_produtos_nome      ON public.trans_produtos (nome) WHERE ativo = TRUE;

DROP TRIGGER IF EXISTS tg_trans_produtos_updated_at ON public.trans_produtos;
CREATE TRIGGER tg_trans_produtos_updated_at
BEFORE INSERT OR UPDATE ON public.trans_produtos
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- trans_produtos_ean
-- N EANs por produto, mas cada EAN pertence a UM único produto.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.trans_produtos_ean (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id      UUID NOT NULL REFERENCES public.trans_produtos(id) ON DELETE CASCADE,
  ean             TEXT NOT NULL UNIQUE,
  descricao       TEXT,
  principal       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_trans_produtos_ean_produto ON public.trans_produtos_ean (produto_id);
-- Garante que só exista 1 EAN "principal" por produto
CREATE UNIQUE INDEX IF NOT EXISTS uq_trans_produtos_ean_principal
  ON public.trans_produtos_ean (produto_id)
  WHERE principal = TRUE;

DROP TRIGGER IF EXISTS tg_trans_produtos_ean_updated_at ON public.trans_produtos_ean;
CREATE TRIGGER tg_trans_produtos_ean_updated_at
BEFORE INSERT OR UPDATE ON public.trans_produtos_ean
FOR EACH ROW EXECUTE FUNCTION public.trans_tg_set_updated_at();

-- ============================================================
-- Comentários
-- ============================================================
COMMENT ON TABLE public.trans_motoristas          IS 'Motoristas da frota. Master no TMS; aqui pode ser sync (upsert por CPF) ou standalone.';
COMMENT ON TABLE public.trans_veiculos            IS 'Veículos da frota. km_atual é monotônico (trigger rejeita retrocesso salvo admin).';
COMMENT ON TABLE public.trans_fornecedores        IS 'Fornecedores de pneus, postos, peças, recapadoras, oficinas.';
COMMENT ON TABLE public.trans_categorias_produto  IS 'Categorias dos produtos (seed: pneu, peça, acessório, combustível, lubrificante, filtro).';
COMMENT ON TABLE public.trans_produtos            IS 'SKU mestre. Sem EAN aqui — EAN está em trans_produtos_ean (N:1).';
COMMENT ON TABLE public.trans_produtos_ean        IS 'EAN de leitura de código de barras. 1 EAN pertence a exatamente 1 SKU (UNIQUE global).';
