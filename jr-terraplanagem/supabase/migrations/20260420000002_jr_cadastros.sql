-- ============================================================
-- Migration: Cadastros base JR
-- Fase J1 - clientes, fornecedores, maquinas, veiculos, config
-- ============================================================

-- ============================================================
-- TABELA: jr_config
-- Parametros globais (chave/valor) para flags e defaults.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_config (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chave          TEXT NOT NULL UNIQUE,
  valor          TEXT,
  descricao      TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

DROP TRIGGER IF EXISTS tg_jr_config_set_updated_at ON public.jr_config;
CREATE TRIGGER tg_jr_config_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_config
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

INSERT INTO public.jr_config (chave, valor, descricao) VALUES
  ('valor_km_deslocamento',   '3.50',  'R$ por km de deslocamento de prancha/caminhao'),
  ('margem_padrao_percent',   '30',    'Margem percentual padrao sobre custo para orcamentos'),
  ('alerta_manut_horas_antes','20',    'Horas de antecedencia para alertar manutencao vencendo'),
  ('alerta_manut_dias_antes', '15',    'Dias de antecedencia para alertar manutencao vencendo'),
  ('permite_sinal',           'true',  'Permite pagamento adiantado antes de faturar OS'),
  ('importacao_executada',    'false', 'Flag: se true, bloqueia re-execucao do importador legacy'),
  ('tms_modo',                'standalone', 'Modo de integracao TMS (unused aqui; para compat com Trans)')
ON CONFLICT (chave) DO NOTHING;

-- ============================================================
-- TABELA: jr_clientes
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_clientes (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  razao_social   TEXT NOT NULL,
  nome_fantasia  TEXT,
  cpf_cnpj       TEXT UNIQUE,
  inscricao_estadual TEXT,
  contato        TEXT,
  email          TEXT,
  telefone       TEXT,
  endereco       TEXT,
  cidade         TEXT,
  uf             CHAR(2),
  cep            TEXT,
  observacao     TEXT,
  ativo          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_clientes_cpf_cnpj ON public.jr_clientes (cpf_cnpj) WHERE cpf_cnpj IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jr_clientes_razao    ON public.jr_clientes USING gin (to_tsvector('portuguese', razao_social));

DROP TRIGGER IF EXISTS tg_jr_clientes_set_updated_at ON public.jr_clientes;
CREATE TRIGGER tg_jr_clientes_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_clientes
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_fornecedores
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_fornecedores (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  razao_social   TEXT NOT NULL,
  nome_fantasia  TEXT,
  cnpj           TEXT UNIQUE,
  tipo           TEXT NOT NULL CHECK (tipo IN ('peca', 'servico', 'combustivel', 'oficina', 'outros')),
  contato        TEXT,
  email          TEXT,
  telefone       TEXT,
  endereco       TEXT,
  cidade         TEXT,
  uf             CHAR(2),
  observacao     TEXT,
  ativo          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_fornecedores_tipo_ativo ON public.jr_fornecedores (tipo) WHERE ativo = TRUE;
CREATE INDEX IF NOT EXISTS idx_jr_fornecedores_cnpj      ON public.jr_fornecedores (cnpj) WHERE cnpj IS NOT NULL;

DROP TRIGGER IF EXISTS tg_jr_fornecedores_set_updated_at ON public.jr_fornecedores;
CREATE TRIGGER tg_jr_fornecedores_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_fornecedores
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_maquinas (equipamentos de terraplanagem)
-- Horimetro e o "KM" dessas maquinas.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_maquinas (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo             TEXT NOT NULL UNIQUE,
  descricao          TEXT NOT NULL,
  tipo               TEXT NOT NULL CHECK (tipo IN ('escavadeira','retro','pa_carregadeira','trator','bulldozer','motoniveladora','rolo_compactador','caminhao_basculante','outra')),
  marca              TEXT,
  modelo             TEXT,
  ano                INT,
  valor_hora         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (valor_hora >= 0),
  horimetro_atual    NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (horimetro_atual >= 0),
  status             TEXT NOT NULL DEFAULT 'ativa' CHECK (status IN ('ativa','parada','manutencao','vendida')),
  foto_url           TEXT,
  observacao         TEXT,
  capacidade_tanque_l NUMERIC(10,2),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by         UUID,
  updated_by         UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_maquinas_status ON public.jr_maquinas (status);
CREATE INDEX IF NOT EXISTS idx_jr_maquinas_tipo   ON public.jr_maquinas (tipo);

DROP TRIGGER IF EXISTS tg_jr_maquinas_set_updated_at ON public.jr_maquinas;
CREATE TRIGGER tg_jr_maquinas_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_maquinas
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_veiculos (prancha/caminhao/utilitario p/ deslocamento)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_veiculos (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  placa          TEXT NOT NULL UNIQUE,
  descricao      TEXT NOT NULL,
  tipo           TEXT NOT NULL CHECK (tipo IN ('prancha','caminhao','utilitario','van','outro')),
  marca          TEXT,
  modelo         TEXT,
  ano            INT,
  km_atual       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (km_atual >= 0),
  status         TEXT NOT NULL DEFAULT 'ativo' CHECK (status IN ('ativo','parado','manutencao','vendido')),
  observacao     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_veiculos_placa  ON public.jr_veiculos (placa);
CREATE INDEX IF NOT EXISTS idx_jr_veiculos_status ON public.jr_veiculos (status);

DROP TRIGGER IF EXISTS tg_jr_veiculos_set_updated_at ON public.jr_veiculos;
CREATE TRIGGER tg_jr_veiculos_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_veiculos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- RLS dos cadastros JR
-- Leitura: qualquer authenticated que esteja em jr_usuarios (perfil != NULL)
-- Escrita: admin + gestor (financeiro/operador so leem ou usam RPCs)
-- ============================================================
ALTER TABLE public.jr_config       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_clientes     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_fornecedores ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_maquinas     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_veiculos     ENABLE ROW LEVEL SECURITY;

-- jr_config: leitura authenticated, escrita admin
DROP POLICY IF EXISTS jr_config_sel ON public.jr_config;
CREATE POLICY jr_config_sel ON public.jr_config FOR SELECT TO authenticated
  USING (public.jr_current_perfil() IS NOT NULL);
DROP POLICY IF EXISTS jr_config_admin ON public.jr_config;
CREATE POLICY jr_config_admin ON public.jr_config FOR ALL TO authenticated
  USING (public.jr_is_admin()) WITH CHECK (public.jr_is_admin());

-- Helper macro para SELECT de qualquer autenticado cadastrado e ESCRITA para admin/gestor
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['jr_clientes','jr_fornecedores','jr_maquinas','jr_veiculos']
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor'))
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

COMMENT ON TABLE public.jr_config       IS 'Parametros globais do modulo JR. Chave/valor.';
COMMENT ON TABLE public.jr_clientes     IS 'Clientes JR Terraplanagem.';
COMMENT ON TABLE public.jr_fornecedores IS 'Fornecedores de pecas, servicos, combustivel, oficina.';
COMMENT ON TABLE public.jr_maquinas     IS 'Maquinas de terraplanagem. Horimetro substitui KM.';
COMMENT ON TABLE public.jr_veiculos     IS 'Veiculos de transporte (prancha, caminhao) para deslocamentos.';
