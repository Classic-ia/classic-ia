-- ============================================================
-- Migration: Stubs minimos para receber dados legados (J1)
-- Fase J1 - tabelas basicas de OS/periodos/pagamentos/despesas/deslocamentos
-- Fases J2-J4 estendem com GENERATED cols, triggers e RPCs.
-- O objetivo aqui e SO viabilizar a importacao do JSON legacy.
-- ============================================================

-- ============================================================
-- TABELA: jr_ordens_servico (nucleo; expandida em J2)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_ordens_servico (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  numero_os          TEXT NOT NULL UNIQUE,
  cliente_id         UUID REFERENCES public.jr_clientes(id) ON DELETE SET NULL,
  data_abertura      DATE NOT NULL DEFAULT CURRENT_DATE,
  data_inicio        DATE,
  data_fim           DATE,
  obra_local         TEXT,
  descricao_servico  TEXT,
  status             TEXT NOT NULL DEFAULT 'aberta' CHECK (status IN ('aberta','em_execucao','concluida','cancelada')),
  valor_total        NUMERIC(14,2) DEFAULT 0,
  observacao         TEXT,
  origem_legacy      TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by         UUID,
  updated_by         UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_os_cliente ON public.jr_ordens_servico (cliente_id);
CREATE INDEX IF NOT EXISTS idx_jr_os_status  ON public.jr_ordens_servico (status);
CREATE INDEX IF NOT EXISTS idx_jr_os_data    ON public.jr_ordens_servico (data_abertura DESC);

DROP TRIGGER IF EXISTS tg_jr_os_set_updated_at ON public.jr_ordens_servico;
CREATE TRIGGER tg_jr_os_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_ordens_servico
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_os_periodos (horas-maquina por dia)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_os_periodos (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  os_id                 UUID NOT NULL REFERENCES public.jr_ordens_servico(id) ON DELETE CASCADE,
  maquina_id            UUID REFERENCES public.jr_maquinas(id) ON DELETE SET NULL,
  data                  DATE NOT NULL,
  horimetro_inicial     NUMERIC(12,2) NOT NULL DEFAULT 0,
  horimetro_final       NUMERIC(12,2) NOT NULL DEFAULT 0,
  horas                 NUMERIC(10,2) GENERATED ALWAYS AS (horimetro_final - horimetro_inicial) STORED,
  valor_hora_snapshot   NUMERIC(12,2) NOT NULL DEFAULT 0,
  valor_calc            NUMERIC(14,2) GENERATED ALWAYS AS ((horimetro_final - horimetro_inicial) * valor_hora_snapshot) STORED,
  operador_nome         TEXT,
  observacao            TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by            UUID,
  updated_by            UUID,
  CHECK (horimetro_final >= horimetro_inicial)
);

CREATE INDEX IF NOT EXISTS idx_jr_osper_os      ON public.jr_os_periodos (os_id);
CREATE INDEX IF NOT EXISTS idx_jr_osper_maquina ON public.jr_os_periodos (maquina_id, data DESC);

DROP TRIGGER IF EXISTS tg_jr_osper_set_updated_at ON public.jr_os_periodos;
CREATE TRIGGER tg_jr_osper_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_os_periodos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_pagamentos
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_pagamentos (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  os_id          UUID NOT NULL REFERENCES public.jr_ordens_servico(id) ON DELETE CASCADE,
  data           DATE NOT NULL DEFAULT CURRENT_DATE,
  valor          NUMERIC(14,2) NOT NULL CHECK (valor > 0),
  forma          TEXT CHECK (forma IN ('pix','cheque','transferencia','dinheiro','boleto','parcela_cartao','outro')),
  parcela_numero INT,
  parcela_total  INT,
  documento      TEXT,
  observacao     TEXT,
  conciliado     BOOLEAN NOT NULL DEFAULT FALSE,
  origem_legacy  TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by     UUID,
  updated_by     UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_pag_os   ON public.jr_pagamentos (os_id);
CREATE INDEX IF NOT EXISTS idx_jr_pag_data ON public.jr_pagamentos (data DESC);

DROP TRIGGER IF EXISTS tg_jr_pag_set_updated_at ON public.jr_pagamentos;
CREATE TRIGGER tg_jr_pag_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_pagamentos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_despesas
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_despesas (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  data            DATE NOT NULL DEFAULT CURRENT_DATE,
  tipo            TEXT NOT NULL CHECK (tipo IN ('peca','manutencao','servico','combustivel','mao_de_obra','aluguel','outros')),
  maquina_id      UUID REFERENCES public.jr_maquinas(id) ON DELETE SET NULL,
  os_id           UUID REFERENCES public.jr_ordens_servico(id) ON DELETE SET NULL,
  fornecedor_id   UUID REFERENCES public.jr_fornecedores(id) ON DELETE SET NULL,
  valor           NUMERIC(14,2) NOT NULL CHECK (valor >= 0),
  descricao       TEXT,
  nota_fiscal_numero TEXT,
  nf_chave        TEXT,
  anexo_url       TEXT,
  origem_legacy   TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_desp_maquina ON public.jr_despesas (maquina_id);
CREATE INDEX IF NOT EXISTS idx_jr_desp_os      ON public.jr_despesas (os_id);
CREATE INDEX IF NOT EXISTS idx_jr_desp_data    ON public.jr_despesas (data DESC);

DROP TRIGGER IF EXISTS tg_jr_desp_set_updated_at ON public.jr_despesas;
CREATE TRIGGER tg_jr_desp_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_despesas
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_deslocamentos
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_deslocamentos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  data            DATE NOT NULL DEFAULT CURRENT_DATE,
  veiculo_id      UUID REFERENCES public.jr_veiculos(id) ON DELETE SET NULL,
  os_id           UUID REFERENCES public.jr_ordens_servico(id) ON DELETE SET NULL,
  maquina_id      UUID REFERENCES public.jr_maquinas(id) ON DELETE SET NULL,
  km_inicial      NUMERIC(12,2),
  km_final        NUMERIC(12,2),
  km_rodado       NUMERIC(12,2) GENERATED ALWAYS AS (COALESCE(km_final,0) - COALESCE(km_inicial,0)) STORED,
  motivo          TEXT,
  valor_calc      NUMERIC(14,2),
  motorista_nome  TEXT,
  observacao      TEXT,
  origem_legacy   TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID,
  updated_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_desl_os      ON public.jr_deslocamentos (os_id);
CREATE INDEX IF NOT EXISTS idx_jr_desl_data    ON public.jr_deslocamentos (data DESC);

DROP TRIGGER IF EXISTS tg_jr_desl_set_updated_at ON public.jr_deslocamentos;
CREATE TRIGGER tg_jr_desl_set_updated_at
BEFORE INSERT OR UPDATE ON public.jr_deslocamentos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- RLS para as tabelas operacionais
-- SELECT: authenticated cadastrado em jr_usuarios
-- INSERT/UPDATE: admin/gestor/operador/financeiro conforme dominio
-- DELETE: apenas admin
-- ============================================================
ALTER TABLE public.jr_ordens_servico ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_os_periodos    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_pagamentos     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_despesas       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_deslocamentos  ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['jr_ordens_servico','jr_os_periodos','jr_pagamentos','jr_despesas','jr_deslocamentos']
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','operador','financeiro'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor','financeiro'))
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','financeiro'))$F$, t, t);

    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

COMMENT ON TABLE public.jr_ordens_servico IS 'Ordens de servico. Fase J2 expande com triggers e RPCs.';
COMMENT ON TABLE public.jr_os_periodos    IS 'Periodos (horas-maquina) de uma OS.';
COMMENT ON TABLE public.jr_pagamentos     IS 'Pagamentos recebidos de clientes por OS.';
COMMENT ON TABLE public.jr_despesas       IS 'Despesas operacionais (peca, manutencao, combustivel, outros).';
COMMENT ON TABLE public.jr_deslocamentos  IS 'Deslocamentos de prancha/caminhao com km rodado.';
