-- ============================================================
-- Migration: Fase J6.1 - Manutencao das maquinas (schema)
-- ============================================================

-- ============================================================
-- TABELA: jr_planos_manutencao (templates por horimetro/dias)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_planos_manutencao (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome                     TEXT NOT NULL UNIQUE,
  tipo_servico             TEXT NOT NULL CHECK (tipo_servico IN (
    'troca_oleo','filtro_oleo','filtro_ar','filtro_combustivel',
    'lubrificacao','revisao','undercarriage','alinhamento','outro'
  )),
  intervalo_horas          NUMERIC(10,2),
  intervalo_dias           INT,
  ativo                    BOOLEAN NOT NULL DEFAULT TRUE,
  maquina_tipo_aplicavel   TEXT[] DEFAULT ARRAY[]::TEXT[],
  observacao               TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by               UUID,
  updated_by               UUID,
  CONSTRAINT chk_jr_planos_intervalo CHECK (intervalo_horas IS NOT NULL OR intervalo_dias IS NOT NULL)
);

DROP TRIGGER IF EXISTS tg_jr_planos_manut_updated_at ON public.jr_planos_manutencao;
CREATE TRIGGER tg_jr_planos_manut_updated_at
BEFORE INSERT OR UPDATE ON public.jr_planos_manutencao
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- Seed planos padrao
INSERT INTO public.jr_planos_manutencao (nome, tipo_servico, intervalo_horas, intervalo_dias, observacao) VALUES
  ('Troca de oleo motor (250h)',     'troca_oleo',         250,  180, 'Padrao escavadeiras/retro'),
  ('Filtro de oleo (250h)',          'filtro_oleo',        250,  180, 'Junto com troca de oleo'),
  ('Filtro de ar (500h)',            'filtro_ar',          500,  365, NULL),
  ('Filtro de combustivel (500h)',   'filtro_combustivel', 500,  365, NULL),
  ('Lubrificacao geral (50h)',       'lubrificacao',        50,   15, 'Pinos e articulacoes'),
  ('Revisao geral (1000h)',          'revisao',           1000,  365, 'Inspecao completa'),
  ('Undercarriage (2000h)',          'undercarriage',     2000, NULL, 'Esteiras e roletes')
ON CONFLICT (nome) DO NOTHING;

-- ============================================================
-- TABELA: jr_planos_maquina (associacao plano <-> maquina)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_planos_maquina (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  plano_id                 UUID NOT NULL REFERENCES public.jr_planos_manutencao(id) ON DELETE CASCADE,
  maquina_id               UUID NOT NULL REFERENCES public.jr_maquinas(id) ON DELETE CASCADE,
  ultima_execucao_horimetro NUMERIC(12,2),
  ultima_execucao_data     DATE,
  ativo                    BOOLEAN NOT NULL DEFAULT TRUE,
  observacao               TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by               UUID,
  updated_by               UUID,
  CONSTRAINT uq_jr_planos_maquina UNIQUE (plano_id, maquina_id)
);

CREATE INDEX IF NOT EXISTS idx_jr_pm_maquina ON public.jr_planos_maquina (maquina_id);

DROP TRIGGER IF EXISTS tg_jr_pm_updated_at ON public.jr_planos_maquina;
CREATE TRIGGER tg_jr_pm_updated_at
BEFORE INSERT OR UPDATE ON public.jr_planos_maquina
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_manutencoes (ordem de servico de manutencao)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_manutencoes (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  maquina_id          UUID NOT NULL REFERENCES public.jr_maquinas(id),
  plano_id            UUID REFERENCES public.jr_planos_manutencao(id),
  tipo                TEXT NOT NULL CHECK (tipo IN ('preventiva','corretiva')),
  servico             TEXT NOT NULL,
  data_abertura       DATE NOT NULL DEFAULT CURRENT_DATE,
  data_execucao       DATE,
  data_encerramento   DATE,
  horimetro_execucao  NUMERIC(12,2),
  status              TEXT NOT NULL DEFAULT 'aberta' CHECK (status IN ('aberta','em_andamento','concluida','cancelada')),
  observacao          TEXT,
  oficina             TEXT,
  executado_por       TEXT,
  os_id               UUID REFERENCES public.jr_ordens_servico(id),
  proximo_horimetro   NUMERIC(12,2),
  proxima_data        DATE,
  custo_total_calc    NUMERIC(14,2) NOT NULL DEFAULT 0,
  cancelada_motivo    TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID,
  updated_by          UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_manut_maquina ON public.jr_manutencoes (maquina_id);
CREATE INDEX IF NOT EXISTS idx_jr_manut_status  ON public.jr_manutencoes (status);
CREATE INDEX IF NOT EXISTS idx_jr_manut_data    ON public.jr_manutencoes (data_abertura DESC);

DROP TRIGGER IF EXISTS tg_jr_manut_updated_at ON public.jr_manutencoes;
CREATE TRIGGER tg_jr_manut_updated_at
BEFORE INSERT OR UPDATE ON public.jr_manutencoes
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- TABELA: jr_manutencao_itens (consumo de estoque por manutencao)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_manutencao_itens (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  manutencao_id            UUID NOT NULL REFERENCES public.jr_manutencoes(id) ON DELETE CASCADE,
  produto_id               UUID NOT NULL REFERENCES public.jr_produtos(id),
  modalidade               TEXT NOT NULL CHECK (modalidade IN ('novo','usado')),
  lote_id                  UUID REFERENCES public.jr_lotes(id),
  condicao                 TEXT CHECK (condicao IN ('A','B','C')),
  quantidade               NUMERIC(14,3) NOT NULL CHECK (quantidade > 0),
  custo_unitario           NUMERIC(14,4) NOT NULL DEFAULT 0,
  custo_total              NUMERIC(14,2) GENERATED ALWAYS AS (quantidade * custo_unitario) STORED,
  movimentacao_estoque_id  UUID REFERENCES public.jr_estoque_movimentacao(id),
  observacao               TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_jr_mi_mod_consist CHECK (
    (modalidade = 'novo'  AND lote_id IS NOT NULL AND condicao IS NULL)
    OR
    (modalidade = 'usado' AND condicao IS NOT NULL AND lote_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_jr_mi_manut   ON public.jr_manutencao_itens (manutencao_id);
CREATE INDEX IF NOT EXISTS idx_jr_mi_produto ON public.jr_manutencao_itens (produto_id);

-- ============================================================
-- TABELA: jr_manutencao_anexos
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_manutencao_anexos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  manutencao_id   UUID NOT NULL REFERENCES public.jr_manutencoes(id) ON DELETE CASCADE,
  url             TEXT NOT NULL,
  tipo            TEXT NOT NULL DEFAULT 'foto' CHECK (tipo IN ('foto','nota','laudo','comprovante','outro')),
  capturada_em    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  usuario_id      UUID,
  observacao      TEXT
);

CREATE INDEX IF NOT EXISTS idx_jr_ma_manut ON public.jr_manutencao_anexos (manutencao_id);

-- ============================================================
-- VIEW: v_jr_planos_maquina_status (proximo vencimento)
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_planos_maquina_status AS
SELECT
  pm.id                AS plano_maquina_id,
  pm.plano_id,
  pm.maquina_id,
  m.codigo             AS maquina_codigo,
  m.descricao          AS maquina_descricao,
  m.horimetro_atual,
  pl.nome              AS plano_nome,
  pl.tipo_servico,
  pl.intervalo_horas,
  pl.intervalo_dias,
  pm.ultima_execucao_horimetro,
  pm.ultima_execucao_data,
  CASE WHEN pl.intervalo_horas IS NOT NULL AND pm.ultima_execucao_horimetro IS NOT NULL
       THEN pm.ultima_execucao_horimetro + pl.intervalo_horas
       WHEN pl.intervalo_horas IS NOT NULL
       THEN m.horimetro_atual + pl.intervalo_horas
       ELSE NULL END                          AS proximo_horimetro,
  CASE WHEN pl.intervalo_dias IS NOT NULL AND pm.ultima_execucao_data IS NOT NULL
       THEN pm.ultima_execucao_data + pl.intervalo_dias
       WHEN pl.intervalo_dias IS NOT NULL
       THEN CURRENT_DATE + pl.intervalo_dias
       ELSE NULL END                          AS proxima_data,
  CASE WHEN pl.intervalo_horas IS NOT NULL AND pm.ultima_execucao_horimetro IS NOT NULL
       THEN (pm.ultima_execucao_horimetro + pl.intervalo_horas) - m.horimetro_atual
       ELSE NULL END                          AS horas_restantes,
  CASE WHEN pl.intervalo_dias IS NOT NULL AND pm.ultima_execucao_data IS NOT NULL
       THEN (pm.ultima_execucao_data + pl.intervalo_dias) - CURRENT_DATE
       ELSE NULL END                          AS dias_restantes,
  pm.ativo
FROM public.jr_planos_maquina pm
JOIN public.jr_maquinas m ON m.id = pm.maquina_id
JOIN public.jr_planos_manutencao pl ON pl.id = pm.plano_id
WHERE pm.ativo AND pl.ativo;

GRANT SELECT ON public.v_jr_planos_maquina_status TO authenticated;

-- ============================================================
-- RLS
-- ============================================================
ALTER TABLE public.jr_planos_manutencao  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_planos_maquina     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_manutencoes        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_manutencao_itens   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_manutencao_anexos  ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'jr_planos_manutencao','jr_planos_maquina','jr_manutencoes',
    'jr_manutencao_itens','jr_manutencao_anexos'
  ] LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','operador','financeiro'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor','operador'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

COMMENT ON TABLE public.jr_planos_manutencao IS 'Templates de manutencao por horimetro/dias.';
COMMENT ON TABLE public.jr_manutencoes       IS 'Ordens de manutencao (preventiva/corretiva) com horimetro.';
COMMENT ON VIEW  public.v_jr_planos_maquina_status IS 'Proximo vencimento de cada plano por maquina.';
