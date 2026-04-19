-- ============================================================
-- Migration: Fase J6.2 - Tanque proprio + Abastecimentos (schema)
-- ============================================================

-- ============================================================
-- TABELA: jr_tanque (tanques de combustivel proprios)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_tanque (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome                 TEXT NOT NULL UNIQUE,
  combustivel_tipo     TEXT NOT NULL CHECK (combustivel_tipo IN ('diesel','s10','arla','gasolina','etanol')),
  capacidade_litros    NUMERIC(12,2) NOT NULL CHECK (capacidade_litros > 0),
  saldo_atual_litros   NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (saldo_atual_litros >= 0),
  localizacao          TEXT,
  ativo                BOOLEAN NOT NULL DEFAULT TRUE,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by           UUID,
  updated_by           UUID
);

DROP TRIGGER IF EXISTS tg_jr_tanque_updated_at ON public.jr_tanque;
CREATE TRIGGER tg_jr_tanque_updated_at
BEFORE INSERT OR UPDATE ON public.jr_tanque
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- Seed tanque padrao
INSERT INTO public.jr_tanque (nome, combustivel_tipo, capacidade_litros, saldo_atual_litros, localizacao)
VALUES ('Tanque Diesel Sede', 'diesel', 5000, 0, 'Sede JR Terraplanagem')
ON CONFLICT (nome) DO NOTHING;

-- ============================================================
-- TABELA: jr_tanque_movimentacao
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_tanque_movimentacao (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tanque_id       UUID NOT NULL REFERENCES public.jr_tanque(id),
  tipo            TEXT NOT NULL CHECK (tipo IN ('entrada','saida','ajuste')),
  litros          NUMERIC(12,3) NOT NULL CHECK (litros > 0),
  valor_unitario  NUMERIC(12,4),
  valor_total     NUMERIC(14,2) GENERATED ALWAYS AS (litros * COALESCE(valor_unitario, 0)) STORED,
  data            DATE NOT NULL DEFAULT CURRENT_DATE,
  fornecedor_id   UUID REFERENCES public.jr_fornecedores(id),
  nf_numero       TEXT,
  abastecimento_id UUID,
  observacao      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID
);

CREATE INDEX IF NOT EXISTS idx_jr_tm_tanque ON public.jr_tanque_movimentacao (tanque_id);
CREATE INDEX IF NOT EXISTS idx_jr_tm_data   ON public.jr_tanque_movimentacao (data DESC);

-- ============================================================
-- TRIGGER: atualiza saldo do tanque a cada movimentacao
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_tanque_atualizar_saldo()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  v_delta NUMERIC;
  v_capacidade NUMERIC;
  v_saldo_atual NUMERIC;
BEGIN
  v_delta := CASE
    WHEN NEW.tipo = 'entrada' THEN NEW.litros
    WHEN NEW.tipo = 'saida'   THEN -NEW.litros
    ELSE NEW.litros
  END;

  SELECT capacidade_litros, saldo_atual_litros INTO v_capacidade, v_saldo_atual
    FROM public.jr_tanque WHERE id = NEW.tanque_id FOR UPDATE;

  IF v_saldo_atual + v_delta < 0 THEN
    RAISE EXCEPTION 'Saldo insuficiente no tanque (atual %, retirada %)', v_saldo_atual, NEW.litros;
  END IF;

  IF NEW.tipo = 'entrada' AND v_saldo_atual + v_delta > v_capacidade THEN
    RAISE EXCEPTION 'Capacidade do tanque excedida (cap %, saldo apos entrada %)', v_capacidade, v_saldo_atual + v_delta;
  END IF;

  UPDATE public.jr_tanque SET saldo_atual_litros = saldo_atual_litros + v_delta WHERE id = NEW.tanque_id;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_jr_tanque_mov_saldo ON public.jr_tanque_movimentacao;
CREATE TRIGGER tg_jr_tanque_mov_saldo
AFTER INSERT ON public.jr_tanque_movimentacao
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_tanque_atualizar_saldo();

-- ============================================================
-- TABELA: jr_abastecimentos
-- ============================================================
CREATE TABLE IF NOT EXISTS public.jr_abastecimentos (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  maquina_id          UUID NOT NULL REFERENCES public.jr_maquinas(id),
  os_id               UUID REFERENCES public.jr_ordens_servico(id),
  origem              TEXT NOT NULL CHECK (origem IN ('tanque_proprio','posto')),
  tanque_id           UUID REFERENCES public.jr_tanque(id),
  posto_nome          TEXT,
  fornecedor_id       UUID REFERENCES public.jr_fornecedores(id),
  data                DATE NOT NULL DEFAULT CURRENT_DATE,
  litros              NUMERIC(12,3) NOT NULL CHECK (litros > 0),
  valor_unitario      NUMERIC(12,4),
  valor_total         NUMERIC(14,2) NOT NULL CHECK (valor_total >= 0),
  horimetro_leitura   NUMERIC(12,2) NOT NULL CHECK (horimetro_leitura >= 0),
  horas_desde_ultimo  NUMERIC(12,2),
  consumo_l_h         NUMERIC(10,3),
  operador_nome       TEXT,
  nf_numero           TEXT,
  observacao          TEXT,
  despesa_id          UUID REFERENCES public.jr_despesas(id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID,
  updated_by          UUID,
  CONSTRAINT chk_jr_abast_origem CHECK (
    (origem = 'tanque_proprio' AND tanque_id IS NOT NULL)
    OR
    (origem = 'posto' AND (posto_nome IS NOT NULL OR fornecedor_id IS NOT NULL))
  )
);

CREATE INDEX IF NOT EXISTS idx_jr_ab_maquina ON public.jr_abastecimentos (maquina_id);
CREATE INDEX IF NOT EXISTS idx_jr_ab_data    ON public.jr_abastecimentos (data DESC);
CREATE INDEX IF NOT EXISTS idx_jr_ab_os      ON public.jr_abastecimentos (os_id) WHERE os_id IS NOT NULL;

DROP TRIGGER IF EXISTS tg_jr_ab_updated_at ON public.jr_abastecimentos;
CREATE TRIGGER tg_jr_ab_updated_at
BEFORE INSERT OR UPDATE ON public.jr_abastecimentos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_set_updated_at();

-- ============================================================
-- VIEW: v_jr_abastecimentos_consumo
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_abastecimentos_consumo AS
SELECT
  a.id,
  a.maquina_id,
  m.codigo            AS maquina_codigo,
  m.descricao         AS maquina_descricao,
  a.data,
  a.origem,
  a.litros,
  a.valor_total,
  a.horimetro_leitura,
  a.horas_desde_ultimo,
  a.consumo_l_h,
  a.os_id,
  a.operador_nome,
  CASE WHEN a.consumo_l_h IS NULL THEN 'sem_baseline'
       WHEN a.consumo_l_h > 30 THEN 'alto'
       WHEN a.consumo_l_h < 4  THEN 'baixo'
       ELSE 'normal' END AS classificacao
FROM public.jr_abastecimentos a
JOIN public.jr_maquinas m ON m.id = a.maquina_id;

GRANT SELECT ON public.v_jr_abastecimentos_consumo TO authenticated;

-- ============================================================
-- RLS
-- ============================================================
ALTER TABLE public.jr_tanque               ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_tanque_movimentacao  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jr_abastecimentos       ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['jr_tanque','jr_tanque_movimentacao','jr_abastecimentos'] LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_sel ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_sel ON public.%I FOR SELECT TO authenticated
      USING (public.jr_current_perfil() IS NOT NULL)$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_ins ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_ins ON public.%I FOR INSERT TO authenticated
      WITH CHECK (public.jr_current_perfil() IN ('admin','gestor','operador','financeiro'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_upd ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_upd ON public.%I FOR UPDATE TO authenticated
      USING (public.jr_current_perfil() IN ('admin','gestor'))$F$, t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_del ON public.%I', t, t);
    EXECUTE format($F$CREATE POLICY %I_del ON public.%I FOR DELETE TO authenticated
      USING (public.jr_is_admin())$F$, t, t);
  END LOOP;
END $$;

COMMENT ON TABLE public.jr_tanque         IS 'Tanques proprios de combustivel.';
COMMENT ON TABLE public.jr_abastecimentos IS 'Abastecimentos de maquinas (tanque proprio ou posto externo).';
