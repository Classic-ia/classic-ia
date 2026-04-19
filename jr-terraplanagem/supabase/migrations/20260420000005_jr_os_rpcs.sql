-- ============================================================
-- Migration: Fase J2 - OS nucleo (triggers + RPCs + views)
-- Complementa os stubs criados em 20260420000003.
-- ============================================================

-- ============================================================
-- Sequencia para numero_os automatico
-- ============================================================
CREATE SEQUENCE IF NOT EXISTS public.jr_os_seq START 1;

CREATE OR REPLACE FUNCTION public.jr_proximo_numero_os()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  v_prox BIGINT;
BEGIN
  v_prox := nextval('public.jr_os_seq');
  RETURN 'OS-' || LPAD(v_prox::text, 6, '0');
END;
$$;

-- ============================================================
-- Constraint UNIQUE (maquina_id, data, horimetro_inicial) para evitar
-- lancamento duplicado do mesmo periodo
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'jr_os_periodos_maq_data_hi_uq'
  ) THEN
    ALTER TABLE public.jr_os_periodos
      ADD CONSTRAINT jr_os_periodos_maq_data_hi_uq
      UNIQUE (maquina_id, data, horimetro_inicial);
  END IF;
END $$;

-- ============================================================
-- TRIGGER: atualiza jr_maquinas.horimetro_atual (monotonico)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_atualizar_horimetro_maquina()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.maquina_id IS NOT NULL AND NEW.horimetro_final IS NOT NULL THEN
    UPDATE public.jr_maquinas
       SET horimetro_atual = GREATEST(COALESCE(horimetro_atual,0), NEW.horimetro_final),
           updated_at = NOW()
     WHERE id = NEW.maquina_id;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_jr_osper_horimetro ON public.jr_os_periodos;
CREATE TRIGGER tg_jr_osper_horimetro
AFTER INSERT OR UPDATE OF horimetro_final, maquina_id ON public.jr_os_periodos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_atualizar_horimetro_maquina();

-- ============================================================
-- TRIGGER: recalcula valor_total da OS quando periodos mudam
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_recalcular_valor_os()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_os_id UUID;
  v_total NUMERIC(14,2);
BEGIN
  v_os_id := COALESCE(NEW.os_id, OLD.os_id);
  IF v_os_id IS NULL THEN
    RETURN COALESCE(NEW, OLD);
  END IF;

  SELECT COALESCE(SUM(valor_calc),0)
    INTO v_total
    FROM public.jr_os_periodos
   WHERE os_id = v_os_id;

  UPDATE public.jr_ordens_servico
     SET valor_total = v_total,
         updated_at = NOW()
   WHERE id = v_os_id;

  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS tg_jr_osper_recalc_os ON public.jr_os_periodos;
CREATE TRIGGER tg_jr_osper_recalc_os
AFTER INSERT OR UPDATE OR DELETE ON public.jr_os_periodos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_recalcular_valor_os();

-- ============================================================
-- TRIGGER: valida horimetro nao retrocede por maquina
-- NEW.horimetro_inicial >= horimetro_final do periodo anterior da mesma maquina
-- (exceto correcao admin via p_correcao=true flag no RPC)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_tg_validar_horimetro_monotonico()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_ultimo_hf NUMERIC(12,2);
BEGIN
  IF NEW.maquina_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT MAX(horimetro_final)
    INTO v_ultimo_hf
    FROM public.jr_os_periodos
   WHERE maquina_id = NEW.maquina_id
     AND id <> NEW.id
     AND (data < NEW.data OR (data = NEW.data AND horimetro_final <= NEW.horimetro_inicial));

  IF v_ultimo_hf IS NOT NULL AND NEW.horimetro_inicial < v_ultimo_hf THEN
    RAISE EXCEPTION 'Horimetro inicial (%) nao pode ser inferior ao ultimo registro da maquina (%)', NEW.horimetro_inicial, v_ultimo_hf;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_jr_osper_monotonico ON public.jr_os_periodos;
CREATE TRIGGER tg_jr_osper_monotonico
BEFORE INSERT OR UPDATE ON public.jr_os_periodos
FOR EACH ROW EXECUTE FUNCTION public.jr_tg_validar_horimetro_monotonico();

-- ============================================================
-- RPC: jr_os_abrir
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_abrir(
  p_cliente_id        UUID,
  p_obra_local        TEXT DEFAULT NULL,
  p_descricao_servico TEXT DEFAULT NULL,
  p_data_abertura     DATE DEFAULT CURRENT_DATE,
  p_observacao        TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_id UUID;
  v_numero TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador') THEN
    RAISE EXCEPTION 'Sem permissao para abrir OS';
  END IF;

  IF p_cliente_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_clientes WHERE id = p_cliente_id) THEN
    RAISE EXCEPTION 'Cliente nao encontrado';
  END IF;

  v_numero := public.jr_proximo_numero_os();

  INSERT INTO public.jr_ordens_servico (
    numero_os, cliente_id, obra_local, descricao_servico, data_abertura, observacao, status
  ) VALUES (
    v_numero, p_cliente_id, p_obra_local, p_descricao_servico, p_data_abertura, p_observacao, 'aberta'
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'id', v_id, 'numero_os', v_numero);
END;
$$;

-- ============================================================
-- RPC: jr_os_periodo_registrar
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_periodo_registrar(
  p_os_id              UUID,
  p_maquina_id         UUID,
  p_data               DATE,
  p_horimetro_inicial  NUMERIC,
  p_horimetro_final    NUMERIC,
  p_operador_nome      TEXT DEFAULT NULL,
  p_valor_hora_snapshot NUMERIC DEFAULT NULL,
  p_observacao         TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_status TEXT;
  v_vh NUMERIC(12,2);
  v_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador') THEN
    RAISE EXCEPTION 'Sem permissao para registrar periodo';
  END IF;

  IF p_horimetro_final <= p_horimetro_inicial THEN
    RAISE EXCEPTION 'Horimetro final deve ser maior que inicial';
  END IF;

  SELECT status INTO v_status FROM public.jr_ordens_servico WHERE id = p_os_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF v_status IN ('concluida','cancelada') THEN
    RAISE EXCEPTION 'OS esta % - nao aceita novos periodos', v_status;
  END IF;

  IF p_valor_hora_snapshot IS NULL OR p_valor_hora_snapshot <= 0 THEN
    SELECT valor_hora INTO v_vh FROM public.jr_maquinas WHERE id = p_maquina_id;
    IF v_vh IS NULL OR v_vh <= 0 THEN
      RAISE EXCEPTION 'Maquina sem valor_hora cadastrado - informe valor_hora_snapshot';
    END IF;
  ELSE
    v_vh := p_valor_hora_snapshot;
  END IF;

  INSERT INTO public.jr_os_periodos (
    os_id, maquina_id, data, horimetro_inicial, horimetro_final,
    valor_hora_snapshot, operador_nome, observacao
  ) VALUES (
    p_os_id, p_maquina_id, p_data, p_horimetro_inicial, p_horimetro_final,
    v_vh, p_operador_nome, p_observacao
  )
  RETURNING id INTO v_id;

  -- Move OS para em_execucao se ainda estava aberta
  UPDATE public.jr_ordens_servico
     SET status = 'em_execucao',
         data_inicio = COALESCE(data_inicio, p_data)
   WHERE id = p_os_id AND status = 'aberta';

  RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================
-- RPC: jr_os_periodo_remover
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_periodo_remover(p_periodo_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_os_id UUID;
  v_status TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para remover periodo';
  END IF;

  SELECT os_id INTO v_os_id FROM public.jr_os_periodos WHERE id = p_periodo_id;
  IF v_os_id IS NULL THEN
    RAISE EXCEPTION 'Periodo nao encontrado';
  END IF;

  SELECT status INTO v_status FROM public.jr_ordens_servico WHERE id = v_os_id;
  IF v_status IN ('concluida','cancelada') AND NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'OS % - apenas admin pode remover periodos', v_status;
  END IF;

  DELETE FROM public.jr_os_periodos WHERE id = p_periodo_id;
  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_os_finalizar
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_finalizar(
  p_os_id     UUID,
  p_data_fim  DATE DEFAULT CURRENT_DATE
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_status TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para finalizar OS';
  END IF;

  SELECT status INTO v_status FROM public.jr_ordens_servico WHERE id = p_os_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF v_status = 'concluida' THEN
    RAISE EXCEPTION 'OS ja esta concluida';
  END IF;
  IF v_status = 'cancelada' THEN
    RAISE EXCEPTION 'OS cancelada nao pode ser finalizada';
  END IF;

  UPDATE public.jr_ordens_servico
     SET status = 'concluida',
         data_fim = p_data_fim,
         updated_at = NOW()
   WHERE id = p_os_id;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_os_reabrir (admin)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_reabrir(
  p_os_id  UUID,
  p_motivo TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_obs_ant TEXT;
BEGIN
  IF NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'Apenas admin pode reabrir OS';
  END IF;

  SELECT observacao INTO v_obs_ant FROM public.jr_ordens_servico WHERE id = p_os_id;

  UPDATE public.jr_ordens_servico
     SET status = 'em_execucao',
         data_fim = NULL,
         observacao = COALESCE(v_obs_ant,'') ||
           E'\n[REABERTA em ' || to_char(NOW(), 'DD/MM/YYYY HH24:MI') || ']: ' || p_motivo,
         updated_at = NOW()
   WHERE id = p_os_id AND status = 'concluida';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'OS nao esta concluida ou nao encontrada';
  END IF;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_os_cancelar
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_os_cancelar(
  p_os_id  UUID,
  p_motivo TEXT
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_status TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para cancelar OS';
  END IF;

  IF COALESCE(TRIM(p_motivo),'') = '' THEN
    RAISE EXCEPTION 'Motivo do cancelamento obrigatorio';
  END IF;

  SELECT status INTO v_status FROM public.jr_ordens_servico WHERE id = p_os_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF v_status = 'cancelada' THEN
    RAISE EXCEPTION 'OS ja esta cancelada';
  END IF;

  UPDATE public.jr_ordens_servico
     SET status = 'cancelada',
         observacao = COALESCE(observacao,'') ||
           E'\n[CANCELADA em ' || to_char(NOW(), 'DD/MM/YYYY HH24:MI') || ']: ' || p_motivo,
         updated_at = NOW()
   WHERE id = p_os_id;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- VIEW: v_jr_os_resumo
-- Resumo com totais de periodos, pagamentos e saldo
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_os_resumo AS
SELECT
  os.id,
  os.numero_os,
  os.cliente_id,
  c.razao_social AS cliente_nome,
  os.obra_local,
  os.descricao_servico,
  os.status,
  os.data_abertura,
  os.data_inicio,
  os.data_fim,
  os.valor_total,
  COALESCE(p.total_pago, 0)                              AS total_pago,
  (os.valor_total - COALESCE(p.total_pago,0))            AS saldo_receber,
  COALESCE(per.periodos_count, 0)                        AS periodos_count,
  COALESCE(per.horas_total, 0)                           AS horas_total,
  os.created_at,
  os.updated_at
FROM public.jr_ordens_servico os
LEFT JOIN public.jr_clientes c ON c.id = os.cliente_id
LEFT JOIN LATERAL (
  SELECT SUM(valor) AS total_pago
    FROM public.jr_pagamentos
   WHERE os_id = os.id
) p ON TRUE
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS periodos_count, SUM(horas) AS horas_total
    FROM public.jr_os_periodos
   WHERE os_id = os.id
) per ON TRUE;

COMMENT ON VIEW public.v_jr_os_resumo IS 'Resumo OS com totais de periodos + pagamentos + saldo. Expandido em J8.';

-- ============================================================
-- RLS nao aplica a views; quem consulta ja passa pelas policies das tabelas base
-- Mas garantimos grant basico para authenticated
-- ============================================================
GRANT SELECT ON public.v_jr_os_resumo TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_abrir              TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_periodo_registrar  TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_periodo_remover    TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_finalizar          TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_reabrir            TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_os_cancelar           TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_proximo_numero_os     TO authenticated;
