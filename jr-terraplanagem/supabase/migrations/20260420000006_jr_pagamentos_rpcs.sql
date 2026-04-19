-- ============================================================
-- Migration: Fase J3 - Financeiro OS (RPCs de pagamentos)
-- Valida valor vs saldo (respeita jr_config.permite_sinal).
-- ============================================================

-- ============================================================
-- RPC: jr_pagamento_registrar
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_pagamento_registrar(
  p_os_id           UUID,
  p_valor           NUMERIC,
  p_forma           TEXT,
  p_data            DATE DEFAULT CURRENT_DATE,
  p_parcela_numero  INT DEFAULT NULL,
  p_parcela_total   INT DEFAULT NULL,
  p_documento       TEXT DEFAULT NULL,
  p_observacao      TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_status TEXT;
  v_valor_total NUMERIC(14,2);
  v_total_pago NUMERIC(14,2);
  v_saldo NUMERIC(14,2);
  v_permite_sinal BOOLEAN;
  v_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para registrar pagamento';
  END IF;

  IF p_valor IS NULL OR p_valor <= 0 THEN
    RAISE EXCEPTION 'Valor do pagamento deve ser maior que zero';
  END IF;

  IF p_forma IS NULL OR p_forma NOT IN ('pix','cheque','transferencia','dinheiro','boleto','parcela_cartao','outro') THEN
    RAISE EXCEPTION 'Forma de pagamento invalida';
  END IF;

  -- Valida OS e status
  SELECT status, valor_total INTO v_status, v_valor_total
    FROM public.jr_ordens_servico WHERE id = p_os_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF v_status = 'cancelada' THEN
    RAISE EXCEPTION 'OS cancelada nao aceita pagamentos';
  END IF;

  -- Total ja pago
  SELECT COALESCE(SUM(valor),0) INTO v_total_pago
    FROM public.jr_pagamentos WHERE os_id = p_os_id;
  v_saldo := COALESCE(v_valor_total,0) - v_total_pago;

  -- Le config permite_sinal
  SELECT COALESCE(valor = 'true', false) INTO v_permite_sinal
    FROM public.jr_config WHERE chave = 'permite_sinal';

  IF p_valor > v_saldo AND NOT COALESCE(v_permite_sinal, false) THEN
    RAISE EXCEPTION 'Valor do pagamento (% ) excede o saldo a receber da OS (%). Ajuste jr_config.permite_sinal=true para permitir sinal.', p_valor, v_saldo;
  END IF;

  -- Valida coerencia de parcelas
  IF p_parcela_numero IS NOT NULL OR p_parcela_total IS NOT NULL THEN
    IF p_parcela_numero IS NULL OR p_parcela_total IS NULL THEN
      RAISE EXCEPTION 'Informe ambos parcela_numero e parcela_total, ou nenhum';
    END IF;
    IF p_parcela_numero <= 0 OR p_parcela_total <= 0 OR p_parcela_numero > p_parcela_total THEN
      RAISE EXCEPTION 'Parcelas invalidas (numero % de %)', p_parcela_numero, p_parcela_total;
    END IF;
  END IF;

  INSERT INTO public.jr_pagamentos (
    os_id, data, valor, forma, parcela_numero, parcela_total,
    documento, observacao, conciliado
  ) VALUES (
    p_os_id, COALESCE(p_data, CURRENT_DATE), p_valor, p_forma,
    p_parcela_numero, p_parcela_total, p_documento, p_observacao, FALSE
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object(
    'ok', true,
    'id', v_id,
    'novo_total_pago', v_total_pago + p_valor,
    'novo_saldo', v_saldo - p_valor
  );
END;
$$;

-- ============================================================
-- RPC: jr_pagamento_remover (admin/gestor)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_pagamento_remover(p_pagamento_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_conciliado BOOLEAN;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para remover pagamento';
  END IF;

  SELECT conciliado INTO v_conciliado
    FROM public.jr_pagamentos WHERE id = p_pagamento_id;

  IF v_conciliado IS NULL THEN
    RAISE EXCEPTION 'Pagamento nao encontrado';
  END IF;

  IF v_conciliado AND NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'Pagamento ja conciliado - apenas admin pode remover';
  END IF;

  DELETE FROM public.jr_pagamentos WHERE id = p_pagamento_id;
  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_pagamento_conciliar (financeiro/admin/gestor)
-- Marca/desmarca pagamento como conciliado (batimento bancario)
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_pagamento_conciliar(
  p_pagamento_id UUID,
  p_conciliado   BOOLEAN DEFAULT TRUE
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para conciliar pagamento';
  END IF;

  UPDATE public.jr_pagamentos
     SET conciliado = COALESCE(p_conciliado, TRUE),
         updated_at = NOW()
   WHERE id = p_pagamento_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Pagamento nao encontrado';
  END IF;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- VIEW: v_jr_pagamentos_detalhado
-- Lista pagamentos com info da OS e cliente (para pagina global)
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_pagamentos_detalhado AS
SELECT
  p.id,
  p.data,
  p.valor,
  p.forma,
  p.parcela_numero,
  p.parcela_total,
  p.documento,
  p.observacao,
  p.conciliado,
  p.os_id,
  os.numero_os,
  os.status              AS os_status,
  os.cliente_id,
  c.razao_social         AS cliente_nome,
  p.created_at,
  p.updated_at
FROM public.jr_pagamentos p
LEFT JOIN public.jr_ordens_servico os ON os.id = p.os_id
LEFT JOIN public.jr_clientes c        ON c.id = os.cliente_id;

GRANT SELECT ON public.v_jr_pagamentos_detalhado TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_pagamento_registrar  TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_pagamento_remover    TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_pagamento_conciliar  TO authenticated;

COMMENT ON FUNCTION public.jr_pagamento_registrar IS 'Registra pagamento em OS; respeita jr_config.permite_sinal.';
COMMENT ON FUNCTION public.jr_pagamento_remover   IS 'Remove pagamento; admin pode remover conciliado.';
COMMENT ON FUNCTION public.jr_pagamento_conciliar IS 'Marca pagamento como conciliado (batimento bancario).';
