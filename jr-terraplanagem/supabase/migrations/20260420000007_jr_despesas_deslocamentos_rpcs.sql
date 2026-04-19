-- ============================================================
-- Migration: Fase J4 - Despesas + Deslocamentos (RPCs + views)
-- ============================================================

-- ============================================================
-- RPC: jr_despesa_registrar
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_despesa_registrar(
  p_data               DATE,
  p_tipo               TEXT,
  p_valor              NUMERIC,
  p_descricao          TEXT DEFAULT NULL,
  p_maquina_id         UUID DEFAULT NULL,
  p_os_id              UUID DEFAULT NULL,
  p_fornecedor_id      UUID DEFAULT NULL,
  p_nota_fiscal_numero TEXT DEFAULT NULL,
  p_nf_chave           TEXT DEFAULT NULL,
  p_anexo_url          TEXT DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_id UUID;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','financeiro','operador') THEN
    RAISE EXCEPTION 'Sem permissao para registrar despesa';
  END IF;

  IF p_valor IS NULL OR p_valor < 0 THEN
    RAISE EXCEPTION 'Valor invalido';
  END IF;

  IF p_tipo NOT IN ('peca','manutencao','servico','combustivel','mao_de_obra','aluguel','outros') THEN
    RAISE EXCEPTION 'Tipo invalido: %', p_tipo;
  END IF;

  IF p_maquina_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_maquinas WHERE id = p_maquina_id) THEN
    RAISE EXCEPTION 'Maquina nao encontrada';
  END IF;
  IF p_os_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_ordens_servico WHERE id = p_os_id) THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF p_fornecedor_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_fornecedores WHERE id = p_fornecedor_id) THEN
    RAISE EXCEPTION 'Fornecedor nao encontrado';
  END IF;

  INSERT INTO public.jr_despesas (
    data, tipo, valor, descricao,
    maquina_id, os_id, fornecedor_id,
    nota_fiscal_numero, nf_chave, anexo_url
  ) VALUES (
    COALESCE(p_data, CURRENT_DATE), p_tipo, p_valor, p_descricao,
    p_maquina_id, p_os_id, p_fornecedor_id,
    p_nota_fiscal_numero, p_nf_chave, p_anexo_url
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================
-- RPC: jr_despesa_remover
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_despesa_remover(p_despesa_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para remover despesa';
  END IF;

  DELETE FROM public.jr_despesas WHERE id = p_despesa_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Despesa nao encontrada';
  END IF;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- RPC: jr_deslocamento_registrar
-- Auto-calcula valor_calc = km_rodado * jr_config.valor_km_deslocamento
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_deslocamento_registrar(
  p_data           DATE,
  p_veiculo_id     UUID,
  p_km_inicial     NUMERIC,
  p_km_final       NUMERIC,
  p_motivo         TEXT DEFAULT NULL,
  p_os_id          UUID DEFAULT NULL,
  p_maquina_id     UUID DEFAULT NULL,
  p_motorista_nome TEXT DEFAULT NULL,
  p_observacao     TEXT DEFAULT NULL,
  p_valor_km       NUMERIC DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_valor_km NUMERIC(12,4);
  v_id UUID;
  v_valor NUMERIC(14,2);
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil NOT IN ('admin','gestor','operador','financeiro') THEN
    RAISE EXCEPTION 'Sem permissao para registrar deslocamento';
  END IF;

  IF p_veiculo_id IS NULL OR NOT EXISTS (SELECT 1 FROM public.jr_veiculos WHERE id = p_veiculo_id) THEN
    RAISE EXCEPTION 'Veiculo invalido';
  END IF;

  IF p_km_inicial IS NULL OR p_km_final IS NULL THEN
    RAISE EXCEPTION 'KM inicial e final obrigatorios';
  END IF;
  IF p_km_final < p_km_inicial THEN
    RAISE EXCEPTION 'KM final nao pode ser menor que inicial';
  END IF;

  IF p_os_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_ordens_servico WHERE id = p_os_id) THEN
    RAISE EXCEPTION 'OS nao encontrada';
  END IF;
  IF p_maquina_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.jr_maquinas WHERE id = p_maquina_id) THEN
    RAISE EXCEPTION 'Maquina nao encontrada';
  END IF;

  -- Valor por km: prioridade parametro > config
  IF p_valor_km IS NOT NULL AND p_valor_km > 0 THEN
    v_valor_km := p_valor_km;
  ELSE
    SELECT valor::NUMERIC INTO v_valor_km
      FROM public.jr_config WHERE chave = 'valor_km_deslocamento';
    IF v_valor_km IS NULL THEN v_valor_km := 0; END IF;
  END IF;

  v_valor := (p_km_final - p_km_inicial) * v_valor_km;

  INSERT INTO public.jr_deslocamentos (
    data, veiculo_id, os_id, maquina_id,
    km_inicial, km_final, motivo, valor_calc,
    motorista_nome, observacao
  ) VALUES (
    COALESCE(p_data, CURRENT_DATE), p_veiculo_id, p_os_id, p_maquina_id,
    p_km_inicial, p_km_final, p_motivo, v_valor,
    p_motorista_nome, p_observacao
  )
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'id', v_id, 'valor_calc', v_valor, 'valor_km_aplicado', v_valor_km);
END;
$$;

-- ============================================================
-- RPC: jr_deslocamento_remover
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_deslocamento_remover(p_deslocamento_id UUID)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF public.jr_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao para remover deslocamento';
  END IF;

  DELETE FROM public.jr_deslocamentos WHERE id = p_deslocamento_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Deslocamento nao encontrado';
  END IF;

  RETURN jsonb_build_object('ok', true);
END;
$$;

-- ============================================================
-- VIEW: v_jr_despesas_detalhado
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_despesas_detalhado AS
SELECT
  d.id,
  d.data,
  d.tipo,
  d.valor,
  d.descricao,
  d.maquina_id,
  m.codigo    AS maquina_codigo,
  m.descricao AS maquina_descricao,
  d.os_id,
  os.numero_os,
  d.fornecedor_id,
  f.razao_social AS fornecedor_nome,
  d.nota_fiscal_numero,
  d.nf_chave,
  d.anexo_url,
  d.created_at,
  d.updated_at
FROM public.jr_despesas d
LEFT JOIN public.jr_maquinas      m  ON m.id  = d.maquina_id
LEFT JOIN public.jr_ordens_servico os ON os.id = d.os_id
LEFT JOIN public.jr_fornecedores  f  ON f.id  = d.fornecedor_id;

-- ============================================================
-- VIEW: v_jr_deslocamentos_detalhado
-- ============================================================
CREATE OR REPLACE VIEW public.v_jr_deslocamentos_detalhado AS
SELECT
  d.id,
  d.data,
  d.veiculo_id,
  v.placa      AS veiculo_placa,
  v.descricao  AS veiculo_descricao,
  d.os_id,
  os.numero_os,
  d.maquina_id,
  m.codigo     AS maquina_codigo,
  m.descricao  AS maquina_descricao,
  d.km_inicial,
  d.km_final,
  d.km_rodado,
  d.valor_calc,
  d.motivo,
  d.motorista_nome,
  d.observacao,
  d.created_at,
  d.updated_at
FROM public.jr_deslocamentos d
LEFT JOIN public.jr_veiculos      v  ON v.id  = d.veiculo_id
LEFT JOIN public.jr_maquinas      m  ON m.id  = d.maquina_id
LEFT JOIN public.jr_ordens_servico os ON os.id = d.os_id;

GRANT SELECT ON public.v_jr_despesas_detalhado      TO authenticated;
GRANT SELECT ON public.v_jr_deslocamentos_detalhado TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_despesa_registrar         TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_despesa_remover           TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_deslocamento_registrar    TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_deslocamento_remover      TO authenticated;
