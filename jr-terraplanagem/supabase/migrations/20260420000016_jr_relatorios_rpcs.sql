-- ============================================================
-- Migration: Fase J8 - RPCs de Relatorios
-- jr_relatorio_dre(p_data_ini, p_data_fim, p_cliente_id?, p_maquina_id?)
-- jr_exportar_csv(p_tipo, p_data_ini, p_data_fim)
-- jr_resultado_os(p_os_id)
-- ============================================================

-- ============================================================
-- RPC: jr_resultado_os
-- Breakdown completo de receita e custos de 1 OS.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_resultado_os(
  p_os_id UUID
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_os RECORD;
  v_resultado RECORD;
  v_periodos JSONB;
  v_pagamentos JSONB;
  v_despesas JSONB;
  v_deslocamentos JSONB;
  v_abastecimentos JSONB;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Sem permissao');
  END IF;

  SELECT o.*, c.nome AS cliente_nome, c.razao_social
    INTO v_os
    FROM public.jr_ordens_servico o
    LEFT JOIN public.jr_clientes c ON c.id = o.cliente_id
   WHERE o.id = p_os_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'OS nao encontrada');
  END IF;

  SELECT * INTO v_resultado FROM public.v_jr_resultado_os WHERE os_id = p_os_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'id', p.id,
           'data', p.data,
           'maquina_id', p.maquina_id,
           'maquina_codigo', mq.codigo,
           'maquina_descricao', mq.descricao,
           'horimetro_inicial', p.horimetro_inicial,
           'horimetro_final', p.horimetro_final,
           'horas', p.horas,
           'valor_hora_snapshot', p.valor_hora_snapshot,
           'valor_calc', p.valor_calc,
           'operador_nome', p.operador_nome,
           'observacao', p.observacao
         ) ORDER BY p.data), '[]'::jsonb)
    INTO v_periodos
    FROM public.jr_os_periodos p
    LEFT JOIN public.jr_maquinas mq ON mq.id = p.maquina_id
   WHERE p.os_id = p_os_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'id', pg.id,
           'data', pg.data,
           'valor', pg.valor,
           'forma', pg.forma,
           'parcela_numero', pg.parcela_numero,
           'parcela_total', pg.parcela_total,
           'documento', pg.documento,
           'conciliado', pg.conciliado
         ) ORDER BY pg.data), '[]'::jsonb)
    INTO v_pagamentos
    FROM public.jr_pagamentos pg
   WHERE pg.os_id = p_os_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'id', d.id,
           'data', d.data,
           'tipo', d.tipo,
           'valor', d.valor,
           'descricao', d.descricao,
           'fornecedor_id', d.fornecedor_id,
           'maquina_id', d.maquina_id
         ) ORDER BY d.data), '[]'::jsonb)
    INTO v_despesas
    FROM public.jr_despesas d
   WHERE d.os_id = p_os_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'id', dl.id,
           'data', dl.data,
           'veiculo_id', dl.veiculo_id,
           'km_rodado', dl.km_rodado,
           'valor_calc', dl.valor_calc,
           'motivo', dl.motivo,
           'motorista_nome', dl.motorista_nome
         ) ORDER BY dl.data), '[]'::jsonb)
    INTO v_deslocamentos
    FROM public.jr_deslocamentos dl
   WHERE dl.os_id = p_os_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'id', a.id,
           'data', a.data,
           'maquina_id', a.maquina_id,
           'origem', a.origem,
           'litros', a.litros,
           'valor_total', a.valor_total,
           'horimetro_leitura', a.horimetro_leitura,
           'consumo_l_h', a.consumo_l_h
         ) ORDER BY a.data), '[]'::jsonb)
    INTO v_abastecimentos
    FROM public.jr_abastecimentos a
   WHERE a.os_id = p_os_id;

  RETURN jsonb_build_object(
    'ok', true,
    'os', jsonb_build_object(
      'id', v_os.id,
      'numero_os', v_os.numero_os,
      'cliente_id', v_os.cliente_id,
      'cliente_nome', COALESCE(v_os.razao_social, v_os.cliente_nome),
      'obra_local', v_os.obra_local,
      'descricao_servico', v_os.descricao_servico,
      'status', v_os.status,
      'data_abertura', v_os.data_abertura,
      'data_inicio', v_os.data_inicio,
      'data_fim', v_os.data_fim,
      'valor_total', v_os.valor_total,
      'observacao', v_os.observacao
    ),
    'resumo', jsonb_build_object(
      'horas_totais', COALESCE(v_resultado.horas_totais, 0),
      'receita_periodos', COALESCE(v_resultado.receita_periodos, 0),
      'receita_pagamentos', COALESCE(v_resultado.receita_pagamentos, 0),
      'receita_total', COALESCE(v_resultado.receita_total, 0),
      'despesas_diretas', COALESCE(v_resultado.despesas_diretas, 0),
      'custo_deslocamentos', COALESCE(v_resultado.custo_deslocamentos, 0),
      'custo_abastecimentos', COALESCE(v_resultado.custo_abastecimentos, 0),
      'custos_totais', COALESCE(v_resultado.custos_totais, 0),
      'resultado_liquido', COALESCE(v_resultado.resultado_liquido, 0),
      'margem_percent', COALESCE(v_resultado.margem_percent, 0),
      'saldo_receber', COALESCE(v_resultado.saldo_receber, 0)
    ),
    'periodos', v_periodos,
    'pagamentos', v_pagamentos,
    'despesas', v_despesas,
    'deslocamentos', v_deslocamentos,
    'abastecimentos', v_abastecimentos
  );
END;
$$;

COMMENT ON FUNCTION public.jr_resultado_os(UUID) IS
  'Breakdown completo de uma OS: receita, custos e detalhes por secao.';

-- ============================================================
-- RPC: jr_relatorio_dre
-- DRE simplificado de um periodo + filtros opcionais.
-- Receita operacional = receita de OS no periodo
-- Custos diretos = despesas operacionais + combustivel + manutencao + deslocamento
-- Despesas admin = jr_despesas tipo=outros sem maquina/os
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_relatorio_dre(
  p_data_ini DATE,
  p_data_fim DATE,
  p_cliente_id UUID DEFAULT NULL,
  p_maquina_id UUID DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_receita_periodos NUMERIC := 0;
  v_receita_pagamentos NUMERIC := 0;
  v_receita_operacional NUMERIC := 0;
  v_desp_combustivel NUMERIC := 0;
  v_desp_manutencao NUMERIC := 0;
  v_desp_pecas NUMERIC := 0;
  v_desp_servicos NUMERIC := 0;
  v_desp_deslocamento NUMERIC := 0;
  v_desp_outros NUMERIC := 0;
  v_desp_admin NUMERIC := 0;
  v_custos_diretos NUMERIC := 0;
  v_resultado_operacional NUMERIC := 0;
  v_resultado_liquido NUMERIC := 0;
  v_margem NUMERIC := 0;
  v_horas_totais NUMERIC := 0;
  v_oss_abertas INT := 0;
  v_oss_concluidas INT := 0;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Sem permissao');
  END IF;

  IF p_data_ini IS NULL OR p_data_fim IS NULL OR p_data_fim < p_data_ini THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Periodo invalido');
  END IF;

  -- Receita: periodos de OS no intervalo (usa snapshot de valor_hora)
  SELECT COALESCE(SUM(p.valor_calc), 0), COALESCE(SUM(p.horas), 0)
    INTO v_receita_periodos, v_horas_totais
    FROM public.jr_os_periodos p
    JOIN public.jr_ordens_servico o ON o.id = p.os_id
   WHERE p.data BETWEEN p_data_ini AND p_data_fim
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id)
     AND (p_maquina_id IS NULL OR p.maquina_id = p_maquina_id);

  -- Pagamentos recebidos no intervalo (para visao de caixa)
  SELECT COALESCE(SUM(pg.valor), 0)
    INTO v_receita_pagamentos
    FROM public.jr_pagamentos pg
    JOIN public.jr_ordens_servico o ON o.id = pg.os_id
   WHERE pg.data BETWEEN p_data_ini AND p_data_fim
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id);

  v_receita_operacional := v_receita_periodos;

  -- Abastecimento (combustivel) - com filtros de maquina e/ou OS do cliente
  SELECT COALESCE(SUM(a.valor_total), 0)
    INTO v_desp_combustivel
    FROM public.jr_abastecimentos a
    LEFT JOIN public.jr_ordens_servico o ON o.id = a.os_id
   WHERE a.data BETWEEN p_data_ini AND p_data_fim
     AND (p_maquina_id IS NULL OR a.maquina_id = p_maquina_id)
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id);

  -- Manutencao (custo_total_calc via manutencoes concluidas/em andamento)
  SELECT COALESCE(SUM(mt.custo_total_calc), 0)
    INTO v_desp_manutencao
    FROM public.jr_manutencoes mt
   WHERE mt.data_abertura BETWEEN p_data_ini AND p_data_fim
     AND mt.status IN ('concluida','em_andamento')
     AND (p_maquina_id IS NULL OR mt.maquina_id = p_maquina_id);

  -- Deslocamentos
  SELECT COALESCE(SUM(dl.valor_calc), 0)
    INTO v_desp_deslocamento
    FROM public.jr_deslocamentos dl
    LEFT JOIN public.jr_ordens_servico o ON o.id = dl.os_id
   WHERE dl.data BETWEEN p_data_ini AND p_data_fim
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id)
     AND (p_maquina_id IS NULL OR dl.maquina_id = p_maquina_id);

  -- Despesas classificadas (exceto combustivel que ja contamos via abastecimento)
  SELECT
    COALESCE(SUM(CASE WHEN d.tipo = 'peca'       THEN d.valor ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN d.tipo = 'servico'    THEN d.valor ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN d.tipo = 'outros' AND d.os_id IS NULL AND d.maquina_id IS NULL
                      THEN d.valor ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN d.tipo = 'outros' AND (d.os_id IS NOT NULL OR d.maquina_id IS NOT NULL)
                      THEN d.valor ELSE 0 END), 0)
    INTO v_desp_pecas, v_desp_servicos, v_desp_admin, v_desp_outros
    FROM public.jr_despesas d
    LEFT JOIN public.jr_ordens_servico o ON o.id = d.os_id
   WHERE d.data BETWEEN p_data_ini AND p_data_fim
     AND d.tipo <> 'combustivel'                    -- nao duplicar
     AND d.tipo <> 'manutencao'                     -- ja contado via custo_total_calc
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id)
     AND (p_maquina_id IS NULL OR d.maquina_id = p_maquina_id);

  v_custos_diretos := v_desp_combustivel + v_desp_manutencao + v_desp_pecas
                     + v_desp_servicos + v_desp_deslocamento + v_desp_outros;
  v_resultado_operacional := v_receita_operacional - v_custos_diretos;
  v_resultado_liquido := v_resultado_operacional - v_desp_admin;
  v_margem := CASE WHEN v_receita_operacional > 0
                   THEN ROUND((v_resultado_liquido / v_receita_operacional) * 100, 2)
                   ELSE 0 END;

  -- Contadores de OS
  SELECT
    COUNT(*) FILTER (WHERE o.status IN ('aberta','em_execucao')),
    COUNT(*) FILTER (WHERE o.status = 'concluida')
    INTO v_oss_abertas, v_oss_concluidas
    FROM public.jr_ordens_servico o
   WHERE o.data_abertura <= p_data_fim
     AND (o.data_fim IS NULL OR o.data_fim >= p_data_ini)
     AND (p_cliente_id IS NULL OR o.cliente_id = p_cliente_id);

  RETURN jsonb_build_object(
    'ok', true,
    'periodo', jsonb_build_object(
      'data_ini', p_data_ini,
      'data_fim', p_data_fim,
      'cliente_id', p_cliente_id,
      'maquina_id', p_maquina_id
    ),
    'receita', jsonb_build_object(
      'operacional', v_receita_operacional,
      'periodos_calc', v_receita_periodos,
      'pagamentos_caixa', v_receita_pagamentos,
      'horas_totais', v_horas_totais
    ),
    'custos_diretos', jsonb_build_object(
      'combustivel', v_desp_combustivel,
      'manutencao', v_desp_manutencao,
      'pecas', v_desp_pecas,
      'servicos', v_desp_servicos,
      'deslocamento', v_desp_deslocamento,
      'outros', v_desp_outros,
      'total', v_custos_diretos
    ),
    'despesas_admin', v_desp_admin,
    'resultado', jsonb_build_object(
      'operacional', v_resultado_operacional,
      'liquido', v_resultado_liquido,
      'margem_percent', v_margem
    ),
    'oss', jsonb_build_object(
      'abertas_no_periodo', v_oss_abertas,
      'concluidas_no_periodo', v_oss_concluidas
    )
  );
END;
$$;

COMMENT ON FUNCTION public.jr_relatorio_dre(DATE, DATE, UUID, UUID) IS
  'DRE simplificado: receita operacional - custos diretos - despesas admin = resultado.';

-- ============================================================
-- RPC: jr_exportar_csv
-- Retorna um array de linhas (header + dados) para serializar no front.
-- Tipos aceitos: os, pagamentos, despesas, deslocamentos, manutencoes, abastecimentos
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_exportar_csv(
  p_tipo TEXT,
  p_data_ini DATE,
  p_data_fim DATE
) RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT;
  v_header JSONB;
  v_rows JSONB;
BEGIN
  v_perfil := public.jr_current_perfil();
  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Sem permissao');
  END IF;

  IF p_data_ini IS NULL OR p_data_fim IS NULL OR p_data_fim < p_data_ini THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Periodo invalido');
  END IF;

  IF p_tipo = 'os' THEN
    v_header := jsonb_build_array(
      'numero_os','cliente','obra_local','status',
      'data_abertura','data_fim','horas_totais','receita_total',
      'custos_totais','resultado_liquido','margem_percent','saldo_receber'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             o.numero_os,
             COALESCE(c.razao_social, c.nome, ''),
             COALESCE(o.obra_local, ''),
             o.status,
             to_char(o.data_abertura, 'YYYY-MM-DD'),
             COALESCE(to_char(o.data_fim, 'YYYY-MM-DD'), ''),
             COALESCE(r.horas_totais, 0),
             COALESCE(r.receita_total, 0),
             COALESCE(r.custos_totais, 0),
             COALESCE(r.resultado_liquido, 0),
             COALESCE(r.margem_percent, 0),
             COALESCE(r.saldo_receber, 0)
           ) ORDER BY o.data_abertura), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_ordens_servico o
      LEFT JOIN public.jr_clientes c ON c.id = o.cliente_id
      LEFT JOIN public.v_jr_resultado_os r ON r.os_id = o.id
     WHERE o.data_abertura BETWEEN p_data_ini AND p_data_fim;

  ELSIF p_tipo = 'pagamentos' THEN
    v_header := jsonb_build_array(
      'data','numero_os','cliente','valor','forma',
      'parcela_numero','parcela_total','documento','conciliado'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             to_char(pg.data, 'YYYY-MM-DD'),
             o.numero_os,
             COALESCE(c.razao_social, c.nome, ''),
             pg.valor,
             pg.forma,
             COALESCE(pg.parcela_numero, 1),
             COALESCE(pg.parcela_total, 1),
             COALESCE(pg.documento, ''),
             CASE WHEN pg.conciliado THEN 'Sim' ELSE 'Nao' END
           ) ORDER BY pg.data), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_pagamentos pg
      JOIN public.jr_ordens_servico o ON o.id = pg.os_id
      LEFT JOIN public.jr_clientes c ON c.id = o.cliente_id
     WHERE pg.data BETWEEN p_data_ini AND p_data_fim;

  ELSIF p_tipo = 'despesas' THEN
    v_header := jsonb_build_array(
      'data','tipo','valor','descricao','fornecedor','maquina','numero_os','nf'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             to_char(d.data, 'YYYY-MM-DD'),
             d.tipo,
             d.valor,
             COALESCE(d.descricao, ''),
             COALESCE(f.razao_social, ''),
             COALESCE(mq.codigo, ''),
             COALESCE(o.numero_os, ''),
             COALESCE(d.nota_fiscal_numero, '')
           ) ORDER BY d.data), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_despesas d
      LEFT JOIN public.jr_fornecedores f ON f.id = d.fornecedor_id
      LEFT JOIN public.jr_maquinas mq    ON mq.id = d.maquina_id
      LEFT JOIN public.jr_ordens_servico o ON o.id = d.os_id
     WHERE d.data BETWEEN p_data_ini AND p_data_fim;

  ELSIF p_tipo = 'deslocamentos' THEN
    v_header := jsonb_build_array(
      'data','veiculo','placa','numero_os','maquina',
      'km_rodado','valor_calc','motivo','motorista'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             to_char(dl.data, 'YYYY-MM-DD'),
             COALESCE(v.descricao, ''),
             COALESCE(v.placa, ''),
             COALESCE(o.numero_os, ''),
             COALESCE(mq.codigo, ''),
             COALESCE(dl.km_rodado, 0),
             COALESCE(dl.valor_calc, 0),
             COALESCE(dl.motivo, ''),
             COALESCE(dl.motorista_nome, '')
           ) ORDER BY dl.data), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_deslocamentos dl
      LEFT JOIN public.jr_veiculos v    ON v.id = dl.veiculo_id
      LEFT JOIN public.jr_ordens_servico o ON o.id = dl.os_id
      LEFT JOIN public.jr_maquinas mq   ON mq.id = dl.maquina_id
     WHERE dl.data BETWEEN p_data_ini AND p_data_fim;

  ELSIF p_tipo = 'manutencoes' THEN
    v_header := jsonb_build_array(
      'data_abertura','data_execucao','maquina','tipo','servico',
      'status','horimetro','custo_total','oficina','executado_por','proximo_horimetro','proxima_data'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             to_char(mt.data_abertura, 'YYYY-MM-DD'),
             COALESCE(to_char(mt.data_execucao, 'YYYY-MM-DD'), ''),
             COALESCE(mq.codigo, ''),
             mt.tipo,
             COALESCE(mt.servico, ''),
             mt.status,
             COALESCE(mt.horimetro_execucao, 0),
             COALESCE(mt.custo_total_calc, 0),
             COALESCE(mt.oficina, ''),
             COALESCE(mt.executado_por, ''),
             COALESCE(mt.proximo_horimetro, 0),
             COALESCE(to_char(mt.proxima_data, 'YYYY-MM-DD'), '')
           ) ORDER BY mt.data_abertura), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_manutencoes mt
      LEFT JOIN public.jr_maquinas mq ON mq.id = mt.maquina_id
     WHERE mt.data_abertura BETWEEN p_data_ini AND p_data_fim;

  ELSIF p_tipo = 'abastecimentos' THEN
    v_header := jsonb_build_array(
      'data','maquina','origem','tanque_combustivel','posto',
      'litros','valor_total','horimetro','consumo_l_h','numero_os','operador'
    );
    SELECT COALESCE(jsonb_agg(jsonb_build_array(
             to_char(a.data, 'YYYY-MM-DD'),
             COALESCE(mq.codigo, ''),
             a.origem,
             COALESCE(t.combustivel_tipo, ''),
             COALESCE(a.posto_nome, ''),
             a.litros,
             a.valor_total,
             COALESCE(a.horimetro_leitura, 0),
             COALESCE(a.consumo_l_h, 0),
             COALESCE(o.numero_os, ''),
             COALESCE(a.operador_nome, '')
           ) ORDER BY a.data), '[]'::jsonb)
      INTO v_rows
      FROM public.jr_abastecimentos a
      LEFT JOIN public.jr_maquinas mq ON mq.id = a.maquina_id
      LEFT JOIN public.jr_tanque t    ON t.id = a.tanque_id
      LEFT JOIN public.jr_ordens_servico o ON o.id = a.os_id
     WHERE a.data BETWEEN p_data_ini AND p_data_fim;

  ELSE
    RETURN jsonb_build_object('ok', false, 'error', 'Tipo invalido: ' || p_tipo);
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'tipo', p_tipo,
    'periodo_ini', p_data_ini,
    'periodo_fim', p_data_fim,
    'header', v_header,
    'rows', v_rows,
    'total_rows', jsonb_array_length(v_rows)
  );
END;
$$;

COMMENT ON FUNCTION public.jr_exportar_csv(TEXT, DATE, DATE) IS
  'Exporta CSV (header + rows) para tipos: os, pagamentos, despesas, deslocamentos, manutencoes, abastecimentos.';

-- ============================================================
-- GRANTS
-- ============================================================
GRANT EXECUTE ON FUNCTION public.jr_resultado_os(UUID) TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_relatorio_dre(DATE, DATE, UUID, UUID) TO authenticated;
GRANT EXECUTE ON FUNCTION public.jr_exportar_csv(TEXT, DATE, DATE) TO authenticated;
