-- =====================================================================
-- Fase 7 - Dashboard Gerencial (RPCs de agregacao)
-- =====================================================================
-- Consolida multiplas queries num unico JSON para o painel inicial.
-- Evita N round-trips do cliente. Retornos:
--   frota: { ativos, parados, manutencao, vendidos }
--   manutencoes: { abertas, vencidas, vence_em_breve }
--   pneus: { em_estoque, instalados, recapagem, sucata, vida_util_baixa }
--   combustivel_30d: { abastecimentos, litros, valor, consumo_medio }
--   alertas: [{ tipo, severidade, count }]
--   km_diario_30d: [{ data, km_total }]  -- para chart
--   custos_30d: { combustivel, manutencao, pneus, total }
--   ranking_motoristas_mes: [{ motorista_id, nome, score, posicao }]
-- =====================================================================

CREATE OR REPLACE FUNCTION trans_dashboard_summary()
RETURNS json LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_frota json;
  v_manut json;
  v_pneus json;
  v_comb json;
  v_alertas json;
  v_km_30d json;
  v_custos json;
  v_rank json;
  v_hoje date := CURRENT_DATE;
  v_30d date := CURRENT_DATE - 30;
  v_mes_ini date := date_trunc('month', CURRENT_DATE)::date;
BEGIN
  -- Frota por status
  SELECT json_build_object(
    'total', COUNT(*),
    'ativos', COUNT(*) FILTER (WHERE status = 'ativo'),
    'parados', COUNT(*) FILTER (WHERE status = 'parado'),
    'manutencao', COUNT(*) FILTER (WHERE status = 'manutencao'),
    'vendidos', COUNT(*) FILTER (WHERE status = 'vendido')
  ) INTO v_frota FROM trans_veiculos;

  -- Manutencoes
  SELECT json_build_object(
    'abertas', (SELECT COUNT(*) FROM trans_manutencoes WHERE status IN ('aberta','em_andamento')),
    'vencidas', (SELECT COUNT(*) FROM v_trans_manutencoes_pendentes WHERE situacao = 'vencida'),
    'vence_em_breve', (SELECT COUNT(*) FROM v_trans_manutencoes_pendentes WHERE situacao = 'vence_em_breve')
  ) INTO v_manut;

  -- Pneus
  SELECT json_build_object(
    'total', COUNT(*),
    'em_estoque', COUNT(*) FILTER (WHERE status = 'estoque'),
    'instalados', COUNT(*) FILTER (WHERE status = 'instalado'),
    'recapagem', COUNT(*) FILTER (WHERE status = 'recapagem'),
    'sucata', COUNT(*) FILTER (WHERE status = 'sucata'),
    'vida_util_baixa', COUNT(*) FILTER (
      WHERE vida_util_km IS NOT NULL AND vida_util_km > 0
      AND km_acumulado >= vida_util_km * 0.9
      AND status IN ('instalado','estoque')
    )
  ) INTO v_pneus FROM trans_pneus;

  -- Combustivel 30 dias
  SELECT json_build_object(
    'abastecimentos', COUNT(*),
    'litros', COALESCE(SUM(litros),0),
    'valor', COALESCE(SUM(valor_total),0),
    'consumo_medio', AVG(consumo_km_l) FILTER (WHERE consumo_km_l IS NOT NULL)
  ) INTO v_comb
  FROM trans_abastecimentos WHERE data::date >= v_30d;

  -- Alertas abertos por tipo/severidade
  SELECT json_agg(row_to_json(a)) INTO v_alertas FROM (
    SELECT tipo, severidade, COUNT(*)::int AS total
    FROM trans_alertas_automaticos
    WHERE resolvido_em IS NULL
    GROUP BY tipo, severidade
    ORDER BY
      CASE severidade WHEN 'critico' THEN 1 WHEN 'aviso' THEN 2 ELSE 3 END,
      total DESC
  ) a;

  -- KM rodado por dia ultimos 30 dias (somado toda frota)
  SELECT json_agg(row_to_json(d) ORDER BY d.data) INTO v_km_30d FROM (
    SELECT data, SUM(km_rodado)::numeric AS km_total
    FROM trans_telemetria_km_diario
    WHERE data >= v_30d
    GROUP BY data
  ) d;

  -- Custos 30 dias agregados
  SELECT json_build_object(
    'combustivel', (SELECT COALESCE(SUM(valor_total),0) FROM trans_abastecimentos WHERE data::date >= v_30d),
    'manutencao', (SELECT COALESCE(SUM(custo_total_calc),0) FROM trans_manutencoes
                   WHERE data_encerramento::date >= v_30d AND status = 'concluida'),
    'pneus', (SELECT COALESCE(SUM(custo),0) FROM trans_pneus_recapagem WHERE data_retorno::date >= v_30d)
  ) INTO v_custos;

  -- Ranking top 5 motoristas do mes
  SELECT json_agg(row_to_json(r) ORDER BY r.posicao) INTO v_rank FROM (
    SELECT motorista_id, motorista_nome, score_calc, km_rodados, viagens_count, posicao
    FROM v_trans_motorista_score_ranking
    WHERE periodo = 'mensal' AND data_ref = v_mes_ini
    ORDER BY posicao
    LIMIT 5
  ) r;

  RETURN json_build_object(
    'frota', v_frota,
    'manutencoes', v_manut,
    'pneus', v_pneus,
    'combustivel_30d', v_comb,
    'alertas', COALESCE(v_alertas, '[]'::json),
    'km_diario_30d', COALESCE(v_km_30d, '[]'::json),
    'custos_30d', v_custos,
    'ranking_motoristas_mes', COALESCE(v_rank, '[]'::json),
    'gerado_em', now()
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_dashboard_summary() TO authenticated;

-- ---------------------------------------------------------------------
-- RPC auxiliar: consumo medio por veiculo ultimos 30 dias
-- (para chart de bar "top consumidores")
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_dashboard_consumo_veiculo(p_limit int DEFAULT 10)
RETURNS TABLE (
  veiculo_id uuid, placa text, modelo text,
  litros numeric, km numeric, consumo_km_l numeric, abastecimentos int
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
BEGIN
  RETURN QUERY
  SELECT
    v.id, v.placa, v.modelo,
    COALESCE(SUM(a.litros),0) AS litros,
    COALESCE(SUM(a.km_percorrido),0) AS km,
    CASE WHEN SUM(a.litros) > 0 THEN SUM(a.km_percorrido) / SUM(a.litros) ELSE NULL END AS consumo_km_l,
    COUNT(a.*)::int AS abastecimentos
  FROM trans_veiculos v
  LEFT JOIN trans_abastecimentos a
    ON a.veiculo_id = v.id AND a.data::date >= CURRENT_DATE - 30
  GROUP BY v.id, v.placa, v.modelo
  HAVING COUNT(a.*) > 0
  ORDER BY COUNT(a.*) DESC
  LIMIT p_limit;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_dashboard_consumo_veiculo(int) TO authenticated;
