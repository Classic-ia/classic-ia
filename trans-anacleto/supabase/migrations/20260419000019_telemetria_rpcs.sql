-- =====================================================================
-- Fase 6.5 parte 2 - RPCs de telemetria
-- =====================================================================
-- Contrato de ingestao:
--   Cada evento vem com placa externa (do Onixsat) e, se possivel, lat/lng.
--   Sistema mapeia placa->veiculo_id, normaliza e faz UPSERT idempotente.
--   Atualiza km_atual monotonicamente e faz geofence match por distancia
--   haversine (centro+raio).
-- =====================================================================

-- ---------------------------------------------------------------------
-- Helper: distancia haversine em metros
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_haversine_m(
  lat1 numeric, lon1 numeric, lat2 numeric, lon2 numeric
) RETURNS numeric LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  R constant numeric := 6371000;
  dlat numeric; dlon numeric; a numeric; c numeric;
BEGIN
  IF lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL THEN
    RETURN NULL;
  END IF;
  dlat := radians(lat2 - lat1);
  dlon := radians(lon2 - lon1);
  a := sin(dlat/2)^2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)^2;
  c := 2 * atan2(sqrt(a), sqrt(1-a));
  RETURN R * c;
END;
$$;

-- ---------------------------------------------------------------------
-- Helper: geofence mais proximo dentro do raio (retorna id ou NULL)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_geofence_match(
  p_lat numeric, p_lng numeric
) RETURNS uuid LANGUAGE plpgsql STABLE AS $$
DECLARE v_id uuid;
BEGIN
  IF p_lat IS NULL OR p_lng IS NULL THEN RETURN NULL; END IF;
  SELECT id INTO v_id FROM trans_geofences
  WHERE ativo = true
    AND trans_haversine_m(latitude, longitude, p_lat, p_lng) <= raio_metros
  ORDER BY trans_haversine_m(latitude, longitude, p_lat, p_lng) ASC
  LIMIT 1;
  RETURN v_id;
END;
$$;

-- ---------------------------------------------------------------------
-- RPC: trans_onixsat_ingestar
-- ---------------------------------------------------------------------
-- Recebe array de eventos (jsonb), faz upsert idempotente, atualiza
-- km_atual monotonicamente e detecta geofence.
CREATE OR REPLACE FUNCTION trans_onixsat_ingestar(p_eventos jsonb)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_ev jsonb;
  v_placa text;
  v_veic_id uuid;
  v_km_atual numeric;
  v_km_novo numeric;
  v_lat numeric; v_lng numeric;
  v_tipo text;
  v_dh timestamptz;
  v_geo_id uuid;
  v_ok int := 0;
  v_dup int := 0;
  v_erro int := 0;
BEGIN
  IF jsonb_typeof(p_eventos) != 'array' THEN
    RAISE EXCEPTION 'Payload deve ser array';
  END IF;

  FOR v_ev IN SELECT * FROM jsonb_array_elements(p_eventos)
  LOOP
    BEGIN
      v_placa := upper(regexp_replace(COALESCE(v_ev->>'placa',''),'[^A-Z0-9]','','g'));
      v_dh := (v_ev->>'data_hora')::timestamptz;
      v_tipo := COALESCE(v_ev->>'tipo_evento','posicao');
      v_lat := NULLIF(v_ev->>'latitude','')::numeric;
      v_lng := NULLIF(v_ev->>'longitude','')::numeric;
      v_km_novo := NULLIF(v_ev->>'km_veiculo','')::numeric;

      IF v_placa IS NULL OR length(v_placa) < 7 THEN
        v_erro := v_erro + 1; CONTINUE;
      END IF;
      IF v_dh IS NULL THEN
        v_erro := v_erro + 1; CONTINUE;
      END IF;

      SELECT id, km_atual INTO v_veic_id, v_km_atual
      FROM trans_veiculos
      WHERE upper(regexp_replace(placa,'[^A-Z0-9]','','g')) = v_placa;

      v_geo_id := trans_geofence_match(v_lat, v_lng);

      BEGIN
        INSERT INTO trans_telemetria_eventos (
          veiculo_id, placa_externa, data_hora, km_veiculo,
          velocidade, ignicao, latitude, longitude, tipo_evento,
          geofence_id, raw
        ) VALUES (
          v_veic_id, v_placa, v_dh, v_km_novo,
          NULLIF(v_ev->>'velocidade','')::numeric,
          NULLIF(v_ev->>'ignicao','')::boolean,
          v_lat, v_lng, v_tipo, v_geo_id, v_ev
        );
        v_ok := v_ok + 1;
      EXCEPTION WHEN unique_violation THEN
        v_dup := v_dup + 1;
      END;

      -- atualiza km_atual monotonicamente (apenas se evento novo ok)
      IF v_veic_id IS NOT NULL AND v_km_novo IS NOT NULL
         AND v_km_novo > COALESCE(v_km_atual, 0) THEN
        UPDATE trans_veiculos SET km_atual = v_km_novo, updated_at = now()
        WHERE id = v_veic_id;
      END IF;

    EXCEPTION WHEN OTHERS THEN
      v_erro := v_erro + 1;
    END;
  END LOOP;

  UPDATE trans_onixsat_config SET ultimo_sync = now(), ultimo_erro = NULL
  WHERE ativo = true;

  RETURN json_build_object('ok', v_ok, 'duplicados', v_dup, 'erro', v_erro);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_onixsat_ingestar(jsonb) TO authenticated, anon;

-- ---------------------------------------------------------------------
-- RPC: trans_telemetria_consolidar_dia
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_telemetria_consolidar_dia(
  p_data date,
  p_veiculo_id uuid DEFAULT NULL
) RETURNS int LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_count int := 0;
BEGIN
  INSERT INTO trans_telemetria_km_diario AS d (
    veiculo_id, data, km_inicio, km_fim, horas_ligadas,
    excessos_velocidade, frenagens_bruscas, velocidade_maxima, eventos_total
  )
  SELECT
    e.veiculo_id,
    p_data,
    MIN(e.km_veiculo) FILTER (WHERE e.km_veiculo IS NOT NULL),
    MAX(e.km_veiculo) FILTER (WHERE e.km_veiculo IS NOT NULL),
    COALESCE(SUM(CASE WHEN e.ignicao = true THEN 1 ELSE 0 END)::numeric / 60, 0),
    COUNT(*) FILTER (WHERE e.tipo_evento = 'velocidade_excesso'),
    COUNT(*) FILTER (WHERE e.tipo_evento = 'frenagem_brusca'),
    MAX(e.velocidade),
    COUNT(*)
  FROM trans_telemetria_eventos e
  WHERE e.data_hora::date = p_data
    AND e.veiculo_id IS NOT NULL
    AND (p_veiculo_id IS NULL OR e.veiculo_id = p_veiculo_id)
  GROUP BY e.veiculo_id
  ON CONFLICT (veiculo_id, data) DO UPDATE SET
    km_inicio = EXCLUDED.km_inicio,
    km_fim = EXCLUDED.km_fim,
    horas_ligadas = EXCLUDED.horas_ligadas,
    excessos_velocidade = EXCLUDED.excessos_velocidade,
    frenagens_bruscas = EXCLUDED.frenagens_bruscas,
    velocidade_maxima = EXCLUDED.velocidade_maxima,
    eventos_total = EXCLUDED.eventos_total,
    consolidado_em = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;

  -- marca eventos consolidados como processados
  UPDATE trans_telemetria_eventos
  SET processado = true
  WHERE data_hora::date = p_data
    AND (p_veiculo_id IS NULL OR veiculo_id = p_veiculo_id)
    AND processado = false;

  RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_telemetria_consolidar_dia(date, uuid) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_consumo_real (km_telemetria / litros no periodo)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_consumo_real(
  p_veiculo_id uuid,
  p_periodo_ini date,
  p_periodo_fim date
) RETURNS json LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_km numeric; v_litros numeric; v_consumo numeric;
BEGIN
  SELECT COALESCE(SUM(km_rodado),0) INTO v_km
  FROM trans_telemetria_km_diario
  WHERE veiculo_id = p_veiculo_id
    AND data BETWEEN p_periodo_ini AND p_periodo_fim;

  SELECT COALESCE(SUM(litros),0) INTO v_litros
  FROM trans_abastecimentos
  WHERE veiculo_id = p_veiculo_id
    AND data::date BETWEEN p_periodo_ini AND p_periodo_fim;

  v_consumo := CASE WHEN v_litros > 0 THEN v_km / v_litros ELSE NULL END;

  RETURN json_build_object(
    'veiculo_id', p_veiculo_id,
    'periodo_ini', p_periodo_ini,
    'periodo_fim', p_periodo_fim,
    'km_rodados', v_km,
    'litros_abastecidos', v_litros,
    'consumo_km_l', v_consumo
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_consumo_real(uuid, date, date) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_score_calcular
-- ---------------------------------------------------------------------
-- Score 100 - (peso*excessos + peso*frenagens + peso*desvio_consumo).
-- Periodo: diario (data_ref=dia), mensal (data_ref=primeiro_dia_mes).
CREATE OR REPLACE FUNCTION trans_score_calcular(
  p_motorista_id uuid,
  p_periodo text,
  p_data_ref date
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_ini date; v_fim date;
  v_exc int := 0; v_fre int := 0; v_parado int := 0;
  v_km numeric := 0; v_viagens int := 0;
  v_consumo numeric;
  v_score int;
  v_penal numeric;
BEGIN
  IF p_periodo NOT IN ('diario','semanal','mensal') THEN
    RAISE EXCEPTION 'Periodo invalido';
  END IF;

  v_ini := CASE p_periodo
    WHEN 'diario' THEN p_data_ref
    WHEN 'semanal' THEN p_data_ref - (EXTRACT(ISODOW FROM p_data_ref)::int - 1)
    WHEN 'mensal' THEN date_trunc('month', p_data_ref)::date
  END;
  v_fim := CASE p_periodo
    WHEN 'diario' THEN p_data_ref
    WHEN 'semanal' THEN v_ini + 6
    WHEN 'mensal' THEN (date_trunc('month', p_data_ref) + interval '1 month - 1 day')::date
  END;

  SELECT
    COALESCE(SUM(excessos_velocidade),0),
    COALESCE(SUM(frenagens_bruscas),0),
    COALESCE(SUM(km_rodados),0)
  INTO v_exc, v_fre, v_km
  FROM trans_viagens
  WHERE motorista_id = p_motorista_id
    AND data_inicio::date BETWEEN v_ini AND v_fim
    AND status = 'concluida';

  SELECT COUNT(*) INTO v_viagens FROM trans_viagens
  WHERE motorista_id = p_motorista_id
    AND data_inicio::date BETWEEN v_ini AND v_fim
    AND status = 'concluida';

  -- consumo medio do motorista no periodo (opcional)
  SELECT AVG(consumo_km_l) INTO v_consumo
  FROM trans_abastecimentos
  WHERE motorista_id = p_motorista_id
    AND data::date BETWEEN v_ini AND v_fim
    AND consumo_km_l IS NOT NULL;

  -- penalidade: cada excesso -2, cada frenagem -1, capped em 100
  v_penal := LEAST(100, v_exc * 2 + v_fre * 1);
  v_score := GREATEST(0, 100 - v_penal::int);

  INSERT INTO trans_motorista_score (
    motorista_id, periodo, data_ref,
    excessos_velocidade, frenagens_bruscas, tempo_parado_min,
    km_rodados, consumo_medio_km_l, viagens_count, score_calc, calculado_em
  ) VALUES (
    p_motorista_id, p_periodo, p_data_ref,
    v_exc, v_fre, v_parado, v_km, v_consumo, v_viagens, v_score, now()
  )
  ON CONFLICT (motorista_id, periodo, data_ref) DO UPDATE SET
    excessos_velocidade = EXCLUDED.excessos_velocidade,
    frenagens_bruscas = EXCLUDED.frenagens_bruscas,
    km_rodados = EXCLUDED.km_rodados,
    consumo_medio_km_l = EXCLUDED.consumo_medio_km_l,
    viagens_count = EXCLUDED.viagens_count,
    score_calc = EXCLUDED.score_calc,
    calculado_em = now();

  RETURN json_build_object(
    'motorista_id', p_motorista_id, 'periodo', p_periodo, 'data_ref', p_data_ref,
    'excessos', v_exc, 'frenagens', v_fre, 'km', v_km,
    'viagens', v_viagens, 'consumo_km_l', v_consumo, 'score', v_score
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_score_calcular(uuid, text, date) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_alertas_avaliar (motor de regras)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_alertas_avaliar()
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_gerados int := 0; v_r int;
BEGIN
  -- manutencao_vencida: plano_veiculo com proximo_km <= km_atual ou proxima_data <= hoje
  WITH pendentes AS (
    SELECT
      v.id AS veiculo_id,
      v.placa,
      p.nome AS plano_nome,
      pv.ultima_execucao_km,
      pv.ultima_execucao_data,
      p.intervalo_km,
      p.intervalo_dias,
      (COALESCE(pv.ultima_execucao_km,0) + COALESCE(p.intervalo_km, 0)) AS proximo_km,
      (COALESCE(pv.ultima_execucao_data, CURRENT_DATE) + COALESCE(p.intervalo_dias,0)) AS proxima_data,
      v.km_atual
    FROM trans_planos_veiculo pv
    JOIN trans_planos_manutencao p ON p.id = pv.plano_id AND p.ativo = true
    JOIN trans_veiculos v ON v.id = pv.veiculo_id
  )
  INSERT INTO trans_alertas_automaticos (
    tipo, severidade, entidade_tipo, entidade_id, mensagem, contexto
  )
  SELECT
    'manutencao_vencida',
    CASE
      WHEN proximo_km > 0 AND km_atual - proximo_km > (intervalo_km * 0.1) THEN 'critico'
      WHEN proxima_data < CURRENT_DATE - 7 THEN 'critico'
      ELSE 'aviso'
    END,
    'veiculo', veiculo_id,
    format('Manutencao "%s" vencida em %s', plano_nome, placa),
    json_build_object(
      'plano', plano_nome, 'proximo_km', proximo_km, 'km_atual', km_atual,
      'proxima_data', proxima_data
    )::jsonb
  FROM pendentes
  WHERE (intervalo_km > 0 AND km_atual > proximo_km)
     OR (intervalo_dias > 0 AND proxima_data <= CURRENT_DATE)
  ON CONFLICT (tipo, entidade_id, data_dia) DO NOTHING;
  GET DIAGNOSTICS v_r = ROW_COUNT; v_gerados := v_gerados + v_r;

  -- lote_vencendo: validade em 30 dias com saldo > 0
  INSERT INTO trans_alertas_automaticos (
    tipo, severidade, entidade_tipo, entidade_id, mensagem, contexto
  )
  SELECT
    'lote_vencendo',
    CASE WHEN l.data_validade <= CURRENT_DATE THEN 'critico'
         WHEN l.data_validade <= CURRENT_DATE + 7 THEN 'aviso'
         ELSE 'info' END,
    'lote', l.id,
    format('Lote %s (%s) vence em %s', l.lote_interno,
           (SELECT nome FROM trans_produtos WHERE id = l.produto_id),
           to_char(l.data_validade,'DD/MM/YYYY')),
    json_build_object('lote_interno', l.lote_interno, 'data_validade', l.data_validade,
                      'saldo', el.quantidade)::jsonb
  FROM trans_lotes l
  JOIN trans_estoque_lote el ON el.lote_id = l.id
  WHERE l.data_validade IS NOT NULL
    AND l.data_validade <= CURRENT_DATE + 30
    AND el.quantidade > 0
    AND l.status = 'liberado'
  ON CONFLICT (tipo, entidade_id, data_dia) DO NOTHING;
  GET DIAGNOSTICS v_r = ROW_COUNT; v_gerados := v_gerados + v_r;

  -- pneu_vida_util: pneu instalado com km rodado >= 90% da vida util
  INSERT INTO trans_alertas_automaticos (
    tipo, severidade, entidade_tipo, entidade_id, mensagem, contexto
  )
  SELECT
    'pneu_vida_util',
    CASE WHEN p.km_acumulado >= p.vida_util_km THEN 'critico' ELSE 'aviso' END,
    'pneu', p.id,
    format('Pneu %s (%s) atingiu %s%% da vida util',
           p.codigo_interno, COALESCE(p.medida,''),
           round(p.km_acumulado * 100.0 / NULLIF(p.vida_util_km,0))),
    json_build_object('km_acumulado', p.km_acumulado, 'vida_util_km', p.vida_util_km)::jsonb
  FROM trans_pneus p
  WHERE p.vida_util_km IS NOT NULL
    AND p.vida_util_km > 0
    AND p.km_acumulado >= p.vida_util_km * 0.9
    AND p.status IN ('instalado','estoque')
  ON CONFLICT (tipo, entidade_id, data_dia) DO NOTHING;
  GET DIAGNOSTICS v_r = ROW_COUNT; v_gerados := v_gerados + v_r;

  -- parado_excessivo: veiculo sem evento ignicao_on em 3+ dias e status=ativo
  INSERT INTO trans_alertas_automaticos (
    tipo, severidade, entidade_tipo, entidade_id, mensagem, contexto
  )
  SELECT
    'parado_excessivo',
    'aviso',
    'veiculo', v.id,
    format('Veiculo %s parado ha %s dias sem telemetria',
           v.placa,
           CASE WHEN max_ev IS NULL THEN 'indefinido' ELSE (CURRENT_DATE - max_ev::date)::text END),
    json_build_object('ultima_atividade', max_ev)::jsonb
  FROM (
    SELECT v.id, v.placa, v.status,
           (SELECT MAX(data_hora) FROM trans_telemetria_eventos
            WHERE veiculo_id = v.id AND tipo_evento = 'ignicao_on') AS max_ev
    FROM trans_veiculos v
    WHERE v.status = 'ativo'
  ) v
  WHERE max_ev IS NOT NULL AND max_ev < now() - interval '3 days'
  ON CONFLICT (tipo, entidade_id, data_dia) DO NOTHING;
  GET DIAGNOSTICS v_r = ROW_COUNT; v_gerados := v_gerados + v_r;

  RETURN json_build_object('gerados', v_gerados);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_alertas_avaliar() TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_alerta_resolver
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_alerta_resolver(
  p_alerta_id uuid,
  p_resolucao text
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_user uuid := auth.uid();
BEGIN
  IF trans_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;
  UPDATE trans_alertas_automaticos
  SET resolvido_em = now(), resolvido_por = v_user, resolucao = p_resolucao
  WHERE id = p_alerta_id AND resolvido_em IS NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'Alerta nao encontrado ou ja resolvido'; END IF;
  RETURN json_build_object('id', p_alerta_id, 'resolvido_em', now());
END;
$$;

GRANT EXECUTE ON FUNCTION trans_alerta_resolver(uuid, text) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_pneu_km_rodado (km real por pneu baseado em posicoes)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_km_rodado(p_pneu_id uuid)
RETURNS numeric LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_km numeric := 0;
  v_veic_km numeric;
  v_pos_km_inst numeric;
BEGIN
  SELECT COALESCE(SUM(
    CASE WHEN p.removido_em IS NOT NULL
         THEN COALESCE(p.km_removido,0) - COALESCE(p.km_instalado,0)
         ELSE 0 END
  ), 0) INTO v_km
  FROM trans_pneus_posicoes p
  WHERE p.pneu_id = p_pneu_id;

  -- soma posicao ativa: km_atual_veiculo - km_instalado
  SELECT pos.km_instalado, v.km_atual INTO v_pos_km_inst, v_veic_km
  FROM trans_pneus_posicoes pos
  JOIN trans_veiculos v ON v.id = pos.veiculo_id
  WHERE pos.pneu_id = p_pneu_id AND pos.removido_em IS NULL;

  IF v_pos_km_inst IS NOT NULL AND v_veic_km IS NOT NULL THEN
    v_km := v_km + GREATEST(0, v_veic_km - v_pos_km_inst);
  END IF;

  RETURN v_km;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_km_rodado(uuid) TO authenticated;

-- ---------------------------------------------------------------------
-- Trigger: abre/fecha viagem por geofence
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_tg_viagem_detectar()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_geofence_tipo text;
  v_viagem_id uuid;
BEGIN
  IF NEW.tipo_evento NOT IN ('geofence_in','geofence_out')
     OR NEW.geofence_id IS NULL OR NEW.veiculo_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT tipo INTO v_geofence_tipo FROM trans_geofences WHERE id = NEW.geofence_id;

  -- entrada em origem: abre viagem se nao existe em_andamento
  IF NEW.tipo_evento = 'geofence_in' AND v_geofence_tipo = 'origem' THEN
    SELECT id INTO v_viagem_id FROM trans_viagens
    WHERE veiculo_id = NEW.veiculo_id AND status = 'em_andamento' LIMIT 1;

    IF v_viagem_id IS NULL THEN
      INSERT INTO trans_viagens (
        veiculo_id, motorista_id, geofence_origem_id, data_inicio,
        km_inicial, status
      )
      SELECT NEW.veiculo_id, v.motorista_atual_id, NEW.geofence_id,
             NEW.data_hora, NEW.km_veiculo, 'em_andamento'
      FROM trans_veiculos v WHERE v.id = NEW.veiculo_id
      RETURNING id INTO v_viagem_id;
    END IF;
    NEW.viagem_id := v_viagem_id;
  END IF;

  -- entrada em destino: fecha viagem em_andamento
  IF NEW.tipo_evento = 'geofence_in' AND v_geofence_tipo = 'destino' THEN
    UPDATE trans_viagens SET
      geofence_destino_id = NEW.geofence_id,
      data_fim = NEW.data_hora,
      km_final = NEW.km_veiculo,
      status = 'concluida',
      fechada_automaticamente = true
    WHERE veiculo_id = NEW.veiculo_id AND status = 'em_andamento'
    RETURNING id INTO v_viagem_id;
    NEW.viagem_id := v_viagem_id;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trans_telem_viagem_tg ON trans_telemetria_eventos;
CREATE TRIGGER trans_telem_viagem_tg
BEFORE INSERT ON trans_telemetria_eventos
FOR EACH ROW EXECUTE FUNCTION trans_tg_viagem_detectar();
