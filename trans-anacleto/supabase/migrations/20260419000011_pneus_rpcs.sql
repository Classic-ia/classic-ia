-- =====================================================================
-- Fase 4: Pneus - RPCs (parte 2/2)
-- =====================================================================

-- -------------------------------------------------------------
-- Cadastro de pneu novo (gera codigo_interno automaticamente)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_cadastrar(
  p_marca text,
  p_modelo text,
  p_medida text,
  p_fornecedor_id uuid DEFAULT NULL,
  p_dot text DEFAULT NULL,
  p_data_fabricacao date DEFAULT NULL,
  p_sulco_inicial_mm numeric DEFAULT NULL,
  p_vida_util_km numeric DEFAULT NULL,
  p_custo_aquisicao numeric DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_codigo text;
  v_id uuid;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  v_codigo := trans_gerar_codigo_pneu();

  INSERT INTO trans_pneus (
    codigo_interno, marca, modelo, medida, fornecedor_id, dot,
    data_fabricacao, sulco_inicial_mm, sulco_atual_mm, vida_util_km,
    custo_aquisicao, observacao, status, localizacao_atual,
    created_by, updated_by
  ) VALUES (
    v_codigo, p_marca, p_modelo, p_medida, p_fornecedor_id, p_dot,
    p_data_fabricacao, p_sulco_inicial_mm, p_sulco_inicial_mm, p_vida_util_km,
    p_custo_aquisicao, p_observacao, 'estoque', 'almoxarifado',
    auth.uid(), auth.uid()
  ) RETURNING id INTO v_id;

  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, motivo, status_novo, observacao
  ) VALUES (
    v_id, 'entrada', auth.uid(),
    'Cadastro inicial de pneu', 'estoque', p_observacao
  );

  RETURN json_build_object(
    'ok', true, 'pneu_id', v_id, 'codigo_interno', v_codigo
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_cadastrar(text,text,text,uuid,text,date,numeric,numeric,numeric,text) TO authenticated;

-- -------------------------------------------------------------
-- Busca pneu por codigo + historico + acoes permitidas
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_buscar_por_codigo(p_codigo text)
RETURNS json
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_pneu_id uuid;
  v_result json;
BEGIN
  SELECT id INTO v_pneu_id FROM trans_pneus WHERE codigo_interno = p_codigo;

  IF v_pneu_id IS NULL THEN
    RETURN json_build_object('encontrado', false, 'codigo', p_codigo);
  END IF;

  SELECT json_build_object(
    'encontrado', true,
    'pneu', (SELECT row_to_json(p) FROM v_trans_pneus_full p WHERE p.id = v_pneu_id),
    'historico', (
      SELECT COALESCE(json_agg(row_to_json(h) ORDER BY h.data_hora DESC), '[]'::json)
      FROM (
        SELECT m.tipo, m.data_hora, m.motivo, m.observacao,
               m.destino, m.origem, m.foto_url,
               m.status_anterior, m.status_novo,
               v.placa AS veiculo_placa, m.posicao, m.km_veiculo,
               f.razao_social AS fornecedor_razao
        FROM trans_pneus_movimentacoes m
        LEFT JOIN trans_veiculos v ON v.id = m.veiculo_id
        LEFT JOIN trans_fornecedores f ON f.id = m.fornecedor_id
        WHERE m.pneu_id = v_pneu_id
        ORDER BY m.data_hora DESC
        LIMIT 50
      ) h
    ),
    'recapagens', (
      SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.data_envio DESC), '[]'::json)
      FROM (
        SELECT rc.*, f.razao_social AS fornecedor_razao
        FROM trans_pneus_recapagem rc
        LEFT JOIN trans_fornecedores f ON f.id = rc.fornecedor_id
        WHERE rc.pneu_id = v_pneu_id
      ) r
    )
  ) INTO v_result;

  RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_buscar_por_codigo(text) TO authenticated;

-- -------------------------------------------------------------
-- Instalar pneu em veiculo/posicao
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_instalar(
  p_pneu_id uuid,
  p_veiculo_id uuid,
  p_posicao text,
  p_km_veiculo numeric,
  p_foto_url text DEFAULT NULL,
  p_latitude numeric DEFAULT NULL,
  p_longitude numeric DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status_atual text;
  v_placa text;
  v_km_veic numeric;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT status INTO v_status_atual FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;
  IF v_status_atual IS NULL THEN RAISE EXCEPTION 'Pneu nao encontrado'; END IF;
  IF v_status_atual NOT IN ('estoque','reserva') THEN
    RAISE EXCEPTION 'Pneu com status % nao pode ser instalado', v_status_atual;
  END IF;

  SELECT placa, COALESCE(km_atual,0) INTO v_placa, v_km_veic
  FROM trans_veiculos WHERE id = p_veiculo_id FOR UPDATE;
  IF v_placa IS NULL THEN RAISE EXCEPTION 'Veiculo nao encontrado'; END IF;

  IF p_km_veiculo < v_km_veic THEN
    RAISE EXCEPTION 'KM do veiculo nao pode retroceder (atual %, informado %)', v_km_veic, p_km_veiculo;
  END IF;

  -- cria posicao ativa (UNIQUE parcial protege contra duplicacao)
  INSERT INTO trans_pneus_posicoes (
    pneu_id, veiculo_id, posicao, km_instalado, instalado_por
  ) VALUES (
    p_pneu_id, p_veiculo_id, p_posicao, p_km_veiculo, auth.uid()
  );

  -- atualiza pneu
  UPDATE trans_pneus
  SET status = 'instalado',
      localizacao_atual = 'veiculo-' || v_placa,
      updated_by = auth.uid()
  WHERE id = p_pneu_id;

  -- atualiza km do veiculo (monotonico)
  UPDATE trans_veiculos SET km_atual = GREATEST(COALESCE(km_atual,0), p_km_veiculo)
  WHERE id = p_veiculo_id;

  -- movimentacao
  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, km_veiculo, veiculo_id, posicao,
    foto_url, latitude, longitude, observacao,
    status_anterior, status_novo
  ) VALUES (
    p_pneu_id, 'instalacao', auth.uid(), p_km_veiculo, p_veiculo_id, p_posicao,
    p_foto_url, p_latitude, p_longitude, p_observacao,
    v_status_atual, 'instalado'
  );

  RETURN json_build_object(
    'ok', true, 'pneu_id', p_pneu_id, 'veiculo_placa', v_placa,
    'posicao', p_posicao, 'km', p_km_veiculo
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_instalar(uuid,uuid,text,numeric,text,numeric,numeric,text) TO authenticated;

-- -------------------------------------------------------------
-- Retirar pneu com destino
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_retirar(
  p_pneu_id uuid,
  p_km_veiculo numeric,
  p_destino text,
  p_motivo text,
  p_foto_url text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_pos_id uuid;
  v_veiculo_id uuid;
  v_posicao text;
  v_km_inst numeric;
  v_novo_status text;
  v_loc text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  IF p_destino NOT IN ('estoque','recapagem','sucata','troca_direta') THEN
    RAISE EXCEPTION 'Destino invalido: %', p_destino;
  END IF;

  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT status INTO v_status FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Pneu nao encontrado'; END IF;
  IF v_status <> 'instalado' THEN
    RAISE EXCEPTION 'Pneu com status % nao pode ser retirado', v_status;
  END IF;

  SELECT id, veiculo_id, posicao, km_instalado
    INTO v_pos_id, v_veiculo_id, v_posicao, v_km_inst
  FROM trans_pneus_posicoes
  WHERE pneu_id = p_pneu_id AND removido_em IS NULL
  FOR UPDATE;

  IF v_pos_id IS NULL THEN
    RAISE EXCEPTION 'Sem posicao ativa para retirada';
  END IF;

  IF p_km_veiculo < v_km_inst THEN
    RAISE EXCEPTION 'KM do veiculo (% ) menor que km de instalacao (%)', p_km_veiculo, v_km_inst;
  END IF;

  -- fecha posicao
  UPDATE trans_pneus_posicoes
  SET removido_em = now(),
      km_removido = p_km_veiculo,
      motivo_remocao = p_motivo,
      removido_por = auth.uid()
  WHERE id = v_pos_id;

  -- atualiza km do veiculo
  UPDATE trans_veiculos SET km_atual = GREATEST(COALESCE(km_atual,0), p_km_veiculo)
  WHERE id = v_veiculo_id;

  -- novo status + localizacao conforme destino
  v_novo_status := CASE
    WHEN p_destino = 'estoque' THEN 'estoque'
    WHEN p_destino = 'recapagem' THEN 'recapagem'
    WHEN p_destino = 'sucata' THEN 'sucata'
    WHEN p_destino = 'troca_direta' THEN 'estoque'
  END;
  v_loc := CASE
    WHEN p_destino = 'estoque' THEN 'almoxarifado'
    WHEN p_destino = 'recapagem' THEN 'recapadora-pendente'
    WHEN p_destino = 'sucata' THEN 'sucata'
    WHEN p_destino = 'troca_direta' THEN 'almoxarifado'
  END;

  UPDATE trans_pneus
  SET status = v_novo_status,
      localizacao_atual = v_loc,
      km_acumulado = km_acumulado + (p_km_veiculo - v_km_inst),
      updated_by = auth.uid()
  WHERE id = p_pneu_id;

  -- movimentacao
  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, km_veiculo, veiculo_id, posicao,
    destino, motivo, foto_url, observacao, status_anterior, status_novo
  ) VALUES (
    p_pneu_id, 'retirada', auth.uid(), p_km_veiculo, v_veiculo_id, v_posicao,
    p_destino, p_motivo, p_foto_url, p_observacao, v_status, v_novo_status
  );

  RETURN json_build_object(
    'ok', true, 'pneu_id', p_pneu_id,
    'km_rodado_trecho', p_km_veiculo - v_km_inst,
    'novo_status', v_novo_status, 'destino', p_destino
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_retirar(uuid,numeric,text,text,text,text) TO authenticated;

-- -------------------------------------------------------------
-- Enviar pneu para recapagem
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_enviar_recapagem(
  p_pneu_id uuid,
  p_fornecedor_id uuid,
  p_sulco_pre_mm numeric DEFAULT NULL,
  p_foto_url text DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_num int;
  v_km_rodado numeric;
  v_recap_id uuid;
  v_forn_nome text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT status, numero_recapagens, km_acumulado
    INTO v_status, v_num, v_km_rodado
  FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;

  IF v_status IS NULL THEN RAISE EXCEPTION 'Pneu nao encontrado'; END IF;
  IF v_status NOT IN ('estoque','recapagem') THEN
    RAISE EXCEPTION 'Pneu com status % nao pode ir para recapagem', v_status;
  END IF;

  SELECT razao_social INTO v_forn_nome FROM trans_fornecedores WHERE id = p_fornecedor_id;

  INSERT INTO trans_pneus_recapagem (
    pneu_id, fornecedor_id, sulco_pre_mm, km_rodado_antes,
    numero_recapagem, status, observacao, created_by
  ) VALUES (
    p_pneu_id, p_fornecedor_id, p_sulco_pre_mm, v_km_rodado,
    v_num + 1, 'enviado', p_observacao, auth.uid()
  ) RETURNING id INTO v_recap_id;

  UPDATE trans_pneus
  SET status = 'recapagem',
      localizacao_atual = COALESCE('recapadora-' || v_forn_nome, 'recapadora'),
      updated_by = auth.uid()
  WHERE id = p_pneu_id;

  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, fornecedor_id, recapagem_id,
    foto_url, motivo, observacao, status_anterior, status_novo
  ) VALUES (
    p_pneu_id, 'envio_recap', auth.uid(), p_fornecedor_id, v_recap_id,
    p_foto_url, 'Envio para recapagem #' || (v_num + 1), p_observacao,
    v_status, 'recapagem'
  );

  RETURN json_build_object(
    'ok', true, 'recapagem_id', v_recap_id, 'numero_recapagem', v_num + 1
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_enviar_recapagem(uuid,uuid,numeric,text,text) TO authenticated;

-- -------------------------------------------------------------
-- Retornar pneu da recapagem
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_retornar_recapagem(
  p_pneu_id uuid,
  p_sulco_pos_mm numeric DEFAULT NULL,
  p_custo numeric DEFAULT NULL,
  p_aprovado boolean DEFAULT true,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
  v_recap_id uuid;
  v_num int;
  v_novo_status text;
  v_nova_loc text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  SELECT status INTO v_status FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;
  IF v_status <> 'recapagem' THEN
    RAISE EXCEPTION 'Pneu nao esta em recapagem (status=%)', v_status;
  END IF;

  SELECT id, numero_recapagem INTO v_recap_id, v_num
  FROM trans_pneus_recapagem
  WHERE pneu_id = p_pneu_id AND status = 'enviado'
  ORDER BY data_envio DESC
  LIMIT 1 FOR UPDATE;

  IF v_recap_id IS NULL THEN
    RAISE EXCEPTION 'Nenhuma recapagem pendente para este pneu';
  END IF;

  UPDATE trans_pneus_recapagem
  SET status = CASE WHEN p_aprovado THEN 'retornado' ELSE 'reprovado' END,
      data_retorno = now(),
      sulco_pos_mm = p_sulco_pos_mm,
      custo = p_custo,
      observacao = COALESCE(p_observacao, observacao)
  WHERE id = v_recap_id;

  IF p_aprovado THEN
    v_novo_status := 'estoque';
    v_nova_loc := 'almoxarifado';
  ELSE
    v_novo_status := 'sucata';
    v_nova_loc := 'sucata';
  END IF;

  UPDATE trans_pneus
  SET status = v_novo_status,
      localizacao_atual = v_nova_loc,
      sulco_atual_mm = COALESCE(p_sulco_pos_mm, sulco_atual_mm),
      numero_recapagens = CASE WHEN p_aprovado THEN numero_recapagens + 1 ELSE numero_recapagens END,
      updated_by = auth.uid()
  WHERE id = p_pneu_id;

  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, recapagem_id, motivo, observacao,
    status_anterior, status_novo
  ) VALUES (
    p_pneu_id, 'retorno_recap', auth.uid(), v_recap_id,
    CASE WHEN p_aprovado THEN 'Retorno aprovado #' || v_num ELSE 'Retorno reprovado #' || v_num END,
    p_observacao, 'recapagem', v_novo_status
  );

  RETURN json_build_object(
    'ok', true, 'recapagem_id', v_recap_id,
    'aprovado', p_aprovado, 'novo_status', v_novo_status
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_retornar_recapagem(uuid,numeric,numeric,boolean,text) TO authenticated;

-- -------------------------------------------------------------
-- Descartar pneu (sucata)
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_sucatar(
  p_pneu_id uuid,
  p_motivo text,
  p_foto_url text,
  p_observacao text DEFAULT NULL
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
  v_status text;
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  IF p_foto_url IS NULL OR length(trim(p_foto_url)) = 0 THEN
    RAISE EXCEPTION 'Foto obrigatoria para descarte';
  END IF;

  IF p_motivo IS NULL OR length(trim(p_motivo)) = 0 THEN
    RAISE EXCEPTION 'Motivo obrigatorio';
  END IF;

  SELECT status INTO v_status FROM trans_pneus WHERE id = p_pneu_id FOR UPDATE;
  IF v_status IS NULL THEN RAISE EXCEPTION 'Pneu nao encontrado'; END IF;
  IF v_status = 'sucata' THEN RAISE EXCEPTION 'Pneu ja esta sucatado'; END IF;
  IF v_status = 'instalado' THEN
    RAISE EXCEPTION 'Retire o pneu do veiculo antes de sucatar';
  END IF;

  UPDATE trans_pneus
  SET status = 'sucata',
      localizacao_atual = 'sucata',
      updated_by = auth.uid()
  WHERE id = p_pneu_id;

  INSERT INTO trans_pneus_movimentacoes (
    pneu_id, tipo, usuario_id, motivo, foto_url, observacao,
    status_anterior, status_novo
  ) VALUES (
    p_pneu_id, 'sucata', auth.uid(), p_motivo, p_foto_url, p_observacao,
    v_status, 'sucata'
  );

  INSERT INTO trans_pneus_fotos (pneu_id, url, tipo, usuario_id)
  VALUES (p_pneu_id, p_foto_url, 'sucata', auth.uid());

  RETURN json_build_object('ok', true, 'pneu_id', p_pneu_id);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_sucatar(uuid,text,text,text) TO authenticated;

-- -------------------------------------------------------------
-- Inventario: dado localizacao + codigos lidos, retorna divergencias
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_pneu_inventario(
  p_localizacao text,
  p_codigos_lidos text[]
) RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil text := trans_current_perfil();
BEGIN
  IF v_perfil NOT IN ('admin','gestor','almoxarife') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  RETURN json_build_object(
    'ok', true,
    'localizacao', p_localizacao,
    'esperados', (
      SELECT COALESCE(json_agg(row_to_json(e)), '[]'::json)
      FROM (
        SELECT id, codigo_interno, marca, modelo, medida, status
        FROM trans_pneus
        WHERE localizacao_atual = p_localizacao
          AND status <> 'sucata'
      ) e
    ),
    'lidos', to_json(p_codigos_lidos),
    'faltantes', (
      SELECT COALESCE(json_agg(codigo_interno), '[]'::json)
      FROM trans_pneus
      WHERE localizacao_atual = p_localizacao
        AND status <> 'sucata'
        AND codigo_interno <> ALL(p_codigos_lidos)
    ),
    'extras', (
      SELECT COALESCE(json_agg(c), '[]'::json)
      FROM unnest(p_codigos_lidos) c
      WHERE NOT EXISTS (
        SELECT 1 FROM trans_pneus
        WHERE codigo_interno = c AND localizacao_atual = p_localizacao
      )
    )
  );
END;
$$;

GRANT EXECUTE ON FUNCTION trans_pneu_inventario(text,text[]) TO authenticated;
