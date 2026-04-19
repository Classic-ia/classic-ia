-- ============================================================
-- Migration: RPC jr_importar_localstorage (Fase J1)
-- Recebe JSON exportado do sistema legado (localStorage) e upserta
-- todas as entidades em suas tabelas. Idempotente via UNIQUE.
-- ============================================================

-- Formato esperado do payload:
-- {
--   "clientes":      [{ "nome": "...", "cpf_cnpj": "...", "contato": "...", ... }],
--   "fornecedores":  [{ "razao_social": "...", "cnpj": "...", "tipo": "...", ... }],
--   "maquinas":      [{ "codigo": "M001", "descricao": "Escavadeira CAT 320", "tipo": "escavadeira", "valor_hora": 180, "horimetro_atual": 1234.5, ... }],
--   "veiculos":      [{ "placa": "ABC1D23", "descricao": "Prancha 3 eixos", "tipo": "prancha", "km_atual": 58000, ... }],
--   "ordens_servico":[{ "numero_os": "OS-001", "cliente_codigo": "C001" | "cliente_nome": "...", "obra_local": "...", "data_abertura": "2025-10-15", "status": "concluida", "periodos": [{ "maquina_codigo": "M001", "data": "...", "horimetro_inicial": 1200, "horimetro_final": 1208, "valor_hora": 180, "operador_nome": "..." }] }],
--   "pagamentos":    [{ "os_numero": "OS-001", "data": "...", "valor": 5000, "forma": "pix", "documento": "..." }],
--   "despesas":      [{ "data": "...", "tipo": "peca", "valor": 1200, "maquina_codigo": "M001"|null, "os_numero": "OS-001"|null, "fornecedor_cnpj": "..."|null, "descricao": "..." }],
--   "deslocamentos": [{ "data": "...", "veiculo_placa": "...", "os_numero": "..."|null, "maquina_codigo": "..."|null, "km_inicial": 10, "km_final": 120, "motivo": "...", "motorista_nome": "..." }]
-- }

CREATE OR REPLACE FUNCTION public.jr_importar_localstorage(p_payload jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_item jsonb;
  v_cliente_id UUID;
  v_forn_id UUID;
  v_maq_id UUID;
  v_veic_id UUID;
  v_os_id UUID;

  -- mapas de codigo-legado -> id-interno para resolver foreign keys
  v_cli_map jsonb := '{}'::jsonb;  -- nome|cpf_cnpj -> id
  v_forn_map jsonb := '{}'::jsonb; -- cnpj -> id
  v_maq_map jsonb := '{}'::jsonb;  -- codigo -> id
  v_veic_map jsonb := '{}'::jsonb; -- placa -> id
  v_os_map jsonb := '{}'::jsonb;   -- numero_os -> id

  v_count_cli int := 0;
  v_count_forn int := 0;
  v_count_maq int := 0;
  v_count_veic int := 0;
  v_count_os int := 0;
  v_count_per int := 0;
  v_count_pag int := 0;
  v_count_desp int := 0;
  v_count_desl int := 0;
  v_erros jsonb := '[]'::jsonb;

  v_bloqueado BOOLEAN;
BEGIN
  -- gate via jr_config.importacao_executada (admin pode forcar re-run via RPC jr_config_reset)
  SELECT COALESCE(valor = 'true', false) INTO v_bloqueado
    FROM public.jr_config WHERE chave = 'importacao_executada';
  IF v_bloqueado THEN
    RAISE EXCEPTION 'Importacao legacy ja foi executada. Ajuste jr_config.importacao_executada=false para re-rodar.';
  END IF;

  IF NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'Apenas admin pode executar a importacao legacy';
  END IF;

  -- ========== CLIENTES ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'clientes','[]'::jsonb))
  LOOP
    BEGIN
      INSERT INTO public.jr_clientes (
        razao_social, nome_fantasia, cpf_cnpj, contato, email, telefone, endereco, cidade, uf, observacao
      ) VALUES (
        COALESCE(v_item->>'razao_social', v_item->>'nome', '(sem nome)'),
        v_item->>'nome_fantasia',
        NULLIF(v_item->>'cpf_cnpj',''),
        v_item->>'contato',
        v_item->>'email',
        v_item->>'telefone',
        v_item->>'endereco',
        v_item->>'cidade',
        UPPER(NULLIF(v_item->>'uf','')),
        v_item->>'observacao'
      )
      ON CONFLICT (cpf_cnpj) DO UPDATE SET
        razao_social = COALESCE(EXCLUDED.razao_social, public.jr_clientes.razao_social),
        contato = COALESCE(EXCLUDED.contato, public.jr_clientes.contato),
        updated_at = NOW()
      RETURNING id INTO v_cliente_id;

      -- fallback: buscar sem conflito (sem cpf_cnpj)
      IF v_cliente_id IS NULL THEN
        SELECT id INTO v_cliente_id FROM public.jr_clientes
          WHERE razao_social = COALESCE(v_item->>'razao_social', v_item->>'nome')
          ORDER BY created_at DESC LIMIT 1;
      END IF;

      IF v_item ? 'cpf_cnpj' AND NULLIF(v_item->>'cpf_cnpj','') IS NOT NULL THEN
        v_cli_map := v_cli_map || jsonb_build_object(v_item->>'cpf_cnpj', v_cliente_id::text);
      END IF;
      IF v_item ? 'nome' THEN
        v_cli_map := v_cli_map || jsonb_build_object(v_item->>'nome', v_cliente_id::text);
      END IF;
      IF v_item ? 'razao_social' THEN
        v_cli_map := v_cli_map || jsonb_build_object(v_item->>'razao_social', v_cliente_id::text);
      END IF;
      IF v_item ? 'codigo' THEN
        v_cli_map := v_cli_map || jsonb_build_object(v_item->>'codigo', v_cliente_id::text);
      END IF;

      v_count_cli := v_count_cli + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','cliente','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== FORNECEDORES ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'fornecedores','[]'::jsonb))
  LOOP
    BEGIN
      INSERT INTO public.jr_fornecedores (
        razao_social, cnpj, tipo, contato, email, telefone
      ) VALUES (
        COALESCE(v_item->>'razao_social', v_item->>'nome', '(sem nome)'),
        NULLIF(v_item->>'cnpj',''),
        COALESCE(v_item->>'tipo','outros'),
        v_item->>'contato',
        v_item->>'email',
        v_item->>'telefone'
      )
      ON CONFLICT (cnpj) DO UPDATE SET
        razao_social = COALESCE(EXCLUDED.razao_social, public.jr_fornecedores.razao_social),
        updated_at = NOW()
      RETURNING id INTO v_forn_id;

      IF v_forn_id IS NULL THEN
        SELECT id INTO v_forn_id FROM public.jr_fornecedores
          WHERE razao_social = COALESCE(v_item->>'razao_social', v_item->>'nome')
          ORDER BY created_at DESC LIMIT 1;
      END IF;

      IF v_item ? 'cnpj' AND NULLIF(v_item->>'cnpj','') IS NOT NULL THEN
        v_forn_map := v_forn_map || jsonb_build_object(v_item->>'cnpj', v_forn_id::text);
      END IF;
      IF v_item ? 'codigo' THEN
        v_forn_map := v_forn_map || jsonb_build_object(v_item->>'codigo', v_forn_id::text);
      END IF;
      v_count_forn := v_count_forn + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','fornecedor','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== MAQUINAS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'maquinas','[]'::jsonb))
  LOOP
    BEGIN
      INSERT INTO public.jr_maquinas (
        codigo, descricao, tipo, marca, modelo, ano, valor_hora, horimetro_atual, status
      ) VALUES (
        COALESCE(v_item->>'codigo', 'M-' || substring(gen_random_uuid()::text,1,6)),
        COALESCE(v_item->>'descricao', '(sem descricao)'),
        COALESCE(v_item->>'tipo','outra'),
        v_item->>'marca',
        v_item->>'modelo',
        NULLIF(v_item->>'ano','')::INT,
        COALESCE((v_item->>'valor_hora')::NUMERIC, 0),
        COALESCE((v_item->>'horimetro_atual')::NUMERIC, 0),
        COALESCE(v_item->>'status','ativa')
      )
      ON CONFLICT (codigo) DO UPDATE SET
        descricao = COALESCE(EXCLUDED.descricao, public.jr_maquinas.descricao),
        valor_hora = EXCLUDED.valor_hora,
        horimetro_atual = GREATEST(public.jr_maquinas.horimetro_atual, EXCLUDED.horimetro_atual),
        status = EXCLUDED.status,
        updated_at = NOW()
      RETURNING id INTO v_maq_id;

      v_maq_map := v_maq_map || jsonb_build_object(COALESCE(v_item->>'codigo',''), v_maq_id::text);
      v_count_maq := v_count_maq + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','maquina','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== VEICULOS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'veiculos','[]'::jsonb))
  LOOP
    BEGIN
      INSERT INTO public.jr_veiculos (
        placa, descricao, tipo, marca, modelo, ano, km_atual, status
      ) VALUES (
        UPPER(COALESCE(v_item->>'placa','SEM-PLACA-' || substring(gen_random_uuid()::text,1,4))),
        COALESCE(v_item->>'descricao', v_item->>'modelo', '(sem descricao)'),
        COALESCE(v_item->>'tipo','outro'),
        v_item->>'marca',
        v_item->>'modelo',
        NULLIF(v_item->>'ano','')::INT,
        COALESCE((v_item->>'km_atual')::NUMERIC, 0),
        COALESCE(v_item->>'status','ativo')
      )
      ON CONFLICT (placa) DO UPDATE SET
        descricao = COALESCE(EXCLUDED.descricao, public.jr_veiculos.descricao),
        km_atual = GREATEST(public.jr_veiculos.km_atual, EXCLUDED.km_atual),
        status = EXCLUDED.status,
        updated_at = NOW()
      RETURNING id INTO v_veic_id;

      v_veic_map := v_veic_map || jsonb_build_object(UPPER(COALESCE(v_item->>'placa','')), v_veic_id::text);
      v_count_veic := v_count_veic + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','veiculo','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== ORDENS DE SERVICO + PERIODOS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'ordens_servico','[]'::jsonb))
  LOOP
    BEGIN
      -- resolve cliente por cpf_cnpj ou nome/razao
      v_cliente_id := NULL;
      IF v_item ? 'cliente_cpf_cnpj' AND v_cli_map ? (v_item->>'cliente_cpf_cnpj') THEN
        v_cliente_id := (v_cli_map->>(v_item->>'cliente_cpf_cnpj'))::UUID;
      ELSIF v_item ? 'cliente_nome' AND v_cli_map ? (v_item->>'cliente_nome') THEN
        v_cliente_id := (v_cli_map->>(v_item->>'cliente_nome'))::UUID;
      ELSIF v_item ? 'cliente_codigo' AND v_cli_map ? (v_item->>'cliente_codigo') THEN
        v_cliente_id := (v_cli_map->>(v_item->>'cliente_codigo'))::UUID;
      END IF;

      INSERT INTO public.jr_ordens_servico (
        numero_os, cliente_id, data_abertura, data_inicio, data_fim,
        obra_local, descricao_servico, status, valor_total, observacao, origem_legacy
      ) VALUES (
        COALESCE(v_item->>'numero_os', 'OS-LEG-' || substring(gen_random_uuid()::text,1,6)),
        v_cliente_id,
        COALESCE(NULLIF(v_item->>'data_abertura','')::DATE, CURRENT_DATE),
        NULLIF(v_item->>'data_inicio','')::DATE,
        NULLIF(v_item->>'data_fim','')::DATE,
        v_item->>'obra_local',
        v_item->>'descricao_servico',
        COALESCE(v_item->>'status','aberta'),
        COALESCE((v_item->>'valor_total')::NUMERIC, 0),
        v_item->>'observacao',
        'localStorage'
      )
      ON CONFLICT (numero_os) DO UPDATE SET
        cliente_id = COALESCE(EXCLUDED.cliente_id, public.jr_ordens_servico.cliente_id),
        status = EXCLUDED.status,
        valor_total = EXCLUDED.valor_total,
        updated_at = NOW()
      RETURNING id INTO v_os_id;

      v_os_map := v_os_map || jsonb_build_object(v_item->>'numero_os', v_os_id::text);
      v_count_os := v_count_os + 1;

      -- periodos embutidos (opcional)
      IF v_item ? 'periodos' THEN
        DECLARE v_per jsonb;
        BEGIN
          FOR v_per IN SELECT * FROM jsonb_array_elements(v_item->'periodos')
          LOOP
            BEGIN
              v_maq_id := NULL;
              IF v_per ? 'maquina_codigo' AND v_maq_map ? (v_per->>'maquina_codigo') THEN
                v_maq_id := (v_maq_map->>(v_per->>'maquina_codigo'))::UUID;
              END IF;

              INSERT INTO public.jr_os_periodos (
                os_id, maquina_id, data, horimetro_inicial, horimetro_final,
                valor_hora_snapshot, operador_nome, observacao
              ) VALUES (
                v_os_id, v_maq_id,
                COALESCE(NULLIF(v_per->>'data','')::DATE, CURRENT_DATE),
                COALESCE((v_per->>'horimetro_inicial')::NUMERIC, 0),
                COALESCE((v_per->>'horimetro_final')::NUMERIC, 0),
                COALESCE((v_per->>'valor_hora')::NUMERIC, 0),
                v_per->>'operador_nome',
                v_per->>'observacao'
              );
              v_count_per := v_count_per + 1;
            EXCEPTION WHEN OTHERS THEN
              v_erros := v_erros || jsonb_build_object('entidade','periodo','item', v_per, 'erro', SQLERRM);
            END;
          END LOOP;
        END;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','os','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== PAGAMENTOS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'pagamentos','[]'::jsonb))
  LOOP
    BEGIN
      v_os_id := NULL;
      IF v_item ? 'os_numero' AND v_os_map ? (v_item->>'os_numero') THEN
        v_os_id := (v_os_map->>(v_item->>'os_numero'))::UUID;
      END IF;
      IF v_os_id IS NULL THEN
        v_erros := v_erros || jsonb_build_object('entidade','pagamento','item', v_item, 'erro','OS nao encontrada');
        CONTINUE;
      END IF;

      INSERT INTO public.jr_pagamentos (
        os_id, data, valor, forma, parcela_numero, parcela_total, documento, observacao, origem_legacy
      ) VALUES (
        v_os_id,
        COALESCE(NULLIF(v_item->>'data','')::DATE, CURRENT_DATE),
        COALESCE((v_item->>'valor')::NUMERIC, 0),
        COALESCE(v_item->>'forma','outro'),
        NULLIF(v_item->>'parcela_numero','')::INT,
        NULLIF(v_item->>'parcela_total','')::INT,
        v_item->>'documento',
        v_item->>'observacao',
        'localStorage'
      );
      v_count_pag := v_count_pag + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','pagamento','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== DESPESAS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'despesas','[]'::jsonb))
  LOOP
    BEGIN
      v_maq_id := NULL; v_os_id := NULL; v_forn_id := NULL;
      IF v_item ? 'maquina_codigo' AND v_maq_map ? (v_item->>'maquina_codigo') THEN
        v_maq_id := (v_maq_map->>(v_item->>'maquina_codigo'))::UUID;
      END IF;
      IF v_item ? 'os_numero' AND v_os_map ? (v_item->>'os_numero') THEN
        v_os_id := (v_os_map->>(v_item->>'os_numero'))::UUID;
      END IF;
      IF v_item ? 'fornecedor_cnpj' AND v_forn_map ? (v_item->>'fornecedor_cnpj') THEN
        v_forn_id := (v_forn_map->>(v_item->>'fornecedor_cnpj'))::UUID;
      END IF;

      INSERT INTO public.jr_despesas (
        data, tipo, maquina_id, os_id, fornecedor_id, valor, descricao,
        nota_fiscal_numero, origem_legacy
      ) VALUES (
        COALESCE(NULLIF(v_item->>'data','')::DATE, CURRENT_DATE),
        COALESCE(v_item->>'tipo','outros'),
        v_maq_id, v_os_id, v_forn_id,
        COALESCE((v_item->>'valor')::NUMERIC, 0),
        v_item->>'descricao',
        v_item->>'nota_fiscal_numero',
        'localStorage'
      );
      v_count_desp := v_count_desp + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','despesa','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- ========== DESLOCAMENTOS ==========
  FOR v_item IN SELECT * FROM jsonb_array_elements(COALESCE(p_payload->'deslocamentos','[]'::jsonb))
  LOOP
    BEGIN
      v_veic_id := NULL; v_os_id := NULL; v_maq_id := NULL;
      IF v_item ? 'veiculo_placa' AND v_veic_map ? UPPER(v_item->>'veiculo_placa') THEN
        v_veic_id := (v_veic_map->>UPPER(v_item->>'veiculo_placa'))::UUID;
      END IF;
      IF v_item ? 'os_numero' AND v_os_map ? (v_item->>'os_numero') THEN
        v_os_id := (v_os_map->>(v_item->>'os_numero'))::UUID;
      END IF;
      IF v_item ? 'maquina_codigo' AND v_maq_map ? (v_item->>'maquina_codigo') THEN
        v_maq_id := (v_maq_map->>(v_item->>'maquina_codigo'))::UUID;
      END IF;

      INSERT INTO public.jr_deslocamentos (
        data, veiculo_id, os_id, maquina_id, km_inicial, km_final, motivo,
        valor_calc, motorista_nome, observacao, origem_legacy
      ) VALUES (
        COALESCE(NULLIF(v_item->>'data','')::DATE, CURRENT_DATE),
        v_veic_id, v_os_id, v_maq_id,
        NULLIF(v_item->>'km_inicial','')::NUMERIC,
        NULLIF(v_item->>'km_final','')::NUMERIC,
        v_item->>'motivo',
        NULLIF(v_item->>'valor_calc','')::NUMERIC,
        v_item->>'motorista_nome',
        v_item->>'observacao',
        'localStorage'
      );
      v_count_desl := v_count_desl + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erros := v_erros || jsonb_build_object('entidade','deslocamento','item', v_item, 'erro', SQLERRM);
    END;
  END LOOP;

  -- marca flag de importacao executada
  UPDATE public.jr_config SET valor = 'true', updated_at = NOW()
   WHERE chave = 'importacao_executada';

  RETURN jsonb_build_object(
    'ok', true,
    'clientes', v_count_cli,
    'fornecedores', v_count_forn,
    'maquinas', v_count_maq,
    'veiculos', v_count_veic,
    'ordens_servico', v_count_os,
    'periodos', v_count_per,
    'pagamentos', v_count_pag,
    'despesas', v_count_desp,
    'deslocamentos', v_count_desl,
    'erros', v_erros,
    'erros_count', jsonb_array_length(v_erros)
  );
END;
$$;

GRANT EXECUTE ON FUNCTION public.jr_importar_localstorage(jsonb) TO authenticated;

COMMENT ON FUNCTION public.jr_importar_localstorage(jsonb) IS
  'Importa JSON exportado do sistema legado (localStorage). Bloqueado apos primeira execucao via jr_config.importacao_executada. Admin-only.';

-- ============================================================
-- RPC jr_importar_reset: permite admin re-executar importacao
-- ============================================================
CREATE OR REPLACE FUNCTION public.jr_importar_reset()
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT public.jr_is_admin() THEN
    RAISE EXCEPTION 'Apenas admin pode resetar a flag de importacao';
  END IF;
  UPDATE public.jr_config SET valor = 'false', updated_at = NOW()
   WHERE chave = 'importacao_executada';
  RETURN TRUE;
END;
$$;

GRANT EXECUTE ON FUNCTION public.jr_importar_reset() TO authenticated;
COMMENT ON FUNCTION public.jr_importar_reset() IS 'Reseta flag importacao_executada para permitir nova importacao.';
