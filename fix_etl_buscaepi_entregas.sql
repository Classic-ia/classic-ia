-- ============================================================================
-- fix_etl_buscaepi_entregas.sql
-- Corrige sync de entregas BuscaEPI → sst_epi_entrega
-- STATUS: APLICADO no banco em 2026-04-09
-- ============================================================================
--
-- PROBLEMAS:
--   1. Match de funcionário excluía desligados (AND status != 'desligado')
--      → 340 entregas históricas de ex-funcionários não entravam
--   2. motivo_entrega = 'importado_buscaepi' violava CHECK constraint
--      → valores aceitos: primeira_entrega, substituicao, perda, desgaste, mudanca_funcao
--   3. Faltava match por nome exato (sem fuzzy) como passo intermediário
--
-- RESULTADO: 462/500 processados (92.4%), 38 erros (funcionários inexistentes no RH)
-- ============================================================================

CREATE OR REPLACE FUNCTION public.etl_processar_buscaepi_entregas(p_importacao_id uuid DEFAULT NULL::uuid)
RETURNS TABLE(processados integer, inseridos integer, erros integer, ignorados integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public'
AS $function$
DECLARE
  r RECORD;
  v_proc INT := 0; v_ins INT := 0; v_err INT := 0; v_ign INT := 0;
  v_payload JSONB; v_entrega JSONB; v_colab JSONB;
  v_func_id UUID; v_cat_id UUID;
  v_data_entrega DATE;
  v_ca TEXT; v_matricula TEXT; v_nome_colab TEXT; v_cpf_colab TEXT;
  v_motivo TEXT;
BEGIN
  FOR r IN
    SELECT * FROM stg_buscaepi_entregas
    WHERE status_processamento IN ('pendente','erro')
      AND (importacao_id = p_importacao_id OR p_importacao_id IS NULL)
    LIMIT 5000
  LOOP
    BEGIN
      v_payload := r.payload_json;
      v_entrega := v_payload->'entrega';
      v_colab := v_entrega->'colaborador';

      v_ca := v_payload->>'ca';
      v_data_entrega := etl_parse_data(COALESCE(v_entrega->>'data_entrega', v_entrega->>'data_cadastro'));
      v_matricula := v_colab->>'matricula';
      v_nome_colab := UPPER(TRIM(v_colab->>'nome'));
      v_cpf_colab := LPAD(REGEXP_REPLACE(COALESCE(v_colab->>'cpf', ''), '[^0-9]', '', 'g'), 11, '0');

      -- Map motivo from BuscaEPI finalidade_entrega
      v_motivo := CASE LOWER(COALESCE(v_entrega->'finalidade_entrega'->>'descricao', ''))
        WHEN 'entrega inicial' THEN 'primeira_entrega'
        WHEN 'substituição' THEN 'substituicao'
        WHEN 'substituicao' THEN 'substituicao'
        WHEN 'perda' THEN 'perda'
        WHEN 'desgaste' THEN 'desgaste'
        WHEN 'mudança de função' THEN 'mudanca_funcao'
        ELSE 'primeira_entrega'
      END;

      IF v_data_entrega IS NULL THEN
        UPDATE stg_buscaepi_entregas SET status_processamento = 'ignorado' WHERE id = r.id;
        v_ign := v_ign + 1; CONTINUE;
      END IF;

      -- Match EPI: CA → nome → criar
      v_cat_id := NULL;
      IF v_ca IS NOT NULL AND v_ca != '' AND v_ca != 'null' THEN
        SELECT id INTO v_cat_id FROM sst_catalogo_epi WHERE ca_numero = v_ca::text LIMIT 1;
      END IF;
      IF v_cat_id IS NULL AND v_payload->>'descricao' IS NOT NULL THEN
        SELECT id INTO v_cat_id FROM sst_catalogo_epi
        WHERE UPPER(TRIM(descricao)) = UPPER(TRIM(v_payload->>'descricao')) LIMIT 1;
      END IF;
      IF v_cat_id IS NULL THEN
        INSERT INTO sst_catalogo_epi (descricao, ca_numero, categoria)
        VALUES (COALESCE(v_payload->>'descricao', 'EPI Importado'), NULLIF(v_ca::text, 'null'), 'EPI')
        RETURNING id INTO v_cat_id;
      END IF;

      -- Match funcionário: matrícula → CPF → mapa → nome exato → nome fuzzy
      -- IMPORTANTE: inclui desligados (entregas históricas são válidas)
      v_func_id := NULL;

      IF v_matricula IS NOT NULL AND v_matricula != '' THEN
        SELECT id INTO v_func_id FROM rh_funcionarios WHERE matricula = v_matricula LIMIT 1;
      END IF;

      IF v_func_id IS NULL AND v_cpf_colab != '00000000000' AND LENGTH(v_cpf_colab) = 11 THEN
        SELECT id INTO v_func_id FROM rh_funcionarios WHERE cpf = v_cpf_colab LIMIT 1;
      END IF;

      IF v_func_id IS NULL AND v_colab->>'id_colaborador' IS NOT NULL THEN
        SELECT id_core INTO v_func_id FROM etl_mapa_identidade
        WHERE origem_sistema = 'buscaepi' AND tipo_entidade = 'colaborador'
          AND id_origem = (v_colab->>'id_colaborador') LIMIT 1;
      END IF;

      -- Nome EXATO (incluindo desligados)
      IF v_func_id IS NULL AND v_nome_colab IS NOT NULL AND LENGTH(v_nome_colab) > 3 THEN
        SELECT id INTO v_func_id FROM rh_funcionarios
        WHERE UPPER(TRIM(nome_completo)) = v_nome_colab LIMIT 1;
      END IF;

      -- Nome fuzzy (incluindo desligados)
      IF v_func_id IS NULL AND v_nome_colab IS NOT NULL AND LENGTH(v_nome_colab) > 5
         AND v_nome_colab NOT LIKE '%TESTE%' THEN
        SELECT id INTO v_func_id FROM rh_funcionarios
        WHERE reconc_similaridade_nome(nome_completo, v_nome_colab) >= 85
        ORDER BY reconc_similaridade_nome(nome_completo, v_nome_colab) DESC LIMIT 1;
      END IF;

      IF v_func_id IS NULL THEN
        UPDATE stg_buscaepi_entregas SET status_processamento = 'erro',
          erro_detalhe = 'Funcionario nao encontrado: ' || COALESCE(v_nome_colab, '?') || ' mat:' || COALESCE(v_matricula, '?')
        WHERE id = r.id;
        v_err := v_err + 1; CONTINUE;
      END IF;

      -- Mapa de identidade
      IF v_colab->>'id_colaborador' IS NOT NULL THEN
        INSERT INTO etl_mapa_identidade (origem_sistema, tipo_entidade, id_origem, id_core, confianca)
        VALUES ('buscaepi', 'colaborador', v_colab->>'id_colaborador', v_func_id, 'alta')
        ON CONFLICT (origem_sistema, tipo_entidade, id_origem) DO UPDATE SET id_core = v_func_id, updated_at = NOW();
      END IF;

      -- Inserir entrega
      INSERT INTO sst_epi_entrega (funcionario_id, catalogo_epi_id, data_entrega, quantidade, motivo_entrega)
      VALUES (v_func_id, v_cat_id, v_data_entrega, 1, v_motivo);

      UPDATE stg_buscaepi_entregas SET status_processamento = 'processado' WHERE id = r.id;
      v_ins := v_ins + 1; v_proc := v_proc + 1;
    EXCEPTION WHEN OTHERS THEN
      UPDATE stg_buscaepi_entregas SET status_processamento = 'erro', erro_detalhe = SQLERRM WHERE id = r.id;
      v_err := v_err + 1;
    END;
  END LOOP;

  RETURN QUERY SELECT v_proc, v_ins, v_err, v_ign;
END;
$function$;
