-- ════════════════════════════════════════════════════════════════════════════
-- ETL: stg_convenia_funcionarios → rh_funcionarios
-- Promove dados do staging para o core, com lookup de cargo/setor por nome.
--
-- CHAVE DE CONCILIACAO: CPF (varchar(11), UNIQUE NOT NULL em rh_funcionarios)
--   - id_origem no staging = ID Convenia (uuid/int do employee na API)
--   - CPF e extraido de payload_json->>'cpf' e limpo (so digitos, 11 chars)
--   - Se CPF ja existe em rh_funcionarios → UPDATE campos nao-criticos
--   - Se CPF nao existe → INSERT (exige empresa/filial/setor/cargo resolvidos)
--
-- REGRAS:
--   - NUNCA sobrescreve cargo_id/setor_id se ja preenchidos no core
--   - Atualiza apenas: nome, email, telefone, salario, status, data_nascimento
--   - Registra match em reconc_correspondencia.convenia_id
--   - Pula registros sem CPF valido (11 digitos)
--   - Marca staging como 'processado' ou 'erro'
--
-- DEPENDENCIAS: rh_funcionarios, rh_setores, rh_cargos, rh_empresas, rh_filiais
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.etl_processar_convenia_funcionarios(
  p_importacao_id UUID DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  r RECORD;
  v_cpf VARCHAR(11);
  v_nome TEXT;
  v_email TEXT;
  v_telefone TEXT;
  v_salario NUMERIC(12,2);
  v_data_admissao DATE;
  v_data_nascimento DATE;
  v_status VARCHAR(20);
  v_cargo_nome TEXT;
  v_setor_nome TEXT;
  v_cargo_id UUID;
  v_setor_id UUID;
  v_empresa_id UUID;
  v_filial_id UUID;
  v_func_id UUID;
  v_convenia_id TEXT;
  v_total INTEGER := 0;
  v_atualizados INTEGER := 0;
  v_inseridos INTEGER := 0;
  v_erros INTEGER := 0;
  v_pulados INTEGER := 0;
BEGIN
  -- Empresa e filial padrao (primeira de cada tabela)
  SELECT id INTO v_empresa_id FROM rh_empresas ORDER BY created_at LIMIT 1;
  SELECT id INTO v_filial_id FROM rh_filiais WHERE empresa_id = v_empresa_id ORDER BY created_at LIMIT 1;

  IF v_empresa_id IS NULL OR v_filial_id IS NULL THEN
    RETURN jsonb_build_object(
      'erro', 'Nenhuma empresa/filial cadastrada. Cadastre antes de processar.',
      'total', 0
    );
  END IF;

  -- Iterar registros pendentes no staging
  FOR r IN
    SELECT id, id_origem, payload_json
    FROM stg_convenia_funcionarios
    WHERE status_processamento = 'pendente'
      AND origem_sistema = 'convenia'
      AND (p_importacao_id IS NULL OR importacao_id = p_importacao_id)
    ORDER BY created_at
  LOOP
    v_total := v_total + 1;
    BEGIN
      -- ── Extrair campos do payload ──────────────────────────────────
      v_convenia_id   := r.payload_json->>'id';
      v_cpf           := regexp_replace(COALESCE(r.payload_json->>'cpf', ''), '\D', '', 'g');
      v_nome          := COALESCE(r.payload_json->>'name', r.payload_json->>'nome', '');
      v_email         := COALESCE(r.payload_json->>'email', r.payload_json->>'work_email', '');
      v_telefone      := COALESCE(r.payload_json->>'phone', r.payload_json->>'mobile', '');
      v_cargo_nome    := COALESCE(r.payload_json->>'position', r.payload_json->>'cargo', '');
      v_setor_nome    := COALESCE(
                           r.payload_json->'department'->>'name',
                           r.payload_json->>'departamento', '');
      v_data_admissao := (r.payload_json->>'admission_date')::date;
      v_data_nascimento := (r.payload_json->>'birthday')::date;
      v_salario       := (r.payload_json->>'salary')::numeric;
      v_status        := CASE
                           WHEN (r.payload_json->>'active')::boolean THEN 'ativo'
                           ELSE 'desligado'
                         END;

      -- ── Validar CPF ────────────────────────────────────────────────
      IF length(v_cpf) <> 11 THEN
        UPDATE stg_convenia_funcionarios
          SET status_processamento = 'erro',
              erro_detalhe = 'CPF invalido: ' || COALESCE(v_cpf, 'null')
          WHERE id = r.id;
        v_erros := v_erros + 1;
        CONTINUE;
      END IF;

      -- ── Lookup cargo por nome (case-insensitive) ───────────────────
      v_cargo_id := NULL;
      IF v_cargo_nome <> '' THEN
        SELECT id INTO v_cargo_id
        FROM rh_cargos
        WHERE lower(nome) = lower(v_cargo_nome)
        LIMIT 1;
      END IF;

      -- ── Lookup setor por nome (case-insensitive) ───────────────────
      v_setor_id := NULL;
      IF v_setor_nome <> '' THEN
        SELECT id INTO v_setor_id
        FROM rh_setores
        WHERE lower(nome) = lower(v_setor_nome)
        LIMIT 1;
      END IF;

      -- ── Verificar se funcionario ja existe por CPF ─────────────────
      SELECT id, cargo_id, setor_id INTO v_func_id, v_cargo_id, v_setor_id
      FROM rh_funcionarios
      WHERE cpf = v_cpf;

      IF v_func_id IS NOT NULL THEN
        -- UPDATE: atualiza apenas campos nao-criticos
        UPDATE rh_funcionarios SET
          nome_completo   = COALESCE(NULLIF(v_nome, ''), nome_completo),
          email_pessoal   = COALESCE(NULLIF(v_email, ''), email_pessoal),
          telefone        = COALESCE(NULLIF(v_telefone, ''), telefone),
          salario_base    = COALESCE(v_salario, salario_base),
          data_nascimento = COALESCE(v_data_nascimento, data_nascimento),
          status          = v_status,
          updated_at      = NOW()
        WHERE id = v_func_id;

        v_atualizados := v_atualizados + 1;
      ELSE
        -- INSERT: exige cargo e setor resolvidos
        IF v_cargo_id IS NULL THEN
          -- Criar cargo generico se nao encontrar
          INSERT INTO rh_cargos (nome) VALUES (COALESCE(NULLIF(v_cargo_nome,''), 'Nao Definido'))
          ON CONFLICT (nome) DO NOTHING;
          SELECT id INTO v_cargo_id FROM rh_cargos
            WHERE lower(nome) = lower(COALESCE(NULLIF(v_cargo_nome,''), 'Nao Definido')) LIMIT 1;
        END IF;

        IF v_setor_id IS NULL THEN
          INSERT INTO rh_setores (nome) VALUES (COALESCE(NULLIF(v_setor_nome,''), 'Nao Definido'))
          ON CONFLICT (nome) DO NOTHING;
          SELECT id INTO v_setor_id FROM rh_setores
            WHERE lower(nome) = lower(COALESCE(NULLIF(v_setor_nome,''), 'Nao Definido')) LIMIT 1;
        END IF;

        INSERT INTO rh_funcionarios (
          nome_completo, cpf, email_pessoal, telefone, salario_base,
          data_admissao, data_nascimento, status,
          empresa_id, filial_id, setor_id, cargo_id
        ) VALUES (
          v_nome, v_cpf, NULLIF(v_email, ''), NULLIF(v_telefone, ''),
          v_salario, COALESCE(v_data_admissao, CURRENT_DATE), v_data_nascimento,
          v_status, v_empresa_id, v_filial_id, v_setor_id, v_cargo_id
        );

        v_inseridos := v_inseridos + 1;
        v_func_id := (SELECT id FROM rh_funcionarios WHERE cpf = v_cpf);
      END IF;

      -- ── Registrar no mapa de identidade ────────────────────────────
      INSERT INTO etl_mapa_identidade (origem_sistema, tipo_entidade, id_origem, funcionario_id)
      VALUES ('convenia', 'funcionarios', v_convenia_id, v_func_id)
      ON CONFLICT (origem_sistema, tipo_entidade, id_origem) DO UPDATE
        SET funcionario_id = EXCLUDED.funcionario_id;

      -- ── Registrar na reconciliacao ─────────────────────────────────
      INSERT INTO reconc_correspondencia (funcionario_id, cpf, nome, convenia_id)
      VALUES (v_func_id, v_cpf, v_nome, v_convenia_id)
      ON CONFLICT (cpf) DO UPDATE
        SET convenia_id = EXCLUDED.convenia_id,
            nome = COALESCE(EXCLUDED.nome, reconc_correspondencia.nome);

      -- ── Marcar staging como processado ─────────────────────────────
      UPDATE stg_convenia_funcionarios
        SET status_processamento = 'processado',
            funcionario_id_core = v_func_id
        WHERE id = r.id;

    EXCEPTION WHEN OTHERS THEN
      UPDATE stg_convenia_funcionarios
        SET status_processamento = 'erro',
            erro_detalhe = SQLERRM
        WHERE id = r.id;
      v_erros := v_erros + 1;
    END;
  END LOOP;

  RETURN jsonb_build_object(
    'total', v_total,
    'inseridos', v_inseridos,
    'atualizados', v_atualizados,
    'erros', v_erros,
    'pulados', v_pulados
  );
END;
$function$;

-- Unique index para dedup no staging de funcionarios
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_conv_func_dedup
  ON public.stg_convenia_funcionarios (id_origem, hash_registro);

-- Unique index para dedup no staging de afastamentos
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_conv_afas_dedup
  ON public.stg_convenia_afastamentos (id_origem, hash_registro);

-- Unique index para dedup no staging de ferias
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_conv_fer_dedup
  ON public.stg_convenia_ferias (id_origem, hash_registro);
