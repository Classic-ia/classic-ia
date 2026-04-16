-- ════════════════════════════════════════════════════════════════════════════
-- ETL: stg_convenia_afastamentos → stg_convenia_documentos (normalizado)
--       + correção das views para consumir dados Convenia
--
-- FLUXO COMPLETO:
--
--   Convenia API /leaves
--     → Edge Function integ-convenia-proxy (sync_afastamentos)
--       → stg_convenia_afastamentos (payload_json cru da Convenia)
--         → etl_processar_convenia_afastamentos()  [ESTA FUNCAO]
--           → stg_convenia_documentos (payload_json normalizado)
--             → vw_absenteismo_real  (view corrigida)
--               → vw_absenteismo_recorrencia
--                 → motor_decisao_alertas()
--             → vw_absenteismo_por_setor (view corrigida)
--               → motor_decisao_alertas()
--
-- CHAVE DE CONCILIACAO:
--   Staging afastamentos: id_origem = ID do leave na API Convenia
--   Staging documentos: id_origem = 'conv_leave_' || ID Convenia (prefixado)
--   Funcionario: resolucao por CPF (via payload) ou convenia_id (via reconc)
--
-- DEDUP:
--   stg_convenia_afastamentos: UNIQUE(id_origem, hash_registro)
--   stg_convenia_documentos: UNIQUE(id_origem, hash_registro) [idx existente]
--
-- PAYLOAD NORMALIZADO (formato que vw_absenteismo_real espera):
--   {
--     "funcionario_id": "uuid-do-rh_funcionarios",
--     "data_inicio": "2026-01-15",
--     "data_fim": "2026-01-18",
--     "dias": 3,
--     "cid": null,
--     "tipo": "afastamento_convenia"
--   }
--
-- DEPENDENCIAS:
--   rh_funcionarios (resolve CPF)
--   reconc_correspondencia (resolve convenia_id → funcionario_id)
--   etl_mapa_identidade (registra match)
--   stg_convenia_documentos (destino normalizado)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.etl_processar_convenia_afastamentos(
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
  v_func_id UUID;
  v_convenia_emp_id TEXT;
  v_leave_id TEXT;
  v_data_inicio DATE;
  v_data_fim DATE;
  v_dias INTEGER;
  v_reason TEXT;
  v_hash TEXT;
  v_total INTEGER := 0;
  v_inseridos INTEGER := 0;
  v_atualizados INTEGER := 0;
  v_erros INTEGER := 0;
  v_sem_match INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, id_origem, payload_json
    FROM stg_convenia_afastamentos
    WHERE status_processamento = 'pendente'
      AND origem_sistema = 'convenia'
      AND (p_importacao_id IS NULL OR importacao_id = p_importacao_id)
    ORDER BY created_at
  LOOP
    v_total := v_total + 1;
    BEGIN
      -- ── Extrair campos do payload Convenia ─────────────────────────
      v_leave_id      := r.id_origem;
      v_data_inicio   := COALESCE(
                           (r.payload_json->>'start_date')::date,
                           (r.payload_json->>'inicio')::date
                         );
      v_data_fim      := COALESCE(
                           (r.payload_json->>'end_date')::date,
                           (r.payload_json->>'fim')::date
                         );
      v_dias          := COALESCE(
                           (r.payload_json->>'days')::integer,
                           (r.payload_json->>'dias')::integer,
                           -- Calcular se nao informado
                           CASE WHEN v_data_inicio IS NOT NULL AND v_data_fim IS NOT NULL
                             THEN (v_data_fim - v_data_inicio + 1)
                             ELSE NULL END
                         );
      v_reason        := COALESCE(
                           r.payload_json->>'reason',
                           r.payload_json->>'motivo',
                           'afastamento_convenia'
                         );

      -- ── Resolver funcionario ───────────────────────────────────────
      -- Tentativa 1: CPF do employee no payload
      v_cpf := regexp_replace(
        COALESCE(
          r.payload_json->'employee'->>'cpf',
          r.payload_json->>'cpf',
          ''
        ), '\D', '', 'g'
      );
      v_func_id := NULL;

      IF length(v_cpf) = 11 THEN
        SELECT id INTO v_func_id FROM rh_funcionarios WHERE cpf = v_cpf;
      END IF;

      -- Tentativa 2: convenia employee ID → reconc_correspondencia
      IF v_func_id IS NULL THEN
        v_convenia_emp_id := COALESCE(
          r.payload_json->'employee'->>'id',
          r.payload_json->>'employee_id',
          ''
        );
        IF v_convenia_emp_id <> '' THEN
          SELECT funcionario_id INTO v_func_id
          FROM reconc_correspondencia
          WHERE convenia_id = v_convenia_emp_id
          LIMIT 1;
        END IF;
      END IF;

      -- Tentativa 3: nome exato
      IF v_func_id IS NULL THEN
        DECLARE v_nome TEXT;
        BEGIN
          v_nome := COALESCE(
            r.payload_json->'employee'->>'name',
            r.payload_json->>'funcionario',
            ''
          );
          IF v_nome <> '' THEN
            SELECT id INTO v_func_id
            FROM rh_funcionarios
            WHERE lower(nome_completo) = lower(v_nome)
            LIMIT 1;
          END IF;
        END;
      END IF;

      -- Sem match: registra erro e pula
      IF v_func_id IS NULL THEN
        UPDATE stg_convenia_afastamentos
          SET status_processamento = 'erro',
              erro_detalhe = 'Funcionario nao encontrado. CPF=' || COALESCE(v_cpf,'?')
                || ' ConveniaEmpID=' || COALESCE(v_convenia_emp_id,'?')
          WHERE id = r.id;
        v_sem_match := v_sem_match + 1;
        CONTINUE;
      END IF;

      -- Validar datas minimas
      IF v_data_inicio IS NULL THEN
        UPDATE stg_convenia_afastamentos
          SET status_processamento = 'erro',
              erro_detalhe = 'data_inicio ausente'
          WHERE id = r.id;
        v_erros := v_erros + 1;
        CONTINUE;
      END IF;

      -- Se dias nao veio, calcular como 1
      IF v_dias IS NULL OR v_dias < 1 THEN
        v_dias := 1;
      END IF;

      -- Se data_fim nao veio, calcular a partir de dias
      IF v_data_fim IS NULL THEN
        v_data_fim := v_data_inicio + (v_dias - 1);
      END IF;

      -- ── Construir payload normalizado ──────────────────────────────
      -- Formato identico ao que vw_absenteismo_real espera
      v_hash := encode(sha256(
        ('conv_leave_' || v_leave_id || '|' || v_func_id || '|' || v_data_inicio)::bytea
      ), 'hex');

      INSERT INTO stg_convenia_documentos (
        id_origem, origem_sistema, payload_json, hash_registro,
        importacao_id, status_processamento
      ) VALUES (
        'conv_leave_' || v_leave_id,
        'convenia_leave',
        jsonb_build_object(
          'funcionario_id', v_func_id,
          'data_inicio', v_data_inicio,
          'data_fim', v_data_fim,
          'dias', v_dias,
          'cid', NULL,
          'tipo', v_reason,
          'convenia_leave_id', v_leave_id,
          'origem', 'convenia_api'
        ),
        v_hash,
        p_importacao_id,
        'processado'
      )
      ON CONFLICT (id_origem, hash_registro) DO UPDATE
        SET payload_json = EXCLUDED.payload_json,
            importacao_id = EXCLUDED.importacao_id,
            status_processamento = 'processado';

      -- ── Marcar staging original como processado ────────────────────
      UPDATE stg_convenia_afastamentos
        SET status_processamento = 'processado'
        WHERE id = r.id;

      v_inseridos := v_inseridos + 1;

    EXCEPTION WHEN OTHERS THEN
      UPDATE stg_convenia_afastamentos
        SET status_processamento = 'erro',
            erro_detalhe = SQLERRM
        WHERE id = r.id;
      v_erros := v_erros + 1;
    END;
  END LOOP;

  RETURN jsonb_build_object(
    'total', v_total,
    'inseridos', v_inseridos,
    'erros', v_erros,
    'sem_match_funcionario', v_sem_match
  );
END;
$function$;


-- ════════════════════════════════════════════════════════════════════════════
-- CORRIGIR VIEWS: aceitar dados da Convenia alem de manuais
--
-- ANTES:  WHERE d.origem_sistema = 'manual'
-- DEPOIS: WHERE d.origem_sistema IN ('manual', 'convenia_leave')
--
-- IMPACTO:
--   vw_absenteismo_real → vw_absenteismo_recorrencia → motor_decisao_alertas()
--   vw_absenteismo_por_setor → motor_decisao_alertas()
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW public.vw_absenteismo_real AS
SELECT f.id AS funcionario_id, f.nome_completo, c.nome AS cargo, s.nome AS setor,
  f.salario_base, round(f.salario_base / 22, 2) AS salario_dia,
  (d.payload_json ->> 'data_inicio')::date AS data_inicio,
  (d.payload_json ->> 'data_fim')::date AS data_fim,
  (d.payload_json ->> 'dias')::integer AS dias,
  d.payload_json ->> 'cid' AS cid,
  d.payload_json ->> 'tipo' AS tipo_original,
  CASE WHEN (d.payload_json ->> 'dias')::integer <= 3 THEN 'curto'
    WHEN (d.payload_json ->> 'dias')::integer <= 14 THEN 'medio'
    ELSE 'longo_inss' END AS faixa,
  CASE WHEN (d.payload_json ->> 'dias')::integer <= 14 THEN (d.payload_json ->> 'dias')::integer ELSE 14 END AS dias_empresa,
  CASE WHEN (d.payload_json ->> 'dias')::integer > 14 THEN (d.payload_json ->> 'dias')::integer - 14 ELSE 0 END AS dias_inss,
  round(LEAST((d.payload_json ->> 'dias')::integer, 14)::numeric * (f.salario_base / 22) *
    CASE WHEN (d.payload_json ->> 'dias')::integer <= 3 THEN 1.5 ELSE 1.3 END, 2) AS custo_empresa_real,
  round(LEAST((d.payload_json ->> 'dias')::integer, 14)::numeric * (f.salario_base / 22), 2) AS custo_empresa_bruto
FROM stg_convenia_documentos d
JOIN rh_funcionarios f ON f.id = (d.payload_json ->> 'funcionario_id')::uuid
JOIN rh_cargos c ON c.id = f.cargo_id
JOIN rh_setores s ON s.id = f.setor_id
WHERE d.origem_sistema IN ('manual', 'convenia_leave');


CREATE OR REPLACE VIEW public.vw_absenteismo_por_setor AS
SELECT s.nome AS setor,
  count(DISTINCT f.id) AS funcionarios_afetados,
  (SELECT count(*) FROM rh_funcionarios ff WHERE ff.setor_id = s.id AND ff.status IN ('ativo','ferias')) AS total_setor,
  count(*) AS atestados,
  sum((d.payload_json ->> 'dias')::integer) AS total_dias,
  round(sum((d.payload_json ->> 'dias')::integer)::numeric * avg(f.salario_base) / 22, 2) AS custo_estimado
FROM stg_convenia_documentos d
JOIN rh_funcionarios f ON f.id = (d.payload_json ->> 'funcionario_id')::uuid
JOIN rh_setores s ON s.id = f.setor_id
WHERE d.origem_sistema IN ('manual', 'convenia_leave')
GROUP BY s.id, s.nome
ORDER BY sum((d.payload_json ->> 'dias')::integer) DESC;
