-- ============================================================================
-- UNIFICACAO_SST.sql
-- Sprint 1 (P0) do PLANO_CORRECOES_SIGA.md — correcao do bug sistemico
-- read/write divergente dos modulos SST (F-43 ASOs, F-44 Treinamentos,
-- F-45 Acidentes).
--
-- DECISAO: a familia sst_* e a CANONICA (ja consumida por
-- sst_dashboard_completo, ficha 360 e monitor eSocial). As tabelas
-- rh_asos / rh_aso / rh_treinamentos / rh_treinamentos_tipos /
-- rh_acidentes / rh_asos_config viram LEGADO: dados migrados, escrita
-- revogada, leitura mantida durante a transicao.
--
-- O QUE ESTE SCRIPT FAZ:
--   1. Amplia sst_acidente (taxonomia real de 5 tipos, status da tela,
--      colunas de investigacao 5 Porques / acoes corretivas / CAT)
--   2. Amplia sst_aso (exames_complementares) e sst_tipo_treinamento
--      (obrigatorio)
--   3. Cria sst_aso_config (versao canonica da drift rh_asos_config)
--      com RLS RBAC (padrao cipa_acoes)
--   4. Migra dados das tabelas legadas (defensivo: so se existirem;
--      suporta os DOIS shapes conhecidos de rh_treinamentos/rh_acidentes
--      — o versionado no repo e o drift de producao)
--   5. Congela as legadas (REVOKE INSERT/UPDATE/DELETE + comentario)
--
-- Idempotente. Executavel em producao (com drift) e no rebuild local
-- (sem as tabelas drift). Blocos de migracao usam SQL dinamico para nao
-- falhar quando a tabela/coluna de origem nao existe no ambiente.
--
-- Depende de: MODULO_SST_v2.sql (sst_*), FUNDACAO_BANCO_v2.sql
-- (rh_funcionarios, rh_usuarios).
-- Frontend correspondente: rh/asos.html, rh/treinamentos.html,
-- rh/acidentes.html (corrigidos no mesmo commit para ler/gravar sst_*).
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 0. HELPERS TEMPORARIOS
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION pg_temp.tbl_exists(p_table text) RETURNS boolean
LANGUAGE sql AS $$ SELECT to_regclass('public.' || p_table) IS NOT NULL $$;

CREATE OR REPLACE FUNCTION pg_temp.col_exists(p_table text, p_col text) RETURNS boolean
LANGUAGE sql AS $$
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = p_table AND column_name = p_col
  )
$$;

-- ════════════════════════════════════════════════════════════════════════
-- 1. AMPLIAR sst_acidente
--    Taxonomia real da operacao (5 tipos da tela) + status da tela +
--    colunas de investigacao. Ordem: dropar CHECKs -> mapear valores
--    antigos -> recriar CHECKs.
-- ════════════════════════════════════════════════════════════════════════

-- 1a. Dropar CHECKs antigos de tipo_acidente e status (nome convencional
--     + varredura defensiva por definicao, caso o nome difira em prod)
ALTER TABLE sst_acidente DROP CONSTRAINT IF EXISTS sst_acidente_tipo_acidente_check;
ALTER TABLE sst_acidente DROP CONSTRAINT IF EXISTS sst_acidente_status_check;
ALTER TABLE sst_acidente DROP CONSTRAINT IF EXISTS chk_sst_acidente_tipo;
ALTER TABLE sst_acidente DROP CONSTRAINT IF EXISTS chk_sst_acidente_status;

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = 'public.sst_acidente'::regclass
      AND contype = 'c'
      AND (pg_get_constraintdef(oid) ILIKE '%tipo_acidente%'
        OR (pg_get_constraintdef(oid) ILIKE '%status%'
            AND pg_get_constraintdef(oid) NOT ILIKE '%data_validade%'))
  LOOP
    EXECUTE format('ALTER TABLE sst_acidente DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

-- 1b. Mapear valores antigos para a taxonomia nova
UPDATE sst_acidente
   SET tipo_acidente = CASE WHEN COALESCE(dias_afastamento, 0) > 0
                            THEN 'com_afastamento' ELSE 'sem_afastamento' END
 WHERE tipo_acidente = 'tipico';

UPDATE sst_acidente SET status = 'registrado'   WHERE status = 'aberto';
UPDATE sst_acidente SET status = 'investigando' WHERE status = 'investigacao';
UPDATE sst_acidente SET status = 'concluido'    WHERE status = 'encerrado';

-- 1c. Recriar CHECKs com a taxonomia unificada
ALTER TABLE sst_acidente ADD CONSTRAINT chk_sst_acidente_tipo CHECK (
  tipo_acidente IN ('com_afastamento','sem_afastamento','trajeto','incidente','doenca_ocupacional')
);
ALTER TABLE sst_acidente ADD CONSTRAINT chk_sst_acidente_status CHECK (
  status IN ('registrado','investigando','concluido')
);

-- 1d. Colunas de investigacao/CAT usadas pela tela (rh_acidentes prod as
--     tinha; a canonica nao). testemunha_1/2 e acao_imediata permanecem
--     por compatibilidade, deprecadas em favor de testemunhas/medidas_imediatas.
ALTER TABLE sst_acidente ADD COLUMN IF NOT EXISTS testemunhas            TEXT;
ALTER TABLE sst_acidente ADD COLUMN IF NOT EXISTS medidas_imediatas      TEXT;
ALTER TABLE sst_acidente ADD COLUMN IF NOT EXISTS investigacao_5porques  JSONB;
ALTER TABLE sst_acidente ADD COLUMN IF NOT EXISTS acoes_corretivas       JSONB DEFAULT '[]'::jsonb;
ALTER TABLE sst_acidente ADD COLUMN IF NOT EXISTS licoes_aprendidas      TEXT;

-- Copiar dados dos campos antigos para os novos onde os novos estao vazios
UPDATE sst_acidente
   SET testemunhas = NULLIF(TRIM(BOTH ', ' FROM
         COALESCE(testemunha_1,'') || CASE WHEN testemunha_2 IS NOT NULL THEN ', ' || testemunha_2 ELSE '' END), '')
 WHERE testemunhas IS NULL AND (testemunha_1 IS NOT NULL OR testemunha_2 IS NOT NULL);

UPDATE sst_acidente SET medidas_imediatas = acao_imediata
 WHERE medidas_imediatas IS NULL AND acao_imediata IS NOT NULL;

-- 1e. A tela permite descricao vazia (validacao exige apenas tipo,
--     colaborador, data/hora e gravidade)
ALTER TABLE sst_acidente ALTER COLUMN descricao DROP NOT NULL;

COMMENT ON COLUMN sst_acidente.testemunha_1 IS 'DEPRECADO — usar testemunhas (texto livre, CSV)';
COMMENT ON COLUMN sst_acidente.testemunha_2 IS 'DEPRECADO — usar testemunhas (texto livre, CSV)';
COMMENT ON COLUMN sst_acidente.acao_imediata IS 'DEPRECADO — usar medidas_imediatas';

-- ════════════════════════════════════════════════════════════════════════
-- 2. AMPLIAR sst_aso E sst_tipo_treinamento
-- ════════════════════════════════════════════════════════════════════════

-- Exames complementares em coluna propria (antes: concatenados em observacoes)
ALTER TABLE sst_aso ADD COLUMN IF NOT EXISTS exames_complementares TEXT[];

-- Flag global de obrigatoriedade do tipo (a tela de tipos usa; a matriz
-- por cargo continua em sst_cargo_treinamento)
ALTER TABLE sst_tipo_treinamento ADD COLUMN IF NOT EXISTS obrigatorio BOOLEAN NOT NULL DEFAULT TRUE;

-- ════════════════════════════════════════════════════════════════════════
-- 3. sst_aso_config — versao canonica da drift rh_asos_config
--    (risco/periodicidade NR-07 por cargo + exames padrao por tipo)
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.sst_aso_config (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cargo                 TEXT NOT NULL,
  cbo                   TEXT,
  risco                 VARCHAR(10) NOT NULL DEFAULT 'baixo'
                        CHECK (risco IN ('baixo','alto')),
  exames_admissional    TEXT,
  exames_periodico      TEXT,
  exames_retorno        TEXT,
  exames_mudanca_funcao TEXT,
  exames_demissional    TEXT,
  ativo                 BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by            UUID REFERENCES auth.users(id),
  UNIQUE (cargo)
);

COMMENT ON TABLE public.sst_aso_config IS
  'Nivel de risco NR-07 e exames padrao por cargo. Substitui a tabela drift rh_asos_config.';

-- RLS RBAC (padrao cipa_acoes: leitura autenticado, escrita por perfil)
ALTER TABLE public.sst_aso_config ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_sst_aso_config_read   ON public.sst_aso_config;
DROP POLICY IF EXISTS p_sst_aso_config_write  ON public.sst_aso_config;
DROP POLICY IF EXISTS p_sst_aso_config_update ON public.sst_aso_config;
DROP POLICY IF EXISTS p_sst_aso_config_delete ON public.sst_aso_config;

CREATE POLICY p_sst_aso_config_read ON public.sst_aso_config
  FOR SELECT TO authenticated USING (true);

CREATE POLICY p_sst_aso_config_write ON public.sst_aso_config
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh','sst')
       AND ativo = true
  ));

CREATE POLICY p_sst_aso_config_update ON public.sst_aso_config
  FOR UPDATE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh','sst')
       AND ativo = true
  ));

CREATE POLICY p_sst_aso_config_delete ON public.sst_aso_config
  FOR DELETE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh','sst')
       AND ativo = true
  ));

GRANT SELECT, INSERT, UPDATE, DELETE ON public.sst_aso_config TO authenticated;

-- Copiar config da drift, se existir
DO $$
BEGIN
  IF pg_temp.tbl_exists('rh_asos_config') THEN
    EXECUTE $sql$
      INSERT INTO sst_aso_config (cargo, cbo, risco,
        exames_admissional, exames_periodico, exames_retorno,
        exames_mudanca_funcao, exames_demissional)
      SELECT c.cargo, c.cbo, COALESCE(c.risco,'baixo'),
        c.exames_admissional, c.exames_periodico, c.exames_retorno,
        c.exames_mudanca_funcao, c.exames_demissional
      FROM rh_asos_config c
      WHERE NOT EXISTS (SELECT 1 FROM sst_aso_config s WHERE s.cargo = c.cargo)
    $sql$;
    RAISE NOTICE 'sst_aso_config: config copiada de rh_asos_config';
  END IF;
EXCEPTION WHEN undefined_column THEN
  -- rh_asos_config drift sem as colunas exames_*: copiar so cargo/cbo/risco
  EXECUTE $sql$
    INSERT INTO sst_aso_config (cargo, cbo, risco)
    SELECT c.cargo, c.cbo, COALESCE(c.risco,'baixo')
    FROM rh_asos_config c
    WHERE NOT EXISTS (SELECT 1 FROM sst_aso_config s WHERE s.cargo = c.cargo)
  $sql$;
  RAISE NOTICE 'sst_aso_config: copiada (shape reduzido, sem exames_*)';
END $$;

-- ════════════════════════════════════════════════════════════════════════
-- 4. MIGRACAO DE DADOS DAS TABELAS LEGADAS
--    Todas as insercoes: dedup por (funcionario, data, tipo) + JOIN em
--    rh_funcionarios para garantir a FK. Registros sem campos NOT NULL
--    obrigatorios sao PULADOS (contados em NOTICE) — nao fabricamos
--    resultado de exame nem gravidade de acidente.
-- ════════════════════════════════════════════════════════════════════════

-- 4a. rh_asos (plural, shape versionado: colaborador_id/tipo_aso/...)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_asos') AND pg_temp.col_exists('rh_asos','tipo_aso') THEN
    EXECUTE $sql$
      INSERT INTO sst_aso (funcionario_id, tipo_exame, data_exame, data_validade,
                           resultado, restricoes, medico_responsavel, crm, observacoes)
      SELECT a.colaborador_id,
             CASE a.tipo_aso WHEN 'retorno' THEN 'retorno_trabalho' ELSE a.tipo_aso END,
             a.data_realizacao,
             GREATEST(COALESCE(a.data_vencimento, (a.data_realizacao + INTERVAL '12 months')::date),
                      a.data_realizacao),
             a.resultado,
             a.riscos_avaliados,
             COALESCE(NULLIF(TRIM(a.medico_nome), ''), '(nao informado)'),
             a.medico_crm,
             a.observacoes
      FROM rh_asos a
      JOIN rh_funcionarios f ON f.id = a.colaborador_id
      WHERE a.resultado IS NOT NULL
        AND COALESCE(a.status, 'valido') <> 'cancelado'
        AND NOT EXISTS (
          SELECT 1 FROM sst_aso s
          WHERE s.funcionario_id = a.colaborador_id
            AND s.data_exame = a.data_realizacao
            AND s.tipo_exame = CASE a.tipo_aso WHEN 'retorno' THEN 'retorno_trabalho' ELSE a.tipo_aso END
        )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_aso: % registros migrados de rh_asos', v_count;
  END IF;
END $$;

-- 4b. rh_aso (singular, shape ja alinhado a sst_aso)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_aso') AND pg_temp.col_exists('rh_aso','tipo_exame') THEN
    EXECUTE $sql$
      INSERT INTO sst_aso (funcionario_id, tipo_exame, data_exame, data_validade,
                           resultado, medico_responsavel, crm, observacoes)
      SELECT a.funcionario_id, a.tipo_exame, a.data_exame,
             GREATEST(a.data_validade, a.data_exame),
             a.resultado,
             COALESCE(NULLIF(TRIM(a.medico_responsavel), ''), '(nao informado)'),
             a.crm, a.observacoes
      FROM rh_aso a
      JOIN rh_funcionarios f ON f.id = a.funcionario_id
      WHERE NOT EXISTS (
        SELECT 1 FROM sst_aso s
        WHERE s.funcionario_id = a.funcionario_id
          AND s.data_exame = a.data_exame
          AND s.tipo_exame = a.tipo_exame
      )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_aso: % registros migrados de rh_aso', v_count;
  END IF;
END $$;

-- 4c. rh_treinamentos_tipos (drift) -> sst_tipo_treinamento
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_treinamentos_tipos') THEN
    EXECUTE $sql$
      INSERT INTO sst_tipo_treinamento (nome, norma_regulamentadora, validade_meses,
                                        obrigatorio, descricao, ativo)
      SELECT t.nome, t.nr, t.validade_meses,
             COALESCE(t.obrigatorio, true), t.descricao, COALESCE(t.ativo, true)
      FROM rh_treinamentos_tipos t
      WHERE NOT EXISTS (
        SELECT 1 FROM sst_tipo_treinamento s
        WHERE s.nome = t.nome
          AND s.norma_regulamentadora IS NOT DISTINCT FROM t.nr
      )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_tipo_treinamento: % tipos migrados de rh_treinamentos_tipos', v_count;
  END IF;
END $$;

-- 4d. rh_treinamentos, shape PROD (normalizado: colaborador_id + tipo_id FK)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_treinamentos')
     AND pg_temp.col_exists('rh_treinamentos','tipo_id')
     AND pg_temp.tbl_exists('rh_treinamentos_tipos') THEN
    EXECUTE $sql$
      INSERT INTO sst_treinamento (funcionario_id, tipo_treinamento_id, data_realizacao,
                                   data_validade, carga_horaria, instrutor,
                                   certificado_numero, observacoes)
      SELECT r.colaborador_id, s.id, r.data_realizacao,
             CASE WHEN r.data_vencimento >= r.data_realizacao THEN r.data_vencimento ELSE NULL END,
             r.carga_horaria, r.instituicao, r.numero_certificado, r.observacoes
      FROM rh_treinamentos r
      JOIN rh_treinamentos_tipos rt ON rt.id = r.tipo_id
      JOIN sst_tipo_treinamento s
        ON s.nome = rt.nome AND s.norma_regulamentadora IS NOT DISTINCT FROM rt.nr
      JOIN rh_funcionarios f ON f.id = r.colaborador_id
      WHERE NOT EXISTS (
        SELECT 1 FROM sst_treinamento t
        WHERE t.funcionario_id = r.colaborador_id
          AND t.tipo_treinamento_id = s.id
          AND t.data_realizacao = r.data_realizacao
      )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_treinamento: % registros migrados de rh_treinamentos (shape prod)', v_count;
  END IF;
END $$;

-- 4e. rh_treinamentos, shape VERSIONADO (denormalizado: treinamento TEXT)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_treinamentos')
     AND pg_temp.col_exists('rh_treinamentos','treinamento') THEN
    -- criar tipos que ainda nao existam
    EXECUTE $sql$
      INSERT INTO sst_tipo_treinamento (nome, norma_regulamentadora)
      SELECT DISTINCT r.treinamento, r.nr_referencia
      FROM rh_treinamentos r
      WHERE r.treinamento IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM sst_tipo_treinamento s
          WHERE s.nome = r.treinamento
            AND s.norma_regulamentadora IS NOT DISTINCT FROM r.nr_referencia
        )
    $sql$;
    EXECUTE $sql$
      INSERT INTO sst_treinamento (funcionario_id, tipo_treinamento_id, data_realizacao,
                                   data_validade, carga_horaria, instrutor, observacoes)
      SELECT r.colaborador_id, s.id, r.data_realizacao,
             CASE WHEN r.data_vencimento >= r.data_realizacao THEN r.data_vencimento ELSE NULL END,
             r.carga_horaria_horas, r.instrutor, NULL
      FROM rh_treinamentos r
      JOIN sst_tipo_treinamento s
        ON s.nome = r.treinamento AND s.norma_regulamentadora IS NOT DISTINCT FROM r.nr_referencia
      JOIN rh_funcionarios f ON f.id = r.colaborador_id
      WHERE COALESCE(r.status, 'valido') <> 'cancelado'
        AND NOT EXISTS (
          SELECT 1 FROM sst_treinamento t
          WHERE t.funcionario_id = r.colaborador_id
            AND t.tipo_treinamento_id = s.id
            AND t.data_realizacao = r.data_realizacao
        )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_treinamento: % registros migrados de rh_treinamentos (shape versionado)', v_count;
  END IF;
END $$;

-- 4e-bis. rh_treinamentos, shape schema_rh_sst_completo
--         (funcionario_id + tipo_treinamento TEXT)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_treinamentos')
     AND pg_temp.col_exists('rh_treinamentos','tipo_treinamento') THEN
    EXECUTE $sql$
      INSERT INTO sst_tipo_treinamento (nome)
      SELECT DISTINCT r.tipo_treinamento
      FROM rh_treinamentos r
      WHERE r.tipo_treinamento IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM sst_tipo_treinamento s
          WHERE s.nome = r.tipo_treinamento AND s.norma_regulamentadora IS NULL
        )
    $sql$;
    EXECUTE $sql$
      INSERT INTO sst_treinamento (funcionario_id, tipo_treinamento_id, data_realizacao,
                                   data_validade, carga_horaria, instrutor, observacoes)
      SELECT r.funcionario_id, s.id, r.data_realizacao,
             CASE WHEN r.data_validade >= r.data_realizacao THEN r.data_validade ELSE NULL END,
             r.carga_horaria, r.instrutor, r.observacoes
      FROM rh_treinamentos r
      JOIN sst_tipo_treinamento s
        ON s.nome = r.tipo_treinamento AND s.norma_regulamentadora IS NULL
      JOIN rh_funcionarios f ON f.id = r.funcionario_id
      WHERE NOT EXISTS (
        SELECT 1 FROM sst_treinamento t
        WHERE t.funcionario_id = r.funcionario_id
          AND t.tipo_treinamento_id = s.id
          AND t.data_realizacao = r.data_realizacao
      )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_treinamento: % registros migrados de rh_treinamentos (shape rh_sst_completo)', v_count;
  END IF;
END $$;

-- 4f. rh_acidentes, shape PROD (rico: tipo 5 valores, data_hora, JSONB)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_acidentes')
     AND pg_temp.col_exists('rh_acidentes','data_hora') THEN
    EXECUTE $sql$
      INSERT INTO sst_acidente (funcionario_id, data_ocorrencia, hora_ocorrencia,
        tipo_acidente, gravidade, parte_corpo, agente_causador, descricao,
        local_ocorrencia, cat_emitida, cat_numero, dias_afastamento,
        testemunhas, medidas_imediatas, investigacao_5porques, acoes_corretivas,
        licoes_aprendidas, causa_raiz, status)
      SELECT a.colaborador_id,
             a.data_hora::date, a.data_hora::time,
             a.tipo,
             a.gravidade, a.parte_corpo, a.agente_causador, a.descricao,
             a.local_setor, COALESCE(a.cat_emitida, false), a.numero_cat,
             COALESCE(a.dias_afastados, 0),
             a.testemunhas, a.medidas_imediatas, a.investigacao_5porques,
             COALESCE(a.acoes_corretivas, '[]'::jsonb),
             a.licoes_aprendidas, a.causa_raiz,
             COALESCE(a.status, 'registrado')
      FROM rh_acidentes a
      JOIN rh_funcionarios f ON f.id = a.colaborador_id
      WHERE a.gravidade IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM sst_acidente s
          WHERE s.funcionario_id = a.colaborador_id
            AND s.data_ocorrencia = a.data_hora::date
            AND s.tipo_acidente = a.tipo
        )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_acidente: % registros migrados de rh_acidentes (shape prod)', v_count;
  END IF;
END $$;

-- 4g. rh_acidentes, shape VERSIONADO (enxuto: funcionario_id, tipo_acidente 3 valores)
DO $$
DECLARE v_count INT;
BEGIN
  IF pg_temp.tbl_exists('rh_acidentes')
     AND pg_temp.col_exists('rh_acidentes','data_ocorrencia')
     AND NOT pg_temp.col_exists('rh_acidentes','data_hora') THEN
    EXECUTE $sql$
      INSERT INTO sst_acidente (funcionario_id, data_ocorrencia, hora_ocorrencia,
        tipo_acidente, gravidade, parte_corpo, descricao, local_ocorrencia,
        cat_emitida, dias_afastamento, medidas_imediatas, status)
      SELECT a.funcionario_id, a.data_ocorrencia, a.hora_ocorrencia,
             CASE a.tipo_acidente
               WHEN 'tipico' THEN CASE WHEN COALESCE(a.dias_afastamento,0) > 0
                                       THEN 'com_afastamento' ELSE 'sem_afastamento' END
               ELSE a.tipo_acidente
             END,
             a.gravidade, a.parte_corpo, a.descricao, a.setor,
             COALESCE(a.cat_emitida, false), COALESCE(a.dias_afastamento, 0),
             a.acao_corretiva, 'registrado'
      FROM rh_acidentes a
      JOIN rh_funcionarios f ON f.id = a.funcionario_id
      WHERE a.gravidade IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM sst_acidente s
          WHERE s.funcionario_id = a.funcionario_id
            AND s.data_ocorrencia = a.data_ocorrencia
        )
    $sql$;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'sst_acidente: % registros migrados de rh_acidentes (shape versionado)', v_count;
  END IF;
END $$;

-- ════════════════════════════════════════════════════════════════════════
-- 5. CONGELAR AS TABELAS LEGADAS
--    Leitura mantida (transicao); escrita revogada; drop somente apos
--    homologacao do Sprint 1 (nova migration dedicada).
-- ════════════════════════════════════════════════════════════════════════

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_asos', 'rh_aso', 'rh_asos_config',
    'rh_treinamentos', 'rh_treinamentos_tipos',
    'rh_acidentes'
  ] LOOP
    IF pg_temp.tbl_exists(t) THEN
      EXECUTE format('REVOKE INSERT, UPDATE, DELETE ON public.%I FROM authenticated', t);
      EXECUTE format('REVOKE INSERT, UPDATE, DELETE ON public.%I FROM anon', t);
      EXECUTE format(
        'COMMENT ON TABLE public.%I IS %L', t,
        'LEGADO (UNIFICACAO_SST 2026-07): dados migrados para a familia sst_*. '
        || 'Escrita revogada; leitura mantida durante a transicao. '
        || 'NAO gravar aqui — usar sst_aso / sst_treinamento / sst_acidente / sst_aso_config.'
      );
      RAISE NOTICE 'Legado congelado: %', t;
    END IF;
  END LOOP;
END $$;

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

-- V1: CHECKs novos de sst_acidente
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'public.sst_acidente'::regclass AND contype = 'c'
ORDER BY conname;

-- V2: colunas novas presentes
SELECT column_name FROM information_schema.columns
WHERE table_name = 'sst_acidente'
  AND column_name IN ('testemunhas','medidas_imediatas','investigacao_5porques',
                      'acoes_corretivas','licoes_aprendidas')
ORDER BY column_name;

-- V3: contagens pos-migracao (comparar com as legadas)
SELECT 'sst_aso' AS tabela, COUNT(*) FROM sst_aso
UNION ALL SELECT 'sst_treinamento', COUNT(*) FROM sst_treinamento
UNION ALL SELECT 'sst_acidente', COUNT(*) FROM sst_acidente
UNION ALL SELECT 'sst_tipo_treinamento', COUNT(*) FROM sst_tipo_treinamento
UNION ALL SELECT 'sst_aso_config', COUNT(*) FROM sst_aso_config;

-- V4: nenhum valor fora dos CHECKs novos
SELECT tipo_acidente, status, COUNT(*)
FROM sst_acidente GROUP BY 1, 2 ORDER BY 1, 2;
