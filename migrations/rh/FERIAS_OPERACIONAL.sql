-- ============================================================================
-- FERIAS_OPERACIONAL.sql
-- Sprint 2 (P1) do PLANO_CORRECOES_SIGA.md — item 3.2 (F-42 D-01/D-02/D-03/D-04)
--
-- CONTEXTO:
--   O modulo de ferias era read-only: ferias.html so exibia o dashboard
--   (ferias_dashboard) e NADA escrevia em rh_ferias. O status 'vencida' era
--   definido externamente (importacao manual), sem job versionado (D-03);
--   rh_ferias.status nao tinha CHECK (D-02); abono/dias vendidos existiam
--   na tabela mas sem UI (D-04); nao havia como programar ferias (D-01).
--
-- O QUE ESTE SCRIPT FAZ (script 27 da ORDEM_REBUILD):
--   1. Normaliza status legados e adiciona CHECK
--      (vencida|pendente|programada|em_gozo|gozada)
--   2. FK rh_ferias.funcionario_id -> rh_funcionarios NOT VALID
--      (integridade p/ dados novos + habilita embedding PostgREST;
--      NOT VALID nao trava em eventuais orfaos de producao)
--   3. Trigger updated_at (rh_set_updated_at resiliente do script 24)
--   4. ferias_atualizar_vencidas() — job de transicao automatica:
--      pendente -> vencida   (periodo concessivo estourado: aquisitivo_fim
--                             + 12 meses < hoje; CLT art. 134/137)
--      programada -> em_gozo (data_inicio alcancada)
--      em_gozo -> gozada     (data_fim ultrapassada)
--      Agendar diario via pg_cron ou n8n; tambem acionavel pelo botao
--      "Atualizar status" da tela (admin/rh).
--   5. ferias_programar(...) — RPC de programacao (guard administrador/rh):
--      valida dias vs. direito, abono ate 1/3 (CLT art. 143), calcula
--      data_fim/data_retorno e marca 'programada'.
--
-- FORA DE ESCOPO (deliberado):
--   ETL stg_convenia_ferias -> rh_ferias (item 3 do plano 3.2) fica ADIADO:
--   o shape do payload_json da Convenia nao esta versionado no repo e o
--   staging local esta vazio — mapear as colunas exige uma amostra real de
--   producao. Registrado como pendencia no PLANO_CORRECOES_SIGA 3.2.
--
-- Idempotente. Frontend correspondente: rh/ferias.html (secao "Programacao
-- de Ferias" para admin/rh, no mesmo commit).
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 1. Normalizacao de status legados + CHECK
-- ════════════════════════════════════════════════════════════════════════

UPDATE rh_ferias SET status = CASE
    WHEN status IN ('agendada','marcada')            THEN 'programada'
    WHEN status IN ('concluida','concluido','usada') THEN 'gozada'
    WHEN status IN ('em_andamento','gozando')        THEN 'em_gozo'
    ELSE 'pendente'
  END
WHERE status NOT IN ('vencida','pendente','programada','em_gozo','gozada');

ALTER TABLE rh_ferias DROP CONSTRAINT IF EXISTS chk_rh_ferias_status;
ALTER TABLE rh_ferias ADD CONSTRAINT chk_rh_ferias_status
  CHECK (status IN ('vencida','pendente','programada','em_gozo','gozada'));

-- ════════════════════════════════════════════════════════════════════════
-- 2. FK NOT VALID (integridade prospectiva + embedding PostgREST)
-- ════════════════════════════════════════════════════════════════════════

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_rh_ferias_funcionario' AND conrelid = 'rh_ferias'::regclass
  ) THEN
    ALTER TABLE rh_ferias ADD CONSTRAINT fk_rh_ferias_funcionario
      FOREIGN KEY (funcionario_id) REFERENCES rh_funcionarios(id) NOT VALID;
  END IF;
END $$;

-- ════════════════════════════════════════════════════════════════════════
-- 3. Trigger updated_at
-- ════════════════════════════════════════════════════════════════════════

DROP TRIGGER IF EXISTS trg_rh_ferias_updated ON rh_ferias;
CREATE TRIGGER trg_rh_ferias_updated
  BEFORE UPDATE ON rh_ferias
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

-- ════════════════════════════════════════════════════════════════════════
-- 4. ferias_atualizar_vencidas() — job de transicao automatica
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.ferias_atualizar_vencidas()
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_vencidas  INT;
  v_iniciadas INT;
  v_gozadas   INT;
BEGIN
  -- pendente -> vencida: periodo concessivo (12 meses apos o aquisitivo)
  -- estourado sem gozo programado/realizado (risco de dobra, CLT art. 137)
  UPDATE rh_ferias
     SET status = 'vencida'
   WHERE status = 'pendente'
     AND periodo_aquisitivo_fim + INTERVAL '12 months' < CURRENT_DATE;
  GET DIAGNOSTICS v_vencidas = ROW_COUNT;

  -- programada -> em_gozo: gozo iniciado
  UPDATE rh_ferias
     SET status = 'em_gozo'
   WHERE status = 'programada'
     AND data_inicio IS NOT NULL AND data_inicio <= CURRENT_DATE
     AND (data_fim IS NULL OR data_fim >= CURRENT_DATE);
  GET DIAGNOSTICS v_iniciadas = ROW_COUNT;

  -- em_gozo/programada -> gozada: gozo encerrado
  UPDATE rh_ferias
     SET status = 'gozada',
         dias_gozados = GREATEST(COALESCE(dias_gozados,0),
                                 (data_fim - data_inicio) + 1)
   WHERE status IN ('em_gozo','programada')
     AND data_fim IS NOT NULL AND data_fim < CURRENT_DATE;
  GET DIAGNOSTICS v_gozadas = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok', true,
    'promovidas_vencida', v_vencidas,
    'iniciadas_gozo', v_iniciadas,
    'concluidas_gozada', v_gozadas,
    'executado_em', now()
  );
END; $fn$;

-- anon NAO pode acionar o job; authenticated (tela) e service_role (n8n/cron) sim
REVOKE ALL ON FUNCTION public.ferias_atualizar_vencidas() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ferias_atualizar_vencidas() TO authenticated, service_role;

COMMENT ON FUNCTION public.ferias_atualizar_vencidas() IS
  'Job diario de transicao de status de ferias (pendente->vencida, programada->em_gozo, em_gozo->gozada). Agendar via pg_cron/n8n; tambem acionavel pela tela ferias.html.';

-- ════════════════════════════════════════════════════════════════════════
-- 5. ferias_programar — programacao/reprogramacao (admin/rh)
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.ferias_programar(
  p_ferias_id UUID,
  p_data_inicio DATE,
  p_dias INT,
  p_dias_vendidos INT DEFAULT 0,
  p_abono BOOLEAN DEFAULT false,
  p_observacoes TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_uid UUID;
  v_perfil TEXT;
  v_f rh_ferias%ROWTYPE;
  v_data_fim DATE;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  SELECT perfil INTO v_perfil FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh') THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Apenas perfis administrador ou rh podem programar ferias. Perfil atual: ' || v_perfil);
  END IF;

  SELECT * INTO v_f FROM rh_ferias WHERE id = p_ferias_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Periodo de ferias nao encontrado');
  END IF;
  IF v_f.status NOT IN ('pendente','vencida','programada') THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Somente ferias pendentes, vencidas ou ja programadas podem ser (re)programadas. Status atual: ' || v_f.status);
  END IF;

  IF p_data_inicio IS NULL OR p_data_inicio < CURRENT_DATE THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Data de inicio deve ser hoje ou futura');
  END IF;
  IF p_dias IS NULL OR p_dias < 5 THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Minimo de 5 dias corridos por periodo (CLT art. 134)');
  END IF;
  IF COALESCE(p_dias_vendidos, 0) < 0 THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Dias vendidos nao pode ser negativo');
  END IF;
  IF COALESCE(p_dias_vendidos, 0) > FLOOR(v_f.dias_direito / 3.0) THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Abono pecuniario limitado a 1/3 do direito (' || FLOOR(v_f.dias_direito / 3.0) || ' dias — CLT art. 143)');
  END IF;
  IF p_dias + COALESCE(p_dias_vendidos, 0) > v_f.dias_direito THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Dias programados + vendidos (' || (p_dias + COALESCE(p_dias_vendidos,0)) ||
               ') excedem o direito (' || v_f.dias_direito || ')');
  END IF;

  v_data_fim := p_data_inicio + (p_dias - 1);

  UPDATE rh_ferias SET
    data_inicio      = p_data_inicio,
    data_fim         = v_data_fim,
    data_retorno     = v_data_fim + 1,
    dias_vendidos    = COALESCE(p_dias_vendidos, 0),
    abono_pecuniario = COALESCE(p_abono, false) OR COALESCE(p_dias_vendidos, 0) > 0,
    observacoes      = COALESCE(p_observacoes, observacoes),
    status           = 'programada'
  WHERE id = p_ferias_id;

  RETURN jsonb_build_object(
    'ok', true,
    'ferias_id', p_ferias_id,
    'data_inicio', p_data_inicio,
    'data_fim', v_data_fim,
    'data_retorno', v_data_fim + 1,
    'status', 'programada'
  );
END; $fn$;

REVOKE ALL ON FUNCTION public.ferias_programar(UUID, DATE, INT, INT, BOOLEAN, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ferias_programar(UUID, DATE, INT, INT, BOOLEAN, TEXT) TO authenticated;

COMMENT ON FUNCTION public.ferias_programar(UUID, DATE, INT, INT, BOOLEAN, TEXT) IS
  'Programa/reprograma um periodo de ferias (guard administrador/rh). Valida minimo CLT, abono 1/3 e saldo; calcula data_fim/data_retorno e marca programada.';

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

SELECT conname FROM pg_constraint WHERE conrelid = 'rh_ferias'::regclass ORDER BY conname;
SELECT proname FROM pg_proc WHERE proname IN ('ferias_atualizar_vencidas','ferias_programar');
