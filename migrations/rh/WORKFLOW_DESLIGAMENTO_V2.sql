-- ============================================================================
-- WORKFLOW_DESLIGAMENTO_V2.sql
-- Sprint 1 (P0) do PLANO_CORRECOES_SIGA.md — item 2.2 (F-38 D-01/D-02)
--
-- CONTEXTO:
--   O workflow de desligamento (rh_abrir_desligamento + trigger de bloqueio +
--   rh_executar_desligamento) foi aplicado em prod em 2026-04-09 pela
--   migration_workflow_desligamento.sql, que vivia na RAIZ do repo, fora da
--   ORDEM_REBUILD — num rebuild limpo o workflow nao existia. O frontend
--   (desligamentos_turnover.html) continuava fazendo POST rh_desligamentos +
--   PATCH direto de status (bloqueado pelo trigger em prod).
--
-- O QUE ESTE SCRIPT FAZ (versionado como script 24 da ORDEM_REBUILD):
--   1. rh_abrir_desligamento — versao identica a aplicada em prod
--   2. fn_bloquear_desligamento_direto + trigger — idem
--   3. rh_executar_desligamento V2 — alem do fluxo original (checklist ->
--      autorizacao via session var -> status desligado -> processo concluido),
--      agora tambem grava o REGISTRO ANALITICO em rh_desligamentos (com
--      entrevista e campos denormalizados do dashboard de turnover), com
--      colunas resolvidas dinamicamente (prod e rebuild tem shapes distintos)
--   4. rh_desligamentos — ADD COLUMN IF NOT EXISTS das colunas denormalizadas
--      que o dashboard usa (paridade rebuild <-> prod)
--   5. registrar_desligamento_completo — DEPRECADA (gravava status 'inativo',
--      divergente do fluxo; nunca chamada pelo frontend). Agora lanca excecao
--      orientando ao workflow.
--
-- Idempotente. O frontend correspondente (desligamentos_turnover.html) passa
-- a chamar rh_abrir_desligamento -> PATCH rh_proc_desligamento (checklist) ->
-- rh_executar_desligamento.
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 0. FIX LATENTE: rh_set_updated_at() resiliente
--    schema_rh_sst_completo.sql define a funcao gravando NEW.updated_at e
--    a anexa a rh_funcionarios/rh_desligamentos/etc.; MODULO_SAUDE_MENTAL.sql
--    REDEFINE a mesma funcao gravando NEW.atualizado_em (para rh_sm_*).
--    Resultado: a definicao que rodar por ultimo quebra TODO UPDATE nas
--    tabelas da outra familia ("record new has no field ...").
--    Versao resiliente: seta a(s) coluna(s) que existirem na tabela alvo.
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rh_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  BEGIN NEW.updated_at = now();    EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN NEW.atualizado_em = now(); EXCEPTION WHEN OTHERS THEN NULL; END;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION rh_set_updated_at() IS
  'Resiliente (WORKFLOW_DESLIGAMENTO_V2): seta updated_at e/ou atualizado_em conforme a coluna existir. Corrige conflito entre schema_rh_sst_completo e MODULO_SAUDE_MENTAL que redefinia a funcao e quebrava UPDATEs.';

-- ════════════════════════════════════════════════════════════════════════
-- 1. RPC de abertura do processo (identica a versao aplicada em prod)
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rh_abrir_desligamento(
  p_funcionario_id UUID,
  p_tipo_desligamento TEXT,
  p_data_desligamento DATE,
  p_categoria_causa TEXT,
  p_motivo_detalhado TEXT,
  p_entrevista_p1 TEXT DEFAULT NULL,
  p_entrevista_p2 TEXT DEFAULT NULL,
  p_entrevista_p3 TEXT DEFAULT NULL,
  p_entrevista_p4 TEXT DEFAULT NULL,
  p_entrevista_p5 TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_perfil TEXT;
  v_uid UUID;
  v_processo_id UUID;
  v_desl_id UUID;
  v_func RECORD;
  v_tem_entrevista BOOLEAN;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;

  SELECT perfil INTO v_perfil FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh') THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null,
      'error', 'Apenas perfis administrador ou rh podem abrir desligamento. Perfil atual: ' || v_perfil);
  END IF;

  SELECT id, nome_completo, status INTO v_func FROM rh_funcionarios WHERE id = p_funcionario_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Funcionario nao encontrado');
  END IF;
  IF v_func.status = 'desligado' THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Funcionario ja esta desligado');
  END IF;

  IF EXISTS (SELECT 1 FROM rh_processo WHERE funcionario_id = p_funcionario_id
    AND tipo = 'desligamento' AND status NOT IN ('concluido','cancelado')) THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null,
      'error', 'Ja existe processo de desligamento em aberto para este funcionario');
  END IF;

  IF p_tipo_desligamento IS NULL OR TRIM(p_tipo_desligamento) = '' THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Tipo de desligamento obrigatorio');
  END IF;
  IF p_data_desligamento IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Data de desligamento obrigatoria');
  END IF;
  IF p_categoria_causa IS NULL OR TRIM(p_categoria_causa) = '' THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Categoria da causa obrigatoria');
  END IF;
  IF p_motivo_detalhado IS NULL OR TRIM(p_motivo_detalhado) = '' THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Motivo detalhado obrigatorio');
  END IF;

  INSERT INTO rh_processo (funcionario_id, tipo, status, data_solicitacao, solicitado_por, created_by)
  VALUES (p_funcionario_id, 'desligamento', 'rascunho', CURRENT_DATE, v_uid, v_uid)
  RETURNING id INTO v_processo_id;

  v_tem_entrevista := (p_entrevista_p1 IS NOT NULL OR p_entrevista_p2 IS NOT NULL
    OR p_entrevista_p3 IS NOT NULL OR p_entrevista_p4 IS NOT NULL OR p_entrevista_p5 IS NOT NULL);

  INSERT INTO rh_proc_desligamento (
    processo_id, tipo_desligamento, data_desligamento, categoria_causa, motivo_detalhado,
    entrevista_realizada, entrevista_p1, entrevista_p2, entrevista_p3, entrevista_p4, entrevista_p5,
    status_anterior
  ) VALUES (
    v_processo_id, TRIM(p_tipo_desligamento), p_data_desligamento,
    TRIM(p_categoria_causa), TRIM(p_motivo_detalhado),
    v_tem_entrevista, p_entrevista_p1, p_entrevista_p2, p_entrevista_p3, p_entrevista_p4, p_entrevista_p5,
    v_func.status
  ) RETURNING id INTO v_desl_id;

  -- Fluxo simplificado: avancar direto para em_execucao
  UPDATE rh_processo SET status = 'pendente_aprovacao' WHERE id = v_processo_id;
  UPDATE rh_processo SET status = 'aprovado' WHERE id = v_processo_id;
  UPDATE rh_processo SET status = 'em_execucao' WHERE id = v_processo_id;

  RETURN jsonb_build_object(
    'ok', true, 'processo_id', v_processo_id, 'detalhe_id', v_desl_id,
    'status', 'em_execucao', 'error', null,
    'mensagem', 'Processo de desligamento aberto. Preencha o checklist e efetive.'
  );
EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Erro: ' || SQLERRM);
END;
$fn$;

-- ════════════════════════════════════════════════════════════════════════
-- 2. Trigger de bloqueio do bypass direto
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.fn_bloquear_desligamento_direto()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status != 'desligado' THEN RETURN NEW; END IF;
  IF OLD.status = 'desligado' THEN RETURN NEW; END IF;
  IF current_setting('app.desligamento_autorizado', true) = 'true' THEN RETURN NEW; END IF;
  RAISE EXCEPTION 'Desligamento deve ser feito via processo RH (rh_abrir_desligamento + rh_executar_desligamento). Alteracao direta nao permitida.';
END;
$$;

DROP TRIGGER IF EXISTS trg_bloquear_desligamento_direto ON rh_funcionarios;
CREATE TRIGGER trg_bloquear_desligamento_direto
  BEFORE UPDATE ON rh_funcionarios FOR EACH ROW
  EXECUTE FUNCTION fn_bloquear_desligamento_direto();

-- ════════════════════════════════════════════════════════════════════════
-- 3. rh_desligamentos — colunas denormalizadas do dashboard de turnover
--    (paridade entre o shape versionado e o drift de prod)
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS colaborador_id     UUID;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS colaborador_nome   TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS nome               TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS tipo               TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS motivo             TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS data_admissao      DATE;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS cargo              TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS salario            NUMERIC(12,2);
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS setor              TEXT;
ALTER TABLE public.rh_desligamentos ADD COLUMN IF NOT EXISTS tempo_empresa_dias INTEGER;

-- ════════════════════════════════════════════════════════════════════════
-- 4. rh_executar_desligamento V2
--    Original + gravacao do registro analitico em rh_desligamentos com
--    colunas resolvidas dinamicamente (nao falha o desligamento se o
--    espelho analitico der erro — RAISE WARNING).
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rh_executar_desligamento(p_processo_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $function$
DECLARE
  v_proc RECORD; v_desl RECORD; v_checklist_ok BOOLEAN;
  v_func RECORD;
  v_map JSONB; v_key TEXT;
  v_cols TEXT[] := '{}'; v_vals TEXT[] := '{}';
  v_idcol TEXT := NULL; v_dup BOOLEAN := false;
BEGIN
  SELECT * INTO v_proc FROM rh_processo WHERE id = p_processo_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Processo nao encontrado'; END IF;
  IF v_proc.tipo != 'desligamento' THEN RAISE EXCEPTION 'Nao e processo de desligamento'; END IF;
  IF v_proc.status != 'em_execucao' THEN RAISE EXCEPTION 'Status deve ser em_execucao (atual: %)', v_proc.status; END IF;

  SELECT * INTO v_desl FROM rh_proc_desligamento WHERE processo_id = p_processo_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'Detalhes de desligamento nao encontrados'; END IF;

  v_checklist_ok := v_desl.exame_demissional_ok AND v_desl.devolucao_epis_ok
    AND v_desl.devolucao_patrimonio_ok AND v_desl.bloqueio_acessos_ok;
  IF NOT v_checklist_ok THEN
    RAISE EXCEPTION 'Checklist incompleto: exame(%), epis(%), patrimonio(%), acessos(%)',
      v_desl.exame_demissional_ok, v_desl.devolucao_epis_ok, v_desl.devolucao_patrimonio_ok, v_desl.bloqueio_acessos_ok;
  END IF;

  UPDATE rh_proc_desligamento SET status_anterior = (SELECT status FROM rh_funcionarios WHERE id = v_proc.funcionario_id)
  WHERE processo_id = p_processo_id;

  PERFORM set_config('app.desligamento_autorizado', 'true', true);
  UPDATE rh_funcionarios SET status = 'desligado', data_desligamento = v_desl.data_desligamento
  WHERE id = v_proc.funcionario_id;
  PERFORM set_config('app.desligamento_autorizado', 'false', true);

  UPDATE rh_processo SET status = 'concluido', data_efetivacao = v_desl.data_desligamento WHERE id = p_processo_id;

  -- ── NOVO: registro analitico em rh_desligamentos (dashboard de turnover) ──
  BEGIN
    SELECT f.nome_completo AS nome, f.data_admissao, f.salario_base AS salario,
           c.nome AS cargo, s.nome AS setor
      INTO v_func
      FROM rh_funcionarios f
      LEFT JOIN rh_cargos c ON c.id = f.cargo_id
      LEFT JOIN rh_setores s ON s.id = f.setor_id
     WHERE f.id = v_proc.funcionario_id;

    v_map := jsonb_build_object(
      'funcionario_id',     v_proc.funcionario_id::text,
      'colaborador_id',     v_proc.funcionario_id::text,
      'colaborador_nome',   v_func.nome,
      'nome',               v_func.nome,
      'data_desligamento',  v_desl.data_desligamento::text,
      'tipo_desligamento',  v_desl.tipo_desligamento,
      'tipo',               v_desl.tipo_desligamento,
      'categoria_causa',    v_desl.categoria_causa,
      'descricao',          v_desl.motivo_detalhado,
      'motivo',             '[' || upper(COALESCE(v_desl.categoria_causa,'')) || '] ' || COALESCE(v_desl.motivo_detalhado,''),
      'data_admissao',      v_func.data_admissao::text,
      'cargo',              v_func.cargo,
      'salario',            v_func.salario::text,
      'setor',              v_func.setor,
      'tempo_empresa_dias', (v_desl.data_desligamento - v_func.data_admissao)::text,
      'entrevista_p1',      v_desl.entrevista_p1,
      'entrevista_p2',      v_desl.entrevista_p2,
      'entrevista_p3',      v_desl.entrevista_p3,
      'entrevista_p4',      v_desl.entrevista_p4,
      'entrevista_p5',      v_desl.entrevista_p5
    );

    FOR v_key IN SELECT jsonb_object_keys(v_map) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'rh_desligamentos'
                   AND column_name = v_key) THEN
        v_cols := array_append(v_cols, v_key);
        v_vals := array_append(v_vals, quote_nullable(v_map ->> v_key));
      END IF;
    END LOOP;

    -- dedup por (id do funcionario, data) na coluna de id que existir
    SELECT column_name INTO v_idcol FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'rh_desligamentos'
       AND column_name IN ('colaborador_id','funcionario_id')
     ORDER BY column_name LIMIT 1;
    IF v_idcol IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM rh_desligamentos WHERE %I = $1 AND data_desligamento = $2)', v_idcol)
        INTO v_dup USING v_proc.funcionario_id, v_desl.data_desligamento;
    END IF;

    IF array_length(v_cols, 1) > 0 AND NOT v_dup THEN
      EXECUTE format('INSERT INTO rh_desligamentos (%s) VALUES (%s)',
        (SELECT string_agg(format('%I', c), ', ') FROM unnest(v_cols) c),
        array_to_string(v_vals, ', '));
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'rh_executar_desligamento: registro analitico em rh_desligamentos falhou (%). Desligamento efetivado mesmo assim.', SQLERRM;
  END;
END;
$function$;

-- ════════════════════════════════════════════════════════════════════════
-- 5. Deprecar registrar_desligamento_completo (gravava status 'inativo';
--    divergia do workflow e do trigger — F-38 D-02)
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION registrar_desligamento_completo(
  p_funcionario_id    UUID,
  p_data_desligamento DATE,
  p_tipo_desligamento TEXT,
  p_categoria_causa   TEXT,
  p_descricao         TEXT,
  p_categorias        JSONB DEFAULT '[]'::JSONB,
  p_entrevista        JSONB DEFAULT '[]'::JSONB
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  RAISE EXCEPTION 'DEPRECADA (WORKFLOW_DESLIGAMENTO_V2): use rh_abrir_desligamento(...) + rh_executar_desligamento(processo_id). Esta funcao gravava status ''inativo'' e nao passava pelo checklist rescisorio.';
END;
$$;

-- ════════════════════════════════════════════════════════════════════════
-- 6. GRANTS
-- ════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION public.rh_abrir_desligamento TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_executar_desligamento TO authenticated;

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

-- V1: funcoes presentes
SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name IN ('rh_abrir_desligamento','rh_executar_desligamento','fn_bloquear_desligamento_direto')
ORDER BY routine_name;

-- V2: trigger ativo
SELECT tgname FROM pg_trigger WHERE tgname = 'trg_bloquear_desligamento_direto';

-- V3: colunas denormalizadas presentes
SELECT COUNT(*) AS colunas_dashboard FROM information_schema.columns
WHERE table_name = 'rh_desligamentos'
  AND column_name IN ('colaborador_id','colaborador_nome','tipo','motivo','cargo','setor','salario','tempo_empresa_dias');
