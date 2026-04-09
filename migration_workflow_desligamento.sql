-- ============================================================================
-- migration_workflow_desligamento.sql
-- Implementação correta do workflow de desligamento via rh_processo
-- STATUS: APLICADO no banco em 2026-04-09
-- ============================================================================
--
-- FLUXO:
--   1. Frontend chama rh_abrir_desligamento() com dados do formulário
--   2. RPC cria rh_processo (pai, tipo='desligamento') + rh_proc_desligamento (filho)
--   3. RPC avança status: rascunho → pendente_aprovacao → aprovado → em_execucao
--   4. Usuário preenche checklist rescisório (PATCH rh_proc_desligamento)
--   5. Usuário efetiva via rh_executar_desligamento(processo_id)
--   6. RPC verifica checklist → muda funcionário para 'desligado' → conclui processo
--
-- PROTEÇÃO:
--   Trigger fn_bloquear_desligamento_direto() impede UPDATE direto de
--   rh_funcionarios.status para 'desligado'. Apenas rh_executar_desligamento()
--   pode fazer isso (via session variable app.desligamento_autorizado).
-- ============================================================================

-- 1. RPC para abertura do processo de desligamento
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
  v_processo_id UUID;
  v_desl_id UUID;
  v_func RECORD;
  v_tem_entrevista BOOLEAN;
BEGIN
  v_perfil := rh_perfil_atual();
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
  VALUES (p_funcionario_id, 'desligamento', 'rascunho', CURRENT_DATE, auth.uid(), auth.uid())
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

  -- Fluxo simplificado: avançar direto para em_execucao
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

-- 2. Trigger para bloquear bypass direto
CREATE OR REPLACE FUNCTION public.fn_bloquear_desligamento_direto()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status != 'desligado' THEN RETURN NEW; END IF;
  IF OLD.status = 'desligado' THEN RETURN NEW; END IF;
  IF current_setting('app.desligamento_autorizado', true) = 'true' THEN RETURN NEW; END IF;
  RAISE EXCEPTION 'Desligamento deve ser feito via processo RH (rh_abrir_desligamento + rh_executar_desligamento). Alteracao direta nao permitida.';
END;
$$;

CREATE TRIGGER trg_bloquear_desligamento_direto
  BEFORE UPDATE ON rh_funcionarios FOR EACH ROW
  EXECUTE FUNCTION fn_bloquear_desligamento_direto();

-- 3. rh_executar_desligamento atualizado com session variable
CREATE OR REPLACE FUNCTION public.rh_executar_desligamento(p_processo_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $function$
DECLARE v_proc RECORD; v_desl RECORD; v_checklist_ok BOOLEAN;
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
END;
$function$;

GRANT EXECUTE ON FUNCTION public.rh_abrir_desligamento TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_executar_desligamento TO authenticated;
