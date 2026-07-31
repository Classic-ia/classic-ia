-- ============================================================================
-- WORKFLOW_ADMISSAO_V2.sql
-- Sprint 2 (P1) do PLANO_CORRECOES_SIGA.md — item 3.1 (F-39 D-01)
--
-- CONTEXTO:
--   A aprovacao no pipeline de recrutamento (rs_pipeline -> 'aprovado')
--   apenas marcava o candidato como 'contratado' — nenhum funcionario era
--   criado. O MODULO_PROCESSOS_RH_v2.sql documentava rh_executar_admissao
--   mas NAO continha DDL (0 CREATE) — a funcao nunca existiu em ambiente
--   algum. As tabelas rh_processo/rh_proc_admissao existem (TABELAS_RH_CORE)
--   mas rh_processo.funcionario_id era NOT NULL, incompativel com admissao
--   (o funcionario ainda nao existe quando o processo abre).
--
-- O QUE ESTE SCRIPT FAZ (script 26 da ORDEM_REBUILD):
--   1. rh_processo.funcionario_id passa a NULLABLE (preenchido na execucao
--      da admissao; processos de desligamento seguem preenchendo na abertura)
--   2. Rastreabilidade candidato -> colaborador:
--      rs_candidatos.funcionario_criado_id + rs_pipeline.processo_admissao_id
--   3. rh_abrir_admissao(...) — cria rh_processo (tipo=admissao) +
--      rh_proc_admissao e avanca ate em_execucao (guard administrador/rh;
--      valida CPF de 11 digitos e unicidade em rh_funcionarios)
--   4. rh_executar_admissao(p_processo_id) — cria o funcionario em
--      rh_funcionarios a partir do detalhe, vincula funcionario_criado_id,
--      conclui o processo. Retorna jsonb {ok, funcionario_id, ...}
--
-- Idempotente. Frontend correspondente: rh/recrutamento_selecao.html
-- (modal de admissao ao aprovar candidato, no mesmo commit).
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 1. rh_processo.funcionario_id nullable (admissao abre sem funcionario)
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE public.rh_processo ALTER COLUMN funcionario_id DROP NOT NULL;

COMMENT ON COLUMN public.rh_processo.funcionario_id IS
  'NULL enquanto um processo de admissao esta em andamento; preenchido por rh_executar_admissao. Demais tipos preenchem na abertura.';

-- ════════════════════════════════════════════════════════════════════════
-- 2. Rastreabilidade candidato -> colaborador
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE public.rs_candidatos ADD COLUMN IF NOT EXISTS funcionario_criado_id UUID;
ALTER TABLE public.rs_pipeline  ADD COLUMN IF NOT EXISTS processo_admissao_id  UUID;

-- ════════════════════════════════════════════════════════════════════════
-- 3. rh_abrir_admissao
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rh_abrir_admissao(
  p_nome_completo TEXT,
  p_cpf TEXT,
  p_empresa_id UUID,
  p_filial_id UUID,
  p_setor_id UUID,
  p_cargo_id UUID,
  p_data_admissao DATE,
  p_salario NUMERIC DEFAULT NULL,
  p_tipo_vinculo TEXT DEFAULT 'CLT',
  p_turno TEXT DEFAULT 'comercial',
  p_carga_horaria NUMERIC DEFAULT 44.0,
  p_gestor_id UUID DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_perfil TEXT;
  v_uid UUID;
  v_cpf TEXT;
  v_processo_id UUID;
  v_detalhe_id UUID;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  SELECT perfil INTO v_perfil FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh') THEN
    RETURN jsonb_build_object('ok', false, 'processo_id', null,
      'error', 'Apenas perfis administrador ou rh podem abrir admissao. Perfil atual: ' || v_perfil);
  END IF;

  IF p_nome_completo IS NULL OR TRIM(p_nome_completo) = '' THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Nome completo obrigatorio');
  END IF;

  v_cpf := regexp_replace(COALESCE(p_cpf, ''), '\D', '', 'g');
  IF v_cpf !~ '^\d{11}$' THEN
    RETURN jsonb_build_object('ok', false, 'error', 'CPF invalido (11 digitos obrigatorios)');
  END IF;
  IF EXISTS (SELECT 1 FROM rh_funcionarios WHERE cpf = v_cpf) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Ja existe funcionario com este CPF');
  END IF;

  IF p_data_admissao IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Data de admissao obrigatoria');
  END IF;
  IF p_tipo_vinculo NOT IN ('CLT','APAC','PJ','Terceiro') THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Tipo de vinculo invalido (CLT/APAC/PJ/Terceiro)');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM rh_empresas WHERE id = p_empresa_id) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Empresa nao encontrada');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM rh_filiais WHERE id = p_filial_id AND empresa_id = p_empresa_id) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Filial nao encontrada (ou nao pertence a empresa)');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM rh_setores WHERE id = p_setor_id) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Setor nao encontrado');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM rh_cargos WHERE id = p_cargo_id) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Cargo nao encontrado');
  END IF;

  -- Um processo de admissao em aberto por CPF
  IF EXISTS (
    SELECT 1 FROM rh_processo pr
    JOIN rh_proc_admissao pa ON pa.processo_id = pr.id
    WHERE pr.tipo = 'admissao' AND pr.status NOT IN ('concluido','cancelado')
      AND pa.cpf = v_cpf
  ) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Ja existe processo de admissao em aberto para este CPF');
  END IF;

  INSERT INTO rh_processo (funcionario_id, tipo, status, data_solicitacao, solicitado_por, created_by)
  VALUES (NULL, 'admissao', 'rascunho', CURRENT_DATE, v_uid, v_uid)
  RETURNING id INTO v_processo_id;

  INSERT INTO rh_proc_admissao (
    processo_id, nome_completo, cpf, cargo_id, setor_id, empresa_id, filial_id,
    gestor_id, tipo_vinculo, salario_proposto, data_admissao_prevista, turno,
    carga_horaria_semanal
  ) VALUES (
    v_processo_id, TRIM(p_nome_completo), v_cpf, p_cargo_id, p_setor_id, p_empresa_id, p_filial_id,
    p_gestor_id, p_tipo_vinculo, p_salario, p_data_admissao, COALESCE(NULLIF(TRIM(p_turno),''),'comercial'),
    COALESCE(p_carga_horaria, 44.0)
  ) RETURNING id INTO v_detalhe_id;

  -- Fluxo simplificado (mesmo padrao do desligamento)
  UPDATE rh_processo SET status = 'pendente_aprovacao' WHERE id = v_processo_id;
  UPDATE rh_processo SET status = 'aprovado' WHERE id = v_processo_id;
  UPDATE rh_processo SET status = 'em_execucao' WHERE id = v_processo_id;

  RETURN jsonb_build_object(
    'ok', true, 'processo_id', v_processo_id, 'detalhe_id', v_detalhe_id,
    'status', 'em_execucao', 'error', null
  );
EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object('ok', false, 'processo_id', null, 'error', 'Erro: ' || SQLERRM);
END;
$fn$;

-- ════════════════════════════════════════════════════════════════════════
-- 4. rh_executar_admissao — cria o funcionario e conclui o processo
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rh_executar_admissao(p_processo_id UUID)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_perfil TEXT;
  v_uid UUID;
  v_proc RECORD;
  v_adm RECORD;
  v_func_id UUID;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  SELECT perfil INTO v_perfil FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh') THEN
    RETURN jsonb_build_object('ok', false, 'error',
      'Apenas perfis administrador ou rh podem executar admissao. Perfil atual: ' || v_perfil);
  END IF;

  SELECT * INTO v_proc FROM rh_processo WHERE id = p_processo_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Processo nao encontrado');
  END IF;
  IF v_proc.tipo != 'admissao' THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Nao e processo de admissao');
  END IF;
  IF v_proc.status != 'em_execucao' THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Status deve ser em_execucao (atual: ' || v_proc.status || ')');
  END IF;

  SELECT * INTO v_adm FROM rh_proc_admissao WHERE processo_id = p_processo_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Detalhes de admissao nao encontrados');
  END IF;
  IF v_adm.funcionario_criado_id IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Funcionario ja criado para este processo');
  END IF;

  -- Revalidar unicidade do CPF (pode ter mudado entre abrir e executar)
  IF EXISTS (SELECT 1 FROM rh_funcionarios WHERE cpf = v_adm.cpf) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Ja existe funcionario com este CPF');
  END IF;

  INSERT INTO rh_funcionarios (
    nome_completo, cpf, empresa_id, filial_id, setor_id, cargo_id,
    centro_custo_id, gestor_id, tipo_vinculo, modalidade_contrato,
    salario_base, data_admissao, turno, carga_horaria_semanal, status
  ) VALUES (
    v_adm.nome_completo, v_adm.cpf, v_adm.empresa_id, v_adm.filial_id, v_adm.setor_id, v_adm.cargo_id,
    v_adm.centro_custo_id, v_adm.gestor_id, COALESCE(v_adm.tipo_vinculo, 'CLT'), v_adm.modalidade_contrato,
    v_adm.salario_proposto, v_adm.data_admissao_prevista,
    COALESCE(v_adm.turno, 'comercial'), COALESCE(v_adm.carga_horaria_semanal, 44.0), 'ativo'
  ) RETURNING id INTO v_func_id;

  UPDATE rh_proc_admissao SET funcionario_criado_id = v_func_id WHERE processo_id = p_processo_id;
  UPDATE rh_processo SET funcionario_id = v_func_id, status = 'concluido',
         data_efetivacao = v_adm.data_admissao_prevista,
         executado_por = v_uid, executado_em = now()
   WHERE id = p_processo_id;

  -- trilha de auditoria (AUDITORIA_TRILHA, script 30)
  BEGIN
    INSERT INTO rh_audit_log (tabela, registro_id, acao, dados_novos, usuario_id)
    VALUES ('rh_funcionarios', v_func_id::text, 'rh_executar_admissao',
            jsonb_build_object('processo_id', p_processo_id, 'nome', v_adm.nome_completo, 'cpf', v_adm.cpf),
            v_uid);
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Audit log rh_executar_admissao falhou: %', SQLERRM;
  END;

  RETURN jsonb_build_object(
    'ok', true, 'funcionario_id', v_func_id, 'processo_id', p_processo_id,
    'nome', v_adm.nome_completo, 'error', null
  );
EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object('ok', false, 'error', 'Erro: ' || SQLERRM);
END;
$fn$;

-- ════════════════════════════════════════════════════════════════════════
-- 5. GRANTS
-- ════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION public.rh_abrir_admissao TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_executar_admissao TO authenticated;

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name IN ('rh_abrir_admissao','rh_executar_admissao')
ORDER BY routine_name;

SELECT is_nullable FROM information_schema.columns
WHERE table_name = 'rh_processo' AND column_name = 'funcionario_id';

SELECT COUNT(*) AS colunas_rastreio FROM information_schema.columns
WHERE (table_name = 'rs_candidatos' AND column_name = 'funcionario_criado_id')
   OR (table_name = 'rs_pipeline'  AND column_name = 'processo_admissao_id');
