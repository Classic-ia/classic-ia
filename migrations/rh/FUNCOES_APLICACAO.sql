-- ════════════════════════════════════════════════════════════════════════════
-- FUNCOES DE APLICACAO -- RPCs chamadas pelo frontend e ingestores
-- Classic / APAC · Supabase (PostgreSQL 17)
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao RH/SST)
-- Data de exportacao: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
--
-- PROPOSITO:
--
-- Este arquivo versiona as 18 funcoes RPC de nivel de aplicacao que sao
-- chamadas diretamente pelo frontend (via supabase.rpc()) ou pelos
-- ingestores Python, mas que nunca foram commitadas no repositorio.
--
-- Todas as definicoes foram extraidas da producao via pg_get_functiondef()
-- e sao idempotentes (CREATE OR REPLACE).
--
-- GRUPOS:
--   1. Dashboard RPCs  (12 funcoes) -- retornam JSONB para os paineis
--   2. CRUD RPCs       (1 funcao)   -- operacoes de escrita do frontend
--   3. ETL/Import RPCs (5 funcoes)  -- usadas pelos ingestores Python
--
-- NOTA: Estas funcoes dependem de tabelas e views definidas em outros
-- modulos (FUNDACAO_BANCO_v2, MODULO_SST_v2, MODULO_PROCESSOS_RH_v2, etc).
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- GRUPO 1 -- RPCs de Dashboard (chamadas pelo frontend)
-- ════════════════════════════════════════════════════════════════════════════

-- ── sst_dashboard_completo() ── chamada por: sst_dashboard.html
CREATE OR REPLACE FUNCTION public.sst_dashboard_completo()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN jsonb_build_object(
    'epis', jsonb_build_object(
      'catalogo_ativo', (SELECT COUNT(*) FROM sst_catalogo_epi WHERE ativo = true),
      'total_entregas', (SELECT COUNT(*) FROM sst_epi_entrega),
      'entregas_12m', (SELECT COUNT(*) FROM sst_epi_entrega WHERE data_entrega >= CURRENT_DATE - 365),
      'funcionarios_com_epi', (SELECT COUNT(DISTINCT funcionario_id) FROM sst_epi_entrega WHERE data_entrega >= CURRENT_DATE - 365),
      'funcionarios_sem_epi', (SELECT COUNT(*) FROM rh_funcionarios f WHERE f.status = 'ativo' AND NOT EXISTS (SELECT 1 FROM sst_epi_entrega e WHERE e.funcionario_id = f.id AND e.data_entrega >= CURRENT_DATE - 365))
    ),
    'asos', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM sst_aso),
      'vencidos', (
        SELECT COUNT(*) FROM (
          SELECT DISTINCT ON (a.funcionario_id) a.funcionario_id, a.data_validade
          FROM sst_aso a
          JOIN rh_funcionarios f ON f.id = a.funcionario_id
          WHERE f.status IN ('ativo','experiencia','ferias','afastado')
          ORDER BY a.funcionario_id, a.data_exame DESC
        ) ultimo
        WHERE ultimo.data_validade < CURRENT_DATE
      ),
      'vencendo_30d', (
        SELECT COUNT(*) FROM (
          SELECT DISTINCT ON (a.funcionario_id) a.funcionario_id, a.data_validade
          FROM sst_aso a
          JOIN rh_funcionarios f ON f.id = a.funcionario_id
          WHERE f.status IN ('ativo','experiencia','ferias','afastado')
          ORDER BY a.funcionario_id, a.data_exame DESC
        ) ultimo
        WHERE ultimo.data_validade BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
      ),
      'sem_aso', (
        SELECT COUNT(*) FROM rh_funcionarios f
        WHERE f.status IN ('ativo','experiencia')
        AND NOT EXISTS (SELECT 1 FROM sst_aso a WHERE a.funcionario_id = f.id)
      )
    ),
    'treinamentos', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM sst_treinamento),
      'validos', (SELECT COUNT(*) FROM sst_treinamento WHERE data_validade IS NULL OR data_validade >= CURRENT_DATE),
      'vencidos', (SELECT COUNT(*) FROM sst_treinamento t
        JOIN rh_funcionarios f ON f.id = t.funcionario_id
        WHERE f.status IN ('ativo','experiencia','ferias','afastado')
        AND t.data_validade < CURRENT_DATE),
      'tipos', (SELECT COUNT(*) FROM sst_tipo_treinamento WHERE ativo = true)
    ),
    'os', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM sst_ordem_servico),
      'vigentes', (SELECT COUNT(*) FROM sst_ordem_servico WHERE status = 'vigente'),
      'ciencias_total', (SELECT COUNT(*) FROM sst_os_ciencia),
      'sem_ciencia', (SELECT COUNT(DISTINCT f.id) FROM rh_funcionarios f WHERE f.status = 'ativo' AND NOT EXISTS (SELECT 1 FROM sst_os_ciencia c WHERE c.funcionario_id = f.id))
    ),
    'acidentes', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM sst_acidente),
      'dias_sem_acidente', COALESCE((SELECT (CURRENT_DATE - MAX(data_ocorrencia))::INT FROM sst_acidente), 365)
    )
  );
END; $function$;

-- ── ferias_dashboard() ── chamada por: ferias.html
CREATE OR REPLACE FUNCTION public.ferias_dashboard()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN jsonb_build_object(
    'resumo', (SELECT jsonb_build_object(
      'vencidas', COUNT(*) FILTER (WHERE fe.status = 'vencida'),
      'pendentes', COUNT(*) FILTER (WHERE fe.status = 'pendente'),
      'programadas', COUNT(*) FILTER (WHERE fe.status = 'programada'),
      'em_gozo', COUNT(*) FILTER (WHERE fe.status = 'em_gozo'),
      'gozadas', COUNT(*) FILTER (WHERE fe.status = 'gozada')
    ) FROM rh_ferias fe JOIN rh_funcionarios f ON f.id = fe.funcionario_id WHERE f.status = 'ativo'),
    'vencidas', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'nome', f.nome_completo, 'setor', s.nome, 'periodo_fim', fe.periodo_aquisitivo_fim,
      'dias_atraso', (CURRENT_DATE - fe.periodo_aquisitivo_fim), 'dias_gozados', fe.dias_gozados
    ) ORDER BY fe.periodo_aquisitivo_fim) FROM rh_ferias fe JOIN rh_funcionarios f ON f.id = fe.funcionario_id
    LEFT JOIN rh_setores s ON s.id = f.setor_id WHERE fe.status = 'vencida' AND f.status = 'ativo'), '[]'::jsonb),
    'proximas_vencer', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'nome', f.nome_completo, 'setor', s.nome, 'periodo_fim', fe.periodo_aquisitivo_fim,
      'dias_restantes', (fe.periodo_aquisitivo_fim - CURRENT_DATE), 'dias_gozados', fe.dias_gozados
    ) ORDER BY fe.periodo_aquisitivo_fim) FROM rh_ferias fe JOIN rh_funcionarios f ON f.id = fe.funcionario_id
    LEFT JOIN rh_setores s ON s.id = f.setor_id WHERE fe.status = 'pendente' AND f.status = 'ativo'
    AND fe.periodo_aquisitivo_fim <= CURRENT_DATE + 60), '[]'::jsonb)
  );
END; $function$;

-- ── gestor_dashboard() ── chamada por: gestor.html
CREATE OR REPLACE FUNCTION public.gestor_dashboard()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE v_setor_id UUID; v_setor_nome TEXT;
BEGIN
  v_setor_id := rh_setor_do_gestor();
  SELECT nome INTO v_setor_nome FROM rh_setores WHERE id = v_setor_id;

  RETURN jsonb_build_object(
    'setor', v_setor_nome,
    -- Equipe
    'equipe', jsonb_build_object(
      'ativos', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'ativo'),
      'afastados', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'afastado'),
      'experiencia', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'ativo' AND data_admissao >= CURRENT_DATE - 90),
      'total', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status IN ('ativo','afastado','ferias'))
    ),
    -- Score do setor (motor v2)
    'score', (SELECT jsonb_build_object(
      'medio', ROUND(AVG(score_final), 1),
      'consistentes', COUNT(*) FILTER (WHERE classificacao = 'consistente'),
      'monitorar', COUNT(*) FILTER (WHERE classificacao = 'monitorar'),
      'atencao', COUNT(*) FILTER (WHERE classificacao = 'atencao_operacional')
    ) FROM motor_score WHERE setor_nome = v_setor_nome AND periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score)),
    -- Ponto (ultimos 30 dias)
    'ponto', jsonb_build_object(
      'dias_trabalhados', (SELECT COUNT(DISTINCT data) FROM rh_ponto p JOIN rh_funcionarios f ON f.id = p.funcionario_id WHERE f.setor_id = v_setor_id AND p.data >= CURRENT_DATE - 30 AND p.justificativa IS NULL),
      'faltas', (SELECT COUNT(*) FROM rh_ponto p JOIN rh_funcionarios f ON f.id = p.funcionario_id WHERE f.setor_id = v_setor_id AND p.data >= CURRENT_DATE - 30 AND p.justificativa = 'Falta')
    ),
    -- Pendencias do setor (sem SST sensivel)
    'pendencias', (SELECT COUNT(*) FROM agente_pendencia WHERE setor_nome = v_setor_nome AND status = 'aberta' AND origem NOT IN ('SST')),
    -- Planos de acao do setor
    'planos_acao', (SELECT COUNT(*) FROM acao_plano WHERE setor_nome = v_setor_nome AND status NOT IN ('concluida','cancelada','descartada') AND impacto != 'juridico'),
    -- SST (agregado, sem dados de saude)
    'sst', jsonb_build_object(
      'com_epi', (SELECT COUNT(DISTINCT e.funcionario_id) FROM sst_epi_entrega e JOIN rh_funcionarios f ON f.id = e.funcionario_id WHERE f.setor_id = v_setor_id AND e.data_entrega >= CURRENT_DATE - 365),
      'trein_validos', (SELECT COUNT(*) FROM sst_treinamento t JOIN rh_funcionarios f ON f.id = t.funcionario_id WHERE f.setor_id = v_setor_id AND (t.data_validade IS NULL OR t.data_validade >= CURRENT_DATE)),
      'trein_vencidos', (SELECT COUNT(*) FROM sst_treinamento t JOIN rh_funcionarios f ON f.id = t.funcionario_id WHERE f.setor_id = v_setor_id AND t.data_validade < CURRENT_DATE)
    ),
    -- Atestados do setor (apenas contagem, sem nomes)
    'atestados_30d', (SELECT COUNT(*) FROM rh_ocorrencias oc JOIN rh_funcionarios f ON f.id = oc.colaborador_id WHERE f.setor_id = v_setor_id AND oc.tipo = 'atestado' AND oc.data_inicio >= CURRENT_DATE - 30),
    -- Experiencias vencendo
    'experiencias', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'nome', f.nome_completo, 'data_admissao', f.data_admissao,
      'dias', CASE WHEN (CURRENT_DATE - f.data_admissao) < 45 THEN 45 - (CURRENT_DATE - f.data_admissao) ELSE 90 - (CURRENT_DATE - f.data_admissao) END
    ) ORDER BY f.data_admissao) FROM rh_funcionarios f WHERE f.setor_id = v_setor_id AND f.status = 'ativo' AND f.data_admissao >= CURRENT_DATE - 90), '[]'::jsonb)
  );
END; $function$;

-- ── gestor_confianca_dashboard() ── chamada por: painel_gestao.html
CREATE OR REPLACE FUNCTION public.gestor_confianca_dashboard()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE v_setor_id UUID; v_setor_nome TEXT; v_gestor_id UUID;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh','gestor_confianca') THEN
    RETURN jsonb_build_object('erro', 'Acesso restrito');
  END IF;

  v_setor_id := rh_setor_do_gestor();
  SELECT nome INTO v_setor_nome FROM rh_setores WHERE id = v_setor_id;
  v_gestor_id := rh_funcionario_atual_id();

  RETURN jsonb_build_object(
    'setor', v_setor_nome,
    'perfil', 'gestor_confianca',

    -- Equipe detalhada (com salario para confianca)
    'equipe', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'id', f.id, 'nome', f.nome_completo, 'cargo', c.nome, 'status', f.status,
      'data_admissao', f.data_admissao, 'salario_base', f.salario_base,
      'turno', f.turno, 'cargo_confianca', f.cargo_confianca,
      'dias_empresa', (CURRENT_DATE - f.data_admissao),
      'atestados_6m', (SELECT COUNT(*) FROM rh_ocorrencias oc WHERE oc.colaborador_id = f.id AND oc.tipo = 'atestado' AND oc.data_inicio >= CURRENT_DATE - 180),
      'score', (SELECT score_final FROM motor_score WHERE funcionario_id = f.id ORDER BY periodo_referencia DESC LIMIT 1),
      'classificacao', (SELECT classificacao FROM motor_score WHERE funcionario_id = f.id ORDER BY periodo_referencia DESC LIMIT 1),
      'ultimo_acomp', (SELECT MAX(data_registro) FROM rh_acompanhamento WHERE funcionario_id = f.id)
    ) ORDER BY f.nome_completo)
    FROM rh_funcionarios f LEFT JOIN rh_cargos c ON c.id = f.cargo_id
    WHERE f.setor_id = v_setor_id AND f.status IN ('ativo','afastado','ferias')
    ), '[]'::jsonb),

    -- KPIs
    'kpis', jsonb_build_object(
      'ativos', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'ativo'),
      'score_medio', (SELECT ROUND(AVG(score_final),1) FROM motor_score WHERE setor_nome = v_setor_nome AND periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score)),
      'atestados_30d', (SELECT COUNT(*) FROM rh_ocorrencias oc JOIN rh_funcionarios f ON f.id = oc.colaborador_id WHERE f.setor_id = v_setor_id AND oc.tipo = 'atestado' AND oc.data_inicio >= CURRENT_DATE - 30),
      'pendencias', (SELECT COUNT(*) FROM agente_pendencia WHERE setor_nome = v_setor_nome AND status = 'aberta'),
      'acompanhamentos_mes', (SELECT COUNT(*) FROM rh_acompanhamento WHERE gestor_id = v_gestor_id AND data_registro >= CURRENT_DATE - 30),
      'trein_vencidos', (SELECT COUNT(*) FROM sst_treinamento t JOIN rh_funcionarios f ON f.id = t.funcionario_id WHERE f.setor_id = v_setor_id AND t.data_validade < CURRENT_DATE)
    ),

    -- Turnover do setor (12 meses)
    'turnover', jsonb_build_object(
      'desligados_12m', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'desligado' AND data_desligamento >= CURRENT_DATE - 365),
      'ativos', (SELECT COUNT(*) FROM rh_funcionarios WHERE setor_id = v_setor_id AND status = 'ativo')
    ),

    -- Ultimos acompanhamentos registrados
    'acompanhamentos_recentes', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'id', a.id, 'tipo', a.tipo, 'assunto', a.assunto, 'data', a.data_registro,
      'funcionario', f.nome_completo, 'resultado', a.resultado,
      'proxima_acao', a.proxima_acao, 'prazo', a.prazo_proxima_acao
    ) ORDER BY a.data_registro DESC)
    FROM rh_acompanhamento a JOIN rh_funcionarios f ON f.id = a.funcionario_id
    WHERE a.gestor_id = v_gestor_id AND a.data_registro >= CURRENT_DATE - 60
    LIMIT 20), '[]'::jsonb)
  );
END; $function$;

-- ── motor_v2_dashboard_executivo() ── chamada por: motor_v2.html
CREATE OR REPLACE FUNCTION public.motor_v2_dashboard_executivo()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_perfil TEXT;
  v_result JSONB;
BEGIN
  v_perfil := rh_perfil_atual();

  -- BASE: KPIs + resumo + confiabilidade (todos os perfis autenticados)
  v_result := jsonb_build_object(
    'perfil', v_perfil,
    'kpis', COALESCE((SELECT jsonb_agg(row_to_json(k)) FROM vw_motor_kpis_executivos k), '[]'::jsonb),
    'resumo_score', COALESCE((SELECT jsonb_agg(row_to_json(r)) FROM vw_motor_score_resumo r), '[]'::jsonb),
    'confiabilidade', COALESCE((SELECT jsonb_agg(row_to_json(c)) FROM vw_motor_confiabilidade_dado c), '[]'::jsonb),
    'setores', COALESCE((SELECT jsonb_agg(row_to_json(s)) FROM vw_motor_setor_risco s), '[]'::jsonb),
    'alertas_abertos', (SELECT COUNT(*) FROM motor_alerta WHERE status = 'aberto'),
    'alertas_criticos', (SELECT COUNT(*) FROM motor_alerta WHERE status = 'aberto' AND prioridade = 'critica'),
    'evolucao', COALESCE((SELECT jsonb_agg(row_to_json(e)) FROM vw_motor_evolucao_mensal e), '[]'::jsonb)
  );

  -- GESTOR: alertas de setor
  IF v_perfil = 'gestor' THEN
    v_result := v_result || jsonb_build_object(
      'alertas', COALESCE((SELECT jsonb_agg(row_to_json(a)) FROM vw_motor_gestor_alertas a), '[]'::jsonb)
    );
  END IF;

  -- RH/ADMIN: scores individuais + alertas completos + padroes
  IF v_perfil IN ('administrador', 'rh') THEN
    v_result := v_result || jsonb_build_object(
      'scores_detalhados', COALESCE((
        SELECT jsonb_agg(row_to_json(s))
        FROM (SELECT * FROM vw_motor_scores_detalhado WHERE classificacao IN ('atencao_operacional','monitorar') LIMIT 50) s
      ), '[]'::jsonb),
      'alertas', COALESCE((SELECT jsonb_agg(row_to_json(a)) FROM vw_motor_alertas_abertos a), '[]'::jsonb),
      'padroes', COALESCE((SELECT jsonb_agg(row_to_json(p)) FROM vw_motor_padroes p), '[]'::jsonb)
    );
  END IF;

  RETURN v_result;
END;
$function$;

-- ── motor_v2_executar() ── chamada por: motor_v2.html (recalcular motor)
CREATE OR REPLACE FUNCTION public.motor_v2_executar(p_periodo text DEFAULT to_char((CURRENT_DATE - '1 mon'::interval), 'YYYY-MM'::text))
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_exec_id UUID;
  v_scores INT;
  v_indicadores INT;
  v_padroes INT;
  v_alertas INT;
  v_result JSONB;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador', 'rh') THEN
    RAISE EXCEPTION 'Apenas administrador ou RH podem executar o motor';
  END IF;

  INSERT INTO motor_execucao (tipo, status) VALUES ('completo', 'executando') RETURNING id INTO v_exec_id;

  BEGIN
    v_scores := motor_v2_calcular_scores(v_exec_id, p_periodo);
    v_indicadores := motor_v2_calcular_indicadores(v_exec_id, p_periodo);
    v_padroes := motor_v2_detectar_padroes(v_exec_id, p_periodo);
    v_alertas := motor_v2_gerar_alertas(v_exec_id, p_periodo);

    UPDATE motor_execucao SET
      status = 'concluido', fim = now(),
      registros_processados = v_scores + v_indicadores + v_padroes + v_alertas,
      detalhes = jsonb_build_object('scores', v_scores, 'indicadores', v_indicadores, 'padroes', v_padroes, 'alertas', v_alertas, 'periodo', p_periodo)
    WHERE id = v_exec_id;

    v_result := jsonb_build_object('execucao_id', v_exec_id, 'status', 'concluido', 'scores', v_scores, 'indicadores', v_indicadores, 'padroes', v_padroes, 'alertas', v_alertas, 'periodo', p_periodo);
  EXCEPTION WHEN OTHERS THEN
    UPDATE motor_execucao SET status = 'erro', fim = now(), erro = SQLERRM WHERE id = v_exec_id;
    RAISE;
  END;

  RETURN v_result;
END;
$function$;

-- ── motor_v2_auditoria() ── chamada por: motor_v2_auditoria.html
CREATE OR REPLACE FUNCTION public.motor_v2_auditoria()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_perfil TEXT;
BEGIN
  v_perfil := rh_perfil_atual();
  IF v_perfil NOT IN ('administrador', 'rh') THEN
    RETURN jsonb_build_object('erro', 'Acesso restrito a admin/rh');
  END IF;

  RETURN jsonb_build_object(
    'resumo', (SELECT jsonb_build_object(
      'total_funcionarios', (SELECT COUNT(*) FROM motor_score WHERE periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score)),
      'consistentes', (SELECT COUNT(*) FROM motor_score WHERE periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score) AND classificacao = 'consistente'),
      'monitorar', (SELECT COUNT(*) FROM motor_score WHERE periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score) AND classificacao = 'monitorar'),
      'atencao_operacional', (SELECT COUNT(*) FROM motor_score WHERE periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score) AND classificacao = 'atencao_operacional'),
      'dado_insuficiente', (SELECT COUNT(*) FROM motor_score WHERE periodo_referencia = (SELECT MAX(periodo_referencia) FROM motor_score) AND classificacao = 'dado_insuficiente')
    )),
    'causas', COALESCE((SELECT jsonb_agg(row_to_json(c)) FROM vw_motor_causas_perda c), '[]'::jsonb),
    'por_setor', COALESCE((SELECT jsonb_agg(row_to_json(s)) FROM vw_motor_setor_nao_consistente s), '[]'::jsonb),
    'detalhes', COALESCE((SELECT jsonb_agg(row_to_json(a)) FROM vw_motor_auditoria_score a), '[]'::jsonb)
  );
END;
$function$;

-- ── motor_decisoes() ── chamada por: motor_decisoes.html
CREATE OR REPLACE FUNCTION public.motor_decisoes()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_problemas JSONB := '[]'::JSONB;
  v_excluidos JSONB := '[]'::JSONB;
  r RECORD;
BEGIN
  -- ═══ 1. RECORRENTES (exclui licenças) ═══
  FOR r IN
    SELECT rc.*, f.status AS f_status, f.tipo_afastamento
    FROM vw_absenteismo_recorrencia rc
      INNER JOIN rh_funcionarios f ON f.id = rc.funcionario_id
    WHERE rc.classificacao IN ('RECORRENTE CRITICO','RECORRENTE')
    ORDER BY rc.custo_empresa_total DESC LIMIT 15
  LOOP
    IF r.tipo_afastamento IN ('licenca_maternidade','licenca_paternidade') THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Licenca maternidade',
        'atestados', r.total_atestados, 'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;
    IF r.f_status = 'afastado' AND r.tipo_afastamento IN ('inss_doenca','acidente_trabalho') THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Afastamento INSS',
        'atestados', r.total_atestados, 'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;

    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'recorrencia',
      'prioridade', CASE r.classificacao WHEN 'RECORRENTE CRITICO' THEN 1 ELSE 2 END,
      'risco', CASE WHEN r.atestados_por_mes >= 3 THEN 'alto' WHEN r.atestados_por_mes >= 2 THEN 'medio' ELSE 'baixo' END,
      'funcionario', r.nome_completo, 'cargo', r.cargo, 'setor', r.setor,
      'impacto_financeiro', r.custo_empresa_total,
      'diagnostico', r.total_atestados || ' atestados (' || r.atestados_curtos || ' curtos), ' ||
        ROUND(r.atestados_por_mes, 1) || '/mes. Custo: R$ ' || r.custo_empresa_total,
      'causa_provavel', CASE
        WHEN r.atestados_curtos >= r.total_atestados * 0.8 THEN 'Padrao de faltas curtas recorrentes — possivel desmotivacao, problema pessoal ou condicao cronica'
        ELSE 'Faltas curtas + afastamentos — possivel problema de saude em agravamento'
      END,
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'Conversa individual com RH e lideranca', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Revisar historico de atestados e CIDs', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Encaminhar para avaliacao medica ocupacional', 'responsavel', 'sst', 'prazo_dias', 14),
        jsonb_build_object('acao', 'Acompanhamento mensal por 3 meses', 'responsavel', 'gestor', 'prazo_dias', 30)
      )
    );
  END LOOP;

  -- ═══ 2. AFASTAMENTOS PROLONGADOS (exclui licenças) ═══
  FOR r IN
    SELECT rc.*, f.tipo_afastamento
    FROM vw_absenteismo_recorrencia rc
      INNER JOIN rh_funcionarios f ON f.id = rc.funcionario_id
    WHERE rc.classificacao = 'AFASTAMENTO PROLONGADO'
    ORDER BY rc.total_dias DESC LIMIT 5
  LOOP
    IF r.tipo_afastamento = 'licenca_maternidade' THEN
      v_excluidos := v_excluidos || jsonb_build_object(
        'funcionario', r.nome_completo, 'motivo_exclusao', 'Licenca maternidade',
        'custo_removido', r.custo_empresa_total);
      CONTINUE;
    END IF;
    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'afastamento_prolongado', 'prioridade', 3,
      'risco', CASE WHEN r.total_dias >= 90 THEN 'alto' ELSE 'medio' END,
      'funcionario', r.nome_completo, 'cargo', r.cargo, 'setor', r.setor,
      'impacto_financeiro', r.custo_empresa_total,
      'diagnostico', r.total_dias || ' dias (' || r.dias_pagos_empresa || ' empresa + ' || r.dias_pagos_inss || ' INSS)',
      'causa_provavel', 'Lesao ou doenca grave — verificar componente ergonomico ou ocupacional',
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'ASO de retorno obrigatorio', 'responsavel', 'sst', 'prazo_dias', 1),
        jsonb_build_object('acao', 'Planejar retorno com restricoes', 'responsavel', 'sst', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Avaliar posto de trabalho', 'responsavel', 'sst', 'prazo_dias', 14),
        jsonb_build_object('acao', 'Acompanhamento semanal no 1o mes', 'responsavel', 'gestor', 'prazo_dias', 30)
      )
    );
  END LOOP;

  -- ═══ 3. SETORES CRÍTICOS ═══
  FOR r IN
    SELECT * FROM vw_absenteismo_por_setor WHERE total_dias > 50
  LOOP
    v_problemas := v_problemas || jsonb_build_object(
      'tipo', 'setor_critico', 'prioridade', CASE WHEN r.total_dias >= 500 THEN 1 ELSE 3 END,
      'risco', CASE WHEN r.total_dias >= 500 THEN 'alto' ELSE 'medio' END,
      'setor', r.setor, 'impacto_financeiro', r.custo_estimado,
      'diagnostico', r.setor || ': ' || r.funcionarios_afetados || '/' || r.total_setor ||
        ' afetados, ' || r.atestados || ' atestados, ' || r.total_dias || ' dias',
      'causa_provavel', CASE
        WHEN r.funcionarios_afetados::NUMERIC / NULLIF(r.total_setor, 0) > 0.5 THEN 'Mais de 50% afetado — possivel problema ambiental ou de gestao'
        ELSE 'Concentrado em poucos — verificar casos individuais'
      END,
      'plano_acao', jsonb_build_array(
        jsonb_build_object('acao', 'Reuniao com lideranca sobre absenteismo', 'responsavel', 'rh', 'prazo_dias', 7),
        jsonb_build_object('acao', 'Analise ergonomica (AET)', 'responsavel', 'sst', 'prazo_dias', 30),
        jsonb_build_object('acao', 'Revisar condicoes de trabalho', 'responsavel', 'sst', 'prazo_dias', 14)
      )
    );
  END LOOP;

  -- ═══ 4. SST — CORRIGIDO: considerar carga incompleta ═══
  DECLARE
    v_sem_epi INTEGER;
    v_pct_carregado NUMERIC;
    v_total_entregas_api INTEGER := 41787;  -- total conhecido do BuscaEPI
    v_carregadas INTEGER;
  BEGIN
    SELECT COUNT(*) INTO v_carregadas FROM stg_buscaepi_entregas;
    v_pct_carregado := ROUND(v_carregadas::NUMERIC / GREATEST(v_total_entregas_api, 1) * 100, 1);

    SELECT COUNT(*) INTO v_sem_epi FROM rh_funcionarios f
    WHERE f.status IN ('ativo','ferias') AND f.elegivel_sst = TRUE
      AND NOT EXISTS (SELECT 1 FROM sst_epi_entrega e WHERE e.funcionario_id = f.id);

    -- Só alertar como problema real se carga >= 80%
    IF v_pct_carregado >= 80 AND v_sem_epi > 5 THEN
      v_problemas := v_problemas || jsonb_build_object(
        'tipo', 'sst_epi', 'prioridade', 2, 'risco', 'alto',
        'impacto_financeiro', v_sem_epi * 3000,  -- estimativa realista, não multa máxima
        'diagnostico', v_sem_epi || ' funcionarios sem EPI registrado (carga ' || v_pct_carregado || '% concluida)',
        'causa_provavel', 'Funcionarios que realmente nao receberam EPI — requer acao imediata',
        'plano_acao', jsonb_build_array(
          jsonb_build_object('acao', 'Verificar quais funcionarios nunca receberam EPI', 'responsavel', 'sst', 'prazo_dias', 7),
          jsonb_build_object('acao', 'Providenciar entrega imediata', 'responsavel', 'sst', 'prazo_dias', 14)
        )
      );
    ELSE
      -- Carga incompleta — informativo, não alarme
      v_excluidos := v_excluidos || jsonb_build_object(
        'motivo_exclusao', 'Carga BuscaEPI incompleta (' || v_pct_carregado || '%). ' ||
          v_sem_epi || ' sem EPI no sistema, mas maioria recebe fisicamente. Aguardar carga automatica.',
        'custo_removido', 0,
        'dados', jsonb_build_object(
          'entregas_carregadas', v_carregadas,
          'entregas_total_api', v_total_entregas_api,
          'pct_carregado', v_pct_carregado,
          'sem_epi_sistema', v_sem_epi
        )
      );
    END IF;
  END;

  -- Ordenar
  SELECT COALESCE(jsonb_agg(elem ORDER BY (elem->>'prioridade')::INTEGER, (elem->>'impacto_financeiro')::NUMERIC DESC), '[]'::JSONB)
  INTO v_problemas FROM jsonb_array_elements(v_problemas) AS elem;

  RETURN jsonb_build_object(
    'total_problemas', jsonb_array_length(v_problemas),
    'problemas', v_problemas,
    'excluidos_por_contexto', v_excluidos,
    'total_excluidos', jsonb_array_length(v_excluidos),
    'resumo', jsonb_build_object(
      'impacto_total', (SELECT COALESCE(SUM((e->>'impacto_financeiro')::NUMERIC), 0) FROM jsonb_array_elements(v_problemas) e),
      'alto_risco', (SELECT COUNT(*) FROM jsonb_array_elements(v_problemas) e WHERE e->>'risco' = 'alto'),
      'acoes_total', (SELECT COALESCE(SUM(jsonb_array_length(e->'plano_acao')), 0) FROM jsonb_array_elements(v_problemas) e),
      'custo_excluido', (SELECT COALESCE(SUM((e->>'custo_removido')::NUMERIC), 0) FROM jsonb_array_elements(v_excluidos) e)
    ),
    'gerado_em', NOW()
  );
END;
$function$;

-- ── relatorio_atestados_dashboard() ── chamada por: relatorio_atestados.html
CREATE OR REPLACE FUNCTION public.relatorio_atestados_dashboard(p_ano integer DEFAULT (EXTRACT(year FROM CURRENT_DATE))::integer, p_mes integer DEFAULT NULL::integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador', 'rh') THEN
    RETURN jsonb_build_object('erro', 'Acesso restrito');
  END IF;

  RETURN jsonb_build_object(
    'resumo', (SELECT jsonb_build_object(
      'total', COUNT(*), 'dias', COALESCE(SUM(GREATEST(dias,0)),0), 'funcionarios', COUNT(DISTINCT funcionario),
      'media_dias', ROUND(AVG(GREATEST(dias,0)), 1)
    ) FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes)),

    'por_mes', COALESCE((SELECT jsonb_agg(row_to_json(x) ORDER BY x.mes) FROM (
      SELECT mes, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias, COUNT(DISTINCT funcionario) as func
      FROM vw_atestados_analise WHERE ano = p_ano AND mes IS NOT NULL AND mes <= 12
      GROUP BY mes ORDER BY mes
    ) x), '[]'::jsonb),

    'por_setor', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT setor, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias, COUNT(DISTINCT funcionario) as func
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND setor IS NOT NULL
      GROUP BY setor ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb),

    'por_cid', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT grupo_cid, cid, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND cid IS NOT NULL
      GROUP BY grupo_cid, cid ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb),

    'por_medico', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT medico_crm as crm, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias,
        COUNT(DISTINCT funcionario) as func, ROUND(AVG(GREATEST(dias,0)), 1) as media_dias
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND medico_crm IS NOT NULL
      GROUP BY medico_crm ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb),

    'por_local', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT local_atendimento, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND local_atendimento IS NOT NULL
      GROUP BY local_atendimento ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb),

    -- DIA DA SEMANA com mais atestados
    'por_dia_semana', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT 
        CASE EXTRACT(DOW FROM data_inicio)
          WHEN 0 THEN 'Domingo' WHEN 1 THEN 'Segunda' WHEN 2 THEN 'Terca'
          WHEN 3 THEN 'Quarta' WHEN 4 THEN 'Quinta' WHEN 5 THEN 'Sexta' WHEN 6 THEN 'Sabado'
        END as dia,
        EXTRACT(DOW FROM data_inicio)::INT as dow,
        COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND data_inicio IS NOT NULL
      GROUP BY EXTRACT(DOW FROM data_inicio) ORDER BY EXTRACT(DOW FROM data_inicio)
    ) x), '[]'::jsonb),

    -- MOTIVOS (categoria dos atestados)
    'por_categoria', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT categoria, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes) AND categoria IS NOT NULL
      GROUP BY categoria ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb),

    'top_funcionarios', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT funcionario, setor, COUNT(*) as atestados, COALESCE(SUM(GREATEST(dias,0)),0) as dias, ROUND(AVG(GREATEST(dias,0)),1) as media
      FROM vw_atestados_analise WHERE ano = p_ano AND (p_mes IS NULL OR mes = p_mes)
      GROUP BY funcionario, setor ORDER BY COUNT(*) DESC LIMIT 20
    ) x), '[]'::jsonb),

    'alertas_medico', COALESCE((SELECT jsonb_agg(row_to_json(x)) FROM (
      SELECT medico_crm as crm, local_atendimento as local, COUNT(*) as atestados,
        COALESCE(SUM(GREATEST(dias,0)),0) as dias, COUNT(DISTINCT funcionario) as func,
        ROUND(AVG(GREATEST(dias,0)), 1) as media_dias
      FROM vw_atestados_analise WHERE medico_crm IS NOT NULL AND ano >= p_ano - 1
      GROUP BY medico_crm, local_atendimento HAVING COUNT(*) >= 3
      ORDER BY COUNT(*) DESC
    ) x), '[]'::jsonb)
  );
END; $function$;

-- ── contar_notificacoes() ── chamada por: notificacoes.html
-- CORRIGIDO 2026-04-13: ORDER BY duplicado causava erro GROUP BY
CREATE OR REPLACE FUNCTION public.contar_notificacoes()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN jsonb_build_object(
    'total', (SELECT COUNT(*) FROM rh_notificacao WHERE lida = false),
    'criticas', (SELECT COUNT(*) FROM rh_notificacao WHERE lida = false AND criticidade IN ('critica','alta')),
    'recentes', COALESCE((
      SELECT jsonb_agg(sub ORDER BY sub.created_at DESC)
      FROM (
        SELECT id, tipo, titulo, criticidade, created_at
        FROM rh_notificacao WHERE lida = false
        ORDER BY created_at DESC LIMIT 10
      ) sub
    ), '[]'::jsonb)
  );
END; $function$;

-- ── ficha_completa() ── chamada por: ficha.html
CREATE OR REPLACE FUNCTION public.ficha_completa(p_funcionario_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE v_perfil TEXT; v_result JSONB;
BEGIN
  v_perfil := rh_perfil_atual();
  IF v_perfil NOT IN ('administrador','rh','gestor') THEN
    RETURN jsonb_build_object('erro', 'Acesso restrito');
  END IF;

  -- Dados cadastrais
  SELECT jsonb_build_object(
    'id', f.id, 'nome', f.nome_completo, 'cpf', f.cpf, 'matricula', f.matricula,
    'data_nascimento', f.data_nascimento, 'sexo', f.sexo, 'estado_civil', f.estado_civil,
    'telefone', f.telefone, 'email', f.email_pessoal,
    'data_admissao', f.data_admissao, 'data_desligamento', f.data_desligamento,
    'status', f.status, 'turno', f.turno, 'salario_base', f.salario_base,
    'cargo', c.nome, 'setor', s.nome, 'ghe', f.ghe,
    'cargo_confianca', f.cargo_confianca, 'jovem_aprendiz', f.jovem_aprendiz,
    'transporte_onibus', f.transporte_onibus, 'transporte_rota', f.transporte_rota,
    'tipo_vinculo', f.tipo_vinculo, 'carga_horaria', f.carga_horaria_semanal,
    'tempo_empresa_dias', (CURRENT_DATE - f.data_admissao),
    'tempo_empresa_meses', ROUND((CURRENT_DATE - f.data_admissao)::NUMERIC / 30.44, 1)
  ) INTO v_result
  FROM rh_funcionarios f LEFT JOIN rh_cargos c ON c.id = f.cargo_id LEFT JOIN rh_setores s ON s.id = f.setor_id
  WHERE f.id = p_funcionario_id;

  IF v_result IS NULL THEN RETURN jsonb_build_object('erro', 'Funcionario nao encontrado'); END IF;

  -- Score motor v2 (ultimo)
  v_result := v_result || jsonb_build_object('score', COALESCE((
    SELECT jsonb_build_object('final', score_final, 'presenca', score_presenca, 'producao', score_producao,
      'treinamento', score_treinamento, 'estabilidade', score_estabilidade, 'epi', score_epi,
      'qualidade', score_qualidade, 'classificacao', classificacao, 'confiabilidade', confiabilidade,
      'fontes', fontes_ativas, 'periodo', periodo_referencia)
    FROM motor_score WHERE funcionario_id = p_funcionario_id ORDER BY periodo_referencia DESC LIMIT 1
  ), '{}'::jsonb));

  -- Atestados (resumo sem CID para gestor)
  IF v_perfil IN ('administrador','rh') THEN
    v_result := v_result || jsonb_build_object('atestados', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'dias_total', (SELECT COALESCE(SUM(GREATEST(dias,0)),0) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'ultimos_6m', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado' AND data_inicio >= CURRENT_DATE - 180),
      'ultimo', (SELECT data_inicio FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado' ORDER BY data_inicio DESC LIMIT 1),
      'recentes', COALESCE((SELECT jsonb_agg(jsonb_build_object('data', data_inicio, 'dias', dias, 'obs', observacoes) ORDER BY data_inicio DESC)
        FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado' AND data_inicio >= CURRENT_DATE - 365 LIMIT 10), '[]'::jsonb)
    ));
  ELSE
    v_result := v_result || jsonb_build_object('atestados', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'dias_total', (SELECT COALESCE(SUM(GREATEST(dias,0)),0) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'restrito', true
    ));
  END IF;

  -- Ponto (ultimos 30 dias)
  v_result := v_result || jsonb_build_object('ponto', jsonb_build_object(
    'dias_registrados', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30),
    'dias_trabalhados', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30 AND justificativa IS NULL AND (entrada1 IS NOT NULL OR saida2 IS NOT NULL)),
    'faltas_30d', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30 AND justificativa = 'Falta'),
    'ferias_30d', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30 AND justificativa ILIKE '%eria%')
  ));

  -- EPI (ultimas entregas)
  v_result := v_result || jsonb_build_object('epi', jsonb_build_object(
    'total_entregas', (SELECT COUNT(*) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id),
    'ultima_entrega', (SELECT MAX(data_entrega) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id),
    'entregas_12m', (SELECT COUNT(*) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id AND data_entrega >= CURRENT_DATE - 365)
  ));

  -- Treinamentos
  v_result := v_result || jsonb_build_object('treinamentos', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id),
    'validos', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id AND (data_validade IS NULL OR data_validade >= CURRENT_DATE)),
    'vencidos', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id AND data_validade < CURRENT_DATE)
  ));

  -- ASOs
  v_result := v_result || jsonb_build_object('asos', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM sst_aso WHERE funcionario_id = p_funcionario_id),
    'ultimo_tipo', (SELECT tipo_exame FROM sst_aso WHERE funcionario_id = p_funcionario_id ORDER BY data_exame DESC LIMIT 1),
    'ultima_data', (SELECT data_exame FROM sst_aso WHERE funcionario_id = p_funcionario_id ORDER BY data_exame DESC LIMIT 1),
    'validade', (SELECT data_validade FROM sst_aso WHERE funcionario_id = p_funcionario_id ORDER BY data_exame DESC LIMIT 1)
  ));

  -- Beneficios
  v_result := v_result || jsonb_build_object('beneficios', COALESCE((
    SELECT jsonb_agg(jsonb_build_object('tipo', tipo, 'status', status, 'valor', valor) ORDER BY tipo)
    FROM (SELECT DISTINCT ON (tipo) tipo, status, valor FROM rh_beneficio WHERE funcionario_id = p_funcionario_id ORDER BY tipo, created_at DESC) sub
  ), '[]'::jsonb));

  -- Documentos digitais (contagem por categoria)
  v_result := v_result || jsonb_build_object('documentos', COALESCE((
    SELECT jsonb_agg(jsonb_build_object('categoria', categoria, 'total', total))
    FROM (SELECT categoria, COUNT(*) as total FROM rh_documento_digital WHERE funcionario_id = p_funcionario_id GROUP BY categoria ORDER BY total DESC) sub
  ), '[]'::jsonb));

  -- Pendencias do agente
  v_result := v_result || jsonb_build_object('pendencias', COALESCE((
    SELECT jsonb_agg(jsonb_build_object('tipo', tipo, 'criticidade', criticidade, 'descricao', descricao, 'status', status))
    FROM agente_pendencia WHERE referencia_id = p_funcionario_id AND status = 'aberta'
  ), '[]'::jsonb));

  -- Ferias (calculo basico)
  v_result := v_result || jsonb_build_object('ferias', jsonb_build_object(
    'periodo_aquisitivo_inicio', (SELECT CASE WHEN data_admissao IS NOT NULL THEN
      data_admissao + (INTERVAL '1 year' * FLOOR((CURRENT_DATE - data_admissao)::NUMERIC / 365.25))
      ELSE NULL END FROM rh_funcionarios WHERE id = p_funcionario_id),
    'dias_direito', 30,
    'dias_gozados_ponto', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND justificativa ILIKE '%eria%' AND data >= CURRENT_DATE - 365)
  ));

  RETURN v_result;
END;
$function$;

-- ── ficha_360_colaborador() ── chamada por: ficha_360.html
CREATE OR REPLACE FUNCTION public.ficha_360_colaborador(p_funcionario_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE 
  v_perfil TEXT;
  v_result JSONB;
  v_s BOOLEAN; -- pode_ver_sensivel
BEGIN
  v_perfil := rh_perfil_atual();
  v_s := v_perfil IN ('administrador','rh');

  -- B1: CADASTRAIS
  SELECT jsonb_build_object(
    'id', f.id, 'nome', f.nome_completo,
    'cpf', CASE WHEN v_s THEN f.cpf ELSE '***' || RIGHT(f.cpf, 4) END,
    'matricula', f.matricula, 'data_nascimento', f.data_nascimento, 'sexo', f.sexo,
    'estado_civil', f.estado_civil, 'telefone', f.telefone, 'email', f.email_pessoal,
    'endereco', f.endereco,
    'pis_pasep', CASE WHEN v_s THEN f.pis_pasep ELSE NULL END,
    'ctps', CASE WHEN v_s THEN jsonb_build_object('numero', f.ctps_numero, 'serie', f.ctps_serie) ELSE NULL END,
    'empresa', e.razao_social, 'filial', fl.nome_fantasia,
    'setor', s.nome, 'setor_id', f.setor_id,
    'cargo', c.nome, 'cargo_id', f.cargo_id, 'cbo', c.cbo,
    'tipo_vinculo', f.tipo_vinculo, 'modalidade_contrato', f.modalidade_contrato,
    'data_admissao', f.data_admissao, 'data_desligamento', f.data_desligamento,
    'salario_base', CASE WHEN v_s OR v_perfil = 'gestor_confianca' THEN f.salario_base ELSE NULL END,
    'turno', f.turno, 'carga_horaria', f.carga_horaria_semanal,
    'status', f.status, 'grau_risco', f.grau_risco_sst,
    'gestor_id', f.gestor_id,
    'gestor_nome', (SELECT nome_completo FROM rh_funcionarios WHERE id = f.gestor_id),
    'centro_custo_id', f.centro_custo_id,
    'cargo_confianca', f.cargo_confianca, 'jovem_aprendiz', f.jovem_aprendiz,
    'ghe', f.ghe,
    'transporte_onibus', f.transporte_onibus, 'transporte_rota', f.transporte_rota, 'transporte_custo', f.transporte_custo,
    'afastamento', CASE WHEN f.tipo_afastamento IS NOT NULL THEN jsonb_build_object('tipo', f.tipo_afastamento, 'inicio', f.afastamento_inicio, 'previsao_retorno', f.afastamento_previsao_retorno, 'origem', f.afastamento_origem) ELSE NULL END,
    'tempo_empresa_dias', (CURRENT_DATE - f.data_admissao),
    'experiencia', CASE WHEN (CURRENT_DATE - f.data_admissao) <= 90 THEN jsonb_build_object('dias', (CURRENT_DATE - f.data_admissao), 'venc_45', f.data_admissao + 45, 'venc_90', f.data_admissao + 90, 'em_experiencia', true) ELSE jsonb_build_object('em_experiencia', false) END,
    'observacoes', f.observacoes, 'created_at', f.created_at, 'updated_at', f.updated_at
  ) INTO v_result
  FROM rh_funcionarios f
  LEFT JOIN rh_cargos c ON c.id = f.cargo_id
  LEFT JOIN rh_setores s ON s.id = f.setor_id
  LEFT JOIN rh_empresas e ON e.id = f.empresa_id
  LEFT JOIN rh_filiais fl ON fl.id = f.filial_id
  WHERE f.id = p_funcionario_id;

  IF v_result IS NULL THEN RETURN jsonb_build_object('erro', 'Funcionario nao encontrado'); END IF;

  -- B2: SCORE
  v_result := v_result || jsonb_build_object('score', COALESCE((SELECT jsonb_build_object('final', score_final, 'presenca', score_presenca, 'producao', score_producao, 'treinamento', score_treinamento, 'estabilidade', score_estabilidade, 'epi', score_epi, 'qualidade', score_qualidade, 'classificacao', classificacao, 'confiabilidade', confiabilidade, 'periodo', periodo_referencia) FROM motor_score WHERE funcionario_id = p_funcionario_id ORDER BY periodo_referencia DESC LIMIT 1), '{}'::jsonb));

  -- B3: ASOs
  v_result := v_result || jsonb_build_object('asos', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM sst_aso WHERE funcionario_id = p_funcionario_id),
    'ultimo', (SELECT jsonb_build_object('tipo', tipo_exame, 'data', data_exame, 'validade', data_validade, 'resultado', resultado, 'medico', medico_responsavel, 'restricoes', restricoes) FROM sst_aso WHERE funcionario_id = p_funcionario_id ORDER BY data_exame DESC LIMIT 1),
    'vencido', (SELECT data_validade < CURRENT_DATE FROM sst_aso WHERE funcionario_id = p_funcionario_id ORDER BY data_exame DESC LIMIT 1),
    'lista', COALESCE((SELECT jsonb_agg(jsonb_build_object('id', id, 'tipo', tipo_exame, 'data', data_exame, 'validade', data_validade, 'resultado', resultado, 'medico', medico_responsavel) ORDER BY data_exame DESC) FROM sst_aso WHERE funcionario_id = p_funcionario_id), '[]'::jsonb)
  ));

  -- B4: TREINAMENTOS
  v_result := v_result || jsonb_build_object('treinamentos', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id),
    'validos', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id AND (data_validade IS NULL OR data_validade >= CURRENT_DATE)),
    'vencidos', (SELECT COUNT(*) FROM sst_treinamento WHERE funcionario_id = p_funcionario_id AND data_validade < CURRENT_DATE),
    'lista', COALESCE((SELECT jsonb_agg(jsonb_build_object('id', t.id, 'tipo', tt.nome, 'data', t.data_realizacao, 'validade', t.data_validade, 'instrutor', t.instrutor, 'carga_horaria', t.carga_horaria, 'vencido', (t.data_validade IS NOT NULL AND t.data_validade < CURRENT_DATE)) ORDER BY t.data_realizacao DESC) FROM sst_treinamento t LEFT JOIN sst_tipo_treinamento tt ON tt.id = t.tipo_treinamento_id WHERE t.funcionario_id = p_funcionario_id), '[]'::jsonb)
  ));

  -- B5: EPI (catalogo_epi_id, ca_numero)
  v_result := v_result || jsonb_build_object('epi', jsonb_build_object(
    'total_entregas', (SELECT COUNT(*) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id),
    'ultima_entrega', (SELECT MAX(data_entrega) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id),
    'entregas_12m', (SELECT COUNT(*) FROM sst_epi_entrega WHERE funcionario_id = p_funcionario_id AND data_entrega >= CURRENT_DATE - 365),
    'recentes', COALESCE((SELECT jsonb_agg(sub ORDER BY sub.dt DESC) FROM (SELECT ee.id, ce.descricao AS epi, ce.ca_numero AS ca, ee.data_entrega AS dt, ee.quantidade FROM sst_epi_entrega ee LEFT JOIN sst_catalogo_epi ce ON ce.id = ee.catalogo_epi_id WHERE ee.funcionario_id = p_funcionario_id AND ee.data_entrega >= CURRENT_DATE - 365 ORDER BY ee.data_entrega DESC LIMIT 20) sub), '[]'::jsonb)
  ));

  -- B6: OCORRENCIAS
  IF v_s THEN
    v_result := v_result || jsonb_build_object('ocorrencias', jsonb_build_object(
      'total', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id),
      'atestados', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'dias_atestado', (SELECT COALESCE(SUM(GREATEST(dias,0)),0) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'),
      'advertencias', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo IN ('advertencia','suspensao')),
      'recentes', COALESCE((SELECT jsonb_agg(sub ORDER BY sub.dt DESC) FROM (SELECT tipo, data_inicio AS dt, dias, observacoes AS descricao FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id ORDER BY data_inicio DESC LIMIT 15) sub), '[]'::jsonb)
    ));
  ELSE
    v_result := v_result || jsonb_build_object('ocorrencias', jsonb_build_object('total', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id), 'atestados', (SELECT COUNT(*) FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo = 'atestado'), 'restrito', true));
  END IF;

  -- B7: PONTO (colunas TEXT)
  v_result := v_result || jsonb_build_object('ponto', jsonb_build_object(
    'registros_30d', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30),
    'faltas_30d', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30 AND justificativa = 'Falta'),
    'com_extra_30d', (SELECT COUNT(*) FROM rh_ponto WHERE funcionario_id = p_funcionario_id AND data >= CURRENT_DATE - 30 AND horas_extra_50 IS NOT NULL AND horas_extra_50 <> '' AND horas_extra_50 <> '00:00'),
    'ultimos_dias', COALESCE((SELECT jsonb_agg(sub ORDER BY sub.data DESC) FROM (SELECT data, dia_semana, entrada1, saida1, entrada2, saida2, horas_normais, horas_faltas, horas_extra_50, horas_extra_100, justificativa FROM rh_ponto WHERE funcionario_id = p_funcionario_id ORDER BY data DESC LIMIT 15) sub), '[]'::jsonb),
    'totais', COALESCE((SELECT jsonb_agg(sub ORDER BY sub.periodo DESC) FROM (SELECT periodo, dias_trabalhados, dias_falta, horas_faltas, horas_extra_50, horas_extra_100, horas_noturnas FROM rh_ponto_totais WHERE funcionario_id = p_funcionario_id ORDER BY periodo DESC LIMIT 6) sub), '[]'::jsonb)
  ));

  -- B8: FERIAS (dias_gozados, dias_vendidos, abono_pecuniario, periodo_aquisitivo_inicio/fim)
  v_result := v_result || jsonb_build_object('ferias', jsonb_build_object(
    'total_periodos', (SELECT COUNT(*) FROM rh_ferias WHERE funcionario_id = p_funcionario_id),
    'vencidas', (SELECT COUNT(*) FROM rh_ferias WHERE funcionario_id = p_funcionario_id AND status = 'vencida'),
    'proxima', (SELECT jsonb_build_object('inicio', data_inicio, 'fim', data_fim, 'dias', dias_gozados, 'status', status) FROM rh_ferias WHERE funcionario_id = p_funcionario_id AND data_inicio >= CURRENT_DATE ORDER BY data_inicio LIMIT 1),
    'lista', COALESCE((SELECT jsonb_agg(jsonb_build_object('periodo_inicio', periodo_aquisitivo_inicio, 'periodo_fim', periodo_aquisitivo_fim, 'inicio', data_inicio, 'fim', data_fim, 'dias', dias_gozados, 'vendidos', dias_vendidos, 'abono', abono_pecuniario, 'status', status) ORDER BY data_inicio DESC) FROM rh_ferias WHERE funcionario_id = p_funcionario_id), '[]'::jsonb)
  ));

  -- B9: BENEFICIOS
  v_result := v_result || jsonb_build_object('beneficios', COALESCE((SELECT jsonb_agg(jsonb_build_object('tipo', tipo, 'status', status, 'valor', valor, 'data_inicio', data_inicio) ORDER BY tipo) FROM (SELECT DISTINCT ON (tipo) tipo, status, valor, data_inicio FROM rh_beneficio WHERE funcionario_id = p_funcionario_id ORDER BY tipo, created_at DESC) sub), '[]'::jsonb));

  -- B10: DOCUMENTOS
  v_result := v_result || jsonb_build_object('documentos', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM rh_documento_digital WHERE funcionario_id = p_funcionario_id),
    'por_categoria', COALESCE((SELECT jsonb_agg(jsonb_build_object('categoria', categoria, 'total', total)) FROM (SELECT categoria, COUNT(*) as total FROM rh_documento_digital WHERE funcionario_id = p_funcionario_id GROUP BY categoria ORDER BY total DESC) sub), '[]'::jsonb),
    'recentes', COALESCE((SELECT jsonb_agg(sub ORDER BY sub.dt DESC) FROM (SELECT nome_arquivo, categoria, created_at AS dt FROM rh_documento_digital WHERE funcionario_id = p_funcionario_id ORDER BY created_at DESC LIMIT 10) sub), '[]'::jsonb)
  ));

  -- B11: CUSTOS
  IF v_s THEN
    v_result := v_result || jsonb_build_object('custos', jsonb_build_object(
      'salario', (SELECT salario_base FROM rh_funcionarios WHERE id = p_funcionario_id),
      'transporte_mensal', (SELECT transporte_custo FROM rh_funcionarios WHERE id = p_funcionario_id),
      'beneficios_total', (SELECT COALESCE(SUM(valor),0) FROM rh_beneficio WHERE funcionario_id = p_funcionario_id AND status = 'ativo'),
      'sst_12m', (SELECT COALESCE(SUM(valor_total_estimado),0) FROM sst_fatura_historico_colaborador WHERE funcionario_id = p_funcionario_id AND data_realizacao >= CURRENT_DATE - 365),
      'historico', COALESCE((SELECT jsonb_agg(jsonb_build_object('competencia', competencia, 'custo_total', custo_total) ORDER BY competencia DESC) FROM rh_custo_colaborador WHERE funcionario_id = p_funcionario_id LIMIT 6), '[]'::jsonb)
    ));
  ELSE
    v_result := v_result || jsonb_build_object('custos', jsonb_build_object('restrito', true));
  END IF;

  -- B12: HISTORICO (vigencia_inicio)
  v_result := v_result || jsonb_build_object('historico_cargo', COALESCE((SELECT jsonb_agg(jsonb_build_object('cargo', c.nome, 'data', h.vigencia_inicio, 'motivo', h.motivo) ORDER BY h.vigencia_inicio DESC) FROM rh_hist_cargo h LEFT JOIN rh_cargos c ON c.id = h.cargo_id WHERE h.funcionario_id = p_funcionario_id), '[]'::jsonb));
  v_result := v_result || jsonb_build_object('historico_salario', COALESCE((SELECT jsonb_agg(jsonb_build_object('valor', h.salario_novo, 'anterior', h.salario_anterior, 'data', h.vigencia_inicio, 'motivo', h.motivo) ORDER BY h.vigencia_inicio DESC) FROM rh_hist_salario h WHERE h.funcionario_id = p_funcionario_id), '[]'::jsonb));
  v_result := v_result || jsonb_build_object('historico_setor', COALESCE((SELECT jsonb_agg(jsonb_build_object('setor', s.nome, 'data', h.vigencia_inicio, 'motivo', h.motivo) ORDER BY h.vigencia_inicio DESC) FROM rh_hist_setor h LEFT JOIN rh_setores s ON s.id = h.setor_id WHERE h.funcionario_id = p_funcionario_id), '[]'::jsonb));

  -- B13: ACIDENTES (dias_afastamento)
  v_result := v_result || jsonb_build_object('acidentes', jsonb_build_object(
    'total', (SELECT COUNT(*) FROM sst_acidente WHERE funcionario_id = p_funcionario_id),
    'lista', COALESCE((SELECT jsonb_agg(jsonb_build_object('id', id, 'tipo', tipo_acidente, 'data', data_ocorrencia, 'gravidade', gravidade, 'descricao', descricao, 'dias_afastamento', dias_afastamento, 'cat_emitida', cat_emitida) ORDER BY data_ocorrencia DESC) FROM sst_acidente WHERE funcionario_id = p_funcionario_id), '[]'::jsonb)
  ));

  -- B14: PENDENCIAS (severidade, created_at)
  v_result := v_result || jsonb_build_object('pendencias', COALESCE((SELECT jsonb_agg(jsonb_build_object('tipo', tipo, 'titulo', titulo, 'severidade', severidade, 'status', status, 'criado_em', created_at)) FROM orch_alerta WHERE funcionario_id = p_funcionario_id AND status = 'aberto'), '[]'::jsonb));

  -- B15: TIMELINE
  v_result := v_result || jsonb_build_object('timeline', COALESCE((
    SELECT jsonb_agg(jsonb_build_object('data', t.data, 'tipo', t.tipo, 'titulo', t.titulo, 'detalhe', t.detalhe) ORDER BY t.data DESC) FROM (
      SELECT data_admissao AS data, 'admissao' AS tipo, 'Admissao' AS titulo, status AS detalhe FROM rh_funcionarios WHERE id = p_funcionario_id
      UNION ALL SELECT h.vigencia_inicio, 'cargo', 'Mudanca de Cargo', c.nome FROM rh_hist_cargo h LEFT JOIN rh_cargos c ON c.id = h.cargo_id WHERE h.funcionario_id = p_funcionario_id
      UNION ALL SELECT h.vigencia_inicio, 'setor', 'Mudanca de Setor', s.nome FROM rh_hist_setor h LEFT JOIN rh_setores s ON s.id = h.setor_id WHERE h.funcionario_id = p_funcionario_id
      UNION ALL SELECT h.vigencia_inicio, 'salario', 'Alteracao Salarial', 'R$ ' || h.salario_anterior::TEXT || ' -> R$ ' || h.salario_novo::TEXT FROM rh_hist_salario h WHERE h.funcionario_id = p_funcionario_id
      UNION ALL SELECT data_exame, 'aso', 'ASO ' || tipo_exame, resultado FROM sst_aso WHERE funcionario_id = p_funcionario_id
      UNION ALL SELECT t.data_realizacao, 'treinamento', 'Treinamento', tt.nome FROM sst_treinamento t LEFT JOIN sst_tipo_treinamento tt ON tt.id = t.tipo_treinamento_id WHERE t.funcionario_id = p_funcionario_id
      UNION ALL SELECT data_inicio, 'ferias', 'Ferias', dias_gozados || ' dias' FROM rh_ferias WHERE funcionario_id = p_funcionario_id
      UNION ALL SELECT data_ocorrencia, 'acidente', 'Acidente ' || tipo_acidente, gravidade FROM sst_acidente WHERE funcionario_id = p_funcionario_id
      UNION ALL SELECT data_inicio, tipo, CASE tipo WHEN 'advertencia' THEN 'Advertencia' WHEN 'suspensao' THEN 'Suspensao' WHEN 'atestado' THEN 'Atestado' ELSE tipo END, dias || ' dia(s)' FROM rh_ocorrencias WHERE colaborador_id = p_funcionario_id AND tipo IN ('advertencia','suspensao','atestado')
      UNION ALL SELECT data_desligamento, 'desligamento', 'Desligamento', status FROM rh_funcionarios WHERE id = p_funcionario_id AND data_desligamento IS NOT NULL
    ) t WHERE t.data IS NOT NULL LIMIT 50
  ), '[]'::jsonb));

  v_result := v_result || jsonb_build_object('_perfil', v_perfil, '_pode_ver_sensivel', v_s, '_gerado_em', NOW());
  RETURN v_result;
END;
$function$;


-- ════════════════════════════════════════════════════════════════════════════
-- GRUPO 2 -- RPCs de CRUD
-- ════════════════════════════════════════════════════════════════════════════

-- ── registrar_acompanhamento() ── chamada por: painel_gestao.html
CREATE OR REPLACE FUNCTION public.registrar_acompanhamento(p_funcionario_id uuid, p_tipo text, p_assunto text, p_descricao text DEFAULT NULL::text, p_resultado text DEFAULT NULL::text, p_proxima_acao text DEFAULT NULL::text, p_prazo date DEFAULT NULL::date, p_duracao integer DEFAULT NULL::integer, p_confidencial boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE v_gestor_id UUID; v_id UUID;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh','gestor_confianca') THEN
    RETURN jsonb_build_object('erro', 'Apenas gestor de confianca, RH ou admin podem registrar acompanhamentos');
  END IF;

  v_gestor_id := rh_funcionario_atual_id();

  INSERT INTO rh_acompanhamento (funcionario_id, gestor_id, tipo, assunto, descricao, resultado, proxima_acao, prazo_proxima_acao, duracao_minutos, confidencial)
  VALUES (p_funcionario_id, v_gestor_id, p_tipo, p_assunto, p_descricao, p_resultado, p_proxima_acao, p_prazo, p_duracao, p_confidencial)
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('id', v_id, 'ok', true);
END; $function$;


-- ════════════════════════════════════════════════════════════════════════════
-- GRUPO 3 -- RPCs de ETL / Importacao
-- ════════════════════════════════════════════════════════════════════════════

-- ── rh_cpf_map() ── chamada por: ingestores Python (mapeamento CPF)
CREATE OR REPLACE FUNCTION public.rh_cpf_map()
 RETURNS TABLE(id uuid, cpf character varying)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT id, cpf FROM rh_funcionarios WHERE cpf IS NOT NULL AND status != 'desligado';
$function$;

-- ── file_check_and_log() ── chamada por: ingestor_atak.py
CREATE OR REPLACE FUNCTION public.file_check_and_log(p_hash text, p_nome text, p_status text DEFAULT 'processado'::text, p_registros integer DEFAULT 0, p_erro text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE v_exists BOOLEAN;
BEGIN
  -- Verificar se hash já existe
  SELECT EXISTS (SELECT 1 FROM file_ingest_log WHERE hash_arquivo = p_hash AND status = 'processado')
  INTO v_exists;

  -- Se é novo, registrar
  IF NOT v_exists THEN
    INSERT INTO file_ingest_log (caminho, nome_arquivo, tipo_arquivo, hash_arquivo, categoria, status, registros_extraidos, erro_detalhe, processado_em)
    VALUES ('atak_ingest', p_nome, SPLIT_PART(p_nome, '.', -1), p_hash, 'outro', p_status, p_registros, p_erro, NOW())
    ON CONFLICT (hash_arquivo) DO NOTHING;
  END IF;

  RETURN v_exists;
END;
$function$;

-- ── file_ingest_atestados() ── chamada por: importador_local.py
CREATE OR REPLACE FUNCTION public.file_ingest_atestados(p_registros jsonb)
 RETURNS TABLE(total integer, inseridos integer, erros integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  r JSONB;
  v_total INTEGER := 0; v_ins INTEGER := 0; v_err INTEGER := 0;
BEGIN
  FOR r IN SELECT jsonb_array_elements(p_registros) LOOP
    BEGIN
      INSERT INTO stg_convenia_documentos (id_origem, origem_sistema, payload_json, hash_registro)
      VALUES (
        r->>'id_origem',
        'manual',
        r->'payload_json',
        r->>'hash_registro'
      )
      ON CONFLICT (id_origem, hash_registro) DO NOTHING;
      v_ins := v_ins + 1;
    EXCEPTION WHEN OTHERS THEN
      v_err := v_err + 1;
    END;
    v_total := v_total + 1;
  END LOOP;
  RETURN QUERY SELECT v_total, v_ins, v_err;
END;
$function$;

-- ── importar_producao_atak() ── chamada por: ingestor_atak.py
CREATE OR REPLACE FUNCTION public.importar_producao_atak(p_registros jsonb)
 RETURNS TABLE(total integer, inseridos integer, erros integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE r JSONB; v_total INTEGER := 0; v_ins INTEGER := 0; v_err INTEGER := 0;
BEGIN
  FOR r IN SELECT jsonb_array_elements(p_registros) LOOP
    BEGIN
      INSERT INTO producao_real (data, produto_codigo, produto_nome, quantidade_kg, quantidade_pcs, unidade, producao_hora)
      VALUES (
        (r->>'data')::DATE,
        r->>'produto_codigo',
        r->>'produto_nome',
        COALESCE((r->>'quantidade_kg')::NUMERIC, 0),
        COALESCE((r->>'quantidade_pcs')::INTEGER, 0),
        COALESCE(r->>'unidade', 'KG'),
        COALESCE((r->>'producao_hora')::NUMERIC, 0)
      );
      v_ins := v_ins + 1;
    EXCEPTION WHEN OTHERS THEN v_err := v_err + 1;
    END;
    v_total := v_total + 1;
  END LOOP;
  RETURN QUERY SELECT v_total, v_ins, v_err;
END;
$function$;

-- ── importar_ocorrencias_lote() ── chamada por: ingestor_atestados.py
CREATE OR REPLACE FUNCTION public.importar_ocorrencias_lote(p_dados jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE r JSONB; v_func_id UUID; v_count INT := 0; v_sem INT := 0;
BEGIN
  FOR r IN SELECT * FROM jsonb_array_elements(p_dados)
  LOOP
    v_func_id := NULL;
    -- Match por nome (primeiras palavras)
    DECLARE v_nome TEXT; v_words TEXT[];
    BEGIN
      v_nome := UPPER(COALESCE(r->>'nome_parseado', ''));
      v_words := string_to_array(v_nome, ' ');
      -- Tentar match com 2 primeiras palavras significativas
      IF array_length(v_words, 1) >= 2 THEN
        SELECT id INTO v_func_id FROM rh_funcionarios
        WHERE UPPER(nome_completo) LIKE '%' || v_words[1] || '%' || v_words[2] || '%'
          AND status IN ('ativo','afastado','ferias') LIMIT 1;
      END IF;
      IF v_func_id IS NULL AND array_length(v_words, 1) >= 1 AND LENGTH(v_words[1]) > 3 THEN
        SELECT id INTO v_func_id FROM rh_funcionarios
        WHERE UPPER(nome_completo) LIKE '%' || v_words[1] || '%'
          AND status IN ('ativo','afastado','ferias','desligado') LIMIT 1;
      END IF;
    END;

    IF v_func_id IS NULL THEN v_sem := v_sem + 1; CONTINUE; END IF;

    INSERT INTO rh_ocorrencias (colaborador_id, tipo, data_inicio, data_fim, dias, competencia, observacoes, arquivo_nome)
    VALUES (v_func_id, COALESCE(r->>'tipo', 'atestado'),
      NULLIF(r->>'data_inicio','')::DATE, NULLIF(r->>'data_fim','')::DATE,
      COALESCE((r->>'dias')::INT, 1), r->>'competencia', r->>'observacoes', r->>'arquivo_nome')
    ON CONFLICT DO NOTHING;
    v_count := v_count + 1;
  END LOOP;
  RETURN jsonb_build_object('importados', v_count, 'sem_match', v_sem);
END; $function$;
