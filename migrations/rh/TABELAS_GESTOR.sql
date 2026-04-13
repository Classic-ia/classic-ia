-- ════════════════════════════════════════════════════════════════════════════
-- TABELAS GESTOR — Motor Score, Agentes IA, Orquestracao, Acoes, Armarios
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao)
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Criticidade: ALTA
-- Dependencias: rh_funcionarios, rh_setores (FUNDACAO_BANCO_v2.sql)
--
-- Tabelas incluidas (23):
--   motor_execucao, motor_score, motor_alerta, motor_indicador, motor_padrao,
--   agente_execucao, agente_analise, agente_pendencia,
--   agente_integracao_status, agente_qualidade_incidente,
--   orch_alerta, orch_job, orch_job_log, orch_tarefa,
--   acao_plano, acao_item, acao_comentario, acao_transicao,
--   rh_acoes, rh_audit_trail,
--   core_auditoria, core_criticidade, golive_checklist, golive_relatorio_diario
-- ════════════════════════════════════════════════════════════════════════════

-- ── MOTOR SCORE ENGINE ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.motor_execucao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo text NOT NULL,
  inicio timestamptz NOT NULL DEFAULT now(),
  fim timestamptz,
  status text NOT NULL DEFAULT 'executando',
  registros_processados integer DEFAULT 0,
  detalhes jsonb DEFAULT '{}',
  erro text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.motor_score (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  funcionario_id uuid NOT NULL,
  funcionario_nome text NOT NULL,
  setor_nome text,
  cargo_nome text,
  score_presenca numeric(5,2) DEFAULT 0,
  score_producao numeric(5,2) DEFAULT 0,
  score_treinamento numeric(5,2) DEFAULT 0,
  score_estabilidade numeric(5,2) DEFAULT 0,
  score_epi numeric(5,2) DEFAULT 0,
  score_final numeric(5,2) NOT NULL DEFAULT 0,
  classificacao text NOT NULL,
  fatores text[] DEFAULT '{}',
  periodo_referencia text NOT NULL,
  created_at timestamptz DEFAULT now(),
  score_qualidade numeric(5,2) DEFAULT 0,
  confiabilidade text DEFAULT 'indeterminada',
  cobertura_presenca numeric(5,2) DEFAULT 0,
  cobertura_producao numeric(5,2) DEFAULT 0,
  cobertura_treinamento numeric(5,2) DEFAULT 0,
  cobertura_epi numeric(5,2) DEFAULT 0,
  fontes_ativas integer DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.motor_alerta (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  tipo text NOT NULL,
  prioridade text NOT NULL,
  titulo text NOT NULL,
  descricao text NOT NULL,
  referencia_tipo text NOT NULL,
  referencia_id uuid,
  referencia_nome text NOT NULL,
  dados jsonb DEFAULT '{}',
  status text NOT NULL DEFAULT 'aberto',
  responsavel text,
  resolucao text,
  resolvido_em timestamptz,
  chave_dedup text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.motor_indicador (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  categoria text NOT NULL,
  indicador text NOT NULL,
  periodo_inicio date NOT NULL,
  periodo_fim date NOT NULL,
  valor numeric NOT NULL,
  unidade text NOT NULL DEFAULT '%',
  referencia_tipo text,
  referencia_id uuid,
  referencia_nome text,
  meta numeric,
  tendencia text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.motor_padrao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  tipo text NOT NULL,
  referencia_tipo text NOT NULL,
  referencia_id uuid,
  referencia_nome text NOT NULL,
  periodo text NOT NULL,
  descricao text NOT NULL,
  dados jsonb DEFAULT '{}',
  severidade text NOT NULL,
  created_at timestamptz DEFAULT now()
);

-- ── AGENTES IA ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.agente_execucao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  agente text NOT NULL,
  inicio timestamptz NOT NULL DEFAULT now(),
  fim timestamptz,
  status text NOT NULL DEFAULT 'executando',
  incidentes_gerados integer DEFAULT 0,
  incidentes_resolvidos integer DEFAULT 0,
  duracao_ms integer,
  erro text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.agente_analise (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  agente text NOT NULL,
  tipo text NOT NULL,
  subtipo text,
  criticidade text NOT NULL,
  impacto text NOT NULL,
  referencia_tipo text NOT NULL,
  referencia_id uuid,
  referencia_nome text NOT NULL,
  setor_nome text,
  descricao text NOT NULL,
  recomendacao text,
  plano_acao text,
  prazo_dias integer,
  responsavel_perfil text,
  status text NOT NULL DEFAULT 'aberto',
  dados jsonb DEFAULT '{}',
  chave_dedup text,
  detectado_em timestamptz DEFAULT now(),
  resolvido_em timestamptz,
  resolucao text,
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.agente_pendencia (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  tipo text NOT NULL,
  criticidade text NOT NULL,
  origem text NOT NULL,
  referencia_tipo text NOT NULL,
  referencia_id uuid,
  referencia_nome text NOT NULL,
  setor_nome text,
  descricao text NOT NULL,
  recomendacao text,
  status text NOT NULL DEFAULT 'aberta',
  responsavel text,
  resolvido_em timestamptz,
  resolucao text,
  chave_dedup text,
  detectado_em timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.agente_integracao_status (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  conector_nome text NOT NULL,
  saude text NOT NULL,
  sla text NOT NULL,
  ultima_execucao_ok timestamptz,
  tempo_sem_sync_min integer,
  registros_ultima_exec integer DEFAULT 0,
  erros_consecutivos integer DEFAULT 0,
  fila_pendente integer DEFAULT 0,
  fila_erro integer DEFAULT 0,
  retry_excessivo boolean DEFAULT false,
  descricao text NOT NULL,
  recomendacao text,
  proxima_acao text,
  detectado_em timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.agente_qualidade_incidente (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  tipo text NOT NULL,
  severidade text NOT NULL,
  impacto text NOT NULL,
  referencia_tipo text NOT NULL,
  referencia_id uuid,
  referencia_nome text NOT NULL,
  descricao text NOT NULL,
  recomendacao text,
  status text NOT NULL DEFAULT 'aberto',
  resolvido_em timestamptz,
  resolucao text,
  chave_dedup text,
  detectado_em timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- ── ORQUESTRACAO ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.orch_alerta (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  categoria varchar(30) NOT NULL,
  tipo varchar(40) NOT NULL,
  severidade varchar(10) NOT NULL,
  titulo text NOT NULL,
  descricao text,
  funcionario_id uuid,
  setor_id uuid,
  conector_id uuid,
  job_id uuid,
  responsavel_perfil varchar(20),
  responsavel_id uuid,
  data_limite date,
  status varchar(20) NOT NULL DEFAULT 'aberto',
  resolvido_por uuid,
  resolvido_em timestamptz,
  resolucao text,
  chave_dedup text,
  dados_extra jsonb DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orch_job (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo varchar(40) NOT NULL,
  cron_expression varchar(30),
  proximo_disparo timestamptz,
  intervalo_min integer,
  status varchar(20) NOT NULL DEFAULT 'agendado',
  prioridade smallint NOT NULL DEFAULT 5,
  payload jsonb DEFAULT '{}',
  tentativas integer NOT NULL DEFAULT 0,
  max_tentativas integer NOT NULL DEFAULT 3,
  ultimo_erro text,
  erros_historico jsonb DEFAULT '[]',
  retry_delay_seg integer DEFAULT 60,
  retry_backoff numeric(3,1) DEFAULT 2.0,
  criado_em timestamptz NOT NULL DEFAULT now(),
  iniciado_em timestamptz,
  finalizado_em timestamptz,
  duracao_ms integer,
  resultado jsonb,
  registros_afetados integer DEFAULT 0,
  conector_id uuid,
  disparado_por varchar(20) DEFAULT 'cron',
  usuario_id uuid,
  habilitado boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orch_job_log (
  id bigint NOT NULL,
  job_id uuid NOT NULL,
  tipo varchar(40) NOT NULL,
  tentativa integer NOT NULL,
  status varchar(20) NOT NULL,
  inicio timestamptz NOT NULL,
  fim timestamptz,
  duracao_ms integer,
  registros_afetados integer DEFAULT 0,
  resultado jsonb,
  erro text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orch_tarefa (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  alerta_id uuid,
  titulo text NOT NULL,
  descricao text,
  categoria varchar(30) NOT NULL,
  prioridade varchar(10) NOT NULL DEFAULT 'media',
  responsavel_perfil varchar(20),
  responsavel_id uuid,
  funcionario_id uuid,
  setor_id uuid,
  prazo date,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  concluida_por uuid,
  concluida_em timestamptz,
  nota_conclusao text,
  origem varchar(20) NOT NULL DEFAULT 'automatica',
  gatilho text,
  dados_extra jsonb DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- ── PLANOS DE ACAO ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.acao_plano (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo text NOT NULL,
  descricao text,
  modulo_origem text NOT NULL,
  origem_tipo text,
  origem_id uuid,
  criticidade text NOT NULL,
  impacto text NOT NULL,
  prioridade text NOT NULL,
  responsavel_perfil text,
  responsavel_id uuid,
  responsavel_nome text,
  responsavel_pendente boolean DEFAULT false,
  setor_nome text,
  funcionario_id uuid,
  prazo_dias integer,
  data_limite date,
  status text NOT NULL DEFAULT 'aberta',
  data_abertura timestamptz DEFAULT now(),
  data_triagem timestamptz,
  data_aprovacao timestamptz,
  data_inicio_exec timestamptz,
  data_conclusao timestamptz,
  data_cancelamento timestamptz,
  motivo_cancelamento text,
  criado_por uuid DEFAULT auth.uid(),
  atualizado_por uuid,
  chave_dedup text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.acao_item (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  plano_id uuid NOT NULL,
  ordem integer NOT NULL DEFAULT 1,
  descricao text NOT NULL,
  responsavel text,
  prazo_dias integer,
  status text NOT NULL DEFAULT 'pendente',
  data_conclusao timestamptz,
  observacao text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.acao_comentario (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  plano_id uuid NOT NULL,
  tipo text NOT NULL DEFAULT 'comentario',
  texto text NOT NULL,
  usuario_id uuid DEFAULT auth.uid(),
  usuario_nome text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.acao_transicao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  plano_id uuid NOT NULL,
  status_anterior text NOT NULL,
  status_novo text NOT NULL,
  motivo text,
  usuario_id uuid DEFAULT auth.uid(),
  usuario_nome text,
  created_at timestamptz DEFAULT now()
);

-- ── ACOES E AUDIT ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_acoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo text NOT NULL,
  descricao text NOT NULL,
  funcionario_id uuid,
  funcionario_nome text,
  setor text,
  urgencia text DEFAULT 'media',
  status text DEFAULT 'pendente',
  acao_tomada text,
  responsavel_id uuid,
  responsavel_nome text,
  delegado_para text,
  justificativa text,
  data_limite date,
  data_resolucao timestamptz,
  origem text,
  origem_id uuid,
  created_at timestamptz DEFAULT now(),
  created_by uuid,
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_audit_trail (
  id bigint NOT NULL,
  tabela text NOT NULL,
  operacao text NOT NULL,
  registro_id uuid,
  dados_antigos jsonb,
  dados_novos jsonb,
  campos_alterados text[],
  usuario_id uuid,
  usuario_email text,
  ip_address text,
  created_at timestamptz DEFAULT now()
);

-- ── CORE / GOLIVE ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.core_auditoria (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  dominio text NOT NULL,
  acao text NOT NULL,
  tabela_afetada text NOT NULL,
  registro_id uuid,
  dados_antes jsonb,
  dados_depois jsonb,
  justificativa text,
  usuario_id uuid DEFAULT auth.uid(),
  usuario_nome text,
  ip_address text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.core_criticidade (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  origem_dominio text NOT NULL,
  origem_modulo text NOT NULL,
  origem_id uuid,
  criticidade text NOT NULL,
  titulo text NOT NULL,
  descricao text,
  referencia_tipo text,
  referencia_id uuid,
  referencia_nome text,
  status text NOT NULL DEFAULT 'aberto',
  responsavel_perfil text,
  data_limite date,
  resolvido_em timestamptz,
  chave_dedup text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.golive_checklist (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  fase varchar(20) NOT NULL,
  item text NOT NULL,
  obrigatorio boolean NOT NULL DEFAULT true,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  evidencia text,
  executado_por uuid,
  executado_em timestamptz,
  observacoes text,
  ordem smallint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.golive_relatorio_diario (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  dia date NOT NULL,
  dia_numero smallint NOT NULL,
  status_geral varchar(20) NOT NULL DEFAULT 'pendente',
  syncs_executados integer DEFAULT 0,
  syncs_sucesso integer DEFAULT 0,
  syncs_erro integer DEFAULT 0,
  registros_importados integer DEFAULT 0,
  funcionarios_core integer DEFAULT 0,
  conflitos_novos integer DEFAULT 0,
  conflitos_abertos integer DEFAULT 0,
  duplicatas_detectadas integer DEFAULT 0,
  erros_staging integer DEFAULT 0,
  alertas_gerados integer DEFAULT 0,
  tarefas_geradas integer DEFAULT 0,
  jobs_sucesso integer DEFAULT 0,
  jobs_erro integer DEFAULT 0,
  tempo_medio_job_ms integer DEFAULT 0,
  taxa_sucesso_pct numeric(5,1) DEFAULT 0,
  problemas_encontrados jsonb DEFAULT '[]',
  acoes_realizadas jsonb DEFAULT '[]',
  riscos_identificados jsonb DEFAULT '[]',
  observacoes text,
  gerado_em timestamptz NOT NULL DEFAULT now(),
  validado_por uuid,
  validado_em timestamptz
);
