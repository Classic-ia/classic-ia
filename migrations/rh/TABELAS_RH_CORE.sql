-- ════════════════════════════════════════════════════════════════════════════
-- TABELAS RH CORE — Processos, documentos, ponto, ferias, beneficios
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao)
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Criticidade: ALTA
-- Dependencias: rh_funcionarios, rh_cargos, rh_setores, rh_empresas, rh_filiais
--               (definidos em FUNDACAO_BANCO_v2.sql / 00_schema_supabase.sql)
--
-- Tabelas incluidas (17):
--   rh_processo, rh_proc_desligamento, rh_proc_admissao, rh_proc_afastamento,
--   rh_proc_movimentacao, rh_processo_documento, rh_processo_transicao,
--   rh_tipo_documento, rh_ferias, rh_beneficio, rh_ponto, rh_ponto_totais,
--   rh_acompanhamento, rh_documento_digital, rh_documento_ocr,
--   rh_notificacao, rh_funcionario_historico
-- ════════════════════════════════════════════════════════════════════════════

-- ── WORKFLOW DE PROCESSOS ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_processo (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  tipo varchar(30) NOT NULL,
  status varchar(30) NOT NULL DEFAULT 'rascunho',
  data_solicitacao date NOT NULL DEFAULT CURRENT_DATE,
  data_efetivacao date,
  solicitado_por uuid,
  aprovado_por uuid,
  aprovado_em timestamptz,
  executado_por uuid,
  executado_em timestamptz,
  cancelado_por uuid,
  cancelado_em timestamptz,
  motivo_cancelamento text,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.rh_proc_desligamento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  tipo_desligamento varchar(30) NOT NULL,
  data_desligamento date NOT NULL,
  data_aviso_previo date,
  aviso_previo_trabalhado boolean DEFAULT false,
  aviso_previo_indenizado boolean DEFAULT false,
  dias_aviso_previo integer,
  categoria_causa varchar(30) NOT NULL,
  motivo_detalhado text NOT NULL,
  entrevista_realizada boolean DEFAULT false,
  entrevista_p1 text,
  entrevista_p2 text,
  entrevista_p3 text,
  entrevista_p4 text,
  entrevista_p5 text,
  exame_demissional_ok boolean DEFAULT false,
  devolucao_epis_ok boolean DEFAULT false,
  devolucao_patrimonio_ok boolean DEFAULT false,
  bloqueio_acessos_ok boolean DEFAULT false,
  calculo_rescisao_ok boolean DEFAULT false,
  status_anterior varchar(20),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_proc_admissao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  nome_completo text NOT NULL,
  cpf varchar(11) NOT NULL,
  cargo_id uuid NOT NULL,
  setor_id uuid NOT NULL,
  empresa_id uuid NOT NULL,
  filial_id uuid NOT NULL,
  centro_custo_id uuid,
  gestor_id uuid,
  tipo_vinculo varchar(20) NOT NULL DEFAULT 'CLT',
  modalidade_contrato varchar(30),
  salario_proposto numeric(12,2),
  data_admissao_prevista date NOT NULL,
  turno varchar(30) DEFAULT 'comercial',
  carga_horaria_semanal numeric(4,1) DEFAULT 44.0,
  funcionario_criado_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_proc_afastamento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  tipo_afastamento varchar(30) NOT NULL,
  data_inicio date NOT NULL,
  data_previsao_retorno date,
  data_retorno_efetivo date,
  cid varchar(10),
  dias_previstos integer,
  beneficio_inss boolean DEFAULT false,
  numero_beneficio text,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_proc_movimentacao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  tipo_movimentacao varchar(30) NOT NULL,
  cargo_anterior_id uuid,
  setor_anterior_id uuid,
  filial_anterior_id uuid,
  gestor_anterior_id uuid,
  salario_anterior numeric(12,2),
  turno_anterior varchar(30),
  centro_custo_anterior_id uuid,
  cargo_novo_id uuid,
  setor_novo_id uuid,
  filial_nova_id uuid,
  gestor_novo_id uuid,
  salario_novo numeric(12,2),
  turno_novo varchar(30),
  centro_custo_novo_id uuid,
  justificativa text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_processo_documento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  tipo_documento_id uuid NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  data_recebimento date,
  verificado_por uuid,
  verificado_em timestamptz,
  motivo_rejeicao text,
  motivo_dispensa text,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.rh_processo_transicao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  processo_id uuid NOT NULL,
  status_anterior varchar(30) NOT NULL,
  status_novo varchar(30) NOT NULL,
  usuario_id uuid,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_tipo_documento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  descricao text,
  categoria varchar(30) NOT NULL,
  obrigatorio_admissao boolean NOT NULL DEFAULT false,
  obrigatorio_desligamento boolean NOT NULL DEFAULT false,
  aplicavel_vinculo text[] DEFAULT '{CLT}',
  ativo boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

-- ── FERIAS E BENEFICIOS ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_ferias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  periodo_aquisitivo_inicio date NOT NULL,
  periodo_aquisitivo_fim date NOT NULL,
  dias_direito integer NOT NULL DEFAULT 30,
  dias_gozados integer DEFAULT 0,
  dias_vendidos integer DEFAULT 0,
  data_inicio date,
  data_fim date,
  data_retorno date,
  status text NOT NULL DEFAULT 'pendente',
  abono_pecuniario boolean DEFAULT false,
  observacoes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_beneficio (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  tipo text NOT NULL,
  status text NOT NULL DEFAULT 'ativo',
  data_inicio date,
  data_fim date,
  valor numeric(10,2),
  observacoes text,
  documento_ref text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- ── PONTO ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_ponto (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  funcionario_nome text NOT NULL,
  cpf text,
  data date NOT NULL,
  dia_semana text,
  entrada1 text,
  saida1 text,
  entrada2 text,
  saida2 text,
  entrada3 text,
  saida3 text,
  horas_normais text,
  horas_faltas text,
  horas_extra_50 text,
  horas_extra_100 text,
  horas_noturnas text,
  horas_extra_noturna text,
  carga_horaria text,
  banco_credito text,
  banco_debito text,
  banco_saldo text,
  justificativa text,
  periodo text,
  importacao_id uuid,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_ponto_totais (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  funcionario_nome text NOT NULL,
  cpf text,
  periodo text NOT NULL,
  horas_normais text,
  horas_faltas text,
  horas_extra_50 text,
  horas_extra_100 text,
  horas_noturnas text,
  carga_horaria text,
  dias_trabalhados integer DEFAULT 0,
  dias_falta integer DEFAULT 0,
  dias_ferias integer DEFAULT 0,
  dias_atestado integer DEFAULT 0,
  dias_ausencia integer DEFAULT 0,
  importacao_id uuid,
  created_at timestamptz DEFAULT now()
);

-- ── DOCUMENTOS E ACOMPANHAMENTO ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_acompanhamento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  gestor_id uuid,
  tipo text NOT NULL,
  data_registro date NOT NULL DEFAULT CURRENT_DATE,
  assunto text NOT NULL,
  descricao text,
  participantes text[],
  local text,
  duracao_minutos integer,
  resultado text,
  proxima_acao text,
  prazo_proxima_acao date,
  tem_evidencia boolean DEFAULT false,
  evidencia_tipo text,
  confidencial boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  created_by uuid DEFAULT auth.uid()
);

CREATE TABLE IF NOT EXISTS public.rh_documento_digital (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  funcionario_nome text NOT NULL,
  categoria text NOT NULL,
  subcategoria text,
  nome_arquivo text NOT NULL,
  caminho_relativo text,
  ano_referencia integer,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_documento_ocr (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  funcionario_nome text,
  tipo_documento text NOT NULL,
  arquivo_nome text NOT NULL,
  arquivo_caminho text,
  texto_extraido text,
  data_documento date,
  dias_afastamento integer,
  cid text,
  medico text,
  crm text,
  resultado text,
  nr_referencia text,
  observacoes text,
  confianca_ocr numeric(5,2),
  processado_em timestamptz DEFAULT now(),
  ano_referencia integer,
  mes_referencia integer,
  gravidade text,
  created_at timestamptz DEFAULT now()
);

-- ── NOTIFICACOES E HISTORICO ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_notificacao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo text NOT NULL,
  titulo text NOT NULL,
  descricao text,
  criticidade text NOT NULL,
  referencia_tipo text,
  referencia_id uuid,
  referencia_nome text,
  destinatario_perfil text,
  lida boolean DEFAULT false,
  lida_em timestamptz,
  lida_por uuid,
  chave_dedup text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_funcionario_historico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  data date NOT NULL DEFAULT CURRENT_DATE,
  tipo varchar(30) NOT NULL,
  descricao text NOT NULL,
  responsavel text,
  gravidade varchar(10) NOT NULL DEFAULT 'baixa',
  relacionado_sst boolean DEFAULT false,
  relacionado_absenteismo boolean DEFAULT false,
  privado boolean DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);
