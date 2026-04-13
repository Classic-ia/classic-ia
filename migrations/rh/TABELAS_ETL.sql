-- ════════════════════════════════════════════════════════════════════════════
-- TABELAS ETL — Staging, Integracoes, Producao, Reconciliacao, Logistica
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao)
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Criticidade: MEDIA
-- Dependencias: rh_funcionarios (FUNDACAO_BANCO_v2.sql)
--
-- Tabelas incluidas (29):
--   stg_convenia_documentos, stg_convenia_funcionarios, stg_convenia_afastamentos,
--   stg_convenia_ferias,
--   stg_buscaepi_ca, stg_buscaepi_colaboradores, stg_buscaepi_entregas,
--   stg_buscaepi_epis, stg_buscaepi_os,
--   stg_atak_centros_custo, stg_atak_estrutura, stg_atak_financeiro,
--   stg_erros_importacao, file_ingest_log,
--   etl_importacao, etl_mapa_identidade, etl_depara, etl_sync_controle,
--   integ_conector, integ_execucao, integ_fila,
--   producao_real, producao_consolidada, producao_rateada,
--   reconc_correspondencia, reconc_conflito, reconc_decisao, reconc_duplicata_candidato,
--   rs_vagas_distribuicao,
--   log_municipios, log_rotas, log_veiculos, log_transporte_colaborador,
--   log_transporte_historico,
--   arm_blocos, arm_armarios, arm_ocupacoes, arm_bloqueios, arm_historico
-- ════════════════════════════════════════════════════════════════════════════

-- ── STAGING CONVENIA ────────────────────────────────────────────────────
-- Critica: stg_convenia_documentos e usada por vw_absenteismo_real

CREATE TABLE IF NOT EXISTS public.stg_convenia_documentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'convenia',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_convenia_funcionarios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'convenia',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  funcionario_id_core uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_convenia_afastamentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'convenia',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_convenia_ferias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'convenia',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── STAGING BUSCAEPI ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.stg_buscaepi_ca (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'buscaepi',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_buscaepi_colaboradores (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar DEFAULT 'buscaepi',
  payload_json jsonb NOT NULL,
  hash_registro text,
  importacao_id uuid,
  data_importacao timestamptz DEFAULT now(),
  status_processamento varchar DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_buscaepi_entregas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'buscaepi',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_buscaepi_epis (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'buscaepi',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  catalogo_epi_id_core uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_buscaepi_os (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'buscaepi',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── STAGING ATAK ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.stg_atak_centros_custo (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'atak',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  centro_custo_id_core uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_atak_estrutura (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'atak',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.stg_atak_financeiro (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_origem text NOT NULL,
  origem_sistema varchar(20) NOT NULL DEFAULT 'atak',
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  importacao_id uuid,
  data_importacao timestamptz NOT NULL DEFAULT now(),
  atualizado_em_origem timestamptz,
  status_processamento varchar(20) NOT NULL DEFAULT 'pendente',
  erro_detalhe text,
  competencia varchar(7),
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── ERROS E LOG DE IMPORTACAO ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.stg_erros_importacao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  importacao_id uuid,
  tabela_staging text NOT NULL,
  registro_staging_id uuid,
  id_origem text,
  tipo_erro varchar(30) NOT NULL,
  severidade varchar(10) NOT NULL DEFAULT 'erro',
  campo text,
  valor_recebido text,
  valor_esperado text,
  mensagem text NOT NULL,
  resolvido boolean NOT NULL DEFAULT false,
  resolvido_por uuid,
  resolvido_em timestamptz,
  resolucao text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.file_ingest_log (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  caminho text NOT NULL,
  nome_arquivo text NOT NULL,
  tipo_arquivo varchar(10),
  tamanho_bytes bigint,
  hash_arquivo text,
  categoria varchar(30),
  status varchar(20) NOT NULL DEFAULT 'pendente',
  registros_extraidos integer DEFAULT 0,
  erro_detalhe text,
  processado_em timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── ETL INFRAESTRUTURA ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.etl_importacao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  origem_sistema varchar(20) NOT NULL,
  tipo_entidade varchar(30) NOT NULL,
  data_inicio timestamptz NOT NULL DEFAULT now(),
  data_fim timestamptz,
  status varchar(20) NOT NULL DEFAULT 'em_andamento',
  qtd_registros_brutos integer DEFAULT 0,
  qtd_processados integer DEFAULT 0,
  qtd_inseridos integer DEFAULT 0,
  qtd_atualizados integer DEFAULT 0,
  qtd_ignorados integer DEFAULT 0,
  qtd_erros integer DEFAULT 0,
  parametros jsonb,
  executado_por uuid,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.etl_mapa_identidade (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  origem_sistema varchar(20) NOT NULL,
  tipo_entidade varchar(30) NOT NULL,
  id_origem text NOT NULL,
  id_core uuid NOT NULL,
  chave_fallback text,
  confianca varchar(10) NOT NULL DEFAULT 'alta',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.etl_depara (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  categoria varchar(30) NOT NULL,
  origem_sistema varchar(20) NOT NULL,
  valor_origem text NOT NULL,
  valor_padrao text NOT NULL,
  id_core uuid,
  ativo boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.etl_sync_controle (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  origem_sistema varchar(20) NOT NULL,
  tipo_entidade varchar(30) NOT NULL,
  ultimo_sync timestamptz NOT NULL DEFAULT now(),
  ultimo_id_origem text,
  ultimo_hash text,
  sync_mode varchar(20) NOT NULL DEFAULT 'incremental',
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- ── INTEGRACAO ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.integ_conector (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  tipo varchar(20) NOT NULL,
  base_url text,
  auth_method varchar(20),
  vault_secret_name text,
  sync_intervalo_minutos integer DEFAULT 60,
  sync_habilitado boolean NOT NULL DEFAULT true,
  sync_modo varchar(20) NOT NULL DEFAULT 'incremental',
  rate_limit_rpm integer DEFAULT 60,
  rate_limit_burst integer DEFAULT 10,
  max_retries integer NOT NULL DEFAULT 3,
  retry_delay_seconds integer NOT NULL DEFAULT 30,
  retry_backoff_multiplier numeric(3,1) DEFAULT 2.0,
  ultimo_heartbeat timestamptz,
  status varchar(20) NOT NULL DEFAULT 'ativo',
  erro_consecutivos integer DEFAULT 0,
  config_extra jsonb DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.integ_execucao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  conector_id uuid NOT NULL,
  tipo_entidade varchar(30) NOT NULL,
  modo varchar(20) NOT NULL DEFAULT 'incremental',
  inicio timestamptz NOT NULL DEFAULT now(),
  fim timestamptz,
  duracao_ms integer,
  registros_lidos integer DEFAULT 0,
  registros_novos integer DEFAULT 0,
  registros_alterados integer DEFAULT 0,
  registros_ignorados integer DEFAULT 0,
  registros_erro integer DEFAULT 0,
  cursor_inicio text,
  cursor_fim text,
  filtro_desde timestamptz,
  status varchar(20) NOT NULL DEFAULT 'executando',
  erro_mensagem text,
  importacao_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.integ_fila (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  execucao_id uuid,
  conector_id uuid NOT NULL,
  tipo_entidade varchar(30) NOT NULL,
  id_origem text NOT NULL,
  payload_json jsonb NOT NULL,
  hash_registro text NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  tentativas integer NOT NULL DEFAULT 0,
  max_tentativas integer NOT NULL DEFAULT 3,
  proxima_tentativa timestamptz,
  ultimo_erro text,
  erros_historico jsonb DEFAULT '[]',
  tabela_staging text,
  registro_staging_id uuid,
  criado_em timestamptz NOT NULL DEFAULT now(),
  processado_em timestamptz
);

-- ── PRODUCAO ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.producao_real (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  data date NOT NULL,
  produto_codigo text,
  produto_nome text NOT NULL,
  quantidade_kg numeric(12,2) NOT NULL,
  quantidade_pcs integer DEFAULT 0,
  unidade varchar(10) DEFAULT 'KG',
  horas_total numeric(8,2) DEFAULT 0,
  producao_hora numeric(10,2) DEFAULT 0,
  origem varchar(20) NOT NULL DEFAULT 'atak',
  importacao_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.producao_consolidada (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  data date NOT NULL,
  total_atak_kg numeric(12,2) DEFAULT 0,
  total_rateado numeric(12,2) DEFAULT 0,
  divergencia numeric(12,2) DEFAULT 0,
  percentual_dif numeric(6,2) DEFAULT 0,
  status varchar(10) NOT NULL DEFAULT 'OK',
  funcionarios_dia integer DEFAULT 0,
  horas_totais numeric(8,2) DEFAULT 0,
  kg_por_func numeric(10,2) DEFAULT 0,
  kg_por_hora numeric(10,2) DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.producao_rateada (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  data date NOT NULL,
  funcionario_id uuid,
  nome_funcionario text NOT NULL,
  matricula text,
  setor_producao text NOT NULL,
  quantidade numeric(10,2) NOT NULL DEFAULT 0,
  media_hora numeric(10,2) DEFAULT 0,
  horas_trabalhadas numeric(6,2) DEFAULT 0,
  origem varchar(20) NOT NULL DEFAULT 'planilha',
  semana_ref text,
  importacao_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── RECONCILIACAO ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.reconc_correspondencia (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  cpf varchar(11),
  email text,
  nome text,
  data_nascimento date,
  convenia_id text,
  buscaepi_id text,
  atak_id text,
  secullum_id text,
  score_confianca numeric(5,2) NOT NULL DEFAULT 0,
  metodo_match varchar(30),
  status varchar(20) NOT NULL DEFAULT 'pendente',
  resolvido_por uuid,
  resolvido_em timestamptz,
  resolucao_nota text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.reconc_conflito (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  correspondencia_id uuid NOT NULL,
  funcionario_id uuid,
  campo text NOT NULL,
  valor_core text,
  valor_convenia text,
  valor_buscaepi text,
  valor_atak text,
  data_core timestamptz,
  data_convenia timestamptz,
  data_buscaepi timestamptz,
  data_atak timestamptz,
  valor_escolhido text,
  fonte_escolhida varchar(20),
  regra_aplicada varchar(30),
  status varchar(20) NOT NULL DEFAULT 'aberto',
  resolvido_por uuid,
  resolvido_em timestamptz,
  justificativa text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.reconc_decisao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  correspondencia_id uuid,
  conflito_id uuid,
  tipo_decisao varchar(30) NOT NULL,
  descricao text NOT NULL,
  dados_antes jsonb,
  dados_depois jsonb,
  usuario_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.reconc_duplicata_candidato (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_a_id uuid NOT NULL,
  funcionario_b_id uuid NOT NULL,
  score_similaridade numeric(5,2) NOT NULL,
  motivo text NOT NULL,
  detalhes jsonb,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  resolvido_por uuid,
  resolvido_em timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ── RECRUTAMENTO (complemento) ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rs_vagas_distribuicao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  vaga_id uuid NOT NULL,
  setor text NOT NULL,
  quantidade integer NOT NULL DEFAULT 1,
  sexo_preferencial text,
  turno text,
  observacoes text,
  preenchidas integer DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

-- ── LOGISTICA / TRANSPORTE ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.log_municipios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  uf text NOT NULL DEFAULT 'PR',
  distancia_km numeric,
  tempo_estimado_min integer,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.log_rotas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  codigo text NOT NULL,
  nome text NOT NULL,
  municipio_id uuid NOT NULL,
  filial_id uuid,
  turno text DEFAULT 'comercial',
  tipo text DEFAULT 'onibus',
  capacidade integer NOT NULL DEFAULT 40,
  horario_saida time,
  horario_retorno time,
  veiculo_id uuid,
  ativo boolean DEFAULT true,
  observacoes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.log_veiculos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  placa text NOT NULL,
  tipo text DEFAULT 'onibus',
  capacidade integer NOT NULL,
  empresa text,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.log_transporte_colaborador (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  rota_id uuid NOT NULL,
  municipio_id uuid,
  data_inicio date NOT NULL DEFAULT CURRENT_DATE,
  data_fim date,
  tipo text DEFAULT 'regular',
  ponto_embarque text,
  observacoes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.log_transporte_historico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  rota_id uuid,
  acao text NOT NULL,
  detalhes jsonb,
  usuario_id uuid,
  created_at timestamptz DEFAULT now()
);

-- ── ARMARIOS ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.arm_blocos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  codigo text NOT NULL,
  nome text NOT NULL,
  tipo text NOT NULL,
  localizacao text,
  filial_id uuid,
  capacidade integer,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.arm_armarios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  codigo text NOT NULL,
  codigo_completo text,
  bloco_id uuid NOT NULL,
  setor_id uuid,
  filial_id uuid,
  tipo text NOT NULL,
  turno text DEFAULT 'comercial',
  tipo_uso text DEFAULT 'fixo',
  posicao text,
  status text NOT NULL DEFAULT 'disponivel',
  observacoes text,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.arm_ocupacoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  armario_id uuid NOT NULL,
  funcionario_id uuid NOT NULL,
  data_atribuicao date NOT NULL DEFAULT CURRENT_DATE,
  data_devolucao date,
  atribuido_por uuid,
  devolvido_por uuid,
  motivo_devolucao text,
  chave_entregue boolean DEFAULT true,
  chave_numero text,
  observacoes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.arm_bloqueios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  armario_id uuid NOT NULL,
  motivo text NOT NULL,
  tipo_bloqueio text DEFAULT 'manutencao',
  bloqueado_por uuid,
  bloqueado_em timestamptz DEFAULT now(),
  desbloqueado_por uuid,
  desbloqueado_em timestamptz,
  previsao_liberacao date,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.arm_historico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  armario_id uuid NOT NULL,
  funcionario_id uuid,
  acao text NOT NULL,
  detalhes jsonb,
  usuario_id uuid,
  created_at timestamptz DEFAULT now()
);
