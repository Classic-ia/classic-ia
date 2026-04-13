-- ════════════════════════════════════════════════════════════════════════════
-- TABELAS SST — Estoque EPI, Faturas, Ordens de Servico, CIPA, eSocial
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao)
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Criticidade: ALTA
-- Dependencias: sst_catalogo_epi, sst_epi_entrega, rh_funcionarios, rh_setores
--               (definidos em schema_rh_sst_completo.sql / MODULO_SST_v2.sql)
--
-- Tabelas incluidas (17):
--   sst_estoque_epi, sst_estoque_movimentacao,
--   sst_faturas_servicos, sst_fatura_resumo_procedimentos,
--   sst_fatura_historico_colaborador,
--   sst_ordem_servico, sst_os_ciencia, sst_os_revisao,
--   cipa_mandato, cipa_membros, cipa_reunioes, cipa_presencas,
--   cipa_assinaturas, cipa_inspecoes, cipa_denuncias, cipa_checklist_padrao,
--   esocial_eventos, esocial_s2240
-- ════════════════════════════════════════════════════════════════════════════

-- ── ESTOQUE EPI ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.sst_estoque_epi (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  catalogo_epi_id uuid NOT NULL,
  quantidade_atual integer NOT NULL DEFAULT 0,
  quantidade_minima integer DEFAULT 5,
  localizacao text,
  ultima_entrada date,
  ultima_saida date,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.sst_estoque_movimentacao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  catalogo_epi_id uuid NOT NULL,
  tipo text NOT NULL,
  quantidade integer NOT NULL,
  motivo text,
  entrega_id uuid,
  responsavel text,
  created_at timestamptz DEFAULT now()
);

-- ── FATURAS SST ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.sst_faturas_servicos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  empresa_id uuid,
  unidade_id uuid,
  fornecedor_nome text NOT NULL,
  fornecedor_cnpj varchar(18),
  numero_contrato text,
  numero_conta text,
  numero_documento text,
  data_emissao date NOT NULL,
  vencimento date,
  competencia date NOT NULL,
  valor_total numeric(14,2) NOT NULL DEFAULT 0,
  valor_pago numeric(14,2) DEFAULT 0,
  status_pagamento text DEFAULT 'aberto',
  observacoes text,
  origem_lancamento text DEFAULT 'manual',
  arquivo_url text,
  hash_documento text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.sst_fatura_resumo_procedimentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  fatura_id uuid NOT NULL,
  descricao_original text NOT NULL,
  nome_padronizado text,
  categoria text,
  subtipo text,
  valor_unitario numeric(14,2) NOT NULL DEFAULT 0,
  quantidade numeric(12,2) NOT NULL DEFAULT 0,
  valor_total numeric(14,2) NOT NULL DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.sst_fatura_historico_colaborador (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  fatura_id uuid NOT NULL,
  funcionario_id uuid,
  nome_funcionario_origem text NOT NULL,
  tipo_evento text NOT NULL,
  categoria text,
  subtipo text,
  nr_relacionada text,
  data_realizacao date,
  data_faturamento date,
  valor_unitario_estimado numeric(14,2),
  valor_total_estimado numeric(14,2),
  setor_id uuid,
  vinculo text,
  centro_custo_id uuid,
  status_integracao text DEFAULT 'pendente',
  observacoes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- ── ORDENS DE SERVICO ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.sst_ordem_servico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  codigo text,
  titulo text NOT NULL,
  setor_id uuid,
  cargo_id uuid,
  atividade_principal text,
  riscos jsonb DEFAULT '[]',
  medidas_preventivas jsonb DEFAULT '[]',
  epis_obrigatorios jsonb DEFAULT '[]',
  treinamentos_obrigatorios jsonb DEFAULT '[]',
  procedimentos_emergencia text,
  regras_seguranca text,
  versao integer NOT NULL DEFAULT 1,
  status text NOT NULL DEFAULT 'rascunho',
  data_emissao date,
  data_revisao date,
  data_vigencia_inicio date,
  data_vigencia_fim date,
  responsavel_tecnico text,
  responsavel_tecnico_registro text,
  aprovado_por text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  created_by uuid DEFAULT auth.uid()
);

CREATE TABLE IF NOT EXISTS public.sst_os_ciencia (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  os_id uuid NOT NULL,
  funcionario_id uuid NOT NULL,
  data_ciencia date NOT NULL DEFAULT CURRENT_DATE,
  meio_assinatura text,
  observacao text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.sst_os_revisao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  os_id uuid NOT NULL,
  versao_anterior integer NOT NULL,
  versao_nova integer NOT NULL,
  motivo_revisao text NOT NULL,
  alteracoes text,
  revisado_por text,
  created_at timestamptz DEFAULT now()
);

-- ── CIPA ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.cipa_mandato (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  numero text NOT NULL,
  empresa_id uuid,
  data_inicio date NOT NULL,
  data_fim date NOT NULL,
  cnae varchar(10),
  grau_risco integer,
  efetivos integer DEFAULT 0,
  suplentes integer DEFAULT 0,
  status varchar(20) DEFAULT 'ativo',
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cipa_membros (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  mandato_id uuid NOT NULL,
  funcionario_id uuid,
  nome text NOT NULL,
  cargo_cipa varchar(30) NOT NULL,
  tipo varchar(20),
  categoria varchar(20),
  data_posse date,
  estabilidade_ate date,
  treinado boolean DEFAULT false,
  data_treinamento date,
  ativo boolean DEFAULT true,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cipa_reunioes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  mandato_id uuid NOT NULL,
  numero text NOT NULL,
  data date NOT NULL,
  tipo varchar(20) DEFAULT 'ordinaria',
  local text,
  pauta text,
  ata_texto text,
  status varchar(20) DEFAULT 'rascunho',
  proxima_data date,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cipa_presencas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  reuniao_id uuid NOT NULL,
  membro_id uuid NOT NULL,
  presente boolean DEFAULT false,
  justificativa text
);

CREATE TABLE IF NOT EXISTS public.cipa_assinaturas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  reuniao_id uuid NOT NULL,
  membro_id uuid NOT NULL,
  assinado_em timestamptz DEFAULT now(),
  ip_address varchar(45)
);

CREATE TABLE IF NOT EXISTS public.cipa_inspecoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  data date NOT NULL DEFAULT CURRENT_DATE,
  setor text NOT NULL,
  inspetor_id uuid,
  resultado_json jsonb DEFAULT '[]',
  observacoes text,
  nao_conformidades integer DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cipa_denuncias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  empresa_id uuid,
  setor text,
  descricao text NOT NULL,
  status varchar(20) DEFAULT 'aberta',
  resposta text,
  respondido_por uuid,
  respondido_em timestamptz,
  anonima boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cipa_checklist_padrao (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  setor text NOT NULL,
  item text NOT NULL,
  ordem integer DEFAULT 0,
  ativo boolean DEFAULT true
);

-- ── eSOCIAL ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.esocial_eventos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo varchar(10) NOT NULL,
  funcionario_id uuid,
  empresa_id uuid,
  data_fato date NOT NULL,
  prazo_envio date NOT NULL,
  subtipo text,
  descricao text,
  status varchar(20) DEFAULT 'pendente',
  protocolo varchar(50),
  observacoes text,
  enviado_por uuid,
  enviado_em timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.esocial_s2240 (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  setor text NOT NULL,
  empresa_id uuid,
  fator_risco text,
  descricao text,
  data_alteracao date NOT NULL,
  prazo_envio date NOT NULL,
  status varchar(20) DEFAULT 'pendente',
  protocolo varchar(50),
  created_at timestamptz DEFAULT now()
);
