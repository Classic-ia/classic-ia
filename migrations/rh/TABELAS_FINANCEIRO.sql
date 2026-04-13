-- ════════════════════════════════════════════════════════════════════════════
-- TABELAS FINANCEIRO — Custos, Transporte, CT-e, Folha de Pagamento
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co (producao)
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Criticidade: MEDIA
-- Dependencias: rh_funcionarios, rh_empresas, rh_filiais, rh_setores
--
-- Tabelas incluidas (15):
--   rh_custo_colaborador, rh_custos_transporte, rh_custos_transporte_funcionarios,
--   rh_custos_transporte_rateio, rh_cte, rh_cte_componente, rh_cte_importacao_log,
--   rh_divergencias, rh_sync_log,
--   fp_rubrica, fp_lote, fp_folha, fp_lancamento,
--   fp_lote_pagamento, fp_pagamento_funcionario, fp_variavel_mensal
-- ════════════════════════════════════════════════════════════════════════════

-- ── CUSTOS DE PESSOAL ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_custo_colaborador (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid,
  competencia date NOT NULL,
  nome_colaborador text,
  cpf varchar(14),
  matricula varchar(20),
  cargo text,
  setor text,
  filial text,
  salario_base numeric(15,2) DEFAULT 0,
  horas_extras numeric(15,2) DEFAULT 0,
  adicional_noturno numeric(15,2) DEFAULT 0,
  adicional_insalubridade numeric(15,2) DEFAULT 0,
  adicional_periculosidade numeric(15,2) DEFAULT 0,
  vale_transporte numeric(15,2) DEFAULT 0,
  vale_alimentacao numeric(15,2) DEFAULT 0,
  plano_saude numeric(15,2) DEFAULT 0,
  fgts numeric(15,2) DEFAULT 0,
  inss_empresa numeric(15,2) DEFAULT 0,
  outros_proventos numeric(15,2) DEFAULT 0,
  outros_descontos numeric(15,2) DEFAULT 0,
  custo_total numeric(15,2) DEFAULT 0,
  arquivo_origem text,
  importado_em timestamptz DEFAULT now(),
  importado_por uuid,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  auxilio_deslocamento numeric DEFAULT 0,
  adicional_assiduidade numeric DEFAULT 0
);

-- ── TRANSPORTE ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_custos_transporte (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  empresa_id uuid,
  filial_id uuid,
  fornecedor_nome text NOT NULL,
  fornecedor_cnpj varchar(18),
  numero_documento text,
  numero_contrato text,
  competencia date NOT NULL,
  data_emissao date,
  vencimento date,
  valor_total numeric(14,2) NOT NULL DEFAULT 0,
  valor_pago numeric(14,2) DEFAULT 0,
  status_pagamento text DEFAULT 'aberto',
  observacoes text,
  arquivo_url text,
  hash_documento text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_custos_transporte_funcionarios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  transporte_id uuid NOT NULL,
  funcionario_id uuid,
  nome_funcionario text NOT NULL,
  rota text NOT NULL,
  tipo_vinculo text NOT NULL,
  valor_individual numeric(14,2),
  observacoes text,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_custos_transporte_rateio (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  transporte_id uuid NOT NULL,
  rota text NOT NULL,
  tipo_vinculo text NOT NULL,
  quantidade_funcionarios integer NOT NULL DEFAULT 0,
  valor_base numeric(14,2) NOT NULL DEFAULT 0,
  valor_extras numeric(14,2) DEFAULT 0,
  valor_total numeric(14,2) NOT NULL DEFAULT 0,
  custo_unitario numeric(14,2),
  setor_id uuid,
  centro_custo_id uuid,
  observacoes text,
  created_at timestamptz DEFAULT now()
);

-- ── CT-e ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_cte (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  chave_acesso varchar(44) NOT NULL,
  numero_cte integer,
  serie integer,
  cfop varchar(10),
  nat_operacao text,
  data_emissao timestamptz,
  emit_cnpj varchar(18),
  emit_razao_social text,
  emit_nome_fantasia text,
  emit_uf varchar(2),
  rem_cnpj varchar(18),
  rem_razao_social text,
  rem_uf varchar(2),
  rem_municipio text,
  dest_cnpj varchar(18),
  dest_razao_social text,
  dest_uf varchar(2),
  dest_municipio text,
  valor_total numeric(15,2) DEFAULT 0,
  valor_receber numeric(15,2) DEFAULT 0,
  valor_icms numeric(15,2) DEFAULT 0,
  base_icms numeric(15,2) DEFAULT 0,
  aliq_icms numeric(5,2) DEFAULT 0,
  produto_predominante text,
  peso_bruto numeric(12,3),
  peso_cubado numeric(12,3),
  qtd_volumes integer,
  periodo_ref_inicio date,
  periodo_ref_fim date,
  local_origem text,
  local_destino text,
  uf_inicio varchar(2),
  uf_fim varchar(2),
  municipio_inicio text,
  municipio_fim text,
  protocolo_autorizacao varchar(20),
  status varchar(20) DEFAULT 'autorizado',
  xml_original text,
  arquivo_origem text,
  importado_em timestamptz DEFAULT now(),
  importado_por uuid,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_cte_componente (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cte_id uuid NOT NULL,
  nome text NOT NULL,
  valor numeric(15,2) DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_cte_importacao_log (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  arquivo text NOT NULL,
  total_xmls integer DEFAULT 0,
  importados integer DEFAULT 0,
  erros integer DEFAULT 0,
  duplicados integer DEFAULT 0,
  detalhes_erros jsonb DEFAULT '[]',
  importado_por uuid,
  created_at timestamptz DEFAULT now()
);

-- ── DIVERGENCIAS / SYNC ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.rh_divergencias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo text NOT NULL,
  nivel text DEFAULT 'medio',
  colaborador_id uuid,
  descricao text NOT NULL,
  dado_sistema text,
  dado_externo text,
  origem text,
  status text DEFAULT 'pendente',
  resolvido_em timestamptz,
  resolvido_por uuid,
  criado_em timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.rh_sync_log (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  origem text NOT NULL,
  tipo text NOT NULL,
  acao text NOT NULL,
  total_processados integer DEFAULT 0,
  total_ok integer DEFAULT 0,
  total_erros integer DEFAULT 0,
  detalhes jsonb,
  usuario_id uuid,
  iniciado_em timestamptz DEFAULT now(),
  finalizado_em timestamptz
);

-- ── FOLHA DE PAGAMENTO ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.fp_rubrica (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  codigo text NOT NULL,
  nome text NOT NULL,
  tipo varchar(15) NOT NULL,
  natureza varchar(30) NOT NULL,
  incide_inss boolean NOT NULL DEFAULT false,
  incide_fgts boolean NOT NULL DEFAULT false,
  incide_irrf boolean NOT NULL DEFAULT false,
  incide_ferias boolean NOT NULL DEFAULT false,
  incide_13 boolean NOT NULL DEFAULT false,
  conta_debito text,
  conta_credito text,
  centro_custo_fixo boolean DEFAULT false,
  centro_custo_id uuid,
  automatica boolean NOT NULL DEFAULT false,
  editavel boolean NOT NULL DEFAULT true,
  ativo boolean NOT NULL DEFAULT true,
  ordem_exibicao smallint DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_lote (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  competencia varchar(7) NOT NULL,
  tipo_folha varchar(20) NOT NULL DEFAULT 'mensal',
  empresa_id uuid NOT NULL,
  periodo_inicio date NOT NULL,
  periodo_fim date NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'aberto',
  total_proventos numeric(14,2) DEFAULT 0,
  total_descontos numeric(14,2) DEFAULT 0,
  total_liquido numeric(14,2) DEFAULT 0,
  qtd_funcionarios integer DEFAULT 0,
  calculado_por uuid,
  calculado_em timestamptz,
  conferido_por uuid,
  conferido_em timestamptz,
  fechado_por uuid,
  fechado_em timestamptz,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_folha (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  lote_id uuid NOT NULL,
  funcionario_id uuid NOT NULL,
  cargo_snapshot text,
  setor_snapshot text,
  salario_base_snapshot numeric(12,2),
  tipo_vinculo_snapshot varchar(20),
  centro_custo_snapshot text,
  total_proventos numeric(12,2) DEFAULT 0,
  total_descontos numeric(12,2) DEFAULT 0,
  liquido numeric(12,2) DEFAULT 0,
  base_inss numeric(12,2) DEFAULT 0,
  base_fgts numeric(12,2) DEFAULT 0,
  base_irrf numeric(12,2) DEFAULT 0,
  status varchar(20) NOT NULL DEFAULT 'aberto',
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_lancamento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  folha_id uuid NOT NULL,
  rubrica_id uuid NOT NULL,
  referencia numeric(10,2),
  unidade_ref varchar(10),
  valor numeric(12,2) NOT NULL,
  origem varchar(20) NOT NULL DEFAULT 'manual',
  editado boolean NOT NULL DEFAULT false,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_lote_pagamento (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  lote_folha_id uuid NOT NULL,
  tipo_pagamento varchar(30) NOT NULL,
  data_pagamento date NOT NULL,
  banco text,
  forma_pagamento varchar(20) DEFAULT 'transferencia',
  status varchar(20) NOT NULL DEFAULT 'pendente',
  total_bruto numeric(14,2) DEFAULT 0,
  total_liquido numeric(14,2) DEFAULT 0,
  qtd_funcionarios integer DEFAULT 0,
  arquivo_bancario text,
  gerado_por uuid,
  gerado_em timestamptz,
  pago_em timestamptz,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_pagamento_funcionario (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  lote_pagamento_id uuid NOT NULL,
  funcionario_id uuid NOT NULL,
  folha_id uuid NOT NULL,
  valor_liquido numeric(12,2) NOT NULL,
  forma_pagamento varchar(20),
  banco text,
  agencia text,
  conta text,
  chave_pix text,
  status varchar(20) NOT NULL DEFAULT 'pendente',
  pago_em timestamptz,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);

CREATE TABLE IF NOT EXISTS public.fp_variavel_mensal (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  funcionario_id uuid NOT NULL,
  competencia varchar(7) NOT NULL,
  tipo varchar(30) NOT NULL,
  quantidade numeric(10,2) NOT NULL,
  unidade varchar(10) NOT NULL DEFAULT 'horas',
  valor_unitario numeric(12,2),
  valor_total numeric(12,2),
  origem varchar(20) NOT NULL DEFAULT 'manual',
  referencia_origem text,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid
);
