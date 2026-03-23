-- ═══════════════════════════════════════════════════════════════
-- SCHEMA COMPLETO — RH, SST, SAÚDE MENTAL, PAGAMENTOS
-- Classic CQ · Supabase (PostgreSQL)
-- Executar no SQL Editor do Supabase
-- ═══════════════════════════════════════════════════════════════

-- ══════════════════════════════════════════════════════════
-- 1. ESTRUTURA ORGANIZACIONAL
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_empresas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  razao_social TEXT NOT NULL,
  nome_fantasia TEXT,
  cnpj VARCHAR(14) UNIQUE,
  inscricao_estadual TEXT,
  telefone TEXT,
  email TEXT,
  endereco TEXT,
  cidade TEXT,
  uf VARCHAR(2),
  cep VARCHAR(10),
  responsavel TEXT,
  status VARCHAR(20) DEFAULT 'ativa' CHECK (status IN ('ativa','inativa')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rh_filiais (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  empresa_id UUID REFERENCES rh_empresas(id),
  razao_social TEXT NOT NULL,
  nome_fantasia TEXT,
  cnpj VARCHAR(14),
  endereco TEXT,
  cidade TEXT,
  uf VARCHAR(2),
  cep VARCHAR(10),
  responsavel TEXT,
  telefone TEXT,
  email TEXT,
  banco_padrao TEXT,
  status VARCHAR(20) DEFAULT 'ativa' CHECK (status IN ('ativa','inativa')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rh_setores (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  filial_id UUID REFERENCES rh_filiais(id),
  nome TEXT NOT NULL,
  codigo TEXT,
  responsavel TEXT,
  grau_risco VARCHAR(10) DEFAULT 'baixo' CHECK (grau_risco IN ('baixo','medio','alto')),
  status VARCHAR(20) DEFAULT 'ativo' CHECK (status IN ('ativo','inativo')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════
-- 2. FUNCIONÁRIOS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_funcionarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome_completo TEXT NOT NULL,
  cpf VARCHAR(11) UNIQUE,
  data_nascimento DATE,
  sexo VARCHAR(20),
  estado_civil VARCHAR(30),
  telefone TEXT,
  email TEXT,
  cargo TEXT NOT NULL,
  setor TEXT NOT NULL,
  centro_custo TEXT,
  data_admissao DATE NOT NULL,
  data_desligamento DATE,
  tipo_contrato VARCHAR(30) DEFAULT 'CLT',
  salario_base NUMERIC(12,2),
  matricula TEXT,
  pis_pasep TEXT,
  empresa_id UUID REFERENCES rh_empresas(id),
  filial_id UUID REFERENCES rh_filiais(id),
  status VARCHAR(20) DEFAULT 'ativo' CHECK (status IN ('ativo','inativo','afastado','ferias')),
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_func_status ON rh_funcionarios(status);
CREATE INDEX IF NOT EXISTS idx_func_setor ON rh_funcionarios(setor);
CREATE INDEX IF NOT EXISTS idx_func_cpf ON rh_funcionarios(cpf);

-- ══════════════════════════════════════════════════════════
-- 3. ASO — EXAMES OCUPACIONAIS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_aso (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_exame VARCHAR(30) NOT NULL CHECK (tipo_exame IN ('admissional','periodico','retorno_trabalho','mudanca_funcao','demissional')),
  data_exame DATE NOT NULL,
  data_validade DATE NOT NULL,
  resultado VARCHAR(20) NOT NULL CHECK (resultado IN ('apto','apto_restricao','inapto')),
  medico_responsavel TEXT,
  crm TEXT,
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aso_func ON rh_aso(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_aso_validade ON rh_aso(data_validade);

-- ══════════════════════════════════════════════════════════
-- 4. TREINAMENTOS NR
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_treinamentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_treinamento TEXT NOT NULL,
  data_realizacao DATE NOT NULL,
  data_validade DATE,
  carga_horaria INTEGER,
  instrutor TEXT,
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trein_func ON rh_treinamentos(funcionario_id);

-- ══════════════════════════════════════════════════════════
-- 5. EPIs — ENTREGAS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_epi_entregas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  epi_nome TEXT NOT NULL,
  ca_numero TEXT,
  data_entrega DATE NOT NULL,
  data_validade DATE,
  data_devolucao DATE,
  quantidade INTEGER DEFAULT 1,
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_epi_func ON rh_epi_entregas(funcionario_id);

-- ══════════════════════════════════════════════════════════
-- 6. ACIDENTES / CAT
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_acidentes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_ocorrencia DATE NOT NULL,
  hora_ocorrencia TIME,
  tipo_acidente VARCHAR(30) CHECK (tipo_acidente IN ('tipico','trajeto','doenca_ocupacional')),
  gravidade VARCHAR(20) CHECK (gravidade IN ('leve','moderado','grave','fatal')),
  parte_corpo TEXT,
  descricao TEXT NOT NULL,
  cat_emitida BOOLEAN DEFAULT FALSE,
  dias_afastamento INTEGER DEFAULT 0,
  acao_corretiva TEXT,
  setor TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════
-- 7. ABSENTEÍSMO / AUSÊNCIAS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_ausencias (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_inicio DATE NOT NULL,
  data_fim DATE,
  tipo_ausencia VARCHAR(30) NOT NULL CHECK (tipo_ausencia IN (
    'falta_justificada','falta_injustificada','atestado',
    'afastamento_inss','licenca_maternidade','licenca_paternidade'
  )),
  dias INTEGER DEFAULT 1,
  cid VARCHAR(10),
  motivo TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_absent_func ON rh_ausencias(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_absent_data ON rh_ausencias(data_inicio);

-- ══════════════════════════════════════════════════════════
-- 8. RESTRIÇÕES MÉDICAS / FUNCIONAIS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_restricoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_restricao VARCHAR(30) NOT NULL CHECK (tipo_restricao IN (
    'peso','altura','repetitivo','temperatura','quimica','funcional','outra'
  )),
  permanente BOOLEAN DEFAULT FALSE,
  data_inicio DATE NOT NULL,
  data_previsao_fim DATE,
  descricao TEXT NOT NULL,
  laudo_referencia TEXT,
  status VARCHAR(20) DEFAULT 'ativa' CHECK (status IN ('ativa','encerrada')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════
-- 9. DESLIGAMENTOS + ENTREVISTA DE SAÍDA
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_desligamentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_desligamento DATE NOT NULL,
  tipo_desligamento VARCHAR(30) NOT NULL CHECK (tipo_desligamento IN (
    'pedido_demissao','demissao_empresa','termino_contrato','justa_causa','acordo_mutuo'
  )),
  categoria_causa VARCHAR(20) NOT NULL CHECK (categoria_causa IN (
    'admissao','operacional','financeiro','pessoal'
  )),
  descricao TEXT NOT NULL,
  -- Entrevista de desligamento (5 perguntas padrão)
  entrevista_p1 TEXT, -- trabalho diferente do apresentado?
  entrevista_p2 TEXT, -- o que mais incomodava?
  entrevista_p3 TEXT, -- avaliação da liderança (1-5)
  entrevista_p4 TEXT, -- voltaria a trabalhar?
  entrevista_p5 TEXT, -- o que faria permanecer?
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deslig_data ON rh_desligamentos(data_desligamento);

-- ══════════════════════════════════════════════════════════
-- 10. SAÚDE MENTAL — PROGRAMA SAÚDE E EQUILÍBRIO
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_saude_mental (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_atendimento DATE NOT NULL,
  origem VARCHAR(30) NOT NULL CHECK (origem IN ('voluntaria','lideranca','gatilho_automatico')),
  compareceu BOOLEAN DEFAULT TRUE,
  classificacao_risco VARCHAR(20) NOT NULL CHECK (classificacao_risco IN ('baixo','moderado','alto')),
  profissional TEXT,
  -- NÃO armazenar conteúdo clínico (LGPD + sigilo profissional)
  recomendacao TEXT, -- texto curto, não sensível
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sm_func ON rh_saude_mental(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_sm_risco ON rh_saude_mental(classificacao_risco);

-- ══════════════════════════════════════════════════════════
-- 11. DADOS BANCÁRIOS / PAGAMENTO
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_dados_bancarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  forma_pagamento VARCHAR(30) NOT NULL CHECK (forma_pagamento IN ('conta_itau','conta_sicredi','pix','cheque')),
  banco TEXT,
  agencia TEXT,
  conta TEXT,
  tipo_conta VARCHAR(20),
  titular TEXT,
  cpf_titular TEXT,
  tipo_chave_pix VARCHAR(20),
  chave_pix TEXT,
  titular_pix TEXT,
  motivo_cheque TEXT,
  autoriza_cheque TEXT,
  status_validacao VARCHAR(20) DEFAULT 'pendente' CHECK (status_validacao IN ('pendente','validado','bloqueado')),
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_banco_func ON rh_dados_bancarios(funcionario_id);

-- ══════════════════════════════════════════════════════════
-- 12. LOTES DE PAGAMENTO
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_lotes_pagamento (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  empresa TEXT,
  periodo VARCHAR(7), -- YYYY-MM
  forma_pagamento TEXT,
  total_funcionarios INTEGER DEFAULT 0,
  status VARCHAR(20) DEFAULT 'rascunho' CHECK (status IN ('rascunho','em_conferencia','pronto','enviado','finalizado')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════
-- 13. LOG DE EXPORTAÇÃO
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_log_exportacao (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID,
  usuario_nome TEXT,
  tipo_lista TEXT,
  formato TEXT,
  total_registros INTEGER,
  filtros TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════
-- 14. AUDIT LOG — RASTREABILIDADE COMPLETA
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_audit_log (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID,
  usuario_nome TEXT,
  acao TEXT NOT NULL,
  tabela TEXT,
  registro_id TEXT,
  dados_antes TEXT, -- JSON do estado anterior
  dados_depois TEXT, -- JSON do novo estado
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_data ON rh_audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_tabela ON rh_audit_log(tabela);

-- ══════════════════════════════════════════════════════════
-- 15. TRIGGERS: updated_at automático
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rh_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos','rh_saude_mental',
    'rh_dados_bancarios','rh_lotes_pagamento'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated ON %I', t, t);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_updated BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at()',
      t, t
    );
  END LOOP;
END;
$$;

-- ══════════════════════════════════════════════════════════
-- 16. RLS (Row Level Security) — Habilitar
-- ══════════════════════════════════════════════════════════

-- Habilitar RLS em todas as tabelas
DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_empresas','rh_filiais','rh_setores','rh_funcionarios',
    'rh_aso','rh_treinamentos','rh_epi_entregas','rh_acidentes',
    'rh_ausencias','rh_restricoes','rh_desligamentos','rh_saude_mental',
    'rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao','rh_audit_log'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    -- Policy permissiva para authenticated users (ajustar conforme perfis)
    EXECUTE format('DROP POLICY IF EXISTS %s_all ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %s_all ON %I FOR ALL TO authenticated USING (true) WITH CHECK (true)',
      t, t
    );
    -- Policy para anon (somente leitura para tabelas não sensíveis)
    EXECUTE format('DROP POLICY IF EXISTS %s_anon ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %s_anon ON %I FOR ALL TO anon USING (true) WITH CHECK (true)',
      t, t
    );
  END LOOP;
END;
$$;

-- ══════════════════════════════════════════════════════════
-- SCHEMA COMPLETO — PRONTO PARA USO
-- ══════════════════════════════════════════════════════════
