-- ============================================================
-- Schema RH & SST (Saúde e Segurança do Trabalho)
-- Classic BI — v2026.03
-- Inclui: Permissões granulares, Audit Log, Motor Lógico,
--         Saúde Mental, Restrições, Indicadores
-- ============================================================

-- ══════════════════════════════════════════════════════
-- PARTE 1 — ESTRUTURA BASE RH
-- ══════════════════════════════════════════════════════

-- ── DEPARTAMENTOS ──────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_departamentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL UNIQUE,
  responsavel TEXT,
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── CARGOS ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cargos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  departamento_id UUID REFERENCES rh_departamentos(id),
  cbo TEXT,
  grau_risco INT DEFAULT 1 CHECK (grau_risco BETWEEN 1 AND 4),
  periculosidade BOOLEAN DEFAULT false,
  insalubridade BOOLEAN DEFAULT false,
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── FUNCIONÁRIOS ───────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_funcionarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  cpf TEXT UNIQUE,
  rg TEXT,
  data_nascimento DATE,
  telefone TEXT,
  email TEXT,
  endereco TEXT,
  cidade TEXT,
  uf CHAR(2),
  cargo_id UUID REFERENCES rh_cargos(id),
  departamento_id UUID REFERENCES rh_departamentos(id),
  data_admissao DATE NOT NULL,
  data_demissao DATE,
  tipo_contrato TEXT DEFAULT 'CLT' CHECK (tipo_contrato IN ('CLT','PJ','Temporário','Estagiário','Aprendiz')),
  turno TEXT DEFAULT 'Diurno' CHECK (turno IN ('Diurno','Noturno','Revezamento')),
  matricula TEXT UNIQUE,
  status TEXT DEFAULT 'Ativo' CHECK (status IN ('Ativo','Afastado','Férias','Desligado')),
  foto_url TEXT,
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── RESTRIÇÕES DO FUNCIONÁRIO ──────────────────────
-- Ex: "Não pode pegar peso", "Afastado de ruído", etc.
CREATE TABLE IF NOT EXISTS rh_restricoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo TEXT NOT NULL CHECK (tipo IN ('Médica','Ergonômica','Psicossocial','Judicial','Outra')),
  descricao TEXT NOT NULL,
  data_inicio DATE NOT NULL,
  data_fim DATE,                    -- NULL = permanente
  origem TEXT CHECK (origem IN ('ASO','Atestado','Perícia','Liderança','RH')),
  ativa BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ABSENTEÍSMO (Faltas/Ausências) ────────────────
CREATE TABLE IF NOT EXISTS rh_absenteismo (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data DATE NOT NULL,
  tipo TEXT NOT NULL CHECK (tipo IN ('Falta Justificada','Falta Injustificada','Atestado','Afastamento INSS','Licença','Atraso')),
  horas NUMERIC(4,1),
  motivo TEXT,
  documento_url TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 2 — SST (Saúde e Segurança do Trabalho)
-- ══════════════════════════════════════════════════════

-- ── ASO (Atestado de Saúde Ocupacional) ───────────
CREATE TABLE IF NOT EXISTS sst_aso (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo TEXT NOT NULL CHECK (tipo IN ('Admissional','Periódico','Retorno ao Trabalho','Mudança de Função','Demissional')),
  data_exame DATE NOT NULL,
  data_vencimento DATE NOT NULL,
  resultado TEXT DEFAULT 'Apto' CHECK (resultado IN ('Apto','Inapto','Apto com Restrição')),
  restricoes_texto TEXT,            -- detalhe das restrições quando "Apto com Restrição"
  medico_nome TEXT,
  crm TEXT,
  observacoes TEXT,
  arquivo_url TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── TREINAMENTOS (catálogo) ────────────────────────
CREATE TABLE IF NOT EXISTS sst_treinamentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  descricao TEXT,
  carga_horaria INT,
  validade_meses INT DEFAULT 12,
  obrigatorio BOOLEAN DEFAULT true,
  nr_referencia TEXT,
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── TREINAMENTOS × FUNCIONÁRIO ─────────────────────
CREATE TABLE IF NOT EXISTS sst_treinamento_funcionario (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  treinamento_id UUID NOT NULL REFERENCES sst_treinamentos(id),
  data_realizacao DATE NOT NULL,
  data_vencimento DATE NOT NULL,
  instrutor TEXT,
  certificado_url TEXT,
  status TEXT DEFAULT 'Válido' CHECK (status IN ('Válido','Vencido','Agendado')),
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── EPIs (catálogo) ────────────────────────────────
CREATE TABLE IF NOT EXISTS sst_epis (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  ca TEXT,
  ca_validade DATE,
  fabricante TEXT,
  categoria TEXT CHECK (categoria IN ('Cabeça','Olhos/Face','Respiratório','Auditivo','Tronco','Membros Superiores','Membros Inferiores','Corpo Inteiro','Queda')),
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ENTREGA DE EPI ─────────────────────────────────
CREATE TABLE IF NOT EXISTS sst_epi_entrega (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  epi_id UUID NOT NULL REFERENCES sst_epis(id),
  data_entrega DATE NOT NULL,
  quantidade INT DEFAULT 1,
  motivo TEXT DEFAULT 'Primeira Entrega' CHECK (motivo IN ('Primeira Entrega','Substituição','Desgaste','Perda','Devolução')),
  assinatura_url TEXT,
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── EPIs OBRIGATÓRIOS POR CARGO ────────────────────
CREATE TABLE IF NOT EXISTS sst_epi_cargo (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cargo_id UUID NOT NULL REFERENCES rh_cargos(id),
  epi_id UUID NOT NULL REFERENCES sst_epis(id),
  obrigatorio BOOLEAN DEFAULT true,
  UNIQUE(cargo_id, epi_id)
);

-- ── CAT (Comunicação de Acidente de Trabalho) ──────
CREATE TABLE IF NOT EXISTS sst_cat (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_acidente TIMESTAMPTZ NOT NULL,
  data_comunicacao DATE,
  tipo TEXT NOT NULL CHECK (tipo IN ('Típico','Trajeto','Doença Ocupacional')),
  gravidade TEXT DEFAULT 'Leve' CHECK (gravidade IN ('Leve','Moderado','Grave','Fatal')),
  parte_corpo TEXT,
  descricao TEXT NOT NULL,
  local_acidente TEXT,
  setor TEXT,
  testemunhas TEXT,
  dias_afastamento INT DEFAULT 0,
  numero_cat TEXT,
  status TEXT DEFAULT 'Aberta' CHECK (status IN ('Aberta','Investigando','Encerrada')),
  causa_raiz TEXT,
  acoes_corretivas TEXT,
  arquivo_url TEXT,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── INSPEÇÕES DE SEGURANÇA ─────────────────────────
CREATE TABLE IF NOT EXISTS sst_inspecoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo TEXT NOT NULL CHECK (tipo IN ('Rotina','Extraordinária','CIPA','Auditoria')),
  setor TEXT,
  data_inspecao DATE NOT NULL,
  inspetor TEXT NOT NULL,
  conformidades INT DEFAULT 0,
  nao_conformidades INT DEFAULT 0,
  observacoes TEXT,
  status TEXT DEFAULT 'Aberta' CHECK (status IN ('Aberta','Em Andamento','Concluída')),
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 3 — SAÚDE MENTAL (Programa Saúde e Equilíbrio)
-- ══════════════════════════════════════════════════════

-- ── ATENDIMENTOS PSICOLÓGICOS ──────────────────────
CREATE TABLE IF NOT EXISTS sm_atendimentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_agendamento TIMESTAMPTZ NOT NULL,
  data_atendimento TIMESTAMPTZ,
  origem TEXT NOT NULL CHECK (origem IN ('Voluntário','Indicação Liderança','Gatilho Automático')),
  gatilho_detalhe TEXT,             -- qual regra disparou (se automático)
  compareceu BOOLEAN,
  classificacao_risco TEXT CHECK (classificacao_risco IN ('Baixo','Moderado','Alto')),
  recomendacao TEXT,                -- texto curto, não sensível
  profissional TEXT,
  crp TEXT,                         -- registro do psicólogo
  status TEXT DEFAULT 'Agendado' CHECK (status IN ('Agendado','Realizado','Não Compareceu','Cancelado')),
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);
-- IMPORTANTE: NÃO armazena conteúdo clínico (LGPD + sigilo)

-- ── GATILHOS AUTOMÁTICOS (configuração) ────────────
CREATE TABLE IF NOT EXISTS sm_gatilhos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  descricao TEXT,
  tipo TEXT NOT NULL CHECK (tipo IN ('faltas','produtividade','afastamento','manual')),
  parametro_valor NUMERIC,          -- ex: 3 (faltas), 20 (% queda)
  parametro_periodo_dias INT,       -- ex: 30 (últimos 30 dias)
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ALERTAS GERADOS ────────────────────────────────
CREATE TABLE IF NOT EXISTS sm_alertas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  gatilho_id UUID REFERENCES sm_gatilhos(id),
  tipo TEXT NOT NULL CHECK (tipo IN ('Sugestão Atendimento','Alerta RH','Flag Atenção')),
  descricao TEXT NOT NULL,
  dados_json JSONB,                 -- dados que geraram o alerta
  lido BOOLEAN DEFAULT false,
  acao_tomada TEXT,
  resolvido_por TEXT,
  resolvido_em TIMESTAMPTZ,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 3B — ANÁLISE DE DESLIGAMENTOS / TURNOVER
-- ══════════════════════════════════════════════════════

-- ── DESLIGAMENTOS ──────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_desligamentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_desligamento DATE NOT NULL,
  data_aviso DATE,                  -- data do aviso prévio

  -- NÍVEL 1: Tipo
  tipo TEXT NOT NULL CHECK (tipo IN ('Pedido de Demissão','Demissão pela Empresa','Término de Contrato','Acordo Mútuo','Justa Causa')),

  -- NÍVEL 2: Categoria (causa raiz)
  categoria TEXT NOT NULL CHECK (categoria IN ('Admissão','Operacional','Financeiro','Pessoal','Disciplinar')),

  -- NÍVEL 2: Subcategoria detalhada
  subcategoria TEXT CHECK (subcategoria IN (
    -- Admissão
    'Não se adaptou','Expectativa errada','Falta de habilidade','Perfil inadequado',
    -- Operacional
    'Liderança','Ambiente de trabalho','Carga excessiva','Conflito com equipe','Falta de crescimento',
    -- Financeiro
    'Salário','Benefícios','Proposta externa',
    -- Pessoal
    'Família','Mudança de cidade','Saúde','Estudos',
    -- Disciplinar
    'Indisciplina','Abandono','Insubordinação'
  )),

  -- NÍVEL 3: Descrição
  descricao TEXT NOT NULL,

  -- Metadados para análise
  tempo_empresa_dias INT,           -- calculado automaticamente
  setor_no_desligamento TEXT,
  cargo_no_desligamento TEXT,
  lider_no_desligamento TEXT,
  turno_no_desligamento TEXT,
  salario_no_desligamento NUMERIC(10,2),
  custo_estimado_desligamento NUMERIC(10,2),  -- custo rescisão + reposição

  -- Quem registrou
  registrado_por TEXT,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ENTREVISTA DE DESLIGAMENTO ─────────────────────
CREATE TABLE IF NOT EXISTS rh_entrevista_desligamento (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  desligamento_id UUID NOT NULL REFERENCES rh_desligamentos(id),
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_entrevista DATE NOT NULL,
  entrevistador TEXT,

  -- Perguntas obrigatórias (respostas estruturadas)
  trabalho_diferente_admissao TEXT CHECK (trabalho_diferente_admissao IN ('Sim','Não','Parcialmente')),
  trabalho_diferente_detalhe TEXT,

  maior_incomodo TEXT,              -- campo aberto

  avaliacao_lideranca INT CHECK (avaliacao_lideranca BETWEEN 1 AND 5),
  avaliacao_lideranca_comentario TEXT,

  avaliacao_ambiente INT CHECK (avaliacao_ambiente BETWEEN 1 AND 5),
  avaliacao_treinamento INT CHECK (avaliacao_treinamento BETWEEN 1 AND 5),
  avaliacao_salario INT CHECK (avaliacao_salario BETWEEN 1 AND 5),

  voltaria_trabalhar TEXT CHECK (voltaria_trabalhar IN ('Sim','Não','Talvez')),
  voltaria_comentario TEXT,

  o_que_faria_permanecer TEXT,      -- campo aberto

  recomendaria_empresa TEXT CHECK (recomendaria_empresa IN ('Sim','Não','Talvez')),
  nps_score INT CHECK (nps_score BETWEEN 0 AND 10),  -- Net Promoter Score

  observacoes_gerais TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 4 — PERMISSÕES GRANULARES RH/SST
-- ══════════════════════════════════════════════════════

-- ── PERFIS RH/SST ──────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_perfis (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL UNIQUE,
  descricao TEXT,
  cor TEXT DEFAULT '#2E6B8A',
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── PERMISSÕES POR PERFIL (padrão) ─────────────────
CREATE TABLE IF NOT EXISTS rh_perfil_permissoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  perfil_id UUID NOT NULL REFERENCES rh_perfis(id),
  modulo TEXT NOT NULL,             -- 'funcionarios','aso','treinamentos','epis','cat','saude_mental','dashboard','ajuda'
  pode_ver BOOLEAN DEFAULT false,
  pode_criar BOOLEAN DEFAULT false,
  pode_editar BOOLEAN DEFAULT false,
  pode_excluir BOOLEAN DEFAULT false,
  UNIQUE(perfil_id, modulo)
);

-- ── PERMISSÕES CUSTOMIZADAS POR USUÁRIO ────────────
-- Sobrescreve o perfil quando necessário
CREATE TABLE IF NOT EXISTS rh_usuario_permissoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID NOT NULL,         -- ref cq_usuarios.id
  rh_perfil_id UUID REFERENCES rh_perfis(id),
  modulo TEXT,                      -- se preenchido, é override granular
  pode_ver BOOLEAN,
  pode_criar BOOLEAN,
  pode_editar BOOLEAN,
  pode_excluir BOOLEAN,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 5 — AUDIT LOG (Antes/Depois)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_audit_log (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID,
  usuario_nome TEXT,
  acao TEXT NOT NULL,                -- 'criar','editar','excluir','entregar_epi','alterar_cargo', etc.
  tabela TEXT NOT NULL,             -- nome da tabela afetada
  registro_id UUID,                 -- ID do registro afetado
  dados_antes JSONB,                -- snapshot antes da alteração
  dados_depois JSONB,               -- snapshot depois da alteração
  descricao TEXT,                   -- descrição legível da ação
  ip TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 6 — ÍNDICES
-- ══════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_func_status ON rh_funcionarios(status);
CREATE INDEX IF NOT EXISTS idx_func_depto ON rh_funcionarios(departamento_id);
CREATE INDEX IF NOT EXISTS idx_func_cargo ON rh_funcionarios(cargo_id);
CREATE INDEX IF NOT EXISTS idx_aso_func ON sst_aso(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_aso_venc ON sst_aso(data_vencimento);
CREATE INDEX IF NOT EXISTS idx_trein_func ON sst_treinamento_funcionario(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_trein_venc ON sst_treinamento_funcionario(data_vencimento);
CREATE INDEX IF NOT EXISTS idx_epi_func ON sst_epi_entrega(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cat_func ON sst_cat(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cat_data ON sst_cat(data_acidente);
CREATE INDEX IF NOT EXISTS idx_sm_atend_func ON sm_atendimentos(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_sm_alertas_func ON sm_alertas(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_sm_alertas_lido ON sm_alertas(lido);
CREATE INDEX IF NOT EXISTS idx_absenteismo_func ON rh_absenteismo(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_absenteismo_data ON rh_absenteismo(data);
CREATE INDEX IF NOT EXISTS idx_restricoes_func ON rh_restricoes(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_deslig_func ON rh_desligamentos(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_deslig_data ON rh_desligamentos(data_desligamento);
CREATE INDEX IF NOT EXISTS idx_deslig_tipo ON rh_desligamentos(tipo);
CREATE INDEX IF NOT EXISTS idx_deslig_categ ON rh_desligamentos(categoria);
CREATE INDEX IF NOT EXISTS idx_entrev_deslig ON rh_entrevista_desligamento(desligamento_id);
CREATE INDEX IF NOT EXISTS idx_audit_tabela ON rh_audit_log(tabela);
CREATE INDEX IF NOT EXISTS idx_audit_registro ON rh_audit_log(registro_id);
CREATE INDEX IF NOT EXISTS idx_audit_data ON rh_audit_log(criado_em);

-- ══════════════════════════════════════════════════════
-- PARTE 7 — DADOS INICIAIS
-- ══════════════════════════════════════════════════════

-- Departamentos
INSERT INTO rh_departamentos (nome, responsavel) VALUES
  ('Produção', NULL),
  ('Manutenção', NULL),
  ('Logística', NULL),
  ('Qualidade', NULL),
  ('Administrativo', NULL),
  ('RH / SST', NULL),
  ('Comercial', NULL)
ON CONFLICT (nome) DO NOTHING;

-- Treinamentos obrigatórios
INSERT INTO sst_treinamentos (nome, nr_referencia, carga_horaria, validade_meses, obrigatorio) VALUES
  ('Integração de Segurança', 'NR-01', 8, 24, true),
  ('EPI - Uso e Conservação', 'NR-06', 4, 12, true),
  ('CIPA', 'NR-05', 20, 12, true),
  ('Combate a Incêndio', 'NR-23', 8, 12, true),
  ('Primeiros Socorros', 'NR-07', 8, 24, true),
  ('Trabalho em Altura', 'NR-35', 8, 24, true),
  ('Espaço Confinado', 'NR-33', 16, 12, true),
  ('Operação de Empilhadeira', 'NR-11', 16, 12, true),
  ('Segurança em Máquinas', 'NR-12', 8, 24, true),
  ('Ergonomia', 'NR-17', 4, 24, true),
  ('Produtos Químicos', 'NR-26', 4, 12, true),
  ('Instalações Elétricas (NR-10)', 'NR-10', 40, 24, true)
ON CONFLICT DO NOTHING;

-- EPIs comuns em frigorífico
INSERT INTO sst_epis (nome, categoria, fabricante) VALUES
  ('Capacete de Segurança', 'Cabeça', NULL),
  ('Óculos de Proteção', 'Olhos/Face', NULL),
  ('Protetor Auricular Plug', 'Auditivo', NULL),
  ('Protetor Auricular Concha', 'Auditivo', NULL),
  ('Luva de Malha de Aço', 'Membros Superiores', NULL),
  ('Luva Nitrílica', 'Membros Superiores', NULL),
  ('Luva Térmica', 'Membros Superiores', NULL),
  ('Bota de Segurança PVC', 'Membros Inferiores', NULL),
  ('Bota de Segurança Couro', 'Membros Inferiores', NULL),
  ('Avental PVC', 'Tronco', NULL),
  ('Avental Malha de Aço', 'Tronco', NULL),
  ('Máscara Respiratória PFF2', 'Respiratório', NULL),
  ('Cinto de Segurança', 'Queda', NULL),
  ('Mangote de Proteção', 'Membros Superiores', NULL),
  ('Touca Descartável', 'Cabeça', NULL)
ON CONFLICT DO NOTHING;

-- Perfis RH/SST
INSERT INTO rh_perfis (nome, descricao, cor) VALUES
  ('rh_gestor', 'Acesso total ao módulo RH/SST', '#c8a96e'),
  ('rh_operador', 'Consulta e lançamentos operacionais', '#2E6B8A'),
  ('sst_tecnico', 'Técnico de Segurança — foco SST', '#27ae60'),
  ('psicologo', 'Profissional de Saúde Mental', '#b07fd3'),
  ('lider_setor', 'Líder de setor — visualização limitada', '#f39c12')
ON CONFLICT (nome) DO NOTHING;

-- Permissões padrão: rh_gestor (tudo)
INSERT INTO rh_perfil_permissoes (perfil_id, modulo, pode_ver, pode_criar, pode_editar, pode_excluir)
SELECT p.id, m.modulo, true, true, true, true
FROM rh_perfis p, (VALUES
  ('funcionarios'),('aso'),('treinamentos'),('epis'),('cat'),
  ('saude_mental'),('dashboard'),('restricoes'),('absenteismo'),('desligamentos'),('ajuda')
) AS m(modulo)
WHERE p.nome = 'rh_gestor'
ON CONFLICT (perfil_id, modulo) DO NOTHING;

-- Permissões padrão: rh_operador
INSERT INTO rh_perfil_permissoes (perfil_id, modulo, pode_ver, pode_criar, pode_editar, pode_excluir)
SELECT p.id, m.modulo, m.ver, m.criar, m.editar, false
FROM rh_perfis p, (VALUES
  ('funcionarios', true, false, false),
  ('aso', true, true, false),
  ('treinamentos', true, true, false),
  ('epis', true, true, false),
  ('cat', true, true, false),
  ('dashboard', true, false, false),
  ('ajuda', true, false, false)
) AS m(modulo, ver, criar, editar)
WHERE p.nome = 'rh_operador'
ON CONFLICT (perfil_id, modulo) DO NOTHING;

-- Permissões padrão: sst_tecnico
INSERT INTO rh_perfil_permissoes (perfil_id, modulo, pode_ver, pode_criar, pode_editar, pode_excluir)
SELECT p.id, m.modulo, m.ver, m.criar, m.editar, false
FROM rh_perfis p, (VALUES
  ('funcionarios', true, false, false),
  ('aso', true, true, true),
  ('treinamentos', true, true, true),
  ('epis', true, true, true),
  ('cat', true, true, true),
  ('restricoes', true, true, true),
  ('dashboard', true, false, false),
  ('ajuda', true, false, false)
) AS m(modulo, ver, criar, editar)
WHERE p.nome = 'sst_tecnico'
ON CONFLICT (perfil_id, modulo) DO NOTHING;

-- Permissões padrão: psicologo
INSERT INTO rh_perfil_permissoes (perfil_id, modulo, pode_ver, pode_criar, pode_editar, pode_excluir)
SELECT p.id, m.modulo, m.ver, m.criar, m.editar, false
FROM rh_perfis p, (VALUES
  ('funcionarios', true, false, false),
  ('saude_mental', true, true, true),
  ('dashboard', true, false, false),
  ('ajuda', true, false, false)
) AS m(modulo, ver, criar, editar)
WHERE p.nome = 'psicologo'
ON CONFLICT (perfil_id, modulo) DO NOTHING;

-- Permissões padrão: lider_setor
INSERT INTO rh_perfil_permissoes (perfil_id, modulo, pode_ver, pode_criar, pode_editar, pode_excluir)
SELECT p.id, m.modulo, true, false, false, false
FROM rh_perfis p, (VALUES ('dashboard'),('ajuda')) AS m(modulo)
WHERE p.nome = 'lider_setor'
ON CONFLICT (perfil_id, modulo) DO NOTHING;

-- Gatilhos automáticos de Saúde Mental
INSERT INTO sm_gatilhos (nome, descricao, tipo, parametro_valor, parametro_periodo_dias) VALUES
  ('Faltas frequentes', 'Mais de 3 faltas no mês', 'faltas', 3, 30),
  ('Queda de produtividade', 'Queda >20% na produtividade', 'produtividade', 20, 30),
  ('Retorno de afastamento', 'Funcionário retornando de afastamento INSS', 'afastamento', NULL, NULL),
  ('Indicação manual', 'Indicação direta da liderança', 'manual', NULL, NULL)
ON CONFLICT DO NOTHING;
