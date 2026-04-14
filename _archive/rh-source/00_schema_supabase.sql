-- ================================================================
-- CLASSIC RH — Schema Supabase (PostgreSQL)
-- Projeto separado do CQ — dados sensíveis de RH
-- Executar no SQL Editor do Supabase após criar o projeto
-- ================================================================

-- ────────────────────────────────────────────────────────────────
-- 1. TABELA DE USUÁRIOS DO SISTEMA RH
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_usuarios (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  auth_uid      UUID UNIQUE,                    -- FK para auth.users
  nome          TEXT NOT NULL,
  email         TEXT UNIQUE NOT NULL,
  perfil        TEXT NOT NULL DEFAULT 'rh'
                CHECK (perfil IN ('administrador','rh','gestor','visualizador')),
  ativo         BOOLEAN DEFAULT TRUE,
  criado_em     TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ────────────────────────────────────────────────────────────────
-- 2. TABELA DE COLABORADORES
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_colaboradores (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome            TEXT NOT NULL,
  cpf             TEXT UNIQUE NOT NULL,
  matricula       TEXT,
  cargo           TEXT,
  cbo             TEXT,
  salario_base    NUMERIC(12,2),
  data_admissao   DATE NOT NULL,
  data_desligamento DATE,
  cidade          TEXT,
  estado          TEXT DEFAULT 'PR',
  categoria       TEXT DEFAULT '101',              -- 101=CLT, 103=experiência
  estabelecimento TEXT,
  turno           TEXT DEFAULT 'comercial',         -- comercial, 1º turno, 2º turno
  status          TEXT DEFAULT 'ativo'
                  CHECK (status IN ('ativo','afastado','ferias','desligado')),
  unidade         TEXT DEFAULT 'classic'
                  CHECK (unidade IN ('classic','apac')),
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_colab_status ON rh_colaboradores(status);
CREATE INDEX idx_colab_cidade ON rh_colaboradores(cidade);
CREATE INDEX idx_colab_cpf    ON rh_colaboradores(cpf);

-- ────────────────────────────────────────────────────────────────
-- 3. BENEFÍCIOS — CONFIGURAÇÃO POR COLABORADOR
-- ────────────────────────────────────────────────────────────────

-- 3a. VALE ALIMENTAÇÃO
CREATE TABLE IF NOT EXISTS rh_vale_alimentacao (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id  UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
  ativo           BOOLEAN DEFAULT TRUE,
  valor_mensal    NUMERIC(10,2) DEFAULT 300.00,
  percentual_desconto NUMERIC(5,2) DEFAULT 8.00,   -- 8% do salário
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now(),
  UNIQUE(colaborador_id)
);

-- 3b. AUXÍLIO DESLOCAMENTO
CREATE TABLE IF NOT EXISTS rh_auxilio_deslocamento (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id  UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
  ativo           BOOLEAN DEFAULT TRUE,
  cidade_origem   TEXT NOT NULL,
  valor_mensal    NUMERIC(10,2) NOT NULL,
  usa_transporte_empresa BOOLEAN DEFAULT FALSE,
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now(),
  UNIQUE(colaborador_id)
);

-- 3c. PLANO ALIANÇA
CREATE TABLE IF NOT EXISTS rh_plano_alianca (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id  UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
  ativo           BOOLEAN DEFAULT TRUE,
  data_adesao     DATE DEFAULT CURRENT_DATE,
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now(),
  UNIQUE(colaborador_id)
);

-- ────────────────────────────────────────────────────────────────
-- 4. TABELA DE VALORES DO AUXÍLIO POR CIDADE (referência)
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_deslocamento_cidades (
  id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cidade    TEXT UNIQUE NOT NULL,
  valor     NUMERIC(10,2) NOT NULL,
  distancia_km NUMERIC(5,1),
  ativo     BOOLEAN DEFAULT TRUE
);

INSERT INTO rh_deslocamento_cidades (cidade, valor, distancia_km) VALUES
  ('Jardim Alegre', 150.00, 7.6),
  ('Lidianópolis',  200.00, 9.0),
  ('Ivaiporã',      200.00, 16.5),
  ('Lunardelli',    270.00, 8.0),
  ('São João do Ivaí', 270.00, NULL),
  ('São Pedro do Ivaí', 320.00, NULL);

-- ────────────────────────────────────────────────────────────────
-- 5. APURAÇÃO MENSAL (competência 27–26)
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_apuracao_mensal (
  id                UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id    UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
  competencia       TEXT NOT NULL,                   -- ex: '2026-03' (mês de pagamento)
  periodo_inicio    DATE NOT NULL,                   -- ex: 2026-02-27
  periodo_fim       DATE NOT NULL,                   -- ex: 2026-03-26

  -- Dados de frequência (importados do Secullum)
  dias_trabalhados       INT DEFAULT 0,
  dias_uteis_periodo     INT DEFAULT 0,
  faltas_justificadas    INT DEFAULT 0,
  faltas_injustificadas  INT DEFAULT 0,
  atrasos_acima_10min    INT DEFAULT 0,
  dias_atestado          INT DEFAULT 0,
  saidas_antecipadas     INT DEFAULT 0,
  advertencias           INT DEFAULT 0,
  suspensoes             INT DEFAULT 0,
  em_ferias              BOOLEAN DEFAULT FALSE,
  dias_ferias            INT DEFAULT 0,
  afastado_inss          BOOLEAN DEFAULT FALSE,
  licenca_maternidade    BOOLEAN DEFAULT FALSE,

  -- Resultados calculados
  elegivel_assiduidade   BOOLEAN DEFAULT FALSE,
  valor_assiduidade      NUMERIC(10,2) DEFAULT 0,
  motivo_perda_assiduidade TEXT,

  elegivel_va            BOOLEAN DEFAULT TRUE,
  valor_va               NUMERIC(10,2) DEFAULT 0,
  desconto_va            NUMERIC(10,2) DEFAULT 0,

  elegivel_deslocamento  BOOLEAN DEFAULT FALSE,
  valor_deslocamento     NUMERIC(10,2) DEFAULT 0,
  motivo_suspensao_desloc TEXT,

  plano_alianca_ativo    BOOLEAN DEFAULT FALSE,

  -- Totais
  total_beneficios       NUMERIC(10,2) DEFAULT 0,
  total_descontos        NUMERIC(10,2) DEFAULT 0,

  -- Controle
  status                 TEXT DEFAULT 'rascunho'
                         CHECK (status IN ('rascunho','calculado','validado','pago')),
  calculado_em           TIMESTAMPTZ,
  validado_por           TEXT,
  validado_em            TIMESTAMPTZ,
  criado_em              TIMESTAMPTZ DEFAULT now(),
  atualizado_em          TIMESTAMPTZ DEFAULT now(),

  UNIQUE(colaborador_id, competencia)
);

CREATE INDEX idx_apuracao_comp   ON rh_apuracao_mensal(competencia);
CREATE INDEX idx_apuracao_status ON rh_apuracao_mensal(status);

-- ────────────────────────────────────────────────────────────────
-- 5b. OCORRÊNCIAS (atestados, advertências, suspensões)
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_ocorrencias (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id  UUID REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
  tipo            TEXT NOT NULL
                  CHECK (tipo IN ('atestado','advertencia_verbal','advertencia_escrita','suspensao')),
  data_inicio     DATE NOT NULL,
  data_fim        DATE,
  dias            INT DEFAULT 1,
  competencia     TEXT,                              -- ex: '2026-03'
  categoria       TEXT,                              -- '3_dias_ou_mais' ou 'ate_2_dias' (para atestados)
  observacoes     TEXT,
  arquivo_nome    TEXT,                              -- nome original do arquivo
  criado_em       TIMESTAMPTZ DEFAULT now(),
  atualizado_em   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_ocorr_colab ON rh_ocorrencias(colaborador_id);
CREATE INDEX idx_ocorr_comp  ON rh_ocorrencias(competencia);
CREATE INDEX idx_ocorr_tipo  ON rh_ocorrencias(tipo);

-- ────────────────────────────────────────────────────────────────
-- 6. SESSÕES E AUDIT LOG
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_sessoes (
  id             UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID,
  usuario_email  TEXT,
  session_token  TEXT,
  dispositivo    TEXT,
  ativa          BOOLEAN DEFAULT TRUE,
  expira_em      TIMESTAMPTZ,
  ultimo_acesso  TIMESTAMPTZ,
  encerrada_em   TIMESTAMPTZ,
  criado_em      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rh_audit_log (
  id             UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID,
  usuario_email  TEXT,
  usuario_nome   TEXT,
  acao           TEXT NOT NULL,
  detalhes       TEXT,
  criado_em      TIMESTAMPTZ DEFAULT now()
);

-- ────────────────────────────────────────────────────────────────
-- 7. RPC: Perfil do usuário autenticado
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION rh_meu_perfil()
RETURNS SETOF rh_usuarios
LANGUAGE sql SECURITY DEFINER
AS $$
  SELECT * FROM rh_usuarios
  WHERE auth_uid = auth.uid()
    AND ativo = true
  LIMIT 1;
$$;

-- ────────────────────────────────────────────────────────────────
-- 8. RLS (Row Level Security)
-- ────────────────────────────────────────────────────────────────
ALTER TABLE rh_usuarios            ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_colaboradores       ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_vale_alimentacao    ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_auxilio_deslocamento ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_plano_alianca       ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_deslocamento_cidades ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_apuracao_mensal     ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_sessoes             ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_ocorrencias         ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_audit_log           ENABLE ROW LEVEL SECURITY;

-- Política: usuários autenticados podem ler/escrever
CREATE POLICY "rh_all_auth" ON rh_usuarios          FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_colaboradores     FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_vale_alimentacao  FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_auxilio_deslocamento FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_plano_alianca     FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_deslocamento_cidades FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_apuracao_mensal   FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_sessoes           FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_ocorrencias        FOR ALL USING (auth.role() = 'authenticated');
CREATE POLICY "rh_all_auth" ON rh_audit_log         FOR ALL USING (auth.role() = 'authenticated');

-- Leitura anon para cidades (referência pública)
CREATE POLICY "rh_cidades_anon" ON rh_deslocamento_cidades FOR SELECT USING (true);
