-- ════════════════════════════════════════════════════════════════════════════
-- MÓDULO SST — SEGURANÇA E SAÚDE DO TRABALHO
-- Classic / APAC · Supabase (PostgreSQL 17)
-- Depende de: FUNDACAO_BANCO_v2.sql
-- ════════════════════════════════════════════════════════════════════════════
--
-- DECISÕES ARQUITETURAIS:
--
-- 1. CATÁLOGOS SEPARADOS: sst_tipo_treinamento e sst_catalogo_epi são tabelas
--    de referência. Evitam repetição de metadados (norma, validade_meses, CA)
--    em cada registro operacional.
--
-- 2. MATRIZES DE ELEGIBILIDADE: sst_cargo_treinamento e sst_cargo_epi definem
--    QUEM precisa de QUÊ baseado no cargo. Motor de elegibilidade cruza essas
--    matrizes com os registros realizados.
--
-- 3. MOTOR DE ELEGIBILIDADE (3 funções):
--    - sst_elegibilidade_aso(): quem precisa de ASO e situação do último
--    - sst_elegibilidade_treinamento(): pendências por cargo×treinamento
--    - sst_elegibilidade_epi(): pendências por cargo×EPI
--    Todas retornam 'situacao' classificada e ordenada por urgência.
--
-- 4. MOTOR DE APTIDÃO (2 funções):
--    - sst_aptidao_funcionario(id): classifica 1 pessoa com motivos
--    - sst_aptidao_geral(): classifica todos, ordenados por criticidade
--    Regras: sem ASO = inapto, ASO vencido = inapto, treinamento obrigatório
--    pendente/vencido = inapto, restrição ativa = apto_restricao.
--
-- 5. TRIGGER AUTOMÁTICO: data_validade do treinamento é calculada
--    automaticamente a partir da periodicidade do catálogo. Se informada
--    manualmente, não sobrescreve.
--
-- 6. ESTRUTURA EPI preparada para integração com BuscaEPI via ca_numero.
--
-- 7. NÃO CONFORMIDADES podem ser vinculadas a funcionário (comportamental)
--    OU a setor (ambiental), com workflow de tratamento.
--
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════
-- PARTE 1: TABELAS DE CATÁLOGO (REFERÊNCIA)
-- ════════════════════════════════════════════════════════════════

-- Catálogo de tipos de treinamento NR
CREATE TABLE sst_tipo_treinamento (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome                TEXT NOT NULL,
  norma_regulamentadora TEXT,
  carga_horaria_minima INTEGER,
  validade_meses      INTEGER,
  descricao           TEXT,
  ativo               BOOLEAN NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  UNIQUE(nome, norma_regulamentadora)
);

-- Catálogo de EPIs (preparado para BuscaEPI)
CREATE TABLE sst_catalogo_epi (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  descricao           TEXT NOT NULL,
  ca_numero           TEXT,
  ca_validade         DATE,
  fabricante          TEXT,
  categoria           TEXT,
  grupo_risco         TEXT,
  ativo               BOOLEAN NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id)
);


-- ════════════════════════════════════════════════════════════════
-- PARTE 2: MATRIZES DE ELEGIBILIDADE
-- ════════════════════════════════════════════════════════════════

-- Matriz cargo × treinamento obrigatório
CREATE TABLE sst_cargo_treinamento (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cargo_id            UUID NOT NULL REFERENCES rh_cargos(id),
  tipo_treinamento_id UUID NOT NULL REFERENCES sst_tipo_treinamento(id),
  obrigatorio         BOOLEAN NOT NULL DEFAULT TRUE,
  observacao          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  UNIQUE(cargo_id, tipo_treinamento_id)
);

-- Matriz cargo × EPI obrigatório
CREATE TABLE sst_cargo_epi (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cargo_id            UUID NOT NULL REFERENCES rh_cargos(id),
  catalogo_epi_id     UUID NOT NULL REFERENCES sst_catalogo_epi(id),
  obrigatorio         BOOLEAN NOT NULL DEFAULT TRUE,
  quantidade_padrao   INTEGER NOT NULL DEFAULT 1,
  periodicidade_meses INTEGER,
  observacao          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  UNIQUE(cargo_id, catalogo_epi_id)
);


-- ════════════════════════════════════════════════════════════════
-- PARTE 3: TABELAS OPERACIONAIS
-- ════════════════════════════════════════════════════════════════

-- ASO — Atestados de Saúde Ocupacional
CREATE TABLE sst_aso (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_exame          VARCHAR(30) NOT NULL
                      CHECK (tipo_exame IN ('admissional','periodico','retorno_trabalho','mudanca_funcao','demissional')),
  data_exame          DATE NOT NULL,
  data_validade       DATE NOT NULL,
  resultado           VARCHAR(30) NOT NULL
                      CHECK (resultado IN ('apto','apto_restricao','inapto')),
  restricoes          TEXT,
  medico_responsavel  TEXT NOT NULL,
  crm                 TEXT,
  clinica             TEXT,
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  CONSTRAINT chk_aso_datas CHECK (data_validade >= data_exame)
);

-- Treinamentos realizados
CREATE TABLE sst_treinamento (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_treinamento_id UUID NOT NULL REFERENCES sst_tipo_treinamento(id),
  data_realizacao     DATE NOT NULL,
  data_validade       DATE,
  carga_horaria       INTEGER,
  instrutor           TEXT,
  entidade            TEXT,
  certificado_numero  TEXT,
  aprovado            BOOLEAN NOT NULL DEFAULT TRUE,
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  CONSTRAINT chk_trein_validade CHECK (data_validade IS NULL OR data_validade >= data_realizacao)
);

-- Entregas de EPI
CREATE TABLE sst_epi_entrega (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id),
  catalogo_epi_id     UUID NOT NULL REFERENCES sst_catalogo_epi(id),
  data_entrega        DATE NOT NULL,
  data_devolucao      DATE,
  quantidade          INTEGER NOT NULL DEFAULT 1 CHECK (quantidade > 0),
  motivo_entrega      VARCHAR(30) DEFAULT 'primeira_entrega'
                      CHECK (motivo_entrega IN ('primeira_entrega','substituicao','perda','desgaste','mudanca_funcao')),
  assinatura_digital  BOOLEAN DEFAULT FALSE,
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id)
);

-- Restrições médicas/funcionais
CREATE TABLE sst_restricao (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_restricao      VARCHAR(30) NOT NULL
                      CHECK (tipo_restricao IN ('peso','altura','repetitivo','temperatura','quimica','ruido','funcional','ergonomica','outra')),
  permanente          BOOLEAN NOT NULL DEFAULT FALSE,
  data_inicio         DATE NOT NULL,
  data_previsao_fim   DATE,
  descricao           TEXT NOT NULL,
  laudo_referencia    TEXT,
  aso_origem_id       UUID REFERENCES sst_aso(id),
  status              VARCHAR(20) NOT NULL DEFAULT 'ativa'
                      CHECK (status IN ('ativa','encerrada','reavaliacao')),
  encerrada_em        DATE,
  encerrada_por       UUID REFERENCES auth.users(id),
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  CONSTRAINT chk_restr_permanente CHECK (permanente = TRUE OR data_previsao_fim IS NOT NULL),
  CONSTRAINT chk_restr_encerrada CHECK (status != 'encerrada' OR encerrada_em IS NOT NULL)
);

-- Acidentes de trabalho
CREATE TABLE sst_acidente (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_ocorrencia     DATE NOT NULL,
  hora_ocorrencia     TIME,
  tipo_acidente       VARCHAR(30) NOT NULL
                      CHECK (tipo_acidente IN ('tipico','trajeto','doenca_ocupacional')),
  gravidade           VARCHAR(20) NOT NULL
                      CHECK (gravidade IN ('leve','moderado','grave','fatal')),
  parte_corpo         TEXT,
  agente_causador     TEXT,
  descricao           TEXT NOT NULL,
  local_ocorrencia    TEXT,
  setor_id            UUID REFERENCES rh_setores(id),
  testemunha_1        TEXT,
  testemunha_2        TEXT,
  cat_emitida         BOOLEAN NOT NULL DEFAULT FALSE,
  cat_numero          TEXT,
  dias_afastamento    INTEGER NOT NULL DEFAULT 0,
  data_retorno        DATE,
  acao_imediata       TEXT,
  causa_raiz          TEXT,
  acao_corretiva      TEXT,
  status              VARCHAR(20) NOT NULL DEFAULT 'aberto'
                      CHECK (status IN ('aberto','investigacao','encerrado')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id)
);

-- Não conformidades SST
CREATE TABLE sst_nao_conformidade (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo                VARCHAR(30) NOT NULL
                      CHECK (tipo IN ('epi_uso_incorreto','epi_nao_uso','procedimento','condicao_insegura','ato_inseguro','documentacao','treinamento','equipamento','outra')),
  descricao           TEXT NOT NULL,
  data_identificacao  DATE NOT NULL DEFAULT CURRENT_DATE,
  identificado_por    TEXT NOT NULL,
  funcionario_id      UUID REFERENCES rh_funcionarios(id),
  setor_id            UUID REFERENCES rh_setores(id),
  local_ocorrencia    TEXT,
  gravidade           VARCHAR(20) NOT NULL DEFAULT 'media'
                      CHECK (gravidade IN ('baixa','media','alta','critica')),
  acao_imediata       TEXT,
  acao_corretiva      TEXT,
  prazo_correcao      DATE,
  responsavel_correcao TEXT,
  status              VARCHAR(20) NOT NULL DEFAULT 'aberta'
                      CHECK (status IN ('aberta','em_tratamento','verificacao','encerrada')),
  data_encerramento   DATE,
  evidencia_fechamento TEXT,
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID REFERENCES auth.users(id),
  CONSTRAINT chk_nc_encerrada CHECK (status != 'encerrada' OR data_encerramento IS NOT NULL)
);


-- ════════════════════════════════════════════════════════════════
-- PARTE 4: TRIGGER — CÁLCULO AUTOMÁTICO DE VALIDADE DE TREINAMENTO
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_sst_trein_calc_validade()
RETURNS TRIGGER LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE v_meses INTEGER;
BEGIN
  IF NEW.data_validade IS NOT NULL THEN RETURN NEW; END IF;
  SELECT validade_meses INTO v_meses FROM sst_tipo_treinamento WHERE id = NEW.tipo_treinamento_id;
  IF v_meses IS NOT NULL THEN
    NEW.data_validade := NEW.data_realizacao + (v_meses || ' months')::INTERVAL;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_trein_calc_validade
  BEFORE INSERT OR UPDATE ON sst_treinamento
  FOR EACH ROW EXECUTE FUNCTION fn_sst_trein_calc_validade();


-- ════════════════════════════════════════════════════════════════
-- PARTE 5: MOTORES SQL
-- ════════════════════════════════════════════════════════════════
-- (Ver código completo nas migrações aplicadas no Supabase)
-- Funções criadas:
--   sst_elegibilidade_aso(date)
--   sst_elegibilidade_treinamento(date)
--   sst_elegibilidade_epi(date)
--   sst_aptidao_funcionario(uuid, date)
--   sst_aptidao_geral(date)
--   sst_resumo_dashboard(date)
--   sst_ficha_funcionario(uuid, date)
--   sst_alertas(integer, date)


-- ════════════════════════════════════════════════════════════════
-- EXEMPLOS DE USO (via Supabase RPC)
-- ════════════════════════════════════════════════════════════════
--
-- 1. Ver todos os ASOs vencidos/vencendo:
--    SELECT * FROM sst_elegibilidade_aso();
--    SELECT * FROM sst_elegibilidade_aso('2026-04-05');
--
-- 2. Ver treinamentos pendentes/vencidos:
--    SELECT * FROM sst_elegibilidade_treinamento();
--
-- 3. Ver EPIs pendentes de entrega/troca:
--    SELECT * FROM sst_elegibilidade_epi();
--
-- 4. Classificar aptidão de 1 funcionário:
--    SELECT * FROM sst_aptidao_funcionario('uuid-do-funcionario');
--
-- 5. Ver aptidão de todos (ordenado por criticidade):
--    SELECT * FROM sst_aptidao_geral();
--
-- 6. Dashboard resumo (contadores):
--    SELECT * FROM sst_resumo_dashboard();
--
-- 7. Ficha SST completa (JSONB):
--    SELECT sst_ficha_funcionario('uuid-do-funcionario');
--
-- 8. Todos os alertas ordenados por prioridade:
--    SELECT * FROM sst_alertas();
--    SELECT * FROM sst_alertas(60); -- 60 dias de antecedência
--
-- 9. Via Supabase JS:
--    const { data } = await supabase.rpc('sst_elegibilidade_aso');
--    const { data } = await supabase.rpc('sst_aptidao_funcionario', { p_funcionario_id: '...' });
--    const { data } = await supabase.rpc('sst_ficha_funcionario', { p_funcionario_id: '...' });
--    const { data } = await supabase.rpc('sst_alertas');
--    const { data } = await supabase.rpc('sst_resumo_dashboard');
