-- ════════════════════════════════════════════════════════════════════════════
-- FUNDAÇÃO DO BANCO DE DADOS — SISTEMA RH/SST
-- Classic / APAC · Supabase (PostgreSQL 17)
-- ════════════════════════════════════════════════════════════════════════════
--
-- DECISÕES ARQUITETURAIS:
--
-- 1. PREFIXO rh_ em todas as tabelas para isolar o domínio no schema public.
--
-- 2. UUID como PK em todas as tabelas. Motivo: compatibilidade nativa com
--    Supabase Auth (auth.users usa UUID), evita colisão em cenários de
--    merge/migração, e funciona bem com RLS.
--
-- 3. created_at / updated_at / created_by em TODAS as tabelas.
--    - created_by referencia auth.users(id) para rastreabilidade.
--    - updated_at é mantido por trigger automático.
--
-- 4. NORMALIZAÇÃO 3FN: cargo, setor, centro de custo e nível hierárquico
--    são tabelas próprias referenciadas por FK. Valores desnormalizados
--    (cargo_id, setor_id, salario_base) existem em rh_funcionarios para
--    performance de queries frequentes, mas são SEMPRE acompanhados de
--    tabelas de histórico que registram cada mudança.
--
-- 5. TABELAS DE HISTÓRICO (hist_cargo, hist_salario, hist_setor, hist_gestor):
--    - Usam padrão vigencia_inicio / vigencia_fim (effective dating).
--    - vigencia_fim NULL = registro vigente.
--    - Populadas automaticamente por TRIGGERS no UPDATE de rh_funcionarios.
--    - NUNCA se sobrescreve dado: toda mudança gera novo registro histórico.
--
-- 6. RBAC via rh_usuarios.perfil:
--    - administrador: acesso total + gestão de usuários
--    - rh: CRUD em dados de funcionários, sem gestão de usuários
--    - gestor: leitura de subordinados diretos
--    - visualizador: somente leitura
--
-- 7. RLS GRANULAR: políticas diferenciadas por perfil usando função helper.
--    Gestores só veem seus subordinados diretos.
--
-- 8. AUDITORIA COMPLETA: trigger genérico que captura INSERT/UPDATE/DELETE
--    com dados_antigos/dados_novos em JSONB + lista de campos alterados.
--
-- 9. DELETE BLOQUEADO em rh_funcionarios: trigger BEFORE DELETE que impede
--    exclusão física. Desligamento é feito via UPDATE de status.
--
-- 10. TIPO DE VÍNCULO como coluna com CHECK, não tabela separada.
--     Motivo: são categorias fixas da legislação brasileira (CLT, APAC, PJ,
--     Terceiro) que não mudam. Tabela seria over-engineering.
--
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════
-- PARTE 0: LIMPEZA DO SCHEMA ANTIGO
-- ════════════════════════════════════════════════════════════════

-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_audit_log CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_sessoes CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_ocorrencias CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_apuracao_mensal CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_plano_alianca CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_auxilio_deslocamento CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_vale_alimentacao CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_deslocamento_cidades CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_colaboradores CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_usuarios CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_vagas CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_candidatos CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_pipeline CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_entrevistas CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_avaliacoes CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rs_banco_talentos CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_epi_entregas CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_aso CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS rh_desligamentos CASCADE;
-- [DISABLED for rebuild] DROP TABLE IF EXISTS cadastros_atak CASCADE;

-- Drop old functions
-- [DISABLED for rebuild] DROP FUNCTION IF EXISTS rh_meu_perfil() CASCADE;


-- ════════════════════════════════════════════════════════════════
-- PARTE 1: FUNÇÕES AUXILIARES
-- ════════════════════════════════════════════════════════════════

-- 1.1 Trigger para manter updated_at sincronizado
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- [MOVED] rh_perfil_atual() e rh_funcionario_atual_id() movidas para depois dos CREATE TABLEs
-- (necessario porque referenciam rh_usuarios e rh_funcionarios)

-- PARTE 2: ESTRUTURA ORGANIZACIONAL
-- ════════════════════════════════════════════════════════════════

-- 2.1 EMPRESAS
CREATE TABLE IF NOT EXISTS rh_empresas (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  razao_social    TEXT NOT NULL,
  nome_fantasia   TEXT,
  cnpj            VARCHAR(14) UNIQUE NOT NULL,
  inscricao_estadual TEXT,
  telefone        TEXT,
  email           TEXT,
  endereco        JSONB,  -- {logradouro, numero, complemento, bairro, cidade, uf, cep}
  responsavel     TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

COMMENT ON TABLE rh_empresas IS 'Empresas do grupo (Classic, APAC, etc.)';
COMMENT ON COLUMN rh_empresas.endereco IS 'JSONB: {logradouro, numero, complemento, bairro, cidade, uf, cep}';

-- 2.2 FILIAIS
CREATE TABLE IF NOT EXISTS rh_filiais (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  empresa_id      UUID NOT NULL REFERENCES rh_empresas(id),
  razao_social    TEXT NOT NULL,
  nome_fantasia   TEXT,
  cnpj            VARCHAR(14) UNIQUE,
  endereco        JSONB,
  telefone        TEXT,
  email           TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

COMMENT ON TABLE rh_filiais IS 'Filiais/estabelecimentos vinculados a uma empresa';

-- 2.3 CENTROS DE CUSTO
CREATE TABLE IF NOT EXISTS rh_centros_custo (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  empresa_id      UUID NOT NULL REFERENCES rh_empresas(id),
  codigo          TEXT NOT NULL,
  descricao       TEXT NOT NULL,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id),
  UNIQUE(empresa_id, codigo)
);

COMMENT ON TABLE rh_centros_custo IS 'Centros de custo para alocação contábil. Vinculados à empresa (não filial) para flexibilidade.';

-- 2.4 NÍVEIS HIERÁRQUICOS
CREATE TABLE IF NOT EXISTS rh_niveis_hierarquicos (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome            TEXT NOT NULL UNIQUE,
  ordem           SMALLINT NOT NULL UNIQUE, -- 1 = topo (Diretor), 9+ = base
  descricao       TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

COMMENT ON TABLE rh_niveis_hierarquicos IS 'Níveis da hierarquia organizacional. Ordem 1 = mais alto.';

-- 2.5 SETORES
CREATE TABLE IF NOT EXISTS rh_setores (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  filial_id       UUID NOT NULL REFERENCES rh_filiais(id),
  nome            TEXT NOT NULL,
  codigo          TEXT,
  grau_risco      VARCHAR(10) NOT NULL DEFAULT 'baixo'
                  CHECK (grau_risco IN ('baixo','medio','alto')),
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id),
  UNIQUE(filial_id, nome)
);

COMMENT ON TABLE rh_setores IS 'Setores/departamentos vinculados a uma filial. grau_risco indica exposição SST.';

-- 2.6 CARGOS
CREATE TABLE IF NOT EXISTS rh_cargos (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome            TEXT NOT NULL,
  cbo             VARCHAR(10),  -- Classificação Brasileira de Ocupações
  nivel_hierarquico_id UUID REFERENCES rh_niveis_hierarquicos(id),
  descricao       TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id),
  UNIQUE(nome, cbo)
);

COMMENT ON TABLE rh_cargos IS 'Cargos com CBO e vínculo ao nível hierárquico.';


-- ════════════════════════════════════════════════════════════════
-- PARTE 3: USUÁRIOS DO SISTEMA (RBAC)
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_usuarios (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  auth_uid        UUID UNIQUE REFERENCES auth.users(id),
  nome            TEXT NOT NULL,
  email           TEXT UNIQUE NOT NULL,
  perfil          TEXT NOT NULL DEFAULT 'visualizador'
                  CHECK (perfil IN ('administrador','rh','gestor','visualizador')),
  filiais_acesso  UUID[],  -- IDs de filiais que o usuário pode acessar (NULL = todas)
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

COMMENT ON TABLE rh_usuarios IS 'Usuários do sistema RH com perfil RBAC. filiais_acesso restringe acesso multi-tenant.';
COMMENT ON COLUMN rh_usuarios.filiais_acesso IS 'Array de filial_ids. NULL = acesso a todas as filiais.';
COMMENT ON COLUMN rh_usuarios.perfil IS 'administrador: total | rh: CRUD dados | gestor: leitura subordinados | visualizador: somente leitura';


-- ════════════════════════════════════════════════════════════════
-- PARTE 4: FUNCIONÁRIOS (TABELA PRINCIPAL)
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_funcionarios (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,

  -- ── Dados pessoais ──
  nome_completo   TEXT NOT NULL,
  cpf             VARCHAR(11) UNIQUE NOT NULL,
  data_nascimento DATE,
  sexo            VARCHAR(20),
  estado_civil    VARCHAR(30),
  telefone        TEXT,
  email_pessoal   TEXT,
  endereco        JSONB,

  -- ── Identificação trabalhista ──
  matricula       TEXT UNIQUE,
  pis_pasep       VARCHAR(11),
  ctps_numero     TEXT,
  ctps_serie      TEXT,

  -- ── Vínculo organizacional (valores correntes — desnormalizados) ──
  empresa_id      UUID NOT NULL REFERENCES rh_empresas(id),
  filial_id       UUID NOT NULL REFERENCES rh_filiais(id),
  setor_id        UUID NOT NULL REFERENCES rh_setores(id),
  cargo_id        UUID NOT NULL REFERENCES rh_cargos(id),
  centro_custo_id UUID REFERENCES rh_centros_custo(id),
  gestor_id       UUID REFERENCES rh_funcionarios(id),

  -- ── Contrato ──
  tipo_vinculo    VARCHAR(20) NOT NULL DEFAULT 'CLT'
                  CHECK (tipo_vinculo IN ('CLT','APAC','PJ','Terceiro')),
  modalidade_contrato VARCHAR(30)
                  CHECK (modalidade_contrato IS NULL OR modalidade_contrato IN (
                    'Indeterminado','Experiencia','Temporario',
                    'Intermitente','Safra','Aprendiz','Estagio'
                  )),

  -- ── Remuneração (corrente — desnormalizado) ──
  salario_base    NUMERIC(12,2),

  -- ── Datas ──
  data_admissao   DATE NOT NULL,
  data_desligamento DATE,

  -- ── Jornada ──
  turno           VARCHAR(30) DEFAULT 'comercial',
  carga_horaria_semanal NUMERIC(4,1) DEFAULT 44.0,

  -- ── Status ──
  status          VARCHAR(20) NOT NULL DEFAULT 'ativo'
                  CHECK (status IN ('ativo','afastado','ferias','desligado','experiencia')),

  -- ── SST ──
  grau_risco_sst  VARCHAR(10) NOT NULL DEFAULT 'padrao'
                  CHECK (grau_risco_sst IN ('alto','padrao','baixo','isento')),

  -- ── Elegibilidade (derivadas por regras de negócio) ──
  elegivel_beneficios  BOOLEAN NOT NULL DEFAULT TRUE,
  elegivel_sst         BOOLEAN NOT NULL DEFAULT TRUE,

  -- ── Observações ──
  observacoes     TEXT,

  -- ── Metadata ──
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id),

  -- ── Constraints ──
  CONSTRAINT chk_datas_coerentes
    CHECK (data_desligamento IS NULL OR data_desligamento >= data_admissao),
  CONSTRAINT chk_vinculo_modalidade
    CHECK (
      (tipo_vinculo = 'CLT')
      OR (tipo_vinculo != 'CLT' AND modalidade_contrato IS NULL)
    )
);

-- Índices para queries frequentes
CREATE INDEX IF NOT EXISTS idx_func_status      ON rh_funcionarios(status);
CREATE INDEX IF NOT EXISTS idx_func_empresa     ON rh_funcionarios(empresa_id);
CREATE INDEX IF NOT EXISTS idx_func_filial      ON rh_funcionarios(filial_id);
CREATE INDEX IF NOT EXISTS idx_func_setor       ON rh_funcionarios(setor_id);
CREATE INDEX IF NOT EXISTS idx_func_cargo       ON rh_funcionarios(cargo_id);
CREATE INDEX IF NOT EXISTS idx_func_gestor      ON rh_funcionarios(gestor_id);
CREATE INDEX IF NOT EXISTS idx_func_cpf         ON rh_funcionarios(cpf);
CREATE INDEX IF NOT EXISTS idx_func_vinculo     ON rh_funcionarios(tipo_vinculo);
CREATE INDEX IF NOT EXISTS idx_func_admissao    ON rh_funcionarios(data_admissao);

COMMENT ON TABLE rh_funcionarios IS 'Tabela principal de funcionários. Campos desnormalizados (cargo_id, setor_id, salario_base) são mantidos em sincronia com tabelas de histórico via triggers.';
COMMENT ON COLUMN rh_funcionarios.tipo_vinculo IS 'CLT = regime CLT | APAC = cooperativa | PJ = pessoa jurídica | Terceiro = terceirizado';
COMMENT ON COLUMN rh_funcionarios.modalidade_contrato IS 'Aplicável apenas para CLT. NULL para outros vínculos.';
COMMENT ON COLUMN rh_funcionarios.status IS 'desligado = registro permanente, nunca deletar.';


-- ════════════════════════════════════════════════════════════════
-- PARTE 5: TABELAS DE HISTÓRICO
-- ════════════════════════════════════════════════════════════════
-- Padrão: vigencia_inicio/vigencia_fim (effective dating)
-- vigencia_fim NULL = registro vigente atual

-- 5.1 HISTÓRICO DE CARGO
CREATE TABLE IF NOT EXISTS rh_hist_cargo (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id  UUID NOT NULL REFERENCES rh_funcionarios(id),
  cargo_id        UUID NOT NULL REFERENCES rh_cargos(id),
  vigencia_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
  vigencia_fim    DATE,  -- NULL = vigente
  motivo          TEXT,  -- promoção, transferência, reestruturação
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_hist_cargo_func ON rh_hist_cargo(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_hist_cargo_vig  ON rh_hist_cargo(vigencia_fim) WHERE vigencia_fim IS NULL;

COMMENT ON TABLE rh_hist_cargo IS 'Histórico de mudanças de cargo. Cada UPDATE em rh_funcionarios.cargo_id gera registro aqui.';

-- 5.2 HISTÓRICO DE SALÁRIO
CREATE TABLE IF NOT EXISTS rh_hist_salario (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id  UUID NOT NULL REFERENCES rh_funcionarios(id),
  salario_anterior NUMERIC(12,2),
  salario_novo    NUMERIC(12,2) NOT NULL,
  percentual_reajuste NUMERIC(6,2), -- calculado automaticamente
  vigencia_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
  vigencia_fim    DATE,
  motivo          TEXT,  -- reajuste, promoção, dissídio, acordo coletivo
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_hist_sal_func ON rh_hist_salario(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_hist_sal_vig  ON rh_hist_salario(vigencia_fim) WHERE vigencia_fim IS NULL;

COMMENT ON TABLE rh_hist_salario IS 'Histórico de reajustes salariais. Calcula percentual automaticamente.';

-- 5.3 HISTÓRICO DE SETOR
CREATE TABLE IF NOT EXISTS rh_hist_setor (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id  UUID NOT NULL REFERENCES rh_funcionarios(id),
  setor_id        UUID NOT NULL REFERENCES rh_setores(id),
  vigencia_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
  vigencia_fim    DATE,
  motivo          TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_hist_setor_func ON rh_hist_setor(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_hist_setor_vig  ON rh_hist_setor(vigencia_fim) WHERE vigencia_fim IS NULL;

-- 5.4 HISTÓRICO DE GESTOR
CREATE TABLE IF NOT EXISTS rh_hist_gestor (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id  UUID NOT NULL REFERENCES rh_funcionarios(id),
  gestor_id       UUID REFERENCES rh_funcionarios(id),  -- NULL = sem gestor
  vigencia_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
  vigencia_fim    DATE,
  motivo          TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_hist_gestor_func ON rh_hist_gestor(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_hist_gestor_vig  ON rh_hist_gestor(vigencia_fim) WHERE vigencia_fim IS NULL;


-- ════════════════════════════════════════════════════════════════
-- PARTE 6: TRIGGERS DE HISTÓRICO
-- ════════════════════════════════════════════════════════════════

-- 6.1 Trigger: mudança de cargo
CREATE OR REPLACE FUNCTION fn_hist_cargo()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  IF OLD.cargo_id IS DISTINCT FROM NEW.cargo_id THEN
    -- Encerrar registro vigente
    UPDATE rh_hist_cargo
    SET vigencia_fim = CURRENT_DATE
    WHERE funcionario_id = NEW.id
      AND vigencia_fim IS NULL;
    -- Inserir novo registro
    INSERT INTO rh_hist_cargo (funcionario_id, cargo_id, vigencia_inicio, created_by)
    VALUES (NEW.id, NEW.cargo_id, CURRENT_DATE, auth.uid());
  END IF;
  RETURN NEW;
END;
$$;

-- 6.2 Trigger: mudança de salário
CREATE OR REPLACE FUNCTION fn_hist_salario()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_pct NUMERIC(6,2);
BEGIN
  IF OLD.salario_base IS DISTINCT FROM NEW.salario_base THEN
    -- Calcular percentual de reajuste
    IF OLD.salario_base IS NOT NULL AND OLD.salario_base > 0 THEN
      v_pct := ROUND(((NEW.salario_base - OLD.salario_base) / OLD.salario_base) * 100, 2);
    END IF;
    -- Encerrar registro vigente
    UPDATE rh_hist_salario
    SET vigencia_fim = CURRENT_DATE
    WHERE funcionario_id = NEW.id
      AND vigencia_fim IS NULL;
    -- Inserir novo registro
    INSERT INTO rh_hist_salario (funcionario_id, salario_anterior, salario_novo, percentual_reajuste, vigencia_inicio, created_by)
    VALUES (NEW.id, OLD.salario_base, NEW.salario_base, v_pct, CURRENT_DATE, auth.uid());
  END IF;
  RETURN NEW;
END;
$$;

-- 6.3 Trigger: mudança de setor
CREATE OR REPLACE FUNCTION fn_hist_setor()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  IF OLD.setor_id IS DISTINCT FROM NEW.setor_id THEN
    UPDATE rh_hist_setor
    SET vigencia_fim = CURRENT_DATE
    WHERE funcionario_id = NEW.id
      AND vigencia_fim IS NULL;
    INSERT INTO rh_hist_setor (funcionario_id, setor_id, vigencia_inicio, created_by)
    VALUES (NEW.id, NEW.setor_id, CURRENT_DATE, auth.uid());
  END IF;
  RETURN NEW;
END;
$$;

-- 6.4 Trigger: mudança de gestor
CREATE OR REPLACE FUNCTION fn_hist_gestor()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  IF OLD.gestor_id IS DISTINCT FROM NEW.gestor_id THEN
    UPDATE rh_hist_gestor
    SET vigencia_fim = CURRENT_DATE
    WHERE funcionario_id = NEW.id
      AND vigencia_fim IS NULL;
    INSERT INTO rh_hist_gestor (funcionario_id, gestor_id, vigencia_inicio, created_by)
    VALUES (NEW.id, NEW.gestor_id, CURRENT_DATE, auth.uid());
  END IF;
  RETURN NEW;
END;
$$;

-- 6.5 Trigger: inserção inicial (popula primeiro registro de histórico)
CREATE OR REPLACE FUNCTION fn_hist_inicial()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  INSERT INTO rh_hist_cargo (funcionario_id, cargo_id, vigencia_inicio, created_by)
  VALUES (NEW.id, NEW.cargo_id, NEW.data_admissao, auth.uid());

  IF NEW.salario_base IS NOT NULL THEN
    INSERT INTO rh_hist_salario (funcionario_id, salario_anterior, salario_novo, vigencia_inicio, created_by)
    VALUES (NEW.id, NULL, NEW.salario_base, NEW.data_admissao, auth.uid());
  END IF;

  INSERT INTO rh_hist_setor (funcionario_id, setor_id, vigencia_inicio, created_by)
  VALUES (NEW.id, NEW.setor_id, NEW.data_admissao, auth.uid());

  IF NEW.gestor_id IS NOT NULL THEN
    INSERT INTO rh_hist_gestor (funcionario_id, gestor_id, vigencia_inicio, created_by)
    VALUES (NEW.id, NEW.gestor_id, NEW.data_admissao, auth.uid());
  END IF;

  RETURN NEW;
END;
$$;

-- Vincular triggers à tabela de funcionários (idempotente)
DROP TRIGGER IF EXISTS trg_func_hist_cargo ON rh_funcionarios;
CREATE TRIGGER trg_func_hist_cargo
  AFTER UPDATE ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_hist_cargo();

DROP TRIGGER IF EXISTS trg_func_hist_salario ON rh_funcionarios;
CREATE TRIGGER trg_func_hist_salario
  AFTER UPDATE ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_hist_salario();

DROP TRIGGER IF EXISTS trg_func_hist_setor ON rh_funcionarios;
CREATE TRIGGER trg_func_hist_setor
  AFTER UPDATE ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_hist_setor();

DROP TRIGGER IF EXISTS trg_func_hist_gestor ON rh_funcionarios;
CREATE TRIGGER trg_func_hist_gestor
  AFTER UPDATE ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_hist_gestor();

DROP TRIGGER IF EXISTS trg_func_hist_inicial ON rh_funcionarios;
CREATE TRIGGER trg_func_hist_inicial
  AFTER INSERT ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_hist_inicial();


-- ════════════════════════════════════════════════════════════════
-- PARTE 6b: FUNÇÕES RBAC (movidas para depois dos CREATE TABLEs)
-- 1.2 Função para obter o perfil do usuário autenticado (usada em RLS)
CREATE OR REPLACE FUNCTION rh_perfil_atual()
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT perfil FROM rh_usuarios
  WHERE auth_uid = auth.uid()
    AND ativo = TRUE
  LIMIT 1;
$$;

-- 1.3 Função para obter o ID do funcionário vinculado ao usuário (para gestores)
CREATE OR REPLACE FUNCTION rh_funcionario_atual_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT f.id FROM rh_funcionarios f
  INNER JOIN rh_usuarios u ON u.auth_uid = auth.uid()
  WHERE f.email_pessoal = u.email
    AND f.status != 'desligado'
  LIMIT 1;
$$;


-- PARTE 7: AUDITORIA E GOVERNANÇA
-- ════════════════════════════════════════════════════════════════

-- 7.1 Tabela de auditoria (log de todas as alterações)
CREATE TABLE IF NOT EXISTS rh_audit_log (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tabela          TEXT NOT NULL,
  registro_id     TEXT NOT NULL,  -- TEXT para suportar UUID e BIGINT
  acao            TEXT NOT NULL CHECK (acao IN ('INSERT','UPDATE','DELETE_BLOCKED')),
  dados_antigos   JSONB,
  dados_novos     JSONB,
  campos_alterados TEXT[],
  usuario_id      UUID,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_tabela    ON rh_audit_log(tabela);
CREATE INDEX IF NOT EXISTS idx_audit_registro  ON rh_audit_log(registro_id);
CREATE INDEX IF NOT EXISTS idx_audit_data      ON rh_audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_usuario   ON rh_audit_log(usuario_id);

COMMENT ON TABLE rh_audit_log IS 'Log imutável de todas as alterações. acao DELETE_BLOCKED registra tentativas de exclusão bloqueadas.';

-- 7.2 Função genérica de auditoria
CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_old JSONB;
  v_new JSONB;
  v_id TEXT;
  v_changed TEXT[];
  v_key TEXT;
BEGIN
  IF TG_OP = 'INSERT' THEN
    v_new := to_jsonb(NEW);
    v_id := v_new->>'id';
    INSERT INTO rh_audit_log (tabela, registro_id, acao, dados_novos, usuario_id)
    VALUES (TG_TABLE_NAME, v_id, 'INSERT', v_new, auth.uid());
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    v_old := to_jsonb(OLD);
    v_new := to_jsonb(NEW);
    v_id := v_new->>'id';

    -- Identificar campos alterados (ignorando updated_at)
    SELECT ARRAY_AGG(key) INTO v_changed
    FROM jsonb_each(v_new) AS n(key, value)
    WHERE key != 'updated_at'
      AND (v_old->key IS DISTINCT FROM v_new->key);

    -- Só auditar se houve mudança real
    IF v_changed IS NOT NULL AND array_length(v_changed, 1) > 0 THEN
      INSERT INTO rh_audit_log (tabela, registro_id, acao, dados_antigos, dados_novos, campos_alterados, usuario_id)
      VALUES (TG_TABLE_NAME, v_id, 'UPDATE', v_old, v_new, v_changed, auth.uid());
    END IF;
    RETURN NEW;

  END IF;

  RETURN NULL;
END;
$$;

-- 7.3 Bloqueio de DELETE em funcionários
CREATE OR REPLACE FUNCTION fn_block_delete_funcionario()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- Registrar tentativa
  INSERT INTO rh_audit_log (tabela, registro_id, acao, dados_antigos, usuario_id)
  VALUES ('rh_funcionarios', OLD.id::TEXT, 'DELETE_BLOCKED', to_jsonb(OLD), auth.uid());

  RAISE EXCEPTION 'DELETE não permitido em rh_funcionarios. Use UPDATE status = ''desligado'' para desligar funcionário. ID: %', OLD.id;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_block_delete_func ON rh_funcionarios;
CREATE TRIGGER trg_block_delete_func
  BEFORE DELETE ON rh_funcionarios
  FOR EACH ROW EXECUTE FUNCTION fn_block_delete_funcionario();

-- 7.4 Vincular triggers de auditoria a TODAS as tabelas (idempotente)
-- updated_at triggers
DROP TRIGGER IF EXISTS trg_updated_empresas ON rh_empresas;
CREATE TRIGGER trg_updated_empresas     BEFORE UPDATE ON rh_empresas     FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_filiais ON rh_filiais;
CREATE TRIGGER trg_updated_filiais      BEFORE UPDATE ON rh_filiais      FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_cc ON rh_centros_custo;
CREATE TRIGGER trg_updated_cc           BEFORE UPDATE ON rh_centros_custo FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_niveis ON rh_niveis_hierarquicos;
CREATE TRIGGER trg_updated_niveis       BEFORE UPDATE ON rh_niveis_hierarquicos FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_setores ON rh_setores;
CREATE TRIGGER trg_updated_setores      BEFORE UPDATE ON rh_setores      FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_cargos ON rh_cargos;
CREATE TRIGGER trg_updated_cargos       BEFORE UPDATE ON rh_cargos       FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_usuarios ON rh_usuarios;
CREATE TRIGGER trg_updated_usuarios     BEFORE UPDATE ON rh_usuarios     FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
DROP TRIGGER IF EXISTS trg_updated_func ON rh_funcionarios;
CREATE TRIGGER trg_updated_func         BEFORE UPDATE ON rh_funcionarios FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- Audit triggers
DROP TRIGGER IF EXISTS trg_audit_empresas ON rh_empresas;
CREATE TRIGGER trg_audit_empresas     AFTER INSERT OR UPDATE ON rh_empresas     FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_filiais ON rh_filiais;
CREATE TRIGGER trg_audit_filiais      AFTER INSERT OR UPDATE ON rh_filiais      FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_cc ON rh_centros_custo;
CREATE TRIGGER trg_audit_cc           AFTER INSERT OR UPDATE ON rh_centros_custo FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_niveis ON rh_niveis_hierarquicos;
CREATE TRIGGER trg_audit_niveis       AFTER INSERT OR UPDATE ON rh_niveis_hierarquicos FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_setores ON rh_setores;
CREATE TRIGGER trg_audit_setores      AFTER INSERT OR UPDATE ON rh_setores      FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_cargos ON rh_cargos;
CREATE TRIGGER trg_audit_cargos       AFTER INSERT OR UPDATE ON rh_cargos       FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_usuarios ON rh_usuarios;
CREATE TRIGGER trg_audit_usuarios     AFTER INSERT OR UPDATE ON rh_usuarios     FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
DROP TRIGGER IF EXISTS trg_audit_func ON rh_funcionarios;
CREATE TRIGGER trg_audit_func         AFTER INSERT OR UPDATE ON rh_funcionarios FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();


-- ════════════════════════════════════════════════════════════════
-- PARTE 8: RLS — ROW LEVEL SECURITY (GRANULAR POR PERFIL)
-- ════════════════════════════════════════════════════════════════

-- Habilitar RLS em TODAS as tabelas
ALTER TABLE rh_empresas              ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_filiais               ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_centros_custo         ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_niveis_hierarquicos   ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_setores               ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_cargos                ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_usuarios              ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_funcionarios          ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_hist_cargo            ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_hist_salario          ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_hist_setor            ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_hist_gestor           ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_audit_log             ENABLE ROW LEVEL SECURITY;

-- ── 8.1 TABELAS ORGANIZACIONAIS (leitura ampla, escrita restrita) ──

-- Leitura: qualquer autenticado
-- Escrita: apenas administrador e rh

-- EMPRESAS
DROP POLICY IF EXISTS "empresas_select" ON rh_empresas;
CREATE POLICY "empresas_select" ON rh_empresas
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "empresas_insert" ON rh_empresas;
CREATE POLICY "empresas_insert" ON rh_empresas
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "empresas_update" ON rh_empresas;
CREATE POLICY "empresas_update" ON rh_empresas
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "empresas_delete" ON rh_empresas;
CREATE POLICY "empresas_delete" ON rh_empresas
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- FILIAIS
DROP POLICY IF EXISTS "filiais_select" ON rh_filiais;
CREATE POLICY "filiais_select" ON rh_filiais
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "filiais_insert" ON rh_filiais;
CREATE POLICY "filiais_insert" ON rh_filiais
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "filiais_update" ON rh_filiais;
CREATE POLICY "filiais_update" ON rh_filiais
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "filiais_delete" ON rh_filiais;
CREATE POLICY "filiais_delete" ON rh_filiais
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- CENTROS DE CUSTO
DROP POLICY IF EXISTS "cc_select" ON rh_centros_custo;
CREATE POLICY "cc_select" ON rh_centros_custo
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "cc_insert" ON rh_centros_custo;
CREATE POLICY "cc_insert" ON rh_centros_custo
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "cc_update" ON rh_centros_custo;
CREATE POLICY "cc_update" ON rh_centros_custo
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "cc_delete" ON rh_centros_custo;
CREATE POLICY "cc_delete" ON rh_centros_custo
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- NÍVEIS HIERÁRQUICOS
DROP POLICY IF EXISTS "niveis_select" ON rh_niveis_hierarquicos;
CREATE POLICY "niveis_select" ON rh_niveis_hierarquicos
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "niveis_insert" ON rh_niveis_hierarquicos;
CREATE POLICY "niveis_insert" ON rh_niveis_hierarquicos
  FOR INSERT WITH CHECK (rh_perfil_atual() = 'administrador');
DROP POLICY IF EXISTS "niveis_update" ON rh_niveis_hierarquicos;
CREATE POLICY "niveis_update" ON rh_niveis_hierarquicos
  FOR UPDATE USING (rh_perfil_atual() = 'administrador');
DROP POLICY IF EXISTS "niveis_delete" ON rh_niveis_hierarquicos;
CREATE POLICY "niveis_delete" ON rh_niveis_hierarquicos
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- SETORES
DROP POLICY IF EXISTS "setores_select" ON rh_setores;
CREATE POLICY "setores_select" ON rh_setores
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "setores_insert" ON rh_setores;
CREATE POLICY "setores_insert" ON rh_setores
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "setores_update" ON rh_setores;
CREATE POLICY "setores_update" ON rh_setores
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "setores_delete" ON rh_setores;
CREATE POLICY "setores_delete" ON rh_setores
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- CARGOS
DROP POLICY IF EXISTS "cargos_select" ON rh_cargos;
CREATE POLICY "cargos_select" ON rh_cargos
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "cargos_insert" ON rh_cargos;
CREATE POLICY "cargos_insert" ON rh_cargos
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "cargos_update" ON rh_cargos;
CREATE POLICY "cargos_update" ON rh_cargos
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "cargos_delete" ON rh_cargos;
CREATE POLICY "cargos_delete" ON rh_cargos
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- ── 8.2 USUÁRIOS (somente administrador gerencia) ──
DROP POLICY IF EXISTS "usuarios_select" ON rh_usuarios;
CREATE POLICY "usuarios_select" ON rh_usuarios
  FOR SELECT USING (
    auth.role() = 'authenticated'
    AND (
      rh_perfil_atual() IN ('administrador','rh')
      OR auth_uid = auth.uid()  -- pode ver a si mesmo
    )
  );
DROP POLICY IF EXISTS "usuarios_insert" ON rh_usuarios;
CREATE POLICY "usuarios_insert" ON rh_usuarios
  FOR INSERT WITH CHECK (rh_perfil_atual() = 'administrador');
DROP POLICY IF EXISTS "usuarios_update" ON rh_usuarios;
CREATE POLICY "usuarios_update" ON rh_usuarios
  FOR UPDATE USING (rh_perfil_atual() = 'administrador');
DROP POLICY IF EXISTS "usuarios_delete" ON rh_usuarios;
CREATE POLICY "usuarios_delete" ON rh_usuarios
  FOR DELETE USING (rh_perfil_atual() = 'administrador');

-- ── 8.3 FUNCIONÁRIOS (RBAC granular) ──

-- SELECT: admin/rh veem todos; gestor vê subordinados diretos; visualizador vê todos (read-only)
DROP POLICY IF EXISTS "func_select" ON rh_funcionarios;
CREATE POLICY "func_select" ON rh_funcionarios
  FOR SELECT USING (
    auth.role() = 'authenticated'
    AND (
      rh_perfil_atual() IN ('administrador','rh','visualizador')
      OR (
        rh_perfil_atual() = 'gestor'
        AND gestor_id = rh_funcionario_atual_id()
      )
    )
  );

-- INSERT: admin e rh
DROP POLICY IF EXISTS "func_insert" ON rh_funcionarios;
CREATE POLICY "func_insert" ON rh_funcionarios
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- UPDATE: admin e rh
DROP POLICY IF EXISTS "func_update" ON rh_funcionarios;
CREATE POLICY "func_update" ON rh_funcionarios
  FOR UPDATE USING (rh_perfil_atual() IN ('administrador','rh'));

-- DELETE: ninguém (trigger já bloqueia, mas defense in depth)
DROP POLICY IF EXISTS "func_delete" ON rh_funcionarios;
CREATE POLICY "func_delete" ON rh_funcionarios
  FOR DELETE USING (FALSE);

-- ── 8.4 HISTÓRICO (leitura ampla, escrita apenas via trigger) ──

DROP POLICY IF EXISTS "hist_cargo_select" ON rh_hist_cargo;
CREATE POLICY "hist_cargo_select" ON rh_hist_cargo
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "hist_cargo_insert" ON rh_hist_cargo;
CREATE POLICY "hist_cargo_insert" ON rh_hist_cargo
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS "hist_salario_select" ON rh_hist_salario;
CREATE POLICY "hist_salario_select" ON rh_hist_salario
  FOR SELECT USING (
    rh_perfil_atual() IN ('administrador','rh')
  );
DROP POLICY IF EXISTS "hist_salario_insert" ON rh_hist_salario;
CREATE POLICY "hist_salario_insert" ON rh_hist_salario
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS "hist_setor_select" ON rh_hist_setor;
CREATE POLICY "hist_setor_select" ON rh_hist_setor
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "hist_setor_insert" ON rh_hist_setor;
CREATE POLICY "hist_setor_insert" ON rh_hist_setor
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS "hist_gestor_select" ON rh_hist_gestor;
CREATE POLICY "hist_gestor_select" ON rh_hist_gestor
  FOR SELECT USING (auth.role() = 'authenticated');
DROP POLICY IF EXISTS "hist_gestor_insert" ON rh_hist_gestor;
CREATE POLICY "hist_gestor_insert" ON rh_hist_gestor
  FOR INSERT WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 8.5 AUDIT LOG (somente leitura para admin) ──
DROP POLICY IF EXISTS "audit_select" ON rh_audit_log;
CREATE POLICY "audit_select" ON rh_audit_log
  FOR SELECT USING (rh_perfil_atual() = 'administrador');
DROP POLICY IF EXISTS "audit_insert" ON rh_audit_log;
CREATE POLICY "audit_insert" ON rh_audit_log
  FOR INSERT WITH CHECK (TRUE);  -- triggers precisam inserir


-- ════════════════════════════════════════════════════════════════
-- PARTE 9: DADOS INICIAIS (SEED)
-- ════════════════════════════════════════════════════════════════

-- Níveis hierárquicos padrão
INSERT INTO rh_niveis_hierarquicos (nome, ordem, descricao) VALUES
  ('Diretor',       1, 'Direção executiva'),
  ('Gerente',       2, 'Gerência de área'),
  ('Coordenador',   3, 'Coordenação de equipe'),
  ('Supervisor',    4, 'Supervisão operacional'),
  ('Analista',      5, 'Análise e gestão de processos'),
  ('Técnico',       6, 'Execução técnica especializada'),
  ('Assistente',    7, 'Suporte administrativo/operacional'),
  ('Operacional',   8, 'Execução operacional'),
  ('Aprendiz',      9, 'Programa de aprendizagem'),
  ('Estagiário',   10, 'Programa de estágio')
ON CONFLICT (nome) DO NOTHING;


-- ════════════════════════════════════════════════════════════════
-- PARTE 10: VIEWS ÚTEIS
-- ════════════════════════════════════════════════════════════════

-- View: funcionários ativos com dados completos (joins resolvidos)
CREATE OR REPLACE VIEW vw_funcionarios_ativos AS
SELECT
  f.id,
  f.nome_completo,
  f.cpf,
  f.matricula,
  f.data_admissao,
  f.status,
  f.tipo_vinculo,
  f.modalidade_contrato,
  f.salario_base,
  f.turno,
  f.data_nascimento,
  f.telefone,
  f.email_pessoal,
  c.nome AS cargo,
  c.cbo,
  s.nome AS setor,
  s.grau_risco AS setor_grau_risco,
  cc.codigo AS centro_custo_codigo,
  cc.descricao AS centro_custo,
  e.nome_fantasia AS empresa,
  fl.nome_fantasia AS filial,
  nh.nome AS nivel_hierarquico,
  nh.ordem AS nivel_ordem,
  g.nome_completo AS gestor_nome,
  f.empresa_id,
  f.filial_id,
  f.setor_id,
  f.cargo_id,
  f.centro_custo_id,
  f.gestor_id,
  f.grau_risco_sst,
  f.elegivel_beneficios,
  f.elegivel_sst,
  f.created_at,
  f.updated_at
FROM rh_funcionarios f
  INNER JOIN rh_cargos c ON c.id = f.cargo_id
  INNER JOIN rh_setores s ON s.id = f.setor_id
  INNER JOIN rh_empresas e ON e.id = f.empresa_id
  INNER JOIN rh_filiais fl ON fl.id = f.filial_id
  LEFT JOIN rh_centros_custo cc ON cc.id = f.centro_custo_id
  LEFT JOIN rh_niveis_hierarquicos nh ON nh.id = c.nivel_hierarquico_id
  LEFT JOIN rh_funcionarios g ON g.id = f.gestor_id
WHERE f.status != 'desligado';

-- View: histórico completo de um funcionário (cargo + salário + setor + gestor)
CREATE OR REPLACE VIEW vw_historico_funcionario AS
SELECT
  h.funcionario_id,
  'cargo' AS tipo,
  c.nome AS valor,
  NULL::NUMERIC AS valor_numerico,
  h.vigencia_inicio,
  h.vigencia_fim,
  h.motivo,
  h.created_at
FROM rh_hist_cargo h
  INNER JOIN rh_cargos c ON c.id = h.cargo_id

UNION ALL

SELECT
  h.funcionario_id,
  'salario' AS tipo,
  h.salario_novo::TEXT AS valor,
  h.salario_novo AS valor_numerico,
  h.vigencia_inicio,
  h.vigencia_fim,
  h.motivo,
  h.created_at
FROM rh_hist_salario h

UNION ALL

SELECT
  h.funcionario_id,
  'setor' AS tipo,
  s.nome AS valor,
  NULL::NUMERIC AS valor_numerico,
  h.vigencia_inicio,
  h.vigencia_fim,
  h.motivo,
  h.created_at
FROM rh_hist_setor h
  INNER JOIN rh_setores s ON s.id = h.setor_id

UNION ALL

SELECT
  h.funcionario_id,
  'gestor' AS tipo,
  g.nome_completo AS valor,
  NULL::NUMERIC AS valor_numerico,
  h.vigencia_inicio,
  h.vigencia_fim,
  h.motivo,
  h.created_at
FROM rh_hist_gestor h
  LEFT JOIN rh_funcionarios g ON g.id = h.gestor_id

ORDER BY funcionario_id, created_at;


-- ════════════════════════════════════════════════════════════════
-- FIM DA FUNDAÇÃO
-- ════════════════════════════════════════════════════════════════
-- Tabelas criadas: 13
-- Views criadas: 2
-- Triggers: 14 (5 histórico + 8 updated_at + 8 audit + 1 block delete = 22 total)
-- RLS policies: 33
-- Pronto para módulos futuros: benefícios, apuração, SST, recrutamento
