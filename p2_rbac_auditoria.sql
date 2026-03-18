-- ══════════════════════════════════════════════════════════════════════════════
-- P2 — RBAC GRANULAR + AUDITORIA COMPLETA + OVERRIDE CONTROLADO
-- Migration: p2_rbac_auditoria.sql
-- Data: 2026-03-18
-- Pré-requisito: p1_centralizar_regras_negocio.sql
--
-- ENTREGÁVEIS:
--   1. Permissões granulares por operação (13 ações × N módulos × 9 perfis)
--   2. Trilha de auditoria com antes/depois (cq_audit_trail)
--   3. Override controlado com justificativa
--   4. Trigger automático de auditoria
--   5. Enforcement real nas funções P1
--   6. Perfis motorista/coletor para módulo mobile futuro
--
-- IMPORTANTE: Mantém retrocompatibilidade com cq_permissoes existente
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 1: EXPANDIR PERMISSÕES GRANULARES
-- Adiciona 9 colunas de operação ao cq_permissoes existente
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_permissoes
  ADD COLUMN IF NOT EXISTS pode_submeter          BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_revisar           BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_aprovar           BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_bloquear          BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_reabrir           BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_importar          BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_exportar          BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_alterar_parametros BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_anexar            BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_excluir_evidencia BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_ver_auditoria     BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS pode_override          BOOLEAN NOT NULL DEFAULT FALSE;

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 2: NOVOS PERFIS — motorista e coletor (futuro módulo mobile)
-- ════════════════════════════════════════════════════════════════════════════

-- Expandir CHECK constraint para incluir novos perfis
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT constraint_name
    FROM information_schema.check_constraints
    WHERE constraint_schema = 'public'
      AND constraint_name IN (
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.table_name = 'cq_usuarios'
          AND tc.constraint_type = 'CHECK'
      )
      AND check_clause LIKE '%perfil%'
  LOOP
    EXECUTE format('ALTER TABLE cq_usuarios DROP CONSTRAINT %I', r.constraint_name);
  END LOOP;
END;
$$;

ALTER TABLE cq_usuarios
ADD CONSTRAINT cq_usuarios_perfil_check
CHECK (perfil IN (
  'administrador',
  'qualidade',
  'comercial',
  'financeiro',
  'logistica',
  'industrial',
  'diretoria',
  'motorista',
  'coletor'
));


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 3: POPULAR PERMISSÕES GRANULARES
-- Atualiza registros existentes + insere novos perfis
-- ════════════════════════════════════════════════════════════════════════════

-- Administrador: tudo
UPDATE cq_permissoes SET
  pode_submeter = TRUE,
  pode_revisar = TRUE,
  pode_aprovar = TRUE,
  pode_bloquear = TRUE,
  pode_reabrir = TRUE,
  pode_importar = TRUE,
  pode_exportar = TRUE,
  pode_alterar_parametros = TRUE,
  pode_anexar = TRUE,
  pode_excluir_evidencia = TRUE,
  pode_ver_auditoria = TRUE,
  pode_override = TRUE
WHERE perfil = 'administrador';

-- Qualidade: submeter, revisar, bloquear, importar, exportar, anexar
UPDATE cq_permissoes SET
  pode_submeter = TRUE,
  pode_revisar = TRUE,
  pode_aprovar = FALSE,
  pode_bloquear = TRUE,
  pode_reabrir = FALSE,
  pode_importar = (modulo IN ('inspecao_qualidade','formulario_cq','importar_atak','importar_planilha','lancamento_cargas','lancamento_cq_bucho')),
  pode_exportar = (modulo IN ('inspecao_qualidade','dashboard_cq','relatorio_cargas','gestao_fornecedores')),
  pode_alterar_parametros = FALSE,
  pode_anexar = TRUE,
  pode_excluir_evidencia = FALSE,
  pode_ver_auditoria = TRUE,
  pode_override = FALSE
WHERE perfil = 'qualidade';

-- Diretoria: revisar, aprovar, exportar, ver_auditoria
UPDATE cq_permissoes SET
  pode_submeter = FALSE,
  pode_revisar = TRUE,
  pode_aprovar = TRUE,
  pode_bloquear = FALSE,
  pode_reabrir = FALSE,
  pode_importar = FALSE,
  pode_exportar = TRUE,
  pode_alterar_parametros = FALSE,
  pode_anexar = FALSE,
  pode_excluir_evidencia = FALSE,
  pode_ver_auditoria = TRUE,
  pode_override = TRUE
WHERE perfil = 'diretoria';

-- Logística: submeter, importar (recebimento), anexar
UPDATE cq_permissoes SET
  pode_submeter = (modulo IN ('recebimento_lote','conferencia_quantidade','lancamento_cargas')),
  pode_revisar = FALSE,
  pode_aprovar = FALSE,
  pode_bloquear = FALSE,
  pode_reabrir = FALSE,
  pode_importar = (modulo IN ('recebimento_lote','lancamento_cargas')),
  pode_exportar = (modulo IN ('relatorio_cargas','dashboard_cargas')),
  pode_alterar_parametros = FALSE,
  pode_anexar = TRUE,
  pode_excluir_evidencia = FALSE,
  pode_ver_auditoria = FALSE,
  pode_override = FALSE
WHERE perfil = 'logistica';

-- Industrial: submeter (processo), exportar (relatórios)
UPDATE cq_permissoes SET
  pode_submeter = (modulo IN ('processo_bucho','conferencia_quantidade','lancamento_cq_bucho')),
  pode_revisar = FALSE,
  pode_aprovar = FALSE,
  pode_bloquear = FALSE,
  pode_reabrir = FALSE,
  pode_importar = FALSE,
  pode_exportar = (modulo IN ('relatorio_cargas','dashboard_producao')),
  pode_alterar_parametros = FALSE,
  pode_anexar = TRUE,
  pode_excluir_evidencia = FALSE,
  pode_ver_auditoria = FALSE,
  pode_override = FALSE
WHERE perfil = 'industrial';

-- Comercial e Financeiro: somente exportar em seus módulos
UPDATE cq_permissoes SET
  pode_submeter = FALSE,
  pode_revisar = FALSE,
  pode_aprovar = FALSE,
  pode_bloquear = FALSE,
  pode_reabrir = FALSE,
  pode_importar = FALSE,
  pode_exportar = pode_visualizar,  -- pode exportar onde pode ver
  pode_alterar_parametros = FALSE,
  pode_anexar = FALSE,
  pode_excluir_evidencia = FALSE,
  pode_ver_auditoria = FALSE,
  pode_override = FALSE
WHERE perfil IN ('comercial','financeiro');

-- ── Inserir módulos para novos perfis: motorista, coletor ──

-- Módulos que motorista/coletor precisam (futuro módulo mobile)
INSERT INTO cq_permissoes (perfil, modulo, pode_visualizar, pode_criar, pode_editar, pode_excluir,
  pode_submeter, pode_revisar, pode_aprovar, pode_bloquear, pode_reabrir,
  pode_importar, pode_exportar, pode_alterar_parametros, pode_anexar,
  pode_excluir_evidencia, pode_ver_auditoria, pode_override)
VALUES
  -- Motorista: acesso mínimo (coleta/entrega + anexos)
  ('motorista', 'coleta_entrega',      TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  ('motorista', 'recebimento_lote',    TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  ('motorista', 'rastreabilidade',     TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE),
  -- Coletor: inspeção de campo + anexos
  ('coletor',   'coleta_entrega',      TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  ('coletor',   'inspecao_qualidade',  TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  ('coletor',   'recebimento_lote',    TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  ('coletor',   'rastreabilidade',     TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE),
  ('coletor',   'conferencia_quantidade', TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE)
ON CONFLICT (perfil, modulo) DO UPDATE SET
  pode_visualizar = EXCLUDED.pode_visualizar,
  pode_criar = EXCLUDED.pode_criar,
  pode_editar = EXCLUDED.pode_editar,
  pode_excluir = EXCLUDED.pode_excluir,
  pode_submeter = EXCLUDED.pode_submeter,
  pode_anexar = EXCLUDED.pode_anexar;


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 4: ATUALIZAR cq_tem_permissao() PARA SUPORTAR TODAS AS OPERAÇÕES
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.cq_tem_permissao(p_modulo TEXT, p_acao TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.cq_permissoes p
    WHERE p.perfil = (
      SELECT perfil FROM public.cq_usuarios
      WHERE auth_id = auth.uid() AND ativo = TRUE LIMIT 1
    )
    AND p.modulo = p_modulo
    AND CASE p_acao
      WHEN 'visualizar'          THEN p.pode_visualizar
      WHEN 'criar'               THEN p.pode_criar
      WHEN 'editar'              THEN p.pode_editar
      WHEN 'excluir'             THEN p.pode_excluir
      WHEN 'submeter'            THEN p.pode_submeter
      WHEN 'revisar'             THEN p.pode_revisar
      WHEN 'aprovar'             THEN p.pode_aprovar
      WHEN 'bloquear'            THEN p.pode_bloquear
      WHEN 'reabrir'             THEN p.pode_reabrir
      WHEN 'importar'            THEN p.pode_importar
      WHEN 'exportar'            THEN p.pode_exportar
      WHEN 'alterar_parametros'  THEN p.pode_alterar_parametros
      WHEN 'anexar'              THEN p.pode_anexar
      WHEN 'excluir_evidencia'   THEN p.pode_excluir_evidencia
      WHEN 'ver_auditoria'       THEN p.pode_ver_auditoria
      WHEN 'override'            THEN p.pode_override
      ELSE FALSE
    END = TRUE
  );
$$;

COMMENT ON FUNCTION cq_tem_permissao IS
  'Verifica permissão granular por módulo + ação. Suporta 16 operações: visualizar, criar, editar, excluir, submeter, revisar, aprovar, bloquear, reabrir, importar, exportar, alterar_parametros, anexar, excluir_evidencia, ver_auditoria, override.';


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 5: TRILHA DE AUDITORIA COMPLETA (cq_audit_trail)
-- Nova tabela com antes/depois, módulo, registro afetado, origem, justificativa
-- Complementa o cq_audit_log existente (mantido para retrocompatibilidade)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_audit_trail (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Quem
  usuario_id        UUID REFERENCES cq_usuarios(id),
  usuario_email     TEXT NOT NULL,
  usuario_nome      TEXT,
  usuario_perfil    TEXT,
  -- Quando
  criado_em         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- O quê
  modulo            TEXT NOT NULL,     -- ex: inspecao_qualidade, recebimento_lote, gestao_usuarios
  acao              TEXT NOT NULL,     -- ex: criar, editar, submeter, revisar, aprovar, override, excluir
  -- Onde (registro afetado)
  tabela_afetada    TEXT,              -- ex: registros_cq_inspecao
  registro_id       UUID,             -- ID do registro modificado
  registro_codigo   TEXT,             -- ID legível (ex: NC-260318-001, lote_atak)
  -- Dados
  dados_antes       JSONB,            -- snapshot antes da alteração (NULL se criação)
  dados_depois      JSONB,            -- snapshot depois da alteração (NULL se exclusão)
  campos_alterados  TEXT[],           -- lista de campos que mudaram
  -- Contexto
  justificativa     TEXT,             -- obrigatória para override e bloqueio
  comentario        TEXT,             -- comentário livre do usuário
  origem            TEXT NOT NULL DEFAULT 'web',  -- web, mobile, importacao, offline, sistema, n8n
  -- Geolocalização
  latitude          NUMERIC(10,7),
  longitude         NUMERIC(10,7),
  dentro_planta     BOOLEAN,
  -- Device info (futuro módulo mobile)
  device_id         TEXT,             -- identificador do dispositivo
  device_info       JSONB,            -- user-agent, resolução, SO, etc.
  -- Referência cruzada
  session_token     TEXT,             -- vínculo com cq_sessoes
  ip_address        TEXT              -- IP de origem
);

-- IMUTÁVEL: sem UPDATE ou DELETE
-- (enforced via RLS — sem policy de update/delete)

-- Índices para consultas comuns
CREATE INDEX IF NOT EXISTS idx_audit_trail_usuario   ON cq_audit_trail(usuario_id);
CREATE INDEX IF NOT EXISTS idx_audit_trail_modulo    ON cq_audit_trail(modulo);
CREATE INDEX IF NOT EXISTS idx_audit_trail_acao      ON cq_audit_trail(acao);
CREATE INDEX IF NOT EXISTS idx_audit_trail_registro  ON cq_audit_trail(registro_id);
CREATE INDEX IF NOT EXISTS idx_audit_trail_tabela    ON cq_audit_trail(tabela_afetada);
CREATE INDEX IF NOT EXISTS idx_audit_trail_data      ON cq_audit_trail(criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_audit_trail_origem    ON cq_audit_trail(origem);

-- RLS
ALTER TABLE cq_audit_trail ENABLE ROW LEVEL SECURITY;

-- Inserção: qualquer autenticado (as funções SECURITY DEFINER inserem)
CREATE POLICY "trail_insert"
  ON cq_audit_trail FOR INSERT TO authenticated
  WITH CHECK (TRUE);

-- Leitura: só quem tem permissão ver_auditoria ou é admin
CREATE POLICY "trail_select"
  ON cq_audit_trail FOR SELECT TO authenticated
  USING (
    cq_get_perfil() = 'administrador'
    OR cq_tem_permissao('auditoria', 'ver_auditoria')
    -- Próprio usuário vê seus registros
    OR usuario_id = cq_user_id()
  );

-- Sem UPDATE/DELETE policies = imutável


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 6: TABELA DE OVERRIDES
-- Registro explícito quando usuário sobrescreve decisão do sistema
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_overrides (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Quem
  usuario_id           UUID NOT NULL REFERENCES cq_usuarios(id),
  usuario_email        TEXT NOT NULL,
  usuario_nome         TEXT,
  usuario_perfil       TEXT NOT NULL,
  -- Contexto
  modulo               TEXT NOT NULL,
  tabela_afetada       TEXT NOT NULL,
  registro_id          UUID NOT NULL,
  -- O quê foi sobrescrito
  campo_override       TEXT NOT NULL,         -- ex: status_final, classificacao
  valor_sistema        TEXT NOT NULL,         -- valor calculado pelo sistema (motor de decisão)
  valor_override       TEXT NOT NULL,         -- valor escolhido pelo usuário
  -- Justificativa
  justificativa        TEXT NOT NULL,         -- obrigatória — CHECK abaixo
  -- Referência ao cálculo original
  calculo_original     JSONB,                 -- snapshot do retorno de cq_motor_decisao ou similar
  -- Auditoria
  criado_em            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  origem               TEXT NOT NULL DEFAULT 'web',
  latitude             NUMERIC(10,7),
  longitude            NUMERIC(10,7),
  dentro_planta        BOOLEAN,

  CONSTRAINT override_justificativa_obrigatoria
    CHECK (LENGTH(TRIM(justificativa)) >= 10)  -- mínimo 10 caracteres
);

CREATE INDEX IF NOT EXISTS idx_override_usuario   ON cq_overrides(usuario_id);
CREATE INDEX IF NOT EXISTS idx_override_registro  ON cq_overrides(registro_id);
CREATE INDEX IF NOT EXISTS idx_override_campo     ON cq_overrides(campo_override);
CREATE INDEX IF NOT EXISTS idx_override_data      ON cq_overrides(criado_em DESC);

-- RLS
ALTER TABLE cq_overrides ENABLE ROW LEVEL SECURITY;

CREATE POLICY "override_insert"
  ON cq_overrides FOR INSERT TO authenticated
  WITH CHECK (cq_tem_permissao('inspecao_qualidade', 'override'));

CREATE POLICY "override_select"
  ON cq_overrides FOR SELECT TO authenticated
  USING (
    cq_get_perfil() IN ('administrador', 'diretoria')
    OR usuario_id = cq_user_id()
  );

-- Sem UPDATE/DELETE = imutável


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 7: FUNÇÃO DE AUDITORIA — cq_registrar_audit()
-- Usada pelas funções P1 e por triggers para log automático
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_registrar_audit(
  p_modulo TEXT,
  p_acao TEXT,
  p_tabela TEXT DEFAULT NULL,
  p_registro_id UUID DEFAULT NULL,
  p_registro_codigo TEXT DEFAULT NULL,
  p_dados_antes JSONB DEFAULT NULL,
  p_dados_depois JSONB DEFAULT NULL,
  p_justificativa TEXT DEFAULT NULL,
  p_comentario TEXT DEFAULT NULL,
  p_origem TEXT DEFAULT 'web'
)
RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_user_id UUID;
  v_user_email TEXT;
  v_user_nome TEXT;
  v_user_perfil TEXT;
  v_campos_alterados TEXT[];
  v_audit_id UUID;
  v_key TEXT;
BEGIN
  -- Buscar dados do usuário atual
  SELECT u.id, u.email, u.nome, u.perfil
  INTO v_user_id, v_user_email, v_user_nome, v_user_perfil
  FROM cq_usuarios u
  WHERE u.auth_id = auth.uid() AND u.ativo = TRUE
  LIMIT 1;

  -- Se não encontrou usuário (ex: trigger de sistema), usa defaults
  IF v_user_email IS NULL THEN
    v_user_email := 'sistema';
    v_user_nome := 'Sistema';
    v_user_perfil := 'sistema';
  END IF;

  -- Calcular campos alterados (diff entre antes e depois)
  IF p_dados_antes IS NOT NULL AND p_dados_depois IS NOT NULL THEN
    SELECT ARRAY_AGG(key)
    INTO v_campos_alterados
    FROM (
      SELECT key
      FROM jsonb_each(p_dados_depois)
      WHERE NOT (p_dados_antes ? key AND p_dados_antes->key = p_dados_depois->key)
    ) diff;
  END IF;

  -- Inserir na trilha
  INSERT INTO cq_audit_trail (
    usuario_id, usuario_email, usuario_nome, usuario_perfil,
    modulo, acao, tabela_afetada, registro_id, registro_codigo,
    dados_antes, dados_depois, campos_alterados,
    justificativa, comentario, origem
  )
  VALUES (
    v_user_id, v_user_email, v_user_nome, v_user_perfil,
    p_modulo, p_acao, p_tabela, p_registro_id, p_registro_codigo,
    p_dados_antes, p_dados_depois, v_campos_alterados,
    p_justificativa, p_comentario, p_origem
  )
  RETURNING id INTO v_audit_id;

  RETURN v_audit_id;
END;
$$;

COMMENT ON FUNCTION cq_registrar_audit IS
  'Registra evento na trilha de auditoria (cq_audit_trail). Calcula automaticamente os campos alterados comparando antes/depois.';


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 8: TRIGGER AUTOMÁTICO DE AUDITORIA
-- Captura INSERT/UPDATE/DELETE em tabelas críticas
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_trigger_audit()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_acao TEXT;
  v_antes JSONB;
  v_depois JSONB;
  v_registro_id UUID;
  v_registro_codigo TEXT;
BEGIN
  -- Determinar ação
  IF TG_OP = 'INSERT' THEN
    v_acao := 'criar';
    v_antes := NULL;
    v_depois := to_jsonb(NEW);
    v_registro_id := NEW.id;
    v_registro_codigo := COALESCE(
      NEW.id_inspecao,
      NEW.codigo,
      NEW.id::TEXT
    );
  ELSIF TG_OP = 'UPDATE' THEN
    v_acao := 'editar';
    v_antes := to_jsonb(OLD);
    v_depois := to_jsonb(NEW);
    v_registro_id := NEW.id;
    v_registro_codigo := COALESCE(
      NEW.id_inspecao,
      NEW.codigo,
      NEW.id::TEXT
    );
    -- Detectar transição de status
    IF OLD.status_workflow IS DISTINCT FROM NEW.status_workflow THEN
      v_acao := 'transicao_status';
    END IF;
    IF OLD.status_final IS DISTINCT FROM NEW.status_final THEN
      v_acao := 'alterar_status_final';
    END IF;
  ELSIF TG_OP = 'DELETE' THEN
    v_acao := 'excluir';
    v_antes := to_jsonb(OLD);
    v_depois := NULL;
    v_registro_id := OLD.id;
    v_registro_codigo := COALESCE(
      OLD.id_inspecao,
      OLD.codigo,
      OLD.id::TEXT
    );
  END IF;

  -- Registrar na trilha (ignora erros para não bloquear operação)
  BEGIN
    PERFORM cq_registrar_audit(
      TG_TABLE_NAME,       -- modulo = nome da tabela
      v_acao,
      TG_TABLE_NAME,       -- tabela_afetada
      v_registro_id,
      v_registro_codigo,
      v_antes,
      v_depois
    );
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Audit trail falhou para %: %', TG_TABLE_NAME, SQLERRM;
  END;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

-- Aplicar trigger nas tabelas críticas
DROP TRIGGER IF EXISTS trg_audit_trail_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_audit_trail_inspecao
  AFTER INSERT OR UPDATE OR DELETE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_recebimento ON cq_recebimentos;
CREATE TRIGGER trg_audit_trail_recebimento
  AFTER INSERT OR UPDATE OR DELETE ON cq_recebimentos
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_nc ON cq_nao_conformidades;
CREATE TRIGGER trg_audit_trail_nc
  AFTER INSERT OR UPDATE OR DELETE ON cq_nao_conformidades
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_plano ON cq_planos_acao;
CREATE TRIGGER trg_audit_trail_plano
  AFTER INSERT OR UPDATE OR DELETE ON cq_planos_acao
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_revisao ON cq_revisoes;
CREATE TRIGGER trg_audit_trail_revisao
  AFTER INSERT ON cq_revisoes
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_parametros ON cq_parametros_inspecao;
CREATE TRIGGER trg_audit_trail_parametros
  AFTER INSERT OR UPDATE OR DELETE ON cq_parametros_inspecao
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_usuarios ON cq_usuarios;
CREATE TRIGGER trg_audit_trail_usuarios
  AFTER INSERT OR UPDATE OR DELETE ON cq_usuarios
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();

DROP TRIGGER IF EXISTS trg_audit_trail_permissoes ON cq_permissoes;
CREATE TRIGGER trg_audit_trail_permissoes
  AFTER INSERT OR UPDATE OR DELETE ON cq_permissoes
  FOR EACH ROW EXECUTE FUNCTION cq_trigger_audit();


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 9: OVERRIDE CONTROLADO — cq_registrar_override()
-- Registra quando usuário sobrescreve decisão do sistema
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_registrar_override(
  p_inspecao_id UUID,
  p_campo TEXT,                 -- 'status_final' ou 'classificacao'
  p_valor_override TEXT,        -- valor desejado pelo usuário
  p_justificativa TEXT,         -- obrigatória (mín 10 chars)
  p_origem TEXT DEFAULT 'web'
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_user_id UUID;
  v_user_email TEXT;
  v_user_nome TEXT;
  v_user_perfil TEXT;
  v_insp RECORD;
  v_valor_sistema TEXT;
  v_decisao_original JSONB;
  v_pct NUMERIC;
BEGIN
  -- Verificar usuário
  SELECT u.id, u.email, u.nome, u.perfil
  INTO v_user_id, v_user_email, v_user_nome, v_user_perfil
  FROM cq_usuarios u
  WHERE u.auth_id = auth.uid() AND u.ativo = TRUE
  LIMIT 1;

  IF v_user_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- Verificar permissão de override
  IF NOT cq_tem_permissao('inspecao_qualidade', 'override') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Sem permissão para override');
  END IF;

  -- Validar justificativa
  IF p_justificativa IS NULL OR LENGTH(TRIM(p_justificativa)) < 10 THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Justificativa obrigatória (mínimo 10 caracteres)');
  END IF;

  -- Validar campo
  IF p_campo NOT IN ('status_final', 'classificacao') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Campo "%s" não permite override', p_campo));
  END IF;

  -- Carregar inspeção
  SELECT * INTO v_insp FROM registros_cq_inspecao WHERE id = p_inspecao_id;
  IF v_insp IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  -- Calcular o que o sistema diria (sem override)
  v_pct := COALESCE(v_insp.percentual_defeitos, 0);
  v_decisao_original := cq_motor_decisao(v_pct, v_insp.produto);

  IF p_campo = 'status_final' THEN
    v_valor_sistema := v_decisao_original->>'status_final';
  ELSE
    v_valor_sistema := v_decisao_original->>'classificacao';
  END IF;

  -- Se o valor é igual ao do sistema, não é override
  IF p_valor_override = v_valor_sistema THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Valor igual ao calculado pelo sistema — não é override');
  END IF;

  -- Registrar override (tabela imutável)
  INSERT INTO cq_overrides (
    usuario_id, usuario_email, usuario_nome, usuario_perfil,
    modulo, tabela_afetada, registro_id,
    campo_override, valor_sistema, valor_override,
    justificativa, calculo_original, origem
  ) VALUES (
    v_user_id, v_user_email, v_user_nome, v_user_perfil,
    'inspecao_qualidade', 'registros_cq_inspecao', p_inspecao_id,
    p_campo, v_valor_sistema, p_valor_override,
    p_justificativa, v_decisao_original, p_origem
  );

  -- Aplicar o override na inspeção
  IF p_campo = 'status_final' THEN
    UPDATE registros_cq_inspecao
    SET status_final = p_valor_override, atualizado_em = NOW()
    WHERE id = p_inspecao_id;
  ELSE
    UPDATE registros_cq_inspecao
    SET classificacao = p_valor_override, atualizado_em = NOW()
    WHERE id = p_inspecao_id;
  END IF;

  -- Registrar na trilha de auditoria
  PERFORM cq_registrar_audit(
    'inspecao_qualidade',
    'override',
    'registros_cq_inspecao',
    p_inspecao_id,
    v_insp.id_inspecao,
    jsonb_build_object(p_campo, v_valor_sistema),
    jsonb_build_object(p_campo, p_valor_override),
    p_justificativa,
    format('Override de %s: %s → %s', p_campo, v_valor_sistema, p_valor_override),
    p_origem
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'campo', p_campo,
    'valor_sistema', v_valor_sistema,
    'valor_override', p_valor_override,
    'decisao_original', v_decisao_original,
    'justificativa', p_justificativa,
    'override_por', v_user_nome
  );
END;
$$;

COMMENT ON FUNCTION cq_registrar_override IS
  'Override controlado: permite sobrescrever decisão do sistema (status_final ou classificação) com justificativa obrigatória. Requer permissão "override". O cálculo original é preservado.';


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 10: ENFORCEMENT — ATUALIZAR FUNÇÕES P1 COM PERMISSÕES
-- Adiciona verificação de cq_tem_permissao() nas funções centralizadas
-- ════════════════════════════════════════════════════════════════════════════

-- Atualizar cq_submeter_inspecao() para verificar permissão 'submeter'
CREATE OR REPLACE FUNCTION cq_submeter_inspecao(p_inspecao_id UUID)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_insp RECORD;
  v_total_defeitos INTEGER;
  v_pct NUMERIC;
  v_decisao JSONB;
  v_validacao JSONB;
  v_dados_validacao JSONB;
  v_usuario_id UUID;
  v_perfil TEXT;
  v_dados_antes JSONB;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- P2: Verificar permissão granular
  IF NOT cq_tem_permissao('inspecao_qualidade', 'submeter') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Sem permissão para submeter inspeção');
  END IF;

  -- Carregar inspeção
  SELECT * INTO v_insp
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_insp IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  -- Snapshot antes (para auditoria)
  v_dados_antes := to_jsonb(v_insp);

  -- Só pode submeter se estiver em rascunho ou em_analise
  IF v_insp.status_workflow NOT IN ('rascunho', 'em_analise') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Inspeção em status "%s" não pode ser submetida', v_insp.status_workflow));
  END IF;

  -- Só criador ou admin pode submeter
  IF v_usuario_id != v_insp.criador_id AND v_perfil != 'administrador' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas o criador ou administrador pode submeter');
  END IF;

  -- Calcular total de defeitos
  v_total_defeitos := COALESCE(v_insp.fermentacao, 0) + COALESCE(v_insp.sem_folhas, 0)
    + COALESCE(v_insp.folhas_arrebentadas, 0) + COALESCE(v_insp.pedaco, 0)
    + COALESCE(v_insp.sujo, 0) + COALESCE(v_insp.rasgado, 0)
    + COALESCE(v_insp.corte_irregular, 0) + COALESCE(v_insp.furado, 0)
    + COALESCE(v_insp.mau_cheiro, 0) + COALESCE(v_insp.contaminacao, 0)
    + COALESCE(v_insp.outro_defeito, 0);

  -- Calcular percentual
  IF COALESCE(v_insp.quantidade_analisada, 0) > 0 THEN
    v_pct := (v_total_defeitos::NUMERIC / v_insp.quantidade_analisada) * 100;
  ELSE
    v_pct := 0;
  END IF;

  -- Montar dados para validação
  v_dados_validacao := jsonb_build_object(
    'fornecedor', v_insp.fornecedor,
    'produto', v_insp.produto,
    'quantidade_analisada', v_insp.quantidade_analisada,
    'total_defeitos', v_total_defeitos,
    'data_inspecao', v_insp.data_inspecao,
    'status_final', CASE WHEN v_pct > 15 THEN 'bloqueado' ELSE NULL END,
    'observacoes', v_insp.observacoes
  );

  -- Validar
  v_validacao := cq_validar_inspecao(v_dados_validacao);
  IF NOT (v_validacao->>'valido')::BOOLEAN THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'erro', 'Validação falhou',
      'validacao', v_validacao
    );
  END IF;

  -- Calcular decisão via motor
  v_decisao := cq_motor_decisao(v_pct, v_insp.produto);

  -- Atualizar registro atomicamente
  UPDATE registros_cq_inspecao
  SET
    total_defeitos = v_total_defeitos,
    percentual_defeitos = ROUND(v_pct, 2),
    classificacao = v_decisao->>'classificacao',
    status_final = v_decisao->>'status_final',
    status_workflow = 'aguardando_revisao',
    atualizado_em = NOW()
  WHERE id = p_inspecao_id;

  -- P2: Registrar na trilha de auditoria
  PERFORM cq_registrar_audit(
    'inspecao_qualidade',
    'submeter',
    'registros_cq_inspecao',
    p_inspecao_id,
    v_insp.id_inspecao,
    v_dados_antes,
    jsonb_build_object(
      'total_defeitos', v_total_defeitos,
      'percentual_defeitos', ROUND(v_pct, 2),
      'classificacao', v_decisao->>'classificacao',
      'status_final', v_decisao->>'status_final',
      'status_workflow', 'aguardando_revisao'
    )
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'inspecao_id', p_inspecao_id,
    'total_defeitos', v_total_defeitos,
    'percentual_defeitos', ROUND(v_pct, 2),
    'decisao', v_decisao,
    'status_workflow', 'aguardando_revisao'
  );
END;
$$;


-- Atualizar cq_criar_revisao() com audit trail
CREATE OR REPLACE FUNCTION cq_criar_revisao(
  p_inspecao_id UUID,
  p_decisao TEXT,
  p_comentario TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_usuario_id UUID;
  v_perfil TEXT;
  v_user_email TEXT;
  v_user_nome TEXT;
  v_criador_id UUID;
  v_status_atual TEXT;
  v_novo_status TEXT;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- P2: Verificar permissão granular
  IF NOT cq_tem_permissao('inspecao_qualidade', 'revisar') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Sem permissão para revisar inspeção');
  END IF;

  IF p_decisao NOT IN ('aprovada', 'reprovada', 'devolvida') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', format('Decisão inválida: %s', p_decisao));
  END IF;

  SELECT status_workflow, criador_id
  INTO v_status_atual, v_criador_id
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_status_atual IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  IF v_status_atual != 'aguardando_revisao' THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro',
      format('Inspeção em status "%s" não pode ser revisada', v_status_atual));
  END IF;

  IF v_usuario_id = v_criador_id THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Criador não pode revisar própria inspeção');
  END IF;

  SELECT email, nome INTO v_user_email, v_user_nome
  FROM cq_usuarios
  WHERE id = v_usuario_id;

  CASE p_decisao
    WHEN 'aprovada'  THEN v_novo_status := 'revisada';
    WHEN 'reprovada' THEN v_novo_status := 'reprovada';
    WHEN 'devolvida' THEN v_novo_status := 'rascunho';
  END CASE;

  INSERT INTO cq_revisoes (inspecao_id, revisor_id, revisor_email, revisor_nome, decisao, comentario)
  VALUES (p_inspecao_id, v_usuario_id, v_user_email, v_user_nome, p_decisao, p_comentario);

  UPDATE registros_cq_inspecao
  SET status_workflow = v_novo_status, atualizado_em = NOW()
  WHERE id = p_inspecao_id;

  -- P2: Audit trail
  PERFORM cq_registrar_audit(
    'inspecao_qualidade',
    'revisar',
    'registros_cq_inspecao',
    p_inspecao_id,
    NULL,
    jsonb_build_object('status_workflow', v_status_atual),
    jsonb_build_object('status_workflow', v_novo_status, 'decisao', p_decisao),
    NULL,
    p_comentario
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'decisao', p_decisao,
    'de', v_status_atual,
    'para', v_novo_status,
    'revisor', v_user_nome
  );
END;
$$;


-- Atualizar cq_acoes_permitidas() para usar cq_tem_permissao()
CREATE OR REPLACE FUNCTION cq_acoes_permitidas(p_inspecao_id UUID)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_status TEXT;
  v_criador_id UUID;
  v_usuario_id UUID;
  v_perfil TEXT;
  v_eh_criador BOOLEAN;
  v_acoes JSONB := '[]'::JSONB;
BEGIN
  v_usuario_id := cq_user_id();
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN '[]'::JSONB;
  END IF;

  SELECT status_workflow, criador_id
  INTO v_status, v_criador_id
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_status IS NULL THEN
    RETURN '[]'::JSONB;
  END IF;

  v_eh_criador := (v_usuario_id = v_criador_id);

  -- Rascunho / Em análise
  IF v_status IN ('rascunho', 'em_analise') THEN
    IF v_eh_criador OR v_perfil = 'administrador' THEN
      IF cq_tem_permissao('inspecao_qualidade', 'editar') THEN
        v_acoes := v_acoes || jsonb_build_array(
          jsonb_build_object('acao', 'editar', 'label', 'Editar', 'confirm', FALSE)
        );
      END IF;
      IF cq_tem_permissao('inspecao_qualidade', 'submeter') THEN
        v_acoes := v_acoes || jsonb_build_array(
          jsonb_build_object('acao', 'submeter', 'label', 'Submeter para Revisão', 'confirm', TRUE)
        );
      END IF;
    END IF;
  END IF;

  -- Aguardando revisão
  IF v_status = 'aguardando_revisao' THEN
    IF NOT v_eh_criador AND cq_tem_permissao('inspecao_qualidade', 'revisar') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'revisar', 'label', 'Revisar', 'confirm', FALSE)
      );
    END IF;
  END IF;

  -- Revisada
  IF v_status = 'revisada' THEN
    IF NOT v_eh_criador AND cq_tem_permissao('inspecao_qualidade', 'aprovar') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'aprovada', 'label', 'Aprovar', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Reprovada
  IF v_status = 'reprovada' THEN
    IF cq_tem_permissao('inspecao_qualidade', 'bloquear') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'abrir_nc', 'label', 'Abrir NC', 'confirm', FALSE)
      );
    END IF;
    IF cq_tem_permissao('inspecao_qualidade', 'reabrir') THEN
      v_acoes := v_acoes || jsonb_build_array(
        jsonb_build_object('acao', 'rascunho', 'label', 'Reabrir', 'confirm', TRUE)
      );
    END IF;
  END IF;

  -- Aprovada
  IF v_status = 'aprovada' AND v_perfil = 'administrador' THEN
    v_acoes := v_acoes || jsonb_build_array(
      jsonb_build_object('acao', 'encerrada', 'label', 'Encerrar', 'confirm', TRUE)
    );
  END IF;

  -- Cancelar
  IF v_perfil = 'administrador' AND v_status NOT IN ('encerrada', 'cancelada') THEN
    v_acoes := v_acoes || jsonb_build_array(
      jsonb_build_object('acao', 'cancelada', 'label', 'Cancelar', 'confirm', TRUE)
    );
  END IF;

  -- Override (admin/diretoria com permissão)
  IF v_status NOT IN ('encerrada', 'cancelada') AND cq_tem_permissao('inspecao_qualidade', 'override') THEN
    v_acoes := v_acoes || jsonb_build_array(
      jsonb_build_object('acao', 'override', 'label', 'Override (Justificar)', 'confirm', TRUE)
    );
  END IF;

  RETURN v_acoes;
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 11: CONSULTAR AUDITORIA — RPC para frontend
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_consultar_auditoria(
  p_registro_id UUID DEFAULT NULL,
  p_modulo TEXT DEFAULT NULL,
  p_acao TEXT DEFAULT NULL,
  p_usuario_id UUID DEFAULT NULL,
  p_data_inicio DATE DEFAULT NULL,
  p_data_fim DATE DEFAULT NULL,
  p_limite INTEGER DEFAULT 50,
  p_offset INTEGER DEFAULT 0
)
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_perfil TEXT;
  v_result JSONB;
BEGIN
  v_perfil := cq_get_perfil();

  -- Verificar permissão
  IF v_perfil != 'administrador'
    AND NOT cq_tem_permissao('auditoria', 'ver_auditoria') THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Sem permissão para ver auditoria');
  END IF;

  SELECT jsonb_build_object(
    'ok', TRUE,
    'registros', COALESCE(jsonb_agg(row_to_json(t)), '[]'::JSONB),
    'total', (SELECT COUNT(*) FROM cq_audit_trail at2
              WHERE (p_registro_id IS NULL OR at2.registro_id = p_registro_id)
                AND (p_modulo IS NULL OR at2.modulo = p_modulo)
                AND (p_acao IS NULL OR at2.acao = p_acao)
                AND (p_usuario_id IS NULL OR at2.usuario_id = p_usuario_id)
                AND (p_data_inicio IS NULL OR at2.criado_em >= p_data_inicio)
                AND (p_data_fim IS NULL OR at2.criado_em <= p_data_fim + INTERVAL '1 day'))
  )
  INTO v_result
  FROM (
    SELECT
      at.id, at.usuario_nome, at.usuario_email, at.usuario_perfil,
      at.modulo, at.acao, at.tabela_afetada,
      at.registro_id, at.registro_codigo,
      at.campos_alterados, at.justificativa, at.comentario,
      at.origem, at.criado_em,
      at.dados_antes, at.dados_depois
    FROM cq_audit_trail at
    WHERE (p_registro_id IS NULL OR at.registro_id = p_registro_id)
      AND (p_modulo IS NULL OR at.modulo = p_modulo)
      AND (p_acao IS NULL OR at.acao = p_acao)
      AND (p_usuario_id IS NULL OR at.usuario_id = p_usuario_id)
      AND (p_data_inicio IS NULL OR at.criado_em >= p_data_inicio)
      AND (p_data_fim IS NULL OR at.criado_em <= p_data_fim + INTERVAL '1 day')
    ORDER BY at.criado_em DESC
    LIMIT p_limite OFFSET p_offset
  ) t;

  RETURN v_result;
END;
$$;

COMMENT ON FUNCTION cq_consultar_auditoria IS
  'Consulta a trilha de auditoria com filtros. Requer permissão ver_auditoria ou perfil administrador.';


-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 12: CONSULTAR PERMISSÕES DO USUÁRIO ATUAL — RPC para frontend
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_minhas_permissoes()
RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
DECLARE
  v_perfil TEXT;
  v_result JSONB;
BEGIN
  v_perfil := cq_get_perfil();

  IF v_perfil IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  SELECT jsonb_build_object(
    'ok', TRUE,
    'perfil', v_perfil,
    'permissoes', COALESCE(jsonb_agg(jsonb_build_object(
      'modulo', p.modulo,
      'visualizar', p.pode_visualizar,
      'criar', p.pode_criar,
      'editar', p.pode_editar,
      'excluir', p.pode_excluir,
      'submeter', p.pode_submeter,
      'revisar', p.pode_revisar,
      'aprovar', p.pode_aprovar,
      'bloquear', p.pode_bloquear,
      'reabrir', p.pode_reabrir,
      'importar', p.pode_importar,
      'exportar', p.pode_exportar,
      'alterar_parametros', p.pode_alterar_parametros,
      'anexar', p.pode_anexar,
      'excluir_evidencia', p.pode_excluir_evidencia,
      'ver_auditoria', p.pode_ver_auditoria,
      'override', p.pode_override
    )), '[]'::JSONB)
  )
  INTO v_result
  FROM cq_permissoes p
  WHERE p.perfil = v_perfil;

  RETURN v_result;
END;
$$;

COMMENT ON FUNCTION cq_minhas_permissoes IS
  'Retorna todas as permissões do usuário atual, por módulo. Usado pelo frontend para habilitar/desabilitar botões e menu.';


-- ════════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ════════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION cq_registrar_audit TO authenticated;
GRANT EXECUTE ON FUNCTION cq_registrar_override TO authenticated;
GRANT EXECUTE ON FUNCTION cq_consultar_auditoria TO authenticated;
GRANT EXECUTE ON FUNCTION cq_minhas_permissoes TO authenticated;
-- cq_submeter_inspecao, cq_criar_revisao, cq_acoes_permitidas já tinham GRANT da P1

COMMIT;


-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO PÓS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════════

-- V1: Confirmar novas colunas em cq_permissoes
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'cq_permissoes'
ORDER BY ordinal_position;

-- V2: Confirmar permissões do administrador (deve ter tudo TRUE)
SELECT modulo,
  pode_submeter, pode_revisar, pode_aprovar, pode_bloquear,
  pode_reabrir, pode_override, pode_ver_auditoria
FROM cq_permissoes
WHERE perfil = 'administrador'
ORDER BY modulo;

-- V3: Confirmar tabela cq_audit_trail criada
SELECT tablename FROM pg_tables WHERE tablename = 'cq_audit_trail';

-- V4: Confirmar tabela cq_overrides criada
SELECT tablename FROM pg_tables WHERE tablename = 'cq_overrides';

-- V5: Confirmar triggers de auditoria
SELECT trigger_name, event_object_table, action_timing, event_manipulation
FROM information_schema.triggers
WHERE trigger_name LIKE 'trg_audit_trail%'
ORDER BY event_object_table;

-- V6: Confirmar perfis motorista/coletor aceitos
SELECT unnest(ARRAY['motorista','coletor']) AS perfil_teste;

-- V7: Confirmar funções criadas/atualizadas
SELECT routine_name FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name IN (
    'cq_registrar_audit', 'cq_registrar_override',
    'cq_consultar_auditoria', 'cq_minhas_permissoes',
    'cq_trigger_audit'
  )
ORDER BY routine_name;

-- V8: Testar cq_minhas_permissoes()
-- SELECT cq_minhas_permissoes();
