-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 09: Estratégia de Backup e Recuperação de Dados
-- Rodar APÓS 08_migration_eventos_validacoes.sql
--
-- Nota: Supabase gerencia backups automáticos do PostgreSQL (diário, 7 dias).
-- Esta migration cria a camada ADICIONAL de controle no nível da aplicação:
--   • Snapshots antes de operações críticas
--   • Log de execução de backups
--   • Pontos de restauração lógicos
--   • Estrutura para backup incremental (changelog)
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELA DE CONTROLE DE BACKUPS
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_backup_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo            TEXT NOT NULL CHECK (tipo IN (
                    'automatico_diario',   -- backup automático do Supabase
                    'snapshot_pre_import',  -- snapshot antes de importação ATAK
                    'snapshot_pre_merge',   -- snapshot antes de merge de cadastro
                    'snapshot_manual',      -- snapshot manual do admin
                    'incremental'           -- backup incremental (changelog)
                  )),
  status          TEXT NOT NULL DEFAULT 'em_execucao' CHECK (status IN (
                    'em_execucao', 'sucesso', 'falha', 'parcial'
                  )),
  descricao       TEXT,

  -- Escopo
  tabelas         TEXT[],                  -- quais tabelas foram incluídas
  total_registros BIGINT DEFAULT 0,

  -- Timing
  inicio          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  fim             TIMESTAMPTZ,
  duracao_seg     NUMERIC(10,2),

  -- Armazenamento
  storage_bucket  TEXT,                    -- Supabase Storage bucket
  storage_path    TEXT,                    -- caminho do arquivo de backup
  tamanho_bytes   BIGINT,

  -- Contexto
  operacao_origem TEXT,                    -- ex: 'sync_WRCAD009', 'merge_fornecedor'
  n8n_execution_id TEXT,
  executado_por   UUID REFERENCES cq_usuarios(id),
  erro_msg        TEXT,

  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backup_tipo ON cq_backup_log(tipo);
CREATE INDEX IF NOT EXISTS idx_backup_status ON cq_backup_log(status);
CREATE INDEX IF NOT EXISTS idx_backup_data ON cq_backup_log(inicio DESC);


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TABELA DE PONTOS DE RESTAURAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_restore_points (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome            TEXT NOT NULL UNIQUE,    -- ex: 'pre_import_2026-03-18_14:30'
  descricao       TEXT,
  backup_id       UUID REFERENCES cq_backup_log(id),

  -- Snapshot do estado no momento
  contagem_registros JSONB,               -- { tabela: count } para cada tabela
  ultimo_evento_id UUID,                  -- ID do último evento antes do snapshot

  -- Restauração
  restaurado       BOOLEAN DEFAULT false,
  restaurado_em    TIMESTAMPTZ,
  restaurado_por   UUID REFERENCES cq_usuarios(id),

  criado_em       TIMESTAMPTZ DEFAULT NOW()
);


-- ════════════════════════════════════════════════════════════════════════════
-- 3. CHANGELOG INCREMENTAL (para backup incremental)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_changelog (
  id              BIGSERIAL PRIMARY KEY,
  tabela          TEXT NOT NULL,
  operacao        TEXT NOT NULL CHECK (operacao IN ('INSERT','UPDATE','DELETE')),
  registro_id     UUID NOT NULL,
  dados           JSONB,                   -- dados novos (INSERT/UPDATE)
  dados_antigos   JSONB,                   -- dados antigos (UPDATE/DELETE)
  processado      BOOLEAN DEFAULT false,   -- já incluído em backup incremental?
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_changelog_processado ON cq_changelog(processado) WHERE processado = false;
CREATE INDEX IF NOT EXISTS idx_changelog_tabela ON cq_changelog(tabela, criado_em DESC);

-- Trigger genérico de changelog
CREATE OR REPLACE FUNCTION fn_changelog()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO cq_changelog (tabela, operacao, registro_id, dados)
    VALUES (TG_TABLE_NAME, 'INSERT', NEW.id, to_jsonb(NEW));
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO cq_changelog (tabela, operacao, registro_id, dados, dados_antigos)
    VALUES (TG_TABLE_NAME, 'UPDATE', NEW.id, to_jsonb(NEW), to_jsonb(OLD));
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO cq_changelog (tabela, operacao, registro_id, dados_antigos)
    VALUES (TG_TABLE_NAME, 'DELETE', OLD.id, to_jsonb(OLD));
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$ LANGUAGE plpgsql;

-- Aplicar changelog nas tabelas transacionais (movimentações)
DO $$
DECLARE
  v_tabela TEXT;
BEGIN
  FOR v_tabela IN
    SELECT unnest(ARRAY[
      'registros_cq_inspecao', 'cq_lote_inspecao', 'cq_lote_produto',
      'cq_recebimentos', 'cq_cargas', 'cq_planos_acao',
      'cq_validacao_divergencias', 'cq_fornecedor_score'
    ])
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_changelog_%I ON %I', v_tabela, v_tabela);
    EXECUTE format(
      'CREATE TRIGGER trg_changelog_%I
       AFTER INSERT OR UPDATE OR DELETE ON %I
       FOR EACH ROW EXECUTE FUNCTION fn_changelog()',
      v_tabela, v_tabela
    );
  END LOOP;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. FUNCTIONS DE SNAPSHOT
-- ════════════════════════════════════════════════════════════════════════════

-- 4.1 Criar snapshot (ponto de restauração) antes de operação crítica
CREATE OR REPLACE FUNCTION fn_criar_snapshot(
  p_nome TEXT,
  p_descricao TEXT DEFAULT NULL,
  p_operacao TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_backup_id UUID;
  v_restore_id UUID;
  v_contagens JSONB := '{}';
  v_total BIGINT := 0;
  v_tabela TEXT;
  v_count BIGINT;
  v_ultimo_evento UUID;
BEGIN
  -- Contar registros de cada tabela principal
  FOR v_tabela IN
    SELECT unnest(ARRAY[
      'cadastros_atak', 'cq_produtos', 'cq_veiculos', 'cq_transportadoras',
      'cq_clientes', 'registros_cq_inspecao', 'cq_lote_inspecao',
      'cq_recebimentos', 'cq_cargas', 'cq_validacao_divergencias',
      'cq_fornecedor_score', 'atak_cargas_raw'
    ])
  LOOP
    EXECUTE format('SELECT COUNT(*) FROM %I', v_tabela) INTO v_count;
    v_contagens := v_contagens || jsonb_build_object(v_tabela, v_count);
    v_total := v_total + v_count;
  END LOOP;

  -- Último evento
  SELECT id INTO v_ultimo_evento FROM cq_eventos ORDER BY criado_em DESC LIMIT 1;

  -- Criar registro de backup
  INSERT INTO cq_backup_log (tipo, status, descricao, total_registros, operacao_origem, fim, duracao_seg)
  VALUES ('snapshot_pre_import', 'sucesso', p_descricao, v_total, p_operacao, NOW(), 0)
  RETURNING id INTO v_backup_id;

  -- Criar ponto de restauração
  INSERT INTO cq_restore_points (nome, descricao, backup_id, contagem_registros, ultimo_evento_id)
  VALUES (p_nome, p_descricao, v_backup_id, v_contagens, v_ultimo_evento)
  RETURNING id INTO v_restore_id;

  -- Evento
  PERFORM fn_registrar_evento(
    'snapshot_criado', 'sistema',
    FORMAT('Snapshot criado: %s (%s registros)', p_nome, v_total),
    'cq_restore_points', v_restore_id, p_nome,
    jsonb_build_object('contagens', v_contagens, 'operacao', p_operacao)
  );

  RETURN v_restore_id;
END;
$$ LANGUAGE plpgsql;

-- 4.2 Listar pontos de restauração disponíveis
CREATE OR REPLACE VIEW vw_restore_points AS
SELECT
  rp.id,
  rp.nome,
  rp.descricao,
  rp.contagem_registros,
  rp.restaurado,
  rp.criado_em,
  bl.tipo AS backup_tipo,
  bl.operacao_origem,
  bl.total_registros
FROM cq_restore_points rp
LEFT JOIN cq_backup_log bl ON bl.id = rp.backup_id
ORDER BY rp.criado_em DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. VIEW: STATUS DO SISTEMA DE BACKUP
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_backup_status AS
SELECT
  -- Último backup por tipo
  (SELECT MAX(fim) FROM cq_backup_log WHERE tipo = 'automatico_diario' AND status = 'sucesso')
    AS ultimo_backup_diario,
  (SELECT MAX(fim) FROM cq_backup_log WHERE tipo = 'snapshot_pre_import' AND status = 'sucesso')
    AS ultimo_snapshot_import,

  -- Contagem de backups nos últimos 7 dias
  (SELECT COUNT(*) FROM cq_backup_log WHERE inicio >= NOW() - INTERVAL '7 days' AND status = 'sucesso')
    AS backups_7d_ok,
  (SELECT COUNT(*) FROM cq_backup_log WHERE inicio >= NOW() - INTERVAL '7 days' AND status = 'falha')
    AS backups_7d_falha,

  -- Pontos de restauração disponíveis
  (SELECT COUNT(*) FROM cq_restore_points WHERE restaurado = false)
    AS restore_points_disponiveis,

  -- Changelog pendente (não processado)
  (SELECT COUNT(*) FROM cq_changelog WHERE processado = false)
    AS changelog_pendente,

  -- Último changelog
  (SELECT MAX(criado_em) FROM cq_changelog)
    AS ultimo_changelog,

  NOW() AS verificado_em;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_backup_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_restore_points ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_changelog ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_backup" ON cq_backup_log FOR SELECT USING (true);
CREATE POLICY "all_backup" ON cq_backup_log FOR ALL USING (true);
CREATE POLICY "read_restore" ON cq_restore_points FOR SELECT USING (true);
CREATE POLICY "all_restore" ON cq_restore_points FOR ALL USING (true);
CREATE POLICY "read_changelog" ON cq_changelog FOR SELECT USING (true);
CREATE POLICY "all_changelog" ON cq_changelog FOR ALL USING (true);


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 09 CONCLUÍDA — Backup e Recuperação';
  RAISE NOTICE '  ✓ cq_backup_log (controle de backups)';
  RAISE NOTICE '  ✓ cq_restore_points (pontos de restauração)';
  RAISE NOTICE '  ✓ cq_changelog (backup incremental em 8 tabelas)';
  RAISE NOTICE '  ✓ fn_criar_snapshot() — snapshot antes de operações';
  RAISE NOTICE '  ✓ vw_backup_status (saúde do sistema de backup)';
  RAISE NOTICE '  ✓ vw_restore_points (pontos disponíveis)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
