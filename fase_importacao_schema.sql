-- ══════════════════════════════════════════════════════════════
-- IMPORTAÇÃO & RASTREABILIDADE DE DADOS
-- Infraestrutura para controle de importações, log de erros
-- e rastreabilidade de origem em todas as tabelas
-- Rodar no SQL Editor do Supabase
-- ══════════════════════════════════════════════════════════════


-- ── 1. TABELA: cq_importacoes ───────────────────────────────
-- Controle de lote/sessão de importação

CREATE TABLE IF NOT EXISTS cq_importacoes (
  id                UUID    DEFAULT gen_random_uuid() PRIMARY KEY,

  -- Identificação do lote
  codigo_importacao TEXT    NOT NULL UNIQUE,  -- IMP-YYYYMMDDHHMMSS-XXX

  -- Tipo de fonte
  tipo_fonte        TEXT    NOT NULL
    CHECK (tipo_fonte IN (
      'atak_pdf',          -- WRPRD501 (PDF)
      'atak_pcr_xlsx',     -- WRMVE500 (XLSX PCR)
      'planilha_cargas',   -- Aba FECHAMENTO (motoristas)
      'cadastro_atak',     -- RWCAD009 (fornecedores/motoristas)
      'manual',            -- Entrada manual via UI
      'migracao_legado'    -- Migração de dados antigos
    )),

  -- Arquivo de origem
  arquivo_nome      TEXT,                    -- nome original do arquivo
  arquivo_tamanho   BIGINT,                  -- tamanho em bytes
  arquivo_hash      TEXT,                    -- SHA-256 (previne reimportação do mesmo arquivo)

  -- Tabela destino
  tabela_destino    TEXT    NOT NULL,         -- 'cq_recebimentos', 'cq_cargas', etc.

  -- Contadores
  total_linhas      INTEGER DEFAULT 0,       -- total lido do arquivo
  total_importados  INTEGER DEFAULT 0,       -- inseridos com sucesso
  total_duplicados  INTEGER DEFAULT 0,       -- ignorados por duplicata
  total_erros       INTEGER DEFAULT 0,       -- falhas de inserção
  total_avisos      INTEGER DEFAULT 0,       -- warnings (dados inconsistentes)

  -- Status do lote
  status            TEXT    DEFAULT 'pendente'
    CHECK (status IN (
      'pendente',              -- criado, aguardando processamento
      'em_progresso',          -- inserção em andamento
      'concluido',             -- finalizado sem erros
      'concluido_com_erros',   -- finalizado com alguns erros
      'cancelado',             -- cancelado pelo usuário
      'falha'                  -- falha crítica
    )),

  -- Usuário que importou
  usuario_id        UUID    REFERENCES cq_usuarios(id),
  usuario_email     TEXT,
  usuario_nome      TEXT,

  -- Timestamps
  iniciado_em       TIMESTAMPTZ DEFAULT NOW(),
  concluido_em      TIMESTAMPTZ,

  -- Metadados extras (flexível)
  metadados         JSONB   DEFAULT '{}'::JSONB
  -- Ex: { "motorista": "CRISTIANO", "data_viagem": "2026-01-15", "paginas_pdf": 42 }
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_imp_codigo   ON cq_importacoes(codigo_importacao);
CREATE INDEX IF NOT EXISTS idx_imp_tipo     ON cq_importacoes(tipo_fonte);
CREATE INDEX IF NOT EXISTS idx_imp_status   ON cq_importacoes(status);
CREATE INDEX IF NOT EXISTS idx_imp_usuario  ON cq_importacoes(usuario_id);
CREATE INDEX IF NOT EXISTS idx_imp_dt       ON cq_importacoes(iniciado_em DESC);
CREATE INDEX IF NOT EXISTS idx_imp_hash     ON cq_importacoes(arquivo_hash);

-- RLS
ALTER TABLE cq_importacoes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cq_importacoes_select" ON cq_importacoes
  FOR SELECT USING (true);

CREATE POLICY "cq_importacoes_insert" ON cq_importacoes
  FOR INSERT WITH CHECK (true);

CREATE POLICY "cq_importacoes_update" ON cq_importacoes
  FOR UPDATE USING (true);


-- ── 2. TABELA: cq_importacao_erros ──────────────────────────
-- Log de erros e avisos por importação

CREATE TABLE IF NOT EXISTS cq_importacao_erros (
  id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,

  importacao_id   UUID    NOT NULL REFERENCES cq_importacoes(id) ON DELETE CASCADE,

  -- Localização do erro no arquivo
  linha_arquivo   INTEGER,                 -- número da linha no arquivo original
  coluna_arquivo  TEXT,                    -- nome da coluna (se identificável)

  -- Tipo e descrição
  tipo_erro       TEXT    NOT NULL
    CHECK (tipo_erro IN (
      'duplicata',     -- registro já existe no banco
      'validacao',     -- campo obrigatório ausente ou inválido
      'formato',       -- formato de dado incorreto (data, número)
      'insercao',      -- falha ao inserir no banco
      'referencia',    -- referência não encontrada (fornecedor, produto)
      'aviso'          -- dado inconsistente mas importável
    )),
  severidade      TEXT    DEFAULT 'erro'
    CHECK (severidade IN ('erro','aviso','info')),
  mensagem        TEXT    NOT NULL,

  -- Snapshot do registro problemático
  dados_registro  JSONB,

  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_imperr_imp    ON cq_importacao_erros(importacao_id);
CREATE INDEX IF NOT EXISTS idx_imperr_tipo   ON cq_importacao_erros(tipo_erro);
CREATE INDEX IF NOT EXISTS idx_imperr_sev    ON cq_importacao_erros(severidade);

-- RLS
ALTER TABLE cq_importacao_erros ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cq_importacao_erros_select" ON cq_importacao_erros
  FOR SELECT USING (true);

CREATE POLICY "cq_importacao_erros_insert" ON cq_importacao_erros
  FOR INSERT WITH CHECK (true);


-- ══════════════════════════════════════════════════════════════
-- 3. ALTER TABLE: Adicionar rastreabilidade às tabelas existentes
-- ══════════════════════════════════════════════════════════════


-- ── 3.1 registros_cq_inspecao ───────────────────────────────

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS origem_dado    TEXT DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS importacao_id  UUID,
  ADD COLUMN IF NOT EXISTS arquivo_origem TEXT;

-- CHECK constraint separado (ADD CONSTRAINT IF NOT EXISTS não existe no PG < 17)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_insp_origem' AND conrelid = 'registros_cq_inspecao'::regclass
  ) THEN
    ALTER TABLE registros_cq_inspecao
      ADD CONSTRAINT chk_insp_origem
      CHECK (origem_dado IN ('manual','atak','planilha','migracao'));
  END IF;
END $$;

-- FK para cq_importacoes (se não existir)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_insp_importacao' AND conrelid = 'registros_cq_inspecao'::regclass
  ) THEN
    ALTER TABLE registros_cq_inspecao
      ADD CONSTRAINT fk_insp_importacao
      FOREIGN KEY (importacao_id) REFERENCES cq_importacoes(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_insp_origem ON registros_cq_inspecao(origem_dado);
CREATE INDEX IF NOT EXISTS idx_insp_imp    ON registros_cq_inspecao(importacao_id);


-- ── 3.2 cq_recebimentos ────────────────────────────────────

ALTER TABLE cq_recebimentos
  ADD COLUMN IF NOT EXISTS origem_dado    TEXT DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS importacao_id  UUID,
  ADD COLUMN IF NOT EXISTS arquivo_origem TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_rec_origem' AND conrelid = 'cq_recebimentos'::regclass
  ) THEN
    ALTER TABLE cq_recebimentos
      ADD CONSTRAINT chk_rec_origem
      CHECK (origem_dado IN ('manual','atak','planilha','migracao'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_rec_importacao' AND conrelid = 'cq_recebimentos'::regclass
  ) THEN
    ALTER TABLE cq_recebimentos
      ADD CONSTRAINT fk_rec_importacao
      FOREIGN KEY (importacao_id) REFERENCES cq_importacoes(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_rec_origem ON cq_recebimentos(origem_dado);
CREATE INDEX IF NOT EXISTS idx_rec_imp    ON cq_recebimentos(importacao_id);


-- ── 3.3 cq_cargas ───────────────────────────────────────────

ALTER TABLE cq_cargas
  ADD COLUMN IF NOT EXISTS origem_dado    TEXT DEFAULT 'planilha',
  ADD COLUMN IF NOT EXISTS importacao_id  UUID,
  ADD COLUMN IF NOT EXISTS arquivo_origem TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_cargas_origem' AND conrelid = 'cq_cargas'::regclass
  ) THEN
    ALTER TABLE cq_cargas
      ADD CONSTRAINT chk_cargas_origem
      CHECK (origem_dado IN ('manual','planilha','migracao'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_cargas_importacao' AND conrelid = 'cq_cargas'::regclass
  ) THEN
    ALTER TABLE cq_cargas
      ADD CONSTRAINT fk_cargas_importacao
      FOREIGN KEY (importacao_id) REFERENCES cq_importacoes(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_cargas_origem ON cq_cargas(origem_dado);
CREATE INDEX IF NOT EXISTS idx_cargas_imp    ON cq_cargas(importacao_id);


-- ── 3.4 cadastros_atak ─────────────────────────────────────

ALTER TABLE cadastros_atak
  ADD COLUMN IF NOT EXISTS origem_dado    TEXT DEFAULT 'atak',
  ADD COLUMN IF NOT EXISTS importacao_id  UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_cad_origem' AND conrelid = 'cadastros_atak'::regclass
  ) THEN
    ALTER TABLE cadastros_atak
      ADD CONSTRAINT chk_cad_origem
      CHECK (origem_dado IN ('atak','manual','migracao'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_cad_importacao' AND conrelid = 'cadastros_atak'::regclass
  ) THEN
    ALTER TABLE cadastros_atak
      ADD CONSTRAINT fk_cad_importacao
      FOREIGN KEY (importacao_id) REFERENCES cq_importacoes(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_cad_origem ON cadastros_atak(origem_dado);


-- ══════════════════════════════════════════════════════════════
-- 4. BACKFILL: Preencher origem dos registros existentes
-- ══════════════════════════════════════════════════════════════

-- Inspeções existentes são manuais
UPDATE registros_cq_inspecao
SET origem_dado = 'manual'
WHERE origem_dado IS NULL;

-- Recebimentos ATAK (id_recebimento começa com 'ATAK-')
UPDATE cq_recebimentos
SET origem_dado = 'atak', arquivo_origem = 'migracao_historica_atak'
WHERE id_recebimento LIKE 'ATAK-%' AND (origem_dado IS NULL OR origem_dado = 'manual');

-- Recebimentos manuais
UPDATE cq_recebimentos
SET origem_dado = 'manual'
WHERE origem_dado IS NULL;


-- ══════════════════════════════════════════════════════════════
-- 5. REGISTRO DE MIGRAÇÃO HISTÓRICA
-- ══════════════════════════════════════════════════════════════

-- Criar lote de migração para dados ATAK existentes
INSERT INTO cq_importacoes (
  codigo_importacao, tipo_fonte, arquivo_nome, tabela_destino,
  total_importados, status, usuario_nome, concluido_em
)
SELECT
  'MIG-ATAK-HISTORICO',
  'migracao_legado',
  'backfill_atak_existente',
  'cq_recebimentos',
  COUNT(*),
  'concluido',
  'migracao_sistema',
  NOW()
FROM cq_recebimentos
WHERE id_recebimento LIKE 'ATAK-%'
ON CONFLICT (codigo_importacao) DO NOTHING;

-- Vincular registros ATAK ao lote de migração
UPDATE cq_recebimentos
SET importacao_id = (
  SELECT id FROM cq_importacoes
  WHERE codigo_importacao = 'MIG-ATAK-HISTORICO'
)
WHERE id_recebimento LIKE 'ATAK-%'
  AND importacao_id IS NULL;


-- ══════════════════════════════════════════════════════════════
-- 6. VIEW: Resumo de Importações
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_importacoes_resumo AS
SELECT
  i.id,
  i.codigo_importacao,
  i.tipo_fonte,
  i.arquivo_nome,
  i.tabela_destino,
  i.total_linhas,
  i.total_importados,
  i.total_duplicados,
  i.total_erros,
  i.total_avisos,
  i.status,
  i.usuario_nome,
  i.usuario_email,
  i.iniciado_em,
  i.concluido_em,
  ROUND(EXTRACT(EPOCH FROM (COALESCE(i.concluido_em, NOW()) - i.iniciado_em))::NUMERIC, 1) AS duracao_segundos,
  CASE
    WHEN i.total_linhas > 0
    THEN ROUND(i.total_importados::NUMERIC / i.total_linhas * 100, 1)
    ELSE 0
  END AS taxa_sucesso_pct,
  (SELECT COUNT(*) FROM cq_importacao_erros e WHERE e.importacao_id = i.id) AS total_erros_log
FROM cq_importacoes i
ORDER BY i.iniciado_em DESC;


-- ══════════════════════════════════════════════════════════════
-- 7. VERIFICAÇÃO
-- ══════════════════════════════════════════════════════════════

SELECT '── TABELAS CRIADAS ──' AS info;

SELECT 'cq_importacoes' AS tabela, COUNT(*) AS rows FROM cq_importacoes
UNION ALL
SELECT 'cq_importacao_erros', COUNT(*) FROM cq_importacao_erros;

SELECT '── COLUNAS DE RASTREABILIDADE ──' AS info;

SELECT
  table_name,
  column_name,
  column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND column_name IN ('origem_dado','importacao_id','arquivo_origem')
ORDER BY table_name, column_name;

SELECT '── CONSISTÊNCIA DE ORIGEM ──' AS info;

SELECT 'registros_cq_inspecao' AS tabela, COUNT(*) AS total,
  COUNT(*) FILTER (WHERE origem_dado IS NOT NULL) AS com_origem,
  COUNT(*) FILTER (WHERE origem_dado IS NULL) AS sem_origem
FROM registros_cq_inspecao
UNION ALL
SELECT 'cq_recebimentos', COUNT(*),
  COUNT(*) FILTER (WHERE origem_dado IS NOT NULL),
  COUNT(*) FILTER (WHERE origem_dado IS NULL)
FROM cq_recebimentos;
