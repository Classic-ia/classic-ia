-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 10: Merge Unificado — Estrutura Nova + Existente
--
-- ESTRATÉGIA:
--   Não cria tabelas paralelas. Evolui o que existe incorporando o melhor
--   do SQL novo (products, atak_movimentacoes, cq_inspecoes, staging).
--
--   O que foi INCORPORADO do SQL novo:
--     ✓ cq_produtos ganha id_atak, nome_original_atak, especie
--     ✓ product_aliases → cq_cadastro_alias (já existe, reutiliza)
--     ✓ atak_movimentacoes → tabela nova (Entradas + Saídas unificadas)
--     ✓ atak_movimentacoes_staging → tabela nova (dados brutos pré-validação)
--     ✓ cq_inspecao_defeitos → tabela nova (defeitos normalizados por inspeção)
--     ✓ Functions de normalização (trim, parse_numeric_br, parse_date_br)
--     ✓ fn_processar_importacao_atak (pipeline staging → final)
--     ✓ View consolidada ATAK + CQ (com score)
--
--   O que foi MANTIDO do existente:
--     ✓ registros_cq_inspecao (workflow completo, 6+ triggers, geoloc)
--     ✓ atak_cargas_raw (sync via API, diferente de planilha)
--     ✓ cq_tipos_defeito (11 tipos para inspeção individual)
--     ✓ cq_ranking_defeitos (9 tipos para ranking por lote)
--     ✓ Todas as validações (V1-V9), triggers, audit, guards
--
-- Rodar APÓS 09_migration_backup_recuperacao.sql
-- ══════════════════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS pgcrypto;

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. EVOLUÇÃO: cq_produtos — adicionar campos do SQL novo
-- ════════════════════════════════════════════════════════════════════════════
-- cq_produtos já existe com: id, codigo, nome, grupo, ativo, criado_em
-- Adicionamos: id_atak, nome_original_atak, especie, categoria, unidade_padrao

ALTER TABLE cq_produtos
  ADD COLUMN IF NOT EXISTS id_atak            TEXT UNIQUE,
  ADD COLUMN IF NOT EXISTS nome_original_atak TEXT,
  ADD COLUMN IF NOT EXISTS nome_padronizado   TEXT,
  ADD COLUMN IF NOT EXISTS categoria          TEXT,
  ADD COLUMN IF NOT EXISTS especie            TEXT,
  ADD COLUMN IF NOT EXISTS unidade_padrao     TEXT DEFAULT 'PÇ',
  ADD COLUMN IF NOT EXISTS origem             TEXT DEFAULT 'manual'
    CHECK (origem IN ('atak','manual','importacao'));

-- Backfill: nome_padronizado = nome existente, id_atak = codigo existente
UPDATE cq_produtos SET
  nome_padronizado = nome,
  id_atak = COALESCE(codigo_atak, codigo)
WHERE nome_padronizado IS NULL;

CREATE INDEX IF NOT EXISTS idx_produtos_id_atak ON cq_produtos(id_atak);
CREATE INDEX IF NOT EXISTS idx_produtos_nome_pad ON cq_produtos(nome_padronizado);

-- Aliases de produto via cq_cadastro_alias (migration 07, já existe)
-- Não cria product_aliases separada — reutiliza a infra genérica.
-- Uso: INSERT INTO cq_cadastro_alias (entidade, registro_id, alias_tipo, alias_valor)
--      VALUES ('produto', <product_uuid>, 'nome', 'OMASO BOV. CONG.');


-- ════════════════════════════════════════════════════════════════════════════
-- 2. NOVA TABELA: Movimentações ATAK (Entradas + Saídas de planilha)
-- Diferente de atak_cargas_raw (que vem da API).
-- Esta tabela recebe dados de planilha WRMVE001/WRMVS009.
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS atak_movimentacoes (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Rastreabilidade da importação
  import_log_id         UUID REFERENCES atak_sync_log(id) ON DELETE SET NULL,
  origem_arquivo        TEXT,
  origem_planilha       TEXT,            -- 'Entradas (WRMVE001)' / 'Saídas (WRMVS009)'
  numero_linha          INTEGER,

  -- Dados da planilha
  tipo_movimentacao     TEXT NOT NULL CHECK (tipo_movimentacao IN ('Entrada', 'Saída')),
  id_atak_produto       TEXT NOT NULL,
  documento             TEXT NOT NULL,
  participante_codigo   TEXT,
  participante_nome     TEXT NOT NULL,
  quantidade            NUMERIC(18,3) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  status_qualidade      TEXT,            -- A/B/C nas saídas; nulo nas entradas
  data_movimentacao     DATE NOT NULL,

  -- FKs resolvidos (trigger)
  produto_id            UUID REFERENCES cq_produtos(id),
  fornecedor_id         UUID REFERENCES cadastros_atak(id),

  -- Deduplicação
  row_hash              TEXT NOT NULL UNIQUE,

  -- Controle
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mov_documento ON atak_movimentacoes(documento);
CREATE INDEX IF NOT EXISTS idx_mov_id_atak ON atak_movimentacoes(id_atak_produto);
CREATE INDEX IF NOT EXISTS idx_mov_data ON atak_movimentacoes(data_movimentacao DESC);
CREATE INDEX IF NOT EXISTS idx_mov_tipo ON atak_movimentacoes(tipo_movimentacao);
CREATE INDEX IF NOT EXISTS idx_mov_participante ON atak_movimentacoes(participante_nome);
CREATE INDEX IF NOT EXISTS idx_mov_status ON atak_movimentacoes(status_qualidade);
CREATE INDEX IF NOT EXISTS idx_mov_produto_id ON atak_movimentacoes(produto_id);
CREATE INDEX IF NOT EXISTS idx_mov_fornecedor_id ON atak_movimentacoes(fornecedor_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 3. STAGING: Dados brutos antes de validação
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS atak_movimentacoes_staging (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  import_log_id         UUID REFERENCES atak_sync_log(id) ON DELETE SET NULL,

  -- Dados brutos como vieram do Excel
  tipo_raw              TEXT,
  id_atak_raw           TEXT,
  documento_raw         TEXT,
  participante_raw      TEXT,
  quantidade_raw        TEXT,
  status_raw            TEXT,
  data_raw              TEXT,

  -- Metadados
  origem_arquivo        TEXT,
  origem_planilha       TEXT,
  numero_linha          INTEGER,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staging_import ON atak_movimentacoes_staging(import_log_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 4. ERROS DE IMPORTAÇÃO (específicos para staging de movimentações)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS atak_import_erros (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  import_log_id         UUID REFERENCES atak_sync_log(id) ON DELETE CASCADE,
  staging_id            UUID REFERENCES atak_movimentacoes_staging(id) ON DELETE CASCADE,
  numero_linha          INTEGER,
  motivo                TEXT NOT NULL,
  dados_raw             JSONB,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_import_erros_log ON atak_import_erros(import_log_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 5. EVOLUÇÃO: Defeitos normalizados por inspeção
-- registros_cq_inspecao já tem 11 colunas fixas de defeito.
-- Adicionamos tabela relacional para extensibilidade (novos defeitos sem ALTER).
-- As 11 colunas existentes continuam funcionando (retrocompatível).
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_inspecao_defeitos (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inspecao_id           UUID NOT NULL REFERENCES registros_cq_inspecao(id) ON DELETE CASCADE,
  defeito_id            UUID NOT NULL REFERENCES cq_tipos_defeito(id) ON DELETE RESTRICT,
  quantidade            NUMERIC(18,3) NOT NULL DEFAULT 0 CHECK (quantidade >= 0),
  observacao            TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uq_inspecao_defeito UNIQUE (inspecao_id, defeito_id)
);

CREATE INDEX IF NOT EXISTS idx_insp_def_inspecao ON cq_inspecao_defeitos(inspecao_id);
CREATE INDEX IF NOT EXISTS idx_insp_def_defeito ON cq_inspecao_defeitos(defeito_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 6. FUNCTIONS DE NORMALIZAÇÃO (do SQL novo — úteis para importação)
-- ════════════════════════════════════════════════════════════════════════════

-- 6.1 Limpar texto
CREATE OR REPLACE FUNCTION fn_trim_text(p_text TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(REGEXP_REPLACE(TRIM(COALESCE(p_text, '')), '\s+', ' ', 'g'), '');
$$;

-- 6.2 Normalizar tipo de movimentação
CREATE OR REPLACE FUNCTION fn_normalizar_tipo_movimentacao(p_text TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v TEXT;
BEGIN
  v := UPPER(fn_trim_text(p_text));
  IF v IN ('ENTRADA', 'E') THEN RETURN 'Entrada';
  ELSIF v IN ('SAIDA', 'SAÍDA', 'S') THEN RETURN 'Saída';
  ELSE RETURN NULL;
  END IF;
END;
$$;

-- 6.3 Converter quantidade BR (1.234,56 → 1234.56)
CREATE OR REPLACE FUNCTION fn_parse_numeric_br(p_text TEXT)
RETURNS NUMERIC LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v TEXT;
BEGIN
  v := fn_trim_text(p_text);
  IF v IS NULL THEN RETURN NULL; END IF;
  v := REPLACE(v, '.', '');
  v := REPLACE(v, ',', '.');
  RETURN v::NUMERIC;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$$;

-- 6.4 Converter data BR (DD/MM/YYYY ou YYYY-MM-DD)
CREATE OR REPLACE FUNCTION fn_parse_date_br(p_text TEXT)
RETURNS DATE LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v TEXT;
BEGIN
  v := fn_trim_text(p_text);
  IF v IS NULL THEN RETURN NULL; END IF;
  BEGIN RETURN TO_DATE(v, 'DD/MM/YYYY'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN RETURN TO_DATE(v, 'YYYY-MM-DD'); EXCEPTION WHEN OTHERS THEN NULL; END;
  RETURN NULL;
END;
$$;

-- 6.5 Separar participante ("60118 CURTUME NOROESTE" → codigo + nome)
CREATE OR REPLACE FUNCTION fn_parse_participante(p_text TEXT)
RETURNS TABLE (participante_codigo TEXT, participante_nome TEXT)
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v TEXT;
BEGIN
  v := fn_trim_text(p_text);
  IF v IS NULL THEN
    participante_codigo := NULL; participante_nome := NULL;
    RETURN NEXT; RETURN;
  END IF;
  IF v ~ '^[0-9]+\s+' THEN
    participante_codigo := SPLIT_PART(v, ' ', 1);
    participante_nome := fn_trim_text(SUBSTR(v, LENGTH(participante_codigo) + 1));
  ELSE
    participante_codigo := NULL;
    participante_nome := v;
  END IF;
  RETURN NEXT;
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. TRIGGER: Resolver FKs em atak_movimentacoes
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_resolver_ids_movimentacao()
RETURNS TRIGGER AS $$
BEGIN
  -- Produto: id_atak_produto → cq_produtos.id_atak
  IF NEW.produto_id IS NULL AND NEW.id_atak_produto IS NOT NULL THEN
    SELECT id INTO NEW.produto_id
    FROM cq_produtos WHERE id_atak = NEW.id_atak_produto LIMIT 1;
    -- Fallback: tentar por codigo
    IF NEW.produto_id IS NULL THEN
      SELECT id INTO NEW.produto_id
      FROM cq_produtos WHERE codigo = NEW.id_atak_produto LIMIT 1;
    END IF;
  END IF;

  -- Fornecedor: participante_codigo → cadastros_atak.codigo
  IF NEW.fornecedor_id IS NULL AND NEW.participante_codigo IS NOT NULL THEN
    SELECT id INTO NEW.fornecedor_id
    FROM cadastros_atak
    WHERE codigo = NEW.participante_codigo AND tipo = 'fornecedor'
    LIMIT 1;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_resolver_ids_movimentacao
  BEFORE INSERT OR UPDATE ON atak_movimentacoes
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_movimentacao();

-- updated_at automático
CREATE TRIGGER trg_movimentacoes_updated_at
  BEFORE UPDATE ON atak_movimentacoes
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ════════════════════════════════════════════════════════════════════════════
-- 8. VIEW STAGING NORMALIZADA (pré-validação visual)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_atak_staging_normalizada AS
SELECT
  s.id,
  s.import_log_id,
  s.origem_arquivo,
  s.origem_planilha,
  s.numero_linha,

  fn_normalizar_tipo_movimentacao(s.tipo_raw) AS tipo_movimentacao,
  fn_trim_text(s.id_atak_raw) AS id_atak_produto,
  fn_trim_text(s.documento_raw) AS documento,

  p.participante_codigo,
  p.participante_nome,

  fn_parse_numeric_br(s.quantidade_raw) AS quantidade,
  fn_trim_text(s.status_raw) AS status_qualidade,
  fn_parse_date_br(s.data_raw) AS data_movimentacao,

  -- Dados brutos (para debug)
  s.tipo_raw, s.id_atak_raw, s.documento_raw,
  s.participante_raw, s.quantidade_raw, s.status_raw, s.data_raw

FROM atak_movimentacoes_staging s
LEFT JOIN LATERAL fn_parse_participante(s.participante_raw) p ON true;


-- ════════════════════════════════════════════════════════════════════════════
-- 9. FUNCTION: Pipeline de importação staging → final
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_processar_importacao_atak(p_import_log_id UUID)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
  v_total INTEGER := 0;
  v_importadas INTEGER := 0;
  v_rejeitadas INTEGER := 0;
  v_duplicadas INTEGER := 0;
BEGIN
  -- Limpar erros anteriores desse lote
  DELETE FROM atak_import_erros WHERE import_log_id = p_import_log_id;

  -- Contar staging
  SELECT COUNT(*) INTO v_total
  FROM atak_movimentacoes_staging WHERE import_log_id = p_import_log_id;

  -- 9.1 Registrar erros de validação
  INSERT INTO atak_import_erros (import_log_id, staging_id, numero_linha, motivo, dados_raw)
  SELECT
    n.import_log_id, n.id, n.numero_linha,
    CASE
      WHEN n.tipo_movimentacao IS NULL THEN 'Tipo de movimentação inválido'
      WHEN n.id_atak_produto IS NULL THEN 'ID ATAK do produto ausente'
      WHEN n.documento IS NULL THEN 'Documento ausente'
      WHEN n.participante_nome IS NULL THEN 'Participante ausente'
      WHEN n.quantidade IS NULL THEN 'Quantidade inválida'
      WHEN n.data_movimentacao IS NULL THEN 'Data inválida'
      ELSE 'Erro de validação'
    END,
    jsonb_build_object(
      'tipo_raw', n.tipo_raw, 'id_atak_raw', n.id_atak_raw,
      'documento_raw', n.documento_raw, 'participante_raw', n.participante_raw,
      'quantidade_raw', n.quantidade_raw, 'status_raw', n.status_raw,
      'data_raw', n.data_raw
    )
  FROM vw_atak_staging_normalizada n
  WHERE n.import_log_id = p_import_log_id
    AND (n.tipo_movimentacao IS NULL OR n.id_atak_produto IS NULL OR n.documento IS NULL
         OR n.participante_nome IS NULL OR n.quantidade IS NULL OR n.data_movimentacao IS NULL);

  -- 9.2 Inserir registros válidos (dedup por hash)
  WITH validos AS (
    SELECT n.*,
      ENCODE(DIGEST(
        COALESCE(n.tipo_movimentacao,'') || '|' || COALESCE(n.id_atak_produto,'') || '|' ||
        COALESCE(n.documento,'') || '|' || COALESCE(n.participante_codigo,'') || '|' ||
        COALESCE(n.participante_nome,'') || '|' || COALESCE(n.quantidade::TEXT,'') || '|' ||
        COALESCE(n.status_qualidade,'') || '|' || COALESCE(n.data_movimentacao::TEXT,''),
        'sha256'), 'hex') AS row_hash
    FROM vw_atak_staging_normalizada n
    WHERE n.import_log_id = p_import_log_id
      AND NOT EXISTS (SELECT 1 FROM atak_import_erros e WHERE e.staging_id = n.id)
  ),
  inseridos AS (
    INSERT INTO atak_movimentacoes (
      import_log_id, origem_arquivo, origem_planilha, numero_linha,
      tipo_movimentacao, id_atak_produto, documento,
      participante_codigo, participante_nome,
      quantidade, status_qualidade, data_movimentacao, row_hash
    )
    SELECT
      v.import_log_id, v.origem_arquivo, v.origem_planilha, v.numero_linha,
      v.tipo_movimentacao, v.id_atak_produto, v.documento,
      v.participante_codigo, v.participante_nome,
      v.quantidade, v.status_qualidade, v.data_movimentacao, v.row_hash
    FROM validos v
    ON CONFLICT (row_hash) DO NOTHING
    RETURNING id
  )
  SELECT COUNT(*) INTO v_importadas FROM inseridos;

  -- Contar duplicatas
  WITH validos AS (
    SELECT
      ENCODE(DIGEST(
        COALESCE(fn_normalizar_tipo_movimentacao(s.tipo_raw),'') || '|' ||
        COALESCE(fn_trim_text(s.id_atak_raw),'') || '|' ||
        COALESCE(fn_trim_text(s.documento_raw),'') || '|' ||
        COALESCE((fn_parse_participante(s.participante_raw)).participante_codigo,'') || '|' ||
        COALESCE((fn_parse_participante(s.participante_raw)).participante_nome,'') || '|' ||
        COALESCE(fn_parse_numeric_br(s.quantidade_raw)::TEXT,'') || '|' ||
        COALESCE(fn_trim_text(s.status_raw),'') || '|' ||
        COALESCE(fn_parse_date_br(s.data_raw)::TEXT,''),
        'sha256'), 'hex') AS row_hash
    FROM atak_movimentacoes_staging s
    WHERE s.import_log_id = p_import_log_id
      AND NOT EXISTS (SELECT 1 FROM atak_import_erros e WHERE e.staging_id = s.id)
  )
  SELECT COUNT(*) INTO v_duplicadas
  FROM validos v
  WHERE EXISTS (SELECT 1 FROM atak_movimentacoes m WHERE m.row_hash = v.row_hash);

  SELECT COUNT(*) INTO v_rejeitadas
  FROM atak_import_erros WHERE import_log_id = p_import_log_id;

  v_duplicadas := GREATEST(v_duplicadas - v_importadas, 0);

  -- Atualizar log
  UPDATE atak_sync_log SET
    total_registros = v_total,
    novos = v_importadas,
    erros = v_rejeitadas,
    ignorados = v_duplicadas,
    status = CASE WHEN v_rejeitadas > 0 THEN 'parcial' ELSE 'sucesso' END,
    mensagem = FORMAT('Planilha: %s linhas | importadas: %s | rejeitadas: %s | duplicadas: %s',
      v_total, v_importadas, v_rejeitadas, v_duplicadas),
    fim = NOW()
  WHERE id = p_import_log_id;

  -- Evento
  PERFORM fn_registrar_evento(
    'importacao_planilha', 'integracao',
    FORMAT('Importação concluída: %s importadas, %s rejeitadas, %s duplicadas',
      v_importadas, v_rejeitadas, v_duplicadas),
    'atak_movimentacoes', NULL, NULL,
    jsonb_build_object('total', v_total, 'importadas', v_importadas,
      'rejeitadas', v_rejeitadas, 'duplicadas', v_duplicadas),
    NULL,
    CASE WHEN v_rejeitadas > 0 THEN 'warn' ELSE 'info' END
  );

  RETURN jsonb_build_object(
    'import_log_id', p_import_log_id,
    'total_linhas', v_total,
    'linhas_importadas', v_importadas,
    'linhas_rejeitadas', v_rejeitadas,
    'linhas_duplicadas', v_duplicadas
  );
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 10. VIEW CONSOLIDADA: ATAK + CQ (com score de qualidade)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_atak_cq_consolidado AS
SELECT
  m.id,
  m.tipo_movimentacao,
  m.id_atak_produto,
  COALESCE(p.nome_padronizado, p.nome) AS produto,
  p.categoria AS produto_categoria,
  m.documento,
  m.participante_codigo,
  m.participante_nome,
  m.quantidade AS quantidade_erp,
  m.status_qualidade,
  m.data_movimentacao,

  -- Fornecedor resolvido
  m.fornecedor_id,
  COALESCE(ca.apelido, ca.nome) AS fornecedor_nome_cadastro,
  ca.uf AS fornecedor_uf,

  -- Dados CQ (se existir inspeção vinculada)
  i.id AS inspecao_id,
  i.id_inspecao,
  i.quantidade_analisada AS quantidade_inspecionada,
  i.classificacao,
  i.status_final,
  i.total_defeitos,
  i.percentual_defeitos,

  -- Dados CQ do lote de ranking (se existir)
  li.id AS lote_ranking_id,
  li.id_lote,
  li.pct_a, li.pct_b, li.pct_c,
  li.pct_defeitos AS pct_defeitos_lote,
  li.classificacao_lote,

  -- Score do fornecedor
  fs.score AS fornecedor_score,
  fs.status_fornecedor,

  -- Score de qualidade do lote (A*100 + B*60 + C*0) / total
  CASE
    WHEN li.total_pecas > 0 THEN
      ROUND((li.total_class_a * 100.0 + li.total_class_b * 60.0) / li.total_pecas, 2)
    WHEN i.quantidade_analisada > 0 THEN
      ROUND((1 - i.percentual_defeitos / 100.0) * 100, 2)
    ELSE NULL
  END AS score_qualidade

FROM atak_movimentacoes m
LEFT JOIN cq_produtos p ON p.id = m.produto_id
LEFT JOIN cadastros_atak ca ON ca.id = m.fornecedor_id
LEFT JOIN registros_cq_inspecao i
  ON i.fornecedor_codigo = m.participante_codigo
  AND i.lote_atak = m.documento
  AND i.produto = COALESCE(p.nome, m.id_atak_produto)
LEFT JOIN cq_lote_inspecao li
  ON li.fornecedor_codigo = m.participante_codigo
  AND li.data_inspecao = m.data_movimentacao
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_id = m.fornecedor_id;


-- ════════════════════════════════════════════════════════════════════════════
-- 11. VIEWS DE IMPORTAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_import_erros AS
SELECT
  e.id, e.import_log_id, e.numero_linha,
  e.motivo, e.dados_raw, e.created_at
FROM atak_import_erros e
ORDER BY e.created_at DESC, e.numero_linha;

CREATE OR REPLACE VIEW vw_import_resumo AS
SELECT
  l.id,
  l.endpoint_nome AS arquivo_nome,
  l.total_registros AS total_linhas,
  l.novos AS linhas_importadas,
  l.erros AS linhas_rejeitadas,
  l.ignorados AS linhas_duplicadas,
  l.status,
  l.mensagem,
  l.inicio AS created_at,
  l.fim,
  ROUND(EXTRACT(EPOCH FROM (l.fim - l.inicio))::NUMERIC, 1) AS duracao_seg
FROM atak_sync_log l
WHERE l.endpoint_nome LIKE 'PLANILHA_%' OR l.endpoint_nome LIKE 'planilha_%'
ORDER BY l.inicio DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 12. TRIGGER: Sync defeitos colunas ↔ tabela relacional
-- Quando inspeção é salva, popula cq_inspecao_defeitos automaticamente
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_sync_defeitos_inspecao()
RETURNS TRIGGER AS $$
DECLARE
  v_defeito RECORD;
  v_qtd INTEGER;
  v_col TEXT;
BEGIN
  -- Mapear colunas fixas → cq_tipos_defeito
  FOR v_defeito IN
    SELECT id, codigo FROM cq_tipos_defeito WHERE ativo = true
  LOOP
    v_col := v_defeito.codigo;

    -- Ler quantidade da coluna correspondente
    EXECUTE format('SELECT ($1).%I', v_col) INTO v_qtd USING NEW;

    IF v_qtd IS NOT NULL AND v_qtd > 0 THEN
      INSERT INTO cq_inspecao_defeitos (inspecao_id, defeito_id, quantidade)
      VALUES (NEW.id, v_defeito.id, v_qtd)
      ON CONFLICT (inspecao_id, defeito_id)
      DO UPDATE SET quantidade = EXCLUDED.quantidade;
    ELSIF v_qtd = 0 OR v_qtd IS NULL THEN
      -- Registrar mesmo zero (regra: registrar todos os defeitos)
      INSERT INTO cq_inspecao_defeitos (inspecao_id, defeito_id, quantidade)
      VALUES (NEW.id, v_defeito.id, 0)
      ON CONFLICT (inspecao_id, defeito_id)
      DO UPDATE SET quantidade = EXCLUDED.quantidade;
    END IF;
  END LOOP;

  RETURN NEW;
EXCEPTION WHEN OTHERS THEN
  -- Não bloquear a inspeção se o sync falhar
  RAISE WARNING 'fn_sync_defeitos_inspecao falhou: %', SQLERRM;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_defeitos ON registros_cq_inspecao;
CREATE TRIGGER trg_sync_defeitos
  AFTER INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_sync_defeitos_inspecao();


-- ════════════════════════════════════════════════════════════════════════════
-- 13. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE atak_movimentacoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_movimentacoes_staging ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_import_erros ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_inspecao_defeitos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_movimentacoes" ON atak_movimentacoes FOR SELECT USING (true);
CREATE POLICY "all_movimentacoes" ON atak_movimentacoes FOR ALL USING (true);
CREATE POLICY "read_staging" ON atak_movimentacoes_staging FOR SELECT USING (true);
CREATE POLICY "all_staging" ON atak_movimentacoes_staging FOR ALL USING (true);
CREATE POLICY "read_import_erros" ON atak_import_erros FOR SELECT USING (true);
CREATE POLICY "all_import_erros" ON atak_import_erros FOR ALL USING (true);
CREATE POLICY "read_insp_defeitos" ON cq_inspecao_defeitos FOR SELECT USING (true);
CREATE POLICY "all_insp_defeitos" ON cq_inspecao_defeitos FOR ALL USING (true);


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 10 CONCLUÍDA — Merge Unificado';
  RAISE NOTICE '  ';
  RAISE NOTICE '  EVOLUÇÃO de cq_produtos:';
  RAISE NOTICE '    ✓ id_atak, nome_original_atak, nome_padronizado';
  RAISE NOTICE '    ✓ categoria, especie, unidade_padrao, origem';
  RAISE NOTICE '  ';
  RAISE NOTICE '  TABELAS NOVAS:';
  RAISE NOTICE '    ✓ atak_movimentacoes (entradas + saídas de planilha)';
  RAISE NOTICE '    ✓ atak_movimentacoes_staging (dados brutos Excel)';
  RAISE NOTICE '    ✓ atak_import_erros (falhas detalhadas)';
  RAISE NOTICE '    ✓ cq_inspecao_defeitos (defeitos normalizados)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  FUNCTIONS:';
  RAISE NOTICE '    ✓ fn_trim_text, fn_normalizar_tipo_movimentacao';
  RAISE NOTICE '    ✓ fn_parse_numeric_br, fn_parse_date_br';
  RAISE NOTICE '    ✓ fn_parse_participante (separa código + nome)';
  RAISE NOTICE '    ✓ fn_processar_importacao_atak (pipeline completo)';
  RAISE NOTICE '    ✓ fn_sync_defeitos_inspecao (colunas ↔ relacional)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS:';
  RAISE NOTICE '    ✓ vw_atak_staging_normalizada';
  RAISE NOTICE '    ✓ vw_atak_cq_consolidado (ATAK + CQ + score)';
  RAISE NOTICE '    ✓ vw_import_erros, vw_import_resumo';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 11: Views do Dashboard Decisional
--
-- Views otimizadas para tomada de decisão rápida.
-- Fontes: vw_atak_cq_consolidado (primária) + ranking views (fallback)
--
-- Rodar APÓS 10_migration_merge_estrutura.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. vw_dash_alertas — Alertas imediatos (topo do dashboard)
-- Retorna APENAS itens que exigem ação. Ordenados por gravidade.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_alertas AS

-- 1A: Fornecedores com queda de score > 5% nos últimos 30 dias
SELECT
  'fornecedor_piorando' AS tipo_alerta,
  'critico' AS gravidade,
  t.fornecedor_codigo AS codigo,
  t.fornecedor_nome AS nome,
  FORMAT('Score caiu %s pts em 30 dias (de %s para %s)',
    ABS(ROUND(t.variacao_score::NUMERIC, 1)),
    ROUND((t.score_atual + ABS(t.variacao_score))::NUMERIC, 1),
    ROUND(t.score_atual::NUMERIC, 1)
  ) AS descricao,
  t.score_atual AS valor,
  t.variacao_score AS variacao,
  t.tendencia,
  NOW() AS detectado_em
FROM vw_ranking_tendencia_30d t
WHERE t.variacao_score < -5

UNION ALL

-- 1B: Produtos com %C acima de 10%
SELECT
  'produto_critico' AS tipo_alerta,
  CASE WHEN sub.pct_c > 20 THEN 'critico' ELSE 'atencao' END AS gravidade,
  sub.id_atak_produto AS codigo,
  sub.produto AS nome,
  FORMAT('%s%% de Classe C (%s de %s peças)', ROUND(sub.pct_c, 1), sub.qtd_c, sub.total) AS descricao,
  sub.pct_c AS valor,
  NULL::NUMERIC AS variacao,
  NULL AS tendencia,
  NOW() AS detectado_em
FROM (
  SELECT
    c.id_atak_produto,
    COALESCE(c.produto, c.id_atak_produto) AS produto,
    COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') AS qtd_c,
    COUNT(*) AS total,
    ROUND(COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_c
  FROM vw_atak_cq_consolidado c
  WHERE c.data_movimentacao >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY c.id_atak_produto, c.produto
  HAVING COUNT(*) >= 3
) sub
WHERE sub.pct_c > 10

UNION ALL

-- 1C: Alertas do ranking (defeito>5%, fornecedor degradando, etc)
SELECT
  a.tipo_alerta,
  CASE
    WHEN a.tipo_alerta IN ('defeito_recorrente', 'degradacao_fornecedor') THEN 'critico'
    ELSE 'atencao'
  END AS gravidade,
  a.fornecedor_codigo AS codigo,
  a.fornecedor_nome AS nome,
  a.descricao,
  a.valor_referencia AS valor,
  NULL::NUMERIC AS variacao,
  NULL AS tendencia,
  a.criado_em AS detectado_em
FROM vw_ranking_alertas a
WHERE a.criado_em >= CURRENT_DATE - INTERVAL '7 days'

ORDER BY
  CASE gravidade WHEN 'critico' THEN 1 WHEN 'atencao' THEN 2 ELSE 3 END,
  valor DESC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. vw_dash_ranking_fornecedores — Ranking pior → melhor
-- Combina dados do consolidado + ranking existente + tendência
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_ranking_fornecedores AS
SELECT
  r.fornecedor_codigo,
  r.fornecedor_nome,
  r.total_lotes AS volume_total,
  r.score AS score_qualidade,
  r.pct_a,
  r.pct_b,
  r.pct_c,
  r.pct_defeitos,
  r.status_fornecedor,

  -- Tendência 30 dias
  COALESCE(t.variacao_score, 0) AS variacao_30d,
  CASE
    WHEN COALESCE(t.variacao_score, 0) > 3 THEN 'melhorando'
    WHEN COALESCE(t.variacao_score, 0) < -3 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,

  -- Score ponderado por volume (A*100 + B*60 + C*0) / total
  CASE
    WHEN r.total_pecas > 0 THEN
      ROUND((r.total_class_a * 100.0 + r.total_class_b * 60.0) / r.total_pecas, 2)
    ELSE r.score
  END AS score_ponderado

FROM vw_ranking_geral_fornecedor r
LEFT JOIN vw_ranking_tendencia_30d t
  ON t.fornecedor_codigo = r.fornecedor_codigo

ORDER BY score_qualidade ASC, volume_total DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. vw_dash_qualidade_produto — Qualidade por produto (pior → melhor)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_qualidade_produto AS
SELECT
  sub.produto,
  sub.id_atak_produto,
  sub.volume,
  sub.score,
  sub.pct_c,
  sub.pct_defeitos,
  sub.principal_defeito
FROM (
  -- Fonte 1: consolidado (movimentações + inspeções)
  SELECT
    COALESCE(c.produto, c.id_atak_produto) AS produto,
    c.id_atak_produto,
    SUM(c.quantidade_erp) AS volume,
    ROUND(AVG(c.score_qualidade), 2) AS score,
    ROUND(COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_c,
    ROUND(AVG(c.percentual_defeitos), 2) AS pct_defeitos,
    NULL::TEXT AS principal_defeito
  FROM vw_atak_cq_consolidado c
  WHERE c.data_movimentacao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY c.produto, c.id_atak_produto
  HAVING COUNT(*) >= 2

  UNION ALL

  -- Fonte 2: ranking por produto (para produtos sem movimentação)
  SELECT
    rp.produto_nome AS produto,
    NULL AS id_atak_produto,
    rp.total_pecas AS volume,
    rp.score,
    rp.pct_c,
    rp.pct_defeitos,
    rp.principal_defeito
  FROM vw_ranking_fornecedor_produto rp
  WHERE NOT EXISTS (
    SELECT 1 FROM vw_atak_cq_consolidado c
    WHERE COALESCE(c.produto, c.id_atak_produto) = rp.produto_nome
      AND c.data_movimentacao >= CURRENT_DATE - INTERVAL '90 days'
  )
) sub
WHERE sub.volume > 0
ORDER BY sub.pct_c DESC NULLS LAST, sub.score ASC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. vw_dash_defeitos — Análise de defeitos (ranking + por fornecedor/produto)
-- ════════════════════════════════════════════════════════════════════════════

-- 4A: Ranking geral de defeitos
CREATE OR REPLACE VIEW vw_dash_defeitos_ranking AS
SELECT
  d.defeito_codigo,
  d.defeito_nome,
  SUM(d.quantidade_defeito) AS total_ocorrencias,
  SUM(d.total_analisado) AS total_analisado,
  ROUND(SUM(d.quantidade_defeito) * 100.0 / NULLIF(SUM(d.total_analisado), 0), 2) AS pct_sobre_total,
  COUNT(DISTINCT d.fornecedor_codigo) AS qtd_fornecedores_afetados
FROM vw_ranking_defeitos_fornecedor d
GROUP BY d.defeito_codigo, d.defeito_nome
ORDER BY total_ocorrencias DESC;

-- 4B: Defeitos por fornecedor (top ofensores)
CREATE OR REPLACE VIEW vw_dash_defeitos_fornecedor AS
SELECT
  d.fornecedor_codigo,
  d.fornecedor_nome,
  d.defeito_codigo,
  d.defeito_nome,
  d.quantidade_defeito,
  d.total_analisado,
  d.pct_defeito,
  d.alerta_defeito
FROM vw_ranking_defeitos_fornecedor d
WHERE d.pct_defeito > 2
ORDER BY d.pct_defeito DESC;

-- 4C: Defeitos por produto
CREATE OR REPLACE VIEW vw_dash_defeitos_produto AS
SELECT
  sub.produto,
  sub.defeito,
  sub.total_defeito,
  sub.total_analisado,
  sub.pct_defeito
FROM (
  SELECT
    i.produto,
    'fermentacao' AS defeito, SUM(i.fermentacao) AS total_defeito,
    SUM(i.quantidade_analisada) AS total_analisado,
    ROUND(SUM(i.fermentacao) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2) AS pct_defeito
  FROM registros_cq_inspecao i
  WHERE i.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'sem_folhas', SUM(i.sem_folhas), SUM(i.quantidade_analisada),
    ROUND(SUM(i.sem_folhas) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'folhas_arrebentadas', SUM(i.folhas_arrebentadas), SUM(i.quantidade_analisada),
    ROUND(SUM(i.folhas_arrebentadas) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'pedaco', SUM(i.pedaco), SUM(i.quantidade_analisada),
    ROUND(SUM(i.pedaco) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'sujo', SUM(i.sujo), SUM(i.quantidade_analisada),
    ROUND(SUM(i.sujo) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'rasgado', SUM(i.rasgado), SUM(i.quantidade_analisada),
    ROUND(SUM(i.rasgado) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'corte_irregular', SUM(i.corte_irregular), SUM(i.quantidade_analisada),
    ROUND(SUM(i.corte_irregular) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'furado', SUM(i.furado), SUM(i.quantidade_analisada),
    ROUND(SUM(i.furado) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'mau_cheiro', SUM(i.mau_cheiro), SUM(i.quantidade_analisada),
    ROUND(SUM(i.mau_cheiro) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'contaminacao', SUM(i.contaminacao), SUM(i.quantidade_analisada),
    ROUND(SUM(i.contaminacao) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
) sub
WHERE sub.total_defeito > 0
ORDER BY sub.pct_defeito DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. vw_dash_pipeline — Status operacional em tempo real
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_pipeline AS
SELECT
  'cargas_pendentes' AS etapa,
  1 AS ordem,
  COUNT(*) AS quantidade,
  NULL::TEXT AS detalhe
FROM atak_cargas_raw
WHERE processado = false

UNION ALL
SELECT
  'inspecoes_andamento' AS etapa,
  2 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NULL
  AND criado_em >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL
SELECT
  'inspecoes_concluidas_hoje' AS etapa,
  3 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NOT NULL
  AND data_inspecao = CURRENT_DATE

UNION ALL
SELECT
  'inspecoes_concluidas_semana' AS etapa,
  4 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NOT NULL
  AND data_inspecao >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL
SELECT
  'ncs_abertas' AS etapa,
  5 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM cq_nao_conformidades
WHERE status IN ('aberta', 'em_tratamento')

UNION ALL
SELECT
  'divergencias_abertas' AS etapa,
  6 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM atak_divergencias
WHERE resolvido = false

ORDER BY ordem;


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 11 — Views Dashboard Decisional';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS CRIADAS:';
  RAISE NOTICE '    ✓ vw_dash_alertas (alertas imediatos, ordenados por gravidade)';
  RAISE NOTICE '    ✓ vw_dash_ranking_fornecedores (pior → melhor, com tendência)';
  RAISE NOTICE '    ✓ vw_dash_qualidade_produto (pior → melhor, com defeito)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_ranking (ranking geral de defeitos)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_fornecedor (defeitos por fornecedor)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_produto (defeitos por produto)';
  RAISE NOTICE '    ✓ vw_dash_pipeline (status operacional real-time)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 12: Views de Qualidade por 4 Dimensões
--
-- Dimensões:
--   1. Qualidade por Fornecedor (detalhada, com evolução temporal)
--   2. Qualidade por Produto (com defeito principal e tendência)
--   3. Qualidade por Lote (individual, com detalhe por produto)
--   4. Qualidade por Setor (produção + funcionários + turno)
--
-- Rodar APÓS 11_migration_dashboard_decisao.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. QUALIDADE POR FORNECEDOR (detalhada)
-- Diferente do ranking (que é resumo): aqui mostra evolução lote a lote
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_fornecedor AS
SELECT
  f.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome, f.fornecedor_nome) AS fornecedor_nome,
  ca.uf AS fornecedor_uf,
  ca.cnpj AS fornecedor_cnpj,

  -- Volume
  COUNT(DISTINCT f.id_lote) AS total_lotes,
  SUM(f.total_pecas) AS total_pecas,

  -- ABC
  SUM(f.total_class_a) AS total_a,
  SUM(f.total_class_b) AS total_b,
  SUM(f.total_class_c) AS total_c,
  ROUND(SUM(f.total_class_a) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_a,
  ROUND(SUM(f.total_class_b) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_b,
  ROUND(SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_c,

  -- Defeitos
  SUM(f.total_defeitos) AS total_defeitos,
  ROUND(SUM(f.total_defeitos) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_defeitos,

  -- Score ponderado por volume
  ROUND(
    (SUM(f.total_class_a) * 100.0 + SUM(f.total_class_b) * 60.0)
    / NULLIF(SUM(f.total_pecas), 0), 2
  ) AS score,

  -- Classificação dominante
  CASE
    WHEN SUM(f.total_class_a) * 100.0 / NULLIF(SUM(f.total_pecas), 0) >= 80 THEN 'A'
    WHEN SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0) > 15 THEN 'C'
    ELSE 'B'
  END AS classificacao_geral,

  -- Tendência: comparar últimos 15 dias vs 15 dias anteriores
  ROUND(
    (SUM(CASE WHEN f.data_inspecao >= CURRENT_DATE - 15 THEN f.total_class_a * 100.0 + f.total_class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN f.data_inspecao >= CURRENT_DATE - 15 THEN f.total_pecas ELSE 0 END), 0))
    -
    (SUM(CASE WHEN f.data_inspecao < CURRENT_DATE - 15 AND f.data_inspecao >= CURRENT_DATE - 30 THEN f.total_class_a * 100.0 + f.total_class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN f.data_inspecao < CURRENT_DATE - 15 AND f.data_inspecao >= CURRENT_DATE - 30 THEN f.total_pecas ELSE 0 END), 0))
  , 2) AS variacao_30d,

  -- Status do fornecedor
  COALESCE(fs.status_fornecedor, 'normal') AS status_fornecedor,
  fs.score AS score_sistema,

  -- Datas
  MIN(f.data_inspecao) AS primeira_inspecao,
  MAX(f.data_inspecao) AS ultima_inspecao,
  COUNT(DISTINCT f.data_inspecao) AS dias_com_inspecao

FROM cq_lote_inspecao f
LEFT JOIN cadastros_atak ca ON ca.codigo = f.fornecedor_codigo AND ca.tipo = 'fornecedor'
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_codigo = f.fornecedor_codigo
WHERE f.status = 'finalizado'
  AND f.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY f.fornecedor_codigo, ca.apelido, ca.nome, f.fornecedor_nome,
         ca.uf, ca.cnpj, fs.status_fornecedor, fs.score
ORDER BY score ASC NULLS LAST;


-- Evolução temporal do fornecedor (para gráfico sparkline)
CREATE OR REPLACE VIEW vw_qualidade_fornecedor_evolucao AS
SELECT
  f.fornecedor_codigo,
  f.data_inspecao,
  COUNT(*) AS lotes_dia,
  SUM(f.total_pecas) AS pecas_dia,
  ROUND(
    (SUM(f.total_class_a) * 100.0 + SUM(f.total_class_b) * 60.0)
    / NULLIF(SUM(f.total_pecas), 0), 2
  ) AS score_dia,
  ROUND(SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_c_dia
FROM cq_lote_inspecao f
WHERE f.status = 'finalizado'
  AND f.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY f.fornecedor_codigo, f.data_inspecao
ORDER BY f.fornecedor_codigo, f.data_inspecao;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. QUALIDADE POR PRODUTO (com defeito principal e tendência)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_produto AS
SELECT
  lp.produto_codigo,
  COALESCE(p.nome, rp.nome, lp.produto_codigo) AS produto_nome,
  COALESCE(p.grupo, rp.grupo) AS produto_grupo,

  -- Volume
  COUNT(DISTINCT lp.lote_id) AS total_lotes,
  SUM(lp.total_pecas) AS total_pecas,

  -- ABC
  SUM(lp.class_a) AS total_a,
  SUM(lp.class_b) AS total_b,
  SUM(lp.class_c) AS total_c,
  ROUND(SUM(lp.class_a) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_a,
  ROUND(SUM(lp.class_b) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_b,
  ROUND(SUM(lp.class_c) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_c,

  -- Defeitos
  SUM(lp.total_defeitos) AS total_defeitos,
  ROUND(SUM(lp.total_defeitos) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_defeitos,

  -- Score
  ROUND(
    (SUM(lp.class_a) * 100.0 + SUM(lp.class_b) * 60.0)
    / NULLIF(SUM(lp.total_pecas), 0), 2
  ) AS score,

  -- Defeitos desagregados (totais)
  SUM(lp.rasgado) AS def_rasgado,
  SUM(lp.furado) AS def_furado,
  SUM(lp.pelando) AS def_pelando,
  SUM(lp.folha_estourada) AS def_folha_estourada,
  SUM(lp.despojo_sujo) AS def_despojo_sujo,
  SUM(lp.sebo_corte_incorreto) AS def_sebo_corte,
  SUM(lp.carne_sem_folha) AS def_carne_sem_folha,
  SUM(lp.agua) AS def_agua,
  SUM(lp.orelha_misturada) AS def_orelha_misturada,

  -- Principal defeito (o que mais ocorre)
  CASE GREATEST(
    SUM(lp.rasgado), SUM(lp.furado), SUM(lp.pelando),
    SUM(lp.folha_estourada), SUM(lp.despojo_sujo),
    SUM(lp.sebo_corte_incorreto), SUM(lp.carne_sem_folha),
    SUM(lp.agua), SUM(lp.orelha_misturada)
  )
    WHEN SUM(lp.rasgado) THEN 'Rasgado'
    WHEN SUM(lp.furado) THEN 'Furado'
    WHEN SUM(lp.pelando) THEN 'Pelando'
    WHEN SUM(lp.folha_estourada) THEN 'Folha Estourada'
    WHEN SUM(lp.despojo_sujo) THEN 'Despojo/Sujo'
    WHEN SUM(lp.sebo_corte_incorreto) THEN 'Sebo/Corte Incorreto'
    WHEN SUM(lp.carne_sem_folha) THEN 'Carne sem Folha'
    WHEN SUM(lp.agua) THEN 'Agua'
    WHEN SUM(lp.orelha_misturada) THEN 'Orelha Misturada'
    ELSE NULL
  END AS principal_defeito,

  -- Tendência 30d
  ROUND(
    (SUM(CASE WHEN li.data_inspecao >= CURRENT_DATE - 15 THEN lp.class_a * 100.0 + lp.class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN li.data_inspecao >= CURRENT_DATE - 15 THEN lp.total_pecas ELSE 0 END), 0))
    -
    (SUM(CASE WHEN li.data_inspecao < CURRENT_DATE - 15 AND li.data_inspecao >= CURRENT_DATE - 30 THEN lp.class_a * 100.0 + lp.class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN li.data_inspecao < CURRENT_DATE - 15 AND li.data_inspecao >= CURRENT_DATE - 30 THEN lp.total_pecas ELSE 0 END), 0))
  , 2) AS variacao_30d,

  -- Fornecedores que entregam este produto
  COUNT(DISTINCT li.fornecedor_codigo) AS qtd_fornecedores

FROM cq_lote_produto lp
JOIN cq_lote_inspecao li ON li.id = lp.lote_id AND li.status = 'finalizado'
LEFT JOIN cq_produtos p ON p.codigo = lp.produto_codigo
LEFT JOIN cq_ranking_produtos rp ON rp.codigo = lp.produto_codigo
WHERE li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY lp.produto_codigo, p.nome, rp.nome, p.grupo, rp.grupo
ORDER BY score ASC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. QUALIDADE POR LOTE (individual — detalhe completo)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_lote AS
SELECT
  li.id,
  li.id_lote,
  li.data_inspecao,
  li.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome, li.fornecedor_nome) AS fornecedor_nome,
  li.inspetor_nome,

  -- Totais
  li.total_pecas,
  li.total_class_a,
  li.total_class_b,
  li.total_class_c,
  li.pct_a,
  li.pct_b,
  li.pct_c,
  li.total_defeitos,
  li.pct_defeitos,
  li.classificacao_lote,

  -- Score
  CASE WHEN li.total_pecas > 0 THEN
    ROUND((li.total_class_a * 100.0 + li.total_class_b * 60.0) / li.total_pecas, 2)
  ELSE 0 END AS score,

  -- Status e observações
  li.status,
  li.observacoes,

  -- Produtos no lote (JSON array)
  (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'produto', COALESCE(p2.nome, rp2.nome, lp2.produto_codigo),
      'class_a', lp2.class_a,
      'class_b', lp2.class_b,
      'class_c', lp2.class_c,
      'total', lp2.total_pecas,
      'pct_c', lp2.pct_c,
      'principal_defeito', lp2.principal_defeito,
      'total_defeitos', lp2.total_defeitos
    ) ORDER BY lp2.total_pecas DESC), '[]'::jsonb)
    FROM cq_lote_produto lp2
    LEFT JOIN cq_produtos p2 ON p2.codigo = lp2.produto_codigo
    LEFT JOIN cq_ranking_produtos rp2 ON rp2.codigo = lp2.produto_codigo
    WHERE lp2.lote_id = li.id
  ) AS produtos,

  -- Quantidade de produtos no lote
  (SELECT COUNT(*) FROM cq_lote_produto lp3 WHERE lp3.lote_id = li.id) AS qtd_produtos,

  -- Geolocalização
  li.geo_dentro_planta,
  li.criado_em

FROM cq_lote_inspecao li
LEFT JOIN cadastros_atak ca ON ca.codigo = li.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY li.data_inspecao DESC, li.criado_em DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. QUALIDADE POR SETOR
-- Cruza: atak_producao_raw (setor + turno) com inspeções + cadastros
-- ════════════════════════════════════════════════════════════════════════════

-- 4A: Produção vs Qualidade por setor
CREATE OR REPLACE VIEW vw_qualidade_setor AS
SELECT
  sub.setor,
  sub.total_produzido,
  sub.total_aprovado,
  sub.total_rejeitado,
  sub.pct_aprovacao,
  sub.pct_rejeicao,

  -- Score do setor (aprovado/total * 100)
  ROUND(sub.total_aprovado * 100.0 / NULLIF(sub.total_produzido, 0), 2) AS score,

  -- Tendência
  sub.variacao_30d,
  CASE
    WHEN sub.variacao_30d > 3 THEN 'melhorando'
    WHEN sub.variacao_30d < -3 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,

  -- Turnos
  sub.turnos,
  sub.responsaveis,
  sub.dias_producao

FROM (
  SELECT
    pr.setor,
    SUM(pr.qtd_produzida) AS total_produzido,
    SUM(pr.qtd_aprovada) AS total_aprovado,
    SUM(pr.qtd_rejeitada) AS total_rejeitado,
    ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_aprovacao,
    ROUND(SUM(pr.qtd_rejeitada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_rejeicao,

    -- Tendência: últimos 15d vs 15d anteriores
    ROUND(
      (SUM(CASE WHEN pr.data_producao >= CURRENT_DATE - 15 THEN pr.qtd_aprovada ELSE 0 END) * 100.0
       / NULLIF(SUM(CASE WHEN pr.data_producao >= CURRENT_DATE - 15 THEN pr.qtd_produzida ELSE 0 END), 0))
      -
      (SUM(CASE WHEN pr.data_producao < CURRENT_DATE - 15 AND pr.data_producao >= CURRENT_DATE - 30 THEN pr.qtd_aprovada ELSE 0 END) * 100.0
       / NULLIF(SUM(CASE WHEN pr.data_producao < CURRENT_DATE - 15 AND pr.data_producao >= CURRENT_DATE - 30 THEN pr.qtd_produzida ELSE 0 END), 0))
    , 2) AS variacao_30d,

    -- Turnos ativos
    ARRAY_AGG(DISTINCT pr.turno) FILTER (WHERE pr.turno IS NOT NULL) AS turnos,
    ARRAY_AGG(DISTINCT pr.responsavel) FILTER (WHERE pr.responsavel IS NOT NULL) AS responsaveis,
    COUNT(DISTINCT pr.data_producao) AS dias_producao

  FROM atak_producao_raw pr
  WHERE pr.data_producao >= CURRENT_DATE - INTERVAL '90 days'
    AND pr.setor IS NOT NULL
  GROUP BY pr.setor
) sub
WHERE sub.total_produzido > 0
ORDER BY score ASC NULLS LAST;


-- 4B: Qualidade por setor + turno (granular)
CREATE OR REPLACE VIEW vw_qualidade_setor_turno AS
SELECT
  pr.setor,
  pr.turno,
  SUM(pr.qtd_produzida) AS total_produzido,
  SUM(pr.qtd_aprovada) AS total_aprovado,
  SUM(pr.qtd_rejeitada) AS total_rejeitado,
  ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_aprovacao,
  ROUND(SUM(pr.qtd_rejeitada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_rejeicao,
  ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS score,
  COUNT(DISTINCT pr.data_producao) AS dias_producao,
  ARRAY_AGG(DISTINCT pr.responsavel) FILTER (WHERE pr.responsavel IS NOT NULL) AS responsaveis
FROM atak_producao_raw pr
WHERE pr.data_producao >= CURRENT_DATE - INTERVAL '90 days'
  AND pr.setor IS NOT NULL
GROUP BY pr.setor, pr.turno
ORDER BY score ASC NULLS LAST;


-- 4C: Funcionários por setor com produção vinculada
CREATE OR REPLACE VIEW vw_qualidade_setor_equipe AS
SELECT
  ca.setor,
  ca.cargo,
  COUNT(*) AS total_funcionarios,
  COUNT(*) FILTER (WHERE ca.ativo = true) AS ativos,
  ARRAY_AGG(ca.nome ORDER BY ca.nome) FILTER (WHERE ca.ativo = true) AS nomes
FROM cadastros_atak ca
WHERE ca.tipo = 'funcionario'
  AND ca.setor IS NOT NULL
GROUP BY ca.setor, ca.cargo
ORDER BY ca.setor, ca.cargo;


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 12 — Views de Qualidade por 4 Dimensões';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR FORNECEDOR:';
  RAISE NOTICE '    ✓ vw_qualidade_fornecedor (resumo com score + tendência)';
  RAISE NOTICE '    ✓ vw_qualidade_fornecedor_evolucao (dia a dia)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR PRODUTO:';
  RAISE NOTICE '    ✓ vw_qualidade_produto (com 9 defeitos + principal)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR LOTE:';
  RAISE NOTICE '    ✓ vw_qualidade_lote (individual + produtos JSON)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR SETOR:';
  RAISE NOTICE '    ✓ vw_qualidade_setor (produção vs qualidade)';
  RAISE NOTICE '    ✓ vw_qualidade_setor_turno (granular por turno)';
  RAISE NOTICE '    ✓ vw_qualidade_setor_equipe (funcionários)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 13: Motor de Decisão Automática + IQF + Reamostragem
--
-- EVOLUÇÃO INCREMENTAL — não recria tabelas existentes.
--
-- O que ADICIONA:
--   1. Severidade nos defeitos (mapeia gravidade existente → CRITICO/MAIOR/MENOR)
--   2. Motor de decisão automática por inspeção (substitui regra simples)
--   3. Tabela de reamostragem (vínculo inspeção original → reinspeção)
--   4. IQF por lote (score baseado em severidade de defeitos)
--   5. IQF por fornecedor (média ponderada dos lotes recentes)
--   6. Status em_analise_critica no workflow
--   7. Tabela cq_nao_conformidades (que era referenciada mas não existia)
--   8. Views de dashboard de fornecedores com IQF
--   9. Alertas automáticos por regra de negócio
--
-- O que ALTERA:
--   - fn_auto_calcular_inspecao → incorpora severidade
--   - cq_fornecedor_score → usa IQF em vez de fórmula simples
--
-- O que MANTÉM:
--   - Todas as tabelas existentes
--   - Todos os triggers de guard
--   - Workflow 8 etapas
--   - cq_lote_inspecao + cq_lote_produto
--
-- Rodar APÓS 12_migration_qualidade_4_dimensoes.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. EVOLUÇÃO: cq_tipos_defeito — mapear gravidade → severidade
-- gravidade existente: baixa, media, alta, critica
-- severidade motor:    MENOR, MAIOR, CRITICO
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_tipos_defeito
  ADD COLUMN IF NOT EXISTS severidade TEXT
    CHECK (severidade IN ('CRITICO','MAIOR','MENOR'));

-- Mapear gravidade existente → severidade
UPDATE cq_tipos_defeito SET severidade = CASE
  WHEN gravidade = 'critica' THEN 'CRITICO'
  WHEN gravidade = 'alta'    THEN 'MAIOR'
  WHEN gravidade = 'media'   THEN 'MAIOR'
  WHEN gravidade = 'baixa'   THEN 'MENOR'
  ELSE 'MENOR'
END WHERE severidade IS NULL;

-- Tornar obrigatório para novos registros
ALTER TABLE cq_tipos_defeito ALTER COLUMN severidade SET DEFAULT 'MENOR';


-- ════════════════════════════════════════════════════════════════════════════
-- 2. EVOLUÇÃO: registros_cq_inspecao — campos para motor de decisão
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS pct_defeitos_criticos  NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS pct_defeitos_maiores   NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS pct_defeitos_menores   NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS decisao_motor          TEXT
    CHECK (decisao_motor IN ('APROVADO','RESSALVA','BLOQUEADO','EM_ANALISE_CRITICA')),
  ADD COLUMN IF NOT EXISTS score_lote             NUMERIC(6,2) DEFAULT 100,
  ADD COLUMN IF NOT EXISTS requer_reamostragem    BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS reamostragem_id        UUID;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. EVOLUÇÃO: cq_lote_inspecao — campos para motor e IQF
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_lote_inspecao
  ADD COLUMN IF NOT EXISTS decisao_motor       TEXT
    CHECK (decisao_motor IN ('APROVADO','RESSALVA','BLOQUEADO','EM_ANALISE_CRITICA')),
  ADD COLUMN IF NOT EXISTS score_iqf           NUMERIC(6,2) DEFAULT 100,
  ADD COLUMN IF NOT EXISTS requer_reamostragem BOOLEAN DEFAULT FALSE;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. TABELA: cq_reamostragem — vínculo entre inspeção original e reinspeção
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_reamostragem (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Vínculo
  inspecao_original_id  UUID NOT NULL REFERENCES registros_cq_inspecao(id),
  lote_original_id      UUID REFERENCES cq_lote_inspecao(id),
  inspecao_reinspeção_id UUID REFERENCES registros_cq_inspecao(id),
  lote_reinspeção_id    UUID REFERENCES cq_lote_inspecao(id),

  -- Motivo
  motivo                TEXT NOT NULL,  -- ex: 'defeito_critico_detectado'
  motivo_detalhe        TEXT,

  -- Status do fluxo
  status                TEXT NOT NULL DEFAULT 'pendente'
    CHECK (status IN ('pendente','em_andamento','concluida','cancelada')),

  -- Resultado consolidado (após reinspeção)
  decisao_consolidada   TEXT
    CHECK (decisao_consolidada IN ('APROVADO','RESSALVA','BLOQUEADO')),
  score_consolidado     NUMERIC(6,2),
  observacao_consolidada TEXT,

  -- Quem solicitou
  solicitado_por        TEXT,
  solicitado_em         TIMESTAMPTZ DEFAULT NOW(),

  -- Quem concluiu
  concluido_por         TEXT,
  concluido_em          TIMESTAMPTZ,

  criado_em             TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reamos_original ON cq_reamostragem(inspecao_original_id);
CREATE INDEX IF NOT EXISTS idx_reamos_lote ON cq_reamostragem(lote_original_id);
CREATE INDEX IF NOT EXISTS idx_reamos_status ON cq_reamostragem(status);


-- ════════════════════════════════════════════════════════════════════════════
-- 5. TABELA: cq_nao_conformidades (referenciada em views mas não existia)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_nao_conformidades (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo            TEXT UNIQUE NOT NULL,  -- NC-YYYYMMDD-NNN
  tipo              TEXT NOT NULL CHECK (tipo IN ('produto','processo','fornecedor','sistema')),
  gravidade         TEXT NOT NULL CHECK (gravidade IN ('critica','maior','menor')),
  status            TEXT NOT NULL DEFAULT 'aberta'
    CHECK (status IN ('aberta','em_tratamento','concluida','cancelada')),

  -- Vínculo
  inspecao_id       UUID REFERENCES registros_cq_inspecao(id),
  lote_id           UUID REFERENCES cq_lote_inspecao(id),
  fornecedor_codigo TEXT,
  produto_codigo    TEXT,

  -- Descrição
  descricao         TEXT NOT NULL,
  causa_raiz        TEXT,
  acao_corretiva    TEXT,
  acao_preventiva   TEXT,
  evidencia_url     TEXT,

  -- Responsáveis
  registrado_por    TEXT NOT NULL,
  responsavel       TEXT,

  -- Prazos
  data_limite       DATE,
  data_conclusao    DATE,

  -- Validação
  validacao         TEXT CHECK (validacao IN ('eficaz','ineficaz','pendente')),
  validado_por      TEXT,

  criado_em         TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nc_status ON cq_nao_conformidades(status);
CREATE INDEX IF NOT EXISTS idx_nc_fornecedor ON cq_nao_conformidades(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_nc_gravidade ON cq_nao_conformidades(gravidade);
CREATE INDEX IF NOT EXISTS idx_nc_inspecao ON cq_nao_conformidades(inspecao_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 6. FUNCTION: Motor de Decisão Automática
-- Substitui a regra simples (A/B/C por %) por decisão baseada em severidade
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_motor_decisao_inspecao()
RETURNS TRIGGER AS $$
DECLARE
  v_total         INTEGER;
  v_crit          INTEGER := 0;
  v_maior         INTEGER := 0;
  v_menor         INTEGER := 0;
  v_pct_crit      NUMERIC(6,2);
  v_pct_maior     NUMERIC(6,2);
  v_pct_menor     NUMERIC(6,2);
  v_pct_total     NUMERIC(6,2);
  v_decisao       TEXT;
  v_score         NUMERIC(6,2);
  v_requer_reamos BOOLEAN := FALSE;
  v_defeito       RECORD;
  v_qtd           INTEGER;
BEGIN
  v_total := NEW.quantidade_analisada;
  IF v_total IS NULL OR v_total <= 0 THEN
    RETURN NEW;
  END IF;

  -- Somar defeitos por severidade usando cq_tipos_defeito
  FOR v_defeito IN
    SELECT codigo, severidade FROM cq_tipos_defeito WHERE ativo = true
  LOOP
    BEGIN
      EXECUTE format('SELECT ($1).%I', v_defeito.codigo) INTO v_qtd USING NEW;
    EXCEPTION WHEN OTHERS THEN
      v_qtd := 0;
    END;
    v_qtd := COALESCE(v_qtd, 0);

    CASE v_defeito.severidade
      WHEN 'CRITICO' THEN v_crit := v_crit + v_qtd;
      WHEN 'MAIOR'   THEN v_maior := v_maior + v_qtd;
      WHEN 'MENOR'   THEN v_menor := v_menor + v_qtd;
      ELSE                v_menor := v_menor + v_qtd;
    END CASE;
  END LOOP;

  -- Percentuais
  v_pct_crit  := ROUND(v_crit  * 100.0 / v_total, 2);
  v_pct_maior := ROUND(v_maior * 100.0 / v_total, 2);
  v_pct_menor := ROUND(v_menor * 100.0 / v_total, 2);
  v_pct_total := ROUND((v_crit + v_maior + v_menor) * 100.0 / v_total, 2);

  -- ─── REGRAS DE DECISÃO ───
  -- 1. Crítico >= 5% → BLOQUEADO
  IF v_pct_crit >= 5 THEN
    v_decisao := 'BLOQUEADO';
    v_score := 0;
  -- 2. Crítico > 0 e < 5% → EM_ANALISE_CRITICA (requer reamostragem)
  ELSIF v_pct_crit > 0 THEN
    v_decisao := 'EM_ANALISE_CRITICA';
    v_requer_reamos := TRUE;
    v_score := GREATEST(0, 100 - (v_pct_crit * 25) - (v_pct_maior * 8) - (v_pct_menor * 2));
  -- 3. Maior > 10% → BLOQUEADO
  ELSIF v_pct_maior > 10 THEN
    v_decisao := 'BLOQUEADO';
    v_score := GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2));
  -- 4. Maior > 5% → RESSALVA
  ELSIF v_pct_maior > 5 THEN
    v_decisao := 'RESSALVA';
    v_score := LEAST(79, GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2)));
  -- 5. Total > 10% → RESSALVA
  ELSIF v_pct_total > 10 THEN
    v_decisao := 'RESSALVA';
    v_score := LEAST(79, GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2)));
  -- 6. Caso contrário → APROVADO
  ELSE
    v_decisao := 'APROVADO';
    v_score := GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2));
  END IF;

  -- Gravar resultados
  NEW.pct_defeitos_criticos := v_pct_crit;
  NEW.pct_defeitos_maiores  := v_pct_maior;
  NEW.pct_defeitos_menores  := v_pct_menor;
  NEW.total_defeitos        := v_crit + v_maior + v_menor;
  NEW.percentual_defeitos   := v_pct_total;
  NEW.decisao_motor         := v_decisao;
  NEW.score_lote            := ROUND(v_score, 2);
  NEW.requer_reamostragem   := v_requer_reamos;

  -- Manter classificação ABC retrocompatível
  IF v_pct_total <= 5 THEN
    NEW.classificacao := 'A';
  ELSIF v_pct_total <= 15 THEN
    NEW.classificacao := 'B';
  ELSE
    NEW.classificacao := 'C';
  END IF;

  -- status_final retrocompatível
  CASE v_decisao
    WHEN 'APROVADO'            THEN NEW.status_final := 'aprovado';
    WHEN 'RESSALVA'            THEN NEW.status_final := 'ressalva';
    WHEN 'BLOQUEADO'           THEN NEW.status_final := 'bloqueado';
    WHEN 'EM_ANALISE_CRITICA'  THEN NEW.status_final := 'bloqueado'; -- bloqueado até reinspeção
  END CASE;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Substituir o trigger existente pelo motor novo
DROP TRIGGER IF EXISTS trg_auto_calcular_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_motor_decisao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  WHEN (NEW.quantidade_analisada > 0)
  EXECUTE FUNCTION fn_motor_decisao_inspecao();


-- ════════════════════════════════════════════════════════════════════════════
-- 7. FUNCTION: Calcular IQF do Fornecedor (média ponderada por recência)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_calcular_iqf_fornecedor(p_fornecedor_codigo TEXT)
RETURNS NUMERIC LANGUAGE plpgsql AS $$
DECLARE
  v_iqf NUMERIC(6,2);
BEGIN
  -- Média ponderada: lotes mais recentes têm peso maior
  -- Peso = 1 / (dias_atrás + 1), normalizado
  SELECT ROUND(
    SUM(sub.score * sub.peso) / NULLIF(SUM(sub.peso), 0)
  , 2) INTO v_iqf
  FROM (
    SELECT
      COALESCE(i.score_lote, 100) AS score,
      1.0 / (EXTRACT(EPOCH FROM (NOW() - i.criado_em)) / 86400 + 1) AS peso
    FROM registros_cq_inspecao i
    WHERE i.fornecedor_codigo = p_fornecedor_codigo
      AND i.data_inspecao >= CURRENT_DATE - INTERVAL '180 days'
      AND i.quantidade_analisada > 0
    ORDER BY i.data_inspecao DESC
    LIMIT 50
  ) sub;

  RETURN COALESCE(v_iqf, 100);
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 8. FUNCTION: Solicitar reamostragem automaticamente
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_auto_reamostragem()
RETURNS TRIGGER AS $$
BEGIN
  -- Apenas quando decisão muda para EM_ANALISE_CRITICA
  IF NEW.decisao_motor = 'EM_ANALISE_CRITICA'
     AND NEW.requer_reamostragem = TRUE
     AND (OLD.decisao_motor IS DISTINCT FROM 'EM_ANALISE_CRITICA')
  THEN
    -- Verificar se já existe reamostragem pendente
    IF NOT EXISTS (
      SELECT 1 FROM cq_reamostragem
      WHERE inspecao_original_id = NEW.id
        AND status IN ('pendente','em_andamento')
    ) THEN
      INSERT INTO cq_reamostragem (
        inspecao_original_id, motivo, motivo_detalhe, solicitado_por
      ) VALUES (
        NEW.id,
        'defeito_critico_detectado',
        FORMAT('Defeitos criticos: %s%% (%s de %s pecas)',
          NEW.pct_defeitos_criticos,
          ROUND(NEW.pct_defeitos_criticos * NEW.quantidade_analisada / 100),
          NEW.quantidade_analisada),
        NEW.inspetor
      );

      -- Registrar evento
      PERFORM fn_registrar_evento(
        'reamostragem_solicitada', 'qualidade',
        FORMAT('Reamostragem obrigatoria para inspecao %s — %s%% defeitos criticos',
          NEW.id_inspecao, NEW.pct_defeitos_criticos),
        'cq_reamostragem', NULL, NEW.id,
        jsonb_build_object('inspecao_id', NEW.id, 'pct_criticos', NEW.pct_defeitos_criticos),
        NULL, 'critical'
      );
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_reamostragem
  AFTER INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_auto_reamostragem();


-- ════════════════════════════════════════════════════════════════════════════
-- 9. FUNCTION: Concluir reamostragem — consolidar resultado
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_concluir_reamostragem(
  p_reamostragem_id UUID,
  p_inspecao_reinspeção_id UUID,
  p_usuario TEXT
)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
  v_reamos cq_reamostragem;
  v_original registros_cq_inspecao;
  v_reinspeção registros_cq_inspecao;
  v_score_consolidado NUMERIC(6,2);
  v_decisao_consolidada TEXT;
BEGIN
  SELECT * INTO v_reamos FROM cq_reamostragem WHERE id = p_reamostragem_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('erro', 'Reamostragem nao encontrada');
  END IF;
  IF v_reamos.status NOT IN ('pendente','em_andamento') THEN
    RETURN jsonb_build_object('erro', 'Reamostragem ja concluida ou cancelada');
  END IF;

  SELECT * INTO v_original FROM registros_cq_inspecao WHERE id = v_reamos.inspecao_original_id;
  SELECT * INTO v_reinspeção FROM registros_cq_inspecao WHERE id = p_inspecao_reinspeção_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('erro', 'Inspecao de reinspecao nao encontrada');
  END IF;

  -- Consolidar: média dos scores (original + reinspeção)
  v_score_consolidado := ROUND((COALESCE(v_original.score_lote,0) + COALESCE(v_reinspeção.score_lote,0)) / 2, 2);

  -- Decisão consolidada pela reinspeção (prevalece)
  v_decisao_consolidada := CASE
    WHEN v_reinspeção.decisao_motor = 'BLOQUEADO' THEN 'BLOQUEADO'
    WHEN v_reinspeção.decisao_motor = 'EM_ANALISE_CRITICA' THEN 'BLOQUEADO'
    WHEN v_reinspeção.decisao_motor = 'RESSALVA' THEN 'RESSALVA'
    ELSE v_reinspeção.decisao_motor
  END;

  -- Atualizar reamostragem
  UPDATE cq_reamostragem SET
    inspecao_reinspeção_id = p_inspecao_reinspeção_id,
    status = 'concluida',
    decisao_consolidada = v_decisao_consolidada,
    score_consolidado = v_score_consolidado,
    concluido_por = p_usuario,
    concluido_em = NOW(),
    atualizado_em = NOW()
  WHERE id = p_reamostragem_id;

  -- Atualizar inspeção original com resultado consolidado
  UPDATE registros_cq_inspecao SET
    score_lote = v_score_consolidado,
    decisao_motor = v_decisao_consolidada,
    requer_reamostragem = FALSE,
    status_final = CASE v_decisao_consolidada
      WHEN 'APROVADO' THEN 'aprovado'
      WHEN 'RESSALVA' THEN 'ressalva'
      ELSE 'bloqueado'
    END
  WHERE id = v_reamos.inspecao_original_id;

  RETURN jsonb_build_object(
    'reamostragem_id', p_reamostragem_id,
    'decisao_consolidada', v_decisao_consolidada,
    'score_consolidado', v_score_consolidado,
    'status', 'concluida'
  );
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 10. VIEW: Dashboard de fornecedores com IQF
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_fornecedor_iqf AS
SELECT
  ca.codigo AS fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome) AS fornecedor_nome,
  ca.uf,

  -- Volume
  COUNT(DISTINCT i.id) AS total_lotes,
  SUM(i.quantidade_analisada) AS total_pecas,

  -- Decisões
  COUNT(*) FILTER (WHERE i.decisao_motor = 'APROVADO') AS lotes_aprovados,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'RESSALVA') AS lotes_ressalva,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'BLOQUEADO') AS lotes_bloqueados,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'EM_ANALISE_CRITICA') AS lotes_analise_critica,

  -- Percentuais
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'APROVADO') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_aprovados,
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'RESSALVA') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_ressalva,
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'BLOQUEADO') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_bloqueados,

  -- Defeitos médios por severidade
  ROUND(AVG(i.pct_defeitos_criticos), 2) AS media_pct_criticos,
  ROUND(AVG(i.pct_defeitos_maiores), 2) AS media_pct_maiores,

  -- Ocorrências críticas
  COUNT(*) FILTER (WHERE i.pct_defeitos_criticos > 0) AS ocorrencias_criticas,

  -- IQF (calculado)
  fn_calcular_iqf_fornecedor(ca.codigo) AS iqf,

  -- Classificação de risco por cor
  CASE
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 90 THEN 'verde'
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 75 THEN 'amarelo'
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 60 THEN 'laranja'
    ELSE 'vermelho'
  END AS classificacao_risco,

  -- Reamostragens pendentes
  (SELECT COUNT(*) FROM cq_reamostragem r
   JOIN registros_cq_inspecao ri ON ri.id = r.inspecao_original_id
   WHERE ri.fornecedor_codigo = ca.codigo AND r.status IN ('pendente','em_andamento')
  ) AS reamostragens_pendentes

FROM cadastros_atak ca
LEFT JOIN registros_cq_inspecao i
  ON i.fornecedor_codigo = ca.codigo
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '180 days'
  AND i.quantidade_analisada > 0
WHERE ca.tipo = 'fornecedor' AND ca.ativo = true
GROUP BY ca.codigo, ca.apelido, ca.nome, ca.uf
HAVING COUNT(i.id) > 0
ORDER BY fn_calcular_iqf_fornecedor(ca.codigo) ASC;


-- ════════════════════════════════════════════════════════════════════════════
-- 11. VIEW: Alertas automáticos de fornecedor
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_alertas_fornecedor_iqf AS

-- 2+ bloqueios em 30 dias
SELECT
  'bloqueios_recorrentes' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo AS codigo,
  COALESCE(ca.apelido, ca.nome) AS nome,
  FORMAT('%s bloqueios nos ultimos 30 dias', COUNT(*)) AS descricao,
  COUNT(*)::NUMERIC AS valor
FROM registros_cq_inspecao i
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE i.decisao_motor = 'BLOQUEADO'
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY i.fornecedor_codigo, ca.apelido, ca.nome
HAVING COUNT(*) >= 2

UNION ALL

-- Aumento de defeito crítico (qualquer ocorrência)
SELECT
  'defeito_critico_detectado' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome),
  FORMAT('Defeito critico em inspecao %s — %s%%', i.id_inspecao, i.pct_defeitos_criticos),
  i.pct_defeitos_criticos
FROM registros_cq_inspecao i
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE i.pct_defeitos_criticos > 0
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

-- IQF abaixo de 60 (vermelho)
SELECT
  'iqf_vermelho' AS tipo_alerta,
  'atencao' AS gravidade_alerta,
  sub.fornecedor_codigo,
  sub.fornecedor_nome,
  FORMAT('IQF = %s (risco vermelho)', sub.iqf),
  sub.iqf
FROM vw_fornecedor_iqf sub
WHERE sub.iqf < 60

UNION ALL

-- Reamostragem pendente há mais de 48h
SELECT
  'reamostragem_pendente' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome),
  FORMAT('Reamostragem pendente ha %s horas', ROUND(EXTRACT(EPOCH FROM NOW() - r.solicitado_em) / 3600)),
  ROUND(EXTRACT(EPOCH FROM NOW() - r.solicitado_em) / 3600)::NUMERIC
FROM cq_reamostragem r
JOIN registros_cq_inspecao i ON i.id = r.inspecao_original_id
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE r.status = 'pendente'
  AND r.solicitado_em < NOW() - INTERVAL '48 hours'

ORDER BY
  CASE gravidade_alerta WHEN 'critico' THEN 1 ELSE 2 END,
  valor DESC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 12. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_reamostragem ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_nao_conformidades ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_reamostragem" ON cq_reamostragem FOR SELECT USING (true);
CREATE POLICY "all_reamostragem" ON cq_reamostragem FOR ALL USING (true);
CREATE POLICY "read_nc" ON cq_nao_conformidades FOR SELECT USING (true);
CREATE POLICY "all_nc" ON cq_nao_conformidades FOR ALL USING (true);


-- ════════════════════════════════════════════════════════════════════════════
-- 13. GUARD: Impedir encerramento sem reamostragem
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_reamostragem_obrigatoria()
RETURNS TRIGGER AS $$
BEGIN
  -- Impedir aprovação/encerramento se reamostragem pendente
  IF NEW.requer_reamostragem = TRUE
     AND NEW.status_final IN ('aprovado')
  THEN
    RAISE EXCEPTION 'Inspecao requer reamostragem obrigatoria antes de aprovacao. '
      'Realize a reinspecao e conclua a reamostragem.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_guard_reamostragem ON registros_cq_inspecao;
CREATE TRIGGER trg_guard_reamostragem
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_guard_reamostragem_obrigatoria();


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRACAO 13 — Motor de Decisao + IQF + Reamostragem';
  RAISE NOTICE '  ';
  RAISE NOTICE '  MOTOR DE DECISAO:';
  RAISE NOTICE '    ✓ Critico >=5%% → BLOQUEADO (score=0)';
  RAISE NOTICE '    ✓ Critico >0%% e <5%% → EM_ANALISE_CRITICA + reamostragem';
  RAISE NOTICE '    ✓ Maior >10%% → BLOQUEADO';
  RAISE NOTICE '    ✓ Maior >5%% → RESSALVA (score max 79)';
  RAISE NOTICE '    ✓ Total >10%% → RESSALVA';
  RAISE NOTICE '    ✓ Caso contrario → APROVADO';
  RAISE NOTICE '  ';
  RAISE NOTICE '  IQF POR LOTE:';
  RAISE NOTICE '    ✓ Base 100, -25/%%crit, -8/%%maior, -2/%%menor';
  RAISE NOTICE '    ✓ Bloqueado = 0, Ressalva max 79';
  RAISE NOTICE '  ';
  RAISE NOTICE '  IQF POR FORNECEDOR:';
  RAISE NOTICE '    ✓ Media ponderada por recencia (180 dias, max 50 lotes)';
  RAISE NOTICE '    ✓ verde>=90 amarelo>=75 laranja>=60 vermelho<60';
  RAISE NOTICE '  ';
  RAISE NOTICE '  REAMOSTRAGEM:';
  RAISE NOTICE '    ✓ Criada automaticamente quando EM_ANALISE_CRITICA';
  RAISE NOTICE '    ✓ Guard impede aprovacao sem reamostragem';
  RAISE NOTICE '    ✓ fn_concluir_reamostragem consolida resultado';
  RAISE NOTICE '  ';
  RAISE NOTICE '  TABELAS NOVAS:';
  RAISE NOTICE '    ✓ cq_reamostragem';
  RAISE NOTICE '    ✓ cq_nao_conformidades';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS:';
  RAISE NOTICE '    ✓ vw_fornecedor_iqf (ranking com IQF + risco)';
  RAISE NOTICE '    ✓ vw_alertas_fornecedor_iqf (alertas automaticos)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 14: Leitura de Etiqueta / Scanner + Auto-preenchimento
--
-- Estrutura para scan de etiqueta (câmera mobile / scanner desktop):
--   1. Campo de busca: codigo_etiqueta como chave
--   2. Log de leituras (auditoria)
--   3. Log de alterações manuais (quando operador muda dado pós-leitura)
--   4. View de busca unificada (lotes recebidos + cargas + movimentações)
--
-- Rodar APÓS 13_migration_motor_decisao_iqf.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. EVOLUÇÃO: registros_cq_inspecao — campo de etiqueta escaneada
-- O campo 'etiqueta' já existe. Adicionamos metadados de scan.
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS scan_origem       TEXT CHECK (scan_origem IN ('camera','scanner','manual','api')),
  ADD COLUMN IF NOT EXISTS scan_codigo_raw   TEXT,
  ADD COLUMN IF NOT EXISTS scan_timestamp    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS scan_auto_preenchido BOOLEAN DEFAULT FALSE;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TABELA: Log de leituras de etiqueta (auditoria)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_scan_log (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  usuario_email    TEXT NOT NULL,
  usuario_nome     TEXT,

  -- O que foi lido
  codigo_lido      TEXT NOT NULL,
  tipo_codigo      TEXT CHECK (tipo_codigo IN ('barcode','qrcode','manual','rfid')),
  scan_origem      TEXT CHECK (scan_origem IN ('camera','scanner','manual')),

  -- Resultado
  encontrado       BOOLEAN NOT NULL DEFAULT FALSE,
  tabela_origem    TEXT,    -- ex: 'atak_cargas_raw', 'atak_movimentacoes'
  registro_id      TEXT,    -- ID do registro encontrado

  -- Dados auto-preenchidos (snapshot do que foi preenchido)
  dados_preenchidos JSONB,

  -- Vínculo
  inspecao_id      UUID REFERENCES registros_cq_inspecao(id),

  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scan_log_codigo ON cq_scan_log(codigo_lido);
CREATE INDEX IF NOT EXISTS idx_scan_log_usuario ON cq_scan_log(usuario_email);
CREATE INDEX IF NOT EXISTS idx_scan_log_criado ON cq_scan_log(criado_em DESC);


-- ════════════════════════════════════════════════════════════════════════════
-- 3. TABELA: Log de alterações pós-scan (justificativa obrigatória)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_scan_alteracao_log (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inspecao_id      UUID NOT NULL REFERENCES registros_cq_inspecao(id),
  scan_log_id      UUID REFERENCES cq_scan_log(id),
  usuario_email    TEXT NOT NULL,

  campo_alterado   TEXT NOT NULL,    -- ex: 'fornecedor', 'produto', 'lote_atak'
  valor_original   TEXT,             -- valor do auto-preenchimento
  valor_novo       TEXT NOT NULL,    -- valor digitado pelo operador
  justificativa    TEXT NOT NULL,    -- obrigatória

  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scan_alt_inspecao ON cq_scan_alteracao_log(inspecao_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 4. VIEW: Busca unificada por etiqueta/documento
-- Consolida todas as fontes de dados para auto-preenchimento
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_busca_etiqueta AS

-- Fonte 1: atak_cargas_raw (cargas recebidas via API)
SELECT
  COALESCE(c.numero_documento, c.numero_pcr) AS codigo_busca,
  'atak_cargas_raw' AS tabela_origem,
  c.id::TEXT AS registro_id,
  c.fornecedor_nome AS fornecedor,
  c.fornecedor_codigo,
  c.produto,
  c.qtd_frigo AS quantidade,
  c.data_chegada AS data_recebimento,
  c.numero_carga AS pedido_carga,
  c.motorista,
  c.placa_veiculo AS placa,
  c.transportadora
FROM atak_cargas_raw c
WHERE c.numero_documento IS NOT NULL OR c.numero_pcr IS NOT NULL

UNION ALL

-- Fonte 2: atak_movimentacoes (planilha importada)
SELECT
  m.documento AS codigo_busca,
  'atak_movimentacoes' AS tabela_origem,
  m.id::TEXT AS registro_id,
  m.participante_nome AS fornecedor,
  m.participante_codigo AS fornecedor_codigo,
  COALESCE(p.nome, m.id_atak_produto) AS produto,
  m.quantidade::INTEGER AS quantidade,
  m.data_movimentacao AS data_recebimento,
  NULL AS pedido_carga,
  NULL AS motorista,
  NULL AS placa,
  NULL AS transportadora
FROM atak_movimentacoes m
LEFT JOIN cq_produtos p ON p.id = m.produto_id
WHERE m.tipo_movimentacao = 'Entrada'

UNION ALL

-- Fonte 3: registros_cq_inspecao existentes (para lotes já inspecionados)
SELECT
  i.etiqueta AS codigo_busca,
  'registros_cq_inspecao' AS tabela_origem,
  i.id::TEXT AS registro_id,
  i.fornecedor,
  i.fornecedor_codigo,
  i.produto,
  i.quantidade_lote AS quantidade,
  i.data_inspecao AS data_recebimento,
  i.lote_atak AS pedido_carga,
  i.motorista,
  i.placa_caminhao AS placa,
  i.transportadora
FROM registros_cq_inspecao i
WHERE i.etiqueta IS NOT NULL;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. FUNCTION: Buscar por etiqueta (chamada via RPC do frontend)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_buscar_etiqueta(p_codigo TEXT)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
  v_result JSONB;
BEGIN
  -- Buscar match exato primeiro
  SELECT jsonb_build_object(
    'encontrado', true,
    'tabela_origem', b.tabela_origem,
    'registro_id', b.registro_id,
    'fornecedor', b.fornecedor,
    'fornecedor_codigo', b.fornecedor_codigo,
    'produto', b.produto,
    'quantidade', b.quantidade,
    'data_recebimento', b.data_recebimento,
    'pedido_carga', b.pedido_carga,
    'motorista', b.motorista,
    'placa', b.placa,
    'transportadora', b.transportadora
  ) INTO v_result
  FROM vw_busca_etiqueta b
  WHERE b.codigo_busca = p_codigo
  LIMIT 1;

  IF v_result IS NOT NULL THEN
    RETURN v_result;
  END IF;

  -- Buscar match parcial (LIKE)
  SELECT jsonb_build_object(
    'encontrado', true,
    'match_parcial', true,
    'tabela_origem', b.tabela_origem,
    'registro_id', b.registro_id,
    'fornecedor', b.fornecedor,
    'fornecedor_codigo', b.fornecedor_codigo,
    'produto', b.produto,
    'quantidade', b.quantidade,
    'data_recebimento', b.data_recebimento,
    'pedido_carga', b.pedido_carga,
    'motorista', b.motorista,
    'placa', b.placa,
    'transportadora', b.transportadora
  ) INTO v_result
  FROM vw_busca_etiqueta b
  WHERE b.codigo_busca ILIKE '%' || p_codigo || '%'
  LIMIT 1;

  RETURN COALESCE(v_result, jsonb_build_object('encontrado', false, 'codigo', p_codigo));
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_scan_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_scan_alteracao_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_scan_log" ON cq_scan_log FOR SELECT USING (true);
CREATE POLICY "all_scan_log" ON cq_scan_log FOR ALL USING (true);
CREATE POLICY "read_scan_alt" ON cq_scan_alteracao_log FOR SELECT USING (true);
CREATE POLICY "all_scan_alt" ON cq_scan_alteracao_log FOR ALL USING (true);


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRACAO 14 — Scan de Etiqueta + Auto-preenchimento';
  RAISE NOTICE '  ';
  RAISE NOTICE '  TABELAS:';
  RAISE NOTICE '    ✓ cq_scan_log (log de leituras)';
  RAISE NOTICE '    ✓ cq_scan_alteracao_log (alteracoes pos-scan)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS:';
  RAISE NOTICE '    ✓ vw_busca_etiqueta (3 fontes unificadas)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  FUNCTIONS:';
  RAISE NOTICE '    ✓ fn_buscar_etiqueta (RPC para frontend)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
