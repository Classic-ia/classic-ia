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
