-- ══════════════════════════════════════════════════════════════════
-- STAGING SCHEMA: PCR Import Validation
-- Fluxo: ATAK WRMVE500 (XLSX) -> staging tables -> validacao -> pcr_*
-- ══════════════════════════════════════════════════════════════════
--
-- Este schema implementa um pipeline de importacao em 3 etapas:
--
--   1. CARGA (staging_pcr_importacao + staging_pcr_linhas)
--      O arquivo WRMVE500 eh lido e cada linha eh inserida na staging
--      com status 'pendente'. Nenhum dado vai direto para as tabelas finais.
--
--   2. VALIDACAO (fn_validar_staging_pcr)
--      Cada linha eh verificada: campos obrigatorios, duplicatas contra
--      pcr_produtos existentes, formato de dados. Resultado fica em
--      staging_pcr_validacao_log.
--
--   3. PROMOCAO (fn_promover_staging_pcr)
--      Linhas validas sao agrupadas por numero_documento e inseridas
--      em pcr_documentos + pcr_produtos + pcr_qualidade_defeitos.
--      Responsavel_carteira eh auto-atribuido via pcr_carteira_fornecedores.
--
-- Executar no SQL Editor do Supabase.
-- ══════════════════════════════════════════════════════════════════


-- ──────────────────────────────────────────────────────────────────
-- 1. TABELA: staging_pcr_importacao
-- Holding area para cada arquivo importado (1 registro por upload)
-- ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_pcr_importacao (
  id                UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  arquivo_nome      TEXT        NOT NULL,
  importado_em      TIMESTAMPTZ DEFAULT NOW(),
  importado_por     TEXT,                          -- email ou nome do usuario
  total_linhas      INTEGER     DEFAULT 0,
  total_documentos  INTEGER     DEFAULT 0,         -- count(distinct numero_documento)
  status            TEXT        DEFAULT 'pendente'
    CHECK (status IN (
      'pendente',      -- arquivo carregado, aguardando validacao
      'validando',     -- fn_validar_staging_pcr em execucao
      'validada',      -- validacao concluida (ver staging_pcr_linhas.status)
      'importada',     -- linhas promovidas para pcr_* com sucesso
      'erro'           -- falha critica durante o processo
    )),
  erro_global       TEXT,                          -- mensagem de erro geral (se status = 'erro')
  criado_em         TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE staging_pcr_importacao IS
  'Registro de cada upload de planilha WRMVE500 (PCR). Um registro por arquivo.';


-- ──────────────────────────────────────────────────────────────────
-- 2. TABELA: staging_pcr_linhas
-- Cada linha individual da planilha WRMVE500
-- ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_pcr_linhas (
  id                UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  importacao_id     UUID        NOT NULL REFERENCES staging_pcr_importacao(id) ON DELETE CASCADE,

  -- Posicao no arquivo
  linha_numero      INTEGER     NOT NULL,

  -- Dados brutos da planilha WRMVE500
  numero_documento  TEXT,                          -- ex: "PCR-2026-001234"
  data_pcr          DATE,
  motorista         TEXT,
  participante      TEXT,                          -- fornecedor/participante
  origem            TEXT,                          -- cidade/estado de origem
  placa             TEXT,                          -- placa do veiculo
  codigo_produto    TEXT,                          -- ex: "45034"
  descricao_produto TEXT,                          -- ex: "Omaso Bovino Congelado"
  qtd_prevista      NUMERIC(12,2),
  qtd_recebida      NUMERIC(12,2),
  diferenca         NUMERIC(12,2),                 -- qtd_recebida - qtd_prevista
  qualidade_texto   TEXT,                          -- ex: "RASGADO: 15 FURADO: 22 PELANDO: 89"

  -- Resultado da validacao
  status            TEXT        DEFAULT 'pendente'
    CHECK (status IN (
      'pendente',      -- aguardando validacao
      'valida',        -- aprovada para promocao
      'duplicada',     -- ja existe em pcr_produtos (mesmo doc + produto)
      'erro'           -- campo obrigatorio ausente ou invalido
    )),
  erro_msg          TEXT,                          -- detalhe do erro (se status = 'erro')

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE staging_pcr_linhas IS
  'Linhas individuais extraidas da planilha WRMVE500. Cada linha = 1 produto de 1 documento PCR.';


-- ──────────────────────────────────────────────────────────────────
-- 3. TABELA: staging_pcr_validacao_log
-- Log detalhado de cada regra de validacao aplicada
-- ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_pcr_validacao_log (
  id                UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  importacao_id     UUID        NOT NULL REFERENCES staging_pcr_importacao(id) ON DELETE CASCADE,
  linha_id          UUID        REFERENCES staging_pcr_linhas(id) ON DELETE CASCADE,

  regra             TEXT        NOT NULL,          -- ex: 'campo_obrigatorio', 'duplicata_pcr', 'formato_data'
  resultado         TEXT        NOT NULL           -- 'ok', 'erro', 'aviso'
    CHECK (resultado IN ('ok','erro','aviso')),
  detalhe           TEXT,                          -- descricao livre do resultado

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE staging_pcr_validacao_log IS
  'Log de auditoria: cada regra de validacao aplicada a cada linha, com resultado e detalhe.';


-- ──────────────────────────────────────────────────────────────────
-- INDEXES
-- ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_stg_linhas_importacao
  ON staging_pcr_linhas(importacao_id);

CREATE INDEX IF NOT EXISTS idx_stg_linhas_documento
  ON staging_pcr_linhas(numero_documento);

CREATE INDEX IF NOT EXISTS idx_stg_linhas_status
  ON staging_pcr_linhas(status);

CREATE INDEX IF NOT EXISTS idx_stg_linhas_produto
  ON staging_pcr_linhas(codigo_produto);

CREATE INDEX IF NOT EXISTS idx_stg_log_importacao
  ON staging_pcr_validacao_log(importacao_id);

CREATE INDEX IF NOT EXISTS idx_stg_log_linha
  ON staging_pcr_validacao_log(linha_id);

CREATE INDEX IF NOT EXISTS idx_stg_imp_status
  ON staging_pcr_importacao(status);


-- ──────────────────────────────────────────────────────────────────
-- RLS DISABLED (staging tables sao internas, nao expostas via API)
-- ──────────────────────────────────────────────────────────────────

ALTER TABLE staging_pcr_importacao   DISABLE ROW LEVEL SECURITY;
ALTER TABLE staging_pcr_linhas       DISABLE ROW LEVEL SECURITY;
ALTER TABLE staging_pcr_validacao_log DISABLE ROW LEVEL SECURITY;


-- ══════════════════════════════════════════════════════════════════
-- FUNCOES
-- ══════════════════════════════════════════════════════════════════


-- ──────────────────────────────────────────────────────────────────
-- fn_parse_qualidade_texto(texto TEXT)
-- Converte string de defeitos do ATAK para JSONB com campos mapeados.
--
-- Entrada: "RASGADO: 15 FURADO: 22 PELANDO: 89 SUJO: 14 C.I: 31 F.E: 44"
-- Saida:   {"rasgado":15,"furado":22,"fermentacao":89,"sujo":14,
--           "corte_irregular":31,"folhas_arrebentadas":44}
--
-- Mapeamento ATAK -> campo CQ:
--   PELANDO           -> fermentacao
--   SEM FOLHA / S.FOLHA -> sem_folhas
--   F.E. / FOLHA E.   -> folhas_arrebentadas
--   PEDACO            -> pedaco
--   RASGADO           -> rasgado
--   FURADO            -> furado
--   SUJO / DESPOJO    -> sujo
--   C.I. / CORTE I. / SEBO -> corte_irregular
-- ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_parse_qualidade_texto(p_texto TEXT)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_result   JSONB := '{}'::JSONB;
  v_texto    TEXT;
  v_matches  TEXT[];
  v_label    TEXT;
  v_valor    INTEGER;
  v_campo    TEXT;
  v_rec      RECORD;
BEGIN
  IF p_texto IS NULL OR TRIM(p_texto) = '' THEN
    RETURN v_result;
  END IF;

  -- Normalizar: upper + remover espacos extras
  v_texto := UPPER(TRIM(p_texto));

  -- Extrair pares LABEL: VALOR usando regex
  -- Formato esperado: "RASGADO: 15 FURADO: 22 PELANDO: 89"
  FOR v_rec IN
    SELECT
      m[1] AS label,
      m[2]::INTEGER AS valor
    FROM regexp_matches(
      v_texto,
      '([A-Z][A-Z.\s]*?)\s*:\s*(\d+)',
      'g'
    ) AS m
  LOOP
    v_label := TRIM(v_rec.label);
    v_valor := v_rec.valor;

    -- Mapear label ATAK para campo padrao CQ
    v_campo := CASE
      WHEN v_label IN ('PELANDO', 'PELADA', 'FERM', 'FERMENTACAO', 'FERMENTADO')
        THEN 'fermentacao'
      WHEN v_label IN ('SEM FOLHA', 'S.FOLHA', 'S FOLHA', 'SEM FOLHAS')
        THEN 'sem_folhas'
      WHEN v_label IN ('F.E', 'F.E.', 'FE', 'FOLHA E', 'FOLHA E.', 'FOLHAS ARREBENTADAS', 'FOLHA ARRB')
        THEN 'folhas_arrebentadas'
      WHEN v_label IN ('PEDACO', 'PEDACOS', 'PED')
        THEN 'pedaco'
      WHEN v_label IN ('RASGADO', 'RASGADA', 'RASGO', 'RASG')
        THEN 'rasgado'
      WHEN v_label IN ('FURADO', 'FURADA', 'FURO', 'FUR')
        THEN 'furado'
      WHEN v_label IN ('SUJO', 'SUJA', 'DESPOJO', 'DESP')
        THEN 'sujo'
      WHEN v_label IN ('C.I', 'C.I.', 'CI', 'CORTE I', 'CORTE I.', 'CORTE IRREGULAR', 'SEBO')
        THEN 'corte_irregular'
      WHEN v_label IN ('MAU CHEIRO', 'CHEIRO', 'M.C', 'M.C.')
        THEN 'mau_cheiro'
      WHEN v_label IN ('CONTAMINACAO', 'CONTAM', 'CONT')
        THEN 'contaminacao'
      ELSE NULL
    END;

    -- Se nao reconheceu, gravar como outro_defeito com label original
    IF v_campo IS NULL THEN
      v_campo := 'outro_' || LOWER(REGEXP_REPLACE(v_label, '[^A-Z0-9]', '_', 'g'));
    END IF;

    -- Somar ao resultado (caso label apareca mais de uma vez)
    IF v_result ? v_campo THEN
      v_result := jsonb_set(
        v_result,
        ARRAY[v_campo],
        to_jsonb((v_result->>v_campo)::INTEGER + v_valor)
      );
    ELSE
      v_result := v_result || jsonb_build_object(v_campo, v_valor);
    END IF;
  END LOOP;

  RETURN v_result;
END;
$$;

COMMENT ON FUNCTION fn_parse_qualidade_texto(TEXT) IS
  'Converte texto de qualidade do ATAK (ex: "RASGADO: 15 FURADO: 22") em JSONB com campos mapeados para o padrao CQ.';


-- ──────────────────────────────────────────────────────────────────
-- fn_validar_staging_pcr(importacao_id UUID)
--
-- Valida todas as linhas de uma importacao:
--   1. Campos obrigatorios (numero_documento, codigo_produto, data_pcr)
--   2. Duplicata: mesma combinacao numero_documento + codigo_produto
--      ja existe em pcr_produtos
--   3. Marca cada linha como 'valida', 'duplicada' ou 'erro'
--   4. Atualiza staging_pcr_importacao.status para 'validada'
--
-- Retorna: total de linhas validadas
-- ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_validar_staging_pcr(p_importacao_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_linha    RECORD;
  v_total    INTEGER := 0;
  v_erros    INTEGER := 0;
  v_dupl     INTEGER := 0;
  v_validas  INTEGER := 0;
  v_msg      TEXT;
BEGIN
  -- Marcar importacao como 'validando'
  UPDATE staging_pcr_importacao
  SET status = 'validando', atualizado_em = NOW()
  WHERE id = p_importacao_id;

  -- Limpar validacoes anteriores desta importacao
  DELETE FROM staging_pcr_validacao_log WHERE importacao_id = p_importacao_id;

  -- Resetar status das linhas
  UPDATE staging_pcr_linhas
  SET status = 'pendente', erro_msg = NULL
  WHERE importacao_id = p_importacao_id;

  -- Iterar cada linha
  FOR v_linha IN
    SELECT * FROM staging_pcr_linhas
    WHERE importacao_id = p_importacao_id
    ORDER BY linha_numero
  LOOP
    v_total := v_total + 1;
    v_msg := NULL;

    -- ── Regra 1: Campos obrigatorios ──
    IF v_linha.numero_documento IS NULL OR TRIM(v_linha.numero_documento) = '' THEN
      v_msg := 'Campo obrigatorio ausente: numero_documento';
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'campo_obrigatorio', 'erro', v_msg);

      UPDATE staging_pcr_linhas SET status = 'erro', erro_msg = v_msg WHERE id = v_linha.id;
      v_erros := v_erros + 1;
      CONTINUE;
    END IF;

    IF v_linha.codigo_produto IS NULL OR TRIM(v_linha.codigo_produto) = '' THEN
      v_msg := 'Campo obrigatorio ausente: codigo_produto';
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'campo_obrigatorio', 'erro', v_msg);

      UPDATE staging_pcr_linhas SET status = 'erro', erro_msg = v_msg WHERE id = v_linha.id;
      v_erros := v_erros + 1;
      CONTINUE;
    END IF;

    IF v_linha.data_pcr IS NULL THEN
      v_msg := 'Campo obrigatorio ausente: data_pcr';
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'campo_obrigatorio', 'erro', v_msg);

      UPDATE staging_pcr_linhas SET status = 'erro', erro_msg = v_msg WHERE id = v_linha.id;
      v_erros := v_erros + 1;
      CONTINUE;
    END IF;

    -- Log campo obrigatorio OK
    INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
    VALUES (p_importacao_id, v_linha.id, 'campo_obrigatorio', 'ok', 'Todos campos obrigatorios presentes');

    -- ── Regra 2: Duplicata contra pcr_produtos ──
    IF EXISTS (
      SELECT 1 FROM pcr_produtos pp
      JOIN pcr_documentos pd ON pd.id = pp.documento_id
      WHERE pd.numero_documento = v_linha.numero_documento
        AND pp.codigo_produto = v_linha.codigo_produto
    ) THEN
      v_msg := 'Duplicata: documento ' || v_linha.numero_documento ||
               ' + produto ' || v_linha.codigo_produto || ' ja existe em pcr_produtos';
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'duplicata_pcr', 'erro', v_msg);

      UPDATE staging_pcr_linhas SET status = 'duplicada', erro_msg = v_msg WHERE id = v_linha.id;
      v_dupl := v_dupl + 1;
      CONTINUE;
    END IF;

    INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
    VALUES (p_importacao_id, v_linha.id, 'duplicata_pcr', 'ok', 'Sem duplicata encontrada');

    -- ── Regra 3: Validacao de quantidades ──
    IF v_linha.qtd_prevista IS NOT NULL AND v_linha.qtd_recebida IS NOT NULL THEN
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'quantidade', 'ok',
        'prevista=' || v_linha.qtd_prevista || ' recebida=' || v_linha.qtd_recebida);
    ELSE
      INSERT INTO staging_pcr_validacao_log (importacao_id, linha_id, regra, resultado, detalhe)
      VALUES (p_importacao_id, v_linha.id, 'quantidade', 'aviso',
        'Quantidade prevista ou recebida ausente');
    END IF;

    -- Linha valida
    UPDATE staging_pcr_linhas SET status = 'valida' WHERE id = v_linha.id;
    v_validas := v_validas + 1;

  END LOOP;

  -- Atualizar importacao
  UPDATE staging_pcr_importacao
  SET
    status = 'validada',
    total_linhas = v_total,
    total_documentos = (
      SELECT COUNT(DISTINCT numero_documento)
      FROM staging_pcr_linhas
      WHERE importacao_id = p_importacao_id AND status = 'valida'
    ),
    atualizado_em = NOW()
  WHERE id = p_importacao_id;

  RETURN v_total;
END;
$$;

COMMENT ON FUNCTION fn_validar_staging_pcr(UUID) IS
  'Valida todas as linhas de uma importacao PCR: campos obrigatorios, duplicatas, quantidades. Marca cada linha como valida/duplicada/erro.';


-- ──────────────────────────────────────────────────────────────────
-- fn_promover_staging_pcr(importacao_id UUID)
--
-- Move linhas validadas (status='valida') para as tabelas finais:
--   - pcr_documentos: 1 registro por numero_documento (agrupado)
--   - pcr_produtos: 1 registro por linha/produto
--   - pcr_qualidade_defeitos: parseado de qualidade_texto via fn_parse_qualidade_texto
--
-- Tambem:
--   - Calcula diferenca_kg = qtd_recebida - qtd_prevista
--   - Auto-atribui responsavel_carteira de pcr_carteira_fornecedores
--   - Atualiza staging status para 'importada'
--
-- Retorna: total de documentos promovidos
-- ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_promover_staging_pcr(p_importacao_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_doc            RECORD;
  v_linha          RECORD;
  v_doc_id         UUID;
  v_prod_id        UUID;
  v_defeitos       JSONB;
  v_defeito_key    TEXT;
  v_defeito_val    INTEGER;
  v_total_docs     INTEGER := 0;
  v_responsavel    TEXT;
  v_imp_status     TEXT;
BEGIN
  -- Verificar se importacao esta validada
  SELECT status INTO v_imp_status
  FROM staging_pcr_importacao
  WHERE id = p_importacao_id;

  IF v_imp_status IS NULL THEN
    RAISE EXCEPTION 'Importacao % nao encontrada', p_importacao_id;
  END IF;

  IF v_imp_status != 'validada' THEN
    RAISE EXCEPTION 'Importacao % nao esta validada (status atual: %)', p_importacao_id, v_imp_status;
  END IF;

  -- Iterar documentos distintos das linhas validas
  FOR v_doc IN
    SELECT DISTINCT ON (numero_documento)
      numero_documento,
      data_pcr,
      motorista,
      participante,
      origem,
      placa
    FROM staging_pcr_linhas
    WHERE importacao_id = p_importacao_id
      AND status = 'valida'
    ORDER BY numero_documento, linha_numero
  LOOP
    -- Buscar responsavel_carteira pelo participante (fornecedor)
    v_responsavel := NULL;
    BEGIN
      SELECT responsavel INTO v_responsavel
      FROM pcr_carteira_fornecedores
      WHERE UPPER(fornecedor) = UPPER(v_doc.participante)
        AND ativo = TRUE
      LIMIT 1;
    EXCEPTION WHEN undefined_table THEN
      -- pcr_carteira_fornecedores ainda nao existe, ignorar
      v_responsavel := NULL;
    END;

    -- Inserir documento PCR
    INSERT INTO pcr_documentos (
      numero_documento, data_pcr, motorista, participante,
      origem, placa, responsavel_carteira,
      importacao_id, criado_em
    ) VALUES (
      v_doc.numero_documento, v_doc.data_pcr, v_doc.motorista,
      v_doc.participante, v_doc.origem, v_doc.placa,
      v_responsavel,
      p_importacao_id, NOW()
    )
    RETURNING id INTO v_doc_id;

    v_total_docs := v_total_docs + 1;

    -- Inserir produtos deste documento
    FOR v_linha IN
      SELECT * FROM staging_pcr_linhas
      WHERE importacao_id = p_importacao_id
        AND numero_documento = v_doc.numero_documento
        AND status = 'valida'
      ORDER BY linha_numero
    LOOP
      INSERT INTO pcr_produtos (
        documento_id, codigo_produto, descricao_produto,
        qtd_prevista, qtd_recebida,
        diferenca_kg,
        qualidade_texto,
        importacao_id, criado_em
      ) VALUES (
        v_doc_id, v_linha.codigo_produto, v_linha.descricao_produto,
        v_linha.qtd_prevista, v_linha.qtd_recebida,
        COALESCE(v_linha.qtd_recebida, 0) - COALESCE(v_linha.qtd_prevista, 0),
        v_linha.qualidade_texto,
        p_importacao_id, NOW()
      )
      RETURNING id INTO v_prod_id;

      -- Parsear qualidade_texto em defeitos individuais
      IF v_linha.qualidade_texto IS NOT NULL AND TRIM(v_linha.qualidade_texto) != '' THEN
        v_defeitos := fn_parse_qualidade_texto(v_linha.qualidade_texto);

        FOR v_defeito_key, v_defeito_val IN
          SELECT key, value::INTEGER
          FROM jsonb_each_text(v_defeitos)
        LOOP
          IF v_defeito_val > 0 THEN
            INSERT INTO pcr_qualidade_defeitos (
              produto_id, tipo_defeito, quantidade, criado_em
            ) VALUES (
              v_prod_id, v_defeito_key, v_defeito_val, NOW()
            );
          END IF;
        END LOOP;
      END IF;

    END LOOP; -- fim produtos

  END LOOP; -- fim documentos

  -- Atualizar staging
  UPDATE staging_pcr_importacao
  SET
    status = 'importada',
    total_documentos = v_total_docs,
    atualizado_em = NOW()
  WHERE id = p_importacao_id;

  -- Marcar linhas promovidas
  UPDATE staging_pcr_linhas
  SET status = 'valida'  -- manter como valida (ja promovida)
  WHERE importacao_id = p_importacao_id
    AND status = 'valida';

  RETURN v_total_docs;
END;
$$;

COMMENT ON FUNCTION fn_promover_staging_pcr(UUID) IS
  'Move linhas validadas da staging para pcr_documentos + pcr_produtos + pcr_qualidade_defeitos. Agrupa por numero_documento, calcula diferenca_kg, parsea defeitos, auto-atribui responsavel_carteira.';


-- ══════════════════════════════════════════════════════════════════
-- VERIFICACAO
-- ══════════════════════════════════════════════════════════════════

DO $$
BEGIN
  RAISE NOTICE '── staging_schema.sql executado com sucesso ──';
  RAISE NOTICE 'Tabelas: staging_pcr_importacao, staging_pcr_linhas, staging_pcr_validacao_log';
  RAISE NOTICE 'Funcoes: fn_parse_qualidade_texto, fn_validar_staging_pcr, fn_promover_staging_pcr';
  RAISE NOTICE 'RLS: desabilitado em todas as staging tables';
END;
$$;
