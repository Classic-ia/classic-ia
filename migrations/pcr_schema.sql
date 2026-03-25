-- ============================================================================
-- PCR (Purchase Receipt / Protocolo de Conferencia de Recebimento) Schema
-- Sistema Controle de Qualidade
-- Generated: 2026-03-25
-- PostgreSQL 15+
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. ENUM TYPES
-- ============================================================================

-- Status do ciclo de vida da PCR
DO $$ BEGIN
    CREATE TYPE pcr_status AS ENUM (
        'importada',
        'em_apuracao',
        'aguardando_decisao',
        'decidida',
        'executada',
        'encerrada'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Tipo de decisao de desconto
DO $$ BEGIN
    CREATE TYPE decisao_tipo AS ENUM (
        'pode_pagar',
        'descontar_quantidade',
        'descontar_qualidade',
        'descontar_ambos',
        'nao_descontar',
        'descontar_proxima_nota'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Responsavel pela carteira de fornecedor
DO $$ BEGIN
    CREATE TYPE carteira_responsavel AS ENUM (
        'JEAN',
        'DORLY'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ============================================================================
-- 2. TABLES
-- ============================================================================

-- --------------------------------------------------------------------------
-- 2.1 pcr_documentos - Cabecalho principal da PCR
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_documentos (
    id                    BIGSERIAL       PRIMARY KEY,
    numero_documento      VARCHAR(50)     NOT NULL,
    data_pcr              DATE            NOT NULL DEFAULT CURRENT_DATE,
    motorista             VARCHAR(200),
    participante_codigo   VARCHAR(50),
    participante_nome     VARCHAR(300),
    origem                VARCHAR(300),
    placa                 VARCHAR(20),
    status                pcr_status      NOT NULL DEFAULT 'importada',
    responsavel_carteira  carteira_responsavel,
    criado_em             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    atualizado_em         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pcr_numero_documento UNIQUE (numero_documento)
);

COMMENT ON TABLE  pcr_documentos IS 'Cabecalho da PCR - um registro por documento de recebimento';
COMMENT ON COLUMN pcr_documentos.numero_documento IS 'Numero unico do documento (ex: PCR-2026-00001)';
COMMENT ON COLUMN pcr_documentos.participante_codigo IS 'Codigo do fornecedor no ERP (ATAK)';
COMMENT ON COLUMN pcr_documentos.responsavel_carteira IS 'Gestor responsavel pela carteira do fornecedor';


-- --------------------------------------------------------------------------
-- 2.2 pcr_produtos - Produtos por PCR (quantidade prevista vs recebida)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_produtos (
    id                BIGSERIAL     PRIMARY KEY,
    pcr_id            BIGINT        NOT NULL REFERENCES pcr_documentos(id) ON DELETE CASCADE,
    codigo_produto    VARCHAR(50)   NOT NULL,
    descricao         VARCHAR(500),
    qtd_prevista      NUMERIC(12,3) NOT NULL DEFAULT 0,
    qtd_recebida      NUMERIC(12,3) NOT NULL DEFAULT 0,
    diferenca_kg      NUMERIC(12,3) GENERATED ALWAYS AS (qtd_prevista - qtd_recebida) STORED,
    preco_unitario    NUMERIC(12,4) NOT NULL DEFAULT 0,
    valor_diferenca   NUMERIC(14,2) GENERATED ALWAYS AS ((qtd_prevista - qtd_recebida) * preco_unitario) STORED
);

COMMENT ON TABLE  pcr_produtos IS 'Itens/produtos de cada PCR com diferenca de quantidade';
COMMENT ON COLUMN pcr_produtos.diferenca_kg IS 'Calculado: qtd_prevista - qtd_recebida (positivo = falta)';
COMMENT ON COLUMN pcr_produtos.valor_diferenca IS 'Calculado: diferenca_kg * preco_unitario';


-- --------------------------------------------------------------------------
-- 2.3 pcr_qualidade_defeitos - Defeitos de qualidade por PCR
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_qualidade_defeitos (
    id                  BIGSERIAL     PRIMARY KEY,
    pcr_id              BIGINT        NOT NULL REFERENCES pcr_documentos(id) ON DELETE CASCADE,

    -- Tipos de defeito (valores em kg ou unidades)
    fermentacao         NUMERIC(10,3) NOT NULL DEFAULT 0,
    sem_folhas          NUMERIC(10,3) NOT NULL DEFAULT 0,
    folhas_arrebentadas NUMERIC(10,3) NOT NULL DEFAULT 0,
    pedaco              NUMERIC(10,3) NOT NULL DEFAULT 0,
    rasgado             NUMERIC(10,3) NOT NULL DEFAULT 0,
    furado              NUMERIC(10,3) NOT NULL DEFAULT 0,
    sujo                NUMERIC(10,3) NOT NULL DEFAULT 0,
    corte_irregular     NUMERIC(10,3) NOT NULL DEFAULT 0,
    mau_cheiro          NUMERIC(10,3) NOT NULL DEFAULT 0,
    contaminacao        NUMERIC(10,3) NOT NULL DEFAULT 0,
    outro               NUMERIC(10,3) NOT NULL DEFAULT 0,

    -- Totalizadores
    qtd_analisada       NUMERIC(12,3) NOT NULL DEFAULT 0,
    pct_desconto        NUMERIC(6,2)  NOT NULL DEFAULT 0,   -- percentual de desconto apurado
    pct_corretiva       NUMERIC(6,2)  NOT NULL DEFAULT 0,   -- percentual de acao corretiva
    classificacao       VARCHAR(100)                         -- ex: A, B, C ou descritivo
);

COMMENT ON TABLE  pcr_qualidade_defeitos IS 'Registro de defeitos de qualidade encontrados na inspecao';
COMMENT ON COLUMN pcr_qualidade_defeitos.pct_desconto IS 'Percentual total de desconto por qualidade';
COMMENT ON COLUMN pcr_qualidade_defeitos.classificacao IS 'Classificacao geral do lote (A/B/C ou descritivo)';


-- --------------------------------------------------------------------------
-- 2.4 pcr_decisoes - Decisoes de desconto
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_decisoes (
    id                          BIGSERIAL     PRIMARY KEY,
    pcr_id                      BIGINT        NOT NULL REFERENCES pcr_documentos(id) ON DELETE CASCADE,
    decisao_tipo                decisao_tipo  NOT NULL,
    valor_desconto_quantidade   NUMERIC(14,2) NOT NULL DEFAULT 0,
    valor_desconto_qualidade    NUMERIC(14,2) NOT NULL DEFAULT 0,
    valor_desconto_total        NUMERIC(14,2) GENERATED ALWAYS AS (
                                    valor_desconto_quantidade + valor_desconto_qualidade
                                ) STORED,
    obs_desconto                TEXT,
    decidido_por                VARCHAR(200)  NOT NULL,
    decidido_em                 TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  pcr_decisoes IS 'Decisao gerencial sobre desconto a ser aplicado';
COMMENT ON COLUMN pcr_decisoes.valor_desconto_total IS 'Calculado: quantidade + qualidade';


-- --------------------------------------------------------------------------
-- 2.5 pcr_execucoes - Execucao financeira do desconto
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_execucoes (
    id                    BIGSERIAL     PRIMARY KEY,
    pcr_id                BIGINT        NOT NULL REFERENCES pcr_documentos(id) ON DELETE CASCADE,
    executado_por         VARCHAR(200)  NOT NULL,
    executado_em          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    numero_nota_desconto  VARCHAR(100),
    obs_execucao          TEXT
);

COMMENT ON TABLE  pcr_execucoes IS 'Registro da execucao financeira (abatimento em nota, etc.)';


-- --------------------------------------------------------------------------
-- 2.6 pcr_auditoria - Trilha de auditoria
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_auditoria (
    id              BIGSERIAL     PRIMARY KEY,
    pcr_id          BIGINT        REFERENCES pcr_documentos(id) ON DELETE SET NULL,
    acao            VARCHAR(50)   NOT NULL,  -- INSERT, UPDATE, DELETE, STATUS_CHANGE
    campo           VARCHAR(100),
    valor_anterior  TEXT,
    valor_novo      TEXT,
    usuario         VARCHAR(200),
    criado_em       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE pcr_auditoria IS 'Log de auditoria para todas as alteracoes em PCRs';


-- --------------------------------------------------------------------------
-- 2.7 pcr_carteira_fornecedores - Mapeamento fornecedor -> gestor
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pcr_carteira_fornecedores (
    id                BIGSERIAL            PRIMARY KEY,
    fornecedor_codigo VARCHAR(50),
    fornecedor_nome   VARCHAR(300)         NOT NULL,
    responsavel       carteira_responsavel NOT NULL,
    vigencia_inicio   DATE                 NOT NULL DEFAULT CURRENT_DATE,
    vigencia_fim      DATE,

    CONSTRAINT uq_carteira_fornecedor_vigencia UNIQUE (fornecedor_nome, vigencia_inicio)
);

COMMENT ON TABLE pcr_carteira_fornecedores IS 'Define qual gestor e responsavel por cada fornecedor';


-- ============================================================================
-- 3. ROW-LEVEL SECURITY (desabilitada por simplicidade)
-- ============================================================================

ALTER TABLE pcr_documentos             DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_produtos               DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_qualidade_defeitos     DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_decisoes               DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_execucoes              DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_auditoria              DISABLE ROW LEVEL SECURITY;
ALTER TABLE pcr_carteira_fornecedores  DISABLE ROW LEVEL SECURITY;


-- ============================================================================
-- 4. INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pcr_documentos_status
    ON pcr_documentos (status);

CREATE INDEX IF NOT EXISTS idx_pcr_documentos_data_pcr
    ON pcr_documentos (data_pcr DESC);

CREATE INDEX IF NOT EXISTS idx_pcr_documentos_participante
    ON pcr_documentos (participante_codigo);

CREATE INDEX IF NOT EXISTS idx_pcr_documentos_responsavel
    ON pcr_documentos (responsavel_carteira);

CREATE INDEX IF NOT EXISTS idx_pcr_produtos_pcr_id
    ON pcr_produtos (pcr_id);

CREATE INDEX IF NOT EXISTS idx_pcr_qualidade_pcr_id
    ON pcr_qualidade_defeitos (pcr_id);

CREATE INDEX IF NOT EXISTS idx_pcr_decisoes_pcr_id
    ON pcr_decisoes (pcr_id);

CREATE INDEX IF NOT EXISTS idx_pcr_execucoes_pcr_id
    ON pcr_execucoes (pcr_id);

CREATE INDEX IF NOT EXISTS idx_pcr_auditoria_pcr_id
    ON pcr_auditoria (pcr_id);

CREATE INDEX IF NOT EXISTS idx_pcr_auditoria_criado_em
    ON pcr_auditoria (criado_em DESC);

CREATE INDEX IF NOT EXISTS idx_pcr_carteira_fornecedor_nome
    ON pcr_carteira_fornecedores (fornecedor_nome);

CREATE INDEX IF NOT EXISTS idx_pcr_carteira_responsavel
    ON pcr_carteira_fornecedores (responsavel);


-- ============================================================================
-- 5. FUNCTIONS
-- ============================================================================

-- --------------------------------------------------------------------------
-- 5.1 fn_calcular_desconto_quantidade
--     Calcula o desconto de quantidade total para uma PCR
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calcular_desconto_quantidade(p_pcr_id BIGINT)
RETURNS NUMERIC(14,2)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_total NUMERIC(14,2);
BEGIN
    SELECT COALESCE(SUM(
        CASE
            WHEN (qtd_prevista - qtd_recebida) > 0
            THEN (qtd_prevista - qtd_recebida) * preco_unitario
            ELSE 0
        END
    ), 0)
    INTO v_total
    FROM pcr_produtos
    WHERE pcr_id = p_pcr_id;

    RETURN v_total;
END;
$$;

COMMENT ON FUNCTION fn_calcular_desconto_quantidade(BIGINT)
    IS 'Soma o valor da diferenca positiva (falta) de todos os produtos da PCR';


-- --------------------------------------------------------------------------
-- 5.2 fn_atualizar_status_pcr
--     Trigger function: atualiza automaticamente o status da PCR
--     conforme o estado das tabelas relacionadas
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_atualizar_status_pcr()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_pcr_id       BIGINT;
    v_tem_decisao  BOOLEAN;
    v_tem_execucao BOOLEAN;
    v_novo_status  pcr_status;
BEGIN
    -- Determinar o pcr_id conforme a tabela de origem
    IF TG_TABLE_NAME = 'pcr_documentos' THEN
        v_pcr_id := COALESCE(NEW.id, OLD.id);
    ELSIF TG_TABLE_NAME = 'pcr_decisoes' THEN
        v_pcr_id := COALESCE(NEW.pcr_id, OLD.pcr_id);
    ELSIF TG_TABLE_NAME = 'pcr_execucoes' THEN
        v_pcr_id := COALESCE(NEW.pcr_id, OLD.pcr_id);
    ELSE
        RETURN COALESCE(NEW, OLD);
    END IF;

    -- Verificar se existe decisao e execucao
    SELECT EXISTS(SELECT 1 FROM pcr_decisoes  WHERE pcr_id = v_pcr_id) INTO v_tem_decisao;
    SELECT EXISTS(SELECT 1 FROM pcr_execucoes WHERE pcr_id = v_pcr_id) INTO v_tem_execucao;

    -- Determinar o novo status
    IF v_tem_execucao THEN
        v_novo_status := 'executada';
    ELSIF v_tem_decisao THEN
        v_novo_status := 'decidida';
    ELSE
        -- Manter status atual se ja esta em apuracao ou aguardando
        SELECT status INTO v_novo_status
        FROM pcr_documentos
        WHERE id = v_pcr_id;

        -- Se ainda esta como importada, nao alterar automaticamente
        IF v_novo_status IS NULL THEN
            v_novo_status := 'importada';
        END IF;
    END IF;

    -- Atualizar o documento
    UPDATE pcr_documentos
    SET status        = v_novo_status,
        atualizado_em = NOW()
    WHERE id = v_pcr_id
      AND status IS DISTINCT FROM v_novo_status;

    RETURN COALESCE(NEW, OLD);
END;
$$;

COMMENT ON FUNCTION fn_atualizar_status_pcr()
    IS 'Trigger: atualiza status da PCR automaticamente com base em decisoes/execucoes';


-- --------------------------------------------------------------------------
-- 5.3 fn_registrar_auditoria
--     Trigger function: registra alteracoes na tabela de auditoria
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_registrar_auditoria()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_pcr_id  BIGINT;
    v_usuario VARCHAR(200);
BEGIN
    -- Determinar pcr_id
    IF TG_TABLE_NAME = 'pcr_documentos' THEN
        v_pcr_id := COALESCE(NEW.id, OLD.id);
    ELSE
        v_pcr_id := COALESCE(NEW.pcr_id, OLD.pcr_id);
    END IF;

    -- Tentar obter usuario da sessao (se configurado via SET LOCAL)
    BEGIN
        v_usuario := current_setting('app.current_user', TRUE);
    EXCEPTION WHEN OTHERS THEN
        v_usuario := session_user;
    END;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO pcr_auditoria (pcr_id, acao, campo, valor_anterior, valor_novo, usuario)
        VALUES (v_pcr_id, 'INSERT', TG_TABLE_NAME, NULL, row_to_json(NEW)::TEXT, v_usuario);

    ELSIF TG_OP = 'UPDATE' THEN
        -- Para pcr_documentos, registrar mudanca de status especificamente
        IF TG_TABLE_NAME = 'pcr_documentos' AND OLD.status IS DISTINCT FROM NEW.status THEN
            INSERT INTO pcr_auditoria (pcr_id, acao, campo, valor_anterior, valor_novo, usuario)
            VALUES (v_pcr_id, 'STATUS_CHANGE', 'status', OLD.status::TEXT, NEW.status::TEXT, v_usuario);
        END IF;

        -- Log generico da alteracao
        INSERT INTO pcr_auditoria (pcr_id, acao, campo, valor_anterior, valor_novo, usuario)
        VALUES (v_pcr_id, 'UPDATE', TG_TABLE_NAME, row_to_json(OLD)::TEXT, row_to_json(NEW)::TEXT, v_usuario);

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO pcr_auditoria (pcr_id, acao, campo, valor_anterior, valor_novo, usuario)
        VALUES (v_pcr_id, 'DELETE', TG_TABLE_NAME, row_to_json(OLD)::TEXT, NULL, v_usuario);
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$;

COMMENT ON FUNCTION fn_registrar_auditoria()
    IS 'Trigger: registra todas as alteracoes em pcr_auditoria para rastreabilidade';


-- ============================================================================
-- 6. TRIGGERS
-- ============================================================================

-- Atualizar status automaticamente
DROP TRIGGER IF EXISTS trg_pcr_documentos_status  ON pcr_documentos;
DROP TRIGGER IF EXISTS trg_pcr_decisoes_status     ON pcr_decisoes;
DROP TRIGGER IF EXISTS trg_pcr_execucoes_status    ON pcr_execucoes;

CREATE TRIGGER trg_pcr_documentos_status
    AFTER UPDATE ON pcr_documentos
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION fn_atualizar_status_pcr();

CREATE TRIGGER trg_pcr_decisoes_status
    AFTER INSERT OR DELETE ON pcr_decisoes
    FOR EACH ROW
    EXECUTE FUNCTION fn_atualizar_status_pcr();

CREATE TRIGGER trg_pcr_execucoes_status
    AFTER INSERT OR DELETE ON pcr_execucoes
    FOR EACH ROW
    EXECUTE FUNCTION fn_atualizar_status_pcr();

-- Auditoria
DROP TRIGGER IF EXISTS trg_auditoria_documentos ON pcr_documentos;
DROP TRIGGER IF EXISTS trg_auditoria_decisoes   ON pcr_decisoes;
DROP TRIGGER IF EXISTS trg_auditoria_execucoes  ON pcr_execucoes;

CREATE TRIGGER trg_auditoria_documentos
    AFTER INSERT OR UPDATE OR DELETE ON pcr_documentos
    FOR EACH ROW
    EXECUTE FUNCTION fn_registrar_auditoria();

CREATE TRIGGER trg_auditoria_decisoes
    AFTER INSERT OR UPDATE OR DELETE ON pcr_decisoes
    FOR EACH ROW
    EXECUTE FUNCTION fn_registrar_auditoria();

CREATE TRIGGER trg_auditoria_execucoes
    AFTER INSERT OR UPDATE OR DELETE ON pcr_execucoes
    FOR EACH ROW
    EXECUTE FUNCTION fn_registrar_auditoria();


-- ============================================================================
-- 7. VIEWS
-- ============================================================================

-- --------------------------------------------------------------------------
-- 7.1 vw_pcr_resumo - Visao consolidada da PCR
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pcr_resumo AS
SELECT
    d.id,
    d.numero_documento,
    d.data_pcr,
    d.motorista,
    d.participante_codigo,
    d.participante_nome,
    d.origem,
    d.placa,
    d.status,
    d.responsavel_carteira,
    d.criado_em,
    d.atualizado_em,

    -- Agregado de produtos
    COALESCE(p.total_produtos, 0)            AS total_produtos,
    COALESCE(p.total_qtd_prevista, 0)        AS total_qtd_prevista,
    COALESCE(p.total_qtd_recebida, 0)        AS total_qtd_recebida,
    COALESCE(p.total_diferenca_kg, 0)        AS total_diferenca_kg,
    COALESCE(p.total_valor_diferenca, 0)     AS total_valor_diferenca,

    -- Qualidade
    q.qtd_analisada                          AS qualidade_qtd_analisada,
    q.pct_desconto                           AS qualidade_pct_desconto,
    q.pct_corretiva                          AS qualidade_pct_corretiva,
    q.classificacao                          AS qualidade_classificacao,

    -- Decisao (ultima)
    dec.decisao_tipo,
    dec.valor_desconto_quantidade,
    dec.valor_desconto_qualidade,
    dec.valor_desconto_total,
    dec.obs_desconto,
    dec.decidido_por,
    dec.decidido_em,

    -- Execucao (ultima)
    ex.executado_por,
    ex.executado_em,
    ex.numero_nota_desconto,
    ex.obs_execucao

FROM pcr_documentos d

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)            AS total_produtos,
        SUM(qtd_prevista)   AS total_qtd_prevista,
        SUM(qtd_recebida)   AS total_qtd_recebida,
        SUM(qtd_prevista - qtd_recebida) AS total_diferenca_kg,
        SUM((qtd_prevista - qtd_recebida) * preco_unitario) AS total_valor_diferenca
    FROM pcr_produtos
    WHERE pcr_id = d.id
) p ON TRUE

LEFT JOIN LATERAL (
    SELECT *
    FROM pcr_qualidade_defeitos
    WHERE pcr_id = d.id
    ORDER BY id DESC
    LIMIT 1
) q ON TRUE

LEFT JOIN LATERAL (
    SELECT *
    FROM pcr_decisoes
    WHERE pcr_id = d.id
    ORDER BY decidido_em DESC
    LIMIT 1
) dec ON TRUE

LEFT JOIN LATERAL (
    SELECT *
    FROM pcr_execucoes
    WHERE pcr_id = d.id
    ORDER BY executado_em DESC
    LIMIT 1
) ex ON TRUE;

COMMENT ON VIEW vw_pcr_resumo IS 'Visao consolidada: documento + produtos + qualidade + decisao + execucao';


-- --------------------------------------------------------------------------
-- 7.2 vw_pcr_dashboard_fornecedor - Estatisticas por fornecedor
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pcr_dashboard_fornecedor AS
SELECT
    d.participante_codigo,
    d.participante_nome,
    d.responsavel_carteira,
    COUNT(d.id)                                             AS total_pcrs,
    COUNT(d.id) FILTER (WHERE d.status = 'decidida')        AS pcrs_decididas,
    COUNT(d.id) FILTER (WHERE d.status = 'executada')       AS pcrs_executadas,
    COALESCE(AVG(q.pct_desconto), 0)                        AS media_pct_desconto,
    COALESCE(MAX(q.pct_desconto), 0)                        AS max_pct_desconto,
    COALESCE(SUM(dec.valor_desconto_total), 0)              AS total_descontado,

    -- Pior PCR (maior desconto percentual)
    (
        SELECT r.numero_documento
        FROM pcr_documentos r
        LEFT JOIN pcr_qualidade_defeitos rq ON rq.pcr_id = r.id
        WHERE r.participante_codigo = d.participante_codigo
        ORDER BY COALESCE(rq.pct_desconto, 0) DESC
        LIMIT 1
    ) AS pior_pcr_numero

FROM pcr_documentos d
LEFT JOIN pcr_qualidade_defeitos q   ON q.pcr_id = d.id
LEFT JOIN LATERAL (
    SELECT valor_desconto_total
    FROM pcr_decisoes
    WHERE pcr_id = d.id
    ORDER BY decidido_em DESC
    LIMIT 1
) dec ON TRUE

GROUP BY d.participante_codigo, d.participante_nome, d.responsavel_carteira;

COMMENT ON VIEW vw_pcr_dashboard_fornecedor
    IS 'Estatisticas agregadas por fornecedor: media desconto, total PCRs, pior PCR';


-- --------------------------------------------------------------------------
-- 7.3 vw_pcr_pendentes - PCRs aguardando acao
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pcr_pendentes AS
SELECT
    d.id,
    d.numero_documento,
    d.data_pcr,
    d.participante_nome,
    d.responsavel_carteira,
    d.status,
    d.criado_em,
    CURRENT_DATE - d.data_pcr AS dias_aberta,

    CASE
        WHEN d.status IN ('importada', 'em_apuracao')       THEN 'Aguardando apuracao'
        WHEN d.status = 'aguardando_decisao'                THEN 'Aguardando decisao gerencial'
        WHEN d.status = 'decidida'                          THEN 'Aguardando execucao financeira'
        ELSE 'Indefinido'
    END AS proxima_acao

FROM pcr_documentos d
WHERE d.status NOT IN ('executada', 'encerrada')
ORDER BY d.data_pcr ASC;

COMMENT ON VIEW vw_pcr_pendentes
    IS 'PCRs que ainda precisam de alguma acao (nao executadas nem encerradas)';


-- ============================================================================
-- 8. SEED DATA - Carteira de fornecedores
-- ============================================================================

INSERT INTO pcr_carteira_fornecedores (fornecedor_nome, responsavel, vigencia_inicio)
VALUES
    -- Carteira JEAN
    ('FRIGONOR',                        'JEAN',  '2025-01-01'),
    ('MATADOURO REGIONAL DE BRUSQUE',   'JEAN',  '2025-01-01'),
    ('FLORAI',                          'JEAN',  '2025-01-01'),
    ('MONTALFRIG',                      'JEAN',  '2025-01-01'),
    ('FRIGODASKO',                      'JEAN',  '2025-01-01'),
    ('BEZERROS',                        'JEAN',  '2025-01-01'),

    -- Carteira DORLY
    ('GOLD CARNES',                     'DORLY', '2025-01-01'),
    ('CALIBRE DISTRIBUIDORA',           'DORLY', '2025-01-01'),
    ('MA FOOD',                         'DORLY', '2025-01-01'),
    ('FRIGORIFICO MONDELLI',            'DORLY', '2025-01-01'),
    ('MEAT WORLD',                      'DORLY', '2025-01-01')
ON CONFLICT (fornecedor_nome, vigencia_inicio) DO NOTHING;


COMMIT;
