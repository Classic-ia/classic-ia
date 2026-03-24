-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 06: Padronização de IDs e Relacionamentos
-- Eliminar texto-livre em campos de referência — tudo por UUID FK
-- Rodar APÓS 05_migration_ranking_qualidade.sql
--
-- ESTRATÉGIA:
--   1. Adicionar colunas _id (UUID FK) ao lado das textuais existentes
--   2. Criar function de resolução que popula _id a partir do texto
--   3. Trigger on INSERT/UPDATE que resolve automaticamente
--   4. Views atualizadas usam JOINs por _id
--   5. Textos originais mantidos como cache (read-only, populados por trigger)
--
-- NÃO remove colunas texto (breaking change) — apenas adiciona _id + FK + trigger
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELAS DE REFERÊNCIA AUSENTES
-- ════════════════════════════════════════════════════════════════════════════

-- 1.1 Transportadoras (referenciada em inspeção, carga, embarque)
CREATE TABLE IF NOT EXISTS cq_transportadoras (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo      TEXT UNIQUE,
  nome        TEXT NOT NULL,
  cnpj        TEXT UNIQUE,
  uf          TEXT,
  ativo       BOOLEAN DEFAULT true,
  criado_em   TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transportadora_nome ON cq_transportadoras(nome);

ALTER TABLE cq_transportadoras ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth_read_transportadoras" ON cq_transportadoras FOR SELECT USING (true);
CREATE POLICY "auth_all_transportadoras" ON cq_transportadoras FOR ALL USING (true);

-- 1.2 Frigoríficos (distintos de fornecedores genéricos)
-- Já estão em cadastros_atak com tipo='fornecedor', mas campo frigorifico em
-- cq_cargas/atak_cargas_raw pode não ter match exato.
-- Solução: usar cadastros_atak (tipo=fornecedor) como lookup.


-- ════════════════════════════════════════════════════════════════════════════
-- 2. ADICIONAR COLUNAS _id ÀS TABELAS EXISTENTES
-- ════════════════════════════════════════════════════════════════════════════

-- ─── 2.1 registros_cq_inspecao ──────────────────────────────────────────
ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS produto_id       UUID REFERENCES cq_produtos(id),
  ADD COLUMN IF NOT EXISTS motorista_id     UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS veiculo_id       UUID REFERENCES cq_veiculos(id),
  ADD COLUMN IF NOT EXISTS transportadora_id UUID REFERENCES cq_transportadoras(id);

CREATE INDEX IF NOT EXISTS idx_inspecao_fornecedor_id ON registros_cq_inspecao(fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_inspecao_produto_id ON registros_cq_inspecao(produto_id);
CREATE INDEX IF NOT EXISTS idx_inspecao_motorista_id ON registros_cq_inspecao(motorista_id);
CREATE INDEX IF NOT EXISTS idx_inspecao_veiculo_id ON registros_cq_inspecao(veiculo_id);

-- ─── 2.2 cq_cargas ─────────────────────────────────────────────────────
ALTER TABLE cq_cargas
  ADD COLUMN IF NOT EXISTS motorista_id     UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS frigorifico_id   UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS veiculo_id       UUID REFERENCES cq_veiculos(id);

CREATE INDEX IF NOT EXISTS idx_cargas_motorista_id ON cq_cargas(motorista_id);
CREATE INDEX IF NOT EXISTS idx_cargas_frigorifico_id ON cq_cargas(frigorifico_id);

-- ─── 2.3 cq_recebimentos ───────────────────────────────────────────────
ALTER TABLE cq_recebimentos
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS produto_id       UUID REFERENCES cq_produtos(id),
  ADD COLUMN IF NOT EXISTS motorista_id     UUID REFERENCES cadastros_atak(id),
  ADD COLUMN IF NOT EXISTS veiculo_id       UUID REFERENCES cq_veiculos(id),
  ADD COLUMN IF NOT EXISTS transportadora_id UUID REFERENCES cq_transportadoras(id);

CREATE INDEX IF NOT EXISTS idx_receb_fornecedor_id ON cq_recebimentos(fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_receb_produto_id ON cq_recebimentos(produto_id);

-- ─── 2.4 cq_planos_acao ────────────────────────────────────────────────
ALTER TABLE cq_planos_acao
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id);

CREATE INDEX IF NOT EXISTS idx_planos_fornecedor_id ON cq_planos_acao(fornecedor_id);

-- ─── 2.5 cq_lote_inspecao ──────────────────────────────────────────────
ALTER TABLE cq_lote_inspecao
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id);

CREATE INDEX IF NOT EXISTS idx_lote_fornecedor_id ON cq_lote_inspecao(fornecedor_id);

-- ─── 2.6 cq_lote_produto (referenciar por UUID ao invés de TEXT) ────────
ALTER TABLE cq_lote_produto
  ADD COLUMN IF NOT EXISTS produto_id       UUID REFERENCES cq_ranking_produtos(id);

CREATE INDEX IF NOT EXISTS idx_lote_prod_produto_id ON cq_lote_produto(produto_id);

-- ─── 2.7 cq_fornecedor_score ───────────────────────────────────────────
ALTER TABLE cq_fornecedor_score
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id);

CREATE INDEX IF NOT EXISTS idx_score_fornecedor_id ON cq_fornecedor_score(fornecedor_id);

-- ─── 2.8 cq_fornecedor_score_historico ──────────────────────────────────
ALTER TABLE cq_fornecedor_score_historico
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id);

-- ─── 2.9 cq_validacao_divergencias ──────────────────────────────────────
ALTER TABLE cq_validacao_divergencias
  ADD COLUMN IF NOT EXISTS fornecedor_id    UUID REFERENCES cadastros_atak(id);


-- ════════════════════════════════════════════════════════════════════════════
-- 3. FUNCTION DE RESOLUÇÃO DE VÍNCULOS
-- Resolve texto → UUID usando lookup nas tabelas de referência
-- ════════════════════════════════════════════════════════════════════════════

-- 3.1 Resolver fornecedor (por código ou nome)
CREATE OR REPLACE FUNCTION fn_resolver_fornecedor(
  p_codigo TEXT DEFAULT NULL,
  p_nome TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  -- Primeiro por código (mais preciso)
  IF p_codigo IS NOT NULL AND p_codigo != '' THEN
    SELECT id INTO v_id FROM cadastros_atak
    WHERE codigo = p_codigo AND tipo = 'fornecedor' LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  -- Depois por nome exato
  IF p_nome IS NOT NULL AND p_nome != '' THEN
    SELECT id INTO v_id FROM cadastros_atak
    WHERE (nome = p_nome OR apelido = p_nome) AND tipo = 'fornecedor' LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
    -- Por nome parcial (ilike)
    SELECT id INTO v_id FROM cadastros_atak
    WHERE (nome ILIKE p_nome || '%' OR apelido ILIKE p_nome || '%') AND tipo = 'fornecedor'
    ORDER BY nome LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- 3.2 Resolver produto (por código ou nome)
CREATE OR REPLACE FUNCTION fn_resolver_produto(
  p_codigo TEXT DEFAULT NULL,
  p_nome TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  IF p_codigo IS NOT NULL AND p_codigo != '' THEN
    SELECT id INTO v_id FROM cq_produtos WHERE codigo = p_codigo LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
    SELECT id INTO v_id FROM cq_produtos WHERE codigo_atak = p_codigo LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  IF p_nome IS NOT NULL AND p_nome != '' THEN
    SELECT id INTO v_id FROM cq_produtos WHERE nome = p_nome LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
    SELECT id INTO v_id FROM cq_produtos WHERE nome ILIKE p_nome || '%' ORDER BY nome LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- 3.3 Resolver motorista
CREATE OR REPLACE FUNCTION fn_resolver_motorista(
  p_codigo TEXT DEFAULT NULL,
  p_nome TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  IF p_codigo IS NOT NULL AND p_codigo != '' THEN
    SELECT id INTO v_id FROM cadastros_atak
    WHERE codigo = p_codigo AND tipo = 'motorista' LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  IF p_nome IS NOT NULL AND p_nome != '' THEN
    SELECT id INTO v_id FROM cadastros_atak
    WHERE (nome = p_nome OR apelido = p_nome) AND tipo = 'motorista' LIMIT 1;
    IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- 3.4 Resolver veículo (por placa)
CREATE OR REPLACE FUNCTION fn_resolver_veiculo(p_placa TEXT)
RETURNS UUID AS $$
DECLARE
  v_id UUID;
  v_placa_normalizada TEXT;
BEGIN
  IF p_placa IS NULL OR p_placa = '' THEN RETURN NULL; END IF;
  v_placa_normalizada := UPPER(REGEXP_REPLACE(p_placa, '[^A-Z0-9]', '', 'g'));
  SELECT id INTO v_id FROM cq_veiculos
  WHERE UPPER(REGEXP_REPLACE(placa, '[^A-Z0-9]', '', 'g')) = v_placa_normalizada
  LIMIT 1;
  RETURN v_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- 3.5 Resolver transportadora
CREATE OR REPLACE FUNCTION fn_resolver_transportadora(p_nome TEXT)
RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  IF p_nome IS NULL OR p_nome = '' THEN RETURN NULL; END IF;
  SELECT id INTO v_id FROM cq_transportadoras WHERE nome = p_nome LIMIT 1;
  IF v_id IS NOT NULL THEN RETURN v_id; END IF;
  -- Auto-cadastrar se não existir
  INSERT INTO cq_transportadoras (nome) VALUES (p_nome)
  ON CONFLICT DO NOTHING
  RETURNING id INTO v_id;
  IF v_id IS NULL THEN
    SELECT id INTO v_id FROM cq_transportadoras WHERE nome = p_nome LIMIT 1;
  END IF;
  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- 3.6 Resolver produto de ranking (por código)
CREATE OR REPLACE FUNCTION fn_resolver_produto_ranking(p_codigo TEXT)
RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  IF p_codigo IS NULL OR p_codigo = '' THEN RETURN NULL; END IF;
  SELECT id INTO v_id FROM cq_ranking_produtos WHERE codigo = p_codigo LIMIT 1;
  RETURN v_id;
END;
$$ LANGUAGE plpgsql STABLE;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. TRIGGERS DE RESOLUÇÃO AUTOMÁTICA
-- Popula _id automaticamente ao inserir/atualizar com texto
-- ════════════════════════════════════════════════════════════════════════════

-- 4.1 registros_cq_inspecao
CREATE OR REPLACE FUNCTION fn_resolver_ids_inspecao()
RETURNS TRIGGER AS $$
BEGIN
  -- Fornecedor: resolver por código ou nome
  IF NEW.fornecedor_id IS NULL AND (NEW.fornecedor_codigo IS NOT NULL OR NEW.fornecedor IS NOT NULL) THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NEW.fornecedor);
  END IF;

  -- Produto
  IF NEW.produto_id IS NULL AND NEW.produto IS NOT NULL THEN
    NEW.produto_id := fn_resolver_produto(NULL, NEW.produto);
  END IF;

  -- Motorista
  IF NEW.motorista_id IS NULL AND NEW.motorista IS NOT NULL THEN
    NEW.motorista_id := fn_resolver_motorista(NULL, NEW.motorista);
  END IF;

  -- Veículo
  IF NEW.veiculo_id IS NULL AND NEW.placa_caminhao IS NOT NULL THEN
    NEW.veiculo_id := fn_resolver_veiculo(NEW.placa_caminhao);
  END IF;

  -- Transportadora
  IF NEW.transportadora_id IS NULL AND NEW.transportadora IS NOT NULL THEN
    NEW.transportadora_id := fn_resolver_transportadora(NEW.transportadora);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_resolver_ids_inspecao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_inspecao();

-- 4.2 cq_cargas
CREATE OR REPLACE FUNCTION fn_resolver_ids_carga()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.motorista_id IS NULL AND NEW.motorista IS NOT NULL THEN
    NEW.motorista_id := fn_resolver_motorista(NULL, NEW.motorista);
  END IF;
  IF NEW.frigorifico_id IS NULL AND NEW.frigorifico IS NOT NULL THEN
    NEW.frigorifico_id := fn_resolver_fornecedor(NULL, NEW.frigorifico);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_carga ON cq_cargas;
CREATE TRIGGER trg_resolver_ids_carga
  BEFORE INSERT OR UPDATE ON cq_cargas
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_carga();

-- 4.3 cq_recebimentos
CREATE OR REPLACE FUNCTION fn_resolver_ids_recebimento()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.fornecedor_id IS NULL AND (NEW.fornecedor_cod IS NOT NULL OR NEW.fornecedor IS NOT NULL) THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_cod, NEW.fornecedor);
  END IF;
  IF NEW.produto_id IS NULL AND NEW.produto IS NOT NULL THEN
    NEW.produto_id := fn_resolver_produto(NULL, NEW.produto);
  END IF;
  IF NEW.motorista_id IS NULL AND NEW.motorista IS NOT NULL THEN
    NEW.motorista_id := fn_resolver_motorista(NULL, NEW.motorista);
  END IF;
  IF NEW.veiculo_id IS NULL AND NEW.placa_caminhao IS NOT NULL THEN
    NEW.veiculo_id := fn_resolver_veiculo(NEW.placa_caminhao);
  END IF;
  IF NEW.transportadora_id IS NULL AND NEW.transportadora IS NOT NULL THEN
    NEW.transportadora_id := fn_resolver_transportadora(NEW.transportadora);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_recebimento ON cq_recebimentos;
CREATE TRIGGER trg_resolver_ids_recebimento
  BEFORE INSERT OR UPDATE ON cq_recebimentos
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_recebimento();

-- 4.4 cq_planos_acao
CREATE OR REPLACE FUNCTION fn_resolver_ids_plano()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.fornecedor_id IS NULL AND (NEW.fornecedor_cod IS NOT NULL OR NEW.fornecedor IS NOT NULL) THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_cod, NEW.fornecedor);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_plano ON cq_planos_acao;
CREATE TRIGGER trg_resolver_ids_plano
  BEFORE INSERT OR UPDATE ON cq_planos_acao
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_plano();

-- 4.5 cq_lote_inspecao
CREATE OR REPLACE FUNCTION fn_resolver_ids_lote()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.fornecedor_id IS NULL AND NEW.fornecedor_codigo IS NOT NULL THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NEW.fornecedor_nome);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_lote ON cq_lote_inspecao;
CREATE TRIGGER trg_resolver_ids_lote
  BEFORE INSERT OR UPDATE ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_lote();

-- 4.6 cq_lote_produto
CREATE OR REPLACE FUNCTION fn_resolver_ids_lote_produto()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.produto_id IS NULL AND NEW.produto_codigo IS NOT NULL THEN
    NEW.produto_id := fn_resolver_produto_ranking(NEW.produto_codigo);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_lote_produto ON cq_lote_produto;
CREATE TRIGGER trg_resolver_ids_lote_produto
  BEFORE INSERT OR UPDATE ON cq_lote_produto
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_lote_produto();

-- 4.7 cq_fornecedor_score
CREATE OR REPLACE FUNCTION fn_resolver_ids_score()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.fornecedor_id IS NULL AND NEW.fornecedor_codigo IS NOT NULL THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NULL);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_score ON cq_fornecedor_score;
CREATE TRIGGER trg_resolver_ids_score
  BEFORE INSERT OR UPDATE ON cq_fornecedor_score
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_score();

-- 4.8 cq_fornecedor_score_historico
DROP TRIGGER IF EXISTS trg_resolver_ids_score_hist ON cq_fornecedor_score_historico;
CREATE TRIGGER trg_resolver_ids_score_hist
  BEFORE INSERT OR UPDATE ON cq_fornecedor_score_historico
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_score();

-- 4.9 cq_validacao_divergencias
CREATE OR REPLACE FUNCTION fn_resolver_ids_divergencia()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.fornecedor_id IS NULL AND NEW.fornecedor_codigo IS NOT NULL THEN
    NEW.fornecedor_id := fn_resolver_fornecedor(NEW.fornecedor_codigo, NULL);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_resolver_ids_divergencia ON cq_validacao_divergencias;
CREATE TRIGGER trg_resolver_ids_divergencia
  BEFORE INSERT OR UPDATE ON cq_validacao_divergencias
  FOR EACH ROW EXECUTE FUNCTION fn_resolver_ids_divergencia();


-- ════════════════════════════════════════════════════════════════════════════
-- 5. BACKFILL: Popular _id para registros existentes
-- ════════════════════════════════════════════════════════════════════════════

-- Garantir que colunas texto de referência existam antes do backfill
-- 5.1 registros_cq_inspecao
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS fornecedor_codigo TEXT;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS fornecedor TEXT;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS produto TEXT;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS motorista TEXT;
ALTER TABLE registros_cq_inspecao ADD COLUMN IF NOT EXISTS placa_caminhao TEXT;
-- 5.2 cq_cargas
ALTER TABLE cq_cargas ADD COLUMN IF NOT EXISTS motorista TEXT;
ALTER TABLE cq_cargas ADD COLUMN IF NOT EXISTS frigorifico TEXT;
-- 5.3 cq_recebimentos
ALTER TABLE cq_recebimentos ADD COLUMN IF NOT EXISTS fornecedor_cod TEXT;
ALTER TABLE cq_recebimentos ADD COLUMN IF NOT EXISTS fornecedor TEXT;
ALTER TABLE cq_recebimentos ADD COLUMN IF NOT EXISTS produto TEXT;
ALTER TABLE cq_recebimentos ADD COLUMN IF NOT EXISTS motorista TEXT;
ALTER TABLE cq_recebimentos ADD COLUMN IF NOT EXISTS placa_caminhao TEXT;
-- 5.4 cq_planos_acao
ALTER TABLE cq_planos_acao ADD COLUMN IF NOT EXISTS fornecedor_cod TEXT;
ALTER TABLE cq_planos_acao ADD COLUMN IF NOT EXISTS fornecedor TEXT;
-- 5.5 cq_lote_inspecao
ALTER TABLE cq_lote_inspecao ADD COLUMN IF NOT EXISTS fornecedor_codigo TEXT;
ALTER TABLE cq_lote_inspecao ADD COLUMN IF NOT EXISTS fornecedor_nome TEXT;
-- 5.6 cq_lote_produto
ALTER TABLE cq_lote_produto ADD COLUMN IF NOT EXISTS produto_codigo TEXT;
-- 5.7/5.8 cq_fornecedor_score / historico
ALTER TABLE cq_fornecedor_score ADD COLUMN IF NOT EXISTS fornecedor_codigo TEXT;
ALTER TABLE cq_fornecedor_score_historico ADD COLUMN IF NOT EXISTS fornecedor_codigo TEXT;
-- 5.9 cq_validacao_divergencias
ALTER TABLE cq_validacao_divergencias ADD COLUMN IF NOT EXISTS fornecedor_codigo TEXT;

-- 5.1 registros_cq_inspecao
UPDATE registros_cq_inspecao SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_codigo, fornecedor),
  produto_id = fn_resolver_produto(NULL, produto),
  motorista_id = fn_resolver_motorista(NULL, motorista),
  veiculo_id = fn_resolver_veiculo(placa_caminhao)
WHERE fornecedor_id IS NULL OR produto_id IS NULL;

-- 5.2 cq_cargas
UPDATE cq_cargas SET
  motorista_id = fn_resolver_motorista(NULL, motorista),
  frigorifico_id = fn_resolver_fornecedor(NULL, frigorifico)
WHERE motorista_id IS NULL OR frigorifico_id IS NULL;

-- 5.3 cq_recebimentos
UPDATE cq_recebimentos SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_cod, fornecedor),
  produto_id = fn_resolver_produto(NULL, produto),
  motorista_id = fn_resolver_motorista(NULL, motorista),
  veiculo_id = fn_resolver_veiculo(placa_caminhao)
WHERE fornecedor_id IS NULL;

-- 5.4 cq_planos_acao
UPDATE cq_planos_acao SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_cod, fornecedor)
WHERE fornecedor_id IS NULL;

-- 5.5 cq_lote_inspecao
UPDATE cq_lote_inspecao SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_codigo, fornecedor_nome)
WHERE fornecedor_id IS NULL;

-- 5.6 cq_lote_produto
UPDATE cq_lote_produto SET
  produto_id = fn_resolver_produto_ranking(produto_codigo)
WHERE produto_id IS NULL;

-- 5.7 cq_fornecedor_score
UPDATE cq_fornecedor_score SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_codigo, NULL)
WHERE fornecedor_id IS NULL;

-- 5.8 cq_fornecedor_score_historico
UPDATE cq_fornecedor_score_historico SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_codigo, NULL)
WHERE fornecedor_id IS NULL;

-- 5.9 cq_validacao_divergencias
UPDATE cq_validacao_divergencias SET
  fornecedor_id = fn_resolver_fornecedor(fornecedor_codigo, NULL)
WHERE fornecedor_id IS NULL AND fornecedor_codigo IS NOT NULL;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. VIEW DE AUDITORIA: Registros sem vínculo
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_registros_sem_vinculo AS

SELECT 'registros_cq_inspecao' AS tabela, id, 'fornecedor' AS campo,
  fornecedor AS valor_texto, fornecedor_codigo AS codigo_texto
FROM registros_cq_inspecao WHERE fornecedor_id IS NULL AND fornecedor IS NOT NULL

UNION ALL
SELECT 'registros_cq_inspecao', id, 'produto', produto, NULL
FROM registros_cq_inspecao WHERE produto_id IS NULL AND produto IS NOT NULL

UNION ALL
SELECT 'registros_cq_inspecao', id, 'motorista', motorista, NULL
FROM registros_cq_inspecao WHERE motorista_id IS NULL AND motorista IS NOT NULL

UNION ALL
SELECT 'registros_cq_inspecao', id, 'veiculo', placa_caminhao, NULL
FROM registros_cq_inspecao WHERE veiculo_id IS NULL AND placa_caminhao IS NOT NULL

UNION ALL
SELECT 'cq_cargas', id, 'motorista', motorista, NULL
FROM cq_cargas WHERE motorista_id IS NULL AND motorista IS NOT NULL

UNION ALL
SELECT 'cq_cargas', id, 'frigorifico', frigorifico, NULL
FROM cq_cargas WHERE frigorifico_id IS NULL AND frigorifico IS NOT NULL

UNION ALL
SELECT 'cq_lote_inspecao', id, 'fornecedor', fornecedor_nome, fornecedor_codigo
FROM cq_lote_inspecao WHERE fornecedor_id IS NULL AND fornecedor_codigo IS NOT NULL

UNION ALL
SELECT 'cq_fornecedor_score', id, 'fornecedor', fornecedor_nome, fornecedor_codigo
FROM cq_fornecedor_score WHERE fornecedor_id IS NULL

ORDER BY tabela, campo;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. FUNCTION UTILITÁRIA: Diagnóstico de integridade referencial
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_diagnostico_integridade()
RETURNS TABLE (
  tabela TEXT,
  campo TEXT,
  total_registros BIGINT,
  vinculados BIGINT,
  sem_vinculo BIGINT,
  pct_vinculado NUMERIC(5,2)
) AS $$
BEGIN
  RETURN QUERY

  SELECT 'registros_cq_inspecao'::TEXT, 'fornecedor_id'::TEXT,
    COUNT(*)::BIGINT, COUNT(fornecedor_id)::BIGINT,
    (COUNT(*) - COUNT(fornecedor_id))::BIGINT,
    ROUND(COUNT(fornecedor_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM registros_cq_inspecao WHERE fornecedor IS NOT NULL

  UNION ALL
  SELECT 'registros_cq_inspecao', 'produto_id',
    COUNT(*), COUNT(produto_id), COUNT(*) - COUNT(produto_id),
    ROUND(COUNT(produto_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM registros_cq_inspecao WHERE produto IS NOT NULL

  UNION ALL
  SELECT 'registros_cq_inspecao', 'motorista_id',
    COUNT(*), COUNT(motorista_id), COUNT(*) - COUNT(motorista_id),
    ROUND(COUNT(motorista_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM registros_cq_inspecao WHERE motorista IS NOT NULL

  UNION ALL
  SELECT 'registros_cq_inspecao', 'veiculo_id',
    COUNT(*), COUNT(veiculo_id), COUNT(*) - COUNT(veiculo_id),
    ROUND(COUNT(veiculo_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM registros_cq_inspecao WHERE placa_caminhao IS NOT NULL

  UNION ALL
  SELECT 'cq_cargas', 'motorista_id',
    COUNT(*), COUNT(motorista_id), COUNT(*) - COUNT(motorista_id),
    ROUND(COUNT(motorista_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM cq_cargas WHERE motorista IS NOT NULL

  UNION ALL
  SELECT 'cq_cargas', 'frigorifico_id',
    COUNT(*), COUNT(frigorifico_id), COUNT(*) - COUNT(frigorifico_id),
    ROUND(COUNT(frigorifico_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM cq_cargas WHERE frigorifico IS NOT NULL

  UNION ALL
  SELECT 'cq_lote_inspecao', 'fornecedor_id',
    COUNT(*), COUNT(fornecedor_id), COUNT(*) - COUNT(fornecedor_id),
    ROUND(COUNT(fornecedor_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM cq_lote_inspecao WHERE fornecedor_codigo IS NOT NULL

  UNION ALL
  SELECT 'cq_fornecedor_score', 'fornecedor_id',
    COUNT(*), COUNT(fornecedor_id), COUNT(*) - COUNT(fornecedor_id),
    ROUND(COUNT(fornecedor_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)
  FROM cq_fornecedor_score

  ORDER BY tabela, campo;
END;
$$ LANGUAGE plpgsql;


-- ════════════════════════════════════════════════════════════════════════════
-- 8. VIEWS ATUALIZADAS: JOINs por _id (não por texto)
-- ════════════════════════════════════════════════════════════════════════════

-- 8.1 View enriquecida de inspeções (por ID)
CREATE OR REPLACE VIEW vw_inspecoes_completa AS
SELECT
  i.id,
  i.id_inspecao,
  i.data_inspecao,
  i.hora_inspecao,

  -- Fornecedor (por ID)
  i.fornecedor_id,
  COALESCE(ca_f.apelido, ca_f.nome, i.fornecedor) AS fornecedor_nome,
  COALESCE(ca_f.codigo, i.fornecedor_codigo) AS fornecedor_codigo,
  ca_f.uf AS fornecedor_uf,
  ca_f.cnpj AS fornecedor_cnpj,

  -- Produto (por ID)
  i.produto_id,
  COALESCE(prod.nome, i.produto) AS produto_nome,
  prod.codigo AS produto_codigo_ref,
  prod.grupo AS produto_grupo,

  -- Motorista (por ID)
  i.motorista_id,
  COALESCE(ca_m.nome, i.motorista) AS motorista_nome,
  ca_m.cnh AS motorista_cnh,

  -- Veículo (por ID)
  i.veiculo_id,
  COALESCE(veic.placa, i.placa_caminhao) AS veiculo_placa,
  veic.tipo_veiculo,

  -- Transportadora (por ID)
  i.transportadora_id,
  transp.nome AS transportadora_nome,

  -- Inspetor / Usuário
  i.usuario_id,
  u.raw_user_meta_data->>'nome' AS usuario_nome,
  u.email AS usuario_email,

  -- Classificação
  i.classificacao,
  i.status_final,
  i.total_defeitos,
  i.percentual_defeitos,

  -- Quantidades
  i.quantidade_lote,
  i.quantidade_analisada,
  i.temperatura_recebimento,

  -- Defeitos individuais
  i.fermentacao, i.sem_folhas, i.folhas_arrebentadas, i.pedaco,
  i.sujo, i.rasgado, i.corte_irregular, i.furado,
  i.mau_cheiro, i.contaminacao, i.outro_defeito,

  -- Score do fornecedor (JOIN por ID)
  fs.score AS fornecedor_score,
  fs.status_fornecedor AS fornecedor_status,

  i.criado_em,
  i.atualizado_em

FROM registros_cq_inspecao i
LEFT JOIN cadastros_atak ca_f ON ca_f.id = i.fornecedor_id
LEFT JOIN cq_produtos prod ON prod.id = i.produto_id
LEFT JOIN cadastros_atak ca_m ON ca_m.id = i.motorista_id
LEFT JOIN cq_veiculos veic ON veic.id = i.veiculo_id
LEFT JOIN cq_transportadoras transp ON transp.id = i.transportadora_id
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_id = i.fornecedor_id
LEFT JOIN auth.users u ON u.id = i.usuario_id
ORDER BY i.criado_em DESC;


-- 8.2 View enriquecida de cargas (por ID)
CREATE OR REPLACE VIEW vw_cargas_completa AS
SELECT
  c.id,
  c.data_coleta,
  c.data_viagem,
  c.cidade,
  c.tipo_conservacao,

  -- Frigorífico (por ID)
  c.frigorifico_id,
  COALESCE(ca_f.apelido, ca_f.nome, c.frigorifico) AS frigorifico_nome,
  ca_f.codigo AS frigorifico_codigo,
  ca_f.uf AS frigorifico_uf,

  -- Motorista (por ID)
  c.motorista_id,
  COALESCE(ca_m.nome, c.motorista) AS motorista_nome,

  -- Contagens
  c.qtd_frigo, c.qtd_classic, c.diferenca,
  c.total_problemas, c.porcentagem, c.classificacao,

  -- Defeitos
  c.rasgados, c.furados, c.pelando, c.sem_folha,
  c.sujos, c.pedacos, c.corte_irregular, c.folha_extra,

  c.numero_pcr,
  c.observacoes,
  c.situacao,
  c.criado_em

FROM cq_cargas c
LEFT JOIN cadastros_atak ca_f ON ca_f.id = c.frigorifico_id
LEFT JOIN cadastros_atak ca_m ON ca_m.id = c.motorista_id
ORDER BY c.data_coleta DESC;


-- 8.3 View enriquecida de lotes de ranking (por ID)
CREATE OR REPLACE VIEW vw_lotes_ranking_completa AS
SELECT
  li.id,
  li.id_lote,
  li.data_inspecao,
  li.status,

  -- Fornecedor (por ID)
  li.fornecedor_id,
  COALESCE(ca.apelido, ca.nome, li.fornecedor_nome) AS fornecedor_nome,
  COALESCE(ca.codigo, li.fornecedor_codigo) AS fornecedor_codigo,
  ca.uf AS fornecedor_uf,

  -- Inspetor
  li.inspetor_id,
  li.inspetor_nome,

  -- Totais
  li.total_pecas,
  li.total_class_a, li.total_class_b, li.total_class_c,
  li.pct_a, li.pct_b, li.pct_c,
  li.total_defeitos, li.pct_defeitos,
  li.classificacao_lote,

  -- Score do fornecedor
  fs.score AS fornecedor_score,
  fs.status_fornecedor,

  li.criado_em,
  li.atualizado_em,

  -- Produtos do lote (count)
  (SELECT COUNT(*) FROM cq_lote_produto lp WHERE lp.lote_id = li.id) AS total_produtos_inspecionados

FROM cq_lote_inspecao li
LEFT JOIN cadastros_atak ca ON ca.id = li.fornecedor_id
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_id = li.fornecedor_id
ORDER BY li.data_inspecao DESC, li.criado_em DESC;


COMMIT;

-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO PÓS-MIGRAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

-- Executar diagnóstico
SELECT * FROM fn_diagnostico_integridade();

-- Ver registros órfãos
SELECT tabela, campo, COUNT(*) AS sem_vinculo
FROM vw_registros_sem_vinculo
GROUP BY tabela, campo
ORDER BY sem_vinculo DESC;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 06 CONCLUÍDA — Padronização de IDs';
  RAISE NOTICE '  ✓ Tabela cq_transportadoras criada';
  RAISE NOTICE '  ✓ 9 tabelas receberam colunas _id (UUID FK)';
  RAISE NOTICE '  ✓ 6 functions de resolução (fornecedor, produto, motorista, veículo, transp, ranking)';
  RAISE NOTICE '  ✓ 9 triggers de resolução automática';
  RAISE NOTICE '  ✓ Backfill executado em registros existentes';
  RAISE NOTICE '  ✓ vw_registros_sem_vinculo + fn_diagnostico_integridade';
  RAISE NOTICE '  ✓ 3 views enriquecidas por JOIN de UUID';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
