-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO: Sync de Cadastros Mestres ATAK
-- (Fornecedor, Motorista, Funcionário, Veículo, Produto)
-- Rodar APÓS 00_migration_integracao.sql
-- ══════════════════════════════════════════════════════════════════════════════

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 1: Expandir cadastros_atak para suportar 'funcionario'
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cadastros_atak
  DROP CONSTRAINT IF EXISTS cadastros_atak_tipo_check;

ALTER TABLE cadastros_atak
  ADD CONSTRAINT cadastros_atak_tipo_check
  CHECK (tipo IN ('fornecedor', 'motorista', 'funcionario'));

-- Colunas extras para funcionários e motoristas
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS cargo           TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS setor           TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS telefone        TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS email           TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS cnh             TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS categoria_cnh   TEXT;       -- A/B/C/D/E
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS placa_padrao    TEXT;       -- placa principal do motorista
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS data_admissao   DATE;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS data_desligamento DATE;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS atak_id         TEXT;
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS atualizado_em   TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE cadastros_atak ADD COLUMN IF NOT EXISTS sync_origem     TEXT DEFAULT 'manual';

-- Colunas extras para veículos (sync)
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS atak_id        TEXT;
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS codigo_atak    TEXT;
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS renavam        TEXT;
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS ano_fabricacao  INTEGER;
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS capacidade_kg   NUMERIC(10,2);
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS atualizado_em  TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS sync_origem    TEXT DEFAULT 'manual';
ALTER TABLE cq_veiculos ADD COLUMN IF NOT EXISTS funcao_veiculo TEXT DEFAULT 'cavalo'
  CHECK (funcao_veiculo IN ('cavalo','carreta','utilitario','outros'));

-- Colunas extras para produtos (sync)
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS atak_id        TEXT;
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS codigo_atak    TEXT;
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS unidade        TEXT DEFAULT 'PÇ';  -- PÇ, KG, UN
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS peso_medio_kg  NUMERIC(8,3);
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS atualizado_em  TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE cq_produtos ADD COLUMN IF NOT EXISTS sync_origem    TEXT DEFAULT 'manual';

-- Índices adicionais
CREATE INDEX IF NOT EXISTS idx_cadastros_setor     ON cadastros_atak(setor)   WHERE tipo = 'funcionario';
CREATE INDEX IF NOT EXISTS idx_cadastros_cargo     ON cadastros_atak(cargo)   WHERE tipo = 'funcionario';
CREATE INDEX IF NOT EXISTS idx_cadastros_atak_id   ON cadastros_atak(atak_id);
CREATE INDEX IF NOT EXISTS idx_cadastros_ativo     ON cadastros_atak(ativo);
CREATE INDEX IF NOT EXISTS idx_veiculos_funcao     ON cq_veiculos(funcao_veiculo);
CREATE INDEX IF NOT EXISTS idx_veiculos_atak_id    ON cq_veiculos(atak_id);
CREATE INDEX IF NOT EXISTS idx_produtos_atak_id    ON cq_produtos(atak_id);
CREATE INDEX IF NOT EXISTS idx_produtos_codigo_atak ON cq_produtos(codigo_atak);

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 2: Expandir atak_cargas_raw com vínculos cadastrais
-- ════════════════════════════════════════════════════════════════════════════

-- Vínculos com cadastros mestres
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS motorista_codigo     TEXT;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS motorista_id         UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS cavalo_placa         TEXT;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS cavalo_id            UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS carreta1_placa       TEXT;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS carreta1_id          UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS carreta2_placa       TEXT;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS carreta2_id          UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS recebedor_codigo     TEXT;   -- funcionário que recebeu
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS recebedor_id         UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS classificador_codigo TEXT;   -- funcionário que classificou
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS classificador_id     UUID;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS produto_codigo       TEXT;
ALTER TABLE atak_cargas_raw ADD COLUMN IF NOT EXISTS produto_id           UUID;

-- Índices para vínculos
CREATE INDEX IF NOT EXISTS idx_cargas_motorista_id ON atak_cargas_raw(motorista_id);
CREATE INDEX IF NOT EXISTS idx_cargas_cavalo_id    ON atak_cargas_raw(cavalo_id);
CREATE INDEX IF NOT EXISTS idx_cargas_recebedor_id ON atak_cargas_raw(recebedor_id);
CREATE INDEX IF NOT EXISTS idx_cargas_classif_id   ON atak_cargas_raw(classificador_id);
CREATE INDEX IF NOT EXISTS idx_cargas_produto_id   ON atak_cargas_raw(produto_id);

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 3: Função de resolução de vínculos cadastrais
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_resolver_vinculos_carga()
RETURNS TRIGGER AS $$
DECLARE
  v_divergencias TEXT[] := '{}';
BEGIN
  -- ── Resolver motorista ───────────────────────────────
  IF NEW.motorista_codigo IS NOT NULL AND NEW.motorista_id IS NULL THEN
    SELECT id INTO NEW.motorista_id
    FROM cadastros_atak
    WHERE codigo = NEW.motorista_codigo AND tipo = 'motorista' AND ativo = true
    LIMIT 1;

    IF NEW.motorista_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'motorista_codigo=' || NEW.motorista_codigo || ' não encontrado');
    END IF;
  END IF;

  -- ── Resolver fornecedor ──────────────────────────────
  IF NEW.fornecedor_codigo IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1 FROM cadastros_atak
      WHERE codigo = NEW.fornecedor_codigo AND tipo = 'fornecedor' AND ativo = true
    ) THEN
      v_divergencias := array_append(v_divergencias,
        'fornecedor_codigo=' || NEW.fornecedor_codigo || ' não encontrado');
    END IF;
  END IF;

  -- ── Resolver cavalo ──────────────────────────────────
  IF NEW.cavalo_placa IS NOT NULL AND NEW.cavalo_id IS NULL THEN
    SELECT id INTO NEW.cavalo_id
    FROM cq_veiculos
    WHERE placa = UPPER(REPLACE(NEW.cavalo_placa, '-', '')) AND ativo = true
    LIMIT 1;

    IF NEW.cavalo_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'cavalo_placa=' || NEW.cavalo_placa || ' não encontrado');
    END IF;
  END IF;

  -- ── Resolver carreta 1 ──────────────────────────────
  IF NEW.carreta1_placa IS NOT NULL AND NEW.carreta1_id IS NULL THEN
    SELECT id INTO NEW.carreta1_id
    FROM cq_veiculos
    WHERE placa = UPPER(REPLACE(NEW.carreta1_placa, '-', '')) AND ativo = true
    LIMIT 1;

    IF NEW.carreta1_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'carreta1_placa=' || NEW.carreta1_placa || ' não encontrada');
    END IF;
  END IF;

  -- ── Resolver carreta 2 ──────────────────────────────
  IF NEW.carreta2_placa IS NOT NULL AND NEW.carreta2_id IS NULL THEN
    SELECT id INTO NEW.carreta2_id
    FROM cq_veiculos
    WHERE placa = UPPER(REPLACE(NEW.carreta2_placa, '-', '')) AND ativo = true
    LIMIT 1;

    IF NEW.carreta2_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'carreta2_placa=' || NEW.carreta2_placa || ' não encontrada');
    END IF;
  END IF;

  -- ── Resolver recebedor ──────────────────────────────
  IF NEW.recebedor_codigo IS NOT NULL AND NEW.recebedor_id IS NULL THEN
    SELECT id INTO NEW.recebedor_id
    FROM cadastros_atak
    WHERE codigo = NEW.recebedor_codigo AND tipo = 'funcionario' AND ativo = true
    LIMIT 1;

    IF NEW.recebedor_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'recebedor_codigo=' || NEW.recebedor_codigo || ' não encontrado');
    END IF;
  END IF;

  -- ── Resolver classificador ──────────────────────────
  IF NEW.classificador_codigo IS NOT NULL AND NEW.classificador_id IS NULL THEN
    SELECT id INTO NEW.classificador_id
    FROM cadastros_atak
    WHERE codigo = NEW.classificador_codigo AND tipo = 'funcionario' AND ativo = true
    LIMIT 1;

    IF NEW.classificador_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'classificador_codigo=' || NEW.classificador_codigo || ' não encontrado');
    END IF;
  END IF;

  -- ── Resolver produto ────────────────────────────────
  IF NEW.produto_codigo IS NOT NULL AND NEW.produto_id IS NULL THEN
    SELECT id INTO NEW.produto_id
    FROM cq_produtos
    WHERE (codigo = NEW.produto_codigo OR codigo_atak = NEW.produto_codigo) AND ativo = true
    LIMIT 1;

    IF NEW.produto_id IS NULL THEN
      v_divergencias := array_append(v_divergencias,
        'produto_codigo=' || NEW.produto_codigo || ' não encontrado');
    END IF;
  END IF;

  -- ── Registrar divergências de vínculo ──────────────
  IF array_length(v_divergencias, 1) > 0 THEN
    INSERT INTO cq_validacao_divergencias (
      tipo_divergencia, gravidade, tabela_origem, registro_origem_id,
      carga_id, fornecedor_codigo, numero_documento,
      campo_divergente, valor_esperado, valor_encontrado,
      mensagem, detectado_por
    ) VALUES (
      'documento_ausente',
      CASE WHEN array_length(v_divergencias, 1) > 2 THEN 'critica' ELSE 'atencao' END,
      'atak_cargas_raw', NEW.id,
      NEW.id, NEW.fornecedor_codigo, NEW.numero_documento,
      'vinculos_cadastrais',
      'Todos os vínculos resolvidos',
      array_to_string(v_divergencias, '; '),
      FORMAT('%s vínculo(s) sem cadastro: %s',
        array_length(v_divergencias, 1),
        array_to_string(v_divergencias, ' | ')),
      'trigger_vinculo_automatico'
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_resolver_vinculos_carga
  BEFORE INSERT OR UPDATE ON atak_cargas_raw
  FOR EACH ROW
  EXECUTE FUNCTION fn_resolver_vinculos_carga();

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 4: Staging para sync de cadastros (todos os tipos)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS atak_cadastros_staging (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sync_log_id       UUID REFERENCES atak_sync_log(id),
  tipo_cadastro     TEXT NOT NULL
                      CHECK (tipo_cadastro IN ('fornecedor','motorista','funcionario','veiculo','produto')),

  -- Campos comuns
  atak_id           TEXT,
  codigo            TEXT NOT NULL,
  nome              TEXT NOT NULL,
  apelido           TEXT,
  ativo             BOOLEAN DEFAULT true,

  -- Fornecedor
  cnpj              TEXT,
  uf                TEXT,

  -- Funcionário
  cargo             TEXT,
  setor             TEXT,
  telefone          TEXT,
  email             TEXT,
  data_admissao     DATE,

  -- Motorista
  cnh               TEXT,
  categoria_cnh     TEXT,
  placa_padrao      TEXT,

  -- Veículo
  placa             TEXT,
  tipo_veiculo      TEXT,
  funcao_veiculo    TEXT,
  renavam           TEXT,
  ano_fabricacao     INTEGER,
  capacidade_kg     NUMERIC(10,2),

  -- Produto
  grupo             TEXT,
  unidade           TEXT,
  peso_medio_kg     NUMERIC(8,3),

  -- Controle
  hash_registro     TEXT,
  acao              TEXT DEFAULT 'pendente'
                      CHECK (acao IN ('pendente','inserir','atualizar','ignorar','erro')),
  motivo_acao       TEXT,
  payload_json      JSONB,
  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cad_staging_tipo ON atak_cadastros_staging(tipo_cadastro);
CREATE INDEX IF NOT EXISTS idx_cad_staging_acao ON atak_cadastros_staging(acao);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cad_staging_hash ON atak_cadastros_staging(hash_registro);

ALTER TABLE atak_cadastros_staging ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth_read" ON atak_cadastros_staging FOR SELECT TO authenticated USING (true);
CREATE POLICY "service_write" ON atak_cadastros_staging FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 5: Função de promoção: staging → tabelas finais
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_promover_cadastros(p_sync_log_id UUID)
RETURNS TABLE(tipo TEXT, inseridos INTEGER, atualizados INTEGER, erros INTEGER) AS $$
DECLARE
  r             RECORD;
  v_ins         INTEGER := 0;
  v_upd         INTEGER := 0;
  v_err         INTEGER := 0;
  v_tipo_atual  TEXT := '';
  v_resultados  TEXT[] := '{}';
BEGIN
  FOR r IN
    SELECT * FROM atak_cadastros_staging
    WHERE sync_log_id = p_sync_log_id AND acao = 'pendente'
    ORDER BY tipo_cadastro
  LOOP
    BEGIN
      -- ── FORNECEDOR / MOTORISTA / FUNCIONÁRIO ──────────
      IF r.tipo_cadastro IN ('fornecedor','motorista','funcionario') THEN
        IF EXISTS (SELECT 1 FROM cadastros_atak WHERE codigo = r.codigo) THEN
          UPDATE cadastros_atak SET
            nome           = COALESCE(r.nome, nome),
            apelido        = COALESCE(r.apelido, apelido),
            cnpj           = COALESCE(r.cnpj, cnpj),
            uf             = COALESCE(r.uf, uf),
            tipo           = r.tipo_cadastro,
            cargo          = COALESCE(r.cargo, cargo),
            setor          = COALESCE(r.setor, setor),
            telefone       = COALESCE(r.telefone, telefone),
            email          = COALESCE(r.email, email),
            cnh            = COALESCE(r.cnh, cnh),
            categoria_cnh  = COALESCE(r.categoria_cnh, categoria_cnh),
            placa_padrao   = COALESCE(r.placa_padrao, placa_padrao),
            data_admissao  = COALESCE(r.data_admissao, data_admissao),
            ativo          = r.ativo,
            atak_id        = COALESCE(r.atak_id, atak_id),
            atualizado_em  = NOW(),
            sync_origem    = 'n8n'
          WHERE codigo = r.codigo;
          UPDATE atak_cadastros_staging SET acao = 'atualizar' WHERE id = r.id;
          v_upd := v_upd + 1;
        ELSE
          INSERT INTO cadastros_atak (
            codigo, nome, apelido, cnpj, uf, tipo,
            cargo, setor, telefone, email, cnh, categoria_cnh,
            placa_padrao, data_admissao, ativo, atak_id, sync_origem, atualizado_em
          ) VALUES (
            r.codigo, r.nome, r.apelido, r.cnpj, r.uf, r.tipo_cadastro,
            r.cargo, r.setor, r.telefone, r.email, r.cnh, r.categoria_cnh,
            r.placa_padrao, r.data_admissao, r.ativo, r.atak_id, 'n8n', NOW()
          );
          UPDATE atak_cadastros_staging SET acao = 'inserir' WHERE id = r.id;
          v_ins := v_ins + 1;
        END IF;

      -- ── VEÍCULO ───────────────────────────────────────
      ELSIF r.tipo_cadastro = 'veiculo' THEN
        IF EXISTS (SELECT 1 FROM cq_veiculos WHERE placa = UPPER(r.placa)) THEN
          UPDATE cq_veiculos SET
            nome           = COALESCE(r.nome, nome),
            tipo_veiculo   = COALESCE(r.tipo_veiculo, tipo_veiculo),
            funcao_veiculo = COALESCE(r.funcao_veiculo, funcao_veiculo),
            uf             = COALESCE(r.uf, uf),
            renavam        = COALESCE(r.renavam, renavam),
            ano_fabricacao = COALESCE(r.ano_fabricacao, ano_fabricacao),
            capacidade_kg  = COALESCE(r.capacidade_kg, capacidade_kg),
            atak_id        = COALESCE(r.atak_id, atak_id),
            codigo_atak    = COALESCE(r.codigo, codigo_atak),
            ativo          = r.ativo,
            atualizado_em  = NOW(),
            sync_origem    = 'n8n'
          WHERE placa = UPPER(r.placa);
          UPDATE atak_cadastros_staging SET acao = 'atualizar' WHERE id = r.id;
          v_upd := v_upd + 1;
        ELSE
          INSERT INTO cq_veiculos (
            placa, nome, tipo_veiculo, funcao_veiculo, uf,
            renavam, ano_fabricacao, capacidade_kg,
            atak_id, codigo_atak, ativo, atualizado_em, sync_origem
          ) VALUES (
            UPPER(r.placa), r.nome,
            COALESCE(r.tipo_veiculo, 'OUTROS'),
            COALESCE(r.funcao_veiculo, 'outros'),
            r.uf, r.renavam, r.ano_fabricacao, r.capacidade_kg,
            r.atak_id, r.codigo, r.ativo, NOW(), 'n8n'
          );
          UPDATE atak_cadastros_staging SET acao = 'inserir' WHERE id = r.id;
          v_ins := v_ins + 1;
        END IF;

      -- ── PRODUTO ───────────────────────────────────────
      ELSIF r.tipo_cadastro = 'produto' THEN
        IF EXISTS (SELECT 1 FROM cq_produtos WHERE codigo = r.codigo OR codigo_atak = r.codigo) THEN
          UPDATE cq_produtos SET
            nome          = COALESCE(r.nome, nome),
            grupo         = COALESCE(r.grupo, grupo),
            unidade       = COALESCE(r.unidade, unidade),
            peso_medio_kg = COALESCE(r.peso_medio_kg, peso_medio_kg),
            atak_id       = COALESCE(r.atak_id, atak_id),
            codigo_atak   = COALESCE(r.codigo, codigo_atak),
            ativo         = r.ativo,
            atualizado_em = NOW(),
            sync_origem   = 'n8n'
          WHERE codigo = r.codigo OR codigo_atak = r.codigo;
          UPDATE atak_cadastros_staging SET acao = 'atualizar' WHERE id = r.id;
          v_upd := v_upd + 1;
        ELSE
          INSERT INTO cq_produtos (
            codigo, nome, grupo, unidade, peso_medio_kg,
            atak_id, codigo_atak, ativo, atualizado_em, sync_origem
          ) VALUES (
            r.codigo, r.nome, r.grupo, r.unidade, r.peso_medio_kg,
            r.atak_id, r.codigo, r.ativo, NOW(), 'n8n'
          );
          UPDATE atak_cadastros_staging SET acao = 'inserir' WHERE id = r.id;
          v_ins := v_ins + 1;
        END IF;
      END IF;

    EXCEPTION WHEN OTHERS THEN
      UPDATE atak_cadastros_staging SET acao = 'erro', motivo_acao = SQLERRM WHERE id = r.id;
      v_err := v_err + 1;
    END;
  END LOOP;

  tipo := 'total';
  inseridos := v_ins;
  atualizados := v_upd;
  erros := v_err;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 6: Views de consulta
-- ════════════════════════════════════════════════════════════════════════════

-- Resumo por tipo de cadastro
CREATE OR REPLACE VIEW vw_cadastros_resumo AS
SELECT
  tipo,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE ativo = true) AS ativos,
  COUNT(*) FILTER (WHERE ativo = false) AS inativos,
  COUNT(*) FILTER (WHERE sync_origem = 'n8n') AS sync_automatico,
  COUNT(*) FILTER (WHERE sync_origem = 'manual') AS cadastro_manual,
  MAX(atualizado_em) AS ultima_atualizacao
FROM cadastros_atak
GROUP BY tipo
UNION ALL
SELECT
  'veiculo' AS tipo,
  COUNT(*),
  COUNT(*) FILTER (WHERE ativo = true),
  COUNT(*) FILTER (WHERE ativo = false),
  COUNT(*) FILTER (WHERE sync_origem = 'n8n'),
  COUNT(*) FILTER (WHERE sync_origem = 'manual'),
  MAX(atualizado_em)
FROM cq_veiculos
UNION ALL
SELECT
  'produto' AS tipo,
  COUNT(*),
  COUNT(*) FILTER (WHERE ativo = true),
  COUNT(*) FILTER (WHERE ativo = false),
  COUNT(*) FILTER (WHERE sync_origem = 'n8n'),
  COUNT(*) FILTER (WHERE sync_origem = 'manual'),
  MAX(atualizado_em)
FROM cq_produtos
ORDER BY tipo;

-- Motoristas ativos com placa
CREATE OR REPLACE VIEW vw_motoristas_ativos AS
SELECT codigo, nome, apelido, cnh, categoria_cnh, placa_padrao, telefone, ativo, atualizado_em
FROM cadastros_atak WHERE tipo = 'motorista' AND ativo = true ORDER BY nome;

-- Funcionários ativos por setor
CREATE OR REPLACE VIEW vw_funcionarios_ativos AS
SELECT codigo, nome, apelido, cargo, setor, data_admissao, telefone, email, ativo, atualizado_em
FROM cadastros_atak WHERE tipo = 'funcionario' AND ativo = true ORDER BY setor, nome;

-- Cargas com vínculos pendentes
CREATE OR REPLACE VIEW vw_cargas_vinculos_pendentes AS
SELECT
  c.id,
  c.numero_documento,
  c.numero_pcr,
  c.data_coleta,
  c.fornecedor_codigo,
  c.fornecedor_nome,
  CASE WHEN c.motorista_codigo IS NOT NULL AND c.motorista_id IS NULL THEN true ELSE false END AS motorista_pendente,
  CASE WHEN c.cavalo_placa IS NOT NULL AND c.cavalo_id IS NULL THEN true ELSE false END AS cavalo_pendente,
  CASE WHEN c.carreta1_placa IS NOT NULL AND c.carreta1_id IS NULL THEN true ELSE false END AS carreta1_pendente,
  CASE WHEN c.carreta2_placa IS NOT NULL AND c.carreta2_id IS NULL THEN true ELSE false END AS carreta2_pendente,
  CASE WHEN c.recebedor_codigo IS NOT NULL AND c.recebedor_id IS NULL THEN true ELSE false END AS recebedor_pendente,
  CASE WHEN c.classificador_codigo IS NOT NULL AND c.classificador_id IS NULL THEN true ELSE false END AS classificador_pendente,
  CASE WHEN c.produto_codigo IS NOT NULL AND c.produto_id IS NULL THEN true ELSE false END AS produto_pendente,
  c.criado_em
FROM atak_cargas_raw c
WHERE
  (c.motorista_codigo IS NOT NULL AND c.motorista_id IS NULL) OR
  (c.cavalo_placa IS NOT NULL AND c.cavalo_id IS NULL) OR
  (c.carreta1_placa IS NOT NULL AND c.carreta1_id IS NULL) OR
  (c.carreta2_placa IS NOT NULL AND c.carreta2_id IS NULL) OR
  (c.recebedor_codigo IS NOT NULL AND c.recebedor_id IS NULL) OR
  (c.classificador_codigo IS NOT NULL AND c.classificador_id IS NULL) OR
  (c.produto_codigo IS NOT NULL AND c.produto_id IS NULL)
ORDER BY c.criado_em DESC;

-- ════════════════════════════════════════════════════════════════════════════
-- PARTE 7: Seed config de sync para os 5 cadastros
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO atak_sync_config (endpoint_nome, endpoint_url, metodo_http, intervalo_min, parametros_json) VALUES
  ('WRCAD_motoristas', 'https://api.atak.com.br/v1/cadastros/motoristas', 'GET', 1440,
   '{"ativo": true}'::JSONB),
  ('WRCAD_funcionarios', 'https://api.atak.com.br/v1/cadastros/funcionarios', 'GET', 1440,
   '{"ativo": true}'::JSONB),
  ('WRCAD_veiculos', 'https://api.atak.com.br/v1/cadastros/veiculos', 'GET', 1440,
   '{"ativo": true}'::JSONB),
  ('WRCAD_produtos', 'https://api.atak.com.br/v1/cadastros/produtos', 'GET', 1440,
   '{"ativo": true}'::JSONB)
ON CONFLICT (endpoint_nome) DO NOTHING;

-- ── VERIFICAÇÃO ──────────────────────────────────────────────────────────
SELECT * FROM vw_cadastros_resumo;
SELECT endpoint_nome, intervalo_min, ativo FROM atak_sync_config ORDER BY endpoint_nome;
