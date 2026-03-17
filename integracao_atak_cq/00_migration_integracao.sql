-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO: Integração ATAK ↔ CQ via n8n + Supabase
-- Rodar no SQL Editor do Supabase (após tabelas existentes)
-- ══════════════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────────────
-- 1. atak_sync_config — Configuração dos endpoints e parâmetros de sync
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_sync_config (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  endpoint_nome   TEXT NOT NULL UNIQUE,          -- ex: 'WRCAD009', 'WRMVE500', 'WRPRD600'
  endpoint_url    TEXT NOT NULL,                  -- URL base da API ATAK
  metodo_http     TEXT NOT NULL DEFAULT 'GET',
  headers_json    JSONB DEFAULT '{}',
  parametros_json JSONB DEFAULT '{}',             -- query params padrão
  intervalo_min   INTEGER NOT NULL DEFAULT 30,    -- intervalo de sync em minutos
  ativo           BOOLEAN DEFAULT true,
  ultima_sync     TIMESTAMPTZ,
  proximo_sync    TIMESTAMPTZ,
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2. atak_sync_log — Log de cada execução de sincronização
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_sync_log (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  config_id         UUID REFERENCES atak_sync_config(id),
  endpoint_nome     TEXT NOT NULL,
  inicio            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  fim               TIMESTAMPTZ,
  status            TEXT NOT NULL DEFAULT 'em_execucao'
                      CHECK (status IN ('em_execucao','sucesso','erro','parcial')),
  total_registros   INTEGER DEFAULT 0,
  novos             INTEGER DEFAULT 0,
  atualizados       INTEGER DEFAULT 0,
  ignorados         INTEGER DEFAULT 0,
  erros             INTEGER DEFAULT 0,
  mensagem          TEXT,
  detalhes_json     JSONB DEFAULT '{}',
  n8n_execution_id  TEXT,                        -- ID da execução no n8n
  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_log_config ON atak_sync_log(config_id);
CREATE INDEX IF NOT EXISTS idx_sync_log_status ON atak_sync_log(status);
CREATE INDEX IF NOT EXISTS idx_sync_log_inicio ON atak_sync_log(inicio DESC);

-- ────────────────────────────────────────────────────────────────────────────
-- 3. atak_cargas_raw — Dados brutos de cargas vindos da API ATAK
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_cargas_raw (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sync_log_id       UUID REFERENCES atak_sync_log(id),

  -- Identificação ATAK
  atak_id           TEXT,                         -- ID original no ATAK
  numero_documento  TEXT,                          -- NF/NFC/PCR
  numero_pcr        TEXT,
  numero_carga      TEXT,

  -- Dados da carga
  fornecedor_codigo TEXT,
  fornecedor_nome   TEXT,
  frigorifico       TEXT,
  motorista         TEXT,
  placa_veiculo     TEXT,
  transportadora    TEXT,

  -- Datas
  data_coleta       DATE,
  data_chegada      DATE,
  data_processamento DATE,

  -- Quantidades ATAK (contagem do frigorífico)
  qtd_frigo         INTEGER DEFAULT 0,
  peso_frigo_kg     NUMERIC(12,2) DEFAULT 0,

  -- Produto
  produto           TEXT,
  tipo_conservacao  TEXT,                          -- CONGELADO / SALGADO

  -- Classificação ATAK
  class_a           INTEGER DEFAULT 0,
  class_b           INTEGER DEFAULT 0,
  class_c           INTEGER DEFAULT 0,
  total_classificado INTEGER DEFAULT 0,

  -- Controle
  hash_registro     TEXT,                          -- SHA256 para dedup
  status_validacao  TEXT DEFAULT 'pendente'
                      CHECK (status_validacao IN ('pendente','valido','invalido','divergente')),
  processado        BOOLEAN DEFAULT false,
  processado_em     TIMESTAMPTZ,

  -- Raw payload
  payload_json      JSONB,                         -- resposta original da API

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_atak_cargas_hash ON atak_cargas_raw(hash_registro);
CREATE INDEX IF NOT EXISTS idx_atak_cargas_pcr ON atak_cargas_raw(numero_pcr);
CREATE INDEX IF NOT EXISTS idx_atak_cargas_doc ON atak_cargas_raw(numero_documento);
CREATE INDEX IF NOT EXISTS idx_atak_cargas_forn ON atak_cargas_raw(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_atak_cargas_status ON atak_cargas_raw(status_validacao);

-- ────────────────────────────────────────────────────────────────────────────
-- 4. atak_embarques_raw — Dados brutos de embarques vindos da API ATAK
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_embarques_raw (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sync_log_id       UUID REFERENCES atak_sync_log(id),

  atak_id           TEXT,
  numero_embarque   TEXT,
  numero_pedido     TEXT,

  fornecedor_codigo TEXT,
  fornecedor_nome   TEXT,
  destino           TEXT,

  data_embarque     DATE,
  data_previsao     DATE,

  qtd_pecas         INTEGER DEFAULT 0,
  peso_total_kg     NUMERIC(12,2) DEFAULT 0,

  produto           TEXT,
  tipo_conservacao  TEXT,

  status_embarque   TEXT,                          -- em_transito, entregue, cancelado

  hash_registro     TEXT,
  status_validacao  TEXT DEFAULT 'pendente'
                      CHECK (status_validacao IN ('pendente','valido','invalido','divergente')),
  processado        BOOLEAN DEFAULT false,
  payload_json      JSONB,

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_atak_embarques_hash ON atak_embarques_raw(hash_registro);
CREATE INDEX IF NOT EXISTS idx_atak_embarques_num ON atak_embarques_raw(numero_embarque);
CREATE INDEX IF NOT EXISTS idx_atak_embarques_forn ON atak_embarques_raw(fornecedor_codigo);

-- ────────────────────────────────────────────────────────────────────────────
-- 5. atak_producao_raw — Dados brutos de produção da API ATAK
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_producao_raw (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sync_log_id       UUID REFERENCES atak_sync_log(id),

  atak_id           TEXT,
  data_producao     DATE,
  setor             TEXT,
  produto           TEXT,

  qtd_produzida     INTEGER DEFAULT 0,
  qtd_aprovada      INTEGER DEFAULT 0,
  qtd_rejeitada     INTEGER DEFAULT 0,
  peso_total_kg     NUMERIC(12,2) DEFAULT 0,

  turno             TEXT,
  responsavel       TEXT,

  hash_registro     TEXT,
  status_validacao  TEXT DEFAULT 'pendente'
                      CHECK (status_validacao IN ('pendente','valido','invalido','divergente')),
  processado        BOOLEAN DEFAULT false,
  payload_json      JSONB,

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_atak_producao_hash ON atak_producao_raw(hash_registro);
CREATE INDEX IF NOT EXISTS idx_atak_producao_data ON atak_producao_raw(data_producao);

-- ────────────────────────────────────────────────────────────────────────────
-- 6. atak_expedição_raw — Dados brutos de expedição da API ATAK
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atak_expedicao_raw (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sync_log_id       UUID REFERENCES atak_sync_log(id),

  atak_id           TEXT,
  numero_expedicao  TEXT,
  numero_pedido     TEXT,

  fornecedor_codigo TEXT,
  cliente           TEXT,
  destino           TEXT,

  data_expedicao    DATE,

  qtd_pecas         INTEGER DEFAULT 0,
  peso_total_kg     NUMERIC(12,2) DEFAULT 0,

  produto           TEXT,
  lote              TEXT,

  hash_registro     TEXT,
  status_validacao  TEXT DEFAULT 'pendente'
                      CHECK (status_validacao IN ('pendente','valido','invalido','divergente')),
  processado        BOOLEAN DEFAULT false,
  payload_json      JSONB,

  criado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_atak_expedicao_hash ON atak_expedicao_raw(hash_registro);
CREATE INDEX IF NOT EXISTS idx_atak_expedicao_data ON atak_expedicao_raw(data_expedicao);

-- ────────────────────────────────────────────────────────────────────────────
-- 7. cq_validacao_divergencias — Divergências detectadas automaticamente
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cq_validacao_divergencias (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tipo_divergencia  TEXT NOT NULL CHECK (tipo_divergencia IN (
    'abc_total_mismatch',           -- A+B+C != total classificado
    'contagem_frigo_vs_classic',    -- frigorífico vs contagem interna
    'duplicidade_carga',            -- carga/documento duplicado
    'fornecedor_critico',           -- %C acima do limite
    'quantidade_negativa',          -- valores negativos
    'peso_divergente',              -- peso não bate
    'data_inconsistente',           -- data futura ou muito antiga
    'documento_ausente'             -- documento sem referência
  )),

  gravidade         TEXT NOT NULL DEFAULT 'atencao'
                      CHECK (gravidade IN ('critica','atencao','informativa')),
  status            TEXT NOT NULL DEFAULT 'aberta'
                      CHECK (status IN ('aberta','em_analise','resolvida','ignorada','escalada')),

  -- Referências
  tabela_origem     TEXT,                          -- atak_cargas_raw, atak_embarques_raw, etc.
  registro_origem_id UUID,
  carga_id          UUID REFERENCES atak_cargas_raw(id),
  embarque_id       UUID REFERENCES atak_embarques_raw(id),
  fornecedor_codigo TEXT,
  numero_documento  TEXT,

  -- Detalhes da divergência
  campo_divergente  TEXT,
  valor_esperado    TEXT,
  valor_encontrado  TEXT,
  diferenca_abs     NUMERIC(12,2),
  mensagem          TEXT NOT NULL,

  -- Resolução
  resolvido_por     UUID REFERENCES cq_usuarios(id),
  resolucao_obs     TEXT,
  resolvido_em      TIMESTAMPTZ,

  -- Metadata
  detectado_por     TEXT DEFAULT 'n8n_automatico',  -- n8n_automatico, etl, manual
  n8n_execution_id  TEXT,

  criado_em         TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_diverg_tipo ON cq_validacao_divergencias(tipo_divergencia);
CREATE INDEX IF NOT EXISTS idx_diverg_status ON cq_validacao_divergencias(status);
CREATE INDEX IF NOT EXISTS idx_diverg_grav ON cq_validacao_divergencias(gravidade);
CREATE INDEX IF NOT EXISTS idx_diverg_forn ON cq_validacao_divergencias(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_diverg_criado ON cq_validacao_divergencias(criado_em DESC);

-- ────────────────────────────────────────────────────────────────────────────
-- 8. cq_fornecedor_score — Score contínuo do fornecedor (atualizado pelo n8n)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cq_fornecedor_score (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fornecedor_codigo TEXT NOT NULL,
  fornecedor_nome   TEXT,

  -- Métricas agregadas (janela de 90 dias por padrão)
  total_cargas      INTEGER DEFAULT 0,
  total_pecas       INTEGER DEFAULT 0,
  pecas_a           INTEGER DEFAULT 0,
  pecas_b           INTEGER DEFAULT 0,
  pecas_c           INTEGER DEFAULT 0,
  pct_a             NUMERIC(5,2) DEFAULT 0,
  pct_b             NUMERIC(5,2) DEFAULT 0,
  pct_c             NUMERIC(5,2) DEFAULT 0,

  -- Divergências
  total_divergencias INTEGER DEFAULT 0,
  divergencias_abertas INTEGER DEFAULT 0,

  -- Classificação do fornecedor
  score             NUMERIC(5,2) DEFAULT 100,      -- 0-100 (100 = excelente)
  status_fornecedor TEXT DEFAULT 'normal'
                      CHECK (status_fornecedor IN ('normal','atencao','critico','bloqueado')),
  limite_pct_c      NUMERIC(5,2) DEFAULT 15.00,    -- % máximo de C permitido

  -- Flags
  is_critico        BOOLEAN DEFAULT false,
  data_bloqueio     DATE,
  motivo_bloqueio   TEXT,

  atualizado_em     TIMESTAMPTZ DEFAULT NOW(),
  criado_em         TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(fornecedor_codigo)
);

CREATE INDEX IF NOT EXISTS idx_forn_score_status ON cq_fornecedor_score(status_fornecedor);
CREATE INDEX IF NOT EXISTS idx_forn_score_critico ON cq_fornecedor_score(is_critico) WHERE is_critico = true;

-- ────────────────────────────────────────────────────────────────────────────
-- 9. n8n_webhook_tokens — Tokens de autenticação para webhooks do n8n
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS n8n_webhook_tokens (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  token       TEXT NOT NULL UNIQUE,
  descricao   TEXT,
  ativo       BOOLEAN DEFAULT true,
  ultimo_uso  TIMESTAMPTZ,
  criado_em   TIMESTAMPTZ DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 10. RLS para todas as tabelas de integração
-- ────────────────────────────────────────────────────────────────────────────
ALTER TABLE atak_sync_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_sync_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_cargas_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_embarques_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_producao_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE atak_expedicao_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_validacao_divergencias ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_fornecedor_score ENABLE ROW LEVEL SECURITY;
ALTER TABLE n8n_webhook_tokens ENABLE ROW LEVEL SECURITY;

-- Leitura para autenticados
CREATE POLICY "auth_read" ON atak_sync_config FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON atak_sync_log FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON atak_cargas_raw FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON atak_embarques_raw FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON atak_producao_raw FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON atak_expedicao_raw FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON cq_validacao_divergencias FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON cq_fornecedor_score FOR SELECT TO authenticated USING (true);

-- Escrita via service_role (n8n usa service_role key)
CREATE POLICY "service_write" ON atak_sync_config FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON atak_sync_log FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON atak_cargas_raw FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON atak_embarques_raw FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON atak_producao_raw FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON atak_expedicao_raw FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON cq_validacao_divergencias FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON cq_fornecedor_score FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON n8n_webhook_tokens FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Gestores podem gerenciar divergências
CREATE POLICY "gestor_manage_diverg" ON cq_validacao_divergencias
  FOR UPDATE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM cq_usuarios u
    WHERE u.auth_id = auth.uid()
    AND u.perfil IN ('gestor','administrador')
  ));

-- ────────────────────────────────────────────────────────────────────────────
-- 11. Functions: Validação automática de A+B+C = Total
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_validar_abc_total()
RETURNS TRIGGER AS $$
BEGIN
  IF (NEW.class_a + NEW.class_b + NEW.class_c) != NEW.total_classificado
     AND NEW.total_classificado > 0 THEN
    NEW.status_validacao := 'divergente';

    INSERT INTO cq_validacao_divergencias (
      tipo_divergencia, gravidade, tabela_origem, registro_origem_id,
      carga_id, fornecedor_codigo, numero_documento,
      campo_divergente, valor_esperado, valor_encontrado, diferenca_abs, mensagem
    ) VALUES (
      'abc_total_mismatch', 'critica', 'atak_cargas_raw', NEW.id,
      NEW.id, NEW.fornecedor_codigo, NEW.numero_documento,
      'class_a+class_b+class_c',
      NEW.total_classificado::TEXT,
      (NEW.class_a + NEW.class_b + NEW.class_c)::TEXT,
      ABS(NEW.total_classificado - (NEW.class_a + NEW.class_b + NEW.class_c)),
      FORMAT('A(%s) + B(%s) + C(%s) = %s ≠ Total classificado(%s)',
        NEW.class_a, NEW.class_b, NEW.class_c,
        NEW.class_a + NEW.class_b + NEW.class_c,
        NEW.total_classificado)
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_abc_total
  BEFORE INSERT OR UPDATE ON atak_cargas_raw
  FOR EACH ROW
  EXECUTE FUNCTION fn_validar_abc_total();

-- ────────────────────────────────────────────────────────────────────────────
-- 12. Function: Detectar divergência frigorífico vs contagem interna
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_validar_contagem_frigo()
RETURNS TRIGGER AS $$
DECLARE
  v_qtd_classic INTEGER;
BEGIN
  -- Busca contagem interna (Classic) na tabela cq_cargas pelo mesmo PCR
  SELECT qtd_classic INTO v_qtd_classic
  FROM cq_cargas
  WHERE numero_pcr = NEW.numero_pcr
  LIMIT 1;

  IF v_qtd_classic IS NOT NULL AND NEW.qtd_frigo != v_qtd_classic THEN
    INSERT INTO cq_validacao_divergencias (
      tipo_divergencia, gravidade, tabela_origem, registro_origem_id,
      carga_id, fornecedor_codigo, numero_documento,
      campo_divergente, valor_esperado, valor_encontrado, diferenca_abs, mensagem
    ) VALUES (
      'contagem_frigo_vs_classic',
      CASE WHEN ABS(NEW.qtd_frigo - v_qtd_classic) > 10 THEN 'critica' ELSE 'atencao' END,
      'atak_cargas_raw', NEW.id,
      NEW.id, NEW.fornecedor_codigo, NEW.numero_pcr,
      'qtd_frigo vs qtd_classic',
      v_qtd_classic::TEXT,
      NEW.qtd_frigo::TEXT,
      ABS(NEW.qtd_frigo - v_qtd_classic),
      FORMAT('Contagem frigorífico(%s) ≠ Contagem interna(%s), diferença: %s',
        NEW.qtd_frigo, v_qtd_classic, ABS(NEW.qtd_frigo - v_qtd_classic))
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_contagem_frigo
  AFTER INSERT ON atak_cargas_raw
  FOR EACH ROW
  EXECUTE FUNCTION fn_validar_contagem_frigo();

-- ────────────────────────────────────────────────────────────────────────────
-- 13. Function: Atualizar score do fornecedor e marcar crítico
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_atualizar_score_fornecedor(p_fornecedor_codigo TEXT)
RETURNS VOID AS $$
DECLARE
  v_total_pecas   INTEGER;
  v_pecas_a       INTEGER;
  v_pecas_b       INTEGER;
  v_pecas_c       INTEGER;
  v_total_cargas  INTEGER;
  v_pct_c         NUMERIC(5,2);
  v_limite        NUMERIC(5,2);
  v_diverg        INTEGER;
  v_score         NUMERIC(5,2);
  v_status        TEXT;
BEGIN
  -- Agregar dados dos últimos 90 dias
  SELECT
    COUNT(DISTINCT id),
    COALESCE(SUM(total_classificado), 0),
    COALESCE(SUM(class_a), 0),
    COALESCE(SUM(class_b), 0),
    COALESCE(SUM(class_c), 0)
  INTO v_total_cargas, v_total_pecas, v_pecas_a, v_pecas_b, v_pecas_c
  FROM atak_cargas_raw
  WHERE fornecedor_codigo = p_fornecedor_codigo
    AND criado_em >= NOW() - INTERVAL '90 days'
    AND status_validacao != 'invalido';

  -- Calcular %C
  IF v_total_pecas > 0 THEN
    v_pct_c := (v_pecas_c::NUMERIC / v_total_pecas) * 100;
  ELSE
    v_pct_c := 0;
  END IF;

  -- Buscar limite configurado (ou usar padrão 15%)
  SELECT COALESCE(limite_pct_c, 15.00) INTO v_limite
  FROM cq_fornecedor_score
  WHERE fornecedor_codigo = p_fornecedor_codigo;

  IF v_limite IS NULL THEN v_limite := 15.00; END IF;

  -- Contar divergências abertas
  SELECT COUNT(*) INTO v_diverg
  FROM cq_validacao_divergencias
  WHERE fornecedor_codigo = p_fornecedor_codigo
    AND status IN ('aberta','em_analise');

  -- Calcular score (100 base, desconta por %C e divergências)
  v_score := GREATEST(0, 100 - (v_pct_c * 2) - (v_diverg * 5));

  -- Determinar status
  IF v_pct_c > v_limite * 1.5 OR v_score < 30 THEN
    v_status := 'bloqueado';
  ELSIF v_pct_c > v_limite THEN
    v_status := 'critico';
  ELSIF v_pct_c > v_limite * 0.7 OR v_score < 60 THEN
    v_status := 'atencao';
  ELSE
    v_status := 'normal';
  END IF;

  -- Upsert score
  INSERT INTO cq_fornecedor_score (
    fornecedor_codigo, total_cargas, total_pecas,
    pecas_a, pecas_b, pecas_c, pct_a, pct_b, pct_c,
    total_divergencias, divergencias_abertas,
    score, status_fornecedor, is_critico, limite_pct_c,
    atualizado_em
  ) VALUES (
    p_fornecedor_codigo, v_total_cargas, v_total_pecas,
    v_pecas_a, v_pecas_b, v_pecas_c,
    CASE WHEN v_total_pecas > 0 THEN (v_pecas_a::NUMERIC / v_total_pecas) * 100 ELSE 0 END,
    CASE WHEN v_total_pecas > 0 THEN (v_pecas_b::NUMERIC / v_total_pecas) * 100 ELSE 0 END,
    v_pct_c,
    v_diverg, v_diverg,
    v_score, v_status, v_status IN ('critico','bloqueado'), v_limite,
    NOW()
  )
  ON CONFLICT (fornecedor_codigo) DO UPDATE SET
    total_cargas = EXCLUDED.total_cargas,
    total_pecas = EXCLUDED.total_pecas,
    pecas_a = EXCLUDED.pecas_a,
    pecas_b = EXCLUDED.pecas_b,
    pecas_c = EXCLUDED.pecas_c,
    pct_a = EXCLUDED.pct_a,
    pct_b = EXCLUDED.pct_b,
    pct_c = EXCLUDED.pct_c,
    total_divergencias = EXCLUDED.total_divergencias,
    divergencias_abertas = EXCLUDED.divergencias_abertas,
    score = EXCLUDED.score,
    status_fornecedor = EXCLUDED.status_fornecedor,
    is_critico = EXCLUDED.is_critico,
    atualizado_em = NOW();
END;
$$ LANGUAGE plpgsql;

-- ────────────────────────────────────────────────────────────────────────────
-- 14. View: Painel de divergências ativas
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_divergencias_ativas AS
SELECT
  d.id,
  d.tipo_divergencia,
  d.gravidade,
  d.status,
  d.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome, d.fornecedor_codigo) AS fornecedor_nome,
  d.numero_documento,
  d.campo_divergente,
  d.valor_esperado,
  d.valor_encontrado,
  d.diferenca_abs,
  d.mensagem,
  d.criado_em,
  fs.score AS fornecedor_score,
  fs.status_fornecedor
FROM cq_validacao_divergencias d
LEFT JOIN cadastros_atak ca ON ca.codigo = d.fornecedor_codigo AND ca.tipo = 'fornecedor'
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_codigo = d.fornecedor_codigo
WHERE d.status IN ('aberta','em_analise','escalada')
ORDER BY
  CASE d.gravidade WHEN 'critica' THEN 1 WHEN 'atencao' THEN 2 ELSE 3 END,
  d.criado_em DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- 15. View: Fornecedores críticos
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_fornecedores_criticos AS
SELECT
  fs.*,
  COALESCE(ca.apelido, ca.nome) AS nome_display,
  ca.uf,
  ca.cnpj
FROM cq_fornecedor_score fs
LEFT JOIN cadastros_atak ca ON ca.codigo = fs.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE fs.is_critico = true
   OR fs.status_fornecedor IN ('critico','bloqueado')
ORDER BY fs.score ASC;

-- ────────────────────────────────────────────────────────────────────────────
-- 16. Seed: Configurações iniciais dos endpoints ATAK
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO atak_sync_config (endpoint_nome, endpoint_url, metodo_http, intervalo_min, parametros_json) VALUES
  ('WRCAD009_fornecedores', 'https://api.atak.com.br/v1/cadastros/fornecedores', 'GET', 1440,
   '{"tipo": "fornecedor", "ativo": true}'::JSONB),
  ('WRMVE500_cargas', 'https://api.atak.com.br/v1/movimentacao/cargas', 'GET', 30,
   '{"data_inicio": "{{$today}}", "data_fim": "{{$today}}"}'::JSONB),
  ('WRPRD600_producao', 'https://api.atak.com.br/v1/producao/diaria', 'GET', 60,
   '{"data": "{{$today}}"}'::JSONB),
  ('WRLOG510_embarques', 'https://api.atak.com.br/v1/logistica/embarques', 'GET', 30,
   '{"data_inicio": "{{$today}}", "data_fim": "{{$today}}"}'::JSONB),
  ('WREXP_expedicao', 'https://api.atak.com.br/v1/expedicao/saidas', 'GET', 60,
   '{"data": "{{$today}}"}'::JSONB)
ON CONFLICT (endpoint_nome) DO NOTHING;

-- ── VERIFICAÇÃO ──────────────────────────────────────────────────────────
SELECT 'atak_sync_config' AS tabela, COUNT(*) FROM atak_sync_config
UNION ALL SELECT 'atak_sync_log', COUNT(*) FROM atak_sync_log
UNION ALL SELECT 'atak_cargas_raw', COUNT(*) FROM atak_cargas_raw
UNION ALL SELECT 'atak_embarques_raw', COUNT(*) FROM atak_embarques_raw
UNION ALL SELECT 'atak_producao_raw', COUNT(*) FROM atak_producao_raw
UNION ALL SELECT 'atak_expedicao_raw', COUNT(*) FROM atak_expedicao_raw
UNION ALL SELECT 'cq_validacao_divergencias', COUNT(*) FROM cq_validacao_divergencias
UNION ALL SELECT 'cq_fornecedor_score', COUNT(*) FROM cq_fornecedor_score;
