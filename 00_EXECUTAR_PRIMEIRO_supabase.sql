-- ══════════════════════════════════════════════════════════════════
-- CLASSIC CQ — SQL UNIFICADO
-- Executar UMA VEZ no Supabase: Dashboard → SQL Editor → New Query
-- Cole todo este arquivo e clique em Run
-- ══════════════════════════════════════════════════════════════════
-- Ordem de execução:
--   1. Tabelas base (cadastros, produtos, defeitos)
--   2. Tabela de inspeções
--   3. Sessões e Audit Log
--   4. Políticas RLS
--   5. Views úteis
-- ══════════════════════════════════════════════════════════════════

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 1 — CADASTROS BASE
-- ──────────────────────────────────────────────────────────────────

-- 1.1 Fornecedores / Motoristas (importados do Atak)
CREATE TABLE IF NOT EXISTS cadastros_atak (
  id        UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo    TEXT    NOT NULL UNIQUE,
  nome      TEXT    NOT NULL,
  apelido   TEXT,
  cnpj      TEXT,
  uf        TEXT,
  tipo      TEXT    NOT NULL DEFAULT 'fornecedor'
              CHECK (tipo IN ('fornecedor','motorista')),
  ativo     BOOLEAN NOT NULL DEFAULT TRUE,
  criado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cadastros_tipo   ON cadastros_atak(tipo);
CREATE INDEX IF NOT EXISTS idx_cadastros_nome   ON cadastros_atak(nome);
CREATE INDEX IF NOT EXISTS idx_cadastros_codigo ON cadastros_atak(codigo);

-- 1.2 Produtos inspecionáveis
CREATE TABLE IF NOT EXISTS cq_produtos (
  id        UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo    TEXT NOT NULL UNIQUE,          -- ex: 45034
  nome      TEXT NOT NULL,                 -- ex: Omaso Bovino Congelado
  grupo     TEXT,                          -- ex: Bucho, Subproduto
  ativo     BOOLEAN DEFAULT TRUE,
  criado_em TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO cq_produtos (codigo, nome, grupo) VALUES
  ('45034', 'Omaso Bovino Congelado',  'Bucho'),
  ('45084', 'Frozen Beef Omassum A',   'Bucho'),
  ('45085', 'Frozen Beef Omassum B',   'Bucho'),
  ('45086', 'Frozen Beef Omassum C',   'Bucho'),
  ('05001', 'Omaso Bovino Salgado',    'Bucho'),
  ('AORTA', 'Aorta Bovina',           'Subproduto'),
  ('TENDAO','Tendão Bovino',           'Subproduto'),
  ('RUMEN', 'Rúmen Bovino',            'Subproduto')
ON CONFLICT (codigo) DO NOTHING;

-- 1.3 Tipos de defeito padronizados
CREATE TABLE IF NOT EXISTS cq_tipos_defeito (
  id          UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo      TEXT    NOT NULL UNIQUE,   -- ex: fermentacao
  nome        TEXT    NOT NULL,          -- ex: Fermentação
  descricao   TEXT,
  gravidade   TEXT    DEFAULT 'media'
                CHECK (gravidade IN ('baixa','media','alta','critica')),
  ativo       BOOLEAN DEFAULT TRUE,
  ordem       INT     DEFAULT 99,
  criado_em   TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO cq_tipos_defeito (codigo, nome, gravidade, ordem) VALUES
  ('fermentacao',         'Fermentação',          'alta',   1),
  ('sem_folhas',          'Sem Folhas',            'media',  2),
  ('folhas_arrebentadas', 'Folhas Arrebentadas',   'media',  3),
  ('pedaco',              'Pedaço',                'media',  4),
  ('sujo',                'Sujo / Despojo',        'alta',   5),
  ('rasgado',             'Rasgado',               'media',  6),
  ('corte_irregular',     'Corte Irregular',       'baixa',  7),
  ('furado',              'Furado',                'media',  8),
  ('mau_cheiro',          'Mau Cheiro',            'alta',   9),
  ('contaminacao',        'Contaminação',          'critica',10),
  ('outro_defeito',       'Outro Defeito',         'media',  11)
ON CONFLICT (codigo) DO NOTHING;

-- 1.4 Analisadores / Usuários do sistema
CREATE TABLE IF NOT EXISTS cq_usuarios (
  id          UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  auth_id     UUID    UNIQUE,            -- auth.users.id (pode ser nulo se não tem login)
  email       TEXT    UNIQUE NOT NULL,
  nome        TEXT    NOT NULL,
  perfil      TEXT    NOT NULL DEFAULT 'analisador'
                CHECK (perfil IN ('gestor','analisador','conferente','qualidade_externo','financeiro')),
  ativo       BOOLEAN DEFAULT TRUE,
  criado_em   TIMESTAMPTZ DEFAULT NOW(),
  ultimo_login TIMESTAMPTZ
);
INSERT INTO cq_usuarios (email, nome, perfil) VALUES
  ('janaina@classic.com.br', 'Janaína Anacleto', 'gestor')
ON CONFLICT (email) DO NOTHING;

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 2 — TABELA PRINCIPAL DE INSPEÇÕES
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS registros_cq_inspecao (

  -- Identificação
  id                      UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  id_inspecao             TEXT    UNIQUE NOT NULL,   -- CQ-AAMMDD-HHMM-XXX
  data_inspecao           DATE    NOT NULL,
  hora_inspecao           TIME    NOT NULL,

  -- Analisador (preenchido pelo auth)
  inspetor                TEXT    NOT NULL,
  usuario_id              UUID,
  usuario_email           TEXT,
  usuario_nome            TEXT,

  -- Fornecedor / Produto / Lote
  fornecedor              TEXT    NOT NULL,
  fornecedor_codigo       TEXT,
  produto                 TEXT    NOT NULL,
  lote_atak               TEXT    NOT NULL,
  etiqueta                TEXT    NOT NULL,
  quantidade_lote         INTEGER DEFAULT 0,
  quantidade_analisada    INTEGER NOT NULL CHECK (quantidade_analisada > 0),
  tipo_amostragem         TEXT    DEFAULT 'aleatoria',

  -- Temperatura e transporte
  temperatura_recebimento NUMERIC(5,1),
  placa_caminhao          TEXT,
  motorista               TEXT,
  transportadora          TEXT,

  -- Defeitos (11 tipos padronizados)
  fermentacao             INTEGER DEFAULT 0 CHECK (fermentacao >= 0),
  sem_folhas              INTEGER DEFAULT 0 CHECK (sem_folhas >= 0),
  folhas_arrebentadas     INTEGER DEFAULT 0 CHECK (folhas_arrebentadas >= 0),
  pedaco                  INTEGER DEFAULT 0 CHECK (pedaco >= 0),
  sujo                    INTEGER DEFAULT 0 CHECK (sujo >= 0),
  rasgado                 INTEGER DEFAULT 0 CHECK (rasgado >= 0),
  corte_irregular         INTEGER DEFAULT 0 CHECK (corte_irregular >= 0),
  furado                  INTEGER DEFAULT 0 CHECK (furado >= 0),
  mau_cheiro              INTEGER DEFAULT 0 CHECK (mau_cheiro >= 0),
  contaminacao            INTEGER DEFAULT 0 CHECK (contaminacao >= 0),
  outro_defeito           INTEGER DEFAULT 0 CHECK (outro_defeito >= 0),
  outro_defeito_tipo      TEXT,   -- descrição livre: "queimado", "contaminação plástica"

  -- Resultados calculados
  total_defeitos          INTEGER DEFAULT 0,
  percentual_defeitos     NUMERIC(6,2),
  classificacao           TEXT    CHECK (classificacao IN ('A','B','C')),
  status_final            TEXT    CHECK (status_final IN ('aprovado','ressalva','bloqueado')),

  -- Decisão
  obs_reprovado           TEXT,
  responsavel_decisao     TEXT,
  observacoes             TEXT,

  -- Evidências
  foto_etiqueta_url       TEXT,
  foto_produto_url        TEXT,

  -- Geolocalização (anti-fraude)
  latitude                NUMERIC(10,7),
  longitude               NUMERIC(10,7),
  geo_dentro_planta       BOOLEAN,
  session_token           TEXT,

  -- Controle
  offline                 BOOLEAN DEFAULT FALSE,
  criado_em               TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em           TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_insp_lote      ON registros_cq_inspecao(lote_atak);
CREATE INDEX IF NOT EXISTS idx_insp_forn      ON registros_cq_inspecao(fornecedor);
CREATE INDEX IF NOT EXISTS idx_insp_data      ON registros_cq_inspecao(data_inspecao DESC);
CREATE INDEX IF NOT EXISTS idx_insp_usuario   ON registros_cq_inspecao(usuario_id);
CREATE INDEX IF NOT EXISTS idx_insp_cls       ON registros_cq_inspecao(classificacao);
CREATE INDEX IF NOT EXISTS idx_insp_status    ON registros_cq_inspecao(status_final);
CREATE INDEX IF NOT EXISTS idx_insp_criado    ON registros_cq_inspecao(criado_em DESC);

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 3 — SESSÕES E AUDIT LOG
-- ──────────────────────────────────────────────────────────────────

-- 3.1 Sessões ativas (sessão única por usuário)
CREATE TABLE IF NOT EXISTS cq_sessoes (
  id             UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID    NOT NULL,
  usuario_email  TEXT    NOT NULL,
  session_token  TEXT    NOT NULL UNIQUE,
  dispositivo    TEXT,
  criado_em      TIMESTAMPTZ DEFAULT NOW(),
  ultimo_acesso  TIMESTAMPTZ DEFAULT NOW(),
  expira_em      TIMESTAMPTZ DEFAULT NOW() + INTERVAL '1 hour',
  encerrada_em   TIMESTAMPTZ,
  ativa          BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_sess_usuario ON cq_sessoes(usuario_id);
CREATE INDEX IF NOT EXISTS idx_sess_token   ON cq_sessoes(session_token);
CREATE INDEX IF NOT EXISTS idx_sess_ativa   ON cq_sessoes(ativa) WHERE ativa = TRUE;

-- 3.2 Audit log imutável
CREATE TABLE IF NOT EXISTS cq_audit_log (
  id             UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id     UUID,
  usuario_email  TEXT,
  usuario_nome   TEXT,
  acao           TEXT    NOT NULL,
  detalhes       TEXT,
  latitude       NUMERIC(10,7),
  longitude      NUMERIC(10,7),
  dentro_planta  BOOLEAN,
  criado_em      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_usuario ON cq_audit_log(usuario_id);
CREATE INDEX IF NOT EXISTS idx_audit_acao    ON cq_audit_log(acao);
CREATE INDEX IF NOT EXISTS idx_audit_dt      ON cq_audit_log(criado_em DESC);

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 4 — RLS (Row Level Security)
-- ──────────────────────────────────────────────────────────────────

-- Inspeções: acesso livre via anon key (controle feito na aplicação)
ALTER TABLE registros_cq_inspecao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "insp_all" ON registros_cq_inspecao;
CREATE POLICY "insp_all" ON registros_cq_inspecao FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cq_sessoes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "sess_all" ON cq_sessoes;
CREATE POLICY "sess_all" ON cq_sessoes FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cq_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "audit_all" ON cq_audit_log;
CREATE POLICY "audit_all" ON cq_audit_log FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cq_usuarios ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "usuarios_all" ON cq_usuarios;
CREATE POLICY "usuarios_all" ON cq_usuarios FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cadastros_atak ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "atak_all" ON cadastros_atak;
CREATE POLICY "atak_all" ON cadastros_atak FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cq_tipos_defeito ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "def_all" ON cq_tipos_defeito;
CREATE POLICY "def_all" ON cq_tipos_defeito FOR ALL USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE cq_produtos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "prod_all" ON cq_produtos;
CREATE POLICY "prod_all" ON cq_produtos FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 5 — VIEWS ÚTEIS
-- ──────────────────────────────────────────────────────────────────

-- 5.1 View de inspeções enriquecida
CREATE OR REPLACE VIEW vw_inspecoes AS
SELECT
  i.*,
  CASE
    WHEN i.percentual_defeitos <= 5  THEN 'Aprovado'
    WHEN i.percentual_defeitos <= 15 THEN 'Aprovado com Ressalva'
    WHEN i.percentual_defeitos <= 30 THEN 'Sujeito a Negociação'
    ELSE 'Reprovado'
  END AS classificacao_label,
  DATE_TRUNC('week', i.data_inspecao) AS semana,
  DATE_TRUNC('month', i.data_inspecao) AS mes
FROM registros_cq_inspecao i;

-- 5.2 View de auditoria
CREATE OR REPLACE VIEW vw_cq_auditoria AS
SELECT
  a.criado_em,
  a.usuario_nome,
  a.usuario_email,
  a.acao,
  a.dentro_planta,
  a.latitude,
  a.longitude,
  (a.detalhes::jsonb ->> 'lote_atak')     AS lote,
  (a.detalhes::jsonb ->> 'fornecedor')    AS fornecedor,
  (a.detalhes::jsonb ->> 'classificacao') AS classificacao
FROM cq_audit_log a
ORDER BY a.criado_em DESC;

-- 5.3 Ranking de fornecedores
CREATE OR REPLACE VIEW vw_ranking_fornecedores AS
SELECT
  fornecedor,
  COUNT(*)                                           AS total_lotes,
  SUM(quantidade_analisada)                          AS total_amostras,
  SUM(total_defeitos)                                AS total_defeitos,
  ROUND(AVG(percentual_defeitos), 2)                 AS pct_medio,
  COUNT(*) FILTER (WHERE classificacao = 'A')        AS lotes_A,
  COUNT(*) FILTER (WHERE classificacao = 'B')        AS lotes_B,
  COUNT(*) FILTER (WHERE classificacao = 'C')        AS lotes_C,
  ROUND(
    100 - (
      COUNT(*) FILTER (WHERE classificacao = 'C')::NUMERIC / NULLIF(COUNT(*),0) * 50 +
      COUNT(*) FILTER (WHERE classificacao = 'B')::NUMERIC / NULLIF(COUNT(*),0) * 20 -
      COUNT(*) FILTER (WHERE classificacao = 'A')::NUMERIC / NULLIF(COUNT(*),0) * 20
    ), 0
  )                                                  AS score_cq
FROM registros_cq_inspecao
GROUP BY fornecedor
ORDER BY score_cq DESC;

-- 5.4 Alertas: fornecedores com 3+ lotes C consecutivos (últimos 30 dias)
CREATE OR REPLACE VIEW vw_alertas_fornecedor AS
WITH ultimos AS (
  SELECT
    fornecedor,
    lote_atak,
    data_inspecao,
    classificacao,
    ROW_NUMBER() OVER (PARTITION BY fornecedor ORDER BY data_inspecao DESC) AS rn
  FROM registros_cq_inspecao
  WHERE data_inspecao >= CURRENT_DATE - 30
)
SELECT
  fornecedor,
  COUNT(*) FILTER (WHERE classificacao = 'C') AS lotes_C_recentes,
  MAX(data_inspecao) AS ultimo_lote,
  CASE
    WHEN COUNT(*) FILTER (WHERE classificacao = 'C') >= 3 THEN 'CRITICO'
    WHEN COUNT(*) FILTER (WHERE classificacao = 'C') >= 2 THEN 'ATENCAO'
    ELSE 'OK'
  END AS nivel_alerta
FROM ultimos
WHERE rn <= 5
GROUP BY fornecedor
HAVING COUNT(*) FILTER (WHERE classificacao = 'C') >= 2
ORDER BY lotes_C_recentes DESC;

-- ──────────────────────────────────────────────────────────────────
-- BLOCO 6 — FUNÇÃO DE LIMPEZA DE SESSÕES
-- ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION cq_limpar_sessoes()
RETURNS INTEGER AS $$
DECLARE v INT;
BEGIN
  UPDATE cq_sessoes SET ativa = FALSE, encerrada_em = NOW()
  WHERE ativa = TRUE AND expira_em < NOW();
  GET DIAGNOSTICS v = ROW_COUNT;
  RETURN v;
END;
$$ LANGUAGE plpgsql;

-- ──────────────────────────────────────────────────────────────────
-- VERIFICAÇÃO FINAL
-- ──────────────────────────────────────────────────────────────────
SELECT
  schemaname,
  tablename,
  'tabela' AS tipo
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'cadastros_atak','cq_produtos','cq_tipos_defeito','cq_usuarios',
    'registros_cq_inspecao','cq_sessoes','cq_audit_log'
  )
UNION ALL
SELECT 'public', viewname, 'view'
FROM pg_views
WHERE schemaname = 'public'
  AND viewname IN ('vw_inspecoes','vw_ranking_fornecedores','vw_alertas_fornecedor','vw_cq_auditoria')
ORDER BY tipo, tablename;
