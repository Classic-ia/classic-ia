-- ══════════════════════════════════════════════════════════════
-- CLASSIC CQ — TABELAS FALTANDO
-- Cole no Supabase → SQL Editor → Run
-- ══════════════════════════════════════════════════════════════

-- cq_recebimentos
CREATE TABLE IF NOT EXISTS cq_recebimentos (
  id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  id_recebimento  TEXT    NOT NULL UNIQUE,
  fornecedor      TEXT    NOT NULL,
  fornecedor_cod  TEXT,
  produto         TEXT    NOT NULL DEFAULT 'Omaso Bovino',
  lote_atak       TEXT    NOT NULL,
  etiqueta        TEXT    NOT NULL,
  quantidade_lote INTEGER NOT NULL DEFAULT 0,
  peso_kg         NUMERIC(10,2),
  temperatura     NUMERIC(5,1),
  placa_caminhao  TEXT,
  motorista       TEXT,
  transportadora  TEXT,
  foto_etiqueta_url TEXT,
  foto_carga_url  TEXT,
  observacoes     TEXT,
  status          TEXT    DEFAULT 'pendente_inspecao',
  usuario_nome    TEXT,
  usuario_email   TEXT,
  latitude        NUMERIC(10,7),
  longitude       NUMERIC(10,7),
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE cq_recebimentos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "rec_all" ON cq_recebimentos;
CREATE POLICY "rec_all" ON cq_recebimentos FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- cq_planos_acao
CREATE TABLE IF NOT EXISTS cq_planos_acao (
  id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  fornecedor      TEXT    NOT NULL,
  fornecedor_cod  TEXT,
  lote_atak       TEXT,
  data_acao       DATE,
  problema        TEXT    NOT NULL,
  recomendacao    TEXT    NOT NULL,
  status          TEXT    DEFAULT 'aberto',
  retorno_fornecedor TEXT,
  usuario_nome    TEXT,
  usuario_email   TEXT,
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE cq_planos_acao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "plano_all" ON cq_planos_acao;
CREATE POLICY "plano_all" ON cq_planos_acao FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- cq_sessoes
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
ALTER TABLE cq_sessoes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "sess_all" ON cq_sessoes;
CREATE POLICY "sess_all" ON cq_sessoes FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- cq_audit_log
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
ALTER TABLE cq_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "audit_all" ON cq_audit_log;
CREATE POLICY "audit_all" ON cq_audit_log FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- registros_cq_inspecao (tabela principal de inspeções)
CREATE TABLE IF NOT EXISTS registros_cq_inspecao (
  id                      UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
  id_inspecao             TEXT    UNIQUE NOT NULL,
  data_inspecao           DATE    NOT NULL,
  hora_inspecao           TIME    NOT NULL,
  inspetor                TEXT    NOT NULL,
  usuario_id              UUID,
  usuario_email           TEXT,
  usuario_nome            TEXT,
  fornecedor              TEXT    NOT NULL,
  fornecedor_codigo       TEXT,
  produto                 TEXT    NOT NULL,
  lote_atak               TEXT    NOT NULL,
  etiqueta                TEXT    NOT NULL,
  quantidade_lote         INTEGER DEFAULT 0,
  quantidade_analisada    INTEGER NOT NULL DEFAULT 1,
  tipo_amostragem         TEXT    DEFAULT 'aleatoria',
  temperatura_recebimento NUMERIC(5,1),
  placa_caminhao          TEXT,
  motorista               TEXT,
  transportadora          TEXT,
  fermentacao             INTEGER DEFAULT 0,
  sem_folhas              INTEGER DEFAULT 0,
  folhas_arrebentadas     INTEGER DEFAULT 0,
  pedaco                  INTEGER DEFAULT 0,
  sujo                    INTEGER DEFAULT 0,
  rasgado                 INTEGER DEFAULT 0,
  corte_irregular         INTEGER DEFAULT 0,
  furado                  INTEGER DEFAULT 0,
  mau_cheiro              INTEGER DEFAULT 0,
  contaminacao            INTEGER DEFAULT 0,
  outro_defeito           INTEGER DEFAULT 0,
  outro_defeito_tipo      TEXT,
  total_defeitos          INTEGER DEFAULT 0,
  percentual_defeitos     NUMERIC(6,2),
  classificacao           TEXT,
  status_final            TEXT,
  obs_reprovado           TEXT,
  responsavel_decisao     TEXT,
  observacoes             TEXT,
  foto_etiqueta_url       TEXT,
  foto_produto_url        TEXT,
  foto_defeito_url        TEXT,
  latitude                NUMERIC(10,7),
  longitude               NUMERIC(10,7),
  geo_dentro_planta       BOOLEAN,
  session_token           TEXT,
  offline                 BOOLEAN DEFAULT FALSE,
  criado_em               TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em           TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE registros_cq_inspecao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "insp_all" ON registros_cq_inspecao;
CREATE POLICY "insp_all" ON registros_cq_inspecao FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- Verificar resultado
SELECT tablename AS tabela, 'criada' AS status
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'cq_recebimentos','cq_planos_acao',
    'cq_sessoes','cq_audit_log',
    'registros_cq_inspecao'
  )
ORDER BY tablename;
