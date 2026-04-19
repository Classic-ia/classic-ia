-- =====================================================================
-- Fase 6.5 - Telemetria Onixsat (schema + views)
-- =====================================================================
-- Ingere eventos de telemetria (webhook ou pull), cruza com manutencao,
-- pneus, combustivel, viagens e motorista.
--
-- Tabelas criadas:
--   - trans_onixsat_config (1 linha de configuracao da integracao)
--   - trans_telemetria_eventos (ledger bruto, idempotente)
--   - trans_telemetria_km_diario (consolidado diario)
--   - trans_geofences (cercas virtuais com centro+raio)
--   - trans_motorista_score (agregados periodicos)
--   - trans_alertas_automaticos (dedupe por tipo+entidade+dia)
--
-- Alteracoes em trans_viagens (Fase 6.2 criou stub):
--   + geofence_origem_id, geofence_destino_id, paradas_count,
--     tempo_total_min, velocidade_media
--
-- Observacoes:
--   - Sem PostGIS: geofences por centro(lat,lng) + raio(m). Suficiente
--     para origem/destino de viagem. PostGIS fica para backlog.
--   - trans_telemetria_eventos NAO particionada no MVP. Indices por
--     (veiculo_id, data_hora) cobrem acesso. Particionamento vira
--     quando volume justificar (backlog).
-- =====================================================================

-- ---------------------------------------------------------------------
-- trans_onixsat_config (1 linha)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_onixsat_config (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  api_url text,
  api_key_encrypted bytea,  -- pgcrypto se disponivel; senao armazena bytes brutos
  webhook_secret text,       -- HMAC shared secret para validar webhook
  modo_ingestao text NOT NULL DEFAULT 'webhook' CHECK (modo_ingestao IN ('webhook','pull')),
  pull_interval_min int NOT NULL DEFAULT 5 CHECK (pull_interval_min >= 1),
  ultimo_sync timestamptz,
  ultimo_erro text,
  ativo boolean NOT NULL DEFAULT false,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid
);

CREATE TRIGGER trans_onixsat_config_upd BEFORE UPDATE ON trans_onixsat_config
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

ALTER TABLE trans_onixsat_config ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_onixsat_config_sel ON trans_onixsat_config;
CREATE POLICY trans_onixsat_config_sel ON trans_onixsat_config FOR SELECT TO authenticated
USING (trans_current_perfil() = 'admin');

DROP POLICY IF EXISTS trans_onixsat_config_ins ON trans_onixsat_config;
CREATE POLICY trans_onixsat_config_ins ON trans_onixsat_config FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() = 'admin');

DROP POLICY IF EXISTS trans_onixsat_config_upd ON trans_onixsat_config;
CREATE POLICY trans_onixsat_config_upd ON trans_onixsat_config FOR UPDATE TO authenticated
USING (trans_current_perfil() = 'admin');

-- ---------------------------------------------------------------------
-- trans_geofences (centro + raio em metros, sem PostGIS)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_geofences (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text NOT NULL UNIQUE,
  tipo text NOT NULL CHECK (tipo IN ('origem','destino','oficina','posto_proprio','cliente','filial','ponto_parada','outro')),
  latitude numeric(10,7) NOT NULL,
  longitude numeric(10,7) NOT NULL,
  raio_metros numeric(10,2) NOT NULL CHECK (raio_metros > 0),
  ativo boolean NOT NULL DEFAULT true,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid,
  updated_by uuid
);

CREATE INDEX IF NOT EXISTS idx_trans_geofences_tipo_ativo ON trans_geofences(tipo) WHERE ativo = true;

CREATE TRIGGER trans_geofences_upd BEFORE UPDATE ON trans_geofences
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

ALTER TABLE trans_geofences ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_geofences_sel ON trans_geofences;
CREATE POLICY trans_geofences_sel ON trans_geofences FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_geofences_ins ON trans_geofences;
CREATE POLICY trans_geofences_ins ON trans_geofences FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_geofences_upd ON trans_geofences;
CREATE POLICY trans_geofences_upd ON trans_geofences FOR UPDATE TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_geofences_del ON trans_geofences;
CREATE POLICY trans_geofences_del ON trans_geofences FOR DELETE TO authenticated
USING (trans_current_perfil() = 'admin');

-- ---------------------------------------------------------------------
-- Estender trans_viagens (stub criado em 6.2) com campos de telemetria
-- ---------------------------------------------------------------------
ALTER TABLE trans_viagens
  ADD COLUMN IF NOT EXISTS geofence_origem_id uuid REFERENCES trans_geofences(id),
  ADD COLUMN IF NOT EXISTS geofence_destino_id uuid REFERENCES trans_geofences(id),
  ADD COLUMN IF NOT EXISTS paradas_count int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tempo_total_min int,
  ADD COLUMN IF NOT EXISTS velocidade_media numeric(6,2),
  ADD COLUMN IF NOT EXISTS velocidade_maxima numeric(6,2),
  ADD COLUMN IF NOT EXISTS excessos_velocidade int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS litros_consumidos numeric(10,2),
  ADD COLUMN IF NOT EXISTS fechada_automaticamente boolean DEFAULT false;

-- ---------------------------------------------------------------------
-- trans_telemetria_eventos (ledger bruto idempotente)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_telemetria_eventos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  veiculo_id uuid REFERENCES trans_veiculos(id),
  placa_externa text NOT NULL,
  data_hora timestamptz NOT NULL,
  km_veiculo numeric(12,2),
  velocidade numeric(6,2),
  ignicao boolean,
  latitude numeric(10,7),
  longitude numeric(10,7),
  tipo_evento text NOT NULL CHECK (tipo_evento IN (
    'posicao','ignicao_on','ignicao_off','velocidade_excesso',
    'frenagem_brusca','parada_longa','geofence_in','geofence_out'
  )),
  geofence_id uuid REFERENCES trans_geofences(id),
  viagem_id uuid REFERENCES trans_viagens(id),
  raw jsonb,
  processado boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (veiculo_id, data_hora, tipo_evento)
);

CREATE INDEX IF NOT EXISTS idx_trans_telem_ev_veiculo_data
  ON trans_telemetria_eventos(veiculo_id, data_hora DESC);
CREATE INDEX IF NOT EXISTS idx_trans_telem_ev_placa_data
  ON trans_telemetria_eventos(placa_externa, data_hora DESC);
CREATE INDEX IF NOT EXISTS idx_trans_telem_ev_tipo_data
  ON trans_telemetria_eventos(tipo_evento, data_hora DESC);
CREATE INDEX IF NOT EXISTS idx_trans_telem_ev_nao_proc
  ON trans_telemetria_eventos(created_at) WHERE processado = false;

ALTER TABLE trans_telemetria_eventos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_telem_ev_sel ON trans_telemetria_eventos;
CREATE POLICY trans_telem_ev_sel ON trans_telemetria_eventos FOR SELECT TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_telem_ev_ins ON trans_telemetria_eventos;
CREATE POLICY trans_telem_ev_ins ON trans_telemetria_eventos FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- trans_telemetria_km_diario (consolidado)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_telemetria_km_diario (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id),
  data date NOT NULL,
  km_inicio numeric(12,2),
  km_fim numeric(12,2),
  km_rodado numeric(12,2) GENERATED ALWAYS AS (
    CASE WHEN km_fim IS NOT NULL AND km_inicio IS NOT NULL
         THEN km_fim - km_inicio ELSE NULL END
  ) STORED,
  horas_ligadas numeric(6,2),
  horas_paradas numeric(6,2),
  excessos_velocidade int DEFAULT 0,
  frenagens_bruscas int DEFAULT 0,
  velocidade_maxima numeric(6,2),
  eventos_total int DEFAULT 0,
  consolidado_em timestamptz NOT NULL DEFAULT now(),
  UNIQUE (veiculo_id, data)
);

CREATE INDEX IF NOT EXISTS idx_trans_telem_km_data
  ON trans_telemetria_km_diario(data DESC);
CREATE INDEX IF NOT EXISTS idx_trans_telem_km_veiculo_data
  ON trans_telemetria_km_diario(veiculo_id, data DESC);

ALTER TABLE trans_telemetria_km_diario ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_telem_km_sel ON trans_telemetria_km_diario;
CREATE POLICY trans_telem_km_sel ON trans_telemetria_km_diario FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_telem_km_ins ON trans_telemetria_km_diario;
CREATE POLICY trans_telem_km_ins ON trans_telemetria_km_diario FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_telem_km_upd ON trans_telemetria_km_diario;
CREATE POLICY trans_telem_km_upd ON trans_telemetria_km_diario FOR UPDATE TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- trans_motorista_score (agregado periodico)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_motorista_score (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  motorista_id uuid NOT NULL REFERENCES trans_motoristas(id),
  periodo text NOT NULL CHECK (periodo IN ('diario','semanal','mensal')),
  data_ref date NOT NULL,
  excessos_velocidade int DEFAULT 0,
  frenagens_bruscas int DEFAULT 0,
  tempo_parado_min int DEFAULT 0,
  km_rodados numeric(12,2) DEFAULT 0,
  consumo_medio_km_l numeric(6,2),
  viagens_count int DEFAULT 0,
  score_calc int CHECK (score_calc BETWEEN 0 AND 100),
  observacao text,
  calculado_em timestamptz NOT NULL DEFAULT now(),
  UNIQUE (motorista_id, periodo, data_ref)
);

CREATE INDEX IF NOT EXISTS idx_trans_motor_score_periodo
  ON trans_motorista_score(periodo, data_ref DESC);

ALTER TABLE trans_motorista_score ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_motor_score_sel ON trans_motorista_score;
CREATE POLICY trans_motor_score_sel ON trans_motorista_score FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_motor_score_ins ON trans_motorista_score;
CREATE POLICY trans_motor_score_ins ON trans_motorista_score FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_motor_score_upd ON trans_motorista_score;
CREATE POLICY trans_motor_score_upd ON trans_motorista_score FOR UPDATE TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- trans_alertas_automaticos (motor de regras)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_alertas_automaticos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo text NOT NULL CHECK (tipo IN (
    'manutencao_vencida','consumo_desvio','parado_excessivo',
    'pneu_vida_util','lote_vencendo','velocidade_excessiva',
    'cnh_vencendo','outro'
  )),
  severidade text NOT NULL CHECK (severidade IN ('info','aviso','critico')),
  entidade_tipo text NOT NULL CHECK (entidade_tipo IN (
    'veiculo','pneu','lote','motorista','viagem','outro'
  )),
  entidade_id uuid,
  mensagem text NOT NULL,
  contexto jsonb,
  gerado_em timestamptz NOT NULL DEFAULT now(),
  data_dia date GENERATED ALWAYS AS ((gerado_em AT TIME ZONE 'UTC')::date) STORED,
  resolvido_em timestamptz,
  resolvido_por uuid,
  resolucao text,
  UNIQUE (tipo, entidade_id, data_dia)
);

CREATE INDEX IF NOT EXISTS idx_trans_alertas_abertos
  ON trans_alertas_automaticos(tipo, severidade, gerado_em DESC)
  WHERE resolvido_em IS NULL;
CREATE INDEX IF NOT EXISTS idx_trans_alertas_entidade
  ON trans_alertas_automaticos(entidade_tipo, entidade_id)
  WHERE resolvido_em IS NULL;

ALTER TABLE trans_alertas_automaticos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_alertas_sel ON trans_alertas_automaticos;
CREATE POLICY trans_alertas_sel ON trans_alertas_automaticos FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_alertas_ins ON trans_alertas_automaticos;
CREATE POLICY trans_alertas_ins ON trans_alertas_automaticos FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_alertas_upd ON trans_alertas_automaticos;
CREATE POLICY trans_alertas_upd ON trans_alertas_automaticos FOR UPDATE TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- Views auxiliares
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_telemetria_eventos_full AS
SELECT
  e.id, e.veiculo_id, e.placa_externa, e.data_hora, e.km_veiculo,
  e.velocidade, e.ignicao, e.latitude, e.longitude, e.tipo_evento,
  e.geofence_id, e.viagem_id, e.processado, e.created_at,
  v.placa AS veiculo_placa, v.modelo AS veiculo_modelo,
  g.nome AS geofence_nome, g.tipo AS geofence_tipo
FROM trans_telemetria_eventos e
LEFT JOIN trans_veiculos v ON v.id = e.veiculo_id
LEFT JOIN trans_geofences g ON g.id = e.geofence_id;

GRANT SELECT ON v_trans_telemetria_eventos_full TO authenticated;

CREATE OR REPLACE VIEW v_trans_viagens_full AS
SELECT
  v.*,
  ve.placa AS veiculo_placa,
  ve.modelo AS veiculo_modelo,
  m.nome AS motorista_nome,
  go.nome AS origem_nome,
  gd.nome AS destino_nome,
  (SELECT count(*) FROM trans_tms_viagens_ref r WHERE r.viagem_id = v.id) AS ctes_count
FROM trans_viagens v
LEFT JOIN trans_veiculos ve ON ve.id = v.veiculo_id
LEFT JOIN trans_motoristas m ON m.id = v.motorista_id
LEFT JOIN trans_geofences go ON go.id = v.geofence_origem_id
LEFT JOIN trans_geofences gd ON gd.id = v.geofence_destino_id;

GRANT SELECT ON v_trans_viagens_full TO authenticated;

CREATE OR REPLACE VIEW v_trans_motorista_score_ranking AS
SELECT
  s.*,
  m.nome AS motorista_nome,
  rank() OVER (PARTITION BY s.periodo, s.data_ref ORDER BY s.score_calc DESC NULLS LAST) AS posicao
FROM trans_motorista_score s
JOIN trans_motoristas m ON m.id = s.motorista_id;

GRANT SELECT ON v_trans_motorista_score_ranking TO authenticated;

CREATE OR REPLACE VIEW v_trans_alertas_abertos AS
SELECT
  a.*,
  CASE a.entidade_tipo
    WHEN 'veiculo' THEN (SELECT placa FROM trans_veiculos WHERE id = a.entidade_id)
    WHEN 'pneu' THEN (SELECT codigo_interno FROM trans_pneus WHERE id = a.entidade_id)
    WHEN 'motorista' THEN (SELECT nome FROM trans_motoristas WHERE id = a.entidade_id)
    WHEN 'lote' THEN (SELECT lote_interno FROM trans_lotes WHERE id = a.entidade_id)
    ELSE NULL
  END AS entidade_label
FROM trans_alertas_automaticos a
WHERE a.resolvido_em IS NULL;

GRANT SELECT ON v_trans_alertas_abertos TO authenticated;
