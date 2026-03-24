-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO: SLA de Sync + Notificações + Histórico de Score
-- Rodar APÓS 02_migration_guards.sql
-- ══════════════════════════════════════════════════════════════════════════════

-- ════════════════════════════════════════════════════════════════════════════
-- MÓDULO 3: Dashboard SLA de Sync
-- Monitorar se os fluxos n8n estão rodando dentro do intervalo configurado
-- ════════════════════════════════════════════════════════════════════════════

-- 3.1 View: Status de SLA em tempo real por endpoint
CREATE OR REPLACE VIEW vw_sync_sla_status AS
WITH ultimo_sync AS (
  SELECT DISTINCT ON (endpoint_nome)
    endpoint_nome,
    id AS ultimo_log_id,
    inicio,
    fim,
    status,
    total_registros,
    novos,
    atualizados,
    erros,
    mensagem,
    n8n_execution_id
  FROM atak_sync_log
  ORDER BY endpoint_nome, inicio DESC
),
penultimo_sync AS (
  SELECT DISTINCT ON (endpoint_nome)
    endpoint_nome,
    inicio AS penultimo_inicio,
    status AS penultimo_status
  FROM atak_sync_log
  WHERE (endpoint_nome, inicio) NOT IN (
    SELECT endpoint_nome, inicio FROM ultimo_sync
  )
  ORDER BY endpoint_nome, inicio DESC
)
SELECT
  cfg.endpoint_nome,
  cfg.intervalo_min,
  cfg.ativo AS config_ativa,
  us.inicio AS ultima_execucao,
  us.fim AS ultima_finalizacao,
  us.status AS ultimo_status,
  us.total_registros,
  us.novos,
  us.atualizados,
  us.erros,
  us.mensagem AS ultima_mensagem,
  us.n8n_execution_id,

  -- Tempo desde última execução (em minutos)
  EXTRACT(EPOCH FROM (NOW() - COALESCE(us.inicio, cfg.criado_em))) / 60
    AS minutos_desde_ultimo_sync,

  -- Duração da última execução (em segundos)
  EXTRACT(EPOCH FROM (us.fim - us.inicio))
    AS duracao_segundos,

  -- SLA: está dentro do intervalo?
  CASE
    WHEN NOT cfg.ativo THEN 'desativado'
    WHEN us.inicio IS NULL THEN 'nunca_executado'
    WHEN us.status = 'em_execucao' THEN 'executando'
    WHEN EXTRACT(EPOCH FROM (NOW() - us.inicio)) / 60 > cfg.intervalo_min * 2 THEN 'critico'
    WHEN EXTRACT(EPOCH FROM (NOW() - us.inicio)) / 60 > cfg.intervalo_min * 1.5 THEN 'atrasado'
    WHEN us.status = 'erro' THEN 'erro'
    WHEN us.status = 'parcial' THEN 'parcial'
    ELSE 'ok'
  END AS sla_status,

  -- Próxima execução esperada
  COALESCE(us.inicio, cfg.criado_em) + (cfg.intervalo_min || ' minutes')::INTERVAL
    AS proxima_execucao_esperada,

  -- Minutos de atraso (negativo = adiantado)
  GREATEST(0,
    EXTRACT(EPOCH FROM (NOW() - (COALESCE(us.inicio, cfg.criado_em)
      + (cfg.intervalo_min || ' minutes')::INTERVAL))) / 60
  ) AS minutos_atraso,

  -- Penúltima execução (para tendência)
  ps.penultimo_inicio,
  ps.penultimo_status

FROM atak_sync_config cfg
LEFT JOIN ultimo_sync us ON us.endpoint_nome = cfg.endpoint_nome
LEFT JOIN penultimo_sync ps ON ps.endpoint_nome = cfg.endpoint_nome
ORDER BY
  CASE
    WHEN NOT cfg.ativo THEN 9
    WHEN us.inicio IS NULL THEN 1
    WHEN us.status = 'erro' THEN 2
    WHEN EXTRACT(EPOCH FROM (NOW() - us.inicio)) / 60 > cfg.intervalo_min * 2 THEN 3
    WHEN EXTRACT(EPOCH FROM (NOW() - us.inicio)) / 60 > cfg.intervalo_min * 1.5 THEN 4
    ELSE 8
  END,
  cfg.endpoint_nome;

-- 3.2 View: Histórico de SLA (últimas 24h, para gráfico)
CREATE OR REPLACE VIEW vw_sync_sla_historico_24h AS
SELECT
  sl.endpoint_nome,
  sl.inicio,
  sl.fim,
  sl.status,
  sl.total_registros,
  sl.novos,
  sl.atualizados,
  sl.erros,
  EXTRACT(EPOCH FROM (sl.fim - sl.inicio)) AS duracao_segundos,
  sl.n8n_execution_id,
  cfg.intervalo_min,
  -- Houve atraso em relação ao sync anterior?
  LAG(sl.inicio) OVER (PARTITION BY sl.endpoint_nome ORDER BY sl.inicio) AS sync_anterior,
  EXTRACT(EPOCH FROM (sl.inicio -
    LAG(sl.inicio) OVER (PARTITION BY sl.endpoint_nome ORDER BY sl.inicio))) / 60
    AS intervalo_real_min,
  CASE
    WHEN EXTRACT(EPOCH FROM (sl.inicio -
      LAG(sl.inicio) OVER (PARTITION BY sl.endpoint_nome ORDER BY sl.inicio))) / 60
      > cfg.intervalo_min * 1.5 THEN true
    ELSE false
  END AS atrasou
FROM atak_sync_log sl
JOIN atak_sync_config cfg ON cfg.endpoint_nome = sl.endpoint_nome
WHERE sl.inicio >= NOW() - INTERVAL '24 hours'
ORDER BY sl.endpoint_nome, sl.inicio DESC;

-- 3.3 View: Métricas agregadas de SLA por endpoint (últimos 7 dias)
CREATE OR REPLACE VIEW vw_sync_sla_metricas_7d AS
SELECT
  sl.endpoint_nome,
  cfg.intervalo_min,
  COUNT(*) AS total_execucoes,
  COUNT(*) FILTER (WHERE sl.status = 'sucesso') AS execucoes_ok,
  COUNT(*) FILTER (WHERE sl.status = 'erro') AS execucoes_erro,
  COUNT(*) FILTER (WHERE sl.status = 'parcial') AS execucoes_parcial,

  -- Taxa de sucesso
  ROUND(
    COUNT(*) FILTER (WHERE sl.status = 'sucesso')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 1
  ) AS taxa_sucesso_pct,

  -- Duração média/máx/mín
  ROUND(AVG(EXTRACT(EPOCH FROM (sl.fim - sl.inicio)))::NUMERIC, 1) AS duracao_media_seg,
  ROUND(MAX(EXTRACT(EPOCH FROM (sl.fim - sl.inicio)))::NUMERIC, 1) AS duracao_max_seg,
  ROUND(MIN(EXTRACT(EPOCH FROM (sl.fim - sl.inicio)))::NUMERIC, 1) AS duracao_min_seg,

  -- Registros processados
  SUM(sl.total_registros) AS total_registros_processados,
  SUM(sl.novos) AS total_novos,
  SUM(sl.atualizados) AS total_atualizados,
  SUM(sl.erros) AS total_erros,

  -- Execuções esperadas vs realizadas
  ROUND(7 * 24 * 60.0 / cfg.intervalo_min) AS execucoes_esperadas_7d,
  ROUND(
    COUNT(*)::NUMERIC / NULLIF(ROUND(7 * 24 * 60.0 / cfg.intervalo_min), 0) * 100, 1
  ) AS cobertura_sla_pct

FROM atak_sync_log sl
JOIN atak_sync_config cfg ON cfg.endpoint_nome = sl.endpoint_nome
WHERE sl.inicio >= NOW() - INTERVAL '7 days'
GROUP BY sl.endpoint_nome, cfg.intervalo_min
ORDER BY
  ROUND(
    COUNT(*) FILTER (WHERE sl.status = 'sucesso')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 1
  ) ASC NULLS FIRST;

-- 3.4 Function: Verificar SLA e gerar alertas para atrasos
CREATE OR REPLACE FUNCTION fn_verificar_sla_sync()
RETURNS TABLE(
  endpoint_nome TEXT,
  sla_status TEXT,
  minutos_atraso NUMERIC,
  ultimo_status TEXT,
  mensagem TEXT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    v.endpoint_nome,
    v.sla_status,
    v.minutos_atraso,
    v.ultimo_status,
    CASE v.sla_status
      WHEN 'critico' THEN FORMAT('CRÍTICO: %s atrasado %s min (intervalo: %s min)',
        v.endpoint_nome, ROUND(v.minutos_atraso), v.intervalo_min)
      WHEN 'atrasado' THEN FORMAT('ATRASADO: %s atrasado %s min (intervalo: %s min)',
        v.endpoint_nome, ROUND(v.minutos_atraso), v.intervalo_min)
      WHEN 'erro' THEN FORMAT('ERRO: %s última execução falhou: %s',
        v.endpoint_nome, v.ultima_mensagem)
      WHEN 'nunca_executado' THEN FORMAT('NUNCA EXECUTADO: %s não tem histórico de sync',
        v.endpoint_nome)
      ELSE NULL
    END AS mensagem
  FROM vw_sync_sla_status v
  WHERE v.sla_status IN ('critico', 'atrasado', 'erro', 'nunca_executado');
END;
$$ LANGUAGE plpgsql;


-- ════════════════════════════════════════════════════════════════════════════
-- MÓDULO 4: Configuração de Notificações (Slack/WhatsApp/Email)
-- ════════════════════════════════════════════════════════════════════════════

-- 4.1 Tabela de canais de notificação
CREATE TABLE IF NOT EXISTS cq_canais_notificacao (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome            TEXT NOT NULL UNIQUE,                     -- ex: 'slack_qualidade', 'whatsapp_gestor'
  tipo            TEXT NOT NULL
                    CHECK (tipo IN ('slack','whatsapp','email','webhook')),
  config_json     JSONB NOT NULL,                           -- webhook_url, phone, email, etc.
  ativo           BOOLEAN DEFAULT true,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- 4.2 Tabela de regras de notificação (quando notificar)
CREATE TABLE IF NOT EXISTS cq_regras_notificacao (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nome            TEXT NOT NULL,                            -- ex: 'Divergência Crítica', 'Fornecedor Bloqueado'
  canal_id        UUID NOT NULL REFERENCES cq_canais_notificacao(id),
  evento          TEXT NOT NULL CHECK (evento IN (
    'divergencia_critica',          -- nova divergência gravidade=critica
    'fornecedor_bloqueado',         -- status_fornecedor mudou para bloqueado
    'fornecedor_critico',           -- status_fornecedor mudou para critico
    'sync_falhou',                  -- sync com status=erro
    'sync_atrasado',                -- SLA violado (atraso > 1.5x intervalo)
    'cadastro_nao_encontrado',      -- vínculo cadastral não resolvido
    'score_queda_brusca'            -- score caiu mais de 20 pontos em 24h
  )),
  filtro_json     JSONB DEFAULT '{}',                       -- filtros adicionais (ex: fornecedor_codigo, gravidade)
  template_msg    TEXT,                                     -- template da mensagem (com {{variáveis}})
  ativo           BOOLEAN DEFAULT true,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- 4.3 Fila de notificações pendentes
CREATE TABLE IF NOT EXISTS cq_fila_notificacao (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  regra_id        UUID REFERENCES cq_regras_notificacao(id),
  canal_id        UUID NOT NULL REFERENCES cq_canais_notificacao(id),
  evento          TEXT NOT NULL,

  -- Dados da notificação
  titulo          TEXT NOT NULL,
  mensagem        TEXT NOT NULL,
  dados_json      JSONB DEFAULT '{}',                       -- payload completo do evento

  -- Controle de envio
  status          TEXT NOT NULL DEFAULT 'pendente'
                    CHECK (status IN ('pendente','enviando','enviado','erro','cancelado')),
  tentativas      INTEGER DEFAULT 0,
  max_tentativas  INTEGER DEFAULT 3,
  erro_msg        TEXT,

  -- Timestamps
  agendado_para   TIMESTAMPTZ DEFAULT NOW(),
  enviado_em      TIMESTAMPTZ,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fila_notif_status ON cq_fila_notificacao(status);
CREATE INDEX IF NOT EXISTS idx_fila_notif_agendado ON cq_fila_notificacao(agendado_para)
  WHERE status = 'pendente';
CREATE INDEX IF NOT EXISTS idx_fila_notif_canal ON cq_fila_notificacao(canal_id);

-- 4.4 Tabela de log de notificações enviadas (audit)
CREATE TABLE IF NOT EXISTS cq_notificacao_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fila_id         UUID REFERENCES cq_fila_notificacao(id),
  canal_id        UUID REFERENCES cq_canais_notificacao(id),
  canal_tipo      TEXT NOT NULL,
  evento          TEXT NOT NULL,
  titulo          TEXT,
  mensagem        TEXT,
  status_http     INTEGER,
  resposta        TEXT,
  sucesso         BOOLEAN DEFAULT false,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- 4.5 RLS
ALTER TABLE cq_canais_notificacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_regras_notificacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_fila_notificacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_notificacao_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "auth_read" ON cq_canais_notificacao FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON cq_regras_notificacao FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON cq_fila_notificacao FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth_read" ON cq_notificacao_log FOR SELECT TO authenticated USING (true);
CREATE POLICY "service_write" ON cq_canais_notificacao FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON cq_regras_notificacao FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON cq_fila_notificacao FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_write" ON cq_notificacao_log FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Gestores podem gerenciar canais e regras
CREATE POLICY "gestor_manage_canais" ON cq_canais_notificacao FOR ALL TO authenticated
  USING (EXISTS (SELECT 1 FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.perfil IN ('gestor','administrador')))
  WITH CHECK (EXISTS (SELECT 1 FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.perfil IN ('gestor','administrador')));

CREATE POLICY "gestor_manage_regras" ON cq_regras_notificacao FOR ALL TO authenticated
  USING (EXISTS (SELECT 1 FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.perfil IN ('gestor','administrador')))
  WITH CHECK (EXISTS (SELECT 1 FROM cq_usuarios u WHERE u.auth_id = auth.uid() AND u.perfil IN ('gestor','administrador')));

-- 4.6 Function: Enfileirar notificação automaticamente
CREATE OR REPLACE FUNCTION fn_enfileirar_notificacao(
  p_evento TEXT,
  p_titulo TEXT,
  p_mensagem TEXT,
  p_dados JSONB DEFAULT '{}'
) RETURNS INTEGER AS $$
DECLARE
  v_regra RECORD;
  v_count INTEGER := 0;
  v_msg_final TEXT;
BEGIN
  FOR v_regra IN
    SELECT r.*, c.id AS canal_id, c.tipo AS canal_tipo, c.config_json
    FROM cq_regras_notificacao r
    JOIN cq_canais_notificacao c ON c.id = r.canal_id
    WHERE r.evento = p_evento
      AND r.ativo = true
      AND c.ativo = true
  LOOP
    -- Aplicar template se configurado
    v_msg_final := COALESCE(v_regra.template_msg, p_mensagem);
    v_msg_final := REPLACE(v_msg_final, '{{titulo}}', p_titulo);
    v_msg_final := REPLACE(v_msg_final, '{{mensagem}}', p_mensagem);
    v_msg_final := REPLACE(v_msg_final, '{{evento}}', p_evento);

    -- Verificar filtros (se configurados)
    IF v_regra.filtro_json != '{}'::JSONB THEN
      -- Simples: verificar se fornecedor_codigo bate
      IF v_regra.filtro_json ? 'fornecedor_codigo'
         AND p_dados ? 'fornecedor_codigo'
         AND v_regra.filtro_json->>'fornecedor_codigo' != p_dados->>'fornecedor_codigo' THEN
        CONTINUE;
      END IF;
    END IF;

    INSERT INTO cq_fila_notificacao (regra_id, canal_id, evento, titulo, mensagem, dados_json)
    VALUES (v_regra.id, v_regra.canal_id, p_evento, p_titulo, v_msg_final, p_dados);

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- 4.7 Trigger: Enfileirar notificação quando divergência crítica é criada
CREATE OR REPLACE FUNCTION fn_notificar_divergencia_critica()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.gravidade = 'critica' THEN
    PERFORM fn_enfileirar_notificacao(
      'divergencia_critica',
      FORMAT('Divergência Crítica: %s', NEW.tipo_divergencia),
      NEW.mensagem,
      jsonb_build_object(
        'divergencia_id', NEW.id,
        'tipo', NEW.tipo_divergencia,
        'fornecedor_codigo', NEW.fornecedor_codigo,
        'numero_documento', NEW.numero_documento,
        'campo', NEW.campo_divergente,
        'esperado', NEW.valor_esperado,
        'encontrado', NEW.valor_encontrado
      )
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notificar_divergencia_critica
  AFTER INSERT ON cq_validacao_divergencias
  FOR EACH ROW
  WHEN (NEW.gravidade = 'critica')
  EXECUTE FUNCTION fn_notificar_divergencia_critica();

-- 4.8 Trigger: Notificar quando fornecedor muda para bloqueado/crítico
CREATE OR REPLACE FUNCTION fn_notificar_fornecedor_status()
RETURNS TRIGGER AS $$
BEGIN
  -- Só notifica quando STATUS MUDOU
  IF OLD.status_fornecedor IS DISTINCT FROM NEW.status_fornecedor THEN
    IF NEW.status_fornecedor = 'bloqueado' THEN
      PERFORM fn_enfileirar_notificacao(
        'fornecedor_bloqueado',
        FORMAT('BLOQUEIO: Fornecedor %s', COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo)),
        FORMAT('Fornecedor %s bloqueado. Score: %s | %%C: %s%% | %s divergências abertas',
          COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo),
          NEW.score, NEW.pct_c, NEW.divergencias_abertas),
        to_jsonb(NEW)
      );
    ELSIF NEW.status_fornecedor = 'critico' THEN
      PERFORM fn_enfileirar_notificacao(
        'fornecedor_critico',
        FORMAT('CRÍTICO: Fornecedor %s', COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo)),
        FORMAT('Fornecedor %s em estado CRÍTICO. Score: %s | %%C: %s%% (limite: %s%%)',
          COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo),
          NEW.score, NEW.pct_c, NEW.limite_pct_c),
        to_jsonb(NEW)
      );
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notificar_fornecedor_status
  AFTER UPDATE ON cq_fornecedor_score
  FOR EACH ROW
  EXECUTE FUNCTION fn_notificar_fornecedor_status();

-- 4.9 Seed: Canais e regras padrão
INSERT INTO cq_canais_notificacao (nome, tipo, config_json) VALUES
  ('slack_qualidade', 'slack', '{"webhook_url": "https://hooks.slack.com/services/XXX/YYY/ZZZ", "channel": "#cq-alertas"}'::JSONB),
  ('whatsapp_gestor', 'whatsapp', '{"api_url": "https://api.z-api.io/instances/XXX/token/YYY/send-text", "phone": "55419XXXXXXXX"}'::JSONB),
  ('email_qualidade', 'email', '{"to": "qualidade@classiccarnes.com.br", "from": "alertas@classiccarnes.com.br"}'::JSONB),
  ('webhook_n8n', 'webhook', '{"url": "https://n8n.classiccarnes.com.br/webhook/notificacao"}'::JSONB)
ON CONFLICT (nome) DO NOTHING;

INSERT INTO cq_regras_notificacao (nome, canal_id, evento, template_msg) VALUES
  ('Divergência Crítica → Slack',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'slack_qualidade'),
   'divergencia_critica',
   ':rotating_light: *{{titulo}}*\n{{mensagem}}\nFornecedor: `{{fornecedor_codigo}}`'),

  ('Fornecedor Bloqueado → Slack + WhatsApp',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'slack_qualidade'),
   'fornecedor_bloqueado',
   ':no_entry: *BLOQUEIO DE FORNECEDOR*\n{{mensagem}}'),

  ('Fornecedor Bloqueado → WhatsApp Gestor',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'whatsapp_gestor'),
   'fornecedor_bloqueado',
   'CLASSIC CQ - BLOQUEIO DE FORNECEDOR\n{{mensagem}}'),

  ('Fornecedor Crítico → Slack',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'slack_qualidade'),
   'fornecedor_critico',
   ':warning: *FORNECEDOR CRÍTICO*\n{{mensagem}}'),

  ('Sync Falhou → Slack',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'slack_qualidade'),
   'sync_falhou',
   ':x: *Sync Falhou: {{titulo}}*\n{{mensagem}}'),

  ('Sync Atrasado → Slack',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'slack_qualidade'),
   'sync_atrasado',
   ':clock3: *Sync Atrasado: {{titulo}}*\n{{mensagem}}'),

  ('Score Queda Brusca → WhatsApp',
   (SELECT id FROM cq_canais_notificacao WHERE nome = 'whatsapp_gestor'),
   'score_queda_brusca',
   'CLASSIC CQ - QUEDA DE SCORE\n{{mensagem}}')
ON CONFLICT DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- MÓDULO 5: Histórico de Score do Fornecedor
-- ════════════════════════════════════════════════════════════════════════════

-- 5.1 Tabela de histórico (snapshot a cada recálculo)
CREATE TABLE IF NOT EXISTS cq_fornecedor_score_historico (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fornecedor_codigo TEXT NOT NULL,
  fornecedor_nome   TEXT,

  -- Snapshot das métricas
  total_cargas      INTEGER DEFAULT 0,
  total_pecas       INTEGER DEFAULT 0,
  pecas_a           INTEGER DEFAULT 0,
  pecas_b           INTEGER DEFAULT 0,
  pecas_c           INTEGER DEFAULT 0,
  pct_a             NUMERIC(5,2) DEFAULT 0,
  pct_b             NUMERIC(5,2) DEFAULT 0,
  pct_c             NUMERIC(5,2) DEFAULT 0,
  score             NUMERIC(5,2) DEFAULT 100,
  status_fornecedor TEXT,
  divergencias_abertas INTEGER DEFAULT 0,
  limite_pct_c      NUMERIC(5,2),

  -- Variação em relação ao snapshot anterior
  score_anterior    NUMERIC(5,2),
  variacao_score    NUMERIC(5,2),             -- positivo = melhorou, negativo = piorou
  status_anterior   TEXT,
  mudou_status      BOOLEAN DEFAULT false,

  -- Metadata
  calculado_em      TIMESTAMPTZ DEFAULT NOW(),
  janela_dias       INTEGER DEFAULT 90
);

CREATE INDEX IF NOT EXISTS idx_score_hist_forn ON cq_fornecedor_score_historico(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_score_hist_data ON cq_fornecedor_score_historico(calculado_em DESC);
CREATE INDEX IF NOT EXISTS idx_score_hist_forn_data ON cq_fornecedor_score_historico(fornecedor_codigo, calculado_em DESC);

ALTER TABLE cq_fornecedor_score_historico ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth_read" ON cq_fornecedor_score_historico FOR SELECT TO authenticated USING (true);
CREATE POLICY "service_write" ON cq_fornecedor_score_historico FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 5.2 Trigger: Salvar snapshot no histórico a cada UPDATE do score
CREATE OR REPLACE FUNCTION fn_historico_score()
RETURNS TRIGGER AS $$
DECLARE
  v_score_anterior   NUMERIC(5,2);
  v_status_anterior  TEXT;
  v_variacao         NUMERIC(5,2);
BEGIN
  -- Buscar último snapshot
  SELECT score, status_fornecedor
  INTO v_score_anterior, v_status_anterior
  FROM cq_fornecedor_score_historico
  WHERE fornecedor_codigo = NEW.fornecedor_codigo
  ORDER BY calculado_em DESC
  LIMIT 1;

  v_variacao := NEW.score - COALESCE(v_score_anterior, NEW.score);

  INSERT INTO cq_fornecedor_score_historico (
    fornecedor_codigo, fornecedor_nome,
    total_cargas, total_pecas, pecas_a, pecas_b, pecas_c,
    pct_a, pct_b, pct_c, score, status_fornecedor,
    divergencias_abertas, limite_pct_c,
    score_anterior, variacao_score, status_anterior, mudou_status
  ) VALUES (
    NEW.fornecedor_codigo, NEW.fornecedor_nome,
    NEW.total_cargas, NEW.total_pecas, NEW.pecas_a, NEW.pecas_b, NEW.pecas_c,
    NEW.pct_a, NEW.pct_b, NEW.pct_c, NEW.score, NEW.status_fornecedor,
    NEW.divergencias_abertas, NEW.limite_pct_c,
    v_score_anterior, v_variacao, v_status_anterior,
    v_status_anterior IS DISTINCT FROM NEW.status_fornecedor
  );

  -- Detectar queda brusca (> 20 pontos) e notificar
  IF v_variacao < -20 THEN
    PERFORM fn_enfileirar_notificacao(
      'score_queda_brusca',
      FORMAT('Queda de Score: %s', COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo)),
      FORMAT('Score de %s caiu %s pontos (%s → %s). Status: %s → %s',
        COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo),
        ABS(v_variacao), COALESCE(v_score_anterior, 0), NEW.score,
        COALESCE(v_status_anterior, 'novo'), NEW.status_fornecedor),
      jsonb_build_object(
        'fornecedor_codigo', NEW.fornecedor_codigo,
        'score_anterior', v_score_anterior,
        'score_atual', NEW.score,
        'variacao', v_variacao,
        'status_anterior', v_status_anterior,
        'status_atual', NEW.status_fornecedor
      )
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_historico_score
  AFTER INSERT OR UPDATE ON cq_fornecedor_score
  FOR EACH ROW
  EXECUTE FUNCTION fn_historico_score();

-- 5.3 View: Evolução do score por fornecedor (para gráfico)
CREATE OR REPLACE VIEW vw_fornecedor_score_evolucao AS
SELECT
  h.fornecedor_codigo,
  COALESCE(h.fornecedor_nome, ca.apelido, ca.nome) AS fornecedor_nome,
  h.calculado_em,
  h.score,
  h.pct_a,
  h.pct_b,
  h.pct_c,
  h.total_cargas,
  h.total_pecas,
  h.status_fornecedor,
  h.variacao_score,
  h.mudou_status,
  h.divergencias_abertas,
  h.limite_pct_c
FROM cq_fornecedor_score_historico h
LEFT JOIN cadastros_atak ca ON ca.codigo = h.fornecedor_codigo AND ca.tipo = 'fornecedor'
ORDER BY h.fornecedor_codigo, h.calculado_em;

-- 5.4 View: Ranking de fornecedores (snapshot atual + tendência)
CREATE OR REPLACE VIEW vw_fornecedor_ranking AS
WITH tendencia AS (
  SELECT DISTINCT ON (fornecedor_codigo)
    fornecedor_codigo,
    variacao_score AS ultima_variacao,
    mudou_status AS ultimo_mudou_status,
    calculado_em AS ultimo_calculo
  FROM cq_fornecedor_score_historico
  ORDER BY fornecedor_codigo, calculado_em DESC
),
historico_30d AS (
  SELECT
    fornecedor_codigo,
    AVG(variacao_score) AS variacao_media_30d,
    COUNT(*) AS total_snapshots_30d,
    MIN(score) AS score_minimo_30d,
    MAX(score) AS score_maximo_30d
  FROM cq_fornecedor_score_historico
  WHERE calculado_em >= NOW() - INTERVAL '30 days'
  GROUP BY fornecedor_codigo
)
SELECT
  fs.*,
  COALESCE(ca.apelido, ca.nome) AS nome_display,
  ca.uf,
  t.ultima_variacao,
  t.ultimo_mudou_status,
  CASE
    WHEN t.ultima_variacao > 5 THEN 'melhorando'
    WHEN t.ultima_variacao < -5 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,
  h.variacao_media_30d,
  h.score_minimo_30d,
  h.score_maximo_30d,
  h.total_snapshots_30d,
  ROW_NUMBER() OVER (ORDER BY fs.score DESC) AS ranking
FROM cq_fornecedor_score fs
LEFT JOIN cadastros_atak ca ON ca.codigo = fs.fornecedor_codigo AND ca.tipo = 'fornecedor'
LEFT JOIN tendencia t ON t.fornecedor_codigo = fs.fornecedor_codigo
LEFT JOIN historico_30d h ON h.fornecedor_codigo = fs.fornecedor_codigo
ORDER BY fs.score DESC;

-- 5.5 Function: Limpar histórico antigo (manter últimos 365 dias)
CREATE OR REPLACE FUNCTION fn_limpar_historico_score(p_dias INTEGER DEFAULT 365)
RETURNS INTEGER AS $$
DECLARE
  v_deletados INTEGER;
BEGIN
  DELETE FROM cq_fornecedor_score_historico
  WHERE calculado_em < NOW() - (p_dias || ' days')::INTERVAL;

  GET DIAGNOSTICS v_deletados = ROW_COUNT;
  RETURN v_deletados;
END;
$$ LANGUAGE plpgsql;


-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

-- Módulo 3: SLA
SELECT * FROM vw_sync_sla_status LIMIT 5;

-- Módulo 4: Notificações
SELECT nome, tipo, ativo FROM cq_canais_notificacao;
SELECT nome, evento, ativo FROM cq_regras_notificacao;

-- Módulo 5: Histórico
SELECT 'cq_fornecedor_score_historico' AS tabela, COUNT(*) FROM cq_fornecedor_score_historico;
