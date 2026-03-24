-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO: Observabilidade + Views Materializadas + Particionamento
-- Rodar APÓS 03_migration_sla_notif_historico.sql
-- ══════════════════════════════════════════════════════════════════════════════

-- ════════════════════════════════════════════════════════════════════════════
-- 1. Campo duracao_ms no sync log (observabilidade de latência)
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE atak_sync_log
  ADD COLUMN IF NOT EXISTS duracao_ms INTEGER GENERATED ALWAYS AS (
    (EXTRACT(EPOCH FROM (fim - inicio)) * 1000)::INTEGER
  ) STORED;

-- Caso GENERATED ALWAYS não funcione na versão do PG, usar trigger:
-- (comentado — usar se a versão do Supabase não suportar generated columns)
-- CREATE OR REPLACE FUNCTION fn_calc_duracao_sync()
-- RETURNS TRIGGER AS $$
-- BEGIN
--   IF NEW.fim IS NOT NULL AND NEW.inicio IS NOT NULL THEN
--     NEW.duracao_ms := EXTRACT(EPOCH FROM (NEW.fim - NEW.inicio)) * 1000;
--   END IF;
--   RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER trg_calc_duracao_sync
--   BEFORE INSERT OR UPDATE ON atak_sync_log
--   FOR EACH ROW EXECUTE FUNCTION fn_calc_duracao_sync();


-- ════════════════════════════════════════════════════════════════════════════
-- 2. View de percentis de latência (p50, p95, p99) por endpoint
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_sync_latencia_percentis AS
SELECT
  endpoint_nome,
  COUNT(*) AS total_execucoes,

  -- Percentis de duração (ms)
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
    EXTRACT(EPOCH FROM (fim - inicio)) * 1000
  )) AS p50_ms,

  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
    EXTRACT(EPOCH FROM (fim - inicio)) * 1000
  )) AS p95_ms,

  ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY
    EXTRACT(EPOCH FROM (fim - inicio)) * 1000
  )) AS p99_ms,

  -- Média e máximo
  ROUND(AVG(EXTRACT(EPOCH FROM (fim - inicio)) * 1000)) AS media_ms,
  ROUND(MAX(EXTRACT(EPOCH FROM (fim - inicio)) * 1000)) AS max_ms,
  ROUND(MIN(EXTRACT(EPOCH FROM (fim - inicio)) * 1000)) AS min_ms,

  -- Throughput (registros/segundo médio)
  ROUND(AVG(
    CASE WHEN EXTRACT(EPOCH FROM (fim - inicio)) > 0
    THEN total_registros / EXTRACT(EPOCH FROM (fim - inicio))
    ELSE 0 END
  )::NUMERIC, 1) AS throughput_rps,

  -- Degradação: p95 > 2x p50?
  CASE
    WHEN PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
      EXTRACT(EPOCH FROM (fim - inicio)) * 1000
    ) > 2 * PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
      EXTRACT(EPOCH FROM (fim - inicio)) * 1000
    ) THEN true
    ELSE false
  END AS degradacao_detectada

FROM atak_sync_log
WHERE inicio >= NOW() - INTERVAL '7 days'
  AND fim IS NOT NULL
  AND status IN ('sucesso', 'parcial')
GROUP BY endpoint_nome
ORDER BY
  CASE WHEN PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
    EXTRACT(EPOCH FROM (fim - inicio)) * 1000
  ) > 2 * PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
    EXTRACT(EPOCH FROM (fim - inicio)) * 1000
  ) THEN 0 ELSE 1 END,
  endpoint_nome;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. Views Materializadas (refresh periódico via n8n)
-- ════════════════════════════════════════════════════════════════════════════

-- 3.1 Ranking de fornecedores (materializada — refresh a cada 6h junto com score)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fornecedor_ranking AS
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_forn_ranking_code
  ON mv_fornecedor_ranking(fornecedor_codigo);

-- 3.2 Métricas SLA 7d (materializada — refresh a cada 15min)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sync_sla_metricas_7d AS
SELECT
  sl.endpoint_nome,
  cfg.intervalo_min,
  COUNT(*) AS total_execucoes,
  COUNT(*) FILTER (WHERE sl.status = 'sucesso') AS execucoes_ok,
  COUNT(*) FILTER (WHERE sl.status = 'erro') AS execucoes_erro,
  COUNT(*) FILTER (WHERE sl.status = 'parcial') AS execucoes_parcial,
  ROUND(
    COUNT(*) FILTER (WHERE sl.status = 'sucesso')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 1
  ) AS taxa_sucesso_pct,
  ROUND(AVG(EXTRACT(EPOCH FROM (sl.fim - sl.inicio)))::NUMERIC, 1) AS duracao_media_seg,
  ROUND(MAX(EXTRACT(EPOCH FROM (sl.fim - sl.inicio)))::NUMERIC, 1) AS duracao_max_seg,
  SUM(sl.total_registros) AS total_registros_processados,
  SUM(sl.novos) AS total_novos,
  SUM(sl.erros) AS total_erros,
  ROUND(7 * 24 * 60.0 / cfg.intervalo_min) AS execucoes_esperadas_7d,
  ROUND(
    COUNT(*)::NUMERIC / NULLIF(ROUND(7 * 24 * 60.0 / cfg.intervalo_min), 0) * 100, 1
  ) AS cobertura_sla_pct,
  NOW() AS atualizado_em
FROM atak_sync_log sl
JOIN atak_sync_config cfg ON cfg.endpoint_nome = sl.endpoint_nome
WHERE sl.inicio >= NOW() - INTERVAL '7 days'
GROUP BY sl.endpoint_nome, cfg.intervalo_min;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sla_metricas_endpoint
  ON mv_sync_sla_metricas_7d(endpoint_nome);


-- ════════════════════════════════════════════════════════════════════════════
-- 4. Functions para refresh das materialized views
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_refresh_mv_ranking()
RETURNS VOID AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_fornecedor_ranking;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fn_refresh_mv_sla()
RETURNS VOID AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sync_sla_metricas_7d;
END;
$$ LANGUAGE plpgsql;

-- Refresh geral (ambas)
CREATE OR REPLACE FUNCTION fn_refresh_all_mv()
RETURNS VOID AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_fornecedor_ranking;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sync_sla_metricas_7d;
END;
$$ LANGUAGE plpgsql;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. Tabela de saúde do sistema (health check simplificado)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_system_health AS
SELECT
  -- SLA dos syncs
  (SELECT COUNT(*) FROM vw_sync_sla_status WHERE sla_status IN ('critico','erro','nunca_executado'))
    AS syncs_com_problema,
  (SELECT COUNT(*) FROM vw_sync_sla_status WHERE sla_status = 'ok')
    AS syncs_ok,

  -- Divergências
  (SELECT COUNT(*) FROM cq_validacao_divergencias WHERE status IN ('aberta','em_analise'))
    AS divergencias_abertas,
  (SELECT COUNT(*) FROM cq_validacao_divergencias WHERE gravidade = 'critica' AND status = 'aberta')
    AS divergencias_criticas,

  -- Fornecedores
  (SELECT COUNT(*) FROM cq_fornecedor_score WHERE status_fornecedor = 'bloqueado')
    AS fornecedores_bloqueados,
  (SELECT COUNT(*) FROM cq_fornecedor_score WHERE status_fornecedor = 'critico')
    AS fornecedores_criticos,

  -- Notificações
  (SELECT COUNT(*) FROM cq_fila_notificacao WHERE status = 'pendente')
    AS notificacoes_pendentes,
  (SELECT COUNT(*) FROM cq_fila_notificacao WHERE status = 'erro')
    AS notificacoes_com_erro,

  -- Último sync bem-sucedido
  (SELECT MAX(fim) FROM atak_sync_log WHERE status = 'sucesso')
    AS ultimo_sync_sucesso,

  NOW() AS verificado_em;


-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICAÇÃO
-- ════════════════════════════════════════════════════════════════════════════

SELECT * FROM vw_system_health;
