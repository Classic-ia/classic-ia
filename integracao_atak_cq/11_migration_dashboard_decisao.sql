-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 11: Views do Dashboard Decisional
--
-- Views otimizadas para tomada de decisão rápida.
-- Fontes: vw_atak_cq_consolidado (primária) + ranking views (fallback)
--
-- Rodar APÓS 10_migration_merge_estrutura.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. vw_dash_alertas — Alertas imediatos (topo do dashboard)
-- Retorna APENAS itens que exigem ação. Ordenados por gravidade.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_alertas AS

-- 1A: Fornecedores com queda de score > 5% nos últimos 30 dias
SELECT
  'fornecedor_piorando' AS tipo_alerta,
  'critico' AS gravidade,
  t.fornecedor_codigo AS codigo,
  t.fornecedor_nome AS nome,
  FORMAT('Score caiu %s pts em 30 dias (de %s para %s)',
    ABS(ROUND(t.variacao_score::NUMERIC, 1)),
    ROUND((t.score_atual + ABS(t.variacao_score))::NUMERIC, 1),
    ROUND(t.score_atual::NUMERIC, 1)
  ) AS descricao,
  t.score_atual AS valor,
  t.variacao_score AS variacao,
  t.tendencia,
  NOW() AS detectado_em
FROM vw_ranking_tendencia_30d t
WHERE t.variacao_score < -5

UNION ALL

-- 1B: Produtos com %C acima de 10%
SELECT
  'produto_critico' AS tipo_alerta,
  CASE WHEN sub.pct_c > 20 THEN 'critico' ELSE 'atencao' END AS gravidade,
  sub.id_atak_produto AS codigo,
  sub.produto AS nome,
  FORMAT('%s%% de Classe C (%s de %s peças)', ROUND(sub.pct_c, 1), sub.qtd_c, sub.total) AS descricao,
  sub.pct_c AS valor,
  NULL::NUMERIC AS variacao,
  NULL AS tendencia,
  NOW() AS detectado_em
FROM (
  SELECT
    c.id_atak_produto,
    COALESCE(c.produto, c.id_atak_produto) AS produto,
    COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') AS qtd_c,
    COUNT(*) AS total,
    ROUND(COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_c
  FROM vw_atak_cq_consolidado c
  WHERE c.data_movimentacao >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY c.id_atak_produto, c.produto
  HAVING COUNT(*) >= 3
) sub
WHERE sub.pct_c > 10

UNION ALL

-- 1C: Alertas do ranking (defeito>5%, fornecedor degradando, etc)
SELECT
  a.tipo_alerta,
  CASE
    WHEN a.tipo_alerta IN ('defeito_recorrente', 'degradacao_fornecedor') THEN 'critico'
    ELSE 'atencao'
  END AS gravidade,
  a.fornecedor_codigo AS codigo,
  a.fornecedor_nome AS nome,
  a.descricao,
  a.valor_referencia AS valor,
  NULL::NUMERIC AS variacao,
  NULL AS tendencia,
  a.criado_em AS detectado_em
FROM vw_ranking_alertas a
WHERE a.criado_em >= CURRENT_DATE - INTERVAL '7 days'

ORDER BY
  CASE gravidade WHEN 'critico' THEN 1 WHEN 'atencao' THEN 2 ELSE 3 END,
  valor DESC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. vw_dash_ranking_fornecedores — Ranking pior → melhor
-- Combina dados do consolidado + ranking existente + tendência
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_ranking_fornecedores AS
SELECT
  r.fornecedor_codigo,
  r.fornecedor_nome,
  r.total_lotes AS volume_total,
  r.score AS score_qualidade,
  r.pct_a,
  r.pct_b,
  r.pct_c,
  r.pct_defeitos,
  r.status_fornecedor,

  -- Tendência 30 dias
  COALESCE(t.variacao_score, 0) AS variacao_30d,
  CASE
    WHEN COALESCE(t.variacao_score, 0) > 3 THEN 'melhorando'
    WHEN COALESCE(t.variacao_score, 0) < -3 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,

  -- Score ponderado por volume (A*100 + B*60 + C*0) / total
  CASE
    WHEN r.total_pecas > 0 THEN
      ROUND((r.total_class_a * 100.0 + r.total_class_b * 60.0) / r.total_pecas, 2)
    ELSE r.score
  END AS score_ponderado

FROM vw_ranking_geral_fornecedor r
LEFT JOIN vw_ranking_tendencia_30d t
  ON t.fornecedor_codigo = r.fornecedor_codigo

ORDER BY score_qualidade ASC, volume_total DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. vw_dash_qualidade_produto — Qualidade por produto (pior → melhor)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_qualidade_produto AS
SELECT
  sub.produto,
  sub.id_atak_produto,
  sub.volume,
  sub.score,
  sub.pct_c,
  sub.pct_defeitos,
  sub.principal_defeito
FROM (
  -- Fonte 1: consolidado (movimentações + inspeções)
  SELECT
    COALESCE(c.produto, c.id_atak_produto) AS produto,
    c.id_atak_produto,
    SUM(c.quantidade_erp) AS volume,
    ROUND(AVG(c.score_qualidade), 2) AS score,
    ROUND(COUNT(*) FILTER (WHERE c.classificacao = 'C' OR c.classificacao_lote = 'C') * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_c,
    ROUND(AVG(c.percentual_defeitos), 2) AS pct_defeitos,
    NULL::TEXT AS principal_defeito
  FROM vw_atak_cq_consolidado c
  WHERE c.data_movimentacao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY c.produto, c.id_atak_produto
  HAVING COUNT(*) >= 2

  UNION ALL

  -- Fonte 2: ranking por produto (para produtos sem movimentação)
  SELECT
    rp.produto_nome AS produto,
    NULL AS id_atak_produto,
    rp.total_pecas AS volume,
    rp.score,
    rp.pct_c,
    rp.pct_defeitos,
    rp.principal_defeito
  FROM vw_ranking_fornecedor_produto rp
  WHERE NOT EXISTS (
    SELECT 1 FROM vw_atak_cq_consolidado c
    WHERE COALESCE(c.produto, c.id_atak_produto) = rp.produto_nome
      AND c.data_movimentacao >= CURRENT_DATE - INTERVAL '90 days'
  )
) sub
WHERE sub.volume > 0
ORDER BY sub.pct_c DESC NULLS LAST, sub.score ASC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. vw_dash_defeitos — Análise de defeitos (ranking + por fornecedor/produto)
-- ════════════════════════════════════════════════════════════════════════════

-- 4A: Ranking geral de defeitos
CREATE OR REPLACE VIEW vw_dash_defeitos_ranking AS
SELECT
  d.defeito_codigo,
  d.defeito_nome,
  SUM(d.quantidade_defeito) AS total_ocorrencias,
  SUM(d.total_analisado) AS total_analisado,
  ROUND(SUM(d.quantidade_defeito) * 100.0 / NULLIF(SUM(d.total_analisado), 0), 2) AS pct_sobre_total,
  COUNT(DISTINCT d.fornecedor_codigo) AS qtd_fornecedores_afetados
FROM vw_ranking_defeitos_fornecedor d
GROUP BY d.defeito_codigo, d.defeito_nome
ORDER BY total_ocorrencias DESC;

-- 4B: Defeitos por fornecedor (top ofensores)
CREATE OR REPLACE VIEW vw_dash_defeitos_fornecedor AS
SELECT
  d.fornecedor_codigo,
  d.fornecedor_nome,
  d.defeito_codigo,
  d.defeito_nome,
  d.quantidade_defeito,
  d.total_analisado,
  d.pct_defeito,
  d.alerta_defeito
FROM vw_ranking_defeitos_fornecedor d
WHERE d.pct_defeito > 2
ORDER BY d.pct_defeito DESC;

-- 4C: Defeitos por produto
CREATE OR REPLACE VIEW vw_dash_defeitos_produto AS
SELECT
  sub.produto,
  sub.defeito,
  sub.total_defeito,
  sub.total_analisado,
  sub.pct_defeito
FROM (
  SELECT
    i.produto,
    'fermentacao' AS defeito, SUM(i.fermentacao) AS total_defeito,
    SUM(i.quantidade_analisada) AS total_analisado,
    ROUND(SUM(i.fermentacao) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2) AS pct_defeito
  FROM registros_cq_inspecao i
  WHERE i.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'sem_folhas', SUM(i.sem_folhas), SUM(i.quantidade_analisada),
    ROUND(SUM(i.sem_folhas) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'folhas_arrebentadas', SUM(i.folhas_arrebentadas), SUM(i.quantidade_analisada),
    ROUND(SUM(i.folhas_arrebentadas) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'pedaco', SUM(i.pedaco), SUM(i.quantidade_analisada),
    ROUND(SUM(i.pedaco) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'sujo', SUM(i.sujo), SUM(i.quantidade_analisada),
    ROUND(SUM(i.sujo) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'rasgado', SUM(i.rasgado), SUM(i.quantidade_analisada),
    ROUND(SUM(i.rasgado) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'corte_irregular', SUM(i.corte_irregular), SUM(i.quantidade_analisada),
    ROUND(SUM(i.corte_irregular) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'furado', SUM(i.furado), SUM(i.quantidade_analisada),
    ROUND(SUM(i.furado) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'mau_cheiro', SUM(i.mau_cheiro), SUM(i.quantidade_analisada),
    ROUND(SUM(i.mau_cheiro) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
  UNION ALL
  SELECT i.produto, 'contaminacao', SUM(i.contaminacao), SUM(i.quantidade_analisada),
    ROUND(SUM(i.contaminacao) * 100.0 / NULLIF(SUM(i.quantidade_analisada), 0), 2)
  FROM registros_cq_inspecao i WHERE i.data_inspecao >= CURRENT_DATE - 90 GROUP BY i.produto
) sub
WHERE sub.total_defeito > 0
ORDER BY sub.pct_defeito DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. vw_dash_pipeline — Status operacional em tempo real
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_dash_pipeline AS
SELECT
  'cargas_pendentes' AS etapa,
  1 AS ordem,
  COUNT(*) AS quantidade,
  NULL::TEXT AS detalhe
FROM atak_cargas_raw
WHERE processado = false

UNION ALL
SELECT
  'inspecoes_andamento' AS etapa,
  2 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NULL
  AND criado_em >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL
SELECT
  'inspecoes_concluidas_hoje' AS etapa,
  3 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NOT NULL
  AND data_inspecao = CURRENT_DATE

UNION ALL
SELECT
  'inspecoes_concluidas_semana' AS etapa,
  4 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM registros_cq_inspecao
WHERE status_final IS NOT NULL
  AND data_inspecao >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL
SELECT
  'ncs_abertas' AS etapa,
  5 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM cq_nao_conformidades
WHERE status IN ('aberta', 'em_tratamento')

UNION ALL
SELECT
  'divergencias_abertas' AS etapa,
  6 AS ordem,
  COUNT(*) AS quantidade,
  NULL AS detalhe
FROM atak_divergencias
WHERE resolvido = false

ORDER BY ordem;


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 11 — Views Dashboard Decisional';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS CRIADAS:';
  RAISE NOTICE '    ✓ vw_dash_alertas (alertas imediatos, ordenados por gravidade)';
  RAISE NOTICE '    ✓ vw_dash_ranking_fornecedores (pior → melhor, com tendência)';
  RAISE NOTICE '    ✓ vw_dash_qualidade_produto (pior → melhor, com defeito)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_ranking (ranking geral de defeitos)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_fornecedor (defeitos por fornecedor)';
  RAISE NOTICE '    ✓ vw_dash_defeitos_produto (defeitos por produto)';
  RAISE NOTICE '    ✓ vw_dash_pipeline (status operacional real-time)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
