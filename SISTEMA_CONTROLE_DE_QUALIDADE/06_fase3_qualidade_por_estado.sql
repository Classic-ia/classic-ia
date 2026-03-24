-- ══════════════════════════════════════════════════════════════
-- Views Analíticas: Qualidade por Estado (UF)
-- Sistema de Drill-Down: Estado → Fornecedor → Produto
-- Rodar no SQL Editor do Supabase
-- ══════════════════════════════════════════════════════════════

-- ── VIEW 1: Agregação por Estado ─────────────────────────────
CREATE OR REPLACE VIEW vw_qualidade_por_estado AS
SELECT
  COALESCE(ca.uf, 'N/D')                                   AS estado,
  COUNT(*)                                                  AS total_inspecoes,
  COUNT(*) FILTER (WHERE r.status_final = 'aprovado')       AS aprovadas,
  COUNT(*) FILTER (WHERE r.status_final = 'ressalva')       AS ressalvas,
  COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')      AS bloqueios,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'aprovado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_aprovacao,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_bloqueio,
  ROUND(AVG(r.percentual_defeitos), 2)                      AS media_defeitos,
  COUNT(*) FILTER (WHERE r.classificacao = 'A')             AS lotes_a,
  COUNT(*) FILTER (WHERE r.classificacao = 'B')             AS lotes_b,
  COUNT(*) FILTER (WHERE r.classificacao = 'C')             AS lotes_c,
  ROUND(
    GREATEST(0, LEAST(100,
      100 - (
        COUNT(*) FILTER (WHERE r.classificacao = 'C')::NUMERIC / NULLIF(COUNT(*),0) * 50 +
        COUNT(*) FILTER (WHERE r.classificacao = 'B')::NUMERIC / NULLIF(COUNT(*),0) * 20 -
        COUNT(*) FILTER (WHERE r.classificacao = 'A')::NUMERIC / NULLIF(COUNT(*),0) * 20
      )
    )), 0
  )                                                         AS score_cq
FROM registros_cq_inspecao r
LEFT JOIN cadastros_atak ca
  ON r.fornecedor = ca.nome
  AND ca.tipo = 'fornecedor'
GROUP BY COALESCE(ca.uf, 'N/D')
ORDER BY score_cq DESC;


-- ── VIEW 2: Agregação por Estado × Fornecedor ───────────────
CREATE OR REPLACE VIEW vw_qualidade_fornecedor_estado AS
SELECT
  COALESCE(ca.uf, 'N/D')                                   AS estado,
  r.fornecedor,
  ca.codigo AS fornecedor_codigo,
  COUNT(*)                                                  AS total_inspecoes,
  COUNT(*) FILTER (WHERE r.status_final = 'aprovado')       AS aprovadas,
  COUNT(*) FILTER (WHERE r.status_final = 'ressalva')       AS ressalvas,
  COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')      AS bloqueios,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'aprovado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_aprovacao,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_bloqueio,
  ROUND(AVG(r.percentual_defeitos), 2)                      AS media_defeitos,
  COUNT(*) FILTER (WHERE r.classificacao = 'A')             AS lotes_a,
  COUNT(*) FILTER (WHERE r.classificacao = 'B')             AS lotes_b,
  COUNT(*) FILTER (WHERE r.classificacao = 'C')             AS lotes_c,
  ROUND(
    GREATEST(0, LEAST(100,
      100 - (
        COUNT(*) FILTER (WHERE r.classificacao = 'C')::NUMERIC / NULLIF(COUNT(*),0) * 50 +
        COUNT(*) FILTER (WHERE r.classificacao = 'B')::NUMERIC / NULLIF(COUNT(*),0) * 20 -
        COUNT(*) FILTER (WHERE r.classificacao = 'A')::NUMERIC / NULLIF(COUNT(*),0) * 20
      )
    )), 0
  )                                                         AS score_cq
FROM registros_cq_inspecao r
LEFT JOIN cadastros_atak ca
  ON r.fornecedor = ca.nome
  AND ca.tipo = 'fornecedor'
GROUP BY COALESCE(ca.uf, 'N/D'), r.fornecedor, ca.codigo
ORDER BY estado, score_cq DESC;


-- ── VIEW 3: Agregação por Estado × Produto ───────────────────
CREATE OR REPLACE VIEW vw_qualidade_produto_estado AS
SELECT
  COALESCE(ca.uf, 'N/D')                                   AS estado,
  COALESCE(r.produto, 'N/D')                               AS produto,
  COUNT(*)                                                  AS total_inspecoes,
  COUNT(*) FILTER (WHERE r.status_final = 'aprovado')       AS aprovadas,
  COUNT(*) FILTER (WHERE r.status_final = 'ressalva')       AS ressalvas,
  COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')      AS bloqueios,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'aprovado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_aprovacao,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'bloqueado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  )                                                         AS taxa_bloqueio,
  ROUND(AVG(r.percentual_defeitos), 2)                      AS media_defeitos
FROM registros_cq_inspecao r
LEFT JOIN cadastros_atak ca
  ON r.fornecedor = ca.nome
  AND ca.tipo = 'fornecedor'
GROUP BY COALESCE(ca.uf, 'N/D'), COALESCE(r.produto, 'N/D')
ORDER BY estado, total_inspecoes DESC;


-- ── VERIFICAÇÃO ──────────────────────────────────────────────
SELECT 'vw_qualidade_por_estado' AS view, COUNT(*) AS rows FROM vw_qualidade_por_estado
UNION ALL
SELECT 'vw_qualidade_fornecedor_estado', COUNT(*) FROM vw_qualidade_fornecedor_estado
UNION ALL
SELECT 'vw_qualidade_produto_estado', COUNT(*) FROM vw_qualidade_produto_estado;
