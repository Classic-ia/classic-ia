-- ══════════════════════════════════════════════════════════════
-- Fase 4: Views Analíticas Avançadas
-- 5 funcionalidades: Supplier Score, Risk Base, Defect Origin,
--                    Quality Timeline, CQ Efficiency
-- JOIN corrigido: r.fornecedor = ca.nome (registros_cq_inspecao
--   NÃO tem coluna fornecedor_codigo)
-- Rodar no SQL Editor do Supabase
-- ══════════════════════════════════════════════════════════════


-- ── VIEW 1: Supplier Reliability Index ───────────────────────
-- Score de confiabilidade com penalidades por bloqueio, ressalva e NCs
-- Janela: últimos 90 dias

CREATE OR REPLACE VIEW vw_supplier_score AS
WITH base AS (
  SELECT
    r.fornecedor,
    ca.codigo AS fornecedor_codigo,
    COALESCE(ca.uf, 'N/D') AS estado,
    COUNT(*) AS total_inspecoes,
    COUNT(*) FILTER (WHERE r.status_final = 'bloqueado') AS bloqueios,
    COUNT(*) FILTER (WHERE r.status_final = 'ressalva') AS ressalvas,
    COUNT(*) FILTER (WHERE r.status_final = 'aprovado') AS aprovadas,
    ROUND(AVG(r.percentual_defeitos), 2) AS media_defeitos,
    COUNT(*) FILTER (WHERE r.classificacao = 'A') AS lotes_a,
    COUNT(*) FILTER (WHERE r.classificacao = 'B') AS lotes_b,
    COUNT(*) FILTER (WHERE r.classificacao = 'C') AS lotes_c
  FROM registros_cq_inspecao r
  LEFT JOIN cadastros_atak ca ON r.fornecedor = ca.nome AND ca.tipo = 'fornecedor'
  WHERE r.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY r.fornecedor, ca.codigo, ca.uf
),
ncs AS (
  SELECT
    r.fornecedor,
    COUNT(*) FILTER (WHERE nc.gravidade = 'critica') AS nc_criticas,
    COUNT(*) FILTER (WHERE nc.gravidade = 'maior')   AS nc_maiores,
    COUNT(*) FILTER (WHERE nc.gravidade = 'menor')   AS nc_menores
  FROM cq_nao_conformidades nc
  JOIN registros_cq_inspecao r ON nc.inspecao_id = r.id
  WHERE nc.criado_em >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY r.fornecedor
),
calc AS (
  SELECT
    b.*,
    COALESCE(n.nc_criticas, 0) AS nc_criticas,
    COALESCE(n.nc_maiores, 0)  AS nc_maiores,
    COALESCE(n.nc_menores, 0)  AS nc_menores,
    GREATEST(0, LEAST(100,
      100
      - b.bloqueios * 15
      - b.ressalvas * 5
      - COALESCE(n.nc_criticas, 0) * 20
      - COALESCE(n.nc_maiores, 0) * 10
      - COALESCE(n.nc_menores, 0) * 3
    )) AS score_confiabilidade
  FROM base b
  LEFT JOIN ncs n ON b.fornecedor = n.fornecedor
)
SELECT
  c.*,
  CASE
    WHEN c.score_confiabilidade >= 85 THEN 'confiavel'
    WHEN c.score_confiabilidade >= 70 THEN 'regular'
    WHEN c.score_confiabilidade >= 50 THEN 'atencao'
    ELSE 'critico'
  END AS classificacao_confiabilidade
FROM calc c
ORDER BY c.score_confiabilidade DESC;


-- ── VIEW 2: Risk Base (dados para detecção de tendência) ─────
-- Dados brutos dos últimos 90 dias com row_number para análise sequencial

CREATE OR REPLACE VIEW vw_risk_base AS
SELECT
  r.fornecedor,
  ca.codigo AS fornecedor_codigo,
  r.produto,
  r.data_inspecao,
  r.percentual_defeitos,
  r.status_final,
  r.classificacao,
  COALESCE(ca.uf, 'N/D') AS estado,
  ROW_NUMBER() OVER (PARTITION BY r.fornecedor ORDER BY r.data_inspecao DESC) AS rn
FROM registros_cq_inspecao r
LEFT JOIN cadastros_atak ca ON r.fornecedor = ca.nome AND ca.tipo = 'fornecedor'
WHERE r.data_inspecao >= CURRENT_DATE - INTERVAL '90 days';


-- ── VIEW 3: Defect Origin Map ────────────────────────────────
-- Agregação Estado × Fornecedor × Produto com contagem de cada tipo de defeito

CREATE OR REPLACE VIEW vw_defect_origin AS
SELECT
  COALESCE(ca.uf, 'N/D') AS estado,
  r.fornecedor,
  ca.codigo AS fornecedor_codigo,
  r.produto,
  SUM(COALESCE(r.fermentacao, 0))          AS fermentacao,
  SUM(COALESCE(r.sem_folhas, 0))           AS sem_folhas,
  SUM(COALESCE(r.folhas_arrebentadas, 0))  AS folhas_arrebentadas,
  SUM(COALESCE(r.pedaco, 0))               AS pedaco,
  SUM(COALESCE(r.sujo, 0))                 AS sujo,
  SUM(COALESCE(r.rasgado, 0))              AS rasgado,
  SUM(COALESCE(r.corte_irregular, 0))      AS corte_irregular,
  SUM(COALESCE(r.furado, 0))               AS furado,
  SUM(COALESCE(r.mau_cheiro, 0))           AS mau_cheiro,
  SUM(COALESCE(r.contaminacao, 0))         AS contaminacao,
  SUM(COALESCE(r.outro_defeito, 0))        AS outro_defeito,
  SUM(COALESCE(r.total_defeitos, 0))       AS total_defeitos,
  COUNT(*)                                  AS total_inspecoes,
  ROUND(AVG(r.percentual_defeitos), 2)     AS media_defeitos
FROM registros_cq_inspecao r
LEFT JOIN cadastros_atak ca ON r.fornecedor = ca.nome AND ca.tipo = 'fornecedor'
GROUP BY ca.uf, r.fornecedor, ca.codigo, r.produto
ORDER BY total_defeitos DESC;


-- ── VIEW 4: Quality Timeline ─────────────────────────────────
-- JOIN completo: Recebimento → Inspeção → Revisão → NC → Plano
-- ATENÇÃO: usar sempre com filtro de período ou lote (pesada)

CREATE OR REPLACE VIEW vw_quality_timeline AS
SELECT
  r.id AS inspecao_id,
  r.fornecedor,
  r.lote_atak,
  r.etiqueta,
  r.produto,
  r.data_inspecao,
  r.classificacao,
  r.status_final,
  r.percentual_defeitos,
  r.status_workflow,
  r.criado_em AS inspecao_criado_em,
  -- Recebimento
  rec.id AS recebimento_id,
  rec.criado_em AS recebimento_criado_em,
  -- Revisão
  rev.criado_em AS revisao_criado_em,
  rev.decisao AS revisao_decisao,
  rev.revisor_nome,
  -- NC
  nc.id AS nc_id,
  nc.codigo AS nc_codigo,
  nc.gravidade AS nc_gravidade,
  nc.status AS nc_status,
  nc.criado_em AS nc_criado_em,
  -- Plano de Ação
  pa.id AS plano_id,
  pa.status AS plano_status,
  pa.data_limite AS plano_prazo,
  pa.data_conclusao AS plano_conclusao,
  pa.validacao AS plano_validacao,
  pa.criado_em AS plano_criado_em,
  -- Tempos calculados (em horas)
  ROUND((EXTRACT(EPOCH FROM (r.criado_em - rec.criado_em)) / 3600)::NUMERIC, 1) AS horas_recebimento_inspecao,
  ROUND((EXTRACT(EPOCH FROM (rev.criado_em - r.criado_em)) / 3600)::NUMERIC, 1) AS horas_inspecao_revisao
FROM registros_cq_inspecao r
LEFT JOIN cq_recebimentos rec ON r.lote_atak = rec.lote_atak
LEFT JOIN cq_revisoes rev ON rev.inspecao_id = r.id
LEFT JOIN cq_nao_conformidades nc ON nc.inspecao_id = r.id
LEFT JOIN cq_planos_acao pa ON pa.nao_conformidade_id = nc.id
ORDER BY r.data_inspecao DESC;


-- ── VIEW 5: CQ Efficiency Index ──────────────────────────────
-- Métricas de eficiência por analista (últimos 30 dias)

CREATE OR REPLACE VIEW vw_cq_efficiency AS
SELECT
  r.criador_id,
  COALESCE(u.nome, u.email, 'Desconhecido') AS analista,
  COUNT(*) AS total_inspecoes,
  ROUND(AVG(
    CASE WHEN rec.criado_em IS NOT NULL
         THEN EXTRACT(EPOCH FROM (r.criado_em - rec.criado_em)) / 3600
         ELSE NULL END
  )::NUMERIC, 1) AS avg_horas_receb_insp,
  ROUND(AVG(
    CASE WHEN rev.criado_em IS NOT NULL
         THEN EXTRACT(EPOCH FROM (rev.criado_em - r.criado_em)) / 3600
         ELSE NULL END
  )::NUMERIC, 1) AS avg_horas_insp_rev,
  COUNT(*) FILTER (WHERE r.status_final = 'aprovado')  AS aprovadas,
  COUNT(*) FILTER (WHERE r.status_final = 'bloqueado') AS bloqueadas,
  ROUND(
    COUNT(*) FILTER (WHERE r.status_final = 'aprovado')::NUMERIC
    / NULLIF(COUNT(*), 0) * 100, 1
  ) AS taxa_aprovacao
FROM registros_cq_inspecao r
LEFT JOIN cq_recebimentos rec ON r.lote_atak = rec.lote_atak
LEFT JOIN cq_revisoes rev ON rev.inspecao_id = r.id
LEFT JOIN cq_usuarios u ON r.criador_id = u.id
WHERE r.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY r.criador_id, u.nome, u.email
ORDER BY total_inspecoes DESC;


-- ── VERIFICAÇÃO ──────────────────────────────────────────────
SELECT 'vw_supplier_score'    AS view_name, COUNT(*) AS rows FROM vw_supplier_score
UNION ALL SELECT 'vw_risk_base',        COUNT(*) FROM vw_risk_base
UNION ALL SELECT 'vw_defect_origin',    COUNT(*) FROM vw_defect_origin
UNION ALL SELECT 'vw_quality_timeline', COUNT(*) FROM vw_quality_timeline
UNION ALL SELECT 'vw_cq_efficiency',    COUNT(*) FROM vw_cq_efficiency;
