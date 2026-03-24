-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 12: Views de Qualidade por 4 Dimensões
--
-- Dimensões:
--   1. Qualidade por Fornecedor (detalhada, com evolução temporal)
--   2. Qualidade por Produto (com defeito principal e tendência)
--   3. Qualidade por Lote (individual, com detalhe por produto)
--   4. Qualidade por Setor (produção + funcionários + turno)
--
-- Rodar APÓS 11_migration_dashboard_decisao.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. QUALIDADE POR FORNECEDOR (detalhada)
-- Diferente do ranking (que é resumo): aqui mostra evolução lote a lote
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_fornecedor AS
SELECT
  f.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome, f.fornecedor_nome) AS fornecedor_nome,
  ca.uf AS fornecedor_uf,
  ca.cnpj AS fornecedor_cnpj,

  -- Volume
  COUNT(DISTINCT f.id_lote) AS total_lotes,
  SUM(f.total_pecas) AS total_pecas,

  -- ABC
  SUM(f.total_class_a) AS total_a,
  SUM(f.total_class_b) AS total_b,
  SUM(f.total_class_c) AS total_c,
  ROUND(SUM(f.total_class_a) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_a,
  ROUND(SUM(f.total_class_b) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_b,
  ROUND(SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_c,

  -- Defeitos
  SUM(f.total_defeitos) AS total_defeitos,
  ROUND(SUM(f.total_defeitos) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_defeitos,

  -- Score ponderado por volume
  ROUND(
    (SUM(f.total_class_a) * 100.0 + SUM(f.total_class_b) * 60.0)
    / NULLIF(SUM(f.total_pecas), 0), 2
  ) AS score,

  -- Classificação dominante
  CASE
    WHEN SUM(f.total_class_a) * 100.0 / NULLIF(SUM(f.total_pecas), 0) >= 80 THEN 'A'
    WHEN SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0) > 15 THEN 'C'
    ELSE 'B'
  END AS classificacao_geral,

  -- Tendência: comparar últimos 15 dias vs 15 dias anteriores
  ROUND(
    (SUM(CASE WHEN f.data_inspecao >= CURRENT_DATE - 15 THEN f.total_class_a * 100.0 + f.total_class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN f.data_inspecao >= CURRENT_DATE - 15 THEN f.total_pecas ELSE 0 END), 0))
    -
    (SUM(CASE WHEN f.data_inspecao < CURRENT_DATE - 15 AND f.data_inspecao >= CURRENT_DATE - 30 THEN f.total_class_a * 100.0 + f.total_class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN f.data_inspecao < CURRENT_DATE - 15 AND f.data_inspecao >= CURRENT_DATE - 30 THEN f.total_pecas ELSE 0 END), 0))
  , 2) AS variacao_30d,

  -- Status do fornecedor
  COALESCE(fs.status_fornecedor, 'normal') AS status_fornecedor,
  fs.score AS score_sistema,

  -- Datas
  MIN(f.data_inspecao) AS primeira_inspecao,
  MAX(f.data_inspecao) AS ultima_inspecao,
  COUNT(DISTINCT f.data_inspecao) AS dias_com_inspecao

FROM cq_lote_inspecao f
LEFT JOIN cadastros_atak ca ON ca.codigo = f.fornecedor_codigo AND ca.tipo = 'fornecedor'
LEFT JOIN cq_fornecedor_score fs ON fs.fornecedor_codigo = f.fornecedor_codigo
WHERE f.status = 'finalizado'
  AND f.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY f.fornecedor_codigo, ca.apelido, ca.nome, f.fornecedor_nome,
         ca.uf, ca.cnpj, fs.status_fornecedor, fs.score
ORDER BY score ASC NULLS LAST;


-- Evolução temporal do fornecedor (para gráfico sparkline)
CREATE OR REPLACE VIEW vw_qualidade_fornecedor_evolucao AS
SELECT
  f.fornecedor_codigo,
  f.data_inspecao,
  COUNT(*) AS lotes_dia,
  SUM(f.total_pecas) AS pecas_dia,
  ROUND(
    (SUM(f.total_class_a) * 100.0 + SUM(f.total_class_b) * 60.0)
    / NULLIF(SUM(f.total_pecas), 0), 2
  ) AS score_dia,
  ROUND(SUM(f.total_class_c) * 100.0 / NULLIF(SUM(f.total_pecas), 0), 2) AS pct_c_dia
FROM cq_lote_inspecao f
WHERE f.status = 'finalizado'
  AND f.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY f.fornecedor_codigo, f.data_inspecao
ORDER BY f.fornecedor_codigo, f.data_inspecao;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. QUALIDADE POR PRODUTO (com defeito principal e tendência)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_produto AS
SELECT
  lp.produto_codigo,
  COALESCE(p.nome, rp.nome, lp.produto_codigo) AS produto_nome,
  COALESCE(p.grupo, rp.grupo) AS produto_grupo,

  -- Volume
  COUNT(DISTINCT lp.lote_id) AS total_lotes,
  SUM(lp.total_pecas) AS total_pecas,

  -- ABC
  SUM(lp.class_a) AS total_a,
  SUM(lp.class_b) AS total_b,
  SUM(lp.class_c) AS total_c,
  ROUND(SUM(lp.class_a) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_a,
  ROUND(SUM(lp.class_b) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_b,
  ROUND(SUM(lp.class_c) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_c,

  -- Defeitos
  SUM(lp.total_defeitos) AS total_defeitos,
  ROUND(SUM(lp.total_defeitos) * 100.0 / NULLIF(SUM(lp.total_pecas), 0), 2) AS pct_defeitos,

  -- Score
  ROUND(
    (SUM(lp.class_a) * 100.0 + SUM(lp.class_b) * 60.0)
    / NULLIF(SUM(lp.total_pecas), 0), 2
  ) AS score,

  -- Defeitos desagregados (totais)
  SUM(lp.rasgado) AS def_rasgado,
  SUM(lp.furado) AS def_furado,
  SUM(lp.pelando) AS def_pelando,
  SUM(lp.folha_estourada) AS def_folha_estourada,
  SUM(lp.despojo_sujo) AS def_despojo_sujo,
  SUM(lp.sebo_corte_incorreto) AS def_sebo_corte,
  SUM(lp.carne_sem_folha) AS def_carne_sem_folha,
  SUM(lp.agua) AS def_agua,
  SUM(lp.orelha_misturada) AS def_orelha_misturada,

  -- Principal defeito (o que mais ocorre)
  CASE GREATEST(
    SUM(lp.rasgado), SUM(lp.furado), SUM(lp.pelando),
    SUM(lp.folha_estourada), SUM(lp.despojo_sujo),
    SUM(lp.sebo_corte_incorreto), SUM(lp.carne_sem_folha),
    SUM(lp.agua), SUM(lp.orelha_misturada)
  )
    WHEN SUM(lp.rasgado) THEN 'Rasgado'
    WHEN SUM(lp.furado) THEN 'Furado'
    WHEN SUM(lp.pelando) THEN 'Pelando'
    WHEN SUM(lp.folha_estourada) THEN 'Folha Estourada'
    WHEN SUM(lp.despojo_sujo) THEN 'Despojo/Sujo'
    WHEN SUM(lp.sebo_corte_incorreto) THEN 'Sebo/Corte Incorreto'
    WHEN SUM(lp.carne_sem_folha) THEN 'Carne sem Folha'
    WHEN SUM(lp.agua) THEN 'Agua'
    WHEN SUM(lp.orelha_misturada) THEN 'Orelha Misturada'
    ELSE NULL
  END AS principal_defeito,

  -- Tendência 30d
  ROUND(
    (SUM(CASE WHEN li.data_inspecao >= CURRENT_DATE - 15 THEN lp.class_a * 100.0 + lp.class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN li.data_inspecao >= CURRENT_DATE - 15 THEN lp.total_pecas ELSE 0 END), 0))
    -
    (SUM(CASE WHEN li.data_inspecao < CURRENT_DATE - 15 AND li.data_inspecao >= CURRENT_DATE - 30 THEN lp.class_a * 100.0 + lp.class_b * 60.0 ELSE 0 END)
     / NULLIF(SUM(CASE WHEN li.data_inspecao < CURRENT_DATE - 15 AND li.data_inspecao >= CURRENT_DATE - 30 THEN lp.total_pecas ELSE 0 END), 0))
  , 2) AS variacao_30d,

  -- Fornecedores que entregam este produto
  COUNT(DISTINCT li.fornecedor_codigo) AS qtd_fornecedores

FROM cq_lote_produto lp
JOIN cq_lote_inspecao li ON li.id = lp.lote_id AND li.status = 'finalizado'
LEFT JOIN cq_produtos p ON p.codigo = lp.produto_codigo
LEFT JOIN cq_ranking_produtos rp ON rp.codigo = lp.produto_codigo
WHERE li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY lp.produto_codigo, p.nome, rp.nome, p.grupo, rp.grupo
ORDER BY score ASC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. QUALIDADE POR LOTE (individual — detalhe completo)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_qualidade_lote AS
SELECT
  li.id,
  li.id_lote,
  li.data_inspecao,
  li.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome, li.fornecedor_nome) AS fornecedor_nome,
  li.inspetor_nome,

  -- Totais
  li.total_pecas,
  li.total_class_a,
  li.total_class_b,
  li.total_class_c,
  li.pct_a,
  li.pct_b,
  li.pct_c,
  li.total_defeitos,
  li.pct_defeitos,
  li.classificacao_lote,

  -- Score
  CASE WHEN li.total_pecas > 0 THEN
    ROUND((li.total_class_a * 100.0 + li.total_class_b * 60.0) / li.total_pecas, 2)
  ELSE 0 END AS score,

  -- Status e observações
  li.status,
  li.observacoes,

  -- Produtos no lote (JSON array)
  (
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'produto', COALESCE(p2.nome, rp2.nome, lp2.produto_codigo),
      'class_a', lp2.class_a,
      'class_b', lp2.class_b,
      'class_c', lp2.class_c,
      'total', lp2.total_pecas,
      'pct_c', lp2.pct_c,
      'principal_defeito', lp2.principal_defeito,
      'total_defeitos', lp2.total_defeitos
    ) ORDER BY lp2.total_pecas DESC), '[]'::jsonb)
    FROM cq_lote_produto lp2
    LEFT JOIN cq_produtos p2 ON p2.codigo = lp2.produto_codigo
    LEFT JOIN cq_ranking_produtos rp2 ON rp2.codigo = lp2.produto_codigo
    WHERE lp2.lote_id = li.id
  ) AS produtos,

  -- Quantidade de produtos no lote
  (SELECT COUNT(*) FROM cq_lote_produto lp3 WHERE lp3.lote_id = li.id) AS qtd_produtos,

  -- Geolocalização
  li.geo_dentro_planta,
  li.criado_em

FROM cq_lote_inspecao li
LEFT JOIN cadastros_atak ca ON ca.codigo = li.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE li.data_inspecao >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY li.data_inspecao DESC, li.criado_em DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. QUALIDADE POR SETOR
-- Cruza: atak_producao_raw (setor + turno) com inspeções + cadastros
-- ════════════════════════════════════════════════════════════════════════════

-- 4A: Produção vs Qualidade por setor
CREATE OR REPLACE VIEW vw_qualidade_setor AS
SELECT
  sub.setor,
  sub.total_produzido,
  sub.total_aprovado,
  sub.total_rejeitado,
  sub.pct_aprovacao,
  sub.pct_rejeicao,

  -- Score do setor (aprovado/total * 100)
  ROUND(sub.total_aprovado * 100.0 / NULLIF(sub.total_produzido, 0), 2) AS score,

  -- Tendência
  sub.variacao_30d,
  CASE
    WHEN sub.variacao_30d > 3 THEN 'melhorando'
    WHEN sub.variacao_30d < -3 THEN 'piorando'
    ELSE 'estavel'
  END AS tendencia,

  -- Turnos
  sub.turnos,
  sub.responsaveis,
  sub.dias_producao

FROM (
  SELECT
    pr.setor,
    SUM(pr.qtd_produzida) AS total_produzido,
    SUM(pr.qtd_aprovada) AS total_aprovado,
    SUM(pr.qtd_rejeitada) AS total_rejeitado,
    ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_aprovacao,
    ROUND(SUM(pr.qtd_rejeitada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_rejeicao,

    -- Tendência: últimos 15d vs 15d anteriores
    ROUND(
      (SUM(CASE WHEN pr.data_producao >= CURRENT_DATE - 15 THEN pr.qtd_aprovada ELSE 0 END) * 100.0
       / NULLIF(SUM(CASE WHEN pr.data_producao >= CURRENT_DATE - 15 THEN pr.qtd_produzida ELSE 0 END), 0))
      -
      (SUM(CASE WHEN pr.data_producao < CURRENT_DATE - 15 AND pr.data_producao >= CURRENT_DATE - 30 THEN pr.qtd_aprovada ELSE 0 END) * 100.0
       / NULLIF(SUM(CASE WHEN pr.data_producao < CURRENT_DATE - 15 AND pr.data_producao >= CURRENT_DATE - 30 THEN pr.qtd_produzida ELSE 0 END), 0))
    , 2) AS variacao_30d,

    -- Turnos ativos
    ARRAY_AGG(DISTINCT pr.turno) FILTER (WHERE pr.turno IS NOT NULL) AS turnos,
    ARRAY_AGG(DISTINCT pr.responsavel) FILTER (WHERE pr.responsavel IS NOT NULL) AS responsaveis,
    COUNT(DISTINCT pr.data_producao) AS dias_producao

  FROM atak_producao_raw pr
  WHERE pr.data_producao >= CURRENT_DATE - INTERVAL '90 days'
    AND pr.setor IS NOT NULL
  GROUP BY pr.setor
) sub
WHERE sub.total_produzido > 0
ORDER BY score ASC NULLS LAST;


-- 4B: Qualidade por setor + turno (granular)
CREATE OR REPLACE VIEW vw_qualidade_setor_turno AS
SELECT
  pr.setor,
  pr.turno,
  SUM(pr.qtd_produzida) AS total_produzido,
  SUM(pr.qtd_aprovada) AS total_aprovado,
  SUM(pr.qtd_rejeitada) AS total_rejeitado,
  ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_aprovacao,
  ROUND(SUM(pr.qtd_rejeitada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS pct_rejeicao,
  ROUND(SUM(pr.qtd_aprovada) * 100.0 / NULLIF(SUM(pr.qtd_produzida), 0), 2) AS score,
  COUNT(DISTINCT pr.data_producao) AS dias_producao,
  ARRAY_AGG(DISTINCT pr.responsavel) FILTER (WHERE pr.responsavel IS NOT NULL) AS responsaveis
FROM atak_producao_raw pr
WHERE pr.data_producao >= CURRENT_DATE - INTERVAL '90 days'
  AND pr.setor IS NOT NULL
GROUP BY pr.setor, pr.turno
ORDER BY score ASC NULLS LAST;


-- 4C: Funcionários por setor com produção vinculada
CREATE OR REPLACE VIEW vw_qualidade_setor_equipe AS
SELECT
  ca.setor,
  ca.cargo,
  COUNT(*) AS total_funcionarios,
  COUNT(*) FILTER (WHERE ca.ativo = true) AS ativos,
  ARRAY_AGG(ca.nome ORDER BY ca.nome) FILTER (WHERE ca.ativo = true) AS nomes
FROM cadastros_atak ca
WHERE ca.tipo = 'funcionario'
  AND ca.setor IS NOT NULL
GROUP BY ca.setor, ca.cargo
ORDER BY ca.setor, ca.cargo;


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 12 — Views de Qualidade por 4 Dimensões';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR FORNECEDOR:';
  RAISE NOTICE '    ✓ vw_qualidade_fornecedor (resumo com score + tendência)';
  RAISE NOTICE '    ✓ vw_qualidade_fornecedor_evolucao (dia a dia)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR PRODUTO:';
  RAISE NOTICE '    ✓ vw_qualidade_produto (com 9 defeitos + principal)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR LOTE:';
  RAISE NOTICE '    ✓ vw_qualidade_lote (individual + produtos JSON)';
  RAISE NOTICE '  ';
  RAISE NOTICE '  POR SETOR:';
  RAISE NOTICE '    ✓ vw_qualidade_setor (produção vs qualidade)';
  RAISE NOTICE '    ✓ vw_qualidade_setor_turno (granular por turno)';
  RAISE NOTICE '    ✓ vw_qualidade_setor_equipe (funcionários)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
