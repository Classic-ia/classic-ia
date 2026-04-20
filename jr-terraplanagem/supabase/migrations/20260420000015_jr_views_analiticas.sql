-- ============================================================
-- Migration: Fase J8 - Views Analiticas de Resultado
-- v_jr_resultado_os, v_jr_resultado_maquina, v_jr_resultado_cliente
-- v_jr_dre_periodo, v_jr_horimetro_atual, v_jr_km_deslocamento
-- ============================================================

-- ============================================================
-- VIEW: v_jr_resultado_os
-- Receita (pagamentos + periodos calculados) - custos (despesas
-- vinculadas + manutencoes da maquina no periodo da OS +
-- abastecimento vinculado + deslocamento vinculado).
-- ============================================================
DROP VIEW IF EXISTS public.v_jr_resultado_os CASCADE;
CREATE VIEW public.v_jr_resultado_os AS
WITH os_base AS (
  SELECT
    o.id                       AS os_id,
    o.numero_os,
    o.cliente_id,
    o.obra_local,
    o.status,
    o.data_abertura,
    o.data_fim,
    o.valor_total              AS valor_os_snapshot
  FROM public.jr_ordens_servico o
),
rec_periodos AS (
  SELECT p.os_id, COALESCE(SUM(p.valor_calc), 0)::NUMERIC(14,2) AS receita_periodos,
         COALESCE(SUM(p.horas), 0)::NUMERIC(12,2) AS horas_totais
    FROM public.jr_os_periodos p
   GROUP BY p.os_id
),
rec_pagamentos AS (
  SELECT pg.os_id, COALESCE(SUM(pg.valor), 0)::NUMERIC(14,2) AS receita_pagamentos
    FROM public.jr_pagamentos pg
   GROUP BY pg.os_id
),
desp_diretas AS (
  SELECT d.os_id, COALESCE(SUM(d.valor), 0)::NUMERIC(14,2) AS despesas_diretas
    FROM public.jr_despesas d
   WHERE d.os_id IS NOT NULL
   GROUP BY d.os_id
),
desl_direto AS (
  SELECT dl.os_id, COALESCE(SUM(dl.valor_calc), 0)::NUMERIC(14,2) AS deslocamentos
    FROM public.jr_deslocamentos dl
   WHERE dl.os_id IS NOT NULL
   GROUP BY dl.os_id
),
abast_direto AS (
  SELECT a.os_id, COALESCE(SUM(a.valor_total), 0)::NUMERIC(14,2) AS abastecimentos
    FROM public.jr_abastecimentos a
   WHERE a.os_id IS NOT NULL
   GROUP BY a.os_id
)
SELECT
  b.os_id,
  b.numero_os,
  b.cliente_id,
  b.obra_local,
  b.status,
  b.data_abertura,
  b.data_fim,
  COALESCE(rp.horas_totais, 0)         AS horas_totais,
  COALESCE(rp.receita_periodos, 0)     AS receita_periodos,
  COALESCE(rpg.receita_pagamentos, 0)  AS receita_pagamentos,
  GREATEST(COALESCE(rp.receita_periodos, 0),
           COALESCE(rpg.receita_pagamentos, 0))  AS receita_total,
  COALESCE(dd.despesas_diretas, 0)     AS despesas_diretas,
  COALESCE(dl.deslocamentos, 0)        AS custo_deslocamentos,
  COALESCE(ab.abastecimentos, 0)       AS custo_abastecimentos,
  (COALESCE(dd.despesas_diretas, 0)
   + COALESCE(dl.deslocamentos, 0)
   + COALESCE(ab.abastecimentos, 0))::NUMERIC(14,2) AS custos_totais,
  (GREATEST(COALESCE(rp.receita_periodos, 0), COALESCE(rpg.receita_pagamentos, 0))
   - (COALESCE(dd.despesas_diretas, 0)
      + COALESCE(dl.deslocamentos, 0)
      + COALESCE(ab.abastecimentos, 0)))::NUMERIC(14,2) AS resultado_liquido,
  CASE
    WHEN GREATEST(COALESCE(rp.receita_periodos, 0), COALESCE(rpg.receita_pagamentos, 0)) > 0
    THEN ROUND(
      ((GREATEST(COALESCE(rp.receita_periodos, 0), COALESCE(rpg.receita_pagamentos, 0))
        - (COALESCE(dd.despesas_diretas, 0)
           + COALESCE(dl.deslocamentos, 0)
           + COALESCE(ab.abastecimentos, 0)))
       / NULLIF(GREATEST(COALESCE(rp.receita_periodos, 0), COALESCE(rpg.receita_pagamentos, 0)), 0))
      * 100, 2)
    ELSE 0
  END AS margem_percent,
  (GREATEST(COALESCE(rp.receita_periodos, 0), COALESCE(rpg.receita_pagamentos, 0))
   - COALESCE(rpg.receita_pagamentos, 0))::NUMERIC(14,2) AS saldo_receber
FROM os_base b
LEFT JOIN rec_periodos rp  ON rp.os_id  = b.os_id
LEFT JOIN rec_pagamentos rpg ON rpg.os_id = b.os_id
LEFT JOIN desp_diretas dd  ON dd.os_id  = b.os_id
LEFT JOIN desl_direto dl   ON dl.os_id  = b.os_id
LEFT JOIN abast_direto ab  ON ab.os_id  = b.os_id;

COMMENT ON VIEW public.v_jr_resultado_os IS
  'Resultado por OS: receita (max entre periodos calculados e pagamentos) - custos vinculados. Margem em %.';

-- ============================================================
-- VIEW: v_jr_resultado_maquina
-- Para cada maquina: horas totais registradas + receita calculada
-- + despesas atribuidas + abastecimentos + resultado operacional.
-- Sem corte por periodo para simplicidade; filtrar via WHERE na app.
-- ============================================================
DROP VIEW IF EXISTS public.v_jr_resultado_maquina CASCADE;
CREATE VIEW public.v_jr_resultado_maquina AS
WITH m_periodos AS (
  SELECT p.maquina_id,
         COALESCE(SUM(p.horas), 0)::NUMERIC(12,2)      AS horas_totais,
         COALESCE(SUM(p.valor_calc), 0)::NUMERIC(14,2) AS receita_horas,
         MIN(p.data) AS primeira_data,
         MAX(p.data) AS ultima_data
    FROM public.jr_os_periodos p
   GROUP BY p.maquina_id
),
m_desp AS (
  SELECT d.maquina_id,
         COALESCE(SUM(d.valor), 0)::NUMERIC(14,2) AS despesas
    FROM public.jr_despesas d
   WHERE d.maquina_id IS NOT NULL
   GROUP BY d.maquina_id
),
m_abast AS (
  SELECT a.maquina_id,
         COALESCE(SUM(a.valor_total), 0)::NUMERIC(14,2) AS custo_combustivel,
         COALESCE(SUM(a.litros), 0)::NUMERIC(12,2)      AS litros_totais
    FROM public.jr_abastecimentos a
   GROUP BY a.maquina_id
),
m_manut AS (
  SELECT mt.maquina_id,
         COALESCE(SUM(mt.custo_total_calc), 0)::NUMERIC(14,2) AS custo_manutencao,
         COUNT(*) FILTER (WHERE mt.status = 'concluida')      AS manutencoes_concluidas
    FROM public.jr_manutencoes mt
   GROUP BY mt.maquina_id
)
SELECT
  mq.id                              AS maquina_id,
  mq.codigo,
  mq.descricao,
  mq.tipo,
  mq.status,
  mq.valor_hora,
  mq.horimetro_atual,
  COALESCE(mp.horas_totais, 0)       AS horas_totais,
  COALESCE(mp.receita_horas, 0)      AS receita_horas,
  COALESCE(md.despesas, 0)           AS despesas,
  COALESCE(ma.custo_combustivel, 0)  AS custo_combustivel,
  COALESCE(ma.litros_totais, 0)      AS litros_totais,
  COALESCE(mn.custo_manutencao, 0)   AS custo_manutencao,
  COALESCE(mn.manutencoes_concluidas, 0) AS manutencoes_concluidas,
  (COALESCE(md.despesas, 0)
   + COALESCE(ma.custo_combustivel, 0)
   + COALESCE(mn.custo_manutencao, 0))::NUMERIC(14,2) AS custos_totais,
  (COALESCE(mp.receita_horas, 0)
   - COALESCE(md.despesas, 0)
   - COALESCE(ma.custo_combustivel, 0)
   - COALESCE(mn.custo_manutencao, 0))::NUMERIC(14,2) AS resultado_operacional,
  mp.primeira_data,
  mp.ultima_data
FROM public.jr_maquinas mq
LEFT JOIN m_periodos mp ON mp.maquina_id = mq.id
LEFT JOIN m_desp     md ON md.maquina_id = mq.id
LEFT JOIN m_abast    ma ON ma.maquina_id = mq.id
LEFT JOIN m_manut    mn ON mn.maquina_id = mq.id;

COMMENT ON VIEW public.v_jr_resultado_maquina IS
  'Agregado por maquina: horas, receita por hora, custos (despesas + combustivel + manutencao) e resultado.';

-- ============================================================
-- VIEW: v_jr_resultado_cliente
-- Totais de receita/custo agregados por cliente (via OS).
-- ============================================================
DROP VIEW IF EXISTS public.v_jr_resultado_cliente CASCADE;
CREATE VIEW public.v_jr_resultado_cliente AS
SELECT
  c.id                             AS cliente_id,
  COALESCE(c.razao_social, c.nome) AS cliente_nome,
  c.cpf_cnpj,
  COUNT(r.os_id)                   AS oss_total,
  COUNT(r.os_id) FILTER (WHERE r.status = 'concluida') AS oss_concluidas,
  COALESCE(SUM(r.receita_total), 0)::NUMERIC(14,2)  AS receita_total,
  COALESCE(SUM(r.custos_totais), 0)::NUMERIC(14,2)  AS custos_totais,
  COALESCE(SUM(r.resultado_liquido), 0)::NUMERIC(14,2) AS resultado_liquido,
  COALESCE(SUM(r.saldo_receber), 0)::NUMERIC(14,2)  AS saldo_receber_total,
  COALESCE(SUM(r.horas_totais), 0)::NUMERIC(12,2)   AS horas_totais
FROM public.jr_clientes c
LEFT JOIN public.v_jr_resultado_os r ON r.cliente_id = c.id
GROUP BY c.id, c.razao_social, c.nome, c.cpf_cnpj;

COMMENT ON VIEW public.v_jr_resultado_cliente IS
  'Agregado por cliente via OS (receita, custos, resultado, saldo a receber).';

-- ============================================================
-- VIEW: v_jr_horimetro_atual
-- Snapshot do horimetro de cada maquina + ultima movimentacao
-- (ultimo periodo, ultimo abastecimento, proxima manutencao).
-- ============================================================
DROP VIEW IF EXISTS public.v_jr_horimetro_atual CASCADE;
CREATE VIEW public.v_jr_horimetro_atual AS
WITH ult_per AS (
  SELECT DISTINCT ON (p.maquina_id)
         p.maquina_id, p.data AS ultima_data_os, p.horimetro_final AS ultimo_horimetro_os
    FROM public.jr_os_periodos p
   ORDER BY p.maquina_id, p.data DESC, p.created_at DESC
),
ult_abast AS (
  SELECT DISTINCT ON (a.maquina_id)
         a.maquina_id, a.data AS ultima_data_abast, a.horimetro_leitura AS ultimo_horimetro_abast
    FROM public.jr_abastecimentos a
   ORDER BY a.maquina_id, a.data DESC, a.created_at DESC
),
prox_manut AS (
  SELECT DISTINCT ON (s.maquina_id)
         s.maquina_id, s.plano_nome, s.tipo_servico,
         s.proximo_horimetro, s.proxima_data, s.horas_restantes, s.dias_restantes
    FROM public.v_jr_planos_maquina_status s
   ORDER BY s.maquina_id,
            COALESCE(s.horas_restantes, 9999999) ASC,
            COALESCE(s.dias_restantes, 9999999) ASC
)
SELECT
  m.id                         AS maquina_id,
  m.codigo,
  m.descricao,
  m.tipo,
  m.status,
  m.horimetro_atual,
  up.ultima_data_os,
  up.ultimo_horimetro_os,
  ua.ultima_data_abast,
  ua.ultimo_horimetro_abast,
  pm.plano_nome                AS proxima_manut_plano,
  pm.tipo_servico              AS proxima_manut_tipo,
  pm.proximo_horimetro         AS proxima_manut_horimetro,
  pm.proxima_data              AS proxima_manut_data,
  pm.horas_restantes           AS proxima_manut_horas_restantes,
  pm.dias_restantes            AS proxima_manut_dias_restantes
FROM public.jr_maquinas m
LEFT JOIN ult_per    up ON up.maquina_id = m.id
LEFT JOIN ult_abast  ua ON ua.maquina_id = m.id
LEFT JOIN prox_manut pm ON pm.maquina_id = m.id;

COMMENT ON VIEW public.v_jr_horimetro_atual IS
  'Snapshot operacional por maquina: horimetro atual + ultima atividade + proxima manutencao.';

-- ============================================================
-- VIEW: v_jr_km_deslocamento
-- Agregado por veiculo.
-- ============================================================
DROP VIEW IF EXISTS public.v_jr_km_deslocamento CASCADE;
CREATE VIEW public.v_jr_km_deslocamento AS
SELECT
  v.id               AS veiculo_id,
  v.placa,
  v.descricao,
  v.tipo,
  COUNT(dl.id)       AS deslocamentos_total,
  COALESCE(SUM(dl.km_rodado), 0)::NUMERIC(14,2) AS km_total,
  COALESCE(SUM(dl.valor_calc), 0)::NUMERIC(14,2) AS valor_total,
  MIN(dl.data)       AS primeira_data,
  MAX(dl.data)       AS ultima_data
FROM public.jr_veiculos v
LEFT JOIN public.jr_deslocamentos dl ON dl.veiculo_id = v.id
GROUP BY v.id, v.placa, v.descricao, v.tipo;

COMMENT ON VIEW public.v_jr_km_deslocamento IS
  'Agregado por veiculo: km rodados e valor total de deslocamentos.';
