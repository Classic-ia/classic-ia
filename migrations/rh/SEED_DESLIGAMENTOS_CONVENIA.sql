-- ============================================================================
-- SEED_DESLIGAMENTOS_CONVENIA.sql
-- Importacao dos desligamentos do Convenia com tipo classificado
--
-- INSTRUCOES:
-- 1. Executar no Supabase SQL Editor (projeto RH: muiqmtnfvyffborgiwdw)
-- 2. Primeiro limpar registros antigos sem tipo
-- 3. Inserir novos registros com tipo correto baseado no campo do Convenia
--
-- MAPEAMENTO TIPOS CONVENIA → SISTEMA:
--   Pedido do Empregado / Quebra pelo Empregado / Antecipado empregado → pedido_demissao
--   Pedido da Empresa / Quebra pela Empresa / Antecipado empregador → demissao_empresa
--   Justa causa → justa_causa
--   Acordo entre partes → acordo_mutuo
--   Término contrato / Tempo determinado → termino_contrato
--   Morte → morte
-- ============================================================================

-- 1. Funcao auxiliar para classificar tipo
CREATE OR REPLACE FUNCTION pg_temp.classificar_tipo(tipo_convenia text) RETURNS text AS $$
BEGIN
  IF tipo_convenia ILIKE '%Pedido do Empregado%' THEN RETURN 'pedido_demissao';
  ELSIF tipo_convenia ILIKE '%Pedido da Empresa%' THEN RETURN 'demissao_empresa';
  ELSIF tipo_convenia ILIKE '%justa causa%Empresa%' THEN RETURN 'justa_causa';
  ELSIF tipo_convenia ILIKE '%acordo%' THEN RETURN 'acordo_mutuo';
  ELSIF tipo_convenia ILIKE '%Antecipado pelo empregado%' THEN RETURN 'pedido_demissao';
  ELSIF tipo_convenia ILIKE '%Antecipado pelo empregador%' THEN RETURN 'demissao_empresa';
  ELSIF tipo_convenia ILIKE '%Morte%' THEN RETURN 'morte';
  ELSIF tipo_convenia ILIKE '%Término%' OR tipo_convenia ILIKE '%Termino%' THEN RETURN 'termino_contrato';
  ELSIF tipo_convenia ILIKE '%Quebra%Empregado%' THEN RETURN 'pedido_demissao';
  ELSIF tipo_convenia ILIKE '%Quebra%Empresa%' OR tipo_convenia ILIKE '%Quebra%Estágio%' THEN RETURN 'demissao_empresa';
  ELSIF tipo_convenia ILIKE '%Outros%' THEN RETURN 'outros';
  ELSE RETURN 'outros';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- 2. Funcao auxiliar para classificar categoria de causa
CREATE OR REPLACE FUNCTION pg_temp.classificar_causa(motivo text, observacoes text, tipo text, tempo_dias int) RETURNS text AS $$
DECLARE
  txt text;
BEGIN
  txt := LOWER(COALESCE(motivo, '') || ' ' || COALESCE(observacoes, ''));

  -- Se saiu em menos de 90 dias, provavelmente problema de admissao
  IF tempo_dias IS NOT NULL AND tempo_dias <= 90 THEN RETURN 'admissao'; END IF;

  -- Palavras-chave
  IF txt LIKE '%salari%' OR txt LIKE '%financ%' OR txt LIKE '%proposta%' OR txt LIKE '%benefic%' THEN RETURN 'financeiro'; END IF;
  IF txt LIKE '%lider%' OR txt LIKE '%gestor%' OR txt LIKE '%chefe%' OR txt LIKE '%ambient%' OR txt LIKE '%carga%' OR txt LIKE '%turno%' THEN RETURN 'operacional'; END IF;
  IF txt LIKE '%famil%' OR txt LIKE '%pessoal%' OR txt LIKE '%mudanc%' OR txt LIKE '%saude%' OR txt LIKE '%cidade%' THEN RETURN 'pessoal'; END IF;
  IF txt LIKE '%adapt%' OR txt LIKE '%expectativa%' OR txt LIKE '%nao se adaptou%' THEN RETURN 'admissao'; END IF;

  -- Default baseado no tipo
  IF tipo = 'termino_contrato' THEN RETURN 'admissao'; END IF;

  RETURN NULL; -- sem classificacao
END;
$$ LANGUAGE plpgsql;

-- 3. Limpar registros sem tipo (provavelmente importacao anterior incompleta)
-- DELETE FROM rh_desligamentos WHERE tipo IS NULL;
-- DESCOMENTE A LINHA ACIMA SE QUISER LIMPAR OS ANTIGOS

-- 4. Inserir desligamentos
-- Evita duplicatas verificando por colaborador_id (ID Convenia) + data_desligamento
DO $$
DECLARE
  rec record;
  v_tipo text;
  v_categoria text;
  v_tempo int;
BEGIN
  FOR rec IN
    SELECT nome, id_convenia, cpf, cargo, setor, data_admissao, data_desligamento,
           tipo_convenia, motivo, observacoes, salario, aviso_previo, data_aviso,
           genero, vinculo
    FROM (VALUES
      -- LOTE 1: Primeiros registros (2020-2021)
      ('ADEMILSON DOS SANTOS NUNES','e7dd5646-343b-4e10-b87d-ab83dec96f52','063.825.439-69','AUXILIAR DE PRODUÇÃO','Produção','2020-07-21','2021-11-11','Demissão fora do contrato de experiência - Pedido do Empregado',NULL,NULL,1413.46,'Indenizado',NULL,'Masculino','CLT'),
      ('ADRIANA PORTO DA SILVA VICENTE','cd3801fd-75b3-46b5-9858-257ba098447f',NULL,'AUXILIAR DE PRODUÇÃO','Produção','2020-10-07','2023-01-04','Demissão SEM justa causa fora do contrato de experiência - Pedido da Empresa',NULL,NULL,1695.02,'Trabalhado','2022-12-06','Feminino','CLT'),
      ('ALESSANDRA APARECIDA CRUZ DE OLIVEIRA','945f757d-e261-42d6-a851-b9541581a75b','071.868.129-02','AUXILIAR DE PRODUÇÃO','Produção','2020-08-21','2022-06-24','Demissão SEM justa causa fora do contrato de experiência - Pedido da Empresa',NULL,NULL,1539.25,'Trabalhado','2022-05-26','Feminino','CLT'),
      ('ALESSANDRA TRIZOTE FRANCELINO','205fc121-bca5-405d-9a35-19248297d59c','055.314.409-01','AUXILIAR DE PRODUÇÃO','Produção','2020-06-26','2021-07-20','Demissão SEM justa causa fora do contrato de experiência - Pedido da Empresa',NULL,NULL,1413.46,'Trabalhado',NULL,'Feminino','CLT'),
      ('ALEX FURTADO DE OLIVEIRA','5c7a13bc-c571-46f7-a256-859e38bd7e7d','102.253.849-73','AUXILIAR DE PRODUÇÃO','Produção','2020-11-12','2021-05-10','Demissão fora do contrato de experiência - Pedido do Empregado',NULL,NULL,1413.46,'Trabalhado',NULL,'Masculino','CLT'),
      ('ALEX LUCIO DOS SANTOS','d7dee1b5-e521-4a84-9b57-72d22d73174d',NULL,'VIGILANTE','Segurança','2020-09-10','2024-04-11','Demissão fora do contrato de experiência - Pedido do Empregado','Pedido desligamento pelo empreago',NULL,1754.85,'Trabalhado','2024-03-13','Masculino','CLT'),
      ('ALICE APARECIDA FELICIO','5d800cdd-53e7-4cb6-8ef6-58347cb56ecb','158.512.048-00','AUXILIAR DE PRODUÇÃO','Produção','2020-06-03','2026-02-21','Demissão SEM justa causa fora do contrato de experiência - Pedido da Empresa','despedida sem justa causa','despedida sem justa causa',1910.95,'Trabalhado','2026-01-23','Feminino','CLT'),
      ('ALISSON ALMEIDA','3bd0e899-ea7e-4c67-ab5b-de0176775309','140.215.809-21','AUXILIAR DE PRODUÇÃO','Produção','2021-02-01','2022-03-02','Demissão SEM justa causa fora do contrato de experiência - Pedido da Empresa',NULL,NULL,1413.46,'Trabalhado','2022-02-02','Masculino','CLT')
    ) AS t(nome, id_convenia, cpf, cargo, setor, data_admissao, data_desligamento, tipo_convenia, motivo, observacoes, salario, aviso_previo, data_aviso, genero, vinculo)
  LOOP
    v_tipo := pg_temp.classificar_tipo(rec.tipo_convenia);
    v_tempo := CASE WHEN rec.data_admissao IS NOT NULL AND rec.data_desligamento IS NOT NULL
      THEN (rec.data_desligamento::date - rec.data_admissao::date)
      ELSE NULL END;
    v_categoria := pg_temp.classificar_causa(rec.motivo, rec.observacoes, v_tipo, v_tempo);

    -- Inserir se nao existe
    INSERT INTO rh_desligamentos (
      colaborador_id, colaborador_nome, nome, data_desligamento, data_admissao,
      tipo, categoria, motivo, cargo, setor, salario, tempo_empresa_dias
    )
    SELECT
      f.id, rec.nome, rec.nome, rec.data_desligamento::date, rec.data_admissao::date,
      v_tipo, v_categoria, COALESCE(rec.motivo, rec.tipo_convenia),
      rec.cargo, rec.setor, rec.salario, v_tempo
    FROM rh_funcionarios f
    WHERE f.id::text = rec.id_convenia
    AND NOT EXISTS (
      SELECT 1 FROM rh_desligamentos d
      WHERE d.colaborador_id = f.id
      AND d.data_desligamento = rec.data_desligamento::date
    )
    LIMIT 1;

    -- Se nao encontrou por ID, inserir sem colaborador_id
    IF NOT FOUND THEN
      IF NOT EXISTS (
        SELECT 1 FROM rh_desligamentos
        WHERE nome = rec.nome AND data_desligamento = rec.data_desligamento::date
      ) THEN
        INSERT INTO rh_desligamentos (
          colaborador_nome, nome, data_desligamento, data_admissao,
          tipo, categoria, motivo, cargo, setor, salario, tempo_empresa_dias
        ) VALUES (
          rec.nome, rec.nome, rec.data_desligamento::date, rec.data_admissao::date,
          v_tipo, v_categoria, COALESCE(rec.motivo, rec.tipo_convenia),
          rec.cargo, rec.setor, rec.salario, v_tempo
        );
      END IF;
    END IF;
  END LOOP;

  RAISE NOTICE 'Importacao concluida';
END $$;

-- NOTA: Este script contem apenas os primeiros 8 registros como exemplo.
-- O script completo com todos os ~300 registros sera gerado automaticamente
-- ou importado via CSV com a mesma logica de classificacao.
--
-- Para importar via CSV:
-- 1. Exportar os dados do Convenia como CSV
-- 2. Usar a funcao classificar_tipo() para mapear o tipo
-- 3. INSERT INTO rh_desligamentos SELECT ... FROM stg_convenia_desligamentos

-- 5. Verificar resultado
SELECT
  tipo,
  COUNT(*) AS total,
  ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 1) AS pct
FROM rh_desligamentos
WHERE tipo IS NOT NULL
GROUP BY tipo
ORDER BY total DESC;
