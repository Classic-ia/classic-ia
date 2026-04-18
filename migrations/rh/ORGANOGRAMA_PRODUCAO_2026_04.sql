-- ============================================================================
-- ORGANOGRAMA DE PRODUCAO — Classic Couros (abril/2026)
-- Baseado no organograma oficial entregue em 18/04/2026
-- Executar no SQL Editor do Supabase RH (muiqmtnfvyffborgiwdw)
-- ============================================================================
-- Conteudo:
--   1. Setores da funcao PRODUCAO (14 setores)
--   2. Vagas abertas (28 vagas)
--   3. Lista de dispensa/avaliacao (observacoes em rh_funcionarios)
--   4. Hierarquia: gestor_id apontando para chefe imediato
--   5. View vw_organograma_producao para a pagina organograma.html
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 0. Filial padrao (Classic Jardim Alegre)
-- ----------------------------------------------------------------------------
DO $$
DECLARE v_filial_id UUID;
BEGIN
  SELECT id INTO v_filial_id FROM rh_filiais ORDER BY created_at LIMIT 1;
  IF v_filial_id IS NULL THEN
    RAISE NOTICE 'Atencao: nenhuma filial encontrada em rh_filiais. Cadastre uma filial primeiro.';
  END IF;
END $$;

-- ----------------------------------------------------------------------------
-- 1. SETORES DA PRODUCAO
-- ----------------------------------------------------------------------------
-- codigo usa prefixo PROD- para indicar setores vinculados a funcao Producao
INSERT INTO rh_setores (filial_id, nome, codigo, grau_risco, ativo)
SELECT f.id, s.nome, s.codigo, s.grau_risco, TRUE
FROM (SELECT id FROM rh_filiais ORDER BY created_at LIMIT 1) f
CROSS JOIN (VALUES
  ('Muidos / Carga e Descarga',       'PROD-MUI-CAR', 'alto'),
  ('Paletizacao',                     'PROD-PAL',     'medio'),
  ('Salga',                           'PROD-SAL',     'alto'),
  ('Bate Sal',                        'PROD-BAT',     'medio'),
  ('Lavacao',                         'PROD-LAV',     'alto'),
  ('Sebo',                            'PROD-SEB',     'alto'),
  ('Classificacao',                   'PROD-CLA',     'medio'),
  ('Embalagem',                       'PROD-EMB',     'baixo'),
  ('Qualidade',                       'PROD-QUA',     'baixo'),
  ('Couro',                           'PROD-COU',     'alto'),
  ('Limpeza',                         'PROD-LIM',     'medio'),
  ('Doca / Miudos',                   'PROD-DOC',     'medio'),
  ('Marcenaria',                      'PROD-MAR',     'medio'),
  ('Manutencao',                      'PROD-MAN',     'alto')
) AS s(nome, codigo, grau_risco)
ON CONFLICT (filial_id, nome) DO UPDATE
  SET codigo = EXCLUDED.codigo,
      grau_risco = EXCLUDED.grau_risco,
      ativo = TRUE,
      updated_at = NOW();

-- Tambem em rh_colaboradores.setor (texto legado)
-- (nao faz nada aqui; atualizacao por nome fica na etapa 4)

-- ----------------------------------------------------------------------------
-- 2. HIERARQUIA — gestor_id em rh_funcionarios
-- ----------------------------------------------------------------------------
-- Gerente: PAULO ALBERTON
-- Coordenadores setoriais: EVERTON FRAGA, DANILO ELIAS
-- Supervisores: SILVIA, ADRIANO, CLARICE, ANGELA (+ 3 vagas de supervisor)

-- Coordenadores -> Gerente
UPDATE rh_funcionarios
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'PAULO ALBERTON%' LIMIT 1),
    updated_at = NOW()
WHERE nome_completo ILIKE 'EVERTON%FRAGA%'
   OR nome_completo ILIKE 'DANILO%ELIAS%';

-- Supervisores -> Coordenador Everton (producao norte)
UPDATE rh_funcionarios
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'EVERTON%FRAGA%' LIMIT 1),
    updated_at = NOW()
WHERE nome_completo ILIKE 'SILVIA%'
   OR nome_completo ILIKE 'ADRIANO%'
   OR nome_completo ILIKE 'CLARICE%'
   OR nome_completo ILIKE 'ANGELA%';

-- Colaboradores por setor -> respectivo supervisor
-- SETOR: Paletizacao, Salga, Bate Sal, Lavacao  -> SILVIA
UPDATE rh_funcionarios f
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'SILVIA%' LIMIT 1),
    updated_at = NOW()
FROM rh_setores s
WHERE f.setor_id = s.id
  AND s.codigo IN ('PROD-PAL','PROD-SAL','PROD-BAT','PROD-LAV')
  AND f.status IN ('ativo','experiencia','afastado','ferias');

-- SETOR: Sebo, Classificacao -> supervisor (vaga — deixa sem gestor por enquanto)
-- SETOR: Qualidade -> ADRIANO / CLARICE
UPDATE rh_funcionarios f
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'ADRIANO%' LIMIT 1),
    updated_at = NOW()
FROM rh_setores s
WHERE f.setor_id = s.id
  AND s.codigo = 'PROD-QUA'
  AND f.status IN ('ativo','experiencia','afastado','ferias');

-- SETOR: Embalagem -> ANGELA
UPDATE rh_funcionarios f
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'ANGELA%' LIMIT 1),
    updated_at = NOW()
FROM rh_setores s
WHERE f.setor_id = s.id
  AND s.codigo = 'PROD-EMB'
  AND f.status IN ('ativo','experiencia','afastado','ferias');

-- SETOR: Couro, Limpeza, Marcenaria, Manutencao -> DANILO ELIAS
UPDATE rh_funcionarios f
SET gestor_id = (SELECT id FROM rh_funcionarios WHERE nome_completo ILIKE 'DANILO%ELIAS%' LIMIT 1),
    updated_at = NOW()
FROM rh_setores s
WHERE f.setor_id = s.id
  AND s.codigo IN ('PROD-COU','PROD-LIM','PROD-MAR','PROD-MAN')
  AND f.status IN ('ativo','experiencia','afastado','ferias');

-- ----------------------------------------------------------------------------
-- 3. VAGAS ABERTAS (28)
-- Fonte: quadro "VAGAS A OCUPAR" do organograma
-- ----------------------------------------------------------------------------
INSERT INTO rs_vagas (titulo, setor, cargo, tipo_contrato, turno, quantidade, prioridade, status, motivo_abertura, observacoes)
VALUES
  ('Auxiliar de Carga e Descarga',      'Muidos / Carga e Descarga', 'Auxiliar de Producao',  'CLT', 'comercial', 4,  'alta',    'aberta', 'aumento_quadro', 'Masculino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Descongelar Omaso',     'Muidos / Carga e Descarga', 'Auxiliar de Producao',  'CLT', 'comercial', 2,  'alta',    'aberta', 'aumento_quadro', 'Feminino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Paletizacao',           'Paletizacao',               'Auxiliar de Producao',  'CLT', 'comercial', 3,  'normal',  'aberta', 'aumento_quadro', 'Feminino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Sebo',                  'Sebo',                      'Auxiliar de Producao',  'CLT', 'comercial', 10, 'urgente', 'aberta', 'aumento_quadro', 'Masculino. 3 ja para ingressar. Referente ao organograma 04/2026.'),
  ('Auxiliar de Classificacao',         'Classificacao',             'Auxiliar de Classificacao', 'CLT', 'comercial', 3, 'alta', 'aberta', 'aumento_quadro', 'Feminino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Embalagem (Masc.)',     'Embalagem',                 'Auxiliar de Embalagem', 'CLT', 'comercial', 1,  'normal',  'aberta', 'substituicao',   'Masculino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Embalagem (Fem.)',      'Embalagem',                 'Auxiliar de Embalagem', 'CLT', 'comercial', 1,  'normal',  'aberta', 'substituicao',   'Feminino. Referente ao organograma 04/2026.'),
  ('Auxiliar de Couro',                 'Couro',                     'Auxiliar de Producao',  'CLT', 'comercial', 4,  'alta',    'aberta', 'aumento_quadro', 'Masculino. Referente ao organograma 04/2026.');

-- Supervisores em aberto
INSERT INTO rs_vagas (titulo, setor, cargo, tipo_contrato, turno, quantidade, prioridade, status, motivo_abertura, observacoes)
VALUES
  ('Supervisor de Carga e Descarga',    'Muidos / Carga e Descarga', 'Supervisor de Producao', 'CLT', 'comercial', 1, 'urgente', 'aberta', 'nova_funcao', 'Referente ao organograma 04/2026.'),
  ('Supervisor de Producao (Sebo/Classificacao)', 'Sebo',           'Supervisor de Producao', 'CLT', 'comercial', 2, 'urgente', 'aberta', 'nova_funcao', 'Cobre Sebo e Classificacao. Referente ao organograma 04/2026.'),
  ('Supervisor de Producao (Couro/Limpeza)',     'Couro',            'Supervisor de Producao', 'CLT', 'comercial', 1, 'urgente', 'aberta', 'nova_funcao', 'Cobre Couro, Limpeza, Marcenaria e Manutencao. Referente ao organograma 04/2026.'),
  ('Coordenador Setorial',               'Producao',                 'Coordenador Setorial',   'CLT', 'comercial', 1, 'alta',    'aberta', 'nova_funcao', 'Turno 14h-23h. Referente ao organograma 04/2026.');

-- ----------------------------------------------------------------------------
-- 4. LISTA DE DISPENSA / AVALIACAO
-- Fonte: quadro "RELACAO DE COLABORADORES PARA DISPENSA"
-- Grava orientacao em rh_funcionarios.observacoes
-- ----------------------------------------------------------------------------
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar para dispensa'        , updated_at = NOW() WHERE nome_completo ILIKE 'NILTON%NASCI%BEZERRA%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Aguardar'                      , updated_at = NOW() WHERE nome_completo ILIKE 'HEMERSON%SILVA%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar para dispensa'        , updated_at = NOW() WHERE nome_completo ILIKE 'IVANEI%FERNANDES%FARIAS%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar'                       , updated_at = NOW() WHERE nome_completo ILIKE 'PATRICIA%AMSTALDEN%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar para dispensa'        , updated_at = NOW() WHERE nome_completo ILIKE 'NELSON%QUILIS%CANDIDO%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar para dispensa'        , updated_at = NOW() WHERE nome_completo ILIKE 'LUCINEIA%OLIVEIRA%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Manter'                        , updated_at = NOW() WHERE nome_completo ILIKE 'ROZELI%PRESTES%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Dispensar'                     , updated_at = NOW() WHERE nome_completo ILIKE 'VANUSA%SANTOS%RIBEIRO%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar para dispensa'        , updated_at = NOW() WHERE nome_completo ILIKE 'LUZIA%PEREIRA%ANDRADE%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Avaliar (Marrom/Bueno/Amorim)', updated_at = NOW() WHERE nome_completo ILIKE 'MARROM%' OR nome_completo ILIKE '%BUENO%' OR nome_completo ILIKE '%AMORIM%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Aplicar O.S.'                  , updated_at = NOW() WHERE nome_completo ILIKE 'DENILSON%SARTO%';
UPDATE rh_funcionarios SET observacoes = COALESCE(observacoes || E'\n', '') || '[04/2026] Aguardando diretoria'          , updated_at = NOW() WHERE nome_completo ILIKE 'ALESSANDRA%';

-- ----------------------------------------------------------------------------
-- 5. VIEW: vw_organograma_producao
-- Consumida pela pagina organograma.html
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_organograma_producao AS
SELECT
  s.id                          AS setor_id,
  s.nome                        AS setor,
  s.codigo                      AS setor_codigo,
  s.grau_risco,
  COUNT(f.id) FILTER (WHERE f.status = 'ativo')      AS ativos,
  COUNT(f.id) FILTER (WHERE f.status = 'afastado')   AS afastados,
  COUNT(f.id) FILTER (WHERE f.status = 'ferias')     AS ferias,
  COUNT(f.id) FILTER (WHERE f.status = 'experiencia')AS experiencia,
  COUNT(f.id)                                        AS total,
  jsonb_agg(
    jsonb_build_object(
      'id', f.id,
      'nome', f.nome_completo,
      'status', f.status,
      'data_admissao', f.data_admissao,
      'observacoes', f.observacoes,
      'jovem_aprendiz', COALESCE(f.jovem_aprendiz, FALSE)
    ) ORDER BY f.nome_completo
  ) FILTER (WHERE f.id IS NOT NULL)                  AS colaboradores
FROM rh_setores s
LEFT JOIN rh_funcionarios f
  ON f.setor_id = s.id
  AND f.status IN ('ativo','experiencia','afastado','ferias')
WHERE s.codigo LIKE 'PROD-%'
GROUP BY s.id, s.nome, s.codigo, s.grau_risco
ORDER BY s.nome;

COMMENT ON VIEW vw_organograma_producao IS 'Organograma da funcao Producao agrupado por setor com lista de colaboradores.';

COMMIT;

-- ============================================================================
-- VALIDACAO — consultas manuais pos-execucao
-- ============================================================================
-- SELECT * FROM vw_organograma_producao;
-- SELECT COUNT(*) FROM rs_vagas WHERE status = 'aberta';                 -- esperado: 28+
-- SELECT nome_completo, observacoes FROM rh_funcionarios
--   WHERE observacoes ILIKE '%[04/2026]%' ORDER BY nome_completo;
