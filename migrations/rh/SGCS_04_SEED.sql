-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — Seed inicial
-- 10 familias funcionais da Politica Corporativa de C&S da Classic
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- IMPORTANTE:
--   - As bandas salariais (rh_cs_bandas) e os cargos com fiducia (Paulo,
--     Itamar, Kimila, Wanderley) NAO estao no seed: precisam ser cadastrados
--     pelo Comite quando da homologacao das bandas oficiais.
--   - O catalogo de beneficios tambem nao esta no seed: parte vem de
--     rh_beneficio existente. RH ira cadastrar via UI na Fase 2.
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. FAMILIAS FUNCIONAIS ──────────────────────────────────────────────────
INSERT INTO rh_cs_familias (codigo, nome, descricao, ordem) VALUES
  ('PROD',   'Producao',      'Operacao industrial do curtume (caleiros, descarne, recurtimento, acabamento)', 10),
  ('COUR',   'Couros',        'Tecnicos e analistas de couros (classificacao, conferencia)', 20),
  ('QUAL',   'Qualidade',     'CQ, conferencia de quantidade, ranking de fornecedores', 30),
  ('MANUT',  'Manutencao',    'Eletrica, mecanica, hidraulica, predial', 40),
  ('ADM',    'Administrativa','Administracao geral, recepcao, almoxarifado', 50),
  ('FIN',    'Financeira',    'Financeiro, contabilidade interna, fiscal', 60),
  ('RH',     'RH',            'Departamento Pessoal, SST, Beneficios, Recrutamento', 70),
  ('LIDER',  'Lideranca',     'Supervisores e lideres de processo (sem fiducia plena)', 80),
  ('GEST',   'Gestao',        'Cargos de confianca CLT art. 62 II (Paulo, Itamar, Kimila, Wanderley)', 90),
  ('APRE',   'Aprendizagem',  'Jovens aprendizes (Lei 10.097/2000)', 100)
ON CONFLICT (codigo) DO UPDATE SET
  nome = EXCLUDED.nome,
  descricao = EXCLUDED.descricao,
  ordem = EXCLUDED.ordem,
  updated_at = NOW();

-- ── 2. BENEFICIOS PADRAO (catalogo base — ajustar valores via UI) ──────────
-- Apenas codigo + nome + categoria + tipo. Valores e criterios sao calibrados
-- pelo RH em produc¸ao via UI/painel.
INSERT INTO rh_cs_beneficios (codigo, nome, categoria, tipo, exige_adesao, descricao) VALUES
  -- Legais obrigatorios
  ('VT',           'Vale-Transporte',                'legal_obrigatorio', 'monetario', TRUE,  'Lei 7.418/85'),
  ('FGTS',         'FGTS',                            'legal_obrigatorio', 'monetario', FALSE, '8% sobre salario'),
  ('INSS',         'INSS',                            'legal_obrigatorio', 'monetario', FALSE, 'Desconto e contribuicao patronal'),
  ('DECTERCO',     '13o salario',                     'legal_obrigatorio', 'monetario', FALSE, '1 salario adicional/ano'),
  ('FERIAS_13',    'Ferias + 1/3',                    'legal_obrigatorio', 'monetario', FALSE, '30 dias + abono 1/3'),
  ('ATS',          'ATS (3% por triênio)',           'cct',               'monetario', FALSE, 'Clausula 9a CCT Sintracouros'),
  -- CCT
  ('PREMASSI',     'Premio Assiduidade',             'cct',               'monetario', FALSE, 'Zero faltas injustificadas no mes'),
  ('AUXCRECHE',    'Auxilio Creche',                 'cct',               'monetario', FALSE, 'Filhos ate 6 anos com comprovante'),
  ('AUXFUNERAL',   'Auxilio Funeral',                'cct',               'monetario', FALSE, 'Conforme CCT Sintracouros'),
  -- Classic
  ('CESTABASIC',   'Cesta Basica',                   'classic',           'espec',     FALSE, 'Mensal'),
  ('REFEICAO',     'Refeicao/Lanche',                'classic',           'servico',   FALSE, 'Refeitorio interno'),
  ('PLANOSAUDE',   'Plano de Saude',                 'classic',           'servico',   TRUE,  'Co-participacao apos 90 dias'),
  ('PLANODENTAL',  'Plano Odontologico',             'classic',           'servico',   TRUE,  'Adesao opcional'),
  ('CONVFARMA',    'Convenio Farmacia',              'classic',           'servico',   TRUE,  'Desconto em folha'),
  ('SEGUROVIDA',   'Seguro de Vida em Grupo',        'classic',           'servico',   FALSE, 'Cobertura coletiva'),
  ('CESTANATAL',   'Cesta Natal',                    'classic',           'evento',    FALSE, 'Anual (dezembro)'),
  ('BRINDEANIV',   'Brinde Aniversariante',          'classic',           'evento',    FALSE, 'Mensal'),
  ('UNIFORME',     'Uniforme',                       'classic',           'espec',     FALSE, 'Conforme funcao'),
  ('EPI',          'EPI',                             'classic',           'espec',     FALSE, 'Conforme grau de risco SST'),
  -- Por cargo
  ('VEICULO',      'Veiculo da empresa',             'por_cargo',         'servico',   FALSE, 'Cargos de fiducia'),
  ('CARTAOCORP',   'Cartao Corporativo',             'por_cargo',         'servico',   FALSE, 'Cargos de fiducia / representacao'),
  ('AJUVIAG',      'Ajuda de Custo Viagem',          'por_cargo',         'monetario', FALSE, 'Wanderley (representacao externa)')
ON CONFLICT (codigo) DO UPDATE SET
  nome = EXCLUDED.nome,
  categoria = EXCLUDED.categoria,
  tipo = EXCLUDED.tipo,
  exige_adesao = EXCLUDED.exige_adesao,
  descricao = EXCLUDED.descricao,
  updated_at = NOW();

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_04_SEED.sql
-- 10 familias | 22 beneficios catalogados
--
-- PROXIMOS PASSOS POS-MIGRACAO:
--   1. Comite revisa criticidade A/B/C dos cargos (rh_cs_cargo_meta)
--   2. Comite homologa bandas oficiais por (cargo, nivel) -> rh_cs_bandas
--   3. RH atribui nivel I/II/III e familia aos 140 colaboradores via
--      cs_set_perfil_colaborador(...)
--   4. Cadastra os 4 cargos de fiducia (Paulo, Itamar, Kimila, Wanderley)
--      com fiducia_natureza I/II/III
-- ════════════════════════════════════════════════════════════════════════════
