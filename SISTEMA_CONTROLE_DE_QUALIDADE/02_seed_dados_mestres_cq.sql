-- ============================================================
-- SEED: Dados Mestres CQ - Classic Couros
-- Executar APOS migration_novo_modelo_cq.sql
-- ============================================================

-- ************************************************************
-- 1) DEFEITOS (11 tipos padrao)
-- ************************************************************
INSERT INTO public.cq_defeitos (codigo, nome, nome_normalizado, descricao, criticidade_padrao, ordem) VALUES
  ('fermentacao',       'Fermentacao',         'fermentacao',         'Fermentacao ou decomposicao do produto',        'alta',    1),
  ('sem_folhas',        'Sem Folhas',          'sem folhas',          'Produto sem folhas de protecao',                'media',   2),
  ('folhas_arrebentadas','Folhas Arrebentadas','folhas arrebentadas', 'Folhas de protecao danificadas',               'media',   3),
  ('pedaco',            'Pedaco',              'pedaco',              'Produto quebrado ou em pedacos',                'alta',    4),
  ('sujo',              'Sujo / Despojo',      'sujo despojo',        'Produto com sujeira ou despojo',               'media',   5),
  ('rasgado',           'Rasgado',             'rasgado',             'Produto rasgado',                               'alta',    6),
  ('corte_irregular',   'Corte Irregular',     'corte irregular',     'Corte fora do padrao',                          'media',   7),
  ('furado',            'Furado',              'furado',              'Produto com furos',                             'alta',    8),
  ('mau_cheiro',        'Mau Cheiro',          'mau cheiro',          'Produto com odor desagradavel',                 'critica', 9),
  ('contaminacao',      'Contaminacao',        'contaminacao',        'Contaminacao visivel no produto',               'critica', 10),
  ('outro_defeito',     'Outro Defeito',       'outro defeito',       'Outro tipo de defeito nao listado',            'media',   11)
ON CONFLICT (codigo) DO UPDATE SET
  nome = EXCLUDED.nome,
  descricao = EXCLUDED.descricao,
  criticidade_padrao = EXCLUDED.criticidade_padrao,
  ordem = EXCLUDED.ordem;


-- ************************************************************
-- 2) PRODUTOS (baseado nos produtos do ranking existente)
-- ************************************************************
INSERT INTO public.cq_produtos_v2 (codigo_interno, nome, nome_normalizado, categoria, unidade_medida) VALUES
  ('bucho',             'Bucho',               'bucho',               'Subproduto',  'UN'),
  ('omaso',             'Omaso',               'omaso',               'Subproduto',  'UN'),
  ('aorta',             'Aorta',               'aorta',               'Subproduto',  'UN'),
  ('culatra',           'Culatra',             'culatra',             'Subproduto',  'UN'),
  ('despojo',           'Despojo',             'despojo',             'Subproduto',  'UN'),
  ('rabo',              'Rabo',                'rabo',                'Subproduto',  'UN'),
  ('vergalho',          'Vergalho',            'vergalho',            'Subproduto',  'UN'),
  ('bexiga',            'Bexiga',              'bexiga',              'Subproduto',  'UN'),
  ('tripa_grossa',      'Tripa Grossa',        'tripa grossa',        'Subproduto',  'UN'),
  ('tripa_fina',        'Tripa Fina',          'tripa fina',          'Subproduto',  'UN'),
  ('bile',              'Bile',                'bile',                'Subproduto',  'UN'),
  ('carne',             'Carne',               'carne',               'Carne',       'KG'),
  ('couro',             'Couro',               'couro',               'Couro',       'UN'),
  ('couro_salgado',     'Couro Salgado',       'couro salgado',       'Couro',       'UN'),
  ('couro_wet_blue',    'Couro Wet Blue',      'couro wet blue',      'Couro',       'UN'),
  ('lingua',            'Lingua',              'lingua',              'Subproduto',  'UN'),
  ('pe',                'Pe',                  'pe',                  'Subproduto',  'UN'),
  ('cabeca',            'Cabeca',              'cabeca',              'Subproduto',  'UN'),
  ('mocoto',            'Mocoto',              'mocoto',              'Subproduto',  'UN'),
  ('tendao',            'Tendao',              'tendao',              'Subproduto',  'UN'),
  ('orelha',            'Orelha',              'orelha',              'Subproduto',  'UN'),
  ('figado',            'Figado',              'figado',              'Miudo',       'KG'),
  ('coracao',           'Coracao',             'coracao',             'Miudo',       'KG'),
  ('rim',               'Rim',                 'rim',                 'Miudo',       'KG'),
  ('pulmao',            'Pulmao',              'pulmao',              'Miudo',       'KG'),
  ('sebo',              'Sebo',                'sebo',                'Subproduto',  'KG'),
  ('osso',              'Osso',                'osso',                'Subproduto',  'KG'),
  ('cartilagem',        'Cartilagem',          'cartilagem',          'Subproduto',  'KG'),
  ('nervo',             'Nervo',               'nervo',               'Subproduto',  'KG'),
  ('casco',             'Casco',               'casco',               'Subproduto',  'UN'),
  ('outros',            'Outros',              'outros',              'Outros',      'UN')
ON CONFLICT (codigo_interno) DO UPDATE SET
  nome = EXCLUDED.nome,
  categoria = EXCLUDED.categoria,
  unidade_medida = EXCLUDED.unidade_medida;


-- ************************************************************
-- 3) PARAMETROS POR PRODUTO (limites padrao: 5% alerta, 15% reprovacao)
-- ************************************************************
INSERT INTO public.cq_parametros_produto (produto_id, perc_alerta, perc_reprovacao, exige_abc)
SELECT id, 5.0, 15.0, true
FROM public.cq_produtos_v2
WHERE ativo = true
ON CONFLICT (produto_id, ativo) DO NOTHING;


-- ************************************************************
-- 4) VERIFICACAO
-- ************************************************************
SELECT 'cq_defeitos' as tabela, count(*) as total FROM public.cq_defeitos
UNION ALL
SELECT 'cq_produtos_v2', count(*) FROM public.cq_produtos_v2
UNION ALL
SELECT 'cq_parametros_produto', count(*) FROM public.cq_parametros_produto
UNION ALL
SELECT 'cq_fornecedores', count(*) FROM public.cq_fornecedores;
