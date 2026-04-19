-- ============================================================
-- Migration: seed de categorias padrão (Fase 2)
-- Executado sempre que a migration roda (idempotente por UNIQUE).
-- ============================================================

INSERT INTO public.trans_categorias_produto (nome, tipo, descricao)
VALUES
  ('Pneu',              'pneu',          'Pneus novos ou recapados (individualidade via trans_pneus).'),
  ('Óleo de motor',     'lubrificante',  'Óleos de motor (15W40, 10W40, etc).'),
  ('Óleo de câmbio',    'lubrificante',  'Óleo de câmbio e diferencial.'),
  ('Filtro de óleo',    'filtro',        'Filtros de óleo do motor.'),
  ('Filtro de ar',      'filtro',        'Filtros de ar.'),
  ('Filtro de combustível', 'filtro',    'Filtros de combustível / separador de água.'),
  ('Filtro de cabine',  'filtro',        'Filtro de ar-condicionado/cabine.'),
  ('Lubrificante',      'lubrificante',  'Graxas e lubrificantes genéricos.'),
  ('Peça',              'peca',          'Peças avulsas (polia, tensor, bomba, etc).'),
  ('Peça elétrica',     'peca',          'Alternador, motor de partida, chicote.'),
  ('Peça mecânica',     'peca',          'Componentes mecânicos gerais.'),
  ('Peça de suspensão', 'peca',          'Amortecedores, molas, buchas.'),
  ('Peça de freio',     'peca',          'Pastilhas, discos, câmaras de freio.'),
  ('Acessório',         'acessorio',     'Lonas, cordas, catracas, acessórios.'),
  ('Combustível',       'combustivel',   'Diesel, S10, gasolina, ARLA.')
ON CONFLICT (nome) DO NOTHING;
