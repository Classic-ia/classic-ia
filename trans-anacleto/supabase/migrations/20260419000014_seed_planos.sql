-- =====================================================================
-- Fase 5: Seed de planos de manutencao padrao
-- =====================================================================
-- Planos padrao para frota pesada (transporte de carga).
-- Valores comuns em manuais de manutencao de caminhoes.
-- =====================================================================

INSERT INTO trans_planos_manutencao (nome, tipo_servico, intervalo_km, intervalo_dias, veiculo_tipo_aplicavel, observacao)
VALUES
  ('Troca de oleo motor',       'troca_oleo',    10000,  180,  ARRAY['cavalo','truck','toco','utilitario'], 'Verificar com fabricante'),
  ('Filtro de oleo',            'filtro_oleo',   10000,  NULL, ARRAY['cavalo','truck','toco','utilitario'], 'Trocar junto com oleo'),
  ('Filtro de combustivel',     'filtro_comb',   20000,  365,  ARRAY['cavalo','truck','toco','utilitario'], NULL),
  ('Filtro de ar',              'filtro_ar',     30000,  NULL, ARRAY['cavalo','truck','toco','utilitario'], NULL),
  ('Lubrificacao geral',        'lubrificacao',  15000,  90,   ARRAY['cavalo','truck','toco','utilitario','carreta'], 'Pinos, cardan, 5a roda'),
  ('Revisao geral',             'revisao',       20000,  NULL, ARRAY['cavalo','truck','toco','utilitario'], NULL),
  ('Alinhamento',               'alinhamento',   20000,  180,  ARRAY['cavalo','truck','toco','utilitario','carreta'], NULL),
  ('Balanceamento',             'balanceamento', 20000,  180,  ARRAY['cavalo','truck','toco','utilitario'], NULL),
  ('Correia sincronica',        'correia',       60000,  NULL, ARRAY['cavalo','truck','toco','utilitario'], NULL),
  ('Freio (pastilhas/lonas)',   'freio',         30000,  NULL, ARRAY['cavalo','truck','toco','utilitario','carreta'], 'Verificar desgaste a cada revisao'),
  ('Suspensao (molas/bolsas)',  'suspensao',     80000,  NULL, ARRAY['cavalo','truck','toco','carreta'], NULL),
  ('Bateria (verificacao)',     'eletrica',      NULL,   180,  ARRAY['cavalo','truck','toco','utilitario'], 'Testar capacidade a cada 6 meses')
ON CONFLICT (nome) DO NOTHING;
