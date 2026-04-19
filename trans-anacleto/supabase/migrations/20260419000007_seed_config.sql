-- =====================================================================
-- Fase 3: seed de trans_config (parametros globais)
-- =====================================================================

INSERT INTO trans_config (chave, valor, descricao) VALUES
  ('sucata_modo', 'baixa_direta',
    'Modo de tratamento de sucata: baixa_direta (nao entra em estoque) ou estoque_c (entra como condicao C)'),
  ('default_lote_prefix', 'L-',
    'Prefixo dos lotes internos gerados (L-000001, L-000002, ...)'),
  ('alerta_lote_vencendo_dias', '30',
    'Dias antes do vencimento para alertar lote'),
  ('alerta_estoque_min_percent', '10',
    'Margem (%) abaixo do estoque_min para alerta'),
  ('alerta_manut_horas_antes_km', '500',
    'KM antes do vencimento para alerta de manutencao preventiva'),
  ('alerta_manut_horas_antes_dias', '15',
    'Dias antes do vencimento para alerta de manutencao preventiva'),
  ('tms_modo', 'standalone',
    'Modo de integracao com TMS: master_tms (TMS como master de cadastros) ou standalone (novo como master)'),
  ('etiqueta_cols', '3',
    'Colunas da folha A4 de etiquetas (70x37mm padrao = 3 colunas x 8 linhas)'),
  ('etiqueta_rows', '8',
    'Linhas da folha A4 de etiquetas')
ON CONFLICT (chave) DO NOTHING;
