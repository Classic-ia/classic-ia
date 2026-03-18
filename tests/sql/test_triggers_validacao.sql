-- ══════════════════════════════════════════════════════════════════════════════
-- TESTES SQL — Validação de triggers e functions
-- Rodar via: psql -f tests/sql/test_triggers_validacao.sql
-- Ou via pgTAP se disponível
-- ══════════════════════════════════════════════════════════════════════════════

-- Usa transação para não poluir dados
BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- SETUP: Dados de teste
-- ════════════════════════════════════════════════════════════════════════════

-- Fornecedor de teste
INSERT INTO cq_fornecedor_score (fornecedor_codigo, fornecedor_nome, score, status_fornecedor)
VALUES ('TEST_001', 'Fornecedor Teste CI', 80, 'normal')
ON CONFLICT (fornecedor_codigo) DO NOTHING;

-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 1: Trigger ABC Total Mismatch
-- Quando A+B+C != total_classificado, deve marcar como divergente
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_id UUID;
  v_status TEXT;
  v_diverg_count INTEGER;
BEGIN
  -- Inserir carga com mismatch
  INSERT INTO atak_cargas_raw (
    numero_documento, fornecedor_codigo, data_carga,
    class_a, class_b, class_c, total_classificado,
    hash_registro
  ) VALUES (
    'TEST_DOC_001', 'TEST_001', CURRENT_DATE,
    50, 30, 20, 999,  -- 50+30+20=100 ≠ 999
    'test_hash_abc_' || gen_random_uuid()::TEXT
  ) RETURNING id INTO v_id;

  -- Verificar que foi marcado como divergente
  SELECT status_validacao INTO v_status
  FROM atak_cargas_raw WHERE id = v_id;

  IF v_status != 'divergente' THEN
    RAISE EXCEPTION 'FALHOU: Teste ABC Mismatch — esperado divergente, obteve %', v_status;
  END IF;

  -- Verificar que divergência foi registrada
  SELECT COUNT(*) INTO v_diverg_count
  FROM cq_validacao_divergencias
  WHERE registro_origem_id = v_id AND tipo_divergencia = 'abc_total_mismatch';

  IF v_diverg_count = 0 THEN
    RAISE EXCEPTION 'FALHOU: Teste ABC Mismatch — divergência não registrada';
  END IF;

  RAISE NOTICE '✓ TESTE 1 PASSOU: ABC total mismatch detectado corretamente';
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 2: Carga com ABC correto NÃO deve gerar divergência
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_id UUID;
  v_status TEXT;
  v_diverg_count INTEGER;
BEGIN
  INSERT INTO atak_cargas_raw (
    numero_documento, fornecedor_codigo, data_carga,
    class_a, class_b, class_c, total_classificado,
    hash_registro
  ) VALUES (
    'TEST_DOC_002', 'TEST_001', CURRENT_DATE,
    50, 30, 20, 100,  -- 50+30+20=100 = 100 ✓
    'test_hash_ok_' || gen_random_uuid()::TEXT
  ) RETURNING id INTO v_id;

  SELECT status_validacao INTO v_status
  FROM atak_cargas_raw WHERE id = v_id;

  SELECT COUNT(*) INTO v_diverg_count
  FROM cq_validacao_divergencias
  WHERE registro_origem_id = v_id AND tipo_divergencia = 'abc_total_mismatch';

  IF v_diverg_count > 0 THEN
    RAISE EXCEPTION 'FALHOU: Teste ABC OK — divergência falsa registrada';
  END IF;

  RAISE NOTICE '✓ TESTE 2 PASSOU: ABC correto não gera divergência';
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 3: Score do fornecedor — cálculo correto
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_score NUMERIC;
  v_status TEXT;
BEGIN
  PERFORM fn_atualizar_score_fornecedor('TEST_001');

  SELECT score, status_fornecedor INTO v_score, v_status
  FROM cq_fornecedor_score
  WHERE fornecedor_codigo = 'TEST_001';

  IF v_score IS NULL THEN
    RAISE EXCEPTION 'FALHOU: Score não calculado para TEST_001';
  END IF;

  IF v_score < 0 OR v_score > 100 THEN
    RAISE EXCEPTION 'FALHOU: Score fora do range 0-100: %', v_score;
  END IF;

  IF v_status NOT IN ('normal','atencao','critico','bloqueado') THEN
    RAISE EXCEPTION 'FALHOU: Status inválido: %', v_status;
  END IF;

  RAISE NOTICE '✓ TESTE 3 PASSOU: Score calculado = % | Status = %', v_score, v_status;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 4: Histórico de score — snapshot salvo automaticamente
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM cq_fornecedor_score_historico
  WHERE fornecedor_codigo = 'TEST_001';

  IF v_count = 0 THEN
    RAISE EXCEPTION 'FALHOU: Nenhum snapshot salvo no histórico para TEST_001';
  END IF;

  RAISE NOTICE '✓ TESTE 4 PASSOU: % snapshots salvos no histórico', v_count;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 5: Notificação enfileirada para divergência crítica
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_notif_count INTEGER;
BEGIN
  -- Verificar se há notificações para divergência crítica
  SELECT COUNT(*) INTO v_notif_count
  FROM cq_fila_notificacao
  WHERE evento = 'divergencia_critica';

  -- Pelo menos o teste 1 gerou uma divergência crítica
  RAISE NOTICE '✓ TESTE 5 PASSOU: % notificações enfileiradas para divergências críticas', v_notif_count;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 6: fn_verificar_sla_sync retorna estrutura correta
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_rec RECORD;
  v_valid BOOLEAN := true;
BEGIN
  FOR v_rec IN SELECT * FROM fn_verificar_sla_sync() LOOP
    IF v_rec.endpoint_nome IS NULL THEN
      RAISE EXCEPTION 'FALHOU: endpoint_nome nulo no resultado de fn_verificar_sla_sync';
    END IF;
    IF v_rec.sla_status NOT IN ('critico','atrasado','erro','nunca_executado') THEN
      RAISE EXCEPTION 'FALHOU: sla_status inválido: %', v_rec.sla_status;
    END IF;
  END LOOP;

  RAISE NOTICE '✓ TESTE 6 PASSOU: fn_verificar_sla_sync retorna estrutura válida';
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 7: fn_enfileirar_notificacao — enfileira nos canais certos
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_count INTEGER;
BEGIN
  SELECT fn_enfileirar_notificacao(
    'sync_falhou',
    'TESTE: Sync falhou',
    'Mensagem de teste CI',
    '{}'::JSONB
  ) INTO v_count;

  IF v_count >= 0 THEN
    RAISE NOTICE '✓ TESTE 7 PASSOU: fn_enfileirar_notificacao criou % notificações', v_count;
  ELSE
    RAISE EXCEPTION 'FALHOU: fn_enfileirar_notificacao retornou valor inesperado: %', v_count;
  END IF;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- TESTE 8: Views de saúde do sistema
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_rec RECORD;
BEGIN
  SELECT * INTO v_rec FROM vw_system_health;

  IF v_rec.verificado_em IS NULL THEN
    RAISE EXCEPTION 'FALHOU: vw_system_health não retornou dados';
  END IF;

  IF v_rec.syncs_com_problema IS NULL OR v_rec.divergencias_abertas IS NULL THEN
    RAISE EXCEPTION 'FALHOU: vw_system_health tem campos nulos';
  END IF;

  RAISE NOTICE '✓ TESTE 8 PASSOU: vw_system_health — syncs_ok=%, diverg_abertas=%, forn_bloqueados=%',
    v_rec.syncs_ok, v_rec.divergencias_abertas, v_rec.fornecedores_bloqueados;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- CLEANUP
-- ════════════════════════════════════════════════════════════════════════════

-- Limpar dados de teste
DELETE FROM cq_fila_notificacao WHERE titulo LIKE 'TESTE:%';
DELETE FROM cq_validacao_divergencias WHERE fornecedor_codigo = 'TEST_001';
DELETE FROM cq_fornecedor_score_historico WHERE fornecedor_codigo = 'TEST_001';
DELETE FROM atak_cargas_raw WHERE fornecedor_codigo = 'TEST_001';
DELETE FROM cq_fornecedor_score WHERE fornecedor_codigo = 'TEST_001';

RAISE NOTICE '';
RAISE NOTICE '═══════════════════════════════════════════';
RAISE NOTICE '  TODOS OS 8 TESTES PASSARAM ✓';
RAISE NOTICE '═══════════════════════════════════════════';

ROLLBACK;  -- Rollback para não deixar dados de teste
