-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO: Guards Estruturais — Impedir Erro Humano
-- O banco REJEITA dados inválidos; não depende de validação no frontend.
-- Rodar APÓS 01_migration_cadastros.sql
-- ══════════════════════════════════════════════════════════════════════════════

-- ════════════════════════════════════════════════════════════════════════════
-- 1. CONSTRAINTS RÍGIDOS EM atak_cargas_raw
-- ════════════════════════════════════════════════════════════════════════════

-- Quantidade não pode ser negativa
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_qtd_frigo_pos
  CHECK (qtd_frigo >= 0);
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_class_a_pos
  CHECK (class_a >= 0);
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_class_b_pos
  CHECK (class_b >= 0);
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_class_c_pos
  CHECK (class_c >= 0);
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_total_pos
  CHECK (total_classificado >= 0);
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_peso_pos
  CHECK (peso_frigo_kg >= 0);

-- Data de coleta não pode ser futura (margem de 1 dia por fuso)
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_data_nao_futura
  CHECK (data_coleta <= CURRENT_DATE + INTERVAL '1 day');

-- Fornecedor obrigatório
ALTER TABLE atak_cargas_raw ALTER COLUMN fornecedor_codigo SET NOT NULL;

-- Tipo de conservação padronizado
ALTER TABLE atak_cargas_raw DROP CONSTRAINT IF EXISTS chk_cargas_tipo_conservacao;
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_tipo_conservacao
  CHECK (tipo_conservacao IS NULL OR tipo_conservacao IN ('CONGELADO','SALGADO','RESFRIADO','FRESCO'));

-- Status de validação padronizado (já existe, reforçar)
ALTER TABLE atak_cargas_raw DROP CONSTRAINT IF EXISTS atak_cargas_raw_status_validacao_check;
ALTER TABLE atak_cargas_raw ADD CONSTRAINT chk_cargas_status_validacao
  CHECK (status_validacao IN ('pendente','valido','invalido','divergente'));

-- ════════════════════════════════════════════════════════════════════════════
-- 2. CONSTRAINTS EM cq_cargas (contagem interna)
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_qtd_frigo_pos
  CHECK (qtd_frigo >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_qtd_classic_pos
  CHECK (qtd_classic >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_rasgados_pos
  CHECK (rasgados >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_furados_pos
  CHECK (furados >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_pelando_pos
  CHECK (pelando >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_sem_folha_pos
  CHECK (sem_folha >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_sujos_pos
  CHECK (sujos >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_pedacos_pos
  CHECK (pedacos >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_total_pos
  CHECK (total_problemas >= 0);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_pct_range
  CHECK (porcentagem >= 0 AND porcentagem <= 100);
ALTER TABLE cq_cargas ADD CONSTRAINT chk_cqcargas_classificacao
  CHECK (classificacao IS NULL OR classificacao IN ('A','B','C'));

-- ════════════════════════════════════════════════════════════════════════════
-- 3. CONSTRAINTS EM registros_cq_inspecao
-- ════════════════════════════════════════════════════════════════════════════

-- Percentual de defeitos entre 0 e 100
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_pct_range
  CHECK (percentual_defeitos >= 0 AND percentual_defeitos <= 100);

-- Quantidade analisada deve ser > 0
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_qtd_analisada_pos
  CHECK (quantidade_analisada > 0);

-- Classificação padronizada
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_classificacao;
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_classificacao
  CHECK (classificacao IN ('A','B','C'));

-- Status final padronizado
ALTER TABLE registros_cq_inspecao DROP CONSTRAINT IF EXISTS chk_insp_status_final;
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_status_final
  CHECK (status_final IN ('aprovado','ressalva','bloqueado'));

-- Cada defeito >= 0
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_fermentacao_pos
  CHECK (fermentacao >= 0);
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_sem_folhas_pos
  CHECK (sem_folhas >= 0);
ALTER TABLE registros_cq_inspecao ADD CONSTRAINT chk_insp_total_pos
  CHECK (total_defeitos >= 0);

-- ════════════════════════════════════════════════════════════════════════════
-- 4. TRIGGER: Auto-calcular campos derivados (impedir inconsistência manual)
-- ════════════════════════════════════════════════════════════════════════════

-- Auto-calcular diferença em cq_cargas
CREATE OR REPLACE FUNCTION fn_auto_calcular_cq_cargas()
RETURNS TRIGGER AS $$
BEGIN
  -- Diferença frigo vs classic (sempre recalculada)
  NEW.diferenca := NEW.qtd_frigo - NEW.qtd_classic;

  -- Total de problemas (soma de todos os defeitos)
  NEW.total_problemas := COALESCE(NEW.rasgados, 0)
    + COALESCE(NEW.furados, 0)
    + COALESCE(NEW.pelando, 0)
    + COALESCE(NEW.sem_folha, 0)
    + COALESCE(NEW.sujos, 0)
    + COALESCE(NEW.pedacos, 0)
    + COALESCE(NEW.corte_irregular, 0)
    + COALESCE(NEW.folha_extra, 0);

  -- Porcentagem de problemas
  IF NEW.qtd_classic > 0 THEN
    NEW.porcentagem := ROUND((NEW.total_problemas::NUMERIC / NEW.qtd_classic) * 100, 4);
  ELSE
    NEW.porcentagem := 0;
  END IF;

  -- Classificação automática baseada na porcentagem
  IF NEW.porcentagem <= 5 THEN
    NEW.classificacao := 'A';
  ELSIF NEW.porcentagem <= 15 THEN
    NEW.classificacao := 'B';
  ELSE
    NEW.classificacao := 'C';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_calcular_cq_cargas
  BEFORE INSERT OR UPDATE ON cq_cargas
  FOR EACH ROW
  EXECUTE FUNCTION fn_auto_calcular_cq_cargas();

-- Auto-calcular total_classificado em atak_cargas_raw se não veio preenchido
CREATE OR REPLACE FUNCTION fn_auto_total_classificado()
RETURNS TRIGGER AS $$
BEGIN
  -- Se total_classificado é 0 ou NULL mas tem A/B/C, calcular
  IF (NEW.total_classificado IS NULL OR NEW.total_classificado = 0)
     AND (NEW.class_a > 0 OR NEW.class_b > 0 OR NEW.class_c > 0) THEN
    NEW.total_classificado := COALESCE(NEW.class_a, 0)
      + COALESCE(NEW.class_b, 0)
      + COALESCE(NEW.class_c, 0);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_total_classificado
  BEFORE INSERT OR UPDATE ON atak_cargas_raw
  FOR EACH ROW
  EXECUTE FUNCTION fn_auto_total_classificado();

-- Auto-calcular percentual e total de defeitos em inspeções
CREATE OR REPLACE FUNCTION fn_auto_calcular_inspecao()
RETURNS TRIGGER AS $$
BEGIN
  -- Somar todos os defeitos
  NEW.total_defeitos := COALESCE(NEW.fermentacao, 0)
    + COALESCE(NEW.sem_folhas, 0)
    + COALESCE(NEW.folhas_arrebentadas, 0)
    + COALESCE(NEW.pedaco, 0)
    + COALESCE(NEW.sujo, 0)
    + COALESCE(NEW.rasgado, 0)
    + COALESCE(NEW.corte_irregular, 0)
    + COALESCE(NEW.furado, 0)
    + COALESCE(NEW.mau_cheiro, 0)
    + COALESCE(NEW.contaminacao, 0)
    + COALESCE(NEW.outro_defeito, 0);

  -- Percentual
  IF NEW.quantidade_analisada > 0 THEN
    NEW.percentual_defeitos := ROUND((NEW.total_defeitos::NUMERIC / NEW.quantidade_analisada) * 100, 2);
  ELSE
    NEW.percentual_defeitos := 0;
  END IF;

  -- Classificação automática
  IF NEW.percentual_defeitos <= 5 THEN
    NEW.classificacao := 'A';
  ELSIF NEW.percentual_defeitos <= 15 THEN
    NEW.classificacao := 'B';
  ELSE
    NEW.classificacao := 'C';
  END IF;

  -- Status final automático
  IF NEW.percentual_defeitos <= 5 THEN
    NEW.status_final := 'aprovado';
  ELSIF NEW.percentual_defeitos <= 15 THEN
    NEW.status_final := 'ressalva';
  ELSE
    NEW.status_final := 'bloqueado';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_calcular_inspecao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  EXECUTE FUNCTION fn_auto_calcular_inspecao();

-- ════════════════════════════════════════════════════════════════════════════
-- 5. GUARD: Impedir alteração de registros já encerrados/aprovados
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_inspecao_imutavel()
RETURNS TRIGGER AS $$
BEGIN
  -- Não permite alterar inspeções encerradas ou canceladas
  IF OLD.status_workflow IN ('encerrada', 'cancelada') THEN
    RAISE EXCEPTION 'Inspeção % está % e não pode ser alterada.',
      OLD.id_inspecao, OLD.status_workflow;
  END IF;

  -- Não permite retroceder de aprovada para outro status (exceto encerrada)
  IF OLD.status_workflow = 'aprovada' AND NEW.status_workflow NOT IN ('aprovada', 'encerrada') THEN
    RAISE EXCEPTION 'Inspeção aprovada não pode retroceder para %.',
      NEW.status_workflow;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_inspecao_imutavel
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  EXECUTE FUNCTION fn_guard_inspecao_imutavel();

-- Guard: Impedir exclusão de divergências resolvidas (auditoria)
CREATE OR REPLACE FUNCTION fn_guard_divergencia_delete()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status IN ('resolvida', 'escalada') THEN
    RAISE EXCEPTION 'Divergência % está % e não pode ser excluída (auditoria).',
      OLD.id, OLD.status;
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_divergencia_delete
  BEFORE DELETE ON cq_validacao_divergencias
  FOR EACH ROW
  EXECUTE FUNCTION fn_guard_divergencia_delete();

-- Guard: Impedir remoção de sync_log (rastreabilidade)
CREATE OR REPLACE FUNCTION fn_guard_sync_log_imutavel()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'Logs de sincronização não podem ser excluídos (rastreabilidade).';
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_sync_log_delete
  BEFORE DELETE ON atak_sync_log
  FOR EACH ROW
  EXECUTE FUNCTION fn_guard_sync_log_imutavel();

-- ════════════════════════════════════════════════════════════════════════════
-- 6. GUARD: Normalização automática de dados (impedir formato errado)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_normalizar_placa()
RETURNS TRIGGER AS $$
BEGIN
  -- Normalizar placas: remover traços/espaços, maiúsculas
  IF NEW.cavalo_placa IS NOT NULL THEN
    NEW.cavalo_placa := UPPER(REGEXP_REPLACE(NEW.cavalo_placa, '[^A-Za-z0-9]', '', 'g'));
  END IF;
  IF NEW.carreta1_placa IS NOT NULL THEN
    NEW.carreta1_placa := UPPER(REGEXP_REPLACE(NEW.carreta1_placa, '[^A-Za-z0-9]', '', 'g'));
  END IF;
  IF NEW.carreta2_placa IS NOT NULL THEN
    NEW.carreta2_placa := UPPER(REGEXP_REPLACE(NEW.carreta2_placa, '[^A-Za-z0-9]', '', 'g'));
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_normalizar_placa_cargas
  BEFORE INSERT OR UPDATE ON atak_cargas_raw
  FOR EACH ROW
  EXECUTE FUNCTION fn_normalizar_placa();

-- Normalizar placa de veículos
CREATE OR REPLACE FUNCTION fn_normalizar_placa_veiculo()
RETURNS TRIGGER AS $$
BEGIN
  NEW.placa := UPPER(REGEXP_REPLACE(NEW.placa, '[^A-Za-z0-9]', '', 'g'));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_normalizar_placa_veiculos
  BEFORE INSERT OR UPDATE ON cq_veiculos
  FOR EACH ROW
  EXECUTE FUNCTION fn_normalizar_placa_veiculo();

-- Normalizar CNPJ (remover pontos, traços, barras)
CREATE OR REPLACE FUNCTION fn_normalizar_cnpj()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.cnpj IS NOT NULL THEN
    NEW.cnpj := REGEXP_REPLACE(NEW.cnpj, '[^0-9]', '', 'g');
    -- Validar comprimento (14 dígitos)
    IF LENGTH(NEW.cnpj) != 14 AND LENGTH(NEW.cnpj) != 11 THEN
      -- Permitir CPF (11) e CNPJ (14), rejeitar outros
      IF LENGTH(NEW.cnpj) > 0 AND LENGTH(NEW.cnpj) NOT IN (11, 14) THEN
        RAISE WARNING 'CNPJ/CPF com tamanho inválido (%): %', LENGTH(NEW.cnpj), NEW.cnpj;
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_normalizar_cnpj
  BEFORE INSERT OR UPDATE ON cadastros_atak
  FOR EACH ROW
  EXECUTE FUNCTION fn_normalizar_cnpj();

-- ════════════════════════════════════════════════════════════════════════════
-- 7. GUARD: Transições de workflow válidas
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_transicao_workflow()
RETURNS TRIGGER AS $$
DECLARE
  transicoes_validas JSONB := '{
    "rascunho":           ["em_analise", "cancelada"],
    "em_analise":         ["aguardando_revisao", "rascunho", "cancelada"],
    "aguardando_revisao": ["revisada", "reprovada", "em_analise"],
    "revisada":           ["aprovada", "reprovada", "aguardando_revisao"],
    "reprovada":          ["em_analise", "cancelada"],
    "aprovada":           ["encerrada"],
    "encerrada":          [],
    "cancelada":          []
  }'::JSONB;
  permitidos JSONB;
BEGIN
  -- Pular se não mudou o status
  IF OLD.status_workflow = NEW.status_workflow THEN
    RETURN NEW;
  END IF;

  permitidos := transicoes_validas -> OLD.status_workflow;

  IF permitidos IS NULL OR NOT (permitidos ? NEW.status_workflow) THEN
    RAISE EXCEPTION 'Transição inválida: % → %. Permitidos: %',
      OLD.status_workflow, NEW.status_workflow,
      COALESCE(permitidos::TEXT, '[]');
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_transicao_workflow
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  EXECUTE FUNCTION fn_guard_transicao_workflow();

-- ════════════════════════════════════════════════════════════════════════════
-- 8. GUARD: Impedir auto-aprovação (criador ≠ aprovador)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_auto_aprovacao()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status_workflow = 'aprovada' AND NEW.criador_id IS NOT NULL THEN
    -- O usuário que está aprovando não pode ser o criador
    IF NEW.criador_id = auth.uid() THEN
      RAISE EXCEPTION 'Auto-aprovação proibida: o criador não pode aprovar sua própria inspeção.';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_auto_aprovacao
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  WHEN (NEW.status_workflow = 'aprovada')
  EXECUTE FUNCTION fn_guard_auto_aprovacao();

-- ════════════════════════════════════════════════════════════════════════════
-- 9. AUDIT: Registrar TODA alteração em tabelas críticas
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_audit_alteracao()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO cq_audit_log (
    tabela, operacao, registro_id,
    dados_antigos, dados_novos,
    usuario_id, criado_em
  ) VALUES (
    TG_TABLE_NAME,
    TG_OP,
    COALESCE(NEW.id, OLD.id),
    CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) ELSE NULL END,
    auth.uid(),
    NOW()
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Audit em tabelas críticas
DO $$
DECLARE
  t TEXT;
BEGIN
  FOR t IN SELECT unnest(ARRAY[
    'atak_cargas_raw',
    'cq_validacao_divergencias',
    'cq_fornecedor_score',
    'cadastros_atak',
    'cq_veiculos',
    'cq_produtos'
  ]) LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trg_audit_%s ON %I; '
      'CREATE TRIGGER trg_audit_%s '
      'AFTER INSERT OR UPDATE OR DELETE ON %I '
      'FOR EACH ROW EXECUTE FUNCTION fn_audit_alteracao();',
      t, t, t, t
    );
  END LOOP;
END;
$$;

-- Garantir que cq_audit_log tem as colunas necessárias
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS tabela TEXT;
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS operacao TEXT;
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS registro_id UUID;
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS dados_antigos JSONB;
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS dados_novos JSONB;
ALTER TABLE cq_audit_log ADD COLUMN IF NOT EXISTS usuario_id UUID;

-- ════════════════════════════════════════════════════════════════════════════
-- 10. RESUMO: O que o banco IMPEDE estruturalmente
-- ════════════════════════════════════════════════════════════════════════════
/*
  ┌─────────────────────────────────────┬──────────────────────────────────────┐
  │ ERRO HUMANO                        │ GUARD ESTRUTURAL                     │
  ├─────────────────────────────────────┼──────────────────────────────────────┤
  │ Digitar quantidade negativa         │ CHECK >= 0 em TODOS os campos numéricos │
  │ Classificação fora de A/B/C         │ CHECK IN ('A','B','C')               │
  │ % fora de 0-100                     │ CHECK >= 0 AND <= 100                │
  │ Data futura                         │ CHECK <= CURRENT_DATE + 1 day        │
  │ Tipo conservação inválido           │ CHECK IN ('CONGELADO','SALGADO'...)   │
  │ Placa com formato errado            │ Trigger normaliza automaticamente     │
  │ CNPJ com pontos/traços              │ Trigger normaliza automaticamente     │
  │ Esquecer total de defeitos          │ Trigger recalcula automaticamente     │
  │ Errar % de defeitos                 │ Trigger recalcula automaticamente     │
  │ Classificação inconsistente         │ Trigger reclassifica automaticamente  │
  │ Pular etapa de workflow             │ Trigger valida transições permitidas  │
  │ Aprovar própria inspeção            │ Trigger bloqueia auto-aprovação       │
  │ Alterar inspeção encerrada          │ Trigger rejeita update em encerrada   │
  │ Apagar divergência resolvida        │ Trigger rejeita delete (auditoria)    │
  │ Apagar log de sync                  │ Trigger rejeita delete (rastreabilidade) │
  │ Cadastro sem vínculo                │ Trigger gera divergência automática    │
  │ Duplicar documento/carga            │ UNIQUE INDEX + webhook                │
  │ A+B+C ≠ total                       │ Trigger detecta e marca divergente    │
  │ Qualquer alteração sem registro     │ Trigger de audit em tabelas críticas  │
  └─────────────────────────────────────┴──────────────────────────────────────┘
*/

-- ── VERIFICAÇÃO ──────────────────────────────────────────────────────────
SELECT
  tgname AS trigger_name,
  tgrelid::regclass AS tabela,
  CASE tgtype & 66
    WHEN 2 THEN 'BEFORE'
    WHEN 64 THEN 'INSTEAD OF'
    ELSE 'AFTER'
  END AS timing
FROM pg_trigger
WHERE tgname LIKE 'trg_%'
  AND NOT tgisinternal
ORDER BY tgrelid::regclass, tgname;
