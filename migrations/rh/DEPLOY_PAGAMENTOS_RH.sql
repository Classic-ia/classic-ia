-- ============================================================================
-- DEPLOY_PAGAMENTOS_RH.sql
-- Sprint 1 (P0) do PLANO_CORRECOES_SIGA.md — item 2.4 (F-41 D-01/D-04)
--
-- CONTEXTO:
--   exportar_pagamentos.html referencia rh_dados_bancarios,
--   rh_lotes_pagamento e rh_log_exportacao. As DDLs existem no repo
--   (schema_rh_sst_completo.sql, secoes 11-13) mas NUNCA foram aplicadas em
--   producao (DRIFT_MAP) — a tela degrada silenciosamente (try/catch -> [])
--   e nada persiste ("tela fantasma"). Alem disso, as tabelas nao tinham
--   NENHUMA RLS, e dados bancarios sao dado sensivel (LGPD).
--
-- DECISAO (refinada em relacao ao plano original):
--   NAO reapontar a tela para fp_lote_pagamento agora — o lote desta tela e
--   um agrupador leve de listas de exportacao (sem valores), enquanto
--   fp_lote_pagamento exige lote_folha_id de folha processada; forcar o
--   mapeamento criaria semantica errada. A consolidacao com a familia fp_*
--   fica para a Fase 3 do PLANO_MIGRACAO_SIGA (quando pagamentos ganharem
--   valores e remessa bancaria). Aqui: deploy das 3 tabelas + RLS RBAC.
--
-- O QUE ESTE SCRIPT FAZ (script 25 da ORDEM_REBUILD):
--   1. CREATE IF NOT EXISTS das 3 tabelas (DDL identica ao repo — em
--      rebuild e no-op; em prod cria as ausentes)
--   2. RLS RBAC (padrao cipa_acoes):
--      - rh_dados_bancarios: SELECT e escrita SO administrador/rh (LGPD —
--        dados bancarios nao sao para qualquer autenticado)
--      - rh_lotes_pagamento: SELECT autenticado; escrita administrador/rh
--      - rh_log_exportacao: SELECT administrador/rh; INSERT autenticado
--        (qualquer usuario da tela loga a propria exportacao)
--   3. Trigger updated_at (funcao resiliente rh_set_updated_at)
--
-- Idempotente. Nenhuma mudanca de frontend necessaria — a tela ja aponta
-- para estas tabelas.
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 1. TABELAS (espelho do schema_rh_sst_completo.sql, secoes 11-13)
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_dados_bancarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  forma_pagamento VARCHAR(30) NOT NULL CHECK (forma_pagamento IN ('conta_itau','conta_sicredi','pix','cheque')),
  banco TEXT,
  agencia TEXT,
  conta TEXT,
  tipo_conta VARCHAR(20),
  titular TEXT,
  cpf_titular TEXT,
  tipo_chave_pix VARCHAR(20),
  chave_pix TEXT,
  titular_pix TEXT,
  motivo_cheque TEXT,
  autoriza_cheque TEXT,
  status_validacao VARCHAR(20) DEFAULT 'pendente' CHECK (status_validacao IN ('pendente','validado','bloqueado')),
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_banco_func ON rh_dados_bancarios(funcionario_id);

CREATE TABLE IF NOT EXISTS rh_lotes_pagamento (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  empresa TEXT,
  periodo VARCHAR(7),
  forma_pagamento TEXT,
  total_funcionarios INTEGER DEFAULT 0,
  status VARCHAR(20) DEFAULT 'rascunho' CHECK (status IN ('rascunho','em_conferencia','pronto','enviado','finalizado')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rh_log_exportacao (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID,
  usuario_nome TEXT,
  tipo_lista TEXT,
  formato TEXT,
  total_registros INTEGER,
  filtros TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ════════════════════════════════════════════════════════════════════════
-- 2. RLS RBAC
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE rh_dados_bancarios ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_lotes_pagamento ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_log_exportacao  ENABLE ROW LEVEL SECURITY;

-- limpar policies pre-existentes (defensivo — nomes desconhecidos em prod)
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT schemaname, tablename, policyname FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename IN ('rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao')
  LOOP
    EXECUTE format('DROP POLICY %I ON %I.%I', r.policyname, r.schemaname, r.tablename);
  END LOOP;
END $$;

-- rh_dados_bancarios: tudo restrito a administrador/rh (LGPD)
CREATE POLICY p_dados_banc_select ON rh_dados_bancarios
  FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

CREATE POLICY p_dados_banc_write ON rh_dados_bancarios
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

CREATE POLICY p_dados_banc_update ON rh_dados_bancarios
  FOR UPDATE TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

-- rh_lotes_pagamento: leitura autenticado; escrita administrador/rh
CREATE POLICY p_lotes_pag_select ON rh_lotes_pagamento
  FOR SELECT TO authenticated USING (true);

CREATE POLICY p_lotes_pag_write ON rh_lotes_pagamento
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

CREATE POLICY p_lotes_pag_update ON rh_lotes_pagamento
  FOR UPDATE TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

-- rh_log_exportacao: INSERT autenticado (log da propria acao);
-- leitura restrita a administrador/rh
CREATE POLICY p_log_export_select ON rh_log_exportacao
  FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh') AND ativo = true));

CREATE POLICY p_log_export_insert ON rh_log_exportacao
  FOR INSERT TO authenticated WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE ON rh_dados_bancarios TO authenticated;
GRANT SELECT, INSERT, UPDATE ON rh_lotes_pagamento TO authenticated;
GRANT SELECT, INSERT         ON rh_log_exportacao  TO authenticated;

-- ════════════════════════════════════════════════════════════════════════
-- 3. TRIGGERS updated_at (rh_set_updated_at resiliente do script 24)
-- ════════════════════════════════════════════════════════════════════════

DROP TRIGGER IF EXISTS trg_rh_dados_bancarios_updated ON rh_dados_bancarios;
CREATE TRIGGER trg_rh_dados_bancarios_updated
  BEFORE UPDATE ON rh_dados_bancarios
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

DROP TRIGGER IF EXISTS trg_rh_lotes_pagamento_updated ON rh_lotes_pagamento;
CREATE TRIGGER trg_rh_lotes_pagamento_updated
  BEFORE UPDATE ON rh_lotes_pagamento
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

COMMENT ON TABLE rh_dados_bancarios IS
  'Dados bancarios por funcionario (LGPD — RLS restrita a administrador/rh). Deploy via DEPLOY_PAGAMENTOS_RH (Sprint 1). Consolidacao com fp_* prevista para a Fase 3 do SIGA.';
COMMENT ON TABLE rh_lotes_pagamento IS
  'Lotes de listas de pagamento (sem valores — agrupador de exportacao). Consolidacao com fp_lote_pagamento prevista para a Fase 3 do SIGA.';

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

SELECT tablename, COUNT(*) AS policies
FROM pg_policies
WHERE tablename IN ('rh_dados_bancarios','rh_lotes_pagamento','rh_log_exportacao')
GROUP BY tablename ORDER BY tablename;
