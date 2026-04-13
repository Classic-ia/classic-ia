-- ════════════════════════════════════════════════════════════════════════════
-- RLS TABELAS NOVAS — Row Level Security para tabelas sensíveis sem RLS
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Resolve gap critico de seguranca: 11 tabelas com dados sensiveis estavam
-- sem RLS habilitado. Policies baseadas em rh_perfil_atual().
--
-- Principio: dados sensiveis (salario, documentos medicos, restricoes)
-- acessiveis apenas a perfis autorizados. Nenhuma tabela fica aberta.
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. rh_ferias (dados de ferias) ──────────────────────────────────────
ALTER TABLE rh_ferias ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ferias_read" ON rh_ferias;
CREATE POLICY "ferias_read" ON rh_ferias FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','diretoria','financeiro'));
DROP POLICY IF EXISTS "ferias_write" ON rh_ferias;
CREATE POLICY "ferias_write" ON rh_ferias FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 2. rh_beneficio (valores de beneficios) ─────────────────────────────
ALTER TABLE rh_beneficio ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "beneficio_read" ON rh_beneficio;
CREATE POLICY "beneficio_read" ON rh_beneficio FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca','diretoria','financeiro'));
DROP POLICY IF EXISTS "beneficio_write" ON rh_beneficio;
CREATE POLICY "beneficio_write" ON rh_beneficio FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 3. rh_custo_colaborador (salarios e custos — CRITICO) ───────────────
ALTER TABLE rh_custo_colaborador ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "custo_read" ON rh_custo_colaborador;
CREATE POLICY "custo_read" ON rh_custo_colaborador FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca','diretoria','financeiro'));
DROP POLICY IF EXISTS "custo_write" ON rh_custo_colaborador;
CREATE POLICY "custo_write" ON rh_custo_colaborador FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 4. rh_ponto (registro de ponto) ─────────────────────────────────────
ALTER TABLE rh_ponto ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ponto_read" ON rh_ponto;
CREATE POLICY "ponto_read" ON rh_ponto FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','diretoria'));
DROP POLICY IF EXISTS "ponto_write" ON rh_ponto;
CREATE POLICY "ponto_write" ON rh_ponto FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 5. rh_acompanhamento (registros confidenciais gestor-funcionario) ───
ALTER TABLE rh_acompanhamento ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "acomp_read" ON rh_acompanhamento;
CREATE POLICY "acomp_read" ON rh_acompanhamento FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'));
DROP POLICY IF EXISTS "acomp_write" ON rh_acompanhamento;
CREATE POLICY "acomp_write" ON rh_acompanhamento FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'));

-- ── 6. rh_documento_ocr (atestados com CID medico — CRITICO) ────────────
ALTER TABLE rh_documento_ocr ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "doc_ocr_read" ON rh_documento_ocr;
CREATE POLICY "doc_ocr_read" ON rh_documento_ocr FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh'));
DROP POLICY IF EXISTS "doc_ocr_write" ON rh_documento_ocr;
CREATE POLICY "doc_ocr_write" ON rh_documento_ocr FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 7. rh_documento_digital (documentos pessoais) ───────────────────────
ALTER TABLE rh_documento_digital ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "doc_dig_read" ON rh_documento_digital;
CREATE POLICY "doc_dig_read" ON rh_documento_digital FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'));
DROP POLICY IF EXISTS "doc_dig_write" ON rh_documento_digital;
CREATE POLICY "doc_dig_write" ON rh_documento_digital FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 8. rh_proc_desligamento (entrevista demissional) ────────────────────
ALTER TABLE rh_proc_desligamento ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "proc_deslig_read" ON rh_proc_desligamento;
CREATE POLICY "proc_deslig_read" ON rh_proc_desligamento FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','diretoria'));
DROP POLICY IF EXISTS "proc_deslig_write" ON rh_proc_desligamento;
CREATE POLICY "proc_deslig_write" ON rh_proc_desligamento FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 9. rh_notificacao (notificacoes internas) ───────────────────────────
ALTER TABLE rh_notificacao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "notif_read" ON rh_notificacao;
CREATE POLICY "notif_read" ON rh_notificacao FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','sst','diretoria'));
DROP POLICY IF EXISTS "notif_write" ON rh_notificacao;
CREATE POLICY "notif_write" ON rh_notificacao FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 10. sst_restricao (restricoes medicas — CRITICO) ────────────────────
ALTER TABLE sst_restricao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "restricao_read" ON sst_restricao;
CREATE POLICY "restricao_read" ON sst_restricao FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','sst','psicologa'));
DROP POLICY IF EXISTS "restricao_write" ON sst_restricao;
CREATE POLICY "restricao_write" ON sst_restricao FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh','sst'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','sst'));

-- ── 11. motor_score (scores individuais) ─────────────────────────────────
ALTER TABLE motor_score ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "score_read" ON motor_score;
CREATE POLICY "score_read" ON motor_score FOR SELECT
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','diretoria'));
DROP POLICY IF EXISTS "score_write" ON motor_score;
CREATE POLICY "score_write" ON motor_score FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));
