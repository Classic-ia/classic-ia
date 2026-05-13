-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — RLS (Row Level Security)
-- Politicas baseadas em rh_perfil_atual() (mesmo padrao do resto do RH)
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- PRINCIPIO:
--   - Dados sensiveis de C&S (salario, banda, ficha, parecer) sao restritos
--     a perfis com necessidade legitima.
--   - Diretoria + Comite (administrador) tem acesso total.
--   - RH operacional executa CRUD de cadastro.
--   - Gestor/supervisor le sua equipe direta (na Fase 2, via funcao
--     auxiliar de subordinacao; aqui leitura ampla para o perfil 'gestor_confianca').
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. rh_cs_familias (catalogo nao-sensivel — leitura ampla) ───────────────
ALTER TABLE rh_cs_familias ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_fam_read" ON rh_cs_familias;
CREATE POLICY "cs_fam_read" ON rh_cs_familias FOR SELECT
  USING (TRUE);
DROP POLICY IF EXISTS "cs_fam_write" ON rh_cs_familias;
CREATE POLICY "cs_fam_write" ON rh_cs_familias FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 2. rh_cs_cargo_meta (catalogo — leitura ampla) ──────────────────────────
ALTER TABLE rh_cs_cargo_meta ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_meta_read" ON rh_cs_cargo_meta;
CREATE POLICY "cs_meta_read" ON rh_cs_cargo_meta FOR SELECT
  USING (TRUE);
DROP POLICY IF EXISTS "cs_meta_write" ON rh_cs_cargo_meta;
CREATE POLICY "cs_meta_write" ON rh_cs_cargo_meta FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 3. rh_cs_bandas (SENSIVEL — salarios) ───────────────────────────────────
ALTER TABLE rh_cs_bandas ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_banda_read" ON rh_cs_bandas;
CREATE POLICY "cs_banda_read" ON rh_cs_bandas FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria','financeiro'
  ));
DROP POLICY IF EXISTS "cs_banda_write" ON rh_cs_bandas;
CREATE POLICY "cs_banda_write" ON rh_cs_bandas FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 4. rh_cs_colaborador (dados de cargo/nivel/fiducia) ─────────────────────
ALTER TABLE rh_cs_colaborador ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_colab_read" ON rh_cs_colaborador;
CREATE POLICY "cs_colab_read" ON rh_cs_colaborador FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca','diretoria','financeiro'
  ));
DROP POLICY IF EXISTS "cs_colab_write" ON rh_cs_colaborador;
CREATE POLICY "cs_colab_write" ON rh_cs_colaborador FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 5. rh_cs_historico_salarial (CRITICO) ───────────────────────────────────
ALTER TABLE rh_cs_historico_salarial ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_hist_read" ON rh_cs_historico_salarial;
CREATE POLICY "cs_hist_read" ON rh_cs_historico_salarial FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria','financeiro'
  ));
DROP POLICY IF EXISTS "cs_hist_write" ON rh_cs_historico_salarial;
CREATE POLICY "cs_hist_write" ON rh_cs_historico_salarial FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 6. rh_cs_documentos (dossie) ────────────────────────────────────────────
ALTER TABLE rh_cs_documentos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_doc_read" ON rh_cs_documentos;
CREATE POLICY "cs_doc_read" ON rh_cs_documentos FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_doc_write" ON rh_cs_documentos;
CREATE POLICY "cs_doc_write" ON rh_cs_documentos FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 7. rh_cs_fichas_avaliacao (notas individuais — sensivel) ───────────────
ALTER TABLE rh_cs_fichas_avaliacao ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_ficha_read" ON rh_cs_fichas_avaliacao;
CREATE POLICY "cs_ficha_read" ON rh_cs_fichas_avaliacao FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_ficha_write" ON rh_cs_fichas_avaliacao;
CREATE POLICY "cs_ficha_write" ON rh_cs_fichas_avaliacao FOR ALL
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca'
  ))
  WITH CHECK (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca'
  ));

-- ── 8. rh_cs_progressoes (workflow) ─────────────────────────────────────────
ALTER TABLE rh_cs_progressoes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_prog_read" ON rh_cs_progressoes;
CREATE POLICY "cs_prog_read" ON rh_cs_progressoes FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_prog_write" ON rh_cs_progressoes;
CREATE POLICY "cs_prog_write" ON rh_cs_progressoes FOR ALL
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca','diretoria'
  ))
  WITH CHECK (rh_perfil_atual() IN (
    'administrador','rh','gestor','gestor_confianca','diretoria'
  ));

-- ── 9. rh_cs_excecoes (governanca — restrito) ───────────────────────────────
ALTER TABLE rh_cs_excecoes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_exc_read" ON rh_cs_excecoes;
CREATE POLICY "cs_exc_read" ON rh_cs_excecoes FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_exc_write" ON rh_cs_excecoes;
CREATE POLICY "cs_exc_write" ON rh_cs_excecoes FOR ALL
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ))
  WITH CHECK (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));

-- ── 10. rh_cs_reunioes_comite ───────────────────────────────────────────────
ALTER TABLE rh_cs_reunioes_comite ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_reuniao_read" ON rh_cs_reunioes_comite;
CREATE POLICY "cs_reuniao_read" ON rh_cs_reunioes_comite FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_reuniao_write" ON rh_cs_reunioes_comite;
CREATE POLICY "cs_reuniao_write" ON rh_cs_reunioes_comite FOR ALL
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ))
  WITH CHECK (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));

-- ── 11. rh_cs_relatorios_fiducia ───────────────────────────────────────────
ALTER TABLE rh_cs_relatorios_fiducia ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_rel_fid_read" ON rh_cs_relatorios_fiducia;
CREATE POLICY "cs_rel_fid_read" ON rh_cs_relatorios_fiducia FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_rel_fid_write" ON rh_cs_relatorios_fiducia;
CREATE POLICY "cs_rel_fid_write" ON rh_cs_relatorios_fiducia FOR ALL
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ))
  WITH CHECK (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));

-- ── 12. rh_cs_beneficios (catalogo) ────────────────────────────────────────
ALTER TABLE rh_cs_beneficios ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_ben_read" ON rh_cs_beneficios;
CREATE POLICY "cs_ben_read" ON rh_cs_beneficios FOR SELECT
  USING (TRUE);
DROP POLICY IF EXISTS "cs_ben_write" ON rh_cs_beneficios;
CREATE POLICY "cs_ben_write" ON rh_cs_beneficios FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 13. rh_cs_elegibilidade ────────────────────────────────────────────────
ALTER TABLE rh_cs_elegibilidade ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_eleg_read" ON rh_cs_elegibilidade;
CREATE POLICY "cs_eleg_read" ON rh_cs_elegibilidade FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria','financeiro'
  ));
DROP POLICY IF EXISTS "cs_eleg_write" ON rh_cs_elegibilidade;
CREATE POLICY "cs_eleg_write" ON rh_cs_elegibilidade FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 14. rh_cs_historico_beneficios ─────────────────────────────────────────
ALTER TABLE rh_cs_historico_beneficios ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_hist_ben_read" ON rh_cs_historico_beneficios;
CREATE POLICY "cs_hist_ben_read" ON rh_cs_historico_beneficios FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria','financeiro'
  ));
DROP POLICY IF EXISTS "cs_hist_ben_write" ON rh_cs_historico_beneficios;
CREATE POLICY "cs_hist_ben_write" ON rh_cs_historico_beneficios FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- ── 15. rh_cs_alertas ──────────────────────────────────────────────────────
ALTER TABLE rh_cs_alertas ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "cs_alerta_read" ON rh_cs_alertas;
CREATE POLICY "cs_alerta_read" ON rh_cs_alertas FOR SELECT
  USING (rh_perfil_atual() IN (
    'administrador','rh','gestor_confianca','diretoria'
  ));
DROP POLICY IF EXISTS "cs_alerta_write" ON rh_cs_alertas;
CREATE POLICY "cs_alerta_write" ON rh_cs_alertas FOR ALL
  USING (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','gestor_confianca'));

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_02_RLS.sql
-- 15 tabelas com RLS habilitado | 30 policies | 0 tabela aberta
-- ════════════════════════════════════════════════════════════════════════════
