-- ============================================================================
-- migration_rls_rh_sst.sql
-- Correções cirúrgicas de RLS — aplicadas e validadas no banco real
-- Projeto: muiqmtnfvyffborgiwdw (RH Classic)
-- Data: 2026-04-08
--
-- CONTEXTO:
--   O banco JÁ possui RLS implementado com funções helper:
--     rh_perfil_atual()         → perfil do usuario logado (SECURITY DEFINER)
--     rh_perfil_sensivel()      → true se admin/rh
--     rh_funcionario_atual_id() → func.id via match email
--     rh_setor_do_gestor()      → setor_id via match email (CORRIGIDO AQUI)
--     rh_meu_perfil()           → RPC para frontend
--
--   Tabela rh_usuarios: coluna auth_uid (não auth_id)
--   Tabelas SST: prefixo sst_ (não rh_)
--   Perfis: administrador, rh, gestor, gestor_confianca, visualizador,
--           qualidade, financeiro, diretoria
--
-- STATUS: APLICADO E VALIDADO — este arquivo documenta o que foi feito
-- ============================================================================


-- ============================================================================
-- FIX 1 (CRÍTICO): rh_setor_do_gestor() — remover referência a coluna inexistente
--
-- ANTES: JOIN rh_funcionarios f ON f.id = u.funcionario_id  ← CRASH (coluna não existe)
-- DEPOIS: JOIN via email (mesmo padrão de rh_funcionario_atual_id())
-- ============================================================================

CREATE OR REPLACE FUNCTION public.rh_setor_do_gestor()
RETURNS UUID
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE v_setor UUID;
BEGIN
  SELECT f.setor_id INTO v_setor
  FROM rh_usuarios u
  JOIN rh_funcionarios f ON lower(f.email_pessoal) = lower(u.email)
  WHERE u.auth_uid = auth.uid()
    AND u.ativo = TRUE
    AND f.status != 'desligado'
  LIMIT 1;
  RETURN v_setor;  -- NULL se não encontrar = gestor vê 0 rows (fail closed)
END;
$$;


-- ============================================================================
-- FIX 2 (CRÍTICO): rh_perfil_atual() — fail closed
--
-- ANTES: COALESCE(v_perfil, 'visualizador') — desconhecido vira visualizador
-- DEPOIS: retorna '_sem_acesso' — nenhuma policy aceita esse valor
-- ============================================================================

-- NOTA IMPORTANTE (2026-04-09):
-- Todas as funções SECURITY DEFINER devem usar:
--   COALESCE(auth.uid(), NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid)
-- em vez de apenas auth.uid() ou apenas GUC.
-- Motivo: auth.uid() funciona via PostgREST (frontend real), GUC funciona via service role (testes).
-- COALESCE garante que ambos os contextos funcionam.

CREATE OR REPLACE FUNCTION public.rh_perfil_atual()
RETURNS TEXT
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE v_perfil TEXT;
BEGIN
  SELECT perfil INTO v_perfil
  FROM rh_usuarios
  WHERE auth_uid = auth.uid()
    AND ativo = TRUE
  LIMIT 1;
  RETURN COALESCE(v_perfil, '_sem_acesso');
END;
$$;


-- ============================================================================
-- FIX 3 (ALTO): Vincular gestor_confianca (Paulo) ao funcionário
--
-- Paulo existia em rh_funcionarios mas email_pessoal era NULL.
-- Preenchido para que rh_funcionario_atual_id() e rh_setor_do_gestor() funcionem.
-- Resultado: Paulo → setor "Produção"
-- ============================================================================

UPDATE rh_funcionarios
SET email_pessoal = 'pauloalberton.classiccouros@hotmail.com'
WHERE id = 'd8875fcb-3be9-4964-a499-545c1f1be218'
  AND (email_pessoal IS NULL OR email_pessoal = '');


-- ============================================================================
-- FIX 4 (MÉDIO): Endurecer SELECT aberto em tabelas com dados operacionais
--
-- De: auth.role() = 'authenticated' (qualquer autenticado, incluindo qualidade)
-- Para: rh_perfil_atual() IN (...) com perfis explícitos
--
-- NÃO tocamos em catálogos/referência (cargos, setores, empresas, filiais,
-- catálogo EPI, tipos de treinamento, etc.) — esses continuam abertos.
-- ============================================================================

-- 4a. SST operacional
DROP POLICY IF EXISTS trein_select ON sst_treinamento;
CREATE POLICY trein_select ON sst_treinamento
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','visualizador','diretoria'));

DROP POLICY IF EXISTS epi_ent_select ON sst_epi_entrega;
CREATE POLICY epi_ent_select ON sst_epi_entrega
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','visualizador','diretoria'));

-- 4b. Financeiro / CT-e
DROP POLICY IF EXISTS rh_cte_all ON rh_cte;
CREATE POLICY rh_cte_select ON rh_cte
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));
CREATE POLICY rh_cte_write ON rh_cte
  FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS rh_cte_comp_all ON rh_cte_componente;
CREATE POLICY rh_cte_comp_select ON rh_cte_componente
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));
CREATE POLICY rh_cte_comp_write ON rh_cte_componente
  FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS rh_cte_log_all ON rh_cte_importacao_log;
CREATE POLICY rh_cte_log_select ON rh_cte_importacao_log
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));
CREATE POLICY rh_cte_log_write ON rh_cte_importacao_log
  FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS rh_custo_all ON rh_custo_colaborador;
CREATE POLICY rh_custo_select ON rh_custo_colaborador
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));
CREATE POLICY rh_custo_write ON rh_custo_colaborador
  FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

-- 4c. Custos transporte
DROP POLICY IF EXISTS custos_transporte_select ON rh_custos_transporte;
CREATE POLICY custos_transporte_select ON rh_custos_transporte
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

DROP POLICY IF EXISTS transporte_func_select ON rh_custos_transporte_funcionarios;
CREATE POLICY transporte_func_select ON rh_custos_transporte_funcionarios
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

DROP POLICY IF EXISTS custos_transporte_rateio_select ON rh_custos_transporte_rateio;
CREATE POLICY custos_transporte_rateio_select ON rh_custos_transporte_rateio
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

-- 4d. Faturas SST
DROP POLICY IF EXISTS sst_faturas_select ON sst_faturas_servicos;
CREATE POLICY sst_faturas_select ON sst_faturas_servicos
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

DROP POLICY IF EXISTS sst_resumo_select ON sst_fatura_resumo_procedimentos;
CREATE POLICY sst_resumo_select ON sst_fatura_resumo_procedimentos
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

DROP POLICY IF EXISTS sst_historico_select ON sst_fatura_historico_colaborador;
CREATE POLICY sst_historico_select ON sst_fatura_historico_colaborador
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','financeiro','diretoria'));

-- 4e. Audit log — expandir leitura para rh e diretoria
DROP POLICY IF EXISTS audit_select ON rh_audit_log;
CREATE POLICY audit_select ON rh_audit_log
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','diretoria'));


-- ============================================================================
-- TABELAS QUE MANTÊM SELECT ABERTO (DECISÃO DOCUMENTADA)
--
-- Todas são dados de referência/catálogo sem dados pessoais:
--   rh_cargos (39 rows)          — dropdown de cargos
--   rh_setores (8 rows)          — dropdown de setores
--   rh_empresas (1 row)          — empresa única
--   rh_filiais (1 row)           — filial única
--   rh_centros_custo (0 rows)    — referência contábil
--   rh_niveis_hierarquicos (10)  — organograma
--   rh_tipo_documento (0 rows)   — catálogo de docs
--   sst_tipo_treinamento (7)     — NRs
--   sst_catalogo_epi (552)       — catálogo de EPIs com CA
--   sst_cargo_epi (0 rows)       — matriz cargo×EPI
--   sst_cargo_treinamento (0)    — matriz cargo×treinamento
--   fp_rubrica (26 rows)         — rubricas da folha
--   rh_hist_cargo (627)          — histórico movimentação
--   rh_hist_setor (633)          — histórico setores
--   rh_hist_gestor (0 rows)      — histórico gestores
--   rh_proc_* (0 rows cada)     — processos workflow (não populado)
--
-- Nenhuma contém: CPF, salário, dados bancários, saúde, CID,
-- atestados, ou qualquer dado sensível/pessoal.
--
-- WRITE nestas tabelas JÁ está restrito a admin/rh via policies existentes.
-- ============================================================================
