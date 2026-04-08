-- ============================================================================
-- migration_rls_rh_sst.sql
-- Correções de RLS para o banco RH/SST — baseado em auditoria funcional real
--
-- CONTEXTO:
--   O banco JÁ possui RLS implementado com funções helper:
--     - rh_perfil_atual()        → retorna perfil do usuario logado
--     - rh_perfil_sensivel()     → true se admin/rh
--     - rh_funcionario_atual_id()→ id do funcionario vinculado ao usuario
--     - rh_setor_do_gestor()     → setor_id do gestor (QUEBRADA — ver fix abaixo)
--     - rh_meu_perfil()          → RPC para frontend
--
--   Tabela rh_usuarios usa coluna auth_uid (não auth_id).
--   Tabelas SST são prefixadas sst_ (não rh_).
--   Perfis válidos: administrador, rh, gestor, gestor_confianca,
--                   visualizador, qualidade, financeiro, diretoria
--
-- BUGS ENCONTRADOS NA AUDITORIA:
--   1. CRÍTICO: rh_setor_do_gestor() referencia rh_usuarios.funcionario_id
--      que NÃO EXISTE → crash para perfis gestor/gestor_confianca
--   2. ALTO: gestor_confianca (Paulo) não tem match em rh_funcionarios
--      via email → rh_funcionario_atual_id() retorna NULL
--   3. MÉDIO: 35 tabelas usam auth.role()='authenticated' para SELECT
--      sem checar perfil — qualquer autenticado (incluindo qualidade) lê tudo
--   4. BAIXO: sst_treinamento e sst_epi_entrega não têm policy DELETE
--
-- ESTE ARQUIVO CORRIGE APENAS OS BUGS. NÃO REESCREVE POLICIES EXISTENTES.
-- ============================================================================

BEGIN;

-- ============================================================================
-- FIX 1 (CRÍTICO): Adicionar coluna funcionario_id em rh_usuarios
-- e corrigir rh_setor_do_gestor()
-- ============================================================================

-- 1a. Adicionar coluna que a função espera
ALTER TABLE rh_usuarios
  ADD COLUMN IF NOT EXISTS funcionario_id UUID REFERENCES rh_funcionarios(id);

-- 1b. Popular automaticamente a partir do match por email
UPDATE rh_usuarios u
SET funcionario_id = f.id
FROM rh_funcionarios f
WHERE lower(f.email_pessoal) = lower(u.email)
  AND f.status != 'desligado'
  AND u.funcionario_id IS NULL;

-- 1c. Recriar a função com tratamento de NULL
CREATE OR REPLACE FUNCTION public.rh_setor_do_gestor()
RETURNS UUID
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public
AS $$
DECLARE v_setor UUID;
BEGIN
  SELECT f.setor_id INTO v_setor
  FROM rh_usuarios u
  JOIN rh_funcionarios f ON f.id = u.funcionario_id
  WHERE u.auth_uid = auth.uid()
    AND u.ativo = TRUE
    AND f.status != 'desligado';
  RETURN v_setor;  -- retorna NULL se não encontrar (gestor vê 0 rows — seguro)
END;
$$;

COMMENT ON FUNCTION public.rh_setor_do_gestor() IS
  'Retorna setor_id do funcionario vinculado ao usuario logado. NULL se não vinculado.';


-- ============================================================================
-- FIX 2 (ALTO): Vincular gestor_confianca ao funcionario correto
-- Paulo não tem match por email — precisa vincular manualmente
-- ============================================================================

-- Verificar se Paulo tem funcionario_id preenchido após o UPDATE acima
-- Se não, o admin precisa preencher manualmente via:
--   UPDATE rh_usuarios SET funcionario_id = '<uuid-do-funcionario>'
--   WHERE email = 'pauloalberton.classiccouros@hotmail.com';
--
-- Não fazemos match forçado aqui porque não sabemos qual é o registro correto.


-- ============================================================================
-- FIX 3 (MÉDIO): Endurecer policies de tabelas com dados sensíveis
-- que usam apenas auth.role()='authenticated' para SELECT
-- ============================================================================

-- 3a. sst_treinamento — restringir SELECT a perfis RH/SST/gestor
DROP POLICY IF EXISTS trein_select ON sst_treinamento;
CREATE POLICY trein_select ON sst_treinamento
  FOR SELECT TO authenticated
  USING (
    rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','visualizador','diretoria')
  );

-- 3b. sst_epi_entrega — restringir SELECT
DROP POLICY IF EXISTS epi_ent_select ON sst_epi_entrega;
CREATE POLICY epi_ent_select ON sst_epi_entrega
  FOR SELECT TO authenticated
  USING (
    rh_perfil_atual() IN ('administrador','rh','gestor','gestor_confianca','visualizador','diretoria')
  );

-- 3c. rh_cte + componentes + log — dados financeiros de transporte
-- Restringir de "qualquer autenticado" para admin/rh/financeiro/diretoria
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

-- 3d. Audit log SELECT — incluir 'rh' (atualmente apenas admin)
DROP POLICY IF EXISTS audit_select ON rh_audit_log;
CREATE POLICY audit_select ON rh_audit_log
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','diretoria'));


-- ============================================================================
-- FIX 4 (BAIXO): Adicionar DELETE policies ausentes em tabelas SST
-- ============================================================================

-- sst_treinamento (não tinha DELETE policy)
CREATE POLICY trein_delete ON sst_treinamento
  FOR DELETE TO authenticated
  USING (rh_perfil_atual() = 'administrador');

-- sst_epi_entrega (não tinha DELETE policy)
CREATE POLICY epi_ent_delete ON sst_epi_entrega
  FOR DELETE TO authenticated
  USING (rh_perfil_atual() = 'administrador');


COMMIT;
