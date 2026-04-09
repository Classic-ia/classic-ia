-- ============================================================================
-- migration_filial_triunfo.sql
-- Inclusão da filial Triunfo + infraestrutura multi-filial
-- STATUS: APLICADO no banco em 2026-04-09
-- ============================================================================

-- 1. FILIAL TRIUNFO
INSERT INTO rh_filiais (razao_social, cnpj, nome_fantasia, empresa_id, endereco, ativo)
VALUES (
  'CLASSIC IMPORTACAO E EXPORTACAO DE COUROS LTDA',
  '08849964000200',
  'Classic Couros - Triunfo',
  (SELECT id FROM rh_empresas LIMIT 1),
  '{"logradouro":"Est. TF10","numero":"31150","bairro":"Rincão dos Pinheiros","cidade":"Triunfo","uf":"RS","cep":"95840-000"}'::jsonb,
  true
);

-- 2. SETORES PARA TRIUNFO (cópia dos setores da Matriz)
INSERT INTO rh_setores (nome, filial_id, ativo)
SELECT nome, (SELECT id FROM rh_filiais WHERE cnpj = '08849964000200'), true
FROM rh_setores WHERE filial_id = (SELECT id FROM rh_filiais WHERE cnpj = '08849964000110');

-- 3. ATUALIZAR filiais_acesso DOS USUARIOS
-- Admin e RH: acesso a todas as filiais
UPDATE rh_usuarios SET filiais_acesso = (SELECT array_agg(id) FROM rh_filiais WHERE ativo = true)
WHERE perfil IN ('administrador','rh');

-- Gestor: apenas Matriz por padrão
UPDATE rh_usuarios SET filiais_acesso = ARRAY[(SELECT id FROM rh_filiais WHERE cnpj = '08849964000110')]
WHERE perfil NOT IN ('administrador','rh') AND filiais_acesso IS NULL;

-- 4. HELPER FUNCTIONS
CREATE OR REPLACE FUNCTION public.rh_filiais_do_usuario()
RETURNS UUID[]
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $$
  SELECT COALESCE(
    (SELECT filiais_acesso FROM rh_usuarios
     WHERE auth_uid = (NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid)
     AND ativo = true LIMIT 1),
    CASE WHEN (SELECT perfil FROM rh_usuarios WHERE auth_uid = (NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid) AND ativo = true LIMIT 1) = 'administrador'
      THEN (SELECT array_agg(id) FROM rh_filiais WHERE ativo = true)
      ELSE ARRAY[]::uuid[]
    END
  );
$$;

CREATE OR REPLACE FUNCTION public.rh_listar_filiais()
RETURNS TABLE(id uuid, nome_fantasia text, cnpj text, cidade text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $$
  SELECT f.id, f.nome_fantasia, f.cnpj, f.endereco->>'cidade' as cidade
  FROM rh_filiais f
  WHERE f.ativo = true AND f.id = ANY(rh_filiais_do_usuario())
  ORDER BY f.nome_fantasia;
$$;

-- 5. UPDATE rh_meu_perfil to use GUC (consistent with other SECURITY DEFINER functions)
CREATE OR REPLACE FUNCTION public.rh_meu_perfil()
RETURNS SETOF rh_usuarios
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $$
  SELECT * FROM rh_usuarios
  WHERE auth_uid = (NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid)
    AND ativo = TRUE
  LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.rh_filiais_do_usuario() TO authenticated;
GRANT EXECUTE ON FUNCTION public.rh_listar_filiais() TO authenticated;

-- ============================================================================
-- ANÁLISE: TABELAS COM/SEM filial_id
--
-- TÊM filial_id (filtro direto):
--   rh_funcionarios ✓ (empresa_id + filial_id)
--   rh_setores ✓ (filial_id)
--   rh_custos_transporte ✓ (empresa_id + filial_id)
--   rh_proc_admissao ✓ (empresa_id + filial_id)
--
-- NÃO TÊM filial_id (filtro via JOIN funcionario):
--   sst_aso → funcionario_id → rh_funcionarios.filial_id
--   sst_treinamento → funcionario_id → rh_funcionarios.filial_id
--   sst_epi_entrega → funcionario_id → rh_funcionarios.filial_id
--   sst_acidente → funcionario_id → rh_funcionarios.filial_id
--   sst_restricao → funcionario_id → rh_funcionarios.filial_id
--   rh_ocorrencias → colaborador_id → rh_funcionarios.filial_id
--   rh_processo → funcionario_id → rh_funcionarios.filial_id
--   rh_ferias → funcionario_id → rh_funcionarios.filial_id
--   rh_ponto → funcionario_id → rh_funcionarios.filial_id
--   rh_beneficio → funcionario_id → rh_funcionarios.filial_id
--
-- CATÁLOGO (compartilhado entre filiais, sem filtro):
--   sst_catalogo_epi, sst_tipo_treinamento, rh_cargos, fp_rubrica
--
-- DECISÃO: Não adicionar filial_id em tabelas SST/operacionais.
-- O filtro é feito via JOIN com rh_funcionarios.filial_id no frontend/RPC.
-- Isso evita migração de dados e mantém integridade referencial simples.
-- ============================================================================
