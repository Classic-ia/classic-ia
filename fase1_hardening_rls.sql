-- ══════════════════════════════════════════════════════════════════
-- FASE 1 — HARDENING DE RLS + CONTROLE DE ACESSO REAL
-- Classic CQ — Executar no Supabase SQL Editor
-- ══════════════════════════════════════════════════════════════════
-- IMPORTANTE: Execute este script COMPLETO de uma vez.
-- Ele remove todas as policies USING(TRUE) e substitui por
-- policies baseadas em auth.uid() + perfil real do banco.
-- ══════════════════════════════════════════════════════════════════

-- ──────────────────────────────────────────────────────────────────
-- PASSO 0 — Garantir coluna auth_id preenchida em cq_usuarios
-- ──────────────────────────────────────────────────────────────────
-- Vincular usuários existentes pelo email (caso auth_id esteja NULL)
UPDATE cq_usuarios u
SET auth_id = a.id
FROM auth.users a
WHERE u.email = a.email
  AND u.auth_id IS NULL;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 1 — Função auxiliar: retorna perfil do usuário autenticado
-- ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.cq_get_perfil()
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT perfil
  FROM public.cq_usuarios
  WHERE auth_id = auth.uid()
    AND ativo = TRUE
  LIMIT 1;
$$;

-- Função auxiliar: verifica se é admin (gestor)
CREATE OR REPLACE FUNCTION public.cq_is_gestor()
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.cq_usuarios
    WHERE auth_id = auth.uid()
      AND ativo = TRUE
      AND perfil = 'gestor'
  );
$$;

-- Função auxiliar: retorna auth_id do usuário autenticado na cq_usuarios
CREATE OR REPLACE FUNCTION public.cq_user_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT id
  FROM public.cq_usuarios
  WHERE auth_id = auth.uid()
    AND ativo = TRUE
  LIMIT 1;
$$;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 2 — REMOVER TODAS AS POLICIES PERMISSIVAS
-- ──────────────────────────────────────────────────────────────────

-- registros_cq_inspecao
DROP POLICY IF EXISTS "insp_all" ON registros_cq_inspecao;

-- cq_sessoes
DROP POLICY IF EXISTS "sess_all" ON cq_sessoes;
DROP POLICY IF EXISTS "cq_sess_proprio" ON cq_sessoes;

-- cq_audit_log
DROP POLICY IF EXISTS "audit_all" ON cq_audit_log;
DROP POLICY IF EXISTS "cq_audit_insert" ON cq_audit_log;
DROP POLICY IF EXISTS "cq_audit_select" ON cq_audit_log;

-- cq_usuarios
DROP POLICY IF EXISTS "usuarios_all" ON cq_usuarios;

-- cadastros_atak
DROP POLICY IF EXISTS "atak_all" ON cadastros_atak;

-- cq_tipos_defeito
DROP POLICY IF EXISTS "def_all" ON cq_tipos_defeito;

-- cq_produtos
DROP POLICY IF EXISTS "prod_all" ON cq_produtos;

-- cq_recebimentos
DROP POLICY IF EXISTS "rec_all" ON cq_recebimentos;

-- cq_planos_acao
DROP POLICY IF EXISTS "plano_all" ON cq_planos_acao;

-- registros_carga (só executa se a tabela existir)
-- DROP POLICY IF EXISTS "acesso total registros_carga" ON registros_carga;
-- NOTA: tabela registros_carga não existe neste banco. Ignorada.

-- ──────────────────────────────────────────────────────────────────
-- PASSO 3 — NOVAS POLICIES BASEADAS EM auth.uid()
-- ──────────────────────────────────────────────────────────────────

-- ━━━━━ cq_usuarios ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Qualquer autenticado pode ler seus próprios dados
-- Gestor pode ler e modificar todos
CREATE POLICY "usuarios_select_proprio"
  ON cq_usuarios FOR SELECT
  TO authenticated
  USING (auth_id = auth.uid() OR cq_is_gestor());

CREATE POLICY "usuarios_insert_gestor"
  ON cq_usuarios FOR INSERT
  TO authenticated
  WITH CHECK (cq_is_gestor());

CREATE POLICY "usuarios_update_gestor"
  ON cq_usuarios FOR UPDATE
  TO authenticated
  USING (cq_is_gestor())
  WITH CHECK (cq_is_gestor());

CREATE POLICY "usuarios_delete_gestor"
  ON cq_usuarios FOR DELETE
  TO authenticated
  USING (cq_is_gestor());

-- ━━━━━ cq_sessoes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Cada usuário só vê/gerencia suas próprias sessões
-- Gestor pode ver todas (para auditoria)
CREATE POLICY "sess_select"
  ON cq_sessoes FOR SELECT
  TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

CREATE POLICY "sess_insert"
  ON cq_sessoes FOR INSERT
  TO authenticated
  WITH CHECK (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
  );

CREATE POLICY "sess_update"
  ON cq_sessoes FOR UPDATE
  TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

-- ━━━━━ cq_audit_log ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Qualquer autenticado pode inserir (registrar ações)
-- Apenas gestor pode ler o log completo
-- Demais podem ler apenas seus próprios registros
CREATE POLICY "audit_insert"
  ON cq_audit_log FOR INSERT
  TO authenticated
  WITH CHECK (TRUE);

CREATE POLICY "audit_select"
  ON cq_audit_log FOR SELECT
  TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

-- Audit log é imutável — sem UPDATE/DELETE
-- (nenhuma policy de update/delete = bloqueado)

-- ━━━━━ registros_cq_inspecao ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Leitura: qualquer autenticado com perfil ativo
-- Escrita: perfis operacionais (gestor, analisador, conferente, qualidade_externo)
-- Delete: apenas gestor
CREATE POLICY "insp_select"
  ON registros_cq_inspecao FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "insp_insert"
  ON registros_cq_inspecao FOR INSERT
  TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('gestor','analisador','conferente','qualidade_externo')
  );

CREATE POLICY "insp_update"
  ON registros_cq_inspecao FOR UPDATE
  TO authenticated
  USING (
    cq_get_perfil() IN ('gestor','analisador')
  );

CREATE POLICY "insp_delete"
  ON registros_cq_inspecao FOR DELETE
  TO authenticated
  USING (cq_is_gestor());

-- ━━━━━ cq_recebimentos ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Leitura: qualquer autenticado ativo
-- Escrita: operacionais
-- Delete: gestor
CREATE POLICY "rec_select"
  ON cq_recebimentos FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "rec_insert"
  ON cq_recebimentos FOR INSERT
  TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('gestor','analisador','conferente')
  );

CREATE POLICY "rec_update"
  ON cq_recebimentos FOR UPDATE
  TO authenticated
  USING (
    cq_get_perfil() IN ('gestor','analisador')
  );

CREATE POLICY "rec_delete"
  ON cq_recebimentos FOR DELETE
  TO authenticated
  USING (cq_is_gestor());

-- ━━━━━ cq_planos_acao ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Leitura: qualquer autenticado ativo
-- Escrita: gestor, analisador, qualidade_externo
CREATE POLICY "plano_select"
  ON cq_planos_acao FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "plano_insert"
  ON cq_planos_acao FOR INSERT
  TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('gestor','analisador','qualidade_externo')
  );

CREATE POLICY "plano_update"
  ON cq_planos_acao FOR UPDATE
  TO authenticated
  USING (
    cq_get_perfil() IN ('gestor','analisador')
  );

CREATE POLICY "plano_delete"
  ON cq_planos_acao FOR DELETE
  TO authenticated
  USING (cq_is_gestor());

-- ━━━━━ registros_carga ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- NOTA: tabela registros_carga NÃO existe neste banco.
-- Se for criada futuramente, aplicar as policies abaixo:
-- CREATE POLICY "carga_select" ON registros_carga FOR SELECT TO authenticated USING (cq_get_perfil() IS NOT NULL);
-- CREATE POLICY "carga_insert" ON registros_carga FOR INSERT TO authenticated WITH CHECK (cq_get_perfil() IN ('gestor','analisador','conferente'));
-- CREATE POLICY "carga_update" ON registros_carga FOR UPDATE TO authenticated USING (cq_get_perfil() IN ('gestor','analisador'));
-- CREATE POLICY "carga_delete" ON registros_carga FOR DELETE TO authenticated USING (cq_is_gestor());

-- ━━━━━ cadastros_atak (catálogo — somente leitura para maioria) ━━━━━
CREATE POLICY "atak_select"
  ON cadastros_atak FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "atak_modify"
  ON cadastros_atak FOR ALL
  TO authenticated
  USING (cq_get_perfil() IN ('gestor','analisador'))
  WITH CHECK (cq_get_perfil() IN ('gestor','analisador'));

-- ━━━━━ cq_tipos_defeito (catálogo — somente leitura para maioria) ━━━
CREATE POLICY "defeito_select"
  ON cq_tipos_defeito FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "defeito_modify"
  ON cq_tipos_defeito FOR ALL
  TO authenticated
  USING (cq_is_gestor())
  WITH CHECK (cq_is_gestor());

-- ━━━━━ cq_produtos (catálogo — somente leitura para maioria) ━━━━━━━
CREATE POLICY "prod_select"
  ON cq_produtos FOR SELECT
  TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "prod_modify"
  ON cq_produtos FOR ALL
  TO authenticated
  USING (cq_is_gestor())
  WITH CHECK (cq_is_gestor());

-- ──────────────────────────────────────────────────────────────────
-- PASSO 4 — REVOGAR ACESSO ANON (CRUCIAL)
-- ──────────────────────────────────────────────────────────────────
-- As tabelas operacionais NÃO devem ter policies para anon.
-- Apenas a role 'authenticated' tem acesso via RLS acima.
-- Isso significa que o frontend DEVE usar o JWT do Supabase Auth
-- (access_token do login) em vez da anon key para requests REST.

-- ──────────────────────────────────────────────────────────────────
-- PASSO 5 — FUNÇÃO RPC: buscar perfil do usuário autenticado
-- (usada pelo frontend para revalidar perfil do banco)
-- ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.cq_meu_perfil()
RETURNS TABLE(id UUID, email TEXT, nome TEXT, perfil TEXT, ativo BOOLEAN)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT u.id, u.email, u.nome, u.perfil, u.ativo
  FROM public.cq_usuarios u
  WHERE u.auth_id = auth.uid()
    AND u.ativo = TRUE
  LIMIT 1;
$$;

-- ──────────────────────────────────────────────────────────────────
-- VERIFICAÇÃO: Listar todas as policies ativas
-- ──────────────────────────────────────────────────────────────────
SELECT
  schemaname,
  tablename,
  policyname,
  permissive,
  roles,
  cmd,
  qual AS using_clause,
  with_check
FROM pg_policies
WHERE schemaname = 'public'
ORDER BY tablename, policyname;
