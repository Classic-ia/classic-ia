-- ══════════════════════════════════════════════════════════════════
-- FASE 1 — LIMPEZA DE POLICIES LEGADAS (role {public})
-- Executar APÓS fase1_hardening_rls.sql
-- ══════════════════════════════════════════════════════════════════
-- Este script remove TODAS as policies que não são as criadas pelo
-- hardening. As policies legadas usam role {public} (anon key) e
-- anulam o controle de acesso implementado.
-- ══════════════════════════════════════════════════════════════════

-- ──────────────────────────────────────────────────────────────────
-- PASSO 1 — Listar policies atuais (diagnóstico pré-limpeza)
-- ──────────────────────────────────────────────────────────────────
-- SELECT tablename, policyname, roles, cmd FROM pg_policies
-- WHERE schemaname = 'public' ORDER BY tablename, policyname;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 2 — Remover TODAS as policies de TODAS as tabelas
-- (abordagem nuclear: limpar tudo e recriar apenas as corretas)
-- ──────────────────────────────────────────────────────────────────

-- registros_cq_inspecao — remover policies legadas {public}
DROP POLICY IF EXISTS "edicao_gestor" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "edicao_proprio" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insercao_analisador" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "leitura_analisador" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "leitura_gestores" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insp_all" ON registros_cq_inspecao;

-- cq_usuarios — remover quaisquer policies legadas
DROP POLICY IF EXISTS "usuarios_all" ON cq_usuarios;
DROP POLICY IF EXISTS "leitura_usuarios" ON cq_usuarios;
DROP POLICY IF EXISTS "edicao_usuarios" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_select" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_insert" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_update" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_delete" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_public" ON cq_usuarios;

-- cq_sessoes — remover quaisquer policies legadas
DROP POLICY IF EXISTS "sess_all" ON cq_sessoes;
DROP POLICY IF EXISTS "cq_sess_proprio" ON cq_sessoes;
DROP POLICY IF EXISTS "sess_public" ON cq_sessoes;
DROP POLICY IF EXISTS "leitura_sessoes" ON cq_sessoes;

-- cq_audit_log — remover quaisquer policies legadas
DROP POLICY IF EXISTS "audit_all" ON cq_audit_log;
DROP POLICY IF EXISTS "cq_audit_insert" ON cq_audit_log;
DROP POLICY IF EXISTS "cq_audit_select" ON cq_audit_log;
DROP POLICY IF EXISTS "audit_public" ON cq_audit_log;

-- cadastros_atak
DROP POLICY IF EXISTS "atak_all" ON cadastros_atak;
DROP POLICY IF EXISTS "atak_public" ON cadastros_atak;
DROP POLICY IF EXISTS "leitura_atak" ON cadastros_atak;

-- cq_tipos_defeito
DROP POLICY IF EXISTS "def_all" ON cq_tipos_defeito;
DROP POLICY IF EXISTS "defeito_public" ON cq_tipos_defeito;
DROP POLICY IF EXISTS "leitura_defeitos" ON cq_tipos_defeito;

-- cq_produtos
DROP POLICY IF EXISTS "prod_all" ON cq_produtos;
DROP POLICY IF EXISTS "produtos_public" ON cq_produtos;
DROP POLICY IF EXISTS "leitura_produtos" ON cq_produtos;

-- cq_recebimentos
DROP POLICY IF EXISTS "rec_all" ON cq_recebimentos;
DROP POLICY IF EXISTS "rec_public" ON cq_recebimentos;
DROP POLICY IF EXISTS "leitura_recebimentos" ON cq_recebimentos;

-- cq_planos_acao
DROP POLICY IF EXISTS "plano_all" ON cq_planos_acao;
DROP POLICY IF EXISTS "plano_public" ON cq_planos_acao;
DROP POLICY IF EXISTS "leitura_planos" ON cq_planos_acao;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 3 — Remover TODAS as policies restantes que usam {public}
-- (captura qualquer policy com nome não previsto acima)
-- ──────────────────────────────────────────────────────────────────
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT schemaname, tablename, policyname
    FROM pg_policies
    WHERE schemaname = 'public'
      AND roles = '{public}'
      AND tablename IN (
        'cq_usuarios','cq_sessoes','cq_audit_log','registros_cq_inspecao',
        'cq_recebimentos','cq_planos_acao','cadastros_atak',
        'cq_tipos_defeito','cq_produtos'
      )
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I', r.policyname, r.schemaname, r.tablename);
    RAISE NOTICE 'Removida policy legada: % em %', r.policyname, r.tablename;
  END LOOP;
END;
$$;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 4 — Garantir que as policies do hardening existem
-- (recria se alguma foi removida acidentalmente)
-- ──────────────────────────────────────────────────────────────────

-- cq_usuarios
DROP POLICY IF EXISTS "usuarios_select_proprio" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_insert_gestor" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_update_gestor" ON cq_usuarios;
DROP POLICY IF EXISTS "usuarios_delete_gestor" ON cq_usuarios;

CREATE POLICY "usuarios_select_proprio"
  ON cq_usuarios FOR SELECT TO authenticated
  USING (auth_id = auth.uid() OR cq_is_gestor());

CREATE POLICY "usuarios_insert_gestor"
  ON cq_usuarios FOR INSERT TO authenticated
  WITH CHECK (cq_is_gestor());

CREATE POLICY "usuarios_update_gestor"
  ON cq_usuarios FOR UPDATE TO authenticated
  USING (cq_is_gestor()) WITH CHECK (cq_is_gestor());

CREATE POLICY "usuarios_delete_gestor"
  ON cq_usuarios FOR DELETE TO authenticated
  USING (cq_is_gestor());

-- cq_sessoes
DROP POLICY IF EXISTS "sess_select" ON cq_sessoes;
DROP POLICY IF EXISTS "sess_insert" ON cq_sessoes;
DROP POLICY IF EXISTS "sess_update" ON cq_sessoes;

CREATE POLICY "sess_select"
  ON cq_sessoes FOR SELECT TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

CREATE POLICY "sess_insert"
  ON cq_sessoes FOR INSERT TO authenticated
  WITH CHECK (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
  );

CREATE POLICY "sess_update"
  ON cq_sessoes FOR UPDATE TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

-- cq_audit_log
DROP POLICY IF EXISTS "audit_insert" ON cq_audit_log;
DROP POLICY IF EXISTS "audit_select" ON cq_audit_log;

CREATE POLICY "audit_insert"
  ON cq_audit_log FOR INSERT TO authenticated
  WITH CHECK (TRUE);

CREATE POLICY "audit_select"
  ON cq_audit_log FOR SELECT TO authenticated
  USING (
    usuario_id = (SELECT id FROM cq_usuarios WHERE auth_id = auth.uid() LIMIT 1)
    OR cq_is_gestor()
  );

-- registros_cq_inspecao
DROP POLICY IF EXISTS "insp_select" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insp_insert" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insp_update" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insp_delete" ON registros_cq_inspecao;

CREATE POLICY "insp_select"
  ON registros_cq_inspecao FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "insp_insert"
  ON registros_cq_inspecao FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('gestor','analisador','conferente','qualidade_externo'));

CREATE POLICY "insp_update"
  ON registros_cq_inspecao FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('gestor','analisador'));

CREATE POLICY "insp_delete"
  ON registros_cq_inspecao FOR DELETE TO authenticated
  USING (cq_is_gestor());

-- cq_recebimentos
DROP POLICY IF EXISTS "rec_select" ON cq_recebimentos;
DROP POLICY IF EXISTS "rec_insert" ON cq_recebimentos;
DROP POLICY IF EXISTS "rec_update" ON cq_recebimentos;
DROP POLICY IF EXISTS "rec_delete" ON cq_recebimentos;

CREATE POLICY "rec_select"
  ON cq_recebimentos FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "rec_insert"
  ON cq_recebimentos FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('gestor','analisador','conferente'));

CREATE POLICY "rec_update"
  ON cq_recebimentos FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('gestor','analisador'));

CREATE POLICY "rec_delete"
  ON cq_recebimentos FOR DELETE TO authenticated
  USING (cq_is_gestor());

-- cq_planos_acao
DROP POLICY IF EXISTS "plano_select" ON cq_planos_acao;
DROP POLICY IF EXISTS "plano_insert" ON cq_planos_acao;
DROP POLICY IF EXISTS "plano_update" ON cq_planos_acao;
DROP POLICY IF EXISTS "plano_delete" ON cq_planos_acao;

CREATE POLICY "plano_select"
  ON cq_planos_acao FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "plano_insert"
  ON cq_planos_acao FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('gestor','analisador','qualidade_externo'));

CREATE POLICY "plano_update"
  ON cq_planos_acao FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('gestor','analisador'));

CREATE POLICY "plano_delete"
  ON cq_planos_acao FOR DELETE TO authenticated
  USING (cq_is_gestor());

-- cadastros_atak
DROP POLICY IF EXISTS "atak_select" ON cadastros_atak;
DROP POLICY IF EXISTS "atak_modify" ON cadastros_atak;

CREATE POLICY "atak_select"
  ON cadastros_atak FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "atak_modify"
  ON cadastros_atak FOR ALL TO authenticated
  USING (cq_get_perfil() IN ('gestor','analisador'))
  WITH CHECK (cq_get_perfil() IN ('gestor','analisador'));

-- cq_tipos_defeito
DROP POLICY IF EXISTS "defeito_select" ON cq_tipos_defeito;
DROP POLICY IF EXISTS "defeito_modify" ON cq_tipos_defeito;

CREATE POLICY "defeito_select"
  ON cq_tipos_defeito FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "defeito_modify"
  ON cq_tipos_defeito FOR ALL TO authenticated
  USING (cq_is_gestor()) WITH CHECK (cq_is_gestor());

-- cq_produtos
DROP POLICY IF EXISTS "prod_select" ON cq_produtos;
DROP POLICY IF EXISTS "prod_modify" ON cq_produtos;

CREATE POLICY "prod_select"
  ON cq_produtos FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "prod_modify"
  ON cq_produtos FOR ALL TO authenticated
  USING (cq_is_gestor()) WITH CHECK (cq_is_gestor());

-- ──────────────────────────────────────────────────────────────────
-- PASSO 5 — Verificação final: NENHUMA policy com {public}
-- ──────────────────────────────────────────────────────────────────
SELECT
  tablename,
  policyname,
  roles,
  cmd,
  CASE
    WHEN roles = '{authenticated}' THEN 'OK'
    ELSE 'ALERTA — role public!'
  END AS status
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename IN (
    'cq_usuarios','cq_sessoes','cq_audit_log','registros_cq_inspecao',
    'cq_recebimentos','cq_planos_acao','cadastros_atak',
    'cq_tipos_defeito','cq_produtos'
  )
ORDER BY tablename, policyname;
