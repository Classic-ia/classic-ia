-- ============================================================================
-- TRIAGEM_DENUNCIAS.sql
-- Sprint 2 (P1) do PLANO_CORRECOES_SIGA.md — item 3.3 (F-46)
--
-- CONTEXTO:
--   cipa_denuncias recebia denuncias do canal publico (cipa-denuncia.html,
--   INSERT anon direto com Prefer: return=representation) mas NADA fazia a
--   triagem: nenhuma tela listava/respondia, os campos resposta/
--   respondido_por/respondido_em nunca eram preenchidos e a tabela nao
--   tinha RLS (qualquer anon podia LER todas as denuncias — vazamento do
--   canal que se promete anonimo e protegido).
--
-- O QUE ESTE SCRIPT FAZ (script 28 da ORDEM_REBUILD):
--   1. Normaliza status legados + CHECK
--      (aberta | em_analise | respondida | arquivada)
--   2. Trigger updated_at
--   3. RLS: SELECT/UPDATE somente administrador/rh/sst; anon perde TODO
--      acesso direto a tabela (INSERT publico passa a ser via RPC)
--   4. cipa_registrar_denuncia(setor, descricao, empresa_slug) —
--      SECURITY DEFINER aberta a anon: valida descricao (>= 20 chars),
--      insere e devolve o protocolo (id). Substitui o INSERT direto do
--      canal publico (que quebraria com RLS: return=representation exige
--      SELECT, que anon nao tem mais).
--   5. cipa_consultar_protocolo(protocolo) — consulta publica limitada:
--      devolve apenas status/resposta/data da denuncia daquele protocolo
--      exato (nunca a lista).
--   6. cipa_atualizar_denuncia(id, status, resposta) — triagem (guard
--      administrador/rh/sst): aberta -> em_analise -> respondida|arquivada;
--      responder exige texto e carimba respondido_por/respondido_em.
--
-- Idempotente. Frontend correspondente (mesmo commit):
--   - rh/cipa.html: nova aba "Denuncias" (triagem, admin/rh/sst)
--   - rh/cipa-denuncia.html: envio via RPC + consulta de protocolo
-- ============================================================================

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- 0. CHECK de rh_usuarios.perfil desatualizado (descoberto no E2E local)
-- ════════════════════════════════════════════════════════════════════════
-- O CHECK original (FUNDACAO_BANCO_v2/00_schema_supabase) so permite
-- administrador/rh/gestor/visualizador — mas as policies do sistema
-- referenciam tambem sst (modulos SST/CIPA, incl. esta triagem),
-- gestor_confianca, diretoria e financeiro (RLS_TABELAS_NOVAS: ferias,
-- beneficios, dados bancarios). Sem expandir o CHECK esses perfis sao
-- impossiveis de cadastrar. Expande para o conjunto realmente usado.

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = 'rh_usuarios'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%perfil%'
  LOOP
    EXECUTE format('ALTER TABLE rh_usuarios DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE rh_usuarios ADD CONSTRAINT rh_usuarios_perfil_check
  CHECK (perfil IN ('administrador','rh','gestor','visualizador','sst',
                    'gestor_confianca','diretoria','financeiro'));

-- ════════════════════════════════════════════════════════════════════════
-- 1. Normalizacao de status + CHECK
-- ════════════════════════════════════════════════════════════════════════

UPDATE cipa_denuncias SET status = CASE
    WHEN status IN ('aberto','nova','pendente')          THEN 'aberta'
    WHEN status IN ('analise','em_andamento','triagem')  THEN 'em_analise'
    WHEN status IN ('respondido','resolvida','fechada')  THEN 'respondida'
    WHEN status IN ('arquivado','descartada')            THEN 'arquivada'
    ELSE 'aberta'
  END
WHERE status IS NULL
   OR status NOT IN ('aberta','em_analise','respondida','arquivada');

ALTER TABLE cipa_denuncias ALTER COLUMN status SET DEFAULT 'aberta';
ALTER TABLE cipa_denuncias ALTER COLUMN status SET NOT NULL;
ALTER TABLE cipa_denuncias DROP CONSTRAINT IF EXISTS chk_cipa_denuncias_status;
ALTER TABLE cipa_denuncias ADD CONSTRAINT chk_cipa_denuncias_status
  CHECK (status IN ('aberta','em_analise','respondida','arquivada'));

-- ════════════════════════════════════════════════════════════════════════
-- 2. Trigger updated_at
-- ════════════════════════════════════════════════════════════════════════

DROP TRIGGER IF EXISTS trg_cipa_denuncias_updated ON cipa_denuncias;
CREATE TRIGGER trg_cipa_denuncias_updated
  BEFORE UPDATE ON cipa_denuncias
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

-- ════════════════════════════════════════════════════════════════════════
-- 3. RLS — leitura/triagem so admin/rh/sst; anon sem acesso direto
-- ════════════════════════════════════════════════════════════════════════

ALTER TABLE cipa_denuncias ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT policyname FROM pg_policies
    WHERE schemaname = 'public' AND tablename = 'cipa_denuncias'
  LOOP
    EXECUTE format('DROP POLICY %I ON public.cipa_denuncias', r.policyname);
  END LOOP;
END $$;

CREATE POLICY p_denuncias_select ON cipa_denuncias
  FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh','sst') AND ativo = true));

CREATE POLICY p_denuncias_update ON cipa_denuncias
  FOR UPDATE TO authenticated
  USING (EXISTS (SELECT 1 FROM rh_usuarios
    WHERE auth_uid = auth.uid() AND perfil IN ('administrador','rh','sst') AND ativo = true));

REVOKE ALL ON cipa_denuncias FROM anon;
GRANT SELECT, UPDATE ON cipa_denuncias TO authenticated;

-- ════════════════════════════════════════════════════════════════════════
-- 4. cipa_registrar_denuncia — canal publico (anon) via RPC
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.cipa_registrar_denuncia(
  p_setor TEXT,
  p_descricao TEXT,
  p_empresa_slug TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_id UUID;
BEGIN
  IF p_descricao IS NULL OR LENGTH(TRIM(p_descricao)) < 20 THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Descricao deve ter no minimo 20 caracteres');
  END IF;
  IF p_setor IS NULL OR TRIM(p_setor) = '' THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Setor obrigatorio');
  END IF;

  INSERT INTO cipa_denuncias (setor, descricao, empresa_slug, anonima, status)
  VALUES (TRIM(p_setor), TRIM(p_descricao), NULLIF(TRIM(COALESCE(p_empresa_slug,'')), ''), true, 'aberta')
  RETURNING id INTO v_id;

  RETURN jsonb_build_object('ok', true, 'protocolo', v_id);
END; $fn$;

REVOKE ALL ON FUNCTION public.cipa_registrar_denuncia(TEXT, TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cipa_registrar_denuncia(TEXT, TEXT, TEXT) TO anon, authenticated;

COMMENT ON FUNCTION public.cipa_registrar_denuncia(TEXT, TEXT, TEXT) IS
  'Canal publico de denuncias CIPA (cipa-denuncia.html). Unico caminho de escrita anon; devolve o protocolo (id).';

-- ════════════════════════════════════════════════════════════════════════
-- 5. cipa_consultar_protocolo — consulta publica por protocolo exato
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.cipa_consultar_protocolo(p_protocolo UUID)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_d cipa_denuncias%ROWTYPE;
BEGIN
  SELECT * INTO v_d FROM cipa_denuncias WHERE id = p_protocolo;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Protocolo nao encontrado');
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'protocolo', v_d.id,
    'status', v_d.status,
    'registrada_em', v_d.created_at,
    'resposta', CASE WHEN v_d.status = 'respondida' THEN v_d.resposta ELSE NULL END,
    'respondida_em', CASE WHEN v_d.status = 'respondida' THEN v_d.respondido_em ELSE NULL END
  );
END; $fn$;

REVOKE ALL ON FUNCTION public.cipa_consultar_protocolo(UUID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cipa_consultar_protocolo(UUID) TO anon, authenticated;

COMMENT ON FUNCTION public.cipa_consultar_protocolo(UUID) IS
  'Consulta publica limitada de denuncia por protocolo exato (status + resposta quando respondida). Nunca expoe a lista.';

-- ════════════════════════════════════════════════════════════════════════
-- 6. cipa_atualizar_denuncia — triagem (admin/rh/sst)
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.cipa_atualizar_denuncia(
  p_id UUID,
  p_status TEXT,
  p_resposta TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $fn$
DECLARE
  v_uid UUID;
  v_perfil TEXT;
  v_usuario_id UUID;
  v_d cipa_denuncias%ROWTYPE;
BEGIN
  v_uid := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  SELECT perfil, id INTO v_perfil, v_usuario_id
    FROM rh_usuarios WHERE auth_uid = v_uid AND ativo = true LIMIT 1;
  v_perfil := COALESCE(v_perfil, '_sem_acesso');
  IF v_perfil NOT IN ('administrador','rh','sst') THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Apenas perfis administrador, rh ou sst podem triar denuncias. Perfil atual: ' || v_perfil);
  END IF;

  SELECT * INTO v_d FROM cipa_denuncias WHERE id = p_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Denuncia nao encontrada');
  END IF;

  IF p_status NOT IN ('em_analise','respondida','arquivada') THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Status invalido (em_analise/respondida/arquivada)');
  END IF;
  IF v_d.status IN ('respondida','arquivada') THEN
    RETURN jsonb_build_object('ok', false,
      'error', 'Denuncia ja finalizada (status: ' || v_d.status || ')');
  END IF;
  IF p_status = 'respondida' AND (p_resposta IS NULL OR LENGTH(TRIM(p_resposta)) < 5) THEN
    RETURN jsonb_build_object('ok', false, 'error', 'Resposta obrigatoria para concluir a denuncia');
  END IF;

  UPDATE cipa_denuncias SET
    status         = p_status,
    resposta       = CASE WHEN p_status IN ('respondida','arquivada')
                          THEN COALESCE(NULLIF(TRIM(COALESCE(p_resposta,'')), ''), resposta)
                          ELSE resposta END,
    respondido_por = CASE WHEN p_status IN ('respondida','arquivada') THEN v_usuario_id ELSE respondido_por END,
    respondido_em  = CASE WHEN p_status IN ('respondida','arquivada') THEN now() ELSE respondido_em END
  WHERE id = p_id;

  RETURN jsonb_build_object('ok', true, 'denuncia_id', p_id, 'status', p_status);
END; $fn$;

REVOKE ALL ON FUNCTION public.cipa_atualizar_denuncia(UUID, TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cipa_atualizar_denuncia(UUID, TEXT, TEXT) TO authenticated;

COMMENT ON FUNCTION public.cipa_atualizar_denuncia(UUID, TEXT, TEXT) IS
  'Triagem de denuncias CIPA (guard administrador/rh/sst): aberta -> em_analise -> respondida|arquivada; carimba respondido_por/em.';

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- VERIFICACAO POS-MIGRATION
-- ════════════════════════════════════════════════════════════════════════

SELECT policyname FROM pg_policies WHERE tablename = 'cipa_denuncias' ORDER BY policyname;
SELECT proname FROM pg_proc
WHERE proname IN ('cipa_registrar_denuncia','cipa_consultar_protocolo','cipa_atualizar_denuncia');
