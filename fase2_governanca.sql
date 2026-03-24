-- ============================================================
-- FASE 2 — GOVERNANÇA DA QUALIDADE
-- Workflow de status, revisão, aprovação, não conformidade
-- Executar no Supabase SQL Editor como postgres
-- PRÉ-REQUISITO: fase1_complemento.sql já executado
-- ============================================================

BEGIN;

-- ════════════════════════════════════════════════════════════
-- PASSO 1: Adicionar coluna de workflow à inspeção
-- ════════════════════════════════════════════════════════════
-- status_final (aprovado/ressalva/bloqueado) = resultado técnico
-- status_workflow = estado de governança/processo
-- ════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
ADD COLUMN IF NOT EXISTS status_workflow TEXT DEFAULT 'rascunho';

-- Adicionar CHECK separadamente (para compatibilidade)
DO $$
BEGIN
  ALTER TABLE registros_cq_inspecao
  ADD CONSTRAINT inspecao_status_workflow_check
  CHECK (status_workflow IN (
    'rascunho','em_analise','aguardando_revisao',
    'revisada','aprovada','reprovada',
    'encerrada','cancelada'
  ));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Adicionar coluna de criador para regra "quem cria não aprova"
ALTER TABLE registros_cq_inspecao
ADD COLUMN IF NOT EXISTS criador_id UUID;

-- Preencher criador_id com usuario_id existente
UPDATE registros_cq_inspecao
SET criador_id = usuario_id
WHERE criador_id IS NULL AND usuario_id IS NOT NULL;

-- Marcar inspeções existentes como 'encerrada' (já finalizadas)
UPDATE registros_cq_inspecao
SET status_workflow = 'encerrada'
WHERE status_workflow = 'rascunho'
  AND status_final IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_insp_workflow ON registros_cq_inspecao(status_workflow);


-- ════════════════════════════════════════════════════════════
-- PASSO 2: Tabela cq_revisoes
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_revisoes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inspecao_id     UUID NOT NULL REFERENCES registros_cq_inspecao(id) ON DELETE CASCADE,
  revisor_id      UUID NOT NULL REFERENCES cq_usuarios(id),
  revisor_email   TEXT NOT NULL,
  revisor_nome    TEXT NOT NULL,
  decisao         TEXT NOT NULL CHECK (decisao IN ('aprovada','reprovada','devolvida')),
  comentario      TEXT,
  dados_snapshot  JSONB,    -- snapshot dos dados no momento da revisão
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rev_inspecao ON cq_revisoes(inspecao_id);
CREATE INDEX IF NOT EXISTS idx_rev_revisor ON cq_revisoes(revisor_id);
CREATE INDEX IF NOT EXISTS idx_rev_decisao ON cq_revisoes(decisao);

-- RLS
ALTER TABLE cq_revisoes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "rev_select"
  ON cq_revisoes FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "rev_insert"
  ON cq_revisoes FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','diretoria'));

-- Revisões são imutáveis (sem UPDATE/DELETE)


-- ════════════════════════════════════════════════════════════
-- PASSO 3: Tabela cq_nao_conformidades
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_nao_conformidades (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo            TEXT UNIQUE,  -- NC-AAMMDD-XXX
  inspecao_id       UUID NOT NULL REFERENCES registros_cq_inspecao(id) ON DELETE CASCADE,
  tipo              TEXT NOT NULL CHECK (tipo IN (
                      'produto','processo','fornecedor',
                      'transporte','armazenamento','outro'
                    )),
  gravidade         TEXT NOT NULL CHECK (gravidade IN ('menor','maior','critica')),
  descricao         TEXT NOT NULL,
  acao_imediata     TEXT,
  responsavel_id    UUID REFERENCES cq_usuarios(id),
  responsavel_nome  TEXT,
  status            TEXT DEFAULT 'aberta' CHECK (status IN (
                      'aberta','em_tratamento','resolvida','encerrada'
                    )),
  data_resolucao    TIMESTAMPTZ,
  usuario_criacao   TEXT,     -- email de quem criou
  criado_em         TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nc_inspecao ON cq_nao_conformidades(inspecao_id);
CREATE INDEX IF NOT EXISTS idx_nc_status ON cq_nao_conformidades(status);
CREATE INDEX IF NOT EXISTS idx_nc_gravidade ON cq_nao_conformidades(gravidade);
CREATE INDEX IF NOT EXISTS idx_nc_responsavel ON cq_nao_conformidades(responsavel_id);

-- RLS
ALTER TABLE cq_nao_conformidades ENABLE ROW LEVEL SECURITY;

CREATE POLICY "nc_select"
  ON cq_nao_conformidades FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "nc_insert"
  ON cq_nao_conformidades FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade'));

CREATE POLICY "nc_update"
  ON cq_nao_conformidades FOR UPDATE TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade'));

CREATE POLICY "nc_delete"
  ON cq_nao_conformidades FOR DELETE TO authenticated
  USING (cq_get_perfil() = 'administrador');


-- ════════════════════════════════════════════════════════════
-- PASSO 4: Alterar cq_planos_acao — vincular a não conformidades
-- ════════════════════════════════════════════════════════════

ALTER TABLE cq_planos_acao
ADD COLUMN IF NOT EXISTS nao_conformidade_id UUID REFERENCES cq_nao_conformidades(id),
ADD COLUMN IF NOT EXISTS data_limite DATE,
ADD COLUMN IF NOT EXISTS data_conclusao TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS validacao TEXT,
ADD COLUMN IF NOT EXISTS responsavel_id UUID REFERENCES cq_usuarios(id);

-- Adicionar CHECK de validação
DO $$
BEGIN
  ALTER TABLE cq_planos_acao
  ADD CONSTRAINT plano_validacao_check
  CHECK (validacao IN ('pendente','eficaz','ineficaz'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Expandir status possíveis (manter os antigos + adicionar novos)
ALTER TABLE cq_planos_acao DROP CONSTRAINT IF EXISTS cq_planos_acao_status_check;
ALTER TABLE cq_planos_acao ADD CONSTRAINT cq_planos_acao_status_check
CHECK (status IN (
  'aberto','em_execucao','concluido','ineficaz',
  'comunicado','em_tratativa','resolvido'
));

CREATE INDEX IF NOT EXISTS idx_plano_nc ON cq_planos_acao(nao_conformidade_id);
CREATE INDEX IF NOT EXISTS idx_plano_responsavel ON cq_planos_acao(responsavel_id);
CREATE INDEX IF NOT EXISTS idx_plano_validacao ON cq_planos_acao(validacao);


-- ════════════════════════════════════════════════════════════
-- PASSO 5: Função de transição de status (regras de workflow)
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_transicao_status(
  p_inspecao_id UUID,
  p_novo_status TEXT
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_status_atual TEXT;
  v_criador_id UUID;
  v_usuario_atual UUID;
  v_perfil_atual TEXT;
  v_resultado JSONB;
BEGIN
  -- Obter dados atuais
  SELECT status_workflow, criador_id
  INTO v_status_atual, v_criador_id
  FROM registros_cq_inspecao
  WHERE id = p_inspecao_id;

  IF v_status_atual IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção não encontrada');
  END IF;

  -- Obter usuário e perfil atual
  v_usuario_atual := cq_user_id();
  v_perfil_atual := cq_get_perfil();

  IF v_perfil_atual IS NULL THEN
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Usuário sem perfil ativo');
  END IF;

  -- ── Regras de transição ──

  -- rascunho → em_analise (quem criou ou administrador)
  IF v_status_atual = 'rascunho' AND p_novo_status = 'em_analise' THEN
    IF v_usuario_atual = v_criador_id OR v_perfil_atual = 'administrador' THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'em_analise' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas o criador ou administrador pode iniciar análise');
  END IF;

  -- em_analise → aguardando_revisao (quem criou ou administrador)
  IF v_status_atual = 'em_analise' AND p_novo_status = 'aguardando_revisao' THEN
    IF v_usuario_atual = v_criador_id OR v_perfil_atual = 'administrador' THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'aguardando_revisao' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas o criador ou administrador pode enviar para revisão');
  END IF;

  -- aguardando_revisao → revisada (revisor ≠ criador)
  IF v_status_atual = 'aguardando_revisao' AND p_novo_status = 'revisada' THEN
    IF v_usuario_atual != v_criador_id AND v_perfil_atual IN ('administrador','qualidade','diretoria') THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'revisada' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    IF v_usuario_atual = v_criador_id THEN
      RETURN jsonb_build_object('ok', FALSE, 'erro', 'Quem criou a inspeção não pode revisá-la');
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Perfil sem permissão para revisar');
  END IF;

  -- aguardando_revisao → reprovada (revisor ≠ criador, revisão obrigatória)
  IF v_status_atual = 'aguardando_revisao' AND p_novo_status = 'reprovada' THEN
    IF v_usuario_atual != v_criador_id AND v_perfil_atual IN ('administrador','qualidade','diretoria') THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'reprovada' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    IF v_usuario_atual = v_criador_id THEN
      RETURN jsonb_build_object('ok', FALSE, 'erro', 'Quem criou a inspeção não pode reprová-la');
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Perfil sem permissão para reprovar');
  END IF;

  -- aguardando_revisao → rascunho (devolver para correção — revisor ≠ criador)
  IF v_status_atual = 'aguardando_revisao' AND p_novo_status = 'rascunho' THEN
    IF v_usuario_atual != v_criador_id AND v_perfil_atual IN ('administrador','qualidade','diretoria') THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'rascunho' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas revisor (diferente do criador) pode devolver');
  END IF;

  -- revisada → aprovada (administrador ou diretoria, ≠ criador)
  IF v_status_atual = 'revisada' AND p_novo_status = 'aprovada' THEN
    IF v_usuario_atual != v_criador_id AND v_perfil_atual IN ('administrador','diretoria') THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'aprovada' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    IF v_usuario_atual = v_criador_id THEN
      RETURN jsonb_build_object('ok', FALSE, 'erro', 'Quem criou a inspeção não pode aprová-la');
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas administrador ou diretoria pode aprovar');
  END IF;

  -- aprovada → encerrada (administrador)
  IF v_status_atual = 'aprovada' AND p_novo_status = 'encerrada' THEN
    IF v_perfil_atual = 'administrador' THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'encerrada' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas administrador pode encerrar');
  END IF;

  -- Qualquer → cancelada (administrador)
  IF p_novo_status = 'cancelada' THEN
    IF v_perfil_atual = 'administrador' THEN
      IF v_status_atual IN ('encerrada','cancelada') THEN
        RETURN jsonb_build_object('ok', FALSE, 'erro', 'Inspeção já finalizada, não pode ser cancelada');
      END IF;
      UPDATE registros_cq_inspecao SET status_workflow = 'cancelada' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas administrador pode cancelar');
  END IF;

  -- reprovada → rascunho (reabrir para correção — administrador)
  IF v_status_atual = 'reprovada' AND p_novo_status = 'rascunho' THEN
    IF v_perfil_atual = 'administrador' THEN
      UPDATE registros_cq_inspecao SET status_workflow = 'rascunho' WHERE id = p_inspecao_id;
      RETURN jsonb_build_object('ok', TRUE, 'de', v_status_atual, 'para', p_novo_status);
    END IF;
    RETURN jsonb_build_object('ok', FALSE, 'erro', 'Apenas administrador pode reabrir inspeção reprovada');
  END IF;

  -- Transição não permitida
  RETURN jsonb_build_object(
    'ok', FALSE,
    'erro', format('Transição de %s para %s não é permitida', v_status_atual, p_novo_status)
  );
END;
$$;


-- ════════════════════════════════════════════════════════════
-- PASSO 6: Trigger de bloqueio de edição
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_bloquear_edicao_inspecao()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- Permitir mudança de status_workflow (transições controladas pela função)
  IF OLD.status_workflow IS DISTINCT FROM NEW.status_workflow
     AND OLD.* IS NOT DISTINCT FROM NEW.*  -- apenas status mudou? Na prática verificamos campos críticos
  THEN
    RETURN NEW;
  END IF;

  -- Bloquear edição de dados se inspeção está em status final
  IF OLD.status_workflow IN ('aprovada','encerrada','cancelada') THEN
    -- Permitir APENAS mudança de status_workflow por administrador
    IF OLD.status_workflow IS DISTINCT FROM NEW.status_workflow THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Inspeção com status "%" não pode ser editada. Dados bloqueados para preservar integridade.', OLD.status_workflow;
  END IF;

  -- Bloquear edição de dados se está aguardando revisão (criador não pode alterar)
  IF OLD.status_workflow = 'aguardando_revisao' THEN
    IF OLD.status_workflow IS DISTINCT FROM NEW.status_workflow THEN
      RETURN NEW; -- permitir transição de status
    END IF;
    RAISE EXCEPTION 'Inspeção aguardando revisão não pode ser editada. Solicite devolução ao revisor.';
  END IF;

  -- Em análise ou revisada: permitir edição normal
  NEW.atualizado_em := NOW();
  RETURN NEW;
END;
$$;

-- Aplicar trigger
DROP TRIGGER IF EXISTS trg_bloquear_edicao_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_bloquear_edicao_inspecao
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  EXECUTE FUNCTION cq_bloquear_edicao_inspecao();


-- ════════════════════════════════════════════════════════════
-- PASSO 7: Função auxiliar — gerar código de NC
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_gerar_codigo_nc()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_seq INT;
  v_data TEXT;
BEGIN
  v_data := TO_CHAR(NOW(), 'YYMMDD');
  SELECT COALESCE(MAX(
    CAST(SPLIT_PART(codigo, '-', 3) AS INTEGER)
  ), 0) + 1
  INTO v_seq
  FROM cq_nao_conformidades
  WHERE codigo LIKE 'NC-' || v_data || '-%';

  NEW.codigo := 'NC-' || v_data || '-' || LPAD(v_seq::TEXT, 3, '0');
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_gerar_codigo_nc ON cq_nao_conformidades;
CREATE TRIGGER trg_gerar_codigo_nc
  BEFORE INSERT ON cq_nao_conformidades
  FOR EACH ROW
  WHEN (NEW.codigo IS NULL)
  EXECUTE FUNCTION cq_gerar_codigo_nc();


-- ════════════════════════════════════════════════════════════
-- PASSO 8: Atualizar cq_permissoes — novos módulos
-- ════════════════════════════════════════════════════════════

INSERT INTO cq_permissoes (perfil, modulo, pode_visualizar, pode_criar, pode_editar, pode_excluir) VALUES
-- revisao
('administrador', 'revisao', TRUE, TRUE, FALSE, FALSE),
('qualidade',     'revisao', TRUE, TRUE, FALSE, FALSE),
('comercial',     'revisao', FALSE, FALSE, FALSE, FALSE),
('financeiro',    'revisao', FALSE, FALSE, FALSE, FALSE),
('logistica',     'revisao', FALSE, FALSE, FALSE, FALSE),
('industrial',    'revisao', FALSE, FALSE, FALSE, FALSE),
('diretoria',     'revisao', TRUE, TRUE, FALSE, FALSE),
-- nao_conformidade
('administrador', 'nao_conformidade', TRUE, TRUE, TRUE, TRUE),
('qualidade',     'nao_conformidade', TRUE, TRUE, TRUE, FALSE),
('comercial',     'nao_conformidade', FALSE, FALSE, FALSE, FALSE),
('financeiro',    'nao_conformidade', TRUE, FALSE, FALSE, FALSE),
('logistica',     'nao_conformidade', TRUE, FALSE, FALSE, FALSE),
('industrial',    'nao_conformidade', TRUE, FALSE, FALSE, FALSE),
('diretoria',     'nao_conformidade', TRUE, FALSE, FALSE, FALSE)
ON CONFLICT (perfil, modulo) DO NOTHING;


-- ════════════════════════════════════════════════════════════
-- PASSO 9: View de inspeções com workflow
-- ════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS vw_inspecoes_workflow;
CREATE VIEW vw_inspecoes_workflow AS
SELECT
  i.id,
  i.id_inspecao,
  i.data_inspecao,
  i.fornecedor,
  i.produto,
  i.lote_atak,
  i.etiqueta,
  i.classificacao,
  i.status_final,
  i.status_workflow,
  i.percentual_defeitos,
  i.total_defeitos,
  i.inspetor,
  i.usuario_id,
  u.email AS usuario_email,
  u.nome AS usuario_nome,
  i.criador_id,
  i.criado_em,
  i.atualizado_em,
  -- Última revisão
  r.decisao AS ultima_revisao_decisao,
  r.revisor_nome AS ultimo_revisor,
  r.criado_em AS data_ultima_revisao,
  r.comentario AS comentario_revisao,
  -- Não conformidades vinculadas
  (SELECT COUNT(*) FROM cq_nao_conformidades nc WHERE nc.inspecao_id = i.id) AS total_ncs,
  (SELECT COUNT(*) FROM cq_nao_conformidades nc WHERE nc.inspecao_id = i.id AND nc.status = 'aberta') AS ncs_abertas
FROM registros_cq_inspecao i
LEFT JOIN cq_usuarios u ON u.id = i.usuario_id
LEFT JOIN LATERAL (
  SELECT * FROM cq_revisoes rev
  WHERE rev.inspecao_id = i.id
  ORDER BY rev.criado_em DESC
  LIMIT 1
) r ON TRUE;

COMMIT;


-- ════════════════════════════════════════════════════════════
-- VERIFICAÇÃO (rodar após COMMIT)
-- ════════════════════════════════════════════════════════════

-- V1: Tabelas criadas
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('cq_revisoes','cq_nao_conformidades')
ORDER BY tablename;

-- V2: Coluna status_workflow existe
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'registros_cq_inspecao'
  AND column_name IN ('status_workflow','criador_id');

-- V3: Policies novas
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename IN ('cq_revisoes','cq_nao_conformidades')
ORDER BY tablename, policyname;

-- V4: Trigger existe
SELECT trigger_name, event_manipulation, action_statement
FROM information_schema.triggers
WHERE trigger_name = 'trg_bloquear_edicao_inspecao';

-- V5: Função de transição existe
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_name IN ('cq_transicao_status','cq_bloquear_edicao_inspecao','cq_gerar_codigo_nc');

-- V6: Inspeções existentes marcadas como encerrada
SELECT status_workflow, COUNT(*) AS total
FROM registros_cq_inspecao
GROUP BY status_workflow;

-- V7: Permissões atualizadas
SELECT perfil, modulo, pode_visualizar, pode_criar
FROM cq_permissoes
WHERE modulo IN ('revisao','nao_conformidade')
ORDER BY modulo, perfil;

-- V8: Colunas adicionadas a cq_planos_acao
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'cq_planos_acao'
  AND column_name IN ('nao_conformidade_id','data_limite','data_conclusao','validacao','responsavel_id');
