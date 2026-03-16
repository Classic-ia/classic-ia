-- ══════════════════════════════════════════════════════════════════
-- FASE 2 — MIGRAÇÃO DE PERFIS (5 → 7) + TABELA DE PERMISSÕES
-- Classic CQ — Executar no Supabase SQL Editor
-- ══════════════════════════════════════════════════════════════════
-- IMPORTANTE: Execute este script COMPLETO de uma vez.
-- Pré-requisitos:
--   ✓ Fase 1 (hardening RLS) executada e validada
--   ✓ Auditoria de usuários limpa (zero órfãos)
--   ✓ 3 usuários confirmados: gestor→administrador, analisador→qualidade
-- ══════════════════════════════════════════════════════════════════

BEGIN;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 1 — Alterar CHECK constraint para aceitar os 7 novos perfis
-- ──────────────────────────────────────────────────────────────────

-- Remover constraint antigo (nome pode variar)
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT constraint_name
    FROM information_schema.check_constraints
    WHERE constraint_schema = 'public'
      AND constraint_name IN (
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.table_name = 'cq_usuarios'
          AND tc.constraint_type = 'CHECK'
      )
      AND check_clause LIKE '%perfil%'
  LOOP
    EXECUTE format('ALTER TABLE cq_usuarios DROP CONSTRAINT %I', r.constraint_name);
    RAISE NOTICE 'Removido constraint: %', r.constraint_name;
  END LOOP;
END;
$$;

-- Criar novo constraint com os 7 perfis
ALTER TABLE cq_usuarios
ADD CONSTRAINT cq_usuarios_perfil_check
CHECK (perfil IN (
  'administrador',
  'qualidade',
  'comercial',
  'financeiro',
  'logistica',
  'industrial',
  'diretoria'
));

-- ──────────────────────────────────────────────────────────────────
-- PASSO 2 — Migrar perfis existentes em cq_usuarios
-- ──────────────────────────────────────────────────────────────────

-- gestor → administrador
UPDATE cq_usuarios SET perfil = 'administrador' WHERE perfil = 'gestor';

-- analisador → qualidade
UPDATE cq_usuarios SET perfil = 'qualidade' WHERE perfil = 'analisador';

-- conferente → logistica (nenhum usuário atual, mas seguro)
UPDATE cq_usuarios SET perfil = 'logistica' WHERE perfil = 'conferente';

-- qualidade_externo → qualidade (nenhum usuário atual, mas seguro)
UPDATE cq_usuarios SET perfil = 'qualidade' WHERE perfil = 'qualidade_externo';

-- financeiro permanece financeiro (sem mudança)

-- ──────────────────────────────────────────────────────────────────
-- PASSO 3 — Atualizar metadata em auth.users
-- ──────────────────────────────────────────────────────────────────

UPDATE auth.users
SET raw_user_meta_data = raw_user_meta_data || jsonb_build_object('perfil', u.perfil)
FROM cq_usuarios u
WHERE auth.users.id = u.auth_id;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 4 — Atualizar default da coluna perfil
-- ──────────────────────────────────────────────────────────────────

ALTER TABLE cq_usuarios ALTER COLUMN perfil SET DEFAULT 'qualidade';

-- ──────────────────────────────────────────────────────────────────
-- PASSO 5 — Criar tabela cq_permissoes
-- ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cq_permissoes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  perfil TEXT NOT NULL,
  modulo TEXT NOT NULL,
  pode_visualizar BOOLEAN NOT NULL DEFAULT FALSE,
  pode_criar BOOLEAN NOT NULL DEFAULT FALSE,
  pode_editar BOOLEAN NOT NULL DEFAULT FALSE,
  pode_excluir BOOLEAN NOT NULL DEFAULT FALSE,
  criado_em TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(perfil, modulo)
);

-- Habilitar RLS
ALTER TABLE cq_permissoes ENABLE ROW LEVEL SECURITY;

-- Todos os autenticados podem ler permissões (necessário para menu)
CREATE POLICY "perm_select"
  ON cq_permissoes FOR SELECT TO authenticated
  USING (TRUE);

-- Apenas administrador pode modificar permissões
CREATE POLICY "perm_modify"
  ON cq_permissoes FOR ALL TO authenticated
  USING (cq_get_perfil() = 'administrador')
  WITH CHECK (cq_get_perfil() = 'administrador');

-- ──────────────────────────────────────────────────────────────────
-- PASSO 6 — Popular matriz de permissões
-- ──────────────────────────────────────────────────────────────────
-- Legenda dos módulos:
--   recebimento_lote, inspecao_qualidade, rastreabilidade,
--   dashboard_cq, gestao_fornecedores, importar_atak,
--   gestao_usuarios, processo_bucho, calculadora_financeira,
--   conferencia_quantidade, formulario_cq, importar_planilha,
--   lancamento_cargas, lancamento_cq_bucho, relatorio_cargas,
--   dashboard_cargas, dashboard_producao
-- ──────────────────────────────────────────────────────────────────

-- Limpar dados existentes (idempotente)
DELETE FROM cq_permissoes;

INSERT INTO cq_permissoes (perfil, modulo, pode_visualizar, pode_criar, pode_editar, pode_excluir) VALUES

-- ═══ ADMINISTRADOR (ex-gestor) — acesso total ═══
('administrador', 'recebimento_lote',       TRUE, TRUE, TRUE, TRUE),
('administrador', 'inspecao_qualidade',      TRUE, TRUE, TRUE, TRUE),
('administrador', 'rastreabilidade',         TRUE, TRUE, TRUE, TRUE),
('administrador', 'dashboard_cq',            TRUE, TRUE, TRUE, TRUE),
('administrador', 'gestao_fornecedores',     TRUE, TRUE, TRUE, TRUE),
('administrador', 'importar_atak',           TRUE, TRUE, TRUE, TRUE),
('administrador', 'gestao_usuarios',         TRUE, TRUE, TRUE, TRUE),
('administrador', 'processo_bucho',          TRUE, TRUE, TRUE, TRUE),
('administrador', 'calculadora_financeira',  TRUE, TRUE, TRUE, TRUE),
('administrador', 'conferencia_quantidade',  TRUE, TRUE, TRUE, TRUE),
('administrador', 'formulario_cq',           TRUE, TRUE, TRUE, TRUE),
('administrador', 'importar_planilha',       TRUE, TRUE, TRUE, TRUE),
('administrador', 'lancamento_cargas',       TRUE, TRUE, TRUE, TRUE),
('administrador', 'lancamento_cq_bucho',     TRUE, TRUE, TRUE, TRUE),
('administrador', 'relatorio_cargas',        TRUE, TRUE, TRUE, TRUE),
('administrador', 'dashboard_cargas',        TRUE, TRUE, TRUE, TRUE),
('administrador', 'dashboard_producao',      TRUE, TRUE, TRUE, TRUE),

-- ═══ QUALIDADE (ex-analisador + ex-qualidade_externo) ═══
('qualidade', 'recebimento_lote',       TRUE, TRUE, TRUE, FALSE),
('qualidade', 'inspecao_qualidade',      TRUE, TRUE, TRUE, FALSE),
('qualidade', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('qualidade', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('qualidade', 'gestao_fornecedores',     TRUE, TRUE, TRUE, FALSE),
('qualidade', 'importar_atak',           TRUE, TRUE, TRUE, FALSE),
('qualidade', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('qualidade', 'processo_bucho',          TRUE, TRUE, TRUE, FALSE),
('qualidade', 'calculadora_financeira',  FALSE, FALSE, FALSE, FALSE),
('qualidade', 'conferencia_quantidade',  FALSE, FALSE, FALSE, FALSE),
('qualidade', 'formulario_cq',           TRUE, TRUE, TRUE, FALSE),
('qualidade', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('qualidade', 'lancamento_cargas',       TRUE, TRUE, TRUE, FALSE),
('qualidade', 'lancamento_cq_bucho',     TRUE, TRUE, TRUE, FALSE),
('qualidade', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('qualidade', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('qualidade', 'dashboard_producao',      TRUE, FALSE, FALSE, FALSE),

-- ═══ COMERCIAL (novo perfil) ═══
('comercial', 'recebimento_lote',       FALSE, FALSE, FALSE, FALSE),
('comercial', 'inspecao_qualidade',      FALSE, FALSE, FALSE, FALSE),
('comercial', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('comercial', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('comercial', 'gestao_fornecedores',     TRUE, FALSE, FALSE, FALSE),
('comercial', 'importar_atak',           FALSE, FALSE, FALSE, FALSE),
('comercial', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('comercial', 'processo_bucho',          FALSE, FALSE, FALSE, FALSE),
('comercial', 'calculadora_financeira',  FALSE, FALSE, FALSE, FALSE),
('comercial', 'conferencia_quantidade',  FALSE, FALSE, FALSE, FALSE),
('comercial', 'formulario_cq',           FALSE, FALSE, FALSE, FALSE),
('comercial', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('comercial', 'lancamento_cargas',       FALSE, FALSE, FALSE, FALSE),
('comercial', 'lancamento_cq_bucho',     FALSE, FALSE, FALSE, FALSE),
('comercial', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('comercial', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('comercial', 'dashboard_producao',      FALSE, FALSE, FALSE, FALSE),

-- ═══ FINANCEIRO (mantido) ═══
('financeiro', 'recebimento_lote',       FALSE, FALSE, FALSE, FALSE),
('financeiro', 'inspecao_qualidade',      FALSE, FALSE, FALSE, FALSE),
('financeiro', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('financeiro', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('financeiro', 'gestao_fornecedores',     TRUE, FALSE, FALSE, FALSE),
('financeiro', 'importar_atak',           FALSE, FALSE, FALSE, FALSE),
('financeiro', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('financeiro', 'processo_bucho',          FALSE, FALSE, FALSE, FALSE),
('financeiro', 'calculadora_financeira',  TRUE, TRUE, TRUE, FALSE),
('financeiro', 'conferencia_quantidade',  FALSE, FALSE, FALSE, FALSE),
('financeiro', 'formulario_cq',           FALSE, FALSE, FALSE, FALSE),
('financeiro', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('financeiro', 'lancamento_cargas',       FALSE, FALSE, FALSE, FALSE),
('financeiro', 'lancamento_cq_bucho',     FALSE, FALSE, FALSE, FALSE),
('financeiro', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('financeiro', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('financeiro', 'dashboard_producao',      FALSE, FALSE, FALSE, FALSE),

-- ═══ LOGISTICA (ex-conferente) ═══
('logistica', 'recebimento_lote',       TRUE, TRUE, TRUE, FALSE),
('logistica', 'inspecao_qualidade',      FALSE, FALSE, FALSE, FALSE),
('logistica', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('logistica', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('logistica', 'gestao_fornecedores',     FALSE, FALSE, FALSE, FALSE),
('logistica', 'importar_atak',           FALSE, FALSE, FALSE, FALSE),
('logistica', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('logistica', 'processo_bucho',          TRUE, TRUE, TRUE, FALSE),
('logistica', 'calculadora_financeira',  FALSE, FALSE, FALSE, FALSE),
('logistica', 'conferencia_quantidade',  TRUE, TRUE, TRUE, FALSE),
('logistica', 'formulario_cq',           FALSE, FALSE, FALSE, FALSE),
('logistica', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('logistica', 'lancamento_cargas',       FALSE, FALSE, FALSE, FALSE),
('logistica', 'lancamento_cq_bucho',     FALSE, FALSE, FALSE, FALSE),
('logistica', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('logistica', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('logistica', 'dashboard_producao',      TRUE, FALSE, FALSE, FALSE),

-- ═══ INDUSTRIAL (novo perfil) ═══
('industrial', 'recebimento_lote',       TRUE, FALSE, FALSE, FALSE),
('industrial', 'inspecao_qualidade',      TRUE, FALSE, FALSE, FALSE),
('industrial', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('industrial', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('industrial', 'gestao_fornecedores',     FALSE, FALSE, FALSE, FALSE),
('industrial', 'importar_atak',           FALSE, FALSE, FALSE, FALSE),
('industrial', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('industrial', 'processo_bucho',          TRUE, TRUE, TRUE, FALSE),
('industrial', 'calculadora_financeira',  FALSE, FALSE, FALSE, FALSE),
('industrial', 'conferencia_quantidade',  TRUE, TRUE, TRUE, FALSE),
('industrial', 'formulario_cq',           FALSE, FALSE, FALSE, FALSE),
('industrial', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('industrial', 'lancamento_cargas',       FALSE, FALSE, FALSE, FALSE),
('industrial', 'lancamento_cq_bucho',     TRUE, TRUE, TRUE, FALSE),
('industrial', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('industrial', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('industrial', 'dashboard_producao',      TRUE, TRUE, TRUE, FALSE),

-- ═══ DIRETORIA (novo perfil — visualização ampla, sem operação) ═══
('diretoria', 'recebimento_lote',       TRUE, FALSE, FALSE, FALSE),
('diretoria', 'inspecao_qualidade',      TRUE, FALSE, FALSE, FALSE),
('diretoria', 'rastreabilidade',         TRUE, FALSE, FALSE, FALSE),
('diretoria', 'dashboard_cq',            TRUE, FALSE, FALSE, FALSE),
('diretoria', 'gestao_fornecedores',     TRUE, FALSE, FALSE, FALSE),
('diretoria', 'importar_atak',           FALSE, FALSE, FALSE, FALSE),
('diretoria', 'gestao_usuarios',         FALSE, FALSE, FALSE, FALSE),
('diretoria', 'processo_bucho',          TRUE, FALSE, FALSE, FALSE),
('diretoria', 'calculadora_financeira',  TRUE, FALSE, FALSE, FALSE),
('diretoria', 'conferencia_quantidade',  TRUE, FALSE, FALSE, FALSE),
('diretoria', 'formulario_cq',           TRUE, FALSE, FALSE, FALSE),
('diretoria', 'importar_planilha',       FALSE, FALSE, FALSE, FALSE),
('diretoria', 'lancamento_cargas',       TRUE, FALSE, FALSE, FALSE),
('diretoria', 'lancamento_cq_bucho',     TRUE, FALSE, FALSE, FALSE),
('diretoria', 'relatorio_cargas',        TRUE, FALSE, FALSE, FALSE),
('diretoria', 'dashboard_cargas',        TRUE, FALSE, FALSE, FALSE),
('diretoria', 'dashboard_producao',      TRUE, FALSE, FALSE, FALSE);

-- ──────────────────────────────────────────────────────────────────
-- PASSO 7 — Atualizar funções auxiliares de RLS
-- ──────────────────────────────────────────────────────────────────

-- cq_get_perfil() — sem mudança (retorna perfil do banco, já atualizado)

-- cq_is_gestor() → agora verifica 'administrador'
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
      AND perfil = 'administrador'
  );
$$;

-- cq_meu_perfil() — sem mudança (retorna dados do banco, já atualizados)

-- Nova função: verificar permissão por módulo/ação
CREATE OR REPLACE FUNCTION public.cq_tem_permissao(p_modulo TEXT, p_acao TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.cq_permissoes p
    WHERE p.perfil = (
      SELECT perfil FROM public.cq_usuarios
      WHERE auth_id = auth.uid() AND ativo = TRUE LIMIT 1
    )
    AND p.modulo = p_modulo
    AND CASE p_acao
      WHEN 'visualizar' THEN p.pode_visualizar
      WHEN 'criar' THEN p.pode_criar
      WHEN 'editar' THEN p.pode_editar
      WHEN 'excluir' THEN p.pode_excluir
      ELSE FALSE
    END = TRUE
  );
$$;

-- ──────────────────────────────────────────────────────────────────
-- PASSO 8 — Atualizar RLS policies com novos nomes de perfil
-- ──────────────────────────────────────────────────────────────────

-- ━━━━━ registros_cq_inspecao ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DROP POLICY IF EXISTS "insp_insert" ON registros_cq_inspecao;
DROP POLICY IF EXISTS "insp_update" ON registros_cq_inspecao;

CREATE POLICY "insp_insert"
  ON registros_cq_inspecao FOR INSERT TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('administrador','qualidade','logistica')
  );

CREATE POLICY "insp_update"
  ON registros_cq_inspecao FOR UPDATE TO authenticated
  USING (
    cq_get_perfil() IN ('administrador','qualidade')
  );

-- insp_select e insp_delete usam cq_get_perfil() IS NOT NULL e cq_is_gestor()
-- Ambas funções já foram atualizadas — não precisam ser recriadas

-- ━━━━━ cq_recebimentos ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DROP POLICY IF EXISTS "rec_insert" ON cq_recebimentos;
DROP POLICY IF EXISTS "rec_update" ON cq_recebimentos;

CREATE POLICY "rec_insert"
  ON cq_recebimentos FOR INSERT TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('administrador','qualidade','logistica')
  );

CREATE POLICY "rec_update"
  ON cq_recebimentos FOR UPDATE TO authenticated
  USING (
    cq_get_perfil() IN ('administrador','qualidade')
  );

-- rec_select e rec_delete: sem mudança (usam cq_get_perfil() IS NOT NULL e cq_is_gestor())

-- ━━━━━ cq_planos_acao ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DROP POLICY IF EXISTS "plano_insert" ON cq_planos_acao;
DROP POLICY IF EXISTS "plano_update" ON cq_planos_acao;

CREATE POLICY "plano_insert"
  ON cq_planos_acao FOR INSERT TO authenticated
  WITH CHECK (
    cq_get_perfil() IN ('administrador','qualidade')
  );

CREATE POLICY "plano_update"
  ON cq_planos_acao FOR UPDATE TO authenticated
  USING (
    cq_get_perfil() IN ('administrador','qualidade')
  );

-- plano_select e plano_delete: sem mudança

-- ━━━━━ cadastros_atak ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DROP POLICY IF EXISTS "atak_modify" ON cadastros_atak;

CREATE POLICY "atak_modify"
  ON cadastros_atak FOR ALL TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade'))
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade'));

-- atak_select: sem mudança

-- ━━━━━ cq_tipos_defeito e cq_produtos ━━━━━━━━━━━━━━━━━━━━━━━━━
-- Usam cq_is_gestor() que já verifica 'administrador' — sem mudança

-- ━━━━━ cq_usuarios ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Usam cq_is_gestor() e auth_id = auth.uid() — sem mudança

-- ━━━━━ cq_sessoes e cq_audit_log ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Baseadas em usuario_id — sem referência a nomes de perfil

COMMIT;

-- ──────────────────────────────────────────────────────────────────
-- VERIFICAÇÕES PÓS-MIGRAÇÃO (executar separadamente)
-- ──────────────────────────────────────────────────────────────────

-- V1: Confirmar perfis migrados em cq_usuarios
SELECT id, email, nome, perfil, ativo FROM cq_usuarios ORDER BY email;

-- V2: Confirmar metadata atualizado em auth.users
SELECT email, raw_user_meta_data->>'perfil' AS perfil_metadata
FROM auth.users ORDER BY email;

-- V3: Confirmar constraint atualizado
SELECT constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_name LIKE '%cq_usuarios%perfil%';

-- V4: Confirmar tabela cq_permissoes populada
SELECT perfil, COUNT(*) AS modulos,
  SUM(CASE WHEN pode_visualizar THEN 1 ELSE 0 END) AS visualizar,
  SUM(CASE WHEN pode_criar THEN 1 ELSE 0 END) AS criar,
  SUM(CASE WHEN pode_editar THEN 1 ELSE 0 END) AS editar,
  SUM(CASE WHEN pode_excluir THEN 1 ELSE 0 END) AS excluir
FROM cq_permissoes
GROUP BY perfil ORDER BY perfil;

-- V5: Confirmar policies atualizadas
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public'
ORDER BY tablename, policyname;
