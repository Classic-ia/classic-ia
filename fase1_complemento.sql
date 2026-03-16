-- ============================================================
-- FASE 1 — COMPLEMENTO: Base Operacional da Qualidade
-- Tabelas: cq_parametros_inspecao, cq_anexos
-- Executar no Supabase SQL Editor como postgres
-- ============================================================

BEGIN;

-- ────────────────────────────────────────────────────────────
-- 1. TABELA: cq_parametros_inspecao
-- Limites configuráveis por produto para classificação automática
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cq_parametros_inspecao (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  produto_id        UUID REFERENCES cq_produtos(id) ON DELETE CASCADE,
  nome_produto      TEXT,  -- cache do nome para consultas rápidas
  limite_aprovacao   NUMERIC(5,2) NOT NULL DEFAULT 5.0,    -- até X% defeitos = aprovado
  limite_ressalva    NUMERIC(5,2) NOT NULL DEFAULT 15.0,   -- até X% = ressalva, acima = bloqueado
  amostragem_minima  INTEGER NOT NULL DEFAULT 10,           -- quantidade mínima de amostras
  ativo              BOOLEAN DEFAULT TRUE,
  criado_em          TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em      TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(produto_id),
  CHECK (limite_aprovacao > 0),
  CHECK (limite_ressalva > limite_aprovacao),
  CHECK (amostragem_minima > 0)
);

CREATE INDEX IF NOT EXISTS idx_param_produto ON cq_parametros_inspecao(produto_id);

-- RLS
ALTER TABLE cq_parametros_inspecao ENABLE ROW LEVEL SECURITY;

CREATE POLICY "param_select"
  ON cq_parametros_inspecao FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "param_modify"
  ON cq_parametros_inspecao FOR ALL TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade'))
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade'));

-- Seed: um registro padrão (geral) e um por produto existente
INSERT INTO cq_parametros_inspecao (produto_id, nome_produto, limite_aprovacao, limite_ressalva, amostragem_minima)
SELECT id, nome, 5.0, 15.0, 10
FROM cq_produtos
WHERE ativo = TRUE
ON CONFLICT (produto_id) DO NOTHING;

-- Parâmetro padrão (sem produto específico — fallback)
INSERT INTO cq_parametros_inspecao (produto_id, nome_produto, limite_aprovacao, limite_ressalva, amostragem_minima)
VALUES (NULL, 'PADRÃO GERAL', 5.0, 15.0, 10)
ON CONFLICT DO NOTHING;


-- ────────────────────────────────────────────────────────────
-- 2. TABELA: cq_anexos
-- Fotos e documentos vinculados a inspeções ou recebimentos
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cq_anexos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inspecao_id     UUID REFERENCES registros_cq_inspecao(id) ON DELETE CASCADE,
  recebimento_id  UUID REFERENCES cq_recebimentos(id) ON DELETE CASCADE,
  tipo            TEXT NOT NULL CHECK (tipo IN (
                    'foto_etiqueta','foto_produto','foto_defeito',
                    'foto_carga','documento','outro'
                  )),
  url             TEXT NOT NULL,
  nome_arquivo    TEXT,
  tamanho_bytes   BIGINT,
  descricao       TEXT,
  usuario_id      UUID,
  usuario_email   TEXT,
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  -- Pelo menos uma referência deve existir
  CHECK (inspecao_id IS NOT NULL OR recebimento_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_anexo_inspecao ON cq_anexos(inspecao_id);
CREATE INDEX IF NOT EXISTS idx_anexo_recebimento ON cq_anexos(recebimento_id);
CREATE INDEX IF NOT EXISTS idx_anexo_tipo ON cq_anexos(tipo);

-- RLS
ALTER TABLE cq_anexos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anexo_select"
  ON cq_anexos FOR SELECT TO authenticated
  USING (cq_get_perfil() IS NOT NULL);

CREATE POLICY "anexo_insert"
  ON cq_anexos FOR INSERT TO authenticated
  WITH CHECK (cq_get_perfil() IN ('administrador','qualidade','logistica'));

CREATE POLICY "anexo_delete"
  ON cq_anexos FOR DELETE TO authenticated
  USING (cq_get_perfil() = 'administrador');


-- ────────────────────────────────────────────────────────────
-- 3. ATUALIZAR cq_permissoes — novos módulos
-- ────────────────────────────────────────────────────────────

INSERT INTO cq_permissoes (perfil, modulo, pode_visualizar, pode_criar, pode_editar, pode_excluir) VALUES
-- parametros_inspecao
('administrador', 'parametros_inspecao', TRUE, TRUE, TRUE, TRUE),
('qualidade',     'parametros_inspecao', TRUE, TRUE, TRUE, FALSE),
('comercial',     'parametros_inspecao', FALSE, FALSE, FALSE, FALSE),
('financeiro',    'parametros_inspecao', FALSE, FALSE, FALSE, FALSE),
('logistica',     'parametros_inspecao', FALSE, FALSE, FALSE, FALSE),
('industrial',    'parametros_inspecao', TRUE, FALSE, FALSE, FALSE),
('diretoria',     'parametros_inspecao', TRUE, FALSE, FALSE, FALSE),
-- anexos
('administrador', 'anexos', TRUE, TRUE, TRUE, TRUE),
('qualidade',     'anexos', TRUE, TRUE, TRUE, FALSE),
('comercial',     'anexos', FALSE, FALSE, FALSE, FALSE),
('financeiro',    'anexos', TRUE, FALSE, FALSE, FALSE),
('logistica',     'anexos', TRUE, TRUE, FALSE, FALSE),
('industrial',    'anexos', TRUE, FALSE, FALSE, FALSE),
('diretoria',     'anexos', TRUE, FALSE, FALSE, FALSE)
ON CONFLICT (perfil, modulo) DO NOTHING;


-- ────────────────────────────────────────────────────────────
-- 4. FUNÇÃO: buscar parâmetros de inspeção por produto
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION cq_get_parametros(p_produto TEXT)
RETURNS TABLE(limite_aprovacao NUMERIC, limite_ressalva NUMERIC, amostragem_minima INTEGER)
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT p.limite_aprovacao, p.limite_ressalva, p.amostragem_minima
  FROM cq_parametros_inspecao p
  LEFT JOIN cq_produtos prod ON p.produto_id = prod.id
  WHERE (prod.nome = p_produto OR p.nome_produto = p_produto)
    AND p.ativo = TRUE
  UNION ALL
  -- Fallback: parâmetro padrão
  SELECT p.limite_aprovacao, p.limite_ressalva, p.amostragem_minima
  FROM cq_parametros_inspecao p
  WHERE p.produto_id IS NULL AND p.ativo = TRUE
  LIMIT 1;
$$;

COMMIT;


-- ────────────────────────────────────────────────────────────
-- 5. VERIFICAÇÃO
-- ────────────────────────────────────────────────────────────

-- V1: Confirmar tabelas criadas
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('cq_parametros_inspecao','cq_anexos')
ORDER BY tablename;

-- V2: Confirmar policies
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename IN ('cq_parametros_inspecao','cq_anexos')
ORDER BY tablename, policyname;

-- V3: Confirmar seed de parâmetros
SELECT nome_produto, limite_aprovacao, limite_ressalva, amostragem_minima
FROM cq_parametros_inspecao
ORDER BY nome_produto;

-- V4: Confirmar permissões atualizadas
SELECT perfil, modulo, pode_visualizar, pode_criar, pode_editar, pode_excluir
FROM cq_permissoes
WHERE modulo IN ('parametros_inspecao','anexos')
ORDER BY modulo, perfil;
