-- ============================================================
-- FASE 2B — AJUSTES NO BANCO (Bloco 1)
-- Data: 2026-03-16
-- Decisões aprovadas pela usuária
-- ============================================================

-- ============================================================
-- PASSO 1: ALTER cq_anexos — adicionar colunas faltantes
-- Colunas existentes: id, inspecao_id, recebimento_id, tipo, url,
--   nome_arquivo, tamanho_bytes, descricao, usuario_id, usuario_email, criado_em
-- Colunas novas: mime_type, storage_path, origem_upload, ativo
-- ============================================================

ALTER TABLE cq_anexos
ADD COLUMN IF NOT EXISTS mime_type TEXT,
ADD COLUMN IF NOT EXISTS storage_path TEXT,
ADD COLUMN IF NOT EXISTS origem_upload TEXT DEFAULT 'web'
  CHECK (origem_upload IN ('web','mobile','importacao','sistema')),
ADD COLUMN IF NOT EXISTS ativo BOOLEAN DEFAULT TRUE;

-- Verificação P1
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'cq_anexos'
ORDER BY ordinal_position;

-- ============================================================
-- PASSO 2: Restringir policy rev_select (cq_revisoes)
-- Antes: qualquer autenticado com perfil via cq_get_perfil() IS NOT NULL
-- Agora: apenas administrador, qualidade (decisão: diretoria não é revisora padrão)
-- ============================================================

DROP POLICY IF EXISTS "rev_select" ON cq_revisoes;
CREATE POLICY "rev_select"
  ON cq_revisoes FOR SELECT TO authenticated
  USING (cq_get_perfil() IN ('administrador','qualidade','diretoria'));

-- rev_insert permanece como está (administrador, qualidade, diretoria)
-- Nota: diretoria pode VER revisões mas não é revisora operacional padrão

-- Verificação P2
SELECT policyname, cmd, qual
FROM pg_policies
WHERE tablename = 'cq_revisoes';

-- ============================================================
-- PASSO 3: Restringir policy nc_select (cq_nao_conformidades)
-- Antes: qualquer autenticado com perfil
-- Agora: perfis que precisam ver NCs operacionalmente
-- ============================================================

DROP POLICY IF EXISTS "nc_select" ON cq_nao_conformidades;
CREATE POLICY "nc_select"
  ON cq_nao_conformidades FOR SELECT TO authenticated
  USING (cq_get_perfil() IN (
    'administrador','qualidade','financeiro',
    'logistica','industrial','diretoria'
  ));

-- nc_insert, nc_update, nc_delete permanecem como estão

-- Verificação P3
SELECT policyname, cmd, qual
FROM pg_policies
WHERE tablename = 'cq_nao_conformidades';

-- ============================================================
-- PASSO 4: Racionalizar status de cq_planos_acao
-- De 7 status misturados para 4 status claros
-- Validação de eficácia fica no campo separado 'validacao'
-- ============================================================

-- 4a. Verificar se existem registros com status que serão migrados
SELECT status, COUNT(*) FROM cq_planos_acao GROUP BY status;

-- 4b. Migrar registros existentes (se houver) ANTES de mudar constraint
UPDATE cq_planos_acao SET status = 'em_execucao' WHERE status = 'comunicado';
UPDATE cq_planos_acao SET status = 'em_execucao' WHERE status = 'em_tratativa';
UPDATE cq_planos_acao SET status = 'concluido'   WHERE status = 'resolvido';
-- 'ineficaz' → concluido + validacao='ineficaz'
UPDATE cq_planos_acao SET validacao = 'ineficaz', status = 'concluido' WHERE status = 'ineficaz';

-- 4c. Dropar constraint antiga e criar nova
ALTER TABLE cq_planos_acao DROP CONSTRAINT IF EXISTS cq_planos_acao_status_check;
ALTER TABLE cq_planos_acao ADD CONSTRAINT cq_planos_acao_status_check
CHECK (status IN ('aberto','em_execucao','concluido','cancelado'));

-- Verificação P4
SELECT
  conname AS constraint_name,
  pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'cq_planos_acao'::regclass
  AND contype = 'c';

-- ============================================================
-- PASSO 5: Verificação final
-- ============================================================

-- V1: Novas colunas de cq_anexos
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'cq_anexos'
  AND column_name IN ('mime_type','storage_path','origem_upload','ativo');

-- V2: Policies atualizadas
SELECT tablename, policyname, cmd, qual
FROM pg_policies
WHERE tablename IN ('cq_revisoes','cq_nao_conformidades')
ORDER BY tablename, policyname;

-- V3: Constraint de cq_planos_acao
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'cq_planos_acao'::regclass AND contype = 'c';
