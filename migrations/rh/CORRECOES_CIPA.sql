-- ============================================================================
-- CORRECOES_CIPA.sql
-- Sprint 1 (P0) do PLANO_CORRECOES_SIGA.md — item 2.3 (F-46 D-01/D-02/D-06)
--
-- Suporte de banco para as correcoes de cipa.html / cipa-denuncia.html:
--   1. cipa_reunioes.deliberacoes — coluna propria (antes concatenada em
--      ata_texto com separador improvisado; o modal lia r.deliberacoes que
--      nunca existia)
--   2. cipa_denuncias.empresa_slug — o canal publico seleciona a empresa
--      mas descartava a informacao (o formulario anonimo nao tem como
--      resolver o uuid de rh_empresas via chave anon; o slug preserva a
--      atribuicao e permite resolver empresa_id na triagem)
--
-- As demais correcoes do item 2.3 sao de frontend (proxima_data, ata_texto,
-- funcionario_id, mandato_id nas acoes, persistencia de cipa_presencas) e
-- nao exigem DDL — cipa_presencas ja existe em TABELAS_SST.sql.
--
-- Idempotente.
-- ============================================================================

BEGIN;

ALTER TABLE public.cipa_reunioes  ADD COLUMN IF NOT EXISTS deliberacoes TEXT;
ALTER TABLE public.cipa_denuncias ADD COLUMN IF NOT EXISTS empresa_slug TEXT;

COMMENT ON COLUMN public.cipa_reunioes.deliberacoes IS
  'Deliberacoes da reuniao (separadas da ata desde CORRECOES_CIPA 2026-07).';
COMMENT ON COLUMN public.cipa_denuncias.empresa_slug IS
  'Slug da empresa selecionada no canal publico (classic/mundial/transanacleto/ja). Resolver para empresa_id na triagem.';

COMMIT;

-- Verificacao
SELECT column_name, data_type FROM information_schema.columns
WHERE (table_name = 'cipa_reunioes'  AND column_name = 'deliberacoes')
   OR (table_name = 'cipa_denuncias' AND column_name = 'empresa_slug');
