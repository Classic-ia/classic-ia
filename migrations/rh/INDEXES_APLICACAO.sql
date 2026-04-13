-- ════════════════════════════════════════════════════════════════════════════
-- INDEXES_APLICACAO.sql — Indexes e constraints de producao faltantes
-- Exportado de: muiqmtnfvyffborgiwdw.supabase.co
-- Data: 2026-04-13
-- ════════════════════════════════════════════════════════════════════════════
-- Estes indexes sao obrigatorios para ON CONFLICT em RPCs/imports e para
-- performance de queries frequentes. PKEYs sao criados automaticamente
-- pelo DEFAULT gen_random_uuid() PRIMARY KEY — aqui estao apenas os
-- indexes adicionais.
-- ════════════════════════════════════════════════════════════════════════════

-- file_ingest_log: obrigatorio para file_check_and_log() ON CONFLICT
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_ingest_hash
  ON public.file_ingest_log (hash_arquivo) WHERE hash_arquivo IS NOT NULL;

-- motor_score: evita score duplicado por funcionario+periodo
CREATE UNIQUE INDEX IF NOT EXISTS idx_motor_score_func_periodo
  ON public.motor_score (funcionario_id, periodo_referencia);

-- motor_alerta: dedup de alertas
CREATE UNIQUE INDEX IF NOT EXISTS motor_alerta_chave_dedup_key
  ON public.motor_alerta (chave_dedup);

-- acao_plano: dedup de planos de acao
CREATE UNIQUE INDEX IF NOT EXISTS acao_plano_chave_dedup_key
  ON public.acao_plano (chave_dedup);

-- agente_analise: dedup de analises
CREATE UNIQUE INDEX IF NOT EXISTS agente_analise_chave_dedup_key
  ON public.agente_analise (chave_dedup);

-- agente_pendencia: dedup de pendencias
CREATE UNIQUE INDEX IF NOT EXISTS agente_pendencia_chave_dedup_key
  ON public.agente_pendencia (chave_dedup);

-- etl_mapa_identidade: unicidade por sistema+entidade+id_origem
CREATE UNIQUE INDEX IF NOT EXISTS etl_mapa_identidade_origem_sistema_tipo_entidade_id_origem_key
  ON public.etl_mapa_identidade (origem_sistema, tipo_entidade, id_origem);

-- integ_conector: nome unico do conector
CREATE UNIQUE INDEX IF NOT EXISTS integ_conector_nome_key
  ON public.integ_conector (nome);

-- rh_cte: chave de acesso unica
CREATE UNIQUE INDEX IF NOT EXISTS rh_cte_chave_acesso_key
  ON public.rh_cte (chave_acesso);

-- rh_custo_colaborador: um custo por funcionario+competencia
CREATE UNIQUE INDEX IF NOT EXISTS rh_custo_colaborador_funcionario_id_competencia_key
  ON public.rh_custo_colaborador (funcionario_id, competencia);

-- stg_convenia_documentos: dedup de importacao
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_conv_doc_dedup
  ON public.stg_convenia_documentos (id_origem, hash_registro);

-- reconc_correspondencia: CPF unico na reconciliacao
CREATE UNIQUE INDEX IF NOT EXISTS idx_reconc_cpf
  ON public.reconc_correspondencia (cpf) WHERE cpf IS NOT NULL;
