-- ═══════════════════════════════════════════════════════════════════════════
-- MODULO_CIPA_ACOES.sql
-- ═══════════════════════════════════════════════════════════════════════════
-- Cria tabela `cipa_acoes` (seguindo nomenclatura de cipa_mandato, cipa_membros,
-- cipa_reunioes em producao) para dar backend real ao modulo de Plano de Acao
-- da CIPA (cipa.html). Antes desta migration a tabela nao existia, fazendo com
-- que a secao "Acoes" da pagina CIPA ficasse sempre vazia.
--
-- Idempotente.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

CREATE TABLE IF NOT EXISTS public.cipa_acoes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- FK logico (sem constraint dura) porque cipa_reunioes/cipa_mandato em alguns
  -- ambientes podem nao ter PK declarada. Integridade referencial
  -- e' tratada no app e via checks de existencia antes de INSERT.
  reuniao_id      UUID,
  mandato_id      UUID,
  acao            TEXT NOT NULL,
  descricao       TEXT,
  responsavel     TEXT NOT NULL,
  responsavel_id  UUID,
  prazo           DATE,
  status          VARCHAR(20) NOT NULL DEFAULT 'aberto'
                  CHECK (status IN ('aberto','andamento','concluido','cancelado')),
  prioridade      VARCHAR(10) DEFAULT 'media'
                  CHECK (prioridade IN ('baixa','media','alta','critica')),
  data_conclusao  DATE,
  observacoes     TEXT,
  criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
  atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
  criado_por      UUID REFERENCES auth.users(id)
);

CREATE INDEX IF NOT EXISTS idx_cipa_acoes_reuniao   ON public.cipa_acoes(reuniao_id);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_mandato   ON public.cipa_acoes(mandato_id);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_status    ON public.cipa_acoes(status);
CREATE INDEX IF NOT EXISTS idx_cipa_acoes_prazo     ON public.cipa_acoes(prazo);

COMMENT ON TABLE public.cipa_acoes IS
  'Acoes/deliberacoes registradas em reunioes de CIPA com tracking de status e prazo.';

-- Trigger updated_at
CREATE OR REPLACE FUNCTION public.fn_cipa_acoes_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.atualizado_em := now(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS trg_cipa_acoes_updated_at ON public.cipa_acoes;
CREATE TRIGGER trg_cipa_acoes_updated_at
  BEFORE UPDATE ON public.cipa_acoes
  FOR EACH ROW EXECUTE FUNCTION public.fn_cipa_acoes_updated_at();

-- RLS: autenticado le, rh/admin escreve (mesmo padrao das outras tabelas de
-- decisao; usa rh_usuarios como backbone RBAC, nao has_perfil)
ALTER TABLE public.cipa_acoes ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_cipa_acoes_read   ON public.cipa_acoes;
DROP POLICY IF EXISTS p_cipa_acoes_write  ON public.cipa_acoes;
DROP POLICY IF EXISTS p_cipa_acoes_update ON public.cipa_acoes;

CREATE POLICY p_cipa_acoes_read ON public.cipa_acoes
  FOR SELECT TO authenticated USING (true);

CREATE POLICY p_cipa_acoes_write ON public.cipa_acoes
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh')
       AND ativo = true
  ));

CREATE POLICY p_cipa_acoes_update ON public.cipa_acoes
  FOR UPDATE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.rh_usuarios
     WHERE auth_uid = auth.uid()
       AND perfil IN ('administrador','rh')
       AND ativo = true
  ));

GRANT SELECT, INSERT, UPDATE ON public.cipa_acoes TO authenticated;

COMMIT;
