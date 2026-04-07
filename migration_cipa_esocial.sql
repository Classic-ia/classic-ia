-- Migration: CIPA & eSocial Tables
-- Generated: 2026-04-07
-- Description: Creates tables for eSocial SST event monitoring and CIPA management

-- ============================================================================
-- TRIGGER FUNCTION: Auto-calculate eSocial status based on prazo_envio
-- ============================================================================

CREATE OR REPLACE FUNCTION esocial_calcular_status()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = 'enviado' THEN
    RETURN NEW;
  END IF;
  IF NEW.prazo_envio < CURRENT_DATE THEN
    NEW.status := 'atrasado';
  ELSIF NEW.prazo_envio = CURRENT_DATE THEN
    NEW.status := 'urgente';
  ELSIF NEW.prazo_envio <= CURRENT_DATE + 7 THEN
    NEW.status := 'atencao';
  ELSE
    NEW.status := 'pendente';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- eSocial Tables
-- ============================================================================

-- esocial_eventos: Monitors SST events pending for eSocial submission
CREATE TABLE IF NOT EXISTS public.esocial_eventos (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo VARCHAR(10) NOT NULL CHECK (tipo IN ('S-2210','S-2220','S-2240')),
  funcionario_id UUID REFERENCES public.rh_funcionarios(id),
  empresa_id UUID REFERENCES public.rh_empresas(id),
  data_fato DATE NOT NULL,
  prazo_envio DATE NOT NULL,
  subtipo TEXT,
  descricao TEXT,
  status VARCHAR(20) DEFAULT 'pendente' CHECK (status IN ('pendente','atencao','urgente','atrasado','enviado','cancelado')),
  protocolo VARCHAR(50),
  observacoes TEXT,
  enviado_por UUID REFERENCES auth.users(id),
  enviado_em TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.esocial_eventos IS 'Monitora eventos SST pendentes para envio ao eSocial (S-2210, S-2220, S-2240)';

CREATE INDEX IF NOT EXISTS idx_esocial_eventos_tipo ON public.esocial_eventos(tipo);
CREATE INDEX IF NOT EXISTS idx_esocial_eventos_funcionario_id ON public.esocial_eventos(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_esocial_eventos_status ON public.esocial_eventos(status);
CREATE INDEX IF NOT EXISTS idx_esocial_eventos_prazo_envio ON public.esocial_eventos(prazo_envio);

ALTER TABLE public.esocial_eventos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access esocial_eventos"
  ON public.esocial_eventos FOR ALL
  USING (auth.role() = 'authenticated');

CREATE TRIGGER trg_esocial_eventos_calcular_status
  BEFORE INSERT OR UPDATE ON public.esocial_eventos
  FOR EACH ROW
  EXECUTE FUNCTION esocial_calcular_status();

-- esocial_s2240: Tracks environmental condition changes for S-2240
CREATE TABLE IF NOT EXISTS public.esocial_s2240 (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  setor TEXT NOT NULL,
  empresa_id UUID REFERENCES public.rh_empresas(id),
  fator_risco TEXT,
  descricao TEXT,
  data_alteracao DATE NOT NULL,
  prazo_envio DATE NOT NULL,
  status VARCHAR(20) DEFAULT 'pendente' CHECK (status IN ('pendente','atencao','urgente','atrasado','enviado')),
  protocolo VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.esocial_s2240 IS 'Registra alteracoes de condicoes ambientais para evento S-2240 do eSocial';

CREATE INDEX IF NOT EXISTS idx_esocial_s2240_setor ON public.esocial_s2240(setor);
CREATE INDEX IF NOT EXISTS idx_esocial_s2240_empresa_id ON public.esocial_s2240(empresa_id);
CREATE INDEX IF NOT EXISTS idx_esocial_s2240_status ON public.esocial_s2240(status);
CREATE INDEX IF NOT EXISTS idx_esocial_s2240_prazo_envio ON public.esocial_s2240(prazo_envio);

ALTER TABLE public.esocial_s2240 ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access esocial_s2240"
  ON public.esocial_s2240 FOR ALL
  USING (auth.role() = 'authenticated');

CREATE TRIGGER trg_esocial_s2240_calcular_status
  BEFORE INSERT OR UPDATE ON public.esocial_s2240
  FOR EACH ROW
  EXECUTE FUNCTION esocial_calcular_status();

-- ============================================================================
-- CIPA Tables
-- ============================================================================

-- cipa_mandato: CIPA terms/mandates
CREATE TABLE IF NOT EXISTS public.cipa_mandato (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  numero TEXT NOT NULL,
  empresa_id UUID REFERENCES public.rh_empresas(id),
  data_inicio DATE NOT NULL,
  data_fim DATE NOT NULL,
  cnae VARCHAR(10),
  grau_risco INTEGER CHECK (grau_risco BETWEEN 1 AND 4),
  efetivos INTEGER DEFAULT 0,
  suplentes INTEGER DEFAULT 0,
  status VARCHAR(20) DEFAULT 'ativo' CHECK (status IN ('ativo','encerrado','em_eleicao')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.cipa_mandato IS 'Mandatos da CIPA com periodo, composicao e status';

CREATE INDEX IF NOT EXISTS idx_cipa_mandato_empresa_id ON public.cipa_mandato(empresa_id);
CREATE INDEX IF NOT EXISTS idx_cipa_mandato_status ON public.cipa_mandato(status);

ALTER TABLE public.cipa_mandato ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_mandato"
  ON public.cipa_mandato FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_membros: CIPA members
CREATE TABLE IF NOT EXISTS public.cipa_membros (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  mandato_id UUID NOT NULL REFERENCES public.cipa_mandato(id) ON DELETE CASCADE,
  funcionario_id UUID REFERENCES public.rh_funcionarios(id),
  nome TEXT NOT NULL,
  cargo_cipa VARCHAR(30) NOT NULL CHECK (cargo_cipa IN ('presidente','vice','secretario','efetivo','suplente')),
  tipo VARCHAR(20) CHECK (tipo IN ('eleito','designado')),
  categoria VARCHAR(20) CHECK (categoria IN ('empregador','empregado')),
  data_posse DATE,
  estabilidade_ate DATE,
  treinado BOOLEAN DEFAULT false,
  data_treinamento DATE,
  ativo BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.cipa_membros IS 'Membros da CIPA com cargo, tipo de representacao e status de treinamento';

CREATE INDEX IF NOT EXISTS idx_cipa_membros_mandato_id ON public.cipa_membros(mandato_id);
CREATE INDEX IF NOT EXISTS idx_cipa_membros_funcionario_id ON public.cipa_membros(funcionario_id);

ALTER TABLE public.cipa_membros ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_membros"
  ON public.cipa_membros FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_reunioes: CIPA meetings
CREATE TABLE IF NOT EXISTS public.cipa_reunioes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  mandato_id UUID NOT NULL REFERENCES public.cipa_mandato(id),
  numero TEXT NOT NULL,
  data DATE NOT NULL,
  tipo VARCHAR(20) DEFAULT 'ordinaria' CHECK (tipo IN ('ordinaria','extraordinaria')),
  local TEXT,
  pauta TEXT,
  ata_texto TEXT,
  status VARCHAR(20) DEFAULT 'rascunho' CHECK (status IN ('rascunho','em_assinatura','assinada','cancelada')),
  proxima_data DATE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.cipa_reunioes IS 'Reunioes da CIPA com pauta, ata e controle de assinaturas';

CREATE INDEX IF NOT EXISTS idx_cipa_reunioes_mandato_id ON public.cipa_reunioes(mandato_id);
CREATE INDEX IF NOT EXISTS idx_cipa_reunioes_data ON public.cipa_reunioes(data);
CREATE INDEX IF NOT EXISTS idx_cipa_reunioes_status ON public.cipa_reunioes(status);

ALTER TABLE public.cipa_reunioes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_reunioes"
  ON public.cipa_reunioes FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_presencas: Meeting attendance
CREATE TABLE IF NOT EXISTS public.cipa_presencas (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reuniao_id UUID NOT NULL REFERENCES public.cipa_reunioes(id) ON DELETE CASCADE,
  membro_id UUID NOT NULL REFERENCES public.cipa_membros(id),
  presente BOOLEAN DEFAULT false,
  justificativa TEXT
);

COMMENT ON TABLE public.cipa_presencas IS 'Registro de presenca dos membros nas reunioes da CIPA';

CREATE INDEX IF NOT EXISTS idx_cipa_presencas_reuniao_id ON public.cipa_presencas(reuniao_id);
CREATE INDEX IF NOT EXISTS idx_cipa_presencas_membro_id ON public.cipa_presencas(membro_id);

ALTER TABLE public.cipa_presencas ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_presencas"
  ON public.cipa_presencas FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_assinaturas: Meeting signatures
CREATE TABLE IF NOT EXISTS public.cipa_assinaturas (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reuniao_id UUID NOT NULL REFERENCES public.cipa_reunioes(id) ON DELETE CASCADE,
  membro_id UUID NOT NULL REFERENCES public.cipa_membros(id),
  assinado_em TIMESTAMPTZ DEFAULT NOW(),
  ip_address VARCHAR(45),
  UNIQUE(reuniao_id, membro_id)
);

COMMENT ON TABLE public.cipa_assinaturas IS 'Assinaturas digitais das atas de reuniao da CIPA';

CREATE INDEX IF NOT EXISTS idx_cipa_assinaturas_reuniao_id ON public.cipa_assinaturas(reuniao_id);
CREATE INDEX IF NOT EXISTS idx_cipa_assinaturas_membro_id ON public.cipa_assinaturas(membro_id);

ALTER TABLE public.cipa_assinaturas ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_assinaturas"
  ON public.cipa_assinaturas FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_inspecoes: CIPA inspections
CREATE TABLE IF NOT EXISTS public.cipa_inspecoes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  data DATE NOT NULL DEFAULT CURRENT_DATE,
  setor TEXT NOT NULL,
  inspetor_id UUID REFERENCES public.cipa_membros(id),
  resultado_json JSONB DEFAULT '[]'::jsonb,
  observacoes TEXT,
  nao_conformidades INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.cipa_inspecoes IS 'Inspecoes de seguranca realizadas pela CIPA com checklist e nao conformidades';

CREATE INDEX IF NOT EXISTS idx_cipa_inspecoes_data ON public.cipa_inspecoes(data);
CREATE INDEX IF NOT EXISTS idx_cipa_inspecoes_setor ON public.cipa_inspecoes(setor);
CREATE INDEX IF NOT EXISTS idx_cipa_inspecoes_inspetor_id ON public.cipa_inspecoes(inspetor_id);

ALTER TABLE public.cipa_inspecoes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_inspecoes"
  ON public.cipa_inspecoes FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_checklist_padrao: Standard inspection checklist items
CREATE TABLE IF NOT EXISTS public.cipa_checklist_padrao (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  setor TEXT NOT NULL,
  item TEXT NOT NULL,
  ordem INTEGER DEFAULT 0,
  ativo BOOLEAN DEFAULT true
);

COMMENT ON TABLE public.cipa_checklist_padrao IS 'Itens padrao do checklist de inspecao por setor';

CREATE INDEX IF NOT EXISTS idx_cipa_checklist_padrao_setor ON public.cipa_checklist_padrao(setor);
CREATE INDEX IF NOT EXISTS idx_cipa_checklist_padrao_ativo ON public.cipa_checklist_padrao(ativo);

ALTER TABLE public.cipa_checklist_padrao ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_checklist_padrao"
  ON public.cipa_checklist_padrao FOR ALL
  USING (auth.role() = 'authenticated');

-- cipa_denuncias: Anonymous complaints/reports
CREATE TABLE IF NOT EXISTS public.cipa_denuncias (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id UUID REFERENCES public.rh_empresas(id),
  setor TEXT,
  descricao TEXT NOT NULL,
  status VARCHAR(20) DEFAULT 'aberta' CHECK (status IN ('aberta','em_analise','resolvida','arquivada')),
  resposta TEXT,
  respondido_por UUID REFERENCES auth.users(id),
  respondido_em TIMESTAMPTZ,
  anonima BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.cipa_denuncias IS 'Canal de denuncias da CIPA com suporte a anonimato';

CREATE INDEX IF NOT EXISTS idx_cipa_denuncias_empresa_id ON public.cipa_denuncias(empresa_id);
CREATE INDEX IF NOT EXISTS idx_cipa_denuncias_status ON public.cipa_denuncias(status);
CREATE INDEX IF NOT EXISTS idx_cipa_denuncias_created_at ON public.cipa_denuncias(created_at);

ALTER TABLE public.cipa_denuncias ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can access cipa_denuncias"
  ON public.cipa_denuncias FOR ALL
  USING (auth.role() = 'authenticated');
