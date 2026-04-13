-- ═══════════════════════════════════════════════════════════════
-- SCHEMA — RECRUTAMENTO & SELEÇÃO
-- Classic CQ · Supabase (PostgreSQL)
-- Executar no SQL Editor do Supabase
-- ═══════════════════════════════════════════════════════════════

-- ══════════════════════════════════════════════════════════
-- 1. VAGAS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_vagas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  titulo TEXT NOT NULL,
  setor TEXT NOT NULL,
  cargo TEXT NOT NULL,
  tipo_contrato VARCHAR(30) DEFAULT 'CLT' CHECK (tipo_contrato IN ('CLT','PJ','temporario','estagio','jovem_aprendiz')),
  turno VARCHAR(20) CHECK (turno IN ('diurno','noturno','misto','comercial')),
  quantidade INTEGER DEFAULT 1,
  salario_faixa TEXT,
  requisitos TEXT,
  descricao TEXT,
  prioridade VARCHAR(20) DEFAULT 'normal' CHECK (prioridade IN ('baixa','normal','alta','urgente')),
  status VARCHAR(20) DEFAULT 'aberta' CHECK (status IN ('aberta','em_selecao','suspensa','fechada','cancelada')),
  data_abertura DATE NOT NULL DEFAULT CURRENT_DATE,
  data_limite DATE,
  data_fechamento DATE,
  responsavel TEXT,
  motivo_abertura VARCHAR(30) CHECK (motivo_abertura IN ('substituicao','aumento_quadro','nova_funcao','temporario','outro')),
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vagas_status ON rs_vagas(status);
CREATE INDEX IF NOT EXISTS idx_vagas_setor ON rs_vagas(setor);

-- ══════════════════════════════════════════════════════════
-- 2. CANDIDATOS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_candidatos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome_completo TEXT NOT NULL,
  cpf VARCHAR(11),
  telefone TEXT,
  email TEXT,
  cidade TEXT,
  uf VARCHAR(2),
  escolaridade VARCHAR(30) CHECK (escolaridade IN ('fundamental','medio','tecnico','superior','pos_graduacao')),
  experiencia_resumo TEXT,
  pretensao_salarial NUMERIC(12,2),
  disponibilidade VARCHAR(20) CHECK (disponibilidade IN ('imediata','15_dias','30_dias','a_combinar')),
  fonte VARCHAR(30) CHECK (fonte IN ('indicacao','site','agencia','linkedin','whatsapp','presencial','outro')),
  indicado_por TEXT,
  observacoes TEXT,
  status VARCHAR(20) DEFAULT 'ativo' CHECK (status IN ('ativo','contratado','desistiu','banco_talentos','bloqueado')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cand_status ON rs_candidatos(status);
CREATE INDEX IF NOT EXISTS idx_cand_nome ON rs_candidatos(nome_completo);

-- ══════════════════════════════════════════════════════════
-- 3. PIPELINE (Candidato ↔ Vaga — etapas do processo)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_pipeline (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  vaga_id UUID NOT NULL REFERENCES rs_vagas(id),
  candidato_id UUID NOT NULL REFERENCES rs_candidatos(id),
  etapa VARCHAR(30) NOT NULL DEFAULT 'triagem' CHECK (etapa IN (
    'triagem','entrevista_rh','teste_pratico','entrevista_gestor',
    'exame_admissional','documentacao','aprovado','reprovado','desistiu'
  )),
  nota_triagem INTEGER CHECK (nota_triagem BETWEEN 1 AND 5),
  nota_entrevista INTEGER CHECK (nota_entrevista BETWEEN 1 AND 5),
  nota_teste INTEGER CHECK (nota_teste BETWEEN 1 AND 5),
  nota_gestor INTEGER CHECK (nota_gestor BETWEEN 1 AND 5),
  parecer_rh TEXT,
  parecer_gestor TEXT,
  data_entrada DATE DEFAULT CURRENT_DATE,
  data_ultima_etapa DATE DEFAULT CURRENT_DATE,
  motivo_reprovacao TEXT,
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipe_vaga ON rs_pipeline(vaga_id);
CREATE INDEX IF NOT EXISTS idx_pipe_cand ON rs_pipeline(candidato_id);
CREATE INDEX IF NOT EXISTS idx_pipe_etapa ON rs_pipeline(etapa);

-- ══════════════════════════════════════════════════════════
-- 4. AGENDA DE ENTREVISTAS
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_entrevistas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  pipeline_id UUID NOT NULL REFERENCES rs_pipeline(id),
  tipo VARCHAR(30) NOT NULL CHECK (tipo IN ('entrevista_rh','teste_pratico','entrevista_gestor','exame_admissional','outro')),
  data_hora TIMESTAMPTZ NOT NULL,
  duracao_min INTEGER DEFAULT 30,
  local TEXT,
  entrevistador TEXT,
  status VARCHAR(20) DEFAULT 'agendada' CHECK (status IN ('agendada','confirmada','realizada','cancelada','reagendada','nao_compareceu')),
  resultado VARCHAR(20) CHECK (resultado IN ('aprovado','reprovado','pendente')),
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entrev_data ON rs_entrevistas(data_hora);
CREATE INDEX IF NOT EXISTS idx_entrev_status ON rs_entrevistas(status);

-- ══════════════════════════════════════════════════════════
-- 5. FICHA DE AVALIAÇÃO (Roteiro de entrevista padronizado)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_avaliacoes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  pipeline_id UUID NOT NULL REFERENCES rs_pipeline(id),
  avaliador TEXT NOT NULL,
  data_avaliacao DATE DEFAULT CURRENT_DATE,
  -- Critérios de avaliação (1-5)
  apresentacao INTEGER CHECK (apresentacao BETWEEN 1 AND 5),
  comunicacao INTEGER CHECK (comunicacao BETWEEN 1 AND 5),
  experiencia INTEGER CHECK (experiencia BETWEEN 1 AND 5),
  motivacao INTEGER CHECK (motivacao BETWEEN 1 AND 5),
  adequacao_vaga INTEGER CHECK (adequacao_vaga BETWEEN 1 AND 5),
  trabalho_equipe INTEGER CHECK (trabalho_equipe BETWEEN 1 AND 5),
  -- Perguntas padronizadas (respostas livres)
  p1_experiencia TEXT,    -- Conte sobre experiência anterior
  p2_motivo TEXT,         -- Por que quer trabalhar aqui?
  p3_desafio TEXT,        -- Maior desafio profissional
  p4_expectativa TEXT,    -- Expectativas para a vaga
  p5_disponibilidade TEXT,-- Disponibilidade e restrições
  -- Resultado
  nota_geral NUMERIC(3,1),
  parecer TEXT NOT NULL CHECK (parecer IN ('recomendado','recomendado_restricao','nao_recomendado')),
  justificativa TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aval_pipe ON rs_avaliacoes(pipeline_id);

-- ══════════════════════════════════════════════════════════
-- 6. BANCO DE TALENTOS (tags para busca futura)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rs_banco_talentos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  candidato_id UUID NOT NULL REFERENCES rs_candidatos(id),
  tags TEXT,          -- ex: "solda,empilhadeira,noturno"
  area_interesse TEXT,
  ultima_interacao DATE DEFAULT CURRENT_DATE,
  proxima_acao TEXT,
  observacoes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_banco_cand ON rs_banco_talentos(candidato_id);

-- ══════════════════════════════════════════════════════════
-- 7. TRIGGERS: updated_at automático
-- ══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rs_vagas','rs_candidatos','rs_pipeline','rs_entrevistas','rs_avaliacoes','rs_banco_talentos'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated ON %I', t, t);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_updated BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at()',
      t, t
    );
  END LOOP;
END;
$$;

-- ══════════════════════════════════════════════════════════
-- 8. RLS — Habilitar
-- ══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rs_vagas','rs_candidatos','rs_pipeline','rs_entrevistas','rs_avaliacoes','rs_banco_talentos'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS %s_all ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %s_all ON %I FOR ALL TO authenticated USING (true) WITH CHECK (true)',
      t, t
    );
    EXECUTE format('DROP POLICY IF EXISTS %s_anon ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %s_anon ON %I FOR ALL TO anon USING (true) WITH CHECK (true)',
      t, t
    );
  END LOOP;
END;
$$;

-- ══════════════════════════════════════════════════════════
-- SCHEMA R&S — PRONTO PARA USO
-- ══════════════════════════════════════════════════════════
