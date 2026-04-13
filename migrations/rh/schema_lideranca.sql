-- ============================================================
-- MÓDULO GESTÃO & LIDERANÇA — Classic Couros
-- Executar no Supabase SQL Editor
-- ============================================================

-- Avaliações de Desempenho
CREATE TABLE IF NOT EXISTS rh_avaliacoes (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  funcionario_id UUID REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  avaliador_id  UUID REFERENCES rh_usuarios(id),
  periodo       TEXT,                        -- Ex: "1º Semestre 2025"
  nota_geral    SMALLINT CHECK (nota_geral BETWEEN 1 AND 5),
  nota_proat    SMALLINT CHECK (nota_proat BETWEEN 1 AND 5),
  nota_equipe   SMALLINT CHECK (nota_equipe BETWEEN 1 AND 5),
  pontos_fortes TEXT,
  pontos_melhora TEXT,
  observacoes   TEXT,
  created_at    TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE rh_avaliacoes ENABLE ROW LEVEL SECURITY;
CREATE POLICY "rh_avaliacoes_all" ON rh_avaliacoes FOR ALL USING (true);

-- Metas (geral e por setor)
CREATE TABLE IF NOT EXISTS rh_metas (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  titulo      TEXT NOT NULL,
  tipo        TEXT DEFAULT 'geral' CHECK (tipo IN ('geral','setor')),
  setor       TEXT,
  unidade     TEXT,                          -- %, R$, un, etc
  valor_alvo  NUMERIC NOT NULL,
  valor_atual NUMERIC DEFAULT 0,
  data_inicio DATE,
  data_fim    DATE,
  descricao   TEXT,
  criado_por  UUID REFERENCES rh_usuarios(id),
  created_at  TIMESTAMPTZ DEFAULT now(),
  updated_at  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE rh_metas ENABLE ROW LEVEL SECURITY;
CREATE POLICY "rh_metas_all" ON rh_metas FOR ALL USING (true);

-- Pesquisa de Pulso (humor da equipe)
CREATE TABLE IF NOT EXISTS rh_pulsos (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  valor       TEXT NOT NULL,                 -- 😄 Excelente, 🙂 Bem, etc
  setor       TEXT,
  registrado_por UUID REFERENCES rh_usuarios(id),
  created_at  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE rh_pulsos ENABLE ROW LEVEL SECURITY;
CREATE POLICY "rh_pulsos_all" ON rh_pulsos FOR ALL USING (true);

-- Rondas Motivacionais (histórico)
CREATE TABLE IF NOT EXISTS rh_rondas (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  emoji       TEXT,
  titulo      TEXT,
  texto       TEXT,
  tema        TEXT,
  setor_alvo  TEXT,
  gerado_por  UUID REFERENCES rh_usuarios(id),
  created_at  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE rh_rondas ENABLE ROW LEVEL SECURITY;
CREATE POLICY "rh_rondas_all" ON rh_rondas FOR ALL USING (true);

-- Trigger updated_at em metas
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$;

DROP TRIGGER IF EXISTS trg_metas_updated ON rh_metas;
CREATE TRIGGER trg_metas_updated
  BEFORE UPDATE ON rh_metas
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
