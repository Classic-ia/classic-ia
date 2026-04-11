-- migration_ponto.sql — Tabela de registro de ponto (Secullum Ponto Web)
-- Executar no Supabase SQL Editor

CREATE TABLE IF NOT EXISTS rh_ponto (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id uuid REFERENCES rh_funcionarios(id),
  nome_colaborador text NOT NULL,
  cpf varchar(14),
  departamento text,
  funcao text,
  data date NOT NULL,
  dia_semana varchar(3),
  entrada_1 text,
  saida_1 text,
  entrada_2 text,
  saida_2 text,
  entrada_3 text,
  saida_3 text,
  horas_normais text DEFAULT '00:00',
  horas_falta text DEFAULT '00:00',
  horas_extra_50 text DEFAULT '00:00',
  horas_extra_100 text DEFAULT '00:00',
  dsr_debito numeric DEFAULT 0,
  horas_noturnas text DEFAULT '00:00',
  horas_extra_noturna text DEFAULT '00:00',
  ajuste text DEFAULT '00:00',
  carga text DEFAULT '00:00',
  tipo text DEFAULT 'normal',
  periodo_inicio date,
  periodo_fim date,
  arquivo_origem text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(cpf, data)
);

-- RLS
ALTER TABLE rh_ponto ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS ponto_select ON rh_ponto;
CREATE POLICY ponto_select ON rh_ponto
  FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','diretoria','gestor','gestor_confianca'));

DROP POLICY IF EXISTS ponto_insert ON rh_ponto;
CREATE POLICY ponto_insert ON rh_ponto
  FOR INSERT TO authenticated
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS ponto_update ON rh_ponto;
CREATE POLICY ponto_update ON rh_ponto
  FOR UPDATE TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'));

DROP POLICY IF EXISTS ponto_delete ON rh_ponto;
CREATE POLICY ponto_delete ON rh_ponto
  FOR DELETE TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh'));

-- Index for fast queries
CREATE INDEX IF NOT EXISTS idx_rh_ponto_cpf_data ON rh_ponto(cpf, data);
CREATE INDEX IF NOT EXISTS idx_rh_ponto_data ON rh_ponto(data);
CREATE INDEX IF NOT EXISTS idx_rh_ponto_periodo ON rh_ponto(periodo_inicio, periodo_fim);
