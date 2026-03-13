-- ============================================================
-- TABELA: registros_carga
-- Para o módulo "Lançamento de Cargas" do Classic CQ
-- Rodar no Supabase > SQL Editor
-- ============================================================

CREATE TABLE IF NOT EXISTS registros_carga (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at      timestamptz DEFAULT now(),
  numero_carga    text NOT NULL,
  data_chegada    date NOT NULL,
  frigorifico     text NOT NULL,
  lote            text,
  cnt_frigo       integer DEFAULT 0,
  cnt_classic     integer DEFAULT 0,
  pcs_a           integer DEFAULT 0,
  pct_a           numeric(6,2) DEFAULT 0,
  pcs_b           integer DEFAULT 0,
  pct_b           numeric(6,2) DEFAULT 0,
  pcs_c           integer DEFAULT 0,
  pct_c           numeric(6,2) DEFAULT 0,
  classificacao   text CHECK (classificacao IN ('A','B','C')),
  obs             text,
  fotos           text[],         -- array de URLs das fotos no Storage
  usuario_id      uuid REFERENCES auth.users(id)
);

-- Habilitar RLS
ALTER TABLE registros_carga ENABLE ROW LEVEL SECURITY;

-- Política: acesso total para usuários autenticados
CREATE POLICY "acesso total registros_carga"
  ON registros_carga FOR ALL
  USING (true)
  WITH CHECK (true);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_rc_frigorifico  ON registros_carga (frigorifico);
CREATE INDEX IF NOT EXISTS idx_rc_data         ON registros_carga (data_chegada);
CREATE INDEX IF NOT EXISTS idx_rc_carga        ON registros_carga (numero_carga);
CREATE INDEX IF NOT EXISTS idx_rc_class        ON registros_carga (classificacao);

-- ============================================================
-- BUCKET DE FOTOS no Supabase Storage
-- Rodar separado em: Storage > New Bucket
-- Nome: cq-fotos  |  Public: SIM
-- OU via SQL:
-- ============================================================

INSERT INTO storage.buckets (id, name, public)
VALUES ('cq-fotos', 'cq-fotos', true)
ON CONFLICT (id) DO NOTHING;

-- Política de Storage: permitir upload para usuários autenticados
CREATE POLICY "upload fotos cq"
  ON storage.objects FOR INSERT
  WITH CHECK (bucket_id = 'cq-fotos');

CREATE POLICY "leitura publica fotos cq"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'cq-fotos');
