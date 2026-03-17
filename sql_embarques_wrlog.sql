-- Tabela para dados do WRLOG510 (peso e fardos por classe A/B/C)
CREATE TABLE IF NOT EXISTS cq_embarques_wrlog (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  numero_pedido TEXT NOT NULL,
  documento TEXT,
  data_documento TEXT,
  cadastro TEXT,
  fardos_a INTEGER DEFAULT 0,
  peso_a NUMERIC(12,2) DEFAULT 0,
  fardos_b INTEGER DEFAULT 0,
  peso_b NUMERIC(12,2) DEFAULT 0,
  fardos_c INTEGER DEFAULT 0,
  peso_c NUMERIC(12,2) DEFAULT 0,
  fardos_retalho INTEGER DEFAULT 0,
  peso_retalho NUMERIC(12,2) DEFAULT 0,
  total_fardos INTEGER DEFAULT 0,
  total_peso NUMERIC(12,2) DEFAULT 0,
  arquivo_origem TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(numero_pedido)
);

ALTER TABLE cq_embarques_wrlog ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth read wrlog" ON cq_embarques_wrlog FOR SELECT TO authenticated USING (true);
CREATE POLICY "auth insert wrlog" ON cq_embarques_wrlog FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "auth update wrlog" ON cq_embarques_wrlog FOR UPDATE TO authenticated USING (true);
CREATE POLICY "auth delete wrlog" ON cq_embarques_wrlog FOR DELETE TO authenticated USING (true);

-- Permitir acesso anon (para o dashboard)
CREATE POLICY "anon read wrlog" ON cq_embarques_wrlog FOR SELECT TO anon USING (true);
CREATE POLICY "anon insert wrlog" ON cq_embarques_wrlog FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY "anon update wrlog" ON cq_embarques_wrlog FOR UPDATE TO anon USING (true);
CREATE POLICY "anon delete wrlog" ON cq_embarques_wrlog FOR DELETE TO anon USING (true);
