-- ═══════════════════════════════════════════════════════════════
-- MIGRATION: Layouts de Exportacao de Calculos (Secullum Ponto Web)
-- Classic RH & SST
-- ═══════════════════════════════════════════════════════════════

-- Tabela principal: cada layout salvo
CREATE TABLE IF NOT EXISTS rh_layouts_exportacao (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,
  tipo VARCHAR(40) NOT NULL DEFAULT 'um_evento_por_linha'
    CHECK (tipo IN ('um_evento_por_linha','um_funcionario_por_linha','agrupado_dia_evento','por_funcionario_dia')),
  formato VARCHAR(30) NOT NULL DEFAULT 'tamanho_fixo'
    CHECK (formato IN ('tamanho_fixo','delimitado')),
  delimitador VARCHAR(5) DEFAULT ';',
  origem VARCHAR(30) DEFAULT 'manual'
    CHECK (origem IN ('manual','pre_cadastrado','importado')),
  ativo BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Campos de cada layout (ordem define posicao no arquivo)
CREATE TABLE IF NOT EXISTS rh_layouts_exportacao_campos (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  layout_id UUID NOT NULL REFERENCES rh_layouts_exportacao(id) ON DELETE CASCADE,
  ordem INTEGER NOT NULL DEFAULT 0,
  tipo_campo VARCHAR(40) NOT NULL CHECK (tipo_campo IN (
    'cabecalho','codigo_evento','contador_sequencial',
    'data_inicio','data_fim','espacos',
    'horas_inteiro','horas_decimal','horas_minutos',
    'nro_folha','rodape','texto_fixo',
    'valor_inteiro','valor_decimal',
    'valor_horas_inteiro','valor_horas_decimal',
    'nro_identificador','tipo_evento',
    'contador_funcionario','nro_folha_empresa',
    'nro_pis_pasep','cpf','nro_folha_departamento',
    'data_evento','nome','departamento','funcao'
  )),
  configuracao TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_layout_campos_layout ON rh_layouts_exportacao_campos(layout_id);
CREATE INDEX IF NOT EXISTS idx_layout_campos_ordem ON rh_layouts_exportacao_campos(layout_id, ordem);

-- Triggers updated_at
CREATE TRIGGER trg_rh_layouts_exportacao_updated
  BEFORE UPDATE ON rh_layouts_exportacao
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

CREATE TRIGGER trg_rh_layouts_exportacao_campos_updated
  BEFORE UPDATE ON rh_layouts_exportacao_campos
  FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();

-- RLS
ALTER TABLE rh_layouts_exportacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_layouts_exportacao_campos ENABLE ROW LEVEL SECURITY;

CREATE POLICY rh_layouts_exportacao_all ON rh_layouts_exportacao
  FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY rh_layouts_exportacao_anon ON rh_layouts_exportacao
  FOR ALL TO anon USING (true) WITH CHECK (true);

CREATE POLICY rh_layouts_exportacao_campos_all ON rh_layouts_exportacao_campos
  FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY rh_layouts_exportacao_campos_anon ON rh_layouts_exportacao_campos
  FOR ALL TO anon USING (true) WITH CHECK (true);
