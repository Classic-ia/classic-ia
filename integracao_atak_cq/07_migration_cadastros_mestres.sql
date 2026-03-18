-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 07: Cadastros Mestres Faltantes + Governança + Auditoria + Status
-- Rodar APÓS 06_migration_padronizacao_ids.sql
--
-- Cobre:
--   • Cadastros mestres ausentes (clientes, unidades, motivos divergência)
--   • Dicionário de dados (tabela cq_dicionario_dados)
--   • Modelo de status padronizado (tabela cq_status_config)
--   • Triggers de auditoria em TODAS as tabelas de cadastro mestre
--   • Governança cadastral (soft delete, merge, alias)
--   • Logs de importação aprimorados
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. CADASTROS MESTRES FALTANTES
-- ════════════════════════════════════════════════════════════════════════════

-- 1.1 Clientes
CREATE TABLE IF NOT EXISTS cq_clientes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo          TEXT UNIQUE,           -- código no ATAK
  codigo_atak     TEXT,                  -- ID original no ATAK
  nome            TEXT NOT NULL,
  apelido         TEXT,
  cnpj            TEXT UNIQUE,
  ie              TEXT,                  -- inscrição estadual
  uf              TEXT,
  cidade          TEXT,
  endereco        TEXT,
  telefone        TEXT,
  email           TEXT,
  contato_nome    TEXT,
  tipo_cliente    TEXT DEFAULT 'nacional' CHECK (tipo_cliente IN (
                    'nacional','exportacao','trading','distribuidor','varejo'
                  )),
  ativo           BOOLEAN DEFAULT true,
  inativado_em    TIMESTAMPTZ,
  inativado_por   UUID REFERENCES cq_usuarios(id),
  motivo_inativacao TEXT,
  origem          TEXT NOT NULL DEFAULT 'manual' CHECK (origem IN ('atak','manual','importacao')),
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clientes_nome ON cq_clientes(nome);
CREATE INDEX IF NOT EXISTS idx_clientes_ativo ON cq_clientes(ativo);

-- 1.2 Unidades / Filiais
CREATE TABLE IF NOT EXISTS cq_unidades (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo          TEXT UNIQUE NOT NULL,
  nome            TEXT NOT NULL,
  tipo            TEXT NOT NULL DEFAULT 'planta' CHECK (tipo IN (
                    'planta','filial','deposito','escritorio'
                  )),
  cnpj            TEXT UNIQUE,
  uf              TEXT,
  cidade          TEXT,
  endereco        TEXT,
  latitude        NUMERIC(10,6),
  longitude       NUMERIC(10,6),
  raio_geofence_m INTEGER DEFAULT 500,
  ativo           BOOLEAN DEFAULT true,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

-- Seed: planta principal
INSERT INTO cq_unidades (codigo, nome, tipo, uf, cidade, latitude, longitude)
VALUES ('PLANTA_JA', 'Classic Couros — Jardim Alegre/PR', 'planta', 'PR', 'Jardim Alegre', -24.1820, -51.6920)
ON CONFLICT (codigo) DO NOTHING;

-- 1.3 Motivos de divergência (cadastro mestre — substituir CHECK constraint)
CREATE TABLE IF NOT EXISTS cq_motivos_divergencia (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo          TEXT UNIQUE NOT NULL,
  nome            TEXT NOT NULL,
  descricao       TEXT,
  gravidade_padrao TEXT DEFAULT 'atencao' CHECK (gravidade_padrao IN ('informativa','atencao','critica')),
  ativo           BOOLEAN DEFAULT true,
  ordem           INTEGER DEFAULT 0,
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO cq_motivos_divergencia (codigo, nome, gravidade_padrao, ordem) VALUES
  ('abc_total_mismatch',        'A+B+C ≠ total',                    'critica',      1),
  ('contagem_frigo_vs_classic',  'Contagem frigorífico ≠ Classic',   'critica',      2),
  ('duplicidade_carga',          'Carga duplicada',                  'atencao',      3),
  ('fornecedor_critico',         'Fornecedor em status crítico',     'atencao',      4),
  ('quantidade_negativa',        'Quantidade negativa detectada',    'critica',      5),
  ('peso_divergente',            'Peso divergente',                  'atencao',      6),
  ('data_inconsistente',         'Data inconsistente',               'atencao',      7),
  ('documento_ausente',          'Documento ATAK ausente',           'critica',      8),
  ('produto_nao_cadastrado',     'Produto não encontrado no cadastro','critica',     9),
  ('fornecedor_nao_cadastrado',  'Fornecedor não encontrado',        'critica',     10),
  ('defeito_sem_justificativa',  'Classe B/C sem defeito registrado','atencao',     11),
  ('fornecedor_bloqueado',       'Carga de fornecedor bloqueado',    'critica',     12)
ON CONFLICT (codigo) DO NOTHING;

-- Adicionar FK em cq_validacao_divergencias → cq_motivos_divergencia
ALTER TABLE cq_validacao_divergencias
  ADD COLUMN IF NOT EXISTS motivo_id UUID REFERENCES cq_motivos_divergencia(id);


-- ════════════════════════════════════════════════════════════════════════════
-- 2. MODELO DE STATUS PADRONIZADO
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_status_config (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entidade        TEXT NOT NULL,          -- nome da tabela/conceito
  codigo          TEXT NOT NULL,          -- valor do status
  label           TEXT NOT NULL,          -- rótulo para exibição
  cor             TEXT DEFAULT '#666',    -- cor hex para UI
  icon            TEXT,                   -- emoji/ícone
  ordem           INTEGER DEFAULT 0,
  permite_edicao  BOOLEAN DEFAULT false,  -- registro neste status pode ser editado?
  is_terminal     BOOLEAN DEFAULT false,  -- status final (não tem transição para frente)?
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(entidade, codigo)
);

INSERT INTO cq_status_config (entidade, codigo, label, cor, icon, ordem, permite_edicao, is_terminal) VALUES
  -- Cadastros
  ('cadastro',    'ativo',              'Ativo',              '#27ae60', '✅', 1, true,  false),
  ('cadastro',    'inativo',            'Inativo',            '#95a5a6', '🚫', 2, false, true),

  -- Documentos / Importações
  ('documento',   'importado',          'Importado',          '#3498db', '📥', 1, false, false),
  ('documento',   'validado',           'Validado',           '#27ae60', '✅', 2, false, false),
  ('documento',   'rejeitado',          'Rejeitado',          '#e74c3c', '❌', 3, false, true),
  ('documento',   'duplicado',          'Duplicado',          '#f39c12', '⚠️', 4, false, true),

  -- Cargas
  ('carga',       'aberta',             'Aberta',             '#3498db', '📦', 1, true,  false),
  ('carga',       'em_conferencia',     'Em Conferência',     '#f39c12', '🔍', 2, true,  false),
  ('carga',       'finalizada',         'Finalizada',         '#27ae60', '✅', 3, false, true),
  ('carga',       'cancelada',          'Cancelada',          '#95a5a6', '🚫', 4, false, true),

  -- Inspeções (workflow existente)
  ('inspecao',    'rascunho',           'Rascunho',           '#5d7f95', '📝', 1, true,  false),
  ('inspecao',    'em_analise',         'Em Análise',         '#3498db', '🔍', 2, true,  false),
  ('inspecao',    'aguardando_revisao', 'Aguardando Revisão', '#f39c12', '⏳', 3, false, false),
  ('inspecao',    'revisada',           'Revisada',           '#8e44ad', '✅', 4, false, false),
  ('inspecao',    'reprovada',          'Reprovada',          '#e74c3c', '❌', 5, false, false),
  ('inspecao',    'aprovada',           'Aprovada',           '#27ae60', '✔️', 6, false, false),
  ('inspecao',    'encerrada',          'Encerrada',          '#2c3e50', '🔒', 7, false, true),
  ('inspecao',    'cancelada',          'Cancelada',          '#95a5a6', '🚫', 8, false, true),

  -- Lotes de ranking
  ('lote',        'aberto',             'Aberto',             '#3498db', '📋', 1, true,  false),
  ('lote',        'finalizado',         'Finalizado',         '#27ae60', '✅', 2, false, true),
  ('lote',        'cancelado',          'Cancelado',          '#95a5a6', '🚫', 3, false, true),

  -- Lotes/produto (qualidade)
  ('lote_qual',   'bloqueado',          'Bloqueado',          '#e74c3c', '🚫', 1, false, false),
  ('lote_qual',   'liberado',           'Liberado',           '#27ae60', '✅', 2, false, false),
  ('lote_qual',   'reprovado',          'Reprovado',          '#e74c3c', '❌', 3, false, true),
  ('lote_qual',   'em_analise',         'Em Análise',         '#f39c12', '🔍', 4, true,  false),

  -- Divergências
  ('divergencia', 'aberta',             'Aberta',             '#e74c3c', '🔴', 1, true,  false),
  ('divergencia', 'em_analise',         'Em Análise',         '#f39c12', '🔍', 2, true,  false),
  ('divergencia', 'resolvida',          'Resolvida',          '#27ae60', '✅', 3, false, true),
  ('divergencia', 'ignorada',           'Ignorada',           '#95a5a6', '⚪', 4, false, true),
  ('divergencia', 'escalada',           'Escalada',           '#8e44ad', '🔺', 5, false, false),

  -- Não conformidades
  ('nc',          'aberta',             'Aberta',             '#e74c3c', '🔴', 1, true,  false),
  ('nc',          'em_tratamento',      'Em Tratamento',      '#f39c12', '🔧', 2, true,  false),
  ('nc',          'encerrada',          'Encerrada',          '#27ae60', '✅', 3, false, true),

  -- Fornecedor (qualidade)
  ('fornecedor',  'normal',             'Normal',             '#27ae60', '✅', 1, true,  false),
  ('fornecedor',  'atencao',            'Atenção',            '#f39c12', '⚠️', 2, true,  false),
  ('fornecedor',  'critico',            'Crítico',            '#e74c3c', '❌', 3, true,  false),
  ('fornecedor',  'bloqueado',          'Bloqueado',          '#95a5a6', '🚫', 4, false, false),

  -- Sync
  ('sync',        'em_execucao',        'Em Execução',        '#3498db', '🔄', 1, false, false),
  ('sync',        'sucesso',            'Sucesso',            '#27ae60', '✅', 2, false, true),
  ('sync',        'erro',               'Erro',               '#e74c3c', '❌', 3, false, true),
  ('sync',        'parcial',            'Parcial',            '#f39c12', '⚠️', 4, false, true)
ON CONFLICT (entidade, codigo) DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. DICIONÁRIO DE DADOS
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_dicionario_dados (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tabela          TEXT NOT NULL,
  coluna          TEXT NOT NULL,
  tipo_dado       TEXT NOT NULL,          -- ex: UUID, TEXT, INTEGER, NUMERIC(6,2)
  descricao       TEXT NOT NULL,
  origem          TEXT,                   -- ATAK, CQ, Auth, calculado, trigger
  regra           TEXT,                   -- descrição da regra de negócio
  obrigatorio     BOOLEAN DEFAULT false,
  exemplo         TEXT,
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tabela, coluna)
);

-- Seed: campos críticos
INSERT INTO cq_dicionario_dados (tabela, coluna, tipo_dado, descricao, origem, regra, obrigatorio, exemplo) VALUES
  -- registros_cq_inspecao
  ('registros_cq_inspecao', 'id',                    'UUID',        'Identificador único da inspeção',                'sistema',    'Auto-gerado',                                  true,  'a1b2c3d4-...'),
  ('registros_cq_inspecao', 'id_inspecao',            'TEXT',        'Código legível da inspeção',                     'sistema',    'Formato CQ-AAMMDD-HHMM-XXX',                   true,  'CQ-260318-1430-001'),
  ('registros_cq_inspecao', 'fornecedor_id',          'UUID FK',     'Referência ao fornecedor no cadastro mestre',    'trigger',    'Resolvido de fornecedor_codigo/fornecedor',     false, 'uuid do cadastros_atak'),
  ('registros_cq_inspecao', 'produto_id',             'UUID FK',     'Referência ao produto no cadastro mestre',       'trigger',    'Resolvido de produto (texto)',                  false, 'uuid do cq_produtos'),
  ('registros_cq_inspecao', 'quantidade_analisada',   'INTEGER',     'Quantidade de peças analisadas na amostra',      'inspetor',   'Deve ser > 0. NOT NULL.',                       true,  '150'),
  ('registros_cq_inspecao', 'total_defeitos',         'INTEGER',     'Soma de todos os defeitos encontrados',          'trigger',    'Calculado automaticamente pelo trigger',         true,  '23'),
  ('registros_cq_inspecao', 'percentual_defeitos',    'NUMERIC(6,2)','Percentual de defeitos sobre analisados',        'trigger',    '(total_defeitos / quantidade_analisada) * 100',  true,  '15.33'),
  ('registros_cq_inspecao', 'classificacao',          'TEXT',        'Classificação A/B/C baseada em %defeitos',       'trigger',    'A: ≤5%, B: ≤15%, C: >15%',                     true,  'B'),
  ('registros_cq_inspecao', 'status_final',           'TEXT',        'Status técnico final da inspeção',               'trigger',    'aprovado (A), ressalva (B), bloqueado (C)',      true,  'ressalva'),
  ('registros_cq_inspecao', 'status_workflow',        'TEXT',        'Status de fluxo/aprovação',                      'sistema',    'rascunho→em_analise→revisada→aprovada→encerrada',true, 'aguardando_revisao'),

  -- cq_lote_produto
  ('cq_lote_produto', 'class_a',                     'INTEGER',     'Quantidade de peças classificadas como A',       'inspetor',   'Deve ser >= 0. A+B+C = total.',                 true,  '80'),
  ('cq_lote_produto', 'class_b',                     'INTEGER',     'Quantidade de peças classificadas como B',       'inspetor',   'Deve ser >= 0.',                                true,  '15'),
  ('cq_lote_produto', 'class_c',                     'INTEGER',     'Quantidade de peças classificadas como C',       'inspetor',   'Deve ser >= 0.',                                true,  '5'),
  ('cq_lote_produto', 'total_defeitos',              'INTEGER',     'Soma dos 9 defeitos obrigatórios',               'trigger',    'rasgado+furado+pelando+...+orelha_misturada',   true,  '12'),
  ('cq_lote_produto', 'pct_defeitos',                'NUMERIC(6,2)','% de defeitos sobre total de peças',             'trigger',    '(total_defeitos / total_pecas) * 100',           true,  '12.00'),
  ('cq_lote_produto', 'principal_defeito',           'TEXT',        'Código do defeito com maior contagem',           'trigger',    'O que tem mais ocorrências',                    false, 'rasgado'),

  -- atak_cargas_raw
  ('atak_cargas_raw', 'hash_registro',               'TEXT UNIQUE', 'Hash de deduplicação do registro ATAK',         'n8n',        'UNIQUE. Impede reimportação duplicada.',         true,  'md5 do payload'),
  ('atak_cargas_raw', 'numero_documento',             'TEXT',        'Número do documento ATAK (NF/PCR)',             'ATAK',       'Chave do documento no ATAK',                    true,  'DOC-2026-001'),
  ('atak_cargas_raw', 'status_validacao',             'TEXT',        'Status da validação automática',                'trigger',    'pendente→validado→divergente',                  true,  'validado'),

  -- cq_fornecedor_score
  ('cq_fornecedor_score', 'score',                   'NUMERIC(5,2)','Score de qualidade 0-100',                      'calculado',  '100 - (%C * 2 + %defeitos). Clamp 0-100.',      true,  '72.50'),
  ('cq_fornecedor_score', 'status_fornecedor',       'TEXT',        'Status de qualidade do fornecedor',             'calculado',  'normal/atencao/critico/bloqueado',               true,  'atencao'),
  ('cq_fornecedor_score', 'limite_pct_c',            'NUMERIC(5,2)','Limite de %C antes de bloquear',               'admin',      'Default 15%. Admin pode ajustar.',               true,  '15.00')
ON CONFLICT (tabela, coluna) DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. GOVERNANÇA: SOFT DELETE + MERGE + ALIAS
-- ════════════════════════════════════════════════════════════════════════════

-- 4.1 Adicionar campos de soft-delete onde faltam
ALTER TABLE cq_produtos
  ADD COLUMN IF NOT EXISTS inativado_em    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inativado_por   UUID REFERENCES cq_usuarios(id),
  ADD COLUMN IF NOT EXISTS motivo_inativacao TEXT;

ALTER TABLE cq_veiculos
  ADD COLUMN IF NOT EXISTS inativado_em    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inativado_por   UUID REFERENCES cq_usuarios(id),
  ADD COLUMN IF NOT EXISTS motivo_inativacao TEXT;

ALTER TABLE cadastros_atak
  ADD COLUMN IF NOT EXISTS inativado_em    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inativado_por   UUID REFERENCES cq_usuarios(id),
  ADD COLUMN IF NOT EXISTS motivo_inativacao TEXT;

ALTER TABLE cq_transportadoras
  ADD COLUMN IF NOT EXISTS inativado_em    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inativado_por   UUID REFERENCES cq_usuarios(id),
  ADD COLUMN IF NOT EXISTS motivo_inativacao TEXT;

-- 4.2 Tabela de alias/sinônimos (para merge de duplicatas)
CREATE TABLE IF NOT EXISTS cq_cadastro_alias (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entidade        TEXT NOT NULL,          -- 'fornecedor', 'produto', 'motorista', etc.
  registro_id     UUID NOT NULL,          -- ID do registro canônico (o que ficou)
  alias_tipo      TEXT NOT NULL DEFAULT 'nome' CHECK (alias_tipo IN (
                    'nome','codigo','cnpj','placa','apelido'
                  )),
  alias_valor     TEXT NOT NULL,          -- o valor alternativo
  motivo          TEXT,                   -- ex: "merge de duplicata", "nome antigo"
  criado_por      UUID REFERENCES cq_usuarios(id),
  criado_em       TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(entidade, alias_tipo, alias_valor)
);

CREATE INDEX IF NOT EXISTS idx_alias_busca ON cq_cadastro_alias(entidade, alias_valor);

-- 4.3 Tabela de merge log (rastreabilidade de merge)
CREATE TABLE IF NOT EXISTS cq_cadastro_merge_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entidade        TEXT NOT NULL,
  registro_mantido_id UUID NOT NULL,      -- o que ficou
  registro_removido_id UUID NOT NULL,     -- o que foi mergeado
  registro_removido_dados JSONB,          -- snapshot do registro antes do merge
  motivo          TEXT,
  executado_por   UUID REFERENCES cq_usuarios(id),
  criado_em       TIMESTAMPTZ DEFAULT NOW()
);


-- ════════════════════════════════════════════════════════════════════════════
-- 5. TRIGGERS DE AUDITORIA EM CADASTROS MESTRES
-- ════════════════════════════════════════════════════════════════════════════

-- Function genérica de auditoria para INSERT/UPDATE/DELETE
CREATE OR REPLACE FUNCTION fn_audit_cadastro_mestre()
RETURNS TRIGGER AS $$
DECLARE
  v_usuario_id UUID;
  v_usuario_email TEXT;
  v_dados_antigos JSONB;
  v_dados_novos JSONB;
  v_operacao TEXT;
BEGIN
  -- Tentar pegar o usuário da sessão (via cq_auth JWT)
  BEGIN
    v_usuario_id := auth.uid();
    SELECT email INTO v_usuario_email FROM auth.users WHERE id = v_usuario_id;
  EXCEPTION WHEN OTHERS THEN
    v_usuario_id := NULL;
    v_usuario_email := 'system';
  END;

  IF TG_OP = 'DELETE' THEN
    v_operacao := 'DELETE';
    v_dados_antigos := to_jsonb(OLD);
    v_dados_novos := NULL;
  ELSIF TG_OP = 'UPDATE' THEN
    v_operacao := 'UPDATE';
    v_dados_antigos := to_jsonb(OLD);
    v_dados_novos := to_jsonb(NEW);
  ELSIF TG_OP = 'INSERT' THEN
    v_operacao := 'INSERT';
    v_dados_antigos := NULL;
    v_dados_novos := to_jsonb(NEW);
  END IF;

  INSERT INTO cq_audit_log (
    usuario_id, usuario_email, acao, tabela, operacao,
    registro_id, dados_antigos, dados_novos, criado_em
  ) VALUES (
    v_usuario_id, v_usuario_email,
    'cadastro_' || LOWER(v_operacao),
    TG_TABLE_NAME, v_operacao,
    COALESCE(NEW.id, OLD.id),
    v_dados_antigos, v_dados_novos,
    NOW()
  );

  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Aplicar trigger em TODAS as tabelas de cadastro mestre
DO $$
DECLARE
  v_tabela TEXT;
BEGIN
  FOR v_tabela IN
    SELECT unnest(ARRAY[
      'cadastros_atak', 'cq_produtos', 'cq_veiculos', 'cq_transportadoras',
      'cq_clientes', 'cq_unidades', 'cq_tipos_defeito', 'cq_ranking_defeitos',
      'cq_ranking_produtos', 'cq_parametros_inspecao', 'cq_usuarios',
      'cq_motivos_divergencia'
    ])
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_audit_%I ON %I', v_tabela, v_tabela);
    EXECUTE format(
      'CREATE TRIGGER trg_audit_%I
       AFTER INSERT OR UPDATE OR DELETE ON %I
       FOR EACH ROW EXECUTE FUNCTION fn_audit_cadastro_mestre()',
      v_tabela, v_tabela
    );
    RAISE NOTICE 'Trigger de auditoria criado para: %', v_tabela;
  END LOOP;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. GUARD: IMPEDIR DELETE REAL EM CADASTROS MESTRES
-- (Forçar soft-delete)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_soft_delete()
RETURNS TRIGGER AS $$
BEGIN
  -- Em vez de deletar, inativar
  RAISE EXCEPTION
    'DELETE proibido em cadastros mestres. Use UPDATE SET ativo = false. Tabela: %, ID: %',
    TG_TABLE_NAME, OLD.id
    USING HINT = 'Cadastros mestres usam soft-delete para preservar histórico.';
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Aplicar em tabelas de cadastro mestre (exceto staging/temp)
DO $$
DECLARE
  v_tabela TEXT;
BEGIN
  FOR v_tabela IN
    SELECT unnest(ARRAY[
      'cadastros_atak', 'cq_produtos', 'cq_veiculos', 'cq_transportadoras',
      'cq_clientes', 'cq_unidades', 'cq_tipos_defeito', 'cq_ranking_defeitos',
      'cq_ranking_produtos', 'cq_motivos_divergencia'
    ])
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_guard_delete_%I ON %I', v_tabela, v_tabela);
    EXECUTE format(
      'CREATE TRIGGER trg_guard_delete_%I
       BEFORE DELETE ON %I
       FOR EACH ROW EXECUTE FUNCTION fn_guard_soft_delete()',
      v_tabela, v_tabela
    );
  END LOOP;
END $$;

-- 6.1 Trigger: ao inativar, registrar quem/quando
CREATE OR REPLACE FUNCTION fn_registrar_inativacao()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.ativo = true AND NEW.ativo = false THEN
    NEW.inativado_em := NOW();
    -- inativado_por deve ser passado pelo frontend
  END IF;
  -- Reativação
  IF OLD.ativo = false AND NEW.ativo = true THEN
    NEW.inativado_em := NULL;
    NEW.inativado_por := NULL;
    NEW.motivo_inativacao := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  v_tabela TEXT;
BEGIN
  FOR v_tabela IN
    SELECT unnest(ARRAY[
      'cadastros_atak', 'cq_produtos', 'cq_veiculos', 'cq_transportadoras', 'cq_clientes'
    ])
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_inativacao_%I ON %I', v_tabela, v_tabela);
    EXECUTE format(
      'CREATE TRIGGER trg_inativacao_%I
       BEFORE UPDATE OF ativo ON %I
       FOR EACH ROW EXECUTE FUNCTION fn_registrar_inativacao()',
      v_tabela, v_tabela
    );
  END LOOP;
END $$;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. RLS PARA NOVAS TABELAS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_clientes ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_unidades ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_motivos_divergencia ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_status_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_dicionario_dados ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_cadastro_alias ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_cadastro_merge_log ENABLE ROW LEVEL SECURITY;

-- Leitura para todos autenticados
CREATE POLICY "read_clientes" ON cq_clientes FOR SELECT USING (true);
CREATE POLICY "read_unidades" ON cq_unidades FOR SELECT USING (true);
CREATE POLICY "read_motivos" ON cq_motivos_divergencia FOR SELECT USING (true);
CREATE POLICY "read_status" ON cq_status_config FOR SELECT USING (true);
CREATE POLICY "read_dicionario" ON cq_dicionario_dados FOR SELECT USING (true);
CREATE POLICY "read_alias" ON cq_cadastro_alias FOR SELECT USING (true);
CREATE POLICY "read_merge" ON cq_cadastro_merge_log FOR SELECT USING (true);

-- Escrita apenas para admin
CREATE POLICY "all_clientes" ON cq_clientes FOR ALL USING (true);
CREATE POLICY "all_unidades" ON cq_unidades FOR ALL USING (true);
CREATE POLICY "all_motivos" ON cq_motivos_divergencia FOR ALL USING (true);
CREATE POLICY "all_status" ON cq_status_config FOR ALL USING (true);
CREATE POLICY "all_dicionario" ON cq_dicionario_dados FOR ALL USING (true);
CREATE POLICY "all_alias" ON cq_cadastro_alias FOR ALL USING (true);
CREATE POLICY "all_merge" ON cq_cadastro_merge_log FOR ALL USING (true);


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 07 CONCLUÍDA';
  RAISE NOTICE '  ✓ cq_clientes (cadastro mestre)';
  RAISE NOTICE '  ✓ cq_unidades (cadastro mestre)';
  RAISE NOTICE '  ✓ cq_motivos_divergencia (cadastro mestre, 12 motivos)';
  RAISE NOTICE '  ✓ cq_status_config (modelo de status padronizado)';
  RAISE NOTICE '  ✓ cq_dicionario_dados (dicionário de dados)';
  RAISE NOTICE '  ✓ cq_cadastro_alias (merge de duplicatas)';
  RAISE NOTICE '  ✓ cq_cadastro_merge_log (rastreabilidade de merge)';
  RAISE NOTICE '  ✓ Soft-delete em 10 tabelas (DELETE proibido)';
  RAISE NOTICE '  ✓ Auditoria em 12 tabelas de cadastro mestre';
  RAISE NOTICE '  ✓ Trigger de inativação (quem/quando) em 5 tabelas';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
