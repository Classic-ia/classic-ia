-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — Sistema de Governanca de Cargos & Salarios
-- Tabelas do modulo (prefixo rh_cs_)
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- Sustenta o pacote de C&S de Maio/2026:
--   - Politica Corporativa de C&S (bandas, progressao, excecoes)
--   - Politica de Funcao de Confianca (4 cargos, 3 naturezas)
--   - Plano de Carreira por Niveis I/II/III
--   - Comite de C&S trimestral
--
-- Integracao: as tabelas linkam ao rh_funcionarios existente (629 colaboradores)
-- via funcionario_id. Nao duplica dados ja modelados em rh_funcionarios /
-- rh_cargos. Adiciona apenas o que o modulo de governanca exige.
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. FAMILIAS FUNCIONAIS ──────────────────────────────────────────────────
-- 10 familias da Classic: Manutencao, Administrativa, Financeira, RH,
-- Qualidade, Couros, Producao, Lideranca, Gestao, Aprendizagem
CREATE TABLE IF NOT EXISTS rh_cs_familias (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo          TEXT UNIQUE NOT NULL,
  nome            TEXT NOT NULL,
  descricao       TEXT,
  ordem           INT NOT NULL DEFAULT 0,
  ativo           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE rh_cs_familias IS 'Familias funcionais da Politica de C&S (10 familias da Classic).';

-- ── 2. EXTENSAO DE CARGO (familia + criticidade) ────────────────────────────
-- Complementa rh_cargos com atributos do modulo C&S
CREATE TABLE IF NOT EXISTS rh_cs_cargo_meta (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cargo_id        UUID NOT NULL UNIQUE REFERENCES rh_cargos(id) ON DELETE CASCADE,
  familia_id      UUID REFERENCES rh_cs_familias(id),
  criticidade     CHAR(1) CHECK (criticidade IN ('A','B','C')),
  tem_niveis      BOOLEAN NOT NULL DEFAULT FALSE,
  descricao_funcional TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE rh_cs_cargo_meta IS 'Metadados do cargo para o modulo C&S: familia, criticidade A/B/C, trilha I/II/III.';

-- ── 3. BANDAS SALARIAIS ────────────────────────────────────────────────────
-- Banda por (cargo, nivel) com vigencia (versionamento anual)
CREATE TABLE IF NOT EXISTS rh_cs_bandas (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cargo_id            UUID NOT NULL REFERENCES rh_cargos(id),
  nivel               CHAR(3) CHECK (nivel IN ('I','II','III') OR nivel IS NULL),
  salario_min         NUMERIC(12,2) NOT NULL,
  salario_referencia  NUMERIC(12,2) NOT NULL,
  salario_teto        NUMERIC(12,2) NOT NULL,
  vigencia_inicio     DATE NOT NULL,
  vigencia_fim        DATE,
  ata_aprovacao_id    UUID,
  observacao          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_banda_coerente
    CHECK (salario_min <= salario_referencia AND salario_referencia <= salario_teto),
  CONSTRAINT chk_vigencia_coerente
    CHECK (vigencia_fim IS NULL OR vigencia_fim >= vigencia_inicio)
);
CREATE INDEX IF NOT EXISTS idx_cs_bandas_cargo ON rh_cs_bandas(cargo_id);
CREATE INDEX IF NOT EXISTS idx_cs_bandas_vigencia ON rh_cs_bandas(vigencia_inicio, vigencia_fim);
COMMENT ON TABLE rh_cs_bandas IS 'Bandas salariais (min/referencia/teto) por cargo e nivel, com vigencia para versionamento.';

-- ── 4. DADOS C&S DO COLABORADOR (extensao 1:1) ─────────────────────────────
-- Adiciona ao rh_funcionarios: nivel, gratificacao, fiducia, area C&S
CREATE TABLE IF NOT EXISTS rh_cs_colaborador (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL UNIQUE REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  familia_id          UUID REFERENCES rh_cs_familias(id),
  nivel               CHAR(3) CHECK (nivel IN ('I','II','III') OR nivel IS NULL),
  data_no_nivel       DATE,
  gratif_funcao       NUMERIC(12,2) NOT NULL DEFAULT 0,
  fiducia_natureza    CHAR(1) CHECK (fiducia_natureza IN ('I','II','III') OR fiducia_natureza IS NULL),
  fiducia_termo_url   TEXT,
  area_cs             TEXT,
  centro_custo        TEXT,
  reporta_a_id        UUID REFERENCES rh_funcionarios(id),
  observacoes         TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_colab_func ON rh_cs_colaborador(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cs_colab_fam ON rh_cs_colaborador(familia_id);
CREATE INDEX IF NOT EXISTS idx_cs_colab_fid ON rh_cs_colaborador(fiducia_natureza) WHERE fiducia_natureza IS NOT NULL;
COMMENT ON TABLE rh_cs_colaborador IS 'Dados de C&S do colaborador: trilha I/II/III, gratificacao de funcao, natureza de fiducia.';
COMMENT ON COLUMN rh_cs_colaborador.fiducia_natureza IS 'I=Gestao Operacional; II=Gestao Patrimonial/Administrativa; III=Representacao Tecnica Externa';

-- ── 5. HISTORICO SALARIAL ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_historico_salarial (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  data_evento         DATE NOT NULL,
  salario_anterior    NUMERIC(12,2),
  salario_novo        NUMERIC(12,2) NOT NULL,
  gratif_anterior     NUMERIC(12,2) DEFAULT 0,
  gratif_nova         NUMERIC(12,2) DEFAULT 0,
  motivo              TEXT NOT NULL,
  tipo_evento         TEXT CHECK (tipo_evento IN (
                        'admissao','progressao','promocao','reajuste',
                        'fiducia','reversao_fiducia','excecao','correcao','dissidio'
                      )),
  ata_id              UUID,
  aditivo_url         TEXT,
  ciclo_ano           INT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID
);
CREATE INDEX IF NOT EXISTS idx_cs_hist_func ON rh_cs_historico_salarial(funcionario_id, data_evento DESC);
COMMENT ON TABLE rh_cs_historico_salarial IS 'Historico completo de salario e gratificacao com origem (ata, aditivo, motivo).';

-- ── 6. DOSSIE / DOCUMENTOS ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_documentos (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  tipo                TEXT NOT NULL CHECK (tipo IN (
                        'aditivo','termo_fiducia','descricao_funcional',
                        'comunicado','avaliacao','atestado','advertencia',
                        'parecer','outro'
                      )),
  data_documento      DATE NOT NULL,
  titulo              TEXT NOT NULL,
  descricao           TEXT,
  arquivo_url         TEXT,
  arquivo_nome        TEXT,
  versao              INT NOT NULL DEFAULT 1,
  substitui_id        UUID REFERENCES rh_cs_documentos(id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          UUID
);
CREATE INDEX IF NOT EXISTS idx_cs_doc_func ON rh_cs_documentos(funcionario_id, data_documento DESC);
CREATE INDEX IF NOT EXISTS idx_cs_doc_tipo ON rh_cs_documentos(tipo);
COMMENT ON TABLE rh_cs_documentos IS 'Dossie digital do colaborador. Versionamento via substitui_id (nao apaga, encadeia).';

-- ── 7. FICHA DE AVALIACAO ANUAL (estrutura — UI vira na Fase 2) ────────────
CREATE TABLE IF NOT EXISTS rh_cs_fichas_avaliacao (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  ciclo_ano           INT NOT NULL,
  bloco_tecnico       NUMERIC(5,2) CHECK (bloco_tecnico BETWEEN 0 AND 35),
  bloco_comportamental NUMERIC(5,2) CHECK (bloco_comportamental BETWEEN 0 AND 35),
  bloco_frequencia    NUMERIC(5,2) CHECK (bloco_frequencia BETWEEN 0 AND 15),
  bloco_seguranca     NUMERIC(5,2) CHECK (bloco_seguranca BETWEEN 0 AND 15),
  total               NUMERIC(5,2) GENERATED ALWAYS AS (
                        COALESCE(bloco_tecnico,0) + COALESCE(bloco_comportamental,0)
                        + COALESCE(bloco_frequencia,0) + COALESCE(bloco_seguranca,0)
                      ) STORED,
  percent_metas       NUMERIC(5,2),
  supervisor_id       UUID REFERENCES rh_funcionarios(id),
  data_assinatura     DATE,
  status              TEXT NOT NULL DEFAULT 'rascunho'
                      CHECK (status IN ('rascunho','enviada','validada','homologada')),
  observacoes         TEXT,
  anexos_jsonb        JSONB DEFAULT '[]'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(funcionario_id, ciclo_ano)
);
CREATE INDEX IF NOT EXISTS idx_cs_ficha_func ON rh_cs_fichas_avaliacao(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cs_ficha_ciclo ON rh_cs_fichas_avaliacao(ciclo_ano);
COMMENT ON TABLE rh_cs_fichas_avaliacao IS 'Ficha anual com 4 blocos (35+35+15+15=100). Total calculado automaticamente.';

-- ── 8. WORKFLOW DE PROGRESSAO I/II/III ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_progressoes (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  ciclo_ano           INT NOT NULL,
  nivel_anterior      CHAR(3),
  nivel_novo          CHAR(3),
  salario_anterior    NUMERIC(12,2),
  salario_proposto    NUMERIC(12,2),
  ficha_id            UUID REFERENCES rh_cs_fichas_avaliacao(id),
  status              TEXT NOT NULL DEFAULT 'elegivel'
                      CHECK (status IN (
                        'elegivel','bloqueado','recomendada',
                        'aprovada_rh','aprovada_comite','aprovada_diretoria',
                        'homologada','rejeitada'
                      )),
  motivo_bloqueio     TEXT,
  parecer_supervisor  TEXT,
  parecer_rh          TEXT,
  parecer_comite      TEXT,
  parecer_diretoria   TEXT,
  supervisor_id       UUID REFERENCES rh_funcionarios(id),
  rh_user_id          UUID,
  comite_user_id      UUID,
  diretoria_user_id   UUID,
  data_supervisor     TIMESTAMPTZ,
  data_rh             TIMESTAMPTZ,
  data_comite         TIMESTAMPTZ,
  data_diretoria      TIMESTAMPTZ,
  ata_id              UUID,
  aditivo_url         TEXT,
  s2206_xml_url       TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_prog_func ON rh_cs_progressoes(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cs_prog_ciclo ON rh_cs_progressoes(ciclo_ano, status);
COMMENT ON TABLE rh_cs_progressoes IS 'Workflow de progressao I/II/III: Supervisor -> RH -> Comite -> Diretoria.';

-- ── 9. EXCECOES ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_excecoes (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  ciclo_ano           INT NOT NULL,
  tipo                TEXT NOT NULL CHECK (tipo IN (
                        'salario_fora_banda','promocao_extraordinaria',
                        'admissao_acima_referencia','retencao_especial',
                        'bonus_pontual','alteracao_cargo','reversao_fiducia'
                      )),
  motivo              TEXT NOT NULL,
  parecer_tecnico     TEXT,
  valor_envolvido     NUMERIC(12,2),
  status              TEXT NOT NULL DEFAULT 'pendente'
                      CHECK (status IN ('pendente','aprovada','rejeitada')),
  aprovado_por        UUID,
  data_aprovacao      TIMESTAMPTZ,
  ata_id              UUID,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_exc_func ON rh_cs_excecoes(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cs_exc_ciclo ON rh_cs_excecoes(ciclo_ano, status);
COMMENT ON TABLE rh_cs_excecoes IS 'Excecoes salariais (7 tipos). Limite: 5% do quadro por exercicio.';

-- ── 10. COMITE DE C&S (reunioes e atas) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_reunioes_comite (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  data_reuniao        DATE NOT NULL,
  trimestre           CHAR(2) CHECK (trimestre IN ('Q1','Q2','Q3','Q4')),
  ano                 INT NOT NULL,
  participantes_jsonb JSONB NOT NULL DEFAULT '[]'::jsonb,
  pauta               TEXT,
  deliberacoes        TEXT,
  ata_url             TEXT,
  status              TEXT NOT NULL DEFAULT 'agendada'
                      CHECK (status IN ('agendada','realizada','cancelada')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_reuniao_data ON rh_cs_reunioes_comite(data_reuniao DESC);
COMMENT ON TABLE rh_cs_reunioes_comite IS 'Reunioes trimestrais do Comite de C&S (Jul/Out/Jan/Abr).';

-- ── 11. RELATORIOS MENSAIS DE FIDUCIA ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_relatorios_fiducia (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  mes                 INT NOT NULL CHECK (mes BETWEEN 1 AND 12),
  ano                 INT NOT NULL,
  decisoes            TEXT,
  alcadas_exercidas   TEXT,
  fatos_relevantes    TEXT,
  indicadores         JSONB,
  anexos_jsonb        JSONB DEFAULT '[]'::jsonb,
  status              TEXT NOT NULL DEFAULT 'pendente'
                      CHECK (status IN ('pendente','enviado','arquivado')),
  data_envio          TIMESTAMPTZ,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(funcionario_id, mes, ano)
);
CREATE INDEX IF NOT EXISTS idx_cs_rel_fid ON rh_cs_relatorios_fiducia(funcionario_id, ano, mes);
COMMENT ON TABLE rh_cs_relatorios_fiducia IS 'Relatorio mensal dos cargos com fiducia (Paulo, Itamar, Kimila, Wanderley).';

-- ── 12. BENEFICIOS (Modulo 11 — catalogo + elegibilidade) ──────────────────
CREATE TABLE IF NOT EXISTS rh_cs_beneficios (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo              TEXT UNIQUE NOT NULL,
  nome                TEXT NOT NULL,
  categoria           TEXT CHECK (categoria IN (
                        'legal_obrigatorio','cct','classic','por_cargo','por_desempenho'
                      )),
  tipo                TEXT CHECK (tipo IN ('monetario','espec','servico','evento')),
  valor_mensal_padrao NUMERIC(12,2),
  custo_empresa       NUMERIC(12,2),
  descricao           TEXT,
  exige_adesao        BOOLEAN NOT NULL DEFAULT FALSE,
  criterios_jsonb     JSONB DEFAULT '{}'::jsonb,
  ativo               BOOLEAN NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE rh_cs_beneficios IS 'Catalogo de beneficios da empresa com criterios de elegibilidade em JSONB.';

CREATE TABLE IF NOT EXISTS rh_cs_elegibilidade (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  beneficio_id        UUID NOT NULL REFERENCES rh_cs_beneficios(id) ON DELETE CASCADE,
  status              TEXT NOT NULL DEFAULT 'elegivel'
                      CHECK (status IN ('elegivel','inelegivel','suspenso','ativo','recusado')),
  data_inicio         DATE,
  data_fim            DATE,
  motivo_inelegibilidade TEXT,
  adesao_status       TEXT CHECK (adesao_status IN (
                        'pendente','aceito','recusado','nao_aplicavel'
                      )),
  termo_adesao_url    TEXT,
  valor_mensal        NUMERIC(12,2),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(funcionario_id, beneficio_id)
);
CREATE INDEX IF NOT EXISTS idx_cs_eleg_func ON rh_cs_elegibilidade(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_cs_eleg_ben ON rh_cs_elegibilidade(beneficio_id);
COMMENT ON TABLE rh_cs_elegibilidade IS 'Elegibilidade colaborador x beneficio, com adesao formal e historico.';

CREATE TABLE IF NOT EXISTS rh_cs_historico_beneficios (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id      UUID NOT NULL REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  beneficio_id        UUID NOT NULL REFERENCES rh_cs_beneficios(id),
  evento              TEXT NOT NULL,
  data_evento         DATE NOT NULL,
  valor               NUMERIC(12,2),
  observacao          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_hist_ben ON rh_cs_historico_beneficios(funcionario_id, data_evento DESC);

-- ── 13. ALERTAS DE GOVERNANCA ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_cs_alertas (
  id                  UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo                TEXT NOT NULL CHECK (tipo IN (
                        'fora_banda','progressao_pendente','excecao_limite',
                        'fiducia_relatorio_atrasado','elegibilidade_nova',
                        'elegibilidade_perdida','tempo_no_nivel_atingido',
                        'sinal_aderencia_politica','outro'
                      )),
  criticidade         TEXT NOT NULL DEFAULT 'media'
                      CHECK (criticidade IN ('baixa','media','alta','critica')),
  funcionario_id      UUID REFERENCES rh_funcionarios(id) ON DELETE CASCADE,
  beneficio_id        UUID REFERENCES rh_cs_beneficios(id),
  mensagem            TEXT NOT NULL,
  contexto_jsonb      JSONB DEFAULT '{}'::jsonb,
  resolvido           BOOLEAN NOT NULL DEFAULT FALSE,
  data_resolucao      TIMESTAMPTZ,
  resolvido_por       UUID,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cs_alerta_aberto ON rh_cs_alertas(criticidade, created_at DESC) WHERE resolvido = FALSE;
CREATE INDEX IF NOT EXISTS idx_cs_alerta_func ON rh_cs_alertas(funcionario_id) WHERE funcionario_id IS NOT NULL;
COMMENT ON TABLE rh_cs_alertas IS 'Alertas de governanca: salario fora da banda, progressao pendente, limite de excecoes, etc.';

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_01_TABELAS.sql
-- 14 tabelas | prefixo rh_cs_ | FK para rh_funcionarios existente
-- ════════════════════════════════════════════════════════════════════════════
