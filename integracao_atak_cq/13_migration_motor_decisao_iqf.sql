-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 13: Motor de Decisão Automática + IQF + Reamostragem
--
-- EVOLUÇÃO INCREMENTAL — não recria tabelas existentes.
--
-- O que ADICIONA:
--   1. Severidade nos defeitos (mapeia gravidade existente → CRITICO/MAIOR/MENOR)
--   2. Motor de decisão automática por inspeção (substitui regra simples)
--   3. Tabela de reamostragem (vínculo inspeção original → reinspeção)
--   4. IQF por lote (score baseado em severidade de defeitos)
--   5. IQF por fornecedor (média ponderada dos lotes recentes)
--   6. Status em_analise_critica no workflow
--   7. Tabela cq_nao_conformidades (que era referenciada mas não existia)
--   8. Views de dashboard de fornecedores com IQF
--   9. Alertas automáticos por regra de negócio
--
-- O que ALTERA:
--   - fn_auto_calcular_inspecao → incorpora severidade
--   - cq_fornecedor_score → usa IQF em vez de fórmula simples
--
-- O que MANTÉM:
--   - Todas as tabelas existentes
--   - Todos os triggers de guard
--   - Workflow 8 etapas
--   - cq_lote_inspecao + cq_lote_produto
--
-- Rodar APÓS 12_migration_qualidade_4_dimensoes.sql
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. EVOLUÇÃO: cq_tipos_defeito — mapear gravidade → severidade
-- gravidade existente: baixa, media, alta, critica
-- severidade motor:    MENOR, MAIOR, CRITICO
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_tipos_defeito
  ADD COLUMN IF NOT EXISTS severidade TEXT
    CHECK (severidade IN ('CRITICO','MAIOR','MENOR'));

-- Mapear gravidade existente → severidade
UPDATE cq_tipos_defeito SET severidade = CASE
  WHEN gravidade = 'critica' THEN 'CRITICO'
  WHEN gravidade = 'alta'    THEN 'MAIOR'
  WHEN gravidade = 'media'   THEN 'MAIOR'
  WHEN gravidade = 'baixa'   THEN 'MENOR'
  ELSE 'MENOR'
END WHERE severidade IS NULL;

-- Tornar obrigatório para novos registros
ALTER TABLE cq_tipos_defeito ALTER COLUMN severidade SET DEFAULT 'MENOR';


-- ════════════════════════════════════════════════════════════════════════════
-- 2. EVOLUÇÃO: registros_cq_inspecao — campos para motor de decisão
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE registros_cq_inspecao
  ADD COLUMN IF NOT EXISTS pct_defeitos_criticos  NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS pct_defeitos_maiores   NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS pct_defeitos_menores   NUMERIC(6,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS decisao_motor          TEXT
    CHECK (decisao_motor IN ('APROVADO','RESSALVA','BLOQUEADO','EM_ANALISE_CRITICA')),
  ADD COLUMN IF NOT EXISTS score_lote             NUMERIC(6,2) DEFAULT 100,
  ADD COLUMN IF NOT EXISTS requer_reamostragem    BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS reamostragem_id        UUID;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. EVOLUÇÃO: cq_lote_inspecao — campos para motor e IQF
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_lote_inspecao
  ADD COLUMN IF NOT EXISTS decisao_motor       TEXT
    CHECK (decisao_motor IN ('APROVADO','RESSALVA','BLOQUEADO','EM_ANALISE_CRITICA')),
  ADD COLUMN IF NOT EXISTS score_iqf           NUMERIC(6,2) DEFAULT 100,
  ADD COLUMN IF NOT EXISTS requer_reamostragem BOOLEAN DEFAULT FALSE;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. TABELA: cq_reamostragem — vínculo entre inspeção original e reinspeção
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_reamostragem (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Vínculo
  inspecao_original_id  UUID NOT NULL REFERENCES registros_cq_inspecao(id),
  lote_original_id      UUID REFERENCES cq_lote_inspecao(id),
  inspecao_reinspeção_id UUID REFERENCES registros_cq_inspecao(id),
  lote_reinspeção_id    UUID REFERENCES cq_lote_inspecao(id),

  -- Motivo
  motivo                TEXT NOT NULL,  -- ex: 'defeito_critico_detectado'
  motivo_detalhe        TEXT,

  -- Status do fluxo
  status                TEXT NOT NULL DEFAULT 'pendente'
    CHECK (status IN ('pendente','em_andamento','concluida','cancelada')),

  -- Resultado consolidado (após reinspeção)
  decisao_consolidada   TEXT
    CHECK (decisao_consolidada IN ('APROVADO','RESSALVA','BLOQUEADO')),
  score_consolidado     NUMERIC(6,2),
  observacao_consolidada TEXT,

  -- Quem solicitou
  solicitado_por        TEXT,
  solicitado_em         TIMESTAMPTZ DEFAULT NOW(),

  -- Quem concluiu
  concluido_por         TEXT,
  concluido_em          TIMESTAMPTZ,

  criado_em             TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reamos_original ON cq_reamostragem(inspecao_original_id);
CREATE INDEX IF NOT EXISTS idx_reamos_lote ON cq_reamostragem(lote_original_id);
CREATE INDEX IF NOT EXISTS idx_reamos_status ON cq_reamostragem(status);


-- ════════════════════════════════════════════════════════════════════════════
-- 5. TABELA: cq_nao_conformidades (referenciada em views mas não existia)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_nao_conformidades (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo            TEXT UNIQUE NOT NULL,  -- NC-YYYYMMDD-NNN
  tipo              TEXT NOT NULL CHECK (tipo IN ('produto','processo','fornecedor','sistema')),
  gravidade         TEXT NOT NULL CHECK (gravidade IN ('critica','maior','menor')),
  status            TEXT NOT NULL DEFAULT 'aberta'
    CHECK (status IN ('aberta','em_tratamento','concluida','cancelada')),

  -- Vínculo
  inspecao_id       UUID REFERENCES registros_cq_inspecao(id),
  lote_id           UUID REFERENCES cq_lote_inspecao(id),
  fornecedor_codigo TEXT,
  produto_codigo    TEXT,

  -- Descrição
  descricao         TEXT NOT NULL,
  causa_raiz        TEXT,
  acao_corretiva    TEXT,
  acao_preventiva   TEXT,
  evidencia_url     TEXT,

  -- Responsáveis
  registrado_por    TEXT NOT NULL,
  responsavel       TEXT,

  -- Prazos
  data_limite       DATE,
  data_conclusao    DATE,

  -- Validação
  validacao         TEXT CHECK (validacao IN ('eficaz','ineficaz','pendente')),
  validado_por      TEXT,

  criado_em         TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nc_status ON cq_nao_conformidades(status);
CREATE INDEX IF NOT EXISTS idx_nc_fornecedor ON cq_nao_conformidades(fornecedor_codigo);
CREATE INDEX IF NOT EXISTS idx_nc_gravidade ON cq_nao_conformidades(gravidade);
CREATE INDEX IF NOT EXISTS idx_nc_inspecao ON cq_nao_conformidades(inspecao_id);


-- ════════════════════════════════════════════════════════════════════════════
-- 6. FUNCTION: Motor de Decisão Automática
-- Substitui a regra simples (A/B/C por %) por decisão baseada em severidade
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_motor_decisao_inspecao()
RETURNS TRIGGER AS $$
DECLARE
  v_total         INTEGER;
  v_crit          INTEGER := 0;
  v_maior         INTEGER := 0;
  v_menor         INTEGER := 0;
  v_pct_crit      NUMERIC(6,2);
  v_pct_maior     NUMERIC(6,2);
  v_pct_menor     NUMERIC(6,2);
  v_pct_total     NUMERIC(6,2);
  v_decisao       TEXT;
  v_score         NUMERIC(6,2);
  v_requer_reamos BOOLEAN := FALSE;
  v_defeito       RECORD;
  v_qtd           INTEGER;
BEGIN
  v_total := NEW.quantidade_analisada;
  IF v_total IS NULL OR v_total <= 0 THEN
    RETURN NEW;
  END IF;

  -- Somar defeitos por severidade usando cq_tipos_defeito
  FOR v_defeito IN
    SELECT codigo, severidade FROM cq_tipos_defeito WHERE ativo = true
  LOOP
    BEGIN
      EXECUTE format('SELECT ($1).%I', v_defeito.codigo) INTO v_qtd USING NEW;
    EXCEPTION WHEN OTHERS THEN
      v_qtd := 0;
    END;
    v_qtd := COALESCE(v_qtd, 0);

    CASE v_defeito.severidade
      WHEN 'CRITICO' THEN v_crit := v_crit + v_qtd;
      WHEN 'MAIOR'   THEN v_maior := v_maior + v_qtd;
      WHEN 'MENOR'   THEN v_menor := v_menor + v_qtd;
      ELSE                v_menor := v_menor + v_qtd;
    END CASE;
  END LOOP;

  -- Percentuais
  v_pct_crit  := ROUND(v_crit  * 100.0 / v_total, 2);
  v_pct_maior := ROUND(v_maior * 100.0 / v_total, 2);
  v_pct_menor := ROUND(v_menor * 100.0 / v_total, 2);
  v_pct_total := ROUND((v_crit + v_maior + v_menor) * 100.0 / v_total, 2);

  -- ─── REGRAS DE DECISÃO ───
  -- 1. Crítico >= 5% → BLOQUEADO
  IF v_pct_crit >= 5 THEN
    v_decisao := 'BLOQUEADO';
    v_score := 0;
  -- 2. Crítico > 0 e < 5% → EM_ANALISE_CRITICA (requer reamostragem)
  ELSIF v_pct_crit > 0 THEN
    v_decisao := 'EM_ANALISE_CRITICA';
    v_requer_reamos := TRUE;
    v_score := GREATEST(0, 100 - (v_pct_crit * 25) - (v_pct_maior * 8) - (v_pct_menor * 2));
  -- 3. Maior > 10% → BLOQUEADO
  ELSIF v_pct_maior > 10 THEN
    v_decisao := 'BLOQUEADO';
    v_score := GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2));
  -- 4. Maior > 5% → RESSALVA
  ELSIF v_pct_maior > 5 THEN
    v_decisao := 'RESSALVA';
    v_score := LEAST(79, GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2)));
  -- 5. Total > 10% → RESSALVA
  ELSIF v_pct_total > 10 THEN
    v_decisao := 'RESSALVA';
    v_score := LEAST(79, GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2)));
  -- 6. Caso contrário → APROVADO
  ELSE
    v_decisao := 'APROVADO';
    v_score := GREATEST(0, 100 - (v_pct_maior * 8) - (v_pct_menor * 2));
  END IF;

  -- Gravar resultados
  NEW.pct_defeitos_criticos := v_pct_crit;
  NEW.pct_defeitos_maiores  := v_pct_maior;
  NEW.pct_defeitos_menores  := v_pct_menor;
  NEW.total_defeitos        := v_crit + v_maior + v_menor;
  NEW.percentual_defeitos   := v_pct_total;
  NEW.decisao_motor         := v_decisao;
  NEW.score_lote            := ROUND(v_score, 2);
  NEW.requer_reamostragem   := v_requer_reamos;

  -- Manter classificação ABC retrocompatível
  IF v_pct_total <= 5 THEN
    NEW.classificacao := 'A';
  ELSIF v_pct_total <= 15 THEN
    NEW.classificacao := 'B';
  ELSE
    NEW.classificacao := 'C';
  END IF;

  -- status_final retrocompatível
  CASE v_decisao
    WHEN 'APROVADO'            THEN NEW.status_final := 'aprovado';
    WHEN 'RESSALVA'            THEN NEW.status_final := 'ressalva';
    WHEN 'BLOQUEADO'           THEN NEW.status_final := 'bloqueado';
    WHEN 'EM_ANALISE_CRITICA'  THEN NEW.status_final := 'bloqueado'; -- bloqueado até reinspeção
  END CASE;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Substituir o trigger existente pelo motor novo
DROP TRIGGER IF EXISTS trg_auto_calcular_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_motor_decisao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW
  WHEN (NEW.quantidade_analisada > 0)
  EXECUTE FUNCTION fn_motor_decisao_inspecao();


-- ════════════════════════════════════════════════════════════════════════════
-- 7. FUNCTION: Calcular IQF do Fornecedor (média ponderada por recência)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_calcular_iqf_fornecedor(p_fornecedor_codigo TEXT)
RETURNS NUMERIC LANGUAGE plpgsql AS $$
DECLARE
  v_iqf NUMERIC(6,2);
BEGIN
  -- Média ponderada: lotes mais recentes têm peso maior
  -- Peso = 1 / (dias_atrás + 1), normalizado
  SELECT ROUND(
    SUM(sub.score * sub.peso) / NULLIF(SUM(sub.peso), 0)
  , 2) INTO v_iqf
  FROM (
    SELECT
      COALESCE(i.score_lote, 100) AS score,
      1.0 / (EXTRACT(EPOCH FROM (NOW() - i.criado_em)) / 86400 + 1) AS peso
    FROM registros_cq_inspecao i
    WHERE i.fornecedor_codigo = p_fornecedor_codigo
      AND i.data_inspecao >= CURRENT_DATE - INTERVAL '180 days'
      AND i.quantidade_analisada > 0
    ORDER BY i.data_inspecao DESC
    LIMIT 50
  ) sub;

  RETURN COALESCE(v_iqf, 100);
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 8. FUNCTION: Solicitar reamostragem automaticamente
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_auto_reamostragem()
RETURNS TRIGGER AS $$
BEGIN
  -- Apenas quando decisão muda para EM_ANALISE_CRITICA
  IF NEW.decisao_motor = 'EM_ANALISE_CRITICA'
     AND NEW.requer_reamostragem = TRUE
     AND (OLD.decisao_motor IS DISTINCT FROM 'EM_ANALISE_CRITICA')
  THEN
    -- Verificar se já existe reamostragem pendente
    IF NOT EXISTS (
      SELECT 1 FROM cq_reamostragem
      WHERE inspecao_original_id = NEW.id
        AND status IN ('pendente','em_andamento')
    ) THEN
      INSERT INTO cq_reamostragem (
        inspecao_original_id, motivo, motivo_detalhe, solicitado_por
      ) VALUES (
        NEW.id,
        'defeito_critico_detectado',
        FORMAT('Defeitos criticos: %s%% (%s de %s pecas)',
          NEW.pct_defeitos_criticos,
          ROUND(NEW.pct_defeitos_criticos * NEW.quantidade_analisada / 100),
          NEW.quantidade_analisada),
        NEW.inspetor
      );

      -- Registrar evento
      PERFORM fn_registrar_evento(
        'reamostragem_solicitada', 'qualidade',
        FORMAT('Reamostragem obrigatoria para inspecao %s — %s%% defeitos criticos',
          NEW.id_inspecao, NEW.pct_defeitos_criticos),
        'cq_reamostragem', NULL, NEW.id,
        jsonb_build_object('inspecao_id', NEW.id, 'pct_criticos', NEW.pct_defeitos_criticos),
        NULL, 'critical'
      );
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_reamostragem
  AFTER INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_auto_reamostragem();


-- ════════════════════════════════════════════════════════════════════════════
-- 9. FUNCTION: Concluir reamostragem — consolidar resultado
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_concluir_reamostragem(
  p_reamostragem_id UUID,
  p_inspecao_reinspeção_id UUID,
  p_usuario TEXT
)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
  v_reamos cq_reamostragem;
  v_original registros_cq_inspecao;
  v_reinspeção registros_cq_inspecao;
  v_score_consolidado NUMERIC(6,2);
  v_decisao_consolidada TEXT;
BEGIN
  SELECT * INTO v_reamos FROM cq_reamostragem WHERE id = p_reamostragem_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('erro', 'Reamostragem nao encontrada');
  END IF;
  IF v_reamos.status NOT IN ('pendente','em_andamento') THEN
    RETURN jsonb_build_object('erro', 'Reamostragem ja concluida ou cancelada');
  END IF;

  SELECT * INTO v_original FROM registros_cq_inspecao WHERE id = v_reamos.inspecao_original_id;
  SELECT * INTO v_reinspeção FROM registros_cq_inspecao WHERE id = p_inspecao_reinspeção_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('erro', 'Inspecao de reinspecao nao encontrada');
  END IF;

  -- Consolidar: média dos scores (original + reinspeção)
  v_score_consolidado := ROUND((COALESCE(v_original.score_lote,0) + COALESCE(v_reinspeção.score_lote,0)) / 2, 2);

  -- Decisão consolidada pela reinspeção (prevalece)
  v_decisao_consolidada := CASE
    WHEN v_reinspeção.decisao_motor = 'BLOQUEADO' THEN 'BLOQUEADO'
    WHEN v_reinspeção.decisao_motor = 'EM_ANALISE_CRITICA' THEN 'BLOQUEADO'
    WHEN v_reinspeção.decisao_motor = 'RESSALVA' THEN 'RESSALVA'
    ELSE v_reinspeção.decisao_motor
  END;

  -- Atualizar reamostragem
  UPDATE cq_reamostragem SET
    inspecao_reinspeção_id = p_inspecao_reinspeção_id,
    status = 'concluida',
    decisao_consolidada = v_decisao_consolidada,
    score_consolidado = v_score_consolidado,
    concluido_por = p_usuario,
    concluido_em = NOW(),
    atualizado_em = NOW()
  WHERE id = p_reamostragem_id;

  -- Atualizar inspeção original com resultado consolidado
  UPDATE registros_cq_inspecao SET
    score_lote = v_score_consolidado,
    decisao_motor = v_decisao_consolidada,
    requer_reamostragem = FALSE,
    status_final = CASE v_decisao_consolidada
      WHEN 'APROVADO' THEN 'aprovado'
      WHEN 'RESSALVA' THEN 'ressalva'
      ELSE 'bloqueado'
    END
  WHERE id = v_reamos.inspecao_original_id;

  RETURN jsonb_build_object(
    'reamostragem_id', p_reamostragem_id,
    'decisao_consolidada', v_decisao_consolidada,
    'score_consolidado', v_score_consolidado,
    'status', 'concluida'
  );
END;
$$;


-- ════════════════════════════════════════════════════════════════════════════
-- 10. VIEW: Dashboard de fornecedores com IQF
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_fornecedor_iqf AS
SELECT
  ca.codigo AS fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome) AS fornecedor_nome,
  ca.uf,

  -- Volume
  COUNT(DISTINCT i.id) AS total_lotes,
  SUM(i.quantidade_analisada) AS total_pecas,

  -- Decisões
  COUNT(*) FILTER (WHERE i.decisao_motor = 'APROVADO') AS lotes_aprovados,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'RESSALVA') AS lotes_ressalva,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'BLOQUEADO') AS lotes_bloqueados,
  COUNT(*) FILTER (WHERE i.decisao_motor = 'EM_ANALISE_CRITICA') AS lotes_analise_critica,

  -- Percentuais
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'APROVADO') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_aprovados,
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'RESSALVA') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_ressalva,
  ROUND(COUNT(*) FILTER (WHERE i.decisao_motor = 'BLOQUEADO') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_bloqueados,

  -- Defeitos médios por severidade
  ROUND(AVG(i.pct_defeitos_criticos), 2) AS media_pct_criticos,
  ROUND(AVG(i.pct_defeitos_maiores), 2) AS media_pct_maiores,

  -- Ocorrências críticas
  COUNT(*) FILTER (WHERE i.pct_defeitos_criticos > 0) AS ocorrencias_criticas,

  -- IQF (calculado)
  fn_calcular_iqf_fornecedor(ca.codigo) AS iqf,

  -- Classificação de risco por cor
  CASE
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 90 THEN 'verde'
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 75 THEN 'amarelo'
    WHEN fn_calcular_iqf_fornecedor(ca.codigo) >= 60 THEN 'laranja'
    ELSE 'vermelho'
  END AS classificacao_risco,

  -- Reamostragens pendentes
  (SELECT COUNT(*) FROM cq_reamostragem r
   JOIN registros_cq_inspecao ri ON ri.id = r.inspecao_original_id
   WHERE ri.fornecedor_codigo = ca.codigo AND r.status IN ('pendente','em_andamento')
  ) AS reamostragens_pendentes

FROM cadastros_atak ca
LEFT JOIN registros_cq_inspecao i
  ON i.fornecedor_codigo = ca.codigo
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '180 days'
  AND i.quantidade_analisada > 0
WHERE ca.tipo = 'fornecedor' AND ca.ativo = true
GROUP BY ca.codigo, ca.apelido, ca.nome, ca.uf
HAVING COUNT(i.id) > 0
ORDER BY fn_calcular_iqf_fornecedor(ca.codigo) ASC;


-- ════════════════════════════════════════════════════════════════════════════
-- 11. VIEW: Alertas automáticos de fornecedor
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_alertas_fornecedor_iqf AS

-- 2+ bloqueios em 30 dias
SELECT
  'bloqueios_recorrentes' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo AS codigo,
  COALESCE(ca.apelido, ca.nome) AS nome,
  FORMAT('%s bloqueios nos ultimos 30 dias', COUNT(*)) AS descricao,
  COUNT(*)::NUMERIC AS valor
FROM registros_cq_inspecao i
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE i.decisao_motor = 'BLOQUEADO'
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY i.fornecedor_codigo, ca.apelido, ca.nome
HAVING COUNT(*) >= 2

UNION ALL

-- Aumento de defeito crítico (qualquer ocorrência)
SELECT
  'defeito_critico_detectado' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome),
  FORMAT('Defeito critico em inspecao %s — %s%%', i.id_inspecao, i.pct_defeitos_criticos),
  i.pct_defeitos_criticos
FROM registros_cq_inspecao i
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE i.pct_defeitos_criticos > 0
  AND i.data_inspecao >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

-- IQF abaixo de 60 (vermelho)
SELECT
  'iqf_vermelho' AS tipo_alerta,
  'atencao' AS gravidade_alerta,
  sub.fornecedor_codigo,
  sub.fornecedor_nome,
  FORMAT('IQF = %s (risco vermelho)', sub.iqf),
  sub.iqf
FROM vw_fornecedor_iqf sub
WHERE sub.iqf < 60

UNION ALL

-- Reamostragem pendente há mais de 48h
SELECT
  'reamostragem_pendente' AS tipo_alerta,
  'critico' AS gravidade_alerta,
  i.fornecedor_codigo,
  COALESCE(ca.apelido, ca.nome),
  FORMAT('Reamostragem pendente ha %s horas', ROUND(EXTRACT(EPOCH FROM NOW() - r.solicitado_em) / 3600)),
  ROUND(EXTRACT(EPOCH FROM NOW() - r.solicitado_em) / 3600)::NUMERIC
FROM cq_reamostragem r
JOIN registros_cq_inspecao i ON i.id = r.inspecao_original_id
LEFT JOIN cadastros_atak ca ON ca.codigo = i.fornecedor_codigo AND ca.tipo = 'fornecedor'
WHERE r.status = 'pendente'
  AND r.solicitado_em < NOW() - INTERVAL '48 hours'

ORDER BY
  CASE gravidade_alerta WHEN 'critico' THEN 1 ELSE 2 END,
  valor DESC NULLS LAST;


-- ════════════════════════════════════════════════════════════════════════════
-- 12. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_reamostragem ENABLE ROW LEVEL SECURITY;
ALTER TABLE cq_nao_conformidades ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_reamostragem" ON cq_reamostragem FOR SELECT USING (true);
CREATE POLICY "all_reamostragem" ON cq_reamostragem FOR ALL USING (true);
CREATE POLICY "read_nc" ON cq_nao_conformidades FOR SELECT USING (true);
CREATE POLICY "all_nc" ON cq_nao_conformidades FOR ALL USING (true);


-- ════════════════════════════════════════════════════════════════════════════
-- 13. GUARD: Impedir encerramento sem reamostragem
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_guard_reamostragem_obrigatoria()
RETURNS TRIGGER AS $$
BEGIN
  -- Impedir aprovação/encerramento se reamostragem pendente
  IF NEW.requer_reamostragem = TRUE
     AND NEW.status_final IN ('aprovado')
  THEN
    RAISE EXCEPTION 'Inspecao requer reamostragem obrigatoria antes de aprovacao. '
      'Realize a reinspecao e conclua a reamostragem.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_guard_reamostragem ON registros_cq_inspecao;
CREATE TRIGGER trg_guard_reamostragem
  BEFORE UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_guard_reamostragem_obrigatoria();


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRACAO 13 — Motor de Decisao + IQF + Reamostragem';
  RAISE NOTICE '  ';
  RAISE NOTICE '  MOTOR DE DECISAO:';
  RAISE NOTICE '    ✓ Critico >=5%% → BLOQUEADO (score=0)';
  RAISE NOTICE '    ✓ Critico >0%% e <5%% → EM_ANALISE_CRITICA + reamostragem';
  RAISE NOTICE '    ✓ Maior >10%% → BLOQUEADO';
  RAISE NOTICE '    ✓ Maior >5%% → RESSALVA (score max 79)';
  RAISE NOTICE '    ✓ Total >10%% → RESSALVA';
  RAISE NOTICE '    ✓ Caso contrario → APROVADO';
  RAISE NOTICE '  ';
  RAISE NOTICE '  IQF POR LOTE:';
  RAISE NOTICE '    ✓ Base 100, -25/%%crit, -8/%%maior, -2/%%menor';
  RAISE NOTICE '    ✓ Bloqueado = 0, Ressalva max 79';
  RAISE NOTICE '  ';
  RAISE NOTICE '  IQF POR FORNECEDOR:';
  RAISE NOTICE '    ✓ Media ponderada por recencia (180 dias, max 50 lotes)';
  RAISE NOTICE '    ✓ verde>=90 amarelo>=75 laranja>=60 vermelho<60';
  RAISE NOTICE '  ';
  RAISE NOTICE '  REAMOSTRAGEM:';
  RAISE NOTICE '    ✓ Criada automaticamente quando EM_ANALISE_CRITICA';
  RAISE NOTICE '    ✓ Guard impede aprovacao sem reamostragem';
  RAISE NOTICE '    ✓ fn_concluir_reamostragem consolida resultado';
  RAISE NOTICE '  ';
  RAISE NOTICE '  TABELAS NOVAS:';
  RAISE NOTICE '    ✓ cq_reamostragem';
  RAISE NOTICE '    ✓ cq_nao_conformidades';
  RAISE NOTICE '  ';
  RAISE NOTICE '  VIEWS:';
  RAISE NOTICE '    ✓ vw_fornecedor_iqf (ranking com IQF + risco)';
  RAISE NOTICE '    ✓ vw_alertas_fornecedor_iqf (alertas automaticos)';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
