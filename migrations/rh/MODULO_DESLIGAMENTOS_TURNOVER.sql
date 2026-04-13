-- ================================================================
-- MODULO DESLIGAMENTOS & TURNOVER — Analise Estruturada
-- Classic RH · Supabase (PostgreSQL)
-- Executar no SQL Editor do Supabase
-- Depende de: schema_rh_sst_completo.sql (rh_funcionarios,
--   rh_desligamentos, rh_ausencias, rh_acidentes)
-- ================================================================


-- ══════════════════════════════════════════════════════════
-- 1. TAXONOMIA DE CAUSAS — 3 niveis hierarquicos
--    tipo > categoria > descricao
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_desligamento_taxonomia (
  id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo        TEXT NOT NULL,          -- nivel 1: pedido_demissao, demissao_empresa, etc.
  categoria   TEXT NOT NULL,          -- nivel 2: admissao, operacional, financeiro, pessoal, disciplinar
  descricao   TEXT NOT NULL,          -- nivel 3: descricao detalhada da causa
  ativo       BOOLEAN DEFAULT TRUE,
  ordem       INTEGER DEFAULT 0,      -- ordenacao visual
  created_at  TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tipo, categoria, descricao)
);

CREATE INDEX IF NOT EXISTS idx_taxonomia_tipo ON rh_desligamento_taxonomia(tipo);
CREATE INDEX IF NOT EXISTS idx_taxonomia_cat  ON rh_desligamento_taxonomia(categoria);

-- Dados padrao da taxonomia
INSERT INTO rh_desligamento_taxonomia (tipo, categoria, descricao, ordem) VALUES
  -- PEDIDO DE DEMISSAO
  ('pedido_demissao', 'admissao',    'Trabalho diferente do apresentado na contratacao',  1),
  ('pedido_demissao', 'admissao',    'Expectativa salarial nao atendida',                 2),
  ('pedido_demissao', 'admissao',    'Integracao insuficiente / falta de treinamento',    3),
  ('pedido_demissao', 'operacional', 'Condicoes de trabalho inadequadas',                 4),
  ('pedido_demissao', 'operacional', 'Excesso de carga / horas extras',                   5),
  ('pedido_demissao', 'operacional', 'Problemas com lideranca direta',                    6),
  ('pedido_demissao', 'operacional', 'Conflitos com colegas de equipe',                   7),
  ('pedido_demissao', 'operacional', 'Falta de equipamentos ou ferramentas',              8),
  ('pedido_demissao', 'financeiro',  'Proposta melhor no mercado',                        9),
  ('pedido_demissao', 'financeiro',  'Insatisfacao com beneficios',                       10),
  ('pedido_demissao', 'financeiro',  'Falta de perspectiva de crescimento',               11),
  ('pedido_demissao', 'pessoal',     'Mudanca de cidade / estado',                        12),
  ('pedido_demissao', 'pessoal',     'Motivos familiares ou de saude',                    13),
  ('pedido_demissao', 'pessoal',     'Retorno aos estudos',                               14),
  -- DEMISSAO PELA EMPRESA
  ('demissao_empresa', 'operacional', 'Baixo desempenho / produtividade',                 15),
  ('demissao_empresa', 'operacional', 'Inadequacao ao cargo',                             16),
  ('demissao_empresa', 'operacional', 'Reestruturacao do setor',                          17),
  ('demissao_empresa', 'disciplinar', 'Faltas injustificadas recorrentes',                18),
  ('demissao_empresa', 'disciplinar', 'Descumprimento de normas internas',                19),
  ('demissao_empresa', 'disciplinar', 'Advertencias acumuladas',                          20),
  ('demissao_empresa', 'financeiro',  'Reducao de quadro / corte de custos',              21),
  ('demissao_empresa', 'admissao',    'Nao aprovado no periodo de experiencia',           22),
  -- TERMINO DE CONTRATO
  ('termino_contrato', 'admissao',    'Fim do contrato de experiencia (45+45)',           23),
  ('termino_contrato', 'admissao',    'Contrato temporario encerrado',                    24),
  -- JUSTA CAUSA
  ('justa_causa', 'disciplinar', 'Falta grave conforme Art. 482 CLT',                     25),
  ('justa_causa', 'disciplinar', 'Abandono de emprego (30+ dias)',                         26),
  ('justa_causa', 'disciplinar', 'Insubordinacao grave',                                   27),
  -- ACORDO MUTUO
  ('acordo_mutuo', 'pessoal',     'Acordo amigavel (Art. 484-A CLT)',                     28),
  ('acordo_mutuo', 'financeiro',  'Acordo com incentivo financeiro',                      29)
ON CONFLICT (tipo, categoria, descricao) DO NOTHING;


-- ══════════════════════════════════════════════════════════
-- 2. CATEGORIAS VINCULADAS AO DESLIGAMENTO
--    Permite multiplas causas por desligamento com peso
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_desligamento_categorias (
  id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  desligamento_id  UUID NOT NULL REFERENCES rh_desligamentos(id) ON DELETE CASCADE,
  taxonomia_id     UUID REFERENCES rh_desligamento_taxonomia(id),
  nivel            TEXT NOT NULL CHECK (nivel IN ('tipo','categoria','descricao')),
  categoria        TEXT NOT NULL,
  subcategoria     TEXT,
  peso             NUMERIC(3,2) DEFAULT 1.00 CHECK (peso BETWEEN 0 AND 1),
  observacao       TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deslig_cat_deslig  ON rh_desligamento_categorias(desligamento_id);
CREATE INDEX IF NOT EXISTS idx_deslig_cat_cat     ON rh_desligamento_categorias(categoria);
CREATE INDEX IF NOT EXISTS idx_deslig_cat_nivel   ON rh_desligamento_categorias(nivel);


-- ══════════════════════════════════════════════════════════
-- 3. PERGUNTAS PADRAO DA ENTREVISTA DE SAIDA
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_entrevista_perguntas (
  id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo      TEXT UNIQUE NOT NULL,   -- p1, p2, p3, p4, p5 ...
  texto       TEXT NOT NULL,
  tipo_campo  TEXT DEFAULT 'texto' CHECK (tipo_campo IN ('texto','escala_1_5','sim_nao','multipla_escolha')),
  obrigatoria BOOLEAN DEFAULT TRUE,
  ordem       INTEGER NOT NULL,
  ativo       BOOLEAN DEFAULT TRUE,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO rh_entrevista_perguntas (codigo, texto, tipo_campo, ordem) VALUES
  ('p1', 'O trabalho foi diferente do que foi apresentado na contratacao?',           'sim_nao',     1),
  ('p2', 'O que mais incomodava no dia a dia de trabalho?',                           'texto',       2),
  ('p3', 'Como voce avalia a lideranca direta? (1 a 5)',                              'escala_1_5',  3),
  ('p4', 'Voce voltaria a trabalhar na empresa?',                                     'sim_nao',     4),
  ('p5', 'O que a empresa poderia ter feito para voce permanecer?',                   'texto',       5),
  ('p6', 'Voce se sentiu valorizado durante o tempo que trabalhou aqui?',             'escala_1_5',  6),
  ('p7', 'Recomendaria a empresa para um amigo ou familiar?',                         'sim_nao',     7),
  ('p8', 'Houve algum fator decisivo para o seu desligamento? Qual?',                 'texto',       8)
ON CONFLICT (codigo) DO NOTHING;


-- ══════════════════════════════════════════════════════════
-- 4. RESPOSTAS DA ENTREVISTA DE SAIDA (normalizada)
-- ══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rh_desligamento_entrevista (
  id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  desligamento_id  UUID NOT NULL REFERENCES rh_desligamentos(id) ON DELETE CASCADE,
  pergunta_id      UUID NOT NULL REFERENCES rh_entrevista_perguntas(id),
  pergunta_texto   TEXT NOT NULL,
  resposta         TEXT,
  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entrev_deslig ON rh_desligamento_entrevista(desligamento_id);
CREATE INDEX IF NOT EXISTS idx_entrev_perg   ON rh_desligamento_entrevista(pergunta_id);

-- Constraint: uma resposta por pergunta por desligamento
CREATE UNIQUE INDEX IF NOT EXISTS idx_entrev_unico
  ON rh_desligamento_entrevista(desligamento_id, pergunta_id);


-- ══════════════════════════════════════════════════════════
-- 5. TRIGGERS updated_at
-- ══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_desligamento_categorias',
    'rh_desligamento_entrevista',
    'rh_desligamento_taxonomia',
    'rh_entrevista_perguntas'
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
-- 6. RLS — Habilitar em todas as novas tabelas
-- ══════════════════════════════════════════════════════════

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rh_desligamento_taxonomia',
    'rh_desligamento_categorias',
    'rh_entrevista_perguntas',
    'rh_desligamento_entrevista'
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
-- 7. RPC: registrar_desligamento_completo()
--    Cria desligamento + categorias + entrevista em transacao
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION registrar_desligamento_completo(
  p_funcionario_id    UUID,
  p_data_desligamento DATE,
  p_tipo_desligamento TEXT,
  p_categoria_causa   TEXT,
  p_descricao         TEXT,
  p_categorias        JSONB DEFAULT '[]'::JSONB,
  -- formato: [{"taxonomia_id":"...", "nivel":"descricao", "categoria":"...", "subcategoria":"...", "peso":0.8, "observacao":"..."}]
  p_entrevista        JSONB DEFAULT '[]'::JSONB
  -- formato: [{"pergunta_id":"...", "resposta":"..."}]
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  v_desligamento_id UUID;
  v_cat             JSONB;
  v_resp            JSONB;
  v_pergunta_texto  TEXT;
  v_func            RECORD;
BEGIN
  -- Buscar dados do funcionario
  SELECT nome_completo, cargo, setor, salario_base, data_admissao
    INTO v_func
    FROM rh_funcionarios
   WHERE id = p_funcionario_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Funcionario % nao encontrado', p_funcionario_id;
  END IF;

  -- 1. Criar o registro de desligamento
  INSERT INTO rh_desligamentos (
    funcionario_id, data_desligamento, tipo_desligamento,
    categoria_causa, descricao
  ) VALUES (
    p_funcionario_id, p_data_desligamento, p_tipo_desligamento,
    p_categoria_causa, p_descricao
  )
  RETURNING id INTO v_desligamento_id;

  -- 2. Atualizar status do funcionario
  UPDATE rh_funcionarios
     SET status = 'inativo',
         data_desligamento = p_data_desligamento
   WHERE id = p_funcionario_id;

  -- 3. Inserir categorias (multicausal)
  FOR v_cat IN SELECT * FROM jsonb_array_elements(p_categorias)
  LOOP
    INSERT INTO rh_desligamento_categorias (
      desligamento_id, taxonomia_id, nivel, categoria,
      subcategoria, peso, observacao
    ) VALUES (
      v_desligamento_id,
      NULLIF(v_cat->>'taxonomia_id', '')::UUID,
      COALESCE(v_cat->>'nivel', 'descricao'),
      v_cat->>'categoria',
      v_cat->>'subcategoria',
      COALESCE((v_cat->>'peso')::NUMERIC, 1.00),
      v_cat->>'observacao'
    );
  END LOOP;

  -- 4. Inserir respostas da entrevista
  FOR v_resp IN SELECT * FROM jsonb_array_elements(p_entrevista)
  LOOP
    -- Buscar texto da pergunta
    SELECT texto INTO v_pergunta_texto
      FROM rh_entrevista_perguntas
     WHERE id = (v_resp->>'pergunta_id')::UUID;

    IF v_pergunta_texto IS NULL THEN
      v_pergunta_texto := 'Pergunta nao encontrada';
    END IF;

    INSERT INTO rh_desligamento_entrevista (
      desligamento_id, pergunta_id, pergunta_texto, resposta
    ) VALUES (
      v_desligamento_id,
      (v_resp->>'pergunta_id')::UUID,
      v_pergunta_texto,
      v_resp->>'resposta'
    );
  END LOOP;

  -- 5. Registrar no audit log
  INSERT INTO rh_audit_log (usuario_id, acao, tabela, registro_id, dados_depois)
  VALUES (
    auth.uid(),
    'desligamento_completo',
    'rh_desligamentos',
    v_desligamento_id::TEXT,
    jsonb_build_object(
      'funcionario', v_func.nome_completo,
      'cargo', v_func.cargo,
      'setor', v_func.setor,
      'tipo', p_tipo_desligamento,
      'causa', p_categoria_causa,
      'tempo_empresa_dias', (p_data_desligamento - v_func.data_admissao)
    )::TEXT
  );

  RETURN v_desligamento_id;
END;
$$;


-- ══════════════════════════════════════════════════════════
-- 8. RPC: indicadores_turnover(meses)
--    Retorna indicadores gerais, curto prazo, por causa e setor
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION indicadores_turnover(p_meses INT DEFAULT 12)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
AS $$
DECLARE
  v_data_inicio       DATE;
  v_total_ativos      INT;
  v_total_desligados  INT;
  v_deslig_curto      INT;   -- <90 dias
  v_turnover_geral    NUMERIC(5,2);
  v_turnover_curto    NUMERIC(5,2);
  v_tempo_medio_dias  NUMERIC(10,1);
  v_por_causa         JSONB;
  v_por_setor         JSONB;
  v_por_tipo          JSONB;
  v_por_mes           JSONB;
BEGIN
  v_data_inicio := CURRENT_DATE - (p_meses || ' months')::INTERVAL;

  -- Total de ativos (headcount medio ~ ativos agora + desligados no periodo / 2)
  SELECT COUNT(*) INTO v_total_ativos
    FROM rh_funcionarios
   WHERE status = 'ativo';

  -- Total desligados no periodo
  SELECT COUNT(*) INTO v_total_desligados
    FROM rh_desligamentos
   WHERE data_desligamento >= v_data_inicio;

  -- Desligados com menos de 90 dias de empresa
  SELECT COUNT(*) INTO v_deslig_curto
    FROM rh_desligamentos d
    JOIN rh_funcionarios f ON f.id = d.funcionario_id
   WHERE d.data_desligamento >= v_data_inicio
     AND (d.data_desligamento - f.data_admissao) < 90;

  -- Turnover geral = desligados / ((ativos + ativos+desligados) / 2) * 100
  IF (v_total_ativos + v_total_desligados) > 0 THEN
    v_turnover_geral := ROUND(
      (v_total_desligados::NUMERIC / ((v_total_ativos + (v_total_ativos + v_total_desligados)) / 2.0)) * 100,
      2
    );
  ELSE
    v_turnover_geral := 0;
  END IF;

  -- Turnover curto prazo (sobre total de desligados)
  IF v_total_desligados > 0 THEN
    v_turnover_curto := ROUND((v_deslig_curto::NUMERIC / v_total_desligados) * 100, 2);
  ELSE
    v_turnover_curto := 0;
  END IF;

  -- Tempo medio ate desligamento
  SELECT COALESCE(ROUND(AVG(d.data_desligamento - f.data_admissao), 1), 0)
    INTO v_tempo_medio_dias
    FROM rh_desligamentos d
    JOIN rh_funcionarios f ON f.id = d.funcionario_id
   WHERE d.data_desligamento >= v_data_inicio;

  -- Distribuicao por categoria de causa
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_por_causa
    FROM (
      SELECT categoria_causa AS causa,
             COUNT(*) AS total,
             ROUND(COUNT(*)::NUMERIC / NULLIF(v_total_desligados, 0) * 100, 1) AS percentual
        FROM rh_desligamentos
       WHERE data_desligamento >= v_data_inicio
       GROUP BY categoria_causa
       ORDER BY total DESC
    ) r;

  -- Distribuicao por tipo de desligamento
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_por_tipo
    FROM (
      SELECT tipo_desligamento AS tipo,
             COUNT(*) AS total,
             ROUND(COUNT(*)::NUMERIC / NULLIF(v_total_desligados, 0) * 100, 1) AS percentual
        FROM rh_desligamentos
       WHERE data_desligamento >= v_data_inicio
       GROUP BY tipo_desligamento
       ORDER BY total DESC
    ) r;

  -- Distribuicao por setor
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_por_setor
    FROM (
      SELECT f.setor,
             COUNT(*) AS desligados,
             (SELECT COUNT(*) FROM rh_funcionarios f2
               WHERE f2.setor = f.setor AND f2.status = 'ativo') AS ativos,
             ROUND(
               COUNT(*)::NUMERIC /
               NULLIF((SELECT COUNT(*) FROM rh_funcionarios f2
                 WHERE f2.setor = f.setor), 0) * 100,
               1
             ) AS taxa_turnover
        FROM rh_desligamentos d
        JOIN rh_funcionarios f ON f.id = d.funcionario_id
       WHERE d.data_desligamento >= v_data_inicio
       GROUP BY f.setor
       ORDER BY desligados DESC
    ) r;

  -- Evolucao mensal
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_por_mes
    FROM (
      SELECT TO_CHAR(data_desligamento, 'YYYY-MM') AS mes,
             COUNT(*) AS desligados,
             SUM(CASE WHEN tipo_desligamento = 'pedido_demissao' THEN 1 ELSE 0 END) AS voluntarios,
             SUM(CASE WHEN tipo_desligamento IN ('demissao_empresa','justa_causa') THEN 1 ELSE 0 END) AS involuntarios
        FROM rh_desligamentos
       WHERE data_desligamento >= v_data_inicio
       GROUP BY TO_CHAR(data_desligamento, 'YYYY-MM')
       ORDER BY mes
    ) r;

  RETURN jsonb_build_object(
    'periodo_meses',         p_meses,
    'data_inicio',           v_data_inicio,
    'total_ativos',          v_total_ativos,
    'total_desligados',      v_total_desligados,
    'desligados_curto_prazo', v_deslig_curto,
    'turnover_geral_pct',    v_turnover_geral,
    'turnover_curto_prazo_pct', v_turnover_curto,
    'tempo_medio_empresa_dias', v_tempo_medio_dias,
    'por_causa',             v_por_causa,
    'por_tipo',              v_por_tipo,
    'por_setor',             v_por_setor,
    'evolucao_mensal',       v_por_mes
  );
END;
$$;


-- ══════════════════════════════════════════════════════════
-- 9. RPC: analise_cruzada_desligamentos()
--    Cruza: tempo empresa, setor, produtividade, absenteismo
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION analise_cruzada_desligamentos(p_meses INT DEFAULT 12)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
AS $$
DECLARE
  v_data_inicio       DATE;
  v_faixa_tempo       JSONB;
  v_prod_desligados   JSONB;
  v_absent_desligados JSONB;
  v_setor_causa       JSONB;
  v_perfil_risco      JSONB;
BEGIN
  v_data_inicio := CURRENT_DATE - (p_meses || ' months')::INTERVAL;

  -- 1. Distribuicao por faixa de tempo de empresa
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_faixa_tempo
    FROM (
      SELECT
        CASE
          WHEN (d.data_desligamento - f.data_admissao) < 30  THEN '00-30 dias'
          WHEN (d.data_desligamento - f.data_admissao) < 90  THEN '31-90 dias'
          WHEN (d.data_desligamento - f.data_admissao) < 180 THEN '91-180 dias'
          WHEN (d.data_desligamento - f.data_admissao) < 365 THEN '181-365 dias'
          ELSE '365+ dias'
        END AS faixa,
        COUNT(*) AS total,
        ROUND(AVG(d.data_desligamento - f.data_admissao), 0) AS media_dias,
        STRING_AGG(DISTINCT d.tipo_desligamento, ', ') AS tipos_frequentes
      FROM rh_desligamentos d
      JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE d.data_desligamento >= v_data_inicio
      GROUP BY faixa
      ORDER BY MIN(d.data_desligamento - f.data_admissao)
    ) r;

  -- 2. Produtividade dos desligados vs media geral
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_prod_desligados
    FROM (
      SELECT
        f.setor,
        ROUND(AVG(p.percentual_atingimento), 1) AS media_prod_desligados,
        (SELECT ROUND(AVG(p2.percentual_atingimento), 1)
           FROM rh_producao p2
           JOIN rh_funcionarios f2 ON f2.id = p2.colaborador_id
          WHERE f2.setor = f.setor AND f2.status = 'ativo'
        ) AS media_prod_ativos,
        COUNT(DISTINCT f.id) AS total_desligados
      FROM rh_desligamentos d
      JOIN rh_funcionarios f ON f.id = d.funcionario_id
      LEFT JOIN rh_producao p ON p.colaborador_id = f.id
      WHERE d.data_desligamento >= v_data_inicio
        AND p.data_referencia IS NOT NULL
      GROUP BY f.setor
      HAVING COUNT(p.id) > 0
      ORDER BY media_prod_desligados ASC NULLS LAST
    ) r;

  -- 3. Absenteismo dos desligados (faltas nos 90 dias antes do desligamento)
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_absent_desligados
    FROM (
      SELECT
        d.tipo_desligamento AS tipo,
        COUNT(DISTINCT d.id) AS total_desligados,
        ROUND(AVG(sub.dias_ausencia), 1) AS media_dias_ausencia_90d,
        MAX(sub.dias_ausencia) AS max_dias_ausencia
      FROM rh_desligamentos d
      JOIN rh_funcionarios f ON f.id = d.funcionario_id
      LEFT JOIN LATERAL (
        SELECT COALESCE(SUM(
          CASE
            WHEN a.data_fim IS NOT NULL THEN (a.data_fim - a.data_inicio + 1)
            ELSE 1
          END
        ), 0) AS dias_ausencia
        FROM rh_ausencias a
        WHERE a.funcionario_id = f.id
          AND a.data_inicio >= (d.data_desligamento - INTERVAL '90 days')
          AND a.data_inicio <= d.data_desligamento
      ) sub ON TRUE
      WHERE d.data_desligamento >= v_data_inicio
      GROUP BY d.tipo_desligamento
      ORDER BY media_dias_ausencia_90d DESC
    ) r;

  -- 4. Cruzamento setor x causa
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_setor_causa
    FROM (
      SELECT
        f.setor,
        d.categoria_causa AS causa,
        COUNT(*) AS total,
        ROUND(AVG(d.data_desligamento - f.data_admissao), 0) AS media_dias_empresa
      FROM rh_desligamentos d
      JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE d.data_desligamento >= v_data_inicio
      GROUP BY f.setor, d.categoria_causa
      ORDER BY f.setor, total DESC
    ) r;

  -- 5. Perfil de risco (funcionarios ativos com indicadores similares aos desligados)
  SELECT COALESCE(jsonb_agg(row_to_json(r)), '[]'::JSONB)
    INTO v_perfil_risco
    FROM (
      SELECT
        f.id AS funcionario_id,
        f.nome_completo,
        f.setor,
        f.cargo,
        (CURRENT_DATE - f.data_admissao) AS dias_empresa,
        COALESCE(ausencias.total_dias, 0) AS dias_ausencia_90d,
        COALESCE(prod.media_prod, 0) AS media_produtividade,
        CASE
          WHEN (CURRENT_DATE - f.data_admissao) < 90
               AND COALESCE(ausencias.total_dias, 0) > 3 THEN 'alto'
          WHEN COALESCE(prod.media_prod, 0) < 60
               AND COALESCE(ausencias.total_dias, 0) > 5 THEN 'alto'
          WHEN COALESCE(prod.media_prod, 0) < 70
               OR COALESCE(ausencias.total_dias, 0) > 3 THEN 'medio'
          ELSE 'baixo'
        END AS nivel_risco
      FROM rh_funcionarios f
      LEFT JOIN LATERAL (
        SELECT COALESCE(SUM(
          CASE
            WHEN a.data_fim IS NOT NULL THEN (a.data_fim - a.data_inicio + 1)
            ELSE 1
          END
        ), 0) AS total_dias
        FROM rh_ausencias a
        WHERE a.funcionario_id = f.id
          AND a.data_inicio >= (CURRENT_DATE - INTERVAL '90 days')
      ) ausencias ON TRUE
      LEFT JOIN LATERAL (
        SELECT ROUND(AVG(p.percentual_atingimento), 1) AS media_prod
        FROM rh_producao p
        WHERE p.colaborador_id = f.id
          AND p.data_referencia >= (CURRENT_DATE - INTERVAL '90 days')
      ) prod ON TRUE
      WHERE f.status = 'ativo'
      ORDER BY
        CASE
          WHEN (CURRENT_DATE - f.data_admissao) < 90
               AND COALESCE(ausencias.total_dias, 0) > 3 THEN 1
          WHEN COALESCE(prod.media_prod, 0) < 60
               AND COALESCE(ausencias.total_dias, 0) > 5 THEN 1
          WHEN COALESCE(prod.media_prod, 0) < 70
               OR COALESCE(ausencias.total_dias, 0) > 3 THEN 2
          ELSE 3
        END,
        ausencias.total_dias DESC NULLS LAST
      LIMIT 20
    ) r;

  RETURN jsonb_build_object(
    'periodo_meses',              p_meses,
    'faixa_tempo_empresa',        v_faixa_tempo,
    'produtividade_desligados',   v_prod_desligados,
    'absenteismo_pre_deslig',     v_absent_desligados,
    'setor_x_causa',              v_setor_causa,
    'perfil_risco_ativos_top20',  v_perfil_risco
  );
END;
$$;


-- ══════════════════════════════════════════════════════════
-- 10. VIEW: vw_turnover_mensal
--     Taxa mensal de turnover
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_turnover_mensal AS
WITH meses AS (
  SELECT generate_series(
    DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months'),
    DATE_TRUNC('month', CURRENT_DATE),
    '1 month'::INTERVAL
  )::DATE AS mes
),
deslig_mes AS (
  SELECT
    DATE_TRUNC('month', d.data_desligamento)::DATE AS mes,
    COUNT(*) AS total_desligados,
    SUM(CASE WHEN d.tipo_desligamento = 'pedido_demissao' THEN 1 ELSE 0 END) AS voluntarios,
    SUM(CASE WHEN d.tipo_desligamento IN ('demissao_empresa','justa_causa') THEN 1 ELSE 0 END) AS involuntarios,
    SUM(CASE WHEN d.tipo_desligamento = 'acordo_mutuo' THEN 1 ELSE 0 END) AS acordos,
    SUM(CASE WHEN d.tipo_desligamento = 'termino_contrato' THEN 1 ELSE 0 END) AS fim_contrato
  FROM rh_desligamentos d
  WHERE d.data_desligamento >= CURRENT_DATE - INTERVAL '12 months'
  GROUP BY DATE_TRUNC('month', d.data_desligamento)::DATE
),
headcount_mes AS (
  SELECT
    m.mes,
    (SELECT COUNT(*) FROM rh_funcionarios f
      WHERE f.data_admissao <= (m.mes + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        AND (f.data_desligamento IS NULL OR f.data_desligamento >= m.mes)
    ) AS headcount
  FROM meses m
)
SELECT
  TO_CHAR(m.mes, 'YYYY-MM') AS competencia,
  m.mes AS data_referencia,
  COALESCE(h.headcount, 0) AS headcount,
  COALESCE(d.total_desligados, 0) AS desligados,
  COALESCE(d.voluntarios, 0) AS voluntarios,
  COALESCE(d.involuntarios, 0) AS involuntarios,
  COALESCE(d.acordos, 0) AS acordos,
  COALESCE(d.fim_contrato, 0) AS fim_contrato,
  CASE
    WHEN COALESCE(h.headcount, 0) > 0
    THEN ROUND(COALESCE(d.total_desligados, 0)::NUMERIC / h.headcount * 100, 2)
    ELSE 0
  END AS taxa_turnover_pct,
  CASE
    WHEN COALESCE(h.headcount, 0) > 0
    THEN ROUND(COALESCE(d.voluntarios, 0)::NUMERIC / h.headcount * 100, 2)
    ELSE 0
  END AS taxa_voluntario_pct,
  CASE
    WHEN COALESCE(h.headcount, 0) > 0
    THEN ROUND(COALESCE(d.involuntarios, 0)::NUMERIC / h.headcount * 100, 2)
    ELSE 0
  END AS taxa_involuntario_pct
FROM meses m
LEFT JOIN deslig_mes d ON d.mes = m.mes
LEFT JOIN headcount_mes h ON h.mes = m.mes
ORDER BY m.mes;


-- ══════════════════════════════════════════════════════════
-- 11. VIEW: vw_causas_desligamento
--     Distribuicao por categoria e subcategoria
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_causas_desligamento AS
SELECT
  d.categoria_causa AS causa_principal,
  dc.categoria AS causa_detalhada,
  dc.subcategoria,
  d.tipo_desligamento AS tipo,
  COUNT(*) AS total,
  ROUND(
    COUNT(*)::NUMERIC /
    NULLIF((SELECT COUNT(*) FROM rh_desligamentos), 0) * 100,
    1
  ) AS percentual,
  ROUND(AVG(d.data_desligamento - f.data_admissao), 0) AS media_dias_empresa,
  MIN(d.data_desligamento) AS primeira_ocorrencia,
  MAX(d.data_desligamento) AS ultima_ocorrencia
FROM rh_desligamentos d
JOIN rh_funcionarios f ON f.id = d.funcionario_id
LEFT JOIN rh_desligamento_categorias dc ON dc.desligamento_id = d.id
GROUP BY d.categoria_causa, dc.categoria, dc.subcategoria, d.tipo_desligamento
ORDER BY total DESC;


-- ══════════════════════════════════════════════════════════
-- 12. VIEW: vw_turnover_setor
--     Turnover por setor com ranking
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_turnover_setor AS
WITH setores AS (
  SELECT DISTINCT setor FROM rh_funcionarios WHERE setor IS NOT NULL
),
dados AS (
  SELECT
    s.setor,
    (SELECT COUNT(*) FROM rh_funcionarios f WHERE f.setor = s.setor AND f.status = 'ativo') AS ativos,
    (SELECT COUNT(*) FROM rh_funcionarios f WHERE f.setor = s.setor) AS total_historico,
    (SELECT COUNT(*)
       FROM rh_desligamentos d
       JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE f.setor = s.setor
        AND d.data_desligamento >= CURRENT_DATE - INTERVAL '12 months'
    ) AS desligados_12m,
    (SELECT COUNT(*)
       FROM rh_desligamentos d
       JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE f.setor = s.setor
        AND d.data_desligamento >= CURRENT_DATE - INTERVAL '12 months'
        AND (d.data_desligamento - f.data_admissao) < 90
    ) AS desligados_curto_prazo,
    (SELECT ROUND(AVG(d.data_desligamento - f.data_admissao), 0)
       FROM rh_desligamentos d
       JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE f.setor = s.setor
        AND d.data_desligamento >= CURRENT_DATE - INTERVAL '12 months'
    ) AS media_dias_empresa,
    (SELECT d.categoria_causa
       FROM rh_desligamentos d
       JOIN rh_funcionarios f ON f.id = d.funcionario_id
      WHERE f.setor = s.setor
        AND d.data_desligamento >= CURRENT_DATE - INTERVAL '12 months'
      GROUP BY d.categoria_causa
      ORDER BY COUNT(*) DESC
      LIMIT 1
    ) AS causa_principal
  FROM setores s
)
SELECT
  d.setor,
  d.ativos,
  d.total_historico,
  d.desligados_12m,
  d.desligados_curto_prazo,
  CASE
    WHEN d.total_historico > 0
    THEN ROUND(d.desligados_12m::NUMERIC / d.total_historico * 100, 1)
    ELSE 0
  END AS taxa_turnover_pct,
  CASE
    WHEN d.desligados_12m > 0
    THEN ROUND(d.desligados_curto_prazo::NUMERIC / d.desligados_12m * 100, 1)
    ELSE 0
  END AS pct_curto_prazo,
  d.media_dias_empresa,
  d.causa_principal,
  RANK() OVER (ORDER BY
    CASE WHEN d.total_historico > 0
         THEN d.desligados_12m::NUMERIC / d.total_historico
         ELSE 0 END DESC
  ) AS ranking_turnover
FROM dados d
WHERE d.total_historico > 0
ORDER BY taxa_turnover_pct DESC;


-- ══════════════════════════════════════════════════════════
-- 13. VIEW: vw_correlacao_produtividade
--     Desligados vs dados de produtividade
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_correlacao_produtividade AS
SELECT
  f.id AS funcionario_id,
  f.nome_completo,
  f.setor,
  f.cargo,
  d.data_desligamento,
  d.tipo_desligamento,
  d.categoria_causa,
  (d.data_desligamento - f.data_admissao) AS dias_empresa,
  -- Producao: media dos ultimos 3 meses antes do desligamento
  (SELECT ROUND(AVG(p.percentual_atingimento), 1)
     FROM rh_producao p
    WHERE p.colaborador_id = f.id
      AND p.data_referencia >= (d.data_desligamento - INTERVAL '90 days')
      AND p.data_referencia <= d.data_desligamento
  ) AS media_prod_90d,
  -- Producao: media total
  (SELECT ROUND(AVG(p.percentual_atingimento), 1)
     FROM rh_producao p
    WHERE p.colaborador_id = f.id
  ) AS media_prod_total,
  -- Tendencia: diferenca entre ultimo e primeiro trimestre
  (SELECT ROUND(AVG(p.percentual_atingimento), 1)
     FROM rh_producao p
    WHERE p.colaborador_id = f.id
      AND p.data_referencia >= (d.data_desligamento - INTERVAL '90 days')
      AND p.data_referencia <= d.data_desligamento
  ) -
  (SELECT ROUND(AVG(p.percentual_atingimento), 1)
     FROM rh_producao p
    WHERE p.colaborador_id = f.id
      AND p.data_referencia >= f.data_admissao
      AND p.data_referencia <= (f.data_admissao + INTERVAL '90 days')
  ) AS tendencia_producao,
  -- Ausencias nos 90 dias antes do desligamento
  (SELECT COALESCE(SUM(
    CASE WHEN a.data_fim IS NOT NULL THEN (a.data_fim - a.data_inicio + 1) ELSE 1 END
  ), 0)
     FROM rh_ausencias a
    WHERE a.funcionario_id = f.id
      AND a.data_inicio >= (d.data_desligamento - INTERVAL '90 days')
      AND a.data_inicio <= d.data_desligamento
  ) AS ausencias_90d,
  -- Media de produtividade do setor (ativos) para comparacao
  (SELECT ROUND(AVG(p.percentual_atingimento), 1)
     FROM rh_producao p
     JOIN rh_funcionarios f2 ON f2.id = p.colaborador_id
    WHERE f2.setor = f.setor AND f2.status = 'ativo'
      AND p.data_referencia >= CURRENT_DATE - INTERVAL '90 days'
  ) AS media_setor_ativos,
  -- Diferenca vs media do setor
  COALESCE(
    (SELECT ROUND(AVG(p.percentual_atingimento), 1)
       FROM rh_producao p
      WHERE p.colaborador_id = f.id
        AND p.data_referencia >= (d.data_desligamento - INTERVAL '90 days')
        AND p.data_referencia <= d.data_desligamento
    ) -
    (SELECT ROUND(AVG(p.percentual_atingimento), 1)
       FROM rh_producao p
       JOIN rh_funcionarios f2 ON f2.id = p.colaborador_id
      WHERE f2.setor = f.setor AND f2.status = 'ativo'
        AND p.data_referencia >= CURRENT_DATE - INTERVAL '90 days'
    ),
    0
  ) AS delta_vs_setor
FROM rh_desligamentos d
JOIN rh_funcionarios f ON f.id = d.funcionario_id
ORDER BY d.data_desligamento DESC;


-- ══════════════════════════════════════════════════════════
-- 14. VIEW AUXILIAR: vw_entrevista_consolidada
--     Respostas da entrevista de saida consolidadas
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_entrevista_consolidada AS
SELECT
  d.id AS desligamento_id,
  f.nome_completo,
  f.setor,
  f.cargo,
  d.data_desligamento,
  d.tipo_desligamento,
  ep.codigo AS pergunta_codigo,
  ep.texto AS pergunta,
  ep.tipo_campo,
  de.resposta,
  de.criado_em AS data_resposta
FROM rh_desligamento_entrevista de
JOIN rh_desligamentos d ON d.id = de.desligamento_id
JOIN rh_funcionarios f ON f.id = d.funcionario_id
JOIN rh_entrevista_perguntas ep ON ep.id = de.pergunta_id
ORDER BY d.data_desligamento DESC, ep.ordem;


-- ══════════════════════════════════════════════════════════
-- 15. VIEW AUXILIAR: vw_resumo_entrevistas
--     Estatisticas agregadas das entrevistas
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_resumo_entrevistas AS
SELECT
  ep.codigo,
  ep.texto AS pergunta,
  ep.tipo_campo,
  COUNT(de.id) AS total_respostas,
  -- Para escala 1-5: media
  CASE
    WHEN ep.tipo_campo = 'escala_1_5'
    THEN ROUND(AVG(NULLIF(de.resposta, '')::NUMERIC), 2)
    ELSE NULL
  END AS media_nota,
  -- Para sim/nao: percentual de sim
  CASE
    WHEN ep.tipo_campo = 'sim_nao'
    THEN ROUND(
      SUM(CASE WHEN LOWER(de.resposta) IN ('sim','s','yes','1','true') THEN 1 ELSE 0 END)::NUMERIC
      / NULLIF(COUNT(de.id), 0) * 100, 1)
    ELSE NULL
  END AS pct_sim,
  -- Para texto: ultimas 5 respostas
  CASE
    WHEN ep.tipo_campo = 'texto'
    THEN (SELECT jsonb_agg(sub.resposta)
          FROM (
            SELECT de2.resposta
            FROM rh_desligamento_entrevista de2
            WHERE de2.pergunta_id = ep.id AND de2.resposta IS NOT NULL AND de2.resposta != ''
            ORDER BY de2.criado_em DESC
            LIMIT 5
          ) sub)
    ELSE NULL
  END AS ultimas_respostas
FROM rh_entrevista_perguntas ep
LEFT JOIN rh_desligamento_entrevista de ON de.pergunta_id = ep.id
WHERE ep.ativo = TRUE
GROUP BY ep.id, ep.codigo, ep.texto, ep.tipo_campo, ep.ordem
ORDER BY ep.ordem;


-- ══════════════════════════════════════════════════════════
-- 16. INDICES ADICIONAIS PARA PERFORMANCE
-- ══════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_deslig_tipo     ON rh_desligamentos(tipo_desligamento);
CREATE INDEX IF NOT EXISTS idx_deslig_causa    ON rh_desligamentos(categoria_causa);
CREATE INDEX IF NOT EXISTS idx_deslig_func     ON rh_desligamentos(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_func_admissao   ON rh_funcionarios(data_admissao);
CREATE INDEX IF NOT EXISTS idx_func_deslig     ON rh_funcionarios(data_desligamento);
CREATE INDEX IF NOT EXISTS idx_prod_colab_data ON rh_producao(colaborador_id, data_referencia);
CREATE INDEX IF NOT EXISTS idx_ausencia_func_data ON rh_ausencias(funcionario_id, data_inicio);


-- ══════════════════════════════════════════════════════════
-- MODULO DESLIGAMENTOS & TURNOVER — PRONTO
-- ══════════════════════════════════════════════════════════
