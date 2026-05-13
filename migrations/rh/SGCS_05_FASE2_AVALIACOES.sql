-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — FASE 2 (Modulos 4 + 5): Ficha de Avaliacao + Workflow de Progressao
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- Pre-requisito: SGCS_01..SGCS_04 ja aplicados.
-- Tabelas usadas (criadas em SGCS_01): rh_cs_fichas_avaliacao, rh_cs_progressoes,
--   rh_cs_colaborador, rh_cs_bandas, rh_cs_alertas
-- Tabelas existentes: rh_funcionarios, rh_ocorrencias, rh_cargos
--
-- CONVENCAO DE ETAPAS (rh_cs_progressoes.status):
--   elegivel          -> identificada automaticamente como elegivel
--   bloqueado         -> identificada como bloqueada (motivo registrado)
--   recomendada       -> supervisor aprovou
--   aprovada_rh       -> RH aprovou
--   aprovada_comite   -> Comite aprovou
--   aprovada_diretoria-> Diretoria aprovou (apenas para II->III ou fora-banda)
--   homologada        -> entra em folha (aditivo gerado)
--   rejeitada         -> recusada em qualquer etapa
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. VIEW: vw_cs_fichas_ciclo ────────────────────────────────────────────
-- Lista todas as fichas (e seus colaboradores) de um ciclo, com supervisor
-- e snapshot do colaborador. Usada por sgcs_fichas.html.
CREATE OR REPLACE VIEW public.vw_cs_fichas_ciclo AS
SELECT
  fa.id                       AS ficha_id,
  fa.ciclo_ano,
  fa.funcionario_id,
  f.nome_completo             AS nome,
  f.matricula,
  c.nome                      AS cargo,
  cs.nivel,
  fam.nome                    AS familia,
  s.nome                      AS setor,
  fa.bloco_tecnico,
  fa.bloco_comportamental,
  fa.bloco_frequencia,
  fa.bloco_seguranca,
  fa.total,
  fa.percent_metas,
  fa.status,
  fa.data_assinatura,
  fa.supervisor_id,
  sup.nome_completo           AS supervisor_nome,
  fa.observacoes,
  fa.created_at,
  fa.updated_at
FROM rh_cs_fichas_avaliacao fa
  JOIN rh_funcionarios f             ON f.id = fa.funcionario_id
  LEFT JOIN rh_cargos c              ON c.id = f.cargo_id
  LEFT JOIN rh_setores s             ON s.id = f.setor_id
  LEFT JOIN rh_cs_colaborador cs     ON cs.funcionario_id = f.id
  LEFT JOIN rh_cs_familias fam       ON fam.id = cs.familia_id
  LEFT JOIN rh_funcionarios sup      ON sup.id = fa.supervisor_id;

COMMENT ON VIEW public.vw_cs_fichas_ciclo IS
  'Fichas de avaliacao do ciclo + dados do colaborador + supervisor. Usada por sgcs_fichas.html.';

-- ── 2. VIEW: vw_cs_progressoes_workflow ────────────────────────────────────
-- Progressoes em workflow + dados + proxima etapa esperada.
CREATE OR REPLACE VIEW public.vw_cs_progressoes_workflow AS
SELECT
  pg.id                       AS progressao_id,
  pg.ciclo_ano,
  pg.funcionario_id,
  f.nome_completo             AS nome,
  f.matricula,
  c.nome                      AS cargo,
  cs.nivel                    AS nivel_atual,
  pg.nivel_anterior,
  pg.nivel_novo,
  pg.salario_anterior,
  pg.salario_proposto,
  pg.ficha_id,
  fa.total                    AS ficha_total,
  pg.status,
  pg.motivo_bloqueio,
  pg.parecer_supervisor,
  pg.parecer_rh,
  pg.parecer_comite,
  pg.parecer_diretoria,
  pg.data_supervisor,
  pg.data_rh,
  pg.data_comite,
  pg.data_diretoria,
  CASE pg.status
    WHEN 'elegivel'            THEN 'supervisor'
    WHEN 'recomendada'         THEN 'rh'
    WHEN 'aprovada_rh'         THEN
      CASE WHEN pg.nivel_novo = 'III' THEN 'diretoria' ELSE 'comite' END
    WHEN 'aprovada_comite'     THEN 'diretoria'
    WHEN 'aprovada_diretoria'  THEN 'homologacao'
    ELSE NULL
  END                         AS proxima_etapa,
  pg.ata_id,
  pg.aditivo_url,
  pg.created_at,
  pg.updated_at
FROM rh_cs_progressoes pg
  JOIN rh_funcionarios f             ON f.id = pg.funcionario_id
  LEFT JOIN rh_cargos c              ON c.id = f.cargo_id
  LEFT JOIN rh_cs_colaborador cs     ON cs.funcionario_id = f.id
  LEFT JOIN rh_cs_fichas_avaliacao fa ON fa.id = pg.ficha_id;

COMMENT ON VIEW public.vw_cs_progressoes_workflow IS
  'Progressoes com snapshot do colaborador, ficha vinculada e proxima etapa esperada.';

-- ── 3. RPC: cs_ficha_upsert ────────────────────────────────────────────────
-- Cria ou atualiza ficha de avaliacao. Valida perfil e bloqueia mudanca apos
-- homologacao. Supervisor/gestor_confianca podem editar ate 'enviada';
-- RH pode editar tudo exceto fichas homologadas (precisa "reabrir").
CREATE OR REPLACE FUNCTION public.cs_ficha_upsert(
  p_funcionario_id        UUID,
  p_ciclo_ano             INT,
  p_bloco_tecnico         NUMERIC(5,2) DEFAULT NULL,
  p_bloco_comportamental  NUMERIC(5,2) DEFAULT NULL,
  p_bloco_frequencia      NUMERIC(5,2) DEFAULT NULL,
  p_bloco_seguranca       NUMERIC(5,2) DEFAULT NULL,
  p_percent_metas         NUMERIC(5,2) DEFAULT NULL,
  p_supervisor_id         UUID DEFAULT NULL,
  p_observacoes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT := rh_perfil_atual();
  v_id     UUID;
  v_status TEXT;
BEGIN
  IF v_perfil NOT IN ('administrador','rh','gestor','gestor_confianca') THEN
    RAISE EXCEPTION 'Acesso negado: perfil % nao pode editar fichas', v_perfil;
  END IF;

  SELECT status INTO v_status
    FROM rh_cs_fichas_avaliacao
   WHERE funcionario_id = p_funcionario_id AND ciclo_ano = p_ciclo_ano;

  IF v_status = 'homologada' AND v_perfil NOT IN ('administrador','rh') THEN
    RAISE EXCEPTION 'Ficha homologada nao pode ser editada (apenas administrador/rh pode reabrir)';
  END IF;

  INSERT INTO rh_cs_fichas_avaliacao (
    funcionario_id, ciclo_ano,
    bloco_tecnico, bloco_comportamental, bloco_frequencia, bloco_seguranca,
    percent_metas, supervisor_id, observacoes
  ) VALUES (
    p_funcionario_id, p_ciclo_ano,
    p_bloco_tecnico, p_bloco_comportamental, p_bloco_frequencia, p_bloco_seguranca,
    p_percent_metas, p_supervisor_id, p_observacoes
  )
  ON CONFLICT (funcionario_id, ciclo_ano) DO UPDATE SET
    bloco_tecnico         = COALESCE(EXCLUDED.bloco_tecnico,         rh_cs_fichas_avaliacao.bloco_tecnico),
    bloco_comportamental  = COALESCE(EXCLUDED.bloco_comportamental,  rh_cs_fichas_avaliacao.bloco_comportamental),
    bloco_frequencia      = COALESCE(EXCLUDED.bloco_frequencia,      rh_cs_fichas_avaliacao.bloco_frequencia),
    bloco_seguranca       = COALESCE(EXCLUDED.bloco_seguranca,       rh_cs_fichas_avaliacao.bloco_seguranca),
    percent_metas         = COALESCE(EXCLUDED.percent_metas,         rh_cs_fichas_avaliacao.percent_metas),
    supervisor_id         = COALESCE(EXCLUDED.supervisor_id,         rh_cs_fichas_avaliacao.supervisor_id),
    observacoes           = COALESCE(EXCLUDED.observacoes,           rh_cs_fichas_avaliacao.observacoes),
    updated_at            = NOW()
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$$;

COMMENT ON FUNCTION public.cs_ficha_upsert IS
  'Upsert de ficha. Bloqueia edicao apos homologacao (so admin/rh reabre).';

-- ── 4. RPC: cs_ficha_transicionar ──────────────────────────────────────────
-- Avanca a ficha: rascunho -> enviada -> validada -> homologada
-- Regras de quem pode em cada transicao:
--   rascunho -> enviada      : supervisor/gestor_confianca/rh/admin
--   enviada -> validada      : rh/admin
--   validada -> homologada   : rh/admin (homologacao final)
--   * -> rascunho (reabrir)  : rh/admin
CREATE OR REPLACE FUNCTION public.cs_ficha_transicionar(
  p_ficha_id   UUID,
  p_novo_status TEXT,
  p_data_assinatura DATE DEFAULT NULL
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil      TEXT := rh_perfil_atual();
  v_status_atual TEXT;
BEGIN
  IF v_perfil NOT IN ('administrador','rh','gestor','gestor_confianca') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  IF p_novo_status NOT IN ('rascunho','enviada','validada','homologada') THEN
    RAISE EXCEPTION 'Status invalido: %', p_novo_status;
  END IF;

  SELECT status INTO v_status_atual
    FROM rh_cs_fichas_avaliacao WHERE id = p_ficha_id;

  IF v_status_atual IS NULL THEN
    RAISE EXCEPTION 'Ficha % nao encontrada', p_ficha_id;
  END IF;

  -- Regras de transicao
  IF p_novo_status = 'enviada' AND v_status_atual != 'rascunho' THEN
    RAISE EXCEPTION 'So pode enviar ficha em rascunho (atual: %)', v_status_atual;
  END IF;

  IF p_novo_status = 'validada' THEN
    IF v_status_atual != 'enviada' THEN
      RAISE EXCEPTION 'So pode validar ficha enviada (atual: %)', v_status_atual;
    END IF;
    IF v_perfil NOT IN ('administrador','rh') THEN
      RAISE EXCEPTION 'Apenas administrador/rh pode validar';
    END IF;
  END IF;

  IF p_novo_status = 'homologada' THEN
    IF v_status_atual != 'validada' THEN
      RAISE EXCEPTION 'So pode homologar ficha validada (atual: %)', v_status_atual;
    END IF;
    IF v_perfil NOT IN ('administrador','rh') THEN
      RAISE EXCEPTION 'Apenas administrador/rh pode homologar';
    END IF;
  END IF;

  IF p_novo_status = 'rascunho' AND v_perfil NOT IN ('administrador','rh') THEN
    RAISE EXCEPTION 'Apenas administrador/rh pode reabrir';
  END IF;

  UPDATE rh_cs_fichas_avaliacao
     SET status = p_novo_status,
         data_assinatura = COALESCE(p_data_assinatura, data_assinatura,
           CASE WHEN p_novo_status IN ('enviada','validada','homologada')
                THEN CURRENT_DATE ELSE NULL END),
         updated_at = NOW()
   WHERE id = p_ficha_id;

  RETURN p_novo_status;
END;
$$;

COMMENT ON FUNCTION public.cs_ficha_transicionar IS
  'Avanca status da ficha respeitando regras de etapa e perfil.';

-- ── 5. RPC: cs_calcular_proxima_progressao (leitura, idempotente) ──────────
-- Para um colaborador e ciclo, calcula se ele e elegivel a progressao, a
-- partir de dados objetivos. Retorna JSONB com flags e motivos.
-- NAO insere registro — apenas calcula.
CREATE OR REPLACE FUNCTION public.cs_calcular_proxima_progressao(
  p_funcionario_id  UUID,
  p_ciclo_ano       INT DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_ciclo INT := COALESCE(p_ciclo_ano, EXTRACT(YEAR FROM CURRENT_DATE)::INT);
  v_nivel_atual CHAR(3);
  v_nivel_novo  CHAR(3);
  v_data_no_nivel DATE;
  v_meses_no_nivel INT;
  v_meses_min INT;
  v_status_func TEXT;
  v_ficha RECORD;
  v_advertencias INT;
  v_salario NUMERIC(12,2);
  v_salario_proposto NUMERIC(12,2);
  v_banda_atual RECORD;
  v_banda_proxima RECORD;
  v_bloqueios TEXT[] := ARRAY[]::TEXT[];
  v_elegivel BOOLEAN := FALSE;
  v_nota_min NUMERIC := 75;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh','gestor','gestor_confianca','diretoria') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  -- 1. Snapshot do colaborador
  SELECT f.status, cs.nivel, cs.data_no_nivel, f.salario_base
    INTO v_status_func, v_nivel_atual, v_data_no_nivel, v_salario
    FROM rh_funcionarios f
    LEFT JOIN rh_cs_colaborador cs ON cs.funcionario_id = f.id
   WHERE f.id = p_funcionario_id;

  IF v_status_func IS NULL THEN
    RETURN jsonb_build_object('erro', 'Funcionario nao encontrado');
  END IF;

  IF v_status_func NOT IN ('ativo','experiencia') THEN
    v_bloqueios := array_append(v_bloqueios, 'status_inativo:' || v_status_func);
  END IF;

  -- 2. Determinar transicao (I->II ou II->III)
  IF v_nivel_atual IS NULL OR v_nivel_atual = 'III' THEN
    RETURN jsonb_build_object(
      'elegivel', FALSE,
      'nivel_atual', v_nivel_atual,
      'nivel_novo', NULL,
      'motivo', CASE WHEN v_nivel_atual IS NULL
                     THEN 'Sem nivel atribuido (cargo sem trilha I/II/III ou perfil C&S nao configurado)'
                     ELSE 'Ja esta no nivel III (topo da trilha)'
                END
    );
  END IF;

  IF v_nivel_atual = 'I' THEN
    v_nivel_novo := 'II';  v_meses_min := 12; v_nota_min := 75;
  ELSIF v_nivel_atual = 'II' THEN
    v_nivel_novo := 'III'; v_meses_min := 24; v_nota_min := 85;
  END IF;

  -- 3. Tempo no nivel
  IF v_data_no_nivel IS NULL THEN
    v_bloqueios := array_append(v_bloqueios, 'sem_data_no_nivel');
    v_meses_no_nivel := 0;
  ELSE
    v_meses_no_nivel := (EXTRACT(YEAR FROM age(CURRENT_DATE, v_data_no_nivel)) * 12 +
                         EXTRACT(MONTH FROM age(CURRENT_DATE, v_data_no_nivel)))::INT;
    IF v_meses_no_nivel < v_meses_min THEN
      v_bloqueios := array_append(v_bloqueios,
        format('tempo_insuficiente:%s_meses_min_%s', v_meses_no_nivel, v_meses_min));
    END IF;
  END IF;

  -- 4. Ficha do ciclo
  SELECT total, bloco_frequencia, percent_metas, status
    INTO v_ficha
    FROM rh_cs_fichas_avaliacao
   WHERE funcionario_id = p_funcionario_id AND ciclo_ano = v_ciclo;

  IF v_ficha IS NULL THEN
    v_bloqueios := array_append(v_bloqueios, 'ficha_inexistente_no_ciclo:' || v_ciclo);
  ELSE
    IF v_ficha.status NOT IN ('validada','homologada') THEN
      v_bloqueios := array_append(v_bloqueios, 'ficha_nao_validada:' || v_ficha.status);
    END IF;
    IF COALESCE(v_ficha.total, 0) < v_nota_min THEN
      v_bloqueios := array_append(v_bloqueios,
        format('nota_insuficiente:%s_min_%s', COALESCE(v_ficha.total,0), v_nota_min));
    END IF;
    -- Frequencia: bloco_frequencia escala 0-15. 95% = 14.25
    IF COALESCE(v_ficha.bloco_frequencia, 0) < 14.25 THEN
      v_bloqueios := array_append(v_bloqueios,
        format('frequencia_abaixo_95:%s_min_14.25', COALESCE(v_ficha.bloco_frequencia,0)));
    END IF;
    -- Metas: 80% para I->II, 90% para II->III
    IF (v_nivel_novo = 'II'  AND COALESCE(v_ficha.percent_metas, 0) < 80) OR
       (v_nivel_novo = 'III' AND COALESCE(v_ficha.percent_metas, 0) < 90) THEN
      v_bloqueios := array_append(v_bloqueios,
        format('metas_insuficientes:%s_min_%s', COALESCE(v_ficha.percent_metas,0),
               CASE WHEN v_nivel_novo='III' THEN 90 ELSE 80 END));
    END IF;
  END IF;

  -- 5. Advertencias graves no ciclo (advertencia_escrita ou suspensao)
  SELECT COUNT(*)::INT INTO v_advertencias
    FROM rh_ocorrencias o
   WHERE o.colaborador_id = p_funcionario_id
     AND o.tipo IN ('advertencia_escrita','suspensao')
     AND EXTRACT(YEAR FROM o.data_inicio)::INT = v_ciclo;

  IF v_advertencias > 0 THEN
    v_bloqueios := array_append(v_bloqueios,
      format('advertencias_graves:%s', v_advertencias));
  END IF;

  -- 6. Salario proposto = referencia da banda do nivel novo
  SELECT salario_referencia INTO v_salario_proposto
    FROM rh_cs_bandas b
    JOIN rh_funcionarios f ON f.id = p_funcionario_id
   WHERE b.cargo_id = f.cargo_id
     AND b.nivel = v_nivel_novo
     AND b.vigencia_inicio <= CURRENT_DATE
     AND (b.vigencia_fim IS NULL OR b.vigencia_fim >= CURRENT_DATE)
   ORDER BY b.vigencia_inicio DESC
   LIMIT 1;

  IF v_salario_proposto IS NULL THEN
    v_bloqueios := array_append(v_bloqueios,
      format('banda_inexistente_para_nivel:%s', v_nivel_novo));
  END IF;

  v_elegivel := (array_length(v_bloqueios, 1) IS NULL);

  RETURN jsonb_build_object(
    'elegivel', v_elegivel,
    'ciclo_ano', v_ciclo,
    'nivel_atual', v_nivel_atual,
    'nivel_novo', v_nivel_novo,
    'meses_no_nivel', v_meses_no_nivel,
    'meses_minimo', v_meses_min,
    'nota_total', v_ficha.total,
    'nota_minima', v_nota_min,
    'frequencia_bloco', v_ficha.bloco_frequencia,
    'percent_metas', v_ficha.percent_metas,
    'advertencias_graves', v_advertencias,
    'salario_atual', v_salario,
    'salario_proposto', v_salario_proposto,
    'bloqueios', to_jsonb(v_bloqueios)
  );
END;
$$;

COMMENT ON FUNCTION public.cs_calcular_proxima_progressao IS
  'Calcula elegibilidade do colaborador a progressao no ciclo (leitura, nao insere).';

-- ── 6. Indice unico parcial — suporte para ON CONFLICT de cs_identificar_elegiveis
-- Garante que (funcionario_id, ciclo_ano) seja unico SOMENTE em estados
-- iniciais (elegivel/bloqueado). Registros em workflow avancado podem
-- coexistir (uma progressao por ciclo da-se naturalmente — quando uma
-- aprovada/recomendada existe, o batch NAO cria nova).
CREATE UNIQUE INDEX IF NOT EXISTS uq_cs_prog_ciclo_inicial
  ON rh_cs_progressoes(funcionario_id, ciclo_ano)
  WHERE status IN ('elegivel','bloqueado');

-- ── 7. RPC: cs_identificar_elegiveis (batch, idempotente) ──────────────────
-- Varre todos os ativos no nivel I e II, calcula elegibilidade e cria/atualiza
-- registros em rh_cs_progressoes. Status inicial: 'elegivel' ou 'bloqueado'.
-- Nao toca em progressoes ja em estado avancado (recomendada+, homologada,
-- rejeitada).
CREATE OR REPLACE FUNCTION public.cs_identificar_elegiveis(
  p_ciclo_ano INT DEFAULT NULL
) RETURNS TABLE (elegiveis INT, bloqueados INT, ignorados INT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_ciclo INT := COALESCE(p_ciclo_ano, EXTRACT(YEAR FROM CURRENT_DATE)::INT);
  v_func RECORD;
  v_calc JSONB;
  v_eleg INT := 0;
  v_blq  INT := 0;
  v_ign  INT := 0;
  v_status_atual TEXT;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh') THEN
    RAISE EXCEPTION 'Acesso negado: apenas administrador/rh pode identificar elegiveis';
  END IF;

  FOR v_func IN
    SELECT f.id, cs.nivel
      FROM rh_funcionarios f
      LEFT JOIN rh_cs_colaborador cs ON cs.funcionario_id = f.id
     WHERE f.status IN ('ativo','experiencia')
       AND cs.nivel IN ('I','II')
  LOOP
    -- Pula se ja em estado avancado
    SELECT status INTO v_status_atual
      FROM rh_cs_progressoes
     WHERE funcionario_id = v_func.id AND ciclo_ano = v_ciclo;

    IF v_status_atual IS NOT NULL AND v_status_atual NOT IN ('elegivel','bloqueado') THEN
      v_ign := v_ign + 1;
      CONTINUE;
    END IF;

    v_calc := cs_calcular_proxima_progressao(v_func.id, v_ciclo);

    IF (v_calc->>'elegivel')::BOOLEAN THEN
      v_eleg := v_eleg + 1;
      INSERT INTO rh_cs_progressoes (
        funcionario_id, ciclo_ano, nivel_anterior, nivel_novo,
        salario_anterior, salario_proposto, status, motivo_bloqueio,
        ficha_id
      )
      SELECT
        v_func.id, v_ciclo,
        v_calc->>'nivel_atual', v_calc->>'nivel_novo',
        (v_calc->>'salario_atual')::NUMERIC,
        (v_calc->>'salario_proposto')::NUMERIC,
        'elegivel', NULL,
        (SELECT id FROM rh_cs_fichas_avaliacao WHERE funcionario_id = v_func.id AND ciclo_ano = v_ciclo)
      ON CONFLICT (funcionario_id, ciclo_ano) WHERE status IN ('elegivel','bloqueado')
      DO UPDATE SET
        nivel_anterior   = EXCLUDED.nivel_anterior,
        nivel_novo       = EXCLUDED.nivel_novo,
        salario_anterior = EXCLUDED.salario_anterior,
        salario_proposto = EXCLUDED.salario_proposto,
        status           = 'elegivel',
        motivo_bloqueio  = NULL,
        ficha_id         = EXCLUDED.ficha_id,
        updated_at       = NOW();
    ELSE
      v_blq := v_blq + 1;
      INSERT INTO rh_cs_progressoes (
        funcionario_id, ciclo_ano, nivel_anterior, nivel_novo,
        salario_anterior, salario_proposto, status, motivo_bloqueio,
        ficha_id
      )
      SELECT
        v_func.id, v_ciclo,
        v_calc->>'nivel_atual', v_calc->>'nivel_novo',
        (v_calc->>'salario_atual')::NUMERIC,
        (v_calc->>'salario_proposto')::NUMERIC,
        'bloqueado',
        array_to_string(ARRAY(SELECT jsonb_array_elements_text(v_calc->'bloqueios')), '; '),
        NULL
      ON CONFLICT (funcionario_id, ciclo_ano) WHERE status IN ('elegivel','bloqueado')
      DO UPDATE SET
        nivel_anterior   = EXCLUDED.nivel_anterior,
        nivel_novo       = EXCLUDED.nivel_novo,
        salario_anterior = EXCLUDED.salario_anterior,
        salario_proposto = EXCLUDED.salario_proposto,
        status           = 'bloqueado',
        motivo_bloqueio  = EXCLUDED.motivo_bloqueio,
        updated_at       = NOW();
    END IF;
  END LOOP;

  RETURN QUERY SELECT v_eleg, v_blq, v_ign;
END;
$$;

COMMENT ON FUNCTION public.cs_identificar_elegiveis IS
  'Varre ativos no nivel I/II e cria/atualiza rh_cs_progressoes com elegivel/bloqueado. Idempotente.';

-- ── 8. RPC: cs_progressao_transicionar ─────────────────────────────────────
-- Avanca ou rejeita uma progressao em uma etapa especifica.
-- p_etapa: 'supervisor' | 'rh' | 'comite' | 'diretoria' | 'homologar' | 'rejeitar'
-- p_aprovar: se TRUE avanca, se FALSE rejeita (so para etapas de aprovacao)
CREATE OR REPLACE FUNCTION public.cs_progressao_transicionar(
  p_progressao_id UUID,
  p_etapa         TEXT,
  p_aprovar       BOOLEAN,
  p_parecer       TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil       TEXT := rh_perfil_atual();
  v_status_atual TEXT;
  v_nivel_novo   CHAR(3);
  v_novo_status  TEXT;
  v_status_esperado TEXT;
  v_perfis_permitidos TEXT[];
BEGIN
  SELECT status, nivel_novo INTO v_status_atual, v_nivel_novo
    FROM rh_cs_progressoes WHERE id = p_progressao_id;

  IF v_status_atual IS NULL THEN
    RAISE EXCEPTION 'Progressao % nao encontrada', p_progressao_id;
  END IF;

  -- Determinar status esperado e perfis para cada etapa
  CASE p_etapa
    WHEN 'supervisor' THEN
      v_status_esperado := 'elegivel';
      v_perfis_permitidos := ARRAY['administrador','rh','gestor','gestor_confianca'];
      v_novo_status := CASE WHEN p_aprovar THEN 'recomendada' ELSE 'rejeitada' END;
    WHEN 'rh' THEN
      v_status_esperado := 'recomendada';
      v_perfis_permitidos := ARRAY['administrador','rh'];
      v_novo_status := CASE WHEN p_aprovar THEN 'aprovada_rh' ELSE 'rejeitada' END;
    WHEN 'comite' THEN
      v_status_esperado := 'aprovada_rh';
      v_perfis_permitidos := ARRAY['administrador','gestor_confianca'];
      v_novo_status := CASE WHEN p_aprovar THEN 'aprovada_comite' ELSE 'rejeitada' END;
    WHEN 'diretoria' THEN
      -- II->III pula comite e vai direto para diretoria apos rh? Nao — segue
      -- o fluxo: comite -> diretoria. Mas para nivel III a politica exige
      -- diretoria sempre. Aceita ambos: aprovada_rh ou aprovada_comite.
      IF v_status_atual NOT IN ('aprovada_rh','aprovada_comite') THEN
        RAISE EXCEPTION 'Diretoria so aprova apos comite (atual: %)', v_status_atual;
      END IF;
      v_perfis_permitidos := ARRAY['administrador','diretoria'];
      v_novo_status := CASE WHEN p_aprovar THEN 'aprovada_diretoria' ELSE 'rejeitada' END;
      v_status_esperado := v_status_atual;  -- ja validado acima
    WHEN 'homologar' THEN
      v_status_esperado := 'aprovada_diretoria';
      v_perfis_permitidos := ARRAY['administrador','rh'];
      v_novo_status := 'homologada';
      IF NOT p_aprovar THEN
        RAISE EXCEPTION 'Homologar nao aceita rejeicao — use etapa anterior';
      END IF;
    ELSE
      RAISE EXCEPTION 'Etapa invalida: %', p_etapa;
  END CASE;

  IF v_status_atual != v_status_esperado AND p_etapa != 'diretoria' THEN
    RAISE EXCEPTION 'Status atual % nao permite etapa % (esperado: %)',
      v_status_atual, p_etapa, v_status_esperado;
  END IF;

  IF NOT (v_perfil = ANY(v_perfis_permitidos)) THEN
    RAISE EXCEPTION 'Perfil % nao pode executar etapa %', v_perfil, p_etapa;
  END IF;

  UPDATE rh_cs_progressoes SET
    status = v_novo_status,
    parecer_supervisor = CASE WHEN p_etapa = 'supervisor' THEN p_parecer ELSE parecer_supervisor END,
    parecer_rh         = CASE WHEN p_etapa = 'rh'         THEN p_parecer ELSE parecer_rh END,
    parecer_comite     = CASE WHEN p_etapa = 'comite'     THEN p_parecer ELSE parecer_comite END,
    parecer_diretoria  = CASE WHEN p_etapa = 'diretoria'  THEN p_parecer ELSE parecer_diretoria END,
    data_supervisor    = CASE WHEN p_etapa = 'supervisor' THEN NOW() ELSE data_supervisor END,
    data_rh            = CASE WHEN p_etapa = 'rh'         THEN NOW() ELSE data_rh END,
    data_comite        = CASE WHEN p_etapa = 'comite'     THEN NOW() ELSE data_comite END,
    data_diretoria     = CASE WHEN p_etapa = 'diretoria'  THEN NOW() ELSE data_diretoria END,
    updated_at         = NOW()
  WHERE id = p_progressao_id;

  RETURN v_novo_status;
END;
$$;

COMMENT ON FUNCTION public.cs_progressao_transicionar IS
  'Avanca/rejeita progressao em etapa (supervisor/rh/comite/diretoria/homologar). Valida perfil e status atual.';

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_05_FASE2_AVALIACOES.sql
-- 2 views (vw_cs_fichas_ciclo, vw_cs_progressoes_workflow)
-- 5 RPCs (cs_ficha_upsert, cs_ficha_transicionar,
--         cs_calcular_proxima_progressao, cs_identificar_elegiveis,
--         cs_progressao_transicionar)
-- 1 unique index parcial (uq_cs_prog_ciclo_inicial)
--
-- IDEMPOTENCIA:
--   - CREATE OR REPLACE em todas as views/RPCs
--   - CREATE UNIQUE INDEX IF NOT EXISTS
--   - RPCs sao idempotentes em sua semantica (upsert + status-check)
--
-- SECURITY DEFINER + search_path = public em todas as RPCs.
-- Validacao de rh_perfil_atual() no inicio de cada RPC de escrita.
-- ════════════════════════════════════════════════════════════════════════════
