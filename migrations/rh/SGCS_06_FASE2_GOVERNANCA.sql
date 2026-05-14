-- ════════════════════════════════════════════════════════════════════════════
-- SGCS — FASE 2 (Modulos 6 + 7): Governanca de Excecoes + Comite de C&S
-- Data: 2026-05-13
-- ════════════════════════════════════════════════════════════════════════════
-- Pre-requisito: SGCS_01..SGCS_05 ja aplicados.
-- Tabelas usadas (criadas em SGCS_01):
--   rh_cs_excecoes, rh_cs_reunioes_comite, rh_cs_alertas
-- Tabelas existentes: rh_funcionarios
--
-- ALCADAS POR TIPO DE EXCECAO (do prompt mestre, Modulo 6):
--   salario_fora_banda          -> Diretoria
--   promocao_extraordinaria     -> Comite (gestor_confianca)
--   admissao_acima_referencia   -> Comite (gestor_confianca)
--   retencao_especial           -> Diretoria
--   bonus_pontual               -> Diretoria
--   alteracao_cargo             -> Comite (gestor_confianca)
--   reversao_fiducia            -> Diretoria
--
-- LIMITE QUANTITATIVO: 5% do quadro ativo por ano. Alerta automatico ao
-- atingir; bloqueio nao é forcado pela RPC (politica permite excecao
-- estrategica acima do limite, mas exige diretoria).
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1. VIEW: vw_cs_excecoes_ciclo ──────────────────────────────────────────
-- Excecoes do ciclo + snapshot do colaborador para listagem.
CREATE OR REPLACE VIEW public.vw_cs_excecoes_ciclo AS
SELECT
  ex.id                  AS excecao_id,
  ex.ciclo_ano,
  ex.funcionario_id,
  f.nome_completo        AS nome,
  f.matricula,
  c.nome                 AS cargo,
  cs.nivel,
  ex.tipo,
  ex.motivo,
  ex.parecer_tecnico,
  ex.valor_envolvido,
  ex.status,
  ex.aprovado_por,
  ex.data_aprovacao,
  ex.ata_id,
  -- Alcada esperada por tipo
  CASE ex.tipo
    WHEN 'salario_fora_banda'        THEN 'diretoria'
    WHEN 'retencao_especial'         THEN 'diretoria'
    WHEN 'bonus_pontual'             THEN 'diretoria'
    WHEN 'reversao_fiducia'          THEN 'diretoria'
    WHEN 'promocao_extraordinaria'   THEN 'comite'
    WHEN 'admissao_acima_referencia' THEN 'comite'
    WHEN 'alteracao_cargo'           THEN 'comite'
    ELSE 'comite'
  END                    AS alcada_aprovacao,
  ex.created_at,
  ex.updated_at
FROM rh_cs_excecoes ex
  JOIN rh_funcionarios f             ON f.id = ex.funcionario_id
  LEFT JOIN rh_cargos c              ON c.id = f.cargo_id
  LEFT JOIN rh_cs_colaborador cs     ON cs.funcionario_id = f.id;

COMMENT ON VIEW public.vw_cs_excecoes_ciclo IS
  'Excecoes do ciclo com snapshot do colaborador e alcada de aprovacao por tipo.';

-- ── 2. RPC: cs_excecao_criar ───────────────────────────────────────────────
-- Cria excecao em status 'pendente'. Qualquer admin/rh/gestor_confianca pode
-- registrar uma excecao (a decisao final fica para a alcada do tipo).
CREATE OR REPLACE FUNCTION public.cs_excecao_criar(
  p_funcionario_id  UUID,
  p_tipo            TEXT,
  p_motivo          TEXT,
  p_parecer_tecnico TEXT DEFAULT NULL,
  p_valor_envolvido NUMERIC(12,2) DEFAULT NULL,
  p_ciclo_ano       INT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT := rh_perfil_atual();
  v_ciclo  INT := COALESCE(p_ciclo_ano, EXTRACT(YEAR FROM CURRENT_DATE)::INT);
  v_id     UUID;
BEGIN
  IF v_perfil NOT IN ('administrador','rh','gestor_confianca') THEN
    RAISE EXCEPTION 'Acesso negado: perfil % nao pode registrar excecao', v_perfil;
  END IF;

  IF p_tipo NOT IN (
    'salario_fora_banda','promocao_extraordinaria','admissao_acima_referencia',
    'retencao_especial','bonus_pontual','alteracao_cargo','reversao_fiducia'
  ) THEN
    RAISE EXCEPTION 'Tipo de excecao invalido: %', p_tipo;
  END IF;

  IF NULLIF(trim(p_motivo), '') IS NULL THEN
    RAISE EXCEPTION 'Motivo e obrigatorio';
  END IF;

  INSERT INTO rh_cs_excecoes (
    funcionario_id, ciclo_ano, tipo, motivo, parecer_tecnico,
    valor_envolvido, status
  ) VALUES (
    p_funcionario_id, v_ciclo, p_tipo, p_motivo, p_parecer_tecnico,
    p_valor_envolvido, 'pendente'
  )
  RETURNING id INTO v_id;

  -- Recalcular alerta de limite (idempotente)
  PERFORM cs_excecao_recalcular_alerta_limite(v_ciclo);

  RETURN v_id;
END;
$$;

COMMENT ON FUNCTION public.cs_excecao_criar IS
  'Cria excecao pendente. Valida tipo e motivo. Recalcula alerta de limite 5%.';

-- ── 3. RPC: cs_excecao_decidir ─────────────────────────────────────────────
-- Aprova ou rejeita. Valida que o perfil corresponde a alcada do tipo.
CREATE OR REPLACE FUNCTION public.cs_excecao_decidir(
  p_excecao_id UUID,
  p_aprovar    BOOLEAN,
  p_parecer    TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil       TEXT := rh_perfil_atual();
  v_tipo         TEXT;
  v_status       TEXT;
  v_alcada       TEXT;
  v_novo_status  TEXT;
  v_ciclo        INT;
BEGIN
  SELECT tipo, status, ciclo_ano INTO v_tipo, v_status, v_ciclo
    FROM rh_cs_excecoes WHERE id = p_excecao_id;

  IF v_tipo IS NULL THEN
    RAISE EXCEPTION 'Excecao % nao encontrada', p_excecao_id;
  END IF;

  IF v_status != 'pendente' THEN
    RAISE EXCEPTION 'Excecao ja decidida (status: %)', v_status;
  END IF;

  -- Determinar alcada por tipo
  v_alcada := CASE v_tipo
    WHEN 'salario_fora_banda'        THEN 'diretoria'
    WHEN 'retencao_especial'         THEN 'diretoria'
    WHEN 'bonus_pontual'             THEN 'diretoria'
    WHEN 'reversao_fiducia'          THEN 'diretoria'
    WHEN 'promocao_extraordinaria'   THEN 'comite'
    WHEN 'admissao_acima_referencia' THEN 'comite'
    WHEN 'alteracao_cargo'           THEN 'comite'
    ELSE 'comite'
  END;

  IF v_alcada = 'diretoria' AND v_perfil NOT IN ('administrador','diretoria') THEN
    RAISE EXCEPTION 'Excecao tipo % exige aprovacao da Diretoria (perfil atual: %)', v_tipo, v_perfil;
  END IF;

  IF v_alcada = 'comite' AND v_perfil NOT IN ('administrador','gestor_confianca','diretoria') THEN
    RAISE EXCEPTION 'Excecao tipo % exige aprovacao do Comite (perfil atual: %)', v_tipo, v_perfil;
  END IF;

  v_novo_status := CASE WHEN p_aprovar THEN 'aprovada' ELSE 'rejeitada' END;

  UPDATE rh_cs_excecoes SET
    status         = v_novo_status,
    aprovado_por   = auth.uid(),
    data_aprovacao = NOW(),
    parecer_tecnico = COALESCE(p_parecer, parecer_tecnico),
    updated_at     = NOW()
  WHERE id = p_excecao_id;

  -- Recalcular alerta de limite (idempotente)
  PERFORM cs_excecao_recalcular_alerta_limite(v_ciclo);

  RETURN v_novo_status;
END;
$$;

COMMENT ON FUNCTION public.cs_excecao_decidir IS
  'Aprova/rejeita excecao. Valida alcada por tipo (diretoria vs comite).';

-- ── 4. RPC: cs_excecao_indicadores ─────────────────────────────────────────
-- Retorna indicadores do ciclo: contagens, % do quadro, flag 5%.
CREATE OR REPLACE FUNCTION public.cs_excecao_indicadores(
  p_ciclo_ano INT DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_ciclo INT := COALESCE(p_ciclo_ano, EXTRACT(YEAR FROM CURRENT_DATE)::INT);
  v_total     INT;
  v_aprovadas INT;
  v_pendentes INT;
  v_rejeitadas INT;
  v_quadro    INT;
  v_pct       NUMERIC;
BEGIN
  IF rh_perfil_atual() NOT IN ('administrador','rh','gestor_confianca','diretoria','financeiro') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  SELECT COUNT(*) FILTER (WHERE status = 'aprovada'),
         COUNT(*) FILTER (WHERE status = 'pendente'),
         COUNT(*) FILTER (WHERE status = 'rejeitada'),
         COUNT(*)
    INTO v_aprovadas, v_pendentes, v_rejeitadas, v_total
    FROM rh_cs_excecoes
   WHERE ciclo_ano = v_ciclo;

  SELECT COUNT(*) INTO v_quadro
    FROM rh_funcionarios WHERE status IN ('ativo','experiencia');

  v_pct := CASE WHEN v_quadro > 0 THEN ROUND((v_aprovadas::NUMERIC / v_quadro) * 100, 2) ELSE 0 END;

  RETURN jsonb_build_object(
    'ciclo_ano', v_ciclo,
    'total', v_total,
    'aprovadas', v_aprovadas,
    'pendentes', v_pendentes,
    'rejeitadas', v_rejeitadas,
    'quadro_ativo', v_quadro,
    'percent_quadro', v_pct,
    'atingiu_5pct', v_pct >= 5
  );
END;
$$;

COMMENT ON FUNCTION public.cs_excecao_indicadores IS
  'KPIs de excecoes no ciclo: contagens, % do quadro, flag de limite 5%.';

-- ── 5. RPC: cs_excecao_recalcular_alerta_limite (interna) ──────────────────
-- Cria/resolve alerta 'excecao_limite' baseado no estado atual.
CREATE OR REPLACE FUNCTION public.cs_excecao_recalcular_alerta_limite(
  p_ciclo_ano INT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_ind     JSONB;
  v_pct     NUMERIC;
  v_atingiu BOOLEAN;
BEGIN
  v_ind := cs_excecao_indicadores(p_ciclo_ano);
  v_pct := (v_ind->>'percent_quadro')::NUMERIC;
  v_atingiu := (v_ind->>'atingiu_5pct')::BOOLEAN;

  IF v_atingiu THEN
    INSERT INTO rh_cs_alertas (tipo, criticidade, mensagem, contexto_jsonb)
    SELECT
      'excecao_limite', 'alta',
      format('Excecoes do ciclo %s atingiram %s%% do quadro (limite politica: 5%%)',
             p_ciclo_ano, v_pct),
      v_ind
    WHERE NOT EXISTS (
      SELECT 1 FROM rh_cs_alertas
       WHERE tipo = 'excecao_limite'
         AND resolvido = FALSE
         AND (contexto_jsonb->>'ciclo_ano')::INT = p_ciclo_ano
    );
  ELSE
    UPDATE rh_cs_alertas
       SET resolvido = TRUE, data_resolucao = NOW()
     WHERE tipo = 'excecao_limite'
       AND resolvido = FALSE
       AND (contexto_jsonb->>'ciclo_ano')::INT = p_ciclo_ano;
  END IF;
END;
$$;

COMMENT ON FUNCTION public.cs_excecao_recalcular_alerta_limite IS
  'Cria/resolve alerta de limite 5% de excecoes no ciclo. Chamada apos cada criacao/decisao.';

-- ════════════════════════════════════════════════════════════════════════════
-- MODULO 7 — COMITE DE C&S
-- ════════════════════════════════════════════════════════════════════════════

-- ── 6. VIEW: vw_cs_reunioes_comite ─────────────────────────────────────────
-- Reunioes com flag "atrasada" (data passou e ainda 'agendada')
CREATE OR REPLACE VIEW public.vw_cs_reunioes_comite_listagem AS
SELECT
  r.id                         AS reuniao_id,
  r.data_reuniao,
  r.trimestre,
  r.ano,
  r.participantes_jsonb,
  r.pauta,
  r.deliberacoes,
  r.ata_url,
  r.status,
  CASE
    WHEN r.status = 'agendada' AND r.data_reuniao < CURRENT_DATE THEN TRUE
    ELSE FALSE
  END                          AS atrasada,
  CASE
    WHEN r.status = 'agendada' THEN (r.data_reuniao - CURRENT_DATE)
    ELSE NULL
  END                          AS dias_para,
  r.created_at,
  r.updated_at
FROM rh_cs_reunioes_comite r;

COMMENT ON VIEW public.vw_cs_reunioes_comite_listagem IS
  'Reunioes do Comite com flag atrasada e dias_para.';

-- ── 7. RPC: cs_reuniao_upsert ──────────────────────────────────────────────
-- Cria ou atualiza reuniao do Comite. Apenas admin/rh/gestor_confianca/diretoria.
CREATE OR REPLACE FUNCTION public.cs_reuniao_upsert(
  p_id              UUID DEFAULT NULL,
  p_data_reuniao    DATE DEFAULT NULL,
  p_trimestre       CHAR(2) DEFAULT NULL,
  p_ano             INT DEFAULT NULL,
  p_participantes   JSONB DEFAULT NULL,
  p_pauta           TEXT DEFAULT NULL
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
  IF v_perfil NOT IN ('administrador','rh','gestor_confianca','diretoria') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  IF p_id IS NULL THEN
    -- Criar
    IF p_data_reuniao IS NULL OR p_trimestre IS NULL OR p_ano IS NULL THEN
      RAISE EXCEPTION 'Data, trimestre e ano sao obrigatorios para criar reuniao';
    END IF;

    INSERT INTO rh_cs_reunioes_comite (
      data_reuniao, trimestre, ano, participantes_jsonb, pauta, status
    ) VALUES (
      p_data_reuniao, p_trimestre, p_ano,
      COALESCE(p_participantes, '[]'::jsonb), p_pauta, 'agendada'
    )
    RETURNING id INTO v_id;
  ELSE
    -- Atualizar (so se ainda nao realizada — apos realizada, usa cs_reuniao_realizar)
    SELECT status INTO v_status FROM rh_cs_reunioes_comite WHERE id = p_id;
    IF v_status IS NULL THEN
      RAISE EXCEPTION 'Reuniao % nao encontrada', p_id;
    END IF;
    IF v_status = 'realizada' AND v_perfil NOT IN ('administrador','rh') THEN
      RAISE EXCEPTION 'Reuniao realizada nao pode ser editada (so admin/rh pode reabrir)';
    END IF;

    UPDATE rh_cs_reunioes_comite SET
      data_reuniao        = COALESCE(p_data_reuniao, data_reuniao),
      trimestre           = COALESCE(p_trimestre, trimestre),
      ano                 = COALESCE(p_ano, ano),
      participantes_jsonb = COALESCE(p_participantes, participantes_jsonb),
      pauta               = COALESCE(p_pauta, pauta),
      updated_at          = NOW()
    WHERE id = p_id;

    v_id := p_id;
  END IF;

  RETURN v_id;
END;
$$;

COMMENT ON FUNCTION public.cs_reuniao_upsert IS
  'Cria ou atualiza reuniao trimestral do Comite. Reuniao realizada exige admin/rh para editar.';

-- ── 8. RPC: cs_reuniao_realizar ────────────────────────────────────────────
-- Marca reuniao como realizada com deliberacoes.
CREATE OR REPLACE FUNCTION public.cs_reuniao_realizar(
  p_id           UUID,
  p_deliberacoes TEXT,
  p_ata_url      TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT := rh_perfil_atual();
  v_status TEXT;
BEGIN
  IF v_perfil NOT IN ('administrador','rh','gestor_confianca','diretoria') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  SELECT status INTO v_status FROM rh_cs_reunioes_comite WHERE id = p_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'Reuniao % nao encontrada', p_id;
  END IF;
  IF v_status = 'cancelada' THEN
    RAISE EXCEPTION 'Reuniao cancelada nao pode ser realizada';
  END IF;
  IF NULLIF(trim(p_deliberacoes), '') IS NULL THEN
    RAISE EXCEPTION 'Deliberacoes sao obrigatorias para realizar reuniao';
  END IF;

  UPDATE rh_cs_reunioes_comite SET
    status        = 'realizada',
    deliberacoes  = p_deliberacoes,
    ata_url       = COALESCE(p_ata_url, ata_url),
    updated_at    = NOW()
  WHERE id = p_id;

  RETURN 'realizada';
END;
$$;

COMMENT ON FUNCTION public.cs_reuniao_realizar IS
  'Marca reuniao como realizada com deliberacoes (obrigatorias).';

-- ── 9. RPC: cs_reuniao_cancelar ────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.cs_reuniao_cancelar(
  p_id     UUID,
  p_motivo TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_perfil TEXT := rh_perfil_atual();
  v_status TEXT;
BEGIN
  IF v_perfil NOT IN ('administrador','rh','gestor_confianca','diretoria') THEN
    RAISE EXCEPTION 'Acesso negado';
  END IF;

  SELECT status INTO v_status FROM rh_cs_reunioes_comite WHERE id = p_id;
  IF v_status IS NULL THEN
    RAISE EXCEPTION 'Reuniao % nao encontrada', p_id;
  END IF;
  IF v_status = 'realizada' THEN
    RAISE EXCEPTION 'Reuniao realizada nao pode ser cancelada — use admin/rh para reabrir';
  END IF;

  UPDATE rh_cs_reunioes_comite SET
    status        = 'cancelada',
    deliberacoes  = COALESCE(deliberacoes || E'\n\n', '') || '[CANCELADA] ' || p_motivo,
    updated_at    = NOW()
  WHERE id = p_id;

  RETURN 'cancelada';
END;
$$;

COMMENT ON FUNCTION public.cs_reuniao_cancelar IS
  'Cancela reuniao agendada. Reuniao realizada exige reabrir antes.';

-- ════════════════════════════════════════════════════════════════════════════
-- FIM SGCS_06_FASE2_GOVERNANCA.sql
-- 2 views (vw_cs_excecoes_ciclo, vw_cs_reunioes_comite_listagem)
-- 7 RPCs:
--   Excecoes: cs_excecao_criar, cs_excecao_decidir, cs_excecao_indicadores,
--             cs_excecao_recalcular_alerta_limite (interna)
--   Comite:   cs_reuniao_upsert, cs_reuniao_realizar, cs_reuniao_cancelar
--
-- IDEMPOTENCIA: CREATE OR REPLACE em tudo.
-- SECURITY DEFINER + search_path = public + valida rh_perfil_atual no inicio.
-- ════════════════════════════════════════════════════════════════════════════
