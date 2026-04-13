-- ═══════════════════════════════════════════════════════════════════════════
-- MÓDULO: PROGRAMA SAÚDE E EQUILÍBRIO OCUPACIONAL
-- Sistema RH Classic · Supabase (PostgreSQL)
-- Versão: 1.0 | Data: 2026-03-20
-- ═══════════════════════════════════════════════════════════════════════════
--
-- REGRAS FUNDAMENTAIS DO PROGRAMA:
--   1. NUNCA armazenar conteúdo clínico, diagnóstico, CID ou detalhes de sessão
--   2. Apenas registrar: data, comparecimento, classificação de risco, recomendação geral
--   3. Classificação "alto" dispara ação obrigatória do RH
--   4. Três portas de entrada: voluntária, indicação do líder, gatilho automático
--
-- DEPENDÊNCIAS:
--   - rh_colaboradores (id, nome, setor, status, auth_uid)
--   - rh_ocorrencias (colaborador_id, tipo, data_inicio, competencia)
--   - rh_producao (colaborador_id, data_referencia, percentual_atingimento)
--   - rh_perfis_acesso (auth_uid, perfil, setor, ativo)
--   - Funções: has_perfil(), get_user_perfis(), get_meu_colaborador_id(), get_setores_lider()
-- ═══════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════
-- 1. TABELAS
-- ══════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────
-- 1.1 AGENDA DE ATENDIMENTOS
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_sm_agenda (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id    UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    data_agendamento  DATE NOT NULL,
    horario_inicio    TIME NOT NULL,
    horario_fim       TIME,
    tipo_entrada      TEXT NOT NULL CHECK (tipo_entrada IN (
        'voluntario', 'indicacao_lider', 'gatilho_automatico'
    )),
    indicado_por      UUID REFERENCES auth.users(id),   -- quem indicou (líder ou sistema)
    motivo_indicacao  TEXT,                               -- motivo breve da indicação
    status            TEXT NOT NULL DEFAULT 'agendado' CHECK (status IN (
        'agendado', 'confirmado', 'realizado', 'cancelado', 'falta'
    )),
    observacoes       TEXT,                               -- notas administrativas (NÃO clínicas)
    criado_por        UUID REFERENCES auth.users(id),
    criado_em         TIMESTAMPTZ DEFAULT now(),
    atualizado_em     TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE rh_sm_agenda IS 'Agendamento de atendimentos do Programa Saúde e Equilíbrio. NÃO contém dados clínicos.';
COMMENT ON COLUMN rh_sm_agenda.tipo_entrada IS 'Porta de entrada: voluntário, indicação do líder ou gatilho automático do sistema.';
COMMENT ON COLUMN rh_sm_agenda.observacoes IS 'Observações administrativas APENAS. Nunca registrar conteúdo clínico.';


-- ─────────────────────────────────────────────────────────
-- 1.2 REGISTRO DE SESSÕES (sem dados clínicos)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_sm_sessoes (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id        UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    agenda_id             UUID REFERENCES rh_sm_agenda(id) ON DELETE SET NULL,
    data_sessao           DATE NOT NULL,
    compareceu            BOOLEAN NOT NULL DEFAULT true,
    classificacao_risco   TEXT NOT NULL CHECK (classificacao_risco IN (
        'baixo', 'moderado', 'alto'
    )),
    recomendacao_geral    TEXT,                            -- texto curto, NÃO sensível
    proximo_agendamento   DATE,
    avaliador_id          UUID NOT NULL REFERENCES auth.users(id),
    criado_em             TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE rh_sm_sessoes IS 'Registro de sessões realizadas. PROIBIDO armazenar diagnóstico, CID ou conteúdo clínico.';
COMMENT ON COLUMN rh_sm_sessoes.recomendacao_geral IS 'Recomendação breve e NÃO sensível. Ex: "reavaliação em 30 dias", "encaminhamento externo sugerido".';
COMMENT ON COLUMN rh_sm_sessoes.classificacao_risco IS 'Risco "alto" dispara ação obrigatória do RH conforme política do programa.';


-- ─────────────────────────────────────────────────────────
-- 1.3 ALERTAS AUTOMÁTICOS
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_sm_alertas (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id   UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    tipo_alerta      TEXT NOT NULL CHECK (tipo_alerta IN (
        'absenteismo_alto',
        'queda_produtividade',
        'retorno_afastamento',
        'risco_alto',
        'multiplos_atendimentos'
    )),
    descricao        TEXT NOT NULL,
    dados_referencia JSONB DEFAULT '{}'::jsonb,            -- dados que geraram o alerta
    status           TEXT NOT NULL DEFAULT 'pendente' CHECK (status IN (
        'pendente', 'em_andamento', 'resolvido', 'ignorado'
    )),
    responsavel_id   UUID REFERENCES auth.users(id),       -- psicóloga/RH responsável
    criado_em        TIMESTAMPTZ DEFAULT now(),
    resolvido_em     TIMESTAMPTZ
);

COMMENT ON TABLE rh_sm_alertas IS 'Alertas gerados automaticamente pelo sistema com base em gatilhos de absenteísmo, produtividade e risco.';


-- ─────────────────────────────────────────────────────────
-- 1.4 ENCAMINHAMENTOS (indicações de líderes)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rh_sm_encaminhamentos (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id      UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    encaminhado_por     UUID NOT NULL REFERENCES auth.users(id),
    motivo              TEXT NOT NULL,
    data_encaminhamento DATE NOT NULL DEFAULT CURRENT_DATE,
    status              TEXT NOT NULL DEFAULT 'pendente' CHECK (status IN (
        'pendente', 'agendado', 'concluido'
    )),
    agenda_id           UUID REFERENCES rh_sm_agenda(id) ON DELETE SET NULL,
    criado_em           TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE rh_sm_encaminhamentos IS 'Encaminhamentos feitos por líderes para o Programa Saúde e Equilíbrio.';


-- ══════════════════════════════════════════════════════════
-- 2. ÍNDICES DE PERFORMANCE
-- ══════════════════════════════════════════════════════════

-- rh_sm_agenda
CREATE INDEX IF NOT EXISTS idx_sm_agenda_colaborador   ON rh_sm_agenda(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_sm_agenda_data          ON rh_sm_agenda(data_agendamento);
CREATE INDEX IF NOT EXISTS idx_sm_agenda_status        ON rh_sm_agenda(status);
CREATE INDEX IF NOT EXISTS idx_sm_agenda_tipo          ON rh_sm_agenda(tipo_entrada);
CREATE INDEX IF NOT EXISTS idx_sm_agenda_semana        ON rh_sm_agenda(data_agendamento)
    WHERE status IN ('agendado', 'confirmado');

-- rh_sm_sessoes
CREATE INDEX IF NOT EXISTS idx_sm_sessoes_colaborador  ON rh_sm_sessoes(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_sm_sessoes_data         ON rh_sm_sessoes(data_sessao);
CREATE INDEX IF NOT EXISTS idx_sm_sessoes_risco        ON rh_sm_sessoes(classificacao_risco);
CREATE INDEX IF NOT EXISTS idx_sm_sessoes_avaliador    ON rh_sm_sessoes(avaliador_id);

-- rh_sm_alertas
CREATE INDEX IF NOT EXISTS idx_sm_alertas_colaborador  ON rh_sm_alertas(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_sm_alertas_status       ON rh_sm_alertas(status);
CREATE INDEX IF NOT EXISTS idx_sm_alertas_tipo         ON rh_sm_alertas(tipo_alerta);
CREATE INDEX IF NOT EXISTS idx_sm_alertas_pendentes    ON rh_sm_alertas(status, criado_em)
    WHERE status = 'pendente';

-- rh_sm_encaminhamentos
CREATE INDEX IF NOT EXISTS idx_sm_encam_colaborador    ON rh_sm_encaminhamentos(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_sm_encam_status         ON rh_sm_encaminhamentos(status);
CREATE INDEX IF NOT EXISTS idx_sm_encam_por            ON rh_sm_encaminhamentos(encaminhado_por);


-- ══════════════════════════════════════════════════════════
-- 3. TRIGGER: updated_at AUTOMÁTICO
-- ══════════════════════════════════════════════════════════

-- Reutiliza a função rh_set_updated_at() já existente no schema.
-- Caso não exista, criar:
CREATE OR REPLACE FUNCTION rh_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sm_agenda_updated ON rh_sm_agenda;
CREATE TRIGGER trg_sm_agenda_updated
    BEFORE UPDATE ON rh_sm_agenda
    FOR EACH ROW EXECUTE FUNCTION rh_set_updated_at();


-- ══════════════════════════════════════════════════════════
-- 4. ROW LEVEL SECURITY (RLS)
-- ══════════════════════════════════════════════════════════

ALTER TABLE rh_sm_agenda          ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_sm_sessoes         ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_sm_alertas         ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_sm_encaminhamentos ENABLE ROW LEVEL SECURITY;

-- ─────────────────────────────────────────────────────────
-- 4.1 rh_sm_agenda
-- Psicóloga/Admin: tudo. RH: leitura total. Líder: seus encaminhamentos. Colaborador: seus agendamentos.
-- ─────────────────────────────────────────────────────────

-- SELECT
DROP POLICY IF EXISTS "sm_agenda_select_psi_admin_rh" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_select_psi_admin_rh" ON rh_sm_agenda FOR SELECT
    USING (has_perfil('psicologa') OR has_perfil('admin') OR has_perfil('rh'));

DROP POLICY IF EXISTS "sm_agenda_select_lider" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_select_lider" ON rh_sm_agenda FOR SELECT
    USING (
        has_perfil('lider')
        AND (
            indicado_por = auth.uid()
            OR EXISTS (
                SELECT 1 FROM rh_colaboradores c
                WHERE c.id = rh_sm_agenda.colaborador_id
                  AND c.setor = ANY(get_setores_lider())
            )
        )
    );

DROP POLICY IF EXISTS "sm_agenda_select_proprio" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_select_proprio" ON rh_sm_agenda FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

-- INSERT
DROP POLICY IF EXISTS "sm_agenda_insert" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_insert" ON rh_sm_agenda FOR INSERT
    WITH CHECK (has_perfil('psicologa') OR has_perfil('admin') OR has_perfil('rh'));

-- UPDATE
DROP POLICY IF EXISTS "sm_agenda_update" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_update" ON rh_sm_agenda FOR UPDATE
    USING (has_perfil('psicologa') OR has_perfil('admin'))
    WITH CHECK (has_perfil('psicologa') OR has_perfil('admin'));

-- DELETE (apenas admin — cancelamento preferível)
DROP POLICY IF EXISTS "sm_agenda_delete" ON rh_sm_agenda;
CREATE POLICY "sm_agenda_delete" ON rh_sm_agenda FOR DELETE
    USING (has_perfil('admin'));


-- ─────────────────────────────────────────────────────────
-- 4.2 rh_sm_sessoes — MAIS RESTRITA
-- APENAS psicóloga e admin. Nenhum outro perfil tem acesso.
-- ─────────────────────────────────────────────────────────

DROP POLICY IF EXISTS "sm_sessoes_select" ON rh_sm_sessoes;
CREATE POLICY "sm_sessoes_select" ON rh_sm_sessoes FOR SELECT
    USING (has_perfil('psicologa') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_sessoes_insert" ON rh_sm_sessoes;
CREATE POLICY "sm_sessoes_insert" ON rh_sm_sessoes FOR INSERT
    WITH CHECK (has_perfil('psicologa') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_sessoes_update" ON rh_sm_sessoes;
CREATE POLICY "sm_sessoes_update" ON rh_sm_sessoes FOR UPDATE
    USING (has_perfil('psicologa') OR has_perfil('admin'))
    WITH CHECK (has_perfil('psicologa') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_sessoes_delete" ON rh_sm_sessoes;
CREATE POLICY "sm_sessoes_delete" ON rh_sm_sessoes FOR DELETE
    USING (has_perfil('admin'));


-- ─────────────────────────────────────────────────────────
-- 4.3 rh_sm_alertas
-- Psicóloga, RH e Admin.
-- ─────────────────────────────────────────────────────────

DROP POLICY IF EXISTS "sm_alertas_select" ON rh_sm_alertas;
CREATE POLICY "sm_alertas_select" ON rh_sm_alertas FOR SELECT
    USING (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_alertas_insert" ON rh_sm_alertas;
CREATE POLICY "sm_alertas_insert" ON rh_sm_alertas FOR INSERT
    WITH CHECK (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_alertas_update" ON rh_sm_alertas;
CREATE POLICY "sm_alertas_update" ON rh_sm_alertas FOR UPDATE
    USING (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'))
    WITH CHECK (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_alertas_delete" ON rh_sm_alertas;
CREATE POLICY "sm_alertas_delete" ON rh_sm_alertas FOR DELETE
    USING (has_perfil('admin'));


-- ─────────────────────────────────────────────────────────
-- 4.4 rh_sm_encaminhamentos
-- Psicóloga/RH/Admin vê tudo. Líder vê apenas os seus.
-- ─────────────────────────────────────────────────────────

DROP POLICY IF EXISTS "sm_encam_select_psi_rh_admin" ON rh_sm_encaminhamentos;
CREATE POLICY "sm_encam_select_psi_rh_admin" ON rh_sm_encaminhamentos FOR SELECT
    USING (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_encam_select_lider" ON rh_sm_encaminhamentos;
CREATE POLICY "sm_encam_select_lider" ON rh_sm_encaminhamentos FOR SELECT
    USING (
        has_perfil('lider')
        AND encaminhado_por = auth.uid()
    );

DROP POLICY IF EXISTS "sm_encam_insert" ON rh_sm_encaminhamentos;
CREATE POLICY "sm_encam_insert" ON rh_sm_encaminhamentos FOR INSERT
    WITH CHECK (
        has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin') OR has_perfil('lider')
    );

DROP POLICY IF EXISTS "sm_encam_update" ON rh_sm_encaminhamentos;
CREATE POLICY "sm_encam_update" ON rh_sm_encaminhamentos FOR UPDATE
    USING (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'))
    WITH CHECK (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin'));

DROP POLICY IF EXISTS "sm_encam_delete" ON rh_sm_encaminhamentos;
CREATE POLICY "sm_encam_delete" ON rh_sm_encaminhamentos FOR DELETE
    USING (has_perfil('admin'));


-- ══════════════════════════════════════════════════════════
-- 5. FUNÇÕES RPC
-- ══════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────
-- 5.1 AGENDAR ATENDIMENTO
-- Acesso: psicóloga, rh, admin
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION agendar_atendimento(
    p_colaborador_id UUID,
    p_data           DATE,
    p_horario_inicio TIME,
    p_horario_fim    TIME DEFAULT NULL,
    p_tipo_entrada   TEXT DEFAULT 'voluntario',
    p_motivo         TEXT DEFAULT NULL,
    p_indicado_por   UUID DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_id UUID;
BEGIN
    -- Verificar permissão
    IF NOT (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin')) THEN
        RAISE EXCEPTION 'Acesso negado. Apenas psicóloga, RH ou admin podem agendar atendimentos.';
    END IF;

    -- Validar colaborador existe e está ativo
    IF NOT EXISTS (
        SELECT 1 FROM rh_colaboradores
        WHERE id = p_colaborador_id AND status IN ('ativo', 'afastado')
    ) THEN
        RAISE EXCEPTION 'Colaborador não encontrado ou inativo.';
    END IF;

    -- Verificar conflito de horário
    IF EXISTS (
        SELECT 1 FROM rh_sm_agenda
        WHERE data_agendamento = p_data
          AND status IN ('agendado', 'confirmado')
          AND horario_inicio = p_horario_inicio
    ) THEN
        RAISE EXCEPTION 'Já existe um atendimento agendado neste horário.';
    END IF;

    INSERT INTO rh_sm_agenda (
        colaborador_id, data_agendamento, horario_inicio, horario_fim,
        tipo_entrada, indicado_por, motivo_indicacao, criado_por
    ) VALUES (
        p_colaborador_id, p_data, p_horario_inicio, p_horario_fim,
        p_tipo_entrada, p_indicado_por, p_motivo, auth.uid()
    )
    RETURNING id INTO v_id;

    -- Registrar no audit log
    INSERT INTO rh_audit_log (usuario_id, acao, detalhes, criado_em)
    VALUES (
        auth.uid(),
        'sm_agendamento_criado',
        format('Agendamento %s para colaborador %s em %s às %s',
               v_id, p_colaborador_id, p_data, p_horario_inicio),
        now()
    );

    RETURN v_id;
END;
$$;

COMMENT ON FUNCTION agendar_atendimento IS 'Agenda atendimento no Programa Saúde e Equilíbrio. Acesso: psicóloga/rh/admin.';


-- ─────────────────────────────────────────────────────────
-- 5.2 REGISTRAR SESSÃO
-- Acesso: APENAS psicóloga e admin
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION registrar_sessao(
    p_agenda_id           UUID,
    p_compareceu          BOOLEAN,
    p_classificacao_risco TEXT,
    p_recomendacao_geral  TEXT DEFAULT NULL,
    p_proximo_agendamento DATE DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_id UUID;
    v_colaborador_id UUID;
    v_data DATE;
BEGIN
    -- Verificar permissão — MAIS RESTRITA
    IF NOT (has_perfil('psicologa') OR has_perfil('admin')) THEN
        RAISE EXCEPTION 'Acesso negado. Apenas psicóloga ou admin podem registrar sessões.';
    END IF;

    -- Validar classificação
    IF p_classificacao_risco NOT IN ('baixo', 'moderado', 'alto') THEN
        RAISE EXCEPTION 'Classificação de risco inválida. Use: baixo, moderado ou alto.';
    END IF;

    -- Buscar dados do agendamento
    SELECT colaborador_id, data_agendamento
      INTO v_colaborador_id, v_data
      FROM rh_sm_agenda
     WHERE id = p_agenda_id;

    IF v_colaborador_id IS NULL THEN
        RAISE EXCEPTION 'Agendamento não encontrado.';
    END IF;

    -- Atualizar status do agendamento
    UPDATE rh_sm_agenda
       SET status = CASE WHEN p_compareceu THEN 'realizado' ELSE 'falta' END,
           atualizado_em = now()
     WHERE id = p_agenda_id;

    -- Inserir sessão
    INSERT INTO rh_sm_sessoes (
        colaborador_id, agenda_id, data_sessao, compareceu,
        classificacao_risco, recomendacao_geral, proximo_agendamento, avaliador_id
    ) VALUES (
        v_colaborador_id, p_agenda_id, v_data, p_compareceu,
        p_classificacao_risco, p_recomendacao_geral, p_proximo_agendamento, auth.uid()
    )
    RETURNING id INTO v_id;

    -- Se risco ALTO → criar alerta obrigatório para RH
    IF p_classificacao_risco = 'alto' THEN
        INSERT INTO rh_sm_alertas (
            colaborador_id, tipo_alerta, descricao, dados_referencia, status
        ) VALUES (
            v_colaborador_id,
            'risco_alto',
            'Colaborador classificado como RISCO ALTO em sessão. Ação obrigatória do RH.',
            jsonb_build_object(
                'sessao_id', v_id,
                'data_sessao', v_data,
                'classificacao', 'alto'
            ),
            'pendente'
        );
    END IF;

    -- Registrar no audit log
    INSERT INTO rh_audit_log (usuario_id, acao, detalhes, criado_em)
    VALUES (
        auth.uid(),
        'sm_sessao_registrada',
        format('Sessão %s registrada. Compareceu: %s. Risco: %s',
               v_id, p_compareceu, p_classificacao_risco),
        now()
    );

    RETURN v_id;
END;
$$;

COMMENT ON FUNCTION registrar_sessao IS 'Registra resultado de sessão (sem dados clínicos). Risco alto gera alerta automático. Acesso: psicóloga/admin.';


-- ─────────────────────────────────────────────────────────
-- 5.3 VERIFICAR GATILHOS AUTOMÁTICOS
-- Checa: faltas > 3/mês, queda produtividade > 20%, retorno de afastamento
-- Deve ser chamada periodicamente (cron ou trigger)
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION verificar_gatilhos_automaticos()
RETURNS TABLE (
    colaborador_id UUID,
    tipo_alerta    TEXT,
    descricao      TEXT,
    alerta_criado  BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_mes_atual TEXT;
    v_mes_anterior TEXT;
    v_data_30d DATE;
    rec RECORD;
BEGIN
    -- Verificar permissão
    IF NOT (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin')) THEN
        RAISE EXCEPTION 'Acesso negado.';
    END IF;

    v_mes_atual := to_char(CURRENT_DATE, 'YYYY-MM');
    v_mes_anterior := to_char(CURRENT_DATE - INTERVAL '1 month', 'YYYY-MM');
    v_data_30d := CURRENT_DATE - 30;

    -- ── GATILHO 1: Absenteísmo alto (>3 faltas no mês corrente) ──
    FOR rec IN
        SELECT
            o.colaborador_id,
            c.nome,
            COUNT(*) AS total_faltas
        FROM rh_ocorrencias o
        JOIN rh_colaboradores c ON c.id = o.colaborador_id
        WHERE o.tipo IN ('atestado', 'falta_injustificada')
          AND o.competencia = v_mes_atual
          AND c.status = 'ativo'
        GROUP BY o.colaborador_id, c.nome
        HAVING COUNT(*) > 3
    LOOP
        -- Evitar duplicatas: só cria se não existe alerta pendente do mesmo tipo/mês
        IF NOT EXISTS (
            SELECT 1 FROM rh_sm_alertas a
            WHERE a.colaborador_id = rec.colaborador_id
              AND a.tipo_alerta = 'absenteismo_alto'
              AND a.status IN ('pendente', 'em_andamento')
              AND a.criado_em >= date_trunc('month', CURRENT_DATE)
        ) THEN
            INSERT INTO rh_sm_alertas (colaborador_id, tipo_alerta, descricao, dados_referencia)
            VALUES (
                rec.colaborador_id,
                'absenteismo_alto',
                format('Colaborador %s com %s faltas/atestados no mês %s',
                       rec.nome, rec.total_faltas, v_mes_atual),
                jsonb_build_object('total_faltas', rec.total_faltas, 'competencia', v_mes_atual)
            );

            RETURN QUERY SELECT rec.colaborador_id, 'absenteismo_alto'::TEXT,
                format('%s faltas no mês', rec.total_faltas)::TEXT, true;
        END IF;
    END LOOP;

    -- ── GATILHO 2: Queda de produtividade > 20% (mês atual vs anterior) ──
    FOR rec IN
        SELECT
            p_atual.colaborador_id,
            c.nome,
            AVG(p_anterior.percentual_atingimento) AS media_anterior,
            AVG(p_atual.percentual_atingimento) AS media_atual,
            ROUND(
                ((AVG(p_anterior.percentual_atingimento) - AVG(p_atual.percentual_atingimento))
                 / NULLIF(AVG(p_anterior.percentual_atingimento), 0)) * 100
            , 1) AS queda_percentual
        FROM rh_producao p_atual
        JOIN rh_colaboradores c ON c.id = p_atual.colaborador_id
        JOIN rh_producao p_anterior
            ON p_anterior.colaborador_id = p_atual.colaborador_id
            AND to_char(p_anterior.data_referencia, 'YYYY-MM') = v_mes_anterior
        WHERE to_char(p_atual.data_referencia, 'YYYY-MM') = v_mes_atual
          AND c.status = 'ativo'
        GROUP BY p_atual.colaborador_id, c.nome
        HAVING AVG(p_anterior.percentual_atingimento) > 0
           AND ((AVG(p_anterior.percentual_atingimento) - AVG(p_atual.percentual_atingimento))
                / AVG(p_anterior.percentual_atingimento)) > 0.20
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM rh_sm_alertas a
            WHERE a.colaborador_id = rec.colaborador_id
              AND a.tipo_alerta = 'queda_produtividade'
              AND a.status IN ('pendente', 'em_andamento')
              AND a.criado_em >= date_trunc('month', CURRENT_DATE)
        ) THEN
            INSERT INTO rh_sm_alertas (colaborador_id, tipo_alerta, descricao, dados_referencia)
            VALUES (
                rec.colaborador_id,
                'queda_produtividade',
                format('Colaborador %s com queda de %s%% na produtividade (de %s%% para %s%%)',
                       rec.nome, rec.queda_percentual, ROUND(rec.media_anterior,1), ROUND(rec.media_atual,1)),
                jsonb_build_object(
                    'media_anterior', rec.media_anterior,
                    'media_atual', rec.media_atual,
                    'queda_percentual', rec.queda_percentual,
                    'mes_referencia', v_mes_atual
                )
            );

            RETURN QUERY SELECT rec.colaborador_id, 'queda_produtividade'::TEXT,
                format('Queda de %s%%', rec.queda_percentual)::TEXT, true;
        END IF;
    END LOOP;

    -- ── GATILHO 3: Retorno de afastamento (nos últimos 30 dias) ──
    FOR rec IN
        SELECT
            c.id AS colaborador_id,
            c.nome
        FROM rh_colaboradores c
        WHERE c.status = 'ativo'
          AND EXISTS (
              SELECT 1 FROM rh_ocorrencias o
              WHERE o.colaborador_id = c.id
                AND o.tipo = 'atestado'
                AND o.dias >= 15
                AND o.data_fim BETWEEN v_data_30d AND CURRENT_DATE
          )
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM rh_sm_alertas a
            WHERE a.colaborador_id = rec.colaborador_id
              AND a.tipo_alerta = 'retorno_afastamento'
              AND a.status IN ('pendente', 'em_andamento')
              AND a.criado_em >= v_data_30d
        ) THEN
            INSERT INTO rh_sm_alertas (colaborador_id, tipo_alerta, descricao, dados_referencia)
            VALUES (
                rec.colaborador_id,
                'retorno_afastamento',
                format('Colaborador %s retornou de afastamento longo nos últimos 30 dias', rec.nome),
                jsonb_build_object('data_verificacao', CURRENT_DATE)
            );

            RETURN QUERY SELECT rec.colaborador_id, 'retorno_afastamento'::TEXT,
                'Retorno de afastamento longo'::TEXT, true;
        END IF;
    END LOOP;

    -- ── GATILHO 4: Múltiplos atendimentos (3+ em 60 dias sem melhora) ──
    FOR rec IN
        SELECT
            s.colaborador_id,
            c.nome,
            COUNT(*) AS total_sessoes,
            MAX(s.classificacao_risco) AS ultimo_risco
        FROM rh_sm_sessoes s
        JOIN rh_colaboradores c ON c.id = s.colaborador_id
        WHERE s.data_sessao >= CURRENT_DATE - 60
          AND s.compareceu = true
        GROUP BY s.colaborador_id, c.nome
        HAVING COUNT(*) >= 3
           AND MAX(s.classificacao_risco) IN ('moderado', 'alto')
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM rh_sm_alertas a
            WHERE a.colaborador_id = rec.colaborador_id
              AND a.tipo_alerta = 'multiplos_atendimentos'
              AND a.status IN ('pendente', 'em_andamento')
              AND a.criado_em >= CURRENT_DATE - 60
        ) THEN
            INSERT INTO rh_sm_alertas (colaborador_id, tipo_alerta, descricao, dados_referencia)
            VALUES (
                rec.colaborador_id,
                'multiplos_atendimentos',
                format('Colaborador %s com %s atendimentos em 60 dias sem melhora (risco: %s)',
                       rec.nome, rec.total_sessoes, rec.ultimo_risco),
                jsonb_build_object(
                    'total_sessoes_60d', rec.total_sessoes,
                    'ultimo_risco', rec.ultimo_risco
                )
            );

            RETURN QUERY SELECT rec.colaborador_id, 'multiplos_atendimentos'::TEXT,
                format('%s sessões em 60 dias', rec.total_sessoes)::TEXT, true;
        END IF;
    END LOOP;

    RETURN;
END;
$$;

COMMENT ON FUNCTION verificar_gatilhos_automaticos IS 'Verifica gatilhos automáticos: absenteísmo >3/mês, queda produtividade >20%, retorno afastamento, múltiplos atendimentos.';


-- ─────────────────────────────────────────────────────────
-- 5.4 INDICADORES DE SAÚDE MENTAL (Dashboard)
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION indicadores_saude_mental(p_meses INT DEFAULT 3)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_data_inicio DATE;
    v_result JSONB;
    v_total_atendimentos INT;
    v_total_compareceram INT;
    v_taxa_adesao NUMERIC;
    v_distribuicao_risco JSONB;
    v_setores_criticos JSONB;
    v_tendencia_mensal JSONB;
    v_alertas_pendentes INT;
    v_origem_atendimentos JSONB;
BEGIN
    -- Verificar permissão
    IF NOT (has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin') OR has_perfil('diretoria')) THEN
        RAISE EXCEPTION 'Acesso negado.';
    END IF;

    v_data_inicio := CURRENT_DATE - (p_meses || ' months')::INTERVAL;

    -- Total de atendimentos agendados
    SELECT COUNT(*) INTO v_total_atendimentos
    FROM rh_sm_agenda
    WHERE data_agendamento >= v_data_inicio;

    -- Taxa de adesão (comparecimentos / agendados)
    SELECT COUNT(*) INTO v_total_compareceram
    FROM rh_sm_sessoes
    WHERE data_sessao >= v_data_inicio
      AND compareceu = true;

    v_taxa_adesao := CASE
        WHEN v_total_atendimentos > 0
        THEN ROUND((v_total_compareceram::NUMERIC / v_total_atendimentos) * 100, 1)
        ELSE 0
    END;

    -- Distribuição de risco
    SELECT COALESCE(jsonb_object_agg(classificacao_risco, total), '{}'::jsonb)
    INTO v_distribuicao_risco
    FROM (
        SELECT classificacao_risco, COUNT(*) AS total
        FROM rh_sm_sessoes
        WHERE data_sessao >= v_data_inicio AND compareceu = true
        GROUP BY classificacao_risco
    ) sub;

    -- Setores críticos (top 5 com mais encaminhamentos + risco alto)
    SELECT COALESCE(jsonb_agg(row_to_json(sub)), '[]'::jsonb)
    INTO v_setores_criticos
    FROM (
        SELECT
            c.setor,
            COUNT(DISTINCT s.colaborador_id) AS colaboradores_atendidos,
            COUNT(*) FILTER (WHERE s.classificacao_risco = 'alto') AS risco_alto,
            COUNT(*) FILTER (WHERE s.classificacao_risco = 'moderado') AS risco_moderado
        FROM rh_sm_sessoes s
        JOIN rh_colaboradores c ON c.id = s.colaborador_id
        WHERE s.data_sessao >= v_data_inicio AND s.compareceu = true
        GROUP BY c.setor
        ORDER BY risco_alto DESC, risco_moderado DESC
        LIMIT 5
    ) sub;

    -- Tendência mensal
    SELECT COALESCE(jsonb_agg(row_to_json(sub) ORDER BY sub.mes), '[]'::jsonb)
    INTO v_tendencia_mensal
    FROM (
        SELECT
            to_char(data_sessao, 'YYYY-MM') AS mes,
            COUNT(*) AS total_sessoes,
            COUNT(*) FILTER (WHERE compareceu = true) AS comparecimentos,
            COUNT(*) FILTER (WHERE classificacao_risco = 'alto') AS risco_alto,
            COUNT(*) FILTER (WHERE classificacao_risco = 'moderado') AS risco_moderado,
            COUNT(*) FILTER (WHERE classificacao_risco = 'baixo') AS risco_baixo
        FROM rh_sm_sessoes
        WHERE data_sessao >= v_data_inicio
        GROUP BY to_char(data_sessao, 'YYYY-MM')
    ) sub;

    -- Alertas pendentes
    SELECT COUNT(*) INTO v_alertas_pendentes
    FROM rh_sm_alertas
    WHERE status = 'pendente';

    -- Origem dos atendimentos
    SELECT COALESCE(jsonb_object_agg(tipo_entrada, total), '{}'::jsonb)
    INTO v_origem_atendimentos
    FROM (
        SELECT tipo_entrada, COUNT(*) AS total
        FROM rh_sm_agenda
        WHERE data_agendamento >= v_data_inicio
        GROUP BY tipo_entrada
    ) sub;

    -- Montar resultado
    v_result := jsonb_build_object(
        'periodo_meses', p_meses,
        'data_inicio', v_data_inicio,
        'data_fim', CURRENT_DATE,
        'total_atendimentos_agendados', v_total_atendimentos,
        'total_comparecimentos', v_total_compareceram,
        'taxa_adesao_percentual', v_taxa_adesao,
        'distribuicao_risco', v_distribuicao_risco,
        'setores_criticos', v_setores_criticos,
        'tendencia_mensal', v_tendencia_mensal,
        'alertas_pendentes', v_alertas_pendentes,
        'origem_atendimentos', v_origem_atendimentos
    );

    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION indicadores_saude_mental IS 'Retorna KPIs agregados do programa: atendimentos, taxa adesão, risco, setores críticos, tendência mensal.';


-- ─────────────────────────────────────────────────────────
-- 5.5 ENCAMINHAR COLABORADOR (líderes)
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION encaminhar_colaborador(
    p_colaborador_id UUID,
    p_motivo         TEXT
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_id UUID;
    v_nome TEXT;
BEGIN
    -- Líderes, psicóloga, rh e admin podem encaminhar
    IF NOT (has_perfil('lider') OR has_perfil('psicologa') OR has_perfil('rh') OR has_perfil('admin')) THEN
        RAISE EXCEPTION 'Acesso negado. Apenas líderes, psicóloga, RH ou admin podem encaminhar colaboradores.';
    END IF;

    -- Se for líder, verificar que o colaborador é do setor dele
    IF has_perfil('lider')
       AND NOT (has_perfil('rh') OR has_perfil('admin') OR has_perfil('psicologa'))
    THEN
        IF NOT EXISTS (
            SELECT 1 FROM rh_colaboradores
            WHERE id = p_colaborador_id
              AND setor = ANY(get_setores_lider())
              AND status = 'ativo'
        ) THEN
            RAISE EXCEPTION 'Você só pode encaminhar colaboradores do seu setor.';
        END IF;
    END IF;

    -- Validar colaborador
    SELECT nome INTO v_nome FROM rh_colaboradores
    WHERE id = p_colaborador_id AND status IN ('ativo', 'afastado');

    IF v_nome IS NULL THEN
        RAISE EXCEPTION 'Colaborador não encontrado ou inativo.';
    END IF;

    -- Criar encaminhamento
    INSERT INTO rh_sm_encaminhamentos (colaborador_id, encaminhado_por, motivo)
    VALUES (p_colaborador_id, auth.uid(), p_motivo)
    RETURNING id INTO v_id;

    -- Registrar no audit log
    INSERT INTO rh_audit_log (usuario_id, acao, detalhes, criado_em)
    VALUES (
        auth.uid(),
        'sm_encaminhamento_criado',
        format('Encaminhamento %s: %s encaminhado. Motivo: %s', v_id, v_nome, p_motivo),
        now()
    );

    RETURN v_id;
END;
$$;

COMMENT ON FUNCTION encaminhar_colaborador IS 'Permite líderes encaminharem colaboradores do seu setor ao Programa Saúde e Equilíbrio.';


-- ══════════════════════════════════════════════════════════
-- 6. VIEWS
-- ══════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────
-- 6.1 DASHBOARD — KPIs agregados (sem nomes individuais)
-- Para diretoria e gestão
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_sm_dashboard AS
SELECT
    -- Período atual (últimos 30 dias)
    (SELECT COUNT(*) FROM rh_sm_agenda
     WHERE data_agendamento >= CURRENT_DATE - 30)
        AS atendimentos_30d,

    (SELECT COUNT(*) FROM rh_sm_sessoes
     WHERE data_sessao >= CURRENT_DATE - 30 AND compareceu = true)
        AS comparecimentos_30d,

    (SELECT ROUND(
        COUNT(*) FILTER (WHERE compareceu = true)::NUMERIC /
        NULLIF(COUNT(*), 0) * 100, 1
     ) FROM rh_sm_sessoes
     WHERE data_sessao >= CURRENT_DATE - 30)
        AS taxa_adesao_30d,

    -- Distribuição de risco (últimos 30 dias)
    (SELECT COUNT(*) FROM rh_sm_sessoes
     WHERE data_sessao >= CURRENT_DATE - 30 AND compareceu = true
       AND classificacao_risco = 'baixo')
        AS risco_baixo_30d,

    (SELECT COUNT(*) FROM rh_sm_sessoes
     WHERE data_sessao >= CURRENT_DATE - 30 AND compareceu = true
       AND classificacao_risco = 'moderado')
        AS risco_moderado_30d,

    (SELECT COUNT(*) FROM rh_sm_sessoes
     WHERE data_sessao >= CURRENT_DATE - 30 AND compareceu = true
       AND classificacao_risco = 'alto')
        AS risco_alto_30d,

    -- Alertas pendentes
    (SELECT COUNT(*) FROM rh_sm_alertas WHERE status = 'pendente')
        AS alertas_pendentes,

    -- Encaminhamentos pendentes
    (SELECT COUNT(*) FROM rh_sm_encaminhamentos WHERE status = 'pendente')
        AS encaminhamentos_pendentes,

    -- Origem (últimos 90 dias)
    (SELECT COUNT(*) FROM rh_sm_agenda
     WHERE data_agendamento >= CURRENT_DATE - 90 AND tipo_entrada = 'voluntario')
        AS voluntarios_90d,

    (SELECT COUNT(*) FROM rh_sm_agenda
     WHERE data_agendamento >= CURRENT_DATE - 90 AND tipo_entrada = 'indicacao_lider')
        AS indicacoes_lider_90d,

    (SELECT COUNT(*) FROM rh_sm_agenda
     WHERE data_agendamento >= CURRENT_DATE - 90 AND tipo_entrada = 'gatilho_automatico')
        AS gatilhos_automaticos_90d;

COMMENT ON VIEW vw_sm_dashboard IS 'Dashboard agregado do Programa Saúde e Equilíbrio. SEM dados individuais — seguro para diretoria.';


-- ─────────────────────────────────────────────────────────
-- 6.2 AGENDA DA SEMANA
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_sm_agenda_semana AS
SELECT
    a.id,
    a.data_agendamento,
    a.horario_inicio,
    a.horario_fim,
    c.nome AS colaborador_nome,
    c.setor AS colaborador_setor,
    c.cargo AS colaborador_cargo,
    a.tipo_entrada,
    a.motivo_indicacao,
    a.status,
    a.observacoes,
    -- Última classificação de risco (se houver)
    (SELECT s.classificacao_risco
     FROM rh_sm_sessoes s
     WHERE s.colaborador_id = a.colaborador_id
     ORDER BY s.data_sessao DESC
     LIMIT 1) AS ultimo_risco_classificado
FROM rh_sm_agenda a
JOIN rh_colaboradores c ON c.id = a.colaborador_id
WHERE a.data_agendamento BETWEEN date_trunc('week', CURRENT_DATE)
                              AND date_trunc('week', CURRENT_DATE) + INTERVAL '6 days'
  AND a.status IN ('agendado', 'confirmado')
ORDER BY a.data_agendamento, a.horario_inicio;

COMMENT ON VIEW vw_sm_agenda_semana IS 'Agenda semanal dos atendimentos. Herda RLS de rh_sm_agenda.';


-- ─────────────────────────────────────────────────────────
-- 6.3 SETORES CRÍTICOS
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_sm_setores_criticos AS
SELECT
    c.setor,
    COUNT(DISTINCT e.colaborador_id) AS total_encaminhamentos,
    COUNT(DISTINCT s.colaborador_id) AS total_atendidos,
    COUNT(*) FILTER (WHERE s.classificacao_risco = 'alto') AS sessoes_risco_alto,
    COUNT(*) FILTER (WHERE s.classificacao_risco = 'moderado') AS sessoes_risco_moderado,
    COUNT(DISTINCT al.colaborador_id) AS colaboradores_com_alerta,
    -- Score de criticidade: risco_alto*3 + risco_moderado*1 + alertas*2
    (COUNT(*) FILTER (WHERE s.classificacao_risco = 'alto') * 3
     + COUNT(*) FILTER (WHERE s.classificacao_risco = 'moderado') * 1
     + COUNT(DISTINCT al.id) * 2
    ) AS score_criticidade
FROM rh_colaboradores c
LEFT JOIN rh_sm_sessoes s
    ON s.colaborador_id = c.id
    AND s.data_sessao >= CURRENT_DATE - 90
    AND s.compareceu = true
LEFT JOIN rh_sm_encaminhamentos e
    ON e.colaborador_id = c.id
    AND e.data_encaminhamento >= CURRENT_DATE - 90
LEFT JOIN rh_sm_alertas al
    ON al.colaborador_id = c.id
    AND al.status IN ('pendente', 'em_andamento')
WHERE c.status = 'ativo'
  AND c.setor IS NOT NULL
GROUP BY c.setor
HAVING COUNT(DISTINCT s.id) > 0 OR COUNT(DISTINCT e.id) > 0 OR COUNT(DISTINCT al.id) > 0
ORDER BY score_criticidade DESC;

COMMENT ON VIEW vw_sm_setores_criticos IS 'Ranking de setores por criticidade de saúde mental. Últimos 90 dias.';


-- ══════════════════════════════════════════════════════════
-- 7. GRANTS PARA SERVICE ROLE (Supabase)
-- ══════════════════════════════════════════════════════════
-- As funções SECURITY DEFINER já executam com permissão do owner.
-- Para o service_role acessar diretamente (migrations, cron):

GRANT ALL ON rh_sm_agenda          TO service_role;
GRANT ALL ON rh_sm_sessoes         TO service_role;
GRANT ALL ON rh_sm_alertas         TO service_role;
GRANT ALL ON rh_sm_encaminhamentos TO service_role;

-- Authenticated users acessam via RLS
GRANT SELECT, INSERT, UPDATE, DELETE ON rh_sm_agenda          TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON rh_sm_sessoes         TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON rh_sm_alertas         TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON rh_sm_encaminhamentos TO authenticated;

-- Views
GRANT SELECT ON vw_sm_dashboard        TO authenticated;
GRANT SELECT ON vw_sm_agenda_semana    TO authenticated;
GRANT SELECT ON vw_sm_setores_criticos TO authenticated;


-- ══════════════════════════════════════════════════════════
-- FIM DO MÓDULO SAÚDE MENTAL
-- ══════════════════════════════════════════════════════════
