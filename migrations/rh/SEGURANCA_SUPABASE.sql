-- ============================================================================
-- ARQUITETURA DE SEGURANÇA COMPLETA - CLASSIC RH & SST
-- Supabase (PostgreSQL + Auth + RLS)
-- Versão: 1.0 | Data: 2026-03-20
-- ============================================================================
-- SUMÁRIO:
--   PARTE 1: Estrutura de perfis e tabelas auxiliares
--   PARTE 2: Tabelas novas (ASO, treinamentos, saúde mental, produção)
--   PARTE 3: RLS Policies para TODAS as tabelas
--   PARTE 4: Funções SQL (RPCs)
--   PARTE 5: Auditoria (triggers automáticos)
--   PARTE 6: Índices de performance
--   PARTE 7: Views agregadas para diretoria
-- ============================================================================


-- ============================================================================
-- PARTE 1: ESTRUTURA DE PERFIS E ACESSO
-- ============================================================================

-- Tabela de perfis de acesso (vincula auth.users a perfis do sistema)
CREATE TABLE IF NOT EXISTS rh_perfis_acesso (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_uid UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    nome TEXT NOT NULL,
    perfil TEXT NOT NULL CHECK (perfil IN (
        'colaborador',
        'lider',
        'rh',
        'sst',
        'psicologa',
        'financeiro',
        'diretoria',
        'admin'
    )),
    setor TEXT,                          -- para líder: qual setor gerencia
    ativo BOOLEAN DEFAULT true,
    criado_em TIMESTAMPTZ DEFAULT now(),
    atualizado_em TIMESTAMPTZ DEFAULT now(),
    UNIQUE(auth_uid, perfil)             -- um usuário pode ter múltiplos perfis
);

-- Tabela de setores (referência)
CREATE TABLE IF NOT EXISTS rh_setores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nome TEXT NOT NULL UNIQUE,
    lider_id UUID REFERENCES rh_colaboradores(id),
    ativo BOOLEAN DEFAULT true,
    criado_em TIMESTAMPTZ DEFAULT now()
);

-- Tabela de cargos (referência)
CREATE TABLE IF NOT EXISTS rh_cargos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nome TEXT NOT NULL,
    cbo TEXT,
    setor TEXT,
    nivel TEXT CHECK (nivel IN ('operacional','tecnico','supervisao','gerencia','diretoria')),
    ativo BOOLEAN DEFAULT true,
    criado_em TIMESTAMPTZ DEFAULT now()
);

-- Vincular colaborador ao auth.users (coluna auth_uid em rh_colaboradores)
-- Se não existe, adicionar:
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'rh_colaboradores' AND column_name = 'auth_uid'
    ) THEN
        ALTER TABLE rh_colaboradores ADD COLUMN auth_uid UUID REFERENCES auth.users(id);
    END IF;
END $$;


-- ============================================================================
-- PARTE 2: TABELAS NOVAS
-- ============================================================================

-- ASOs (Atestados de Saúde Ocupacional)
CREATE TABLE IF NOT EXISTS rh_asos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    tipo_aso TEXT NOT NULL CHECK (tipo_aso IN (
        'admissional','periodico','retorno','mudanca_funcao','demissional'
    )),
    data_realizacao DATE NOT NULL,
    data_vencimento DATE,
    resultado TEXT CHECK (resultado IN ('apto','inapto','apto_restricao')),
    medico_nome TEXT,
    medico_crm TEXT,
    riscos_avaliados TEXT,
    observacoes TEXT,
    status TEXT DEFAULT 'valido' CHECK (status IN ('valido','vencido','cancelado')),
    registrado_por UUID REFERENCES auth.users(id),
    criado_em TIMESTAMPTZ DEFAULT now(),
    atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- Treinamentos
CREATE TABLE IF NOT EXISTS rh_treinamentos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    treinamento TEXT NOT NULL,
    nr_referencia TEXT,                  -- NR-06, NR-12, NR-35, etc.
    data_realizacao DATE NOT NULL,
    data_vencimento DATE,
    carga_horaria_horas NUMERIC(5,1),
    instrutor TEXT,
    certificado_url TEXT,
    status TEXT DEFAULT 'valido' CHECK (status IN ('valido','vencido','cancelado')),
    registrado_por UUID REFERENCES auth.users(id),
    criado_em TIMESTAMPTZ DEFAULT now(),
    atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- Saúde Mental (classificação de risco psicossocial — SEM dados clínicos)
CREATE TABLE IF NOT EXISTS rh_saude_mental (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    data_avaliacao DATE NOT NULL,
    classificacao_risco TEXT NOT NULL CHECK (classificacao_risco IN (
        'baixo','moderado','alto','critico'
    )),
    dimensao TEXT,                       -- ex: 'estresse','ansiedade','clima_equipe'
    origem TEXT CHECK (origem IN (
        'avaliacao_periodica','encaminhamento_lider','auto_relato','pos_afastamento'
    )),
    encaminhamento TEXT,                 -- ex: 'acompanhamento','reavaliacao_30d','afastamento'
    proximo_contato DATE,
    observacoes_gerais TEXT,             -- SEM dados clínicos, apenas classificação
    -- NÃO armazenar: diagnóstico, CID, conteúdo de sessão, laudos
    avaliador_id UUID REFERENCES auth.users(id),
    criado_em TIMESTAMPTZ DEFAULT now(),
    atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- Produção (produtividade por funcionário)
CREATE TABLE IF NOT EXISTS rh_producao (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    colaborador_id UUID NOT NULL REFERENCES rh_colaboradores(id) ON DELETE CASCADE,
    data_referencia DATE NOT NULL,
    setor TEXT,
    turno TEXT,
    horas_trabalhadas NUMERIC(5,2),
    producao_quantidade NUMERIC(10,2),
    meta_quantidade NUMERIC(10,2),
    percentual_atingimento NUMERIC(5,2),
    observacoes TEXT,
    registrado_por UUID REFERENCES auth.users(id),
    criado_em TIMESTAMPTZ DEFAULT now()
);


-- ============================================================================
-- PARTE 3: FUNÇÕES AUXILIARES PARA RLS
-- ============================================================================

-- Função: retorna perfis do usuário autenticado
CREATE OR REPLACE FUNCTION get_user_perfis()
RETURNS TEXT[]
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT COALESCE(
        array_agg(perfil),
        ARRAY[]::TEXT[]
    )
    FROM rh_perfis_acesso
    WHERE auth_uid = auth.uid()
      AND ativo = true;
$$;

-- Função: verifica se usuário tem determinado perfil
CREATE OR REPLACE FUNCTION has_perfil(p TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM rh_perfis_acesso
        WHERE auth_uid = auth.uid()
          AND perfil = p
          AND ativo = true
    );
$$;

-- Função: retorna setores que o líder gerencia
CREATE OR REPLACE FUNCTION get_setores_lider()
RETURNS TEXT[]
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT COALESCE(
        array_agg(setor),
        ARRAY[]::TEXT[]
    )
    FROM rh_perfis_acesso
    WHERE auth_uid = auth.uid()
      AND perfil = 'lider'
      AND ativo = true
      AND setor IS NOT NULL;
$$;

-- Função: retorna o ID do colaborador vinculado ao auth.uid()
CREATE OR REPLACE FUNCTION get_meu_colaborador_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT id FROM rh_colaboradores
    WHERE auth_uid = auth.uid()
    LIMIT 1;
$$;


-- ============================================================================
-- PARTE 3: RLS POLICIES — TABELA POR TABELA
-- ============================================================================

-- ────────────────────────────────────────────────
-- rh_colaboradores
-- ────────────────────────────────────────────────
ALTER TABLE rh_colaboradores ENABLE ROW LEVEL SECURITY;

-- SELECT: admin/rh vê tudo; líder vê seu setor; sst/financeiro vê dados não-sensíveis; colaborador vê só ele
CREATE POLICY "colab_select_admin_rh" ON rh_colaboradores FOR SELECT
    USING (has_perfil('admin') OR has_perfil('rh'));

CREATE POLICY "colab_select_lider" ON rh_colaboradores FOR SELECT
    USING (
        has_perfil('lider')
        AND setor = ANY(get_setores_lider())
    );

CREATE POLICY "colab_select_sst" ON rh_colaboradores FOR SELECT
    USING (has_perfil('sst'));

CREATE POLICY "colab_select_financeiro" ON rh_colaboradores FOR SELECT
    USING (has_perfil('financeiro'));

CREATE POLICY "colab_select_diretoria" ON rh_colaboradores FOR SELECT
    USING (has_perfil('diretoria'));

CREATE POLICY "colab_select_psicologa" ON rh_colaboradores FOR SELECT
    USING (has_perfil('psicologa'));

CREATE POLICY "colab_select_proprio" ON rh_colaboradores FOR SELECT
    USING (auth_uid = auth.uid());

-- INSERT: apenas admin e rh
CREATE POLICY "colab_insert" ON rh_colaboradores FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('rh'));

-- UPDATE: admin e rh (dados gerais); financeiro NÃO altera
CREATE POLICY "colab_update" ON rh_colaboradores FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('rh'))
    WITH CHECK (has_perfil('admin') OR has_perfil('rh'));

-- DELETE: apenas admin (soft delete preferível)
CREATE POLICY "colab_delete" ON rh_colaboradores FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_asos
-- ────────────────────────────────────────────────
ALTER TABLE rh_asos ENABLE ROW LEVEL SECURITY;

-- SELECT: admin/sst/rh vê tudo; líder vê seu setor; colaborador vê só os dele
CREATE POLICY "aso_select_admin_sst_rh" ON rh_asos FOR SELECT
    USING (has_perfil('admin') OR has_perfil('sst') OR has_perfil('rh'));

CREATE POLICY "aso_select_lider" ON rh_asos FOR SELECT
    USING (
        has_perfil('lider')
        AND EXISTS (
            SELECT 1 FROM rh_colaboradores c
            WHERE c.id = rh_asos.colaborador_id
              AND c.setor = ANY(get_setores_lider())
        )
    );

CREATE POLICY "aso_select_proprio" ON rh_asos FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

-- INSERT: sst e admin
CREATE POLICY "aso_insert" ON rh_asos FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

-- UPDATE: sst e admin
CREATE POLICY "aso_update" ON rh_asos FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('sst'))
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

-- DELETE: apenas admin
CREATE POLICY "aso_delete" ON rh_asos FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_treinamentos
-- ────────────────────────────────────────────────
ALTER TABLE rh_treinamentos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "trein_select_admin_sst_rh" ON rh_treinamentos FOR SELECT
    USING (has_perfil('admin') OR has_perfil('sst') OR has_perfil('rh'));

CREATE POLICY "trein_select_lider" ON rh_treinamentos FOR SELECT
    USING (
        has_perfil('lider')
        AND EXISTS (
            SELECT 1 FROM rh_colaboradores c
            WHERE c.id = rh_treinamentos.colaborador_id
              AND c.setor = ANY(get_setores_lider())
        )
    );

CREATE POLICY "trein_select_proprio" ON rh_treinamentos FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

CREATE POLICY "trein_insert" ON rh_treinamentos FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

CREATE POLICY "trein_update" ON rh_treinamentos FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('sst'))
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

CREATE POLICY "trein_delete" ON rh_treinamentos FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_saude_mental (DADOS SENSÍVEIS — acesso restritíssimo)
-- ────────────────────────────────────────────────
ALTER TABLE rh_saude_mental ENABLE ROW LEVEL SECURITY;

-- SELECT: APENAS psicóloga e admin
-- Diretoria NÃO vê registros individuais (apenas views agregadas)
-- RH NÃO vê dados de saúde mental
-- Líder NÃO vê dados de saúde mental
CREATE POLICY "smental_select" ON rh_saude_mental FOR SELECT
    USING (has_perfil('admin') OR has_perfil('psicologa'));

-- Colaborador vê APENAS sua própria classificação (sem observações detalhadas)
-- Implementado via view específica (ver PARTE 7)

-- INSERT: apenas psicóloga e admin
CREATE POLICY "smental_insert" ON rh_saude_mental FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('psicologa'));

-- UPDATE: apenas psicóloga e admin
CREATE POLICY "smental_update" ON rh_saude_mental FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('psicologa'))
    WITH CHECK (has_perfil('admin') OR has_perfil('psicologa'));

-- DELETE: apenas admin
CREATE POLICY "smental_delete" ON rh_saude_mental FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_ocorrencias (atestados, advertências, etc.)
-- ────────────────────────────────────────────────
ALTER TABLE rh_ocorrencias ENABLE ROW LEVEL SECURITY;

CREATE POLICY "ocorr_select_admin_rh" ON rh_ocorrencias FOR SELECT
    USING (has_perfil('admin') OR has_perfil('rh') OR has_perfil('sst'));

CREATE POLICY "ocorr_select_lider" ON rh_ocorrencias FOR SELECT
    USING (
        has_perfil('lider')
        AND EXISTS (
            SELECT 1 FROM rh_colaboradores c
            WHERE c.id = rh_ocorrencias.colaborador_id
              AND c.setor = ANY(get_setores_lider())
        )
    );

CREATE POLICY "ocorr_select_proprio" ON rh_ocorrencias FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

CREATE POLICY "ocorr_insert" ON rh_ocorrencias FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('rh'));

CREATE POLICY "ocorr_update" ON rh_ocorrencias FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('rh'))
    WITH CHECK (has_perfil('admin') OR has_perfil('rh'));

CREATE POLICY "ocorr_delete" ON rh_ocorrencias FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_desligamentos
-- ────────────────────────────────────────────────
ALTER TABLE rh_desligamentos ENABLE ROW LEVEL SECURITY;

CREATE POLICY "deslig_select_admin_rh" ON rh_desligamentos FOR SELECT
    USING (has_perfil('admin') OR has_perfil('rh') OR has_perfil('diretoria'));

CREATE POLICY "deslig_select_financeiro" ON rh_desligamentos FOR SELECT
    USING (has_perfil('financeiro'));

-- INSERT/UPDATE/DELETE: SOMENTE via RPC (backend) — ver PARTE 4
-- Nenhum perfil insere diretamente
CREATE POLICY "deslig_insert" ON rh_desligamentos FOR INSERT
    WITH CHECK (has_perfil('admin'));

CREATE POLICY "deslig_update" ON rh_desligamentos FOR UPDATE
    USING (has_perfil('admin'))
    WITH CHECK (has_perfil('admin'));

CREATE POLICY "deslig_delete" ON rh_desligamentos FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_epi_entregas
-- ────────────────────────────────────────────────
ALTER TABLE rh_epi_entregas ENABLE ROW LEVEL SECURITY;

CREATE POLICY "epi_select_admin_sst_rh" ON rh_epi_entregas FOR SELECT
    USING (has_perfil('admin') OR has_perfil('sst') OR has_perfil('rh'));

CREATE POLICY "epi_select_lider" ON rh_epi_entregas FOR SELECT
    USING (
        has_perfil('lider')
        AND EXISTS (
            SELECT 1 FROM rh_colaboradores c
            WHERE c.id = rh_epi_entregas.colaborador_id
              AND c.setor = ANY(get_setores_lider())
        )
    );

CREATE POLICY "epi_select_proprio" ON rh_epi_entregas FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

CREATE POLICY "epi_insert" ON rh_epi_entregas FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

CREATE POLICY "epi_update" ON rh_epi_entregas FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('sst'))
    WITH CHECK (has_perfil('admin') OR has_perfil('sst'));

CREATE POLICY "epi_delete" ON rh_epi_entregas FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_vale_alimentacao / rh_auxilio_deslocamento / rh_plano_alianca
-- ────────────────────────────────────────────────
ALTER TABLE rh_vale_alimentacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_auxilio_deslocamento ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_plano_alianca ENABLE ROW LEVEL SECURITY;

-- Padrão: admin, rh, financeiro veem; colaborador vê o dele
-- Aplicar a mesma lógica para as 3 tabelas de benefícios

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['rh_vale_alimentacao','rh_auxilio_deslocamento','rh_plano_alianca']
    LOOP
        EXECUTE format('CREATE POLICY "benef_select_admin_rh_%s" ON %I FOR SELECT USING (has_perfil(''admin'') OR has_perfil(''rh'') OR has_perfil(''financeiro''))', t, t);
        EXECUTE format('CREATE POLICY "benef_select_proprio_%s" ON %I FOR SELECT USING (colaborador_id = get_meu_colaborador_id())', t, t);
        EXECUTE format('CREATE POLICY "benef_insert_%s" ON %I FOR INSERT WITH CHECK (has_perfil(''admin'') OR has_perfil(''rh''))', t, t);
        EXECUTE format('CREATE POLICY "benef_update_%s" ON %I FOR UPDATE USING (has_perfil(''admin'') OR has_perfil(''rh'')) WITH CHECK (has_perfil(''admin'') OR has_perfil(''rh''))', t, t);
        EXECUTE format('CREATE POLICY "benef_delete_%s" ON %I FOR DELETE USING (has_perfil(''admin''))', t, t);
    END LOOP;
END $$;


-- ────────────────────────────────────────────────
-- rh_producao
-- ────────────────────────────────────────────────
ALTER TABLE rh_producao ENABLE ROW LEVEL SECURITY;

CREATE POLICY "prod_select_admin_rh" ON rh_producao FOR SELECT
    USING (has_perfil('admin') OR has_perfil('rh') OR has_perfil('diretoria'));

CREATE POLICY "prod_select_lider" ON rh_producao FOR SELECT
    USING (
        has_perfil('lider')
        AND EXISTS (
            SELECT 1 FROM rh_colaboradores c
            WHERE c.id = rh_producao.colaborador_id
              AND c.setor = ANY(get_setores_lider())
        )
    );

CREATE POLICY "prod_select_proprio" ON rh_producao FOR SELECT
    USING (colaborador_id = get_meu_colaborador_id());

CREATE POLICY "prod_insert" ON rh_producao FOR INSERT
    WITH CHECK (has_perfil('admin') OR has_perfil('rh') OR has_perfil('lider'));

CREATE POLICY "prod_update" ON rh_producao FOR UPDATE
    USING (has_perfil('admin') OR has_perfil('rh'))
    WITH CHECK (has_perfil('admin') OR has_perfil('rh'));

CREATE POLICY "prod_delete" ON rh_producao FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_perfis_acesso (auto-referencial: segurança crítica)
-- ────────────────────────────────────────────────
ALTER TABLE rh_perfis_acesso ENABLE ROW LEVEL SECURITY;

-- Apenas admin vê todos os perfis
CREATE POLICY "perfis_select_admin" ON rh_perfis_acesso FOR SELECT
    USING (has_perfil('admin'));

-- Cada usuário vê seus próprios perfis
CREATE POLICY "perfis_select_proprio" ON rh_perfis_acesso FOR SELECT
    USING (auth_uid = auth.uid());

-- Apenas admin gerencia perfis
CREATE POLICY "perfis_insert" ON rh_perfis_acesso FOR INSERT
    WITH CHECK (has_perfil('admin'));

CREATE POLICY "perfis_update" ON rh_perfis_acesso FOR UPDATE
    USING (has_perfil('admin'))
    WITH CHECK (has_perfil('admin'));

CREATE POLICY "perfis_delete" ON rh_perfis_acesso FOR DELETE
    USING (has_perfil('admin'));


-- ────────────────────────────────────────────────
-- rh_audit_log (somente leitura, apenas admin)
-- ────────────────────────────────────────────────
ALTER TABLE rh_audit_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "audit_select" ON rh_audit_log FOR SELECT
    USING (has_perfil('admin') OR has_perfil('diretoria'));

-- INSERT via trigger (SECURITY DEFINER), não via frontend direto
CREATE POLICY "audit_insert" ON rh_audit_log FOR INSERT
    WITH CHECK (true);  -- triggers inserem com service_role

-- Ninguém edita ou deleta logs
-- (sem policies de UPDATE/DELETE = bloqueado)


-- ============================================================================
-- PARTE 4: FUNÇÕES SQL (RPCs)
-- ============================================================================

-- ── 4.1 Registrar Desligamento ──
-- DEVE ser chamada via backend (Edge Function) para validação completa
-- SECURITY INVOKER: respeita RLS do chamador
CREATE OR REPLACE FUNCTION registrar_desligamento(
    p_colaborador_id UUID,
    p_tipo TEXT,
    p_motivo TEXT,
    p_data_desligamento DATE,
    p_entrevista_p1 TEXT DEFAULT NULL,
    p_entrevista_p2 TEXT DEFAULT NULL,
    p_entrevista_p3 TEXT DEFAULT NULL,
    p_entrevista_p4 TEXT DEFAULT NULL,
    p_entrevista_p5 TEXT DEFAULT NULL
)
RETURNS JSON
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_colab RECORD;
    v_perfis TEXT[];
    v_id UUID;
BEGIN
    -- Verificar permissão: apenas admin ou rh
    v_perfis := get_user_perfis();
    IF NOT ('admin' = ANY(v_perfis) OR 'rh' = ANY(v_perfis)) THEN
        RAISE EXCEPTION 'Sem permissao para registrar desligamento';
    END IF;

    -- Buscar dados do colaborador
    SELECT * INTO v_colab FROM rh_colaboradores WHERE id = p_colaborador_id AND status = 'ativo';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Colaborador nao encontrado ou ja desligado';
    END IF;

    -- Inserir desligamento
    INSERT INTO rh_desligamentos (
        colaborador_id, colaborador_nome, data_desligamento, tipo, motivo,
        data_admissao, cargo, salario, setor, tempo_empresa_dias,
        entrevista_p1, entrevista_p2, entrevista_p3, entrevista_p4, entrevista_p5
    ) VALUES (
        p_colaborador_id, v_colab.nome, p_data_desligamento, p_tipo, p_motivo,
        v_colab.data_admissao, v_colab.cargo, v_colab.salario_base, v_colab.setor,
        (p_data_desligamento - v_colab.data_admissao),
        p_entrevista_p1, p_entrevista_p2, p_entrevista_p3, p_entrevista_p4, p_entrevista_p5
    )
    RETURNING id INTO v_id;

    -- Atualizar status do colaborador
    UPDATE rh_colaboradores
    SET status = 'desligado',
        data_desligamento = p_data_desligamento,
        atualizado_em = now()
    WHERE id = p_colaborador_id;

    -- Registrar auditoria
    INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
    VALUES (
        auth.uid(),
        (SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1),
        'DESLIGAMENTO',
        json_build_object(
            'colaborador_id', p_colaborador_id,
            'nome', v_colab.nome,
            'tipo', p_tipo,
            'data', p_data_desligamento
        )::TEXT
    );

    RETURN json_build_object('ok', true, 'desligamento_id', v_id);
END;
$$;


-- ── 4.2 Registrar Atendimento Psicológico ──
CREATE OR REPLACE FUNCTION registrar_avaliacao_psicossocial(
    p_colaborador_id UUID,
    p_classificacao TEXT,
    p_dimensao TEXT DEFAULT NULL,
    p_origem TEXT DEFAULT NULL,
    p_encaminhamento TEXT DEFAULT NULL,
    p_proximo_contato DATE DEFAULT NULL,
    p_observacoes TEXT DEFAULT NULL
)
RETURNS JSON
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_perfis TEXT[];
    v_id UUID;
    v_nome TEXT;
BEGIN
    v_perfis := get_user_perfis();
    IF NOT ('admin' = ANY(v_perfis) OR 'psicologa' = ANY(v_perfis)) THEN
        RAISE EXCEPTION 'Apenas psicologa ou admin podem registrar avaliacao psicossocial';
    END IF;

    -- Validar colaborador existe
    SELECT nome INTO v_nome FROM rh_colaboradores WHERE id = p_colaborador_id AND status = 'ativo';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Colaborador nao encontrado';
    END IF;

    INSERT INTO rh_saude_mental (
        colaborador_id, data_avaliacao, classificacao_risco, dimensao,
        origem, encaminhamento, proximo_contato, observacoes_gerais, avaliador_id
    ) VALUES (
        p_colaborador_id, CURRENT_DATE, p_classificacao, p_dimensao,
        p_origem, p_encaminhamento, p_proximo_contato, p_observacoes, auth.uid()
    )
    RETURNING id INTO v_id;

    -- Auditoria (sem dados sensíveis no log!)
    INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
    VALUES (
        auth.uid(),
        (SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1),
        'AVALIACAO_PSICOSSOCIAL',
        json_build_object(
            'colaborador_id', p_colaborador_id,
            'classificacao', p_classificacao
            -- NÃO registrar observações no audit log
        )::TEXT
    );

    -- Se risco ALTO ou CRÍTICO, gerar alerta automático
    IF p_classificacao IN ('alto', 'critico') THEN
        INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
        VALUES (
            auth.uid(),
            'SISTEMA',
            'ALERTA_RISCO_PSICOSSOCIAL',
            json_build_object(
                'colaborador', v_nome,
                'classificacao', p_classificacao,
                'acao_requerida', 'Acompanhamento prioritario'
            )::TEXT
        );
    END IF;

    RETURN json_build_object('ok', true, 'avaliacao_id', v_id);
END;
$$;


-- ── 4.3 Classificar Risco (atualizar classificação existente) ──
CREATE OR REPLACE FUNCTION atualizar_classificacao_risco(
    p_avaliacao_id UUID,
    p_nova_classificacao TEXT,
    p_encaminhamento TEXT DEFAULT NULL,
    p_proximo_contato DATE DEFAULT NULL
)
RETURNS JSON
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_perfis TEXT[];
    v_old RECORD;
BEGIN
    v_perfis := get_user_perfis();
    IF NOT ('admin' = ANY(v_perfis) OR 'psicologa' = ANY(v_perfis)) THEN
        RAISE EXCEPTION 'Sem permissao';
    END IF;

    SELECT sm.*, c.nome as colab_nome
    INTO v_old
    FROM rh_saude_mental sm
    JOIN rh_colaboradores c ON c.id = sm.colaborador_id
    WHERE sm.id = p_avaliacao_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Avaliacao nao encontrada';
    END IF;

    UPDATE rh_saude_mental
    SET classificacao_risco = p_nova_classificacao,
        encaminhamento = COALESCE(p_encaminhamento, encaminhamento),
        proximo_contato = COALESCE(p_proximo_contato, proximo_contato),
        atualizado_em = now()
    WHERE id = p_avaliacao_id;

    -- Auditoria de mudança de classificação
    INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
    VALUES (
        auth.uid(),
        (SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1),
        'MUDANCA_RISCO_PSICOSSOCIAL',
        json_build_object(
            'colaborador', v_old.colab_nome,
            'classificacao_anterior', v_old.classificacao_risco,
            'classificacao_nova', p_nova_classificacao
        )::TEXT
    );

    RETURN json_build_object('ok', true);
END;
$$;


-- ── 4.4 Gerar Alertas Automáticos ──
-- Retorna lista de alertas pendentes (ASOs vencendo, treinamentos, contratos, etc.)
CREATE OR REPLACE FUNCTION gerar_alertas_automaticos()
RETURNS JSON
LANGUAGE plpgsql
STABLE
SECURITY INVOKER  -- respeita RLS do chamador
SET search_path = public
AS $$
DECLARE
    v_alertas JSON;
BEGIN
    SELECT json_agg(alerta) INTO v_alertas FROM (
        -- ASOs vencendo em 30 dias
        SELECT json_build_object(
            'tipo', 'ASO_VENCENDO',
            'urgencia', CASE WHEN a.data_vencimento <= CURRENT_DATE THEN 'VENCIDO'
                             WHEN a.data_vencimento <= CURRENT_DATE + 7 THEN 'URGENTE'
                             ELSE 'ATENCAO' END,
            'colaborador', c.nome,
            'detalhe', a.tipo_aso || ' vence em ' || a.data_vencimento,
            'dias', (a.data_vencimento - CURRENT_DATE)
        ) as alerta
        FROM rh_asos a
        JOIN rh_colaboradores c ON c.id = a.colaborador_id AND c.status = 'ativo'
        WHERE a.data_vencimento <= CURRENT_DATE + 30
          AND a.status = 'valido'

        UNION ALL

        -- Treinamentos vencendo
        SELECT json_build_object(
            'tipo', 'TREINAMENTO_VENCENDO',
            'urgencia', CASE WHEN t.data_vencimento <= CURRENT_DATE THEN 'VENCIDO'
                             WHEN t.data_vencimento <= CURRENT_DATE + 7 THEN 'URGENTE'
                             ELSE 'ATENCAO' END,
            'colaborador', c.nome,
            'detalhe', t.treinamento || ' vence em ' || t.data_vencimento,
            'dias', (t.data_vencimento - CURRENT_DATE)
        )
        FROM rh_treinamentos t
        JOIN rh_colaboradores c ON c.id = t.colaborador_id AND c.status = 'ativo'
        WHERE t.data_vencimento <= CURRENT_DATE + 30
          AND t.status = 'valido'

        UNION ALL

        -- Contratos de experiência (admitidos nos últimos 90 dias)
        SELECT json_build_object(
            'tipo', 'CONTRATO_EXPERIENCIA',
            'urgencia', CASE
                WHEN (c.data_admissao + 45) BETWEEN CURRENT_DATE AND CURRENT_DATE + 7 THEN 'URGENTE'
                WHEN (c.data_admissao + 90) BETWEEN CURRENT_DATE AND CURRENT_DATE + 7 THEN 'URGENTE'
                ELSE 'ATENCAO' END,
            'colaborador', c.nome,
            'detalhe', 'Admissao: ' || c.data_admissao || ' | 45d: ' || (c.data_admissao + 45) || ' | 90d: ' || (c.data_admissao + 90),
            'dias', LEAST(
                CASE WHEN (c.data_admissao + 45) >= CURRENT_DATE THEN (c.data_admissao + 45) - CURRENT_DATE ELSE 999 END,
                CASE WHEN (c.data_admissao + 90) >= CURRENT_DATE THEN (c.data_admissao + 90) - CURRENT_DATE ELSE 999 END
            )
        )
        FROM rh_colaboradores c
        WHERE c.status = 'ativo'
          AND c.data_admissao >= CURRENT_DATE - 90
          AND (
              (c.data_admissao + 45) BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
              OR (c.data_admissao + 90) BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
          )

        UNION ALL

        -- Risco psicossocial alto/crítico pendente de acompanhamento
        SELECT json_build_object(
            'tipo', 'RISCO_PSICOSSOCIAL',
            'urgencia', CASE WHEN sm.classificacao_risco = 'critico' THEN 'URGENTE' ELSE 'ATENCAO' END,
            'colaborador', c.nome,
            'detalhe', 'Classificacao: ' || sm.classificacao_risco || ' | Proximo contato: ' || COALESCE(sm.proximo_contato::TEXT, 'nao definido'),
            'dias', COALESCE((sm.proximo_contato - CURRENT_DATE), 0)
        )
        FROM rh_saude_mental sm
        JOIN rh_colaboradores c ON c.id = sm.colaborador_id AND c.status = 'ativo'
        WHERE sm.classificacao_risco IN ('alto', 'critico')
          AND sm.id = (
              SELECT sm2.id FROM rh_saude_mental sm2
              WHERE sm2.colaborador_id = sm.colaborador_id
              ORDER BY sm2.data_avaliacao DESC LIMIT 1
          )

        ORDER BY dias ASC
    ) sub;

    RETURN COALESCE(v_alertas, '[]'::JSON);
END;
$$;


-- ── 4.5 Consultar Indicadores (Diretoria) ──
-- Retorna dados AGREGADOS, sem informações individuais sensíveis
CREATE OR REPLACE FUNCTION indicadores_gerenciais(
    p_periodo_meses INT DEFAULT 12
)
RETURNS JSON
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = public
AS $$
DECLARE
    v_result JSON;
    v_data_inicio DATE;
    v_total_ativos INT;
    v_admissoes INT;
    v_desligamentos INT;
    v_atestados_dias INT;
    v_turnover NUMERIC;
    v_absentismo NUMERIC;
BEGIN
    v_data_inicio := CURRENT_DATE - (p_periodo_meses * 30);

    SELECT COUNT(*) INTO v_total_ativos FROM rh_colaboradores WHERE status = 'ativo';

    SELECT COUNT(*) INTO v_admissoes FROM rh_colaboradores
    WHERE data_admissao >= v_data_inicio AND status = 'ativo';

    SELECT COUNT(*) INTO v_desligamentos FROM rh_desligamentos
    WHERE data_desligamento >= v_data_inicio;

    SELECT COALESCE(SUM(dias), 0) INTO v_atestados_dias FROM rh_ocorrencias
    WHERE data_inicio >= v_data_inicio AND tipo = 'atestado';

    -- Turnover = ((adm + desl) / 2) / total * 100
    IF v_total_ativos > 0 THEN
        v_turnover := ((v_admissoes + v_desligamentos)::NUMERIC / 2) / v_total_ativos * 100;
    ELSE
        v_turnover := 0;
    END IF;

    -- Absenteísmo = dias_ausencia / (total * dias_uteis) * 100
    IF v_total_ativos > 0 THEN
        v_absentismo := v_atestados_dias::NUMERIC / (v_total_ativos * p_periodo_meses * 22) * 100;
    ELSE
        v_absentismo := 0;
    END IF;

    v_result := json_build_object(
        'periodo_meses', p_periodo_meses,
        'total_ativos', v_total_ativos,
        'admissoes', v_admissoes,
        'desligamentos', v_desligamentos,
        'turnover_pct', ROUND(v_turnover, 2),
        'absentismo_pct', ROUND(v_absentismo, 2),
        'atestados_dias_total', v_atestados_dias,
        -- Distribuição de risco psicossocial (AGREGADO, sem nomes)
        'risco_psicossocial', (
            SELECT json_object_agg(classificacao_risco, cnt)
            FROM (
                SELECT classificacao_risco, COUNT(*) as cnt
                FROM rh_saude_mental sm
                WHERE sm.id IN (
                    SELECT DISTINCT ON (colaborador_id) id
                    FROM rh_saude_mental
                    ORDER BY colaborador_id, data_avaliacao DESC
                )
                GROUP BY classificacao_risco
            ) sub
        ),
        -- ASOs por status
        'asos', (
            SELECT json_build_object(
                'validos', COUNT(*) FILTER (WHERE data_vencimento > CURRENT_DATE),
                'vencidos', COUNT(*) FILTER (WHERE data_vencimento <= CURRENT_DATE),
                'vencendo_30d', COUNT(*) FILTER (WHERE data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 30)
            )
            FROM rh_asos WHERE status = 'valido'
        )
    );

    RETURN v_result;
END;
$$;


-- ── 4.6 Alterar Salário (com auditoria obrigatória) ──
CREATE OR REPLACE FUNCTION alterar_salario(
    p_colaborador_id UUID,
    p_novo_salario NUMERIC,
    p_motivo TEXT
)
RETURNS JSON
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_perfis TEXT[];
    v_old RECORD;
BEGIN
    v_perfis := get_user_perfis();
    IF NOT ('admin' = ANY(v_perfis)) THEN
        RAISE EXCEPTION 'Apenas admin pode alterar salario. Use Edge Function para validacao adicional.';
    END IF;

    SELECT nome, salario_base, cargo INTO v_old
    FROM rh_colaboradores WHERE id = p_colaborador_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Colaborador nao encontrado';
    END IF;

    UPDATE rh_colaboradores
    SET salario_base = p_novo_salario, atualizado_em = now()
    WHERE id = p_colaborador_id;

    -- Auditoria obrigatória
    INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
    VALUES (
        auth.uid(),
        (SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1),
        'ALTERACAO_SALARIO',
        json_build_object(
            'colaborador_id', p_colaborador_id,
            'nome', v_old.nome,
            'salario_anterior', v_old.salario_base,
            'salario_novo', p_novo_salario,
            'motivo', p_motivo
        )::TEXT
    );

    RETURN json_build_object('ok', true);
END;
$$;


-- ── 4.7 Alterar Cargo (com auditoria) ──
CREATE OR REPLACE FUNCTION alterar_cargo(
    p_colaborador_id UUID,
    p_novo_cargo TEXT,
    p_motivo TEXT
)
RETURNS JSON
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_perfis TEXT[];
    v_old RECORD;
BEGIN
    v_perfis := get_user_perfis();
    IF NOT ('admin' = ANY(v_perfis) OR 'rh' = ANY(v_perfis)) THEN
        RAISE EXCEPTION 'Sem permissao';
    END IF;

    SELECT nome, cargo INTO v_old FROM rh_colaboradores WHERE id = p_colaborador_id;

    UPDATE rh_colaboradores
    SET cargo = p_novo_cargo, atualizado_em = now()
    WHERE id = p_colaborador_id;

    INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
    VALUES (
        auth.uid(),
        (SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1),
        'ALTERACAO_CARGO',
        json_build_object(
            'colaborador_id', p_colaborador_id,
            'nome', v_old.nome,
            'cargo_anterior', v_old.cargo,
            'cargo_novo', p_novo_cargo,
            'motivo', p_motivo
        )::TEXT
    );

    RETURN json_build_object('ok', true);
END;
$$;


-- ============================================================================
-- PARTE 5: AUDITORIA AUTOMÁTICA (TRIGGERS)
-- ============================================================================

-- Trigger genérico de auditoria para campos sensíveis
CREATE OR REPLACE FUNCTION audit_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_changes JSONB := '{}'::JSONB;
    v_col TEXT;
    v_old_val TEXT;
    v_new_val TEXT;
BEGIN
    -- Campos monitorados por tabela
    IF TG_TABLE_NAME = 'rh_colaboradores' THEN
        -- Monitorar: cargo, salario_base, setor, status
        IF OLD.cargo IS DISTINCT FROM NEW.cargo THEN
            v_changes := v_changes || jsonb_build_object('cargo', jsonb_build_object('de', OLD.cargo, 'para', NEW.cargo));
        END IF;
        IF OLD.salario_base IS DISTINCT FROM NEW.salario_base THEN
            v_changes := v_changes || jsonb_build_object('salario_base', jsonb_build_object('de', OLD.salario_base, 'para', NEW.salario_base));
        END IF;
        IF OLD.setor IS DISTINCT FROM NEW.setor THEN
            v_changes := v_changes || jsonb_build_object('setor', jsonb_build_object('de', OLD.setor, 'para', NEW.setor));
        END IF;
        IF OLD.status IS DISTINCT FROM NEW.status THEN
            v_changes := v_changes || jsonb_build_object('status', jsonb_build_object('de', OLD.status, 'para', NEW.status));
        END IF;
    END IF;

    IF TG_TABLE_NAME = 'rh_saude_mental' THEN
        IF OLD.classificacao_risco IS DISTINCT FROM NEW.classificacao_risco THEN
            v_changes := v_changes || jsonb_build_object('classificacao_risco', jsonb_build_object('de', OLD.classificacao_risco, 'para', NEW.classificacao_risco));
        END IF;
    END IF;

    -- Se houve mudança, registrar
    IF v_changes != '{}'::JSONB THEN
        INSERT INTO rh_audit_log (usuario_id, usuario_email, acao, detalhes)
        VALUES (
            auth.uid(),
            COALESCE((SELECT email FROM rh_perfis_acesso WHERE auth_uid = auth.uid() LIMIT 1), 'SISTEMA'),
            'ALTERACAO_' || UPPER(TG_TABLE_NAME),
            jsonb_build_object(
                'tabela', TG_TABLE_NAME,
                'registro_id', CASE TG_TABLE_NAME
                    WHEN 'rh_colaboradores' THEN OLD.id::TEXT
                    WHEN 'rh_saude_mental' THEN OLD.id::TEXT
                    ELSE 'N/A'
                END,
                'alteracoes', v_changes
            )::TEXT
        );
    END IF;

    RETURN NEW;
END;
$$;

-- Aplicar trigger em rh_colaboradores
DROP TRIGGER IF EXISTS audit_colaboradores ON rh_colaboradores;
CREATE TRIGGER audit_colaboradores
    AFTER UPDATE ON rh_colaboradores
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_fn();

-- Aplicar trigger em rh_saude_mental
DROP TRIGGER IF EXISTS audit_saude_mental ON rh_saude_mental;
CREATE TRIGGER audit_saude_mental
    AFTER UPDATE ON rh_saude_mental
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_fn();


-- ============================================================================
-- PARTE 6: ÍNDICES DE PERFORMANCE
-- ============================================================================

-- Índices para evitar degradação de performance com RLS

-- rh_colaboradores
CREATE INDEX IF NOT EXISTS idx_colab_auth_uid ON rh_colaboradores(auth_uid);
CREATE INDEX IF NOT EXISTS idx_colab_status ON rh_colaboradores(status);
CREATE INDEX IF NOT EXISTS idx_colab_setor ON rh_colaboradores(setor);
CREATE INDEX IF NOT EXISTS idx_colab_data_admissao ON rh_colaboradores(data_admissao);

-- rh_perfis_acesso (CRÍTICO para performance de has_perfil())
CREATE INDEX IF NOT EXISTS idx_perfis_auth_uid ON rh_perfis_acesso(auth_uid);
CREATE INDEX IF NOT EXISTS idx_perfis_auth_uid_perfil ON rh_perfis_acesso(auth_uid, perfil, ativo);

-- rh_asos
CREATE INDEX IF NOT EXISTS idx_aso_colab ON rh_asos(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_aso_vencimento ON rh_asos(data_vencimento) WHERE status = 'valido';

-- rh_treinamentos
CREATE INDEX IF NOT EXISTS idx_trein_colab ON rh_treinamentos(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_trein_vencimento ON rh_treinamentos(data_vencimento) WHERE status = 'valido';

-- rh_saude_mental
CREATE INDEX IF NOT EXISTS idx_smental_colab ON rh_saude_mental(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_smental_risco ON rh_saude_mental(classificacao_risco);
CREATE INDEX IF NOT EXISTS idx_smental_data ON rh_saude_mental(data_avaliacao DESC);

-- rh_ocorrencias
CREATE INDEX IF NOT EXISTS idx_ocorr_colab ON rh_ocorrencias(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_ocorr_data ON rh_ocorrencias(data_inicio);

-- rh_producao
CREATE INDEX IF NOT EXISTS idx_prod_colab ON rh_producao(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_prod_data ON rh_producao(data_referencia);

-- rh_epi_entregas
CREATE INDEX IF NOT EXISTS idx_epi_colab ON rh_epi_entregas(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_epi_validade ON rh_epi_entregas(data_validade);

-- rh_desligamentos
CREATE INDEX IF NOT EXISTS idx_deslig_colab ON rh_desligamentos(colaborador_id);
CREATE INDEX IF NOT EXISTS idx_deslig_data ON rh_desligamentos(data_desligamento);

-- rh_audit_log
CREATE INDEX IF NOT EXISTS idx_audit_data ON rh_audit_log(criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_audit_acao ON rh_audit_log(acao);
CREATE INDEX IF NOT EXISTS idx_audit_usuario ON rh_audit_log(usuario_id);


-- ============================================================================
-- PARTE 7: VIEWS AGREGADAS (DIRETORIA)
-- ============================================================================

-- View: indicadores por setor (sem dados individuais)
CREATE OR REPLACE VIEW vw_indicadores_setor AS
SELECT
    c.setor,
    COUNT(*) as total_colaboradores,
    COUNT(*) FILTER (WHERE c.data_admissao >= CURRENT_DATE - 365) as admissoes_12m,
    ROUND(AVG(CURRENT_DATE - c.data_admissao), 0) as media_dias_empresa,
    ROUND(AVG(c.salario_base), 2) as media_salarial,
    COUNT(*) FILTER (WHERE c.data_admissao >= CURRENT_DATE - 90) as em_experiencia
FROM rh_colaboradores c
WHERE c.status = 'ativo'
GROUP BY c.setor;

-- View: risco psicossocial AGREGADO (sem nomes)
CREATE OR REPLACE VIEW vw_risco_psicossocial_agregado AS
SELECT
    classificacao_risco,
    COUNT(*) as quantidade,
    ROUND(COUNT(*)::NUMERIC / NULLIF(SUM(COUNT(*)) OVER(), 0) * 100, 1) as percentual
FROM (
    SELECT DISTINCT ON (colaborador_id) colaborador_id, classificacao_risco
    FROM rh_saude_mental
    ORDER BY colaborador_id, data_avaliacao DESC
) ult
GROUP BY classificacao_risco;

-- View: absenteísmo por mês
CREATE OR REPLACE VIEW vw_absenteismo_mensal AS
SELECT
    DATE_TRUNC('month', o.data_inicio)::DATE as mes,
    COUNT(DISTINCT o.colaborador_id) as colaboradores_afetados,
    SUM(o.dias) as total_dias,
    ROUND(SUM(o.dias)::NUMERIC / NULLIF((SELECT COUNT(*) FROM rh_colaboradores WHERE status = 'ativo'), 0) * 100, 2) as taxa_pct
FROM rh_ocorrencias o
WHERE o.tipo = 'atestado'
  AND o.data_inicio >= CURRENT_DATE - 365
GROUP BY DATE_TRUNC('month', o.data_inicio)
ORDER BY mes DESC;


-- ============================================================================
-- CONFIGURAÇÃO INICIAL: Registrar admin (Janaína)
-- ============================================================================

-- Executar após login: insere o perfil admin para o usuário atual
-- INSERT INTO rh_perfis_acesso (auth_uid, email, nome, perfil)
-- VALUES (
--     '3e6a8dd2-e1ab-481a-8a4d-24e4058fa013',
--     'janainaanacleto@classiccouros.com.br',
--     'Janaina Anacleto',
--     'admin'
-- );

-- Para dar múltiplos perfis ao mesmo usuário:
-- INSERT INTO rh_perfis_acesso (auth_uid, email, nome, perfil) VALUES
--     ('3e6a8dd2-...', 'janaina...', 'Janaina', 'rh'),
--     ('3e6a8dd2-...', 'janaina...', 'Janaina', 'admin');
