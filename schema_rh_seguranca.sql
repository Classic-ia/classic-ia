-- ============================================================
-- Segurança RH/SST — RLS, RPC Functions, Policies
-- Classic BI — v2026.03
-- ============================================================
-- IMPORTANTE: Executar DEPOIS do schema_rh_sst.sql
-- ============================================================

-- ══════════════════════════════════════════════════════
-- 1. TABELA DE PERFIS DE SISTEMA (auth-level)
-- ══════════════════════════════════════════════════════

-- Mapeamento: usuario do sistema → perfil de segurança
CREATE TABLE IF NOT EXISTS rh_acesso_sistema (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  auth_user_id UUID NOT NULL,         -- auth.uid()
  perfil_seguranca TEXT NOT NULL CHECK (perfil_seguranca IN (
    'colaborador','lider','rh','sst','psicologa','financeiro','diretoria','admin'
  )),
  departamento_id UUID REFERENCES rh_departamentos(id),
  funcionario_id UUID REFERENCES rh_funcionarios(id),  -- vínculo com cadastro RH
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_acesso_auth ON rh_acesso_sistema(auth_user_id);
CREATE INDEX IF NOT EXISTS idx_acesso_perfil ON rh_acesso_sistema(perfil_seguranca);

-- ══════════════════════════════════════════════════════
-- 2. FUNÇÃO AUXILIAR: Obter perfil do usuário logado
-- ══════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rh_meu_perfil()
RETURNS TEXT AS $$
  SELECT perfil_seguranca
  FROM rh_acesso_sistema
  WHERE auth_user_id = auth.uid()
    AND ativo = true
  LIMIT 1;
$$ LANGUAGE sql STABLE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION rh_meu_departamento()
RETURNS UUID AS $$
  SELECT departamento_id
  FROM rh_acesso_sistema
  WHERE auth_user_id = auth.uid()
    AND ativo = true
  LIMIT 1;
$$ LANGUAGE sql STABLE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION rh_meu_funcionario_id()
RETURNS UUID AS $$
  SELECT funcionario_id
  FROM rh_acesso_sistema
  WHERE auth_user_id = auth.uid()
    AND ativo = true
  LIMIT 1;
$$ LANGUAGE sql STABLE SECURITY DEFINER;

-- ══════════════════════════════════════════════════════
-- 3. HABILITAR RLS EM TODAS AS TABELAS
-- ══════════════════════════════════════════════════════

ALTER TABLE rh_funcionarios ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_departamentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_cargos ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_restricoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_absenteismo ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_desligamentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_entrevista_desligamento ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_aso ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_treinamentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_treinamento_funcionario ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_epis ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_epi_entrega ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_epi_cargo ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_cat ENABLE ROW LEVEL SECURITY;
ALTER TABLE sst_inspecoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sm_atendimentos ENABLE ROW LEVEL SECURITY;
ALTER TABLE sm_alertas ENABLE ROW LEVEL SECURITY;
ALTER TABLE rh_audit_log ENABLE ROW LEVEL SECURITY;

-- ══════════════════════════════════════════════════════
-- 4. POLICIES — FUNCIONÁRIOS (dados sensíveis)
-- ══════════════════════════════════════════════════════

-- Colaborador: só vê seus próprios dados
CREATE POLICY func_colaborador_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND id = rh_meu_funcionario_id()
  );

-- Líder: vê apenas sua equipe (mesmo departamento)
CREATE POLICY func_lider_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND departamento_id = rh_meu_departamento()
  );

-- RH: vê todos (dados administrativos)
CREATE POLICY func_rh_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() IN ('rh','admin')
  );
CREATE POLICY func_rh_insert ON rh_funcionarios
  FOR INSERT WITH CHECK (
    rh_meu_perfil() IN ('rh','admin')
  );
CREATE POLICY func_rh_update ON rh_funcionarios
  FOR UPDATE USING (
    rh_meu_perfil() IN ('rh','admin')
  );

-- SST: vê todos (necessário para controle de ASO/EPI)
CREATE POLICY func_sst_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'sst'
  );

-- Psicóloga: vê dados básicos (nome, setor, cargo — sem CPF, salário)
-- Controlado via view (ver abaixo)
CREATE POLICY func_psicologa_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'psicologa'
  );

-- Financeiro: vê dados para folha
CREATE POLICY func_financeiro_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'financeiro'
  );

-- Diretoria: vê todos (somente leitura)
CREATE POLICY func_diretoria_select ON rh_funcionarios
  FOR SELECT USING (
    rh_meu_perfil() = 'diretoria'
  );

-- Admin: delete
CREATE POLICY func_admin_delete ON rh_funcionarios
  FOR DELETE USING (
    rh_meu_perfil() = 'admin'
  );

-- ══════════════════════════════════════════════════════
-- 5. POLICIES — ASO (dados de saúde ocupacional)
-- ══════════════════════════════════════════════════════

-- Colaborador: só seus ASOs
CREATE POLICY aso_colaborador_select ON sst_aso
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );

-- SST + RH + Admin: acesso total
CREATE POLICY aso_sst_select ON sst_aso
  FOR SELECT USING (rh_meu_perfil() IN ('sst','rh','admin'));
CREATE POLICY aso_sst_insert ON sst_aso
  FOR INSERT WITH CHECK (rh_meu_perfil() IN ('sst','rh','admin'));
CREATE POLICY aso_sst_update ON sst_aso
  FOR UPDATE USING (rh_meu_perfil() IN ('sst','rh','admin'));

-- Líder: vê ASOs de sua equipe (somente leitura)
CREATE POLICY aso_lider_select ON sst_aso
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );

-- ══════════════════════════════════════════════════════
-- 6. POLICIES — TREINAMENTOS
-- ══════════════════════════════════════════════════════

-- Catálogo: todos podem ver
CREATE POLICY trein_catalogo_select ON sst_treinamentos
  FOR SELECT USING (true);
CREATE POLICY trein_catalogo_modify ON sst_treinamentos
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

-- Treinamento-funcionário
CREATE POLICY trein_func_colaborador ON sst_treinamento_funcionario
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );
CREATE POLICY trein_func_lider ON sst_treinamento_funcionario
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );
CREATE POLICY trein_func_sst ON sst_treinamento_funcionario
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

-- ══════════════════════════════════════════════════════
-- 7. POLICIES — EPIs
-- ══════════════════════════════════════════════════════

CREATE POLICY epi_catalogo_select ON sst_epis
  FOR SELECT USING (true);
CREATE POLICY epi_catalogo_modify ON sst_epis
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

CREATE POLICY epi_entrega_colaborador ON sst_epi_entrega
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );
CREATE POLICY epi_entrega_sst ON sst_epi_entrega
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

CREATE POLICY epi_cargo_select ON sst_epi_cargo
  FOR SELECT USING (true);
CREATE POLICY epi_cargo_modify ON sst_epi_cargo
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

-- ══════════════════════════════════════════════════════
-- 8. POLICIES — CAT (Acidente de Trabalho)
-- ══════════════════════════════════════════════════════

CREATE POLICY cat_sst_all ON sst_cat
  FOR ALL USING (rh_meu_perfil() IN ('sst','rh','admin'));

CREATE POLICY cat_lider_select ON sst_cat
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );
CREATE POLICY cat_lider_insert ON sst_cat
  FOR INSERT WITH CHECK (
    rh_meu_perfil() = 'lider'
  );

-- ══════════════════════════════════════════════════════
-- 9. POLICIES — SAÚDE MENTAL (máximo isolamento)
-- ══════════════════════════════════════════════════════

-- Psicóloga: acesso total a atendimentos
CREATE POLICY sm_psicologa_all ON sm_atendimentos
  FOR ALL USING (rh_meu_perfil() = 'psicologa');

-- RH: vê apenas classificação e recomendação (não detalhe)
-- Controlado via view sm_atendimentos_rh (abaixo)
CREATE POLICY sm_rh_select ON sm_atendimentos
  FOR SELECT USING (rh_meu_perfil() IN ('rh','admin'));

-- Colaborador: vê só seus agendamentos
CREATE POLICY sm_colaborador_select ON sm_atendimentos
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );

-- Alertas: RH + psicóloga + admin
CREATE POLICY sm_alertas_select ON sm_alertas
  FOR SELECT USING (rh_meu_perfil() IN ('rh','psicologa','admin'));
CREATE POLICY sm_alertas_modify ON sm_alertas
  FOR ALL USING (rh_meu_perfil() IN ('rh','psicologa','admin'));

-- ══════════════════════════════════════════════════════
-- 10. POLICIES — DESLIGAMENTOS
-- ══════════════════════════════════════════════════════

CREATE POLICY deslig_rh_all ON rh_desligamentos
  FOR ALL USING (rh_meu_perfil() IN ('rh','admin'));

CREATE POLICY deslig_diretoria_select ON rh_desligamentos
  FOR SELECT USING (rh_meu_perfil() = 'diretoria');

CREATE POLICY deslig_lider_select ON rh_desligamentos
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );

CREATE POLICY entrev_rh_all ON rh_entrevista_desligamento
  FOR ALL USING (rh_meu_perfil() IN ('rh','admin'));

CREATE POLICY entrev_diretoria_select ON rh_entrevista_desligamento
  FOR SELECT USING (rh_meu_perfil() = 'diretoria');

-- ══════════════════════════════════════════════════════
-- 11. POLICIES — ABSENTEÍSMO, RESTRIÇÕES
-- ══════════════════════════════════════════════════════

CREATE POLICY abs_colaborador ON rh_absenteismo
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );
CREATE POLICY abs_lider ON rh_absenteismo
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );
CREATE POLICY abs_rh_all ON rh_absenteismo
  FOR ALL USING (rh_meu_perfil() IN ('rh','sst','admin'));

CREATE POLICY restr_colaborador ON rh_restricoes
  FOR SELECT USING (
    rh_meu_perfil() = 'colaborador'
    AND funcionario_id = rh_meu_funcionario_id()
  );
CREATE POLICY restr_lider ON rh_restricoes
  FOR SELECT USING (
    rh_meu_perfil() = 'lider'
    AND funcionario_id IN (
      SELECT id FROM rh_funcionarios WHERE departamento_id = rh_meu_departamento()
    )
  );
CREATE POLICY restr_sst_all ON rh_restricoes
  FOR ALL USING (rh_meu_perfil() IN ('rh','sst','admin'));

-- ══════════════════════════════════════════════════════
-- 12. POLICIES — AUDIT LOG (somente admin/rh podem ler)
-- ══════════════════════════════════════════════════════

CREATE POLICY audit_insert ON rh_audit_log
  FOR INSERT WITH CHECK (true);  -- todos podem gerar log

CREATE POLICY audit_select ON rh_audit_log
  FOR SELECT USING (rh_meu_perfil() IN ('rh','admin'));

-- ══════════════════════════════════════════════════════
-- 13. VIEWS SEGURAS (isolamento de dados sensíveis)
-- ══════════════════════════════════════════════════════

-- View para psicóloga: dados básicos do funcionário, sem CPF/salário
CREATE OR REPLACE VIEW v_funcionarios_basico AS
SELECT
  id, nome, departamento_id, cargo_id,
  status, turno, data_admissao
FROM rh_funcionarios;

-- View para RH: atendimentos sem detalhe clínico
CREATE OR REPLACE VIEW v_sm_atendimentos_rh AS
SELECT
  a.id,
  a.funcionario_id,
  f.nome AS funcionario_nome,
  d.nome AS departamento,
  a.data_agendamento,
  a.data_atendimento,
  a.origem,
  a.compareceu,
  a.classificacao_risco,
  a.recomendacao,
  a.status
FROM sm_atendimentos a
JOIN rh_funcionarios f ON f.id = a.funcionario_id
LEFT JOIN rh_departamentos d ON d.id = f.departamento_id;

-- View para diretoria: dados agregados (sem identificação individual)
CREATE OR REPLACE VIEW v_dashboard_diretoria AS
SELECT
  d.nome AS departamento,
  COUNT(f.id) AS total_funcionarios,
  COUNT(f.id) FILTER (WHERE f.status = 'Ativo') AS ativos,
  COUNT(f.id) FILTER (WHERE f.status = 'Desligado') AS desligados,
  COUNT(f.id) FILTER (WHERE f.status = 'Afastado') AS afastados
FROM rh_funcionarios f
LEFT JOIN rh_departamentos d ON d.id = f.departamento_id
GROUP BY d.nome;

-- ══════════════════════════════════════════════════════
-- 14. FUNÇÕES RPC — OPERAÇÕES CONTROLADAS
-- ══════════════════════════════════════════════════════

-- ── MOTOR LÓGICO: Funcionário pode trabalhar hoje? ──
CREATE OR REPLACE FUNCTION rh_verificar_aptidao(p_funcionario_id UUID)
RETURNS JSONB AS $$
DECLARE
  v_resultado JSONB;
  v_aso_valido BOOLEAN;
  v_aso_restricao TEXT;
  v_trein_pendentes INT;
  v_epi_pendentes INT;
  v_restricoes_ativas INT;
  v_status_func TEXT;
  v_apto BOOLEAN := true;
  v_motivos TEXT[] := '{}';
BEGIN
  -- Status do funcionário
  SELECT status INTO v_status_func
  FROM rh_funcionarios WHERE id = p_funcionario_id;

  IF v_status_func IS NULL THEN
    RETURN jsonb_build_object('erro', 'Funcionário não encontrado');
  END IF;

  IF v_status_func != 'Ativo' THEN
    v_apto := false;
    v_motivos := array_append(v_motivos, 'Status: ' || v_status_func);
  END IF;

  -- ASO válido?
  SELECT
    EXISTS(
      SELECT 1 FROM sst_aso
      WHERE funcionario_id = p_funcionario_id
        AND data_vencimento >= CURRENT_DATE
        AND resultado IN ('Apto','Apto com Restrição')
      ORDER BY data_exame DESC LIMIT 1
    ),
    (SELECT resultado FROM sst_aso
     WHERE funcionario_id = p_funcionario_id
       AND data_vencimento >= CURRENT_DATE
     ORDER BY data_exame DESC LIMIT 1)
  INTO v_aso_valido, v_aso_restricao;

  IF NOT v_aso_valido THEN
    v_apto := false;
    v_motivos := array_append(v_motivos, 'ASO vencido ou inexistente');
  ELSIF v_aso_restricao = 'Apto com Restrição' THEN
    v_motivos := array_append(v_motivos, 'ASO: Apto com Restrição');
  END IF;

  -- Treinamentos obrigatórios vencidos?
  SELECT COUNT(*) INTO v_trein_pendentes
  FROM sst_treinamentos t
  WHERE t.obrigatorio = true
    AND t.ativo = true
    AND NOT EXISTS (
      SELECT 1 FROM sst_treinamento_funcionario tf
      WHERE tf.treinamento_id = t.id
        AND tf.funcionario_id = p_funcionario_id
        AND tf.data_vencimento >= CURRENT_DATE
    );

  IF v_trein_pendentes > 0 THEN
    v_apto := false;
    v_motivos := array_append(v_motivos, v_trein_pendentes || ' treinamento(s) obrigatório(s) pendente(s)');
  END IF;

  -- EPIs obrigatórios não entregues?
  SELECT COUNT(*) INTO v_epi_pendentes
  FROM sst_epi_cargo ec
  JOIN rh_funcionarios f ON f.cargo_id = ec.cargo_id
  WHERE f.id = p_funcionario_id
    AND ec.obrigatorio = true
    AND NOT EXISTS (
      SELECT 1 FROM sst_epi_entrega ee
      WHERE ee.funcionario_id = p_funcionario_id
        AND ee.epi_id = ec.epi_id
        AND ee.motivo != 'Devolução'
    );

  IF v_epi_pendentes > 0 THEN
    v_apto := false;
    v_motivos := array_append(v_motivos, v_epi_pendentes || ' EPI(s) obrigatório(s) não entregue(s)');
  END IF;

  -- Restrições ativas?
  SELECT COUNT(*) INTO v_restricoes_ativas
  FROM rh_restricoes
  WHERE funcionario_id = p_funcionario_id
    AND ativa = true
    AND (data_fim IS NULL OR data_fim >= CURRENT_DATE);

  IF v_restricoes_ativas > 0 THEN
    v_motivos := array_append(v_motivos, v_restricoes_ativas || ' restrição(ões) ativa(s)');
  END IF;

  v_resultado := jsonb_build_object(
    'funcionario_id', p_funcionario_id,
    'apto', v_apto,
    'status', v_status_func,
    'aso_valido', v_aso_valido,
    'treinamentos_pendentes', v_trein_pendentes,
    'epis_pendentes', v_epi_pendentes,
    'restricoes_ativas', v_restricoes_ativas,
    'motivos', to_jsonb(v_motivos),
    'verificado_em', now()
  );

  RETURN v_resultado;
END;
$$ LANGUAGE plpgsql STABLE SECURITY INVOKER;

-- ── REGISTRAR DESLIGAMENTO (via backend obrigatório) ──
CREATE OR REPLACE FUNCTION rh_registrar_desligamento(
  p_funcionario_id UUID,
  p_tipo TEXT,
  p_categoria TEXT,
  p_subcategoria TEXT,
  p_descricao TEXT,
  p_data_desligamento DATE DEFAULT CURRENT_DATE,
  p_custo_estimado NUMERIC DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
  v_func RECORD;
  v_deslig_id UUID;
  v_tempo_dias INT;
BEGIN
  -- Verificar permissão
  IF rh_meu_perfil() NOT IN ('rh','admin') THEN
    RAISE EXCEPTION 'Sem permissão para registrar desligamento';
  END IF;

  -- Buscar dados do funcionário para snapshot
  SELECT
    f.nome, f.turno, f.data_admissao,
    c.nome AS cargo, d.nome AS departamento
  INTO v_func
  FROM rh_funcionarios f
  LEFT JOIN rh_cargos c ON c.id = f.cargo_id
  LEFT JOIN rh_departamentos d ON d.id = f.departamento_id
  WHERE f.id = p_funcionario_id;

  v_tempo_dias := p_data_desligamento - v_func.data_admissao;

  -- Inserir desligamento
  INSERT INTO rh_desligamentos (
    funcionario_id, data_desligamento, tipo, categoria, subcategoria,
    descricao, tempo_empresa_dias, setor_no_desligamento,
    cargo_no_desligamento, turno_no_desligamento,
    custo_estimado_desligamento, registrado_por
  ) VALUES (
    p_funcionario_id, p_data_desligamento, p_tipo, p_categoria, p_subcategoria,
    p_descricao, v_tempo_dias, v_func.departamento,
    v_func.cargo, v_func.turno,
    p_custo_estimado, (SELECT usuario_nome FROM rh_audit_log ORDER BY criado_em DESC LIMIT 1)
  ) RETURNING id INTO v_deslig_id;

  -- Atualizar status do funcionário
  UPDATE rh_funcionarios
  SET status = 'Desligado',
      data_demissao = p_data_desligamento,
      atualizado_em = now()
  WHERE id = p_funcionario_id;

  -- Audit log
  INSERT INTO rh_audit_log (acao, tabela, registro_id, dados_depois, descricao)
  VALUES (
    'desligamento', 'rh_desligamentos', v_deslig_id,
    jsonb_build_object('tipo', p_tipo, 'categoria', p_categoria, 'tempo_dias', v_tempo_dias),
    'Desligamento: ' || v_func.nome || ' (' || p_tipo || ' - ' || p_categoria || ')'
  );

  RETURN v_deslig_id;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

-- ── CLASSIFICAR RISCO PSICOSSOCIAL ──────────────────
CREATE OR REPLACE FUNCTION sm_classificar_risco(
  p_atendimento_id UUID,
  p_classificacao TEXT,
  p_recomendacao TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
  IF rh_meu_perfil() != 'psicologa' THEN
    RAISE EXCEPTION 'Apenas psicóloga pode classificar risco';
  END IF;

  UPDATE sm_atendimentos
  SET classificacao_risco = p_classificacao,
      recomendacao = p_recomendacao,
      status = 'Realizado',
      compareceu = true,
      data_atendimento = now()
  WHERE id = p_atendimento_id;

  -- Se risco alto → gerar alerta automático para RH
  IF p_classificacao = 'Alto' THEN
    INSERT INTO sm_alertas (funcionario_id, tipo, descricao)
    SELECT funcionario_id, 'Alerta RH',
           'Classificação de risco ALTO após atendimento psicológico'
    FROM sm_atendimentos WHERE id = p_atendimento_id;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

-- ── VERIFICAR GATILHOS AUTOMÁTICOS DE SAÚDE MENTAL ──
CREATE OR REPLACE FUNCTION sm_verificar_gatilhos()
RETURNS TABLE(funcionario_id UUID, funcionario_nome TEXT, gatilho TEXT, detalhe TEXT) AS $$
BEGIN
  -- Gatilho 1: Faltas > 3 no último mês
  RETURN QUERY
  SELECT a.funcionario_id, f.nome, 'Faltas frequentes'::TEXT,
         COUNT(*)::TEXT || ' faltas nos últimos 30 dias'
  FROM rh_absenteismo a
  JOIN rh_funcionarios f ON f.id = a.funcionario_id
  WHERE a.data >= CURRENT_DATE - 30
    AND a.tipo IN ('Falta Justificada','Falta Injustificada')
    AND f.status = 'Ativo'
  GROUP BY a.funcionario_id, f.nome
  HAVING COUNT(*) >= 3;

  -- Gatilho 2: Retorno de afastamento INSS
  RETURN QUERY
  SELECT a.funcionario_id, f.nome, 'Retorno de afastamento'::TEXT,
         'Retornou de afastamento INSS recentemente'::TEXT
  FROM rh_absenteismo a
  JOIN rh_funcionarios f ON f.id = a.funcionario_id
  WHERE a.tipo = 'Afastamento INSS'
    AND a.data >= CURRENT_DATE - 15
    AND f.status = 'Ativo'
    AND NOT EXISTS (
      SELECT 1 FROM sm_atendimentos sm
      WHERE sm.funcionario_id = a.funcionario_id
        AND sm.criado_em >= CURRENT_DATE - 15
    )
  GROUP BY a.funcionario_id, f.nome;
END;
$$ LANGUAGE plpgsql STABLE SECURITY INVOKER;

-- ── INDICADORES DE TURNOVER ─────────────────────────
CREATE OR REPLACE FUNCTION rh_indicadores_turnover(
  p_data_inicio DATE DEFAULT (CURRENT_DATE - INTERVAL '12 months')::DATE,
  p_data_fim DATE DEFAULT CURRENT_DATE
)
RETURNS JSONB AS $$
DECLARE
  v_total_ativos INT;
  v_desligamentos INT;
  v_deslig_curto_prazo INT;
  v_tempo_medio_dias NUMERIC;
  v_resultado JSONB;
BEGIN
  SELECT COUNT(*) INTO v_total_ativos
  FROM rh_funcionarios WHERE status = 'Ativo';

  SELECT COUNT(*) INTO v_desligamentos
  FROM rh_desligamentos
  WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim;

  SELECT COUNT(*) INTO v_deslig_curto_prazo
  FROM rh_desligamentos
  WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim
    AND tempo_empresa_dias < 90;

  SELECT AVG(tempo_empresa_dias) INTO v_tempo_medio_dias
  FROM rh_desligamentos
  WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim;

  v_resultado := jsonb_build_object(
    'periodo', jsonb_build_object('inicio', p_data_inicio, 'fim', p_data_fim),
    'total_ativos', v_total_ativos,
    'desligamentos', v_desligamentos,
    'turnover_pct', CASE WHEN v_total_ativos > 0
      THEN ROUND((v_desligamentos::NUMERIC / v_total_ativos) * 100, 1)
      ELSE 0 END,
    'turnover_curto_prazo_pct', CASE WHEN v_desligamentos > 0
      THEN ROUND((v_deslig_curto_prazo::NUMERIC / v_desligamentos) * 100, 1)
      ELSE 0 END,
    'tempo_medio_dias', COALESCE(ROUND(v_tempo_medio_dias, 0), 0),
    'por_categoria', (
      SELECT jsonb_object_agg(categoria, cnt)
      FROM (
        SELECT categoria, COUNT(*) AS cnt
        FROM rh_desligamentos
        WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim
        GROUP BY categoria
      ) sub
    ),
    'por_setor', (
      SELECT jsonb_object_agg(setor_no_desligamento, cnt)
      FROM (
        SELECT COALESCE(setor_no_desligamento, 'Não informado') AS setor_no_desligamento, COUNT(*) AS cnt
        FROM rh_desligamentos
        WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim
        GROUP BY setor_no_desligamento
      ) sub
    ),
    'por_faixa_tempo', jsonb_build_object(
      'ate_30_dias', (SELECT COUNT(*) FROM rh_desligamentos WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim AND tempo_empresa_dias <= 30),
      '31_a_90_dias', (SELECT COUNT(*) FROM rh_desligamentos WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim AND tempo_empresa_dias BETWEEN 31 AND 90),
      '91_a_180_dias', (SELECT COUNT(*) FROM rh_desligamentos WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim AND tempo_empresa_dias BETWEEN 91 AND 180),
      '181_a_365_dias', (SELECT COUNT(*) FROM rh_desligamentos WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim AND tempo_empresa_dias BETWEEN 181 AND 365),
      'mais_365_dias', (SELECT COUNT(*) FROM rh_desligamentos WHERE data_desligamento BETWEEN p_data_inicio AND p_data_fim AND tempo_empresa_dias > 365)
    )
  );

  RETURN v_resultado;
END;
$$ LANGUAGE plpgsql STABLE SECURITY INVOKER;

-- ══════════════════════════════════════════════════════
-- 15. OPERAÇÕES QUE DEVEM PASSAR POR BACKEND
-- ══════════════════════════════════════════════════════
-- As seguintes operações NÃO devem ser executadas diretamente
-- pelo frontend (via anon key). Devem usar Edge Functions
-- com service_role:
--
-- 1. Desligamento → altera status, gera snapshot, audit
-- 2. Alteração salarial → dado sensível, precisa double-check
-- 3. ASO (apto/inapto) → impacta liberação do funcionário
-- 4. Classificação risco alto → gera alerta obrigatório
-- 5. Exportação de dados → LGPD, precisa log
--
-- MOTIVO: Evitar manipulação via DevTools/console do navegador.
-- O frontend usa anon key (acesso limitado por RLS).
-- Operações críticas usam service_role via Edge Function
-- com validação adicional no servidor.

-- ══════════════════════════════════════════════════════
-- 16. SEGURANÇA DE CHAVES
-- ══════════════════════════════════════════════════════
-- ┌──────────────────┬─────────────────────────────────┐
-- │ anon key         │ Frontend (browser)              │
-- │                  │ Acesso limitado por RLS          │
-- │                  │ Somente operações de leitura     │
-- │                  │ e inserções controladas          │
-- ├──────────────────┼─────────────────────────────────┤
-- │ service_role     │ APENAS backend/Edge Functions   │
-- │                  │ NUNCA no frontend               │
-- │                  │ Bypass RLS = perigo total        │
-- │                  │ Usar para: desligamento,        │
-- │                  │ alteração salarial, exports      │
-- └──────────────────┴─────────────────────────────────┘

-- ══════════════════════════════════════════════════════
-- 17. CHECKLIST DE VALIDAÇÃO
-- ══════════════════════════════════════════════════════
-- [ ] RLS habilitado em TODAS as tabelas
-- [ ] Nenhuma tabela com acesso público irrestrito
-- [ ] service_role NÃO está no código frontend
-- [ ] Funções SECURITY DEFINER usadas apenas para lookup de perfil
-- [ ] Funções de negócio usam SECURITY INVOKER
-- [ ] Dados de saúde mental isolados (psicóloga only)
-- [ ] Audit log registra todas alterações sensíveis
-- [ ] Views sem dados sensíveis para diretoria
-- [ ] Índices criados para queries de RLS (evitar full scan)
-- [ ] Exportação de dados passa por backend

-- ══════════════════════════════════════════════════════
-- 18. RISCOS E PONTOS CRÍTICOS
-- ══════════════════════════════════════════════════════
-- 1. Se RLS for desabilitado acidentalmente → dados expostos
--    → Revisão mensal: SELECT tablename FROM pg_tables WHERE rowsecurity = false
--
-- 2. Se anon key vazar → acesso limitado por RLS (ok)
--    Se service_role vazar → acesso TOTAL → rotacionar imediatamente
--
-- 3. Psicóloga com acesso a sm_atendimentos pode ver TODOS os atendimentos
--    → Se houver múltiplas psicólogas, criar coluna profissional_id com RLS
--
-- 4. Líder com departamento_id errado vê equipe errada
--    → Validar sempre no cadastro de rh_acesso_sistema
--
-- 5. Performance: RLS com subqueries pode degradar
--    → Índices em departamento_id, funcionario_id, auth_user_id são obrigatórios
--    → Monitorar pg_stat_user_tables para slow queries
