-- ============================================================================
-- migration_armarios.sql
-- Módulo de Controle de Armários para Colaboradores
-- Integrado ao sistema RH/SST via rh_funcionarios.id
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. TABELAS
-- ============================================================================

-- 1.1 Cadastro de armários
CREATE TABLE IF NOT EXISTS arm_armarios (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo        TEXT NOT NULL UNIQUE,                -- Ex: "A-001", "B-012"
  localizacao   TEXT NOT NULL,                       -- Ex: "Vestiário Masculino", "Bloco B"
  setor_id      UUID REFERENCES rh_setores(id),      -- Setor vinculado (opcional)
  filial_id     UUID REFERENCES rh_filiais(id),      -- Filial
  tipo          TEXT DEFAULT 'padrao'                 -- padrao, duplo, especial
    CHECK (tipo IN ('padrao','duplo','especial')),
  status        TEXT NOT NULL DEFAULT 'disponivel'
    CHECK (status IN ('disponivel','ocupado','bloqueado','manutencao')),
  observacoes   TEXT,
  ativo         BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW(),
  created_by    UUID
);

CREATE INDEX idx_arm_armarios_status ON arm_armarios(status);
CREATE INDEX idx_arm_armarios_filial ON arm_armarios(filial_id);
CREATE INDEX idx_arm_armarios_setor ON arm_armarios(setor_id);

-- 1.2 Ocupações (quem está usando qual armário)
CREATE TABLE IF NOT EXISTS arm_ocupacoes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  armario_id      UUID NOT NULL REFERENCES arm_armarios(id),
  funcionario_id  UUID NOT NULL REFERENCES rh_funcionarios(id),
  data_atribuicao DATE NOT NULL DEFAULT CURRENT_DATE,
  data_devolucao  DATE,                               -- NULL = ocupação ativa
  atribuido_por   UUID,                               -- rh_usuarios.id
  devolvido_por   UUID,
  motivo_devolucao TEXT,                               -- demissao, transferencia, solicitacao, manutencao
  chave_entregue  BOOLEAN DEFAULT TRUE,
  observacoes     TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_arm_ocup_armario ON arm_ocupacoes(armario_id) WHERE data_devolucao IS NULL;
CREATE INDEX idx_arm_ocup_func ON arm_ocupacoes(funcionario_id) WHERE data_devolucao IS NULL;

-- 1.3 Histórico (log imutável de todas as ações)
CREATE TABLE IF NOT EXISTS arm_historico (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  armario_id    UUID NOT NULL REFERENCES arm_armarios(id),
  funcionario_id UUID REFERENCES rh_funcionarios(id),
  acao          TEXT NOT NULL
    CHECK (acao IN ('atribuir','liberar','bloquear','desbloquear','manutencao','observacao')),
  detalhes      JSONB,
  usuario_id    UUID,                                 -- quem executou
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_arm_hist_armario ON arm_historico(armario_id);
CREATE INDEX idx_arm_hist_func ON arm_historico(funcionario_id);

-- 1.4 Bloqueios (motivo + período)
CREATE TABLE IF NOT EXISTS arm_bloqueios (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  armario_id    UUID NOT NULL REFERENCES arm_armarios(id),
  motivo        TEXT NOT NULL,                        -- Ex: "Porta quebrada", "Inspeção"
  bloqueado_por UUID,
  bloqueado_em  TIMESTAMPTZ DEFAULT NOW(),
  desbloqueado_por UUID,
  desbloqueado_em TIMESTAMPTZ,
  ativo         BOOLEAN DEFAULT TRUE,
  observacoes   TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_arm_bloq_armario ON arm_bloqueios(armario_id) WHERE ativo = TRUE;


-- ============================================================================
-- 2. CONSTRAINTS DE UNICIDADE
-- ============================================================================

-- 2.1 Impedir mais de uma ocupação ATIVA por armário
CREATE UNIQUE INDEX idx_arm_ocup_armario_unico
  ON arm_ocupacoes(armario_id) WHERE data_devolucao IS NULL;

-- 2.2 Impedir mais de um armário ATIVO por funcionário
CREATE UNIQUE INDEX idx_arm_ocup_func_unico
  ON arm_ocupacoes(funcionario_id) WHERE data_devolucao IS NULL;


-- ============================================================================
-- 3. TRIGGERS
-- ============================================================================

-- 3.1 Ao ATRIBUIR armário → atualizar status + histórico
CREATE OR REPLACE FUNCTION fn_arm_atribuir()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $$
BEGIN
  -- Só disparar em INSERT (nova ocupação)
  IF TG_OP = 'INSERT' AND NEW.data_devolucao IS NULL THEN
    -- Verificar se armário está disponível
    IF (SELECT status FROM arm_armarios WHERE id = NEW.armario_id) NOT IN ('disponivel') THEN
      RAISE EXCEPTION 'Armário não está disponível (status: %)',
        (SELECT status FROM arm_armarios WHERE id = NEW.armario_id);
    END IF;

    -- Atualizar status do armário
    UPDATE arm_armarios SET status = 'ocupado', updated_at = NOW() WHERE id = NEW.armario_id;

    -- Registrar histórico
    INSERT INTO arm_historico (armario_id, funcionario_id, acao, detalhes, usuario_id)
    VALUES (NEW.armario_id, NEW.funcionario_id, 'atribuir',
      jsonb_build_object('data_atribuicao', NEW.data_atribuicao, 'chave_entregue', NEW.chave_entregue),
      NEW.atribuido_por);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_arm_atribuir
  AFTER INSERT ON arm_ocupacoes
  FOR EACH ROW EXECUTE FUNCTION fn_arm_atribuir();

-- 3.2 Ao LIBERAR armário → atualizar status + histórico
CREATE OR REPLACE FUNCTION fn_arm_liberar()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $$
BEGIN
  -- Disparar quando data_devolucao passa de NULL para preenchido
  IF TG_OP = 'UPDATE' AND OLD.data_devolucao IS NULL AND NEW.data_devolucao IS NOT NULL THEN
    -- Atualizar status do armário para disponível
    UPDATE arm_armarios SET status = 'disponivel', updated_at = NOW() WHERE id = NEW.armario_id;

    -- Registrar histórico
    INSERT INTO arm_historico (armario_id, funcionario_id, acao, detalhes, usuario_id)
    VALUES (NEW.armario_id, NEW.funcionario_id, 'liberar',
      jsonb_build_object('data_devolucao', NEW.data_devolucao, 'motivo', NEW.motivo_devolucao, 'chave_entregue', NEW.chave_entregue),
      NEW.devolvido_por);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_arm_liberar
  AFTER UPDATE ON arm_ocupacoes
  FOR EACH ROW EXECUTE FUNCTION fn_arm_liberar();

-- 3.3 Ao BLOQUEAR/DESBLOQUEAR → atualizar status + histórico
CREATE OR REPLACE FUNCTION fn_arm_bloqueio()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $$
BEGIN
  IF TG_OP = 'INSERT' AND NEW.ativo = TRUE THEN
    UPDATE arm_armarios SET status = 'bloqueado', updated_at = NOW() WHERE id = NEW.armario_id;
    INSERT INTO arm_historico (armario_id, acao, detalhes, usuario_id)
    VALUES (NEW.armario_id, 'bloquear',
      jsonb_build_object('motivo', NEW.motivo), NEW.bloqueado_por);
  END IF;

  IF TG_OP = 'UPDATE' AND OLD.ativo = TRUE AND NEW.ativo = FALSE THEN
    -- Só liberar se não tiver ocupação ativa
    IF NOT EXISTS (SELECT 1 FROM arm_ocupacoes WHERE armario_id = NEW.armario_id AND data_devolucao IS NULL) THEN
      UPDATE arm_armarios SET status = 'disponivel', updated_at = NOW() WHERE id = NEW.armario_id;
    ELSE
      UPDATE arm_armarios SET status = 'ocupado', updated_at = NOW() WHERE id = NEW.armario_id;
    END IF;
    INSERT INTO arm_historico (armario_id, acao, detalhes, usuario_id)
    VALUES (NEW.armario_id, 'desbloquear',
      jsonb_build_object('motivo', NEW.motivo), NEW.desbloqueado_por);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_arm_bloqueio
  AFTER INSERT OR UPDATE ON arm_bloqueios
  FOR EACH ROW EXECUTE FUNCTION fn_arm_bloqueio();

-- 3.4 Updated_at automático
CREATE TRIGGER trg_arm_updated
  BEFORE UPDATE ON arm_armarios FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
CREATE TRIGGER trg_arm_ocup_updated
  BEFORE UPDATE ON arm_ocupacoes FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();


-- ============================================================================
-- 4. VIEWS
-- ============================================================================

-- 4.1 Armários disponíveis
CREATE OR REPLACE VIEW vw_arm_disponiveis AS
SELECT a.id, a.codigo, a.localizacao, a.tipo, a.setor_id, s.nome as setor_nome,
  a.filial_id, f.nome_fantasia as filial_nome
FROM arm_armarios a
LEFT JOIN rh_setores s ON s.id = a.setor_id
LEFT JOIN rh_filiais f ON f.id = a.filial_id
WHERE a.status = 'disponivel' AND a.ativo = TRUE
ORDER BY a.codigo;

-- 4.2 Armários ocupados com dados do funcionário
CREATE OR REPLACE VIEW vw_arm_ocupados AS
SELECT a.id as armario_id, a.codigo, a.localizacao, a.tipo,
  o.id as ocupacao_id, o.data_atribuicao, o.chave_entregue,
  func.id as funcionario_id, func.nome_completo as funcionario_nome, func.cpf,
  s.nome as setor_nome, c.nome as cargo_nome,
  fil.nome_fantasia as filial_nome,
  (CURRENT_DATE - o.data_atribuicao) as dias_ocupado
FROM arm_armarios a
JOIN arm_ocupacoes o ON o.armario_id = a.id AND o.data_devolucao IS NULL
JOIN rh_funcionarios func ON func.id = o.funcionario_id
LEFT JOIN rh_setores s ON s.id = func.setor_id
LEFT JOIN rh_cargos c ON c.id = func.cargo_id
LEFT JOIN rh_filiais fil ON fil.id = a.filial_id
WHERE a.status = 'ocupado'
ORDER BY a.codigo;

-- 4.3 Armários por setor (resumo)
CREATE OR REPLACE VIEW vw_arm_por_setor AS
SELECT
  COALESCE(s.nome, 'Sem setor') as setor,
  a.filial_id,
  count(*) as total,
  count(*) FILTER (WHERE a.status = 'disponivel') as disponiveis,
  count(*) FILTER (WHERE a.status = 'ocupado') as ocupados,
  count(*) FILTER (WHERE a.status = 'bloqueado') as bloqueados,
  count(*) FILTER (WHERE a.status = 'manutencao') as manutencao,
  ROUND(count(*) FILTER (WHERE a.status = 'ocupado') * 100.0 / NULLIF(count(*), 0), 1) as taxa_ocupacao
FROM arm_armarios a
LEFT JOIN rh_setores s ON s.id = a.setor_id
WHERE a.ativo = TRUE
GROUP BY s.nome, a.filial_id
ORDER BY total DESC;

-- 4.4 Armários irregulares (ocupados por funcionário desligado, sem chave, etc.)
CREATE OR REPLACE VIEW vw_arm_irregulares AS
SELECT a.codigo, a.localizacao,
  func.nome_completo as funcionario_nome, func.status as func_status,
  o.data_atribuicao, o.chave_entregue,
  (CURRENT_DATE - o.data_atribuicao) as dias_ocupado,
  CASE
    WHEN func.status = 'desligado' THEN 'Funcionário desligado'
    WHEN func.status = 'afastado' THEN 'Funcionário afastado'
    WHEN NOT o.chave_entregue THEN 'Chave não entregue'
    WHEN (CURRENT_DATE - o.data_atribuicao) > 730 THEN 'Ocupação > 2 anos sem revisão'
    ELSE 'Outro'
  END as irregularidade
FROM arm_armarios a
JOIN arm_ocupacoes o ON o.armario_id = a.id AND o.data_devolucao IS NULL
JOIN rh_funcionarios func ON func.id = o.funcionario_id
WHERE func.status IN ('desligado','afastado')
   OR NOT o.chave_entregue
   OR (CURRENT_DATE - o.data_atribuicao) > 730
ORDER BY
  CASE WHEN func.status = 'desligado' THEN 0 ELSE 1 END,
  o.data_atribuicao;


-- ============================================================================
-- 5. RLS
-- ============================================================================

ALTER TABLE arm_armarios ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm_ocupacoes ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm_historico ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm_bloqueios ENABLE ROW LEVEL SECURITY;

-- arm_armarios: RH/SST = total, gestor = setor, demais = leitura
CREATE POLICY arm_armarios_select ON arm_armarios FOR SELECT TO authenticated
  USING (
    CASE rh_perfil_atual()
      WHEN 'administrador' THEN true
      WHEN 'rh' THEN true
      WHEN 'sst' THEN true
      WHEN 'diretoria' THEN true
      WHEN 'gestor' THEN setor_id IN (SELECT s.id FROM rh_setores s WHERE s.id = (SELECT setor_id FROM rh_funcionarios WHERE email_pessoal = (SELECT email FROM rh_usuarios WHERE auth_uid = COALESCE(auth.uid(), NULLIF(current_setting('request.jwt.claim.sub',true),'')::uuid) LIMIT 1) LIMIT 1))
      WHEN 'gestor_confianca' THEN true
      ELSE true  -- leitura básica para todos autenticados
    END
  );

CREATE POLICY arm_armarios_write ON arm_armarios FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','sst'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','sst'));

-- arm_ocupacoes: RH/SST = CRUD, gestor = leitura setor, demais = leitura
CREATE POLICY arm_ocup_select ON arm_ocupacoes FOR SELECT TO authenticated USING (true);
CREATE POLICY arm_ocup_write ON arm_ocupacoes FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','sst'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','sst'));

-- arm_historico: append-only, leitura para RH/SST/admin
CREATE POLICY arm_hist_insert ON arm_historico FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY arm_hist_select ON arm_historico FOR SELECT TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','sst','diretoria'));

-- arm_bloqueios: RH/SST = CRUD
CREATE POLICY arm_bloq_select ON arm_bloqueios FOR SELECT TO authenticated USING (true);
CREATE POLICY arm_bloq_write ON arm_bloqueios FOR ALL TO authenticated
  USING (rh_perfil_atual() IN ('administrador','rh','sst'))
  WITH CHECK (rh_perfil_atual() IN ('administrador','rh','sst'));

-- Bloquear anon
CREATE POLICY arm_armarios_anon ON arm_armarios FOR ALL TO anon USING (false) WITH CHECK (false);
CREATE POLICY arm_ocup_anon ON arm_ocupacoes FOR ALL TO anon USING (false) WITH CHECK (false);
CREATE POLICY arm_hist_anon ON arm_historico FOR ALL TO anon USING (false) WITH CHECK (false);
CREATE POLICY arm_bloq_anon ON arm_bloqueios FOR ALL TO anon USING (false) WITH CHECK (false);


COMMIT;
