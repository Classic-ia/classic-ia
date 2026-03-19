-- ============================================================
-- Schema: Empresas, Filiais, Dados Bancários & Pagamentos
-- Classic BI — v2026.03
-- Executar DEPOIS do schema_rh_sst.sql
-- ============================================================

-- ══════════════════════════════════════════════════════
-- PARTE 1 — ESTRUTURA ORGANIZACIONAL
-- ══════════════════════════════════════════════════════

-- ── EMPRESAS ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS org_empresas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  razao_social TEXT NOT NULL,
  nome_fantasia TEXT,
  cnpj TEXT UNIQUE NOT NULL,
  inscricao_estadual TEXT,
  endereco TEXT,
  cidade TEXT,
  uf CHAR(2),
  cep TEXT,
  telefone TEXT,
  email TEXT,
  responsavel TEXT,
  codigo TEXT UNIQUE,                -- código interno ex: EMP001
  ativa BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── FILIAIS ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS org_filiais (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  empresa_id UUID NOT NULL REFERENCES org_empresas(id),
  razao_social TEXT NOT NULL,
  nome_fantasia TEXT,
  cnpj TEXT UNIQUE NOT NULL,
  inscricao_estadual TEXT,
  endereco TEXT,
  cidade TEXT,
  uf CHAR(2),
  cep TEXT,
  telefone TEXT,
  email TEXT,
  responsavel TEXT,
  codigo TEXT UNIQUE,                -- código interno ex: FIL001
  banco_padrao TEXT,                 -- banco padrão da filial
  ativa BOOLEAN DEFAULT true,
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── SETORES / CENTROS DE CUSTO ─────────────────────
CREATE TABLE IF NOT EXISTS org_setores (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  filial_id UUID NOT NULL REFERENCES org_filiais(id),
  nome TEXT NOT NULL,
  codigo_cc TEXT,                    -- centro de custo
  responsavel TEXT,
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ADICIONAR VÍNCULOS ORGANIZACIONAIS AO FUNCIONÁRIO
-- (se as colunas já existirem, os comandos serão ignorados)
DO $$ BEGIN
  ALTER TABLE rh_funcionarios ADD COLUMN IF NOT EXISTS empresa_id UUID REFERENCES org_empresas(id);
  ALTER TABLE rh_funcionarios ADD COLUMN IF NOT EXISTS filial_id UUID REFERENCES org_filiais(id);
  ALTER TABLE rh_funcionarios ADD COLUMN IF NOT EXISTS setor_id UUID REFERENCES org_setores(id);
  ALTER TABLE rh_funcionarios ADD COLUMN IF NOT EXISTS codigo_func TEXT UNIQUE;
END $$;

CREATE INDEX IF NOT EXISTS idx_func_empresa ON rh_funcionarios(empresa_id);
CREATE INDEX IF NOT EXISTS idx_func_filial ON rh_funcionarios(filial_id);
CREATE INDEX IF NOT EXISTS idx_func_setor ON rh_funcionarios(setor_id);

-- ══════════════════════════════════════════════════════
-- PARTE 2 — DADOS BANCÁRIOS DO COLABORADOR
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_dados_bancarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),

  forma_pagamento TEXT NOT NULL CHECK (forma_pagamento IN ('Conta Salário Itaú','Conta Sicredi','PIX','Cheque')),

  -- Dados bancários (para conta salário)
  banco TEXT,
  agencia TEXT,
  conta TEXT,
  tipo_conta TEXT CHECK (tipo_conta IN ('Corrente','Poupança','Salário')),
  titular TEXT,
  cpf_cnpj_titular TEXT,

  -- Dados PIX
  tipo_chave_pix TEXT CHECK (tipo_chave_pix IN ('CPF','E-mail','Telefone','Aleatória')),
  chave_pix TEXT,

  -- Dados Cheque (exceção)
  cheque_motivo TEXT,                -- motivo da exceção
  cheque_autorizado_por TEXT,        -- responsável pela autorização
  cheque_status_entrega TEXT CHECK (cheque_status_entrega IN ('Pendente','Entregue','Devolvido')),
  cheque_data_prevista DATE,

  -- Validação
  validado BOOLEAN DEFAULT false,
  validado_por TEXT,
  validado_em TIMESTAMPTZ,
  erros_validacao JSONB,             -- lista de erros encontrados

  -- Status
  status TEXT DEFAULT 'Ativo' CHECK (status IN ('Ativo','Pendente Aprovação','Bloqueado','Inativo')),
  bloqueio_motivo TEXT,

  ativo BOOLEAN DEFAULT true,
  observacoes TEXT,
  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pag_func ON pag_dados_bancarios(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_pag_forma ON pag_dados_bancarios(forma_pagamento);
CREATE INDEX IF NOT EXISTS idx_pag_status ON pag_dados_bancarios(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pag_func_ativo ON pag_dados_bancarios(funcionario_id) WHERE ativo = true;

-- ══════════════════════════════════════════════════════
-- PARTE 3 — WORKFLOW DE APROVAÇÃO
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_solicitacoes_alteracao (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  dados_bancarios_id UUID REFERENCES pag_dados_bancarios(id),

  tipo_alteracao TEXT NOT NULL CHECK (tipo_alteracao IN ('Novo Cadastro','Alteração Bancária','Alteração PIX','Alteração Forma Pagamento','Cancelamento')),

  -- Snapshot antes/depois
  valor_anterior JSONB,
  valor_novo JSONB NOT NULL,
  motivo TEXT,

  -- Workflow
  status TEXT DEFAULT 'Pendente' CHECK (status IN ('Pendente','Aprovado','Rejeitado','Cancelado')),
  solicitado_por TEXT NOT NULL,
  solicitado_em TIMESTAMPTZ DEFAULT now(),
  aprovado_por TEXT,
  aprovado_em TIMESTAMPTZ,
  motivo_rejeicao TEXT,

  criado_em TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_solic_func ON pag_solicitacoes_alteracao(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_solic_status ON pag_solicitacoes_alteracao(status);

-- ══════════════════════════════════════════════════════
-- PARTE 4 — LOTES DE PAGAMENTO
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_lotes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  nome TEXT NOT NULL,                -- ex: folha_marco_2026_filial_x
  empresa_id UUID REFERENCES org_empresas(id),
  filial_id UUID REFERENCES org_filiais(id),
  periodo_referencia TEXT,           -- ex: 2026-03
  forma_pagamento TEXT,              -- filtro, ou NULL = todas

  status TEXT DEFAULT 'Rascunho' CHECK (status IN ('Rascunho','Em Conferência','Pronto para Envio','Enviado','Finalizado')),

  total_colaboradores INT DEFAULT 0,
  total_valor NUMERIC(12,2) DEFAULT 0,

  -- Integridade
  tem_erros_criticos BOOLEAN DEFAULT false,
  tem_alertas BOOLEAN DEFAULT false,
  check_integridade JSONB,           -- resultado da última verificação

  criado_por TEXT,
  conferido_por TEXT,
  enviado_por TEXT,
  finalizado_por TEXT,

  criado_em TIMESTAMPTZ DEFAULT now(),
  atualizado_em TIMESTAMPTZ DEFAULT now()
);

-- ── ITENS DO LOTE ──────────────────────────────────
CREATE TABLE IF NOT EXISTS pag_lote_itens (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  lote_id UUID NOT NULL REFERENCES pag_lotes(id) ON DELETE CASCADE,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  dados_bancarios_id UUID REFERENCES pag_dados_bancarios(id),

  valor NUMERIC(10,2),
  status_item TEXT DEFAULT 'OK' CHECK (status_item IN ('OK','Alerta','Bloqueado','Removido')),
  motivo_alerta TEXT,

  UNIQUE(lote_id, funcionario_id)
);

CREATE INDEX IF NOT EXISTS idx_lote_itens_lote ON pag_lote_itens(lote_id);

-- ══════════════════════════════════════════════════════
-- PARTE 5 — LOG DE EXPORTAÇÃO
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_log_exportacao (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  usuario_id UUID,
  usuario_nome TEXT,
  tipo_exportacao TEXT NOT NULL,      -- 'lista_itau','lista_sicredi','lista_pix','lista_cheque','consolidada','pendencias','bloqueados','lote'
  formato TEXT NOT NULL CHECK (formato IN ('xlsx','csv')),
  filtros_utilizados JSONB,          -- snapshot dos filtros
  total_registros INT,
  nome_arquivo TEXT,
  lote_id UUID REFERENCES pag_lotes(id),
  criado_em TIMESTAMPTZ DEFAULT now()
);

-- ══════════════════════════════════════════════════════
-- PARTE 6 — ALERTAS DE PAGAMENTO
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_alertas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID REFERENCES rh_funcionarios(id),
  tipo TEXT NOT NULL CHECK (tipo IN (
    'Sem Forma Pagamento','Cadastro Incompleto','Alteração Recente',
    'Duplicidade Conta','Duplicidade PIX','Divergência Titular',
    'Cheque Frequente','Dados Inválidos','Sem Vínculo Organizacional',
    'Alteração Não Aprovada'
  )),
  descricao TEXT NOT NULL,
  dados_json JSONB,
  severidade TEXT DEFAULT 'Alerta' CHECK (severidade IN ('Info','Alerta','Crítico','Bloqueante')),
  resolvido BOOLEAN DEFAULT false,
  resolvido_por TEXT,
  resolvido_em TIMESTAMPTZ,
  criado_em TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pag_alertas_func ON pag_alertas(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_pag_alertas_tipo ON pag_alertas(tipo);
CREATE INDEX IF NOT EXISTS idx_pag_alertas_resolvido ON pag_alertas(resolvido);

-- ══════════════════════════════════════════════════════
-- PARTE 7 — HISTÓRICO DE ALTERAÇÕES BANCÁRIAS
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pag_historico (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  funcionario_id UUID NOT NULL REFERENCES rh_funcionarios(id),
  tipo_alteracao TEXT NOT NULL,
  dados_antes JSONB,
  dados_depois JSONB,
  alterado_por TEXT,
  motivo TEXT,
  criado_em TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pag_hist_func ON pag_historico(funcionario_id);
CREATE INDEX IF NOT EXISTS idx_pag_hist_data ON pag_historico(criado_em);

-- ══════════════════════════════════════════════════════
-- PARTE 8 — FUNÇÕES RPC
-- ══════════════════════════════════════════════════════

-- ── VALIDAR CPF ────────────────────────────────────
CREATE OR REPLACE FUNCTION pag_validar_cpf(p_cpf TEXT)
RETURNS BOOLEAN AS $$
DECLARE
  v_cpf TEXT;
  v_soma INT;
  v_resto INT;
  v_d1 INT;
  v_d2 INT;
BEGIN
  v_cpf := regexp_replace(p_cpf, '[^0-9]', '', 'g');
  IF length(v_cpf) != 11 THEN RETURN false; END IF;
  IF v_cpf ~ '^(\d)\1{10}$' THEN RETURN false; END IF;

  v_soma := 0;
  FOR i IN 1..9 LOOP
    v_soma := v_soma + (substr(v_cpf, i, 1)::INT * (11 - i));
  END LOOP;
  v_resto := v_soma % 11;
  v_d1 := CASE WHEN v_resto < 2 THEN 0 ELSE 11 - v_resto END;
  IF substr(v_cpf, 10, 1)::INT != v_d1 THEN RETURN false; END IF;

  v_soma := 0;
  FOR i IN 1..10 LOOP
    v_soma := v_soma + (substr(v_cpf, i, 1)::INT * (12 - i));
  END LOOP;
  v_resto := v_soma % 11;
  v_d2 := CASE WHEN v_resto < 2 THEN 0 ELSE 11 - v_resto END;
  IF substr(v_cpf, 11, 1)::INT != v_d2 THEN RETURN false; END IF;

  RETURN true;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ── VALIDAR DADOS BANCÁRIOS ────────────────────────
CREATE OR REPLACE FUNCTION pag_validar_dados(p_dados_id UUID)
RETURNS JSONB AS $$
DECLARE
  v_dados RECORD;
  v_erros TEXT[] := '{}';
  v_valido BOOLEAN := true;
BEGIN
  SELECT * INTO v_dados FROM pag_dados_bancarios WHERE id = p_dados_id;

  IF v_dados IS NULL THEN
    RETURN jsonb_build_object('valido', false, 'erros', '["Registro não encontrado"]'::jsonb);
  END IF;

  -- Validar CPF do titular
  IF v_dados.cpf_cnpj_titular IS NOT NULL AND length(regexp_replace(v_dados.cpf_cnpj_titular, '[^0-9]', '', 'g')) = 11 THEN
    IF NOT pag_validar_cpf(v_dados.cpf_cnpj_titular) THEN
      v_erros := array_append(v_erros, 'CPF do titular inválido');
      v_valido := false;
    END IF;
  END IF;

  -- Validar campos obrigatórios por forma de pagamento
  IF v_dados.forma_pagamento IN ('Conta Salário Itaú', 'Conta Sicredi') THEN
    IF v_dados.banco IS NULL OR v_dados.banco = '' THEN
      v_erros := array_append(v_erros, 'Banco obrigatório'); v_valido := false;
    END IF;
    IF v_dados.agencia IS NULL OR v_dados.agencia = '' THEN
      v_erros := array_append(v_erros, 'Agência obrigatória'); v_valido := false;
    END IF;
    IF v_dados.conta IS NULL OR v_dados.conta = '' THEN
      v_erros := array_append(v_erros, 'Conta obrigatória'); v_valido := false;
    END IF;
    IF v_dados.tipo_conta IS NULL THEN
      v_erros := array_append(v_erros, 'Tipo de conta obrigatório'); v_valido := false;
    END IF;
  END IF;

  IF v_dados.forma_pagamento = 'PIX' THEN
    IF v_dados.chave_pix IS NULL OR v_dados.chave_pix = '' THEN
      v_erros := array_append(v_erros, 'Chave PIX obrigatória'); v_valido := false;
    END IF;
    IF v_dados.tipo_chave_pix IS NULL THEN
      v_erros := array_append(v_erros, 'Tipo de chave PIX obrigatório'); v_valido := false;
    END IF;
    -- Validar formato da chave PIX
    IF v_dados.tipo_chave_pix = 'CPF' AND NOT pag_validar_cpf(v_dados.chave_pix) THEN
      v_erros := array_append(v_erros, 'Chave PIX (CPF) inválida'); v_valido := false;
    END IF;
    IF v_dados.tipo_chave_pix = 'E-mail' AND v_dados.chave_pix NOT LIKE '%@%.%' THEN
      v_erros := array_append(v_erros, 'Chave PIX (E-mail) inválida'); v_valido := false;
    END IF;
    IF v_dados.tipo_chave_pix = 'Telefone' AND length(regexp_replace(v_dados.chave_pix, '[^0-9]', '', 'g')) NOT IN (10,11) THEN
      v_erros := array_append(v_erros, 'Chave PIX (Telefone) inválida'); v_valido := false;
    END IF;
  END IF;

  IF v_dados.forma_pagamento = 'Cheque' THEN
    IF v_dados.cheque_motivo IS NULL OR v_dados.cheque_motivo = '' THEN
      v_erros := array_append(v_erros, 'Motivo do cheque obrigatório'); v_valido := false;
    END IF;
    IF v_dados.cheque_autorizado_por IS NULL OR v_dados.cheque_autorizado_por = '' THEN
      v_erros := array_append(v_erros, 'Autorização do cheque obrigatória'); v_valido := false;
    END IF;
  END IF;

  -- Verificar duplicidade de conta
  IF v_dados.forma_pagamento IN ('Conta Salário Itaú', 'Conta Sicredi')
     AND v_dados.banco IS NOT NULL AND v_dados.agencia IS NOT NULL AND v_dados.conta IS NOT NULL THEN
    IF EXISTS (
      SELECT 1 FROM pag_dados_bancarios
      WHERE banco = v_dados.banco AND agencia = v_dados.agencia AND conta = v_dados.conta
        AND ativo = true AND id != p_dados_id
    ) THEN
      v_erros := array_append(v_erros, 'Conta bancária já cadastrada para outro colaborador');
      v_valido := false;
    END IF;
  END IF;

  -- Verificar duplicidade de PIX
  IF v_dados.forma_pagamento = 'PIX' AND v_dados.chave_pix IS NOT NULL THEN
    IF EXISTS (
      SELECT 1 FROM pag_dados_bancarios
      WHERE chave_pix = v_dados.chave_pix AND ativo = true AND id != p_dados_id
    ) THEN
      v_erros := array_append(v_erros, 'Chave PIX já cadastrada para outro colaborador');
      v_valido := false;
    END IF;
  END IF;

  -- Verificar divergência de titular
  IF v_dados.cpf_cnpj_titular IS NOT NULL THEN
    DECLARE v_cpf_func TEXT;
    BEGIN
      SELECT cpf INTO v_cpf_func FROM rh_funcionarios WHERE id = v_dados.funcionario_id;
      IF v_cpf_func IS NOT NULL
         AND regexp_replace(v_dados.cpf_cnpj_titular, '[^0-9]', '', 'g') != regexp_replace(v_cpf_func, '[^0-9]', '', 'g') THEN
        v_erros := array_append(v_erros, 'CPF do titular diverge do CPF do colaborador');
      END IF;
    END;
  END IF;

  -- Atualizar registro
  UPDATE pag_dados_bancarios
  SET validado = v_valido,
      validado_em = CASE WHEN v_valido THEN now() ELSE NULL END,
      erros_validacao = to_jsonb(v_erros),
      atualizado_em = now()
  WHERE id = p_dados_id;

  RETURN jsonb_build_object('valido', v_valido, 'erros', to_jsonb(v_erros));
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

-- ── CHECK DE INTEGRIDADE DO LOTE ───────────────────
CREATE OR REPLACE FUNCTION pag_check_integridade_lote(p_lote_id UUID)
RETURNS JSONB AS $$
DECLARE
  v_sem_pagamento INT;
  v_incompletos INT;
  v_nao_validados INT;
  v_nao_aprovados INT;
  v_bloqueados INT;
  v_total INT;
  v_tem_erros BOOLEAN := false;
  v_resultado JSONB;
BEGIN
  SELECT COUNT(*) INTO v_total FROM pag_lote_itens WHERE lote_id = p_lote_id AND status_item != 'Removido';

  -- Sem forma de pagamento
  SELECT COUNT(*) INTO v_sem_pagamento
  FROM pag_lote_itens li
  WHERE li.lote_id = p_lote_id AND li.status_item != 'Removido'
    AND NOT EXISTS (SELECT 1 FROM pag_dados_bancarios pb WHERE pb.funcionario_id = li.funcionario_id AND pb.ativo = true);

  -- Cadastro incompleto (não validado)
  SELECT COUNT(*) INTO v_nao_validados
  FROM pag_lote_itens li
  JOIN pag_dados_bancarios pb ON pb.funcionario_id = li.funcionario_id AND pb.ativo = true
  WHERE li.lote_id = p_lote_id AND li.status_item != 'Removido' AND pb.validado = false;

  -- Alterações não aprovadas
  SELECT COUNT(*) INTO v_nao_aprovados
  FROM pag_lote_itens li
  WHERE li.lote_id = p_lote_id AND li.status_item != 'Removido'
    AND EXISTS (
      SELECT 1 FROM pag_solicitacoes_alteracao sa
      WHERE sa.funcionario_id = li.funcionario_id AND sa.status = 'Pendente'
    );

  -- Bloqueados
  SELECT COUNT(*) INTO v_bloqueados
  FROM pag_lote_itens li
  JOIN pag_dados_bancarios pb ON pb.funcionario_id = li.funcionario_id AND pb.ativo = true
  WHERE li.lote_id = p_lote_id AND li.status_item != 'Removido' AND pb.status = 'Bloqueado';

  v_tem_erros := (v_sem_pagamento > 0 OR v_bloqueados > 0);

  -- Atualizar itens com problemas
  UPDATE pag_lote_itens SET status_item = 'Bloqueado', motivo_alerta = 'Sem dados de pagamento'
  WHERE lote_id = p_lote_id AND status_item != 'Removido'
    AND NOT EXISTS (SELECT 1 FROM pag_dados_bancarios pb WHERE pb.funcionario_id = pag_lote_itens.funcionario_id AND pb.ativo = true);

  UPDATE pag_lote_itens SET status_item = 'Alerta', motivo_alerta = 'Dados não validados'
  WHERE lote_id = p_lote_id AND status_item = 'OK'
    AND funcionario_id IN (
      SELECT pb.funcionario_id FROM pag_dados_bancarios pb WHERE pb.ativo = true AND pb.validado = false
    );

  -- Salvar resultado
  v_resultado := jsonb_build_object(
    'total', v_total,
    'sem_pagamento', v_sem_pagamento,
    'nao_validados', v_nao_validados,
    'nao_aprovados', v_nao_aprovados,
    'bloqueados', v_bloqueados,
    'tem_erros_criticos', v_tem_erros,
    'pode_enviar', NOT v_tem_erros,
    'verificado_em', now()
  );

  UPDATE pag_lotes
  SET check_integridade = v_resultado,
      tem_erros_criticos = v_tem_erros,
      tem_alertas = (v_nao_validados > 0 OR v_nao_aprovados > 0),
      atualizado_em = now()
  WHERE id = p_lote_id;

  RETURN v_resultado;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

-- ── GERAR ALERTAS AUTOMÁTICOS ──────────────────────
CREATE OR REPLACE FUNCTION pag_gerar_alertas()
RETURNS INT AS $$
DECLARE
  v_count INT := 0;
BEGIN
  -- Sem forma de pagamento
  INSERT INTO pag_alertas (funcionario_id, tipo, descricao, severidade)
  SELECT f.id, 'Sem Forma Pagamento', 'Colaborador ativo sem dados de pagamento cadastrados', 'Bloqueante'
  FROM rh_funcionarios f
  WHERE f.status = 'Ativo'
    AND NOT EXISTS (SELECT 1 FROM pag_dados_bancarios pb WHERE pb.funcionario_id = f.id AND pb.ativo = true)
    AND NOT EXISTS (SELECT 1 FROM pag_alertas a WHERE a.funcionario_id = f.id AND a.tipo = 'Sem Forma Pagamento' AND a.resolvido = false);
  GET DIAGNOSTICS v_count = ROW_COUNT;

  -- Cadastro incompleto
  INSERT INTO pag_alertas (funcionario_id, tipo, descricao, severidade)
  SELECT pb.funcionario_id, 'Cadastro Incompleto', 'Dados bancários cadastrados mas não validados', 'Alerta'
  FROM pag_dados_bancarios pb
  JOIN rh_funcionarios f ON f.id = pb.funcionario_id
  WHERE pb.ativo = true AND pb.validado = false AND f.status = 'Ativo'
    AND NOT EXISTS (SELECT 1 FROM pag_alertas a WHERE a.funcionario_id = pb.funcionario_id AND a.tipo = 'Cadastro Incompleto' AND a.resolvido = false);

  -- Sem vínculo organizacional
  INSERT INTO pag_alertas (funcionario_id, tipo, descricao, severidade)
  SELECT f.id, 'Sem Vínculo Organizacional', 'Colaborador sem empresa ou filial definida', 'Crítico'
  FROM rh_funcionarios f
  WHERE f.status = 'Ativo' AND (f.empresa_id IS NULL OR f.filial_id IS NULL)
    AND NOT EXISTS (SELECT 1 FROM pag_alertas a WHERE a.funcionario_id = f.id AND a.tipo = 'Sem Vínculo Organizacional' AND a.resolvido = false);

  RETURN v_count;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

-- ══════════════════════════════════════════════════════
-- PARTE 9 — RLS PARA NOVAS TABELAS
-- ══════════════════════════════════════════════════════

ALTER TABLE org_empresas ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_filiais ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_setores ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_dados_bancarios ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_solicitacoes_alteracao ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_lotes ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_lote_itens ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_log_exportacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_alertas ENABLE ROW LEVEL SECURITY;
ALTER TABLE pag_historico ENABLE ROW LEVEL SECURITY;

-- Empresas/Filiais: leitura para todos, escrita para admin/rh
CREATE POLICY org_emp_select ON org_empresas FOR SELECT USING (true);
CREATE POLICY org_emp_modify ON org_empresas FOR ALL USING (rh_meu_perfil() IN ('rh','admin'));

CREATE POLICY org_fil_select ON org_filiais FOR SELECT USING (true);
CREATE POLICY org_fil_modify ON org_filiais FOR ALL USING (rh_meu_perfil() IN ('rh','admin'));

CREATE POLICY org_set_select ON org_setores FOR SELECT USING (true);
CREATE POLICY org_set_modify ON org_setores FOR ALL USING (rh_meu_perfil() IN ('rh','admin'));

-- Dados bancários: colaborador vê só os seus, RH/financeiro vê todos
CREATE POLICY pag_dados_colab ON pag_dados_bancarios
  FOR SELECT USING (rh_meu_perfil() = 'colaborador' AND funcionario_id = rh_meu_funcionario_id());
CREATE POLICY pag_dados_rh ON pag_dados_bancarios
  FOR ALL USING (rh_meu_perfil() IN ('rh','financeiro','admin'));

-- Lotes: somente RH/financeiro/admin
CREATE POLICY pag_lotes_all ON pag_lotes
  FOR ALL USING (rh_meu_perfil() IN ('rh','financeiro','admin'));
CREATE POLICY pag_lote_itens_all ON pag_lote_itens
  FOR ALL USING (rh_meu_perfil() IN ('rh','financeiro','admin'));

-- Solicitações: colaborador cria, RH/financeiro gerencia
CREATE POLICY pag_solic_colab ON pag_solicitacoes_alteracao
  FOR INSERT WITH CHECK (true);
CREATE POLICY pag_solic_select ON pag_solicitacoes_alteracao
  FOR SELECT USING (rh_meu_perfil() IN ('rh','financeiro','admin') OR funcionario_id = rh_meu_funcionario_id());
CREATE POLICY pag_solic_update ON pag_solicitacoes_alteracao
  FOR UPDATE USING (rh_meu_perfil() IN ('rh','financeiro','admin'));

-- Log de exportação: somente admin/rh
CREATE POLICY pag_log_insert ON pag_log_exportacao FOR INSERT WITH CHECK (true);
CREATE POLICY pag_log_select ON pag_log_exportacao FOR SELECT USING (rh_meu_perfil() IN ('rh','financeiro','admin'));

-- Alertas: RH/financeiro
CREATE POLICY pag_alertas_all ON pag_alertas FOR ALL USING (rh_meu_perfil() IN ('rh','financeiro','admin'));

-- Histórico: RH/financeiro/admin leitura
CREATE POLICY pag_hist_insert ON pag_historico FOR INSERT WITH CHECK (true);
CREATE POLICY pag_hist_select ON pag_historico FOR SELECT USING (rh_meu_perfil() IN ('rh','financeiro','admin'));
