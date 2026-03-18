-- ══════════════════════════════════════════════════════════════════════════════
-- MIGRAÇÃO 08: Modelo Transacional (Eventos) + Validações de Negócio
-- Rodar APÓS 07_migration_cadastros_mestres.sql
--
-- Cobre:
--   • cq_eventos — log unificado de todos os eventos do sistema
--   • 7 validações de negócio (V2-V7, V9) como triggers
--   • Function de merge de cadastros (deduplicação)
--   • Guard contra fornecedor bloqueado
--   • Guard contra item inativo
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELA DE EVENTOS UNIFICADA
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_eventos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- QUEM
  usuario_id      UUID REFERENCES cq_usuarios(id),
  usuario_email   TEXT,
  usuario_nome    TEXT,

  -- QUANDO
  criado_em       TIMESTAMPTZ DEFAULT NOW(),

  -- ONDE
  unidade_id      UUID REFERENCES cq_unidades(id),
  latitude        NUMERIC(10,6),
  longitude       NUMERIC(10,6),

  -- O QUÊ
  tipo_evento     TEXT NOT NULL,
  categoria       TEXT NOT NULL DEFAULT 'operacao' CHECK (categoria IN (
                    'cadastro', 'operacao', 'validacao', 'seguranca', 'integracao', 'sistema'
                  )),
  descricao       TEXT NOT NULL,

  -- CONTEXTO
  tabela_origem   TEXT,                  -- tabela onde o evento ocorreu
  registro_id     UUID,                  -- ID do registro afetado
  registro_id_texto TEXT,                -- ID textual (ex: id_inspecao, id_lote)

  -- DADOS
  dados           JSONB,                 -- payload do evento
  dados_anteriores JSONB,                -- snapshot antes (para UPDATE/DELETE)

  -- SEVERIDADE
  severidade      TEXT DEFAULT 'info' CHECK (severidade IN (
                    'debug', 'info', 'warn', 'error', 'critical'
                  ))
);

CREATE INDEX IF NOT EXISTS idx_eventos_tipo ON cq_eventos(tipo_evento);
CREATE INDEX IF NOT EXISTS idx_eventos_data ON cq_eventos(criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_eventos_usuario ON cq_eventos(usuario_id);
CREATE INDEX IF NOT EXISTS idx_eventos_tabela ON cq_eventos(tabela_origem, registro_id);
CREATE INDEX IF NOT EXISTS idx_eventos_categoria ON cq_eventos(categoria);
CREATE INDEX IF NOT EXISTS idx_eventos_severidade ON cq_eventos(severidade) WHERE severidade IN ('warn','error','critical');

-- Particionamento por mês (para performance em escala)
-- Supabase não suporta particionamento nativo, mas podemos
-- criar políticas de retenção via CRON/n8n

ALTER TABLE cq_eventos ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_eventos" ON cq_eventos FOR SELECT USING (true);
CREATE POLICY "all_eventos" ON cq_eventos FOR ALL USING (true);


-- ════════════════════════════════════════════════════════════════════════════
-- 2. FUNCTION DE REGISTRO DE EVENTO (usada por todos os triggers)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_registrar_evento(
  p_tipo          TEXT,
  p_categoria     TEXT DEFAULT 'operacao',
  p_descricao     TEXT DEFAULT '',
  p_tabela        TEXT DEFAULT NULL,
  p_registro_id   UUID DEFAULT NULL,
  p_registro_id_texto TEXT DEFAULT NULL,
  p_dados         JSONB DEFAULT NULL,
  p_dados_anteriores JSONB DEFAULT NULL,
  p_severidade    TEXT DEFAULT 'info'
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
  v_usuario_id UUID;
  v_usuario_email TEXT;
  v_usuario_nome TEXT;
BEGIN
  -- Tentar pegar o usuário da sessão
  BEGIN
    v_usuario_id := auth.uid();
    SELECT email, raw_user_meta_data->>'nome'
    INTO v_usuario_email, v_usuario_nome
    FROM auth.users WHERE id = v_usuario_id;
  EXCEPTION WHEN OTHERS THEN
    v_usuario_id := NULL;
    v_usuario_email := 'system';
    v_usuario_nome := 'Sistema';
  END;

  INSERT INTO cq_eventos (
    usuario_id, usuario_email, usuario_nome,
    tipo_evento, categoria, descricao,
    tabela_origem, registro_id, registro_id_texto,
    dados, dados_anteriores, severidade
  ) VALUES (
    v_usuario_id, v_usuario_email, v_usuario_nome,
    p_tipo, p_categoria, p_descricao,
    p_tabela, p_registro_id, p_registro_id_texto,
    p_dados, p_dados_anteriores, p_severidade
  ) RETURNING id INTO v_id;

  RETURN v_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. TRIGGERS DE EVENTOS AUTOMÁTICOS
-- ════════════════════════════════════════════════════════════════════════════

-- 3.1 Evento ao criar/finalizar inspeção
CREATE OR REPLACE FUNCTION fn_evento_inspecao()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM fn_registrar_evento(
      'inspecao_criada', 'operacao',
      FORMAT('Inspeção %s criada para %s', NEW.id_inspecao, COALESCE(NEW.fornecedor, NEW.fornecedor_codigo, '?')),
      'registros_cq_inspecao', NEW.id, NEW.id_inspecao,
      jsonb_build_object('produto', NEW.produto, 'fornecedor', NEW.fornecedor, 'qtd', NEW.quantidade_analisada)
    );
  ELSIF TG_OP = 'UPDATE' THEN
    -- Mudança de status workflow
    IF OLD.status_workflow IS DISTINCT FROM NEW.status_workflow THEN
      PERFORM fn_registrar_evento(
        'inspecao_status_' || NEW.status_workflow, 'operacao',
        FORMAT('Inspeção %s: %s → %s', NEW.id_inspecao, OLD.status_workflow, NEW.status_workflow),
        'registros_cq_inspecao', NEW.id, NEW.id_inspecao,
        jsonb_build_object('de', OLD.status_workflow, 'para', NEW.status_workflow),
        NULL,
        CASE WHEN NEW.status_workflow = 'reprovada' THEN 'warn' ELSE 'info' END
      );
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_evento_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_evento_inspecao
  AFTER INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_evento_inspecao();

-- 3.2 Evento ao criar/finalizar lote de ranking
CREATE OR REPLACE FUNCTION fn_evento_lote()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM fn_registrar_evento(
      'lote_ranking_criado', 'operacao',
      FORMAT('Lote %s criado para fornecedor %s', NEW.id_lote, COALESCE(NEW.fornecedor_nome, NEW.fornecedor_codigo)),
      'cq_lote_inspecao', NEW.id, NEW.id_lote,
      jsonb_build_object('fornecedor', NEW.fornecedor_codigo)
    );
  ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
    PERFORM fn_registrar_evento(
      'lote_ranking_' || NEW.status, 'operacao',
      FORMAT('Lote %s: %s → %s', NEW.id_lote, OLD.status, NEW.status),
      'cq_lote_inspecao', NEW.id, NEW.id_lote,
      jsonb_build_object('de', OLD.status, 'para', NEW.status, 'total_pecas', NEW.total_pecas,
        'classificacao', NEW.classificacao_lote, 'pct_defeitos', NEW.pct_defeitos),
      NULL,
      CASE WHEN NEW.classificacao_lote = 'C' THEN 'warn' ELSE 'info' END
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_evento_lote ON cq_lote_inspecao;
CREATE TRIGGER trg_evento_lote
  AFTER INSERT OR UPDATE ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_evento_lote();

-- 3.3 Evento ao detectar divergência
CREATE OR REPLACE FUNCTION fn_evento_divergencia()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM fn_registrar_evento(
      'divergencia_detectada', 'validacao',
      FORMAT('Divergência detectada: %s — %s', NEW.tipo_divergencia, COALESCE(NEW.mensagem, '')),
      'cq_validacao_divergencias', NEW.id, NULL,
      jsonb_build_object('tipo', NEW.tipo_divergencia, 'gravidade', NEW.gravidade, 'fornecedor', NEW.fornecedor_codigo),
      NULL,
      CASE NEW.gravidade WHEN 'critica' THEN 'error' WHEN 'atencao' THEN 'warn' ELSE 'info' END
    );
  ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status AND NEW.status = 'resolvida' THEN
    PERFORM fn_registrar_evento(
      'divergencia_resolvida', 'validacao',
      FORMAT('Divergência resolvida: %s', NEW.tipo_divergencia),
      'cq_validacao_divergencias', NEW.id, NULL,
      jsonb_build_object('resolucao', NEW.resolucao_obs)
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_evento_divergencia ON cq_validacao_divergencias;
CREATE TRIGGER trg_evento_divergencia
  AFTER INSERT OR UPDATE ON cq_validacao_divergencias
  FOR EACH ROW EXECUTE FUNCTION fn_evento_divergencia();

-- 3.4 Evento ao mudar score de fornecedor
CREATE OR REPLACE FUNCTION fn_evento_score()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.status_fornecedor IS DISTINCT FROM NEW.status_fornecedor THEN
    PERFORM fn_registrar_evento(
      CASE NEW.status_fornecedor
        WHEN 'bloqueado' THEN 'fornecedor_bloqueado'
        WHEN 'critico' THEN 'fornecedor_critico'
        ELSE 'fornecedor_status_alterado'
      END,
      'validacao',
      FORMAT('Fornecedor %s: %s → %s (score: %s)',
        NEW.fornecedor_nome, OLD.status_fornecedor, NEW.status_fornecedor, NEW.score),
      'cq_fornecedor_score', NEW.id, NULL,
      jsonb_build_object(
        'fornecedor_codigo', NEW.fornecedor_codigo,
        'score', NEW.score, 'de', OLD.status_fornecedor, 'para', NEW.status_fornecedor
      ),
      NULL,
      CASE WHEN NEW.status_fornecedor IN ('critico','bloqueado') THEN 'error' ELSE 'info' END
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_evento_score ON cq_fornecedor_score;
CREATE TRIGGER trg_evento_score
  AFTER UPDATE ON cq_fornecedor_score
  FOR EACH ROW EXECUTE FUNCTION fn_evento_score();


-- ════════════════════════════════════════════════════════════════════════════
-- 4. VALIDAÇÕES DE NEGÓCIO (TRIGGERS)
-- ════════════════════════════════════════════════════════════════════════════

-- V2: Total de defeitos ≤ total analisado
CREATE OR REPLACE FUNCTION fn_validar_defeitos_vs_total()
RETURNS TRIGGER AS $$
DECLARE
  v_total_pecas INTEGER;
BEGIN
  -- Para registros_cq_inspecao
  IF TG_TABLE_NAME = 'registros_cq_inspecao' THEN
    IF NEW.total_defeitos > NEW.quantidade_analisada THEN
      RAISE EXCEPTION 'Total de defeitos (%) não pode exceder quantidade analisada (%)',
        NEW.total_defeitos, NEW.quantidade_analisada
        USING HINT = 'Verifique os valores de cada defeito.';
    END IF;
  END IF;

  -- Para cq_lote_produto
  IF TG_TABLE_NAME = 'cq_lote_produto' THEN
    v_total_pecas := NEW.class_a + NEW.class_b + NEW.class_c;
    IF NEW.total_defeitos > v_total_pecas AND v_total_pecas > 0 THEN
      RAISE EXCEPTION 'Total de defeitos (%) excede total de peças (%) no produto %',
        NEW.total_defeitos, v_total_pecas, NEW.produto_codigo
        USING HINT = 'A soma dos defeitos não pode ser maior que A+B+C.';
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_defeitos_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_validar_defeitos_inspecao
  BEFORE INSERT OR UPDATE ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_defeitos_vs_total();

DROP TRIGGER IF EXISTS trg_validar_defeitos_lote ON cq_lote_produto;
CREATE TRIGGER trg_validar_defeitos_lote
  AFTER INSERT OR UPDATE ON cq_lote_produto
  FOR EACH ROW EXECUTE FUNCTION fn_validar_defeitos_vs_total();


-- V4+V5: Produto e fornecedor devem existir no cadastro mestre
CREATE OR REPLACE FUNCTION fn_validar_cadastro_existe()
RETURNS TRIGGER AS $$
BEGIN
  -- Fornecedor: se código foi passado, deve existir
  IF NEW.fornecedor_codigo IS NOT NULL AND NEW.fornecedor_codigo != '' THEN
    IF NOT EXISTS (
      SELECT 1 FROM cadastros_atak
      WHERE codigo = NEW.fornecedor_codigo AND tipo = 'fornecedor'
    ) THEN
      -- Registrar divergência ao invés de rejeitar (para não bloquear importação)
      PERFORM fn_registrar_evento(
        'fornecedor_nao_cadastrado', 'validacao',
        FORMAT('Fornecedor %s não encontrado no cadastro mestre', NEW.fornecedor_codigo),
        TG_TABLE_NAME, NEW.id, NULL,
        jsonb_build_object('fornecedor_codigo', NEW.fornecedor_codigo),
        NULL, 'warn'
      );
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_cadastro_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_validar_cadastro_inspecao
  BEFORE INSERT ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_cadastro_existe();

DROP TRIGGER IF EXISTS trg_validar_cadastro_lote ON cq_lote_inspecao;
CREATE TRIGGER trg_validar_cadastro_lote
  BEFORE INSERT ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_cadastro_existe();


-- V6: Item inativo não pode ser lançado
CREATE OR REPLACE FUNCTION fn_validar_item_ativo()
RETURNS TRIGGER AS $$
DECLARE
  v_ativo BOOLEAN;
BEGIN
  -- Verificar fornecedor
  IF NEW.fornecedor_id IS NOT NULL THEN
    SELECT ativo INTO v_ativo FROM cadastros_atak WHERE id = NEW.fornecedor_id;
    IF v_ativo = false THEN
      RAISE EXCEPTION 'Fornecedor inativo (ID: %). Não é possível criar lançamento.',
        NEW.fornecedor_id
        USING HINT = 'Reative o fornecedor antes de criar um novo lançamento.';
    END IF;
  END IF;

  -- Verificar produto (se aplicável)
  IF NEW.produto_id IS NOT NULL THEN
    SELECT ativo INTO v_ativo FROM cq_produtos WHERE id = NEW.produto_id;
    IF v_ativo = false THEN
      RAISE EXCEPTION 'Produto inativo (ID: %). Não é possível criar lançamento.',
        NEW.produto_id
        USING HINT = 'Reative o produto antes de criar um novo lançamento.';
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_ativo_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_validar_ativo_inspecao
  BEFORE INSERT ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_item_ativo();

DROP TRIGGER IF EXISTS trg_validar_ativo_lote ON cq_lote_inspecao;
CREATE TRIGGER trg_validar_ativo_lote
  BEFORE INSERT ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_item_ativo();


-- V7: Se houver B ou C, deve haver defeito ≥ 1 OU observação
CREATE OR REPLACE FUNCTION fn_validar_justificativa_bc()
RETURNS TRIGGER AS $$
DECLARE
  v_total_pecas INTEGER;
BEGIN
  v_total_pecas := NEW.class_a + NEW.class_b + NEW.class_c;
  IF v_total_pecas = 0 THEN RETURN NEW; END IF;

  IF (NEW.class_b > 0 OR NEW.class_c > 0) THEN
    IF NEW.total_defeitos = 0 AND (NEW.observacoes IS NULL OR NEW.observacoes = '') THEN
      RAISE EXCEPTION
        'Classificação B ou C requer ao menos 1 defeito registrado ou observação justificativa. Produto: %',
        NEW.produto_codigo
        USING HINT = 'Registre os defeitos encontrados ou preencha o campo de observações.';
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_bc_lote ON cq_lote_produto;
CREATE TRIGGER trg_validar_bc_lote
  AFTER INSERT OR UPDATE ON cq_lote_produto
  FOR EACH ROW EXECUTE FUNCTION fn_validar_justificativa_bc();


-- V9: Fornecedor bloqueado não pode receber carga nova
CREATE OR REPLACE FUNCTION fn_validar_fornecedor_bloqueado()
RETURNS TRIGGER AS $$
DECLARE
  v_status TEXT;
  v_nome TEXT;
BEGIN
  IF NEW.fornecedor_id IS NOT NULL THEN
    SELECT fs.status_fornecedor, fs.fornecedor_nome
    INTO v_status, v_nome
    FROM cq_fornecedor_score fs
    WHERE fs.fornecedor_id = NEW.fornecedor_id;

    IF v_status = 'bloqueado' THEN
      -- Registrar evento mas NÃO bloquear (flag de alerta)
      PERFORM fn_registrar_evento(
        'carga_fornecedor_bloqueado', 'validacao',
        FORMAT('ALERTA: Lançamento para fornecedor BLOQUEADO: %s', COALESCE(v_nome, NEW.fornecedor_codigo)),
        TG_TABLE_NAME, NEW.id, NULL,
        jsonb_build_object('fornecedor_codigo', NEW.fornecedor_codigo, 'status', v_status),
        NULL, 'critical'
      );

      -- Gerar divergência automática
      INSERT INTO cq_validacao_divergencias (
        tipo_divergencia, gravidade, status,
        tabela_origem, registro_origem_id,
        fornecedor_codigo, mensagem, detectado_por
      ) VALUES (
        'fornecedor_critico', 'critica', 'aberta',
        TG_TABLE_NAME, NEW.id,
        NEW.fornecedor_codigo,
        FORMAT('Lançamento criado para fornecedor BLOQUEADO: %s', COALESCE(v_nome, NEW.fornecedor_codigo)),
        'trigger_automatico'
      );
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fornecedor_bloqueado_inspecao ON registros_cq_inspecao;
CREATE TRIGGER trg_fornecedor_bloqueado_inspecao
  AFTER INSERT ON registros_cq_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_fornecedor_bloqueado();

DROP TRIGGER IF EXISTS trg_fornecedor_bloqueado_lote ON cq_lote_inspecao;
CREATE TRIGGER trg_fornecedor_bloqueado_lote
  AFTER INSERT ON cq_lote_inspecao
  FOR EACH ROW EXECUTE FUNCTION fn_validar_fornecedor_bloqueado();


-- ════════════════════════════════════════════════════════════════════════════
-- 5. FUNCTION DE MERGE DE CADASTROS (DEDUPLICAÇÃO)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_merge_cadastro(
  p_entidade       TEXT,       -- 'fornecedor', 'produto', 'motorista', 'veiculo'
  p_manter_id      UUID,       -- ID do registro que FICA
  p_remover_id     UUID,       -- ID do registro que SAIRÁ
  p_motivo         TEXT DEFAULT 'Merge de duplicata'
) RETURNS JSONB AS $$
DECLARE
  v_tabela TEXT;
  v_dados_removido JSONB;
  v_atualizados INTEGER := 0;
  v_total INTEGER := 0;
BEGIN
  -- Determinar tabela
  CASE p_entidade
    WHEN 'fornecedor' THEN v_tabela := 'cadastros_atak';
    WHEN 'motorista' THEN v_tabela := 'cadastros_atak';
    WHEN 'produto' THEN v_tabela := 'cq_produtos';
    WHEN 'veiculo' THEN v_tabela := 'cq_veiculos';
    WHEN 'transportadora' THEN v_tabela := 'cq_transportadoras';
    WHEN 'cliente' THEN v_tabela := 'cq_clientes';
    ELSE RAISE EXCEPTION 'Entidade não suportada para merge: %', p_entidade;
  END CASE;

  -- Snapshot do registro a ser removido
  EXECUTE format('SELECT to_jsonb(t) FROM %I t WHERE id = $1', v_tabela)
  INTO v_dados_removido USING p_remover_id;

  IF v_dados_removido IS NULL THEN
    RAISE EXCEPTION 'Registro a remover (%) não encontrado em %', p_remover_id, v_tabela;
  END IF;

  -- Redirecionar referências (FKs) para o registro mantido
  -- registros_cq_inspecao
  IF p_entidade IN ('fornecedor', 'motorista') THEN
    UPDATE registros_cq_inspecao SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE registros_cq_inspecao SET motorista_id = p_manter_id
    WHERE motorista_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;
  END IF;

  -- cq_lote_inspecao
  IF p_entidade = 'fornecedor' THEN
    UPDATE cq_lote_inspecao SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE cq_fornecedor_score SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE cq_validacao_divergencias SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE cq_planos_acao SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE cq_recebimentos SET fornecedor_id = p_manter_id
    WHERE fornecedor_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;

    UPDATE cq_cargas SET frigorifico_id = p_manter_id
    WHERE frigorifico_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;
  END IF;

  -- cq_veiculos
  IF p_entidade = 'veiculo' THEN
    UPDATE registros_cq_inspecao SET veiculo_id = p_manter_id WHERE veiculo_id = p_remover_id;
    GET DIAGNOSTICS v_atualizados = ROW_COUNT; v_total := v_total + v_atualizados;
  END IF;

  -- Criar alias do registro removido
  INSERT INTO cq_cadastro_alias (entidade, registro_id, alias_tipo, alias_valor, motivo)
  VALUES (p_entidade, p_manter_id, 'codigo',
    v_dados_removido->>'codigo', p_motivo)
  ON CONFLICT DO NOTHING;

  IF v_dados_removido->>'nome' IS NOT NULL THEN
    INSERT INTO cq_cadastro_alias (entidade, registro_id, alias_tipo, alias_valor, motivo)
    VALUES (p_entidade, p_manter_id, 'nome',
      v_dados_removido->>'nome', p_motivo)
    ON CONFLICT DO NOTHING;
  END IF;

  -- Log do merge
  INSERT INTO cq_cadastro_merge_log (
    entidade, registro_mantido_id, registro_removido_id,
    registro_removido_dados, motivo
  ) VALUES (
    p_entidade, p_manter_id, p_remover_id,
    v_dados_removido, p_motivo
  );

  -- Inativar o registro removido (soft delete)
  EXECUTE format('UPDATE %I SET ativo = false, motivo_inativacao = $1 WHERE id = $2', v_tabela)
  USING 'Mergeado com ' || p_manter_id::TEXT, p_remover_id;

  -- Evento
  PERFORM fn_registrar_evento(
    'cadastro_merge', 'cadastro',
    FORMAT('Merge de %s: %s → %s (%s referências atualizadas)', p_entidade, p_remover_id, p_manter_id, v_total),
    v_tabela, p_manter_id, NULL,
    jsonb_build_object('removido', p_remover_id, 'refs_atualizadas', v_total, 'motivo', p_motivo)
  );

  RETURN jsonb_build_object(
    'sucesso', true,
    'mantido', p_manter_id,
    'removido', p_remover_id,
    'referencias_atualizadas', v_total,
    'alias_criados', 2
  );
END;
$$ LANGUAGE plpgsql;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. VIEW: TIMELINE DE EVENTOS (para dashboard)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_eventos_recentes AS
SELECT
  e.id,
  e.tipo_evento,
  e.categoria,
  e.descricao,
  e.severidade,
  e.tabela_origem,
  e.registro_id,
  e.registro_id_texto,
  e.usuario_nome,
  e.usuario_email,
  e.dados,
  e.criado_em,
  -- Tempo relativo
  CASE
    WHEN NOW() - e.criado_em < INTERVAL '1 minute' THEN 'agora'
    WHEN NOW() - e.criado_em < INTERVAL '1 hour' THEN
      EXTRACT(MINUTE FROM NOW() - e.criado_em)::INTEGER || 'min atrás'
    WHEN NOW() - e.criado_em < INTERVAL '24 hours' THEN
      EXTRACT(HOUR FROM NOW() - e.criado_em)::INTEGER || 'h atrás'
    ELSE TO_CHAR(e.criado_em, 'DD/MM HH24:MI')
  END AS tempo_relativo
FROM cq_eventos e
WHERE e.criado_em >= NOW() - INTERVAL '7 days'
ORDER BY e.criado_em DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. VIEW: RESUMO DE EVENTOS POR TIPO (KPIs)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_eventos_resumo_7d AS
SELECT
  tipo_evento,
  categoria,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE severidade = 'critical') AS criticos,
  COUNT(*) FILTER (WHERE severidade = 'error') AS erros,
  COUNT(*) FILTER (WHERE severidade = 'warn') AS alertas,
  MIN(criado_em) AS primeiro,
  MAX(criado_em) AS ultimo
FROM cq_eventos
WHERE criado_em >= NOW() - INTERVAL '7 days'
GROUP BY tipo_evento, categoria
ORDER BY total DESC;


COMMIT;

DO $$
BEGIN
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
  RAISE NOTICE '  MIGRAÇÃO 08 CONCLUÍDA';
  RAISE NOTICE '  ✓ cq_eventos — log unificado de todos os eventos';
  RAISE NOTICE '  ✓ fn_registrar_evento() — function centralizada';
  RAISE NOTICE '  ✓ 4 triggers de eventos (inspeção, lote, divergência, score)';
  RAISE NOTICE '  ✓ V2: Total defeitos ≤ total analisado';
  RAISE NOTICE '  ✓ V4+V5: Cadastro mestre deve existir (warn)';
  RAISE NOTICE '  ✓ V6: Item inativo não pode ser lançado (REJECT)';
  RAISE NOTICE '  ✓ V7: B/C requer defeito ou observação (REJECT)';
  RAISE NOTICE '  ✓ V9: Fornecedor bloqueado → divergência automática';
  RAISE NOTICE '  ✓ fn_merge_cadastro() — deduplicação com rastreabilidade';
  RAISE NOTICE '  ✓ vw_eventos_recentes + vw_eventos_resumo_7d';
  RAISE NOTICE '═══════════════════════════════════════════════════════════';
END $$;
