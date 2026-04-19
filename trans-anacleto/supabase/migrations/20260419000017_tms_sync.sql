-- =====================================================================
-- Fase 6.2 - Integracao TMS (Transporte Facil)
-- =====================================================================
-- Objetivo:
--   1. Sincronizar cadastros master do TMS (veiculos, motoristas) com o
--      novo modulo operacional via CSV/JSON.
--   2. Referenciar viagens operacionais com CT-e emitidos no TMS para
--      que o acerto financeiro cruze custos x receita.
--   3. Exportar custos operacionais agregados (combustivel + manutencao
--      + pneus) para consumo do TMS.
--
-- Dependencias:
--   trans_veiculos, trans_motoristas (Fase 2)
--   trans_abastecimentos (Fase 6)
--   trans_manutencoes + trans_manutencao_itens (Fase 5)
--   trans_pneus_movimentacoes (Fase 4)
--
-- Entregaveis:
--   - Tabela trans_viagens (minimal; extensao em 6.5 com telemetria)
--   - Tabela trans_tms_sync_log (auditoria)
--   - Tabela trans_tms_viagens_ref (vinculo 1:N viagem x CT-e)
--   - RPCs: importar_veiculos, importar_motoristas, vincular_cte,
--           exportar_custos_viagem, exportar_custos_periodo
-- =====================================================================

-- ---------------------------------------------------------------------
-- trans_viagens (stub minimal - sera extendido em Fase 6.5 com telemetria)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_viagens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  veiculo_id uuid NOT NULL REFERENCES trans_veiculos(id),
  motorista_id uuid REFERENCES trans_motoristas(id),
  data_inicio timestamptz NOT NULL,
  data_fim timestamptz,
  km_inicial numeric(12,2),
  km_final numeric(12,2),
  km_rodados numeric(12,2) GENERATED ALWAYS AS (
    CASE WHEN km_final IS NOT NULL AND km_inicial IS NOT NULL
         THEN km_final - km_inicial ELSE NULL END
  ) STORED,
  status text NOT NULL DEFAULT 'em_andamento'
    CHECK (status IN ('em_andamento','concluida','cancelada_auto','cancelada_manual')),
  origem text,
  destino text,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid,
  updated_by uuid,
  CHECK (data_fim IS NULL OR data_fim >= data_inicio),
  CHECK (km_final IS NULL OR km_inicial IS NULL OR km_final >= km_inicial)
);

CREATE INDEX IF NOT EXISTS idx_trans_viagens_veiculo ON trans_viagens(veiculo_id, data_inicio DESC);
CREATE INDEX IF NOT EXISTS idx_trans_viagens_motorista ON trans_viagens(motorista_id, data_inicio DESC);
CREATE INDEX IF NOT EXISTS idx_trans_viagens_status ON trans_viagens(status) WHERE status = 'em_andamento';

CREATE TRIGGER trans_viagens_upd BEFORE UPDATE ON trans_viagens
FOR EACH ROW EXECUTE FUNCTION trans_tg_set_updated_at();

ALTER TABLE trans_viagens ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_viagens_sel ON trans_viagens;
CREATE POLICY trans_viagens_sel ON trans_viagens FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_viagens_ins ON trans_viagens;
CREATE POLICY trans_viagens_ins ON trans_viagens FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor','almoxarife'));

DROP POLICY IF EXISTS trans_viagens_upd ON trans_viagens;
CREATE POLICY trans_viagens_upd ON trans_viagens FOR UPDATE TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- trans_tms_sync_log (auditoria de cada sincronizacao)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_tms_sync_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tabela text NOT NULL CHECK (tabela IN ('veiculos','motoristas','custos_export','cte_vinculo')),
  direcao text NOT NULL CHECK (direcao IN ('in','out')),
  payload jsonb,
  itens_processados int NOT NULL DEFAULT 0,
  itens_erro int NOT NULL DEFAULT 0,
  erros jsonb,
  executado_em timestamptz NOT NULL DEFAULT now(),
  executado_por uuid,
  observacao text
);

CREATE INDEX IF NOT EXISTS idx_trans_tms_sync_log_data ON trans_tms_sync_log(executado_em DESC);
CREATE INDEX IF NOT EXISTS idx_trans_tms_sync_log_tabela ON trans_tms_sync_log(tabela, executado_em DESC);

ALTER TABLE trans_tms_sync_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_tms_sync_log_sel ON trans_tms_sync_log;
CREATE POLICY trans_tms_sync_log_sel ON trans_tms_sync_log FOR SELECT TO authenticated
USING (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_tms_sync_log_ins ON trans_tms_sync_log;
CREATE POLICY trans_tms_sync_log_ins ON trans_tms_sync_log FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

-- ---------------------------------------------------------------------
-- trans_tms_viagens_ref (vinculo 1:N entre viagem operacional e CT-e)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans_tms_viagens_ref (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  viagem_id uuid NOT NULL REFERENCES trans_viagens(id) ON DELETE CASCADE,
  cte_numero text NOT NULL,
  cte_chave text,
  tms_viagem_id_externo text,
  valor_frete numeric(14,2),
  observacao text,
  vinculado_em timestamptz NOT NULL DEFAULT now(),
  vinculado_por uuid,
  UNIQUE(viagem_id, cte_numero)
);

CREATE INDEX IF NOT EXISTS idx_trans_tms_viagens_ref_viagem ON trans_tms_viagens_ref(viagem_id);
CREATE INDEX IF NOT EXISTS idx_trans_tms_viagens_ref_cte ON trans_tms_viagens_ref(cte_numero);

ALTER TABLE trans_tms_viagens_ref ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_tms_viagens_ref_sel ON trans_tms_viagens_ref;
CREATE POLICY trans_tms_viagens_ref_sel ON trans_tms_viagens_ref FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS trans_tms_viagens_ref_ins ON trans_tms_viagens_ref;
CREATE POLICY trans_tms_viagens_ref_ins ON trans_tms_viagens_ref FOR INSERT TO authenticated
WITH CHECK (trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_tms_viagens_ref_del ON trans_tms_viagens_ref;
CREATE POLICY trans_tms_viagens_ref_del ON trans_tms_viagens_ref FOR DELETE TO authenticated
USING (trans_current_perfil() = 'admin');

-- ---------------------------------------------------------------------
-- RPC: trans_tms_importar_veiculos
-- ---------------------------------------------------------------------
-- Recebe array JSON com:
--   [{ "placa": "ABC1D23", "modelo": "FH540", "marca": "Volvo", "ano": 2022,
--      "tipo": "cavalo", "km_atual": 145000, "capacidade_tanque_l": 600 }, ...]
-- Upsert por placa. Campos nulos no payload preservam valor atual.
CREATE OR REPLACE FUNCTION trans_tms_importar_veiculos(p_payload jsonb)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_user uuid := auth.uid();
  v_item jsonb;
  v_placa text;
  v_ok int := 0;
  v_erro int := 0;
  v_erros jsonb := '[]'::jsonb;
  v_existing uuid;
  v_km_novo numeric;
  v_km_atual numeric;
BEGIN
  IF NOT trans_is_admin() AND trans_current_perfil() != 'gestor' THEN
    RAISE EXCEPTION 'Apenas admin/gestor pode importar do TMS';
  END IF;

  IF jsonb_typeof(p_payload) != 'array' THEN
    RAISE EXCEPTION 'Payload deve ser array JSON';
  END IF;

  FOR v_item IN SELECT * FROM jsonb_array_elements(p_payload)
  LOOP
    BEGIN
      v_placa := upper(regexp_replace(v_item->>'placa','[^A-Z0-9]','','g'));
      IF v_placa IS NULL OR length(v_placa) < 7 THEN
        RAISE EXCEPTION 'Placa invalida: %', v_item->>'placa';
      END IF;

      SELECT id, km_atual INTO v_existing, v_km_atual
      FROM trans_veiculos WHERE upper(regexp_replace(placa,'[^A-Z0-9]','','g')) = v_placa;

      v_km_novo := COALESCE((v_item->>'km_atual')::numeric, v_km_atual, 0);
      IF v_km_atual IS NOT NULL THEN
        v_km_novo := GREATEST(v_km_atual, v_km_novo);
      END IF;

      IF v_existing IS NULL THEN
        INSERT INTO trans_veiculos (
          placa, modelo, marca, ano, tipo, km_atual,
          capacidade_tanque_l, status, created_by
        ) VALUES (
          v_placa,
          COALESCE(v_item->>'modelo','(sem modelo)'),
          v_item->>'marca',
          NULLIF(v_item->>'ano','')::int,
          COALESCE(v_item->>'tipo', 'outro'),
          v_km_novo,
          NULLIF(v_item->>'capacidade_tanque_l','')::numeric,
          COALESCE(v_item->>'status','ativo'),
          v_user
        );
      ELSE
        UPDATE trans_veiculos SET
          modelo = COALESCE(v_item->>'modelo', modelo),
          marca = COALESCE(v_item->>'marca', marca),
          ano = COALESCE(NULLIF(v_item->>'ano','')::int, ano),
          tipo = COALESCE(v_item->>'tipo', tipo),
          km_atual = v_km_novo,
          capacidade_tanque_l = COALESCE(NULLIF(v_item->>'capacidade_tanque_l','')::numeric, capacidade_tanque_l),
          updated_by = v_user
        WHERE id = v_existing;
      END IF;

      v_ok := v_ok + 1;
    EXCEPTION WHEN OTHERS THEN
      v_erro := v_erro + 1;
      v_erros := v_erros || jsonb_build_object('placa', v_item->>'placa', 'erro', SQLERRM);
    END;
  END LOOP;

  INSERT INTO trans_tms_sync_log (tabela, direcao, payload, itens_processados, itens_erro, erros, executado_por)
  VALUES ('veiculos', 'in', p_payload, v_ok, v_erro, NULLIF(v_erros,'[]'::jsonb), v_user);

  RETURN json_build_object('ok', v_ok, 'erro', v_erro, 'erros', v_erros);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_tms_importar_veiculos(jsonb) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_tms_importar_motoristas
-- ---------------------------------------------------------------------
-- Upsert por CPF (master). Cai para CNH se CPF nao informado.
CREATE OR REPLACE FUNCTION trans_tms_importar_motoristas(p_payload jsonb)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_user uuid := auth.uid();
  v_item jsonb;
  v_cpf text;
  v_cnh text;
  v_ok int := 0;
  v_erro int := 0;
  v_erros jsonb := '[]'::jsonb;
  v_existing uuid;
BEGIN
  IF NOT trans_is_admin() AND trans_current_perfil() != 'gestor' THEN
    RAISE EXCEPTION 'Apenas admin/gestor pode importar do TMS';
  END IF;

  IF jsonb_typeof(p_payload) != 'array' THEN
    RAISE EXCEPTION 'Payload deve ser array JSON';
  END IF;

  FOR v_item IN SELECT * FROM jsonb_array_elements(p_payload)
  LOOP
    BEGIN
      v_cpf := NULLIF(regexp_replace(COALESCE(v_item->>'cpf',''), '[^0-9]','','g'), '');
      v_cnh := NULLIF(regexp_replace(COALESCE(v_item->>'cnh',''), '[^0-9]','','g'), '');
      IF v_cpf IS NULL AND v_cnh IS NULL THEN
        RAISE EXCEPTION 'CPF ou CNH obrigatorio para motorista: %', v_item->>'nome';
      END IF;

      IF v_cpf IS NOT NULL THEN
        SELECT id INTO v_existing FROM trans_motoristas WHERE cpf = v_cpf;
      END IF;
      IF v_existing IS NULL AND v_cnh IS NOT NULL THEN
        SELECT id INTO v_existing FROM trans_motoristas
        WHERE regexp_replace(COALESCE(cnh,''),'[^0-9]','','g') = v_cnh;
      END IF;

      IF v_existing IS NULL THEN
        INSERT INTO trans_motoristas (
          nome, cpf, cnh, categoria_cnh, validade_cnh, telefone, created_by
        ) VALUES (
          COALESCE(v_item->>'nome', '(sem nome)'),
          v_cpf,
          v_cnh,
          v_item->>'categoria_cnh',
          NULLIF(v_item->>'validade_cnh','')::date,
          v_item->>'telefone',
          v_user
        );
      ELSE
        UPDATE trans_motoristas SET
          nome = COALESCE(v_item->>'nome', nome),
          cnh = COALESCE(v_cnh, cnh),
          cpf = COALESCE(v_cpf, cpf),
          categoria_cnh = COALESCE(v_item->>'categoria_cnh', categoria_cnh),
          validade_cnh = COALESCE(NULLIF(v_item->>'validade_cnh','')::date, validade_cnh),
          telefone = COALESCE(v_item->>'telefone', telefone),
          updated_by = v_user
        WHERE id = v_existing;
      END IF;

      v_ok := v_ok + 1;
      v_existing := NULL;
    EXCEPTION WHEN OTHERS THEN
      v_erro := v_erro + 1;
      v_erros := v_erros || jsonb_build_object('nome', v_item->>'nome', 'cpf', v_item->>'cpf', 'cnh', v_item->>'cnh', 'erro', SQLERRM);
      v_existing := NULL;
    END;
  END LOOP;

  INSERT INTO trans_tms_sync_log (tabela, direcao, payload, itens_processados, itens_erro, erros, executado_por)
  VALUES ('motoristas', 'in', p_payload, v_ok, v_erro, NULLIF(v_erros,'[]'::jsonb), v_user);

  RETURN json_build_object('ok', v_ok, 'erro', v_erro, 'erros', v_erros);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_tms_importar_motoristas(jsonb) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_tms_vincular_cte
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trans_tms_vincular_cte(
  p_viagem_id uuid,
  p_cte_numero text,
  p_cte_chave text DEFAULT NULL,
  p_tms_viagem_id_externo text DEFAULT NULL,
  p_valor_frete numeric DEFAULT NULL,
  p_observacao text DEFAULT NULL
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_user uuid := auth.uid();
  v_id uuid;
BEGIN
  IF trans_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM trans_viagens WHERE id = p_viagem_id) THEN
    RAISE EXCEPTION 'Viagem nao encontrada';
  END IF;

  IF p_cte_numero IS NULL OR length(trim(p_cte_numero)) = 0 THEN
    RAISE EXCEPTION 'Numero do CT-e obrigatorio';
  END IF;

  INSERT INTO trans_tms_viagens_ref (
    viagem_id, cte_numero, cte_chave, tms_viagem_id_externo,
    valor_frete, observacao, vinculado_por
  ) VALUES (
    p_viagem_id, trim(p_cte_numero), NULLIF(trim(p_cte_chave),''),
    NULLIF(trim(p_tms_viagem_id_externo),''), p_valor_frete, p_observacao, v_user
  ) RETURNING id INTO v_id;

  INSERT INTO trans_tms_sync_log (tabela, direcao, payload, itens_processados, executado_por)
  VALUES ('cte_vinculo', 'in',
    json_build_object('viagem_id', p_viagem_id, 'cte_numero', p_cte_numero)::jsonb,
    1, v_user);

  RETURN json_build_object('id', v_id, 'viagem_id', p_viagem_id, 'cte_numero', p_cte_numero);
END;
$$;

GRANT EXECUTE ON FUNCTION trans_tms_vincular_cte(uuid, text, text, text, numeric, text) TO authenticated;

-- ---------------------------------------------------------------------
-- RPC: trans_tms_exportar_custos_periodo
-- ---------------------------------------------------------------------
-- Agrega custos operacionais por veiculo no periodo para consumo no TMS.
-- Componentes: combustivel (trans_abastecimentos.valor_total),
-- manutencao (trans_manutencoes.custo_total_calc + itens consumidos),
-- pneus (custo de recapagem no periodo).
CREATE OR REPLACE FUNCTION trans_tms_exportar_custos_periodo(
  p_periodo_ini date,
  p_periodo_fim date
) RETURNS TABLE (
  veiculo_id uuid,
  placa text,
  modelo text,
  km_rodado numeric,
  litros_total numeric,
  custo_combustivel numeric,
  custo_manutencao numeric,
  custo_pneus numeric,
  custo_total numeric,
  abastecimentos_count int,
  manutencoes_count int
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_user uuid := auth.uid();
BEGIN
  IF trans_current_perfil() NOT IN ('admin','gestor') THEN
    RAISE EXCEPTION 'Sem permissao';
  END IF;

  INSERT INTO trans_tms_sync_log (tabela, direcao, payload, executado_por)
  VALUES ('custos_export', 'out',
    json_build_object('periodo_ini', p_periodo_ini, 'periodo_fim', p_periodo_fim)::jsonb,
    v_user);

  RETURN QUERY
  WITH abast AS (
    SELECT a.veiculo_id,
           COALESCE(SUM(a.litros),0) AS litros,
           COALESCE(SUM(a.valor_total),0) AS valor,
           COALESCE(SUM(a.km_percorrido),0) AS km,
           COUNT(*)::int AS cnt
    FROM trans_abastecimentos a
    WHERE a.data::date BETWEEN p_periodo_ini AND p_periodo_fim
    GROUP BY a.veiculo_id
  ),
  manut AS (
    SELECT m.veiculo_id,
           COALESCE(SUM(m.custo_total_calc),0) AS valor,
           COUNT(*)::int AS cnt
    FROM trans_manutencoes m
    WHERE m.status = 'concluida'
      AND m.data_encerramento::date BETWEEN p_periodo_ini AND p_periodo_fim
    GROUP BY m.veiculo_id
  ),
  pneus_cost AS (
    SELECT pp.veiculo_id,
           COALESCE(SUM(pr.custo),0) AS valor
    FROM trans_pneus_recapagem pr
    JOIN trans_pneus_posicoes pp ON pp.pneu_id = pr.pneu_id
    WHERE pr.data_retorno::date BETWEEN p_periodo_ini AND p_periodo_fim
      AND pp.veiculo_id IS NOT NULL
    GROUP BY pp.veiculo_id
  )
  SELECT
    v.id,
    v.placa,
    v.modelo,
    COALESCE(a.km, 0) AS km_rodado,
    COALESCE(a.litros, 0) AS litros_total,
    COALESCE(a.valor, 0) AS custo_combustivel,
    COALESCE(m.valor, 0) AS custo_manutencao,
    COALESCE(p.valor, 0) AS custo_pneus,
    COALESCE(a.valor,0) + COALESCE(m.valor,0) + COALESCE(p.valor,0) AS custo_total,
    COALESCE(a.cnt, 0) AS abastecimentos_count,
    COALESCE(m.cnt, 0) AS manutencoes_count
  FROM trans_veiculos v
  LEFT JOIN abast a ON a.veiculo_id = v.id
  LEFT JOIN manut m ON m.veiculo_id = v.id
  LEFT JOIN pneus_cost p ON p.veiculo_id = v.id
  WHERE COALESCE(a.cnt,0) + COALESCE(m.cnt,0) + COALESCE(p.valor,0) > 0
  ORDER BY v.placa;
END;
$$;

GRANT EXECUTE ON FUNCTION trans_tms_exportar_custos_periodo(date, date) TO authenticated;

-- ---------------------------------------------------------------------
-- View: v_trans_tms_sync_log_full (ultimas syncs com nome do usuario)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_trans_tms_sync_log_full AS
SELECT
  l.id,
  l.tabela,
  l.direcao,
  l.itens_processados,
  l.itens_erro,
  l.erros,
  l.executado_em,
  l.executado_por,
  u.nome AS executado_por_nome,
  l.observacao
FROM trans_tms_sync_log l
LEFT JOIN trans_usuarios u ON u.auth_user_id = l.executado_por
ORDER BY l.executado_em DESC;

GRANT SELECT ON v_trans_tms_sync_log_full TO authenticated;
