-- ============================================================================
-- SEED_ARMARIOS.sql — Importacao dos dados atuais de armarios
-- Dados fornecidos pela gestao em abril/2026
--
-- INSTRUCOES:
-- 1. Executar no Supabase SQL Editor (projeto RH: muiqmtnfvyffborgiwdw)
-- 2. Os blocos e armarios sao criados automaticamente
-- 3. As ocupacoes sao vinculadas por nome_completo do funcionario
-- ============================================================================

-- 1. CRIAR BLOCOS (vestiarios)
INSERT INTO arm_blocos (id, codigo, nome, tipo, localizacao, capacidade, ativo)
VALUES
  ('a0000000-0000-0000-0000-000000000001', 'FEM', 'Vestiario Feminino', 'feminino', 'Fabrica', 63, true),
  ('a0000000-0000-0000-0000-000000000002', 'MAS', 'Vestiario Masculino', 'masculino', 'Fabrica', 64, true)
ON CONFLICT DO NOTHING;

-- 2. CRIAR ARMARIOS FEMININOS (1-63)
INSERT INTO arm_armarios (codigo, bloco_id, tipo, status, ativo)
SELECT
  n::text,
  'a0000000-0000-0000-0000-000000000001',
  'feminino',
  'disponivel',
  true
FROM generate_series(1, 63) AS n
WHERE NOT EXISTS (
  SELECT 1 FROM arm_armarios WHERE codigo = n::text AND bloco_id = 'a0000000-0000-0000-0000-000000000001'
);

-- 3. CRIAR ARMARIOS MASCULINOS (1-64)
INSERT INTO arm_armarios (codigo, bloco_id, tipo, status, ativo)
SELECT
  n::text,
  'a0000000-0000-0000-0000-000000000002',
  'masculino',
  'disponivel',
  true
FROM generate_series(1, 64) AS n
WHERE NOT EXISTS (
  SELECT 1 FROM arm_armarios WHERE codigo = n::text AND bloco_id = 'a0000000-0000-0000-0000-000000000002'
);

-- 4. ATRIBUIR ARMARIOS FEMININOS
DO $$
DECLARE
  rec record;
  v_arm_id uuid;
  v_func_id uuid;
  v_bloco_fem uuid := 'a0000000-0000-0000-0000-000000000001';
BEGIN
  FOR rec IN
    SELECT column1 AS codigo, column2 AS nome FROM (VALUES
      ('1', 'CLARICE GONCALO MIRANDA'),
      ('2', 'CRISTIANE DOS SANTOS'),
      ('3', 'PATRICIA MAIARA DA CRUZ'),
      ('4', 'INGRID REICHE DA COSTA'),
      ('5', 'CELIA JOSE CAETANO'),
      ('17', 'ANA PAULA EMIDIO MIQUILINI'),
      ('18', 'MARIA ALVES MOURA'),
      ('19', 'NEIVA GALVAO'),
      ('20', 'PRISCILA FERNANDA DOS SANTOS'),
      ('21', 'MARIA DE PAIVA MOTA'),
      ('22', 'LOURDES RAIMUNDO MACIEL'),
      ('23', 'VICTORIA AMSTALDEM HENRIQUES'),
      ('24', 'THAIS APARECIDA SOUZA'),
      ('25', 'ADRIANA DOS SANTOS'),
      ('26', 'ANA ELOIZA DOS SANTOS'),
      ('27', 'SILVIA CRISTINA'),
      ('28', 'ZILMA DA ROCHA'),
      ('29', 'JESSICA CARDOSO'),
      ('30', 'VANUSA DOS SANTOS'),
      ('31', 'ERICA BRUNA BILELA VITTI'),
      ('32', 'VANESSA EMIDIO MIQUILINI'),
      ('33', 'LUCILENE CORDEIRO'),
      ('34', 'MARICA GOMES FRANCELINO'),
      ('35', 'MICHELE EDUARDA CAETANO'),
      ('36', 'ROSENILDA RIBEIRO DE QUADROS'),
      ('37', 'MARCELA APARECIDA FARIA'),
      ('38', 'MARIA BERNARDETE DE BRITO'),
      ('39', 'ANDREA APARECIDA PIOLA'),
      ('40', 'CINTIA NATANI FERREIRA'),
      ('41', 'CHELI PAULA DIAS'),
      ('42', 'JANELICE MARIA DA SILVA'),
      ('43', 'NEIDE DA CONCEICAO ROSA'),
      ('44', 'ADRIANA PEREIRA FERNANDES'),
      ('45', 'ANDRESSA DA SILVA ZENOVELI'),
      ('46', 'ROSINEI TAVARES GARCIA'),
      ('47', 'ROZELI PRESTES'),
      ('48', 'JANETE PORTO DAMASO'),
      ('49', 'ANGELA GUIMARAES'),
      ('50', 'DEBORA DIANA DA SILVA'),
      ('51', 'FERNANDA GABRIELE DA SILVA'),
      ('52', 'PATRICIA AMSTALDEM'),
      ('53', 'FERNANDA MIQUILINI'),
      ('54', 'LUZIA PEREIRA'),
      ('55', 'ANTONIA APARECIDA DA SILVA'),
      ('56', 'LUCINEIA DA SILVA'),
      ('57', 'LARISSA MAYARA RIOBEIRO'),
      ('58', 'ANDRESSA APARECIDA LOPES'),
      ('63', 'ARMARIO DE JOGOS')
    ) AS t(codigo, nome)
  LOOP
    -- Buscar armario
    SELECT id INTO v_arm_id FROM arm_armarios
    WHERE codigo = rec.codigo AND bloco_id = v_bloco_fem LIMIT 1;

    IF v_arm_id IS NULL THEN CONTINUE; END IF;

    -- Caso especial: armario de jogos
    IF rec.nome = 'ARMARIO DE JOGOS' THEN
      UPDATE arm_armarios SET status = 'ocupado', observacoes = 'Armario de jogos' WHERE id = v_arm_id;
      CONTINUE;
    END IF;

    -- Buscar funcionario por nome (fuzzy)
    SELECT id INTO v_func_id FROM rh_funcionarios
    WHERE UPPER(TRANSLATE(nome_completo, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇáéíóúàèìòùâêîôûãõç', 'AEIOUAEIOUAEIOUAOCaeiouaeiouaeiouaoc')) LIKE '%' || UPPER(LEFT(rec.nome, 15)) || '%'
      AND status IN ('ativo', 'ferias', 'experiencia')
    LIMIT 1;

    IF v_func_id IS NOT NULL THEN
      -- Criar ocupacao se nao existe
      IF NOT EXISTS (SELECT 1 FROM arm_ocupacoes WHERE armario_id = v_arm_id AND data_devolucao IS NULL) THEN
        INSERT INTO arm_ocupacoes (armario_id, funcionario_id, data_atribuicao, chave_entregue)
        VALUES (v_arm_id, v_func_id, CURRENT_DATE, true);
      END IF;
      UPDATE arm_armarios SET status = 'ocupado' WHERE id = v_arm_id;
    END IF;
  END LOOP;
END $$;

-- 5. ATRIBUIR ARMARIOS MASCULINOS
DO $$
DECLARE
  rec record;
  v_arm_id uuid;
  v_func_id uuid;
  v_bloco_mas uuid := 'a0000000-0000-0000-0000-000000000002';
BEGIN
  FOR rec IN
    SELECT column1 AS codigo, column2 AS nome FROM (VALUES
      ('1', 'JONAS LOPES'),
      ('2', 'MARIO SERGIO CARVALHO'),
      ('3', 'ANDERSON LUIZ DE SOUZA'),
      ('4', 'BRUNO VILAS BOAS DINIZ DE ANDRADE BORGES'),
      ('5', 'ELIAS MECHIOR DA SILVA'),
      ('6', 'BRUNO FELIPE DA SILVA CARVALHO'),
      ('7', 'SAULO LAUREDO DOS SANTOS'),
      ('8', 'JEAN ROBERTO LINARES'),
      ('9', 'ADAO GALIANO'),
      ('10', 'EDMILSON TEIXEIRA DA SILVA'),
      ('11', 'NILTON NASCIMENTO BEZERRA'),
      ('12', 'LEANDRO JESUS DA SILVA'),
      ('13', 'PEDRO ALVES DOMINGUES'),
      ('14', 'LUIZ DAMASO'),
      ('15', 'AGNALDO MATEUS DA SILVA'),
      ('16', 'MARCO HENRIQUE STIPP DOS SANTOS CAMARGO'),
      ('17', 'GUSTAVO HENRIQUE DE ASSIS ALVES'),
      ('18', 'HELIO BERICA SOBRINHO'),
      ('19', 'ARNALDO NOGUEIRA'),
      ('20', 'JOELSON APARECIDO RODRIGUES'),
      ('21', 'RUBENS BARBOSA DOS SANTOS'),
      ('22', 'ANTONIO LIBRALAO'),
      ('23', 'ARMANDO FERNANDES DE MELO'),
      ('24', 'MARCIO LUIS DE ASSIS'),
      ('25', 'JOAO BATISTA DA SILVA'),
      ('26', 'ADRIANO MIQUILIN'),
      ('27', 'WILSON ALVES DA SILVA'),
      ('28', 'CLAUDECIR DOS SANTOS'),
      ('29', 'JOSIAS ALES DE OLIVEIRA'),
      ('30', 'VALDECIR DOS SANTOS'),
      ('31', 'DAIR FERREIRA GALVAO'),
      ('32', 'CLEITON SOUZA SIMOES'),
      ('33', 'NELSON DE OLIVEIRA'),
      ('34', 'ALEXANDRE DA CRUZ CORREA'),
      ('35', 'MARCELO DE LIMA BARROS'),
      ('36', 'LUCAS DA SILVA ALVES'),
      ('37', 'VALTEIR LOPES DA SILVA'),
      ('38', 'ADRIANO SOARES'),
      ('39', 'ADSON JOSE MORAES'),
      ('40', 'CARLOS HENRIQUE REIS DA SILVA'),
      ('42', 'ADILSON BUENO'),
      ('43', 'DANIEL MENDES RIBEIRO'),
      ('44', 'EVERTON BELAFRONTE FRAGA'),
      ('45', 'HEMERSON DA SILVA'),
      ('46', 'OSVALDO JOAQUIM AMORIM'),
      ('47', 'RAFAEL DE OLIVIERA RODRIGUES'),
      ('48', 'MATHEUS ALVES DA CRUZ'),
      ('49', 'IVANEI FERNANDES FARIAS'),
      ('50', 'JOSE APARECIDO LOURENCO'),
      ('51', 'LUCAS HENRIQUE DOS SANTOS'),
      ('52', 'ROADSON JUNIOR DUARTE'),
      ('53', 'DERICK MACHADO DA SILVA'),
      ('54', 'ROSINEI DE OLIVEIRA'),
      ('55', 'WALLACE PEREIRA'),
      ('56', 'RODOLFO REICHE'),
      ('57', 'ANDREY HENRIQUE DA SILVA SANTOS'),
      ('58', 'NELSON QUILIS CANDIDO'),
      ('59', 'NATANAEL RODRIGUES DE SOUZA'),
      ('60', 'EDIMILSON DE OLIVEIRA XAVIER'),
      ('61', 'ISRAEL SANDER'),
      ('62', 'ALESSANDRO DOS SANTOS')
    ) AS t(codigo, nome)
  LOOP
    SELECT id INTO v_arm_id FROM arm_armarios
    WHERE codigo = rec.codigo AND bloco_id = v_bloco_mas LIMIT 1;

    IF v_arm_id IS NULL THEN CONTINUE; END IF;

    SELECT id INTO v_func_id FROM rh_funcionarios
    WHERE UPPER(TRANSLATE(nome_completo, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇáéíóúàèìòùâêîôûãõç', 'AEIOUAEIOUAEIOUAOCaeiouaeiouaeiouaoc')) LIKE '%' || UPPER(LEFT(rec.nome, 15)) || '%'
      AND status IN ('ativo', 'ferias', 'experiencia')
    LIMIT 1;

    IF v_func_id IS NOT NULL THEN
      IF NOT EXISTS (SELECT 1 FROM arm_ocupacoes WHERE armario_id = v_arm_id AND data_devolucao IS NULL) THEN
        INSERT INTO arm_ocupacoes (armario_id, funcionario_id, data_atribuicao, chave_entregue)
        VALUES (v_arm_id, v_func_id, CURRENT_DATE, true);
      END IF;
      UPDATE arm_armarios SET status = 'ocupado' WHERE id = v_arm_id;
    END IF;
  END LOOP;
END $$;

-- 6. VERIFICAR RESULTADO
SELECT
  b.tipo,
  COUNT(*) AS total,
  SUM(CASE WHEN a.status = 'ocupado' THEN 1 ELSE 0 END) AS ocupados,
  SUM(CASE WHEN a.status = 'disponivel' THEN 1 ELSE 0 END) AS vagos
FROM arm_armarios a
JOIN arm_blocos b ON a.bloco_id = b.id
GROUP BY b.tipo
ORDER BY b.tipo;
