-- ══════════════════════════════════════════════════════════════════════════════
-- CADASTRO MESTRE DE FORNECEDORES — DE-PARA UNIFICADO
-- Migration: migration_fornecedores_depara.sql
-- Data: 2026-03-18
--
-- Tabela cq_fornecedores: cadastro unificado com código ATAK, nome oficial,
-- apelido e aliases (nomes variantes usados em planilhas).
-- ══════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. TABELA PRINCIPAL (adicionar colunas faltantes se tabela ja existe do 01)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cq_fornecedores (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  codigo_atak     TEXT UNIQUE,
  nome            TEXT NOT NULL,
  nome_normalizado TEXT NOT NULL,
  nome_oficial    TEXT,
  apelido         TEXT,
  aliases         TEXT[] NOT NULL DEFAULT '{}',
  documento       TEXT,
  ativo           BOOLEAN NOT NULL DEFAULT true,
  cidade          TEXT,
  estado          TEXT,
  observacoes     TEXT,
  criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
  atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Adicionar colunas do de-para se tabela ja existia sem elas
ALTER TABLE cq_fornecedores ADD COLUMN IF NOT EXISTS nome_oficial TEXT;
ALTER TABLE cq_fornecedores ADD COLUMN IF NOT EXISTS apelido TEXT;
ALTER TABLE cq_fornecedores ADD COLUMN IF NOT EXISTS aliases TEXT[] NOT NULL DEFAULT '{}';

-- Remover unique constraint de nome_normalizado (fornecedores diferentes podem ter mesmo nome)
-- codigo_atak UNIQUE ja garante unicidade real
DROP INDEX IF EXISTS uq_cq_fornecedores_nome_normalizado;

CREATE INDEX IF NOT EXISTS idx_cq_fornecedores_codigo ON cq_fornecedores(codigo_atak);
CREATE INDEX IF NOT EXISTS idx_cq_fornecedores_apelido ON cq_fornecedores(apelido);
CREATE INDEX IF NOT EXISTS idx_cq_fornecedores_aliases ON cq_fornecedores USING GIN(aliases);

COMMENT ON TABLE cq_fornecedores IS 'Cadastro mestre unificado de fornecedores (frigoríficos). DE-PARA planilha → ATAK.';
COMMENT ON COLUMN cq_fornecedores.codigo_atak IS 'Código do fornecedor no sistema ATAK (ex: 60084)';
COMMENT ON COLUMN cq_fornecedores.nome_oficial IS 'Razão social ou nome oficial no ATAK';
COMMENT ON COLUMN cq_fornecedores.apelido IS 'Nome curto usado no dia-a-dia (ex: GOLD CARNES)';
COMMENT ON COLUMN cq_fornecedores.aliases IS 'Array com todos os nomes variantes encontrados em planilhas';

-- ════════════════════════════════════════════════════════════════════════════
-- 2. RLS
-- ════════════════════════════════════════════════════════════════════════════

ALTER TABLE cq_fornecedores ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS cq_fornecedores_select ON cq_fornecedores;
CREATE POLICY cq_fornecedores_select ON cq_fornecedores
  FOR SELECT USING (true);

DROP POLICY IF EXISTS cq_fornecedores_insert ON cq_fornecedores;
CREATE POLICY cq_fornecedores_insert ON cq_fornecedores
  FOR INSERT TO authenticated WITH CHECK (true);

DROP POLICY IF EXISTS cq_fornecedores_update ON cq_fornecedores;
CREATE POLICY cq_fornecedores_update ON cq_fornecedores
  FOR UPDATE TO authenticated USING (true) WITH CHECK (true);

-- ════════════════════════════════════════════════════════════════════════════
-- 3. FUNÇÃO: NORMALIZAR FORNECEDOR
-- Recebe um nome qualquer e retorna o fornecedor normalizado
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_normalizar_fornecedor(p_nome TEXT)
RETURNS TABLE (
  codigo_atak TEXT,
  nome_oficial TEXT,
  apelido TEXT,
  encontrado BOOLEAN
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_nome TEXT;
  v_rec RECORD;
BEGIN
  IF p_nome IS NULL OR trim(p_nome) = '' THEN
    RETURN QUERY SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT, false;
    RETURN;
  END IF;

  v_nome := upper(trim(regexp_replace(p_nome, '\s+', ' ', 'g')));

  -- 1. Busca exata por apelido
  SELECT f.codigo_atak, f.nome_oficial, f.apelido INTO v_rec
  FROM cq_fornecedores f
  WHERE upper(f.apelido) = v_nome AND f.ativo = true
  LIMIT 1;

  IF FOUND THEN
    RETURN QUERY SELECT v_rec.codigo_atak, v_rec.nome_oficial, v_rec.apelido, true;
    RETURN;
  END IF;

  -- 2. Busca por alias
  SELECT f.codigo_atak, f.nome_oficial, f.apelido INTO v_rec
  FROM cq_fornecedores f
  WHERE v_nome = ANY(f.aliases) AND f.ativo = true
  LIMIT 1;

  IF FOUND THEN
    RETURN QUERY SELECT v_rec.codigo_atak, v_rec.nome_oficial, v_rec.apelido, true;
    RETURN;
  END IF;

  -- 3. Busca por código
  SELECT f.codigo_atak, f.nome_oficial, f.apelido INTO v_rec
  FROM cq_fornecedores f
  WHERE f.codigo_atak = v_nome AND f.ativo = true
  LIMIT 1;

  IF FOUND THEN
    RETURN QUERY SELECT v_rec.codigo_atak, v_rec.nome_oficial, v_rec.apelido, true;
    RETURN;
  END IF;

  -- 4. Busca parcial no nome oficial
  SELECT f.codigo_atak, f.nome_oficial, f.apelido INTO v_rec
  FROM cq_fornecedores f
  WHERE upper(f.nome_oficial) LIKE '%' || v_nome || '%' AND f.ativo = true
  LIMIT 1;

  IF FOUND THEN
    RETURN QUERY SELECT v_rec.codigo_atak, v_rec.nome_oficial, v_rec.apelido, true;
    RETURN;
  END IF;

  -- Não encontrado
  RETURN QUERY SELECT NULL::TEXT, NULL::TEXT, p_nome, false;
END;
$$;

-- ════════════════════════════════════════════════════════════════════════════
-- 4. SEED: INSERIR TODOS OS FORNECEDORES
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO cq_fornecedores (codigo_atak, nome, nome_normalizado, nome_oficial, apelido, aliases) VALUES
('60239', 'COUROS 3G - FAXINAL', 'couros 3g - faxinal', 'COUROS 3G - FAXINAL', 'COUROS FAXINAL', ARRAY['3G']),
('81096', 'ABATEDOURO DE BOVINOS LAGOA GRANDE LTDA', 'abatedouro de bovinos lagoa grande ltda', 'ABATEDOURO DE BOVINOS LAGOA GRANDE LTDA', 'ABLG - ARAUCARIA - PR', ARRAY['ABLG']),
('50052', 'DISTRIB CARNES DERIVADOS ALEXANDRE LTDA', 'distrib carnes derivados alexandre ltda', 'DISTRIB CARNES DERIVADOS ALEXANDRE LTDA', 'DISTRIB CARNES DERIVADOS ALEXANDRE LTDA', ARRAY['ALEXANDRE']),
('60285', 'FRIGORIFICO ALIANÇA EIRELI', 'frigorifico aliança eireli', 'FRIGORIFICO ALIANÇA EIRELI', 'ALIANÇA', ARRAY['ALIANCA','ALIANÇA']),
('60013', 'ANGUS CARNES LTDA', 'angus carnes ltda', 'ANGUS CARNES LTDA', 'ANGUS CARNES - WESTFALIA - RS', ARRAY['ANGUS']),
('60052', 'MATADOURO MUNICIPAL DE ARABUTÃ', 'matadouro municipal de arabutã', 'MATADOURO MUNICIPAL DE ARABUTÃ', 'ARABUTA', ARRAY['ARABUTA']),
('60092', 'ASSOCIACAO DOS ACOUGUEIROS DE CIANORTE - ASAC', 'associacao dos acougueiros de cianorte - asac', 'ASSOCIACAO DOS ACOUGUEIROS DE CIANORTE - ASAC', 'ASAC - CIANORTE', ARRAY['ASSOCIAÇÃO','ASSOCIAÇÃO CIANORTE','ASAC']),
('60009', 'FRIGOBECKER LTDA', 'frigobecker ltda', 'FRIGOBECKER LTDA', 'FRIGOBECKER', ARRAY['BECKER','FRIGOBECKER']),
('60352', 'BERRANTE ALIMENTOS LTDA', 'berrante alimentos ltda', 'BERRANTE ALIMENTOS LTDA', 'BERRANTE ALIMENTOS LTDA', ARRAY['BERRANTE']),
('60318', 'FRIGORIFICO BOI DOURADO', 'frigorifico boi dourado', 'FRIGORIFICO BOI DOURADO', 'BOI DOURADO', ARRAY['BO DOURADO','BOI DOURADO']),
('60027', 'JOSÉ FERMINO MAGALHÃES ME', 'josé fermino magalhães me', 'JOSÉ FERMINO MAGALHÃES ME', 'JOSÉ FERMINO MAGALHÃES ME', ARRAY['BOI GORDO']),
('60342', 'BOI NOBRE ALIMENTOS LTDA', 'boi nobre alimentos ltda', 'BOI NOBRE ALIMENTOS LTDA', 'BOI NOBRE ALIMENTOS LTDA', ARRAY['BOI NOBRE','NOBRE']),
('60262', 'FRIGORIFICO BOI SUL LTDA', 'frigorifico boi sul ltda', 'FRIGORIFICO BOI SUL LTDA', 'BOI SUL', ARRAY['BOI SUL']),
('60054', 'COOPERATIVA AGROPECUARIA BONSUCESSO', 'cooperativa agropecuaria bonsucesso', 'COOPERATIVA AGROPECUARIA BONSUCESSO', 'COOPERATIVA AGROPECUARIA BONSUCESSO', ARRAY['BOM SUCESSO']),
('60113', 'BOSSONI E BOSSONI LTDA', 'bossoni e bossoni ltda', 'BOSSONI E BOSSONI LTDA', 'BOSSONI - LUPERCIO', ARRAY['BOSSONI']),
('60133', 'CALIBRE DISTRIBUIDORA DE CARNES LTDA', 'calibre distribuidora de carnes ltda', 'CALIBRE DISTRIBUIDORA DE CARNES LTDA', 'CALIBRE DISTRIBUIDORA - FLORAI', ARRAY['CALIBRE']),
('60163', 'FRIGORIFICO CAMPO DO GADO AGROINDUSTRIA LTDA', 'frigorifico campo do gado agroindustria ltda', 'FRIGORIFICO CAMPO DO GADO AGROINDUSTRIA LTDA', 'FRIGORIFICO CAMPO DO GADO', ARRAY['CAMPO DO GADO']),
('60108', 'ABATEDOURO CAPRINI LTDA', 'abatedouro caprini ltda', 'ABATEDOURO CAPRINI LTDA', 'CAPRINI', ARRAY['CAPRINI']),
('60308', 'FRIGORIFICO CARNE NOBRE LTDA', 'frigorifico carne nobre ltda', 'FRIGORIFICO CARNE NOBRE LTDA', 'CARNE NOBRE', ARRAY['CARNE NOBRE']),
('60017', 'MARCIO LOPES CAVALCANTE', 'marcio lopes cavalcante', 'MARCIO LOPES CAVALCANTE', 'CAVALCANTE', ARRAY['CAVALCANTE','MARCIO CALVACANTE','MARCIO CAVALCANTE']),
('60111', 'FRIGOCENTER FRIGORIFICO LTDA', 'frigocenter frigorifico ltda', 'FRIGOCENTER FRIGORIFICO LTDA', 'FRIGOCENTER', ARRAY['CENTER']),
('50047', 'FRIGORIFICO CHAPARRAL LTDA', 'frigorifico chaparral ltda', 'FRIGORIFICO CHAPARRAL LTDA', 'FRIGORIFICO CHAPARRAL LTDA', ARRAY['CHAPARRAL']),
('60012', 'FRIGOLIDER LTDA', 'frigolider ltda', 'FRIGOLIDER LTDA', 'FRIGOLIDER - COLIDER - MT', ARRAY['COLIDER']),
('60043', 'FRIGONESI FRIGORIFICO LTDA', 'frigonesi frigorifico ltda', 'FRIGONESI FRIGORIFICO LTDA', 'FRIGONESI', ARRAY['COMINESI','NESI']),
('60187', 'FRIGOMATA LTDA - ME', 'frigomata ltda - me', 'FRIGOMATA LTDA - ME', 'FRIGOMATA', ARRAY['DA MATA']),
('60004', 'FRIGODASKO LTDA', 'frigodasko ltda', 'FRIGODASKO LTDA', 'FRIGODASKO', ARRAY['DASKO']),
('60310', 'FRIGODIA FRIGORIFICA DIA LTDA', 'frigodia frigorifica dia ltda', 'FRIGODIA FRIGORIFICA DIA LTDA', 'FRIGODIA', ARRAY['DIA','FRIGODIA']),
('50061', 'DICAVEL DISTRIBUIDORA DE CARNES LTDA', 'dicavel distribuidora de carnes ltda', 'DICAVEL DISTRIBUIDORA DE CARNES LTDA', 'DICAVEL', ARRAY['DISTRIBUIDORA','DICAVEL']),
('60270', 'FRIGORIFICO D'' MATTA COMERCIO E INDUSTRIA LTDA', 'frigorifico d'''' matta comercio e industria ltda', 'FRIGORIFICO D'' MATTA COMERCIO E INDUSTRIA LTDA', 'D'' MATTA', ARRAY['D''MATTA','D MATTA']),
('60341', 'BOI BARÃO ABATEDOURO LTDA', 'boi barão abatedouro ltda', 'BOI BARÃO ABATEDOURO LTDA', 'BOI BARÃO', ARRAY['EJ']),
('60077', 'ENTREPOSTO SAO JOSE LTDA', 'entreposto sao jose ltda', 'ENTREPOSTO SAO JOSE LTDA', 'CAJOBI', ARRAY['ENTREPOSTO','ENTREPOSTO SÃO JOSÉ','SÃO JOSE','SAO JOSE','SÃO JOSÉ','SAO JOSÉ','SÃO JOSÉ CAJOBI']),
('60124', 'FRIGORIFICO EP LTDA', 'frigorifico ep ltda', 'FRIGORIFICO EP LTDA', 'FRIGORIFICO EP', ARRAY['EP']),
('60276', 'MATADOURO ESPERANÇA LTDA', 'matadouro esperança ltda', 'MATADOURO ESPERANÇA LTDA', 'ESPERANCA', ARRAY['ESPERANCA','ESPERANÇA']),
('60337', 'CINCO ESTRELAS ARTIGOS DE COURO LTDA', 'cinco estrelas artigos de couro ltda', 'CINCO ESTRELAS ARTIGOS DE COURO LTDA', 'CINCO ESTRELAS', ARRAY['ESTRELA']),
('60351', 'FRIGORIFICO ESTRELA DO SUL LTDA', 'frigorifico estrela do sul ltda', 'FRIGORIFICO ESTRELA DO SUL LTDA', 'ESTRELA DO SUL', ARRAY['ESTRELA DO SUL']),
('60028', 'FRIGOEURO LTDA', 'frigoeuro ltda', 'FRIGOEURO LTDA', 'FRIGOEURO', ARRAY['EURO']),
('60350', 'FRIGORIFICO REGIONAL PIEMONTE DA CHAPADA', 'frigorifico regional piemonte da chapada', 'FRIGORIFICO REGIONAL PIEMONTE DA CHAPADA', 'FRIGOCESAR', ARRAY['FIROGO CESAR','FRIGOCESAR']),
('60372', 'FRASSUL COMERCIO DE ALIMENTOS LTDA', 'frassul comercio de alimentos ltda', 'FRASSUL COMERCIO DE ALIMENTOS LTDA', 'FRASSUL', ARRAY['FRASSUL']),
('60114', 'FRIBAL FRIGORIFICO BALANCIN LTDA', 'fribal frigorifico balancin ltda', 'FRIBAL FRIGORIFICO BALANCIN LTDA', 'FRIBAL - BALANCIN', ARRAY['FRIBAL']),
('60053', 'FRIBAZ FRIGORIFICO LTDA', 'fribaz frigorifico ltda', 'FRIBAZ FRIGORIFICO LTDA', 'FRIBAZ', ARRAY['FRIBAZ']),
('60381', 'FRIGONOR FRIGORIFICO & COMERCIO LTDA', 'frigonor frigorifico & comercio ltda', 'FRIGONOR FRIGORIFICO & COMERCIO LTDA', 'FRIGONOR', ARRAY['FRIGONOR']),
('60093', 'FRIGORAES AGROINDUSTRIA LTDA', 'frigoraes agroindustria ltda', 'FRIGORAES AGROINDUSTRIA LTDA', 'FRIGORAES', ARRAY['FRIGORAES','FRIGORAIS','RAES']),
('60224', 'FRIGORINI FRIGORIFICA LTDA', 'frigorini frigorifica ltda', 'FRIGORINI FRIGORIFICA LTDA', 'FRIGORINI', ARRAY['FRIGORINI']),
('60003', 'FRIGOTERRA FRIGORIFICO LTDA', 'frigoterra frigorifico ltda', 'FRIGOTERRA FRIGORIFICO LTDA', 'FRIGOTERRA', ARRAY['FRIGOTERRA','TERRA']),
('60235', 'FRIGOVEMA FRIGORIFICO LTDA', 'frigovema frigorifico ltda', 'FRIGOVEMA FRIGORIFICO LTDA', 'IVINHEMA', ARRAY['FRIGOVEMA','VEMA']),
('60137', 'FRIJACUIPE MATADOURO E FRIGORIFICO LTDA', 'frijacuipe matadouro e frigorifico ltda', 'FRIJACUIPE MATADOURO E FRIGORIFICO LTDA', 'FRIJACUIPE', ARRAY['FRIJACUIPE']),
('50090', 'FRIGORIFICO FRILEM LTDA', 'frigorifico frilem ltda', 'FRIGORIFICO FRILEM LTDA', 'FRILEM', ARRAY['FRILEM']),
('60370', 'FRIMATOS AGROINDUSTRIA LTDA', 'frimatos agroindustria ltda', 'FRIMATOS AGROINDUSTRIA LTDA', 'FRIMATOS', ARRAY['FRIMATOS','MATOS']),
('60063', 'FRINORA FRIGORIFICO DO NORTE LTDA', 'frinora frigorifico do norte ltda', 'FRINORA FRIGORIFICO DO NORTE LTDA', 'FRINORA/FRIOURO', ARRAY['FRINORA']),
('60353', 'FRISPAR FRIGORIFICO SUDOESTE DO PARANA LTDA', 'frispar frigorifico sudoeste do parana ltda', 'FRISPAR FRIGORIFICO SUDOESTE DO PARANA LTDA', 'FRISPAR', ARRAY['FRISPAR']),
('60331', 'FRIZONTE - INDUSTRIA DE ALIMENTOS LTDA', 'frizonte - industria de alimentos ltda', 'FRIZONTE - INDUSTRIA DE ALIMENTOS LTDA', 'FRIZONTE', ARRAY['FRIZONTE']),
('60084', 'GOLD CARNES LTDA', 'gold carnes ltda', 'GOLD CARNES LTDA', 'GOLD CARNES', ARRAY['GOLD','GOLD CARNES']),
('60140', 'J GUSMAO & CIA LTDA', 'j gusmao & cia ltda', 'J GUSMAO & CIA LTDA', 'J GUSMAO', ARRAY['GUSMÃO','GUSMAO','J GUSMAO']),
('60361', 'FRIGO HIPER CARNE LTDA', 'frigo hiper carne ltda', 'FRIGO HIPER CARNE LTDA', 'FRIGO HIPER CARNE', ARRAY['HIPER']),
('60362', 'FRIGORIFICO INTER ABATE E COMERCIO DE CARNES', 'frigorifico inter abate e comercio de carnes', 'FRIGORIFICO INTER ABATE E COMERCIO DE CARNES', 'FRIGO INTER ABATE', ARRAY['INTER']),
('60179', 'FRIGORIFICO IPER LTDA', 'frigorifico iper ltda', 'FRIGORIFICO IPER LTDA', 'FRIGORIFICO IPER', ARRAY['IPER']),
('50050', 'ITAJARA COMERCIO DE CARNES LTDA', 'itajara comercio de carnes ltda', 'ITAJARA COMERCIO DE CARNES LTDA', 'ITAJARA', ARRAY['ITAJARA']),
('60334', 'JACOB & BATTISTI LTDA', 'jacob & battisti ltda', 'JACOB & BATTISTI LTDA', 'JACOB & BATTISTI', ARRAY['JACOB']),
('60344', 'FRIGOMAIS LTDA', 'frigomais ltda', 'FRIGOMAIS LTDA', 'FRIGOMAIS', ARRAY['JAGUAR','JARAGUA']),
('96041', 'JEAN JORGE STECKER', 'jean jorge stecker', 'JEAN JORGE STECKER', 'JEAN', ARRAY['JEAN']),
('60148', 'JEAN CARLOS DOS SANTOS - CARNES - ME', 'jean carlos dos santos - carnes - me', 'JEAN CARLOS DOS SANTOS - CARNES - ME', 'JEAN CARLOS DOS SANTOS', ARRAY['JEAN CARLOS','ME']),
('60260', 'J P S DISTRIBUIDORA DE CARNES LTDA', 'j p s distribuidora de carnes ltda', 'J P S DISTRIBUIDORA DE CARNES LTDA', 'J P S / GRAND BULL', ARRAY['JPS']),
('50058', 'J.R. LEME', 'j.r. leme', 'J.R. LEME', 'J.R. LEME', ARRAY['JR']),
('50033', 'J S FERREIRA FRIGORIFICO LTDA', 'j s ferreira frigorifico ltda', 'J S FERREIRA FRIGORIFICO LTDA', 'J S FERREIRA', ARRAY['JS']),
('50092', 'FRIGORIFICO KLEIN LTDA', 'frigorifico klein ltda', 'FRIGORIFICO KLEIN LTDA', 'FRIGORIFICO KLEIN', ARRAY['KLEIN']),
('60273', 'KM DISTRIBUIDORA DE CARNES LTDA', 'km distribuidora de carnes ltda', 'KM DISTRIBUIDORA DE CARNES LTDA', 'KM DISTRIBUIDORA', ARRAY['KM']),
('60112', 'FRIGORIFICO LEME LTDA', 'frigorifico leme ltda', 'FRIGORIFICO LEME LTDA', 'JOSÉ RONALDO', ARRAY['LEME']),
('60168', 'DISCA LUZ DISTRIBUIDORA DE CARNES EIRELI', 'disca luz distribuidora de carnes eireli', 'DISCA LUZ DISTRIBUIDORA DE CARNES EIRELI', 'DISCA LUZ', ARRAY['LUZ']),
('60097', 'FRIGORIFICO LUZENCE LTDA', 'frigorifico luzence ltda', 'FRIGORIFICO LUZENCE LTDA', 'FRIGORIFICO LUZENCE', ARRAY['LUZENSE']),
('60055', 'FRIGOESTE LTDA', 'frigoeste ltda', 'FRIGOESTE LTDA', 'MAGIA - FRIOESTE', ARRAY['MAGIA']),
('60010', 'FRIGORIFICO MAGISTRAL LTDA', 'frigorifico magistral ltda', 'FRIGORIFICO MAGISTRAL LTDA', 'ORION & MAGISTRAL', ARRAY['MAGISTRAL','ORION']),
('60049', 'MARCELO DOS SANTOS ME', 'marcelo dos santos me', 'MARCELO DOS SANTOS ME', 'FRIGONI/FRIGODINO', ARRAY['MARCELO SANTOS']),
('60120', 'MARCIO LOPES CAVALCANTE', 'marcio lopes cavalcante', 'MARCIO LOPES CAVALCANTE', 'MARCIO LOPES CAVALCANTE', ARRAY['MARCIO']),
('60309', 'FRIGORIFICO MAX LTDA', 'frigorifico max ltda', 'FRIGORIFICO MAX LTDA', 'MAX', ARRAY['MAX']),
('60144', 'MBS FRIGORIFICO LTDA', 'mbs frigorifico ltda', 'MBS FRIGORIFICO LTDA', 'MBS FRIGORIFICO', ARRAY['MBS']),
('50068', 'FRIGORIFICO FRIGOMENDES LTDA', 'frigorifico frigomendes ltda', 'FRIGORIFICO FRIGOMENDES LTDA', 'FRIGOMENDES', ARRAY['MENDES']),
('60121', 'MG COUROS LTDA', 'mg couros ltda', 'MG COUROS LTDA', 'MG COUROS', ARRAY['MG','MG COUROS']),
('60175', 'FRIGORIFICO MODELO LTDA', 'frigorifico modelo ltda', 'FRIGORIFICO MODELO LTDA', 'FRIGORIFICO MODELO', ARRAY['MODELO']),
('60078', 'MONTALFRIG LTDA', 'montalfrig ltda', 'MONTALFRIG LTDA', 'MONTALFRIG', ARRAY['MONTALFRIG']),
('60272', 'FRIGORIFICO MORRO AGUDO', 'frigorifico morro agudo', 'FRIGORIFICO MORRO AGUDO', 'MORRO AGUDO', ARRAY['MORRO AGUDO']),
('60106', 'FRIGORIFICO MOYA LTDA', 'frigorifico moya ltda', 'FRIGORIFICO MOYA LTDA', 'COLORADO', ARRAY['MOYA']),
('60136', 'FRIGORIFICO MUQUEM DO SAO FRANCISCO LTDA', 'frigorifico muquem do sao francisco ltda', 'FRIGORIFICO MUQUEM DO SAO FRANCISCO LTDA', 'MUQUEM', ARRAY['MUQUEM']),
('60116', 'FRIGORIFICO NOSSA SENHORA APARECIDA LTDA', 'frigorifico nossa senhora aparecida ltda', 'FRIGORIFICO NOSSA SENHORA APARECIDA LTDA', 'NOSSA SENHORA APARECIDA', ARRAY['NOSSA SENHORA']),
('60149', 'FRIGORIFICO NOVA ALIANCA LTDA', 'frigorifico nova alianca ltda', 'FRIGORIFICO NOVA ALIANCA LTDA', 'NOVA ALIANCA', ARRAY['NOVA ALIANÇA','NOVA ALIANCA']),
('60037', 'NOVA PRATA FRIGORIFICO LTDA', 'nova prata frigorifico ltda', 'NOVA PRATA FRIGORIFICO LTDA', 'NOVA PRATA', ARRAY['NOVA PRATA']),
('60332', 'MATADOURO O T J LTDA EPP', 'matadouro o t j ltda epp', 'MATADOURO O T J LTDA EPP', 'MATADOURO O T J LTDA EPP', ARRAY['OTJ']),
('60025', 'FRIGORIFICO OURO VERDE LTDA', 'frigorifico ouro verde ltda', 'FRIGORIFICO OURO VERDE LTDA', 'OURO VERDE', ARRAY['OURO VERDE']),
('60304', 'FRIGORIFICO PAI NOSSO LTDA', 'frigorifico pai nosso ltda', 'FRIGORIFICO PAI NOSSO LTDA', 'PAI NOSSO', ARRAY['PAI NIOSSO','PAI NOSSO']),
('60320', 'FRIGORIFICO PARACATU', 'frigorifico paracatu', 'FRIGORIFICO PARACATU', 'PARACATU', ARRAY['PARACATU']),
('60377', 'PRO BOI CARNES LTDA', 'pro boi carnes ltda', 'PRO BOI CARNES LTDA', 'PRO BOI CARNES', ARRAY['PRO BOI','PROBOI']),
('60240', 'DISTRIBUIDORA DE CARNES RANK LTDA', 'distribuidora de carnes rank ltda', 'DISTRIBUIDORA DE CARNES RANK LTDA', 'RANK', ARRAY['RANK']),
('60371', 'REAL SUBPRODUTOS LTDA', 'real subprodutos ltda', 'REAL SUBPRODUTOS LTDA', 'REAL SUBPRODUTOS', ARRAY['REAL']),
('50046', 'RECRISUL LTDA', 'recrisul ltda', 'RECRISUL LTDA', 'RECRISUL', ARRAY['RECRISUL']),
('60138', 'FRIGORIFICO REGIONAL DE ALAGOINHAS LTDA', 'frigorifico regional de alagoinhas ltda', 'FRIGORIFICO REGIONAL DE ALAGOINHAS LTDA', 'REGIONAL DE ALAGOINHAS', ARRAY['REGIONAL']),
('60180', 'ABATE E DISTRIBUICAO DE CARNES RODEIO LTDA', 'abate e distribuicao de carnes rodeio ltda', 'ABATE E DISTRIBUICAO DE CARNES RODEIO LTDA', 'RODEIO', ARRAY['RODEIO']),
('60178', 'FRIG ROSO & DALL AGNOL LTDA', 'frig roso & dall agnol ltda', 'FRIG ROSO & DALL AGNOL LTDA', 'ROSO & DALL AGNOL', ARRAY['ROSO']),
('60199', 'ROSSATO COMERCIO DE CARNES LTDA', 'rossato comercio de carnes ltda', 'ROSSATO COMERCIO DE CARNES LTDA', 'ROSSATO', ARRAY['ROSSATO']),
('60135', 'ROSSI FRIGORIFICO LTDA', 'rossi frigorifico ltda', 'ROSSI FRIGORIFICO LTDA', 'ROSSI FRIGORIFICO', ARRAY['ROSSI']),
('60058', 'FRIGORIFICO SALTO VELOSO LTDA', 'frigorifico salto veloso ltda', 'FRIGORIFICO SALTO VELOSO LTDA', 'SALTO VELOSO', ARRAY['SALTO VELOSO','VELOSO']),
('60045', 'FRIGORIFICO SCAPINI LTDA', 'frigorifico scapini ltda', 'FRIGORIFICO SCAPINI LTDA', 'SCAPINI', ARRAY['SCAPINI']),
('60006', 'FRIGOSCHAEDLER LTDA', 'frigoschaedler ltda', 'FRIGOSCHAEDLER LTDA', 'FRIGOSCHAEDLER', ARRAY['SCHAEDLER']),
('60349', 'FRIGOSSERRA LTDA', 'frigosserra ltda', 'FRIGOSSERRA LTDA', 'FRIGOSSERRA', ARRAY['SERRA']),
('60348', 'FRIGORIFICO SERRANO LTDA', 'frigorifico serrano ltda', 'FRIGORIFICO SERRANO LTDA', 'FRIGORIFICO SERRANO', ARRAY['SERRANO']),
('60356', 'COMERCIO DE CARNES SIVIERO LTDA', 'comercio de carnes siviero ltda', 'COMERCIO DE CARNES SIVIERO LTDA', 'SIVIERO', ARRAY['SIVIERO']),
('60110', 'SOMA DISTRIBUIDORA DE CARNES LTDA', 'soma distribuidora de carnes ltda', 'SOMA DISTRIBUIDORA DE CARNES LTDA', 'SOMA - CANAA', ARRAY['SOMA']),
('60095', 'FRIGORIFICO SAO FRANCISCO LTDA', 'frigorifico sao francisco ltda', 'FRIGORIFICO SAO FRANCISCO LTDA', 'SAO FRANCISCO', ARRAY['SÃO FRANCISCO','SAO FRANCISCO']),
('60169', 'MATADOURO SAO GERALDO LTDA', 'matadouro sao geraldo ltda', 'MATADOURO SAO GERALDO LTDA', 'SAO GERALDO', ARRAY['SÃO GERALDO','SAO GERALDO']),
('60297', 'TALISMA CARNES LTDA', 'talisma carnes ltda', 'TALISMA CARNES LTDA', 'TALISMA', ARRAY['TALISMA']),
('60155', 'FRIGORIFICO TMJ LTDA', 'frigorifico tmj ltda', 'FRIGORIFICO TMJ LTDA', 'FRIGORIFICO TMJ', ARRAY['TMJ']),
('60062', 'FRIGOTOTI LTDA', 'frigototi ltda', 'FRIGOTOTI LTDA', 'FRIGOTOTI', ARRAY['TOTI']),
('60382', 'UNIFRIGO FRIGORIFICO LTDA', 'unifrigo frigorifico ltda', 'UNIFRIGO FRIGORIFICO LTDA', 'UNIFRIGO', ARRAY['UNIFRIG']),
('60141', 'VADECAR MATADOURO LTDA', 'vadecar matadouro ltda', 'VADECAR MATADOURO LTDA', 'VADECAR', ARRAY['VADECAR']),
('60322', 'BORRAZOPOLIS DISTRIBUIDORA', 'borrazopolis distribuidora', 'BORRAZOPOLIS DISTRIBUIDORA', 'BORRAZOPOLIS', ARRAY['VALDEIR']),
('60072', 'FRIGORIFICO VALE DO PRATA LTDA', 'frigorifico vale do prata ltda', 'FRIGORIFICO VALE DO PRATA LTDA', 'VALE DO PRATA', ARRAY['VALE DA PRATA','VALE DO PRATA']),
('60125', 'VALE DO BOI ALIMENTOS LTDA', 'vale do boi alimentos ltda', 'VALE DO BOI ALIMENTOS LTDA', 'VALE DO BOI', ARRAY['VALE DO BOI']),
('60094', 'FRIGOVEREDA LTDA', 'frigovereda ltda', 'FRIGOVEREDA LTDA', 'VEREDA', ARRAY['VEREDA']),
('60281', 'FRIGORIFICO VIANNA LTDA', 'frigorifico vianna ltda', 'FRIGORIFICO VIANNA LTDA', 'VIANNA', ARRAY['VIANNA']),
('50025', 'VIDAURRE INDUSTRIA E COMERCIO LTDA', 'vidaurre industria e comercio ltda', 'VIDAURRE INDUSTRIA E COMERCIO LTDA', 'FRIGORIFICO VIDAURRE', ARRAY['VIDAURRE']),
('60096', 'FRIGOVIEIRA LTDA', 'frigovieira ltda', 'FRIGOVIEIRA LTDA', 'RIO DA PRATA', ARRAY['VIEIRA']),
('60046', 'PAULO VIOLA FRIGORIFICO LTDA', 'paulo viola frigorifico ltda', 'PAULO VIOLA FRIGORIFICO LTDA', 'PAULO VIOLA', ARRAY['VIOLA']),
('60339', 'NILSON FERNANDO WOLPI DE OLIVEIRA LTDA', 'nilson fernando wolpi de oliveira ltda', 'NILSON FERNANDO WOLPI DE OLIVEIRA LTDA', 'WOLPI', ARRAY['WOLPI']),
('50051', 'ABATEDOURO WS LTDA ME', 'abatedouro ws ltda me', 'ABATEDOURO WS LTDA ME', 'ABATEDOURO WS', ARRAY['WS'])
ON CONFLICT (codigo_atak) DO UPDATE SET
  nome = EXCLUDED.nome,
  nome_normalizado = EXCLUDED.nome_normalizado,
  nome_oficial = EXCLUDED.nome_oficial,
  apelido = EXCLUDED.apelido,
  aliases = EXCLUDED.aliases,
  atualizado_em = now();

-- ════════════════════════════════════════════════════════════════════════════
-- 5. FUNÇÃO: LISTAR FORNECEDORES (para autocomplete)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION cq_listar_fornecedores(p_termo TEXT DEFAULT NULL)
RETURNS TABLE (
  codigo_atak TEXT,
  nome_oficial TEXT,
  apelido TEXT,
  aliases TEXT[]
)
LANGUAGE sql SECURITY DEFINER STABLE AS $$
  SELECT f.codigo_atak, f.nome_oficial, f.apelido, f.aliases
  FROM cq_fornecedores f
  WHERE f.ativo = true
    AND (
      p_termo IS NULL
      OR upper(f.apelido) LIKE '%' || upper(trim(p_termo)) || '%'
      OR upper(f.nome_oficial) LIKE '%' || upper(trim(p_termo)) || '%'
      OR f.codigo_atak LIKE '%' || trim(p_termo) || '%'
      OR upper(trim(p_termo)) = ANY(f.aliases)
    )
  ORDER BY f.apelido;
$$;

COMMIT;
