-- Importacao de Motoristas RWCAD009 - ATAK
-- Data: 23/03/2026
-- Fonte: RWCAD009-JANAINA.ANACLETO-2026-03-23
-- Total: 72 motoristas
--
-- Executar no Supabase SQL Editor do projeto Classic-ia (CQ)
-- Tabela: cadastros_atak (tipo = 'motorista')

-- Upsert: se o codigo ja existir, atualiza nome/apelido/cpf
INSERT INTO cadastros_atak (codigo, nome, apelido, cnpj, tipo, ativo, sync_origem, atualizado_em)
VALUES
  -- Arapongas
  ('96068', 'CLAUDIO FREZZE', 'CLAUDIO FREZZE', '052.732.548-17', 'motorista', true, 'rwcad009', NOW()),
  -- Arapoti
  ('96007', 'ANDRE MAURICIO GUIOTTI', 'ANDRE MAURICIO GUIOTTI', '034.495.919-83', 'motorista', true, 'rwcad009', NOW()),
  -- Cruzmaltina
  ('96056', 'DIEGO GUSTAVO DA SILVA CADONI', 'DIEGO GUSTAVO', '111.101.149-40', 'motorista', true, 'rwcad009', NOW()),
  ('96025', 'PAULO HENRIQUE SIMOES BAUMANN', 'JILO - PAULO HENRIQUE', '035.295.019-64', 'motorista', true, 'rwcad009', NOW()),
  -- Curitiba
  ('96034', 'JOEL GALVAO FERMINO', 'JOEL GALVAO FERMINO', '035.226.909-07', 'motorista', true, 'rwcad009', NOW()),
  -- Iretama
  ('96027', 'VALMIR ALVES', 'VALMIR ALVES', '019.154.919-32', 'motorista', true, 'rwcad009', NOW()),
  -- Ivaipora
  ('96055', 'ADILSON ARAGAO', 'ADILSON ARAGAO', '883.275.589-00', 'motorista', true, 'rwcad009', NOW()),
  ('96005', 'ALCIDES MENDES DE OLIVEIRA', 'ALCIDES', '756.878.879-20', 'motorista', true, 'rwcad009', NOW()),
  ('96006', 'ALDO HENRIQUE PEREIRA DOS SANTOS', 'ALDO', '107.641.709-45', 'motorista', true, 'rwcad009', NOW()),
  ('96040', 'ALESSANDRO ALVES CRUZ', 'ALESSANDRO ALVES CRUZ', '038.079.339-30', 'motorista', true, 'rwcad009', NOW()),
  ('96058', 'ALEX LANGE GOES', 'ALEX LANGE GOES', '101.116.749-21', 'motorista', true, 'rwcad009', NOW()),
  ('96053', 'ANDERSON DOMINGOS DE OLIVEIRA JUNIOR', 'ANDERSON DOMINGOS DE OLIVEIRA JUNIOR', '085.015.529-09', 'motorista', true, 'rwcad009', NOW()),
  ('96061', 'BRUNO CEZAR FRUTUOSO SINHORETO', 'BRUNO CEZAR FRUTUOSO SINHORETO', '086.315.119-19', 'motorista', true, 'rwcad009', NOW()),
  ('96008', 'CARLOS HENRIQUE OLIVEIRA DA SILVA', 'CARLOS HENRIQUE', '072.771.419-89', 'motorista', true, 'rwcad009', NOW()),
  ('96069', 'CICERO DE ANDRADE', 'CICERO', '749.840.989-53', 'motorista', true, 'rwcad009', NOW()),
  ('96077', 'DANILO ROLDAO BRAZ', 'DANILO ROLDAO BRAZ', '074.635.209-35', 'motorista', true, 'rwcad009', NOW()),
  ('96029', 'DORLY HEZEL', 'DORLY', '396.474.479-49', 'motorista', true, 'rwcad009', NOW()),
  ('96045', 'EDENILSON ANTONIO', 'EDENILSON', '072.433.469-60', 'motorista', true, 'rwcad009', NOW()),
  ('96038', 'GABRIEL BANHETE PEREIRA GONCALVES', 'GABRIEL BANHETE PEREIRA GONCALVES', '481.809.528-19', 'motorista', true, 'rwcad009', NOW()),
  ('96057', 'JACKSON DE PAULA CARIS', 'JACKSON DE PAULA CARIS', '061.255.299-32', 'motorista', true, 'rwcad009', NOW()),
  ('96041', 'JEAN RICARDO ANACLETO PINTO', 'JEAN', '055.393.999-86', 'motorista', true, 'rwcad009', NOW()),
  ('96016', 'JOAO MARCOS DE SENE COSTA', 'JOAO MARCOS SENE - JOIA', '096.991.689-27', 'motorista', true, 'rwcad009', NOW()),
  ('96044', 'LUCIANO MUCIO MACETKO', 'LUCIANO', '058.484.999-00', 'motorista', true, 'rwcad009', NOW()),
  ('96063', 'LUIZ APARECIDO DOS SANTOS', 'LUIZ', '004.602.869-27', 'motorista', true, 'rwcad009', NOW()),
  ('96071', 'MARCELO DE JESUS MOREIRA', 'MARCELO DE JESUS', '054.602.779-22', 'motorista', true, 'rwcad009', NOW()),
  ('96036', 'MARCOS FERNANDES DIAS MORENO', 'MARCOS FERNANDES DIAS MORENO - ZULU', '098.321.219-81', 'motorista', true, 'rwcad009', NOW()),
  ('96075', 'PAULO ROBERTO DA SILVA ALBERTON', 'PAULO ROBERTO DA SILVA ALBERTON', '092.133.529-64', 'motorista', true, 'rwcad009', NOW()),
  ('96037', 'PEDRO DOS PASSOS', 'PEDRO DOS PASSOS', '531.736.519-87', 'motorista', true, 'rwcad009', NOW()),
  ('96066', 'REGINALDO FERNANDES DE SOUZA', 'REGINALDO', '040.501.969-64', 'motorista', true, 'rwcad009', NOW()),
  ('96065', 'ROBERSON NOGUEIRA DE MATTOS', 'ROBERSON NOGUEIRA DE MATTOS', '007.743.769-10', 'motorista', true, 'rwcad009', NOW()),
  ('96051', 'RONALDO DA SILVA DOS SANTOS', 'RONALDO', '061.331.039-06', 'motorista', true, 'rwcad009', NOW()),
  ('96002', 'RUDIER IAN AZEVEDO DA SILVA', 'RUDIER', '080.111.609-07', 'motorista', true, 'rwcad009', NOW()),
  ('96050', 'SUEL DEMIR MEIADO', 'SUEL DEMIR MEIADO', '478.015.499-53', 'motorista', true, 'rwcad009', NOW()),
  ('96049', 'UDSON RAFAEL GOMES DOS PASSOS', 'UDSON', '104.094.529-55', 'motorista', true, 'rwcad009', NOW()),
  -- Jardim Alegre
  ('96070', 'ADEMIR PEDRO GUIMARAES', 'ADEMIR PEDRO', '882.575.259-87', 'motorista', true, 'rwcad009', NOW()),
  ('96004', 'ADILSON LEANDRO', 'ADILSON LEANDRO', '067.177.739-44', 'motorista', true, 'rwcad009', NOW()),
  ('96052', 'AMILTON DIONATAN DO NASCIMENTO', 'AMILTON DIONATAN DO NASCIMENTO', '054.058.489-48', 'motorista', true, 'rwcad009', NOW()),
  ('96033', 'ANTONIO BATISTA DOS SANTOS', 'ANTONIO BATISTA DOS SANTOS', '825.187.699-00', 'motorista', true, 'rwcad009', NOW()),
  ('96076', 'CARLOS DANILO MARQUES', 'CARLOS DANILO MARQUES', '104.313.959-18', 'motorista', true, 'rwcad009', NOW()),
  ('96010', 'CHARLES PEREIRA TAKEDA', 'CHARLES TAKEDA', '070.340.969-70', 'motorista', true, 'rwcad009', NOW()),
  ('96048', 'DANILO ELIAS BISPO DE OLIVEIRA', 'DANILO ELIAS BISPO DE OLIVEIRA', '308.458.858-98', 'motorista', true, 'rwcad009', NOW()),
  ('96062', 'DEVANIR PAVAN', 'DEVANIR', '373.668.409-63', 'motorista', true, 'rwcad009', NOW()),
  ('96067', 'EDUARDO SILVA PINHEIRO', 'EDUARDO SILVA PINHEIRO', '037.400.419-60', 'motorista', true, 'rwcad009', NOW()),
  ('96012', 'ELZIO MENDONCA DA SILVA', 'ELZIO', '177.371.718-94', 'motorista', true, 'rwcad009', NOW()),
  ('96039', 'FELIPE LUAN DA MOTA FITZ', 'FELIPE LUAN DA MOTA FITZ', '101.438.839-25', 'motorista', true, 'rwcad009', NOW()),
  ('96013', 'FERNANDO MARCELINO DOS SANTOS', 'FERNANDO MARCELINO', '134.250.189-65', 'motorista', true, 'rwcad009', NOW()),
  ('96078', 'FRANCISCO RENEU OLIVEIRA SANTOS', 'FRANCISCO RENEU OLIVEIRA SANTOS', '322.124.459-20', 'motorista', true, 'rwcad009', NOW()),
  ('96015', 'IBRAIM HENRIQUE DE CARVALHO PAFUME', 'IBRAIM', '103.530.309-45', 'motorista', true, 'rwcad009', NOW()),
  ('96060', 'JOSE GERALDO DE OLIVEIRA', 'JOSE GERALDO DE OLIVEIRA', '114.702.128-70', 'motorista', true, 'rwcad009', NOW()),
  ('96017', 'JOSE MARIA PONTES', 'JOSE MARIA PONTES', '049.351.409-09', 'motorista', true, 'rwcad009', NOW()),
  ('96018', 'LEANDRO VICENTE LUIZ', 'LEANDRO VICENTE LUIZ', '077.261.239-07', 'motorista', true, 'rwcad009', NOW()),
  ('96019', 'MARCELO APARECIDO FERREIRA', 'MARCELO - MARMITAO', '030.439.529-36', 'motorista', true, 'rwcad009', NOW()),
  ('96020', 'MARIO CESAR OLIVEIRA', 'MARINHO', '832.930.519-87', 'motorista', true, 'rwcad009', NOW()),
  ('96042', 'MATHEUS LATCHUCK FERMINO', 'MATHEUS', '116.213.989-78', 'motorista', true, 'rwcad009', NOW()),
  ('96054', 'NATAN HENRIQUE BARNI', 'NATAN', '098.576.419-89', 'motorista', true, 'rwcad009', NOW()),
  ('96022', 'NIVALDO SPADREZANI', 'NIVALDO SPADREZANI', '748.273.599-20', 'motorista', true, 'rwcad009', NOW()),
  ('96023', 'OSMAEL MONTEIRO', 'OSMAEL MONTEIRO', '009.142.179-92', 'motorista', true, 'rwcad009', NOW()),
  ('96024', 'OSMIR MONTEIRO', 'OSMIR MONTEIRO', '825.178.439-53', 'motorista', true, 'rwcad009', NOW()),
  ('96026', 'ROMILDO GONDI NEVES', 'ROMILDO GONDI NEVES', '882.580.689-20', 'motorista', true, 'rwcad009', NOW()),
  ('96047', 'VALTER VIEL DA SILVA JUNIOR', 'VALTER JUNIOR', '092.289.879-01', 'motorista', true, 'rwcad009', NOW()),
  ('96046', 'WANDERLEY RODRIGUES ZURLO', 'WANDERLEY RODRIGUES ZURLO', '531.521.319-68', 'motorista', true, 'rwcad009', NOW()),
  -- Lunardelli
  ('96072', 'ANDRE LUIZ DE CAMARGO', 'ANDRE LUIZ DE CAMARGO', '071.341.029-98', 'motorista', true, 'rwcad009', NOW()),
  ('96009', 'CASSIANO JOSE DE MELO', 'CASSIANO', '068.459.189-88', 'motorista', true, 'rwcad009', NOW()),
  ('96074', 'JEAN CARLOS DELMASCHIO VAZ', 'JEAN CARLOS DELMASCHIO VAZ', '136.209.719-51', 'motorista', true, 'rwcad009', NOW()),
  -- Mandaguari
  ('96035', 'KENNEDY ANDERSON COLLEONI FRAIMAN', 'KENNEDY ANDERSON COLLEONI FRAIMAN', '108.193.019-57', 'motorista', true, 'rwcad009', NOW()),
  -- Manoel Ribas
  ('96073', 'CRISTIANO MARANGONI DAUFEMBACH', 'CRISTIANO MARANGONI DAUFEMBACH', '060.417.589-22', 'motorista', true, 'rwcad009', NOW()),
  ('96059', 'JEFERSON FONSECA PIMENTEL', 'JEFERSON FONSECA PIMENTEL', '103.768.469-92', 'motorista', true, 'rwcad009', NOW()),
  -- Maringa
  ('96014', 'GILBERTO APARECIDO ROBERTO', 'GILBERTO APARECIDO ROBERTO', '039.728.159-56', 'motorista', true, 'rwcad009', NOW()),
  -- Sao Pedro do Ivai
  ('96032', 'DANIEL BUENO FAGUNDES', 'DANIEL BUENO FAGUNDES', '094.162.179-05', 'motorista', true, 'rwcad009', NOW()),
  -- Sarandi
  ('96003', 'ADILSON DOS SANTOS', 'ADILSON - LEITEIRO', '008.071.699-78', 'motorista', true, 'rwcad009', NOW()),
  -- Triunfo RS
  ('96028', 'FABIANO DOS SANTOS', 'FABIANO - FILIAL - RS', '978.602.390-00', 'motorista', true, 'rwcad009', NOW()),
  -- Novo Hamburgo RS
  ('96011', 'DOUGLAS SCHUCK RELLINGER', 'DOUGLAS - FILIAL RIO GRANDE DO SUL', '011.500.890-08', 'motorista', true, 'rwcad009', NOW()),
  -- Ivaipora SP
  ('96043', 'MARCUS VINICIUS CORADIM AMARAL', 'MARCUS', '087.617.509-48', 'motorista', true, 'rwcad009', NOW()),
  -- Lidianopolis
  ('96064', 'DEIVID APARECIDO FERREIRA', 'DEIVID APARECIDO FERREIRA', '111.718.597-40', 'motorista', true, 'rwcad009', NOW()),
  -- Marialva
  ('96021', 'MAURICIO DA SILVA', 'MAURICIO DA SILVA', '117.417.098-06', 'motorista', true, 'rwcad009', NOW())
ON CONFLICT (codigo) DO UPDATE SET
  nome = EXCLUDED.nome,
  apelido = EXCLUDED.apelido,
  cnpj = EXCLUDED.cnpj,
  tipo = 'motorista',
  ativo = true,
  sync_origem = 'rwcad009',
  atualizado_em = NOW();

-- Verificar resultado
SELECT tipo, COUNT(*) as total, COUNT(*) FILTER (WHERE sync_origem = 'rwcad009') as importados_rwcad009
FROM cadastros_atak
WHERE tipo = 'motorista'
GROUP BY tipo;
