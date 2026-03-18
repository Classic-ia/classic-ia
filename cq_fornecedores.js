/**
 * CQ Fornecedores — Módulo de normalização DE-PARA
 * 
 * Unifica nomes de planilha → código ATAK + nome oficial + apelido.
 * Uso: CQFornecedores.normalizar('GOLD')
 *      → { codigo: '60084', nome_oficial: 'GOLD CARNES LTDA', apelido: 'GOLD CARNES' }
 */

const CQFornecedores = (() => {

  // ── MAPA DE-PARA ───────────────────────────────────────────────
  // Chave: nome da planilha (uppercase, trimmed)
  // Valor: { codigo, nome_oficial, apelido }
  const DEPARA = {
    '3G':                     { codigo: '60239', nome_oficial: 'COUROS 3G - FAXINAL', apelido: 'COUROS FAXINAL' },
    'ABLG':                   { codigo: '81096', nome_oficial: 'ABATEDOURO DE BOVINOS LAGOA GRANDE LTDA', apelido: 'ABLG - ARAUCARIA - PR' },
    'ALEXANDRE':              { codigo: '50052', nome_oficial: 'DISTRIB CARNES DERIVADOS ALEXANDRE LTDA', apelido: 'DISTRIB CARNES DERIVADOS ALEXANDRE LTDA' },
    'ALIANCA':                { codigo: '60285', nome_oficial: 'FRIGORIFICO ALIANÇA EIRELI', apelido: 'ALIANÇA' },
    'ALIANÇA':                { codigo: '60285', nome_oficial: 'FRIGORIFICO ALIANÇA EIRELI', apelido: 'ALIANÇA' },
    'ANGUS':                  { codigo: '60013', nome_oficial: 'ANGUS CARNES LTDA', apelido: 'ANGUS CARNES - WESTFALIA - RS' },
    'ARABUTA':                { codigo: '60052', nome_oficial: 'MATADOURO MUNICIPAL DE ARABUTÃ', apelido: 'ARABUTA' },
    'ASSOCIAÇÃO':             { codigo: '60092', nome_oficial: 'ASSOCIACAO DOS ACOUGUEIROS DE CIANORTE - ASAC', apelido: 'ASAC - CIANORTE' },
    'ASSOCIAÇÃO CIANORTE':    { codigo: '60092', nome_oficial: 'ASSOCIACAO DOS ACOUGUEIROS DE CIANORTE - ASAC', apelido: 'ASAC - CIANORTE' },
    'ASAC':                   { codigo: '60092', nome_oficial: 'ASSOCIACAO DOS ACOUGUEIROS DE CIANORTE - ASAC', apelido: 'ASAC - CIANORTE' },
    'BECKER':                 { codigo: '60009', nome_oficial: 'FRIGOBECKER LTDA', apelido: 'FRIGOBECKER' },
    'BERRANTE':               { codigo: '60352', nome_oficial: 'BERRANTE ALIMENTOS LTDA', apelido: 'BERRANTE ALIMENTOS LTDA' },
    'BO DOURADO':             { codigo: '60318', nome_oficial: 'FRIGORIFICO BOI DOURADO', apelido: 'BOI DOURADO' },
    'BOI DOURADO':            { codigo: '60318', nome_oficial: 'FRIGORIFICO BOI DOURADO', apelido: 'BOI DOURADO' },
    'BOI GORDO':              { codigo: '60027', nome_oficial: 'JOSÉ FERMINO MAGALHÃES ME', apelido: 'JOSÉ FERMINO MAGALHÃES ME' },
    'BOI NOBRE':              { codigo: '60342', nome_oficial: 'BOI NOBRE ALIMENTOS LTDA', apelido: 'BOI NOBRE ALIMENTOS LTDA' },
    'BOI SUL':                { codigo: '60262', nome_oficial: 'FRIGORIFICO BOI SUL LTDA', apelido: 'BOI SUL' },
    'BOM SUCESSO':            { codigo: '60054', nome_oficial: 'COOPERATIVA AGROPECUARIA BONSUCESSO', apelido: 'COOPERATIVA AGROPECUARIA BONSUCESSO' },
    'BOSSONI':                { codigo: '60113', nome_oficial: 'BOSSONI E BOSSONI LTDA', apelido: 'BOSSONI - LUPERCIO' },
    'CALIBRE':                { codigo: '60133', nome_oficial: 'CALIBRE DISTRIBUIDORA DE CARNES LTDA', apelido: 'CALIBRE DISTRIBUIDORA - FLORAI' },
    'CAMPO DO GADO':          { codigo: '60163', nome_oficial: 'FRIGORIFICO CAMPO DO GADO AGROINDUSTRIA LTDA', apelido: 'FRIGORIFICO CAMPO DO GADO' },
    'CAPRINI':                { codigo: '60108', nome_oficial: 'ABATEDOURO CAPRINI LTDA', apelido: 'CAPRINI' },
    'CARNE NOBRE':            { codigo: '60308', nome_oficial: 'FRIGORIFICO CARNE NOBRE LTDA', apelido: 'CARNE NOBRE' },
    'CAVALCANTE':             { codigo: '60017', nome_oficial: 'MARCIO LOPES CAVALCANTE', apelido: 'CAVALCANTE' },
    'CENTER':                 { codigo: '60111', nome_oficial: 'FRIGOCENTER FRIGORIFICO LTDA', apelido: 'FRIGOCENTER' },
    'CHAPARRAL':              { codigo: '50047', nome_oficial: 'FRIGORIFICO CHAPARRAL LTDA', apelido: 'FRIGORIFICO CHAPARRAL LTDA' },
    'COLIDER':                { codigo: '60012', nome_oficial: 'FRIGOLIDER LTDA', apelido: 'FRIGOLIDER - COLIDER - MT' },
    'COMINESI':               { codigo: '60043', nome_oficial: 'FRIGONESI FRIGORIFICO LTDA', apelido: 'FRIGONESI' },
    'DA MATA':                { codigo: '60187', nome_oficial: 'FRIGOMATA LTDA - ME', apelido: 'FRIGOMATA' },
    'DASKO':                  { codigo: '60004', nome_oficial: 'FRIGODASKO LTDA', apelido: 'FRIGODASKO' },
    'DIA':                    { codigo: '60310', nome_oficial: 'FRIGODIA FRIGORIFICA DIA LTDA', apelido: 'FRIGODIA' },
    'DISTRIBUIDORA':          { codigo: '50061', nome_oficial: 'DICAVEL DISTRIBUIDORA DE CARNES LTDA', apelido: 'DICAVEL' },
    'DICAVEL':                { codigo: '50061', nome_oficial: 'DICAVEL DISTRIBUIDORA DE CARNES LTDA', apelido: 'DICAVEL' },
    "D'MATTA":                { codigo: '60270', nome_oficial: "FRIGORIFICO D' MATTA COMERCIO E INDUSTRIA LTDA", apelido: "D' MATTA" },
    'D MATTA':                { codigo: '60270', nome_oficial: "FRIGORIFICO D' MATTA COMERCIO E INDUSTRIA LTDA", apelido: "D' MATTA" },
    'EJ':                     { codigo: '60341', nome_oficial: 'BOI BARÃO ABATEDOURO LTDA', apelido: 'BOI BARÃO' },
    'ENTREPOSTO':             { codigo: '60077', nome_oficial: 'ENTREPOSTO SAO JOSE LTDA', apelido: 'CAJOBI' },
    'ENTREPOSTO SÃO JOSÉ':    { codigo: '60077', nome_oficial: 'ENTREPOSTO SAO JOSE LTDA', apelido: 'CAJOBI' },
    'EP':                     { codigo: '60124', nome_oficial: 'FRIGORIFICO EP LTDA', apelido: 'FRIGORIFICO EP' },
    'ESPERANCA':              { codigo: '60276', nome_oficial: 'MATADOURO ESPERANÇA LTDA', apelido: 'ESPERANCA' },
    'ESPERANÇA':              { codigo: '60276', nome_oficial: 'MATADOURO ESPERANÇA LTDA', apelido: 'ESPERANCA' },
    'ESTRELA':                { codigo: '60337', nome_oficial: 'CINCO ESTRELAS ARTIGOS DE COURO LTDA', apelido: 'CINCO ESTRELAS' },
    'ESTRELA DO SUL':         { codigo: '60351', nome_oficial: 'FRIGORIFICO ESTRELA DO SUL LTDA', apelido: 'ESTRELA DO SUL' },
    'EURO':                   { codigo: '60028', nome_oficial: 'FRIGOEURO LTDA', apelido: 'FRIGOEURO' },
    'FIROGO CESAR':           { codigo: '60350', nome_oficial: 'FRIGORIFICO REGIONAL PIEMONTE DA CHAPADA', apelido: 'FRIGOCESAR' },
    'FRIGOCESAR':             { codigo: '60350', nome_oficial: 'FRIGORIFICO REGIONAL PIEMONTE DA CHAPADA', apelido: 'FRIGOCESAR' },
    'FRASSUL':                { codigo: '60372', nome_oficial: 'FRASSUL COMERCIO DE ALIMENTOS LTDA', apelido: 'FRASSUL' },
    'FRIBAL':                 { codigo: '60114', nome_oficial: 'FRIBAL FRIGORIFICO BALANCIN LTDA', apelido: 'FRIBAL - BALANCIN' },
    'FRIBAZ':                 { codigo: '60053', nome_oficial: 'FRIBAZ FRIGORIFICO LTDA', apelido: 'FRIBAZ' },
    'FRIGOBECKER':            { codigo: '60009', nome_oficial: 'FRIGOBECKER LTDA', apelido: 'FRIGOBECKER' },
    'FRIGODIA':               { codigo: '60310', nome_oficial: 'FRIGODIA FRIGORIFICA DIA LTDA', apelido: 'FRIGODIA' },
    'FRIGONOR':               { codigo: '60381', nome_oficial: 'FRIGONOR FRIGORIFICO & COMERCIO LTDA', apelido: 'FRIGONOR' },
    'FRIGORAES':              { codigo: '60093', nome_oficial: 'FRIGORAES AGROINDUSTRIA LTDA', apelido: 'FRIGORAES' },
    'FRIGORAIS':              { codigo: '60093', nome_oficial: 'FRIGORAES AGROINDUSTRIA LTDA', apelido: 'FRIGORAES' },
    'FRIGORINI':              { codigo: '60224', nome_oficial: 'FRIGORINI FRIGORIFICA LTDA', apelido: 'FRIGORINI' },
    'FRIGOTERRA':             { codigo: '60003', nome_oficial: 'FRIGOTERRA FRIGORIFICO LTDA', apelido: 'FRIGOTERRA' },
    'FRIGOVEMA':              { codigo: '60235', nome_oficial: 'FRIGOVEMA FRIGORIFICO LTDA', apelido: 'IVINHEMA' },
    'FRIJACUIPE':             { codigo: '60137', nome_oficial: 'FRIJACUIPE MATADOURO E FRIGORIFICO LTDA', apelido: 'FRIJACUIPE' },
    'FRILEM':                 { codigo: '50090', nome_oficial: 'FRIGORIFICO FRILEM LTDA', apelido: 'FRILEM' },
    'FRIMATOS':               { codigo: '60370', nome_oficial: 'FRIMATOS AGROINDUSTRIA LTDA', apelido: 'FRIMATOS' },
    'FRINORA':                { codigo: '60063', nome_oficial: 'FRINORA FRIGORIFICO DO NORTE LTDA', apelido: 'FRINORA/FRIOURO' },
    'FRISPAR':                { codigo: '60353', nome_oficial: 'FRISPAR FRIGORIFICO SUDOESTE DO PARANA LTDA', apelido: 'FRISPAR' },
    'FRIZONTE':               { codigo: '60331', nome_oficial: 'FRIZONTE - INDUSTRIA DE ALIMENTOS LTDA', apelido: 'FRIZONTE' },
    'GOLD':                   { codigo: '60084', nome_oficial: 'GOLD CARNES LTDA', apelido: 'GOLD CARNES' },
    'GOLD CARNES':            { codigo: '60084', nome_oficial: 'GOLD CARNES LTDA', apelido: 'GOLD CARNES' },
    'GUSMÃO':                 { codigo: '60140', nome_oficial: 'J GUSMAO & CIA LTDA', apelido: 'J GUSMAO' },
    'GUSMAO':                 { codigo: '60140', nome_oficial: 'J GUSMAO & CIA LTDA', apelido: 'J GUSMAO' },
    'HIPER':                  { codigo: '60361', nome_oficial: 'FRIGO HIPER CARNE LTDA', apelido: 'FRIGO HIPER CARNE' },
    'INTER':                  { codigo: '60362', nome_oficial: 'FRIGORIFICO INTER ABATE E COMERCIO DE CARNES', apelido: 'FRIGO INTER ABATE' },
    'IPER':                   { codigo: '60179', nome_oficial: 'FRIGORIFICO IPER LTDA', apelido: 'FRIGORIFICO IPER' },
    'ITAJARA':                { codigo: '50050', nome_oficial: 'ITAJARA COMERCIO DE CARNES LTDA', apelido: 'ITAJARA' },
    'J GUSMAO':               { codigo: '60140', nome_oficial: 'J GUSMAO & CIA LTDA', apelido: 'J GUSMAO' },
    'JACOB':                  { codigo: '60334', nome_oficial: 'JACOB & BATTISTI LTDA', apelido: 'JACOB & BATTISTI' },
    'JAGUAR':                 { codigo: '60344', nome_oficial: 'JARAGUA ALIMENTOS', apelido: 'JARAGUA ALIMENTOS' },
    'JARAGUA':                { codigo: '60344', nome_oficial: 'FRIGOMAIS LTDA', apelido: 'FRIGOMAIS' },
    'JEAN':                   { codigo: '96041', nome_oficial: 'JEAN JORGE STECKER', apelido: 'JEAN' },
    'JEAN CARLOS':            { codigo: '60148', nome_oficial: 'JEAN CARLOS DOS SANTOS - CARNES - ME', apelido: 'JEAN CARLOS DOS SANTOS' },
    'JPS':                    { codigo: '60260', nome_oficial: 'J P S DISTRIBUIDORA DE CARNES LTDA', apelido: 'J P S / GRAND BULL' },
    'JR':                     { codigo: '50058', nome_oficial: 'J.R. LEME', apelido: 'J.R. LEME' },
    'JS':                     { codigo: '50033', nome_oficial: 'J S FERREIRA FRIGORIFICO LTDA', apelido: 'J S FERREIRA' },
    'KLEIN':                  { codigo: '50092', nome_oficial: 'FRIGORIFICO KLEIN LTDA', apelido: 'FRIGORIFICO KLEIN' },
    'KM':                     { codigo: '60273', nome_oficial: 'KM DISTRIBUIDORA DE CARNES LTDA', apelido: 'KM DISTRIBUIDORA' },
    'LEME':                   { codigo: '60112', nome_oficial: 'FRIGORIFICO LEME LTDA', apelido: 'JOSÉ RONALDO' },
    'LUZ':                    { codigo: '60168', nome_oficial: 'DISCA LUZ DISTRIBUIDORA DE CARNES EIRELI', apelido: 'DISCA LUZ' },
    'LUZENSE':                { codigo: '60097', nome_oficial: 'FRIGORIFICO LUZENCE LTDA', apelido: 'FRIGORIFICO LUZENCE' },
    'MAGIA':                  { codigo: '60055', nome_oficial: 'FRIGOESTE LTDA', apelido: 'MAGIA - FRIOESTE' },
    'MAGISTRAL':              { codigo: '60010', nome_oficial: 'FRIGORIFICO MAGISTRAL LTDA', apelido: 'ORION & MAGISTRAL' },
    'MARCELO SANTOS':         { codigo: '60049', nome_oficial: 'MARCELO DOS SANTOS ME', apelido: 'FRIGONI/FRIGODINO' },
    'MARCIO':                 { codigo: '60120', nome_oficial: 'MARCIO LOPES CAVALCANTE', apelido: 'MARCIO LOPES CAVALCANTE' },
    'MARCIO CALVACANTE':      { codigo: '60017', nome_oficial: 'MARCIO LOPES CAVALCANTE', apelido: 'CAVALCANTE' },
    'MARCIO CAVALCANTE':      { codigo: '60017', nome_oficial: 'MARCIO LOPES CAVALCANTE', apelido: 'CAVALCANTE' },
    'MATOS':                  { codigo: '60370', nome_oficial: 'FRIMATOS AGROINDUSTRIA LTDA', apelido: 'FRIMATOS' },
    'MAX':                    { codigo: '60309', nome_oficial: 'FRIGORIFICO MAX LTDA', apelido: 'MAX' },
    'MBS':                    { codigo: '60144', nome_oficial: 'MBS FRIGORIFICO LTDA', apelido: 'MBS FRIGORIFICO' },
    'ME':                     { codigo: '60148', nome_oficial: 'JEAN CARLOS SANTOS', apelido: 'JEAN CARLOS SANTOS' },
    'MENDES':                 { codigo: '50068', nome_oficial: 'FRIGORIFICO FRIGOMENDES LTDA', apelido: 'FRIGOMENDES' },
    'MG':                     { codigo: '60121', nome_oficial: 'MG COUROS LTDA', apelido: 'MG COUROS' },
    'MG COUROS':              { codigo: '60121', nome_oficial: 'MG COUROS LTDA', apelido: 'MG COUROS' },
    'MODELO':                 { codigo: '60175', nome_oficial: 'FRIGORIFICO MODELO LTDA', apelido: 'FRIGORIFICO MODELO' },
    'MONTALFRIG':             { codigo: '60078', nome_oficial: 'MONTALFRIG LTDA', apelido: 'MONTALFRIG' },
    'MORRO AGUDO':            { codigo: '60272', nome_oficial: 'FRIGORIFICO MORRO AGUDO', apelido: 'MORRO AGUDO' },
    'MOYA':                   { codigo: '60106', nome_oficial: 'FRIGORIFICO MOYA LTDA', apelido: 'COLORADO' },
    'MUQUEM':                 { codigo: '60136', nome_oficial: 'FRIGORIFICO MUQUEM DO SAO FRANCISCO LTDA', apelido: 'MUQUEM' },
    'NESI':                   { codigo: '60043', nome_oficial: 'FRIGONESI FRIGORIFICO LTDA', apelido: 'FRIGONESI' },
    'NOBRE':                  { codigo: '60342', nome_oficial: 'BOI NOBRE ALIMENTOS LTDA', apelido: 'BOI NOBRE' },
    'NOSSA SENHORA':          { codigo: '60116', nome_oficial: 'FRIGORIFICO NOSSA SENHORA APARECIDA LTDA', apelido: 'NOSSA SENHORA APARECIDA' },
    'NOVA ALIANÇA':           { codigo: '60149', nome_oficial: 'FRIGORIFICO NOVA ALIANCA LTDA', apelido: 'NOVA ALIANCA' },
    'NOVA ALIANCA':           { codigo: '60149', nome_oficial: 'FRIGORIFICO NOVA ALIANCA LTDA', apelido: 'NOVA ALIANCA' },
    'NOVA PRATA':             { codigo: '60037', nome_oficial: 'NOVA PRATA FRIGORIFICO LTDA', apelido: 'NOVA PRATA' },
    'ORION':                  { codigo: '60010', nome_oficial: 'FRIGORIFICO MAGISTRAL LTDA', apelido: 'ORION & MAGISTRAL' },
    'OTJ':                    { codigo: '60332', nome_oficial: 'MATADOURO O T J LTDA EPP', apelido: 'MATADOURO O T J LTDA EPP' },
    'OURO VERDE':             { codigo: '60025', nome_oficial: 'FRIGORIFICO OURO VERDE LTDA', apelido: 'OURO VERDE' },
    'PAI NIOSSO':             { codigo: '60304', nome_oficial: 'FRIGORIFICO PAI NOSSO LTDA', apelido: 'PAI NOSSO' },
    'PAI NOSSO':              { codigo: '60304', nome_oficial: 'FRIGORIFICO PAI NOSSO LTDA', apelido: 'PAI NOSSO' },
    'PARACATU':               { codigo: '60320', nome_oficial: 'FRIGORIFICO PARACATU', apelido: 'PARACATU' },
    'PRO BOI':                { codigo: '60377', nome_oficial: 'PRO BOI CARNES LTDA', apelido: 'PRO BOI CARNES' },
    'PROBOI':                 { codigo: '60377', nome_oficial: 'PRO BOI CARNES LTDA', apelido: 'PRO BOI CARNES' },
    'RAES':                   { codigo: '60093', nome_oficial: 'FRIGORAES AGROINDUSTRIA LTDA', apelido: 'FRIGORAES' },
    'RANK':                   { codigo: '60240', nome_oficial: 'DISTRIBUIDORA DE CARNES RANK LTDA', apelido: 'RANK' },
    'REAL':                   { codigo: '60371', nome_oficial: 'REAL SUBPRODUTOS LTDA', apelido: 'REAL SUBPRODUTOS' },
    'RECRISUL':               { codigo: '50046', nome_oficial: 'RECRISUL LTDA', apelido: 'RECRISUL' },
    'REGIONAL':               { codigo: '60138', nome_oficial: 'FRIGORIFICO REGIONAL DE ALAGOINHAS LTDA', apelido: 'REGIONAL DE ALAGOINHAS' },
    'RODEIO':                 { codigo: '60180', nome_oficial: 'ABATE E DISTRIBUICAO DE CARNES RODEIO LTDA', apelido: 'RODEIO' },
    'ROSO':                   { codigo: '60178', nome_oficial: 'FRIG ROSO & DALL AGNOL LTDA', apelido: 'ROSO & DALL AGNOL' },
    'ROSSATO':                { codigo: '60199', nome_oficial: 'ROSSATO COMERCIO DE CARNES LTDA', apelido: 'ROSSATO' },
    'ROSSI':                  { codigo: '60135', nome_oficial: 'ROSSI FRIGORIFICO LTDA', apelido: 'ROSSI FRIGORIFICO' },
    'SALTO VELOSO':           { codigo: '60058', nome_oficial: 'FRIGORIFICO SALTO VELOSO LTDA', apelido: 'SALTO VELOSO' },
    'SCAPINI':                { codigo: '60045', nome_oficial: 'FRIGORIFICO SCAPINI LTDA', apelido: 'SCAPINI' },
    'SCHAEDLER':              { codigo: '60006', nome_oficial: 'FRIGOSCHAEDLER LTDA', apelido: 'FRIGOSCHAEDLER' },
    'SERRA':                  { codigo: '60349', nome_oficial: 'FRIGOSSERRA LTDA', apelido: 'FRIGOSSERRA' },
    'SERRANO':                { codigo: '60348', nome_oficial: 'FRIGORIFICO SERRANO LTDA', apelido: 'FRIGORIFICO SERRANO' },
    'SIVIERO':                { codigo: '60356', nome_oficial: 'COMERCIO DE CARNES SIVIERO LTDA', apelido: 'SIVIERO' },
    'SOMA':                   { codigo: '60110', nome_oficial: 'SOMA DISTRIBUIDORA DE CARNES LTDA', apelido: 'SOMA - CANAA' },
    'SÃO FRANCISCO':          { codigo: '60095', nome_oficial: 'FRIGORIFICO SAO FRANCISCO LTDA', apelido: 'SAO FRANCISCO' },
    'SAO FRANCISCO':          { codigo: '60095', nome_oficial: 'FRIGORIFICO SAO FRANCISCO LTDA', apelido: 'SAO FRANCISCO' },
    'SÃO GERALDO':            { codigo: '60169', nome_oficial: 'MATADOURO SAO GERALDO LTDA', apelido: 'SAO GERALDO' },
    'SAO GERALDO':            { codigo: '60169', nome_oficial: 'MATADOURO SAO GERALDO LTDA', apelido: 'SAO GERALDO' },
    'SÃO JOSE':               { codigo: '60077', nome_oficial: 'ENTREPOSTO DE CARNES E DERIVADOS SÃO JOSE', apelido: 'CAJOBI' },
    'SAO JOSE':               { codigo: '60077', nome_oficial: 'ENTREPOSTO DE CARNES E DERIVADOS SÃO JOSE', apelido: 'CAJOBI' },
    'SÃO JOSÉ':               { codigo: '60077', nome_oficial: 'ENTREPOSTO DE CARNES E DERIVADOS SÃO JOSE', apelido: 'CAJOBI' },
    'SAO JOSÉ':               { codigo: '60077', nome_oficial: 'ENTREPOSTO DE CARNES E DERIVADOS SÃO JOSE', apelido: 'CAJOBI' },
    'SÃO JOSÉ CAJOBI':        { codigo: '60077', nome_oficial: 'ENTREPOSTO DE CARNES E DERIVADOS SÃO JOSE', apelido: 'CAJOBI' },
    'TALISMA':                { codigo: '60297', nome_oficial: 'TALISMA CARNES LTDA', apelido: 'TALISMA' },
    'TERRA':                  { codigo: '60003', nome_oficial: 'FRIGOTERRA FRIGORIFICO LTDA', apelido: 'FRIGOTERRA' },
    'TMJ':                    { codigo: '60155', nome_oficial: 'FRIGORIFICO TMJ LTDA', apelido: 'FRIGORIFICO TMJ' },
    'TOTI':                   { codigo: '60062', nome_oficial: 'FRIGOTOTI LTDA', apelido: 'FRIGOTOTI' },
    'UNIFRIG':                { codigo: '60382', nome_oficial: 'UNIFRIGO FRIGORIFICO LTDA', apelido: 'UNIFRIGO' },
    'VADECAR':                { codigo: '60141', nome_oficial: 'VADECAR MATADOURO LTDA', apelido: 'VADECAR' },
    'VALDEIR':                { codigo: '60322', nome_oficial: 'BORRAZOPOLIS DISTRIBUIDORA', apelido: 'BORRAZOPOLIS' },
    'VALE DA PRATA':          { codigo: '60072', nome_oficial: 'FRIGORIFICO VALE DO PRATA LTDA', apelido: 'VALE DO PRATA' },
    'VALE DO PRATA':          { codigo: '60072', nome_oficial: 'FRIGORIFICO VALE DO PRATA LTDA', apelido: 'VALE DO PRATA' },
    'VALE DO BOI':            { codigo: '60125', nome_oficial: 'VALE DO BOI ALIMENTOS LTDA', apelido: 'VALE DO BOI' },
    'VELOSO':                 { codigo: '60058', nome_oficial: 'FRIGORIFICO SALTO VELOSO LTDA', apelido: 'SALTO VELOSO' },
    'VEMA':                   { codigo: '60235', nome_oficial: 'FRIGOVEMA FRIGORIFICO LTDA', apelido: 'IVINHEMA' },
    'VEREDA':                 { codigo: '60094', nome_oficial: 'FRIGOVEREDA LTDA', apelido: 'VEREDA' },
    'VIANNA':                 { codigo: '60281', nome_oficial: 'FRIGORIFICO VIANNA LTDA', apelido: 'VIANNA' },
    'VIDAURRE':               { codigo: '50025', nome_oficial: 'VIDAURRE INDUSTRIA E COMERCIO LTDA', apelido: 'FRIGORIFICO VIDAURRE' },
    'VIEIRA':                 { codigo: '60096', nome_oficial: 'FRIGOVIEIRA LTDA', apelido: 'RIO DA PRATA' },
    'VIOLA':                  { codigo: '60046', nome_oficial: 'PAULO VIOLA FRIGORIFICO LTDA', apelido: 'PAULO VIOLA' },
    'WOLPI':                  { codigo: '60339', nome_oficial: 'NILSON FERNANDO WOLPI DE OLIVEIRA LTDA', apelido: 'WOLPI' },
    'WS':                     { codigo: '50051', nome_oficial: 'ABATEDOURO WS LTDA ME', apelido: 'ABATEDOURO WS' },
  };

  // ── INDEX POR CÓDIGO ATAK ─────────────────────────────────────
  const POR_CODIGO = {};
  for (const [chave, val] of Object.entries(DEPARA)) {
    if (!POR_CODIGO[val.codigo]) {
      POR_CODIGO[val.codigo] = { ...val, aliases: [chave] };
    } else {
      POR_CODIGO[val.codigo].aliases.push(chave);
    }
  }

  // ── FUNÇÕES PÚBLICAS ──────────────────────────────────────────

  /**
   * Normaliza um nome de fornecedor da planilha para o cadastro ATAK.
   * @param {string} nome - Nome como vem da planilha
   * @returns {{ codigo: string, nome_oficial: string, apelido: string } | null}
   */
  function normalizar(nome) {
    if (!nome) return null;
    const key = nome.toString().trim().toUpperCase()
      .replace(/\s+/g, ' ')
      .replace(/['']/g, "'");
    return DEPARA[key] || null;
  }

  /**
   * Busca por código ATAK.
   * @param {string} codigo
   * @returns {{ codigo: string, nome_oficial: string, apelido: string, aliases: string[] } | null}
   */
  function porCodigo(codigo) {
    if (!codigo) return null;
    return POR_CODIGO[codigo.toString().trim()] || null;
  }

  /**
   * Busca aproximada (contém o texto em qualquer campo).
   * @param {string} termo
   * @returns {Array<{ chave: string, codigo: string, nome_oficial: string, apelido: string }>}
   */
  function buscar(termo) {
    if (!termo) return [];
    const t = termo.toString().trim().toUpperCase();
    const results = [];
    const seen = new Set();

    for (const [chave, val] of Object.entries(DEPARA)) {
      if (seen.has(val.codigo)) continue;
      if (
        chave.includes(t) ||
        val.codigo.includes(t) ||
        val.nome_oficial.toUpperCase().includes(t) ||
        val.apelido.toUpperCase().includes(t)
      ) {
        results.push({ chave, ...val });
        seen.add(val.codigo);
      }
    }
    return results;
  }

  /**
   * Retorna a lista única de fornecedores (sem duplicatas por código).
   * @returns {Array<{ codigo: string, nome_oficial: string, apelido: string, aliases: string[] }>}
   */
  function listarTodos() {
    return Object.values(POR_CODIGO).sort((a, b) => a.apelido.localeCompare(b.apelido));
  }

  /**
   * Normaliza e retorna o apelido para exibição. Se não encontrar, retorna o nome original.
   * @param {string} nome
   * @returns {string}
   */
  function apelidoOuOriginal(nome) {
    const n = normalizar(nome);
    return n ? n.apelido : (nome || '').trim();
  }

  /**
   * Normaliza e retorna o código ATAK. Se não encontrar, retorna null.
   * @param {string} nome
   * @returns {string|null}
   */
  function codigoOuNull(nome) {
    const n = normalizar(nome);
    return n ? n.codigo : null;
  }

  /**
   * Gera array para INSERT no Supabase (tabela cq_fornecedores).
   * @returns {Array<{ codigo_atak: string, nome_oficial: string, apelido: string, aliases: string[] }>}
   */
  function paraInsertSupabase() {
    return Object.entries(POR_CODIGO).map(([codigo, val]) => ({
      codigo_atak: codigo,
      nome_oficial: val.nome_oficial,
      apelido: val.apelido,
      aliases: val.aliases
    }));
  }

  // ── STATS ─────────────────────────────────────────────────────
  const totalAliases = Object.keys(DEPARA).length;
  const totalFornecedores = Object.keys(POR_CODIGO).length;

  return {
    normalizar,
    porCodigo,
    buscar,
    listarTodos,
    apelidoOuOriginal,
    codigoOuNull,
    paraInsertSupabase,
    DEPARA,
    POR_CODIGO,
    stats: { totalAliases, totalFornecedores }
  };

})();

// Export for Node.js if needed
if (typeof module !== 'undefined' && module.exports) {
  module.exports = CQFornecedores;
}
