// estoque.js — Classic CQ: Controle de Estoque via XML NFe
// Parser de NFe, API Supabase, Dashboard e Export Excel

const CNPJ_CLASSIC = '08849964000110';

// Normalização de unidades — agrupa variações em uma unidade padrão
function normalizarUnidade(und) {
  const u = (und || '').toUpperCase().trim();
  const mapa = {
    'UN':'UN', 'UND':'UN', 'UNID':'UN', 'UNIDADE':'UN', 'PCS':'UN', 'PÇ':'UN', 'PC':'PC',
    'KG':'KG', 'kg':'KG', 'Kg':'KG', 'KGS':'KG', 'GR':'KG', 'G':'KG', '1':'UN',
    'LT':'LT', 'L':'LT', 'ML':'ML', 'ML.':'ML',
    'M2':'M2', 'M²':'M2',
    'M3':'M3', 'M³':'M3',
    'CX':'CX', 'CAIXA':'CX',
    'SC':'SC', 'SACO':'SC', 'SAC':'SC',
    'PCT':'PCT', 'PACOTE':'PCT',
    'PR':'PR', 'PRS':'PR', 'PAR':'PR',
    'FD':'FD', 'FARDO':'FD',
    'GL':'GL', 'GALAO':'GL',
    'RL':'RL', 'ROLO':'RL',
    'TB':'TB', 'TUBO':'TB',
    'FR':'FR', 'FRASCO':'FR',
    'BB':'BB', 'BOBINA':'BB',
    'MT':'MT', 'METRO':'MT', 'M':'MT',
  };
  return mapa[u] || u;
}
const CNPJS_CLASSIC = {
  '08849964000110': 'MATRIZ',
  '08849964000382': 'FILIAL SP',
  '08849964000200': 'FILIAL RS'
};
const RADICAL_CLASSIC = '08849964';

// ── API Helpers ─────────────────────────────────────────────────
function sbHeaders() {
  const sess = JSON.parse(localStorage.getItem('cq_sess_v2') || '{}');
  return {
    'apikey': SB_KEY,
    'Authorization': `Bearer ${sess.access_token || SB_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
}

async function sbFetch(path, opts = {}) {
  const r = await fetch(`${SB_URL}/rest/v1/${path}`, { headers: sbHeaders(), ...opts });
  if (!r.ok) {
    const txt = await r.text();
    if (r.status === 401 && txt.includes('JWT expired')) {
      localStorage.removeItem('cq_sess_v2');
      window.location.href = 'login.html';
      return;
    }
    throw new Error(`Supabase ${r.status}: ${txt}`);
  }
  if (r.status === 204) return [];
  return r.json();
}

// Fetch paginado para tabelas grandes (busca TODOS os registros)
async function sbFetchAll(path, opts = {}) {
  const pageSize = 1000;
  let all = [], offset = 0, batch;
  do {
    const sep = path.includes('?') ? '&' : '?';
    batch = await sbFetch(`${path}${sep}limit=${pageSize}&offset=${offset}`, opts);
    all = all.concat(batch);
    offset += pageSize;
  } while (batch.length === pageSize);
  return all;
}

// Normalizar campos numericos de movimentacao
const VALOR_MAX_ITEM = 50000000; // R$ 50M — teto por item individual
function normalizarMov(m) {
  m.quantidade = Number(m.quantidade || 0);
  m.valor_unitario = Number(m.valor_unitario || 0);
  m.valor_total = Number(m.valor_total || 0);
  m.icms = Number(m.icms || 0);
  m.pis = Number(m.pis || 0);
  m.cofins = Number(m.cofins || 0);
  m.compra_liquida = Number(m.compra_liquida || 0);

  // Se valor_total excede teto absoluto, recalcular ou zerar
  if (Math.abs(m.valor_total) > VALOR_MAX_ITEM) {
    if (m.quantidade > 0 && m.valor_unitario > 0) {
      const recalc = m.quantidade * m.valor_unitario;
      if (Math.abs(recalc) <= VALOR_MAX_ITEM) {
        console.warn('valor_total corrigido:', m.produto_xml, 'de', m.valor_total, 'para', recalc);
        m.valor_total = recalc;
        m.compra_liquida = recalc - m.icms - m.pis - m.cofins;
      } else {
        console.warn('valor_total e recalc ambos invalidos, zerando:', m.produto_xml, m.valor_total, recalc);
        m.valor_total = 0;
        m.compra_liquida = 0;
      }
    } else {
      console.warn('valor_total invalido sem unit/qtd para recalc, zerando:', m.produto_xml, m.valor_total);
      m.valor_total = 0;
      m.compra_liquida = 0;
    }
  }
  // Mesma validacao para icms, pis, cofins
  if (Math.abs(m.icms) > VALOR_MAX_ITEM) m.icms = 0;
  if (Math.abs(m.pis) > VALOR_MAX_ITEM) m.pis = 0;
  if (Math.abs(m.cofins) > VALOR_MAX_ITEM) m.cofins = 0;
  if (Math.abs(m.compra_liquida) > VALOR_MAX_ITEM) m.compra_liquida = 0;
  return m;
}

// ── Parser XML NFe ──────────────────────────────────────────────
function parseNFeXML(xmlString) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(xmlString, 'text/xml');

  // Detectar namespace
  const nfeProc = doc.querySelector('nfeProc');
  const ns = nfeProc ? nfeProc.namespaceURI : 'http://www.portalfiscal.inf.br/nfe';

  function q(parent, tag) {
    return parent.getElementsByTagNameNS(ns, tag)[0] || parent.querySelector(tag);
  }
  function qAll(parent, tag) {
    const ns_result = parent.getElementsByTagNameNS(ns, tag);
    return ns_result.length ? ns_result : parent.querySelectorAll(tag);
  }
  function txt(parent, tag) {
    const el = q(parent, tag);
    return el ? el.textContent.trim() : '';
  }
  function num(parent, tag) {
    const v = txt(parent, tag);
    return v ? parseFloat(v) : 0;
  }

  // Verificar cancelamento — cStat 101 ou 135 = NF cancelada
  const protNFe = q(doc, 'protNFe');
  if (protNFe) {
    const cStat = txt(protNFe, 'cStat');
    if (cStat === '101' || cStat === '135') return null; // cancelada
  }

  // Verificar se é XML de evento (cancelamento, carta correcao)
  const evento = doc.getElementsByTagNameNS(ns, 'infEvento')[0] || doc.querySelector('infEvento');
  if (evento) return null; // ignorar XMLs de evento

  const infNFe = q(doc, 'infNFe');
  if (!infNFe) return null;

  const ide = q(infNFe, 'ide');
  const emit = q(infNFe, 'emit');
  const dest = q(infNFe, 'dest');

  if (!ide || !emit || !dest) return null;

  // Verificar finalidade — finNFe 4 = devolução
  const finNFe = txt(ide, 'finNFe');

  // Chave de acesso (44 dígitos)
  const chaveNFe = (infNFe.getAttribute('Id') || '').replace('NFe', '');

  const nNF = txt(ide, 'nNF');
  const dhEmi = txt(ide, 'dhEmi');
  const dataEmissao = dhEmi ? dhEmi.substring(0, 10) : '';

  const emitCNPJ = txt(emit, 'CNPJ');
  const emitNome = txt(emit, 'xNome');
  const destCNPJ = txt(dest, 'CNPJ');
  const destNome = txt(dest, 'xNome');

  // Detectar tipo e filial
  const emitEhClassic = emitCNPJ.startsWith(RADICAL_CLASSIC);
  const destEhClassic = destCNPJ.startsWith(RADICAL_CLASSIC);

  let tipo = 'entrada';
  let parceiro = emitNome;
  let parceiroCNPJ = emitCNPJ;
  let filial = CNPJS_CLASSIC[destCNPJ] || 'MATRIZ';

  if (emitEhClassic && !destEhClassic) {
    tipo = 'saida';
    parceiro = destNome;
    parceiroCNPJ = destCNPJ;
    filial = CNPJS_CLASSIC[emitCNPJ] || 'MATRIZ';
  } else if (emitEhClassic && destEhClassic) {
    tipo = 'entrada'; // transferência — será categorizada pelo CFOP
    parceiro = emitNome + ' → ' + destNome;
    parceiroCNPJ = emitCNPJ;
    filial = 'TRANSFERENCIA';
  }

  // Devoluções: finNFe=4 ou natOp contendo "devolucao"
  let isDevolucao = false;
  const natOp = txt(ide, 'natOp').toUpperCase();
  if (finNFe === '4' || natOp.includes('DEVOLUC') || natOp.includes('DEVOLUÇ')) {
    isDevolucao = true;
    // Devolução inverte: se terceiro devolveu para Classic = entrada, se Classic devolveu = saída
    // Mas o efeito no estoque é inverso: devolução de venda = entrada, devolução de compra = saída
  }

  // Itens (det)
  const detEls = qAll(infNFe, 'det');
  const itens = [];

  for (const det of detEls) {
    const nItem = parseInt(det.getAttribute('nItem') || '0');
    const prod = q(det, 'prod');
    if (!prod) continue;

    const imposto = q(det, 'imposto');
    let vICMS = 0, vPIS = 0, vCOFINS = 0;
    let cstICMS = '', cstPIS = '', cstCOFINS = '';
    let aliqICMS = 0, aliqPIS = 0, aliqCOFINS = 0;
    let bcICMS = 0, bcPIS = 0, bcCOFINS = 0;
    let vIPI = 0, cstIPI = '', aliqIPI = 0;
    let vICMSST = 0, vFCP = 0, vII = 0;
    let modBC = '', orig = '';

    if (imposto) {
      // ICMS — pode estar em vários sub-elementos
      const icmsEl = q(imposto, 'ICMS');
      if (icmsEl) {
        for (const child of icmsEl.children) {
          const v = num(child, 'vICMS');
          if (v) { vICMS = v; }
          const cst = txt(child, 'CST') || txt(child, 'CSOSN');
          if (cst) cstICMS = cst;
          const al = num(child, 'pICMS');
          if (al) aliqICMS = al;
          const bc = num(child, 'vBC');
          if (bc) bcICMS = bc;
          const st = num(child, 'vICMSST');
          if (st) vICMSST = st;
          const fc = num(child, 'vFCP');
          if (fc) vFCP = fc;
          const mb = txt(child, 'modBC');
          if (mb) modBC = mb;
          const or = txt(child, 'orig');
          if (or) orig = or;
          if (vICMS || cstICMS) break;
        }
      }
      // PIS
      const pisEl = q(imposto, 'PIS');
      if (pisEl) {
        for (const child of pisEl.children) {
          const v = num(child, 'vPIS');
          if (v) { vPIS = v; }
          const cst = txt(child, 'CST');
          if (cst) cstPIS = cst;
          const al = num(child, 'pPIS');
          if (al) aliqPIS = al;
          const bc = num(child, 'vBC');
          if (bc) bcPIS = bc;
          if (vPIS || cstPIS) break;
        }
      }
      // COFINS
      const cofinsEl = q(imposto, 'COFINS');
      if (cofinsEl) {
        for (const child of cofinsEl.children) {
          const v = num(child, 'vCOFINS');
          if (v) { vCOFINS = v; }
          const cst = txt(child, 'CST');
          if (cst) cstCOFINS = cst;
          const al = num(child, 'pCOFINS');
          if (al) aliqCOFINS = al;
          const bc = num(child, 'vBC');
          if (bc) bcCOFINS = bc;
          if (vCOFINS || cstCOFINS) break;
        }
      }
      // IPI
      const ipiEl = q(imposto, 'IPI');
      if (ipiEl) {
        const ipiTrib = q(ipiEl, 'IPITrib');
        if (ipiTrib) {
          vIPI = num(ipiTrib, 'vIPI');
          cstIPI = txt(ipiTrib, 'CST');
          aliqIPI = num(ipiTrib, 'pIPI');
        } else {
          const ipiNT = q(ipiEl, 'IPINT');
          if (ipiNT) cstIPI = txt(ipiNT, 'CST');
        }
      }
      // II (Imposto de Importação)
      const iiEl = q(imposto, 'II');
      if (iiEl) vII = num(iiEl, 'vII');
    }

    const vProd = num(prod, 'vProd');
    const compraLiquida = vProd - vICMS - vPIS - vCOFINS;

    itens.push({
      chave_nfe: chaveNFe,
      numero_nf: nNF,
      nitem: nItem,
      data_emissao: dataEmissao,
      nat_operacao: natOp,
      tipo,
      fornecedor_cliente: parceiro,
      cnpj: parceiroCNPJ,
      produto_xml: txt(prod, 'xProd'),
      ncm: txt(prod, 'NCM'),
      cfop: txt(prod, 'CFOP'),
      unidade: normalizarUnidade(txt(prod, 'uCom')),
      quantidade: num(prod, 'qCom'),
      valor_unitario: num(prod, 'vUnCom'),
      valor_total: vProd,
      // Impostos detalhados
      icms: vICMS, cst_icms: cstICMS, aliq_icms: aliqICMS, bc_icms: bcICMS,
      icms_st: vICMSST, fcp: vFCP, orig: orig,
      pis: vPIS, cst_pis: cstPIS, aliq_pis: aliqPIS, bc_pis: bcPIS,
      cofins: vCOFINS, cst_cofins: cstCOFINS, aliq_cofins: aliqCOFINS, bc_cofins: bcCOFINS,
      ipi: vIPI, cst_ipi: cstIPI, aliq_ipi: aliqIPI,
      ii: vII,
      compra_liquida: compraLiquida,
      filial,
      categoria_id: null,
      _mapped: false
    });
  }

  return { chaveNFe, nNF, dataEmissao, tipo, parceiro, parceiroCNPJ, filial, itens };
}

// ── Parser CTe XML ──────────────────────────────────────────────
function parseCTeXML(xmlString) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(xmlString, 'text/xml');
  const nsCte = 'http://www.portalfiscal.inf.br/cte';

  function qc(parent, tag) {
    return parent.getElementsByTagNameNS(nsCte, tag)[0] || parent.querySelector(tag);
  }
  function txtc(parent, tag) {
    const el = qc(parent, tag);
    return el ? el.textContent.trim() : '';
  }

  const infCte = qc(doc, 'infCte');
  if (!infCte) return null;

  // Verificar cancelamento
  const protCTe = qc(doc, 'protCTe');
  if (protCTe) {
    const cStat = txtc(protCTe, 'cStat');
    if (cStat === '101' || cStat === '135') return null;
  }

  const ide = qc(infCte, 'ide');
  if (!ide) return null;

  const nCT = txtc(ide, 'nCT');
  const dhEmi = txtc(ide, 'dhEmi');
  const dataEmissao = dhEmi ? dhEmi.substring(0, 10) : '';

  const emit = qc(infCte, 'emit');
  const rem = qc(infCte, 'rem');
  const dest = qc(infCte, 'dest');
  const vPrest = qc(infCte, 'vPrest');

  const emitNome = emit ? txtc(emit, 'xNome') : '';
  const remCNPJ = rem ? txtc(rem, 'CNPJ') : '';
  const remNome = rem ? txtc(rem, 'xNome') : '';
  const destCNPJ = dest ? txtc(dest, 'CNPJ') : '';
  const destNome = dest ? txtc(dest, 'xNome') : '';
  const valorFrete = vPrest ? parseFloat(txtc(vPrest, 'vTPrest')) || 0 : 0;

  // Chave CTe
  const chaveCTe = (infCte.getAttribute('Id') || '').replace('CTe', '');

  // NF referenciada
  const infNFe = qc(infCte, 'infNFe');
  const chaveNFRef = infNFe ? txtc(infNFe, 'chave') : '';

  // ICMS do frete
  let vICMS = 0;
  const impCte = qc(infCte, 'imp');
  if (impCte) {
    const icmsEl = qc(impCte, 'ICMS');
    if (icmsEl) {
      for (const child of icmsEl.children) {
        const v = child.getElementsByTagNameNS(nsCte, 'vICMS')[0];
        if (v && v.textContent) { vICMS = parseFloat(v.textContent); break; }
      }
    }
  }

  // Produto transportado
  const infCarga = qc(infCte, 'infCarga');
  const proPred = infCarga ? txtc(infCarga, 'proPred') : 'FRETE';

  // Classificar pelo fluxo real:
  // No CTe, dest = quem RECEBE a mercadoria
  // Se dest é Classic → mercadoria veio PARA Classic = FRETE COMPRA
  // Se rem é Classic e dest é terceiro → mercadoria SAIU da Classic = FRETE VENDA
  const remEhClassic = remCNPJ.startsWith(RADICAL_CLASSIC);
  const destEhClassic = destCNPJ.startsWith(RADICAL_CLASSIC);

  let categoria, tipo;
  if (destEhClassic) {
    categoria = 'Fretes Compras';
    tipo = 'entrada';
  } else {
    categoria = 'Fretes Vendas';
    tipo = 'saida';
  }

  const item = {
    chave_nfe: chaveCTe,
    numero_nf: nCT,
    nitem: 1,
    data_emissao: dataEmissao,
    tipo,
    fornecedor_cliente: emitNome,
    cnpj: emit ? txtc(emit, 'CNPJ') : '',
    produto_xml: `FRETE ${proPred} — ${remNome.substring(0,20)} → ${destNome.substring(0,20)}` + (chaveNFRef ? ` [NF:${chaveNFRef.substring(22,34)}]` : ''),
    ncm: '', cfop: txtc(ide, 'CFOP'),
    unidade: 'UN',
    quantidade: 1,
    valor_unitario: valorFrete,
    valor_total: valorFrete,
    icms: vICMS, pis: 0, cofins: 0,
    compra_liquida: valorFrete - vICMS,
    filial: remEhClassic ? (CNPJS_CLASSIC[remCNPJ] || 'MATRIZ') : 'MATRIZ',
    categoria_id: null,
    _mapped: false,
    _catNome: categoria,
    _nfRef: chaveNFRef
  };

  return {
    chaveNFe: chaveCTe, nNF: nCT, dataEmissao, tipo,
    parceiro: emitNome, parceiroCNPJ: emit ? txtc(emit, 'CNPJ') : '',
    filial: item.filial, itens: [item], _fileName: '', _isCTe: true
  };
}

// ── Parser NF Stock (XLS/XLSX do sistema contábil) ──────────────
function parseNFStockXLS(workbook) {
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
  if (!rows.length) return [];

  // Detectar header — procurar linha com "Número" ou "Descrição"
  let headerIdx = 0;
  for (let i = 0; i < Math.min(rows.length, 5); i++) {
    const r = rows[i].map(c => String(c || '').toLowerCase());
    if (r.some(c => c.includes('mero') || c.includes('descri'))) { headerIdx = i; break; }
  }

  const results = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r || !r[0]) continue;
    const nf = String(r[0]).trim();
    if (nf.toLowerCase().includes('valor total') || nf.toLowerCase().includes('quantidade')) continue;

    const cnpjFornec = String(r[1] || '').replace(/[.\-\/]/g, '').trim();
    const fornecedor = String(r[2] || '').trim();
    const dataRaw = r[3];
    let dataEmissao = '';
    if (dataRaw) {
      const ds = String(dataRaw).trim();
      if (ds.includes('/')) {
        const [d, m, y] = ds.split('/');
        dataEmissao = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
      } else if (ds.includes('-')) {
        dataEmissao = ds.substring(0, 10);
      }
    }

    const descricao = String(r[7] || '').trim();
    const cfop = String(r[8] || '').trim();
    const ncm = String(r[5] || '').trim();

    let quantidade = 0, valorUnit = 0, valorTotal = 0;
    let icms = 0, pis = 0, cofins = 0;

    try { quantidade = parseFloat(String(r[9]).replace(/\./g,'').replace(',','.')) || 0; } catch {}
    try { valorUnit = parseFloat(String(r[10]).replace(/\./g,'').replace(',','.')) || 0; } catch {}
    try { valorTotal = parseFloat(String(r[19]).replace(/R\$\s*/g,'').replace(/\./g,'').replace(',','.')) || 0; } catch {}
    try { icms = parseFloat(String(r[12]).replace(/R\$\s*/g,'').replace(/\./g,'').replace(',','.')) || 0; } catch {}
    try { pis = parseFloat(String(r[15]).replace(/R\$\s*/g,'').replace(/\./g,'').replace(',','.')) || 0; } catch {}
    try { cofins = parseFloat(String(r[17]).replace(/R\$\s*/g,'').replace(/\./g,'').replace(',','.')) || 0; } catch {}

    // Detectar tipo pelo CNPJ
    const emitEhClassic = cnpjFornec.startsWith(RADICAL_CLASSIC);
    const tipo = 'entrada'; // relatório é de NFs recebidas
    const filial = emitEhClassic ? (CNPJS_CLASSIC[cnpjFornec] || 'TRANSFERENCIA') : 'MATRIZ';

    // Chave fake baseada no numero NF + nItem (não tem chave de acesso no XLS)
    const chaveNfe = `STOCK_${nf}_${cnpjFornec}_${dataEmissao}`;
    const nitem = i - headerIdx;

    const compraLiquida = valorTotal - icms - pis - cofins;

    results.push({
      chave_nfe: chaveNfe,
      numero_nf: nf,
      nitem,
      data_emissao: dataEmissao,
      tipo,
      fornecedor_cliente: fornecedor,
      cnpj: cnpjFornec,
      produto_xml: descricao,
      ncm,
      cfop,
      unidade: normalizarUnidade('KG'), // Stock não traz unidade, default KG
      quantidade,
      valor_unitario: valorUnit,
      valor_total: valorTotal,
      icms, pis, cofins,
      compra_liquida: compraLiquida,
      filial,
      categoria_id: null,
      _mapped: false,
      _source: 'stock'
    });
  }

  return results;
}

// ── Parser CTe XLS (relatório de fretes) ────────────────────────
function parseCTeXLS(workbook) {
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
  if (!rows.length) return [];

  // Detectar header
  let headerIdx = 0;
  for (let i = 0; i < Math.min(rows.length, 5); i++) {
    const r = rows[i].map(c => String(c || '').toLowerCase());
    if (r.some(c => c.includes('modelo') || c.includes('chave'))) { headerIdx = i; break; }
  }

  const results = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r || !r[0]) continue;
    const modelo = String(r[0]).trim();
    if (modelo !== '57' && modelo !== '67') continue; // só CTe

    const nCTe = String(r[2] || '').trim();
    const cnpjTransp = String(r[3] || '').replace(/[.\-\/]/g, '').trim();
    const nomeTransp = String(r[4] || '').trim();
    const cnpjDest = String(r[6] || '').replace(/[.\-\/]/g, '').trim();
    const nomeDest = String(r[7] || '').trim();
    const chave = String(r[8] || '').trim();
    const status = String(r[10] || '').trim();
    const dataRaw = String(r[11] || '').trim();

    // Ignorar cancelados
    if (status.toLowerCase().includes('cancelad')) continue;

    let dataEmissao = '';
    if (dataRaw.includes('/')) {
      const [d, m, y] = dataRaw.split('/');
      dataEmissao = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
    }

    let vlrPrestacao = 0;
    try { vlrPrestacao = parseFloat(String(r[13]).replace(/R\$\s*/g,'').replace(/\./g,'').replace(',','.')) || 0; } catch {}

    // Classificar: se destinatário é Classic → frete compra, senão → frete venda
    const destEhClassic = cnpjDest.startsWith(RADICAL_CLASSIC);
    const categoria = destEhClassic ? 'Fretes Compras' : 'Fretes Vendas';

    results.push({
      chave_nfe: chave || `CTE_${nCTe}_${cnpjTransp}_${dataEmissao}`,
      numero_nf: nCTe,
      nitem: i - headerIdx,
      data_emissao: dataEmissao,
      tipo: destEhClassic ? 'entrada' : 'saida',
      fornecedor_cliente: nomeTransp,
      cnpj: cnpjTransp,
      produto_xml: `FRETE - ${nomeTransp.substring(0,30)}`,
      ncm: '', cfop: '',
      unidade: 'UN',
      quantidade: 1,
      valor_unitario: vlrPrestacao,
      valor_total: vlrPrestacao,
      icms: 0, pis: 0, cofins: 0,
      compra_liquida: vlrPrestacao,
      filial: 'MATRIZ',
      categoria_id: null,
      _mapped: false,
      _source: 'cte',
      _catNome: categoria
    });
  }

  return results;
}

// Detectar tipo de XLS: NF Stock ou CTe
function detectarTipoXLS(workbook) {
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, range: 0 });
  if (!rows.length) return 'unknown';
  const header = rows[0].map(c => String(c || '').toLowerCase());
  if (header.some(c => c.includes('modelo') && c !== 'modelo')) return 'unknown';
  if (header.some(c => c === 'modelo')) return 'cte';
  if (header.some(c => c.includes('mero') || c.includes('descri'))) return 'nfstock';
  return 'unknown';
}

// ── Mapeamento ──────────────────────────────────────────────────
let _categorias = [];
let _mapeamentos = [];

async function carregarCategorias() {
  _categorias = await sbFetch('estoque_categorias?select=*&ativo=eq.true&order=nome.asc');
  return _categorias;
}

async function carregarMapeamentos() {
  _mapeamentos = await sbFetch('estoque_mapeamento?select=*,estoque_categorias(nome)');
  return _mapeamentos;
}

// CFOPs de transferência entre filiais
const CFOPS_TRANSFERENCIA = ['5151','5152','6151','6152','1151','1152','5901','5902','6901','6902'];

function resolverCategoria(produtoXml, ncm, cfop) {
  const prodUp = produtoXml.toUpperCase().trim();
  const descMaps = _mapeamentos.filter(x => x.tipo_chave === 'descricao');

  // 1. Match exato por descrição (PRIORIDADE sobre CFOP)
  let m = descMaps.find(x => x.chave.toUpperCase() === prodUp);
  if (m) return m.categoria_id;

  // 2. Transferência entre filiais — detectar pelo CFOP
  if (cfop && CFOPS_TRANSFERENCIA.includes(cfop)) {
    const catTransf = _categorias.find(c => c.nome === 'TRANSFERENCIA');
    if (catTransf) return catTransf.id;
  }

  // 3. Match parcial — produto COMEÇA com a chave (palavra completa)
  //    Ordena por tamanho decrescente para priorizar matches mais específicos
  const sorted = [...descMaps].sort((a, b) => b.chave.length - a.chave.length);
  for (const s of sorted) {
    const chUp = s.chave.toUpperCase();
    // Produto começa com a chave seguida de espaço, hífen ou fim
    if (prodUp.startsWith(chUp) && (prodUp.length === chUp.length || ' -.,/('.includes(prodUp[chUp.length]))) {
      return s.categoria_id;
    }
  }

  // 3. Match por NCM
  if (ncm) {
    m = _mapeamentos.find(x => x.tipo_chave === 'ncm' && x.chave === ncm);
    if (m) return m.categoria_id;
  }

  return null;
}

function nomeCategoria(catId) {
  const c = _categorias.find(x => x.id === catId);
  return c ? c.nome : '—';
}

// ── CRUD Categorias ─────────────────────────────────────────────
async function criarCategoria(nome, unidade = 'KG') {
  return sbFetch('estoque_categorias', {
    method: 'POST',
    body: JSON.stringify({ nome, unidade })
  });
}

async function atualizarCategoria(id, dados) {
  return sbFetch(`estoque_categorias?id=eq.${id}`, {
    method: 'PATCH',
    body: JSON.stringify(dados)
  });
}

async function excluirCategoria(id) {
  return sbFetch(`estoque_categorias?id=eq.${id}`, { method: 'DELETE' });
}

// ── CRUD Mapeamentos ────────────────────────────────────────────
async function criarMapeamento(chave, tipoChave, categoriaId) {
  return sbFetch('estoque_mapeamento', {
    method: 'POST',
    body: JSON.stringify({ chave, tipo_chave: tipoChave, categoria_id: categoriaId })
  });
}

async function excluirMapeamento(id) {
  return sbFetch(`estoque_mapeamento?id=eq.${id}`, { method: 'DELETE' });
}

// ── Importar Movimentações ──────────────────────────────────────
async function importarItens(itens, onProgress) {
  const validos = itens.filter(i => i.categoria_id);
  if (!validos.length) throw new Error('Nenhum item com categoria mapeada.');

  let inseridos = 0, duplicados = 0, erros = 0;

  // Inserir item a item para controlar duplicatas
  for (let idx = 0; idx < validos.length; idx++) {
    const i = validos[idx];
    const payload = {
      chave_nfe: i.chave_nfe, numero_nf: i.numero_nf, nitem: i.nitem || 1,
      data_emissao: i.data_emissao, tipo: i.tipo, nat_operacao: i.nat_operacao || null,
      fornecedor_cliente: i.fornecedor_cliente, cnpj: i.cnpj,
      categoria_id: i.categoria_id, produto_xml: i.produto_xml,
      ncm: i.ncm, cfop: i.cfop, unidade: i.unidade,
      quantidade: i.quantidade, valor_unitario: i.valor_unitario,
      valor_total: i.valor_total,
      // Impostos detalhados
      icms: i.icms, cst_icms: i.cst_icms || null, aliq_icms: i.aliq_icms || 0, bc_icms: i.bc_icms || 0,
      icms_st: i.icms_st || 0, fcp: i.fcp || 0, orig: i.orig || null,
      pis: i.pis, cst_pis: i.cst_pis || null, aliq_pis: i.aliq_pis || 0, bc_pis: i.bc_pis || 0,
      cofins: i.cofins, cst_cofins: i.cst_cofins || null, aliq_cofins: i.aliq_cofins || 0, bc_cofins: i.bc_cofins || 0,
      ipi: i.ipi || 0, cst_ipi: i.cst_ipi || null, aliq_ipi: i.aliq_ipi || 0,
      ii: i.ii || 0,
      compra_liquida: i.compra_liquida,
      filial: i.filial || 'MATRIZ'
    };
    try {
      const r = await fetch(`${SB_URL}/rest/v1/estoque_movimentacoes`, {
        method: 'POST', headers: { ...sbHeaders(), 'Prefer': 'return=minimal' },
        body: JSON.stringify(payload)
      });
      if (r.ok) { inseridos++; }
      else if (r.status === 409) { duplicados++; }
      else { erros++; }
    } catch { erros++; }

    // Report progress every 10 items
    if (onProgress && (idx % 10 === 0 || idx === validos.length - 1)) {
      onProgress(idx + 1, validos.length, inseridos, duplicados, erros);
    }
  }

  return { inseridos, duplicados, erros, total: validos.length };
}

// ── Consultas ───────────────────────────────────────────────────
async function carregarMovimentacoes(filtros = {}) {
  let qs = 'estoque_movimentacoes?select=*,estoque_categorias(nome)&order=data_emissao.desc,numero_nf.asc';
  if (filtros.tipo) qs += `&tipo=eq.${filtros.tipo}`;
  if (filtros.categoria_id) qs += `&categoria_id=eq.${filtros.categoria_id}`;
  if (filtros.dataInicio) qs += `&data_emissao=gte.${filtros.dataInicio}`;
  if (filtros.dataFim) qs += `&data_emissao=lte.${filtros.dataFim}`;
  if (filtros.fornecedor) qs += `&fornecedor_cliente=ilike.*${filtros.fornecedor}*`;
  const movs = await sbFetchAll(qs);
  return movs.map(normalizarMov);
}

async function carregarSaldos() {
  return sbFetch('vw_estoque_saldo?select=*');
}

async function carregarEvolucaoMensal() {
  return sbFetch('vw_estoque_mensal?select=*&order=mes.asc');
}

async function verificarDuplicata(chaveNfe) {
  const r = await sbFetch(`estoque_movimentacoes?chave_nfe=eq.${chaveNfe}&select=id&limit=1`);
  return r.length > 0;
}

// ── Novas Consultas (fornecedores, preços, comparativo, alertas, duplicatas) ──

async function carregarFornecedores(tipo) {
  let qs = 'vw_estoque_fornecedores?select=*&order=valor_total.desc';
  if (tipo) qs += `&tipo=eq.${tipo}`;
  return sbFetch(qs);
}

async function carregarPrecoMedio(categoriaId) {
  let qs = 'vw_estoque_preco_medio?select=*&order=mes.asc';
  if (categoriaId) qs += `&categoria_id=eq.${categoriaId}`;
  return sbFetch(qs);
}

async function carregarComparativo() {
  return sbFetch('vw_estoque_comparativo?select=*&order=categoria.asc,mes.asc');
}

async function carregarAlertas() {
  return sbFetch('vw_estoque_alertas?select=*');
}

async function carregarDuplicatas() {
  return sbFetch('vw_estoque_duplicatas?select=*&order=data_emissao.desc');
}

async function atualizarEstoqueMinimo(categoriaId, valor) {
  return sbFetch(`estoque_categorias?id=eq.${categoriaId}`, {
    method: 'PATCH',
    body: JSON.stringify({ estoque_minimo: valor })
  });
}

function custoMedioPonderado(movs) {
  let qtdAcum = 0, custoAcum = 0;
  for (const m of movs.sort((a,b) => a.data_emissao.localeCompare(b.data_emissao))) {
    const qty = Number(m.quantidade || 0);
    const val = Number(m.valor_total || 0);
    if (m.tipo === 'entrada' && qty > 0) {
      custoAcum += val;
      qtdAcum += qty;
    } else if (m.tipo === 'saida' && qtdAcum > 0) {
      const cmp = custoAcum / qtdAcum;
      custoAcum -= cmp * Math.min(qty, qtdAcum);
      qtdAcum = Math.max(0, qtdAcum - qty);
    }
  }
  return qtdAcum > 0 ? custoAcum / qtdAcum : 0;
}

// ── Export Excel (SheetJS) — padrão planilha Susan ──────────────
async function exportarExcel() {
  if (typeof XLSX === 'undefined') throw new Error('SheetJS não carregada.');

  const movs = await carregarMovimentacoes();
  const cats = await carregarCategorias();
  const wb = XLSX.utils.book_new();

  // Ordenar categorias alfabeticamente, nomes em CAIXA ALTA
  const catsOrd = [...cats].sort((a, b) => a.nome.toUpperCase().localeCompare(b.nome.toUpperCase()));

  // ── Aba por categoria (padrão Susan: Calibri, headers bold) ──
  for (const cat of catsOrd) {
    const catMovs = movs.filter(m => m.categoria_id === cat.id)
      .sort((a, b) => a.data_emissao.localeCompare(b.data_emissao));

    // Linha 1: Ficha de Controle + produto + unidade
    const row1 = ['Ficha de Controle de Estoques:', '', '', cat.nome.toUpperCase(), cat.unidade || 'KG'];
    // Linha 2: Headers (igual planilha Susan)
    const header = ['Data', 'NF', 'Operação', 'Cliente / Fornecedor',
      'Qtde Entrada', 'Qtde Entrada KG', 'Vlr Unit Entrada', 'Vlr Total Entrada',
      'ICMS', 'Pis/Cofins (S/N)', 'PIS', 'COFINS', 'Compra Líquida',
      'Qtde Saída', 'Vlr NF Venda', 'ICMS s/ Venda', 'Saldo ' + (cat.unidade || 'KG')];

    const rows = [row1, header];
    let saldo = Number(cat.saldo_inicial || 0);
    rows.push(['', '', '', 'Saldo Anterior', saldo, '', '', '', '', '', '', '', '', '', '', '', saldo]);

    let tVlrE=0, tIcmsE=0, tPisE=0, tCofE=0, tLiqE=0, tVlrS=0, tIcmsS=0;

    for (const m of catMovs) {
      const pisCof = (Number(m.pis||0) > 0 || Number(m.cofins||0) > 0) ? 'S' : 'N';
      if (m.tipo === 'entrada') {
        saldo += Number(m.quantidade||0);
        tVlrE += Number(m.valor_total||0); tIcmsE += Number(m.icms||0); tPisE += Number(m.pis||0); tCofE += Number(m.cofins||0); tLiqE += Number(m.compra_liquida||0);
        rows.push([m.data_emissao, m.numero_nf, 'COM', m.fornecedor_cliente,
          m.quantidade, m.quantidade, m.valor_unitario, m.valor_total,
          m.icms, pisCof, m.pis, m.cofins, m.compra_liquida,
          '', '', '', saldo]);
      } else {
        saldo -= Number(m.quantidade||0);
        tVlrS += Number(m.valor_total||0); tIcmsS += Number(m.icms||0);
        rows.push([m.data_emissao, m.numero_nf, 'VEN', m.fornecedor_cliente,
          '', '', '', '', '', '', '', '', '',
          m.quantidade, m.valor_total, m.icms, saldo]);
      }
    }

    // Linha TOTAL
    rows.push([]);
    rows.push(['', '', '', 'TOTAL', '', '', '', tVlrE, tIcmsE, '', tPisE, tCofE, tLiqE, '', tVlrS, tIcmsS, saldo]);

    const ws = XLSX.utils.aoa_to_sheet(rows);
    ws['!cols'] = [
      {wch:12},{wch:10},{wch:6},{wch:32},{wch:12},{wch:12},{wch:14},{wch:16},
      {wch:12},{wch:10},{wch:10},{wch:10},{wch:14},{wch:12},{wch:14},{wch:12},{wch:12}
    ];
    const sheetName = cat.nome.toUpperCase().substring(0, 31);
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
  }

  // ── Aba ENCERRAMENTO (Apuração Final) ──
  const encRows = [
    ['CLASSIC IMP. E EXP. DE COUROS LTDA', '', '', '', '', '', '', '', '', '', '', 'Encerramento'],
    [],
    ['Estoques e Produção', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
    [],
    ['', '', '', '', 'Apuração Final'],
    ['', '', '', 'PIS', 'COFINS', 'ICMS'],
  ];

  let gPis=0, gCof=0, gIcmsC=0, gIcmsD=0, gVlrC=0, gVlrV=0;
  for (const cat of catsOrd) {
    const catMovs = movs.filter(m => m.categoria_id === cat.id);
    catMovs.forEach(m => {
      if (m.tipo === 'entrada') {
        gPis += Number(m.pis||0); gCof += Number(m.cofins||0);
        gIcmsC += Number(m.icms||0); gVlrC += Number(m.valor_total||0);
      } else {
        gIcmsD += Number(m.icms||0); gVlrV += Number(m.valor_total||0);
      }
    });
  }

  encRows.push(['', '', '', '', '', '']);
  encRows.push(['', 'Saldo Anterior', '', '-', '-', '-']);
  encRows.push(['', 'Créditos', '', gPis, gCof, gIcmsC]);
  encRows.push(['', 'Débitos', '', '-', '-', gIcmsD]);
  encRows.push(['', 'Saldo Final', '', gPis, gCof, gIcmsC - gIcmsD]);
  encRows.push([]);
  encRows.push(['TOTAL', '', '', gVlrC, '', gVlrC + gVlrV]);
  encRows.push([]);
  encRows.push(['', 'VLR DA PIS S/ COMPRAS NESTE MES', '', gPis]);
  encRows.push(['', 'VLR DA COFINS S/ COMPRAS NESTE MES', '', gCof]);
  encRows.push(['', 'VLR REGULARIZACAO DO PIS NESTE MES', '', '-']);
  encRows.push(['', 'VLR DA COFINS S/ VENDAS NESTE MES', '', '-']);
  encRows.push(['', 'VLR REGULARIZACAO DA COFINS NESTE MES', '', '-']);

  const wsEnc = XLSX.utils.aoa_to_sheet(encRows);
  wsEnc['!cols'] = [{wch:35},{wch:40},{wch:5},{wch:16},{wch:16},{wch:16}];
  XLSX.utils.book_append_sheet(wb, wsEnc, 'ENCERRAMENTO');

  XLSX.writeFile(wb, `Controle_Estoque_${new Date().toISOString().slice(0, 10)}.xlsx`);
}

// ── Pasta Automática (File System Access API) ──────────────────
const PASTA_AUTO_DB = 'ClassicCQ_PastaAuto';
const PASTA_AUTO_STORE = 'handles';

function _openPastaDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(PASTA_AUTO_DB, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(PASTA_AUTO_STORE);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function _savePastaHandle(handle) {
  const db = await _openPastaDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(PASTA_AUTO_STORE, 'readwrite');
    tx.objectStore(PASTA_AUTO_STORE).put(handle, 'folderHandle');
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function _getPastaHandle() {
  const db = await _openPastaDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(PASTA_AUTO_STORE, 'readonly');
    const req = tx.objectStore(PASTA_AUTO_STORE).get('folderHandle');
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}

async function _removePastaHandle() {
  const db = await _openPastaDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(PASTA_AUTO_STORE, 'readwrite');
    tx.objectStore(PASTA_AUTO_STORE).delete('folderHandle');
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

function _getLastImportDate() {
  return localStorage.getItem('cq_pasta_auto_last') || null;
}

function _setLastImportDate(iso) {
  localStorage.setItem('cq_pasta_auto_last', iso);
}

// Recursivamente coleta XMLs de uma pasta e subpastas
async function _coletarXMLs(dirHandle, maxFiles = 5000) {
  const xmlFiles = [];
  async function walk(handle) {
    for await (const entry of handle.values()) {
      if (xmlFiles.length >= maxFiles) return;
      if (entry.kind === 'file' && entry.name.toLowerCase().endsWith('.xml')) {
        xmlFiles.push(entry);
      } else if (entry.kind === 'directory') {
        await walk(entry);
      }
    }
  }
  await walk(dirHandle);
  return xmlFiles;
}

async function configurarPastaAuto() {
  if (!window.showDirectoryPicker) {
    toast('Navegador não suporta File System Access API. Use Chrome ou Edge.', 'erro');
    return;
  }
  try {
    const handle = await window.showDirectoryPicker({ mode: 'read' });
    await _savePastaHandle(handle);
    localStorage.setItem('cq_pasta_auto_nome', handle.name);
    _atualizarPastaAutoUI(handle.name);
    toast(`Pasta "${handle.name}" configurada!`, 'ok');
  } catch (e) {
    if (e.name !== 'AbortError') toast('Erro ao selecionar pasta: ' + e.message, 'erro');
  }
}

function _atualizarPastaAutoUI(nome, info) {
  const btn = document.getElementById('btnPastaAuto');
  const status = document.getElementById('pastaAutoStatus');
  const nomeEl = document.getElementById('pastaAutoNome');
  const infoEl = document.getElementById('pastaAutoInfo');
  if (!status) return;

  if (nome) {
    if (btn) btn.style.display = 'none';
    status.style.display = 'block';
    if (nomeEl) nomeEl.textContent = 'Pasta: ' + nome;
    if (infoEl) infoEl.textContent = info || '';
  } else {
    if (btn) btn.style.display = 'flex';
    status.style.display = 'none';
  }
}

async function buscarNovosXMLs() {
  const handle = await _getPastaHandle();
  if (!handle) {
    toast('Nenhuma pasta configurada', 'erro');
    return;
  }

  // Verificar permissão (precisa re-grant após reabrir browser)
  const perm = await handle.queryPermission({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await handle.requestPermission({ mode: 'read' });
    if (req !== 'granted') {
      toast('Permissão negada para ler a pasta', 'erro');
      return;
    }
  }

  const infoEl = document.getElementById('pastaAutoInfo');
  if (infoEl) infoEl.textContent = 'Escaneando pasta...';

  try {
    const xmlEntries = await _coletarXMLs(handle);
    if (!xmlEntries.length) {
      if (infoEl) infoEl.textContent = 'Nenhum XML encontrado na pasta.';
      toast('Nenhum XML encontrado', 'erro');
      return;
    }

    // Filtrar por data de modificação se tiver lastImport
    const lastImport = _getLastImportDate();
    let filesToProcess = [];

    for (const entry of xmlEntries) {
      const file = await entry.getFile();
      if (lastImport && file.lastModified <= new Date(lastImport).getTime()) continue;
      filesToProcess.push(file);
    }

    if (!filesToProcess.length) {
      if (infoEl) infoEl.textContent = `Nenhum XML novo desde última importação. (${xmlEntries.length} XMLs na pasta)`;
      toast('Nenhum XML novo encontrado', 'ok');
      return;
    }

    if (infoEl) infoEl.textContent = `Processando ${filesToProcess.length} XMLs novos de ${xmlEntries.length} total...`;

    // Garantir categorias e mapeamentos carregados
    if (!_categorias.length) await carregarCategorias();
    if (!_mapeamentos.length) await carregarMapeamentos();

    const allParsedNFs = [];
    let erros = 0, dups = 0;

    for (let fi = 0; fi < filesToProcess.length; fi++) {
      const file = filesToProcess[fi];
      if (fi % 20 === 0 && infoEl) {
        const scanPct = Math.round((fi / filesToProcess.length) * 100);
        infoEl.textContent = `Lendo XMLs... ${scanPct}% (${fi}/${filesToProcess.length})`;
      }
      try {
        const text = await file.text();
        const isCTe = text.includes('cteProc') || text.includes('infCte');
        let nf;
        if (isCTe) {
          nf = parseCTeXML(text);
          if (nf) {
            for (const item of nf.itens) {
              const cat = _categorias.find(c => c.nome === item._catNome);
              if (cat) { item.categoria_id = cat.id; item._mapped = true; }
            }
          }
        } else {
          nf = parseNFeXML(text);
        }
        if (!nf || !nf.itens.length) { erros++; continue; }
        if (allParsedNFs.find(n => n.chaveNFe === nf.chaveNFe)) { dups++; continue; }

        // Resolver categorias para NF-e
        if (!isCTe) {
          for (const item of nf.itens) {
            if (!item._mapped) {
              item.categoria_id = resolverCategoria(item.produto_xml, item.ncm, item.cfop);
              item._mapped = !!item.categoria_id;
            }
          }
        }

        nf._fileName = file.name;
        allParsedNFs.push(nf);
      } catch { erros++; }
    }

    // Importar todos os itens mapeados
    const todosItens = allParsedNFs.flatMap(nf => nf.itens);
    const mapeados = todosItens.filter(i => i.categoria_id);
    const naoMapeados = todosItens.filter(i => !i.categoria_id);

    if (infoEl) infoEl.textContent = `Importando ${mapeados.length} itens (${naoMapeados.length} sem categoria)...`;

    // Show progress bar
    const progressDiv = document.getElementById('pastaAutoProgress');
    const progressBar = document.getElementById('pastaAutoBar');
    const progressPct = document.getElementById('pastaAutoPct');
    if (progressDiv) progressDiv.style.display = 'block';

    let resultado = { inseridos: 0, duplicados: 0, erros: 0 };
    if (mapeados.length) {
      resultado = await importarItens(mapeados, (current, total, ins, dup, err) => {
        const pct = Math.round(current / total * 100);
        if (progressBar) progressBar.style.width = pct + '%';
        if (progressPct) progressPct.textContent = `${pct}% — ${current}/${total} (${ins} novos, ${dup} dup, ${err} erros)`;
        if (infoEl) infoEl.textContent = `Importando... ${current}/${total} itens`;
      });
    }

    if (progressDiv) progressDiv.style.display = 'none';
    _setLastImportDate(new Date().toISOString());
    const resumo = `${resultado.inseridos} novos, ${resultado.duplicados} duplicados, ${naoMapeados.length} sem categoria`;
    if (infoEl) infoEl.textContent = `Última busca: ${new Date().toLocaleString('pt-BR')} — ${resumo}`;
    toast(`Importação automática: ${resumo}`, resultado.inseridos > 0 ? 'ok' : 'info');

    // Atualizar dashboard se houver novos
    if (resultado.inseridos > 0 && typeof renderDashboard === 'function') {
      renderDashboard();
    }
  } catch (e) {
    if (infoEl) infoEl.textContent = 'Erro: ' + e.message;
    toast('Erro na busca automática: ' + e.message, 'erro');
  }
}

async function desconfigurarPastaAuto() {
  await _removePastaHandle();
  localStorage.removeItem('cq_pasta_auto_nome');
  localStorage.removeItem('cq_pasta_auto_last');
  _atualizarPastaAutoUI(null);
  toast('Pasta automática removida', 'ok');
}

// Verificar se há pasta configurada ao iniciar
async function verificarPastaAuto() {
  const nome = localStorage.getItem('cq_pasta_auto_nome');
  if (nome) {
    _atualizarPastaAutoUI(nome, 'Clique "Buscar Novos XMLs" para atualizar');
  }
}

// ── Dashboard Data ──────────────────────────────────────────────
async function carregarDashboardData() {
  const [saldos, mensal] = await Promise.all([carregarSaldos(), carregarEvolucaoMensal()]);
  return { saldos, mensal };
}

// ── Formatters ──────────────────────────────────────────────────
function fmtNum(v, dec = 2) {
  return Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function fmtBRL(v) {
  return Number(v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function fmtBRLCompacto(v) {
  v = Number(v || 0);
  if (Math.abs(v) >= 1000000) return 'R$ ' + (v/1000000).toFixed(1).replace('.',',') + 'M';
  if (Math.abs(v) >= 1000) return 'R$ ' + (v/1000).toFixed(1).replace('.',',') + 'K';
  return 'R$ ' + v.toFixed(0);
}

function fmtData(d) {
  if (!d) return '—';
  const [y, m, dd] = d.split('-');
  return `${dd}/${m}/${y}`;
}
