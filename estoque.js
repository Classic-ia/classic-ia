// estoque.js — Classic CQ: Controle de Estoque via XML NFe
// Parser de NFe, API Supabase, Dashboard e Export Excel

const CNPJ_CLASSIC = '08849964000110';

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
    throw new Error(`Supabase ${r.status}: ${txt}`);
  }
  if (r.status === 204) return [];
  return r.json();
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

  const infNFe = q(doc, 'infNFe');
  if (!infNFe) return null;

  const ide = q(infNFe, 'ide');
  const emit = q(infNFe, 'emit');
  const dest = q(infNFe, 'dest');

  if (!ide || !emit || !dest) return null;

  // Chave de acesso (44 dígitos)
  const chaveNFe = (infNFe.getAttribute('Id') || '').replace('NFe', '');

  const nNF = txt(ide, 'nNF');
  const dhEmi = txt(ide, 'dhEmi');
  const dataEmissao = dhEmi ? dhEmi.substring(0, 10) : '';

  const emitCNPJ = txt(emit, 'CNPJ');
  const emitNome = txt(emit, 'xNome');
  const destCNPJ = txt(dest, 'CNPJ');
  const destNome = txt(dest, 'xNome');

  // Detectar tipo: se destinatário é Classic → entrada, se emitente é Classic → saída
  let tipo = 'entrada';
  let parceiro = emitNome;
  let parceiroCNPJ = emitCNPJ;

  if (emitCNPJ === CNPJ_CLASSIC) {
    tipo = 'saida';
    parceiro = destNome;
    parceiroCNPJ = destCNPJ;
  }

  // Itens (det)
  const detEls = qAll(infNFe, 'det');
  const itens = [];

  for (const det of detEls) {
    const prod = q(det, 'prod');
    if (!prod) continue;

    const imposto = q(det, 'imposto');
    let vICMS = 0, vPIS = 0, vCOFINS = 0;

    if (imposto) {
      // ICMS — pode estar em vários sub-elementos
      const icmsEl = q(imposto, 'ICMS');
      if (icmsEl) {
        for (const child of icmsEl.children) {
          const v = num(child, 'vICMS');
          if (v) { vICMS = v; break; }
        }
      }
      // PIS
      const pisEl = q(imposto, 'PIS');
      if (pisEl) {
        for (const child of pisEl.children) {
          const v = num(child, 'vPIS');
          if (v) { vPIS = v; break; }
        }
      }
      // COFINS
      const cofinsEl = q(imposto, 'COFINS');
      if (cofinsEl) {
        for (const child of cofinsEl.children) {
          const v = num(child, 'vCOFINS');
          if (v) { vCOFINS = v; break; }
        }
      }
    }

    const vProd = num(prod, 'vProd');
    const compraLiquida = vProd - vICMS - vPIS - vCOFINS;

    itens.push({
      chave_nfe: chaveNFe,
      numero_nf: nNF,
      data_emissao: dataEmissao,
      tipo,
      fornecedor_cliente: parceiro,
      cnpj: parceiroCNPJ,
      produto_xml: txt(prod, 'xProd'),
      ncm: txt(prod, 'NCM'),
      cfop: txt(prod, 'CFOP'),
      unidade: txt(prod, 'uCom'),
      quantidade: num(prod, 'qCom'),
      valor_unitario: num(prod, 'vUnCom'),
      valor_total: vProd,
      icms: vICMS,
      pis: vPIS,
      cofins: vCOFINS,
      compra_liquida: compraLiquida,
      categoria_id: null, // será preenchido pelo mapeamento
      _mapped: false
    });
  }

  return { chaveNFe, nNF, dataEmissao, tipo, parceiro, parceiroCNPJ, itens };
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

function resolverCategoria(produtoXml, ncm) {
  const prodUp = produtoXml.toUpperCase().trim();
  const descMaps = _mapeamentos.filter(x => x.tipo_chave === 'descricao');

  // 1. Match exato por descrição
  let m = descMaps.find(x => x.chave.toUpperCase() === prodUp);
  if (m) return m.categoria_id;

  // 2. Match parcial — produto COMEÇA com a chave (palavra completa)
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
async function importarItens(itens) {
  const validos = itens.filter(i => i.categoria_id);
  if (!validos.length) throw new Error('Nenhum item com categoria mapeada.');

  const payload = validos.map(i => ({
    chave_nfe: i.chave_nfe,
    numero_nf: i.numero_nf,
    data_emissao: i.data_emissao,
    tipo: i.tipo,
    fornecedor_cliente: i.fornecedor_cliente,
    cnpj: i.cnpj,
    categoria_id: i.categoria_id,
    produto_xml: i.produto_xml,
    ncm: i.ncm,
    cfop: i.cfop,
    unidade: i.unidade,
    quantidade: i.quantidade,
    valor_unitario: i.valor_unitario,
    valor_total: i.valor_total,
    icms: i.icms,
    pis: i.pis,
    cofins: i.cofins,
    compra_liquida: i.compra_liquida
  }));

  return sbFetch('estoque_movimentacoes', {
    method: 'POST',
    headers: { ...sbHeaders(), 'Prefer': 'return=representation,resolution=merge-duplicates' },
    body: JSON.stringify(payload)
  });
}

// ── Consultas ───────────────────────────────────────────────────
async function carregarMovimentacoes(filtros = {}) {
  let qs = 'estoque_movimentacoes?select=*,estoque_categorias(nome)&order=data_emissao.desc,numero_nf.asc';
  if (filtros.tipo) qs += `&tipo=eq.${filtros.tipo}`;
  if (filtros.categoria_id) qs += `&categoria_id=eq.${filtros.categoria_id}`;
  if (filtros.dataInicio) qs += `&data_emissao=gte.${filtros.dataInicio}`;
  if (filtros.dataFim) qs += `&data_emissao=lte.${filtros.dataFim}`;
  if (filtros.fornecedor) qs += `&fornecedor_cliente=ilike.*${filtros.fornecedor}*`;
  return sbFetch(qs);
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
    if (m.tipo === 'entrada' && m.quantidade > 0) {
      custoAcum += m.valor_total;
      qtdAcum += m.quantidade;
    } else if (m.tipo === 'saida' && qtdAcum > 0) {
      const cmp = custoAcum / qtdAcum;
      custoAcum -= cmp * Math.min(m.quantidade, qtdAcum);
      qtdAcum = Math.max(0, qtdAcum - m.quantidade);
    }
  }
  return qtdAcum > 0 ? custoAcum / qtdAcum : 0;
}

// ── Export Excel (SheetJS) ──────────────────────────────────────
async function exportarExcel() {
  if (typeof XLSX === 'undefined') {
    throw new Error('Biblioteca SheetJS não carregada.');
  }

  const movs = await carregarMovimentacoes();
  const cats = await carregarCategorias();
  const wb = XLSX.utils.book_new();

  for (const cat of cats) {
    const catMovs = movs.filter(m => m.categoria_id === cat.id);
    const header = ['Data', 'NF', 'Operação', 'Fornecedor/Cliente', 'Qtde Entrada',
      'Vlr Unit', 'Vlr Total', 'ICMS', 'PIS', 'COFINS', 'Compra Líquida',
      'Qtde Saída', 'Vlr NF Venda', 'Saldo'];

    const rows = [header];
    let saldo = cat.saldo_inicial || 0;
    rows.push(['', '', 'COM', 'Saldo Anterior', saldo, '', '', '', '', '', '', '', '', saldo]);

    for (const m of catMovs.sort((a, b) => a.data_emissao.localeCompare(b.data_emissao))) {
      if (m.tipo === 'entrada') {
        saldo += m.quantidade;
        rows.push([m.data_emissao, m.numero_nf, 'COM', m.fornecedor_cliente,
          m.quantidade, m.valor_unitario, m.valor_total, m.icms, m.pis, m.cofins,
          m.compra_liquida, '', '', saldo]);
      } else {
        saldo -= m.quantidade;
        rows.push([m.data_emissao, m.numero_nf, 'VEN', m.fornecedor_cliente,
          '', '', '', '', '', '', '', m.quantidade, m.valor_total, saldo]);
      }
    }

    const ws = XLSX.utils.aoa_to_sheet(rows);
    // Larguras de coluna
    ws['!cols'] = [
      { wch: 12 }, { wch: 10 }, { wch: 6 }, { wch: 35 }, { wch: 12 },
      { wch: 12 }, { wch: 14 }, { wch: 12 }, { wch: 10 }, { wch: 10 },
      { wch: 14 }, { wch: 12 }, { wch: 14 }, { wch: 12 }
    ];
    const sheetName = cat.nome.substring(0, 31); // Excel limit
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
  }

  XLSX.writeFile(wb, `Controle_Estoque_${new Date().toISOString().slice(0, 10)}.xlsx`);
}

// ── Dashboard Data ──────────────────────────────────────────────
async function carregarDashboardData() {
  const [saldos, mensal] = await Promise.all([carregarSaldos(), carregarEvolucaoMensal()]);
  return { saldos, mensal };
}

// ── Formatters ──────────────────────────────────────────────────
function fmtNum(v, dec = 2) {
  return (v || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function fmtBRL(v) {
  return (v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function fmtData(d) {
  if (!d) return '—';
  const [y, m, dd] = d.split('-');
  return `${dd}/${m}/${y}`;
}
