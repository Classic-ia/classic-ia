/**
 * pcr_engine.js — Classic CQ v2
 * Motor de calculo e consulta para PCR (Produto Conferido no Recebimento)
 *
 * Dependencias:
 *   - config.js (CQ_CONFIG)
 *   - cq_auth.js (CQAuth)
 *
 * Uso:
 *   <script src="config.js"></script>
 *   <script src="cq_auth.js"></script>
 *   <script src="js/pcr_engine.js"></script>
 *
 *   const classe = PCREngine.classificar(12.5);
 *   const dados  = await PCREngine.listarPCRs({ status: 'pendente' });
 */

const PCREngine = (function () {

  // =====================================================================
  //  CONSTANTES
  // =====================================================================

  /** Defeitos que geram desconto financeiro (abatimento em NF) */
  const DEFEITOS_DESCONTO = [
    'fermentacao',
    'sem_folhas',
    'folhas_arrebentadas',
    'pedaco'
  ];

  /** Defeitos que geram acao corretiva (devolutiva / notificacao) */
  const DEFEITOS_CORRETIVA = [
    'rasgado',
    'furado',
    'sujo',
    'corte_irregular',
    'mau_cheiro',
    'contaminacao',
    'outro'
  ];

  /** Todos os defeitos reconhecidos */
  const TODOS_DEFEITOS = [...DEFEITOS_DESCONTO, ...DEFEITOS_CORRETIVA];

  /**
   * Carteira de fornecedores por responsavel.
   * Chaves sao patterns (substring match, case-insensitive) do nome do fornecedor.
   */
  const CARTEIRA = {
    JEAN: [
      'FRIGONOR',
      'MATADOURO REGIONAL',
      'FLORAI',
      'MONTALFRIG',
      'FRIGODASKO',
      'BEZERROS'
    ],
    DORLY: [
      'GOLD CARNES',
      'CALIBRE',
      'MA FOOD',
      'MONDELLI',
      'MEAT WORLD'
    ]
  };

  /**
   * Mapeamento de termos do texto bruto (relatorio ATAK / digitacao manual)
   * para o nome canonico do defeito.
   */
  const PARSE_MAP = {
    'PELANDO':    'fermentacao',
    'FERMENTACAO':'fermentacao',
    'SEM FOLHA':  'sem_folhas',
    'S.FOLHA':    'sem_folhas',
    'S FOLHA':    'sem_folhas',
    'F.E.':       'folhas_arrebentadas',
    'FE':         'folhas_arrebentadas',
    'FOLHAS ARREBENTADAS': 'folhas_arrebentadas',
    'PEDACO':     'pedaco',
    'RASGADO':    'rasgado',
    'FURADO':     'furado',
    'SUJO':       'sujo',
    'DESPOJO':    'sujo',
    'C.I.':       'corte_irregular',
    'CI':         'corte_irregular',
    'SEBO':       'corte_irregular',
    'CORTE IRREGULAR': 'corte_irregular',
    'MAU CHEIRO': 'mau_cheiro',
    'CONTAMINACAO':'contaminacao',
    'OUTRO':      'outro'
  };


  // =====================================================================
  //  HELPERS INTERNOS — SUPABASE
  // =====================================================================

  /** Monta headers padrao para Supabase REST */
  function _headers(extra) {
    const h = {
      'Content-Type':  'application/json',
      'apikey':        CQAuth.getSbKey(),
      'Authorization': 'Bearer ' + (CQAuth.getAuthToken() || CQAuth.getSbKey())
    };
    if (extra) Object.assign(h, extra);
    return h;
  }

  /** URL base do REST */
  function _url(recurso) {
    return CQAuth.getSbUrl() + '/rest/v1/' + recurso;
  }

  /**
   * GET generico com query string
   * @param {string} recurso - tabela ou view
   * @param {string} qs      - query string (sem '?')
   * @param {object} [extraHeaders] - headers adicionais (ex: Prefer, Range)
   * @returns {Promise<any|null>}
   */
  async function _get(recurso, qs, extraHeaders) {
    try {
      const sep = qs ? '?' + qs : '';
      const r = await fetch(_url(recurso) + sep, { headers: _headers(extraHeaders) });
      if (!r.ok) {
        console.error(`[PCREngine] GET ${recurso} ${r.status}`, await r.text());
        return null;
      }
      return r.json();
    } catch (err) {
      console.error('[PCREngine] GET erro:', err);
      return null;
    }
  }

  /**
   * PATCH generico
   * @param {string} recurso - tabela + filtro (ex: "pcr_documentos?id=eq.123")
   * @param {object} body
   * @returns {Promise<boolean>}
   */
  async function _patch(recurso, body) {
    try {
      const r = await fetch(_url(recurso), {
        method:  'PATCH',
        headers: _headers({ 'Prefer': 'return=representation' }),
        body:    JSON.stringify(body)
      });
      if (!r.ok) {
        console.error(`[PCREngine] PATCH ${recurso} ${r.status}`, await r.text());
        return false;
      }
      return true;
    } catch (err) {
      console.error('[PCREngine] PATCH erro:', err);
      return false;
    }
  }


  // =====================================================================
  //  CLASSIFICACAO
  // =====================================================================

  /**
   * Classifica percentual de defeito em A / B / C.
   * @param {number} pct - percentual de defeito (0-100)
   * @returns {{ classe: string, label: string, cor: string, bg: string }}
   */
  function classificar(pct) {
    const p = Number(pct) || 0;
    if (p <= 5)  return { classe: 'A', label: 'Aprovado',          cor: '#4ade80', bg: 'rgba(42,122,74,0.15)' };
    if (p <= 15) return { classe: 'B', label: 'Aprovado com Ressalva', cor: '#fbbf24', bg: 'rgba(122,96,32,0.15)' };
    return                { classe: 'C', label: 'Reprovado',         cor: '#f87171', bg: 'rgba(122,42,42,0.15)' };
  }


  // =====================================================================
  //  DESCONTO POR QUANTIDADE
  // =====================================================================

  /**
   * Calcula desconto/credito por diferenca entre previsto e recebido.
   *
   * @param {Array<{descricao:string, qtd_prevista:number, qtd_recebida:number, preco_unitario:number}>} produtos
   * @returns {{
   *   desconto_falta:  number,
   *   credito_excesso: number,
   *   desconto_liquido:number,
   *   detalhes: Array<{descricao:string, diferenca:number, tipo:string, valor:number}>
   * }}
   */
  function calcularDescontoQuantidade(produtos) {
    let desconto_falta  = 0;
    let credito_excesso = 0;
    const detalhes = [];

    (produtos || []).forEach(p => {
      const prev  = Number(p.qtd_prevista)   || 0;
      const rec   = Number(p.qtd_recebida)   || 0;
      const preco = Number(p.preco_unitario)  || 0;
      const dif   = rec - prev;

      if (dif === 0) return;

      const valor = Math.abs(dif) * preco;
      const tipo  = dif < 0 ? 'falta' : 'excesso';

      if (tipo === 'falta')   desconto_falta  += valor;
      if (tipo === 'excesso') credito_excesso += valor;

      detalhes.push({
        descricao: p.descricao || '',
        diferenca: dif,
        tipo,
        valor
      });
    });

    return {
      desconto_falta:   Math.round(desconto_falta * 100) / 100,
      credito_excesso:  Math.round(credito_excesso * 100) / 100,
      desconto_liquido: Math.round((desconto_falta - credito_excesso) * 100) / 100,
      detalhes
    };
  }


  // =====================================================================
  //  ANALISE DE QUALIDADE
  // =====================================================================

  /**
   * Calcula totais de defeitos separados por categoria.
   *
   * @param {object} defeitos - ex: { fermentacao: 5, rasgado: 12, furado: 3 }
   * @param {number} qtd_analisada - total de unidades analisadas
   * @returns {{
   *   total_desconto:   number,
   *   total_corretiva:  number,
   *   pct_desconto:     number,
   *   pct_corretiva:    number,
   *   classificacao:    object,
   *   detalhes:         Array<{defeito:string, qtd:number, categoria:string, pct:number}>
   * }}
   */
  function calcularQualidade(defeitos, qtd_analisada) {
    const qa = Number(qtd_analisada) || 1; // evita divisao por zero
    let total_desconto  = 0;
    let total_corretiva = 0;
    const detalhes = [];

    Object.entries(defeitos || {}).forEach(([defeito, qtd]) => {
      const n = Number(qtd) || 0;
      if (n === 0) return;

      let categoria = 'outro';
      if (DEFEITOS_DESCONTO.includes(defeito))  categoria = 'desconto';
      if (DEFEITOS_CORRETIVA.includes(defeito)) categoria = 'corretiva';

      if (categoria === 'desconto')  total_desconto  += n;
      if (categoria === 'corretiva') total_corretiva += n;

      detalhes.push({
        defeito,
        qtd:       n,
        categoria,
        pct:       Math.round((n / qa) * 10000) / 100  // 2 casas decimais
      });
    });

    const pct_desconto  = Math.round((total_desconto  / qa) * 10000) / 100;
    const pct_corretiva = Math.round((total_corretiva / qa) * 10000) / 100;
    const pct_total     = pct_desconto + pct_corretiva;

    return {
      total_desconto,
      total_corretiva,
      pct_desconto,
      pct_corretiva,
      classificacao: classificar(pct_total),
      detalhes
    };
  }


  // =====================================================================
  //  PARSE DE TEXTO
  // =====================================================================

  /**
   * Interpreta texto livre com defeitos e quantidades.
   * Formato esperado: "RASGADO: 15 FURADO: 22 PELANDO: 89"
   * Tambem aceita "RASGADO 15  FURADO 22" (sem dois-pontos).
   *
   * @param {string} texto
   * @returns {object} - ex: { rasgado: 15, furado: 22, fermentacao: 89 }
   */
  function parseQualidadeTexto(texto) {
    if (!texto || typeof texto !== 'string') return {};

    const resultado = {};
    const upper = texto.toUpperCase().trim();

    // Ordena patterns do mais longo para o mais curto para evitar matches parciais
    const patterns = Object.keys(PARSE_MAP).sort((a, b) => b.length - a.length);

    // Regex: PATTERN[:\s]+NUMERO
    patterns.forEach(pat => {
      // Escape de pontos para regex
      const escaped = pat.replace(/\./g, '\\.');
      const re = new RegExp(escaped + '[:\\s]+([0-9]+)', 'gi');
      let m;
      while ((m = re.exec(upper)) !== null) {
        const campo = PARSE_MAP[pat];
        const val   = parseInt(m[1], 10);
        resultado[campo] = (resultado[campo] || 0) + val;
      }
    });

    return resultado;
  }


  // =====================================================================
  //  RESPONSAVEL
  // =====================================================================

  /**
   * Determina o responsavel pela carteira do fornecedor.
   * @param {string} fornecedorNome
   * @returns {string|null} 'JEAN', 'DORLY' ou null se nao encontrado
   */
  function getResponsavel(fornecedorNome) {
    if (!fornecedorNome) return null;
    const nome = fornecedorNome.toUpperCase();

    for (const [resp, patterns] of Object.entries(CARTEIRA)) {
      for (const pat of patterns) {
        if (nome.includes(pat)) return resp;
      }
    }
    return null;
  }


  // =====================================================================
  //  SUPABASE — CONSULTAS PCR
  // =====================================================================

  /**
   * Lista PCRs com filtros e paginacao.
   * Consulta a view vw_pcr_resumo.
   *
   * @param {object} [filtros]   - { status, fornecedor, responsavel, data_de, data_ate, classe }
   * @param {number} [pagina=1]
   * @param {number} [porPagina=25]
   * @returns {Promise<{ data: Array, total: number }>}
   */
  async function listarPCRs(filtros, pagina, porPagina) {
    const pag = pagina   || 1;
    const pp  = porPagina || 25;
    const offset = (pag - 1) * pp;

    const params = [];
    params.push('order=data_recebimento.desc');
    params.push('select=*');

    const f = filtros || {};
    if (f.status)       params.push('status=eq.' + encodeURIComponent(f.status));
    if (f.fornecedor)   params.push('fornecedor_nome=ilike.*' + encodeURIComponent(f.fornecedor) + '*');
    if (f.responsavel)  params.push('responsavel=eq.' + encodeURIComponent(f.responsavel));
    if (f.data_de)      params.push('data_recebimento=gte.' + encodeURIComponent(f.data_de));
    if (f.data_ate)     params.push('data_recebimento=lte.' + encodeURIComponent(f.data_ate));
    if (f.classe)       params.push('classe=eq.' + encodeURIComponent(f.classe));

    const qs = params.join('&');
    const rangeEnd = offset + pp - 1;

    const data = await _get('vw_pcr_resumo', qs, {
      'Range':  offset + '-' + rangeEnd,
      'Prefer': 'count=exact'
    });

    // O Supabase retorna Content-Range com total
    return {
      data:  data || [],
      total: Array.isArray(data) ? data.length : 0 // fallback; idealmente ler do header
    };
  }

  /**
   * Carrega um PCR completo com todos os dados relacionados.
   * Faz 4 consultas em paralelo: documento, produtos, qualidade, decisao/execucao.
   *
   * @param {string|number} id - ID do pcr_documento
   * @returns {Promise<object|null>}
   */
  async function carregarPCR(id) {
    if (!id) return null;

    const eid = encodeURIComponent(id);

    const [doc, produtos, qualidade, decisao, execucao] = await Promise.all([
      _get('pcr_documentos',  'id=eq.' + eid + '&select=*'),
      _get('pcr_produtos',    'pcr_id=eq.' + eid + '&select=*&order=id.asc'),
      _get('pcr_qualidade',   'pcr_id=eq.' + eid + '&select=*'),
      _get('pcr_decisoes',    'pcr_id=eq.' + eid + '&select=*&limit=1'),
      _get('pcr_execucoes',   'pcr_id=eq.' + eid + '&select=*&order=created_at.desc')
    ]);

    if (!doc || !doc.length) return null;

    return {
      documento:  doc[0],
      produtos:   produtos  || [],
      qualidade:  qualidade || [],
      decisao:    (decisao && decisao[0]) || null,
      execucoes:  execucao  || []
    };
  }

  /**
   * Salva ou atualiza a decisao de um PCR.
   * @param {string|number} pcrId
   * @param {object} decisao - campos a salvar (ex: { tipo_acao, valor_desconto, observacao })
   * @returns {Promise<boolean>}
   */
  async function salvarDecisao(pcrId, decisao) {
    if (!pcrId || !decisao) return false;
    return _patch('pcr_decisoes?pcr_id=eq.' + encodeURIComponent(pcrId), decisao);
  }

  /**
   * Salva ou atualiza a execucao de um PCR.
   * @param {string|number} pcrId
   * @param {object} execucao - campos a salvar (ex: { executor, data_execucao, observacao })
   * @returns {Promise<boolean>}
   */
  async function salvarExecucao(pcrId, execucao) {
    if (!pcrId || !execucao) return false;
    return _patch('pcr_execucoes?pcr_id=eq.' + encodeURIComponent(pcrId), execucao);
  }

  /**
   * Atualiza o status de um PCR.
   * @param {string|number} pcrId
   * @param {string} novoStatus - ex: 'pendente', 'em_analise', 'concluido'
   * @returns {Promise<boolean>}
   */
  async function atualizarStatus(pcrId, novoStatus) {
    if (!pcrId || !novoStatus) return false;
    return _patch(
      'pcr_documentos?id=eq.' + encodeURIComponent(pcrId),
      { status: novoStatus, updated_at: new Date().toISOString() }
    );
  }


  // =====================================================================
  //  FORMATACAO
  // =====================================================================

  /**
   * Formata valor como moeda brasileira.
   * @param {number} valor
   * @returns {string} ex: "R$ 1.234,56"
   */
  function formatarMoeda(valor) {
    const v = Number(valor) || 0;
    return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }

  /**
   * Formata data ISO para formato brasileiro.
   * @param {string} iso - ex: "2026-03-24T14:00:00Z"
   * @returns {string} ex: "24/03/2026"
   */
  function formatarData(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return '—';
      return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    } catch {
      return '—';
    }
  }

  /**
   * Formata peso em kg.
   * @param {number} kg
   * @returns {string} ex: "1.234,5 kg"
   */
  function formatarPeso(kg) {
    const v = Number(kg) || 0;
    return v.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' kg';
  }


  // =====================================================================
  //  DASHBOARD — AGREGACOES
  // =====================================================================

  /**
   * Dados agregados por fornecedor para dashboard.
   *
   * @param {object} [periodo] - { de: 'YYYY-MM-DD', ate: 'YYYY-MM-DD' }
   * @returns {Promise<Array<{
   *   fornecedor: string,
   *   total_pcrs: number,
   *   media_pct_defeito: number,
   *   classe_predominante: string,
   *   total_desconto: number,
   *   responsavel: string
   * }>>}
   */
  async function dashboardFornecedor(periodo) {
    const params = ['select=*', 'order=data_recebimento.desc'];
    if (periodo && periodo.de)  params.push('data_recebimento=gte.' + encodeURIComponent(periodo.de));
    if (periodo && periodo.ate) params.push('data_recebimento=lte.' + encodeURIComponent(periodo.ate));

    const rows = await _get('vw_pcr_resumo', params.join('&'));
    if (!rows || !rows.length) return [];

    // Agrupa por fornecedor
    const map = {};
    rows.forEach(r => {
      const nome = r.fornecedor_nome || 'DESCONHECIDO';
      if (!map[nome]) {
        map[nome] = {
          fornecedor:     nome,
          total_pcrs:     0,
          soma_pct:       0,
          classes:        { A: 0, B: 0, C: 0 },
          total_desconto: 0,
          responsavel:    getResponsavel(nome) || '—'
        };
      }
      const g = map[nome];
      g.total_pcrs++;
      g.soma_pct       += Number(r.pct_defeito) || 0;
      g.total_desconto += Number(r.valor_desconto) || 0;

      const cl = (r.classe || 'C').toUpperCase();
      if (g.classes[cl] !== undefined) g.classes[cl]++;
    });

    return Object.values(map).map(g => ({
      fornecedor:           g.fornecedor,
      total_pcrs:           g.total_pcrs,
      media_pct_defeito:    Math.round((g.soma_pct / g.total_pcrs) * 100) / 100,
      classe_predominante:  _classePredominante(g.classes),
      total_desconto:       Math.round(g.total_desconto * 100) / 100,
      responsavel:          g.responsavel
    }));
  }

  /**
   * Dados agregados por responsavel para dashboard.
   *
   * @param {object} [periodo] - { de: 'YYYY-MM-DD', ate: 'YYYY-MM-DD' }
   * @returns {Promise<Array<{
   *   responsavel: string,
   *   total_pcrs: number,
   *   total_fornecedores: number,
   *   media_pct_defeito: number,
   *   total_desconto: number
   * }>>}
   */
  async function dashboardResponsavel(periodo) {
    const params = ['select=*', 'order=data_recebimento.desc'];
    if (periodo && periodo.de)  params.push('data_recebimento=gte.' + encodeURIComponent(periodo.de));
    if (periodo && periodo.ate) params.push('data_recebimento=lte.' + encodeURIComponent(periodo.ate));

    const rows = await _get('vw_pcr_resumo', params.join('&'));
    if (!rows || !rows.length) return [];

    const map = {};
    rows.forEach(r => {
      const resp = getResponsavel(r.fornecedor_nome) || 'SEM CARTEIRA';
      if (!map[resp]) {
        map[resp] = {
          responsavel:   resp,
          total_pcrs:    0,
          fornecedores:  new Set(),
          soma_pct:      0,
          total_desconto:0
        };
      }
      const g = map[resp];
      g.total_pcrs++;
      g.fornecedores.add(r.fornecedor_nome || '');
      g.soma_pct       += Number(r.pct_defeito) || 0;
      g.total_desconto += Number(r.valor_desconto) || 0;
    });

    return Object.values(map).map(g => ({
      responsavel:        g.responsavel,
      total_pcrs:         g.total_pcrs,
      total_fornecedores: g.fornecedores.size,
      media_pct_defeito:  Math.round((g.soma_pct / g.total_pcrs) * 100) / 100,
      total_desconto:     Math.round(g.total_desconto * 100) / 100
    }));
  }

  /** Determina a classe com maior contagem */
  function _classePredominante(classes) {
    let max = 0, classe = 'A';
    for (const [c, n] of Object.entries(classes)) {
      if (n > max) { max = n; classe = c; }
    }
    return classe;
  }


  // =====================================================================
  //  API PUBLICA
  // =====================================================================

  return {
    // Constantes
    DEFEITOS_DESCONTO,
    DEFEITOS_CORRETIVA,
    TODOS_DEFEITOS,
    CARTEIRA,
    PARSE_MAP,

    // Classificacao
    classificar,

    // Calculos
    calcularDescontoQuantidade,
    calcularQualidade,

    // Parse
    parseQualidadeTexto,

    // Responsavel
    getResponsavel,

    // Supabase
    listarPCRs,
    carregarPCR,
    salvarDecisao,
    salvarExecucao,
    atualizarStatus,

    // Formatacao
    formatarMoeda,
    formatarData,
    formatarPeso,

    // Dashboard
    dashboardFornecedor,
    dashboardResponsavel
  };

})();
