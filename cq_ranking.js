/**
 * cq_ranking.js — Classic CQ v2
 * Módulo de Ranking de Qualidade por Fornecedor × Produto
 *
 * Depende de: config.js, cq_auth.js
 * Opcional: cq_toast.js (para feedback), cq_cache.js (para cache)
 *
 * Uso:
 *   <script src="config.js"></script>
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_ranking.js"></script>
 *
 *   const ranking = await CQRanking.carregarRankingGeral();
 *   const porProduto = await CQRanking.carregarRankingPorProduto({ fornecedor: '60001' });
 */

const CQRanking = (function () {

  // ── CONSTANTES ────────────────────────────────────────────
  const PRODUTOS = [
    { codigo: 'despojo', nome: 'Despojo', grupo: 'subproduto' },
    { codigo: 'culatra', nome: 'Culatra', grupo: 'subproduto' },
    { codigo: 'couro_suino', nome: 'Couro suíno', grupo: 'couro' },
    { codigo: 'colmeia', nome: 'Colmeia', grupo: 'subproduto' },
    { codigo: 'glote', nome: 'Glote', grupo: 'subproduto' },
    { codigo: 'baco', nome: 'Baço', grupo: 'subproduto' },
    { codigo: 'sebo', nome: 'Sebo', grupo: 'subproduto' },
    { codigo: 'membrana', nome: 'Membrana', grupo: 'subproduto' },
    { codigo: 'orelha_suina', nome: 'Orelha suína', grupo: 'subproduto' },
    { codigo: 'ligamento_cerv', nome: 'Ligamento cervical', grupo: 'subproduto' },
    { codigo: 'calculo_biliar', nome: 'Cálculo biliar', grupo: 'especial' },
    { codigo: 'traqueia', nome: 'Traqueia', grupo: 'subproduto' },
    { codigo: 'capa_vergalho', nome: 'Capa de vergalho', grupo: 'subproduto' },
    { codigo: 'buchao_branco', nome: 'Buchão branco', grupo: 'subproduto' },
    { codigo: 'bucho_suino', nome: 'Bucho suíno', grupo: 'subproduto' },
    { codigo: 'vergalho_suino', nome: 'Vergalho suíno', grupo: 'subproduto' },
    { codigo: 'vergalho_bovino', nome: 'Vergalho bovino', grupo: 'subproduto' },
    { codigo: 'esofago', nome: 'Esôfago', grupo: 'subproduto' },
    { codigo: 'aorta', nome: 'Aorta', grupo: 'subproduto' },
    { codigo: 'bexiga', nome: 'Bexiga', grupo: 'subproduto' },
    { codigo: 'tendao', nome: 'Tendão', grupo: 'subproduto' },
    { codigo: 'pulmao', nome: 'Pulmão', grupo: 'subproduto' },
    { codigo: 'orelha_bovina', nome: 'Orelha bovina', grupo: 'subproduto' },
    { codigo: 'omaso', nome: 'Omaso', grupo: 'subproduto' },
    { codigo: 'couro_verde', nome: 'Couro verde', grupo: 'couro' },
    { codigo: 'couro_wet_blue', nome: 'Couro wet blue', grupo: 'couro' },
    { codigo: 'raspa', nome: 'Raspa', grupo: 'couro' },
    { codigo: 'sal', nome: 'Sal', grupo: 'insumo' },
    { codigo: 'coracao', nome: 'Coração', grupo: 'subproduto' },
    { codigo: 'glandula_mamaria', nome: 'Glândula mamária', grupo: 'subproduto' },
    { codigo: 'ubere', nome: 'Úbere', grupo: 'subproduto' },
  ];

  const DEFEITOS = [
    { codigo: 'rasgado', nome: 'Rasgado', gravidade: 'alta' },
    { codigo: 'furado', nome: 'Furado', gravidade: 'alta' },
    { codigo: 'pelando', nome: 'Pelando', gravidade: 'media' },
    { codigo: 'folha_estourada', nome: 'Folha estourada', gravidade: 'alta' },
    { codigo: 'despojo_sujo', nome: 'Despojo / sujo', gravidade: 'media' },
    { codigo: 'sebo_corte_incorreto', nome: 'Sebo / corte incorreto', gravidade: 'media' },
    { codigo: 'carne_sem_folha', nome: 'Carne sem folha', gravidade: 'alta' },
    { codigo: 'agua', nome: 'Água', gravidade: 'critica' },
    { codigo: 'orelha_misturada', nome: 'Orelha misturada', gravidade: 'media' },
  ];

  // ── HELPERS HTTP ──────────────────────────────────────────
  function _headers() {
    const authToken = typeof CQAuth !== 'undefined' ? CQAuth.getAuthToken() : null;
    return {
      'Content-Type': 'application/json',
      'apikey': CQ_CONFIG.SB_KEY,
      'Authorization': 'Bearer ' + (authToken || CQ_CONFIG.SB_KEY),
    };
  }

  async function _fetch(path, opts = {}) {
    const url = `${CQ_CONFIG.SB_URL}${path}`;
    const r = await fetch(url, {
      headers: { ..._headers(), ...(opts.extraHeaders || {}) },
      method: opts.method || 'GET',
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({ message: r.statusText }));
      throw new Error(err.message || `HTTP ${r.status}`);
    }
    if (opts.noBody) return null;
    const data = await r.json();
    // Extrair total do Content-Range se disponível
    if (opts.extraHeaders?.['Prefer']?.includes('count=exact')) {
      const cr = r.headers.get('content-range');
      const total = cr ? parseInt(cr.split('/')[1]) : data.length;
      return { data, total };
    }
    return data;
  }

  // ── API: CARREGAR RANKINGS ────────────────────────────────

  /** 1. Ranking por fornecedor × produto */
  async function carregarRankingPorProduto(filtros = {}) {
    let qs = 'select=*&order=score.desc';
    if (filtros.fornecedor) qs += `&fornecedor_codigo=eq.${filtros.fornecedor}`;
    if (filtros.produto) qs += `&produto_codigo=eq.${filtros.produto}`;
    return _fetch(`/rest/v1/vw_ranking_fornecedor_produto?${qs}`);
  }

  /** 2. Ranking geral (ponderado por volume) */
  async function carregarRankingGeral(filtros = {}) {
    let qs = 'select=*&order=ranking';
    if (filtros.status) qs += `&status_qualidade=eq.${filtros.status}`;
    if (filtros.uf) qs += `&uf=eq.${filtros.uf}`;
    return _fetch(`/rest/v1/vw_ranking_geral_fornecedor?${qs}`);
  }

  /** 3. Ranking de defeitos por fornecedor */
  async function carregarRankingDefeitos(filtros = {}) {
    let qs = 'select=*&order=fornecedor_codigo,ranking_defeito';
    if (filtros.fornecedor) qs += `&fornecedor_codigo=eq.${filtros.fornecedor}`;
    if (filtros.defeito) qs += `&defeito=eq.${filtros.defeito}`;
    if (filtros.apenasAlertas) qs += '&alerta_acima_5pct=eq.true';
    return _fetch(`/rest/v1/vw_ranking_defeitos_fornecedor?${qs}`);
  }

  /** 4. Tendência 30 dias */
  async function carregarTendencia(filtros = {}) {
    let qs = 'select=*&order=score_30d.desc';
    if (filtros.tendencia) qs += `&tendencia=eq.${filtros.tendencia}`;
    return _fetch(`/rest/v1/vw_ranking_tendencia_30d?${qs}`);
  }

  /** 5. Alertas */
  async function carregarAlertas() {
    return _fetch('/rest/v1/vw_ranking_alertas?select=*&order=severidade.desc');
  }

  // ── API: LOTES ────────────────────────────────────────────

  /** Criar lote de inspeção */
  async function criarLote(dados) {
    const user = typeof CQAuth !== 'undefined' ? CQAuth.getUser() : null;
    const geo = typeof CQAuth !== 'undefined' ? CQAuth.getGeo() : null;

    const payload = {
      fornecedor_codigo: dados.fornecedor_codigo,
      fornecedor_nome: dados.fornecedor_nome,
      fornecedor_id: dados.fornecedor_id || null,  // UUID FK (trigger resolve se null)
      data_inspecao: dados.data_inspecao || new Date().toISOString().split('T')[0],
      inspetor_id: user?.id || null,
      inspetor_nome: user?.nome || null,
      inspetor_email: user?.email || null,
      observacoes: dados.observacoes || null,
      latitude: geo?.latitude || null,
      longitude: geo?.longitude || null,
      geo_dentro_planta: geo?.dentro_planta ?? null,
      session_token: typeof CQAuth !== 'undefined' ? CQAuth.getToken() : null,
    };

    const result = await _fetch('/rest/v1/cq_lote_inspecao', {
      method: 'POST',
      body: payload,
      extraHeaders: { 'Prefer': 'return=representation' },
    });
    return result[0] || result;
  }

  /** Adicionar produto ao lote */
  async function adicionarProduto(loteId, dados) {
    const payload = {
      lote_id: loteId,
      produto_codigo: dados.produto_codigo,
      produto_id: dados.produto_id || null,  // UUID FK (trigger resolve se null)
      class_a: dados.class_a || 0,
      class_b: dados.class_b || 0,
      class_c: dados.class_c || 0,
      rasgado: dados.rasgado || 0,
      furado: dados.furado || 0,
      pelando: dados.pelando || 0,
      folha_estourada: dados.folha_estourada || 0,
      despojo_sujo: dados.despojo_sujo || 0,
      sebo_corte_incorreto: dados.sebo_corte_incorreto || 0,
      carne_sem_folha: dados.carne_sem_folha || 0,
      agua: dados.agua || 0,
      orelha_misturada: dados.orelha_misturada || 0,
      observacoes: dados.observacoes || null,
    };

    return _fetch('/rest/v1/cq_lote_produto', {
      method: 'POST',
      body: payload,
      extraHeaders: { 'Prefer': 'return=representation' },
    });
  }

  /** Finalizar lote */
  async function finalizarLote(loteId) {
    return _fetch(`/rest/v1/cq_lote_inspecao?id=eq.${loteId}`, {
      method: 'PATCH',
      body: { status: 'finalizado' },
      extraHeaders: { 'Prefer': 'return=minimal' },
      noBody: true,
    });
  }

  /** Carregar lote com produtos */
  async function carregarLote(loteId) {
    const [lote, produtos] = await Promise.all([
      _fetch(`/rest/v1/cq_lote_inspecao?id=eq.${loteId}&select=*`),
      _fetch(`/rest/v1/cq_lote_produto?lote_id=eq.${loteId}&select=*&order=produto_codigo`),
    ]);
    return { lote: lote[0], produtos };
  }

  // ── UI: RENDERIZAÇÕES ─────────────────────────────────────

  /** Badge de status de qualidade */
  function renderStatusBadge(status) {
    const cfg = {
      normal:    { cor: '#27ae60', label: 'Normal',    icon: '\u2705' },
      atencao:   { cor: '#f39c12', label: 'Atenção',   icon: '\u26A0\uFE0F' },
      critico:   { cor: '#e74c3c', label: 'Crítico',   icon: '\u274C' },
      bloqueado: { cor: '#95a5a6', label: 'Bloqueado', icon: '\uD83D\uDEAB' },
    };
    const c = cfg[status] || cfg.normal;
    return `<span class="badge" style="background:${c.cor}18;color:${c.cor};border:1px solid ${c.cor}40;">${c.icon} ${c.label}</span>`;
  }

  /** Badge de tendência */
  function renderTendenciaBadge(tendencia) {
    const cfg = {
      melhorando: { cor: '#27ae60', icon: '\u2197\uFE0F', label: 'Melhorando' },
      estavel:    { cor: '#f39c12', icon: '\u2194\uFE0F', label: 'Estável' },
      piorando:   { cor: '#e74c3c', icon: '\u2198\uFE0F', label: 'Piorando' },
    };
    const c = cfg[tendencia] || cfg.estavel;
    return `<span class="badge" style="background:${c.cor}18;color:${c.cor};">${c.icon} ${c.label}</span>`;
  }

  /** Badge de classificação ABC */
  function renderClassBadge(classe) {
    const cfg = {
      A: { cor: '#27ae60', label: 'A' },
      B: { cor: '#f39c12', label: 'B' },
      C: { cor: '#e74c3c', label: 'C' },
    };
    const c = cfg[classe] || { cor: '#666', label: classe || '—' };
    return `<span style="display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:50%;background:${c.cor}20;color:${c.cor};font-weight:800;font-size:12px;border:1px solid ${c.cor}40;">${c.label}</span>`;
  }

  /** Barra de score */
  function renderScoreBar(score) {
    const cor = score >= 70 ? '#27ae60' : score >= 40 ? '#f39c12' : '#e74c3c';
    return `<div class="score-bar">
      <div class="score-track"><div class="score-fill" style="width:${score}%;background:${cor}"></div></div>
      <span class="score-val" style="color:${cor}">${score}</span>
    </div>`;
  }

  /** Barra ABC horizontal */
  function renderBarraABC(pctA, pctB, pctC) {
    return `<div style="display:flex;height:8px;border-radius:4px;overflow:hidden;width:100%;min-width:80px;">
      <div style="width:${pctA || 0}%;background:#27ae60;" title="A: ${pctA}%"></div>
      <div style="width:${pctB || 0}%;background:#f39c12;" title="B: ${pctB}%"></div>
      <div style="width:${pctC || 0}%;background:#e74c3c;" title="C: ${pctC}%"></div>
    </div>
    <div style="display:flex;gap:8px;margin-top:4px;font-size:10px;color:#666;">
      <span style="color:#27ae60;">A ${pctA}%</span>
      <span style="color:#f39c12;">B ${pctB}%</span>
      <span style="color:#e74c3c;">C ${pctC}%</span>
    </div>`;
  }

  /** Mapa de calor de defeitos para um fornecedor */
  function renderHeatmapDefeitos(defeitos) {
    if (!defeitos || !defeitos.length) return '<span style="color:#666;font-size:11px;">Sem dados</span>';
    let html = '<div style="display:flex;flex-wrap:wrap;gap:4px;">';
    for (const d of defeitos) {
      const pct = d.pct_defeito || 0;
      const cor = pct > 10 ? '#e74c3c' : pct > 5 ? '#f39c12' : pct > 2 ? '#3498db' : '#27ae60';
      const opacity = Math.min(1, 0.3 + pct / 15);
      html += `<div title="${d.defeito_nome}: ${pct}% (${d.total_ocorrencias} un.)" style="
        padding:3px 6px;border-radius:4px;font-size:10px;
        background:${cor};opacity:${opacity};color:#fff;cursor:help;
        font-family:'DM Mono',monospace;
      ">${(d.defeito_nome||'').split(' ')[0]||'—'} ${pct}%</div>`;
    }
    html += '</div>';
    return html;
  }

  /** Card de alerta */
  function renderAlertaCard(alerta) {
    const cores = { critica: '#e74c3c', alta: '#f39c12', media: '#3498db' };
    const cor = cores[alerta.severidade] || '#666';
    return `<div style="
      background:${cor}08;border:1px solid ${cor}30;border-radius:8px;
      padding:12px 16px;display:flex;align-items:flex-start;gap:10px;
      margin-bottom:8px;
    ">
      <span style="font-size:16px;">${alerta.tipo_alerta === 'defeito_acima_5pct' ? '\u26A0\uFE0F'
        : alerta.tipo_alerta === 'defeito_recorrente' ? '\uD83D\uDD04'
        : alerta.tipo_alerta === 'inconsistencia_dados' ? '\u274C'
        : '\uD83D\uDCC9'}</span>
      <div style="flex:1;">
        <div style="font-size:13px;font-weight:600;color:${cor};">${_escape(alerta.titulo)}</div>
        <div style="font-size:11px;color:#888;margin-top:2px;">${_escape(alerta.mensagem)}</div>
      </div>
      <span class="badge" style="background:${cor}18;color:${cor};font-size:9px;">${alerta.severidade}</span>
    </div>`;
  }

  /** Formulário de produto (para uso no lançamento) */
  function renderFormProduto(produtoCodigo) {
    const prod = PRODUTOS.find(p => p.codigo === produtoCodigo);
    if (!prod) return '';

    return `<div class="panel" style="margin-bottom:12px;" id="form-prod-${produtoCodigo}">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
        <span style="font-weight:700;color:var(--accent);font-size:13px;">${prod.nome}</span>
        <span style="font-size:10px;color:#666;text-transform:uppercase;">${prod.grupo}</span>
      </div>

      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px;">
        <div>
          <label style="font-size:10px;color:#666;">Classe A</label>
          <input type="number" min="0" value="0" class="field" data-prod="${produtoCodigo}" data-field="class_a"
            style="width:100%;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:#27ae60;font-size:14px;padding:8px;font-weight:700;text-align:center;">
        </div>
        <div>
          <label style="font-size:10px;color:#666;">Classe B</label>
          <input type="number" min="0" value="0" class="field" data-prod="${produtoCodigo}" data-field="class_b"
            style="width:100%;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:#f39c12;font-size:14px;padding:8px;font-weight:700;text-align:center;">
        </div>
        <div>
          <label style="font-size:10px;color:#666;">Classe C</label>
          <input type="number" min="0" value="0" class="field" data-prod="${produtoCodigo}" data-field="class_c"
            style="width:100%;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:#e74c3c;font-size:14px;padding:8px;font-weight:700;text-align:center;">
        </div>
      </div>

      <div style="font-size:10px;color:#666;margin-bottom:6px;text-transform:uppercase;">Defeitos (quantidade)</div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;">
        ${DEFEITOS.map(d => `<div>
          <label style="font-size:9px;color:#555;">${d.nome}</label>
          <input type="number" min="0" value="0" class="field" data-prod="${produtoCodigo}" data-field="${d.codigo}"
            style="width:100%;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:12px;padding:6px;text-align:center;">
        </div>`).join('')}
      </div>
    </div>`;
  }

  /** Extrair dados do formulário de um produto */
  function extrairDadosProduto(produtoCodigo) {
    const fields = document.querySelectorAll(`[data-prod="${produtoCodigo}"]`);
    const dados = { produto_codigo: produtoCodigo };
    fields.forEach(f => {
      dados[f.dataset.field] = parseInt(f.value) || 0;
    });

    // Validar A+B+C > 0
    const total = (dados.class_a || 0) + (dados.class_b || 0) + (dados.class_c || 0);
    if (total === 0) return null; // Produto não inspecionado

    return dados;
  }

  /** Extrair dados de todos os produtos do formulário */
  function extrairTodosProdutos() {
    const resultado = [];
    for (const p of PRODUTOS) {
      const dados = extrairDadosProduto(p.codigo);
      if (dados) resultado.push(dados);
    }
    return resultado;
  }

  // ── UTILITÁRIOS ───────────────────────────────────────────

  function _escape(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function getNomeProduto(codigo) {
    const p = PRODUTOS.find(x => x.codigo === codigo);
    return p ? p.nome : codigo;
  }

  function getNomeDefeito(codigo) {
    const d = DEFEITOS.find(x => x.codigo === codigo);
    return d ? d.nome : codigo;
  }

  // ── API PÚBLICA ───────────────────────────────────────────
  return {
    // Constantes
    PRODUTOS,
    DEFEITOS,

    // API de dados
    carregarRankingGeral,
    carregarRankingPorProduto,
    carregarRankingDefeitos,
    carregarTendencia,
    carregarAlertas,

    // API de lotes
    criarLote,
    adicionarProduto,
    finalizarLote,
    carregarLote,

    // UI
    renderStatusBadge,
    renderTendenciaBadge,
    renderClassBadge,
    renderScoreBar,
    renderBarraABC,
    renderHeatmapDefeitos,
    renderAlertaCard,
    renderFormProduto,
    extrairDadosProduto,
    extrairTodosProdutos,

    // Utilitários
    getNomeProduto,
    getNomeDefeito,
  };

})();
