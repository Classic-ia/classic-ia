/**
 * cq_workflow.js — Classic CQ v2
 * Módulo compartilhado para workflow de inspeções, revisões e NCs
 *
 * Depende de: cq_auth.js (deve ser carregado antes)
 *
 * Uso:
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_workflow.js"></script>
 *   const inspecoes = await CQWorkflow.listarInspecoes({ status: 'aguardando_revisao' });
 */

const CQWorkflow = (function () {

  // ── CONSTANTES DE STATUS ────────────────────────────────────
  const STATUS_WORKFLOW = {
    rascunho:             { label: 'Rascunho',              cor: '#5d7f95', icon: '📝', ordem: 1 },
    em_analise:           { label: 'Em Análise',            cor: '#3498db', icon: '🔍', ordem: 2 },
    aguardando_revisao:   { label: 'Aguardando Revisão',    cor: '#f39c12', icon: '⏳', ordem: 3 },
    revisada:             { label: 'Revisada',              cor: '#8e44ad', icon: '✅', ordem: 4 },
    reprovada:            { label: 'Reprovada',             cor: '#e74c3c', icon: '❌', ordem: 5 },
    aprovada:             { label: 'Aprovada',              cor: '#27ae60', icon: '✔️', ordem: 6 },
    encerrada:            { label: 'Encerrada',             cor: '#2c3e50', icon: '🔒', ordem: 7 },
    cancelada:            { label: 'Cancelada',             cor: '#95a5a6', icon: '🚫', ordem: 8 },
  };

  const STATUS_FINAL = {
    aprovado:  { label: 'Aprovado',  cor: '#27ae60', icon: '✅' },
    ressalva:  { label: 'Ressalva',  cor: '#f39c12', icon: '⚠️' },
    bloqueado: { label: 'Bloqueado', cor: '#e74c3c', icon: '🚫' },
  };

  // Mapa de transições permitidas (espelho do banco — para UI)
  const TRANSICOES = {
    rascunho:           ['em_analise'],
    em_analise:         ['aguardando_revisao'],
    aguardando_revisao: ['revisada', 'reprovada', 'rascunho'], // rascunho = devolução
    revisada:           ['aprovada'],
    reprovada:          ['rascunho'],  // reabertura (só admin)
    aprovada:           ['encerrada'],
    encerrada:          [],
    cancelada:          [],
  };

  // ── HELPERS HTTP ────────────────────────────────────────────

  function _sbUrl() { return CQAuth.getSbUrl(); }
  function _sbKey() { return CQAuth.getSbKey(); }

  function _headers() {
    return {
      'Content-Type':  'application/json',
      'apikey':        _sbKey(),
      'Authorization': 'Bearer ' + (CQAuth.getAuthToken() || _sbKey()),
    };
  }

  async function _fetch(path, opts = {}) {
    const url = `${_sbUrl()}${path}`;
    const r = await fetch(url, {
      headers: { ..._headers(), ...(opts.extraHeaders || {}) },
      method: opts.method || 'GET',
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({ message: r.statusText }));
      throw new Error(err.message || err.error || `HTTP ${r.status}`);
    }
    if (opts.noBody) return null;
    return r.json();
  }

  // ── API: INSPEÇÕES ──────────────────────────────────────────

  /**
   * Listar inspeções com filtros e paginação.
   * Usa a view vw_inspecoes_workflow para dados enriquecidos.
   * @param {Object} filtros - { status, fornecedor, produto, inspetor, dataInicio, dataFim }
   * @param {number} pagina - página (1-based)
   * @param {number} porPagina - itens por página (default 20)
   * @returns {{ dados: Array, total: number }}
   */
  async function listarInspecoes(filtros = {}, pagina = 1, porPagina = 20) {
    let qs = 'select=*';
    const params = [];

    if (filtros.status)      params.push(`status_workflow=eq.${filtros.status}`);
    if (filtros.fornecedor)  params.push(`fornecedor=ilike.*${filtros.fornecedor}*`);
    if (filtros.produto)     params.push(`produto=ilike.*${filtros.produto}*`);
    if (filtros.inspetor)    params.push(`usuario_nome=ilike.*${filtros.inspetor}*`);
    if (filtros.dataInicio)  params.push(`criado_em=gte.${filtros.dataInicio}`);
    if (filtros.dataFim)     params.push(`criado_em=lte.${filtros.dataFim}T23:59:59`);

    if (params.length) qs += '&' + params.join('&');

    const offset = (pagina - 1) * porPagina;
    qs += `&order=criado_em.desc&limit=${porPagina}&offset=${offset}`;

    const dados = await _fetch(`/rest/v1/vw_inspecoes_workflow?${qs}`, {
      extraHeaders: { 'Prefer': 'count=exact' },
    });

    return { dados, total: dados.length };
  }

  /**
   * Carregar uma inspeção completa por ID (da tabela principal)
   */
  async function carregarInspecao(id) {
    const rows = await _fetch(`/rest/v1/registros_cq_inspecao?id=eq.${id}&select=*`);
    if (!rows || rows.length === 0) throw new Error('Inspeção não encontrada');
    return rows[0];
  }

  /**
   * Carregar dados enriquecidos de uma inspeção (da view)
   */
  async function carregarInspecaoView(id) {
    const rows = await _fetch(`/rest/v1/vw_inspecoes_workflow?id=eq.${id}&select=*`);
    if (!rows || rows.length === 0) throw new Error('Inspeção não encontrada');
    return rows[0];
  }

  /**
   * Criar nova inspeção (POST)
   */
  async function criarInspecao(payload) {
    const user = CQAuth.getUser();
    payload.status_workflow = 'rascunho';
    payload.criador_id = user.id;
    payload.usuario_id = user.id;
    // IDs UUID são resolvidos automaticamente pelo trigger trg_resolver_ids_inspecao
    // se fornecedor_id/produto_id/motorista_id não forem passados, o trigger
    // resolve a partir de fornecedor_codigo/fornecedor/produto/motorista/placa_caminhao

    await _fetch('/rest/v1/registros_cq_inspecao', {
      method: 'POST',
      body: payload,
      extraHeaders: { 'Prefer': 'return=representation' },
    });

    return true;
  }

  /**
   * Atualizar inspeção existente (PATCH)
   */
  async function atualizarInspecao(id, dados) {
    await _fetch(`/rest/v1/registros_cq_inspecao?id=eq.${id}`, {
      method: 'PATCH',
      body: dados,
      extraHeaders: { 'Prefer': 'return=minimal' },
      noBody: true,
    });
    return true;
  }

  // ── API: TRANSIÇÃO DE STATUS ────────────────────────────────

  /**
   * Transicionar status de workflow (via RPC no banco)
   * @param {string} inspecaoId - UUID da inspeção
   * @param {string} novoStatus - novo status_workflow
   * @returns {{ sucesso: boolean, mensagem: string }}
   */
  async function transicionarStatus(inspecaoId, novoStatus) {
    const result = await _fetch('/rest/v1/rpc/cq_transicao_status', {
      method: 'POST',
      body: { p_inspecao_id: inspecaoId, p_novo_status: novoStatus },
    });
    return result;
  }

  // ── API: REVISÕES ───────────────────────────────────────────

  /**
   * Listar revisões de uma inspeção
   */
  async function listarRevisoes(inspecaoId) {
    return _fetch(`/rest/v1/cq_revisoes?inspecao_id=eq.${inspecaoId}&select=*&order=criado_em.desc`);
  }

  /**
   * Criar revisão + transicionar status
   * DELEGADO AO BACKEND: usa cq_criar_revisao() RPC para operação atômica.
   * A lógica de mapeamento decisão→status e validação de permissões
   * está centralizada no banco (p1_centralizar_regras_negocio.sql).
   *
   * @param {string} inspecaoId
   * @param {Object} dados - { decisao: 'aprovada'|'reprovada'|'devolvida', comentario }
   */
  async function criarRevisao(inspecaoId, dados) {
    // Usar CQRegras se disponível (operação atômica no backend)
    if (typeof CQRegras !== 'undefined') {
      return CQRegras.criarRevisao(inspecaoId, dados.decisao, dados.comentario || null);
    }

    // Fallback legado (para páginas que ainda não carregam cq_regras.js)
    const result = await _fetch('/rest/v1/rpc/cq_criar_revisao', {
      method: 'POST',
      body: {
        p_inspecao_id: inspecaoId,
        p_decisao: dados.decisao,
        p_comentario: dados.comentario || null,
      },
    });
    return result;
  }

  // ── API: NÃO CONFORMIDADES ─────────────────────────────────

  /**
   * Listar NCs de uma inspeção
   */
  async function listarNCs(inspecaoId) {
    return _fetch(`/rest/v1/cq_nao_conformidades?inspecao_id=eq.${inspecaoId}&select=*&order=criado_em.desc`);
  }

  /**
   * Criar não conformidade
   */
  async function criarNC(inspecaoId, dados) {
    const user = CQAuth.getUser();

    return _fetch('/rest/v1/cq_nao_conformidades', {
      method: 'POST',
      body: {
        inspecao_id:       inspecaoId,
        tipo:              dados.tipo,
        gravidade:         dados.gravidade,
        descricao:         dados.descricao,
        acao_imediata:     dados.acao_imediata || null,
        responsavel_id:    dados.responsavel_id || null,
        responsavel_nome:  dados.responsavel_nome || null,
        usuario_criacao:   user.id,
        status:            'aberta',
      },
      extraHeaders: { 'Prefer': 'return=representation' },
    });
  }

  /**
   * Atualizar NC
   */
  async function atualizarNC(ncId, dados) {
    await _fetch(`/rest/v1/cq_nao_conformidades?id=eq.${ncId}`, {
      method: 'PATCH',
      body: dados,
      extraHeaders: { 'Prefer': 'return=minimal' },
      noBody: true,
    });
    return true;
  }

  // ── API: ANEXOS ─────────────────────────────────────────────

  /**
   * Listar anexos de uma inspeção
   */
  async function listarAnexos(inspecaoId) {
    return _fetch(`/rest/v1/cq_anexos?inspecao_id=eq.${inspecaoId}&ativo=eq.true&select=*&order=criado_em.desc`);
  }

  /**
   * Registrar anexo (metadados — upload do arquivo é separado via Storage)
   */
  async function registrarAnexo(dados) {
    const user = CQAuth.getUser();
    return _fetch('/rest/v1/cq_anexos', {
      method: 'POST',
      body: {
        ...dados,
        usuario_id:    user.id,
        usuario_email: user.email,
        origem_upload: 'web',
        ativo:         true,
      },
      extraHeaders: { 'Prefer': 'return=representation' },
    });
  }

  // ── API: PARÂMETROS DE INSPEÇÃO ─────────────────────────────

  /**
   * Buscar parâmetros de inspeção para um produto (via RPC)
   */
  async function getParametros(produto) {
    const result = await _fetch('/rest/v1/rpc/cq_get_parametros', {
      method: 'POST',
      body: { p_produto: produto },
    });
    return result;
  }

  // ── API: USUÁRIOS ATIVOS ────────────────────────────────────

  /**
   * Listar usuários ativos (para selects de responsável)
   */
  async function listarUsuarios() {
    return _fetch('/rest/v1/cq_usuarios?ativo=eq.true&select=id,nome,email,perfil&order=nome');
  }

  // ── UI: RENDERIZAR STATUS BADGE ─────────────────────────────

  /**
   * Retorna HTML de um badge de status workflow
   */
  function renderStatusBadge(status, opts = {}) {
    const cfg = STATUS_WORKFLOW[status];
    if (!cfg) return `<span style="color:#999">${status || '—'}</span>`;

    const tamanho = opts.grande ? 'font-size:14px;padding:6px 14px;' : 'font-size:11px;padding:3px 10px;';
    const pulsar  = status === 'aguardando_revisao' ? 'animation:cqPulse 2s infinite;' : '';

    return `<span style="
      display:inline-flex;align-items:center;gap:5px;
      background:${cfg.cor}18;color:${cfg.cor};
      border:1px solid ${cfg.cor}40;border-radius:20px;
      ${tamanho}font-weight:600;white-space:nowrap;${pulsar}
      font-family:'DM Sans',sans-serif;
    ">${cfg.icon} ${cfg.label}</span>`;
  }

  /**
   * Retorna HTML de um badge de status final (técnico)
   */
  function renderStatusFinalBadge(statusFinal) {
    const cfg = STATUS_FINAL[statusFinal];
    if (!cfg) return '';
    return `<span style="
      display:inline-flex;align-items:center;gap:4px;
      background:${cfg.cor}18;color:${cfg.cor};
      border:1px solid ${cfg.cor}40;border-radius:20px;
      font-size:11px;padding:3px 10px;font-weight:600;
      font-family:'DM Sans',sans-serif;
    ">${cfg.icon} ${cfg.label}</span>`;
  }

  // ── UI: TIMELINE DE WORKFLOW ────────────────────────────────

  /**
   * Renderiza timeline visual do workflow
   * @param {Object} inspecao - dados da inspeção
   * @param {Array} revisoes - lista de revisões
   * @returns {string} HTML
   */
  function renderTimeline(inspecao, revisoes = []) {
    const eventos = [];

    // Criação
    eventos.push({
      data: inspecao.criado_em,
      titulo: 'Inspeção criada',
      detalhe: inspecao.usuario_nome || inspecao.usuario_email || '—',
      cor: STATUS_WORKFLOW.rascunho.cor,
      icon: '📝',
    });

    // Revisões
    for (const rev of revisoes) {
      const decisaoLabel = rev.decisao === 'aprovada' ? 'Aprovada pelo revisor'
        : rev.decisao === 'reprovada' ? 'Reprovada pelo revisor'
        : 'Devolvida para correção';
      eventos.push({
        data: rev.criado_em,
        titulo: decisaoLabel,
        detalhe: `${rev.revisor_nome} — ${rev.comentario || ''}`,
        cor: rev.decisao === 'aprovada' ? '#27ae60' : rev.decisao === 'reprovada' ? '#e74c3c' : '#f39c12',
        icon: rev.decisao === 'aprovada' ? '✅' : rev.decisao === 'reprovada' ? '❌' : '↩️',
      });
    }

    // Ordenar por data
    eventos.sort((a, b) => new Date(a.data) - new Date(b.data));

    if (eventos.length === 0) return '<p style="color:#5d7f95;font-size:12px;">Sem eventos.</p>';

    let html = '<div style="padding:8px 0;">';
    for (let i = 0; i < eventos.length; i++) {
      const ev = eventos[i];
      const dataFmt = _formatarData(ev.data);
      const isLast = i === eventos.length - 1;
      html += `
        <div style="display:flex;gap:12px;position:relative;padding-bottom:${isLast ? '0' : '16px'};">
          <div style="display:flex;flex-direction:column;align-items:center;flex-shrink:0;">
            <div style="
              width:28px;height:28px;border-radius:50%;background:${ev.cor}20;
              border:2px solid ${ev.cor};display:flex;align-items:center;
              justify-content:center;font-size:13px;
            ">${ev.icon}</div>
            ${!isLast ? `<div style="width:2px;flex:1;background:${ev.cor}30;margin-top:4px;"></div>` : ''}
          </div>
          <div style="flex:1;padding-top:3px;">
            <div style="font-size:13px;font-weight:600;color:#e8f0f5;">${ev.titulo}</div>
            <div style="font-size:11px;color:#5d7f95;margin-top:2px;">${ev.detalhe}</div>
            <div style="font-size:10px;color:#3d5a6e;margin-top:2px;">${dataFmt}</div>
          </div>
        </div>`;
    }
    html += '</div>';
    return html;
  }

  // ── UI: AÇÕES PERMITIDAS ────────────────────────────────────

  /**
   * Retorna lista de ações que o usuário atual pode executar na inspeção.
   * NOTA: Quando CQRegras está disponível, use CQRegras.acoesPermitidas(id)
   * para obter as ações do backend (fonte da verdade com RBAC real).
   * Esta versão local é mantida como fallback para UI instantânea.
   *
   * @param {Object} inspecao - { status_workflow, criador_id }
   * @returns {Array<{ acao, label, cor, icon, confirm }>}
   */
  function getAcoesPermitidas(inspecao) {
    const user = CQAuth.getUser();
    if (!user) return [];

    const perfil   = user.perfil;
    const userId   = user.id;
    const status   = inspecao.status_workflow;
    const criador  = inspecao.criador_id;
    const ehCriador = userId === criador;
    const acoes    = [];

    switch (status) {
      case 'rascunho':
        if (ehCriador || perfil === 'administrador') {
          acoes.push({ acao: 'editar',       label: 'Editar',                cor: '#3498db', icon: '✏️' });
          acoes.push({ acao: 'em_analise',   label: 'Enviar para Análise',   cor: '#3498db', icon: '🔍', confirm: true });
        }
        break;

      case 'em_analise':
        if (ehCriador || perfil === 'administrador') {
          acoes.push({ acao: 'editar',                label: 'Editar',                   cor: '#3498db', icon: '✏️' });
          acoes.push({ acao: 'aguardando_revisao',    label: 'Enviar para Revisão',      cor: '#f39c12', icon: '⏳', confirm: true });
        }
        break;

      case 'aguardando_revisao':
        if (!ehCriador && ['administrador','qualidade','diretoria'].includes(perfil)) {
          acoes.push({ acao: 'revisar', label: 'Revisar', cor: '#8e44ad', icon: '📋' });
        }
        break;

      case 'revisada':
        if (!ehCriador && ['administrador','diretoria'].includes(perfil)) {
          acoes.push({ acao: 'aprovada', label: 'Aprovar', cor: '#27ae60', icon: '✔️', confirm: true });
        }
        break;

      case 'reprovada':
        if (['administrador','qualidade'].includes(perfil)) {
          acoes.push({ acao: 'abrir_nc', label: 'Abrir NC', cor: '#e74c3c', icon: '⚠️' });
        }
        if (perfil === 'administrador') {
          acoes.push({ acao: 'rascunho', label: 'Reabrir', cor: '#f39c12', icon: '↩️', confirm: true });
        }
        break;

      case 'aprovada':
        if (perfil === 'administrador') {
          acoes.push({ acao: 'encerrada', label: 'Encerrar', cor: '#2c3e50', icon: '🔒', confirm: true });
        }
        break;
    }

    // Cancelar: administrador pode cancelar qualquer status (exceto já finalizado)
    if (perfil === 'administrador' && !['encerrada','cancelada'].includes(status)) {
      acoes.push({ acao: 'cancelada', label: 'Cancelar', cor: '#95a5a6', icon: '🚫', confirm: true });
    }

    return acoes;
  }

  /**
   * Verifica se a inspeção é editável pelo usuário atual
   */
  function isEditavel(inspecao) {
    const user = CQAuth.getUser();
    if (!user) return false;

    const status = inspecao.status_workflow;
    // Só rascunho e em_analise são editáveis
    if (!['rascunho', 'em_analise'].includes(status)) return false;

    // Só criador ou admin pode editar
    return (user.id === inspecao.criador_id || user.perfil === 'administrador');
  }

  /**
   * Verifica se o usuário pode revisar esta inspeção
   */
  function podeRevisar(inspecao) {
    const user = CQAuth.getUser();
    if (!user) return false;
    if (inspecao.status_workflow !== 'aguardando_revisao') return false;
    if (user.id === inspecao.criador_id) return false;
    return ['administrador', 'qualidade', 'diretoria'].includes(user.perfil);
  }

  // ── UI: BARRA DE AÇÕES ─────────────────────────────────────

  /**
   * Renderiza barra de ações para uma inspeção
   * @returns {string} HTML
   */
  function renderBarraAcoes(inspecao) {
    const acoes = getAcoesPermitidas(inspecao);
    if (acoes.length === 0) {
      return '<div style="color:#5d7f95;font-size:12px;padding:8px 0;">Nenhuma ação disponível.</div>';
    }

    let html = '<div style="display:flex;flex-wrap:wrap;gap:8px;padding:8px 0;">';
    for (const a of acoes) {
      const dataConfirm = a.confirm ? `data-confirm="true"` : '';
      html += `<button
        class="cq-wf-btn"
        data-acao="${a.acao}"
        data-inspecao-id="${inspecao.id}"
        ${dataConfirm}
        style="
          display:inline-flex;align-items:center;gap:6px;
          padding:8px 16px;border-radius:8px;
          background:${a.cor}18;color:${a.cor};
          border:1px solid ${a.cor}40;
          font-size:13px;font-weight:600;cursor:pointer;
          font-family:'DM Sans',sans-serif;
          transition:all .2s;
        "
        onmouseover="this.style.background='${a.cor}30'"
        onmouseout="this.style.background='${a.cor}18'"
      >${a.icon} ${a.label}</button>`;
    }
    html += '</div>';
    return html;
  }

  // ── UI: OVERLAY DE BLOQUEIO ─────────────────────────────────

  /**
   * Renderiza banner/overlay de bloqueio para inspeções não editáveis
   */
  function renderBloqueio(inspecao) {
    const status = inspecao.status_workflow;
    if (['rascunho', 'em_analise'].includes(status)) return '';

    const msgs = {
      aguardando_revisao: 'Aguardando revisão — edição bloqueada',
      revisada:           'Revisada — aguardando aprovação final',
      aprovada:           'Aprovada — edição bloqueada permanentemente',
      encerrada:          'Encerrada — registro finalizado',
      cancelada:          'Cancelada — registro inativo',
      reprovada:          'Reprovada — aguardando tratamento',
    };

    const msg  = msgs[status] || 'Edição bloqueada';
    const cfg  = STATUS_WORKFLOW[status] || { cor: '#95a5a6', icon: '🔒' };

    return `<div style="
      background:${cfg.cor}12;border:1px solid ${cfg.cor}30;
      border-radius:10px;padding:12px 18px;margin-bottom:16px;
      display:flex;align-items:center;gap:10px;
      font-family:'DM Sans',sans-serif;
    ">
      <span style="font-size:20px;">${cfg.icon}</span>
      <div>
        <div style="font-size:13px;font-weight:600;color:${cfg.cor};">${msg}</div>
        <div style="font-size:11px;color:#5d7f95;margin-top:2px;">
          Status: ${renderStatusBadge(status)}
        </div>
      </div>
    </div>`;
  }

  // ── UTILITÁRIOS ─────────────────────────────────────────────

  function _formatarData(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleDateString('pt-BR', {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  }

  /**
   * Injeta CSS de animação de pulso (chamar uma vez na página)
   */
  function injetarCSS() {
    if (document.getElementById('cq-wf-css')) return;
    const style = document.createElement('style');
    style.id = 'cq-wf-css';
    style.textContent = `
      @keyframes cqPulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.6; }
      }
      .cq-wf-btn:active {
        transform: scale(0.96);
      }
    `;
    document.head.appendChild(style);
  }

  // ── API PÚBLICA ─────────────────────────────────────────────

  return {
    // Constantes
    STATUS_WORKFLOW,
    STATUS_FINAL,
    TRANSICOES,

    // API
    listarInspecoes,
    carregarInspecao,
    carregarInspecaoView,
    criarInspecao,
    atualizarInspecao,
    transicionarStatus,
    listarRevisoes,
    criarRevisao,
    listarNCs,
    criarNC,
    atualizarNC,
    listarAnexos,
    registrarAnexo,
    getParametros,
    listarUsuarios,

    // UI
    renderStatusBadge,
    renderStatusFinalBadge,
    renderTimeline,
    renderBarraAcoes,
    renderBloqueio,
    getAcoesPermitidas,
    isEditavel,
    podeRevisar,
    injetarCSS,

    // Utilitários
    formatarData: _formatarData,
  };

})();
