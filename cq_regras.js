/**
 * cq_regras.js — Classic CQ v2
 * Módulo centralizado de regras de negócio via RPC
 *
 * TODAS as regras de negócio estão no backend (Supabase SQL functions).
 * Este módulo é o ponto único de acesso do frontend para essas funções.
 * O frontend NÃO DEVE calcular classificação, score, IQF ou decisões.
 *
 * Depende de: cq_auth.js (deve ser carregado antes)
 *
 * Uso:
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_regras.js"></script>
 *
 *   const decisao = await CQRegras.motorDecisao(12.5, 'Omaso Bovino');
 *   const score   = await CQRegras.scoreLote(80, 15, 5);
 *   const iqf     = await CQRegras.calcularIQF('60001');
 *   const result  = await CQRegras.submeterInspecao(inspecaoId);
 */

const CQRegras = (function () {

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

  async function _rpc(funcName, params = {}) {
    const url = `${_sbUrl()}/rest/v1/rpc/${funcName}`;
    const r = await fetch(url, {
      method: 'POST',
      headers: _headers(),
      body: JSON.stringify(params),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({ message: r.statusText }));
      throw new Error(err.message || err.error || `RPC ${funcName} falhou: HTTP ${r.status}`);
    }
    return r.json();
  }

  // ── 1. MOTOR DE DECISÃO ────────────────────────────────────
  // Substitui: pct<=5?'aprovado':pct<=15?'ressalva':'bloqueado' (inspecao_qualidade.html)
  // Substitui: p <= 5 ? 'A' : p <= 12 ? 'B' : 'C' (dashboard_cq.html)
  // Usa limites de cq_parametros_inspecao (não mais hardcoded)

  /**
   * Classificar inspeção (A/B/C) + status final (aprovado/ressalva/bloqueado)
   * @param {number} percentualDefeitos - % de defeitos
   * @param {string|null} produtoNome - nome do produto (ou null para padrão)
   * @returns {Promise<{classificacao, status_final, percentual_defeitos, limite_aprovacao, limite_ressalva, requer_analise_critica}>}
   */
  async function motorDecisao(percentualDefeitos, produtoNome = null) {
    return _rpc('cq_motor_decisao', {
      p_percentual_defeitos: percentualDefeitos,
      p_produto_nome: produtoNome,
    });
  }

  /**
   * Versão local para exibição imediata (readonly, SEM decidir status).
   * Usa os mesmos limites padrão mas NÃO é a fonte da verdade.
   * Útil para feedback visual instantâneo durante digitação.
   * NOTA: o status final REAL é sempre o retornado pelo backend.
   */
  function motorDecisaoLocal(percentualDefeitos, limiteAprovacao, limiteRessalva) {
    const la = limiteAprovacao || 5.0;
    const lr = limiteRessalva || 15.0;
    if (percentualDefeitos <= la) return { classificacao: 'A', status_final: 'aprovado', preview: true };
    if (percentualDefeitos <= lr) return { classificacao: 'B', status_final: 'ressalva', preview: true };
    return { classificacao: 'C', status_final: 'bloqueado', preview: true };
  }

  // ── 2. SCORE POR LOTE ─────────────────────────────────────
  // Substitui: 100 - (C/tot*50) - (B/tot*20) + (A/tot*20) em 4+ HTML files

  /**
   * Calcular score de qualidade do lote
   * @returns {Promise<{score, classificacao_lote, pct_a, pct_b, pct_c, total}>}
   */
  async function scoreLote(classA, classB, classC) {
    return _rpc('cq_calcular_score_lote', {
      p_class_a: classA,
      p_class_b: classB,
      p_class_c: classC,
    });
  }

  /**
   * Versão local para exibição instantânea (mesma fórmula, sem rede).
   * Para dashboards que precisam calcular in-place sem aguardar RPC.
   */
  function scoreLoteLocal(classA, classB, classC) {
    const a = classA || 0, b = classB || 0, c = classC || 0;
    const total = a + b + c;
    if (total === 0) return { score: 0, classificacao_lote: null, pct_a: 0, pct_b: 0, pct_c: 0, total: 0, preview: true };
    const pctA = (a / total) * 100;
    const pctB = (b / total) * 100;
    const pctC = (c / total) * 100;
    const score = Math.max(0, Math.min(100, 100 - (pctC * 0.50) - (pctB * 0.20) + (pctA * 0.20)));
    const cl = score >= 80 ? 'A' : score >= 50 ? 'B' : 'C';
    return {
      score: Math.round(score * 10) / 10,
      classificacao_lote: cl,
      pct_a: Math.round(pctA * 10) / 10,
      pct_b: Math.round(pctB * 10) / 10,
      pct_c: Math.round(pctC * 10) / 10,
      total,
      preview: true,
    };
  }

  // ── 3. IQF (ÍNDICE DE QUALIDADE DO FORNECEDOR) ────────────

  /**
   * Calcular IQF de um fornecedor (janela de N dias)
   * @param {string} fornecedorCodigo
   * @param {number} janelaDias - padrão 90
   * @returns {Promise<{iqf_score, classificacao, tendencia, ...}>}
   */
  async function calcularIQF(fornecedorCodigo, janelaDias = 90) {
    return _rpc('cq_calcular_iqf', {
      p_fornecedor_codigo: fornecedorCodigo,
      p_janela_dias: janelaDias,
    });
  }

  // ── 4. VALIDAÇÕES ──────────────────────────────────────────

  /**
   * Validar dados de inspeção
   * @param {Object} dados - campos da inspeção
   * @returns {Promise<{valido, erros, total_erros, total_criticos}>}
   */
  async function validarInspecao(dados) {
    return _rpc('cq_validar_inspecao', { p_dados: dados });
  }

  /**
   * Validar dados de carga
   * @param {Object} dados - campos da carga
   * @returns {Promise<{valido, divergencias, total_criticas, total_atencao, total_informativas}>}
   */
  async function validarCarga(dados) {
    return _rpc('cq_validar_carga', { p_dados: dados });
  }

  /**
   * Avaliar se fornecedor é crítico (%C elevado)
   * @param {string} fornecedorCodigo
   * @param {number|null} pctC - se null, calcula dos últimos 90 dias
   * @returns {Promise<{critico, status_sugerido, gravidade, pct_c, mensagem}>}
   */
  async function avaliarFornecedorCritico(fornecedorCodigo, pctC = null) {
    return _rpc('cq_avaliar_fornecedor_critico', {
      p_fornecedor_codigo: fornecedorCodigo,
      p_pct_c: pctC,
    });
  }

  // ── 5. AÇÕES DE WORKFLOW ───────────────────────────────────

  /**
   * Submeter inspeção (validar + classificar + transicionar)
   * @param {string} inspecaoId - UUID
   * @returns {Promise<{ok, inspecao_id, total_defeitos, percentual_defeitos, decisao, status_workflow}>}
   */
  async function submeterInspecao(inspecaoId) {
    return _rpc('cq_submeter_inspecao', { p_inspecao_id: inspecaoId });
  }

  /**
   * Obter ações permitidas para inspeção (para o usuário atual)
   * @param {string} inspecaoId - UUID
   * @returns {Promise<Array<{acao, label, confirm}>>}
   */
  async function acoesPermitidas(inspecaoId) {
    return _rpc('cq_acoes_permitidas', { p_inspecao_id: inspecaoId });
  }

  /**
   * Criar revisão (atômica: insere revisão + transiciona status)
   * @param {string} inspecaoId - UUID
   * @param {string} decisao - 'aprovada'|'reprovada'|'devolvida'
   * @param {string|null} comentario
   * @returns {Promise<{ok, decisao, de, para, revisor}>}
   */
  async function criarRevisao(inspecaoId, decisao, comentario = null) {
    return _rpc('cq_criar_revisao', {
      p_inspecao_id: inspecaoId,
      p_decisao: decisao,
      p_comentario: comentario,
    });
  }

  // ── CONSTANTES DE LIMITES (para UI — feedback instantâneo) ─
  // Estes valores devem ser iguais aos defaults de cq_parametros_inspecao.
  // A fonte da verdade é SEMPRE o backend.
  const LIMITES_DEFAULT = {
    APROVACAO: 5.0,
    RESSALVA: 15.0,
    PCT_C_ATENCAO: 10.0,
    PCT_C_CRITICO: 15.0,
    PCT_C_BLOQUEIO: 25.0,
    SCORE_ATENCAO: 60,
    SCORE_CRITICO: 30,
    JANELA_DIAS: 90,
  };

  // ── API PÚBLICA ────────────────────────────────────────────

  return {
    // Backend RPCs (fonte da verdade)
    motorDecisao,
    scoreLote,
    calcularIQF,
    validarInspecao,
    validarCarga,
    avaliarFornecedorCritico,
    submeterInspecao,
    acoesPermitidas,
    criarRevisao,

    // Local helpers (preview, sem autoridade)
    motorDecisaoLocal,
    scoreLoteLocal,

    // Constantes
    LIMITES_DEFAULT,
  };

})();
