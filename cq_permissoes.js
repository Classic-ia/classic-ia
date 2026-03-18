/**
 * cq_permissoes.js — Classic CQ v2
 * Módulo de controle de permissões granulares no frontend
 *
 * Carrega a matriz de permissões do usuário atual via RPC cq_minhas_permissoes()
 * e fornece gates para habilitar/desabilitar ações na UI.
 *
 * Depende de: cq_auth.js (deve ser carregado antes)
 *
 * Uso:
 *   <script src="cq_auth.js"></script>
 *   <script src="cq_permissoes.js"></script>
 *
 *   await CQPermissoes.carregar();
 *   if (CQPermissoes.pode('inspecao_qualidade', 'submeter')) { ... }
 */

const CQPermissoes = (function () {

  let _permissoes = {};  // { modulo: { visualizar, criar, ..., override } }
  let _perfil = null;
  let _carregado = false;

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
      throw new Error(err.message || `RPC ${funcName} falhou: HTTP ${r.status}`);
    }
    return r.json();
  }

  // ── CARREGAR PERMISSÕES ────────────────────────────────────

  /**
   * Carrega a matriz de permissões do usuário atual do backend.
   * Deve ser chamado após CQAuth.init().
   * Resultado é cacheado — chamar carregar(true) para forçar refresh.
   */
  async function carregar(forceRefresh) {
    if (_carregado && !forceRefresh) return;

    try {
      const result = await _rpc('cq_minhas_permissoes');
      if (result && result.ok) {
        _perfil = result.perfil;
        _permissoes = {};
        for (const p of (result.permissoes || [])) {
          _permissoes[p.modulo] = p;
        }
        _carregado = true;
      }
    } catch (e) {
      console.warn('[CQPermissoes] Falha ao carregar permissões:', e.message);
      // Fallback: usar perfil do CQAuth para permissões básicas
      _perfil = CQAuth.getUser()?.perfil || null;
      _carregado = false;
    }
  }

  // ── CONSULTAR PERMISSÃO ────────────────────────────────────

  /**
   * Verifica se o usuário atual tem permissão para executar ação no módulo.
   *
   * @param {string} modulo - ex: 'inspecao_qualidade', 'recebimento_lote'
   * @param {string} acao - ex: 'visualizar', 'criar', 'submeter', 'revisar', 'override'
   * @returns {boolean}
   */
  function pode(modulo, acao) {
    // Admin sempre pode
    if (_perfil === 'administrador') return true;

    const perm = _permissoes[modulo];
    if (!perm) return false;

    return !!perm[acao];
  }

  /**
   * Verifica permissão e retorna mensagem de erro se não tem.
   * Útil para UI: mostrar toast quando ação é bloqueada.
   *
   * @returns {{ permitido: boolean, mensagem: string|null }}
   */
  function verificar(modulo, acao) {
    if (pode(modulo, acao)) {
      return { permitido: true, mensagem: null };
    }
    return {
      permitido: false,
      mensagem: `Sem permissão para "${acao}" no módulo "${modulo}". Perfil: ${_perfil || 'desconhecido'}.`,
    };
  }

  /**
   * Retorna todas as permissões do módulo para o usuário atual.
   * Útil para habilitar/desabilitar múltiplos botões de uma vez.
   *
   * @param {string} modulo
   * @returns {Object} { visualizar, criar, editar, submeter, ... }
   */
  function doModulo(modulo) {
    if (_perfil === 'administrador') {
      // Admin: tudo habilitado
      return {
        visualizar: true, criar: true, editar: true, excluir: true,
        submeter: true, revisar: true, aprovar: true, bloquear: true,
        reabrir: true, importar: true, exportar: true,
        alterar_parametros: true, anexar: true, excluir_evidencia: true,
        ver_auditoria: true, override: true,
      };
    }
    return _permissoes[modulo] || {};
  }

  // ── APLICAR NA UI ──────────────────────────────────────────

  /**
   * Aplica permissões a elementos da página.
   * Elementos com data-permissao="modulo:acao" são mostrados/escondidos.
   * Elementos com data-requer="modulo:acao" são desabilitados se não tem permissão.
   *
   * Uso:
   *   <button data-requer="inspecao_qualidade:submeter">Submeter</button>
   *   <div data-permissao="gestao_usuarios:visualizar">Conteúdo admin</div>
   */
  function aplicarNaPagina() {
    // data-permissao: mostrar/esconder
    document.querySelectorAll('[data-permissao]').forEach(function(el) {
      const [modulo, acao] = el.dataset.permissao.split(':');
      el.style.display = pode(modulo, acao) ? '' : 'none';
    });

    // data-requer: habilitar/desabilitar
    document.querySelectorAll('[data-requer]').forEach(function(el) {
      const [modulo, acao] = el.dataset.requer.split(':');
      if (!pode(modulo, acao)) {
        el.disabled = true;
        el.style.opacity = '0.4';
        el.style.cursor = 'not-allowed';
        el.title = `Sem permissão: ${acao}`;
      }
    });
  }

  /**
   * Retorna o perfil atual.
   */
  function getPerfil() {
    return _perfil;
  }

  /**
   * Verifica se as permissões foram carregadas do backend.
   */
  function isCarregado() {
    return _carregado;
  }

  // ── API PÚBLICA ────────────────────────────────────────────

  return {
    carregar,
    pode,
    verificar,
    doModulo,
    aplicarNaPagina,
    getPerfil,
    isCarregado,
  };

})();
