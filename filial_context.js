/**
 * filial_context.js — Módulo de contexto multi-filial
 * Incluir em qualquer página que precise filtrar por filial.
 * Depende de: localStorage('filial_ativa'), rh_api.js
 *
 * Uso:
 *   const filial = FilialContext.getFilialAtiva();  // UUID ou null
 *   const params = FilialContext.filtroQuery();       // '&filial_id=eq.UUID' ou ''
 *   const rpcParams = FilialContext.filtroRPC();      // {p_filial_id: UUID} ou {}
 */
const FilialContext = (() => {

  function getFilialAtiva() {
    return localStorage.getItem('filial_ativa') || null;
  }

  function setFilialAtiva(id) {
    if (id) localStorage.setItem('filial_ativa', id);
    else localStorage.removeItem('filial_ativa');
  }

  // For REST queries: append to endpoint
  // Example: 'rh_funcionarios?status=eq.ativo' + FilialContext.filtroQuery()
  // Returns: '&filial_id=eq.UUID' or ''
  function filtroQuery(campo) {
    const f = getFilialAtiva();
    if (!f) return '';
    return '&' + (campo || 'filial_id') + '=eq.' + f;
  }

  // For RPC calls: merge into params object
  // Example: API.rpc('hub_resumo', { ...FilialContext.filtroRPC() })
  // Returns: {p_filial_id: UUID} or {}
  function filtroRPC() {
    const f = getFilialAtiva();
    if (!f) return {};
    return { p_filial_id: f };
  }

  // For tables that filter via funcionario_id JOIN
  // Returns subquery filter: '&funcionario_id=in.(uuid1,uuid2,...)'
  // Use only when table has no filial_id column
  // NOTE: This requires loading func IDs first — use filtroQuery when possible
  function filtroFuncionarioIds(funcIds) {
    if (!funcIds || !funcIds.length) return '';
    return '&funcionario_id=in.(' + funcIds.join(',') + ')';
  }

  // Header for RPC calls (alternative approach)
  function headers() {
    const f = getFilialAtiva();
    if (!f) return {};
    return { 'x-filial-ativa': f };
  }

  return { getFilialAtiva, setFilialAtiva, filtroQuery, filtroRPC, filtroFuncionarioIds, headers };
})();
