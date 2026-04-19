/**
 * page_bootstrap.js — JR Terraplanagem
 * Lifecycle padrao de pagina desktop.
 *
 * USO:
 *   PageBootstrap.init({
 *     page: 'maquinas',
 *     title: 'Maquinas',
 *     perfisPermitidos: ['admin', 'gestor', 'operador'],
 *     loadData: async () => API.get('jr_maquinas?order=codigo'),
 *     render: (data) => { ... },
 *     bindEvents: () => { ... },
 *   });
 */

const PageBootstrap = (() => {
  async function init(opts) {
    const { page, title, breadcrumb, perfisPermitidos, loadData, render, bindEvents, onError } = opts;

    const user = await Shell.init({ page, title, breadcrumb, perfisPermitidos });
    if (!user) return null;

    API.setPerfil(user.perfil);
    API.logEvent('PAGE_LOAD', { page, perfil: user.perfil });

    if (typeof bindEvents === 'function') {
      try { bindEvents(user); } catch (e) {
        console.error('[Bootstrap] bindEvents error:', e);
        API.logEvent('UI_ERROR', { phase: 'bindEvents', error: e.message });
      }
    }

    if (typeof loadData === 'function') {
      try {
        const result = await loadData(user);
        if (typeof render === 'function') {
          try { render(result, user); } catch (e) {
            console.error('[Bootstrap] render error:', e);
            API.logEvent('UI_ERROR', { phase: 'render', error: e.message });
            showPageError('Erro ao renderizar dados: ' + e.message);
          }
        }
      } catch (e) {
        console.error('[Bootstrap] loadData error:', e);
        API.logEvent('API_ERROR', { phase: 'loadData', error: e.message });
        if (typeof onError === 'function') onError(e);
        else showPageError('Erro ao carregar dados: ' + e.message);
      }
    }

    return user;
  }

  function showPageError(msg) {
    const content = document.getElementById('page-content') || document.getElementById('shell-content');
    if (content) {
      content.innerHTML = `
        <div style="text-align:center;padding:60px 20px;">
          <div style="font-size:48px;margin-bottom:16px;">&#9888;</div>
          <div style="font-size:16px;font-weight:600;color:#1E293B;margin-bottom:8px;">Erro ao carregar pagina</div>
          <div style="font-size:14px;color:#64748B;margin-bottom:24px;">${API.esc(msg)}</div>
          <button onclick="location.reload()" style="padding:8px 24px;background:#2563EB;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px;">Tentar novamente</button>
        </div>`;
    }
  }

  return { init, showPageError };
})();
