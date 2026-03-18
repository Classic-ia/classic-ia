// auth.js — incluir em todas as páginas protegidas
// Uso: <script src="auth.js"></script>
// Depois chamar: await authGuard(['administrador','qualidade']) — passa array de perfis permitidos

// Usa config.js centralizado (deve ser carregado antes deste script)
// <script src="config.js"></script>
// <script src="auth.js"></script>

// Instância global
const sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON);

let usuarioAtual = null;
let perfilAtual = null;

async function authGuard(perfisPermitidos = []) {
  const { data: { session } } = await sb.auth.getSession();
  if (!session) {
    window.location.href = 'login.html';
    return null;
  }

  const { data: perfil } = await sb
    .from('perfis')
    .select('*')
    .eq('id', session.user.id)
    .single();

  if (!perfil || !perfil.ativo) {
    await sb.auth.signOut();
    window.location.href = 'login.html';
    return null;
  }

  if (perfisPermitidos.length > 0 && !perfisPermitidos.includes(perfil.perfil)) {
    alert('Acesso negado para seu perfil.');
    window.location.href = 'app.html';
    return null;
  }

  usuarioAtual = session.user;
  perfilAtual = perfil;
  return perfil;
}

async function fazerLogout() {
  await sb.auth.signOut();
  window.location.href = 'login.html';
}

function renderHeader(nomeModulo) {
  return `
    <header style="
      background:#141414; border-bottom:1px solid #2a2a2a;
      padding:12px 24px; display:flex; align-items:center;
      justify-content:space-between; position:sticky; top:0; z-index:100;">
      <div style="display:flex;align-items:center;gap:12px;">
        <a href="app.html" style="color:#888;font-size:12px;text-decoration:none;letter-spacing:0.1em;">← MENU</a>
        <span style="color:#2a2a2a;">|</span>
        <span style="font-family:'Syne',sans-serif;font-size:13px;font-weight:700;letter-spacing:0.12em;color:#c8a96e;">${nomeModulo}</span>
      </div>
      <div style="display:flex;align-items:center;gap:16px;">
        <span style="font-size:12px;color:#888;" id="headerUser">Carregando...</span>
        <button onclick="fazerLogout()" style="
          background:transparent;border:1px solid #2a2a2a;border-radius:2px;
          color:#888;font-size:11px;padding:6px 12px;cursor:pointer;
          letter-spacing:0.1em;text-transform:uppercase;
          transition:border-color 0.2s,color 0.2s;">Sair</button>
      </div>
    </header>`;
}

function atualizarHeaderUser() {
  const el = document.getElementById('headerUser');
  if (el && perfilAtual) {
    const labels = {
      administrador: 'Administrador', qualidade: 'Qualidade',
      logistica: 'Logística', financeiro: 'Financeiro',
      comercial: 'Comercial', industrial: 'Industrial',
      diretoria: 'Diretoria'
    };
    el.textContent = perfilAtual.nome + ' · ' + (labels[perfilAtual.perfil] || perfilAtual.perfil);
  }
}
