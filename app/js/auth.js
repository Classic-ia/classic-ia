// ══════════════════════════════════════════════════════
// auth.js — Autenticação e autorização (JWT + RLS)
// Sprint: S0.2 · BLOCO 002 (Dashboard shell)
// Expõe: login, logout, exigirAuth, temPerfil, carregarPerfil
// IAP-12: segurança em 3 camadas (interface + RLS + trigger).
//         Este arquivo é a camada de interface — RLS é a fonte da verdade.
// ══════════════════════════════════════════════════════
import { supabase } from './config.js';
import { Logger } from './logger.js';

// Hierarquia de perfis (menor privilégio → maior). Um perfil superior herda
// o acesso de todos os inferiores. Ver CLAUDE.md · Perfis de usuário.
export const HIERARQUIA = [
  'operador',
  'producao',
  'expedicao',
  'manutencao',
  'laboratorio',
  'qualidade',
  'rt',
  'gerente',
  'admin',
];

const PAGINA_LOGIN = 'index.html';
const PAGINA_INICIAL = 'dashboard.html';

let _perfilCache = null;

export async function login(email, senha) {
  const { data, error } = await supabase.auth.signInWithPassword({
    email,
    password: senha,
  });
  if (error) {
    Logger.warn('Falha de login', { email, erro: error.message });
    return { ok: false, error: error.message };
  }
  _perfilCache = null;
  return { ok: true, user: data.user };
}

export async function usuarioLogado() {
  const {
    data: { session },
  } = await supabase.auth.getSession();
  return session?.user ?? null;
}

// Carrega o perfil do usuário logado (colunas explícitas — regra #7: sem SELECT *).
export async function carregarPerfil() {
  if (_perfilCache) return _perfilCache;

  const user = await usuarioLogado();
  if (!user) return null;

  const { data, error } = await supabase
    .from('usuarios_perfil')
    .select('id, nome, email, perfil, empresa_id, crmv, ativo')
    .eq('id', user.id)
    .single();

  if (error) {
    Logger.error('Falha ao carregar perfil do usuário', { erro: error.message });
    return null;
  }

  _perfilCache = data;
  return data;
}

// temPerfil(perfilUsuario, perfisPermitidos)
// true se o perfil do usuário satisfaz algum dos perfis exigidos,
// respeitando a hierarquia (perfil superior herda acesso do inferior).
export function temPerfil(perfilUsuario, perfisPermitidos = []) {
  if (!perfisPermitidos || perfisPermitidos.length === 0) return true;
  const nivelUsuario = HIERARQUIA.indexOf(perfilUsuario);
  if (nivelUsuario === -1) return false;
  return perfisPermitidos.some((p) => {
    const nivelExigido = HIERARQUIA.indexOf(p);
    return nivelExigido !== -1 && nivelUsuario >= nivelExigido;
  });
}

// Guardião de página protegida. Redireciona para login se não autenticado,
// ou para a página inicial se o perfil não tiver acesso.
export async function exigirAuth(perfisPermitidos = []) {
  const user = await usuarioLogado();
  if (!user) {
    window.location.replace(PAGINA_LOGIN);
    return null;
  }

  const perfil = await carregarPerfil();
  if (!perfil || !perfil.ativo) {
    Logger.warn('Usuário sem perfil ativo — encerrando sessão', { id: user.id });
    await logout();
    return null;
  }

  if (!temPerfil(perfil.perfil, perfisPermitidos)) {
    Logger.warn('Acesso negado à página', {
      perfil: perfil.perfil,
      exigido: perfisPermitidos,
    });
    window.location.replace(PAGINA_INICIAL);
    return null;
  }

  return perfil;
}

export async function logout() {
  try {
    await supabase.auth.signOut();
  } catch (e) {
    Logger.error('Falha ao encerrar sessão', { erro: e.message });
  } finally {
    _perfilCache = null;
    window.location.replace(PAGINA_LOGIN);
  }
}
