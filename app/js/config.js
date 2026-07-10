// ══════════════════════════════════════════════════════
// config.js — Cliente Supabase (singleton)
// Sprint: S0.2 · BLOCO 002 (Dashboard shell)
// IAP-06: nada hardcoded — o único ponto de configuração da app.
// ══════════════════════════════════════════════════════
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

// Projeto Supabase da J.A. Agroindustrial (ver CONTEXT.md).
const SUPABASE_URL = 'https://wiwjplhrbgsvyeaatjld.supabase.co';

// Chave anon é pública (protegida por RLS). Pode ser sobrescrita em runtime
// via window.__JA_IOS_ANON_KEY__ (ex.: injeção no deploy Netlify).
// TODO(S0.2): preencher com a anon key do projeto antes do deploy.
const SUPABASE_ANON_KEY =
  (typeof window !== 'undefined' && window.__JA_IOS_ANON_KEY__) ||
  'SUBSTITUA_PELA_ANON_KEY';

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
  },
});

export const APP = Object.freeze({
  nome: 'JA-IOS',
  descricao: 'Sistema Operacional Industrial · J.A. Agroindustrial',
  versao: '0.2.0',
  projeto: 'wiwjplhrbgsvyeaatjld',
});
