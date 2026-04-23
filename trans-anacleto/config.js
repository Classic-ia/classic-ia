// config.js — Configuracao centralizada do modulo Trans Anacleto
// Em DEV aponta para Supabase CLI local (docker).
// Em PROD, atualizar com URL/anon_key do projeto cloud transanacleto.

const TRANS_CONFIG = {
  // Supabase cloud (producao).
  SB_URL: 'https://apqnflujltsobwedufdm.supabase.co',
  SB_KEY: 'sb_publishable_qD_wa4K-KhJf1544lAf0GA_e6-8SP0A',
  APP_NAME: 'Trans Anacleto',
  VERSION: 'v2026.04',
  SESSION_KEY: 'trans_sess_v1',
};

const SB_URL = TRANS_CONFIG.SB_URL;
const SB_KEY = TRANS_CONFIG.SB_KEY;
