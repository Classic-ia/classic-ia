// config.js — Configuracao centralizada do modulo Trans Anacleto
// Em DEV aponta para Supabase CLI local (docker).
// Em PROD, atualizar com URL/anon_key do projeto cloud transanacleto.

const TRANS_CONFIG = {
  // Supabase CLI local (default). Para producao, substituir pelos valores do projeto cloud.
  SB_URL: 'http://127.0.0.1:54321',
  SB_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJvbGUiOiJhbm9uIiwiaWF0IjoxNjQwOTk1MjAwLCJleHAiOjE5NTg0MTMyMDB9.M9jrxyvPLkUxWgOYSf5dNdJ8v_eRrq810ShFRT8N-6M',
  APP_NAME: 'Trans Anacleto',
  VERSION: 'v2026.04',
  SESSION_KEY: 'trans_sess_v1',
};

const SB_URL = TRANS_CONFIG.SB_URL;
const SB_KEY = TRANS_CONFIG.SB_KEY;
