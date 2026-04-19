// config.js — Configuracao centralizada do modulo JR Terraplanagem.
// Mesmo projeto Supabase do Trans Anacleto (prefixo jr_ isola tabelas).
// Em DEV aponta para Supabase CLI local.

const JR_CONFIG = {
  SB_URL: 'http://127.0.0.1:54321',
  SB_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJvbGUiOiJhbm9uIiwiaWF0IjoxNjQwOTk1MjAwLCJleHAiOjE5NTg0MTMyMDB9.M9jrxyvPLkUxWgOYSf5dNdJ8v_eRrq810ShFRT8N-6M',
  APP_NAME: 'JR Terraplanagem',
  VERSION: 'v2026.04',
  SESSION_KEY: 'jr_sess_v1',
};

const SB_URL = JR_CONFIG.SB_URL;
const SB_KEY = JR_CONFIG.SB_KEY;
