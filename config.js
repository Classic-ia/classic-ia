// config.js — Configuração centralizada do Classic CQ
// Todas as constantes do projeto em um único lugar.

const CQ_CONFIG = {
  SB_URL: 'https://nvqxsulntpftcwtkjedu.supabase.co',
  SB_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im52cXhzdWxudHBmdGN3dGtqZWR1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMzNTIwODksImV4cCI6MjA4ODkyODA4OX0.JvahlmaqJUzd2FQp-27uADe3mL7Wccg68PA2_3YWRhw',
  APP_NAME: 'Classic CQ',
  VERSION: 'v2026.03',
};

// Aliases para compatibilidade com código existente
const SB_URL = CQ_CONFIG.SB_URL;
const SB_KEY = CQ_CONFIG.SB_KEY;
const SUPABASE_URL = CQ_CONFIG.SB_URL;
const SUPABASE_KEY = CQ_CONFIG.SB_KEY;
const SUPABASE_ANON = CQ_CONFIG.SB_KEY;
