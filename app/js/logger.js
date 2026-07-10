// ══════════════════════════════════════════════════════
// logger.js — Logger central da aplicação
// Sprint: S0.2 · BLOCO 002 (Dashboard shell)
// Regra obrigatória #8: PROIBIDO console.log direto — usar Logger.
// Níveis ERROR/FATAL são persistidos em `system_logs`.
// ══════════════════════════════════════════════════════
import { supabase } from './config.js';

const NIVEIS = { TRACE: 10, DEBUG: 20, INFO: 30, WARN: 40, ERROR: 50, FATAL: 60 };

// Nível mínimo exibido no console. Em produção pode ser elevado para INFO/WARN.
const NIVEL_MINIMO = NIVEIS.DEBUG;

async function persistir(nivel, mensagem, contexto) {
  try {
    await supabase.from('system_logs').insert({
      nivel,
      mensagem,
      contexto: contexto ? JSON.stringify(contexto) : null,
      origem: 'frontend/app',
      url: typeof location !== 'undefined' ? location.href : null,
    });
  } catch (_) {
    // Logar nunca pode derrubar a aplicação — falha de persistência é silenciosa.
  }
}

function emitir(nivel, mensagem, contexto) {
  if (NIVEIS[nivel] < NIVEL_MINIMO) return;

  const linha = `[JA-IOS][${nivel}] ${mensagem}`;
  // Única saída de console autorizada em toda a app (encapsulada aqui).
  const canal =
    NIVEIS[nivel] >= NIVEIS.ERROR ? 'error' : nivel === 'WARN' ? 'warn' : 'log';
  window.console[canal](linha, contexto ?? '');

  if (NIVEIS[nivel] >= NIVEIS.ERROR) persistir(nivel, mensagem, contexto);
}

export const Logger = {
  trace: (m, c) => emitir('TRACE', m, c),
  debug: (m, c) => emitir('DEBUG', m, c),
  info: (m, c) => emitir('INFO', m, c),
  warn: (m, c) => emitir('WARN', m, c),
  error: (m, c) => emitir('ERROR', m, c),
  fatal: (m, c) => emitir('FATAL', m, c),
};
