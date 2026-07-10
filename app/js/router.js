// ══════════════════════════════════════════════════════
// router.js — Navegação por hash (SPA leve, sem dependências)
// Sprint: S0.2 · BLOCO 002 (Dashboard shell)
// Uso:
//   Router.registrar('inicio', render, { default: true })
//   Router.aoNavegar((hash, rota) => { ... })
//   Router.iniciar()
// ══════════════════════════════════════════════════════
import { Logger } from './logger.js';

const _rotas = new Map();
let _rotaPadrao = null;
let _handlerNaoEncontrada = null;
let _handlerNavegar = null;

function hashAtual() {
  return (window.location.hash || '').replace(/^#/, '');
}

function resolver() {
  const hash = hashAtual() || _rotaPadrao;
  const rota = _rotas.get(hash);

  if (!rota) {
    Logger.warn('Rota não encontrada', { hash });
    if (_handlerNaoEncontrada) _handlerNaoEncontrada(hash);
    return;
  }

  if (_handlerNavegar) _handlerNavegar(hash, rota);

  try {
    rota.handler(hash, rota);
  } catch (e) {
    Logger.error('Erro ao renderizar rota', { hash, erro: e.message });
  }
}

export const Router = {
  registrar(hash, handler, opts = {}) {
    _rotas.set(hash, { hash, handler, ...opts });
    if (opts.default) _rotaPadrao = hash;
    return this;
  },

  naoEncontrada(handler) {
    _handlerNaoEncontrada = handler;
    return this;
  },

  aoNavegar(callback) {
    _handlerNavegar = callback;
    return this;
  },

  ir(hash) {
    if (hashAtual() !== hash) {
      window.location.hash = hash;
    } else {
      resolver();
    }
  },

  atual: hashAtual,

  rotas() {
    return [..._rotas.values()];
  },

  iniciar() {
    window.addEventListener('hashchange', resolver);
    if (!window.location.hash && _rotaPadrao) {
      window.location.replace('#' + _rotaPadrao);
    }
    resolver();
  },
};
