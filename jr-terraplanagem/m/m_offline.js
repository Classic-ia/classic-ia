/**
 * m_offline.js - Fila offline para chamadas de escrita no mobile.
 *
 * Armazena RPCs pendentes em IndexedDB enquanto o dispositivo esta
 * offline (ou o Supabase inacessivel) e drena ao voltar a conexao.
 *
 * Uso:
 *   // Na pagina do formulario:
 *   const enviado = await MOffline.enviarRpc('jr_despesa_registrar', params, {
 *     label: 'Despesa R$ ' + valor,
 *     kind: 'despesa'
 *   });
 *   // Retorna:
 *   //   { ok:true, data, online:true }    quando conseguiu enviar direto;
 *   //   { ok:true, queued:true, id }      quando ficou na fila offline;
 *   //   { ok:false, error }               erro de validacao sincrono.
 *
 *   // Drenar manualmente (ex: botao "Tentar enviar"):
 *   await MOffline.flush();
 *
 *   // Contar pendentes:
 *   const n = await MOffline.pendingCount();
 *
 *   // Listar:
 *   const ops = await MOffline.list();
 */
const MOffline = (function () {
  const DB_NAME  = 'jr-offline';
  const DB_VER   = 1;
  const STORE    = 'ops';
  const listeners = new Set();
  let _dbPromise = null;

  function openDb() {
    if (_dbPromise) return _dbPromise;
    _dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VER);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          const store = db.createObjectStore(STORE, { keyPath: 'id', autoIncrement: true });
          store.createIndex('kind', 'kind', { unique: false });
          store.createIndex('created_at', 'created_at', { unique: false });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
    return _dbPromise;
  }

  function tx(mode) {
    return openDb().then(db => db.transaction(STORE, mode).objectStore(STORE));
  }

  async function add(op) {
    const store = await tx('readwrite');
    return new Promise((resolve, reject) => {
      const req = store.add(op);
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
  }

  async function remove(id) {
    const store = await tx('readwrite');
    return new Promise((resolve, reject) => {
      const req = store.delete(id);
      req.onsuccess = () => resolve();
      req.onerror   = () => reject(req.error);
    });
  }

  async function update(op) {
    const store = await tx('readwrite');
    return new Promise((resolve, reject) => {
      const req = store.put(op);
      req.onsuccess = () => resolve();
      req.onerror   = () => reject(req.error);
    });
  }

  async function list() {
    const store = await tx('readonly');
    return new Promise((resolve, reject) => {
      const req = store.getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror   = () => reject(req.error);
    });
  }

  async function pendingCount() {
    const store = await tx('readonly');
    return new Promise((resolve) => {
      const req = store.count();
      req.onsuccess = () => resolve(req.result || 0);
      req.onerror   = () => resolve(0);
    });
  }

  function isOnline() {
    return typeof navigator !== 'undefined' ? navigator.onLine !== false : true;
  }

  function notify() {
    listeners.forEach(fn => { try { fn(); } catch (e) { /* ignora */ } });
  }

  function onChange(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  }

  async function enviarRpc(fn, params, opts = {}) {
    const label = opts.label || fn;
    const kind  = opts.kind || 'rpc';

    // Tenta online primeiro
    if (isOnline() && typeof API !== 'undefined') {
      try {
        const r = await API.rpc(fn, params);
        if (r.ok) return { ok: true, data: r.data, online: true };
        // Erros 4xx sao de validacao - nao enfileira (dados invalidos nao
        // viram validos depois). Status 0/5xx enfileira.
        if (r.status >= 400 && r.status < 500) {
          return { ok: false, error: r.error || 'erro', status: r.status };
        }
      } catch (e) { /* segue para enfileirar */ }
    }

    // Enfileira
    const op = {
      op_type: 'rpc',
      kind, label, fn, params,
      created_at: new Date().toISOString(),
      retry_count: 0,
      last_error: null
    };
    const id = await add(op);
    notify();
    return { ok: true, queued: true, id };
  }

  async function enviarPost(table, payload, opts = {}) {
    const label = opts.label || ('POST ' + table);
    const kind  = opts.kind || 'post';

    if (isOnline() && typeof API !== 'undefined') {
      try {
        const r = await API.post(table, payload, 'return=representation');
        if (r.ok) return { ok: true, data: r.data, online: true };
        if (r.status >= 400 && r.status < 500) {
          return { ok: false, error: r.error || 'erro', status: r.status };
        }
      } catch (e) { /* segue para enfileirar */ }
    }

    const op = {
      op_type: 'post',
      kind, label, table, payload,
      created_at: new Date().toISOString(),
      retry_count: 0,
      last_error: null
    };
    const id = await add(op);
    notify();
    return { ok: true, queued: true, id };
  }

  async function _executar(op) {
    if (op.op_type === 'post') return API.post(op.table, op.payload, 'return=representation');
    // default: rpc (compat com ops antigas sem op_type)
    return API.rpc(op.fn, op.params);
  }

  let _flushing = false;

  async function flush() {
    if (_flushing) return { ok: true, skipped: true };
    if (!isOnline() || typeof API === 'undefined') return { ok: false, offline: true };

    _flushing = true;
    let enviados = 0, falhas = 0;
    try {
      const ops = await list();
      // FIFO por created_at
      ops.sort((a, b) => (a.created_at || '').localeCompare(b.created_at || ''));
      for (const op of ops) {
        try {
          const r = await _executar(op);
          if (r.ok) {
            await remove(op.id);
            enviados++;
          } else if (r.status >= 400 && r.status < 500) {
            // Erro permanente: retira da fila e registra em falhas arquivadas
            op.last_error = r.error || 'erro ' + r.status;
            op.retry_count = (op.retry_count || 0) + 1;
            op.permanent_error = true;
            await update(op);
            falhas++;
          } else {
            op.last_error = r.error || 'erro ' + r.status;
            op.retry_count = (op.retry_count || 0) + 1;
            await update(op);
            falhas++;
          }
        } catch (e) {
          op.last_error = String(e?.message || e);
          op.retry_count = (op.retry_count || 0) + 1;
          await update(op);
          falhas++;
        }
      }
    } finally {
      _flushing = false;
      notify();
    }
    return { ok: true, enviados, falhas };
  }

  async function clearPermanentes() {
    const ops = await list();
    for (const op of ops) if (op.permanent_error) await remove(op.id);
    notify();
  }

  async function clearAll() {
    const ops = await list();
    for (const op of ops) await remove(op.id);
    notify();
  }

  if (typeof window !== 'undefined') {
    window.addEventListener('online', () => { flush(); });
    // Tenta drenar ao carregar a pagina caso haja pendencias antigas.
    if (isOnline()) {
      setTimeout(() => flush().catch(() => {}), 1500);
    }
  }

  return {
    enviarRpc, enviarPost, flush, list, pendingCount, isOnline, onChange,
    clearPermanentes, clearAll, remove
  };
})();
