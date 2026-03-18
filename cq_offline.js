/**
 * cq_offline.js — Classic CQ v2
 * Modo offline: salva inspeções no IndexedDB e sincroniza ao reconectar
 *
 * Uso:
 *   <script src="cq_offline.js"></script>
 *
 *   // Salvar inspeção (online ou offline)
 *   await CQOffline.salvarInspecao(dados);
 *
 *   // Sincronizar pendentes manualmente
 *   await CQOffline.sync();
 *
 *   // Verificar status
 *   const status = CQOffline.status(); // { online, pendentes }
 */

const CQOffline = (function () {

  const DB_NAME = 'classic_cq_offline';
  const DB_VERSION = 1;
  const STORE_INSPECOES = 'inspecoes_pendentes';
  const STORE_CACHE = 'dados_cache';

  let _db = null;

  // ── INDEXEDDB ─────────────────────────────────────────────
  function _openDB() {
    return new Promise((resolve, reject) => {
      if (_db) { resolve(_db); return; }
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = (e) => {
        const db = e.target.result;

        if (!db.objectStoreNames.contains(STORE_INSPECOES)) {
          const store = db.createObjectStore(STORE_INSPECOES, { keyPath: 'id', autoIncrement: true });
          store.createIndex('criado_em', 'criado_em', { unique: false });
          store.createIndex('status', 'status', { unique: false });
        }

        if (!db.objectStoreNames.contains(STORE_CACHE)) {
          db.createObjectStore(STORE_CACHE, { keyPath: 'key' });
        }
      };

      request.onsuccess = (e) => {
        _db = e.target.result;
        resolve(_db);
      };

      request.onerror = () => reject(request.error);
    });
  }

  // ── SALVAR INSPEÇÃO ───────────────────────────────────────
  /**
   * Salva inspeção. Se online, tenta enviar direto.
   * Se offline, salva no IndexedDB para sync posterior.
   * @param {Object} dados — payload da inspeção
   * @returns {{ sucesso: boolean, offline: boolean }}
   */
  async function salvarInspecao(dados) {
    if (navigator.onLine) {
      try {
        const r = await fetch(`${CQ_CONFIG.SB_URL}/rest/v1/registros_cq_inspecao`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'apikey': CQ_CONFIG.SB_KEY,
            'Authorization': 'Bearer ' + (CQAuth.getAuthToken() || CQ_CONFIG.SB_KEY),
            'Prefer': 'return=representation',
          },
          body: JSON.stringify(dados),
        });

        if (r.ok) {
          if (typeof CQToast !== 'undefined') CQToast.success('Inspe\u00e7\u00e3o salva com sucesso');
          return { sucesso: true, offline: false };
        }
        throw new Error(`HTTP ${r.status}`);
      } catch (e) {
        // Falha de rede → salvar offline
        console.warn('[CQOffline] Falha ao enviar online, salvando offline:', e.message);
      }
    }

    // Salvar no IndexedDB
    const db = await _openDB();
    const tx = db.transaction(STORE_INSPECOES, 'readwrite');
    const store = tx.objectStore(STORE_INSPECOES);

    const registro = {
      ...dados,
      _offline: true,
      _criado_offline_em: new Date().toISOString(),
      status: 'pendente',
    };

    await new Promise((resolve, reject) => {
      const req = store.add(registro);
      req.onsuccess = resolve;
      req.onerror = () => reject(req.error);
    });

    if (typeof CQToast !== 'undefined') {
      CQToast.warn('Inspe\u00e7\u00e3o salva offline', {
        detalhe: 'Ser\u00e1 sincronizada automaticamente quando a conex\u00e3o voltar.',
      });
    }

    // Registrar para background sync
    if ('serviceWorker' in navigator && 'SyncManager' in window) {
      const reg = await navigator.serviceWorker.ready;
      await reg.sync.register('sync-inspecoes').catch(e => {
        console.warn('[CQOffline] Background sync registration failed:', e.message);
      });
    }

    return { sucesso: true, offline: true };
  }

  // ── SYNC PENDENTES ────────────────────────────────────────
  /**
   * Sincroniza todas as inspeções pendentes no IndexedDB.
   * @returns {{ total: number, enviados: number, erros: number }}
   */
  async function sync() {
    if (!navigator.onLine) return { total: 0, enviados: 0, erros: 0 };

    const db = await _openDB();
    const tx = db.transaction(STORE_INSPECOES, 'readonly');
    const store = tx.objectStore(STORE_INSPECOES);

    const pendentes = await new Promise((resolve, reject) => {
      const req = store.getAll();
      req.onsuccess = () => resolve(req.result.filter(r => r.status === 'pendente' || r.status === 'erro'));
      req.onerror = () => reject(req.error);
    });

    if (pendentes.length === 0) return { total: 0, enviados: 0, erros: 0 };

    let enviados = 0;
    let erros = 0;

    for (const registro of pendentes) {
      try {
        // Mark as syncing to prevent duplicate sends
        const markTx = db.transaction(STORE_INSPECOES, 'readwrite');
        markTx.objectStore(STORE_INSPECOES).put({ ...registro, status: 'sincronizando' });

        // Remover campos internos
        const { id, _offline, _criado_offline_em, status, _erro, ...payload } = registro;

        const r = await fetch(`${CQ_CONFIG.SB_URL}/rest/v1/registros_cq_inspecao`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'apikey': CQ_CONFIG.SB_KEY,
            'Authorization': 'Bearer ' + (CQAuth.getAuthToken() || CQ_CONFIG.SB_KEY),
            'Prefer': 'return=minimal',
          },
          body: JSON.stringify(payload),
        });

        if (r.ok) {
          // Remover do IndexedDB
          const delTx = db.transaction(STORE_INSPECOES, 'readwrite');
          delTx.objectStore(STORE_INSPECOES).delete(registro.id);
          enviados++;
        } else {
          // Marcar como erro
          const errTx = db.transaction(STORE_INSPECOES, 'readwrite');
          errTx.objectStore(STORE_INSPECOES).put({ ...registro, status: 'erro', _erro: `HTTP ${r.status}` });
          erros++;
        }
      } catch (e) {
        erros++;
        console.warn('[CQOffline] Falha ao sincronizar registro:', e.message);
      }
    }

    if (enviados > 0 && typeof CQToast !== 'undefined') {
      CQToast.success(`${enviados} inspe\u00e7\u00f5es sincronizadas`);
    }

    return { total: pendentes.length, enviados, erros };
  }

  // ── STATUS ────────────────────────────────────────────────
  async function status() {
    try {
      const db = await _openDB();
      const tx = db.transaction(STORE_INSPECOES, 'readonly');
      const store = tx.objectStore(STORE_INSPECOES);

      const count = await new Promise((resolve) => {
        const req = store.count();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => resolve(0);
      });

      return {
        online: navigator.onLine,
        pendentes: count,
      };
    } catch {
      return { online: navigator.onLine, pendentes: 0 };
    }
  }

  // ── LISTENERS ─────────────────────────────────────────────
  if (typeof window !== 'undefined') {
    // Auto-sync ao reconectar
    window.addEventListener('online', () => {
      if (typeof CQToast !== 'undefined') CQToast.info('Conex\u00e3o restabelecida. Sincronizando...');
      setTimeout(() => sync(), 2000);
    });

    window.addEventListener('offline', () => {
      if (typeof CQToast !== 'undefined') {
        CQToast.warn('Sem conex\u00e3o', {
          detalhe: 'Inspe\u00e7\u00f5es ser\u00e3o salvas localmente.',
          duracao: 8000,
        });
      }
    });

    // Escutar mensagem do Service Worker
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.addEventListener('message', (e) => {
        if (e.data?.type === 'SYNC_INSPECOES') {
          sync();
        }
      });
    }

    // Registrar Service Worker
    if ('serviceWorker' in navigator) {
      window.addEventListener('load', () => {
        navigator.serviceWorker.register('/sw.js').catch(err => {
          console.warn('[CQOffline] SW registration failed:', err.message);
        });
      });
    }
  }

  // ── API PÚBLICA ───────────────────────────────────────────
  return {
    salvarInspecao,
    sync,
    status,
  };

})();
