/**
 * cq_scan.js — Classic CQ · Leitura de Etiqueta por Câmera/Scanner
 *
 * Mobile: abre câmera para ler barcode/QR.
 * Desktop: captura input de scanner USB (teclado emulado).
 * Ambos: busca dados via fn_buscar_etiqueta e preenche formulário.
 *
 * Requer: config.js, cq_api.js, cq_toast.js
 *
 * Uso:
 *   <script src="cq_scan.js"></script>
 *
 *   const scanner = CQScan.init({
 *     inputId: 'inputDocumento',        // campo de texto onde o código aparece
 *     onFound: (dados) => { ... },      // callback quando encontra dados
 *     onNotFound: (codigo) => { ... },  // callback quando não encontra
 *     usuario: { email, nome },         // para log
 *   });
 *
 *   // Abrir câmera manualmente
 *   CQScan.openCamera();
 *
 *   // Parar
 *   CQScan.stop();
 */

const CQScan = (function () {

  let _opts = {};
  let _stream = null;
  let _scannerBuffer = '';
  let _scannerTimer = null;
  let _videoEl = null;
  let _overlayEl = null;
  let _scanning = false;
  let _keydownHandler = null;

  // ══════════════════════════════════════════════════════════════
  // INIT
  // ══════════════════════════════════════════════════════════════
  function init(opts = {}) {
    _opts = {
      inputId: opts.inputId || 'inputDocumento',
      onFound: opts.onFound || (() => {}),
      onNotFound: opts.onNotFound || (() => {}),
      usuario: opts.usuario || {},
      autoSearch: opts.autoSearch !== false,
      debounceMs: opts.debounceMs || 400,
    };

    // Desktop: detectar input de scanner USB (caracteres rápidos + Enter)
    _initScannerListener();

    return {
      openCamera,
      stop,
      search: _buscar,
    };
  }


  // ══════════════════════════════════════════════════════════════
  // SCANNER USB (Desktop) — detecta input rápido seguido de Enter
  // Scanners USB emulam teclado: enviam caracteres + Enter em <50ms
  // ══════════════════════════════════════════════════════════════
  function _initScannerListener() {
    // Remove previous listener to prevent accumulation
    if (_keydownHandler) document.removeEventListener('keydown', _keydownHandler);
    _keydownHandler = (e) => {
      // Ignorar se usuário está digitando em um campo
      const active = document.activeElement;
      const isInput = active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA');

      // Se Enter e temos buffer → é scan de scanner
      if (e.key === 'Enter' && _scannerBuffer.length >= 3) {
        e.preventDefault();
        const codigo = _scannerBuffer.trim();
        _scannerBuffer = '';

        // Colocar no input
        const input = document.getElementById(_opts.inputId);
        if (input) input.value = codigo;

        _buscar(codigo, 'scanner');
        return;
      }

      // Acumular caracteres (scanner envia rápido)
      if (e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey) {
        // Se usuário está no campo de documento, deixar o input normal funcionar
        if (isInput && active.id === _opts.inputId) return;

        _scannerBuffer += e.key;
        clearTimeout(_scannerTimer);
        _scannerTimer = setTimeout(() => {
          // Se demorou mais de 100ms entre chars, não é scanner
          _scannerBuffer = '';
        }, 100);
      }
    };
    document.addEventListener('keydown', _keydownHandler);
  }


  // ══════════════════════════════════════════════════════════════
  // CÂMERA (Mobile) — Abre câmera traseira para barcode
  // Usa BarcodeDetector API (Chrome/Android) ou fallback input file
  // ══════════════════════════════════════════════════════════════
  async function openCamera() {
    // Verificar se BarcodeDetector está disponível
    if ('BarcodeDetector' in window) {
      await _openBarcodeDetector();
    } else {
      // Fallback: input type=file com capture=environment
      _openFileFallback();
    }
  }

  async function _openBarcodeDetector() {
    try {
      _stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } }
      });

      // Criar overlay com vídeo
      _createVideoOverlay();
      _videoEl.srcObject = _stream;
      await _videoEl.play();
      _scanning = true;

      const detector = new BarcodeDetector({ formats: ['code_128', 'code_39', 'ean_13', 'ean_8', 'qr_code', 'data_matrix'] });

      const scanFrame = async () => {
        if (!_scanning) return;
        try {
          const barcodes = await detector.detect(_videoEl);
          if (barcodes.length > 0) {
            const codigo = barcodes[0].rawValue;
            stop();

            const input = document.getElementById(_opts.inputId);
            if (input) input.value = codigo;

            _buscar(codigo, 'camera');
            return;
          }
        } catch (e) { /* ignore frame errors */ }
        if (_scanning) requestAnimationFrame(scanFrame);
      };

      requestAnimationFrame(scanFrame);

    } catch (e) {
      console.warn('Camera access denied, using fallback:', e.message);
      _openFileFallback();
    }
  }

  function _openFileFallback() {
    // Criar input file invisível
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.capture = 'environment';
    input.style.display = 'none';
    document.body.appendChild(input);

    input.onchange = async () => {
      if (input.files.length === 0) return;

      if ('BarcodeDetector' in window) {
        try {
          const bitmap = await createImageBitmap(input.files[0]);
          const detector = new BarcodeDetector();
          const barcodes = await detector.detect(bitmap);
          if (barcodes.length > 0) {
            const codigo = barcodes[0].rawValue;
            const el = document.getElementById(_opts.inputId);
            if (el) el.value = codigo;
            _buscar(codigo, 'camera');
          } else {
            if (typeof CQToast !== 'undefined') CQToast.warn('Nenhum codigo detectado na imagem.');
          }
        } catch (e) {
          if (typeof CQToast !== 'undefined') CQToast.error('Erro ao processar imagem.');
        }
      } else {
        if (typeof CQToast !== 'undefined') CQToast.info('Leitura por camera nao suportada neste navegador. Digite o codigo manualmente.');
      }

      input.remove();
    };

    input.click();
  }

  function _createVideoOverlay() {
    _overlayEl = document.createElement('div');
    _overlayEl.id = 'cq-scan-overlay';
    _overlayEl.style.cssText = `
      position:fixed;inset:0;z-index:99999;background:rgba(0,0,0,.95);
      display:flex;flex-direction:column;align-items:center;justify-content:center;
    `;

    _videoEl = document.createElement('video');
    _videoEl.setAttribute('playsinline', '');
    _videoEl.style.cssText = 'width:90%;max-width:500px;border-radius:12px;border:2px solid #c8a96e;';

    const hint = document.createElement('div');
    hint.style.cssText = 'color:#999;font-size:13px;margin-top:16px;';
    hint.textContent = 'Aponte para o codigo de barras';

    const btnCancel = document.createElement('button');
    btnCancel.textContent = 'Cancelar';
    btnCancel.style.cssText = `
      margin-top:20px;padding:12px 32px;border-radius:8px;border:1px solid #333;
      background:transparent;color:#999;font-size:14px;cursor:pointer;
    `;
    btnCancel.onclick = () => stop();

    _overlayEl.appendChild(_videoEl);
    _overlayEl.appendChild(hint);
    _overlayEl.appendChild(btnCancel);
    document.body.appendChild(_overlayEl);
  }


  // ══════════════════════════════════════════════════════════════
  // BUSCAR — consulta backend e preenche
  // ══════════════════════════════════════════════════════════════
  async function _buscar(codigo, origem) {
    if (!codigo || codigo.length < 2) return;

    try {
      // Chamar RPC fn_buscar_etiqueta
      const res = await CQ_API.fetch(`rpc/fn_buscar_etiqueta`, {
        method: 'POST',
        body: JSON.stringify({ p_codigo: codigo }),
      });

      const data = await res.json();

      // Registrar log de scan
      _registrarLog(codigo, origem, data);

      if (data && data.encontrado) {
        if (typeof CQToast !== 'undefined') {
          CQToast.success('Etiqueta encontrada', {
            detalhe: `${data.fornecedor || ''} — ${data.produto || ''}`,
          });
        }
        _opts.onFound(data);
      } else {
        if (typeof CQToast !== 'undefined') {
          CQToast.warn('Etiqueta nao encontrada', {
            detalhe: 'Preencha os dados manualmente.',
          });
        }
        _opts.onNotFound(codigo);
      }

    } catch (e) {
      console.error('Erro ao buscar etiqueta:', e);
      // Fallback: tentar lookup simples
      if (typeof CQ !== 'undefined' && CQ.lookupDocumento) {
        const fallback = await CQ.lookupDocumento(codigo);
        if (fallback) {
          _opts.onFound({
            encontrado: true,
            fornecedor: fallback.fornecedor,
            fornecedor_codigo: fallback.fornecedor_codigo,
            produto: fallback.produto,
            quantidade: fallback.quantidade,
            data_recebimento: fallback.data,
          });
        } else {
          _opts.onNotFound(codigo);
        }
      }
    }
  }


  // ══════════════════════════════════════════════════════════════
  // LOG — registrar leitura
  // ══════════════════════════════════════════════════════════════
  async function _registrarLog(codigo, origem, resultado) {
    try {
      await CQ_API.post('cq_scan_log', {
        usuario_email: _opts.usuario?.email || '',
        usuario_nome: _opts.usuario?.nome || '',
        codigo_lido: codigo,
        tipo_codigo: origem === 'camera' ? 'barcode' : (origem === 'scanner' ? 'barcode' : 'manual'),
        scan_origem: origem || 'manual',
        encontrado: resultado?.encontrado || false,
        tabela_origem: resultado?.tabela_origem || null,
        registro_id: resultado?.registro_id || null,
        dados_preenchidos: resultado?.encontrado ? resultado : null,
      }, 'return=minimal');
    } catch (e) {
      // Log falhou — não bloquear fluxo
      console.warn('Falha ao registrar log de scan:', e);
    }
  }


  // ══════════════════════════════════════════════════════════════
  // REGISTRAR ALTERAÇÃO PÓS-SCAN
  // ══════════════════════════════════════════════════════════════
  async function registrarAlteracao(inspecaoId, scanLogId, campo, valorOriginal, valorNovo, justificativa) {
    try {
      await CQ_API.post('cq_scan_alteracao_log', {
        inspecao_id: inspecaoId,
        scan_log_id: scanLogId,
        usuario_email: _opts.usuario?.email || '',
        campo_alterado: campo,
        valor_original: valorOriginal,
        valor_novo: valorNovo,
        justificativa: justificativa,
      }, 'return=minimal');
    } catch (e) {
      console.warn('Falha ao registrar alteracao pos-scan:', e);
    }
  }


  // ══════════════════════════════════════════════════════════════
  // STOP
  // ══════════════════════════════════════════════════════════════
  function stop() {
    _scanning = false;
    if (_stream) {
      _stream.getTracks().forEach(t => t.stop());
      _stream = null;
    }
    if (_overlayEl) {
      _overlayEl.remove();
      _overlayEl = null;
    }
    _videoEl = null;
    if (_keydownHandler) {
      document.removeEventListener('keydown', _keydownHandler);
      _keydownHandler = null;
    }
    clearTimeout(_scannerTimer);
  }


  // ══════════════════════════════════════════════════════════════
  // API PÚBLICA
  // ══════════════════════════════════════════════════════════════
  return {
    init,
    openCamera,
    stop,
    registrarAlteracao,
  };

})();
