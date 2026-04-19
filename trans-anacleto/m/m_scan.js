/**
 * m_scan.js — Trans Anacleto Mobile
 *
 * Wrapper do BarcodeDetector API para leitura de QR/barcode via cam.
 * Fallback: input[type=file capture=environment] + processamento de imagem.
 * Inspiracao direta em /home/user/classic-ia/cq_scan.js
 *
 * Uso:
 *   MScan.openCamera({
 *     onFound: (codigo) => { ... },
 *     onCancel: () => { ... },
 *     prompt: 'Aponte a camera para o QR do pneu'
 *   });
 */
const MScan = (function () {
  let _stream = null;
  let _videoEl = null;
  let _overlayEl = null;
  let _scanning = false;
  let _opts = {};

  async function openCamera(opts = {}) {
    _opts = {
      onFound: opts.onFound || (() => {}),
      onCancel: opts.onCancel || (() => {}),
      prompt: opts.prompt || 'Aponte a camera para o codigo',
      formats: opts.formats || ['code_128','code_39','ean_13','ean_8','qr_code','data_matrix']
    };
    if ('BarcodeDetector' in window) {
      await _openBarcodeDetector();
    } else {
      _openFileFallback();
    }
  }

  async function _openBarcodeDetector() {
    try {
      _stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } }
      });
      _createOverlay();
      _videoEl.srcObject = _stream;
      await _videoEl.play();
      _scanning = true;

      const detector = new BarcodeDetector({ formats: _opts.formats });
      const loop = async () => {
        if (!_scanning) return;
        try {
          const barcodes = await detector.detect(_videoEl);
          if (barcodes.length > 0) {
            const codigo = barcodes[0].rawValue;
            stop();
            _opts.onFound(codigo);
            return;
          }
        } catch (e) { /* ignore frame errors */ }
        if (_scanning) requestAnimationFrame(loop);
      };
      requestAnimationFrame(loop);
    } catch (e) {
      console.warn('Camera error, using fallback:', e.message);
      _openFileFallback();
    }
  }

  function _openFileFallback() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.capture = 'environment';
    input.style.display = 'none';
    document.body.appendChild(input);
    input.onchange = async () => {
      if (input.files.length === 0) { _opts.onCancel(); input.remove(); return; }
      if ('BarcodeDetector' in window) {
        try {
          const bitmap = await createImageBitmap(input.files[0]);
          const detector = new BarcodeDetector();
          const barcodes = await detector.detect(bitmap);
          if (barcodes.length > 0) {
            _opts.onFound(barcodes[0].rawValue);
          } else {
            alert('Nenhum codigo detectado na imagem.');
            _opts.onCancel();
          }
        } catch (e) { alert('Erro ao processar imagem.'); _opts.onCancel(); }
      } else {
        alert('Leitura por camera nao suportada. Digite o codigo manualmente.');
        _opts.onCancel();
      }
      input.remove();
    };
    input.click();
  }

  function _createOverlay() {
    _overlayEl = document.createElement('div');
    _overlayEl.id = 'm-scan-overlay';
    _overlayEl.style.cssText = `
      position:fixed;inset:0;z-index:99999;background:rgba(0,0,0,.95);
      display:flex;flex-direction:column;align-items:center;justify-content:center;
    `;
    _videoEl = document.createElement('video');
    _videoEl.setAttribute('playsinline', '');
    _videoEl.setAttribute('autoplay', '');
    _videoEl.setAttribute('muted', '');
    _videoEl.style.cssText = 'max-width:100%;max-height:70vh;border:2px solid #fff;border-radius:8px';

    const msg = document.createElement('div');
    msg.textContent = _opts.prompt;
    msg.style.cssText = 'color:#fff;font-size:16px;margin-bottom:12px;text-align:center;padding:0 16px';

    const btn = document.createElement('button');
    btn.textContent = 'Cancelar';
    btn.style.cssText = 'margin-top:18px;padding:12px 24px;background:#dc2626;color:#fff;border:none;border-radius:8px;font-size:16px;font-weight:600';
    btn.onclick = () => { stop(); _opts.onCancel(); };

    _overlayEl.appendChild(msg);
    _overlayEl.appendChild(_videoEl);
    _overlayEl.appendChild(btn);
    document.body.appendChild(_overlayEl);
  }

  function stop() {
    _scanning = false;
    if (_stream) {
      _stream.getTracks().forEach(t => t.stop());
      _stream = null;
    }
    if (_overlayEl) { _overlayEl.remove(); _overlayEl = null; }
    _videoEl = null;
  }

  return { openCamera, stop };
})();
