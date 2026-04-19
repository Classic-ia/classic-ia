/**
 * m_scan.js - JR Terraplanagem Mobile
 *
 * Wrapper leve em volta da BarcodeDetector API para leitura de QR/barcode
 * no celular. Fallback: input type=file com capture=environment ou digitacao
 * manual via prompt.
 *
 * Uso:
 *   MScan.openCamera({
 *     onDetect: (codigo) => { ... },   // callback com o valor lido
 *     title: 'Ler codigo da maquina',  // titulo no overlay
 *   });
 */
const MScan = (function () {
  let _stream = null;
  let _overlay = null;
  let _video = null;
  let _scanning = false;
  let _onDetect = null;

  async function openCamera(opts = {}) {
    _onDetect = opts.onDetect || (() => {});
    const title = opts.title || 'Aponte para o codigo';
    const formats = opts.formats || ['qr_code', 'code_128', 'code_39', 'ean_13', 'ean_8', 'data_matrix'];

    if ('BarcodeDetector' in window) {
      await _openDetector(title, formats);
    } else {
      _openFallback(title);
    }
  }

  async function _openDetector(title, formats) {
    try {
      _stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } }
      });
      _buildOverlay(title);
      _video.srcObject = _stream;
      await _video.play();
      _scanning = true;

      const detector = new BarcodeDetector({ formats });

      const frame = async () => {
        if (!_scanning) return;
        try {
          const codes = await detector.detect(_video);
          if (codes && codes.length > 0) {
            const value = codes[0].rawValue;
            stop();
            _onDetect(value);
            return;
          }
        } catch { /* ignore per-frame */ }
        if (_scanning) requestAnimationFrame(frame);
      };
      requestAnimationFrame(frame);
    } catch (e) {
      console.warn('Camera indisponivel:', e.message);
      _openFallback(title);
    }
  }

  function _openFallback(title) {
    const codigo = prompt(title + '\n\nDigite o codigo manualmente:');
    if (codigo && codigo.trim()) _onDetect(codigo.trim());
  }

  function _buildOverlay(title) {
    _overlay = document.createElement('div');
    _overlay.style.cssText = `
      position:fixed; inset:0; z-index:9999; background:rgba(0,0,0,0.95);
      display:flex; flex-direction:column; align-items:center; justify-content:center;
      padding:20px;
    `;
    _video = document.createElement('video');
    _video.setAttribute('playsinline', '');
    _video.style.cssText = 'width:100%; max-width:520px; border-radius:12px; border:2px solid #D97706;';

    const hint = document.createElement('div');
    hint.style.cssText = 'color:#F8FAFC; font-size:15px; margin-top:18px; font-weight:600; text-align:center;';
    hint.textContent = title;

    const row = document.createElement('div');
    row.style.cssText = 'display:flex; gap:10px; margin-top:22px;';

    const btnManual = document.createElement('button');
    btnManual.textContent = 'Digitar manual';
    btnManual.style.cssText = 'padding:12px 20px; border-radius:8px; border:1px solid #64748B; background:transparent; color:#F8FAFC; font-size:14px; font-weight:600;';
    btnManual.onclick = () => {
      stop();
      const codigo = prompt('Digite o codigo:');
      if (codigo && codigo.trim()) _onDetect(codigo.trim());
    };

    const btnCancel = document.createElement('button');
    btnCancel.textContent = 'Cancelar';
    btnCancel.style.cssText = 'padding:12px 20px; border-radius:8px; border:none; background:#DC2626; color:#fff; font-size:14px; font-weight:600;';
    btnCancel.onclick = () => stop();

    row.appendChild(btnManual);
    row.appendChild(btnCancel);
    _overlay.appendChild(_video);
    _overlay.appendChild(hint);
    _overlay.appendChild(row);
    document.body.appendChild(_overlay);
  }

  function stop() {
    _scanning = false;
    if (_stream) { _stream.getTracks().forEach(t => t.stop()); _stream = null; }
    if (_overlay) { _overlay.remove(); _overlay = null; }
    _video = null;
  }

  return { openCamera, stop };
})();
