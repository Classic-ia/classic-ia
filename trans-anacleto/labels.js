/* =====================================================================
 * labels.js — Geracao de etiquetas QR/barcode + impressao A4
 * =====================================================================
 * Dependencias (inseridas nas paginas que usam):
 *  - https://cdn.jsdelivr.net/npm/qrcodejs@1.0.0/qrcode.min.js   (window.QRCode)
 *  - https://cdn.jsdelivr.net/npm/jsbarcode@3.11.6/dist/JsBarcode.all.min.js (window.JsBarcode)
 *  - https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js (window.jspdf)
 *
 * API:
 *  Labels.qrcode(container, data, opts)     -> desenha QR em container
 *  Labels.barcode(svgEl, data, opts)        -> desenha barcode em SVG
 *  Labels.renderSheet(items, opts)          -> monta DOM A4 paginado
 *  Labels.printPDF(items, opts)             -> gera PDF via jsPDF
 *  Labels.gerarCodigo(produto)              -> "TA-${sku}-${timestamp}"
 *
 * Formato etiqueta: 70x37mm (3 cols x 8 rows em A4 210x297mm).
 * ====================================================================*/
(function (global) {
  'use strict';

  const MM_PER_INCH = 25.4;
  const DPI = 96;
  const mmToPx = (mm) => Math.round(mm * DPI / MM_PER_INCH);

  const SHEET = {
    widthMm: 210, heightMm: 297,
    marginTopMm: 8, marginLeftMm: 7,
    colGapMm: 2, rowGapMm: 0,
    defaultCols: 3, defaultRows: 8,
    labelWidthMm: 70 - 2, labelHeightMm: 36
  };

  function qrcode(container, data, opts = {}) {
    if (!global.QRCode) throw new Error('QRCode lib nao carregada');
    container.innerHTML = '';
    return new global.QRCode(container, {
      text: data,
      width: opts.size || 80,
      height: opts.size || 80,
      correctLevel: global.QRCode.CorrectLevel.M
    });
  }

  function barcode(svgEl, data, opts = {}) {
    if (!global.JsBarcode) throw new Error('JsBarcode lib nao carregada');
    global.JsBarcode(svgEl, data, {
      format: opts.format || 'CODE128',
      width: opts.width || 1.6,
      height: opts.height || 40,
      displayValue: opts.displayValue !== false,
      fontSize: opts.fontSize || 11,
      margin: 0
    });
  }

  function gerarCodigo(produto) {
    const base = (produto && produto.sku) ? produto.sku : 'X';
    const ts = Date.now().toString(36).toUpperCase();
    return `TA-${base}-${ts}`;
  }

  /**
   * items: [{ codigo, sku, nome, localizacao, lote_interno?, formato? }]
   * opts: { cols, rows, formato ('qr'|'barcode'), onReady }
   */
  function renderSheet(items, opts = {}) {
    const cols = opts.cols || SHEET.defaultCols;
    const rows = opts.rows || SHEET.defaultRows;
    const perPage = cols * rows;
    const formato = opts.formato || 'qr';

    const container = document.createElement('div');
    container.className = 'labels-sheet-container';

    const pages = Math.ceil(items.length / perPage);
    for (let p = 0; p < pages; p++) {
      const page = document.createElement('div');
      page.className = 'labels-sheet-page';
      page.style.cssText = `
        width: ${SHEET.widthMm}mm; height: ${SHEET.heightMm}mm;
        padding: ${SHEET.marginTopMm}mm ${SHEET.marginLeftMm}mm;
        background: #fff; box-sizing: border-box;
        display: grid;
        grid-template-columns: repeat(${cols}, ${SHEET.labelWidthMm}mm);
        grid-template-rows: repeat(${rows}, ${SHEET.labelHeightMm}mm);
        gap: 0 ${SHEET.colGapMm}mm;
        page-break-after: always;
        margin-bottom: 10mm;
        box-shadow: 0 2px 8px rgba(0,0,0,.08);
      `;

      const slice = items.slice(p * perPage, (p + 1) * perPage);
      for (let i = 0; i < perPage; i++) {
        const cell = document.createElement('div');
        cell.className = 'labels-cell';
        cell.style.cssText = `
          border: 1px dashed #cbd5e1; padding: 2mm;
          display: flex; gap: 3mm; align-items: center;
          overflow: hidden; font-family: 'Inter', sans-serif;
        `;
        if (i < slice.length) {
          const it = slice[i];
          const codeBox = document.createElement('div');
          codeBox.style.cssText = 'width:22mm;height:22mm;display:flex;align-items:center;justify-content:center;flex-shrink:0';

          const info = document.createElement('div');
          info.style.cssText = 'flex:1;min-width:0;font-size:9pt;line-height:1.25';
          const sku = it.sku ? `<div style="font-weight:700;font-size:10pt">${escapeHtml(it.sku)}</div>` : '';
          const nome = it.nome ? `<div style="font-size:8pt;color:#334155;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(it.nome)}</div>` : '';
          const loc = it.localizacao ? `<div style="font-size:7pt;color:#64748b">📍 ${escapeHtml(it.localizacao)}</div>` : '';
          const lote = it.lote_interno ? `<div style="font-size:7pt;color:#64748b">Lote: ${escapeHtml(it.lote_interno)}</div>` : '';
          info.innerHTML = sku + nome + loc + lote;

          cell.appendChild(codeBox);
          cell.appendChild(info);

          if ((it.formato || formato) === 'barcode') {
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.style.cssText = 'width:100%;height:100%';
            codeBox.appendChild(svg);
            try { barcode(svg, it.codigo, { height: mmToPx(16), width: 1.2, fontSize: 9 }); }
            catch (e) { codeBox.textContent = it.codigo; }
          } else {
            try { qrcode(codeBox, it.codigo, { size: mmToPx(22) }); }
            catch (e) { codeBox.textContent = it.codigo; }
          }
        }
        page.appendChild(cell);
      }
      container.appendChild(page);
    }
    if (typeof opts.onReady === 'function') opts.onReady(container);
    return container;
  }

  async function printPDF(items, opts = {}) {
    if (!global.jspdf) throw new Error('jsPDF nao carregada');
    const { jsPDF } = global.jspdf;

    const cols = opts.cols || SHEET.defaultCols;
    const rows = opts.rows || SHEET.defaultRows;
    const perPage = cols * rows;
    const formato = opts.formato || 'qr';

    const MAX = 200;
    if (items.length > MAX) {
      throw new Error(`Limite de ${MAX} etiquetas por PDF. Gere em lotes menores.`);
    }

    const doc = new jsPDF({ unit: 'mm', format: 'a4' });
    const labelW = SHEET.labelWidthMm;
    const labelH = SHEET.labelHeightMm;

    for (let idx = 0; idx < items.length; idx++) {
      const it = items[idx];
      const posInPage = idx % perPage;
      if (idx > 0 && posInPage === 0) doc.addPage();

      const col = posInPage % cols;
      const row = Math.floor(posInPage / cols);
      const x = SHEET.marginLeftMm + col * (labelW + SHEET.colGapMm);
      const y = SHEET.marginTopMm + row * labelH;

      // QR/barcode em canvas temporario
      const tmp = document.createElement('div');
      tmp.style.cssText = 'position:absolute;left:-9999px;top:0';
      document.body.appendChild(tmp);
      try {
        if ((it.formato || formato) === 'barcode') {
          const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
          tmp.appendChild(svg);
          barcode(svg, it.codigo, { height: 80, width: 2, fontSize: 12 });
          const svgStr = new XMLSerializer().serializeToString(svg);
          const img = await svgToPng(svgStr, 240, 90);
          doc.addImage(img, 'PNG', x + 2, y + 4, 34, 14);
        } else {
          qrcode(tmp, it.codigo, { size: 140 });
          const canvas = tmp.querySelector('canvas');
          if (canvas) doc.addImage(canvas.toDataURL('image/png'), 'PNG', x + 2, y + 3, 20, 20);
        }
      } finally {
        tmp.remove();
      }

      // Texto
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(10);
      const textX = x + 26;
      let cursorY = y + 7;
      if (it.sku) { doc.text(String(it.sku), textX, cursorY); cursorY += 4; }
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(8);
      if (it.nome) {
        const nomeTrunc = doc.splitTextToSize(String(it.nome), labelW - 28);
        doc.text(nomeTrunc.slice(0, 2), textX, cursorY);
        cursorY += 3.5 * Math.min(nomeTrunc.length, 2);
      }
      doc.setFontSize(7);
      doc.setTextColor(100);
      if (it.localizacao) { doc.text('Local: ' + it.localizacao, textX, cursorY); cursorY += 3; }
      if (it.lote_interno) { doc.text('Lote: ' + it.lote_interno, textX, cursorY); }
      doc.setTextColor(0);
    }

    const nome = opts.filename || `etiquetas-${Date.now()}.pdf`;
    doc.save(nome);
    return true;
  }

  function svgToPng(svgStr, w, h) {
    return new Promise((resolve, reject) => {
      const blob = new Blob([svgStr], { type: 'image/svg+xml;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const img = new Image();
      img.onload = () => {
        const canvas = document.createElement('canvas');
        canvas.width = w; canvas.height = h;
        canvas.getContext('2d').drawImage(img, 0, 0, w, h);
        URL.revokeObjectURL(url);
        resolve(canvas.toDataURL('image/png'));
      };
      img.onerror = reject;
      img.src = url;
    });
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  global.Labels = {
    qrcode, barcode, gerarCodigo, renderSheet, printPDF, SHEET
  };
})(window);
