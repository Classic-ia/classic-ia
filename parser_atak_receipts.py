"""
sourceiq/etl/parser_atak_receipts.py  —  v2.0
==============================================
Parses ATAK WRMVE500 incoming movement reports.

Source:   PDF file exported from ATAK, converted to text via pdfplumber.
          This parser operates on the extracted TEXT string, not the PDF
          file directly. The caller (run_etl.py) performs PDF→text extraction.

Report structure (each document block):
  DOCUMENTO: <doc_number>
  DATA PCR: DD/MM/YYYY
  MOTORISTA: <name>
  PARTICIPANTE: <supplier name>
  PLACA: <plate>
  ORIGEM: <city>
  PRODUTO: <product name>
  QTD PREVISTA: <number>
  QTD RECEBIDA: <number>
  DEFEITOS:
    <defect name>: <number>
    ...

Target DB:  public.atak_receipts + public.atak_quality_defects

Critical rules:
  1. quantity_difference = quantity_received - quantity_expected.
     Computed here; also stored as generated column in DB — these must agree.
  2. pcr_date is normalised to datetime.date.
  3. vehicle_plate normalised via normalize_plate().
  4. Defects parsed into a separate list of {defect_name_raw, quantity}
     dicts stored in row["defects"] — the promoter writes these to
     atak_quality_defects separately.
  5. source_reference = PDF filename passed via source_ref parameter.

Anti-duplication key: content_hash over (document_number, pcr_date)
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from common import (
    ParseResult, ParseLog,
    normalize_text, normalize_number, normalize_date, normalize_plate,
    content_hash, base_row,
    validate_required, validate_non_negative,
    log_info, log_warn, log_err,
    make_logger,
)

logger = make_logger("parser_atak_receipts")

# ─── REGEX PATTERNS ──────────────────────────────────────────────────────────
# Each pattern is anchored to the start of a token + flexible whitespace.
# Group 1 always captures the value.

RE_DOC      = re.compile(r"DOCUMENTO\s*:\s*(\S+)")
RE_DATE     = re.compile(r"DATA\s+PCR\s*:\s*(\d{1,2}/\d{1,2}/\d{2,4})")
RE_DRIVER   = re.compile(r"MOTORISTA\s*:\s*(.+)")
RE_SUPPLIER = re.compile(r"PARTICIPANTE\s*:\s*(.+)")
RE_PLATE    = re.compile(r"PLACA\s*:\s*(\S+)")
RE_ORIGIN   = re.compile(r"ORIGEM\s*:\s*(.+)")
RE_PRODUCT  = re.compile(r"PRODUTO\s*:\s*(.+)")
RE_MOVEMENT = re.compile(r"TIPO\s+MOVIM\w*\s*:\s*(.+)")
RE_QTD_EXP  = re.compile(r"QTD\s+PREV\w*\s*:\s*([\d\.,]+)")
RE_QTD_REC  = re.compile(r"QTD\s+RECEB\w*\s*:\s*([\d\.,]+)")
# Defect line: "  FERMENTACAO: 12" or "FERMENTACAO   12"
RE_DEFECT   = re.compile(r"^\s{0,6}([A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ][A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ\s/]+?)\s*[:\s]\s*([\d\.,]+)\s*$")

# Block separator — a new document always starts with DOCUMENTO:
RE_BLOCK_START = re.compile(r"(?=DOCUMENTO\s*:)", re.IGNORECASE)

REQUIRED_FIELDS = ["document_number", "pcr_date"]


def _parse_defects(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Extract defect lines from a document block.
    Defects appear after a line containing "DEFEITO" (the section header).
    Returns list of {defect_name_raw, quantity} dicts.
    """
    defects: List[Dict[str, Any]] = []
    in_defects = False

    for line in lines:
        stripped = line.strip()
        if re.search(r"DEFEITO", stripped, re.IGNORECASE):
            in_defects = True
            continue
        if not in_defects:
            continue
        # Stop at a blank line or a new section keyword
        if not stripped:
            continue
        if re.match(r"^(DOCUMENTO|MOTORISTA|PARTICIPANTE|PRODUTO|TIPO)", stripped, re.IGNORECASE):
            in_defects = False
            continue
        m = RE_DEFECT.match(line)
        if m:
            qty = normalize_number(m.group(2))
            if qty is not None and qty > 0:
                defects.append({
                    "defect_name_raw": m.group(1).strip(),
                    "quantity":        qty,
                })

    return defects


def _parse_block(block: str, block_idx: int, source_ref: str,
                 logs: List[ParseLog]) -> Optional[Dict[str, Any]]:
    """
    Parse one document block from the WRMVE500 text.
    Returns a row dict or None if the block does not contain a document number.
    """
    lines = block.splitlines()

    def _search(pattern: re.Pattern) -> Optional[str]:
        for line in lines:
            m = pattern.search(line)
            if m:
                return m.group(1).strip()
        return None

    doc_number = _search(RE_DOC)
    if not doc_number:
        return None  # Not a data block (page header, footer, etc.)

    row_num = block_idx  # used for log entries

    r = base_row(source_ref, data_source="atak")

    r["document_number"]   = doc_number
    r["pcr_date"]          = normalize_date(_search(RE_DATE))
    r["driver_name"]       = normalize_text(_search(RE_DRIVER))
    r["supplier_name"]     = normalize_text(_search(RE_SUPPLIER))
    r["vehicle_plate"]     = normalize_plate(_search(RE_PLATE))
    r["origin_city"]       = normalize_text(_search(RE_ORIGIN))
    r["product_name"]      = normalize_text(_search(RE_PRODUCT))
    r["movement_type"]     = normalize_text(_search(RE_MOVEMENT))
    r["quantity_expected"] = normalize_number(_search(RE_QTD_EXP))
    r["quantity_received"] = normalize_number(_search(RE_QTD_REC))

    # Flag when expected quantity is absent (prevents NULL divergence = zero divergence)
    r["qty_expected_missing"] = r["quantity_expected"] is None

    # Compute difference
    if r["quantity_expected"] is not None and r["quantity_received"] is not None:
        r["quantity_difference"] = round(r["quantity_received"] - r["quantity_expected"], 3)
    else:
        r["quantity_difference"] = None

    # Parse defects section
    r["defects"] = _parse_defects(lines)

    # Validations
    validate_required(logs, row_num, r, REQUIRED_FIELDS)
    validate_non_negative(logs, row_num, r, ["quantity_expected", "quantity_received"])

    if r["driver_name"] is None:
        log_warn(logs, row_num, "driver_name",
                 f"Document {doc_number}: MOTORISTA not found in block.")

    if r["supplier_name"] is None:
        log_warn(logs, row_num, "supplier_name",
                 f"Document {doc_number}: PARTICIPANTE not found in block.")

    if r["quantity_expected"] is None and r["quantity_received"] is None:
        log_warn(logs, row_num, "quantity_expected",
                 f"Document {doc_number}: both QTD PREVISTA and QTD RECEBIDA are missing.")

    if r["pcr_date"] is None:
        log_warn(logs, row_num, "pcr_date",
                 f"Document {doc_number}: DATA PCR could not be parsed.",
                 _search(RE_DATE))

    r["content_hash"] = content_hash(r, fields=["document_number", "pcr_date"])

    return r


def parse_atak_receipts_text(text: str, source_ref: str = "WRMVE500") -> ParseResult:
    """
    Main entry point for text-based parsing.

    Args:
        text:       Full text extracted from the WRMVE500 PDF.
        source_ref: Filename of the source PDF (stored as source_reference).

    Returns ParseResult with rows ready for promotion to atak_receipts.
    """
    logs:   List[ParseLog] = []
    rows:   List[Dict[str, Any]] = []

    logger.info("Parsing WRMVE500 receipts text (%d chars, source=%s)",
                len(text), source_ref)

    if not text or not text.strip():
        log_err(logs, None, "text", "Input text is empty — nothing to parse.")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source_ref,
                           entity_type="receipt_requisition")

    # Split on DOCUMENTO: boundaries
    blocks    = RE_BLOCK_START.split(text)
    total_in  = len(blocks)
    skipped   = 0

    for i, block in enumerate(blocks):
        if not block.strip():
            skipped += 1
            continue

        row = _parse_block(block, block_idx=i + 1, source_ref=source_ref, logs=logs)
        if row is None:
            skipped += 1
            log_info(logs, i + 1, "document_number",
                     f"Block {i+1} skipped — no DOCUMENTO: found (page header/footer).")
            continue

        rows.append(row)

    logger.info(
        "WRMVE500 '%s': %d blocks, %d accepted, %d skipped, "
        "%d warnings, %d errors",
        source_ref, total_in, len(rows), skipped,
        sum(1 for l in logs if l.severity == "warning"),
        sum(1 for l in logs if l.severity == "error"),
    )

    return ParseResult(
        rows=rows, logs=logs,
        total_input_rows=total_in,
        skipped_rows=skipped,
        source_reference=source_ref,
        entity_type="receipt_requisition",
    )


# ─── PDF EXTRACTION HELPER ───────────────────────────────────────────────────

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extract full text from a WRMVE500 PDF using pdfplumber.
    Concatenates all pages with a newline separator.
    Raises ImportError clearly if pdfplumber is not installed.
    Raises RuntimeError if the PDF cannot be opened or contains no text.
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF extraction. "
            "Install it: pip install pdfplumber"
        )

    pages_text: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            raise RuntimeError(f"PDF has no pages: {pdf_path}")
        for page in pdf.pages:
            t = page.extract_text(x_tolerance=2, y_tolerance=2)
            if t:
                pages_text.append(t)

    if not pages_text:
        raise RuntimeError(f"No text could be extracted from PDF: {pdf_path}")

    return "\n".join(pages_text)


def parse_atak_receipts_pdf(pdf_path: str) -> ParseResult:
    """
    Convenience entry point: extract text from PDF then parse.
    Use parse_atak_receipts_text() directly when text is already available.
    """
    source_ref = Path(pdf_path).name
    text = extract_text_from_pdf(pdf_path)
    return parse_atak_receipts_text(text, source_ref=source_ref)
