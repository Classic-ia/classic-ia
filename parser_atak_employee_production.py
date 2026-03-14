"""
sourceiq/etl/parser_atak_employee_production.py  —  v2.0
=========================================================
Parses ATAK WRPRD600 production-by-employee reports.

Source:   PDF text extracted from WRPRD600 ATAK report.

Report structure (hierarchical — one date → N employees → M products):

  DATA DE PRODUÇÃO: DD/MM/YYYY
    FUNCIONÁRIO: <code> - <name>
      <5-digit product code>  <product name>  <hours> HORAS <qty_produced>
      QTDE/HORA: <rate>
      <5-digit product code>  <product name>  <hours> HORAS <qty_produced>
      QTDE/HORA: <rate>
    FUNCIONÁRIO: <code> - <name>
      ...
  DATA DE PRODUÇÃO: DD/MM/YYYY
    ...

Target DB:  public.employee_production_atak

Critical rules:
  1. production_date normalised to datetime.date (was raw string in v1).
  2. hours_worked: WRPRD600 uses integer hours in the product line
     ("8 HORAS") but may also express fractional hours. Both handled.
  3. quantity_per_hour: source value verified against recomputed.
     Discrepancy > 1% → warning; we store the recomputed value.
  4. State machine: current_date / current_employee / current_product
     are all reset correctly at section boundaries. A QTDE/HORA line
     without a preceding product line is ignored with a warning.
  5. data_source field was MISSING in v1 — added here.

Anti-duplication key: content_hash over
    (production_date, employee_code, product_code)
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
    validate_required, validate_non_negative, validate_formula,
    log_info, log_warn, log_err,
    make_logger,
)

logger = make_logger("parser_atak_employee_production")

# ─── REGEX PATTERNS ──────────────────────────────────────────────────────────

RE_DATE     = re.compile(r"DATA\s+DE\s+PRODUÇÃO\s*:\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.IGNORECASE)
RE_EMPLOYEE = re.compile(r"FUNCIONÁRIO\s*:\s*(\d+)\s*[-–]\s*(.+)", re.IGNORECASE)
# Product line: 5-digit code, product name, optional decimal hours, HORAS keyword, quantity
# Examples:
#   "10001 BUCHO BOVINO 8 HORAS 1.250,00"
#   "10002 TRIPA GROSSA 7,5 HORAS 980"
RE_PRODUCT  = re.compile(
    r"^(\d{5})\s+(.+?)\s+([\d]+(?:[,\.]\d+)?)\s+HORAS\s+([\d\.,]+)$",
    re.IGNORECASE,
)
RE_RATE     = re.compile(r"QTDE[/\s]HORA\s*:\s*([\d\.,]+)", re.IGNORECASE)

REQUIRED_FIELDS     = ["production_date", "employee_code", "product_code",
                       "hours_worked", "quantity_produced"]
NON_NEGATIVE_FIELDS = ["hours_worked", "quantity_produced", "quantity_per_hour"]


def _safe_number(raw: str) -> Optional[float]:
    """
    Parse a number from ATAK PDF text.
    In ATAK reports, dots are ALWAYS thousands separators and commas are
    decimal separators. '1.200' means 1200, '80,12' means 80.12.
    Always strip dots first, then replace comma with dot.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    cleaned = s.replace('.', '').replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None


class _ParserState:
    """
    Encapsulates the mutable state machine for the hierarchical WRPRD600 layout.
    Each call to reset_* clears the corresponding level and all levels below it.
    """
    def __init__(self):
        self.reset_all()

    def reset_all(self):
        self.current_date:     Optional[date]  = None
        self.current_emp_code: Optional[str]   = None
        self.current_emp_name: Optional[str]   = None
        self.current_prod_code: Optional[str]  = None
        self.current_prod_name: Optional[str]  = None
        self.current_hours:    Optional[float] = None
        self.current_qty:      Optional[float] = None

    def reset_employee(self):
        self.current_emp_code  = None
        self.current_emp_name  = None
        self.reset_product()

    def reset_product(self):
        self.current_prod_code = None
        self.current_prod_name = None
        self.current_hours     = None
        self.current_qty       = None

    def has_product(self) -> bool:
        return self.current_prod_code is not None

    def has_employee(self) -> bool:
        return self.current_emp_code is not None


def parse_atak_employee_production_text(
    text: str,
    source_ref: str = "WRPRD600",
) -> ParseResult:
    """
    Main entry point for text-based parsing.

    Args:
        text:       Full text extracted from the WRPRD600 PDF.
        source_ref: Filename of the source PDF.

    Returns ParseResult with rows ready for employee_production_atak.
    """
    logs:  List[ParseLog] = []
    rows:  List[Dict[str, Any]] = []
    state  = _ParserState()

    logger.info("Parsing WRPRD600 production text (%d chars, source=%s)",
                len(text), source_ref)

    if not text or not text.strip():
        log_err(logs, None, "text", "Input text is empty — nothing to parse.")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source_ref,
                           entity_type="atak_employee_production")

    lines_raw   = text.splitlines()
    total_input = len(lines_raw)
    line_num    = 0
    skipped     = 0
    committed   = 0

    for raw_line in lines_raw:
        line_num += 1
        line = raw_line.strip()
        if not line:
            continue

        # ── DATE HEADER ───────────────────────────────────────────
        m = RE_DATE.search(line)
        if m:
            state.reset_all()
            state.current_date = normalize_date(m.group(1))
            if state.current_date is None:
                log_warn(logs, line_num, "production_date",
                         f"Could not parse date: {repr(m.group(1))}")
            continue

        # ── EMPLOYEE HEADER ───────────────────────────────────────
        m = RE_EMPLOYEE.search(line)
        if m:
            state.reset_employee()
            state.current_emp_code = m.group(1).strip()
            state.current_emp_name = normalize_text(m.group(2))
            if state.current_date is None:
                log_warn(logs, line_num, "production_date",
                         f"FUNCIONÁRIO found before any DATA DE PRODUÇÃO: "
                         f"code={state.current_emp_code}. Row will have null date.")
            continue

        # ── PRODUCT LINE ──────────────────────────────────────────
        m = RE_PRODUCT.match(line)
        if m:
            state.reset_product()
            state.current_prod_code = m.group(1)
            state.current_prod_name = normalize_text(m.group(2))
            state.current_hours     = _safe_number(m.group(3))
            state.current_qty       = _safe_number(m.group(4))

            if state.current_hours is None:
                log_warn(logs, line_num, "hours_worked",
                         f"Could not parse hours from product line: {repr(line)}")
            if state.current_qty is None:
                log_warn(logs, line_num, "quantity_produced",
                         f"Could not parse quantity from product line: {repr(line)}")
            continue

        # ── RATE LINE (triggers row commit) ───────────────────────
        m = RE_RATE.search(line)
        if m:
            source_rate = _safe_number(m.group(1))

            if not state.has_product():
                log_warn(logs, line_num, "quantity_per_hour",
                         "QTDE/HORA found without preceding product line — skipped.",
                         line)
                skipped += 1
                continue

            if not state.has_employee():
                log_warn(logs, line_num, "employee_code",
                         "QTDE/HORA found without preceding FUNCIONÁRIO — skipped.",
                         line)
                skipped += 1
                state.reset_product()
                continue

            # Recompute rate
            if (state.current_hours is not None and state.current_hours > 0
                    and state.current_qty is not None):
                recomputed_rate = round(state.current_qty / state.current_hours, 4)
            else:
                recomputed_rate = None

            validate_formula(logs, line_num, recomputed_rate, source_rate,
                             "quantity_per_hour", tolerance=0.01)

            r = base_row(source_ref, data_source="atak")
            r["production_date"]  = state.current_date
            r["employee_code"]    = state.current_emp_code
            r["employee_name"]    = state.current_emp_name
            r["product_code"]     = state.current_prod_code
            r["product_name_raw"] = state.current_prod_name
            r["hours_worked"]     = state.current_hours
            r["quantity_produced"] = state.current_qty
            r["quantity_per_hour"] = recomputed_rate  # always store recomputed

            validate_required(logs, line_num, r, REQUIRED_FIELDS)
            validate_non_negative(logs, line_num, r, NON_NEGATIVE_FIELDS)

            r["content_hash"] = content_hash(r, fields=[
                "production_date", "employee_code", "product_code",
            ])

            rows.append(r)
            committed += 1
            state.reset_product()  # ready for next product under same employee
            continue

        # Line matched nothing — ignore silently (totals, footers, etc.)

    logger.info(
        "WRPRD600 '%s': %d lines, %d rows committed, %d skipped, "
        "%d warnings, %d errors",
        source_ref, total_input, committed, skipped,
        sum(1 for l in logs if l.severity == "warning"),
        sum(1 for l in logs if l.severity == "error"),
    )

    return ParseResult(
        rows=rows, logs=logs,
        total_input_rows=total_input,
        skipped_rows=skipped,
        source_reference=source_ref,
        entity_type="atak_employee_production",
    )


def parse_atak_employee_production_pdf(pdf_path: str) -> ParseResult:
    """
    Convenience wrapper: extract text from PDF then parse.
    """
    from parser_atak_receipts import extract_text_from_pdf  # shared helper
    source_ref = Path(pdf_path).name
    text = extract_text_from_pdf(pdf_path)
    return parse_atak_employee_production_text(text, source_ref=source_ref)
