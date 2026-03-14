"""
sourceiq/etl/parser_load_classification.py  —  v2.0
====================================================
Parses CARGAS A/B/C load quality classification spreadsheets.

Source:     One .xlsx file covering a period (e.g. CARGAS 2026.xlsx).
Sheets:     Any sheet whose name starts with "CARGA" (case-insensitive)
            or is a pure integer string (e.g. "1", "2", "12").
            All other sheets are ignored.
Layout:     Has header row (usually row 0, but detected dynamically).
            Column names matched by keyword substring — layout varies.

Target DB:  public.load_quality_classifications

Critical rules:
  1. class_a + class_b + class_c must be recalculated and compared
     against classic_count. Mismatch → has_formula_mismatch=True + warning.
  2. Class percentages computed here (NOT relied upon from source).
  3. Rows where supplier is TOTAL/SEM FICHA/purely numeric are skipped.
  4. source_reference = filename (MISSING in v1 — fixed here).

Anti-duplication key: content_hash over
    (import_file_id, sheet_name, arrival_date, supplier_name_raw)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from common import (
    ParseResult, ParseLog,
    normalize_text, normalize_number, normalize_date,
    is_skip_supplier,
    build_col_map, find_column, detect_header_row,
    content_hash, base_row,
    validate_required, validate_non_negative, validate_formula,
    log_info, log_warn, log_err,
    make_logger,
)

logger = make_logger("parser_load_classification")

# Sheet name patterns that identify a CARGAS sheet
def _is_cargas_sheet(name: str) -> bool:
    n = name.strip()
    if n.upper().startswith("CARGA"):
        return True
    if n.isdigit():   # sheet named "1", "2", etc.
        return True
    return False

# Keywords used to detect the header row inside a sheet
HEADER_KEYWORDS = [
    "FRIGOR", "DATA", "CONTAGEM", "CLASS", "REPASSE", "CLASSIC",
]

# Column name patterns for find_column() — first match wins
COL_DATE     = ("DATA",)
COL_SUPPLIER = ("FRIGOR",)            # header often says "FRIGORIFICO" or "FRIGORÍFICO"
COL_FRIGO    = ("CONTAGEM FRIGO",)
COL_CLASSIC  = ("CONTAGEM CLASSIC",)
COL_A        = ('PÇS "A"', 'PCS "A"', "PCS A",  "PECAS A",  "PEÇAS A",  "CLASS A", "CLASSE A")
COL_B        = ('PÇS "B"', 'PCS "B"', "PCS B",  "PECAS B",  "PEÇAS B",  "CLASS B", "CLASSE B")
COL_C        = ('PÇS "C"', 'PCS "C"', "PCS C",  "PECAS C",  "PEÇAS C",  "CLASS C", "CLASSE C")
COL_REPASS   = ("REPASSE",)

REQUIRED_FIELDS     = ["arrival_date", "supplier_name_raw"]
NON_NEGATIVE_FIELDS = [
    "frigo_count", "classic_count",
    "class_a_count", "class_b_count", "class_c_count", "repass_count",
]


def _compute_percentages(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute class_a/b/c percentages from counts.
    Also sets has_formula_mismatch when a+b+c != classic_count.
    Mutates row in-place and returns it.
    """
    a = row.get("class_a_count") or 0.0
    b = row.get("class_b_count") or 0.0
    c = row.get("class_c_count") or 0.0
    abc_sum = a + b + c

    classic = row.get("classic_count")
    if classic is None:
        # If classic_count was absent, derive it from the sum
        row["classic_count"] = abc_sum if abc_sum > 0 else None
        classic = row["classic_count"]

    # Formula mismatch: abs(classic - sum(a+b+c)) > 0.001
    row["has_formula_mismatch"] = (
        classic is not None and abs(classic - abc_sum) > 0.001
    )

    if classic and classic > 0:
        row["class_a_percent"] = round((a / classic) * 100, 4)
        row["class_b_percent"] = round((b / classic) * 100, 4)
        row["class_c_percent"] = round((c / classic) * 100, 4)
    else:
        row["class_a_percent"] = None
        row["class_b_percent"] = None
        row["class_c_percent"] = None

    return row


def _parse_sheet(df: pd.DataFrame, sheet_name: str,
                 source_ref: str, base_row_offset: int,
                 logs: List[ParseLog]) -> tuple[List[Dict[str, Any]], int, int]:
    """
    Parse a single CARGAS sheet.
    Returns (rows, accepted_count, skipped_count).
    """
    col_map  = build_col_map(df)
    date_col    = find_column(col_map, *COL_DATE)
    supp_col    = find_column(col_map, *COL_SUPPLIER)
    frigo_col   = find_column(col_map, *COL_FRIGO)
    classic_col = find_column(col_map, *COL_CLASSIC)
    a_col       = find_column(col_map, *COL_A)
    b_col       = find_column(col_map, *COL_B)
    c_col       = find_column(col_map, *COL_C)
    rep_col     = find_column(col_map, *COL_REPASS)

    if supp_col is None:
        log_warn(logs, None, "sheet",
                 f"Sheet '{sheet_name}': supplier column not found "
                 f"(searched for FRIGOR). Sheet skipped.")
        return [], 0, 0

    rows:    List[Dict[str, Any]] = []
    skipped = 0

    for i, (_, r) in enumerate(df.iterrows()):
        row_num = base_row_offset + i + 2  # 1-based absolute row number

        # Supplier filter
        supplier_raw = normalize_text(r[supp_col]) if supp_col is not None else None
        if is_skip_supplier(supplier_raw):
            log_info(logs, row_num, "supplier_name_raw",
                     f"Sheet '{sheet_name}' row skipped: {repr(supplier_raw)}")
            skipped += 1
            continue

        row = base_row(source_ref, data_source="spreadsheet")
        row["sheet_name"]        = sheet_name
        row["arrival_date"]      = normalize_date(r[date_col])    if date_col    is not None else None
        row["supplier_name_raw"] = supplier_raw
        row["frigo_count"]       = normalize_number(r[frigo_col]) if frigo_col   is not None else None
        row["classic_count"]     = normalize_number(r[classic_col]) if classic_col is not None else None
        row["class_a_count"]     = normalize_number(r[a_col])     if a_col       is not None else None
        row["class_b_count"]     = normalize_number(r[b_col])     if b_col       is not None else None
        row["class_c_count"]     = normalize_number(r[c_col])     if c_col       is not None else None
        row["repass_count"]      = normalize_number(r[rep_col])   if rep_col     is not None else None

        # Warn when no count data at all
        if (row["classic_count"] is None
                and row["class_a_count"] is None
                and row["class_b_count"] is None
                and row["class_c_count"] is None):
            log_warn(logs, row_num, "classic_count",
                     f"Sheet '{sheet_name}': all count fields are null for "
                     f"supplier '{supplier_raw}'. Row retained with nulls.")

        # Compute percentages + mismatch flag
        _compute_percentages(row)

        if row["has_formula_mismatch"]:
            log_warn(logs, row_num, "classic_count",
                     f"Formula mismatch in sheet '{sheet_name}': "
                     f"a+b+c={row.get('class_a_count', 0)+row.get('class_b_count', 0)+row.get('class_c_count', 0):.0f} "
                     f"!= classic={row['classic_count']:.0f}")

        # Validations
        if row["arrival_date"] is None:
            log_warn(logs, row_num, "arrival_date",
                     "arrival_date could not be parsed.",
                     str(r[date_col]) if date_col is not None else None)

        validate_non_negative(logs, row_num, row, NON_NEGATIVE_FIELDS)

        # Anti-dup hash
        row["content_hash"] = content_hash(row, fields=[
            "sheet_name", "arrival_date", "supplier_name_raw",
        ])

        rows.append(row)

    return rows, len(rows), skipped


def parse_load_classification(file_path: str) -> ParseResult:
    """
    Main entry point. Iterates all CARGAS sheets, returns ParseResult.
    """
    logs:   List[ParseLog] = []
    rows:   List[Dict[str, Any]] = []
    path    = Path(file_path)
    source  = path.name

    logger.info("Parsing CARGAS classification file: %s", source)

    try:
        xls = pd.ExcelFile(file_path)
    except Exception as exc:
        log_err(logs, None, "file", f"Cannot open file: {exc}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="load_classification")

    cargas_sheets = [s for s in xls.sheet_names if _is_cargas_sheet(s)]
    if not cargas_sheets:
        log_err(logs, None, "sheet",
                f"No CARGAS sheets found. Available: {xls.sheet_names}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="load_classification")

    logger.info("Found %d CARGAS sheets: %s", len(cargas_sheets), cargas_sheets)

    total_input = 0
    total_skip  = 0

    for sheet in cargas_sheets:
        try:
            # Read raw first to detect header row
            raw = pd.read_excel(file_path, sheet_name=sheet, header=None)
            hdr = detect_header_row(raw, HEADER_KEYWORDS, max_scan=10)
            df  = pd.read_excel(file_path, sheet_name=sheet, header=hdr)
            total_input += len(df)
        except Exception as exc:
            log_err(logs, None, "sheet",
                    f"Failed to read sheet '{sheet}': {exc}")
            continue

        sheet_rows, accepted, skipped = _parse_sheet(
            df, sheet, source, base_row_offset=hdr, logs=logs
        )
        rows.extend(sheet_rows)
        total_skip += skipped

    logger.info(
        "CARGAS '%s': %d total input rows, %d accepted, %d skipped, "
        "%d warnings, %d errors",
        source, total_input, len(rows), total_skip,
        sum(1 for l in logs if l.severity == "warning"),
        sum(1 for l in logs if l.severity == "error"),
    )

    return ParseResult(
        rows=rows, logs=logs,
        total_input_rows=total_input,
        skipped_rows=total_skip,
        source_reference=source,
        entity_type="load_classification",
    )
