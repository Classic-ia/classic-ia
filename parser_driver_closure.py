"""
sourceiq/etl/parser_driver_closure.py  —  v2.0
===============================================
Parses FECHAMENTO driver closure spreadsheets.

Source:     One .xlsx file per driver.  Filename stem = driver name.
Sheet:      Named "FECHAMENTO" (or first sheet whose name contains "FECHA").
Layout:     Headerless rows; columns at fixed positions 0-12:
              0  closure_date
              1  collection_date
              2  city
              3  supplier_name_raw
              4  storage_type
              5  frigo_count
              6  classic_count
              7  difference_count      ← IGNORED from source; always recomputed
              8  torn_count
              9  pierced_count
              10 peeling_count
              11 no_leaf_count
              12 dirty_count

Target DB:  public.receipt_load_closures

Critical rule: difference_count is RECOMPUTED as (classic_count - frigo_count).
               The spreadsheet value is stored in difference_count_source for
               audit purposes but the recomputed value is what is promoted.

Anti-duplication key: content_hash over
    (driver_name, closure_date, supplier_name_raw, frigo_count, classic_count)
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd

from common import (
    ParseResult, ParseLog,
    normalize_text, normalize_number, normalize_date,
    is_skip_supplier,
    content_hash, base_row,
    validate_required, validate_non_negative, validate_formula,
    detect_header_row,
    log_info, log_warn, log_err,
    make_logger,
)

logger = make_logger("parser_driver_closure")

# The sheet we expect. Also accepted: any sheet whose name contains FECHA.
PRIMARY_SHEET_NAME = "FECHAMENTO"
HEADER_KEYWORDS    = ["MOTORISTA", "DATA", "FRIGOR", "CONFER", "CLASSIC"]

# Column positions (0-based) when the layout has NO header row detected
FIXED_POSITIONS = {
    "closure_date":    0,
    "collection_date": 1,
    "city":            2,
    "supplier_name":   3,
    "storage_type":    4,
    "frigo_count":     5,
    "classic_count":   6,
    "difference_count_source": 7,   # from spreadsheet — audit only
    "torn_count":      8,
    "pierced_count":   9,
    "peeling_count":   10,
    "no_leaf_count":   11,
    "dirty_count":     12,
}

# Required for a row to be valid
REQUIRED_FIELDS = ["supplier_name_raw", "closure_date"]
NON_NEGATIVE_FIELDS = [
    "frigo_count", "classic_count", "torn_count",
    "pierced_count", "peeling_count", "no_leaf_count", "dirty_count",
]


def _find_sheet(xls: pd.ExcelFile) -> Optional[str]:
    """Return the sheet name to parse, or None if not found."""
    if PRIMARY_SHEET_NAME in xls.sheet_names:
        return PRIMARY_SHEET_NAME
    for name in xls.sheet_names:
        if "FECHA" in name.upper():
            return name
    return None


def _read_sheet_with_header_detection(path: str, sheet: str) -> pd.DataFrame:
    """
    Try to auto-detect a header row in the first 12 rows.
    Fall back to treating the file as fully headerless (positions only).
    Returns a DataFrame with positional integer column names when no
    header is found (header=None mode), or named columns otherwise.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    hdr_row = detect_header_row(raw, HEADER_KEYWORDS, max_scan=12)
    if hdr_row > 0:
        # Real header found at row hdr_row — re-read with that as header
        df = pd.read_excel(path, sheet_name=sheet, header=hdr_row)
        logger.info("Header row detected at row %d in sheet '%s'", hdr_row, sheet)
        return df
    # No header — use positional integer columns (0, 1, 2 …)
    return raw


def _extract_row_positional(vals: list, row_num: int,
                             driver_name: str, source_ref: str,
                             logs: List[ParseLog]) -> Optional[Dict[str, Any]]:
    """
    Extract one data row from a list of cell values using fixed column positions.
    Returns None if the row should be skipped.
    """
    def _get(pos):
        return vals[pos] if len(vals) > pos else None

    supplier_raw = normalize_text(_get(FIXED_POSITIONS["supplier_name"]))
    if is_skip_supplier(supplier_raw):
        log_info(logs, row_num, "supplier_name_raw",
                 f"Row skipped — supplier value is a totals/empty token: {repr(supplier_raw)}")
        return None

    frigo   = normalize_number(_get(FIXED_POSITIONS["frigo_count"]))
    classic = normalize_number(_get(FIXED_POSITIONS["classic_count"]))

    # A row must have at least one count to be meaningful
    if frigo is None and classic is None:
        log_info(logs, row_num, "frigo_count",
                 "Row skipped — both frigo_count and classic_count are null.")
        return None

    row = base_row(source_ref, data_source="spreadsheet")
    row["driver_name"]        = driver_name
    row["closure_date"]       = normalize_date(_get(FIXED_POSITIONS["closure_date"]))
    row["collection_date"]    = normalize_date(_get(FIXED_POSITIONS["collection_date"]))
    row["city"]               = normalize_text(_get(FIXED_POSITIONS["city"]))
    row["supplier_name_raw"]  = supplier_raw
    row["storage_type"]       = normalize_text(_get(FIXED_POSITIONS["storage_type"]))
    row["frigo_count"]        = frigo
    row["classic_count"]      = classic

    # ALWAYS recompute difference — never trust the spreadsheet value
    src_diff = normalize_number(_get(FIXED_POSITIONS["difference_count_source"]))
    if frigo is not None and classic is not None:
        row["difference_count"] = round(classic - frigo, 3)
        # Validate against spreadsheet value (warn only — don't reject)
        validate_formula(logs, row_num, row["difference_count"], src_diff,
                         "difference_count")
        if frigo > 0:
            row["difference_percent"] = round(
                ((classic - frigo) / frigo) * 100, 4
            )
        else:
            row["difference_percent"] = None
    else:
        row["difference_count"]   = None
        row["difference_percent"] = None

    row["difference_count_source"] = src_diff   # preserved for audit

    row["torn_count"]    = normalize_number(_get(FIXED_POSITIONS["torn_count"]))
    row["pierced_count"] = normalize_number(_get(FIXED_POSITIONS["pierced_count"]))
    row["peeling_count"] = normalize_number(_get(FIXED_POSITIONS["peeling_count"]))
    row["no_leaf_count"] = normalize_number(_get(FIXED_POSITIONS["no_leaf_count"]))
    row["dirty_count"]   = normalize_number(_get(FIXED_POSITIONS["dirty_count"]))

    # Validation
    validate_required(logs, row_num, row, REQUIRED_FIELDS)
    validate_non_negative(logs, row_num, row, NON_NEGATIVE_FIELDS)

    if row["closure_date"] is None:
        log_warn(logs, row_num, "closure_date",
                 "closure_date could not be parsed.",
                 str(_get(FIXED_POSITIONS["closure_date"])))

    # Content hash for deduplication
    row["content_hash"] = content_hash(row, fields=[
        "driver_name", "closure_date", "supplier_name_raw",
        "frigo_count", "classic_count",
    ])

    return row


def parse_driver_closure(file_path: str) -> ParseResult:
    """
    Main entry point.  Returns ParseResult with all accepted rows and logs.
    Never raises — all exceptions are caught and logged as errors.
    """
    logs:   List[ParseLog] = []
    rows:   List[Dict[str, Any]] = []
    path    = Path(file_path)
    source  = path.name
    driver  = path.stem

    logger.info("Parsing FECHAMENTO file: %s (driver=%s)", source, driver)

    try:
        xls = pd.ExcelFile(file_path)
    except Exception as exc:
        log_err(logs, None, "file", f"Cannot open file: {exc}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="receipt_load_closure")

    sheet = _find_sheet(xls)
    if sheet is None:
        log_err(logs, None, "sheet",
                f"No FECHAMENTO sheet found. Available: {xls.sheet_names}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="receipt_load_closure")

    try:
        df = _read_sheet_with_header_detection(file_path, sheet)
    except Exception as exc:
        log_err(logs, None, "sheet", f"Failed to read sheet '{sheet}': {exc}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="receipt_load_closure")

    total_input = len(df)
    skipped     = 0

    for row_idx, row in df.iterrows():
        row_num = int(row_idx) + 2  # 1-based, accounting for header row

        # DataFrame with named columns: look up by FIXED_POSITIONS keys
        # but the column names are integers (positional) when no header found
        vals = row.tolist()

        result = _extract_row_positional(vals, row_num, driver, source, logs)
        if result is None:
            skipped += 1
            continue
        rows.append(result)

    logger.info(
        "FECHAMENTO '%s': %d input rows, %d accepted, %d skipped, "
        "%d warnings, %d errors",
        source, total_input, len(rows), skipped,
        sum(1 for l in logs if l.severity == "warning"),
        sum(1 for l in logs if l.severity == "error"),
    )

    return ParseResult(
        rows=rows, logs=logs,
        total_input_rows=total_input,
        skipped_rows=skipped,
        source_reference=source,
        entity_type="receipt_load_closure",
    )
