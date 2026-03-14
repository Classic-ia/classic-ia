"""
sourceiq/etl/parser_sector_productivity.py  —  v2.0
====================================================
Parses weekly sector productivity spreadsheets.

Source:     One .xlsx file per week.  Filename stem = week reference.
            e.g. "Semana_10_2026.xlsx" → week_reference = "Semana_10_2026"
Sheets:     Only sheets in VALID_SHEETS are processed; all others ignored.
Layout:     Headerless rows.  Columns are not labelled consistently.
            Row structure (variable width):
              col 0 or 1  employee name (may include trailing employee code)
              remaining numeric cells: [day values..., total_pieces, hours, pph]

Target DB:  public.employee_production_logs

Critical rules:
  1. pieces_per_hour is RECOMPUTED as pieces / hours.
     The last numeric value per row is only used if we have exactly 2+
     numeric values; otherwise pieces_per_hour is derived.
  2. Employee code is extracted from the trailing numeric token in the name.
     str.replace() is UNSAFE if code appears elsewhere in the name —
     we split on whitespace and take only the last token.
  3. source_reference was MISSING in v1 — added here.
  4. Rows that are header/day-name rows are filtered before any processing.

Anti-duplication key: content_hash over
    (week_reference, sector_name_raw, employee_name, employee_code)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from common import (
    ParseResult, ParseLog,
    normalize_text, normalize_number,
    is_header_row,
    normalize_employee_name,
    content_hash, base_row,
    validate_required, validate_non_negative,
    log_info, log_warn, log_err,
    make_logger,
)

logger = make_logger("parser_sector_productivity")

# Must match production_sectors.sector_name in the DB and VALID_SHEETS in run_etl.py
import re as _re
import unicodedata as _ud

def _normalize_sheet_name(name: str) -> str:
    """
    Normalize a sheet name for VALID_SHEETS comparison.
    Strips accents, lowercases, collapses all whitespace to single space.
    "BATE   SAL", "BATE  SAL", "BATE SAL" all → "bate sal"
    "CLASSIFICAÇÃO", "CLASSIFICACAO" both → "classificacao"
    """
    nfd = _ud.normalize("NFD", name)
    no_acc = "".join(c for c in nfd if _ud.category(c) != "Mn")
    return _re.sub(r"\s+", " ", no_acc.lower().strip())


# Canonical sector names (accented, as stored in production_sectors)
_CANONICAL_SECTORS: frozenset = frozenset({
    "PRODUCAO MAQ", "BATE   SAL", "CLASSIFICAÇÃO",
    "LAVAÇÃO", "SEBO", "CONFERENCIA SALGA", "SALGA", "QUALIDADE",
})

# Normalized lookup set — comparison always goes through _normalize_sheet_name()
VALID_SHEETS_NORMALIZED: frozenset = frozenset({
    _normalize_sheet_name(s) for s in _CANONICAL_SECTORS
})

# Keep VALID_SHEETS as alias for external callers that may reference it
VALID_SHEETS = _CANONICAL_SECTORS

REQUIRED_FIELDS      = ["employee_name", "sector_name_raw", "week_reference"]
NON_NEGATIVE_FIELDS  = ["pieces_produced", "hours_worked", "pieces_per_hour"]

# Minimum number of numeric values in a row to attempt extraction.
# A row with < MIN_NUMS numbers is likely a label or partial entry.
MIN_NUMS = 2


def _extract_numeric_values(vals: list) -> List[float]:
    """
    Return all non-None normalize_number results from a row's cell values,
    in order.  Float values that came from pandas as NaN are already None
    after normalize_number so they are excluded.
    """
    return [n for v in vals if (n := normalize_number(v)) is not None]


def _extract_production_numbers(
    nums: List[float],
    row_num: int,
    logs: List[ParseLog],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Given all numeric values extracted from a row, derive:
      (pieces_produced, hours_worked, pieces_per_hour)

    Layout observed in CARGAS sheets:
      [day1, day2, ..., dayN, total_pieces, hours_worked, pieces_per_hour]

    Strategy:
      - pieces_per_hour = last element if len >= 2
      - hours_worked    = second-to-last if len >= 2
      - pieces_produced = first element (col 0 numeric, or the total)

    We ALWAYS recompute pph = pieces / hours and compare to the source value.
    If they differ by more than 1% we log a warning but keep the recomputed value.
    """
    if len(nums) < MIN_NUMS:
        return nums[0] if nums else None, None, None

    # With 2 values: interpret as (pieces, hours)
    if len(nums) == 2:
        pieces, hours = nums[0], nums[1]
        source_pph = None
    else:
        # Multiple values: first = pieces total, last = pph, second-to-last = hours
        pieces    = nums[0]
        hours     = nums[-2]
        source_pph = nums[-1]

    # Recompute pph
    if hours and hours > 0 and pieces is not None:
        recomputed_pph = round(pieces / hours, 4)
    else:
        recomputed_pph = None

    if source_pph is not None and recomputed_pph is not None:
        relative_diff = abs(recomputed_pph - source_pph) / max(abs(source_pph), 0.001)
        if relative_diff > 0.01:  # > 1% discrepancy
            log_warn(logs, row_num, "pieces_per_hour",
                     f"PPH mismatch: source={source_pph:.4f}, "
                     f"recomputed={recomputed_pph:.4f} (diff={relative_diff:.1%})",
                     str(source_pph))

    return pieces, hours, recomputed_pph


def _parse_sheet(df: pd.DataFrame, sheet_name: str,
                 week_ref: str, source_ref: str,
                 logs: List[ParseLog]) -> tuple[List[Dict], int, int]:
    """Parse one sector sheet. Returns (rows, accepted, skipped)."""
    accepted: List[Dict[str, Any]] = []
    skipped = 0

    for row_idx, row in df.iterrows():
        row_num = int(row_idx) + 2  # 1-based
        vals = row.tolist()

        # Name is in col 0 or col 1 (some sheets have a date column first)
        raw_name = normalize_text(vals[0]) if vals else None
        if raw_name is None and len(vals) > 1:
            raw_name = normalize_text(vals[1])

        if not raw_name:
            skipped += 1
            continue

        if is_header_row(raw_name):
            log_info(logs, row_num, "employee_name",
                     f"Sheet '{sheet_name}' row {row_num} skipped (header): {repr(raw_name)}")
            skipped += 1
            continue

        nums = _extract_numeric_values(vals)
        if len(nums) < 1:
            log_info(logs, row_num, "pieces_produced",
                     f"Sheet '{sheet_name}' row {row_num} skipped (no numeric data).")
            skipped += 1
            continue

        pieces, hours, pph = _extract_production_numbers(nums, row_num, logs)

        # Split name + code
        emp_name, emp_code = normalize_employee_name(raw_name)

        r = base_row(source_ref, data_source="spreadsheet")
        r["employee_name"]    = emp_name
        r["employee_code"]    = emp_code
        r["sector_name_raw"]  = sheet_name
        r["week_reference"]   = week_ref
        r["pieces_produced"]  = pieces
        r["hours_worked"]     = hours
        r["pieces_per_hour"]  = pph

        validate_required(logs, row_num, r, REQUIRED_FIELDS)
        validate_non_negative(logs, row_num, r, NON_NEGATIVE_FIELDS)

        if pieces is None:
            log_warn(logs, row_num, "pieces_produced",
                     f"pieces_produced is null for employee '{emp_name}' "
                     f"in sheet '{sheet_name}'.")

        r["content_hash"] = content_hash(r, fields=[
            "week_reference", "sector_name_raw", "employee_name", "employee_code",
        ])

        accepted.append(r)

    return accepted, len(accepted), skipped


def parse_sector_productivity(file_path: str) -> ParseResult:
    """Main entry point."""
    logs:   List[ParseLog] = []
    rows:   List[Dict[str, Any]] = []
    path    = Path(file_path)
    source  = path.name
    week    = path.stem

    logger.info("Parsing sector productivity file: %s (week_ref=%s)", source, week)

    try:
        xls = pd.ExcelFile(file_path)
    except Exception as exc:
        log_err(logs, None, "file", f"Cannot open file: {exc}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="employee_production")

    matched_sheets = [s for s in xls.sheet_names
                      if _normalize_sheet_name(s) in VALID_SHEETS_NORMALIZED]

    if not matched_sheets:
        ignored = [s for s in xls.sheet_names
                   if s.strip().upper() not in {v.upper() for v in VALID_SHEETS}]
        log_warn(logs, None, "sheet",
                 f"No VALID_SHEETS found. Sheets in file: {xls.sheet_names}. "
                 f"Ignored: {ignored}")
        return ParseResult(rows=[], logs=logs, total_input_rows=0,
                           skipped_rows=0, source_reference=source,
                           entity_type="employee_production")

    logger.info("Processing %d sheets: %s", len(matched_sheets), matched_sheets)

    total_input = 0
    total_skip  = 0

    for sheet in matched_sheets:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, header=None)
            total_input += len(df)
        except Exception as exc:
            log_err(logs, None, "sheet",
                    f"Failed to read sheet '{sheet}': {exc}")
            continue

        sheet_rows, _, skipped = _parse_sheet(df, sheet, week, source, logs)
        rows.extend(sheet_rows)
        total_skip += skipped

    logger.info(
        "Productivity '%s': %d input rows, %d accepted, %d skipped, "
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
        entity_type="employee_production",
    )
