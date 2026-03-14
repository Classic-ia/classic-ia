"""
sourceiq/etl/run_etl.py  —  v2.0
==================================
ETL orchestrator: file registration → parse → stage → validate → promote.

Usage (CLI):
  python run_etl.py --file path/to/file.xlsx --entity_type load_classification
  python run_etl.py --file WRMVE500_2026.pdf  --entity_type receipt_requisition
  python run_etl.py --file Semana_10_2026.xlsx --entity_type employee_production

Phases:
  1. REGISTER   — write import_files row; compute file_hash; detect duplicates
  2. PARSE      — dispatch to parser; get ParseResult
  3. STAGE      — write rows to staging table via Supabase REST
  4. VALIDATE   — run validation rules; update staging row statuses; write logs
  5. COMMIT     — on operator approval: promote valid staging rows to final tables

Commit is NOT automatic. After STAGE+VALIDATE the script prints a summary
and exits. The operator reviews in the UI and triggers commit via:
  POST /api/v1/imports/files/:id/commit
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from common import (
    ParseResult, ParseLog,
    file_hash, make_logger,
)

logger = make_logger("run_etl")

# ─── SUPABASE CONFIG ─────────────────────────────────────────────────────────
# Read from environment. Never hardcode credentials.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")  # service_role key

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning(
        "SUPABASE_URL or SUPABASE_SERVICE_KEY not set. "
        "DB writes will fail. Set these environment variables."
    )

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation",
}


# ─── PARSER DISPATCH MAP ─────────────────────────────────────────────────────
# Maps import_entity_type enum values to their parser functions.
# Each function must accept (file_path: str) and return ParseResult.

def _get_parser(entity_type: str):
    """
    Lazy imports to keep startup fast and avoid import errors for missing deps.
    Returns the parser function for entity_type, or raises ValueError.
    """
    if entity_type == "receipt_load_closure":
        from parser_driver_closure import parse_driver_closure
        return parse_driver_closure

    if entity_type == "load_classification":
        from parser_load_classification import parse_load_classification
        return parse_load_classification

    if entity_type == "employee_production":
        from parser_sector_productivity import parse_sector_productivity
        return parse_sector_productivity

    if entity_type == "receipt_requisition":
        # PDF path → extract text → parse
        def _parse_receipt(path: str):
            from parser_atak_receipts import parse_atak_receipts_pdf
            return parse_atak_receipts_pdf(path)
        return _parse_receipt

    if entity_type == "atak_employee_production":
        def _parse_prod(path: str):
            from parser_atak_employee_production import parse_atak_employee_production_pdf
            return parse_atak_employee_production_pdf(path)
        return _parse_prod

    raise ValueError(
        f"Unknown entity_type: '{entity_type}'. "
        f"Valid values: receipt_load_closure, load_classification, "
        f"employee_production, receipt_requisition, atak_employee_production"
    )


# ─── TARGET TABLE MAP ────────────────────────────────────────────────────────
# Maps entity_type to the final Supabase table for promotion.
ENTITY_TARGET_TABLE = {
    "receipt_load_closure":    "receipt_load_closures",
    "load_classification":     "load_quality_classifications",
    "employee_production":     "employee_production_logs",
    "receipt_requisition":     "atak_receipts",
    "atak_employee_production": "employee_production_atak",
}

# ─── STAGING TABLE ───────────────────────────────────────────────────────────
# All entity types use the generic staging table first.
# Defects (from atak_receipts) are promoted to atak_quality_defects separately.
STAGING_TABLE = "staging_quality_import"


# ─── SUPABASE REST HELPERS ───────────────────────────────────────────────────

class SupabaseWriter:
    """Thin wrapper around Supabase REST API for ETL writes."""

    def __init__(self, url: str = SUPABASE_URL, key: str = SUPABASE_KEY):
        self.base = url
        self.headers = {
            "apikey":        key,
            "Authorization": f"Bearer {key}",
            "Content-Type":  "application/json",
            "Prefer":        "return=representation",
        }

    def _url(self, table: str) -> str:
        return f"{self.base}/rest/v1/{table}"

    def insert_one(self, table: str, row: dict) -> Optional[dict]:
        """Insert a single row. Returns the inserted row or None on failure."""
        resp = requests.post(self._url(table), headers=self.headers,
                             data=json.dumps(row, default=str))
        if resp.status_code in (200, 201):
            data = resp.json()
            return data[0] if isinstance(data, list) else data
        logger.error("insert_one failed [%d]: %s", resp.status_code, resp.text[:300])
        return None

    def insert_batch(self, table: str, rows: List[dict],
                     batch_size: int = 100) -> tuple[int, int]:
        """
        Insert rows in batches. Returns (inserted_count, failed_count).
        Uses upsert with on_conflict=content_hash to handle retries safely.
        """
        inserted = 0
        failed   = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            resp  = requests.post(
                self._url(table),
                headers={**self.headers, "Prefer": "resolution=merge-duplicates,return=representation"},
                data=json.dumps(batch, default=str),
            )
            if resp.status_code in (200, 201):
                inserted += len(batch)
            else:
                logger.error(
                    "Batch insert failed [%d] rows %d-%d: %s",
                    resp.status_code, i, i + len(batch), resp.text[:300]
                )
                failed += len(batch)
        return inserted, failed

    def update_one(self, table: str, record_id: str, patch: dict) -> bool:
        """PATCH a single row by id."""
        resp = requests.patch(
            f"{self._url(table)}?id=eq.{record_id}",
            headers=self.headers,
            data=json.dumps(patch, default=str),
        )
        return resp.status_code in (200, 204)

    def insert_logs(self, file_id: str, logs: List[ParseLog]) -> None:
        """Write ParseLog entries to import_logs table in batches."""
        if not logs:
            return
        rows = [
            {
                "import_file_id": file_id,
                "row_number":     lg.row_number,
                "phase":          lg.phase,
                "level":          lg.severity,
                "field_name":     lg.field,
                "message":        lg.message,
                "raw_value":      lg.raw_value,
            }
            for lg in logs
        ]
        inserted, failed = self.insert_batch("import_logs", rows)
        if failed:
            logger.warning("Failed to write %d log entries to import_logs.", failed)


# ─── PHASE 1: REGISTER ───────────────────────────────────────────────────────

def register_file(writer: SupabaseWriter, file_path: str,
                  entity_type: str, source_label: str = "") -> Optional[str]:
    """
    Create import_files record. Detect duplicate via file_hash.
    Returns import_file_id (UUID) or None if duplicate/failed.
    """
    path  = Path(file_path)
    fhash = file_hash(file_path)

    # Check for duplicate
    resp = requests.get(
        f"{writer.base}/rest/v1/import_files?file_hash=eq.{fhash}&select=id,file_name,status",
        headers=writer.headers,
    )
    if resp.status_code == 200:
        existing = resp.json()
        if existing:
            prior = existing[0]
            logger.warning(
                "Duplicate file detected: hash=%s matches existing import_file "
                "id=%s (name='%s', status=%s). Aborting.",
                fhash, prior["id"], prior["file_name"], prior["status"]
            )
            return None

    row = {
        "file_name":    path.name,
        "file_hash":    fhash,
        "file_type":    path.suffix.lstrip(".").lower(),
        "file_size_bytes": path.stat().st_size,
        "entity_type":  entity_type,
        "source_label": source_label or path.stem,
        "status":       "uploaded",
        "uploaded_at":  datetime.now(timezone.utc).isoformat(),
    }

    result = writer.insert_one("import_files", row)
    if result:
        logger.info("Registered import_file id=%s for '%s'", result["id"], path.name)
        return result["id"]
    return None


# ─── PHASE 2: PARSE ──────────────────────────────────────────────────────────

def run_parse(file_path: str, entity_type: str) -> ParseResult:
    """Dispatch to the appropriate parser. Never raises."""
    try:
        parser_fn = _get_parser(entity_type)
    except ValueError as exc:
        logger.error(str(exc))
        from common import ParseLog
        return ParseResult(
            rows=[], logs=[ParseLog(None, "parse", "error", "entity_type", str(exc))],
            total_input_rows=0, skipped_rows=0,
            source_reference=Path(file_path).name,
            entity_type=entity_type,
        )

    try:
        return parser_fn(file_path)
    except Exception as exc:
        logger.exception("Parser raised an unhandled exception: %s", exc)
        from common import ParseLog
        return ParseResult(
            rows=[], logs=[ParseLog(None, "parse", "error", "file",
                                   f"Unhandled parser exception: {exc}")],
            total_input_rows=0, skipped_rows=0,
            source_reference=Path(file_path).name,
            entity_type=entity_type,
        )


# ─── PHASE 3: STAGE ──────────────────────────────────────────────────────────

def stage_rows(writer: SupabaseWriter, result: ParseResult,
               file_id: str) -> tuple[int, int]:
    """
    Write ParseResult.rows to staging_quality_import.
    Adds import_file_id and validation_status='pending' to each row.
    Returns (inserted, failed).
    """
    staging_rows = []
    for row in result.rows:
        # Flatten the row into staging columns
        sr = {
            "import_file_id":     file_id,
            "row_number":         row.get("_row_number"),
            "content_hash":       row.get("content_hash"),
            "validation_status":  "pending",
            # Raw fields
            "raw_supplier":       row.get("supplier_name_raw"),
            "raw_date":           str(row.get("closure_date") or row.get("arrival_date")
                                      or row.get("production_date") or row.get("pcr_date") or ""),
            "raw_fields":         json.dumps(
                {k: str(v) for k, v in row.items()
                 if k not in ("content_hash", "data_source", "source_reference")},
                default=str,
            ),
            "data_source":        row.get("data_source"),
            "source_reference":   row.get("source_reference"),
        }
        # Normalised fields where applicable
        if "arrival_date" in row:
            sr["normalized_date"] = str(row["arrival_date"]) if row["arrival_date"] else None
        if "closure_date" in row:
            sr["normalized_date"] = str(row["closure_date"]) if row["closure_date"] else None
        if "production_date" in row:
            sr["normalized_date"] = str(row["production_date"]) if row["production_date"] else None
        if "pcr_date" in row:
            sr["normalized_date"] = str(row["pcr_date"]) if row["pcr_date"] else None

        staging_rows.append(sr)

    ins, fail = writer.insert_batch(STAGING_TABLE, staging_rows)
    logger.info("Staged %d rows (%d failed) for file_id=%s", ins, fail, file_id)
    return ins, fail


# ─── PHASE 4: VALIDATE ───────────────────────────────────────────────────────

# Validation rules per entity type.
# Each rule: (field_name, condition_fn, message, severity)
# condition_fn(row) -> True means the row PASSES; False triggers the log entry.

VALIDATION_RULES: Dict[str, List[tuple]] = {
    "receipt_load_closure": [
        ("supplier_name_raw", lambda r: r.get("supplier_name_raw") is not None,
         "supplier_name_raw is required.", "error"),
        ("closure_date",      lambda r: r.get("closure_date") is not None,
         "closure_date could not be parsed.", "warning"),
        ("frigo_count",       lambda r: r.get("frigo_count") is None or r["frigo_count"] >= 0,
         "frigo_count is negative.", "warning"),
        ("classic_count",     lambda r: r.get("classic_count") is None or r["classic_count"] >= 0,
         "classic_count is negative.", "warning"),
    ],
    "load_classification": [
        ("supplier_name_raw", lambda r: r.get("supplier_name_raw") is not None,
         "supplier_name_raw is required.", "error"),
        ("arrival_date",      lambda r: r.get("arrival_date") is not None,
         "arrival_date could not be parsed.", "warning"),
        ("class_sum_check",   lambda r: not r.get("has_formula_mismatch", False),
         "class_a + class_b + class_c does not equal classic_count.", "warning"),
    ],
    "employee_production": [
        ("employee_name",    lambda r: r.get("employee_name") is not None,
         "employee_name is required.", "error"),
        ("week_reference",   lambda r: r.get("week_reference") is not None,
         "week_reference is required.", "error"),
        ("pieces_produced",  lambda r: r.get("pieces_produced") is None or r["pieces_produced"] >= 0,
         "pieces_produced is negative.", "warning"),
        ("hours_worked",     lambda r: r.get("hours_worked") is None or r["hours_worked"] >= 0,
         "hours_worked is negative.", "warning"),
    ],
    "receipt_requisition": [
        ("document_number",  lambda r: r.get("document_number") is not None,
         "document_number is required.", "error"),
        ("pcr_date",         lambda r: r.get("pcr_date") is not None,
         "pcr_date could not be parsed.", "warning"),
    ],
    "atak_employee_production": [
        ("production_date",  lambda r: r.get("production_date") is not None,
         "production_date is required.", "error"),
        ("employee_code",    lambda r: r.get("employee_code") is not None,
         "employee_code is required.", "error"),
        ("product_code",     lambda r: r.get("product_code") is not None,
         "product_code is required.", "error"),
        ("quantity_produced", lambda r: r.get("quantity_produced") is None or r["quantity_produced"] >= 0,
         "quantity_produced is negative.", "warning"),
    ],
}


def validate_rows(result: ParseResult) -> List[Dict[str, Any]]:
    """
    Run entity-specific validation rules against each row.
    Returns list of {row_index, field, message, severity, status} dicts
    used to update staging row statuses.
    """
    rules = VALIDATION_RULES.get(result.entity_type, [])
    validation_results = []

    for idx, row in enumerate(result.rows):
        row_errors   = 0
        row_warnings = 0

        for field, condition, message, severity in rules:
            if not condition(row):
                validation_results.append({
                    "row_index": idx,
                    "field":     field,
                    "message":   message,
                    "severity":  severity,
                })
                if severity == "error":
                    row_errors += 1
                else:
                    row_warnings += 1

        # Set validation_status on the row itself for staging write
        if row_errors > 0:
            row["_validation_status"] = "invalid"
        elif row_warnings > 0:
            row["_validation_status"] = "warning"
        else:
            row["_validation_status"] = "valid"

    return validation_results


# ─── PHASE 5: PROMOTE (stub — real promote triggered via API) ─────────────────

def promote_to_final(writer: SupabaseWriter, file_id: str,
                     result: ParseResult) -> tuple[int, int]:
    """
    Write valid rows from ParseResult directly to the final operational table.
    In production this is triggered by POST /imports/files/:id/commit via the API.
    This function is provided for CLI batch mode only.

    Returns (promoted, failed).
    """
    target = ENTITY_TARGET_TABLE.get(result.entity_type)
    if not target:
        logger.error("No target table for entity_type '%s'.", result.entity_type)
        return 0, len(result.rows)

    valid_rows = [r for r in result.rows if r.get("_validation_status") != "invalid"]

    # Strip internal fields before inserting
    clean_rows = []
    skip_keys  = {"_validation_status", "_row_number", "content_hash"}
    for r in valid_rows:
        clean = {k: v for k, v in r.items()
                 if k not in skip_keys and not k.startswith("_")}
        # Defects get promoted separately
        clean.pop("defects", None)
        clean_rows.append(clean)

    ins, fail = writer.insert_batch(target, clean_rows)

    # Promote defects for receipt_requisition
    if result.entity_type == "receipt_requisition":
        _promote_defects(writer, valid_rows)

    # Update import_files status
    status = "imported" if fail == 0 else "imported_with_errors"
    writer.update_one("import_files", file_id, {
        "status":        status,
        "imported_rows": ins,
        "rejected_rows": fail + len(result.rows) - len(valid_rows),
        "committed_at":  datetime.now(timezone.utc).isoformat(),
    })

    return ins, fail


def _promote_defects(writer: SupabaseWriter, receipt_rows: List[Dict]) -> None:
    """Write defect sub-rows to atak_quality_defects."""
    defect_rows = []
    for r in receipt_rows:
        defects = r.get("defects") or []
        for d in defects:
            defect_rows.append({
                "defect_name_raw": d["defect_name_raw"],
                "quantity":        d["quantity"],
                "data_source":     r.get("data_source", "atak"),
                "source_reference": r.get("source_reference"),
                # receipt_id FK is set by a second-pass resolution job
            })
    if defect_rows:
        ins, fail = writer.insert_batch("atak_quality_defects", defect_rows)
        logger.info("Defects promoted: %d inserted, %d failed.", ins, fail)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="SourceIQ ETL orchestrator: parse → stage → validate"
    )
    ap.add_argument("--file",        required=True, help="Path to the input file")
    ap.add_argument("--entity_type", required=True,
                    choices=list(ENTITY_TARGET_TABLE.keys()),
                    help="Entity type (must match import_entity_type enum)")
    ap.add_argument("--source_label", default="",
                    help="Human-readable label for the import (e.g. 'FECHAMENTO Jan 2026')")
    ap.add_argument("--commit",  action="store_true",
                    help="Automatically promote valid rows to final tables (use with caution)")
    ap.add_argument("--dry_run", action="store_true",
                    help="Parse and validate only; do not write to DB")
    args = ap.parse_args()

    if not Path(args.file).exists():
        logger.error("File not found: %s", args.file)
        sys.exit(1)

    writer = SupabaseWriter()

    # ── Phase 1: Register ──────────────────────────────────────────────
    if not args.dry_run:
        file_id = register_file(writer, args.file, args.entity_type, args.source_label)
        if file_id is None:
            logger.error("File registration failed (possible duplicate). Aborting.")
            sys.exit(1)
    else:
        file_id = "dry_run_file_id"
        logger.info("[DRY RUN] Skipping file registration.")

    # ── Phase 2: Parse ─────────────────────────────────────────────────
    result = run_parse(args.file, args.entity_type)
    logger.info("Parse summary: %s", result.summary())

    # ── Phase 3 & 4: Stage + Validate ─────────────────────────────────
    validation_issues = validate_rows(result)

    if not args.dry_run:
        staged_ok, staged_fail = stage_rows(writer, result, file_id)
        writer.insert_logs(file_id, result.logs)

        # Update import_files with counts
        writer.update_one("import_files", file_id, {
            "status":         "validated",
            "total_rows":     result.total_input_rows,
            "validated_rows": sum(1 for r in result.rows if r.get("_validation_status") == "valid"),
            "warning_rows":   sum(1 for r in result.rows if r.get("_validation_status") == "warning"),
            "rejected_rows":  sum(1 for r in result.rows if r.get("_validation_status") == "invalid"),
            "validated_at":   datetime.now(timezone.utc).isoformat(),
        })
    else:
        logger.info("[DRY RUN] Skipping stage/log writes.")
        logger.info("Validation issues: %d", len(validation_issues))
        for iss in validation_issues[:20]:
            logger.info("  row %d | %s | %s | %s",
                        iss["row_index"], iss["field"], iss["severity"], iss["message"])

    # ── Phase 5: Promote (optional) ────────────────────────────────────
    if args.commit and not args.dry_run:
        if result.has_critical_errors:
            logger.error(
                "COMMIT BLOCKED: error rate %.1f%% exceeds 10%% threshold. "
                "Review validation errors before committing.",
                result.error_count / max(result.total_input_rows, 1) * 100
            )
            sys.exit(2)
        promoted, failed = promote_to_final(writer, file_id, result)
        logger.info("Promotion complete: %d promoted, %d failed.", promoted, failed)
    elif not args.dry_run:
        logger.info(
            "STAGE complete. %d rows staged. "
            "Review in UI and POST /api/v1/imports/files/%s/commit to promote.",
            result.accepted_rows, file_id
        )

    # ── Exit code ──────────────────────────────────────────────────────
    sys.exit(0 if result.error_count == 0 else 1)


if __name__ == "__main__":
    main()
