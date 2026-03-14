"""
sourceiq/etl/common.py  —  v2.0
================================
Shared foundation for all SourceIQ ETL parsers.

Design rules:
  - normalize_* never raises; failures return None.
  - ParseLog records every row-level event with phase, severity, field.
  - ParseResult is the only permitted return type from parser functions.
  - content_hash is deterministic regardless of dict insertion order.
  - file_hash detects duplicate uploads before any parsing begins.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

import pandas as pd

# ─── CONSTANTS ───────────────────────────────────────────────────────────────

SUPPLIER_SKIP_TOKENS: frozenset = frozenset({
    "TOTAL", "SUBTOTAL", "GRAND TOTAL", "SEM FICHA", "SEM FORNECEDOR",
    "MEDIA", "MÉDIAS", "AVERAGE", "SOMA", "SUM", "#N/A", "N/A",
    "NONE", "NULL", "NAN", "#REF!", "#VALUE!", "#NAME?",
})

HEADER_TOKENS: frozenset = frozenset({
    "DATA", "NOME", "TOTAL", "SABADO", "DOMINGO",
    "SEGUNDA", "TERCA", "TERCA-FEIRA", "TERÇA", "QUARTA", "QUINTA", "SEXTA",
    "FUNCIONARIO", "FUNCIONÁRIO", "SETOR", "SEMANA", "PERIODO", "PERÍODO",
    "HORA", "HORAS", "QTDE", "QUANTIDADE", "PECAS", "PEÇAS",
})

BR_DATE_FORMATS: Tuple[str, ...] = (
    "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y",
    "%Y-%m-%d", "%d.%m.%Y",
)

Severity = Literal["info", "warning", "error"]
Phase    = Literal["parse", "normalize", "validate", "promote"]


# ─── RESULT STRUCTURES ───────────────────────────────────────────────────────

@dataclass
class ParseLog:
    row_number:  Optional[int]
    phase:       Phase
    severity:    Severity
    field:       Optional[str]
    message:     str
    raw_value:   Optional[str] = None

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class ParseResult:
    """Returned by every parser function — no exceptions."""
    rows:             List[Dict[str, Any]]
    logs:             List[ParseLog]
    total_input_rows: int
    skipped_rows:     int
    source_reference: str
    entity_type:      str

    @property
    def accepted_rows(self) -> int: return len(self.rows)
    @property
    def error_count(self) -> int: return sum(1 for l in self.logs if l.severity == "error")
    @property
    def warning_count(self) -> int: return sum(1 for l in self.logs if l.severity == "warning")
    @property
    def has_critical_errors(self) -> bool:
        return self.total_input_rows > 0 and (self.error_count / self.total_input_rows) > 0.10

    def summary(self) -> dict:
        return {
            "total_input_rows":   self.total_input_rows,
            "accepted_rows":      self.accepted_rows,
            "skipped_rows":       self.skipped_rows,
            "error_count":        self.error_count,
            "warning_count":      self.warning_count,
            "source_reference":   self.source_reference,
            "entity_type":        self.entity_type,
            "has_critical_errors": self.has_critical_errors,
        }


# ─── LOG HELPERS ─────────────────────────────────────────────────────────────

def _log(logs, row, phase, sev, fld, msg, raw=None):
    logs.append(ParseLog(row, phase, sev, fld, msg, raw))

def log_info(logs, row, field, msg, raw=None): _log(logs, row, "parse",    "info",    field, msg, raw)
def log_warn(logs, row, field, msg, raw=None): _log(logs, row, "validate", "warning", field, msg, raw)
def log_err (logs, row, field, msg, raw=None): _log(logs, row, "validate", "error",   field, msg, raw)


# ─── NORMALISATION ───────────────────────────────────────────────────────────

def normalize_text(value: object) -> Optional[str]:
    if value is None: return None
    if isinstance(value, float) and pd.isna(value): return None
    s = re.sub(r"\s+", " ", str(value).strip())
    return s if s else None


def normalize_number(value: object) -> Optional[float]:
    """
    Brazilian decimal: dot=thousands, comma=decimal.
    '1.234,56' -> 1234.56 | '1,5' -> 1.5 | '1.5' -> 1.5 (only-dot kept as-is)
    """
    if value is None: return None
    if isinstance(value, float): return None if pd.isna(value) else value
    if isinstance(value, int): return float(value)
    s = str(value).strip()
    if not s: return None
    if "," in s and "." in s:
        cleaned = s.replace(".", "").replace(",", ".")
    elif "," in s:
        cleaned = s.replace(",", ".")
    else:
        cleaned = s
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_integer(value: object) -> Optional[int]:
    f = normalize_number(value)
    return int(f) if f is not None else None


def normalize_date(value: object) -> Optional[date]:
    if value is None: return None
    if isinstance(value, float) and pd.isna(value): return None
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    s = str(value).strip()
    if not s: return None
    for fmt in BR_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(s, dayfirst=True).date()
    except Exception:
        return None


def normalize_plate(value: object) -> Optional[str]:
    raw = normalize_text(value)
    if not raw: return None
    c = raw.upper().replace(" ", "").replace("-", "")
    if re.match(r"^[A-Z]{3}\d{4}$", c): return f"{c[:3]}-{c[3:]}"
    if re.match(r"^[A-Z]{3}\d[A-Z]\d{2}$", c): return c
    return c if len(c) >= 6 else None


def normalize_supplier_name(value: object) -> Optional[str]:
    """Lowercase + no-accent + no legal suffix — for fuzzy matching ONLY. Never for storage."""
    raw = normalize_text(value)
    if not raw: return None
    if raw.upper().strip() in SUPPLIER_SKIP_TOKENS: return None
    nfd = unicodedata.normalize("NFD", raw)
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    s = re.sub(r"\b(ltda|s\.?a\.?|me|eireli|epp|ss|microempresa|limitada)\.?\b", "", s.lower()).strip()
    return re.sub(r"\s+", " ", s).strip() or None


def normalize_employee_name(raw: str) -> Tuple[str, Optional[str]]:
    """'JOAO SILVA 1234' -> ('JOAO SILVA', '1234'). Last all-digit token = code."""
    if not raw: return raw, None
    parts = raw.strip().split()
    if not parts: return raw, None
    if parts[-1].isdigit():
        return (" ".join(parts[:-1]).strip() or raw), parts[-1]
    return raw.strip(), None


# ─── FILTER HELPERS ──────────────────────────────────────────────────────────

def is_skip_supplier(value: object) -> bool:
    raw = normalize_text(value)
    if not raw: return True
    u = raw.upper().strip()
    if u in SUPPLIER_SKIP_TOKENS: return True
    if re.match(r"^\d[\d\s]*$", u): return True  # purely numeric = totals
    return False


def is_header_row(raw_name: str) -> bool:
    if not raw_name: return True
    u = raw_name.upper()
    return any(tok in u for tok in HEADER_TOKENS)


# ─── COLUMN DETECTION ────────────────────────────────────────────────────────

def build_col_map(df: pd.DataFrame) -> Dict[str, Any]:
    """Return {NORMALISED_HEADER_STR: original_column_name}."""
    def _n(s):
        nfd = unicodedata.normalize("NFD", str(s))
        return re.sub(r"\s+", " ",
               "".join(c for c in nfd if unicodedata.category(c) != "Mn").upper().strip())
    return {_n(c): c for c in df.columns}


def find_column(col_map: Dict[str, Any], *patterns: str) -> Optional[Any]:
    """Substring search; returns first match or None."""
    for p in patterns:
        p_norm = re.sub(r"\s+", " ", p.upper().strip())
        for key, val in col_map.items():
            if p_norm in key:
                return val
    return None


def detect_header_row(df: pd.DataFrame, keywords: Sequence[str],
                      max_scan: int = 12) -> int:
    """Scan first max_scan rows; return index of row containing a keyword."""
    for i in range(min(max_scan, len(df))):
        cells = [str(v).upper() for v in df.iloc[i].tolist() if pd.notna(v)]
        for kw in keywords:
            if any(kw.upper() in c for c in cells):
                return i
    return 0


# ─── HASHING ─────────────────────────────────────────────────────────────────

def content_hash(row: Dict[str, Any], fields: Optional[List[str]] = None) -> str:
    """32-char SHA-256 of row values, stable regardless of insertion order."""
    subset = {k: v for k, v in row.items() if fields is None or k in fields}
    def _s(v):
        if v is None: return "null"
        if isinstance(v, date): return v.isoformat()
        if isinstance(v, float): return repr(v)
        return str(v)
    payload = json.dumps({k: _s(v) for k, v in sorted(subset.items())},
                         ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def file_hash(path: str) -> str:
    """SHA-256 of file bytes — duplicate-upload guard."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── VALIDATION HELPERS ───────────────────────────────────────────────────────

def validate_required(logs, row_num, row: dict, fields: List[str]) -> bool:
    ok = True
    for f in fields:
        if row.get(f) is None:
            log_err(logs, row_num, f, f"Required field '{f}' is missing or null.")
            ok = False
    return ok


def validate_non_negative(logs, row_num, row: dict, fields: List[str]) -> None:
    for f in fields:
        v = row.get(f)
        if v is not None and isinstance(v, (int, float)) and v < 0:
            log_warn(logs, row_num, f, f"'{f}' is negative ({v}); expected ≥ 0.")


def validate_formula(logs, row_num, computed, stored, field_name, tolerance=0.001) -> bool:
    if computed is None or stored is None: return True
    diff = abs(computed - stored)
    if diff > tolerance:
        log_warn(logs, row_num, field_name,
                 f"Formula mismatch: recomputed={computed:.4f}, source={stored:.4f}, diff={diff:.4f}",
                 str(stored))
        return False
    return True


def base_row(source_reference: str, data_source: str = "spreadsheet") -> Dict[str, Any]:
    """Pre-populated mandatory source-tracking fields for every staging row."""
    return {"data_source": data_source, "source_reference": source_reference, "content_hash": None}


def make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"sourceiq.etl.{name}")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s  %(message)s",
                                         datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger
