"""
watch_folders.py — Varredura automatica de pastas do OneDrive
=============================================================
Busca arquivos novos nas pastas configuradas em etl_folders.json,
detecta o tipo, roteia para o parser correto via run_etl.py e move
os processados para _processados/ (ou _erros/ se falhar).

Uso:
  # Execucao unica (varrer e sair):
  python watch_folders.py

  # Loop continuo (a cada N segundos):
  python watch_folders.py --watch

  # Dry-run (nao grava no Supabase, nao move arquivos):
  python watch_folders.py --dry-run

  # Sobrescrever caminho base:
  python watch_folders.py --base-path "D:\\Producao"

  # Auto-commit (promover direto sem revisao):
  python watch_folders.py --commit
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

# Adiciona o diretorio pai ao path para importar run_etl e parsers
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common import make_logger, file_hash

logger = make_logger("watch_folders")

# ─── CONFIG ──────────────────────────────────────────────────────────────────

CONFIG_FILE = SCRIPT_DIR / "etl_folders.json"


def load_config() -> dict:
    """Carrega etl_folders.json."""
    if not CONFIG_FILE.exists():
        logger.error("Config nao encontrado: %s", CONFIG_FILE)
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_base_path(config: dict, cli_override: str | None = None) -> Path:
    """Resolve o caminho base com prioridade: CLI > env > config."""
    base = cli_override or os.environ.get("ETL_BASE_PATH") or config.get("base_path", "")
    if not base:
        logger.error("Caminho base nao definido. Use --base-path, ETL_BASE_PATH ou etl_folders.json")
        sys.exit(1)
    p = Path(base)
    if not p.exists():
        logger.error("Caminho base nao existe: %s", p)
        sys.exit(1)
    return p


# ─── TRACKING (evitar reprocessar) ──────────────────────────────────────────

TRACKING_FILE = SCRIPT_DIR / ".etl_processed_hashes.json"


def load_tracking() -> dict:
    """Carrega hashes de arquivos ja processados."""
    if TRACKING_FILE.exists():
        try:
            with open(TRACKING_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_tracking(data: dict) -> None:
    """Salva hashes processados."""
    with open(TRACKING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


# ─── SCANNER ─────────────────────────────────────────────────────────────────

def scan_folder(base: Path, folder_cfg: dict) -> list[Path]:
    """
    Varre uma subpasta e retorna arquivos novos com extensao valida.
    Ignora arquivos em _processados/ e _erros/.
    """
    subpath = folder_cfg["subpath"]
    extensions = set(e.lower() for e in folder_cfg.get("extensions", [".xlsx", ".xls", ".pdf"]))
    folder = base / subpath

    if not folder.exists():
        # Tenta buscar com variações de nome (acentos, case)
        candidates = [d for d in base.iterdir() if d.is_dir()
                      and _normalize_name(d.name) == _normalize_name(subpath)]
        if candidates:
            folder = candidates[0]
        else:
            logger.debug("Pasta nao encontrada: %s — criando...", folder)
            folder.mkdir(parents=True, exist_ok=True)
            return []

    files = []
    for f in folder.iterdir():
        if f.is_file() and f.suffix.lower() in extensions:
            # Ignorar arquivos temporarios do Excel (~$)
            if f.name.startswith("~$"):
                continue
            files.append(f)

    return sorted(files, key=lambda f: f.stat().st_mtime)


def _normalize_name(name: str) -> str:
    """Normaliza nome de pasta para comparação fuzzy."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_name.lower().strip().replace(" ", "").replace("-", "").replace("_", "")


# ─── PROCESSAMENTO ───────────────────────────────────────────────────────────

def process_file(
    file_path: Path,
    entity_type: str,
    processed_dir: Path,
    error_dir: Path,
    tracking: dict,
    dry_run: bool = False,
    auto_commit: bool = False,
) -> bool:
    """
    Processa um arquivo via run_etl.py.
    Move para _processados/ se OK, ou _erros/ se falhar.
    Retorna True se processou com sucesso.
    """
    fhash = file_hash(str(file_path))

    # Ja processado?
    if fhash in tracking:
        logger.debug("Ja processado (hash): %s", file_path.name)
        return False

    logger.info("=" * 60)
    logger.info("Processando: %s", file_path.name)
    logger.info("  Tipo: %s | Tamanho: %s KB", entity_type, file_path.stat().st_size // 1024)

    if dry_run:
        logger.info("  [DRY RUN] Simulando parse...")
        # Importar e rodar parse apenas para validar
        try:
            from run_etl import run_parse, validate_rows
            result = run_parse(str(file_path), entity_type)
            validate_rows(result)
            logger.info("  Parse OK: %d linhas, %d aceitas, %d erros",
                        result.total_input_rows, result.accepted_rows, result.error_count)

            # Registrar no tracking mesmo em dry-run
            tracking[fhash] = {
                "file": file_path.name,
                "entity_type": entity_type,
                "processed_at": datetime.now().isoformat(),
                "dry_run": True,
                "rows": result.total_input_rows,
            }
            return True
        except Exception as e:
            logger.error("  Parse FALHOU: %s", e)
            return False

    # Processamento real via run_etl
    try:
        from run_etl import (
            SupabaseWriter, register_file, run_parse,
            validate_rows, stage_rows, promote_to_final,
        )

        writer = SupabaseWriter()

        # Phase 1: Register
        file_id = register_file(writer, str(file_path), entity_type, file_path.stem)
        if file_id is None:
            logger.warning("  Arquivo duplicado no Supabase. Movendo para processados.")
            _move_file(file_path, processed_dir, "dup")
            tracking[fhash] = {
                "file": file_path.name,
                "entity_type": entity_type,
                "processed_at": datetime.now().isoformat(),
                "status": "duplicate",
            }
            return False

        # Phase 2: Parse
        result = run_parse(str(file_path), entity_type)
        logger.info("  Parse: %d linhas, %d aceitas", result.total_input_rows, result.accepted_rows)

        # Phase 3 & 4: Validate + Stage
        validation_issues = validate_rows(result)
        staged_ok, staged_fail = stage_rows(writer, result, file_id)
        writer.insert_logs(file_id, result.logs)

        writer.update_one("import_files", file_id, {
            "status": "validated",
            "total_rows": result.total_input_rows,
            "validated_rows": sum(1 for r in result.rows if r.get("_validation_status") == "valid"),
            "warning_rows": sum(1 for r in result.rows if r.get("_validation_status") == "warning"),
            "rejected_rows": sum(1 for r in result.rows if r.get("_validation_status") == "invalid"),
            "validated_at": datetime.now().isoformat(),
        })

        # Phase 5: Promote (se auto_commit)
        if auto_commit and not result.has_critical_errors:
            promoted, failed = promote_to_final(writer, file_id, result)
            logger.info("  Promovido: %d linhas (%d falhas)", promoted, failed)

        # Mover para processados
        _move_file(file_path, processed_dir)

        tracking[fhash] = {
            "file": file_path.name,
            "entity_type": entity_type,
            "processed_at": datetime.now().isoformat(),
            "status": "ok",
            "file_id": file_id,
            "rows": result.total_input_rows,
            "errors": result.error_count,
        }
        return True

    except Exception as e:
        logger.exception("  ERRO ao processar %s: %s", file_path.name, e)
        _move_file(file_path, error_dir)
        tracking[fhash] = {
            "file": file_path.name,
            "entity_type": entity_type,
            "processed_at": datetime.now().isoformat(),
            "status": "error",
            "error": str(e),
        }
        return False


def _move_file(src: Path, dest_dir: Path, suffix: str = "") -> None:
    """Move arquivo para pasta destino, evitando sobrescrever."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_name = src.name
    if suffix:
        dest_name = f"{src.stem}_{suffix}{src.suffix}"
    dest = dest_dir / dest_name

    # Se ja existe, adiciona timestamp
    if dest.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{src.stem}_{ts}{src.suffix}"

    shutil.move(str(src), str(dest))
    logger.info("  Movido para: %s", dest)


# ─── MAIN LOOP ───────────────────────────────────────────────────────────────

def run_scan(config: dict, base: Path, dry_run: bool = False,
             auto_commit: bool = False) -> dict:
    """Executa uma varredura completa. Retorna resumo."""
    tracking = load_tracking()
    folders = config.get("folders", [])
    processed_name = config.get("processed_folder", "_processados")
    error_name = config.get("error_folder", "_erros")

    summary = {"scanned": 0, "processed": 0, "skipped": 0, "errors": 0}

    for fcfg in folders:
        files = scan_folder(base, fcfg)
        entity_type = fcfg["entity_type"]
        folder_path = base / fcfg["subpath"]

        # Pastas de destino dentro de cada subpasta
        processed_dir = folder_path / processed_name
        error_dir = folder_path / error_name

        for f in files:
            summary["scanned"] += 1
            fhash = file_hash(str(f))

            if fhash in tracking:
                summary["skipped"] += 1
                continue

            ok = process_file(
                f, entity_type, processed_dir, error_dir,
                tracking, dry_run, auto_commit
            )
            if ok:
                summary["processed"] += 1
            else:
                summary["errors"] += 1

    save_tracking(tracking)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Varredura automatica de pastas do OneDrive para ETL"
    )
    ap.add_argument("--base-path", default=None,
                    help="Caminho base (sobrescreve config e env)")
    ap.add_argument("--watch", action="store_true",
                    help="Modo continuo: varre a cada N segundos")
    ap.add_argument("--dry-run", action="store_true",
                    help="Apenas parseia e valida, nao grava no DB")
    ap.add_argument("--commit", action="store_true",
                    help="Auto-promover linhas validas (sem revisao manual)")
    ap.add_argument("--interval", type=int, default=None,
                    help="Intervalo em segundos (padrao: config ou 300)")
    args = ap.parse_args()

    config = load_config()
    base = resolve_base_path(config, args.base_path)

    logger.info("=" * 60)
    logger.info("Classic CQ — ETL Folder Watcher")
    logger.info("Base: %s", base)
    logger.info("Dry-run: %s | Auto-commit: %s | Watch: %s",
                args.dry_run, args.commit, args.watch)
    logger.info("=" * 60)

    # Listar pastas configuradas
    for fcfg in config.get("folders", []):
        folder = base / fcfg["subpath"]
        exists = "OK" if folder.exists() else "NAO EXISTE"
        logger.info("  [%s] %s -> %s", exists, fcfg["subpath"], fcfg["entity_type"])

    if args.watch:
        interval = args.interval or config.get("poll_interval_seconds", 300)
        logger.info("Modo watch: intervalo = %ds", interval)
        while True:
            try:
                summary = run_scan(config, base, args.dry_run, args.commit)
                logger.info(
                    "Ciclo concluido: %d varridos, %d processados, %d ignorados, %d erros",
                    summary["scanned"], summary["processed"],
                    summary["skipped"], summary["errors"]
                )
            except KeyboardInterrupt:
                logger.info("Interrompido pelo usuario.")
                break
            except Exception as e:
                logger.exception("Erro no ciclo: %s", e)
            time.sleep(interval)
    else:
        summary = run_scan(config, base, args.dry_run, args.commit)
        logger.info("Concluido: %d varridos, %d processados, %d ignorados, %d erros",
                     summary["scanned"], summary["processed"],
                     summary["skipped"], summary["errors"])


if __name__ == "__main__":
    main()
