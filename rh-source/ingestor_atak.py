"""
ingestor_atak.py — Classic RH & SST
Ingestão automática de arquivos Atak ERP -> Supabase

Monitora C:\dados_atak\entrada\ e processa arquivos .xlsx/.xls/.csv
Após processamento: sucesso -> /processados, erro -> /erro

Uso:
  python ingestor_atak.py              # processar arquivos pendentes
  python ingestor_atak.py --watch      # monitorar continuamente (a cada 5min)
  python ingestor_atak.py --dry-run    # simular sem inserir

Estratégia de duplicidade: hash SHA-256 do arquivo inteiro.
Se o hash já existe em file_ingest_log -> arquivo ignorado.

Inserção via requests (API REST Supabase) + RPC SECURITY DEFINER.
Justificativa: não requer instalação de psycopg2, funciona com anon key,
RPCs bypassam RLS e são mais seguras que conexão direta.
"""

import os
import sys
import json
import hashlib
import shutil
import logging
import datetime
import time
import re
import requests
import zipfile
import xml.etree.ElementTree as ET

# ═══════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════

CONFIG = {
    "supabase_url": "https://muiqmtnfvyffborgiwdw.supabase.co",
    "supabase_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im11aXFtdG5mdnlmZmJvcmdpd2R3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM4NzQ2MTAsImV4cCI6MjA4OTQ1MDYxMH0.ReerGoDWcLrxJ1WIIKflN4Sc-st4zR1b6yWagCaPq_A",
    "pasta_entrada": r"C:\dados_atak\entrada",
    "pasta_processados": r"C:\dados_atak\processados",
    "pasta_erro": r"C:\dados_atak\erro",
    "extensoes_validas": {".xlsx", ".xls", ".csv"},
    "watch_intervalo_seg": 300,  # 5 minutos
}

DRY_RUN = "--dry-run" in sys.argv
WATCH = "--watch" in sys.argv

# ═══════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(r"C:\dados_atak\ingestor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ingestor_atak")


# ═══════════════════════════════════════════
# SUPABASE CLIENT
# ═══════════════════════════════════════════

class SupabaseClient:
    def __init__(self, url, key):
        self.url = url
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    def rpc(self, name, params):
        r = requests.post(
            f"{self.url}/rest/v1/rpc/{name}",
            headers=self.headers,
            json=params,
            timeout=30,
        )
        if not r.ok:
            raise Exception(f"RPC {name} falhou: {r.status_code} {r.text[:200]}")
        return r.json()

    def rpc_safe(self, name, params):
        try:
            return self.rpc(name, params)
        except Exception as e:
            log.error(f"RPC {name}: {e}")
            return None


sb = SupabaseClient(CONFIG["supabase_url"], CONFIG["supabase_key"])


# ═══════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════

def file_hash(path):
    """SHA-256 do arquivo inteiro para dedup"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def mover_arquivo(path, destino_dir):
    """Move arquivo para pasta de destino, renomeando se já existir"""
    nome = os.path.basename(path)
    dest = os.path.join(destino_dir, nome)
    if os.path.exists(dest):
        base, ext = os.path.splitext(nome)
        dest = os.path.join(destino_dir, f"{base}_{datetime.datetime.now().strftime('%H%M%S')}{ext}")
    if not DRY_RUN:
        shutil.move(path, dest)
    log.info(f"  Movido para: {dest}")


def parse_data(val):
    """Converte string de data para YYYY-MM-DD"""
    if not val:
        return None
    s = str(val).strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]:
        try:
            return datetime.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def registrar_log(nome, status, registros=0, erro=None, hash_arq=None):
    """Registra no file_ingest_log via RPC"""
    if DRY_RUN:
        log.info(f"  [DRY] Log: {nome} -> {status} ({registros} registros)")
        return
    sb.rpc_safe("file_check_and_log", {
        "p_hash": hash_arq or hashlib.sha256(nome.encode()).hexdigest(),
        "p_nome": nome,
        "p_status": status,
        "p_registros": registros,
        "p_erro": erro,
    })


def hash_ja_processado(hash_arq):
    """Verifica se arquivo com esse hash já foi processado (via RPC)"""
    try:
        result = sb.rpc("file_check_and_log", {"p_hash": hash_arq, "p_nome": "", "p_status": "verificando", "p_registros": 0})
        return result is True or result == True
    except:
        return False


# ═══════════════════════════════════════════
# PARSERS DE ARQUIVO
# ═══════════════════════════════════════════

def ler_xlsx_atak(path):
    """Lê arquivo Atak .xlsx via XML (robusto contra formato BR)"""
    registros = []
    with zipfile.ZipFile(path) as z:
        if "xl/sharedStrings.xml" not in z.namelist():
            raise ValueError("Arquivo não contém sharedStrings (não é Excel válido)")

        with z.open("xl/sharedStrings.xml") as f:
            tree = ET.parse(f)
            ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            strings = []
            for si in tree.findall(".//s:si", ns):
                text = "".join(t.text or "" for t in si.findall(".//s:t", ns))
                strings.append(text)

        sheet_files = [n for n in z.namelist() if "worksheets/Sheet" in n or "worksheets/sheet" in n]
        if not sheet_files:
            raise ValueError("Nenhuma sheet encontrada")

        with z.open(sheet_files[0]) as f:
            tree = ET.parse(f)
            rows = tree.findall(".//s:row", ns)

    data_atual = None
    for row in rows:
        cells = []
        for cell in row.findall("s:c", ns):
            t = cell.get("t")
            val_elem = cell.find("s:v", ns)
            if val_elem is not None:
                if t == "s":
                    idx = int(val_elem.text)
                    cells.append(strings[idx] if idx < len(strings) else "")
                else:
                    cells.append(val_elem.text)
            else:
                cells.append("")

        if not cells:
            continue

        # Detectar data
        if len(cells) >= 1 and re.match(r"^\d{2}/\d{2}/\d{4}$", cells[0]):
            data_atual = parse_data(cells[0])
            continue

        if not data_atual:
            continue
        if cells[0].startswith("Total") or cells[0] == "Produto" or not cells[0]:
            continue

        # Parsear linha
        produto = cells[0]
        try:
            qtd_kg = float(str(cells[4]).replace(",", ".")) if len(cells) > 4 and cells[4] else 0
            prod_hora = float(str(cells[9]).replace(",", ".")) if len(cells) > 9 and cells[9] else 0
            parts = produto.split("-", 1)
            codigo = parts[0].strip()
            nome = parts[1].strip() if len(parts) > 1 else produto

            registros.append({
                "data": data_atual,
                "produto_codigo": codigo,
                "produto_nome": nome,
                "quantidade_kg": round(qtd_kg, 2),
                "producao_hora": round(prod_hora, 2),
            })
        except (ValueError, IndexError):
            continue

    return registros


def ler_xls_atak(path):
    """Lê arquivo .xls via xlrd"""
    import xlrd
    wb = xlrd.open_workbook(path, ignore_workbook_corruption=True)
    ws = wb.sheet_by_index(0)
    registros = []
    data_atual = None

    for r in range(ws.nrows):
        cells = [str(ws.cell_value(r, c)) for c in range(min(15, ws.ncols))]
        if not cells:
            continue

        if re.match(r"^\d{2}/\d{2}/\d{4}$", cells[0]):
            data_atual = parse_data(cells[0])
            continue

        if not data_atual or cells[0].startswith("Total") or not cells[0]:
            continue

        try:
            produto = cells[0]
            qtd_kg = float(cells[4].replace(",", ".")) if len(cells) > 4 and cells[4] else 0
            parts = produto.split("-", 1)
            registros.append({
                "data": data_atual,
                "produto_codigo": parts[0].strip(),
                "produto_nome": parts[1].strip() if len(parts) > 1 else produto,
                "quantidade_kg": round(qtd_kg, 2),
                "producao_hora": 0,
            })
        except:
            continue

    return registros


def ler_csv_atak(path):
    """Lê CSV com colunas DATA, PRODUTO, QUANTIDADE"""
    import csv
    registros = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            data = parse_data(row.get("DATA") or row.get("data"))
            produto = row.get("PRODUTO") or row.get("produto") or ""
            qtd = row.get("QUANTIDADE") or row.get("quantidade_kg") or row.get("QTD") or "0"
            try:
                qtd_num = float(str(qtd).replace(",", "."))
            except:
                qtd_num = 0
            if data and produto:
                registros.append({
                    "data": data,
                    "produto_codigo": "",
                    "produto_nome": produto.strip(),
                    "quantidade_kg": round(qtd_num, 2),
                    "producao_hora": 0,
                })
    return registros


# ═══════════════════════════════════════════
# VALIDAÇÃO
# ═══════════════════════════════════════════

def validar_registros(registros):
    """Valida registros antes de inserir"""
    validos = []
    erros = 0
    for r in registros:
        if not r.get("data"):
            erros += 1
            continue
        if not r.get("produto_nome") and not r.get("produto_codigo"):
            erros += 1
            continue
        if r.get("quantidade_kg", 0) <= 0:
            erros += 1
            continue
        validos.append(r)
    return validos, erros


# ═══════════════════════════════════════════
# PROCESSAMENTO PRINCIPAL
# ═══════════════════════════════════════════

def processar_arquivo(path):
    """Processa um arquivo: ler -> validar -> inserir -> mover"""
    nome = os.path.basename(path)
    ext = os.path.splitext(nome)[1].lower()
    log.info(f"Processando: {nome}")

    # 1. Hash para dedup
    h = file_hash(path)
    if hash_ja_processado(h):
        log.info(f"  SKIP: arquivo já processado (hash {h[:12]})")
        mover_arquivo(path, CONFIG["pasta_processados"])
        return True

    try:
        # 2. Ler arquivo
        if ext == ".xlsx":
            registros = ler_xlsx_atak(path)
        elif ext == ".xls":
            registros = ler_xls_atak(path)
        elif ext == ".csv":
            registros = ler_csv_atak(path)
        else:
            raise ValueError(f"Extensão não suportada: {ext}")

        log.info(f"  Registros lidos: {len(registros)}")

        if not registros:
            raise ValueError("Nenhum registro extraído do arquivo")

        # 3. Validar
        validos, erros_val = validar_registros(registros)
        log.info(f"  Válidos: {len(validos)}, Inválidos: {erros_val}")

        if not validos:
            raise ValueError(f"Todos os {len(registros)} registros falharam na validação")

        # 4. Inserir no Supabase
        total_inseridos = 0
        if not DRY_RUN:
            for i in range(0, len(validos), 100):
                lote = validos[i : i + 100]
                result = sb.rpc("importar_producao_atak", {"p_registros": lote})
                if isinstance(result, list) and result:
                    total_inseridos += result[0].get("inseridos", 0)
                time.sleep(0.3)
        else:
            total_inseridos = len(validos)

        log.info(f"  Inseridos: {total_inseridos}")

        # 5. Registrar log + mover
        registrar_log(nome, "processado", total_inseridos, hash_arq=h)
        mover_arquivo(path, CONFIG["pasta_processados"])
        log.info(f"  OK: {nome} -> processados/")
        return True

    except Exception as e:
        log.error(f"  ERRO: {nome} -> {e}")
        registrar_log(nome, "erro", 0, str(e), hash_arq=h)
        mover_arquivo(path, CONFIG["pasta_erro"])
        return False


def listar_arquivos_pendentes():
    """Lista arquivos válidos na pasta de entrada"""
    pasta = CONFIG["pasta_entrada"]
    if not os.path.exists(pasta):
        log.error(f"Pasta não existe: {pasta}")
        return []

    arquivos = []
    for nome in sorted(os.listdir(pasta)):
        ext = os.path.splitext(nome)[1].lower()
        if ext in CONFIG["extensoes_validas"]:
            arquivos.append(os.path.join(pasta, nome))

    return arquivos


def executar_ciclo():
    """Executa um ciclo de processamento"""
    arquivos = listar_arquivos_pendentes()
    if not arquivos:
        log.info("Nenhum arquivo pendente.")
        return

    log.info(f"Encontrados {len(arquivos)} arquivo(s) para processar")
    sucesso = 0
    erros = 0

    for path in arquivos:
        if processar_arquivo(path):
            sucesso += 1
        else:
            erros += 1

    log.info(f"Ciclo concluído: {sucesso} sucesso, {erros} erros")


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("Classic RH & SST — Ingestor Atak")
    log.info(f"Pasta: {CONFIG['pasta_entrada']}")
    log.info(f"Dry run: {DRY_RUN}")
    log.info(f"Watch: {WATCH}")
    log.info("=" * 60)

    if WATCH:
        log.info(f"Modo watch: verificando a cada {CONFIG['watch_intervalo_seg']}s")
        while True:
            try:
                executar_ciclo()
            except Exception as e:
                log.error(f"Erro no ciclo: {e}")
            time.sleep(CONFIG["watch_intervalo_seg"])
    else:
        executar_ciclo()
