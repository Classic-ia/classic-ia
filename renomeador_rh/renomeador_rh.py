# -*- coding: utf-8 -*-
"""
Renomeador RH PRO - Classic Couros - v5.0 (FASE 5)
==================================================
Sistema desktop para renomeacao, classificacao e arquivamento automatico de
documentos digitalizados do RH, com IA (Gemini -> Claude), integracao Convenia,
dossie por colaborador, base de dados e painel gerencial.

Apenas bibliotecas padrao do Python 3.8+ (Tkinter).
Autor: Classic Importacao e Exportacao de Couros EIRELI
"""

import base64
import csv
import ctypes
import difflib
import getpass
import hashlib
import html
import json
import os
import queue
import re
import shutil
import threading
import time
import unicodedata
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# ---------------------------------------------------------------------------
# Caminhos e constantes
# ---------------------------------------------------------------------------

APP_VERSAO = "v5.0 - FASE 5"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(APP_DIR, ".env")


def _config_dir():
    """Config por maquina, cifrada com a conta do Windows (DPAPI)."""
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
        d = os.path.join(base, "RenomeadorRH")
    else:
        d = os.path.join(os.path.expanduser("~"), ".renomeador_rh")
    os.makedirs(d, exist_ok=True)
    return d


CONFIG_PATH = os.path.join(_config_dir(), "config.json")

PROGRESSO_NOME = "_renomeador_progresso.json"
LOCK_NOME = "_renomeador_EM_USO.lock"
PASTA_RELATORIOS = "_RELATORIOS RH"
HISTORICO_NOME = "historico_rh.csv"

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)
CLAUDE_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_VERSION = "2023-06-01"

CONVENIA_BASE = "https://public-api.convenia.com.br/api/v3"
CONVENIA_HEADER = "api-token"
CONVENIA_DOC_ENDPOINT = "/employees/{id}/documents"

EXTENSOES_VALIDAS = (".pdf", ".jpg", ".jpeg", ".png")

MESES = [
    "", "01-Janeiro", "02-Fevereiro", "03-Marco", "04-Abril", "05-Maio",
    "06-Junho", "07-Julho", "08-Agosto", "09-Setembro", "10-Outubro",
    "11-Novembro", "12-Dezembro",
]

# Tipos reconhecidos (no nome do arquivo - sem acentos)
TIPOS_VALIDOS = [
    "HOLERITE", "RECIBO", "ADIANTAMENTO", "CARTAO PONTO", "13 SALARIO",
    "ATESTADO", "ACORDO DE COMPENSACAO", "PLANO DE SAUDE", "FERIAS",
    "RESCISAO", "AVISO PREVIO", "DDS", "PORTABILIDADE", "CONTRATO",
    "ADMISSAO", "DOCUMENTO",
]

# Tipo -> pasta base (nomes com acento, batem com as pastas atuais do RH)
PASTA_BASE = {
    "HOLERITE": "HOLERITES",
    "RECIBO": "RECIBOS",
    "ADIANTAMENTO": "ADIANTAMENTOS",
    "CARTAO PONTO": "CARTÃO PONTO",
    "13 SALARIO": "13 SALARIO",
    "ATESTADO": "ATESTADO",
    "ACORDO DE COMPENSACAO": "ACORDO DE COMPENSAÇÃO",
    "PLANO DE SAUDE": "PLANO ALIANÇA",
    "FERIAS": "FERIAS",
    "RESCISAO": "RESCISAO",
    "AVISO PREVIO": "RESCISAO",
    "DDS": "DDS",
    "PORTABILIDADE": "PORTABILIDADES",
    "CONTRATO": "DOCUMENTOS 2025",
    "ADMISSAO": "DOCUMENTOS 2025",
    "DOCUMENTO": "OUTROS",
}
PASTA_PADRAO = "OUTROS"
PASTA_REVISAR = "_REVISAR"
PASTA_COLABORADORES = "COLABORADORES"
PASTA_DESLIGADOS = "DESLIGADOS"

# Tipos organizados por ano/mes e por ano
COM_ANO_MES = {"HOLERITE", "RECIBO", "ADIANTAMENTO", "CARTAO PONTO"}
COM_ANO = {"13 SALARIO"}

GEMINI_INTERVALO_MIN = 10
JANELA_RETORNO_GEMINI = 300
ESPERA_429 = 10
MAX_FALHAS_GEMINI = 3
CUTOFF_NOME = 0.82

# Paleta (tema escuro)
COR_BG = "#1e1e2e"
COR_SURFACE = "#27293d"
COR_SURFACE2 = "#2f3147"
COR_FG = "#e4e6f0"
COR_FG_DIM = "#9aa0b5"
COR_BORDA = "#3a3d57"
COR_VERDE = "#22c55e"
COR_LARANJA = "#f59e0b"
COR_VERMELHO = "#ef4444"
COR_AZUL = "#3b82f6"

LOG_CORES = {
    "sucesso": "#4ade80",
    "erro": "#f87171",
    "aviso": "#facc15",
    "info": "#60a5fa",
    "claude": "#c084fc",
    "convenia": "#2dd4bf",
    "revisar": "#fb923c",
}

# ---------------------------------------------------------------------------
# Utilitarios de texto / arquivo
# ---------------------------------------------------------------------------


def interruptible_sleep(seconds, stop_event):
    fim = time.time() + seconds
    while time.time() < fim:
        if stop_event.is_set():
            return False
        time.sleep(0.1)
    return True


def ler_env(caminho):
    valores = {}
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, _, valor = linha.partition("=")
                valores[chave.strip().upper()] = valor.strip().strip('"').strip("'")
    except Exception:
        pass
    return valores


def remover_acentos(texto):
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalizar_nome(texto):
    texto = remover_acentos(texto or "").upper()
    texto = re.sub(r"[^A-Z0-9 ]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def so_digitos(texto):
    return re.sub(r"\D", "", str(texto or ""))


def media_type_de(ext):
    ext = ext.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext == ".png":
        return "image/png"
    return "image/jpeg"


def hash_arquivo(caminho):
    h = hashlib.sha1()
    try:
        with open(caminho, "rb") as f:
            for bloco in iter(lambda: f.read(65536), b""):
                h.update(bloco)
        return h.hexdigest()[:16]
    except Exception:
        return ""


def extrair_json(texto):
    if not texto:
        return None
    t = texto.strip()
    t = re.sub(r"^```(?:json)?", "", t).strip()
    t = re.sub(r"```$", "", t).strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    inicio = t.find("{")
    if inicio == -1:
        return None
    prof = 0
    for i in range(inicio, len(t)):
        if t[i] == "{":
            prof += 1
        elif t[i] == "}":
            prof -= 1
            if prof == 0:
                try:
                    return json.loads(t[inicio : i + 1])
                except Exception:
                    return None
    return None


def canonizar_tipo(tipo):
    t = normalizar_nome(tipo)
    if not t:
        return "DOCUMENTO"
    if t in TIPOS_VALIDOS:
        return t
    if "HOLERITE" in t or "CONTRACHEQUE" in t or ("RECIBO" in t and "PAGAMENTO" in t):
        return "HOLERITE"
    if "AVISO" in t and "PREVIO" in t:
        return "AVISO PREVIO"
    if "RESCISAO" in t or "TRCT" in t or "HOMOLOG" in t:
        return "RESCISAO"
    if "ADIANTAMENTO" in t or "VALE" in t:
        return "ADIANTAMENTO"
    if ("13" in t and "SALARIO" in t) or "DECIMO TERCEIRO" in t:
        return "13 SALARIO"
    if "PONTO" in t or "CARTAO" in t or "ESPELHO" in t:
        return "CARTAO PONTO"
    if "ATESTADO" in t or "AFASTAMENTO" in t:
        return "ATESTADO"
    if "ACORDO" in t or "COMPENSACAO" in t:
        return "ACORDO DE COMPENSACAO"
    if ("PLANO" in t and "SAUDE" in t) or "ALIANCA" in t:
        return "PLANO DE SAUDE"
    if "FERIAS" in t:
        return "FERIAS"
    if "PORTABILIDADE" in t:
        return "PORTABILIDADE"
    if t == "DDS" or "DIALOGO" in t:
        return "DDS"
    if "RECIBO" in t:
        return "RECIBO"
    if "CONTRATO" in t:
        return "CONTRATO"
    if "ADMISSAO" in t:
        return "ADMISSAO"
    return "DOCUMENTO"


def parse_ano_mes(data_ref):
    """Devolve (ano:str|None, mes:int|None) a partir de 'YYYY-MM' ou 'YYYY'."""
    if not data_ref:
        return None, None
    m = re.match(r"(\d{4})-(\d{2})$", str(data_ref))
    if m:
        return m.group(1), int(m.group(2))
    m = re.match(r"(\d{4})$", str(data_ref))
    if m:
        return m.group(1), None
    return None, None


def validar_data(data):
    if not data:
        return "SEM-DATA"
    s = str(data)
    m = re.search(r"(\d{4})[-/ .](\d{1,2})\b", s)
    if m:
        ano, mes = m.group(1), int(m.group(2))
        if 1 <= mes <= 12:
            return f"{ano}-{mes:02d}"
    m = re.search(r"\b(\d{1,2})[-/ .](\d{4})\b", s)
    if m:
        mes, ano = int(m.group(1)), m.group(2)
        if 1 <= mes <= 12:
            return f"{ano}-{mes:02d}"
    m = re.search(r"(\d{4})", s)
    if m:
        return m.group(1)
    return "SEM-DATA"


def parse_data_iso(valor):
    """Tenta interpretar uma data (varios formatos) em datetime, ou None."""
    if not valor:
        return None
    s = str(valor).strip()[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def classificar_atestado(dias, meio_periodo):
    if meio_periodo:
        return "Ate 3 dias"
    if dias is None:
        return ""  # vai para a raiz de ATESTADO
    if dias <= 3:
        return "Ate 3 dias"
    if dias <= 15:
        return "4 a 15 dias"
    return "16+ dias (INSS)"


def pasta_relativa(tipo, data_ref, dias, meio_periodo):
    """Caminho relativo (dentro do destino) para arquivar por tipo/competencia."""
    base = PASTA_BASE.get(tipo, PASTA_PADRAO)
    if tipo == "ATESTADO":
        sub = classificar_atestado(dias, meio_periodo)
        return os.path.join(base, sub) if sub else base
    if tipo in COM_ANO_MES:
        ano, mes = parse_ano_mes(data_ref)
        if ano and mes:
            return os.path.join(base, ano, MESES[mes])
        if ano:
            return os.path.join(base, ano)
        return base
    if tipo in COM_ANO:
        ano, _ = parse_ano_mes(data_ref)
        return os.path.join(base, ano) if ano else base
    return base


def nome_pasta_seguro(nome):
    n = normalizar_nome(nome) or "SEM NOME"
    return n[:80]


def identificador(matricula, cpf):
    """Identificador unico para o nome do arquivo / dossie."""
    mat = re.sub(r"[^A-Z0-9]", "", normalizar_nome(matricula))
    if mat:
        return f"MAT{mat}"
    d = so_digitos(cpf)
    if len(d) >= 3:
        return f"CPF{d[-3:]}"
    return "SEMID"


# Padroes de nome de arquivo de scanner (vao direto para a IA)
_RE_SCANNER = re.compile(r"^(BRW|SKM|SCAN|IMG|DOC|FILE|2\d{6,}|\d{8,}|[0-9A-F]{10,})")


def extrair_do_nome_arquivo(nome_arquivo, cadastro):
    """Tenta extrair nome+tipo do PROPRIO nome do arquivo (gratis, sem IA).
    Retorna dict {nome,tipo,data,confianca,fonte} ou None se nao for confiavel."""
    base = os.path.splitext(os.path.basename(nome_arquivo))[0]
    norm = normalizar_nome(base)
    if not norm:
        return None
    if _RE_SCANNER.match(norm.replace(" ", "")):
        return None
    # precisa de um tipo reconhecivel no nome
    tipo = None
    for t in TIPOS_VALIDOS:
        if t != "DOCUMENTO" and t in norm:
            tipo = t
            break
    if not tipo:
        # palavras-chave soltas
        tipo_cand = canonizar_tipo(norm)
        if tipo_cand != "DOCUMENTO":
            tipo = tipo_cand
    if not tipo:
        return None
    data = validar_data(base)
    # remove tokens de tipo e numeros para sobrar o nome
    sobra = norm
    for palavra in tipo.split():
        sobra = sobra.replace(palavra, " ")
    sobra = re.sub(r"\b\d+\b", " ", sobra)
    sobra = re.sub(r"\s+", " ", sobra).strip()
    # se houver cadastro, exige casar o nome
    if cadastro and len(cadastro):
        match = cadastro.encontrar(sobra)
        if match:
            return {
                "nome": match["nome"], "cpf": match.get("cpf", ""),
                "tipo": tipo, "data": data, "dias": None, "meio_periodo": False,
                "periodo": "", "confianca": "ALTA", "fonte": "Nome do arquivo",
            }
        return None
    # sem cadastro: aceita se sobrarem ao menos 2 palavras (provavel nome)
    if len(sobra.split()) >= 2:
        return {
            "nome": sobra, "cpf": "", "tipo": tipo, "data": data, "dias": None,
            "meio_periodo": False, "periodo": "", "confianca": "MEDIA",
            "fonte": "Nome do arquivo",
        }
    return None


# ---------------------------------------------------------------------------
# DPAPI (Windows) - cifra as chaves no config local
# ---------------------------------------------------------------------------


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.c_void_p)]


def dpapi_proteger(texto):
    if os.name != "nt" or not texto:
        return texto
    try:
        dados = texto.encode("utf-8")
        buf = ctypes.create_string_buffer(dados, len(dados))
        entrada = _DataBlob(len(dados), ctypes.cast(buf, ctypes.c_void_p))
        saida = _DataBlob()
        if ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(entrada), None, None, None, None, 0, ctypes.byref(saida)
        ):
            cifrado = ctypes.string_at(saida.pbData, saida.cbData)
            ctypes.windll.kernel32.LocalFree(saida.pbData)
            return "dpapi:" + base64.b64encode(cifrado).decode("ascii")
    except Exception:
        pass
    return texto


def dpapi_abrir(texto):
    if not texto or not str(texto).startswith("dpapi:"):
        return texto
    if os.name != "nt":
        return ""
    try:
        cifrado = base64.b64decode(texto[len("dpapi:"):])
        buf = ctypes.create_string_buffer(cifrado, len(cifrado))
        entrada = _DataBlob(len(cifrado), ctypes.cast(buf, ctypes.c_void_p))
        saida = _DataBlob()
        if ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(entrada), None, None, None, None, 0, ctypes.byref(saida)
        ):
            dados = ctypes.string_at(saida.pbData, saida.cbData)
            ctypes.windll.kernel32.LocalFree(saida.pbData)
            return dados.decode("utf-8", errors="replace")
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def _http_post_json(url, payload, headers, timeout=120):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def _http_get(url, headers, timeout=60):
    req = urllib.request.Request(url, method="GET")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def _http_post_multipart(url, campos, arquivos, headers, timeout=180):
    boundary = "----RenomeadorRH" + os.urandom(8).hex()
    corpo = b""
    for nome, valor in (campos or {}).items():
        corpo += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{nome}"\r\n\r\n{valor}\r\n'
        ).encode("utf-8")
    for campo, (fname, dados, ctype) in (arquivos or {}).items():
        corpo += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{campo}"; filename="{fname}"\r\n'
            f"Content-Type: {ctype}\r\n\r\n"
        ).encode("utf-8")
        corpo += dados + b"\r\n"
    corpo += f"--{boundary}--\r\n".encode("utf-8")
    req = urllib.request.Request(url, data=corpo, method="POST")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Leitor de XLSX (biblioteca padrao)
# ---------------------------------------------------------------------------

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _col_index(ref):
    """'B3' -> 1 (indice 0-based da coluna)."""
    letras = re.match(r"[A-Z]+", ref or "")
    if not letras:
        return None
    s = letras.group(0)
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1


def ler_xlsx(caminho):
    with zipfile.ZipFile(caminho) as z:
        nomes = z.namelist()
        compart = []
        if "xl/sharedStrings.xml" in nomes:
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall(f"{_NS}si"):
                compart.append("".join(t.text or "" for t in si.iter(f"{_NS}t")))
        planilhas = sorted(
            n for n in nomes
            if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
        )
        if not planilhas:
            return []
        root = ET.fromstring(z.read(planilhas[0]))
        sheet_data = root.find(f"{_NS}sheetData")
        linhas = []
        if sheet_data is None:
            return []
        for row in sheet_data.findall(f"{_NS}row"):
            celulas = []
            for c in row.findall(f"{_NS}c"):
                idx = _col_index(c.get("r", ""))
                if idx is not None:
                    while len(celulas) < idx:
                        celulas.append("")
                tipo = c.get("t")
                v = c.find(f"{_NS}v")
                if tipo == "s" and v is not None:
                    try:
                        celulas.append(compart[int(v.text)])
                    except (ValueError, IndexError):
                        celulas.append("")
                elif tipo == "inlineStr":
                    is_el = c.find(f"{_NS}is")
                    celulas.append(
                        "".join(x.text or "" for x in is_el.iter(f"{_NS}t"))
                        if is_el is not None else ""
                    )
                else:
                    celulas.append(v.text if v is not None else "")
            linhas.append(celulas)
        return linhas


# ---------------------------------------------------------------------------
# Cadastro de colaboradores
# ---------------------------------------------------------------------------


class Cadastro:
    def __init__(self):
        self.itens = []
        self.origem = ""

    def __len__(self):
        return len(self.itens)

    def _add(self, nome, ident="", cpf="", matricula="", admissao="", desligamento=""):
        nome = (nome or "").strip()
        if not nome:
            return
        self.itens.append(
            {
                "id": str(ident or ""),
                "nome": nome,
                "nome_norm": normalizar_nome(nome),
                "cpf": so_digitos(cpf),
                "matricula": str(matricula or "").strip(),
                "admissao": admissao or "",
                "desligamento": desligamento or "",
            }
        )

    def _achar_col(self, cabecalho, *chaves):
        for i, h in enumerate(cabecalho):
            for ch in chaves:
                if ch in h:
                    return i
        return None

    def carregar_xlsx(self, caminho):
        self.itens = []
        linhas = ler_xlsx(caminho)
        if not linhas:
            raise ValueError("Planilha vazia ou ilegivel.")
        cab = [normalizar_nome(c) for c in linhas[0]]
        i_nome = self._achar_col(cab, "NOME") or 0
        i_cpf = self._achar_col(cab, "CPF")
        i_mat = self._achar_col(cab, "MATRICULA", "MATRICULA", "CODIGO")
        i_adm = self._achar_col(cab, "ADMISSAO")
        i_des = self._achar_col(cab, "DESLIGAMENTO", "DEMISSAO", "RESCISAO")
        comeco = 1 if any("NOME" in h for h in cab) else 0

        def pega(linha, idx):
            return linha[idx] if idx is not None and idx < len(linha) else ""

        for linha in linhas[comeco:]:
            if i_nome >= len(linha):
                continue
            self._add(
                linha[i_nome], pega(linha, i_mat), pega(linha, i_cpf),
                pega(linha, i_mat), pega(linha, i_adm), pega(linha, i_des),
            )
        self.origem = "XLSX"
        return len(self.itens)

    def carregar_convenia(self, client):
        self.itens = []
        for f in client.listar_funcionarios():
            nome = f.get("name") or f.get("nome") or f.get("full_name") or ""
            self._add(
                nome, f.get("id"), f.get("cpf"),
                f.get("registration") or f.get("matricula") or "",
                f.get("admission_date") or f.get("data_admissao") or "",
                f.get("dismissal_date") or f.get("data_desligamento")
                or f.get("termination_date") or "",
            )
        self.origem = "CONVENIA"
        return len(self.itens)

    def encontrar(self, nome=None, cpf=None):
        """Casa por CPF (preferencial) ou por nome aproximado."""
        if not self.itens:
            return None
        d = so_digitos(cpf)
        if len(d) >= 11:
            for i in self.itens:
                if i["cpf"] and i["cpf"] == d:
                    return i
        alvo = normalizar_nome(nome)
        if not alvo or alvo == "DESCONHECIDO":
            return None
        nomes = [i["nome_norm"] for i in self.itens]
        match = difflib.get_close_matches(alvo, nomes, n=1, cutoff=CUTOFF_NOME)
        if not match:
            return None
        for i in self.itens:
            if i["nome_norm"] == match[0]:
                return i
        return None

    def esta_desligado(self, item):
        return bool(item and parse_data_iso(item.get("desligamento")))


# ---------------------------------------------------------------------------
# Cliente Convenia
# ---------------------------------------------------------------------------


class ConveniaClient:
    def __init__(self, token):
        self.token = (token or "").strip()

    def _headers(self):
        return {CONVENIA_HEADER: self.token, "Accept": "application/json"}

    def validar(self):
        if not self.token:
            return False, "Token vazio."
        try:
            status, _ = _http_get(f"{CONVENIA_BASE}/employees?per_page=1", self._headers())
            return (200 <= status < 300), f"HTTP {status}"
        except urllib.error.HTTPError as e:
            return False, ("Token invalido ou expirado (401)" if e.code == 401 else f"HTTP {e.code}")
        except Exception as e:  # noqa: BLE001
            return False, str(e)

    def listar_funcionarios(self, max_paginas=30):
        todos, pagina, total = [], 1, 1
        while pagina <= total and pagina <= max_paginas:
            status, corpo = _http_get(
                f"{CONVENIA_BASE}/employees?page={pagina}&per_page=100", self._headers()
            )
            if not (200 <= status < 300):
                raise ValueError(f"HTTP {status}")
            obj = json.loads(corpo)
            lista = obj.get("data") or obj.get("employees") or []
            if not isinstance(lista, list):
                break
            todos.extend(lista)
            meta = obj.get("meta") or {}
            total = meta.get("last_page") or meta.get("total_pages") or 1
            pagina += 1
        return todos

    def enviar_documento(self, employee_id, caminho, tipo):
        if not self.token:
            return False, "Sem token."
        if not employee_id:
            return False, "Sem ID do colaborador."
        url = CONVENIA_BASE + CONVENIA_DOC_ENDPOINT.format(id=employee_id)
        with open(caminho, "rb") as f:
            dados = f.read()
        ctype = media_type_de(os.path.splitext(caminho)[1])
        try:
            status, corpo = _http_post_multipart(
                url,
                campos={"name": os.path.basename(caminho), "type": tipo},
                arquivos={"file": (os.path.basename(caminho), dados, ctype)},
                headers={CONVENIA_HEADER: self.token, "Accept": "application/json"},
            )
            if 200 <= status < 300:
                return True, f"HTTP {status}"
            return False, f"HTTP {status}: {corpo[:120]}"
        except urllib.error.HTTPError as e:
            corpo = ""
            try:
                corpo = e.read().decode("utf-8", errors="replace")[:120]
            except Exception:
                pass
            return False, f"HTTP {e.code}: {corpo}"
        except Exception as e:  # noqa: BLE001
            return False, str(e)


# ---------------------------------------------------------------------------
# IA com fallback
# ---------------------------------------------------------------------------

PROMPT_EXTRACAO = (
    "Voce e um assistente que analisa documentos de Recursos Humanos brasileiros.\n"
    "Analise o documento anexado e responda APENAS com um objeto JSON valido "
    "(sem texto antes ou depois), neste formato:\n"
    "{\n"
    '  "nome": "NOME COMPLETO DO FUNCIONARIO",\n'
    '  "cpf": "000.000.000-00",\n'
    '  "tipo": "TIPO",\n'
    '  "data": "AAAA-MM",\n'
    '  "dias": 0,\n'
    '  "meio_periodo": false,\n'
    '  "periodo": "",\n'
    '  "confianca": "ALTA"\n'
    "}\n\n"
    "Regras:\n"
    "- nome: nome completo, MAIUSCULAS e SEM ACENTOS. Se nao achar, \"DESCONHECIDO\".\n"
    "- cpf: o CPF do funcionario, se aparecer; senao \"\".\n"
    "- tipo: UM destes: " + ", ".join(TIPOS_VALIDOS) + ". Se nao encaixar, DOCUMENTO.\n"
    "- data: competencia no formato AAAA-MM. Se nao houver, \"SEM-DATA\".\n"
    "- dias: SO para ATESTADO, o numero de dias de afastamento (inteiro). Senao 0.\n"
    "- meio_periodo: true se o atestado for de meio periodo / por horas; senao false.\n"
    "- periodo: se meio periodo, o horario (ex.: \"13h as 18h\"); senao \"\".\n"
    "- confianca: ALTA, MEDIA ou BAIXA.\n"
)


class ResultadoIA:
    def __init__(self, ok, dados=None, erro=None, rate_limited=False):
        self.ok = ok
        self.dados = dados
        self.erro = erro
        self.rate_limited = rate_limited


class GerenciadorIA:
    def __init__(self, gemini_key, claude_key, stop_event, log_fn):
        self.gemini_key = (gemini_key or "").strip()
        self.claude_key = (claude_key or "").strip()
        self.stop_event = stop_event
        self.log = log_fn
        self.ativo = "GEMINI" if self.gemini_key else "CLAUDE"
        self.falhas_gemini = 0
        self.trocou_para_claude_em = None
        self.rate_limit_ate = 0
        self._ultimo_erro = None

    def em_rate_limit(self):
        return time.time() < self.rate_limit_ate

    def _ativar_rate_limit(self):
        self.rate_limit_ate = time.time() + JANELA_RETORNO_GEMINI

    def talvez_voltar_gemini(self):
        if (self.ativo == "CLAUDE" and self.gemini_key and self.trocou_para_claude_em
                and time.time() - self.trocou_para_claude_em >= JANELA_RETORNO_GEMINI):
            self.ativo = "GEMINI"
            self.falhas_gemini = 0
            self.trocou_para_claude_em = None
            self.log("Voltando a tentar o Gemini (passaram 5 min).", "info")

    def _trocar_para_claude(self):
        if self.ativo != "CLAUDE":
            self.ativo = "CLAUDE"
            self.trocou_para_claude_em = time.time()
            if self.claude_key:
                self.log("Gemini falhou 3x -> trocando para Claude (fallback).", "claude")
            else:
                self.log("Gemini falhou 3x, mas nao ha chave Claude.", "aviso")

    def _chamar_gemini(self, b64, media_type, prompt):
        url = GEMINI_URL.format(key=self.gemini_key)
        partes = [{"text": prompt}]
        if b64:
            partes.insert(0, {"inline_data": {"mime_type": media_type, "data": b64}})
        payload = {
            "contents": [{"parts": partes}],
            "generationConfig": {"temperature": 0, "response_mime_type": "application/json"}
            if b64 else {"temperature": 0},
        }
        _, corpo = _http_post_json(url, payload, headers={})
        obj = json.loads(corpo)
        try:
            return obj["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            raise ValueError("Resposta do Gemini sem texto.")

    def _chamar_claude(self, b64, media_type, ext, prompt):
        conteudo = []
        if b64:
            if ext.lower() == ".pdf":
                conteudo.append({"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": b64}})
            else:
                conteudo.append({"type": "image", "source": {
                    "type": "base64", "media_type": media_type, "data": b64}})
        conteudo.append({"type": "text", "text": prompt})
        payload = {"model": CLAUDE_MODEL, "max_tokens": 512,
                   "messages": [{"role": "user", "content": conteudo}]}
        headers = {"x-api-key": self.claude_key, "anthropic-version": CLAUDE_VERSION}
        _, corpo = _http_post_json(CLAUDE_URL, payload, headers=headers)
        obj = json.loads(corpo)
        try:
            return "".join(p.get("text", "") for p in obj["content"] if p.get("type") == "text")
        except (KeyError, TypeError):
            raise ValueError("Resposta do Claude sem texto.")

    def _tentar_gemini(self, b64, media_type):
        for tentativa in range(2):
            try:
                dados = extrair_json(self._chamar_gemini(b64, media_type, PROMPT_EXTRACAO))
                if not dados:
                    return ResultadoIA(False, erro="Gemini: JSON invalido.")
                return ResultadoIA(True, dados=dados)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    self._ativar_rate_limit()
                    if tentativa == 0:
                        self.log(f"Gemini limite (429). Aguardando {ESPERA_429}s...", "aviso")
                        if not interruptible_sleep(ESPERA_429, self.stop_event):
                            return ResultadoIA(False, erro="Interrompido.", rate_limited=True)
                        continue
                    return ResultadoIA(False, erro="Gemini 429 persistente.", rate_limited=True)
                corpo = ""
                try:
                    corpo = e.read().decode("utf-8", errors="replace")[:160]
                except Exception:
                    pass
                return ResultadoIA(False, erro=f"Gemini HTTP {e.code}: {corpo}")
            except Exception as e:  # noqa: BLE001
                return ResultadoIA(False, erro=f"Gemini: {e}")
        return ResultadoIA(False, erro="Gemini: falha.", rate_limited=True)

    def _tentar_claude(self, b64, media_type, ext):
        if not self.claude_key:
            return ResultadoIA(False, erro="Sem chave Claude.")
        try:
            dados = extrair_json(self._chamar_claude(b64, media_type, ext, PROMPT_EXTRACAO))
            if not dados:
                return ResultadoIA(False, erro="Claude: JSON invalido.")
            return ResultadoIA(True, dados=dados)
        except urllib.error.HTTPError as e:
            corpo = ""
            try:
                corpo = e.read().decode("utf-8", errors="replace")[:160]
            except Exception:
                pass
            return ResultadoIA(False, erro=f"Claude HTTP {e.code}: {corpo}")
        except Exception as e:  # noqa: BLE001
            return ResultadoIA(False, erro=f"Claude: {e}")

    def extrair(self, b64, media_type, ext):
        self.talvez_voltar_gemini()
        self._ultimo_erro = None
        if self.ativo == "GEMINI" and self.gemini_key:
            res = self._tentar_gemini(b64, media_type)
            if res.ok:
                self.falhas_gemini = 0
                return res.dados, "GEMINI"
            self.falhas_gemini += 1
            self._ultimo_erro = res.erro
            self.log(f"Falha no Gemini ({self.falhas_gemini}/{MAX_FALHAS_GEMINI}): {res.erro}", "aviso")
            if self.falhas_gemini >= MAX_FALHAS_GEMINI:
                self._trocar_para_claude()
        if self.claude_key:
            self.log("Usando Claude (fallback) para este arquivo.", "claude")
            res = self._tentar_claude(b64, media_type, ext)
            if res.ok:
                return res.dados, "CLAUDE"
            self._ultimo_erro = res.erro
            self.log(f"Falha no Claude: {res.erro}", "erro")
        return None, self.ativo

    def testar_chaves(self):
        """Retorna lista de (provedor, ok, msg)."""
        resultados = []
        if self.gemini_key:
            try:
                self._chamar_gemini(None, None, "Responda apenas: OK")
                resultados.append(("Gemini", True, "chave valida"))
            except urllib.error.HTTPError as e:
                resultados.append(("Gemini", False, f"HTTP {e.code}" + (" (limite)" if e.code == 429 else "")))
            except Exception as e:  # noqa: BLE001
                resultados.append(("Gemini", False, str(e)[:80]))
        if self.claude_key:
            try:
                self._chamar_claude(None, None, "", "Responda apenas: OK")
                resultados.append(("Claude", True, "chave valida"))
            except urllib.error.HTTPError as e:
                resultados.append(("Claude", False, f"HTTP {e.code}"))
            except Exception as e:  # noqa: BLE001
                resultados.append(("Claude", False, str(e)[:80]))
        return resultados


# ---------------------------------------------------------------------------
# Historico e relatorios
# ---------------------------------------------------------------------------

HIST_COLS = ["HASH", "NOME", "IDENTIFICADOR", "TIPO", "REFERENCIA", "DIAS",
             "MEIO PERIODO", "PERIODO", "PROVEDOR", "PASTA", "DATA PROCESSAMENTO"]


class Relatorios:
    """Le/atualiza o historico_rh.csv e gera os relatorios e o painel."""

    def __init__(self, destino):
        self.dir = os.path.join(destino, PASTA_RELATORIOS)
        os.makedirs(self.dir, exist_ok=True)
        self.hist_path = os.path.join(self.dir, HISTORICO_NOME)
        self.registros = self._carregar()

    def _carregar(self):
        regs = {}
        if os.path.exists(self.hist_path):
            try:
                with open(self.hist_path, "r", encoding="utf-8-sig", newline="") as f:
                    for row in csv.DictReader(f, delimiter=";"):
                        if row.get("HASH"):
                            regs[row["HASH"]] = row
            except Exception:
                pass
        return regs

    def upsert(self, h, **campos):
        if not h:
            return
        row = {c: "" for c in HIST_COLS}
        row.update({k: ("" if v is None else str(v)) for k, v in campos.items()})
        row["HASH"] = h
        self.registros[h] = row

    def salvar_historico(self):
        with open(self.hist_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HIST_COLS, delimiter=";")
            w.writeheader()
            for row in self.registros.values():
                w.writerow({c: row.get(c, "") for c in HIST_COLS})

    # ---- agregacoes ----
    def _linhas(self):
        return list(self.registros.values())

    def gerar_todos(self, cadastro=None):
        self.salvar_historico()
        self._rel_documentos_por_tipo_mes()
        self._rel_atestados_por_mes()
        self._rel_absenteismo()
        if cadastro and len(cadastro):
            self._rel_pendencias(cadastro)
            self._rel_checklist_admissao(cadastro)
            self._rel_checklist_desligamento(cadastro)
        self._gerar_painel(cadastro)

    def _escrever(self, nome, cabecalho, linhas):
        caminho = os.path.join(self.dir, nome)
        with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(cabecalho)
            w.writerows(linhas)

    def _rel_documentos_por_tipo_mes(self):
        cont = {}
        for r in self._linhas():
            chave = (r.get("TIPO", ""), r.get("REFERENCIA", ""))
            cont[chave] = cont.get(chave, 0) + 1
        linhas = [[t, ref, n] for (t, ref), n in sorted(cont.items())]
        self._escrever("documentos_por_tipo_mes.csv", ["TIPO", "REFERENCIA", "QTD"], linhas)

    def _rel_atestados_por_mes(self):
        agg = {}
        for r in self._linhas():
            if r.get("TIPO") != "ATESTADO":
                continue
            ref = r.get("REFERENCIA", "") or "SEM-DATA"
            d = agg.setdefault(ref, {"n": 0, "dias": 0, "f1": 0, "f2": 0, "f3": 0})
            d["n"] += 1
            dias = int(r.get("DIAS") or 0)
            d["dias"] += dias
            if dias and dias <= 3:
                d["f1"] += 1
            elif 4 <= dias <= 15:
                d["f2"] += 1
            elif dias >= 16:
                d["f3"] += 1
        linhas = [[ref, v["n"], v["dias"], v["f1"], v["f2"], v["f3"]]
                  for ref, v in sorted(agg.items())]
        self._escrever("atestados_por_mes.csv",
                       ["REFERENCIA", "ATESTADOS", "DIAS TOTAIS",
                        "ATE 3 DIAS", "4 A 15", "16+ (INSS)"], linhas)
        return agg

    def _rel_absenteismo(self):
        agg = {}
        for r in self._linhas():
            if r.get("TIPO") != "ATESTADO":
                continue
            nome = r.get("NOME", "")
            d = agg.setdefault(nome, {"n": 0, "dias": 0, "meio": 0})
            d["n"] += 1
            d["dias"] += int(r.get("DIAS") or 0)
            if str(r.get("MEIO PERIODO", "")).lower() in ("sim", "true", "1"):
                d["meio"] += 1
        linhas = [[nome, v["n"], v["dias"], v["meio"]]
                  for nome, v in sorted(agg.items(), key=lambda x: -x[1]["dias"])]
        self._escrever("absenteismo_por_colaborador.csv",
                       ["COLABORADOR", "ATESTADOS", "DIAS TOTAIS", "MEIO PERIODO"], linhas)
        return linhas

    def _docs_da_pessoa(self, nome_norm):
        return [r for r in self._linhas() if normalizar_nome(r.get("NOME", "")) == nome_norm]

    def _rel_pendencias(self, cadastro):
        ref_atual = datetime.now().strftime("%Y-%m")
        linhas = []
        for it in cadastro.itens:
            if cadastro.esta_desligado(it):
                continue
            docs = self._docs_da_pessoa(it["nome_norm"])
            tem_hol = any(d.get("TIPO") == "HOLERITE" and d.get("REFERENCIA") == ref_atual for d in docs)
            tem_rec = any(d.get("TIPO") == "RECIBO" and d.get("REFERENCIA") == ref_atual for d in docs)
            if not (tem_hol or tem_rec):
                linhas.append([it["nome"], ref_atual,
                               "SEM HOLERITE" if not tem_hol else "OK",
                               "SEM RECIBO" if not tem_rec else "OK"])
        self._escrever("pendencias_documentos.csv",
                       ["COLABORADOR", "REFERENCIA", "HOLERITE", "RECIBO"], linhas)
        return linhas

    def _rel_checklist_admissao(self, cadastro):
        limite = datetime.now() - timedelta(days=120)
        linhas = []
        for it in cadastro.itens:
            adm = parse_data_iso(it.get("admissao"))
            if not adm or adm < limite:
                continue
            docs = self._docs_da_pessoa(it["nome_norm"])
            tem = lambda *tt: "OK" if any(d.get("TIPO") in tt for d in docs) else "FALTA"
            linhas.append([it["nome"], it.get("admissao", ""),
                           tem("CONTRATO", "ADMISSAO"), tem("DOCUMENTO")])
        self._escrever("checklist_admissao.csv",
                       ["COLABORADOR", "ADMISSAO", "CONTRATO/ADMISSAO", "OUTROS DOCS"], linhas)
        return linhas

    def _rel_checklist_desligamento(self, cadastro):
        limite = datetime.now() - timedelta(days=180)
        linhas = []
        for it in cadastro.itens:
            des = parse_data_iso(it.get("desligamento"))
            if not des or des < limite:
                continue
            docs = self._docs_da_pessoa(it["nome_norm"])
            tem = lambda *tt: "OK" if any(d.get("TIPO") in tt for d in docs) else "FALTA"
            linhas.append([it["nome"], it.get("desligamento", ""),
                           tem("RESCISAO"), tem("AVISO PREVIO")])
        self._escrever("checklist_desligamento.csv",
                       ["COLABORADOR", "DESLIGAMENTO", "RESCISAO", "AVISO PREVIO"], linhas)
        return linhas

    def _gerar_painel(self, cadastro):
        linhas = self._linhas()
        total = len(linhas)
        por_tipo = {}
        for r in linhas:
            por_tipo[r.get("TIPO", "")] = por_tipo.get(r.get("TIPO", ""), 0) + 1
        atest = self._rel_atestados_por_mes()
        ranking = self._rel_absenteismo()[:15]
        inss = sum(v["f3"] for v in atest.values())
        pend = self._rel_pendencias(cadastro)[:30] if cadastro and len(cadastro) else []

        def barras(dic):
            if not dic:
                return "<p class='vazio'>Sem dados.</p>"
            mx = max(dic.values()) or 1
            out = ""
            for k, v in sorted(dic.items(), key=lambda x: -x[1]):
                pct = int(v / mx * 100)
                out += (f"<div class='bar-row'><span class='bar-lbl'>{html.escape(str(k))}</span>"
                        f"<span class='bar'><span style='width:{pct}%'></span></span>"
                        f"<span class='bar-val'>{v}</span></div>")
            return out

        def tabela(cab, linhas_):
            th = "".join(f"<th>{html.escape(c)}</th>" for c in cab)
            tr = ""
            for ln in linhas_:
                tr += "<tr>" + "".join(f"<td>{html.escape(str(x))}</td>" for x in ln) + "</tr>"
            return f"<table><thead><tr>{th}</tr></thead><tbody>{tr or '<tr><td>Sem dados</td></tr>'}</tbody></table>"

        atest_tab = [[ref, v["n"], v["dias"], v["f1"], v["f2"], v["f3"]]
                     for ref, v in sorted(atest.items())]
        agora = datetime.now().strftime("%d/%m/%Y %H:%M")
        doc = f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="utf-8">
<title>Painel RH - Classic Couros</title>
<style>
body{{font-family:Segoe UI,Arial,sans-serif;background:#0f1117;color:#e4e6f0;margin:0;padding:24px}}
h1{{color:#60a5fa}} h2{{color:#93c5fd;border-bottom:1px solid #2f3147;padding-bottom:6px;margin-top:32px}}
.kpis{{display:flex;flex-wrap:wrap;gap:16px}}
.kpi{{background:#1b1e2b;border:1px solid #2f3147;border-radius:10px;padding:16px 22px;min-width:140px}}
.kpi b{{display:block;font-size:28px;color:#4ade80}} .kpi.alerta b{{color:#f87171}}
.kpi span{{color:#9aa0b5;font-size:13px}}
table{{border-collapse:collapse;width:100%;margin-top:10px}}
th,td{{border:1px solid #2f3147;padding:6px 10px;text-align:left;font-size:13px}}
th{{background:#1b1e2b;color:#93c5fd}}
.bar-row{{display:flex;align-items:center;gap:10px;margin:4px 0;font-size:13px}}
.bar-lbl{{width:200px}} .bar{{flex:1;background:#1b1e2b;border-radius:5px;height:16px;overflow:hidden}}
.bar span{{display:block;height:100%;background:#3b82f6}} .bar-val{{width:40px;text-align:right}}
.vazio{{color:#9aa0b5}} .rodape{{margin-top:30px;color:#9aa0b5;font-size:12px}}
</style></head><body>
<h1>Painel RH - Classic Couros</h1>
<p style="color:#9aa0b5">Gerado em {agora} - dados de {total} documento(s) processado(s).</p>
<div class="kpis">
  <div class="kpi"><b>{total}</b><span>Documentos processados</span></div>
  <div class="kpi"><b>{por_tipo.get('ATESTADO',0)}</b><span>Atestados</span></div>
  <div class="kpi alerta"><b>{inss}</b><span>Atestados 16+ dias (INSS)</span></div>
  <div class="kpi alerta"><b>{len(pend)}</b><span>Pendencias de documentos</span></div>
</div>
<h2>Documentos por tipo</h2>{barras(por_tipo)}
<h2>Atestados por mes</h2>
{tabela(["Referencia","Atestados","Dias","Ate 3","4 a 15","16+ (INSS)"], atest_tab)}
<h2>Ranking de absenteismo (mais dias)</h2>
{tabela(["Colaborador","Atestados","Dias totais","Meio periodo"], ranking)}
<h2>Pendencias de documentos (mes atual)</h2>
{tabela(["Colaborador","Referencia","Holerite","Recibo"], pend)}
<p class="rodape">LGPD: este painel contem dados de pessoas. Restrinja o acesso a pasta {PASTA_RELATORIOS}.</p>
</body></html>"""
        with open(os.path.join(self.dir, "painel_rh.html"), "w", encoding="utf-8") as f:
            f.write(doc)


# ---------------------------------------------------------------------------
# Interface grafica
# ---------------------------------------------------------------------------


class App:
    def __init__(self, root):
        self.root = root
        self.root.title(f"Renomeador RH PRO - {APP_VERSAO}")
        self.root.configure(bg=COR_BG)
        self.root.geometry("1100x860")
        self.root.minsize(960, 780)

        self.fila = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker = None
        self.progresso = {"origem": "", "processados": {}}
        self.cadastro = Cadastro()
        self.lock_path = None

        self._construir_estilo()
        self._construir_ui()
        self._carregar_config()
        self._carregar_env()

        self.root.protocol("WM_DELETE_WINDOW", self._ao_fechar)
        self.root.after(120, self._processar_fila)

    def _construir_estilo(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Dark.Horizontal.TProgressbar", troughcolor=COR_SURFACE2,
                        background=COR_AZUL, bordercolor=COR_SURFACE2,
                        lightcolor=COR_AZUL, darkcolor=COR_AZUL)

    def _label(self, parent, texto, **kw):
        return tk.Label(parent, text=texto, bg=kw.pop("bg", COR_BG),
                        fg=kw.pop("fg", COR_FG), **kw)

    def _construir_ui(self):
        cab = tk.Frame(self.root, bg=COR_SURFACE)
        cab.pack(fill="x")
        tk.Label(cab, text=f"  RENOMEADOR RH PRO  -  {APP_VERSAO}", bg=COR_SURFACE,
                 fg=COR_AZUL, font=("Segoe UI", 16, "bold")).pack(side="left", pady=10)
        tk.Label(cab, text="IA Gemini/Claude  -  Convenia  -  Dossie  -  Painel RH   ",
                 bg=COR_SURFACE, fg=COR_FG_DIM, font=("Segoe UI", 9)).pack(side="right", pady=12)

        corpo = tk.Frame(self.root, bg=COR_BG)
        corpo.pack(fill="both", expand=True, padx=14, pady=8)

        cfg = tk.LabelFrame(corpo, text=" Configuracao ", bg=COR_BG, fg=COR_FG,
                            font=("Segoe UI", 10, "bold"), labelanchor="nw", bd=1, relief="solid")
        cfg.pack(fill="x")
        cfg.grid_columnconfigure(1, weight=1)

        self.var_gemini = tk.StringVar()
        self.var_claude = tk.StringVar()
        self.var_convenia = tk.StringVar()
        self.var_origem = tk.StringVar()
        self.var_destino = tk.StringVar()
        self.var_cadastro = tk.StringVar()
        self.var_intervalo = tk.IntVar(value=10)
        self.var_mover = tk.BooleanVar(value=False)
        self.var_teste = tk.BooleanVar(value=False)
        self.var_ler_nomes = tk.BooleanVar(value=True)
        self.var_agrupar = tk.BooleanVar(value=True)
        self.var_enviar_convenia = tk.BooleanVar(value=False)

        self._campo_senha(cfg, "Chave Gemini:", self.var_gemini, 0)
        self._campo_senha(cfg, "Chave Claude:", self.var_claude, 1)

        self._label(cfg, "Token Convenia:").grid(row=2, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(cfg, textvariable=self.var_convenia, show="*", bg=COR_SURFACE2, fg=COR_FG,
                 insertbackground=COR_FG, relief="flat").grid(
            row=2, column=1, sticky="we", padx=10, pady=4, ipady=4)
        self.btn_validar = tk.Button(cfg, text="Validar", command=self.validar_convenia,
                                     bg=COR_AZUL, fg="white", relief="flat", padx=14)
        self.btn_validar.grid(row=2, column=2, sticky="e", padx=10, pady=4)

        self._campo_pasta(cfg, "Pasta origem:", self.var_origem, 3)
        self._campo_pasta(cfg, "Pasta destino:", self.var_destino, 4)

        self._label(cfg, "Cadastro:").grid(row=5, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(cfg, textvariable=self.var_cadastro, bg=COR_SURFACE2, fg=COR_FG,
                 insertbackground=COR_FG, relief="flat").grid(
            row=5, column=1, sticky="we", padx=10, pady=4, ipady=4)
        cad_btns = tk.Frame(cfg, bg=COR_BG)
        cad_btns.grid(row=5, column=2, sticky="e", padx=10, pady=4)
        tk.Button(cad_btns, text="XLSX", command=self.carregar_cadastro_xlsx, bg=COR_SURFACE2,
                  fg=COR_FG, relief="flat", padx=10).pack(side="left", padx=(0, 6))
        tk.Button(cad_btns, text="API", command=self.carregar_cadastro_api, bg=COR_SURFACE2,
                  fg=COR_FG, relief="flat", padx=10).pack(side="left")

        self._label(cfg, "Intervalo (s):").grid(row=6, column=0, sticky="w", padx=10, pady=(8, 4))
        sld = tk.Frame(cfg, bg=COR_BG)
        sld.grid(row=6, column=1, columnspan=2, sticky="we", padx=10, pady=(8, 4))
        self.slider = tk.Scale(sld, from_=6, to=30, orient="horizontal", variable=self.var_intervalo,
                               bg=COR_BG, fg=COR_FG, highlightthickness=0, troughcolor=COR_SURFACE2,
                               activebackground=COR_AZUL, length=360, showvalue=False,
                               command=lambda _v: self._atualizar_label_intervalo())
        self.slider.pack(side="left", fill="x", expand=True)
        self.lbl_intervalo = self._label(sld, "10s", fg=COR_AZUL)
        self.lbl_intervalo.pack(side="left", padx=12)

        chks = tk.Frame(cfg, bg=COR_BG)
        chks.grid(row=7, column=0, columnspan=3, sticky="w", padx=8, pady=(2, 10))
        self._check(chks, "Mover (nao copiar)", self.var_mover)
        self._check(chks, "Modo teste (5 arquivos)", self.var_teste)
        self._check(chks, "Ler nome do arquivo antes de usar IA (gratuito)", self.var_ler_nomes)
        self._check(chks, "Agrupar por colaborador (dossie)", self.var_agrupar)
        self._check(chks, "Enviar para Convenia (FASE 5)", self.var_enviar_convenia)

        botoes = tk.Frame(corpo, bg=COR_BG)
        botoes.pack(fill="x", pady=(10, 6))
        self.btn_iniciar = self._botao(botoes, "INICIAR", COR_VERDE, self.iniciar)
        self.btn_pausar = self._botao(botoes, "PAUSAR", COR_LARANJA, self.pausar_retomar)
        self.btn_parar = self._botao(botoes, "PARAR", COR_VERMELHO, self.parar)
        self.btn_iniciar.pack(side="left", padx=(0, 8))
        self.btn_pausar.pack(side="left", padx=8)
        self.btn_parar.pack(side="left", padx=8)
        self.btn_reprocessar = tk.Button(botoes, text="Reprocessar falhas", command=self.reprocessar,
                                         bg=COR_SURFACE2, fg=COR_FG, relief="flat", padx=16, pady=8)
        self.btn_reprocessar.pack(side="right")
        self.btn_testar = tk.Button(botoes, text="Testar Chaves", command=self.testar_chaves,
                                    bg=COR_SURFACE2, fg=COR_FG, relief="flat", padx=16, pady=8)
        self.btn_testar.pack(side="right", padx=8)
        self.btn_pausar.config(state="disabled")
        self.btn_parar.config(state="disabled")

        ind = tk.Frame(corpo, bg=COR_BG)
        ind.pack(fill="x", pady=(6, 4))
        self._label(ind, "Progresso de Processamento", fg=COR_AZUL,
                    font=("Segoe UI", 10, "bold")).pack(anchor="w")
        prog = tk.Frame(ind, bg=COR_BG)
        prog.pack(fill="x", pady=(6, 4))
        self.barra = ttk.Progressbar(prog, style="Dark.Horizontal.TProgressbar", maximum=100)
        self.barra.pack(side="left", fill="x", expand=True)
        self.lbl_percent = self._label(prog, "0%", fg=COR_FG)
        self.lbl_percent.pack(side="left", padx=10)
        linha = tk.Frame(ind, bg=COR_BG)
        linha.pack(fill="x")
        self.lbl_status = self._label(linha, "Pronto. Preencha a configuracao e clique em INICIAR.",
                                      fg=COR_FG_DIM)
        self.lbl_status.pack(side="left")
        self.lbl_provedor = self._label(linha, "", fg=COR_FG_DIM)
        self.lbl_provedor.pack(side="left", padx=16)
        self.lbl_countdown = self._label(linha, "", fg=COR_LARANJA)
        self.lbl_countdown.pack(side="right")

        logf = tk.LabelFrame(corpo, text=" Registro de Atividades ", bg=COR_BG, fg=COR_FG,
                             font=("Segoe UI", 10, "bold"), bd=1, relief="solid")
        logf.pack(fill="both", expand=True, pady=(8, 0))
        self.log_text = tk.Text(logf, bg="#15161f", fg=COR_FG, insertbackground=COR_FG,
                                wrap="word", height=12, relief="flat", font=("Consolas", 9))
        scroll = tk.Scrollbar(logf, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.log_text.pack(side="left", fill="both", expand=True, padx=4, pady=4)
        for nivel, cor in LOG_CORES.items():
            self.log_text.tag_config(nivel, foreground=cor)
        self.log_text.config(state="disabled")

    def _campo_senha(self, parent, rotulo, var, row):
        self._label(parent, rotulo).grid(row=row, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(parent, textvariable=var, show="*", bg=COR_SURFACE2, fg=COR_FG,
                 insertbackground=COR_FG, relief="flat").grid(
            row=row, column=1, columnspan=2, sticky="we", padx=10, pady=4, ipady=4)

    def _campo_pasta(self, parent, rotulo, var, row):
        self._label(parent, rotulo).grid(row=row, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(parent, textvariable=var, bg=COR_SURFACE2, fg=COR_FG,
                 insertbackground=COR_FG, relief="flat").grid(
            row=row, column=1, sticky="we", padx=10, pady=4, ipady=4)
        tk.Button(parent, text="Procurar", command=lambda: self._escolher_pasta(var),
                  bg=COR_SURFACE2, fg=COR_FG, relief="flat", padx=10).grid(
            row=row, column=2, sticky="e", padx=10, pady=4)

    def _check(self, parent, texto, var):
        tk.Checkbutton(parent, text=texto, variable=var, bg=COR_BG, fg=COR_FG,
                       selectcolor=COR_SURFACE2, activebackground=COR_BG,
                       activeforeground=COR_FG, anchor="w").pack(side="top", anchor="w", padx=4)

    def _botao(self, parent, texto, cor, comando):
        return tk.Button(parent, text=texto, command=comando, bg=cor, fg="#0b0b12",
                         activebackground=cor, font=("Segoe UI", 11, "bold"),
                         relief="flat", padx=24, pady=8, cursor="hand2")

    def _escolher_pasta(self, var):
        d = filedialog.askdirectory()
        if d:
            var.set(d)

    def _atualizar_label_intervalo(self):
        self.lbl_intervalo.config(text=f"{self.var_intervalo.get()}s")

    # ----- log / fila -----
    def log(self, msg, nivel="info"):
        self.fila.put(("log", msg, nivel))

    def _escrever_log(self, msg, nivel):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.config(state="normal")
        self.log_text.insert("end", f"[{ts}] {msg}\n", nivel)
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def _processar_fila(self):
        try:
            while True:
                item = self.fila.get_nowait()
                t = item[0]
                if t == "log":
                    self._escrever_log(item[1], item[2])
                elif t == "status":
                    self.lbl_status.config(text=item[1])
                elif t == "provedor":
                    cor = LOG_CORES["claude"] if item[1] == "CLAUDE" else COR_AZUL
                    self.lbl_provedor.config(text=f"Provedor: {item[1]}", fg=cor)
                elif t == "progresso":
                    self.barra["value"] = item[1]
                    self.lbl_percent.config(text=f"{int(item[1])}%")
                    if item[2]:
                        self.lbl_status.config(text=item[2])
                elif t == "countdown":
                    self.lbl_countdown.config(text=item[1])
                elif t == "fim":
                    self._ao_terminar()
        except queue.Empty:
            pass
        self.root.after(120, self._processar_fila)

    # ----- persistencia -----
    def _carregar_config(self):
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    c = json.load(f)
                self.var_gemini.set(dpapi_abrir(c.get("gemini_key", "")))
                self.var_claude.set(dpapi_abrir(c.get("claude_key", "")))
                self.var_convenia.set(dpapi_abrir(c.get("convenia_token", "")))
                self.var_origem.set(c.get("origem", ""))
                self.var_destino.set(c.get("destino", ""))
                self.var_cadastro.set(c.get("cadastro", ""))
                self.var_intervalo.set(int(c.get("intervalo", 10)))
                self.var_mover.set(bool(c.get("mover", False)))
                self.var_teste.set(bool(c.get("teste", False)))
                self.var_ler_nomes.set(bool(c.get("ler_nomes", True)))
                self.var_agrupar.set(bool(c.get("agrupar", True)))
                self.var_enviar_convenia.set(bool(c.get("enviar_convenia", False)))
                self._atualizar_label_intervalo()
            except Exception:
                pass

    def _carregar_env(self):
        if not os.path.exists(ENV_PATH):
            return
        env = ler_env(ENV_PATH)
        if env.get("GEMINI_KEY"):
            self.var_gemini.set(env["GEMINI_KEY"])
        if env.get("CLAUDE_KEY"):
            self.var_claude.set(env["CLAUDE_KEY"])
        if env.get("CONVENIA_TOKEN"):
            self.var_convenia.set(env["CONVENIA_TOKEN"])
        if env:
            self.log("Credenciais carregadas do arquivo .env compartilhado.", "info")

    def _salvar_config(self):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump({
                    "gemini_key": dpapi_proteger(self.var_gemini.get().strip()),
                    "claude_key": dpapi_proteger(self.var_claude.get().strip()),
                    "convenia_token": dpapi_proteger(self.var_convenia.get().strip()),
                    "origem": self.var_origem.get().strip(),
                    "destino": self.var_destino.get().strip(),
                    "cadastro": self.var_cadastro.get().strip(),
                    "intervalo": self.var_intervalo.get(),
                    "mover": self.var_mover.get(),
                    "teste": self.var_teste.get(),
                    "ler_nomes": self.var_ler_nomes.get(),
                    "agrupar": self.var_agrupar.get(),
                    "enviar_convenia": self.var_enviar_convenia.get(),
                }, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _progresso_path(self, destino):
        return os.path.join(destino, PROGRESSO_NOME)

    def _carregar_progresso(self, destino):
        p = self._progresso_path(destino)
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"origem": "", "processados": {}}

    def _salvar_progresso(self):
        destino = self.var_destino.get().strip()
        if not destino:
            return
        try:
            with open(self._progresso_path(destino), "w", encoding="utf-8") as f:
                json.dump(self.progresso, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ----- lock de equipe -----
    def _adquirir_lock(self, destino):
        self.lock_path = os.path.join(destino, LOCK_NOME)
        if os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, "r", encoding="utf-8") as f:
                    info = json.load(f)
                quando = parse_data_iso(info.get("quando", "")) or datetime.min
                # lock velho (>2h) e considerado abandonado
                if (datetime.now() - quando) < timedelta(hours=2):
                    return False, info.get("usuario", "outro usuario")
            except Exception:
                pass
        try:
            with open(self.lock_path, "w", encoding="utf-8") as f:
                json.dump({"usuario": getpass.getuser(),
                           "quando": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)
        except Exception:
            pass
        return True, ""

    def _liberar_lock(self):
        try:
            if self.lock_path and os.path.exists(self.lock_path):
                os.remove(self.lock_path)
        except Exception:
            pass

    # ----- Convenia / cadastro / testes (em thread) -----
    def validar_convenia(self):
        token = self.var_convenia.get().strip()
        if not token:
            messagebox.showwarning("Token", "Informe o Token da Convenia.")
            return
        self.btn_validar.config(state="disabled", text="Validando...")
        self.log("Validando token Convenia...", "info")
        self.log(f"Token Convenia recebido (primeiros 10 chars): {token[:10]}...", "info")

        def tarefa():
            self.log("Testando token Convenia...", "info")
            ok, msg = ConveniaClient(token).validar()
            self.log("Token Convenia validado com sucesso." if ok
                     else f"Erro ao validar token Convenia: {msg}", "convenia" if ok else "erro")
            self.root.after(0, lambda: self.btn_validar.config(state="normal", text="Validar"))
        threading.Thread(target=tarefa, daemon=True).start()

    def testar_chaves(self):
        gem = self.var_gemini.get().strip()
        cla = self.var_claude.get().strip()
        if not gem and not cla:
            messagebox.showwarning("Chaves", "Informe ao menos uma chave (Gemini ou Claude).")
            return
        self.btn_testar.config(state="disabled", text="Testando...")
        self.log("Testando chaves de IA...", "info")

        def tarefa():
            g = GerenciadorIA(gem, cla, self.stop_event, self.log)
            for prov, ok, msg in g.testar_chaves():
                self.log(f"{prov}: {'OK - ' if ok else 'FALHA - '}{msg}", "sucesso" if ok else "erro")
            self.root.after(0, lambda: self.btn_testar.config(state="normal", text="Testar Chaves"))
        threading.Thread(target=tarefa, daemon=True).start()

    def carregar_cadastro_xlsx(self):
        caminho = filedialog.askopenfilename(
            title="Selecione a planilha de colaboradores (Convenia)",
            filetypes=[("Planilha Excel", "*.xlsx")])
        if not caminho:
            return
        try:
            n = self.cadastro.carregar_xlsx(caminho)
            self.var_cadastro.set(caminho)
            self.log(f"Cadastro XLSX carregado: {n} colaboradores.", "sucesso")
        except Exception as e:  # noqa: BLE001
            messagebox.showerror("Cadastro", f"Nao foi possivel ler a planilha:\n{e}")
            self.log(f"Erro ao ler XLSX: {e}", "erro")

    def carregar_cadastro_api(self):
        token = self.var_convenia.get().strip()
        if not token:
            messagebox.showwarning("Token", "Informe o Token da Convenia para usar a API.")
            return
        self.log("Carregando colaboradores da Convenia...", "info")

        def tarefa():
            try:
                n = self.cadastro.carregar_convenia(ConveniaClient(token))
                self.var_cadastro.set(f"Convenia API ({n} colaboradores)")
                self.log(f"Cadastro Convenia carregado: {n} colaboradores.", "convenia")
            except Exception as e:  # noqa: BLE001
                self.log(f"Erro ao carregar cadastro da Convenia: {e}", "erro")
        threading.Thread(target=tarefa, daemon=True).start()

    # ----- controle -----
    def iniciar(self, reprocessar=False):
        if self.worker and self.worker.is_alive():
            return
        gemini = self.var_gemini.get().strip()
        claude = self.var_claude.get().strip()
        origem = self.var_origem.get().strip()
        destino = self.var_destino.get().strip()
        if not gemini and not claude:
            messagebox.showerror("Falta a chave", "Informe a Chave Gemini (ou Claude).")
            return
        if not origem or not os.path.isdir(origem):
            messagebox.showerror("Pasta invalida", "Selecione uma pasta de ORIGEM valida.")
            return
        if not destino:
            messagebox.showerror("Pasta invalida", "Selecione a pasta DESTINO (raiz do RH).")
            return
        os.makedirs(destino, exist_ok=True)
        if self.var_enviar_convenia.get() and not self.var_convenia.get().strip():
            messagebox.showerror("Convenia", "Para enviar a Convenia, informe o Token.")
            return

        ok, usuario = self._adquirir_lock(destino)
        if not ok:
            messagebox.showwarning("Sistema em uso",
                                   f"Sistema em uso por {usuario} - aguarde ele terminar.")
            return

        self._salvar_config()
        arquivos = sorted(f for f in os.listdir(origem)
                          if os.path.isfile(os.path.join(origem, f))
                          and f.lower().endswith(EXTENSOES_VALIDAS))
        if not arquivos:
            messagebox.showinfo("Nada a fazer", "Nenhum PDF/JPG/PNG na pasta de origem.")
            self._liberar_lock()
            return

        self.progresso = self._carregar_progresso(destino)
        if self.progresso.get("origem") != origem:
            self.progresso = {"origem": origem, "processados": {}}

        if reprocessar:
            alvos = [n for n in arquivos
                     if self.progresso["processados"].get(n, {}).get("status") in ("ERRO", "PENDENTE")
                     or self.progresso["processados"].get(n, {}).get("confianca") == "BAIXA"]
            if not alvos:
                messagebox.showinfo("Reprocessar", "Nao ha falhas/pendencias/BAIXA para reprocessar.")
                self._liberar_lock()
                return
            for n in alvos:
                self.progresso["processados"].pop(n, None)
            arquivos = alvos
            self.log(f"Reprocessando {len(alvos)} arquivo(s).", "aviso")
        else:
            ja = [n for n, i in self.progresso.get("processados", {}).items()
                  if i.get("status") == "SUCESSO" and n in arquivos]
            if ja:
                if not messagebox.askyesno("Retomar progresso",
                                           f"Encontrei {len(ja)} arquivo(s) ja processado(s).\n\n"
                                           "Continuar de onde parou?\nSim = pular os ja feitos\n"
                                           "Nao = recomecar do zero"):
                    self.progresso = {"origem": origem, "processados": {}}
                    self._salvar_progresso()

        if self.var_teste.get():
            arquivos = arquivos[:5]
            self.log("Modo teste: no maximo 5 arquivos.", "aviso")

        self.stop_event.clear()
        self.pause_event.clear()
        self.btn_iniciar.config(state="disabled")
        self.btn_reprocessar.config(state="disabled")
        self.btn_testar.config(state="disabled")
        self.btn_pausar.config(state="normal", text="PAUSAR")
        self.btn_parar.config(state="normal")
        self._limpar_log()
        self.worker = threading.Thread(target=self._executar, args=(arquivos, origem, destino),
                                       daemon=True)
        self.worker.start()

    def reprocessar(self):
        self.iniciar(reprocessar=True)

    def pausar_retomar(self):
        if not (self.worker and self.worker.is_alive()):
            return
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.btn_pausar.config(text="PAUSAR")
            self.log("Processamento retomado.", "info")
        else:
            self.pause_event.set()
            self.btn_pausar.config(text="RETOMAR")
            self.log("Processamento pausado.", "aviso")

    def parar(self):
        if self.worker and self.worker.is_alive():
            if not messagebox.askyesno("Parar", "Deseja realmente parar o processamento?"):
                return
            self.stop_event.set()
            self.pause_event.clear()
            self.log("Parando... aguarde o arquivo atual terminar.", "aviso")

    def _limpar_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")

    # ----- worker -----
    def _executar(self, arquivos, origem, destino):
        try:
            self._loop(arquivos, origem, destino)
        except Exception as e:  # noqa: BLE001
            self.log(f"Erro inesperado: {e}", "erro")
        finally:
            self._liberar_lock()
            self.fila.put(("fim",))

    def _esperar_se_pausado(self):
        while self.pause_event.is_set() and not self.stop_event.is_set():
            self.fila.put(("countdown", "PAUSADO"))
            time.sleep(0.2)

    def _loop(self, arquivos, origem, destino):
        gerente = GerenciadorIA(self.var_gemini.get().strip(), self.var_claude.get().strip(),
                                self.stop_event, self.log)
        enviar = self.var_enviar_convenia.get()
        agrupar = self.var_agrupar.get()
        ler_nome = self.var_ler_nomes.get()
        usar_cad = len(self.cadastro) > 0
        convenia = ConveniaClient(self.var_convenia.get().strip()) if enviar else None
        relat = Relatorios(destino)

        total = len(arquivos)
        self.log(f"Iniciando. {total} arquivo(s) a processar.", "info")
        if usar_cad:
            self.log(f"Cadastro carregado: {len(self.cadastro)} colaboradores.", "info")
        self.fila.put(("provedor", gerente.ativo))
        feitos = 0
        n_revisar = 0

        for idx, nome in enumerate(arquivos, start=1):
            if self.stop_event.is_set():
                break
            self._esperar_se_pausado()
            if self.stop_event.is_set():
                break

            prev = self.progresso["processados"].get(nome)
            if prev and prev.get("status") == "SUCESSO":
                self.log(f"[{idx}/{total}] {nome}: ja processado, pulando.", "info")
                feitos += 1
                self._atualizar_barra(feitos, total)
                continue

            self.fila.put(("status", f"[{idx}/{total}] {nome}"))
            caminho = os.path.join(origem, nome)
            ext = os.path.splitext(nome)[1].lower()
            chamou_ia = False

            # 1) tentar pelo nome do arquivo (gratis)
            dados, provedor = None, None
            if ler_nome:
                d = extrair_do_nome_arquivo(nome, self.cadastro if usar_cad else None)
                if d:
                    dados, provedor = d, "Nome do arquivo"
                    self.log(f"[{idx}/{total}] {nome}: lido pelo nome do arquivo.", "info")

            # 2) IA
            if dados is None:
                try:
                    with open(caminho, "rb") as f:
                        b64 = base64.b64encode(f.read()).decode("ascii")
                except Exception as e:  # noqa: BLE001
                    self.log(f"[{idx}/{total}] Erro ao ler {nome}: {e}", "erro")
                    self._registrar(nome, "", "", "-", "ERRO", "-", "-", f"leitura: {e}")
                    continue
                chamou_ia = True
                dados, provedor = gerente.extrair(b64, media_type_de(ext), ext)
                self.fila.put(("provedor", gerente.ativo))

            if not dados:
                erro = gerente._ultimo_erro or "falha na IA"
                self.log(f"[{idx}/{total}] {nome}: FALHOU ({erro}).", "erro")
                self._registrar(nome, "", "", provedor or "-", "ERRO", "-", "-", str(erro))
                if chamou_ia:
                    self._contagem(gerente, idx, total)
                continue

            # normaliza campos
            funcionario = normalizar_nome(dados.get("nome")) or "DESCONHECIDO"
            cpf = so_digitos(dados.get("cpf"))
            tipo = canonizar_tipo(dados.get("tipo"))
            data_ref = validar_data(dados.get("data"))
            try:
                dias = int(dados.get("dias") or 0) or None
            except (TypeError, ValueError):
                dias = None
            meio = bool(dados.get("meio_periodo"))
            periodo = str(dados.get("periodo") or "").strip()
            conf = (dados.get("confianca") or "MEDIA").upper()
            if conf not in ("ALTA", "MEDIA", "BAIXA"):
                conf = "MEDIA"

            # cadastro: corrige nome, pega id/cpf/matricula, detecta desligado
            colaborador = None
            if usar_cad:
                colaborador = self.cadastro.encontrar(nome=funcionario, cpf=cpf)
                if colaborador:
                    oficial = normalizar_nome(colaborador["nome"])
                    if oficial != funcionario:
                        self.log(f"   Nome ajustado pelo cadastro: {funcionario} -> {oficial}", "info")
                    funcionario = oficial
                    cpf = colaborador.get("cpf") or cpf

            # decide _REVISAR
            motivo_revisar = None
            if conf == "BAIXA":
                motivo_revisar = "confianca BAIXA"
            elif funcionario == "DESCONHECIDO":
                motivo_revisar = "funcionario nao identificado"
            elif usar_cad and not colaborador:
                motivo_revisar = "nao encontrado no cadastro"

            if motivo_revisar:
                self._para_revisar(caminho, destino, nome)
                n_revisar += 1
                self.log(f"[{idx}/{total}] {nome} -> _REVISAR ({motivo_revisar}).", "revisar")
                self._registrar(nome, nome, PASTA_REVISAR, provedor, "PENDENTE", conf, "-", motivo_revisar)
                if chamou_ia:
                    self._contagem(gerente, idx, total)
                continue

            id_tag = identificador(colaborador.get("matricula") if colaborador else "", cpf)
            desligado = self.cadastro.esta_desligado(colaborador) or tipo in ("RESCISAO", "AVISO PREVIO")

            pasta_rel = pasta_relativa(tipo, data_ref, dias, meio)
            sufixo = "_meio-periodo" if (tipo == "ATESTADO" and meio) else ""
            novo_base = f"{funcionario}_{id_tag}_{tipo}_{data_ref}{sufixo}"
            novo_nome, caminho_final = self._arquivar(caminho, ext, destino, pasta_rel, novo_base)

            # dossie por colaborador
            if agrupar and caminho_final:
                self._dossie(destino, funcionario, id_tag, caminho_final, novo_nome, desligado)

            self.log(f"[{idx}/{total}] {nome} -> {pasta_rel}/{novo_nome} [{provedor}, {conf}]", "sucesso")

            # historico
            relat.upsert(hash_arquivo(caminho_final) or hash_arquivo(caminho),
                         NOME=funcionario, IDENTIFICADOR=id_tag, TIPO=tipo, REFERENCIA=data_ref,
                         DIAS=dias or "", **{"MEIO PERIODO": "Sim" if meio else "Nao"},
                         PERIODO=periodo, PROVEDOR=provedor, PASTA=pasta_rel,
                         **{"DATA PROCESSAMENTO": datetime.now().strftime("%Y-%m-%d %H:%M")})

            # Convenia
            envio = "-"
            if enviar and convenia and caminho_final:
                cid = colaborador.get("id") if colaborador else ""
                if cid:
                    ok, msg = convenia.enviar_documento(cid, caminho_final, tipo)
                    envio = "ENVIADO" if ok else "FALHA"
                    self.log(f"   Convenia: {'enviado' if ok else 'falha'} ({msg}).",
                             "convenia" if ok else "erro")
                else:
                    envio = "SEM ID"
                    self.log("   Convenia: colaborador sem ID, envio ignorado.", "aviso")

            self._remover_de_revisar(destino, nome)
            self._registrar(nome, novo_nome, pasta_rel, provedor, "SUCESSO", conf, envio, "")
            feitos += 1
            self._atualizar_barra(feitos, total)
            if chamou_ia and idx < total and not self.stop_event.is_set():
                self._contagem(gerente, idx, total)

        self.fila.put(("countdown", ""))
        try:
            relat.gerar_todos(self.cadastro if len(self.cadastro) else None)
            self.log(f"Base e relatorios atualizados em '{PASTA_RELATORIOS}'.", "info")
        except Exception as e:  # noqa: BLE001
            self.log(f"Falha ao gerar relatorios: {e}", "erro")
        self._gerar_relatorio_execucao(destino)
        if n_revisar:
            self.log(f"Atencao: {n_revisar} arquivo(s) foram para _REVISAR.", "revisar")
        self.log("Processamento PARADO pelo usuario." if self.stop_event.is_set()
                 else "Processamento CONCLUIDO!", "aviso" if self.stop_event.is_set() else "sucesso")
        self._atualizar_barra(feitos, total)

    def _atualizar_barra(self, feitos, total):
        self.fila.put(("progresso", (feitos / total * 100) if total else 0, None))

    def _contagem(self, gerente, idx, total):
        base = max(6, int(self.var_intervalo.get()))
        intervalo = max(base, GEMINI_INTERVALO_MIN) if gerente.ativo == "GEMINI" else base
        if gerente.em_rate_limit():
            intervalo = max(intervalo * 2, 30)
            self.log(f"Modo cautela (limite recente): aguardando {intervalo}s.", "aviso")
        restante = intervalo
        while restante > 0:
            if self.stop_event.is_set():
                return
            self._esperar_se_pausado()
            if self.stop_event.is_set():
                return
            self.fila.put(("countdown", f"Proximo arquivo em {restante}s"))
            time.sleep(1)
            restante -= 1
        self.fila.put(("countdown", ""))

    def _nome_unico(self, pasta, base, ext):
        nome = f"{base}{ext}"
        alvo = os.path.join(pasta, nome)
        c = 1
        while os.path.exists(alvo):
            nome = f"{base}_{c}{ext}"
            alvo = os.path.join(pasta, nome)
            c += 1
        return nome, alvo

    def _arquivar(self, origem_caminho, ext, destino, pasta_rel, base):
        pasta = os.path.join(destino, pasta_rel)
        os.makedirs(pasta, exist_ok=True)
        nome, alvo = self._nome_unico(pasta, base, ext)
        try:
            if self.var_mover.get():
                shutil.move(origem_caminho, alvo)
            else:
                shutil.copy2(origem_caminho, alvo)
        except Exception as e:  # noqa: BLE001
            self.log(f"Erro ao arquivar: {e}", "erro")
            return nome, ""
        return nome, alvo

    def _dossie(self, destino, funcionario, id_tag, caminho_final, nome_arquivo, desligado):
        pasta_pessoa = f"{nome_pasta_seguro(funcionario)} - {id_tag}"
        base_ativos = os.path.join(destino, PASTA_COLABORADORES, pasta_pessoa)
        base_deslig = os.path.join(destino, PASTA_DESLIGADOS, pasta_pessoa)
        if desligado:
            # se ja existe dossie em ATIVOS, move tudo para DESLIGADOS
            if os.path.isdir(base_ativos):
                os.makedirs(os.path.dirname(base_deslig), exist_ok=True)
                try:
                    if os.path.isdir(base_deslig):
                        for f in os.listdir(base_ativos):
                            shutil.move(os.path.join(base_ativos, f),
                                        os.path.join(base_deslig, f))
                        os.rmdir(base_ativos)
                    else:
                        shutil.move(base_ativos, base_deslig)
                    self.log(f"   Dossie de {funcionario} movido para DESLIGADOS.", "aviso")
                except Exception:
                    pass
            destino_dossie = base_deslig
        else:
            destino_dossie = base_ativos
        os.makedirs(destino_dossie, exist_ok=True)
        try:
            alvo = os.path.join(destino_dossie, nome_arquivo)
            if not os.path.exists(alvo):
                shutil.copy2(caminho_final, alvo)
        except Exception:
            pass

    def _para_revisar(self, origem_caminho, destino, nome_original):
        pasta = os.path.join(destino, PASTA_REVISAR)
        os.makedirs(pasta, exist_ok=True)
        try:
            alvo = os.path.join(pasta, nome_original)
            if not os.path.exists(alvo):
                shutil.copy2(origem_caminho, alvo)
        except Exception:
            pass

    def _remover_de_revisar(self, destino, nome_original):
        try:
            alvo = os.path.join(destino, PASTA_REVISAR, nome_original)
            if os.path.exists(alvo):
                os.remove(alvo)
        except Exception:
            pass

    def _registrar(self, original, novo, pasta, provedor, status, conf, convenia, erro):
        self.progresso.setdefault("origem", self.var_origem.get().strip())
        self.progresso.setdefault("processados", {})
        self.progresso["processados"][original] = {
            "novo_nome": novo, "pasta": pasta, "provedor": provedor, "status": status,
            "confianca": conf, "convenia": convenia, "erro": erro,
            "quando": datetime.now().isoformat(timespec="seconds"),
        }
        self._salvar_progresso()

    def _gerar_relatorio_execucao(self, destino):
        try:
            nome = f"relatorio_renomeacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            with open(os.path.join(destino, PASTA_RELATORIOS, nome), "w",
                      encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["ARQUIVO ORIGINAL", "NOVO NOME", "PASTA DESTINO", "PROVEDOR USADO",
                            "STATUS", "CONFIANCA", "CONVENIA"])
                for orig, i in self.progresso.get("processados", {}).items():
                    w.writerow([orig, i.get("novo_nome", ""), i.get("pasta", ""),
                                i.get("provedor", ""), i.get("status", ""),
                                i.get("confianca", ""), i.get("convenia", "")])
            self.log(f"Relatorio da execucao gerado: {nome}", "info")
        except Exception as e:  # noqa: BLE001
            self.log(f"Nao foi possivel gerar o relatorio: {e}", "erro")

    def _ao_terminar(self):
        self.btn_iniciar.config(state="normal")
        self.btn_reprocessar.config(state="normal")
        self.btn_testar.config(state="normal")
        self.btn_pausar.config(state="disabled", text="PAUSAR")
        self.btn_parar.config(state="disabled")
        self.fila.put(("countdown", ""))

    def _ao_fechar(self):
        if self.worker and self.worker.is_alive():
            if not messagebox.askyesno("Sair", "Ha processamento em andamento. Sair mesmo assim?"):
                return
            self.stop_event.set()
        self._liberar_lock()
        self._salvar_config()
        self.root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
