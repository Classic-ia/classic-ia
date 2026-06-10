# -*- coding: utf-8 -*-
"""
Renomeador RH PRO - Classic Couros - v5.0 (FASE 5)
==================================================
Sistema desktop para renomeacao automatica de documentos digitalizados do RH
usando IA, com fallback automatico entre provedores (Google Gemini -> Claude)
e integracao com a Convenia (cadastro de colaboradores + envio de documentos).

Recursos:
- Fallback Gemini 2.0 Flash (primario) -> Claude Haiku (reserva)
- Cadastro de colaboradores via planilha XLSX local OU API Convenia
- Correcao do nome do colaborador pelo cadastro ("Ler nomes antes")
- Organizacao por tipo e, opcionalmente, agrupada por colaborador
- Envio do documento renomeado para a Convenia (FASE 5)
- Modo teste (5 arquivos), reprocessamento de falhas, retomada de progresso
- Controle de rate limit, contagem regressiva e relatorio CSV

Interface: Tkinter (tema escuro) - apenas bibliotecas padrao do Python 3.8+.
Autor: Classic Importacao e Exportacao de Couros EIRELI
"""

import base64
import csv
import difflib
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
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

APP_VERSAO = "v5.0 - FASE 5"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, "config.json")
PROGRESSO_PATH = os.path.join(APP_DIR, "progresso.json")

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)
CLAUDE_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_VERSION = "2023-06-01"

# Convenia - mesma API usada em rh/convenia_integracao.html
CONVENIA_BASE = "https://public-api.convenia.com.br/api/v3"
CONVENIA_HEADER = "api-token"
# Endpoint de upload de documento por colaborador. Confirme o caminho exato
# na documentacao da Convenia; e o unico ponto a ajustar se mudar.
CONVENIA_DOC_ENDPOINT = "/employees/{id}/documents"

EXTENSOES_VALIDAS = (".pdf", ".jpg", ".jpeg", ".png")

TIPOS_VALIDOS = [
    "HOLERITE",
    "ATESTADO",
    "CARTAO PONTO",
    "13 SALARIO",
    "ACORDO DE COMPENSACAO",
    "ADIANTAMENTO",
    "PLANO DE SAUDE",
    "RECIBO",
    "CONTRATO",
    "ADMISSAO",
    "DOCUMENTO",
]

# Os nomes das pastas seguem o padrao das pastas existentes do RH (com acentos).
# Apenas os NOMES DOS ARQUIVOS sao normalizados sem acentos.
MAPA_PASTAS = {
    "HOLERITE": "HOLERITES",
    "HOLERITES": "HOLERITES",
    "ATESTADO": "ATESTADO",
    "ATESTADO MEDICO": "ATESTADO",
    "CARTAO PONTO": "CARTÃO PONTO",
    "PONTO": "CARTÃO PONTO",
    "13 SALARIO": "13 SALARIO",
    "ACORDO DE COMPENSACAO": "ACORDO DE COMPENSAÇÃO",
    "ADIANTAMENTO": "ADIANTAMENTOS",
    "PLANO DE SAUDE": "PLANO ALIANÇA",
    "RECIBO": "RECIBO",
    "CONTRATO": "DOCUMENTOS 2025",
    "ADMISSAO": "DOCUMENTOS 2025",
}
PASTA_PADRAO = "OUTROS"

GEMINI_INTERVALO_MIN = 10
JANELA_RETORNO_GEMINI = 300
ESPERA_429 = 10
MAX_FALHAS_GEMINI = 3
# Similaridade minima para casar um nome lido com o cadastro
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
}

# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------


def interruptible_sleep(seconds, stop_event):
    """Dorme em pequenos passos, abortando se stop_event for acionado."""
    fim = time.time() + seconds
    while time.time() < fim:
        if stop_event.is_set():
            return False
        time.sleep(0.1)
    return True


def remover_acentos(texto):
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalizar_nome(texto):
    """Remove acentos, maiusculas, sem caracteres especiais de nome de arquivo."""
    texto = remover_acentos(texto or "").upper()
    texto = re.sub(r"[^A-Z0-9 ]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def media_type_de(ext):
    ext = ext.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext == ".png":
        return "image/png"
    return "image/jpeg"


def extrair_json(texto):
    """Extrai o primeiro objeto JSON de uma string (tolerante a cercas ```)."""
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
    profundidade = 0
    for i in range(inicio, len(t)):
        if t[i] == "{":
            profundidade += 1
        elif t[i] == "}":
            profundidade -= 1
            if profundidade == 0:
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
    if "HOLERITE" in t or "PAGAMENTO" in t or "CONTRACHEQUE" in t:
        return "HOLERITE"
    if "ATESTADO" in t:
        return "ATESTADO"
    if "PONTO" in t or "CARTAO" in t:
        return "CARTAO PONTO"
    if ("13" in t and "SALARIO" in t) or "DECIMO TERCEIRO" in t:
        return "13 SALARIO"
    if "ACORDO" in t or "COMPENSACAO" in t:
        return "ACORDO DE COMPENSACAO"
    if "ADIANTAMENTO" in t or "VALE" in t:
        return "ADIANTAMENTO"
    if ("PLANO" in t and "SAUDE" in t) or "ALIANCA" in t:
        return "PLANO DE SAUDE"
    if "RECIBO" in t:
        return "RECIBO"
    if "CONTRATO" in t:
        return "CONTRATO"
    if "ADMISSAO" in t:
        return "ADMISSAO"
    return "DOCUMENTO"


def pasta_destino_de(tipo_canonico):
    return MAPA_PASTAS.get(tipo_canonico, PASTA_PADRAO)


def validar_data(data):
    """Garante formato YYYY-MM. Retorna a data valida ou 'SEM-DATA'."""
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


def nome_pasta_seguro(nome):
    """Nome de pasta seguro para o colaborador (sem barras/caracteres invalidos)."""
    n = normalizar_nome(nome) or "SEM NOME"
    return n[:80]


PROMPT_EXTRACAO = (
    "Voce e um assistente que analisa documentos de Recursos Humanos brasileiros.\n"
    "Analise o documento anexado e extraia as informacoes solicitadas.\n\n"
    "Responda APENAS com um objeto JSON valido (sem texto antes ou depois), "
    "exatamente neste formato:\n"
    "{\n"
    '  "nome": "NOME COMPLETO DO FUNCIONARIO",\n'
    '  "tipo": "TIPO DO DOCUMENTO",\n'
    '  "data": "AAAA-MM",\n'
    '  "confianca": "ALTA"\n'
    "}\n\n"
    "Regras:\n"
    "- nome: nome completo do funcionario, em MAIUSCULAS e SEM ACENTOS. Se nao "
    'encontrar, use "DESCONHECIDO".\n'
    "- tipo: classifique em UM destes tipos: " + ", ".join(TIPOS_VALIDOS) + ". "
    "Se nao se encaixar, use DOCUMENTO.\n"
    "- data: data de referencia no formato AAAA-MM (ano-mes). Para holerite use o "
    'mes de competencia. Se nao houver, use "SEM-DATA".\n'
    "- confianca: ALTA, MEDIA ou BAIXA, conforme sua certeza na leitura.\n"
)


# ---------------------------------------------------------------------------
# HTTP (urllib - sem dependencias)
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
    """POST multipart/form-data. arquivos = {campo: (nome, bytes, content_type)}."""
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
# Leitor de XLSX (apenas biblioteca padrao)
# ---------------------------------------------------------------------------

_NS_XLSX = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def ler_xlsx(caminho):
    """Le a primeira planilha de um .xlsx e devolve uma lista de linhas (listas)."""
    with zipfile.ZipFile(caminho) as z:
        nomes = z.namelist()
        compartilhadas = []
        if "xl/sharedStrings.xml" in nomes:
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall(f"{_NS_XLSX}si"):
                texto = "".join(t.text or "" for t in si.iter(f"{_NS_XLSX}t"))
                compartilhadas.append(texto)

        planilhas = sorted(
            n for n in nomes
            if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
        )
        if not planilhas:
            return []
        root = ET.fromstring(z.read(planilhas[0]))
        sheet_data = root.find(f"{_NS_XLSX}sheetData")
        linhas = []
        if sheet_data is None:
            return []
        for row in sheet_data.findall(f"{_NS_XLSX}row"):
            celulas = []
            for c in row.findall(f"{_NS_XLSX}c"):
                tipo = c.get("t")
                v = c.find(f"{_NS_XLSX}v")
                if tipo == "s" and v is not None:
                    try:
                        celulas.append(compartilhadas[int(v.text)])
                    except (ValueError, IndexError):
                        celulas.append("")
                elif tipo == "inlineStr":
                    is_el = c.find(f"{_NS_XLSX}is")
                    celulas.append(
                        "".join(x.text or "" for x in is_el.iter(f"{_NS_XLSX}t"))
                        if is_el is not None
                        else ""
                    )
                else:
                    celulas.append(v.text if v is not None else "")
            linhas.append(celulas)
        return linhas


# ---------------------------------------------------------------------------
# Cadastro de colaboradores
# ---------------------------------------------------------------------------


class Cadastro:
    """Lista de colaboradores para correcao de nomes e envio a Convenia."""

    def __init__(self):
        self.itens = []  # [{"id":..,"nome":..,"nome_norm":..,"cpf":..}]
        self.origem = ""  # "XLSX" | "CONVENIA" | ""

    def __len__(self):
        return len(self.itens)

    def _adicionar(self, nome, ident="", cpf=""):
        nome = (nome or "").strip()
        if not nome:
            return
        self.itens.append(
            {
                "id": str(ident or ""),
                "nome": nome,
                "nome_norm": normalizar_nome(nome),
                "cpf": re.sub(r"\D", "", str(cpf or "")),
            }
        )

    def carregar_xlsx(self, caminho):
        self.itens = []
        linhas = ler_xlsx(caminho)
        if not linhas:
            raise ValueError("Planilha vazia ou ilegivel.")
        cabecalho = [normalizar_nome(c) for c in linhas[0]]
        idx_nome = next(
            (i for i, h in enumerate(cabecalho) if "NOME" in h), 0
        )
        idx_cpf = next((i for i, h in enumerate(cabecalho) if "CPF" in h), None)
        idx_id = next(
            (i for i, h in enumerate(cabecalho) if h in ("ID", "MATRICULA", "CODIGO")),
            None,
        )
        # Se a primeira linha parece dado (sem "NOME" no cabecalho), inclui ela
        comeco = 1 if any("NOME" in h for h in cabecalho) else 0
        for linha in linhas[comeco:]:
            if idx_nome >= len(linha):
                continue
            nome = linha[idx_nome]
            cpf = linha[idx_cpf] if idx_cpf is not None and idx_cpf < len(linha) else ""
            ident = linha[idx_id] if idx_id is not None and idx_id < len(linha) else ""
            self._adicionar(nome, ident, cpf)
        self.origem = "XLSX"
        return len(self.itens)

    def carregar_convenia(self, client):
        self.itens = []
        for f in client.listar_funcionarios():
            nome = f.get("name") or f.get("nome") or f.get("full_name") or ""
            self._adicionar(nome, f.get("id"), f.get("cpf"))
        self.origem = "CONVENIA"
        return len(self.itens)

    def encontrar(self, nome):
        """Retorna o item de cadastro mais parecido com 'nome', ou None."""
        if not self.itens:
            return None
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


# ---------------------------------------------------------------------------
# Cliente Convenia
# ---------------------------------------------------------------------------


class ConveniaClient:
    def __init__(self, token):
        self.token = (token or "").strip()

    def _headers(self):
        return {CONVENIA_HEADER: self.token, "Accept": "application/json"}

    def validar(self):
        """Testa o token. Retorna (ok, mensagem)."""
        if not self.token:
            return False, "Token vazio."
        try:
            status, _ = _http_get(
                f"{CONVENIA_BASE}/employees?per_page=1", self._headers()
            )
            return (200 <= status < 300), f"HTTP {status}"
        except urllib.error.HTTPError as e:
            return False, f"HTTP {e.code}"
        except Exception as e:  # noqa: BLE001
            return False, str(e)

    def listar_funcionarios(self, max_paginas=20):
        todos = []
        pagina = 1
        total_paginas = 1
        while pagina <= total_paginas and pagina <= max_paginas:
            status, corpo = _http_get(
                f"{CONVENIA_BASE}/employees?page={pagina}&per_page=100",
                self._headers(),
            )
            if not (200 <= status < 300):
                raise ValueError(f"HTTP {status}")
            obj = json.loads(corpo)
            lista = obj.get("data") or obj.get("employees") or []
            if not isinstance(lista, list):
                break
            todos.extend(lista)
            meta = obj.get("meta") or {}
            total_paginas = meta.get("last_page") or meta.get("total_pages") or 1
            pagina += 1
        return todos

    def enviar_documento(self, employee_id, caminho, tipo):
        """Envia um documento para o colaborador. Retorna (ok, mensagem)."""
        if not self.token:
            return False, "Sem token."
        if not employee_id:
            return False, "Sem ID do colaborador."
        url = CONVENIA_BASE + CONVENIA_DOC_ENDPOINT.format(id=employee_id)
        with open(caminho, "rb") as f:
            dados = f.read()
        ext = os.path.splitext(caminho)[1].lower()
        ctype = media_type_de(ext)
        try:
            status, corpo = _http_post_multipart(
                url,
                campos={"name": os.path.basename(caminho), "type": tipo},
                arquivos={"file": (os.path.basename(caminho), dados, ctype)},
                headers={CONVENIA_HEADER: self.token, "Accept": "application/json"},
            )
            if 200 <= status < 300:
                return True, f"HTTP {status}"
            return False, f"HTTP {status}: {corpo[:160]}"
        except urllib.error.HTTPError as e:
            corpo = ""
            try:
                corpo = e.read().decode("utf-8", errors="replace")[:160]
            except Exception:
                pass
            return False, f"HTTP {e.code}: {corpo}"
        except Exception as e:  # noqa: BLE001
            return False, str(e)


class ResultadoIA:
    def __init__(self, ok, dados=None, erro=None, rate_limited=False):
        self.ok = ok
        self.dados = dados
        self.erro = erro
        self.rate_limited = rate_limited


class GerenciadorIA:
    """Orquestra Gemini (primario) e Claude (fallback)."""

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
        if (
            self.ativo == "CLAUDE"
            and self.gemini_key
            and self.trocou_para_claude_em
            and time.time() - self.trocou_para_claude_em >= JANELA_RETORNO_GEMINI
        ):
            self.ativo = "GEMINI"
            self.falhas_gemini = 0
            self.trocou_para_claude_em = None
            self.log("Voltando a tentar o Gemini (passaram 5 min).", "info")

    def _trocar_para_claude(self):
        if self.ativo != "CLAUDE":
            self.ativo = "CLAUDE"
            self.trocou_para_claude_em = time.time()
            if self.claude_key:
                self.log(
                    "Gemini falhou 3x seguidas -> trocando para Claude (fallback).",
                    "claude",
                )
            else:
                self.log(
                    "Gemini falhou 3x, mas nenhuma chave Claude foi informada.",
                    "aviso",
                )

    def _chamar_gemini(self, b64, media_type):
        url = GEMINI_URL.format(key=self.gemini_key)
        payload = {
            "contents": [
                {
                    "parts": [
                        {"inline_data": {"mime_type": media_type, "data": b64}},
                        {"text": PROMPT_EXTRACAO},
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "response_mime_type": "application/json",
            },
        }
        _, corpo = _http_post_json(url, payload, headers={})
        obj = json.loads(corpo)
        try:
            return obj["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            raise ValueError("Resposta do Gemini sem conteudo de texto.")

    def _chamar_claude(self, b64, media_type, ext):
        if ext.lower() == ".pdf":
            bloco = {
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": b64},
            }
        else:
            bloco = {
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": b64},
            }
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 512,
            "messages": [
                {"role": "user", "content": [bloco, {"type": "text", "text": PROMPT_EXTRACAO}]}
            ],
        }
        headers = {"x-api-key": self.claude_key, "anthropic-version": CLAUDE_VERSION}
        _, corpo = _http_post_json(CLAUDE_URL, payload, headers=headers)
        obj = json.loads(corpo)
        try:
            return "".join(
                p.get("text", "") for p in obj["content"] if p.get("type") == "text"
            )
        except (KeyError, TypeError):
            raise ValueError("Resposta do Claude sem conteudo de texto.")

    def _tentar_gemini(self, b64, media_type):
        for tentativa in range(2):
            try:
                texto = self._chamar_gemini(b64, media_type)
                dados = extrair_json(texto)
                if not dados:
                    return ResultadoIA(False, erro="Gemini: JSON invalido na resposta.")
                return ResultadoIA(True, dados=dados)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    self._ativar_rate_limit()
                    if tentativa == 0:
                        self.log(
                            f"Gemini atingiu o limite (429). Aguardando "
                            f"{ESPERA_429}s e tentando novamente...",
                            "aviso",
                        )
                        if not interruptible_sleep(ESPERA_429, self.stop_event):
                            return ResultadoIA(False, erro="Interrompido.", rate_limited=True)
                        continue
                    return ResultadoIA(
                        False, erro="Gemini: limite (429) persistente.", rate_limited=True
                    )
                corpo = ""
                try:
                    corpo = e.read().decode("utf-8", errors="replace")[:200]
                except Exception:
                    pass
                return ResultadoIA(False, erro=f"Gemini HTTP {e.code}: {corpo}")
            except Exception as e:  # noqa: BLE001
                return ResultadoIA(False, erro=f"Gemini: {e}")
        return ResultadoIA(False, erro="Gemini: falha desconhecida.", rate_limited=True)

    def _tentar_claude(self, b64, media_type, ext):
        if not self.claude_key:
            return ResultadoIA(False, erro="Sem chave Claude configurada.")
        try:
            texto = self._chamar_claude(b64, media_type, ext)
            dados = extrair_json(texto)
            if not dados:
                return ResultadoIA(False, erro="Claude: JSON invalido na resposta.")
            return ResultadoIA(True, dados=dados)
        except urllib.error.HTTPError as e:
            corpo = ""
            try:
                corpo = e.read().decode("utf-8", errors="replace")[:200]
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
            self.log(
                f"Falha no Gemini ({self.falhas_gemini}/{MAX_FALHAS_GEMINI}): {res.erro}",
                "aviso",
            )
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


# ---------------------------------------------------------------------------
# Interface grafica
# ---------------------------------------------------------------------------


class App:
    def __init__(self, root):
        self.root = root
        self.root.title(f"Renomeador RH PRO - {APP_VERSAO}")
        self.root.configure(bg=COR_BG)
        self.root.geometry("1080x820")
        self.root.minsize(940, 760)

        self.fila = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker = None
        self.progresso = {"origem": "", "processados": {}}
        self.cadastro = Cadastro()

        self._construir_estilo()
        self._construir_ui()
        self._carregar_config()

        self.root.protocol("WM_DELETE_WINDOW", self._ao_fechar)
        self.root.after(120, self._processar_fila)

    def _construir_estilo(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure(
            "Dark.Horizontal.TProgressbar",
            troughcolor=COR_SURFACE2,
            background=COR_AZUL,
            bordercolor=COR_SURFACE2,
            lightcolor=COR_AZUL,
            darkcolor=COR_AZUL,
        )

    def _label(self, parent, texto, **kw):
        return tk.Label(
            parent, text=texto, bg=kw.pop("bg", COR_BG), fg=kw.pop("fg", COR_FG), **kw
        )

    def _construir_ui(self):
        cab = tk.Frame(self.root, bg=COR_SURFACE)
        cab.pack(fill="x")
        tk.Label(
            cab,
            text=f"  RENOMEADOR RH PRO  -  {APP_VERSAO}",
            bg=COR_SURFACE,
            fg=COR_AZUL,
            font=("Segoe UI", 16, "bold"),
        ).pack(side="left", pady=10)
        tk.Label(
            cab,
            text="Organizacao com IA  -  Integracao Convenia API   ",
            bg=COR_SURFACE,
            fg=COR_FG_DIM,
            font=("Segoe UI", 9),
        ).pack(side="right", pady=12)

        corpo = tk.Frame(self.root, bg=COR_BG)
        corpo.pack(fill="both", expand=True, padx=14, pady=10)

        cfg = tk.LabelFrame(
            corpo,
            text=" Configuracao ",
            bg=COR_BG,
            fg=COR_FG,
            font=("Segoe UI", 10, "bold"),
            labelanchor="nw",
            bd=1,
            relief="solid",
        )
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

        # Token Convenia + Validar
        self._label(cfg, "Token Convenia:").grid(row=2, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(
            cfg, textvariable=self.var_convenia, show="*", bg=COR_SURFACE2, fg=COR_FG,
            insertbackground=COR_FG, relief="flat",
        ).grid(row=2, column=1, sticky="we", padx=10, pady=4, ipady=4)
        self.btn_validar = tk.Button(
            cfg, text="Validar", command=self.validar_convenia, bg=COR_AZUL, fg="white",
            activebackground=COR_AZUL, relief="flat", padx=14,
        )
        self.btn_validar.grid(row=2, column=2, sticky="e", padx=10, pady=4)

        self._campo_pasta(cfg, "Pasta origem:", self.var_origem, 3)
        self._campo_pasta(cfg, "Pasta destino:", self.var_destino, 4)

        # Cadastro + XLSX/API
        self._label(cfg, "Cadastro:").grid(row=5, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(
            cfg, textvariable=self.var_cadastro, bg=COR_SURFACE2, fg=COR_FG,
            insertbackground=COR_FG, relief="flat",
        ).grid(row=5, column=1, sticky="we", padx=10, pady=4, ipady=4)
        cad_btns = tk.Frame(cfg, bg=COR_BG)
        cad_btns.grid(row=5, column=2, sticky="e", padx=10, pady=4)
        tk.Button(
            cad_btns, text="XLSX", command=self.carregar_cadastro_xlsx, bg=COR_SURFACE2,
            fg=COR_FG, activebackground=COR_BORDA, relief="flat", padx=10,
        ).pack(side="left", padx=(0, 6))
        tk.Button(
            cad_btns, text="API", command=self.carregar_cadastro_api, bg=COR_SURFACE2,
            fg=COR_FG, activebackground=COR_BORDA, relief="flat", padx=10,
        ).pack(side="left")

        # Slider
        self._label(cfg, "Intervalo (s):").grid(row=6, column=0, sticky="w", padx=10, pady=(8, 4))
        sld = tk.Frame(cfg, bg=COR_BG)
        sld.grid(row=6, column=1, columnspan=2, sticky="we", padx=10, pady=(8, 4))
        self.slider = tk.Scale(
            sld, from_=6, to=30, orient="horizontal", variable=self.var_intervalo,
            bg=COR_BG, fg=COR_FG, highlightthickness=0, troughcolor=COR_SURFACE2,
            activebackground=COR_AZUL, length=360, showvalue=False,
            command=lambda _v: self._atualizar_label_intervalo(),
        )
        self.slider.pack(side="left", fill="x", expand=True)
        self.lbl_intervalo = self._label(sld, "10s", fg=COR_AZUL)
        self.lbl_intervalo.pack(side="left", padx=12)

        # Checkboxes
        chks = tk.Frame(cfg, bg=COR_BG)
        chks.grid(row=7, column=0, columnspan=3, sticky="w", padx=8, pady=(2, 10))
        self._check(chks, "Mover (nao copiar)", self.var_mover)
        self._check(chks, "Modo teste (5 arquivos)", self.var_teste)
        self._check(chks, "Ler nomes pelo cadastro (gratuito)", self.var_ler_nomes)
        self._check(chks, "Agrupar por colaborador", self.var_agrupar)
        self._check(chks, "Enviar para Convenia (FASE 5)", self.var_enviar_convenia)

        # Botoes
        botoes = tk.Frame(corpo, bg=COR_BG)
        botoes.pack(fill="x", pady=(12, 6))
        self.btn_iniciar = self._botao(botoes, "INICIAR", COR_VERDE, self.iniciar)
        self.btn_pausar = self._botao(botoes, "PAUSAR", COR_LARANJA, self.pausar_retomar)
        self.btn_parar = self._botao(botoes, "PARAR", COR_VERMELHO, self.parar)
        self.btn_iniciar.pack(side="left", padx=(0, 8))
        self.btn_pausar.pack(side="left", padx=8)
        self.btn_parar.pack(side="left", padx=8)
        self.btn_reprocessar = tk.Button(
            botoes, text="Reprocessar falhas", command=self.reprocessar, bg=COR_SURFACE2,
            fg=COR_FG, activebackground=COR_BORDA, relief="flat", padx=16, pady=8,
        )
        self.btn_reprocessar.pack(side="right")
        self.btn_pausar.config(state="disabled")
        self.btn_parar.config(state="disabled")

        # Indicadores
        ind = tk.Frame(corpo, bg=COR_BG)
        ind.pack(fill="x", pady=(8, 4))
        self._label(ind, "Progresso de Processamento", fg=COR_AZUL,
                    font=("Segoe UI", 10, "bold")).pack(anchor="w")
        prog = tk.Frame(ind, bg=COR_BG)
        prog.pack(fill="x", pady=(6, 4))
        self.barra = ttk.Progressbar(
            prog, style="Dark.Horizontal.TProgressbar", maximum=100, length=600
        )
        self.barra.pack(side="left", fill="x", expand=True)
        self.lbl_percent = self._label(prog, "0%", fg=COR_FG)
        self.lbl_percent.pack(side="left", padx=10)

        linha = tk.Frame(ind, bg=COR_BG)
        linha.pack(fill="x")
        self.lbl_status = self._label(linha, "Pronto. Preencha a configuracao e clique em INICIAR.", fg=COR_FG_DIM)
        self.lbl_status.pack(side="left")
        self.lbl_provedor = self._label(linha, "", fg=COR_FG_DIM)
        self.lbl_provedor.pack(side="left", padx=16)
        self.lbl_countdown = self._label(linha, "", fg=COR_LARANJA)
        self.lbl_countdown.pack(side="right")

        # Log
        logf = tk.LabelFrame(
            corpo, text=" Registro de Atividades ", bg=COR_BG, fg=COR_FG,
            font=("Segoe UI", 10, "bold"), bd=1, relief="solid",
        )
        logf.pack(fill="both", expand=True, pady=(8, 0))
        self.log_text = tk.Text(
            logf, bg="#15161f", fg=COR_FG, insertbackground=COR_FG, wrap="word",
            height=12, relief="flat", font=("Consolas", 9),
        )
        scroll = tk.Scrollbar(logf, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.log_text.pack(side="left", fill="both", expand=True, padx=4, pady=4)
        for nivel, cor in LOG_CORES.items():
            self.log_text.tag_config(nivel, foreground=cor)
        self.log_text.config(state="disabled")

    def _campo_senha(self, parent, rotulo, var, row):
        self._label(parent, rotulo).grid(row=row, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(
            parent, textvariable=var, show="*", bg=COR_SURFACE2, fg=COR_FG,
            insertbackground=COR_FG, relief="flat",
        ).grid(row=row, column=1, columnspan=2, sticky="we", padx=10, pady=4, ipady=4)

    def _campo_pasta(self, parent, rotulo, var, row):
        self._label(parent, rotulo).grid(row=row, column=0, sticky="w", padx=10, pady=4)
        tk.Entry(
            parent, textvariable=var, bg=COR_SURFACE2, fg=COR_FG,
            insertbackground=COR_FG, relief="flat",
        ).grid(row=row, column=1, sticky="we", padx=10, pady=4, ipady=4)
        tk.Button(
            parent, text="Procurar", command=lambda: self._escolher_pasta(var),
            bg=COR_SURFACE2, fg=COR_FG, activebackground=COR_BORDA, relief="flat", padx=10,
        ).grid(row=row, column=2, sticky="e", padx=10, pady=4)

    def _check(self, parent, texto, var):
        tk.Checkbutton(
            parent, text=texto, variable=var, bg=COR_BG, fg=COR_FG,
            selectcolor=COR_SURFACE2, activebackground=COR_BG, activeforeground=COR_FG,
            anchor="w",
        ).pack(side="top", anchor="w", padx=4)

    def _botao(self, parent, texto, cor, comando):
        return tk.Button(
            parent, text=texto, command=comando, bg=cor, fg="#0b0b12",
            activebackground=cor, activeforeground="#0b0b12",
            font=("Segoe UI", 11, "bold"), relief="flat", padx=24, pady=8, cursor="hand2",
        )

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
                tipo = item[0]
                if tipo == "log":
                    self._escrever_log(item[1], item[2])
                elif tipo == "status":
                    self.lbl_status.config(text=item[1])
                elif tipo == "provedor":
                    cor = LOG_CORES["claude"] if item[1] == "CLAUDE" else COR_AZUL
                    self.lbl_provedor.config(text=f"Provedor: {item[1]}", fg=cor)
                elif tipo == "progresso":
                    pct = item[1]
                    self.barra["value"] = pct
                    self.lbl_percent.config(text=f"{int(pct)}%")
                    if item[2]:
                        self.lbl_status.config(text=item[2])
                elif tipo == "countdown":
                    self.lbl_countdown.config(text=item[1])
                elif tipo == "fim":
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
                self.var_gemini.set(c.get("gemini_key", ""))
                self.var_claude.set(c.get("claude_key", ""))
                self.var_convenia.set(c.get("convenia_token", ""))
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

    def _salvar_config(self):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "gemini_key": self.var_gemini.get().strip(),
                        "claude_key": self.var_claude.get().strip(),
                        "convenia_token": self.var_convenia.get().strip(),
                        "origem": self.var_origem.get().strip(),
                        "destino": self.var_destino.get().strip(),
                        "cadastro": self.var_cadastro.get().strip(),
                        "intervalo": self.var_intervalo.get(),
                        "mover": self.var_mover.get(),
                        "teste": self.var_teste.get(),
                        "ler_nomes": self.var_ler_nomes.get(),
                        "agrupar": self.var_agrupar.get(),
                        "enviar_convenia": self.var_enviar_convenia.get(),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
        except Exception:
            pass

    def _carregar_progresso(self):
        if os.path.exists(PROGRESSO_PATH):
            try:
                with open(PROGRESSO_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {"origem": "", "processados": {}}
        return {"origem": "", "processados": {}}

    def _salvar_progresso(self):
        try:
            with open(PROGRESSO_PATH, "w", encoding="utf-8") as f:
                json.dump(self.progresso, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ----- Convenia / cadastro (rodam em thread para nao travar a UI) -----
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
            if ok:
                self.log("Token Convenia validado com sucesso.", "convenia")
            else:
                self.log(f"Falha ao validar token Convenia: {msg}", "erro")
            self.fila.put(("countdown", ""))
            self.root.after(0, lambda: self.btn_validar.config(state="normal", text="Validar"))

        threading.Thread(target=tarefa, daemon=True).start()

    def carregar_cadastro_xlsx(self):
        caminho = filedialog.askopenfilename(
            title="Selecione a planilha de colaboradores",
            filetypes=[("Planilha Excel", "*.xlsx")],
        )
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
        origem = self.var_origem.get().strip()
        destino = self.var_destino.get().strip()

        if not gemini and not self.var_claude.get().strip():
            messagebox.showerror("Falta a chave", "Informe a Chave Gemini (ou Claude).")
            return
        if not origem or not os.path.isdir(origem):
            messagebox.showerror("Pasta invalida", "Selecione uma pasta de ORIGEM valida.")
            return
        if not destino:
            messagebox.showerror("Pasta invalida", "Selecione a pasta DESTINO (raiz do RH).")
            return
        if self.var_enviar_convenia.get():
            if not self.var_convenia.get().strip():
                messagebox.showerror("Convenia", "Para enviar a Convenia, informe o Token.")
                return
            if self.cadastro.origem != "CONVENIA":
                if not messagebox.askyesno(
                    "Convenia",
                    "O envio para a Convenia precisa do cadastro carregado pela API "
                    "(para localizar o ID de cada colaborador).\n\n"
                    "Continuar mesmo assim? Os envios sem ID serao ignorados.",
                ):
                    return

        self._salvar_config()

        arquivos = sorted(
            f for f in os.listdir(origem)
            if os.path.isfile(os.path.join(origem, f))
            and f.lower().endswith(EXTENSOES_VALIDAS)
        )
        if not arquivos:
            messagebox.showinfo("Nada a fazer", "Nenhum PDF/JPG/PNG na pasta de origem.")
            return

        self.progresso = self._carregar_progresso()
        if self.progresso.get("origem") != origem:
            self.progresso = {"origem": origem, "processados": {}}

        if reprocessar:
            alvos = [
                nome for nome in arquivos
                if self.progresso["processados"].get(nome, {}).get("status") == "ERRO"
                or self.progresso["processados"].get(nome, {}).get("confianca") == "BAIXA"
            ]
            if not alvos:
                messagebox.showinfo("Reprocessar", "Nao ha falhas nem leituras BAIXA para reprocessar.")
                return
            # limpa o registro dos alvos para forcar reprocessamento
            for nome in alvos:
                self.progresso["processados"].pop(nome, None)
            arquivos = alvos
            self.log(f"Reprocessando {len(alvos)} arquivo(s) com falha/baixa confianca.", "aviso")
        else:
            ja_ok = [
                nome for nome, info in self.progresso.get("processados", {}).items()
                if info.get("status") == "SUCESSO" and nome in arquivos
            ]
            if ja_ok:
                resp = messagebox.askyesno(
                    "Retomar progresso",
                    f"Encontrei {len(ja_ok)} arquivo(s) ja processado(s).\n\n"
                    "Deseja continuar de onde parou?\n\n"
                    "Sim = pular os ja feitos\nNao = recomecar tudo do zero",
                )
                if not resp:
                    self.progresso = {"origem": origem, "processados": {}}
                    self._salvar_progresso()

        if self.var_teste.get():
            arquivos = arquivos[:5]
            self.log("Modo teste: processando no maximo 5 arquivos.", "aviso")

        self.stop_event.clear()
        self.pause_event.clear()
        self.btn_iniciar.config(state="disabled")
        self.btn_reprocessar.config(state="disabled")
        self.btn_pausar.config(state="normal", text="PAUSAR")
        self.btn_parar.config(state="normal")
        self._limpar_log()

        self.worker = threading.Thread(
            target=self._executar, args=(arquivos, origem, destino), daemon=True
        )
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
            self._loop_processamento(arquivos, origem, destino)
        except Exception as e:  # noqa: BLE001
            self.log(f"Erro inesperado: {e}", "erro")
        finally:
            self.fila.put(("fim",))

    def _esperar_se_pausado(self):
        while self.pause_event.is_set() and not self.stop_event.is_set():
            self.fila.put(("countdown", "PAUSADO"))
            time.sleep(0.2)

    def _loop_processamento(self, arquivos, origem, destino):
        gemini = self.var_gemini.get().strip()
        claude = self.var_claude.get().strip()
        gerente = GerenciadorIA(gemini, claude, self.stop_event, self.log)
        enviar_convenia = self.var_enviar_convenia.get()
        agrupar = self.var_agrupar.get()
        usar_cadastro = self.var_ler_nomes.get() and len(self.cadastro) > 0
        convenia = ConveniaClient(self.var_convenia.get().strip()) if enviar_convenia else None

        total = len(arquivos)
        os.makedirs(destino, exist_ok=True)
        self.log(f"Iniciando. {total} arquivo(s) a processar.", "info")
        if usar_cadastro:
            self.log(f"Correcao de nomes pelo cadastro ATIVA ({len(self.cadastro)} colaboradores).", "info")
        self.fila.put(("provedor", gerente.ativo))

        feitos = 0
        for idx, nome in enumerate(arquivos, start=1):
            if self.stop_event.is_set():
                break
            self._esperar_se_pausado()
            if self.stop_event.is_set():
                break

            info_prev = self.progresso["processados"].get(nome)
            if info_prev and info_prev.get("status") == "SUCESSO":
                self.log(f"[{idx}/{total}] {nome}: ja processado, pulando.", "info")
                feitos += 1
                self._atualizar_barra(feitos, total)
                continue

            self.fila.put(("status", f"[{idx}/{total}] Lendo: {nome}"))
            caminho = os.path.join(origem, nome)
            ext = os.path.splitext(nome)[1].lower()

            try:
                with open(caminho, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode("ascii")
            except Exception as e:  # noqa: BLE001
                self.log(f"[{idx}/{total}] Erro ao ler {nome}: {e}", "erro")
                self._registrar(nome, "", "", "-", "ERRO", "-", "-", f"leitura: {e}")
                continue

            media = media_type_de(ext)
            dados, provedor = gerente.extrair(b64, media, ext)
            self.fila.put(("provedor", gerente.ativo))

            if not dados:
                erro = gerente._ultimo_erro or "falha na IA"
                self.log(f"[{idx}/{total}] {nome}: FALHOU ({erro}).", "erro")
                self._registrar(nome, "", "", provedor, "ERRO", "-", "-", str(erro))
                self._contagem_regressiva(gerente, idx, total)
                continue

            funcionario = normalizar_nome(dados.get("nome")) or "DESCONHECIDO"
            tipo = canonizar_tipo(dados.get("tipo"))
            data_ref = validar_data(dados.get("data"))
            confianca = (dados.get("confianca") or "MEDIA").upper()
            if confianca not in ("ALTA", "MEDIA", "BAIXA"):
                confianca = "MEDIA"

            # Correcao do nome pelo cadastro
            colaborador_id = ""
            if usar_cadastro:
                match = self.cadastro.encontrar(funcionario)
                if match:
                    oficial = normalizar_nome(match["nome"])
                    if oficial != funcionario:
                        self.log(f"   Nome ajustado pelo cadastro: {funcionario} -> {oficial}", "info")
                    funcionario = oficial
                    colaborador_id = match.get("id", "")
                else:
                    self.log(f"   Nome '{funcionario}' nao encontrado no cadastro.", "aviso")

            pasta_tipo = pasta_destino_de(tipo)
            if agrupar:
                pasta_rel = os.path.join(nome_pasta_seguro(funcionario), pasta_tipo)
            else:
                pasta_rel = pasta_tipo

            novo_base = f"{funcionario}_{tipo}_{data_ref}"
            novo_nome, caminho_final = self._mover_ou_copiar(
                caminho, ext, destino, pasta_rel, novo_base
            )

            self.log(
                f"[{idx}/{total}] {nome} -> {pasta_rel}/{novo_nome} "
                f"[{provedor}, conf. {confianca}]",
                "sucesso",
            )

            # Envio para a Convenia
            envio = "-"
            if enviar_convenia and convenia and caminho_final:
                if colaborador_id:
                    ok, msg = convenia.enviar_documento(colaborador_id, caminho_final, tipo)
                    envio = "ENVIADO" if ok else f"FALHA"
                    self.log(
                        f"   Convenia: {'documento enviado' if ok else 'falha no envio'} ({msg}).",
                        "convenia" if ok else "erro",
                    )
                else:
                    envio = "SEM ID"
                    self.log("   Convenia: colaborador sem ID no cadastro, envio ignorado.", "aviso")

            self._registrar(nome, novo_nome, pasta_rel, provedor, "SUCESSO", confianca, envio, "")
            feitos += 1
            self._atualizar_barra(feitos, total)

            if idx < total and not self.stop_event.is_set():
                self._contagem_regressiva(gerente, idx, total)

        self.fila.put(("countdown", ""))
        self._gerar_relatorio(destino)
        if self.stop_event.is_set():
            self.log("Processamento PARADO pelo usuario.", "aviso")
        else:
            self.log("Processamento CONCLUIDO!", "sucesso")
        self._atualizar_barra(feitos, total)

    def _atualizar_barra(self, feitos, total):
        pct = (feitos / total * 100) if total else 0
        self.fila.put(("progresso", pct, None))

    def _contagem_regressiva(self, gerente, idx, total):
        base = max(6, int(self.var_intervalo.get()))
        intervalo = base
        if gerente.ativo == "GEMINI":
            intervalo = max(intervalo, GEMINI_INTERVALO_MIN)
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

    def _mover_ou_copiar(self, origem_caminho, ext, destino_raiz, pasta_rel, novo_base):
        pasta_destino = os.path.join(destino_raiz, pasta_rel)
        os.makedirs(pasta_destino, exist_ok=True)
        nome_final = f"{novo_base}{ext}"
        alvo = os.path.join(pasta_destino, nome_final)
        contador = 1
        while os.path.exists(alvo):
            nome_final = f"{novo_base}_{contador}{ext}"
            alvo = os.path.join(pasta_destino, nome_final)
            contador += 1
        try:
            if self.var_mover.get():
                shutil.move(origem_caminho, alvo)
            else:
                shutil.copy2(origem_caminho, alvo)
        except Exception as e:  # noqa: BLE001
            self.log(f"Erro ao mover/copiar: {e}", "erro")
            return nome_final, ""
        return nome_final, alvo

    def _registrar(self, original, novo, pasta, provedor, status, confianca, convenia, erro):
        self.progresso.setdefault("origem", self.var_origem.get().strip())
        self.progresso.setdefault("processados", {})
        self.progresso["processados"][original] = {
            "novo_nome": novo,
            "pasta": pasta,
            "provedor": provedor,
            "status": status,
            "confianca": confianca,
            "convenia": convenia,
            "erro": erro,
            "quando": datetime.now().isoformat(timespec="seconds"),
        }
        self._salvar_progresso()

    def _gerar_relatorio(self, destino):
        try:
            nome = f"relatorio_renomeacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            caminho = os.path.join(destino, nome)
            with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(
                    ["ARQUIVO ORIGINAL", "NOVO NOME", "PASTA DESTINO", "PROVEDOR USADO",
                     "STATUS", "CONFIANCA", "CONVENIA"]
                )
                for original, info in self.progresso.get("processados", {}).items():
                    w.writerow(
                        [
                            original,
                            info.get("novo_nome", ""),
                            info.get("pasta", ""),
                            info.get("provedor", ""),
                            info.get("status", ""),
                            info.get("confianca", ""),
                            info.get("convenia", ""),
                        ]
                    )
            self.log(f"Relatorio gerado: {nome}", "info")
        except Exception as e:  # noqa: BLE001
            self.log(f"Nao foi possivel gerar o relatorio: {e}", "erro")

    def _ao_terminar(self):
        self.btn_iniciar.config(state="normal")
        self.btn_reprocessar.config(state="normal")
        self.btn_pausar.config(state="disabled", text="PAUSAR")
        self.btn_parar.config(state="disabled")
        self.fila.put(("countdown", ""))

    def _ao_fechar(self):
        if self.worker and self.worker.is_alive():
            if not messagebox.askyesno("Sair", "Ha processamento em andamento. Sair mesmo assim?"):
                return
            self.stop_event.set()
        self._salvar_config()
        self.root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
