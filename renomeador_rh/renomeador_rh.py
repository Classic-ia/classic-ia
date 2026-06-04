# -*- coding: utf-8 -*-
"""
Renomeador de Documentos RH - Classic Couros
=============================================
Sistema desktop para renomeacao automatica de documentos digitalizados do RH
usando IA, com fallback automatico entre provedores (Google Gemini -> Claude).

- Interface: Tkinter (tema escuro) - roda sem instalar dependencias extras
- Estado local em JSON (sem banco de dados)
- Apenas bibliotecas padrao do Python 3.8+

Autor: Classic Importacao e Exportacao de Couros EIRELI
"""

import base64
import csv
import json
import os
import queue
import re
import shutil
import threading
import time
import unicodedata
import webbrowser
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

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

EXTENSOES_VALIDAS = (".pdf", ".jpg", ".jpeg", ".png")

# Tipos reconhecidos (usados no nome do arquivo - sem acentos, maiusculas)
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

# Mapeamento: tipo reconhecido -> pasta de destino.
# Os nomes das pastas seguem exatamente o padrao das pastas existentes do RH
# (com acentos). Apenas os NOMES DOS ARQUIVOS sao normalizados sem acentos.
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

# Intervalo minimo (segundos) para nao estourar o plano gratuito do Gemini
GEMINI_INTERVALO_MIN = 10
# A cada 5 minutos tenta voltar para o Gemini
JANELA_RETORNO_GEMINI = 300
# Espera apos um 429 antes de tentar Gemini novamente
ESPERA_429 = 10
# Falhas seguidas do Gemini antes de trocar para Claude
MAX_FALHAS_GEMINI = 3

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

# Cores do log
LOG_CORES = {
    "sucesso": "#4ade80",
    "erro": "#f87171",
    "aviso": "#facc15",
    "info": "#60a5fa",
    "claude": "#c084fc",
}

# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------


def interruptible_sleep(seconds, stop_event):
    """Dorme em pequenos passos, abortando se stop_event for acionado.

    Retorna True se completou o tempo, False se foi interrompido."""
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
    # Substitui qualquer coisa que nao seja letra/numero/espaco por espaco
    texto = re.sub(r"[^A-Z0-9 ]+", " ", texto)
    # Colapsa espacos
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def media_type_de(ext):
    ext = ext.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext == ".png":
        return "image/png"
    return "image/jpeg"  # .jpg / .jpeg


def extrair_json(texto):
    """Extrai o primeiro objeto JSON de uma string (tolerante a cercas ```)."""
    if not texto:
        return None
    t = texto.strip()
    t = re.sub(r"^```(?:json)?", "", t).strip()
    t = re.sub(r"```$", "", t).strip()
    # Tenta direto
    try:
        return json.loads(t)
    except Exception:
        pass
    # Procura o primeiro { ... } balanceado
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
    """Converte o tipo lido pela IA para um dos tipos reconhecidos."""
    t = normalizar_nome(tipo)
    if not t:
        return "DOCUMENTO"
    if t in TIPOS_VALIDOS:
        return t
    # heuristicas de aproximacao
    if "HOLERITE" in t or "PAGAMENTO" in t or "CONTRACHEQUE" in t:
        return "HOLERITE"
    if "ATESTADO" in t:
        return "ATESTADO"
    if "PONTO" in t or "CARTAO" in t:
        return "CARTAO PONTO"
    if "13" in t and "SALARIO" in t or "DECIMO TERCEIRO" in t:
        return "13 SALARIO"
    if "ACORDO" in t or "COMPENSACAO" in t:
        return "ACORDO DE COMPENSACAO"
    if "ADIANTAMENTO" in t or "VALE" in t:
        return "ADIANTAMENTO"
    if "PLANO" in t and "SAUDE" in t or "ALIANCA" in t:
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
    # Formato AAAA-MM (ano primeiro)
    m = re.search(r"(\d{4})[-/ .](\d{1,2})\b", s)
    if m:
        ano, mes = m.group(1), int(m.group(2))
        if 1 <= mes <= 12:
            return f"{ano}-{mes:02d}"
    # Formato MM/AAAA (mes primeiro)
    m = re.search(r"\b(\d{1,2})[-/ .](\d{4})\b", s)
    if m:
        mes, ano = int(m.group(1)), m.group(2)
        if 1 <= mes <= 12:
            return f"{ano}-{mes:02d}"
    # Talvez so o ano
    m = re.search(r"(\d{4})", s)
    if m:
        return m.group(1)
    return "SEM-DATA"


PROMPT_EXTRACAO = (
    "Voce e um assistente que analisa documentos de Recursos Humanos brasileiros.\n"
    "Analise o documento anexado e extraia as informacoes solicitadas.\n\n"
    "Responda APENAS com um objeto JSON valido (sem texto antes ou depois, sem "
    "comentarios), exatamente neste formato:\n"
    "{\n"
    '  "nome": "NOME COMPLETO DO FUNCIONARIO",\n'
    '  "tipo": "TIPO DO DOCUMENTO",\n'
    '  "data": "AAAA-MM",\n'
    '  "confianca": "ALTA"\n'
    "}\n\n"
    "Regras:\n"
    "- nome: nome completo do funcionario, em MAIUSCULAS e SEM ACENTOS. Se nao "
    "encontrar, use \"DESCONHECIDO\".\n"
    "- tipo: classifique em UM destes tipos: " + ", ".join(TIPOS_VALIDOS) + ". "
    "Se nao se encaixar, use DOCUMENTO.\n"
    "- data: a data de referencia do documento no formato AAAA-MM (ano-mes). "
    "Para holerite use o mes de competencia. Se nao houver data, use \"SEM-DATA\".\n"
    "- confianca: ALTA, MEDIA ou BAIXA, conforme sua certeza na leitura.\n"
)


# ---------------------------------------------------------------------------
# Camada de IA (HTTP via urllib - sem dependencias)
# ---------------------------------------------------------------------------

import urllib.error
import urllib.request


class ResultadoIA:
    def __init__(self, ok, dados=None, erro=None, rate_limited=False):
        self.ok = ok
        self.dados = dados
        self.erro = erro
        self.rate_limited = rate_limited


def _http_post_json(url, payload, headers, timeout=120):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        corpo = resp.read().decode("utf-8", errors="replace")
        return resp.status, corpo


class GerenciadorIA:
    """Orquestra Gemini (primario) e Claude (fallback) com a logica de fallback."""

    def __init__(self, gemini_key, claude_key, stop_event, log_fn):
        self.gemini_key = (gemini_key or "").strip()
        self.claude_key = (claude_key or "").strip()
        self.stop_event = stop_event
        self.log = log_fn

        self.ativo = "GEMINI" if self.gemini_key else "CLAUDE"
        self.falhas_gemini = 0
        self.trocou_para_claude_em = None
        self.rate_limit_ate = 0  # timestamp ate quando aumentar o intervalo

    # --- helpers de estado ---
    def em_rate_limit(self):
        return time.time() < self.rate_limit_ate

    def _ativar_rate_limit(self):
        self.rate_limit_ate = time.time() + JANELA_RETORNO_GEMINI

    def talvez_voltar_gemini(self):
        """A cada 5 minutos tenta voltar automaticamente para o Gemini."""
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

    # --- chamadas aos provedores ---
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
        status, corpo = _http_post_json(url, payload, headers={})
        obj = json.loads(corpo)
        try:
            texto = obj["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            raise ValueError("Resposta do Gemini sem conteudo de texto.")
        return texto

    def _chamar_claude(self, b64, media_type, ext):
        if ext.lower() == ".pdf":
            bloco = {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": b64,
                },
            }
        else:
            bloco = {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": b64,
                },
            }
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 512,
            "messages": [
                {
                    "role": "user",
                    "content": [bloco, {"type": "text", "text": PROMPT_EXTRACAO}],
                }
            ],
        }
        headers = {
            "x-api-key": self.claude_key,
            "anthropic-version": CLAUDE_VERSION,
        }
        status, corpo = _http_post_json(CLAUDE_URL, payload, headers=headers)
        obj = json.loads(corpo)
        try:
            partes = obj["content"]
            texto = "".join(
                p.get("text", "") for p in partes if p.get("type") == "text"
            )
        except (KeyError, TypeError):
            raise ValueError("Resposta do Claude sem conteudo de texto.")
        return texto

    def _tentar_gemini(self, b64, media_type):
        """Tenta o Gemini tratando 429 (espera 10s e tenta de novo uma vez)."""
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
        """Extrai dados de um documento aplicando a logica de fallback.

        Retorna (dados, provedor_usado) em sucesso, ou (None, provedor_tentado)
        com self._ultimo_erro preenchido em falha."""
        self.talvez_voltar_gemini()
        self._ultimo_erro = None

        # 1) Gemini (se for o provedor ativo)
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

        # 2) Claude (fallback para este arquivo)
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
        self.root.title("Renomeador de Documentos RH - Classic Couros")
        self.root.configure(bg=COR_BG)
        self.root.geometry("960x780")
        self.root.minsize(860, 700)

        # Estado de execucao
        self.fila = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()  # set = pausado
        self.worker = None
        self.progresso = {"origem": "", "processados": {}}

        self._construir_estilo()
        self._construir_ui()
        self._carregar_config()

        self.root.protocol("WM_DELETE_WINDOW", self._ao_fechar)
        self.root.after(120, self._processar_fila)

    # ----- estilo ttk (tema escuro) -----
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

    # ----- construcao da UI -----
    def _label(self, parent, texto, **kw):
        return tk.Label(parent, text=texto, bg=kw.pop("bg", COR_BG), fg=kw.pop("fg", COR_FG), **kw)

    def _construir_ui(self):
        # Cabecalho
        cab = tk.Frame(self.root, bg=COR_SURFACE)
        cab.pack(fill="x")
        tk.Label(
            cab,
            text="  Renomeador de Documentos RH",
            bg=COR_SURFACE,
            fg=COR_FG,
            font=("Segoe UI", 16, "bold"),
        ).pack(side="left", pady=10)
        tk.Label(
            cab,
            text="Classic Couros - IA com fallback Gemini / Claude   ",
            bg=COR_SURFACE,
            fg=COR_FG_DIM,
            font=("Segoe UI", 9),
        ).pack(side="right", pady=12)

        corpo = tk.Frame(self.root, bg=COR_BG)
        corpo.pack(fill="both", expand=True, padx=14, pady=10)

        # ---- Configuracoes ----
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

        for i in range(2):
            cfg.grid_columnconfigure(i * 2 + 1, weight=1)

        self.var_gemini = tk.StringVar()
        self.var_claude = tk.StringVar()
        self.var_origem = tk.StringVar()
        self.var_destino = tk.StringVar()
        self.var_intervalo = tk.IntVar(value=10)
        self.var_mover = tk.BooleanVar(value=True)

        # Chaves de API
        self._campo_senha(cfg, "Chave API Gemini (obrigatoria):", self.var_gemini, 0)
        self._campo_senha(cfg, "Chave API Claude (opcional):", self.var_claude, 1)
        # Pastas
        self._campo_pasta(cfg, "Pasta de ORIGEM (documentos):", self.var_origem, 2)
        self._campo_pasta(cfg, "Pasta DESTINO (raiz do RH):", self.var_destino, 3)

        # Slider intervalo
        self._label(cfg, "Intervalo entre arquivos:").grid(
            row=4, column=0, sticky="w", padx=10, pady=(8, 4)
        )
        slider_frame = tk.Frame(cfg, bg=COR_BG)
        slider_frame.grid(row=4, column=1, columnspan=3, sticky="we", padx=10, pady=(8, 4))
        self.slider = tk.Scale(
            slider_frame,
            from_=6,
            to=30,
            orient="horizontal",
            variable=self.var_intervalo,
            bg=COR_BG,
            fg=COR_FG,
            highlightthickness=0,
            troughcolor=COR_SURFACE2,
            activebackground=COR_AZUL,
            length=300,
            label=None,
            command=lambda _v: self._atualizar_label_intervalo(),
        )
        self.slider.pack(side="left")
        self.lbl_intervalo = self._label(slider_frame, "10 segundos", fg=COR_FG_DIM)
        self.lbl_intervalo.pack(side="left", padx=12)

        # Checkbox mover/copiar
        chk = tk.Checkbutton(
            cfg,
            text="Mover arquivos (desmarque para COPIAR e manter os originais)",
            variable=self.var_mover,
            bg=COR_BG,
            fg=COR_FG,
            selectcolor=COR_SURFACE2,
            activebackground=COR_BG,
            activeforeground=COR_FG,
        )
        chk.grid(row=5, column=0, columnspan=4, sticky="w", padx=10, pady=(2, 10))

        # ---- Botoes ----
        botoes = tk.Frame(corpo, bg=COR_BG)
        botoes.pack(fill="x", pady=(12, 6))
        self.btn_iniciar = self._botao(botoes, "INICIAR", COR_VERDE, self.iniciar)
        self.btn_pausar = self._botao(botoes, "PAUSAR", COR_LARANJA, self.pausar_retomar)
        self.btn_parar = self._botao(botoes, "PARAR", COR_VERMELHO, self.parar)
        self.btn_iniciar.pack(side="left", padx=(0, 8))
        self.btn_pausar.pack(side="left", padx=8)
        self.btn_parar.pack(side="left", padx=8)
        self.btn_pausar.config(state="disabled")
        self.btn_parar.config(state="disabled")

        # ---- Indicadores ----
        ind = tk.Frame(corpo, bg=COR_BG)
        ind.pack(fill="x", pady=(8, 4))

        self.lbl_status = self._label(
            ind, "Pronto. Preencha a configuracao e clique em INICIAR.", fg=COR_FG
        )
        self.lbl_status.pack(anchor="w")

        linha2 = tk.Frame(ind, bg=COR_BG)
        linha2.pack(fill="x", pady=(4, 0))
        self.lbl_provedor = self._label(linha2, "Provedor: -", fg=COR_FG_DIM)
        self.lbl_provedor.pack(side="left")
        self.lbl_countdown = self._label(linha2, "", fg=COR_LARANJA)
        self.lbl_countdown.pack(side="right")

        prog = tk.Frame(corpo, bg=COR_BG)
        prog.pack(fill="x", pady=(6, 4))
        self.barra = ttk.Progressbar(
            prog, style="Dark.Horizontal.TProgressbar", maximum=100, length=600
        )
        self.barra.pack(side="left", fill="x", expand=True)
        self.lbl_percent = self._label(prog, "0%", fg=COR_FG)
        self.lbl_percent.pack(side="left", padx=10)

        # ---- Log ----
        logf = tk.LabelFrame(
            corpo,
            text=" Registro de atividades ",
            bg=COR_BG,
            fg=COR_FG,
            font=("Segoe UI", 10, "bold"),
            bd=1,
            relief="solid",
        )
        logf.pack(fill="both", expand=True, pady=(8, 0))
        self.log_text = tk.Text(
            logf,
            bg="#15161f",
            fg=COR_FG,
            insertbackground=COR_FG,
            wrap="word",
            height=14,
            relief="flat",
            font=("Consolas", 9),
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
        e = tk.Entry(
            parent,
            textvariable=var,
            show="*",
            bg=COR_SURFACE2,
            fg=COR_FG,
            insertbackground=COR_FG,
            relief="flat",
        )
        e.grid(row=row, column=1, columnspan=3, sticky="we", padx=10, pady=4, ipady=4)

    def _campo_pasta(self, parent, rotulo, var, row):
        self._label(parent, rotulo).grid(row=row, column=0, sticky="w", padx=10, pady=4)
        e = tk.Entry(
            parent,
            textvariable=var,
            bg=COR_SURFACE2,
            fg=COR_FG,
            insertbackground=COR_FG,
            relief="flat",
        )
        e.grid(row=row, column=1, columnspan=2, sticky="we", padx=10, pady=4, ipady=4)
        b = tk.Button(
            parent,
            text="Procurar...",
            command=lambda: self._escolher_pasta(var),
            bg=COR_SURFACE2,
            fg=COR_FG,
            activebackground=COR_BORDA,
            activeforeground=COR_FG,
            relief="flat",
            padx=10,
        )
        b.grid(row=row, column=3, sticky="e", padx=10, pady=4)

    def _botao(self, parent, texto, cor, comando):
        return tk.Button(
            parent,
            text=texto,
            command=comando,
            bg=cor,
            fg="#0b0b12",
            activebackground=cor,
            activeforeground="#0b0b12",
            font=("Segoe UI", 11, "bold"),
            relief="flat",
            padx=24,
            pady=8,
            cursor="hand2",
        )

    def _escolher_pasta(self, var):
        d = filedialog.askdirectory()
        if d:
            var.set(d)

    def _atualizar_label_intervalo(self):
        self.lbl_intervalo.config(text=f"{self.var_intervalo.get()} segundos")

    # ----- log e indicadores (thread-safe via fila) -----
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
                    pct, txt = item[1], item[2]
                    self.barra["value"] = pct
                    self.lbl_percent.config(text=f"{int(pct)}%")
                    if txt:
                        self.lbl_status.config(text=txt)
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
                self.var_origem.set(c.get("origem", ""))
                self.var_destino.set(c.get("destino", ""))
                self.var_intervalo.set(int(c.get("intervalo", 10)))
                self.var_mover.set(bool(c.get("mover", True)))
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
                        "origem": self.var_origem.get().strip(),
                        "destino": self.var_destino.get().strip(),
                        "intervalo": self.var_intervalo.get(),
                        "mover": self.var_mover.get(),
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

    # ----- controle -----
    def iniciar(self):
        if self.worker and self.worker.is_alive():
            return
        gemini = self.var_gemini.get().strip()
        claude = self.var_claude.get().strip()
        origem = self.var_origem.get().strip()
        destino = self.var_destino.get().strip()

        if not gemini:
            messagebox.showerror("Falta a chave", "Informe a Chave API Gemini (obrigatoria).")
            return
        if not origem or not os.path.isdir(origem):
            messagebox.showerror("Pasta invalida", "Selecione uma pasta de ORIGEM valida.")
            return
        if not destino:
            messagebox.showerror("Pasta invalida", "Selecione a pasta DESTINO (raiz do RH).")
            return

        self._salvar_config()

        # Lista de arquivos
        arquivos = sorted(
            f
            for f in os.listdir(origem)
            if os.path.isfile(os.path.join(origem, f))
            and f.lower().endswith(EXTENSOES_VALIDAS)
        )
        if not arquivos:
            messagebox.showinfo("Nada a fazer", "Nenhum PDF/JPG/PNG encontrado na pasta de origem.")
            return

        # Retomada de progresso
        self.progresso = self._carregar_progresso()
        if self.progresso.get("origem") != origem:
            # progresso de outra pasta -> recomeca
            self.progresso = {"origem": origem, "processados": {}}

        ja_ok = [
            nome
            for nome, info in self.progresso.get("processados", {}).items()
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

        # Prepara execucao
        self.stop_event.clear()
        self.pause_event.clear()
        self.btn_iniciar.config(state="disabled")
        self.btn_pausar.config(state="normal", text="PAUSAR")
        self.btn_parar.config(state="normal")
        self._limpar_log()

        self.worker = threading.Thread(
            target=self._executar,
            args=(arquivos, origem, destino, gemini, claude),
            daemon=True,
        )
        self.worker.start()

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

    # ----- worker (thread) -----
    def _executar(self, arquivos, origem, destino, gemini, claude):
        try:
            self._loop_processamento(arquivos, origem, destino, gemini, claude)
        except Exception as e:  # noqa: BLE001
            self.log(f"Erro inesperado: {e}", "erro")
        finally:
            self.fila.put(("fim",))

    def _esperar_se_pausado(self):
        while self.pause_event.is_set() and not self.stop_event.is_set():
            self.fila.put(("countdown", "PAUSADO"))
            time.sleep(0.2)

    def _loop_processamento(self, arquivos, origem, destino, gemini, claude):
        gerente = GerenciadorIA(gemini, claude, self.stop_event, self.log)
        total = len(arquivos)
        os.makedirs(destino, exist_ok=True)

        self.log(f"Iniciando. {total} arquivo(s) na pasta de origem.", "info")
        self.fila.put(("provedor", gerente.ativo))

        feitos = 0
        for idx, nome in enumerate(arquivos, start=1):
            if self.stop_event.is_set():
                break
            self._esperar_se_pausado()
            if self.stop_event.is_set():
                break

            # pula ja processados com sucesso
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
                self._registrar(nome, "", "", "-", "ERRO", "-", f"leitura: {e}")
                continue

            media = media_type_de(ext)
            dados, provedor = gerente.extrair(b64, media, ext)
            self.fila.put(("provedor", gerente.ativo))

            if not dados:
                erro = getattr(gerente, "_ultimo_erro", "falha na IA")
                self.log(f"[{idx}/{total}] {nome}: FALHOU ({erro}).", "erro")
                self._registrar(nome, "", "", provedor, "ERRO", "-", str(erro))
                # mesmo em erro respeita o intervalo para nao floodar a API
                self._contagem_regressiva(gerente, idx, total)
                continue

            # Monta novo nome
            funcionario = normalizar_nome(dados.get("nome")) or "DESCONHECIDO"
            tipo = canonizar_tipo(dados.get("tipo"))
            data_ref = validar_data(dados.get("data"))
            confianca = (dados.get("confianca") or "MEDIA").upper()
            if confianca not in ("ALTA", "MEDIA", "BAIXA"):
                confianca = "MEDIA"

            pasta = pasta_destino_de(tipo)
            novo_base = f"{funcionario}_{tipo}_{data_ref}"
            novo_nome = self._mover_ou_copiar(
                caminho, ext, destino, pasta, novo_base
            )

            self.log(
                f"[{idx}/{total}] {nome} -> {pasta}/{novo_nome} "
                f"[{provedor}, confianca {confianca}]",
                "sucesso",
            )
            self._registrar(
                nome, novo_nome, pasta, provedor, "SUCESSO", confianca, ""
            )
            feitos += 1
            self._atualizar_barra(feitos, total)

            # Intervalo entre arquivos (com contagem regressiva)
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
            self.log(
                f"Modo cautela (limite recente): aguardando {intervalo}s.", "aviso"
            )
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

    def _mover_ou_copiar(self, origem_caminho, ext, destino_raiz, pasta, novo_base):
        pasta_destino = os.path.join(destino_raiz, pasta)
        os.makedirs(pasta_destino, exist_ok=True)
        # Trata duplicados
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
        return nome_final

    def _registrar(self, original, novo, pasta, provedor, status, confianca, erro):
        self.progresso.setdefault("origem", self.var_origem.get().strip())
        self.progresso.setdefault("processados", {})
        self.progresso["processados"][original] = {
            "novo_nome": novo,
            "pasta": pasta,
            "provedor": provedor,
            "status": status,
            "confianca": confianca,
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
                    [
                        "ARQUIVO ORIGINAL",
                        "NOVO NOME",
                        "PASTA DESTINO",
                        "PROVEDOR USADO",
                        "STATUS",
                        "CONFIANCA",
                    ]
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
                        ]
                    )
            self.log(f"Relatorio gerado: {nome}", "info")
        except Exception as e:  # noqa: BLE001
            self.log(f"Nao foi possivel gerar o relatorio: {e}", "erro")

    def _ao_terminar(self):
        self.btn_iniciar.config(state="normal")
        self.btn_pausar.config(state="disabled", text="PAUSAR")
        self.btn_parar.config(state="disabled")
        self.fila.put(("countdown", ""))

    def _ao_fechar(self):
        if self.worker and self.worker.is_alive():
            if not messagebox.askyesno(
                "Sair", "Ha processamento em andamento. Deseja sair mesmo assim?"
            ):
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
