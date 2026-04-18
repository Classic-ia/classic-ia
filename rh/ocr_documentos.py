"""
ocr_documentos.py - OCR de atestados, ASOs, CATs e certificados NR
Usa Tesseract + PyMuPDF + Pillow para extrair texto de PDFs e imagens
"""
import os, re, json, requests, time, sys
import fitz  # PyMuPDF
from PIL import Image
import pytesseract

# Config
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
SB_URL = os.environ.get("SUPABASE_RH_URL") or os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SUPABASE_RH_KEY") or os.environ.get("SUPABASE_KEY")
if not SB_URL or not SB_KEY:
    raise RuntimeError("Defina SUPABASE_RH_URL e SUPABASE_RH_KEY no ambiente (ou SUPABASE_URL/SUPABASE_KEY).")
HEADERS = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"}

BASE_ATESTADOS = os.path.join(r'C:\Users\janai\OneDrive\RH e SEGURANÇA DO TRABALHO CLASSIC', 'RH_E_SEGURANCA_DO_TRABALHO_CLASSIC', 'Atestados')
BASE_SST = os.path.join(r'C:\Users\janai\OneDrive\RH e SEGURANÇA DO TRABALHO CLASSIC', 'RH_E_SEGURANCA_DO_TRABALHO_CLASSIC', 'SEGURANÇA DO TRABALHO')
BASE_COLAB = os.path.join(r'C:\Users\janai\OneDrive\RH e SEGURANÇA DO TRABALHO CLASSIC', 'RH_E_SEGURANCA_DO_TRABALHO_CLASSIC', 'COLABORADORES', '001 - CLASSIC JARDIM ALEGRE-PR')

# Cache de funcionarios
_func_cache = {}
def match_funcionario(nome):
    if not nome: return None, None
    nome_up = nome.strip().upper()
    if nome_up in _func_cache: return _func_cache[nome_up]
    r = requests.get(f'{SB_URL}/rest/v1/rh_funcionarios?nome_completo=ilike.*{nome_up[:15]}*&select=id,nome_completo&limit=1', headers=HEADERS, timeout=10)
    if r.ok and r.json():
        result = (r.json()[0]['id'], r.json()[0]['nome_completo'])
        _func_cache[nome_up] = result
        return result
    _func_cache[nome_up] = (None, None)
    return None, None

def ocr_pdf(filepath):
    """Extrai texto de PDF - primeiro tenta texto embutido, depois OCR"""
    try:
        doc = fitz.open(filepath)
        text = ""
        for page in doc:
            t = page.get_text()
            if t.strip():
                text += t
            else:
                # OCR da pagina como imagem
                pix = page.get_pixmap(dpi=200)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                t = pytesseract.image_to_string(img, lang='por')
                text += t
        doc.close()
        return text
    except Exception as e:
        return f"ERRO: {e}"

def ocr_image(filepath):
    """OCR de imagem (JPEG, PNG)"""
    try:
        img = Image.open(filepath)
        return pytesseract.image_to_string(img, lang='por')
    except Exception as e:
        return f"ERRO: {e}"

def extrair_dados_atestado(texto, filename, path_info):
    """Extrai dados estruturados de texto de atestado"""
    dados = {
        'dias_afastamento': None,
        'cid': None,
        'medico': None,
        'crm': None,
        'data_documento': None,
    }
    if not texto or texto.startswith('ERRO'): return dados

    texto_upper = texto.upper()

    # CID
    cids = re.findall(r'\b([A-Z]\d{2}(?:\.\d{1,2})?)\b', texto_upper)
    # Filtrar CIDs validos (A00-Z99)
    for c in cids:
        if re.match(r'^[A-Z]\d{2}', c) and c[0] not in ('R', 'S') or c.startswith('R') or c.startswith('S'):
            dados['cid'] = c
            break

    # Dias
    dias_match = re.findall(r'(\d{1,3})\s*(?:dia|dias|DIA|DIAS)', texto)
    if dias_match:
        dados['dias_afastamento'] = int(dias_match[0])

    # Extrair dias do nome do arquivo
    if not dados['dias_afastamento']:
        dias_file = re.findall(r'(\d{1,2})\s*(?:dia|d\b)', filename.lower())
        if dias_file:
            dados['dias_afastamento'] = int(dias_file[0])

    # Inferir dias pela pasta (mais de 3 / menos de 3)
    if not dados['dias_afastamento']:
        if 'MAIS DE 3' in str(path_info).upper() or '3 DIAS OU MAIS' in str(path_info).upper():
            dados['dias_afastamento'] = 4  # minimo 4
        elif 'MENOS DE 3' in str(path_info).upper() or 'ATE 2' in str(path_info).upper() or 'ATÉ 2' in str(path_info).upper():
            dados['dias_afastamento'] = 1

    # CRM
    crm_match = re.findall(r'CRM[:\s]*(\d{3,6})', texto_upper)
    if crm_match: dados['crm'] = crm_match[0]

    # Data
    datas = re.findall(r'(\d{2}[/.-]\d{2}[/.-]\d{4})', texto)
    if datas:
        try:
            d, m, y = re.split(r'[/.-]', datas[0])
            dados['data_documento'] = f"{y}-{m}-{d}"
        except: pass

    # Data do nome do arquivo
    if not dados['data_documento']:
        date_match = re.findall(r'(\d{2})(\d{2})(20\d{2})', filename.replace(' ', ''))
        if date_match:
            d, m, y = date_match[0]
            if 1 <= int(d) <= 31 and 1 <= int(m) <= 12:
                dados['data_documento'] = f"{y}-{m}-{d}"

    # Medico
    dr_match = re.findall(r'(?:Dr[a.]?\.?\s+|Médico[a]?:?\s*)([A-ZÀ-Ú][a-zà-ú]+ [A-ZÀ-Ú][a-zà-ú]+)', texto)
    if dr_match: dados['medico'] = dr_match[0]

    return dados

def extrair_nome_do_path(filepath, base):
    """Extrai nome do funcionario do caminho do arquivo ou nome do arquivo"""
    rel = os.path.relpath(filepath, base)
    parts = rel.split(os.sep)

    # Para atestados: nome geralmente esta no nome do arquivo
    fname = os.path.splitext(os.path.basename(filepath))[0]
    # Remover datas e numeros
    nome = re.sub(r'\d{2,8}', '', fname)
    nome = re.sub(r'[_\-.]', ' ', nome)
    nome = re.sub(r'\s+(a|de|do|da|dos|das|em|para)\s+', ' ', nome, flags=re.IGNORECASE)
    nome = re.sub(r'\s+', ' ', nome).strip()

    # Pegar primeiras 2-3 palavras como nome
    words = [w for w in nome.split() if len(w) > 2 and w.upper() not in ('PDF', 'JPG', 'JPEG', 'PNG', 'ATESTADO', 'VESPERTINO', 'MATUTINO', 'NOTURNO', 'DIAS', 'DIA')]
    if len(words) >= 2:
        return ' '.join(words[:3]).upper()
    return None

def processar_pasta(base_path, tipo_doc, max_files=500):
    """Processa todos os PDFs/imagens de uma pasta"""
    resultados = []
    processados = 0
    erros = 0

    for root, dirs, files in os.walk(base_path):
        for f in sorted(files):
            if processados >= max_files: break

            ext = os.path.splitext(f)[1].lower()
            if ext not in ('.pdf', '.jpeg', '.jpg', '.png'): continue

            filepath = os.path.join(root, f)
            rel_path = os.path.relpath(root, base_path)

            # OCR
            if ext == '.pdf':
                texto = ocr_pdf(filepath)
            else:
                texto = ocr_image(filepath)

            if texto.startswith('ERRO'):
                erros += 1
                continue

            # Extrair nome do funcionario
            nome_func = extrair_nome_do_path(filepath, base_path)
            func_id, func_nome = match_funcionario(nome_func) if nome_func else (None, None)

            # Extrair dados estruturados
            dados = extrair_dados_atestado(texto, f, rel_path)

            # Extrair ano/mes da pasta
            ano = None; mes = None
            for part in rel_path.split(os.sep):
                if re.match(r'^20\d{2}$', part): ano = int(part)
                m = re.search(r'(\d{1,2})\s*[-]?\s*(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro|JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO|JAN|FEV|MAR|ABR|MAI|JUN|JUL|AGO|SET|OUT|NOV|DEZ)', part, re.IGNORECASE)
                if m:
                    try: mes = int(m.group(1))
                    except: pass
                if re.match(r'^\d{1,2}\s*[-]?\s*\w+', part) and not mes:
                    try: mes = int(re.match(r'^(\d{1,2})', part).group(1))
                    except: pass

            # Gravidade
            gravidade = None
            if 'MAIS DE 3' in rel_path.upper() or '3 DIAS OU MAIS' in rel_path.upper():
                gravidade = 'moderado'
            elif 'MENOS DE 3' in rel_path.upper() or 'ATE 2' in rel_path.upper():
                gravidade = 'leve'

            resultados.append({
                'funcionario_id': func_id,
                'funcionario_nome': func_nome or nome_func,
                'tipo_documento': tipo_doc,
                'arquivo_nome': f,
                'arquivo_caminho': rel_path,
                'texto_extraido': texto[:500] if texto else None,
                'data_documento': dados.get('data_documento'),
                'dias_afastamento': dados.get('dias_afastamento'),
                'cid': dados.get('cid'),
                'medico': dados.get('medico'),
                'crm': dados.get('crm'),
                'ano_referencia': ano,
                'mes_referencia': mes,
                'gravidade': gravidade,
                'confianca_ocr': 70 if len(texto or '') > 50 else 30,
            })

            processados += 1
            if processados % 50 == 0:
                print(f'  ... {processados} processados')

    return resultados, erros

def enviar_para_banco(resultados):
    """Envia resultados para rh_documento_ocr via REST"""
    total = 0
    batch = 50
    for i in range(0, len(resultados), batch):
        lote = resultados[i:i+batch]
        for r in lote:
            try:
                payload = {k: v for k, v in r.items() if v is not None}
                res = requests.post(f'{SB_URL}/rest/v1/rh_documento_ocr', headers={**HEADERS, 'Prefer': 'return=minimal'}, json=payload, timeout=10)
                if res.ok: total += 1
            except: pass
        time.sleep(0.2)
    return total

def main():
    print("=" * 60)
    print("OCR de Documentos - Atestados, ASOs, CATs, Certificados")
    print("=" * 60)

    # 1. Atestados (prioridade - 2025 e 2026 apenas para ser rapido)
    print("\n[1/4] Processando ATESTADOS 2025-2026...")
    resultados_atestados = []
    erros_total = 0
    for year in ['2025', '2026']:
        yp = os.path.join(BASE_ATESTADOS, year)
        if os.path.isdir(yp):
            print(f'  Ano {year}:')
            res, erros = processar_pasta(yp, 'atestado', max_files=300)
            resultados_atestados.extend(res)
            erros_total += erros

    print(f'  Total atestados OCR: {len(resultados_atestados)} | Erros: {erros_total}')
    com_cid = sum(1 for r in resultados_atestados if r.get('cid'))
    com_dias = sum(1 for r in resultados_atestados if r.get('dias_afastamento'))
    com_func = sum(1 for r in resultados_atestados if r.get('funcionario_id'))
    print(f'  Com CID: {com_cid} | Com dias: {com_dias} | Com funcionario: {com_func}')

    # 2. Exames Ocupacionais (ASOs historicos)
    print("\n[2/4] Processando EXAMES OCUPACIONAIS...")
    aso_path = os.path.join(BASE_SST, '15 - EXAMES OCUPACIONAIS')
    if os.path.isdir(aso_path):
        res_aso, erros = processar_pasta(aso_path, 'aso', max_files=100)
        print(f'  Total ASOs OCR: {len(res_aso)} | Erros: {erros}')
    else:
        res_aso = []
        print('  Pasta nao encontrada')

    # 3. CATs
    print("\n[3/4] Processando CATs...")
    cat_path = os.path.join(BASE_SST, '16 - CAT (Comunicação de Acidente de Trabalho)')
    if os.path.isdir(cat_path):
        res_cat, erros = processar_pasta(cat_path, 'cat', max_files=20)
        print(f'  Total CATs OCR: {len(res_cat)} | Erros: {erros}')
    else:
        res_cat = []
        print('  Pasta nao encontrada')

    # 4. Certificados NR
    print("\n[4/4] Processando CERTIFICADOS NR...")
    nr_path = os.path.join(BASE_SST, '21 - CERTIFICADOS E CURSOS DAS NRs')
    if os.path.isdir(nr_path):
        res_nr, erros = processar_pasta(nr_path, 'certificado_nr', max_files=100)
        print(f'  Total Certificados OCR: {len(res_nr)} | Erros: {erros}')
    else:
        res_nr = []
        print('  Pasta nao encontrada')

    # Enviar tudo para o banco
    todos = resultados_atestados + res_aso + res_cat + res_nr
    print(f'\n=== ENVIANDO {len(todos)} RESULTADOS PARA O BANCO ===')
    total_enviado = enviar_para_banco(todos)

    print(f"\n{'='*60}")
    print(f"RESULTADO FINAL OCR:")
    print(f"  Atestados processados: {len(resultados_atestados)}")
    print(f"  ASOs processados: {len(res_aso)}")
    print(f"  CATs processados: {len(res_cat)}")
    print(f"  Certificados NR processados: {len(res_nr)}")
    print(f"  Total enviado ao banco: {total_enviado}")
    print(f"  CIDs extraidos: {com_cid}")
    print(f"  Dias extraidos: {com_dias}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
