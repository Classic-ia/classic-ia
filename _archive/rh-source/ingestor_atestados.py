"""
ingestor_atestados.py - Monitora pasta de atestados e importa automaticamente
Uso:
  python ingestor_atestados.py          # processa uma vez
  python ingestor_atestados.py --watch  # monitora continuamente (5min)
"""
import os, re, json, requests, sys, time, hashlib
from datetime import datetime

SB_URL = "https://muiqmtnfvyffborgiwdw.supabase.co"
SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im11aXFtdG5mdnlmZmJvcmdpd2R3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM4NzQ2MTAsImV4cCI6MjA4OTQ1MDYxMH0.ReerGoDWcLrxJ1WIIKflN4Sc-st4zR1b6yWagCaPq_A"
HEADERS = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"}

BASE_RH = os.path.join(r"C:\Users\janai\OneDrive\RH e SEGURANÇA DO TRABALHO CLASSIC", "RH_E_SEGURANCA_DO_TRABALHO_CLASSIC")
BASE_ATESTADOS = os.path.join(BASE_RH, "Atestados")
BASE_ADVERTENCIAS = os.path.join(BASE_RH, "ADVERTENCIAS")

# Cache de hashes ja processados
CACHE_FILE = os.path.join(os.path.dirname(__file__), ".atestados_cache.json")

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

def file_hash(filepath):
    return hashlib.md5(filepath.encode()).hexdigest()

def parse_data_8dig(s):
    """Parse DDMMAAAA ou DDMMYYYY"""
    if not s or len(s) != 8: return None
    try:
        d, m, y = int(s[:2]), int(s[2:4]), int(s[4:8])
        if 1 <= d <= 31 and 1 <= m <= 12 and 2020 <= y <= 2030:
            return datetime(y, m, d)
    except: pass
    return None

def parse_filename(filename):
    """Extrai nome, data inicio, data fim do nome do arquivo"""
    name = re.sub(r'\.(pdf|jpeg|jpg|png)$', '', filename, flags=re.IGNORECASE).strip()

    # Padrao: NOME DDMMAAAA a DDMMAAAA
    m = re.match(r'^(.+?)\s+(\d{8})\s*[aA]\s*(\d{8})$', name)
    if m:
        return m.group(1).strip(), parse_data_8dig(m.group(2)), parse_data_8dig(m.group(3))

    # Padrao: NOME DDMMAAAA
    m = re.match(r'^(.+?)\s+(\d{8})$', name)
    if m:
        return m.group(1).strip(), parse_data_8dig(m.group(2)), None

    # Fallback: qualquer 8 digitos
    datas = re.findall(r'(\d{8})', name)
    if datas:
        nome_part = re.split(r'\d{8}', name)[0].strip().rstrip(' -_a')
        d1 = parse_data_8dig(datas[0])
        d2 = parse_data_8dig(datas[1]) if len(datas) >= 2 else None
        return nome_part or name, d1, d2

    return name, None, None

def inferir_categoria_da_pasta(rel_path):
    """Infere categoria pela estrutura de pastas"""
    upper = rel_path.upper()
    if 'MAIS DE 3' in upper or '3 DIAS OU MAIS' in upper:
        return '3_dias_ou_mais'
    elif 'MENOS DE 3' in upper or 'ATE 2' in upper or 'ATÉ 2' in upper:
        return 'ate_2_dias'
    return None

def inferir_ano_mes_da_pasta(rel_path):
    """Extrai ano e mes da estrutura de pastas"""
    ano = None; mes = None
    for part in rel_path.split(os.sep):
        if re.match(r'^20\d{2}$', part):
            ano = int(part)
        m = re.match(r'^(\d{1,2})\s*[-]?\s*', part)
        if m and not mes:
            try:
                v = int(m.group(1))
                if 1 <= v <= 12: mes = v
            except: pass
    return ano, mes

def match_funcionario_rpc(nome):
    """Match por nome via RPC"""
    if not nome or len(nome) < 4: return None, None
    nome_clean = nome.upper().strip()
    # Pegar primeiras 2 palavras significativas
    words = [w for w in nome_clean.split() if len(w) > 2 and w not in ('DA', 'DE', 'DO', 'DOS', 'DAS')]
    if len(words) < 1: return None, None
    search = words[0][:10]

    try:
        r = requests.get(
            f"{SB_URL}/rest/v1/rpc/rh_cpf_map",
            headers=HEADERS, timeout=15
        )
        if not r.ok: return None, None
        funcionarios = r.json()
        if isinstance(funcionarios, list):
            for f in funcionarios:
                fn = f.get('nome_completo', '').upper()
                if search in fn:
                    return f.get('id'), fn
    except: pass
    return None, None

# Cache de funcionarios
_func_cache = {}
_func_list = None

def load_funcionarios():
    global _func_list
    if _func_list is not None: return
    try:
        r = requests.post(f"{SB_URL}/rest/v1/rpc/rh_cpf_map", headers=HEADERS, json={}, timeout=15)
        if r.ok:
            _func_list = r.json() if isinstance(r.json(), list) else []
            for f in _func_list:
                nome = f.get('nome_completo', '').upper()
                _func_cache[nome] = f.get('id')
    except:
        _func_list = []

def match_funcionario(nome):
    if not nome or len(nome) < 3: return None, None
    load_funcionarios()
    nome_up = nome.upper().strip()

    # Match exato
    if nome_up in _func_cache:
        return _func_cache[nome_up], nome_up

    # Match parcial (nome contem)
    words = [w for w in nome_up.split() if len(w) > 2 and w not in ('DA','DE','DO','DOS','DAS')]
    if not words: return None, None

    for fn, fid in _func_cache.items():
        if all(w in fn for w in words[:2]):
            return fid, fn

    # Match por primeiro + ultimo nome
    if len(words) >= 2:
        for fn, fid in _func_cache.items():
            if words[0] in fn and words[-1] in fn:
                return fid, fn

    return None, None

def rpc(name, params):
    r = requests.post(f"{SB_URL}/rest/v1/rpc/{name}", headers=HEADERS, json=params, timeout=30)
    if not r.ok: raise Exception(f"RPC {name}: {r.status_code}")
    return r.json()

def processar():
    print(f"\n{'='*60}")
    print(f"Ingestor de Atestados - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*60}")
    print(f"Pasta: {BASE_ATESTADOS}")

    cache = load_cache()
    registros = []
    novos = 0
    ja_processados = 0

    for root, dirs, files in os.walk(BASE_ATESTADOS):
        for f in sorted(files):
            ext = os.path.splitext(f)[1].lower()
            if ext not in ('.pdf', '.jpeg', '.jpg', '.png'): continue

            filepath = os.path.join(root, f)
            fhash = file_hash(filepath)

            # Pular ja processados
            if fhash in cache:
                ja_processados += 1
                continue

            rel_path = os.path.relpath(root, BASE_ATESTADOS)
            nome, data_inicio, data_fim = parse_filename(f)

            # Calcular dias
            if data_inicio and data_fim:
                dias = (data_fim - data_inicio).days + 1
            else:
                dias = 1

            # Auto-detectar categoria
            cat_pasta = inferir_categoria_da_pasta(rel_path)
            if dias >= 3:
                categoria = '3_dias_ou_mais'
            elif cat_pasta:
                categoria = cat_pasta
            else:
                categoria = 'ate_2_dias'

            # Competencia da data
            if data_inicio:
                competencia = data_inicio.strftime('%Y-%m')
            else:
                ano, mes = inferir_ano_mes_da_pasta(rel_path)
                competencia = f"{ano or 2026}-{str(mes or 1).zfill(2)}"

            # Match funcionario
            func_id, func_nome = match_funcionario(nome)

            registros.append({
                "colaborador_id": func_id,
                "tipo": "atestado",
                "data_inicio": data_inicio.strftime('%Y-%m-%d') if data_inicio else None,
                "data_fim": data_fim.strftime('%Y-%m-%d') if data_fim else (data_inicio.strftime('%Y-%m-%d') if data_inicio else None),
                "dias": dias,
                "competencia": competencia,
                "observacoes": categoria.replace('_', ' '),
                "arquivo_nome": f,
                "nome_parseado": nome,
                "func_nome_match": func_nome,
                "hash": fhash,
            })
            novos += 1

    print(f"\nArquivos encontrados: {novos + ja_processados}")
    print(f"Ja processados: {ja_processados}")
    print(f"Novos: {novos}")

    # === ADVERTENCIAS ===
    print(f"\nEscaneando advertencias: {BASE_ADVERTENCIAS}")
    if os.path.isdir(BASE_ADVERTENCIAS):
        for root, dirs, files in os.walk(BASE_ADVERTENCIAS):
            for f in sorted(files):
                ext = os.path.splitext(f)[1].lower()
                if ext not in ('.pdf', '.jpeg', '.jpg', '.png'): continue
                filepath = os.path.join(root, f)
                fhash = file_hash(filepath)
                if fhash in cache:
                    ja_processados += 1
                    continue

                nome, data_inicio, data_fim = parse_filename(f)
                func_id, func_nome = match_funcionario(nome)

                registros.append({
                    "colaborador_id": func_id,
                    "tipo": "advertencia",
                    "data_inicio": data_inicio.strftime('%Y-%m-%d') if data_inicio else None,
                    "data_fim": None,
                    "dias": 0,
                    "competencia": data_inicio.strftime('%Y-%m') if data_inicio else None,
                    "observacoes": "Advertencia importada da pasta",
                    "arquivo_nome": f,
                    "nome_parseado": nome,
                    "func_nome_match": func_nome,
                    "hash": fhash,
                })
                novos += 1
        print(f"  Advertencias novas: {sum(1 for r in registros if r['tipo']=='advertencia')}")

    if novos == 0:
        print("Nenhum arquivo novo para processar.")
        return

    # Enviar via RPC (match por nome feito no servidor)
    total_ok = 0
    total_sem = 0
    batch = 200

    for i in range(0, len(registros), batch):
        lote = registros[i:i+batch]
        lote_rpc = [{
            "nome_parseado": r["nome_parseado"],
            "tipo": r["tipo"],
            "data_inicio": r["data_inicio"],
            "data_fim": r["data_fim"],
            "dias": r["dias"],
            "competencia": r["competencia"],
            "observacoes": r["observacoes"],
            "arquivo_nome": r["arquivo_nome"],
        } for r in lote]

        try:
            res = requests.post(
                f"{SB_URL}/rest/v1/rpc/importar_ocorrencias_lote",
                headers=HEADERS, json={"p_dados": lote_rpc}, timeout=60
            )
            if res.ok:
                result = res.json()
                if isinstance(result, list): result = result[0] if result else {}
                imp = result.get("importados", 0)
                sem = result.get("sem_match", 0)
                total_ok += imp
                total_sem += sem
                print(f"  Lote {i//batch+1}: +{imp} importados, {sem} sem match")
            else:
                print(f"  Erro lote {i//batch+1}: {res.status_code}")
        except Exception as e:
            print(f"  Erro lote {i//batch+1}: {e}")

        # Marcar como processados no cache
        for r in lote:
            cache[r["hash"]] = {"data": r["data_inicio"], "nome": r["nome_parseado"]}

        time.sleep(0.3)

    save_cache(cache)

    print(f"\n{'='*60}")
    print(f"RESULTADO:")
    print(f"  Importados: {total_ok}")
    print(f"  Sem match funcionario: {total_sem}")
    print(f"  Total no cache: {len(cache)}")
    print(f"{'='*60}")

def main():
    if '--watch' in sys.argv:
        print("Modo watch: verificando a cada 5 minutos...")
        while True:
            processar()
            print("\nAguardando 5 minutos...")
            time.sleep(300)
    else:
        processar()

if __name__ == "__main__":
    main()
