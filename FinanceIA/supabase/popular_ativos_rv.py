"""
popular_ativos_rv.py
--------------------
Popula ativos_rv com ações da B3.

Fontes:
  - B3 GetInitialCompanies → codigo_b3, nome, setor (type=1, segmentos classificados)
  - Brapi /quote/{ticker}  → tickers completos (PETR3, PETR4) agrupados por codigo_b3

Uso:
  pip install requests supabase python-dotenv --break-system-packages
  python popular_ativos_rv.py
"""

import os
import json
import time
import requests
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime

load_dotenv()

BRAPI_TOKEN   = os.getenv("BRAPI_TOKEN")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

CHECKPOINT_FILE    = "checkpoint_ativos_rv.json"
B3_URL             = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIn0="
BRAPI_BASE         = "https://brapi.dev/api"
SEGMENTOS_IGNORAR  = {"Não Classificados", "Não Classificado", "Outros", ""}
DELAY_BRAPI        = 0.5
BATCH_BRAPI        = 10   # tickers por request

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Checkpoint ────────────────────────────────────────────────

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"processados": []}

def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ── Etapa 1: B3 → dict por codigo_b3 ─────────────────────────

def buscar_acoes_b3() -> dict:
    """
    Retorna dict {codigo_b3: {nome, setor}} para ações classificadas.
    Exclui segmentos genéricos e type != 1.
    """
    resp = requests.get(B3_URL, timeout=15)
    resp.raise_for_status()
    results = resp.json()["results"]

    acoes = {}
    for d in results:
        if d.get("type") != "1":
            continue
        seg = d.get("segment", "")
        if seg in SEGMENTOS_IGNORAR:
            continue
        codigo = d.get("issuingCompany", "").strip()
        if not codigo:
            continue
        acoes[codigo] = {
            "nome":  d.get("companyName", "").strip(),
            "setor": seg.strip(),
        }

    print(f"  → {len(acoes)} empresas classificadas na B3")
    return acoes

# ── Etapa 2: Brapi → tickers agrupados por codigo_b3 ─────────

def extrair_codigo(ticker: str) -> str:
    """PETR4 → PETR, MXRF11 → MXRF"""
    return ticker.rstrip("0123456789")

def buscar_tickers_brapi(codigos_b3: set) -> dict:
    """
    Busca tickers da Brapi e agrupa por codigo_b3.
    Retorna dict {codigo_b3: [ticker1, ticker2, ...]}
    """
    # Primeiro passo: obter lista de tickers via busca por código
    # Estratégia: buscar cada codigo_b3 no parâmetro search
    grupos = {c: [] for c in codigos_b3}
    total = len(codigos_b3)

    codigos_lista = list(codigos_b3)
    for i, codigo in enumerate(codigos_lista):
        url = f"{BRAPI_BASE}/quote/list?search={codigo}&limit=10&token={BRAPI_TOKEN}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            stocks = data.get("stocks", [])
            for s in stocks:
                ticker = s.get("stock", "").upper()
                cod = extrair_codigo(ticker)
                if cod == codigo and ticker not in grupos[codigo]:
                    grupos[codigo].append(ticker)
        except Exception as e:
            print(f"  [ERRO] Brapi search {codigo}: {e}")

        if (i + 1) % 50 == 0:
            print(f"  Brapi: {i+1}/{total} códigos processados")
        time.sleep(DELAY_BRAPI)

    return grupos

# ── Etapa 3: Upsert no Supabase ───────────────────────────────

def upsert_ativo(codigo_b3: str, nome: str, setor: str, tickers: list):
    record = {
        "codigo_b3":    codigo_b3,
        "nome":         nome,
        "tipo":         "ACAO",
        "setor":        setor or None,
        "segmento":     None,   # enriquecido depois via summaryProfile
        "gestora":      None,
        "tickers":      tickers,
        "ativo_na_b3":  True,
        "atualizado_em": datetime.utcnow().isoformat(),
    }
    supabase.table("ativos_rv").upsert(record, on_conflict="codigo_b3").execute()

# ── Main ──────────────────────────────────────────────────────

def main():
    checkpoint  = load_checkpoint()
    processados = set(checkpoint.get("processados", []))
    print(f"Checkpoint: {len(processados)} empresas já processadas\n")

    # Etapa 1
    print("=== Etapa 1: Buscando ações da B3 ===")
    acoes_b3 = buscar_acoes_b3()
    pendentes = [c for c in acoes_b3 if c not in processados]
    print(f"Pendentes: {len(pendentes)}\n")

    # Etapa 2
    print("=== Etapa 2: Buscando tickers na Brapi ===")
    grupos = buscar_tickers_brapi(set(pendentes))

    # Etapa 3
    print("\n=== Etapa 3: Inserindo no Supabase ===")
    erros = []
    for codigo in pendentes:
        info    = acoes_b3[codigo]
        tickers = grupos.get(codigo, [])
        try:
            upsert_ativo(
                codigo_b3=codigo,
                nome=info["nome"],
                setor=info["setor"],
                tickers=tickers,
            )
            processados.add(codigo)
            print(f"  ✓ {codigo} | {info['setor']} | tickers: {tickers}")
        except Exception as e:
            print(f"  ✗ {codigo}: {e}")
            erros.append(codigo)

        checkpoint["processados"] = list(processados)
        save_checkpoint(checkpoint)

    print(f"\n=== Concluído ===")
    print(f"  Inseridos: {len(processados)}")
    print(f"  Sem tickers encontrados: {sum(1 for c in pendentes if not grupos.get(c))}")
    print(f"  Erros: {len(erros)}")

if __name__ == "__main__":
    main()