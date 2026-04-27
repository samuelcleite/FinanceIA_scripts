"""
descobrir_tickers_rv.py
-----------------------
Para cada codigo_b3 já inserido em ativos_rv,
testa sufixos 3, 4 e 11 via Brapi e atualiza o campo tickers.

Uso:
  python descobrir_tickers_rv.py
"""

import os
import json
import time
import requests
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime

load_dotenv()

BRAPI_TOKEN  = os.getenv("BRAPI_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CHECKPOINT_FILE = "checkpoint_tickers_rv.json"
BRAPI_BASE      = "https://brapi.dev/api"
SUFIXOS         = ["3", "4", "11"]
DELAY           = 0.3   # segundos entre chamadas

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

# ── Brapi: testar se ticker existe ────────────────────────────

def ticker_existe(ticker: str) -> bool:
    url = f"{BRAPI_BASE}/quote/{ticker}?token={BRAPI_TOKEN}"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return False
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return False
        # Ticker inválido retorna regularMarketPrice None
        preco = results[0].get("regularMarketPrice")
        return preco is not None
    except Exception:
        return False

# ── Supabase: atualizar tickers ───────────────────────────────

def atualizar_tickers(codigo_b3: str, tickers: list):
    supabase.table("ativos_rv").update({
        "tickers":      tickers,
        "atualizado_em": datetime.utcnow().isoformat(),
    }).eq("codigo_b3", codigo_b3).execute()

# ── Main ──────────────────────────────────────────────────────

def main():
    checkpoint  = load_checkpoint()
    processados = set(checkpoint.get("processados", []))

    # Busca todos os codigos_b3 da tabela
    resp = supabase.table("ativos_rv").select("codigo_b3").execute()
    todos = [r["codigo_b3"] for r in resp.data]
    pendentes = [c for c in todos if c not in processados]

    print(f"Total: {len(todos)} | Já processados: {len(processados)} | Pendentes: {len(pendentes)}\n")

    encontrados = 0
    sem_ticker  = 0

    for i, codigo in enumerate(pendentes):
        tickers_validos = []

        for sufixo in SUFIXOS:
            ticker = f"{codigo}{sufixo}"
            if ticker_existe(ticker):
                tickers_validos.append(ticker)
                print(f"  ✓ {ticker}")
            time.sleep(DELAY)

        if tickers_validos:
            atualizar_tickers(codigo, tickers_validos)
            encontrados += 1
        else:
            sem_ticker += 1
            print(f"  ✗ {codigo} — nenhum ticker encontrado")

        processados.add(codigo)
        checkpoint["processados"] = list(processados)

        # Salva checkpoint a cada 10 empresas
        if (i + 1) % 10 == 0:
            save_checkpoint(checkpoint)
            print(f"  [{i+1}/{len(pendentes)}] checkpoint salvo")

    save_checkpoint(checkpoint)

    print(f"\n=== Concluído ===")
    print(f"  Com tickers: {encontrados}")
    print(f"  Sem tickers: {sem_ticker}")

if __name__ == "__main__":
    main()