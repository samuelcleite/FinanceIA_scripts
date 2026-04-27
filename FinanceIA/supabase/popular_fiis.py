"""
popular_fiis_rv.py
------------------
Popula ativos_rv com FIIs a partir do CSV da B3 (fundosListados.csv).
Testa ticker {codigo}11 na Brapi para confirmar se está ativo.

Uso:
  pip install requests supabase python-dotenv --break-system-packages
  python popular_fiis_rv.py
"""

import os
import csv
import json
import time
import requests
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime, timezone

load_dotenv()

BRAPI_TOKEN  = os.getenv("BRAPI_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CSV_PATH        = "aux/fundosListados.csv"
CHECKPOINT_FILE = "aux/checkpoint_fiis_rv.json"
BRAPI_BASE      = "https://brapi.dev/api"
DELAY           = 0.3

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

# ── Brapi: verificar se ticker existe ────────────────────────

def ticker_existe(ticker: str) -> bool:
    url = f"{BRAPI_BASE}/quote/{ticker}?token={BRAPI_TOKEN}"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return False
        results = resp.json().get("results", [])
        if not results:
            return False
        return results[0].get("regularMarketPrice") is not None
    except Exception:
        return False

# ── Supabase: upsert ──────────────────────────────────────────

def upsert_fii(codigo_b3: str, nome: str, tickers: list):
    record = {
        "codigo_b3":    codigo_b3,
        "nome":         nome,
        "tipo":         "FII",
        "setor":        None,   # preenchido na etapa de qualitativos
        "segmento":     None,
        "gestora":      None,
        "tickers":      tickers,
        "ativo_na_b3":  len(tickers) > 0,
        "atualizado_em": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("ativos_rv").upsert(record, on_conflict="codigo_b3").execute()

# ── Leitura do CSV ────────────────────────────────────────────

def ler_csv(path: str) -> list[dict]:
    fiis = []
    with open(path, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            codigo = row.get("Código", "").strip()
            nome   = row.get("Razão Social", "").strip()
            if codigo and nome:
                fiis.append({"codigo_b3": codigo, "nome": nome})
    return fiis

# ── Main ──────────────────────────────────────────────────────

def main():
    checkpoint  = load_checkpoint()
    processados = set(checkpoint.get("processados", []))

    fiis = ler_csv(CSV_PATH)
    pendentes = [f for f in fiis if f["codigo_b3"] not in processados]

    print(f"Total no CSV: {len(fiis)} | Já processados: {len(processados)} | Pendentes: {len(pendentes)}\n")

    com_ticker  = 0
    sem_ticker  = 0
    erros       = []

    for i, fii in enumerate(pendentes):
        codigo = fii["codigo_b3"]
        nome   = fii["nome"]
        ticker = f"{codigo}11"

        if ticker_existe(ticker):
            tickers = [ticker]
            com_ticker += 1
            print(f"  ✓ {ticker} | {nome[:50]}")
        else:
            tickers = []
            sem_ticker += 1
            print(f"  ✗ {codigo} — ticker {ticker} não encontrado")

        try:
            upsert_fii(codigo_b3=codigo, nome=nome, tickers=tickers)
        except Exception as e:
            print(f"  [ERRO] {codigo}: {e}")
            erros.append(codigo)

        processados.add(codigo)

        if (i + 1) % 20 == 0:
            checkpoint["processados"] = list(processados)
            save_checkpoint(checkpoint)
            print(f"  [{i+1}/{len(pendentes)}] checkpoint salvo")

        time.sleep(DELAY)

    checkpoint["processados"] = list(processados)
    save_checkpoint(checkpoint)

    print(f"\n=== Concluído ===")
    print(f"  Com ticker ativo: {com_ticker}")
    print(f"  Sem ticker:       {sem_ticker}")
    print(f"  Erros:            {len(erros)}")

if __name__ == "__main__":
    main()