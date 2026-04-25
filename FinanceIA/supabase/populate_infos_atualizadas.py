"""
populate_infos_atualizadas.py
Popula a tabela fundo_infos_atualizadas no Supabase a partir do Excel de dados de mercado (CVM).

Tabela fundo_infos_atualizadas: dados quantitativos dinâmicos por fundo
  - rentabilidade 12/24/36m e desde o início
  - PL atual, captação líquida
  - volatilidade, Sharpe
  - histórico mensal de PL (colunas dinâmicas)

Uso:
    python populate_infos_atualizadas.py
    python populate_infos_atualizadas.py --input dados_mercado_cvm.xlsx
"""

import argparse
import json
import math
import re
import time
from datetime import datetime

import pandas as pd
from supabase import create_client

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

SUPABASE_URL = "SUA_PROJECT_URL"
SUPABASE_KEY = "SUA_SERVICE_ROLE_KEY"
EXCEL_PATH   = "dados_mercado_cvm.xlsx"
SHEET_NAME   = "Dados de Mercado"
PROGRESS_FILE = "progress_infos.json"
BATCH_SIZE    = 50

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def safe(val):
    if val is None:
        return None
    try:
        if isinstance(val, float) and math.isnan(val):
            return None
    except Exception:
        pass
    return val


def normalizar_cnpj(cnpj) -> str:
    if not cnpj:
        return ""
    return re.sub(r'[^0-9]', '', str(cnpj))


def carregar_progresso() -> dict:
    try:
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"processados": []}


def salvar_progresso(prog: dict) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(prog, f)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=EXCEL_PATH)
    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print(f"📂 Carregando {args.input}...")
    df = pd.read_excel(args.input, sheet_name=SHEET_NAME)
    df = df.fillna(value=float('nan'))
    print(f"   {len(df)} fundos | {len(df.columns)} colunas")

    progresso  = carregar_progresso()
    processados = set(progresso["processados"])

    pendentes = [row for _, row in df.iterrows() if normalizar_cnpj(row.get("CNPJ")) not in processados]
    print(f"   Pendentes: {len(pendentes)} | Já processados: {len(processados)}")

    # Identifica colunas de PL mensal (formato PL_YYYY/MM)
    pl_cols = [c for c in df.columns if str(c).startswith("PL_")]

    ok_count = 0
    err_count = 0

    for i in range(0, len(pendentes), BATCH_SIZE):
        batch_rows = pendentes[i:i+BATCH_SIZE]
        batch_data = []

        for row in batch_rows:
            cnpj = normalizar_cnpj(row.get("CNPJ"))
            if not cnpj:
                continue

            # Histórico PL como JSON
            pl_historico = {}
            for col in pl_cols:
                mes = col.replace("PL_", "")
                val = safe(row.get(col))
                if val is not None:
                    pl_historico[mes] = float(val)

            registro = {
                "cnpj":               cnpj,
                "data_referencia":    str(safe(row.get("Data_Ref")) or ""),
                "rent_12m":           safe(row.get("Rent_12m_%")),
                "rent_24m":           safe(row.get("Rent_24m_%")),
                "rent_36m":           safe(row.get("Rent_36m_%")),
                "rent_desde_inicio":  safe(row.get("Rent_Inicio_%")),
                "pl_atual":           safe(row.get("PL_Atual")),
                "nr_cotistas":        safe(row.get("Cotistas")),
                "captacao_30d":       safe(row.get("Capt_30d")),
                "resgate_30d":        safe(row.get("Resg_30d")),
                "pl_historico":       pl_historico if pl_historico else None,
                "atualizado_em":      datetime.utcnow().isoformat(),
            }
            # Remove valores None para não sobrescrever com null
            registro = {k: v for k, v in registro.items() if v is not None}
            batch_data.append(registro)

        if not batch_data:
            continue

        try:
            supabase.table("fundo_infos_atualizadas") \
                .upsert(batch_data, on_conflict="cnpj") \
                .execute()
            for d in batch_data:
                processados.add(d["cnpj"])
            progresso["processados"] = list(processados)
            salvar_progresso(progresso)
            ok_count += len(batch_data)
            print(f"   ✅ Batch {i//BATCH_SIZE + 1}: {len(batch_data)} registros inseridos")
        except Exception as e:
            err_count += len(batch_data)
            print(f"   ❌ ERRO batch {i//BATCH_SIZE + 1}: {e}")
        time.sleep(0.2)

    print(f"\n✅ Concluído! {ok_count} inseridos | {err_count} erros")


if __name__ == "__main__":
    main()
