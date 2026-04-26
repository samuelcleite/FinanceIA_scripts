import os
"""
populate_supabase.py
Popula as tabelas fundos e fundo_plataformas no Supabase a partir do Excel consolidado.

Tabelas:
  - fundos: dados qualitativos e estruturais (1 linha por CNPJ único)
  - fundo_plataformas: disponibilidade por plataforma (N linhas por fundo)

Uso:
    python populate_supabase.py
    python populate_supabase.py --reset   # apaga tudo e reinserere
"""

import argparse
import json
import math
import time
from datetime import datetime

import pandas as pd
from supabase import create_client

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")        # ex: https://abcdef.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")   # service_role key (admin)
EXCEL_PATH    = "fundos_revisados_total.xlsx"
PROGRESS_FILE = "progress_fundos.json"

PLATAFORMAS = ["BTG", "Bradesco", "Inter", "Itau", "Santander", "XP"]

BATCH_SIZE = 50   # registros por insert batch

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def safe(val):
    """Converte NaN/None para None (compatível com Supabase)."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def normalizar_cnpj(cnpj) -> str:
    import re
    if not cnpj:
        return ""
    return re.sub(r'[^0-9]', '', str(cnpj))


def carregar_progresso() -> dict:
    try:
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"fundos": [], "plataformas": []}


def salvar_progresso(prog: dict) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(prog, f, ensure_ascii=False)


def linha_para_fundo(row: dict) -> dict:
    """Converte linha do Excel para dicionário da tabela fundos."""
    return {
        "cnpj":                  normalizar_cnpj(safe(row.get("CNPJ"))),
        "nome":                  safe(row.get("Nome")),
        "gestor":                safe(row.get("Gestor")),
        "categoria":             safe(row.get("Categoria")),
        "subcategoria":          safe(row.get("Subcategoria")),
        "tipo_produto":          "Fundo",
        "indexador":             safe(row.get("Indexador")),
        "benchmark":             safe(row.get("Benchmark")),
        "liquidez_descricao":    safe(row.get("Liquidez")),
        "tributacao":            safe(row.get("Tributação")),
        "descricao_tributacao":  safe(row.get("Descrição Tributação")),
        "come_cotas":            str(safe(row.get("Come-Cotas")) or "").upper() == "SIM",
        "taxa_adm":              safe(row.get("Taxa de Adm")),
        "taxa_performance":      safe(row.get("Taxa de Performance")),
        "publico_alvo":          safe(row.get("Público Alvo")),
        "horizonte_minimo_anos": safe(row.get("Horizonte Mínimo (anos)")),
        "quando_indicar":        safe(row.get("Quando Indicar")),
        "quando_nao_indicar":    safe(row.get("Quando não indicar")),
        "vantagens":             safe(row.get("Vantagens")),
        "desvantagens":          safe(row.get("Desvantagens")),
        "alertas":               safe(row.get("Alertas")),
        "descricao_simples":     safe(row.get("Descrição Simples")),
        "descricao_tecnica":     safe(row.get("Descrição Técnica")),
        "validacao_status":      safe(row.get("validacao_status")),
        "validacao_obs":         safe(row.get("validacao_obs")),
        "atualizado_em":         datetime.utcnow().isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Apaga dados existentes antes de inserir")
    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.reset:
        print("⚠️  Modo --reset: apagando dados existentes...")
        supabase.table("fundo_plataformas").delete().neq("cnpj", "").execute()
        supabase.table("fundos").delete().neq("cnpj", "").execute()
        print("   Tabelas limpas.")

    # Carrega Excel
    print(f"\n📂 Carregando {EXCEL_PATH}...")
    df = pd.read_excel(EXCEL_PATH, sheet_name="Fundos Revisados")
    df = df.fillna("")
    print(f"   {len(df)} linhas carregadas")

    progresso = carregar_progresso()
    ja_inseridos = set(progresso.get("fundos", []))

    # ── Tabela fundos ────────────────────────────────────────────────────────
    print(f"\n🏦 Inserindo na tabela fundos...")
    pendentes = [row for _, row in df.iterrows() if normalizar_cnpj(row.get("CNPJ")) not in ja_inseridos]
    print(f"   {len(pendentes)} pendentes | {len(ja_inseridos)} já inseridos")

    for i in range(0, len(pendentes), BATCH_SIZE):
        batch_rows = pendentes[i:i+BATCH_SIZE]
        batch_data = [linha_para_fundo(dict(row)) for row in batch_rows]
        # Remove entradas sem CNPJ
        batch_data = [d for d in batch_data if d.get("cnpj")]

        try:
            supabase.table("fundos").upsert(batch_data, on_conflict="cnpj").execute()
            for d in batch_data:
                ja_inseridos.add(d["cnpj"])
            progresso["fundos"] = list(ja_inseridos)
            salvar_progresso(progresso)
            print(f"   ✅ Batch {i//BATCH_SIZE + 1}: {len(batch_data)} fundos inseridos")
        except Exception as e:
            print(f"   ❌ ERRO batch fundos: {e}")
        time.sleep(0.2)

    # ── Tabela fundo_plataformas ─────────────────────────────────────────────
    print(f"\n🔗 Inserindo na tabela fundo_plataformas...")

    # Verifica se há aba "Disponibilidade"
    try:
        df_disp = pd.read_excel(EXCEL_PATH, sheet_name="Disponibilidade")
        df_disp = df_disp.fillna("")
    except Exception:
        print("   ⚠️  Aba 'Disponibilidade' não encontrada — pulando fundo_plataformas")
        df_disp = pd.DataFrame()

    if not df_disp.empty:
        ja_plataformas = set(progresso.get("plataformas", []))
        registros_plat = []

        for _, row in df_disp.iterrows():
            cnpj = normalizar_cnpj(str(row.get("CNPJ", "")))
            if not cnpj:
                continue
            for plat in PLATAFORMAS:
                disponivel = str(row.get(plat, "")).strip().upper() in ["X", "✓", "SIM", "1", "TRUE"]
                if disponivel:
                    chave = f"{cnpj}_{plat}"
                    if chave not in ja_plataformas:
                        registros_plat.append({
                            "cnpj":       cnpj,
                            "plataforma": plat.lower(),
                        })

        for i in range(0, len(registros_plat), BATCH_SIZE):
            batch = registros_plat[i:i+BATCH_SIZE]
            try:
                supabase.table("fundo_plataformas").upsert(batch, on_conflict="cnpj,plataforma").execute()
                for r in batch:
                    ja_plataformas.add(f"{r['cnpj']}_{r['plataforma']}")
                progresso["plataformas"] = list(ja_plataformas)
                salvar_progresso(progresso)
                print(f"   ✅ Batch plataformas {i//BATCH_SIZE + 1}: {len(batch)} registros")
            except Exception as e:
                print(f"   ❌ ERRO batch plataformas: {e}")
            time.sleep(0.2)

    print(f"\n✅ Importação concluída!")
    print(f"   Fundos inseridos: {len(ja_inseridos)}")


if __name__ == "__main__":
    main()
