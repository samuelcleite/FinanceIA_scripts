import os
"""
marcar_incentivadas_cetip.py
==============================
Marca debêntures incentivadas (tributacao = "Isento") usando correlação
DIRETA via Código CETIP da planilha ANBIMA Lei 12.431.

Isso substitui a heurística anterior (por emissor) com acurácia de 100%
para os títulos presentes na planilha ANBIMA.

Fonte: Planilha ANBIMA de Acompanhamento dos Projetos que emitiram
       Debêntures Incentivadas - Lei 12.431 (consolidado histórico)
       Disponível em: anbima.com.br -> Informar -> Estatísticas ->
       Mercado de Capitais -> Projetos e Emissões Incentivadas

GERA RELATÓRIO COMPLETO de auditoria em JSON.

MODO DE EXECUÇÃO:
  --dry-run (padrão): gera relatório SEM alterar o Supabase
  --apply: aplica as alterações

Uso:
  python marcar_incentivadas_cetip.py              # dry-run
  python marcar_incentivadas_cetip.py --apply       # aplica
"""

import sys
import json
import pandas as pd
from datetime import datetime
from supabase import create_client

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

XLSX_ANBIMA = "Tabela_12_431_042024_Copia_04f6ca4b59.xlsx"
RELATORIO = "relatorio_incentivadas_cetip.json"

DRY_RUN = "--apply" not in sys.argv

# ─── LISTAS DE EXCEÇÃO ───────────────────────────────────────────────────────
# Preencha após revisar o relatório JSON do dry-run.
# Tickers que estão na planilha ANBIMA mas você quer forçar como Longo Prazo.
FORCADO_LONGO_PRAZO = [
    # "CART13",
]


# ─── PIPELINE ────────────────────────────────────────────────────────────────

def main():
    modo = "DRY-RUN" if DRY_RUN else "APPLY"
    print("=" * 60)
    print(f"MARCAR INCENTIVADAS — Código CETIP ANBIMA -> Supabase")
    print(f"MODO: {modo}")
    print("=" * 60)

    if not DRY_RUN:
        print("\nATENCAO: Modo --apply. Alteracoes serao feitas no Supabase.")
        print("Pressione ENTER para continuar ou Ctrl+C para cancelar...")
        input()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── 1. Carregar planilha ANBIMA ─────────────────────────────────────
    print("\n[1/3] Carregando planilha ANBIMA Lei 12.431...")

    df1 = pd.read_excel(XLSX_ANBIMA, sheet_name="Pág. 2 - Debêntures Art. 1º", header=2)
    df2 = pd.read_excel(XLSX_ANBIMA, sheet_name="Pág. 3 - Debêntures Art. 2º", header=2)

    # Montar set de códigos incentivados com contexto
    incentivadas = {}
    for _, row in df1.iterrows():
        cod = row.get("Código CETIP")
        if pd.notna(cod):
            incentivadas[str(cod).strip()] = {
                "lei": "Art. 1º (investimento)",
                "emissor_anbima": str(row.get("Companhia Emissora", "")),
                "setor": str(row.get("Setor", "")),
                "indexador": str(row.get("Indexador", "")),
                "vencimento": str(row.get("Data de Vencimento das Debêntures", "")),
            }

    for _, row in df2.iterrows():
        cod = row.get("Código CETIP")
        if pd.notna(cod):
            incentivadas[str(cod).strip()] = {
                "lei": "Art. 2º (infraestrutura)",
                "emissor_anbima": str(row.get("Companhia Emissora", "")),
                "setor": str(row.get("Setor", "")),
                "indexador": str(row.get("Indexador", "")),
                "vencimento": str(row.get("Data de Vencimento das Debêntures", "")),
            }

    print(f"  -> {len(incentivadas)} debêntures incentivadas na planilha ANBIMA")
    print(f"     Art. 1º (investimento): {len(df1.dropna(subset=['Código CETIP']))}")
    print(f"     Art. 2º (infraestrutura): {len(df2.dropna(subset=['Código CETIP']))}")

    # ── 2. Buscar emissões do Supabase ──────────────────────────────────
    print("\n[2/3] Buscando emissoes no Supabase...")
    emissoes = supabase.table("rf_emissoes").select(
        "id, ticker, nome, tributacao"
    ).eq("tipo_produto", "debenture").execute()

    print(f"  -> {len(emissoes.data)} debentures no Supabase")

    # ── 3. Cruzar e marcar ──────────────────────────────────────────────
    print("\n[3/3] Cruzando e marcando incentivadas...")

    log = []
    stats = {
        "total": len(emissoes.data),
        "ja_isento": 0,
        "marcar_isento": 0,
        "manter_longo_prazo": 0,
        "forcado_longo_prazo": 0,
        "sem_match_planilha": 0,
    }

    for emissao in emissoes.data:
        ticker = emissao.get("ticker", "")
        tributacao_atual = emissao.get("tributacao", "")
        info_anbima = incentivadas.get(ticker)

        entry = {
            "emissao_id": emissao["id"],
            "ticker": ticker,
            "nome": emissao["nome"],
            "tributacao_atual": tributacao_atual,
        }

        if ticker in FORCADO_LONGO_PRAZO:
            entry["decisao"] = "forcado_longo_prazo"
            entry["acao"] = "manter"
            entry["motivo"] = "ticker em FORCADO_LONGO_PRAZO"
            stats["forcado_longo_prazo"] += 1

        elif info_anbima:
            entry["encontrado_na_planilha"] = True
            entry["lei"] = info_anbima["lei"]
            entry["emissor_anbima"] = info_anbima["emissor_anbima"]
            entry["setor"] = info_anbima["setor"]
            entry["indexador_planilha"] = info_anbima["indexador"]

            if tributacao_atual == "Isento":
                entry["decisao"] = "ja_isento"
                entry["acao"] = "nenhuma"
                stats["ja_isento"] += 1
            else:
                entry["decisao"] = "marcar_isento"
                entry["acao"] = "alterar_para_isento"
                entry["tributacao_nova"] = "Isento"
                stats["marcar_isento"] += 1

                if not DRY_RUN:
                    try:
                        supabase.table("rf_emissoes").update(
                            {"tributacao": "Isento"}
                        ).eq("id", emissao["id"]).execute()
                        entry["resultado"] = "sucesso"
                    except Exception as e:
                        entry["resultado"] = f"erro: {str(e)}"
                else:
                    entry["resultado"] = "dry-run"

        else:
            entry["encontrado_na_planilha"] = False
            entry["decisao"] = "manter_longo_prazo"
            entry["acao"] = "nenhuma"
            stats["sem_match_planilha"] += 1

        log.append(entry)

    # ── Salvar relatório ────────────────────────────────────────────────
    relatorio = {
        "timestamp": datetime.now().isoformat(),
        "modo": "dry-run" if DRY_RUN else "apply",
        "fonte": XLSX_ANBIMA,
        "resumo": stats,
        "detalhes": log,
    }

    with open(RELATORIO, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)

    # ── Resumo ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  Modo:                {modo}")
    print(f"  Total debentures:    {stats['total']}")
    print(f"  Ja estavam Isento:   {stats['ja_isento']}")
    print(f"  Marcadas Isento:     {stats['marcar_isento']}")
    print(f"  Forcado Longo Prazo: {stats['forcado_longo_prazo']}")
    print(f"  Sem match planilha:  {stats['sem_match_planilha']} (serao Longo Prazo)")
    print(f"  Relatorio:           {RELATORIO}")
    print("=" * 60)

    if DRY_RUN:
        print("\nRevise o relatorio e execute com --apply para aplicar:")
        print(f"  python marcar_incentivadas_cetip.py --apply")


if __name__ == "__main__":
    main()