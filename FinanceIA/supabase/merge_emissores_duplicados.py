import os
"""
merge_emissores_duplicados.py
==============================
Lê o Excel de auditoria (auditoria_emissores.xlsx), processa as linhas
marcadas como DUPLICATA e:
  1. Redireciona todas as rf_emissoes do ID_REMOVER para o ID_MANTER
  2. Deleta o emissor ID_REMOVER

MODO DE EXECUÇÃO:
  --dry-run (padrão): mostra o que faria SEM alterar o Supabase
  --apply: executa as alterações

Uso:
  python merge_emissores_duplicados.py              # dry-run
  python merge_emissores_duplicados.py --apply       # aplica
"""

import sys
import json
from datetime import datetime
import pandas as pd
from supabase import create_client

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

EXCEL_PATH = "auditoria_emissores.xlsx"
RELATORIO = "relatorio_merge_emissores.json"

DRY_RUN = "--apply" not in sys.argv


# ─── PIPELINE ────────────────────────────────────────────────────────────────

def main():
    modo = "DRY-RUN" if DRY_RUN else "APPLY"
    print("=" * 60)
    print(f"MERGE EMISSORES DUPLICADOS")
    print(f"MODO: {modo}")
    print("=" * 60)

    if not DRY_RUN:
        print("\nATENCAO: Modo --apply. Alteracoes serao feitas no Supabase.")
        print("Pressione ENTER para continuar ou Ctrl+C para cancelar...")
        input()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── 1. Ler Excel ─────────────────────────────────────────────────────
    print(f"\n[1/2] Lendo Excel: {EXCEL_PATH}")

    df = pd.read_excel(EXCEL_PATH, sheet_name="Duplicatas", header=2)
    df.columns = [c.strip() for c in df.columns]

    # Filtrar apenas linhas marcadas como DUPLICATA
    duplicatas = df[df.iloc[:, 0].str.strip().str.upper() == "DUPLICATA"].copy()
    print(f"  -> {len(duplicatas)} linhas marcadas como DUPLICATA")

    if len(duplicatas) == 0:
        print("\nNenhuma duplicata marcada. Abra o Excel, marque as linhas como DUPLICATA e rode novamente.")
        return

    # ── 2. Executar merges ───────────────────────────────────────────────
    print(f"\n[2/2] Executando merges...")

    log = []
    merges_ok = 0
    erros = 0

    # Processar agrupando por ID_MANTER para evitar conflitos
    # (um mesmo ID_MANTER pode aparecer várias vezes)
    grupos = {}
    for _, row in duplicatas.iterrows():
        id_manter = str(row.iloc[1]).strip()
        id_remover = str(row.iloc[4]).strip()
        nome_manter = str(row.iloc[2]).strip()
        nome_remover = str(row.iloc[5]).strip()

        if id_manter not in grupos:
            grupos[id_manter] = {"nome": nome_manter, "remover": []}
        grupos[id_manter]["remover"].append({"id": id_remover, "nome": nome_remover})

    for id_manter, dados in grupos.items():
        nome_manter = dados["nome"]
        print(f"\n  MANTER: {nome_manter} ({id_manter})")

        for item in dados["remover"]:
            id_remover = item["id"]
            nome_remover = item["nome"]

            entry = {
                "id_manter": id_manter,
                "nome_manter": nome_manter,
                "id_remover": id_remover,
                "nome_remover": nome_remover,
            }

            # Contar emissoes que serão redirecionadas
            try:
                count_res = supabase.table("rf_emissoes").select("id", count="exact").eq("emissor_id", id_remover).execute()
                qtd_emissoes = count_res.count
                entry["emissoes_redirecionadas"] = qtd_emissoes
                print(f"    REMOVER: {nome_remover} ({id_remover}) — {qtd_emissoes} emissoes")
            except Exception as e:
                entry["erro_contagem"] = str(e)
                qtd_emissoes = 0

            if not DRY_RUN:
                # Redirecionar emissoes
                try:
                    supabase.table("rf_emissoes").update(
                        {"emissor_id": id_manter}
                    ).eq("emissor_id", id_remover).execute()
                    entry["redirect_resultado"] = "sucesso"
                    print(f"      -> {qtd_emissoes} emissoes redirecionadas para {id_manter}")
                except Exception as e:
                    entry["redirect_resultado"] = f"erro: {str(e)}"
                    print(f"      ERRO ao redirecionar: {e}")
                    erros += 1
                    log.append(entry)
                    continue

                # Deletar emissor duplicado
                try:
                    supabase.table("rf_emissores").delete().eq("id", id_remover).execute()
                    entry["delete_resultado"] = "sucesso"
                    print(f"      -> Emissor {id_remover} deletado")
                    merges_ok += 1
                except Exception as e:
                    entry["delete_resultado"] = f"erro: {str(e)}"
                    print(f"      ERRO ao deletar: {e}")
                    erros += 1
            else:
                entry["redirect_resultado"] = "dry-run"
                entry["delete_resultado"] = "dry-run"
                merges_ok += 1
                print(f"      [DRY-RUN] Redirecionaria {qtd_emissoes} emissoes e deletaria {id_remover}")

            log.append(entry)

    # Salvar relatório
    relatorio = {
        "timestamp": datetime.now().isoformat(),
        "modo": "dry-run" if DRY_RUN else "apply",
        "merges_processados": merges_ok,
        "erros": erros,
        "detalhes": log,
    }

    with open(RELATORIO, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Modo:              {modo}")
    print(f"  Merges:            {merges_ok}")
    print(f"  Erros:             {erros}")
    print(f"  Relatorio:         {RELATORIO}")
    print("=" * 60)

    if DRY_RUN:
        print("\nRevise o relatorio e execute com --apply:")
        print("  python merge_emissores_duplicados.py --apply")


if __name__ == "__main__":
    main()