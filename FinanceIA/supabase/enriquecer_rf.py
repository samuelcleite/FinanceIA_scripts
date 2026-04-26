import os
"""
enriquecer_debentures_cvm.py
==============================
Cruza dados de debêntures do Supabase (vindos da ANBIMA) com dados
da CVM para enriquecer com: CNPJ do emissor, flag de incentivada
(Lei 12.431 → tributação Isento), e espécie/garantia.

GERA DOIS RELATÓRIOS DE AUDITORIA:
1. relatorio_cruzamento_emissores.json — detalhe de cada match de emissor
2. relatorio_incentivadas.json — detalhe de cada decisão de tributação

Estratégia de cruzamento:
1. Normaliza nomes de emissores (remove acentos, S/A, pontuação)
2. Match exato por nome normalizado
3. Match parcial por primeiras palavras
4. Para incentivada: se o emissor tem PELO MENOS 1 emissão incentivada
   na CVM com indexador IPCA, E a debênture no Supabase é indexada a IPCA,
   marca como provável incentivada

MODO DE EXECUÇÃO:
  --dry-run (padrão): gera relatórios SEM alterar o Supabase
  --apply: gera relatórios E aplica as alterações no Supabase

Uso:
  python enriquecer_debentures_cvm.py              # dry-run (só gera relatórios)
  python enriquecer_debentures_cvm.py --apply       # aplica alterações
"""

import csv
import json
import sys
import unicodedata
from datetime import datetime
from supabase import create_client

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CSV_CVM = "oferta_distribuicao.csv"

RELATORIO_EMISSORES = "relatorio_cruzamento_emissores.json"
RELATORIO_INCENTIVADAS = "relatorio_incentivadas.json"

DRY_RUN = "--apply" not in sys.argv

# --- LISTAS DE EXCLUSAO ---------------------------------------------------
# Preencha apos revisar os relatorios JSON do dry-run.

# Emissores com match incorreto com a CVM -- NAO receberao CNPJ.
# Copie o campo "emissor_id" do relatorio_cruzamento_emissores.json.
EXCLUIR_CNPJ_EMISSORES = [
    "7351bb13-958e-4714-b611-1e5aa012bac1",
    "84dc8c84-bbdb-40e4-aad6-2841c7199293",
    "ce92f784-eec8-43ef-857d-490e2ce02228",
    "160387b3-6d8e-467a-bb46-7001a31c69d9",
    "94fba427-92aa-4b06-a60e-57c1a75cb709",
    "4e226df6-7b56-45ef-9bd1-021303233e55",
    "3e6f2467-ce92-41e0-a10c-459657edc6a6",
    "0458400c-1b6e-47a7-8119-ae9a72ab66e4",
    "a4e9dd42-090f-4ce3-8016-95c6abf0ab83",
    "91c6efc4-3fbb-4154-99b7-925c14facd89",
    "8d80e75d-c9e6-4cdd-a135-cf78ba93ad69",
    "734b902d-89a1-40e6-a9c3-4936c2c5e52a",
    "dfc097b2-5e1b-4beb-9584-34bc6dd41d6a",
    "dfc097b2-5e1b-4beb-9584-34bc6dd41d6a",
    "54e4f16c-208c-4bbf-81fd-e94ef74be07f",
    "adbc2505-9452-4330-a2ee-b3ea6914184d",
    "cb5930cc-5199-4df9-b342-51ebac959c0f",
    "e3e04904-6429-4eab-b724-d231b31cdb8e",
    "cf42f074-1374-40a6-9177-588b04efa602",
    "a06e3183-1086-492e-bc79-fc72f6d96914",


    # "id-do-emissor-errado-1",
    # "id-do-emissor-errado-2",
]

# Debentures classificadas como incentivadas incorretamente.
# Serao mantidas como "Longo Prazo" mesmo que a heuristica diga Isento.
# Copie o campo "emissao_id" do relatorio_incentivadas.json.
FORCADO_LONGO_PRAZO = [
    # "id-da-emissao-errada-1",
    # "id-da-emissao-errada-2",
]
# --------------------------------------------------------------------------

# ─── FUNÇÕES AUXILIARES ──────────────────────────────────────────────────────

def normalizar(nome):
    """Normaliza nome para comparação."""
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    nome = nome.upper()
    for suf in [" S/A", " S.A.", " SA", " LTDA", " S.A", " - RESP LIMITADA",
                " PARTICIPACOES", " PARTICIPAÇÕES"]:
        nome = nome.replace(suf, "")
    nome = nome.replace(".", "").replace("-", " ").replace("/", " ")
    nome = " ".join(nome.split())
    return nome.strip()


def primeiras_palavras(nome_norm, n=3):
    """Retorna tupla com as primeiras n palavras."""
    return tuple(nome_norm.split()[:n])


# ─── CARREGAR CVM ───────────────────────────────────────────────────────────

def carregar_cvm(csv_path):
    """Carrega dados da CVM e retorna dicts para cruzamento."""
    emissores = {}

    with open(csv_path, "r", encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            tipo = row.get("Tipo_Ativo", "")
            cnpj = row.get("CNPJ_Emissor", "").strip()
            nome = row.get("Nome_Emissor", "").strip()
            incentivo = row.get("Oferta_Incentivo_Fiscal", "").strip().upper() == "S"
            especie = row.get("Especie_Ativo", "").strip()
            idx_cvm = row.get("Atualizacao_Monetaria", "").strip().upper()

            if "deb" not in tipo.lower() or not cnpj:
                continue

            nome_norm = normalizar(nome)

            if nome_norm not in emissores:
                emissores[nome_norm] = {
                    "nome_original_cvm": nome,
                    "cnpj": cnpj,
                    "tem_incentivada": False,
                    "tem_incentivada_ipca": False,
                    "tem_incentivada_di": False,
                    "especies": [],
                    "total_emissoes_cvm": 0,
                }

            emissores[nome_norm]["total_emissoes_cvm"] += 1

            if especie and especie not in emissores[nome_norm]["especies"]:
                emissores[nome_norm]["especies"].append(especie)

            if incentivo:
                emissores[nome_norm]["tem_incentivada"] = True
                if "IPCA" in idx_cvm:
                    emissores[nome_norm]["tem_incentivada_ipca"] = True
                elif "DI" in idx_cvm or "CDI" in idx_cvm or not idx_cvm:
                    emissores[nome_norm]["tem_incentivada_di"] = True

    # Índice parcial
    indice_parcial = {}
    for nome_norm, data in emissores.items():
        chave = primeiras_palavras(nome_norm)
        if chave not in indice_parcial:
            indice_parcial[chave] = []
        indice_parcial[chave].append((nome_norm, data))

    return emissores, indice_parcial


def buscar_emissor_cvm(nome_anbima, emissores_cvm, indice_parcial):
    """Tenta encontrar o emissor na CVM por nome normalizado."""
    nome_norm = normalizar(nome_anbima)

    # 1. Match exato
    if nome_norm in emissores_cvm:
        return emissores_cvm[nome_norm], "exato", nome_norm

    # 2. Match por primeiras 3 palavras
    chave = primeiras_palavras(nome_norm)
    if chave in indice_parcial:
        candidatos = indice_parcial[chave]
        if len(candidatos) == 1:
            return candidatos[0][1], "parcial_3", candidatos[0][0]

    # 3. Match por primeiras 2 palavras
    chave2 = primeiras_palavras(nome_norm, 2)
    for chave_cvm, candidatos in indice_parcial.items():
        if chave_cvm[:2] == chave2:
            if len(candidatos) == 1:
                return candidatos[0][1], "parcial_2", candidatos[0][0]

    return None, "sem_match", None


# ─── PIPELINE ────────────────────────────────────────────────────────────────

def main():
    modo = "DRY-RUN (nenhuma alteracao sera feita)" if DRY_RUN else "APPLY (alteracoes serao aplicadas!)"
    print("=" * 60)
    print(f"ENRIQUECER DEBENTURES — CVM -> Supabase")
    print(f"MODO: {modo}")
    print("=" * 60)

    if not DRY_RUN:
        print("\n  ATENCAO: Modo --apply ativo. Alteracoes serao feitas no Supabase.")
        print("  Pressione ENTER para continuar ou Ctrl+C para cancelar...")
        input()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── 1. Carregar CVM ─────────────────────────────────────────────────
    print("\n[1/3] Carregando dados CVM...")
    emissores_cvm, indice_parcial = carregar_cvm(CSV_CVM)
    print(f"  -> {len(emissores_cvm)} emissores unicos na CVM")

    # ── 2. Cruzar emissores ─────────────────────────────────────────────
    print("\n[2/3] Cruzando emissores com CVM...")

    emissores_db = supabase.table("rf_emissores").select("id, nome, cnpj").eq("tipo", "empresa").execute()
    print(f"  -> {len(emissores_db.data)} emissores no Supabase")

    stats = {"exato": 0, "parcial_3": 0, "parcial_2": 0, "sem_match": 0}
    log_emissores = []

    for emissor in emissores_db.data:
        cvm_data, match_tipo, nome_cvm_norm = buscar_emissor_cvm(
            emissor["nome"], emissores_cvm, indice_parcial
        )
        stats[match_tipo] += 1

        entry = {
            "emissor_id": emissor["id"],
            "nome_anbima": emissor["nome"],
            "nome_normalizado": normalizar(emissor["nome"]),
            "match_tipo": match_tipo,
            "cnpj_atual": emissor.get("cnpj"),
        }

        if emissor["id"] in EXCLUIR_CNPJ_EMISSORES:
            entry["acao"] = "excluido_manualmente"
            entry["resultado"] = "ignorado por EXCLUIR_CNPJ_EMISSORES"
            log_emissores.append(entry)
            continue

        if cvm_data:
            entry["nome_cvm"] = cvm_data["nome_original_cvm"]
            entry["cnpj_cvm"] = cvm_data["cnpj"]
            entry["tem_incentivada"] = cvm_data["tem_incentivada"]
            entry["tem_incentivada_ipca"] = cvm_data["tem_incentivada_ipca"]
            entry["tem_incentivada_di"] = cvm_data["tem_incentivada_di"]
            entry["especies_cvm"] = cvm_data["especies"]
            entry["total_emissoes_cvm"] = cvm_data["total_emissoes_cvm"]

            # Atualizar CNPJ se não tem
            if not emissor.get("cnpj") and cvm_data["cnpj"]:
                entry["acao"] = "atualizar_cnpj"
                entry["cnpj_novo"] = cvm_data["cnpj"]

                if not DRY_RUN:
                    try:
                        supabase.table("rf_emissores").update(
                            {"cnpj": cvm_data["cnpj"]}
                        ).eq("id", emissor["id"]).execute()
                        entry["resultado"] = "sucesso"
                    except Exception as e:
                        entry["resultado"] = f"erro: {str(e)}"
                else:
                    entry["resultado"] = "dry-run"
            else:
                entry["acao"] = "nenhuma"
        else:
            entry["acao"] = "sem_match_cvm"

        log_emissores.append(entry)

    # Salvar relatório de emissores
    relatorio_emissores = {
        "timestamp": datetime.now().isoformat(),
        "modo": "dry-run" if DRY_RUN else "apply",
        "resumo": {
            "total_supabase": len(emissores_db.data),
            "match_exato": stats["exato"],
            "match_parcial_3": stats["parcial_3"],
            "match_parcial_2": stats["parcial_2"],
            "sem_match": stats["sem_match"],
            "match_rate_pct": round(100 * (stats["exato"] + stats["parcial_3"] + stats["parcial_2"]) / max(len(emissores_db.data), 1), 1),
        },
        "detalhes": log_emissores,
    }

    with open(RELATORIO_EMISSORES, "w", encoding="utf-8") as f:
        json.dump(relatorio_emissores, f, ensure_ascii=False, indent=2)

    print(f"  Match exato: {stats['exato']}")
    print(f"  Match parcial (3 palavras): {stats['parcial_3']}")
    print(f"  Match parcial (2 palavras): {stats['parcial_2']}")
    print(f"  Sem match: {stats['sem_match']}")
    print(f"  -> Relatorio salvo em {RELATORIO_EMISSORES}")

    # ── 3. Identificar incentivadas ─────────────────────────────────────
    print("\n[3/3] Identificando debentures incentivadas...")

    emissoes_db = supabase.table("rf_emissoes").select(
        "id, nome, emissor_id, indexador, tributacao"
    ).eq("tipo_produto", "debenture").execute()

    emissor_nome_map = {e["id"]: e["nome"] for e in emissores_db.data}

    log_incentivadas = []
    incentivadas = 0
    mantidas = 0
    sem_info = 0

    for emissao in emissoes_db.data:
        emissor_nome = emissor_nome_map.get(emissao["emissor_id"], "")
        cvm_data, match_tipo, _ = buscar_emissor_cvm(emissor_nome, emissores_cvm, indice_parcial)

        entry = {
            "emissao_id": emissao["id"],
            "nome_emissao": emissao["nome"],
            "emissor": emissor_nome,
            "indexador": emissao["indexador"],
            "tributacao_atual": emissao.get("tributacao"),
        }

        if not cvm_data:
            entry["decisao"] = "sem_dados_cvm"
            entry["acao"] = "nenhuma"
            sem_info += 1
        else:
            entry["emissor_tem_incentivada"] = cvm_data["tem_incentivada"]
            entry["emissor_tem_incentivada_ipca"] = cvm_data["tem_incentivada_ipca"]
            entry["emissor_tem_incentivada_di"] = cvm_data["tem_incentivada_di"]

            is_incentivada = False
            motivo = ""

            if emissao["indexador"] == "IPCA" and cvm_data["tem_incentivada_ipca"]:
                is_incentivada = True
                motivo = "emissor tem incentivada IPCA na CVM + debenture é IPCA"
            elif emissao["indexador"] == "CDI" and cvm_data["tem_incentivada_di"]:
                is_incentivada = True
                motivo = "emissor tem incentivada DI na CVM + debenture é CDI"

            entry["classificada_incentivada"] = is_incentivada
            entry["motivo"] = motivo

            if emissao["id"] in FORCADO_LONGO_PRAZO:
                is_incentivada = False
                entry["motivo"] = "forcado Longo Prazo manualmente (FORCADO_LONGO_PRAZO)"

            if is_incentivada and emissao.get("tributacao") != "Isento":
                entry["acao"] = "alterar_para_isento"
                entry["tributacao_nova"] = "Isento"

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

                incentivadas += 1
            else:
                entry["acao"] = "manter"
                mantidas += 1

        log_incentivadas.append(entry)

    # Salvar relatório de incentivadas
    relatorio_incentivadas = {
        "timestamp": datetime.now().isoformat(),
        "modo": "dry-run" if DRY_RUN else "apply",
        "resumo": {
            "total_debentures": len(emissoes_db.data),
            "marcadas_isento": incentivadas,
            "mantidas_longo_prazo": mantidas,
            "sem_dados_cvm": sem_info,
        },
        "detalhes": log_incentivadas,
    }

    with open(RELATORIO_INCENTIVADAS, "w", encoding="utf-8") as f:
        json.dump(relatorio_incentivadas, f, ensure_ascii=False, indent=2)

    print(f"  Marcadas como Isento: {incentivadas}")
    print(f"  Mantidas como Longo Prazo: {mantidas}")
    print(f"  Sem dados CVM: {sem_info}")
    print(f"  -> Relatorio salvo em {RELATORIO_INCENTIVADAS}")

    # ── Resumo ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  Modo:                   {'DRY-RUN' if DRY_RUN else 'APPLY'}")
    print(f"  Match rate emissores:   {stats['exato'] + stats['parcial_3'] + stats['parcial_2']}/{len(emissores_db.data)} ({relatorio_emissores['resumo']['match_rate_pct']}%)")
    print(f"  Debentures incentivadas: {incentivadas}")
    print(f"  Debentures tributadas:   {mantidas}")
    print(f"  Sem dados CVM:           {sem_info}")
    print("=" * 60)

    if DRY_RUN:
        print("\nNenhuma alteracao foi feita (modo dry-run).")
        print("Revise os relatorios JSON e execute com --apply para aplicar:")
        print(f"  python enriquecer_debentures_cvm.py --apply")
    else:
        print("\nAlteracoes aplicadas com sucesso.")

    print(f"\nRelatorios gerados:")
    print(f"  {RELATORIO_EMISSORES}")
    print(f"  {RELATORIO_INCENTIVADAS}")


if __name__ == "__main__":
    main()