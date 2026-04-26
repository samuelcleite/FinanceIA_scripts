import os
"""
populate_rf_cri_cra.py
=======================
Popula rf_emissores e rf_emissoes com dados de CRI e CRA exportados da ANBIMA.
NÃO popula rf_disponibilidade.

Fonte: https://www.anbima.com.br/pt_br/informar/precos-e-indices/precos/taxas-de-cri-e-cra/

Decisões de design:
  - Emissor = empresa tomadora do crédito (Risco de Crédito), não a securitizadora
  - tipo = "empresa" para todos os emissores
  - setor = NULL
  - tipo_produto = "cri_cra" (fixo para todos)
  - tributacao = "Isento" (CRI e CRA sempre isentos para PF)
  - cupom_semestral = NULL
  - subcategoria = derivada do indexador:
      CDI -> "Pós-Fixado"
      IPCA -> "Juro Real"
      Pré-Fixado -> "Pré-Fixado"
  - Emissores que já existem no Supabase são reutilizados (sem duplicar)

Uso:
  1. Exporte o CSV da página ANBIMA e salve na pasta do script
  2. Ajuste SUPABASE_URL e SUPABASE_KEY
  3. Execute: python populate_rf_cri_cra.py
"""

import json
import uuid
import unicodedata
import pandas as pd
from supabase import create_client

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CSV_PATH = "taxas_CRI_CRA.csv"
PROGRESS_FILE = "progress_cri_cra.json"

# ─── MATCHES CONHECIDOS (CSV -> Supabase) ────────────────────────────────────
# Emissores que já existem no Supabase com nome diferente do CSV.
# Chave = nome normalizado do CSV, valor = nome exato no Supabase.
MATCHES_CONHECIDOS = {
    "ACO VERDE DO BRASIL": "ACO VERDE DO BRASIL S/A",
    "COGNA EDUCACAO": "COGNA EDUCACAO S.A",
    "COLOMBO AGROINDUSTRIA": "COLOMBO AGROINDUSTRIA SA",
    "COMPANHIA DE LOCACAO DAS AMERICAS": "COMPANHIA DE LOCAÇÃO DAS AMÉRICAS",
    "DIAGNOSTICOS DA AMERICA": "DIAGNÓSTICOS DA AMÉRICA S/A",
    "ELDORADO BRASIL CELULOSE": "ELDORADO BRASIL CELULOSE S.A.",
    "HYPERA": "HYPERA S/A",
    "IGUATEMI EMPRESA DE SHOPPING CENTERS": "IGUATEMI EMPRESA DE SHOPPING CENTERS S/A",
    "JALLES MACHADO": "JALLES MACHADO S.A.",
    "JHSF PARTICIPACOES": "JHSF PARTICIPAÇÕES S/A",
    "JSL": "JSL S/A",
    "KLABIN": "KLABIN S/A",
    "LOCALIZA RENT A CAR": "LOCALIZA RENT A CAR",
    "LOG COMMERCIAL PROPERTIES E PARTICIPACOES": "LOG COMMERCIAL PROPERTIES E PARTICIPAÇÕES S/A",
    "MADERO INDUSTRIA E COMERCIO": "MADERO INDUSTRIA E COMERCIO S.A.",
    "MARFRIG GLOBAL FOODS": "MARFRIG GLOBAL FOODS S.A.",
    "MINERVA": "MINERVA S/A",
    "MOVIDA PARTICIPACOES": "MOVIDA PARTICIPACOES S/A",
    "MULTIPLAN EMPREENDIMENTOS IMOBILIARIOS": "MULTIPLAN EMPREENDIMENTOS IMOBILIÁRIOS S/A",
    "NATURA COSMETICOS": "NATURA COSMÉTICOS S/A",
    "RAIA DROGASIL": "RAIA DROGASIL S/A",
    "RAIZEN ENERGIA": "RAIZEN ENERGIA S/A",
    "SENDAS DISTRIBUIDORA": "SENDAS DISTRIBUIDORA S/A",
    "SAO MARTINHO": "SAO MARTINHO S/A",
    "UNIDAS LOCADORA": "UNIDAS LOCADORA S.A.",
    "UNIDAS LOCACOES E SERVICOS": "UNIDAS LOCACOES E SERVICOS S.A.",
    "USINA SANTA FE": "USINA SANTA FE S.A.",
    "VAMOS LOCACAO DE CAMINHOES MAQUINAS E EQUIPAMENTOS": "VAMOS LOCACAO DE CAMINHOES, MAQUINAS E EQUIPAMENTOS S.A.",
    "YDUQS PARTICIPACOES": "YDUQS PARTICIPACOES S.A",
}


# ─── FUNÇÕES AUXILIARES ──────────────────────────────────────────────────────

def normalizar(nome):
    """Normaliza nome para comparação e lookup."""
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    nome = nome.upper()
    for suf in [" S/A", " S.A.", " SA", " LTDA", " LTDA.", " S.A", " S/A."]:
        nome = nome.replace(suf, "")
    nome = nome.replace(".", "").replace("-", " ").replace("/", " ").replace(",", "")
    nome = " ".join(nome.split())
    return nome.strip()


def classificar_indexador(idx_str):
    """Padroniza indexador."""
    s = str(idx_str).strip().upper()
    if "IPCA" in s:
        return "IPCA"
    elif "IGPM" in s or "IGP-M" in s or "IGP" in s:
        return "IGPM"
    elif "PRE" in s or "PRÉ" in s:
        return "Pré-Fixado"
    elif "DI" in s or "CDI" in s:
        return "CDI"
    else:
        return "CDI"


def subcategoria_por_indexador(indexador):
    """Mapeia indexador padronizado para subcategoria."""
    mapa = {
        "IPCA": "Juro Real",
        "CDI": "Pós-Fixado",
        "Pré-Fixado": "Pré-Fixado",
        "IGPM": "Juro Real",
    }
    return mapa.get(indexador, "Pós-Fixado")


def parse_date_br(date_str):
    """Converte data BR (dd/mm/yyyy) para ISO."""
    from datetime import datetime
    if not date_str or str(date_str).strip() == "":
        return None
    try:
        return datetime.strptime(str(date_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def load_progress():
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"emissores_criados": [], "emissoes_criadas": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


# ─── PIPELINE ────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("POPULATE RF CRI/CRA — ANBIMA -> Supabase")
    print("=" * 60)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    progress = load_progress()

    # ── 1. Ler CSV ───────────────────────────────────────────────────────
    print(f"\n[1/3] Lendo CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, sep=";", encoding="latin-1", header=0,
                     names=["data_ref", "risco_credito", "emissor", "serie",
                            "emissao", "codigo", "vencimento", "indexador",
                            "taxa_compra", "taxa_venda", "taxa_indicativa",
                            "desvio_padrao", "pu", "pu_par_pct", "duration",
                            "ref_ntnb", "reune_pct"])

    for col in ["data_ref", "risco_credito", "codigo", "vencimento", "indexador"]:
        df[col] = df[col].str.strip()

    # Usar apenas última data
    ultima_data = df["data_ref"].value_counts().idxmax()
    df = df[df["data_ref"] == ultima_data].copy()

    # Remover linhas sem código ou empresa
    df = df.dropna(subset=["codigo", "risco_credito"])
    df = df[df["risco_credito"].str.strip() != ""]

    print(f"  -> Data: {ultima_data} | {len(df)} registros")

    # Campos derivados
    df["indexador_pad"] = df["indexador"].apply(classificar_indexador)
    df["subcategoria"] = df["indexador_pad"].apply(subcategoria_por_indexador)
    df["vencimento_iso"] = df["vencimento"].apply(parse_date_br)

    print(f"  -> Indexadores: {df['indexador_pad'].value_counts().to_dict()}")

    # ── 2. Criar / reutilizar emissores ──────────────────────────────────
    print("\n[2/3] Verificando emissores...")

    # Buscar todos os emissores existentes no Supabase
    existing_emissores = supabase.table("rf_emissores").select("id, nome").execute()
    emissores_map = {}  # nome_supabase -> id
    for row in existing_emissores.data:
        emissores_map[row["nome"]] = row["id"]
    emissores_norm_map = {normalizar(n): i for n, i in emissores_map.items()}

    ja_criados = set(progress.get("emissores_criados", []))
    criados = 0
    reutilizados = 0
    erros = 0

    # Mapa final: nome_csv -> id_supabase
    csv_para_id = {}

    empresas_unicas = df["risco_credito"].dropna().unique()

    for empresa in empresas_unicas:
        empresa = str(empresa).strip()
        if not empresa:
            continue

        norm = normalizar(empresa)

        # 1. Verificar match conhecido
        if norm in MATCHES_CONHECIDOS:
            nome_sup = MATCHES_CONHECIDOS[norm]
            if nome_sup in emissores_map:
                csv_para_id[empresa] = emissores_map[nome_sup]
                reutilizados += 1
                continue

        # 2. Verificar match exato normalizado
        if norm in emissores_norm_map:
            csv_para_id[empresa] = emissores_norm_map[norm]
            reutilizados += 1
            continue

        # 3. Já criado nesta sessão
        if empresa in ja_criados:
            continue

        # 4. Criar novo emissor
        emissor_id = str(uuid.uuid4())
        emissor_data = {
            "id": emissor_id,
            "nome": empresa,
            "tipo": "empresa",
            "setor": None,
        }

        try:
            supabase.table("rf_emissores").insert(emissor_data).execute()
            csv_para_id[empresa] = emissor_id
            emissores_map[empresa] = emissor_id
            emissores_norm_map[norm] = emissor_id
            ja_criados.add(empresa)
            criados += 1
        except Exception as e:
            print(f"  ERRO emissor {empresa}: {e}")
            erros += 1

    progress["emissores_criados"] = list(ja_criados)
    save_progress(progress)
    print(f"  Reutilizados (ja existiam): {reutilizados}")
    print(f"  Criados novos: {criados}")
    print(f"  Erros: {erros}")

    # ── 3. Criar emissões ────────────────────────────────────────────────
    print("\n[3/3] Criando emissoes...")

    # Buscar tickers já existentes
    existing_emissoes = supabase.table("rf_emissoes").select("id, ticker").eq("tipo_produto", "cri_cra").execute()
    tickers_existentes = {row["ticker"]: row["id"] for row in existing_emissoes.data if row["ticker"]}

    ja_criadas = set(progress.get("emissoes_criadas", []))
    criadas_em = 0
    puladas = 0
    erros_em = 0

    for _, row in df.iterrows():
        codigo = str(row["codigo"]).strip()
        empresa = str(row["risco_credito"]).strip()

        if codigo in tickers_existentes or codigo in ja_criadas:
            puladas += 1
            continue

        emissor_id = csv_para_id.get(empresa)
        if not emissor_id:
            # Tentar achar por nome normalizado
            norm = normalizar(empresa)
            emissor_id = emissores_norm_map.get(norm)
        if not emissor_id:
            print(f"  AVISO: sem emissor_id para '{empresa}' (codigo {codigo})")
            continue

        nome_emissao = f"{empresa} - {codigo}"

        emissao_data = {
            "id": str(uuid.uuid4()),
            "emissor_id": emissor_id,
            "ticker": codigo,
            "nome": nome_emissao,
            "tipo_produto": "cri_cra",
            "indexador": row["indexador_pad"],
            "vencimento": row["vencimento_iso"],
            "garantia_fgc": False,
            "tributacao": "Isento",
            "cupom_semestral": None,
            "categoria": "Renda Fixa",
            "subcategoria": row["subcategoria"],
        }

        try:
            supabase.table("rf_emissoes").insert(emissao_data).execute()
            ja_criadas.add(codigo)
            criadas_em += 1

            if criadas_em % 50 == 0:
                print(f"  -> {criadas_em} emissoes criadas...")
                progress["emissoes_criadas"] = list(ja_criadas)
                save_progress(progress)

        except Exception as e:
            print(f"  ERRO emissao {codigo}: {e}")
            erros_em += 1

    progress["emissoes_criadas"] = list(ja_criadas)
    save_progress(progress)

    print(f"  Criadas: {criadas_em}")
    print(f"  Ja existiam (puladas): {puladas}")
    print(f"  Erros: {erros_em}")

    # ── Resumo ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  Emissores reutilizados: {reutilizados}")
    print(f"  Emissores criados:      {criados}")
    print(f"  Emissoes criadas:       {criadas_em}")
    print(f"  tipo_produto:           cri_cra (fixo)")
    print(f"  tributacao:             Isento (fixo)")
    print(f"  cupom_semestral:        NULL (fixo)")
    print(f"  rf_disponibilidade:     NAO populado")
    print("=" * 60)


if __name__ == "__main__":
    main()