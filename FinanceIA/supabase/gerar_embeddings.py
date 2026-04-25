"""
gerar_embeddings.py
Gera embeddings text-embedding-3-large para todos os fundos na tabela fundos do Supabase
e insere na tabela fundo_embeddings (vector(1536)).

Pré-requisitos:
  - Tabela fundo_embeddings criada com vector(1536) + HNSW index
  - Função buscar_fundos criada no Supabase

Uso:
    python gerar_embeddings.py
    python gerar_embeddings.py --limit 3   # testar com 3 fundos
"""

import argparse
import json
import time
from datetime import datetime

from openai import OpenAI
from supabase import create_client

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

SUPABASE_URL  = "SUA_PROJECT_URL"
SUPABASE_KEY  = "SUA_SERVICE_ROLE_KEY"
OPENAI_KEY    = "SUA_OPENAI_API_KEY"

EMBEDDING_MODEL      = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 1536
PROGRESS_FILE        = "progress_embeddings.json"

# ─── CLIENTE ─────────────────────────────────────────────────────────────────

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai   = OpenAI(api_key=OPENAI_KEY)

# ─── MONTAGEM DO DOCUMENTO ───────────────────────────────────────────────────

def montar_documento(fundo: dict) -> str:
    """
    Monta o texto que será embedado.
    Inclui dados estruturados no início para melhorar retrieval por categoria/tipo.
    """
    partes = [
        f"Nome: {fundo.get('nome', '')}",
        f"Categoria: {fundo.get('categoria', '')} | Subcategoria: {fundo.get('subcategoria', '')}",
        f"Tipo: {fundo.get('tipo_produto', '')} | Indexador: {fundo.get('indexador', 'N/A')}",
        f"Benchmark: {fundo.get('benchmark', '')}",
        "",
    ]

    # Campos qualitativos
    for campo in ["descricao_tecnica", "quando_indicar", "quando_nao_indicar",
                  "vantagens", "desvantagens", "alertas", "descricao_simples"]:
        val = fundo.get(campo, "")
        if val:
            partes.append(val)

    return "\n".join(filter(None, partes))


def gerar_embedding(texto: str) -> list[float]:
    """Chama a API da OpenAI para gerar o embedding."""
    resp = openai.embeddings.create(
        input=texto,
        model=EMBEDDING_MODEL,
        dimensions=EMBEDDING_DIMENSIONS,
    )
    return resp.data[0].embedding


# ─── CHECKPOINT ──────────────────────────────────────────────────────────────

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

def gerar_embeddings(limit: int = 0):
    # Busca todos os fundos do Supabase
    query = supabase.table("fundos").select(
        "cnpj, nome, gestor, tipo_produto, categoria, subcategoria, "
        "benchmark, indexador, tributacao, come_cotas, horizonte_minimo_anos, "
        "liquidez_descricao, publico_alvo, quando_indicar, quando_nao_indicar, "
        "vantagens, desvantagens, alertas, descricao_tecnica, descricao_simples"
    )
    if limit:
        query = query.limit(limit)

    resultado = query.execute()
    fundos    = resultado.data
    total     = len(fundos)

    progresso      = carregar_progresso()
    ja_processados = set(progresso["processados"])

    print(f"Total de fundos: {total} | Já processados: {len(ja_processados)}\n")

    for i, fundo in enumerate(fundos):
        cnpj = fundo.get("cnpj")

        if not cnpj or cnpj in ja_processados:
            continue

        nome = fundo.get("nome", "")
        print(f"[{i+1}/{total}] {cnpj} — {nome}")

        try:
            documento = montar_documento(fundo)
            embedding = gerar_embedding(documento)

            supabase.table("fundo_embeddings").upsert({
                "cnpj":            cnpj,
                "documento_texto": documento,
                "embedding":       embedding,
                "modelo":          EMBEDDING_MODEL,
                "gerado_em":       datetime.now().isoformat()
            }).execute()

            ja_processados.add(cnpj)
            progresso["processados"] = list(ja_processados)
            salvar_progresso(progresso)

            print(f"  ✅ ok")
            time.sleep(0.1)  # respeita rate limit da OpenAI

        except Exception as e:
            print(f"  ❌ Erro: {e}")
            continue

    print(f"\nConcluído! {len(ja_processados)} embeddings gerados.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limitar a N fundos (para teste)")
    args = parser.parse_args()
    gerar_embeddings(limit=args.limit)
