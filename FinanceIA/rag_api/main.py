"""
main.py — FinanceIA RAG API
API intermediária entre o Base44 e o Supabase/pgvector.

Deploy: Railway (financeia-rag-api-production.up.railway.app)

Fluxo por requisição:
1. Recebe pergunta do usuário (+ filtros opcionais)
2. Expande a query via Claude Haiku (3 variações semânticas)
3. Gera embedding via OpenAI text-embedding-3-large
4. Busca ampla no Supabase via RPC buscar_fundos (top 50)
5. Enriquece com dados quantitativos (fundo_infos_atualizadas)
6. Aplica reranking quantitativo (rentabilidade, volatilidade, PL, consistência, captação)
7. Retorna os melhores fundos formatados para o Claude no Base44

Endpoints:
  POST /buscar         — busca semântica de fundos
  GET  /premissas      — premissas macroeconômicas e de alocação (tabela Supabase)
  GET  /stats          — estatísticas do catálogo
  PUT  /premissas/{id} — atualizar premissa

Autenticação: X-API-Key header
"""

import logging
import math
import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from openai import OpenAI
from pydantic import BaseModel, Field
from supabase import Client, create_client

# ──────────────────────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("financeia-rag")

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY", "")
OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
API_SECRET_KEY    = os.environ.get("API_SECRET_KEY", "dev-key-trocar-em-producao")

EMBEDDING_MODEL      = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 1536

# Busca semântica ampla → reranking quantitativo
RERANK_FETCH_K       = 50
RERANK_MIN_SIMILARITY = 0.25

RERANK_WEIGHTS = {
    "rentabilidade": 0.35,
    "volatilidade":  0.25,
    "pl":            0.20,
    "consistencia":  0.12,
    "captacao":      0.08,
}

# ──────────────────────────────────────────────────────────────
# App e segurança
# ──────────────────────────────────────────────────────────────

app = FastAPI(title="FinanceIA RAG API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verificar_api_key(api_key: str = Security(api_key_header)) -> str:
    if api_key != API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="API Key inválida")
    return api_key

# ──────────────────────────────────────────────────────────────
# Clients (inicializados no startup)
# ──────────────────────────────────────────────────────────────

openai_client: Optional[OpenAI] = None
supabase_client: Optional[Client] = None

@app.on_event("startup")
async def startup():
    global openai_client, supabase_client
    openai_client   = OpenAI(api_key=OPENAI_API_KEY)
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("FinanceIA RAG API iniciada")

# ──────────────────────────────────────────────────────────────
# Models
# ──────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    query: str = Field(..., description="Pergunta do usuário em linguagem natural")
    top_k: int = Field(5, ge=1, le=20)
    filtro_plataforma: Optional[str] = None
    filtro_categoria: Optional[str] = None
    filtro_tipo_produto: Optional[str] = None
    historico: Optional[list] = None   # para detecção de follow-up

class AtualizarPremissaRequest(BaseModel):
    conteudo: str

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def expandir_query(query: str) -> list[str]:
    """Expande a query em 3 variações semânticas via Claude Haiku."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": f"""Você é um assistente especializado em investimentos brasileiros.
Gere 3 variações da query abaixo para melhorar a busca semântica de fundos.
As variações devem usar sinônimos e termos técnicos de investimentos.
Retorne SOMENTE as 3 variações, uma por linha, sem numeração ou marcadores.

Query original: {query}"""}]
        )
        variacoes = [v.strip() for v in resp.content[0].text.strip().split("\n") if v.strip()]
        return [query] + variacoes[:3]
    except Exception as e:
        logger.warning(f"Falha na expansão de query: {e}")
        return [query]


def gerar_embedding(texto: str) -> list[float]:
    resp = openai_client.embeddings.create(
        input=texto,
        model=EMBEDDING_MODEL,
        dimensions=EMBEDDING_DIMENSIONS,
    )
    return resp.data[0].embedding


def buscar_fundos_supabase(
    embedding: list[float],
    filtro_plataforma: Optional[str] = None,
    filtro_categoria: Optional[str] = None,
    filtro_tipo_produto: Optional[str] = None,
    top_k: int = 50,
) -> list[dict]:
    resp = supabase_client.rpc("buscar_fundos", {
        "query_embedding":    embedding,
        "filtro_plataforma":  filtro_plataforma,
        "filtro_categoria":   filtro_categoria,
        "filtro_tipo_produto": filtro_tipo_produto,
        "top_k":              top_k,
    }).execute()
    return resp.data or []


def enriquecer_com_quantitativos(fundos: list[dict]) -> list[dict]:
    """Busca dados quantitativos da tabela fundo_infos_atualizadas."""
    if not fundos:
        return fundos

    cnpjs = [f.get("cnpj") for f in fundos if f.get("cnpj")]
    if not cnpjs:
        return fundos

    try:
        resp = supabase_client.table("fundo_infos_atualizadas") \
            .select("cnpj, rent_12m, rent_24m, volatilidade_12m, pl_atual, captacao_liquida_12m, sharpe_12m") \
            .in_("cnpj", cnpjs) \
            .execute()

        infos = {r["cnpj"]: r for r in (resp.data or [])}
        for fundo in fundos:
            cnpj = fundo.get("cnpj")
            if cnpj and cnpj in infos:
                fundo.update(infos[cnpj])
    except Exception as e:
        logger.warning(f"Falha ao enriquecer com quantitativos: {e}")

    return fundos


def calcular_score_reranking(fundo: dict, fundos_grupo: list[dict]) -> float:
    """Calcula score quantitativo normalizado (0-1) para reranking."""
    score = 0.0

    # Coleta valores do grupo para normalização
    def valores_validos(campo: str) -> list[float]:
        return [float(f[campo]) for f in fundos_grupo if f.get(campo) is not None]

    def normalizar(valor: float, lista: list[float], inverter: bool = False) -> float:
        if not lista or len(lista) < 2:
            return 0.5
        mn, mx = min(lista), max(lista)
        if mx == mn:
            return 0.5
        norm = (valor - mn) / (mx - mn)
        return 1.0 - norm if inverter else norm

    # Rentabilidade 12m (maior = melhor)
    rent_vals = valores_validos("rent_12m")
    if fundo.get("rent_12m") is not None:
        score += RERANK_WEIGHTS["rentabilidade"] * normalizar(float(fundo["rent_12m"]), rent_vals)

    # Volatilidade (menor = melhor → invertido)
    vol_vals = valores_validos("volatilidade_12m")
    if fundo.get("volatilidade_12m") is not None:
        score += RERANK_WEIGHTS["volatilidade"] * normalizar(float(fundo["volatilidade_12m"]), vol_vals, inverter=True)

    # PL (maior = melhor)
    pl_vals = valores_validos("pl_atual")
    if fundo.get("pl_atual") is not None:
        score += RERANK_WEIGHTS["pl"] * normalizar(float(fundo["pl_atual"]), pl_vals)

    # Consistência (Sharpe)
    sharpe_vals = valores_validos("sharpe_12m")
    if fundo.get("sharpe_12m") is not None:
        score += RERANK_WEIGHTS["consistencia"] * normalizar(float(fundo["sharpe_12m"]), sharpe_vals)

    # Captação líquida
    capt_vals = valores_validos("captacao_liquida_12m")
    if fundo.get("captacao_liquida_12m") is not None:
        score += RERANK_WEIGHTS["captacao"] * normalizar(float(fundo["captacao_liquida_12m"]), capt_vals)

    return score


def formatar_fundo_contexto(fundo: dict) -> str:
    """Formata os dados do fundo para o contexto enviado ao Claude."""
    linhas = [
        f"**{fundo.get('nome', 'N/A')}**",
        f"Classificação: {fundo.get('categoria', '')} > {fundo.get('subcategoria', '')}",
        f"Gestor: {fundo.get('gestor', 'N/A')}",
        f"Benchmark: {fundo.get('benchmark', 'N/A')} | Indexador: {fundo.get('indexador', 'N/A')}",
        f"Liquidez: {fundo.get('liquidez_descricao', 'N/A')}",
        f"Taxa de Adm: {fundo.get('taxa_adm', 'N/A')} | Performance: {fundo.get('taxa_performance', 'N/A')}",
    ]

    # Dados quantitativos
    rent = fundo.get("rent_12m")
    vol  = fundo.get("volatilidade_12m")
    pl   = fundo.get("pl_atual")
    if rent is not None:
        linhas.append(f"Rentabilidade 12m: {rent:.2f}%")
    if vol is not None:
        linhas.append(f"Volatilidade 12m: {vol:.2f}%")
    if pl is not None:
        pl_m = pl / 1_000_000
        linhas.append(f"PL: R$ {pl_m:.1f}M")

    # Qualitativos
    for campo in ["quando_indicar", "quando_nao_indicar", "alertas", "descricao_tecnica"]:
        val = fundo.get(campo, "")
        if val:
            label = campo.replace("_", " ").title()
            linhas.append(f"{label}: {val}")

    return "\n".join(linhas)


# ──────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────

@app.post("/buscar")
async def buscar(request: BuscarRequest, _: str = Security(verificar_api_key)):
    logger.info(f"Busca: '{request.query[:80]}'")

    # 1. Expansão de query
    queries = expandir_query(request.query)
    logger.info(f"Queries expandidas: {len(queries)}")

    # 2. Gerar embeddings e combinar resultados
    todos_fundos = {}
    for q in queries:
        emb = gerar_embedding(q)
        resultados = buscar_fundos_supabase(
            emb,
            request.filtro_plataforma,
            request.filtro_categoria,
            request.filtro_tipo_produto,
            top_k=RERANK_FETCH_K,
        )
        for f in resultados:
            cnpj = f.get("cnpj")
            if cnpj and cnpj not in todos_fundos:
                if f.get("similarity", 0) >= RERANK_MIN_SIMILARITY:
                    todos_fundos[cnpj] = f

    fundos = list(todos_fundos.values())
    logger.info(f"{len(fundos)} fundos únicos após busca")

    if not fundos:
        return {"fundos": [], "contexto": "", "total_encontrado": 0}

    # 3. Enriquecer com quantitativos
    fundos = enriquecer_com_quantitativos(fundos)

    # 4. Reranking quantitativo
    for f in fundos:
        f["_score_reranking"] = calcular_score_reranking(f, fundos)
    fundos.sort(key=lambda x: x.get("_score_reranking", 0), reverse=True)

    # 5. Retorna top_k
    top = fundos[:request.top_k]
    contexto = "\n\n---\n\n".join(formatar_fundo_contexto(f) for f in top)

    return {
        "fundos":          top,
        "contexto":        contexto,
        "total_encontrado": len(fundos),
        "queries_usadas":  queries,
    }


@app.get("/premissas")
async def listar_premissas(_: str = Security(verificar_api_key)):
    try:
        resp = supabase_client.table("premissas") \
            .select("*") \
            .order("categoria") \
            .execute()
        return {"premissas": resp.data or []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/premissas/{premissa_id}")
async def atualizar_premissa(
    premissa_id: int,
    body: AtualizarPremissaRequest,
    _: str = Security(verificar_api_key),
):
    try:
        resp = supabase_client.table("premissas") \
            .update({"conteudo": body.conteudo}) \
            .eq("id", premissa_id) \
            .execute()
        return {"updated": resp.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def stats(_: str = Security(verificar_api_key)):
    try:
        total = supabase_client.table("fundos").select("cnpj", count="exact").execute()
        com_embedding = supabase_client.table("fundo_embeddings").select("cnpj", count="exact").execute()
        return {
            "total_fundos":       total.count,
            "com_embedding":      com_embedding.count,
            "cobertura_pct":      round(com_embedding.count / max(total.count, 1) * 100, 1),
            "embedding_model":    EMBEDDING_MODEL,
            "embedding_dimensions": EMBEDDING_DIMENSIONS,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0"}
