"""
Carrega o catálogo de ativos do Supabase (ativos_rv, rf_emissoes, rf_emissores).

Uso típico:
    from core.catalog_loader import carregar_acoes, carregar_fiis
    acoes = carregar_acoes()  # list[dict]

Cache em memória por execução — evita bater no Supabase múltiplas vezes
no mesmo run.
"""
from functools import lru_cache
from typing import Optional

from core.supabase_client import get_client


# Tamanho da página do PostgREST (Supabase limita por padrão)
_PAGE_SIZE = 1000


def _paginate(table: str, select: str, *, filtros: Optional[list[tuple]] = None) -> list[dict]:
    """Pagina resultados do Supabase; útil para tabelas com mais de 1000 linhas."""
    sb = get_client()
    todos: list[dict] = []
    inicio = 0
    while True:
        q = sb.table(table).select(select)
        if filtros:
            for col, op, val in filtros:
                q = getattr(q, op)(col, val)
        res = q.range(inicio, inicio + _PAGE_SIZE - 1).execute()
        if not res.data:
            break
        todos.extend(res.data)
        if len(res.data) < _PAGE_SIZE:
            break
        inicio += _PAGE_SIZE
    return todos


@lru_cache(maxsize=1)
def carregar_acoes() -> list[dict]:
    """
    Retorna lista de ações ativas do catálogo.
    Espera tabela ativos_rv com colunas: codigo_b3, tipo, nome.
    Normaliza tipo para minúsculas no retorno (convenção do pipeline).
    """
    rows = _paginate(
        "ativos_rv",
        "codigo_b3, nome, tipo",
        filtros=[("tipo", "eq", "ACAO")],
    )
    for r in rows:
        r["tipo"] = "acao"
    return rows


@lru_cache(maxsize=1)
def carregar_fiis() -> list[dict]:
    """Retorna lista de FIIs ativos do catálogo (tipo normalizado pra minúsculas)."""
    rows = _paginate(
        "ativos_rv",
        "codigo_b3, nome, tipo",
        filtros=[("tipo", "eq", "FII")],
    )
    for r in rows:
        r["tipo"] = "fii"
    return rows

@lru_cache(maxsize=1)
def carregar_debentures() -> list[dict]:
    """
    Retorna debêntures de rf_emissoes.
    Inclui ticker (CETIP/ANBIMA), emissor_id e CNPJ do emissor.
    """
    return _paginate(
        "rf_emissoes",
        "id, ticker, emissor_id, tipo_produto",
        filtros=[("tipo_produto", "eq", "debenture")],
    )


@lru_cache(maxsize=1)
def carregar_cri_cra() -> list[dict]:
    """Retorna CRIs e CRAs de rf_emissoes."""
    return _paginate(
        "rf_emissoes",
        "id, ticker, emissor_id, tipo_produto",
        filtros=[("tipo_produto", "in", "(cri_cra)")],
    )


@lru_cache(maxsize=1)
def carregar_emissores_rf() -> list[dict]:
    """Lista de emissores de RF (para análises por emissor, não por emissão)."""
    return _paginate("rf_emissores", "id, nome, cnpj")


def buscar_ativo_rv(codigo_b3: str) -> Optional[dict]:
    """Busca pontual de um ticker, sem cache (uso em fluxo de descoberta)."""
    sb = get_client()
    res = (
        sb.table("ativos_rv")
        .select("codigo_b3, nome, tipo")
        .eq("codigo_b3", codigo_b3)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def limpar_cache() -> None:
    """Limpa todos os caches lru — útil em testes."""
    carregar_acoes.cache_clear()
    carregar_fiis.cache_clear()
    carregar_debentures.cache_clear()
    carregar_cri_cra.cache_clear()
    carregar_emissores_rf.cache_clear()
