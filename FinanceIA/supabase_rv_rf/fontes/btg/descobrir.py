"""
Descoberta de análises do BTG Research.

A API pública do site BTG (`content.btgpactual.com/api/research/public-router/...`)
expõe um endpoint paginado por CATEGORIA que já devolve o conteúdo completo da
análise no campo `analyzeComponents.body` (HTML), os ativos relacionados em
`analyzeAsset[]` (com ticker individual e tipo), `referenceDate`, `recommendation`
e `targetPrice`. Não há PDF — todo o pipeline é JSON.

Categorias usadas (MVP):
- ACS = Ações brasileiras
- FII = Fundos Imobiliários

Fora do escopo do MVP: BDR (sem cobertura em ativos_rv) e RF (precisa de
mapeamento nome do emissor → emissor_id).
"""
import time
from dataclasses import dataclass, field
from typing import Optional

from curl_cffi import requests as cffi_requests

from core.config import HTTP_TIMEOUT
from core.http import IMPERSONATE


FONTE = "btg_research"

CATEGORIAS_MVP: tuple[str, ...] = ("ACS", "FII")
PAGE_SIZE_PADRAO = 50

# Pausa entre páginas da API BTG (segundos). Polidez/anti-rate-limit.
SLEEP_ENTRE_PAGINAS = 0.3

URL_BASE = (
    "https://content.btgpactual.com/api/research/public-router/"
    "content-hub/analysis/category/{categoria}"
)


@dataclass
class AlvoBTG:
    btg_id: str                   # ObjectID-like, único por análise (chave de checkpoint)
    titulo: str                   # analyzeComponents.title
    referencia_iso: str           # ISO datetime de `referenceDate`
    categoria: str                # "ACS" | "FII"
    body_html: str                # analyzeComponents.body (HTML bruto, contém <p>, <strong>, <figure>, ...)
    ativos_brutos: list[dict]     # entries de analyzeAsset[] (asset, recommendation, targetPrice)
    tier: str = ""                # "AREA_PUBLICA" | "AREA_LOGADA"
    extras: dict = field(default_factory=dict)


def _fetch_pagina(categoria: str, pagina: int, page_size: int) -> dict:
    """Chama 1 página da API. Retorna dict JSON ou levanta exceção."""
    url = (
        URL_BASE.format(categoria=categoria)
        + f"?pageNumber={pagina}&pageSize={page_size}"
        + "&channel=research&locale=pt-br"
    )
    r = cffi_requests.get(
        url,
        timeout=HTTP_TIMEOUT,
        impersonate=IMPERSONATE,
        headers={"Accept": "application/json"},
    )
    r.raise_for_status()
    return r.json()


def _parse_item(item: dict, categoria: str) -> Optional[AlvoBTG]:
    """Converte um item da API em AlvoBTG. Retorna None se faltar conteúdo essencial."""
    btg_id = item.get("id") or ""
    components = item.get("analyzeComponents") or {}
    body_html = components.get("body") or ""

    if not btg_id or not body_html.strip():
        return None

    return AlvoBTG(
        btg_id=btg_id,
        titulo=(components.get("title") or "").strip(),
        referencia_iso=item.get("referenceDate") or "",
        categoria=categoria,
        body_html=body_html,
        ativos_brutos=item.get("analyzeAsset") or [],
        tier=item.get("tier") or "",
    )


def descobrir(
    categorias: tuple[str, ...] = CATEGORIAS_MVP,
    *,
    max_paginas: Optional[int] = None,
    page_size: int = PAGE_SIZE_PADRAO,
) -> list[AlvoBTG]:
    """
    Pagina cada categoria (ordem DESC implícita da API: mais recentes primeiro)
    e retorna lista de AlvoBTG.

    Args:
        categorias: tupla de categorias da API (ex.: ("ACS", "FII")).
        max_paginas: se fornecido, limita N páginas POR CATEGORIA (debug/incremental).
                     None = todas até esgotar.
        page_size: itens por página (API aceita até 200; padrão 50).
    """
    alvos: list[AlvoBTG] = []

    for cat in categorias:
        try:
            primeira = _fetch_pagina(cat, 1, page_size)
        except Exception as e:
            print(f"[descobrir.btg] aviso: falha em página 1 de {cat}: {e}")
            continue

        total = primeira.get("paging", {}).get("totalResults", 0)
        n_paginas = (total + page_size - 1) // page_size if page_size else 1
        if max_paginas:
            n_paginas = min(n_paginas, max_paginas)

        for item in primeira.get("response", []) or []:
            alvo = _parse_item(item, cat)
            if alvo:
                alvos.append(alvo)

        for p in range(2, n_paginas + 1):
            time.sleep(SLEEP_ENTRE_PAGINAS)
            try:
                data = _fetch_pagina(cat, p, page_size)
            except Exception as e:
                print(f"[descobrir.btg] aviso: falha em página {p} de {cat}: {e}")
                break
            for item in data.get("response", []) or []:
                alvo = _parse_item(item, cat)
                if alvo:
                    alvos.append(alvo)

    return alvos
