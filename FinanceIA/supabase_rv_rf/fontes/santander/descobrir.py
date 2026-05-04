"""
Descoberta de PDFs Santander Research.

Estratégia híbrida:
- Lista fixa de PDFs conhecidos (mais confiável; URLs estáveis e mensais)
- Crawl heurístico da página índice para descobrir novos PDFs publicados

Cada alvo descoberto é um AlvoSantander com a URL do PDF e o tipo_ativo
inferido pelo slug (filename). Não baixa nada aqui — só lista.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import urljoin

from core import http  # curl_cffi com impersonate=chrome131

log = logging.getLogger(__name__)

BASE_URL = "https://investimentos.santander.com.br/materiais/relatorios/"

# PDFs conhecidos e ativos (verificados em 05/2026).
# tipo_ativo segue o vocabulário do schema: "acao" | "fii".
PDFS_CONHECIDOS: list[dict] = [
    {
        "slug": "Fundos-Imobiliarios",
        "tipo_ativo": "fii",
        "carteira": "Carteira Recomendada de FIIs",
    },
    {
        "slug": "Carteira-Valor",
        "tipo_ativo": "acao",
        "carteira": "Carteira Valor",
    },
    {
        "slug": "Carteira-Dividendos",
        "tipo_ativo": "acao",
        "carteira": "Carteira Dividendos",
    },
    {
        "slug": "Small-Caps",
        "tipo_ativo": "acao",
        "carteira": "Carteira Small Caps",
    },
]

# Slugs a ignorar mesmo quando aparecem na página índice
# (em construção, descontinuados, fora do escopo de RV).
SLUGS_IGNORADOS = {
    "Ibovespa-Mais",  # confirmado "Página em construção" em 05/2026
}

# Heurística: slugs com qualquer um destes radicais são tratados como FII;
# o resto vira "acao" por padrão (carteiras Santander cobrem só RV).
RADICAIS_FII = ("fundo", "imobiliari", "fii")


@dataclass
class AlvoSantander:
    """Alvo de extração: um PDF com 1+ ativos dentro."""

    slug: str                       # ex: "Fundos-Imobiliarios"
    url: str                        # URL completa do PDF
    tipo_ativo: str                 # "acao" | "fii"
    carteira: str | None = None     # nome humano da carteira, se conhecido
    origem: str = "fixo"            # "fixo" | "crawl"
    extras: dict = field(default_factory=dict)


def _inferir_tipo_ativo(slug: str) -> str:
    """Infere tipo_ativo pelo slug do PDF. Default: 'acao'."""
    s = slug.lower()
    if any(r in s for r in RADICAIS_FII):
        return "fii"
    return "acao"


def _crawl_indice() -> list[str]:
    """
    Varre a página índice do Santander e retorna slugs de PDFs encontrados.

    Estratégia simples: regex em qualquer href que termine em .pdf dentro
    do path /materiais/relatorios/.
    """
    try:
        html = http.fetch_html(BASE_URL, fonte="santander")
    except Exception as exc:
        log.warning("Crawl da página índice falhou: %s", exc)
        return []

    if not html:
        log.warning("Página índice retornou vazia (404 ou similar)")
        return []

    # Captura .../materiais/relatorios/<Slug>.pdf — com ou sem aspas
    padrao = re.compile(
        r'/materiais/relatorios/([A-Za-z0-9\-_]+)\.pdf',
        re.IGNORECASE,
    )
    encontrados = padrao.findall(html)

    slugs_unicos: list[str] = []
    visto: set[str] = set()
    for slug in encontrados:
        if slug in visto:
            continue
        visto.add(slug)
        slugs_unicos.append(slug)

    log.info("Crawl encontrou %d slugs únicos no índice", len(slugs_unicos))
    return slugs_unicos


def descobrir(usar_crawl: bool = True) -> list[AlvoSantander]:
    """
    Retorna lista de AlvoSantander prontos para extração.

    Args:
        usar_crawl: se True, complementa a lista fixa com slugs descobertos
            no crawl da página índice. Slugs ignorados (em construção etc.)
            são filtrados.
    """
    alvos: list[AlvoSantander] = []
    slugs_processados: set[str] = set()

    # 1) Lista fixa (alta confiança)
    for entry in PDFS_CONHECIDOS:
        slug = entry["slug"]
        if slug in slugs_processados:
            continue
        slugs_processados.add(slug)
        alvos.append(
            AlvoSantander(
                slug=slug,
                url=urljoin(BASE_URL, f"{slug}.pdf"),
                tipo_ativo=entry["tipo_ativo"],
                carteira=entry.get("carteira"),
                origem="fixo",
            )
        )

    # 2) Crawl heurístico — só adiciona slugs novos
    if usar_crawl:
        for slug in _crawl_indice():
            if slug in slugs_processados:
                continue
            if slug in SLUGS_IGNORADOS:
                log.debug("Slug ignorado por configuração: %s", slug)
                continue
            slugs_processados.add(slug)
            tipo_inferido = _inferir_tipo_ativo(slug)
            log.info(
                "Slug novo descoberto via crawl: %s (tipo inferido=%s)",
                slug,
                tipo_inferido,
            )
            alvos.append(
                AlvoSantander(
                    slug=slug,
                    url=urljoin(BASE_URL, f"{slug}.pdf"),
                    tipo_ativo=tipo_inferido,
                    carteira=None,
                    origem="crawl",
                )
            )

    log.info(
        "Santander: %d alvos descobertos (%d fixos, %d crawl)",
        len(alvos),
        sum(1 for a in alvos if a.origem == "fixo"),
        sum(1 for a in alvos if a.origem == "crawl"),
    )
    return alvos


def iter_alvos(usar_crawl: bool = True) -> Iterable[AlvoSantander]:
    """Generator-friendly wrapper sobre descobrir()."""
    yield from descobrir(usar_crawl=usar_crawl)