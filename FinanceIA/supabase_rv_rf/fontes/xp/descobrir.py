"""
Descoberta de URLs do XP Research.

Ações:  URL canônica por ticker → /acoes/{ticker}/
FIIs:   crawl paginado de /fundos-imobiliarios/relatorios/ → URL mais
        recente por ticker (extraído do slug via regex).

Estratégia "1 alvo por raiz" (ações):
  Como o XP publica 1 relatório por empresa (não por classe de ação),
  geramos UM alvo por codigo_b3 raiz e tentamos os tickers individuais
  do array `tickers` em ordem (preferindo ON, depois PN, depois Units).

Estratégia "mais recente por ticker" (FIIs):
  O índice é cronológico desc. A primeira URL encontrada para cada ticker
  no slug é a mais recente. Filtra pelo catálogo de FIIs.
"""
import re
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup

from core.catalog_loader import carregar_acoes, carregar_fiis
from core.http import fetch_html


BASE_ACOES = "https://conteudos.xpi.com.br/acoes/{ticker}/"
INDICE_ACOES = "https://conteudos.xpi.com.br/acoes/"
INDICE_FIIS_P1 = "https://conteudos.xpi.com.br/fundos-imobiliarios/relatorios/"
INDICE_FIIS_PN = "https://conteudos.xpi.com.br/fundos-imobiliarios/relatorios/page/{}/"
MAX_PAGINAS_FIIS = 60

FONTE = "xp_research"

# Ticker FII: 4 letras + 1-2 dígitos
_RE_TICKER_FII = re.compile(r"\b([A-Z]{4}[0-9]{1,2})\b")

# Slugs de relatórios genéricos (sem ticker específico) — ignorar
_PREFIXOS_IGNORAR = (
    "top-fundos", "carteira-", "fiis-na-semana", "radar-",
    "one-pager", "mapa-de-conteudos", "calendario-de-dividendos",
    "superclassicos", "estrategia-", "mercado-livre",
)


@dataclass
class AlvoXP:
    codigo_b3: str
    ticker_url: str
    tipo_ativo: str                 # "acao" | "fii"
    url: str
    no_catalogo: bool
    tickers_alternativos: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Ações
# ---------------------------------------------------------------------------

def _ordenar_tickers(tickers: list[str]) -> list[str]:
    def chave(tk: str) -> tuple:
        i = 0
        while i < len(tk) and tk[i].isalpha():
            i += 1
        sufixo = tk[i:]
        try:
            n = int(sufixo)
        except ValueError:
            n = 99
        ordem = {3: 0, 4: 1, 11: 2}.get(n, 9)
        return (ordem, n, tk)
    return sorted(tickers, key=chave)


def _extrair_tickers_indice_acoes(html: str) -> set[str]:
    soup = BeautifulSoup(html, "lxml")
    tickers: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/acoes/" not in href:
            continue
        parts = [p for p in href.strip("/").split("/") if p]
        if not parts:
            continue
        cand = parts[-1].upper()
        if 5 <= len(cand) <= 7:
            i = 0
            while i < len(cand) and cand[i].isalpha():
                i += 1
            letras, digitos = cand[:i], cand[i:]
            if 3 <= len(letras) <= 5 and 1 <= len(digitos) <= 2 and digitos.isdigit():
                tickers.add(cand)
    return tickers


def _alvos_acoes() -> list[AlvoXP]:
    alvos = []
    for acao in carregar_acoes():
        raiz = acao["codigo_b3"]
        tickers = acao.get("tickers") or [raiz]
        ordenados = _ordenar_tickers(tickers) if tickers else [raiz]
        principal = ordenados[0]
        alternativos = ordenados[1:]
        alvos.append(AlvoXP(
            codigo_b3=raiz,
            ticker_url=principal,
            tipo_ativo="acao",
            url=BASE_ACOES.format(ticker=principal.lower()),
            no_catalogo=True,
            tickers_alternativos=alternativos,
        ))
    return alvos


# ---------------------------------------------------------------------------
# FIIs — crawl do índice de relatórios
# ---------------------------------------------------------------------------

def _slug_e_generico(slug: str) -> bool:
    return any(slug.startswith(p) for p in _PREFIXOS_IGNORAR)


def _tickers_do_slug(slug: str) -> list[str]:
    slug_upper = slug.upper().replace("-", " ")
    return _RE_TICKER_FII.findall(slug_upper)


def _links_de_pagina(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    base = "https://conteudos.xpi.com.br/fundos-imobiliarios/relatorios/"
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/fundos-imobiliarios/relatorios/" not in href:
            continue
        slug = href.rstrip("/").split("/")[-1]
        if len(slug) < 5:          # âncora ou diretório raiz
            continue
        if href not in seen:
            seen.add(href)
            links.append(href)
    return links


def _alvos_fiis() -> list[AlvoXP]:
    # Monta conjunto de tickers do catálogo
    catalogo: set[str] = set()
    for fii in carregar_fiis():
        for tk in (fii.get("tickers") or [fii["codigo_b3"]]):
            catalogo.add(tk.upper())

    print(f"[descobrir.fiis] {len(catalogo)} tickers no catálogo")

    mais_recente: dict[str, str] = {}  # ticker -> url

    for pagina in range(1, MAX_PAGINAS_FIIS + 1):
        url_pag = INDICE_FIIS_P1 if pagina == 1 else INDICE_FIIS_PN.format(pagina)
        html = fetch_html(url_pag, fonte=FONTE, use_cache=False)
        if not html:
            print(f"[descobrir.fiis] página {pagina}: None — encerrando")
            break

        links = _links_de_pagina(html)
        if not links:
            print(f"[descobrir.fiis] página {pagina}: sem links — encerrando")
            break

        novos = 0
        for href in links:
            slug = href.rstrip("/").split("/")[-1]
            if _slug_e_generico(slug):
                continue
            for tk in _tickers_do_slug(slug):
                if tk not in mais_recente:
                    mais_recente[tk] = href
                    novos += 1

        print(f"[descobrir.fiis] página {pagina}: {len(links)} links, {novos} tickers novos")

    print(f"[descobrir.fiis] total tickers no índice: {len(mais_recente)}")

    alvos = []
    for tk, url in sorted(mais_recente.items()):
        alvos.append(AlvoXP(
            codigo_b3=tk,
            ticker_url=tk,
            tipo_ativo="fii",
            url=url,
            no_catalogo=(tk in catalogo),
        ))

    em_cat = sum(1 for a in alvos if a.no_catalogo)
    print(f"[descobrir.fiis] {em_cat} no catálogo, {len(alvos)-em_cat} fora")
    return alvos


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------

def descobrir_do_catalogo() -> list[AlvoXP]:
    """Ações + FIIs do catálogo."""
    return _alvos_acoes() + [a for a in _alvos_fiis() if a.no_catalogo]


def descobrir_todos() -> tuple[list[AlvoXP], list[AlvoXP]]:
    """
    Retorna (alvos_catalogo, novos_fora_catalogo).
    Compatível com run_xp.py.
    """
    acoes = _alvos_acoes()

    # Crawl do índice uma única vez
    todos_fiis = _alvos_fiis()
    fiis_cat = [a for a in todos_fiis if a.no_catalogo]
    fiis_novos = [a for a in todos_fiis if not a.no_catalogo]

    # "Novos" de ações (crawl do índice de ações — mantém comportamento original)
    tickers_cat_acoes: set[str] = set()
    for a in acoes:
        tickers_cat_acoes.add(a.ticker_url.upper())
        for alt in a.tickers_alternativos:
            tickers_cat_acoes.add(alt.upper())

    novos_acoes: list[AlvoXP] = []
    try:
        html_acoes = fetch_html(INDICE_ACOES, fonte=FONTE)
        if html_acoes:
            for tk in _extrair_tickers_indice_acoes(html_acoes):
                if tk not in tickers_cat_acoes:
                    novos_acoes.append(AlvoXP(
                        codigo_b3=tk, ticker_url=tk, tipo_ativo="acao",
                        url=BASE_ACOES.format(ticker=tk.lower()),
                        no_catalogo=False,
                    ))
    except Exception as e:
        print(f"[descobrir] aviso: índice de ações indisponível ({e})")

    alvos_cat = acoes + fiis_cat
    novos = novos_acoes + fiis_novos
    return alvos_cat, novos
