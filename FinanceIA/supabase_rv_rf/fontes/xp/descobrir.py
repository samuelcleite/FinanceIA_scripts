"""
Descoberta de URLs do XP Research.

Padrão da fonte:
- Ações:  https://conteudos.xpi.com.br/acoes/{ticker_lower}/
- FIIs:   https://conteudos.xpi.com.br/fundos-imobiliarios/{ticker_lower}/

Estratégia híbrida:
1. Catálogo: monta URL para cada ticker ativo em ativos_rv
2. Descoberta: scrape das páginas índice para flagar tickers fora do catálogo
"""
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup

from core.catalog_loader import carregar_acoes, carregar_fiis
from core.http import fetch_html, head_ok


BASE_ACOES = "https://conteudos.xpi.com.br/acoes/{ticker}/"
BASE_FIIS = "https://conteudos.xpi.com.br/fundos-imobiliarios/{ticker}/"

INDICE_ACOES = "https://conteudos.xpi.com.br/acoes/"
INDICE_FIIS = "https://conteudos.xpi.com.br/fundos-imobiliarios/"

FONTE = "xp_research"


@dataclass
class AlvoXP:
    codigo_b3: str
    tipo_ativo: str  # "acao" | "fii"
    url: str
    no_catalogo: bool


def _url_para(ticker: str, tipo: str) -> str:
    base = BASE_ACOES if tipo == "acao" else BASE_FIIS
    return base.format(ticker=ticker.lower())


def descobrir_do_catalogo() -> list[AlvoXP]:
    """Monta candidatos a partir do catálogo Supabase."""
    alvos: list[AlvoXP] = []

    for acao in carregar_acoes():
        ticker = acao["codigo_b3"]
        alvos.append(AlvoXP(
            codigo_b3=ticker,
            tipo_ativo="acao",
            url=_url_para(ticker, "acao"),
            no_catalogo=True,
        ))

    for fii in carregar_fiis():
        ticker = fii["codigo_b3"]
        alvos.append(AlvoXP(
            codigo_b3=ticker,
            tipo_ativo="fii",
            url=_url_para(ticker, "fii"),
            no_catalogo=True,
        ))

    return alvos


def _extrair_tickers_indice(html: str, padrao_caminho: str) -> set[str]:
    """
    Extrai tickers das páginas índice do XP.
    `padrao_caminho` ex: "/acoes/" ou "/fundos-imobiliarios/"
    """
    soup = BeautifulSoup(html, "lxml")
    tickers: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if padrao_caminho not in href:
            continue
        # Ex: /acoes/petr4/  →  petr4
        parts = [p for p in href.strip("/").split("/") if p]
        if len(parts) >= 2 and parts[-2] in padrao_caminho.strip("/"):
            cand = parts[-1].upper()
            # Validação simples: tickers B3 têm 4 letras + 1-2 dígitos
            if 5 <= len(cand) <= 6 and cand[:4].isalpha() and cand[4:].isdigit():
                tickers.add(cand)
    return tickers


def descobrir_do_indice() -> dict[str, set[str]]:
    """
    Faz crawl das páginas índice para descobrir tickers que o XP cobre.
    Retorna {"acao": {tickers}, "fii": {tickers}}.

    Se o crawl falhar (403, timeout etc.), retorna sets vazios sem
    abortar o pipeline — a descoberta de "novos" é apenas auxiliar.
    """
    out = {"acao": set(), "fii": set()}

    try:
        html_acoes = fetch_html(INDICE_ACOES, fonte=FONTE)
        if html_acoes:
            out["acao"] = _extrair_tickers_indice(html_acoes, "/acoes/")
    except Exception as e:
        print(f"[descobrir.xp] aviso: índice de ações indisponível ({e})")

    try:
        html_fiis = fetch_html(INDICE_FIIS, fonte=FONTE)
        if html_fiis:
            out["fii"] = _extrair_tickers_indice(html_fiis, "/fundos-imobiliarios/")
    except Exception as e:
        print(f"[descobrir.xp] aviso: índice de FIIs indisponível ({e})")

    return out

def descobrir_todos() -> tuple[list[AlvoXP], list[AlvoXP]]:
    """
    Combina catálogo + descoberta de índice.

    Retorna:
      (alvos_para_processar, novos_descobertos_fora_do_catalogo)
    """
    alvos_cat = descobrir_do_catalogo()
    tickers_cat = {a.codigo_b3 for a in alvos_cat}

    descobertos = descobrir_do_indice()

    novos: list[AlvoXP] = []
    for tipo, tickers in descobertos.items():
        for tk in tickers:
            if tk not in tickers_cat:
                novos.append(AlvoXP(
                    codigo_b3=tk,
                    tipo_ativo=tipo,
                    url=_url_para(tk, tipo),
                    no_catalogo=False,
                ))

    # Para processar = só os do catálogo (novos ficam pra revisão manual)
    return alvos_cat, novos
