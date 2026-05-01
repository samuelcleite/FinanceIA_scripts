"""
Descoberta de URLs do XP Research.

Padrão da fonte:
- Ações:  https://conteudos.xpi.com.br/acoes/{ticker_individual_lower}/
- FIIs:   https://conteudos.xpi.com.br/fundos-imobiliarios/{ticker_lower}/

Estratégia "1 alvo por raiz":
  Como o XP publica 1 relatório por empresa (não por classe de ação),
  geramos UM alvo por codigo_b3 raiz e tentamos os tickers individuais
  do array `tickers` em ordem (preferindo ON, depois PN, depois Units).
  O primeiro que retornar conteúdo válido vence — os outros são ignorados.

Isso elimina chamadas Claude redundantes (PETR3 e PETR4 antes faziam
2 chamadas pra produzir o mesmo registro).
"""
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup

from core.catalog_loader import carregar_acoes, carregar_fiis
from core.http import fetch_html


BASE_ACOES = "https://conteudos.xpi.com.br/acoes/{ticker}/"
BASE_FIIS = "https://conteudos.xpi.com.br/fundos-imobiliarios/{ticker}/"

INDICE_ACOES = "https://conteudos.xpi.com.br/acoes/"
INDICE_FIIS = "https://conteudos.xpi.com.br/fundos-imobiliarios/"

FONTE = "xp_research"


@dataclass
class AlvoXP:
    codigo_b3: str                  # raiz (PETR, B3SA) — chave do registro
    ticker_url: str                 # ticker que está sendo tentado agora
    tipo_ativo: str                 # "acao" | "fii"
    url: str
    no_catalogo: bool
    tickers_alternativos: list[str] = field(default_factory=list)  # fallback


def _url_para(ticker_url: str, tipo: str) -> str:
    base = BASE_ACOES if tipo == "acao" else BASE_FIIS
    return base.format(ticker=ticker_url.lower())


def _ordenar_tickers(tickers: list[str]) -> list[str]:
    """
    Ordena tickers individuais para escolher o "principal" da empresa.
    Prioridade:
      1. ON (sufixo 3) — geralmente mais negociado
      2. PN (sufixo 4)
      3. Units (sufixo 11)
      4. Demais (5, 6, etc.)
    """
    def chave(tk: str) -> tuple:
        # Extrai sufixo numérico
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


def descobrir_do_catalogo() -> list[AlvoXP]:
    """
    Monta UM candidato por codigo_b3 raiz.

    Para cada raiz, escolhe o ticker "principal" (ON > PN > Units) como
    URL primária, e guarda os outros como `tickers_alternativos` para o
    extractor tentar como fallback se o primeiro não tiver conteúdo.
    """
    alvos: list[AlvoXP] = []

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
            url=_url_para(principal, "acao"),
            no_catalogo=True,
            tickers_alternativos=alternativos,
        ))

    for fii in carregar_fiis():
        raiz = fii["codigo_b3"]
        tickers = fii.get("tickers") or [raiz]
        # FII normalmente tem só um ticker
        principal = tickers[0]
        alternativos = tickers[1:]

        alvos.append(AlvoXP(
            codigo_b3=raiz,
            ticker_url=principal,
            tipo_ativo="fii",
            url=_url_para(principal, "fii"),
            no_catalogo=True,
            tickers_alternativos=alternativos,
        ))

    return alvos


def _extrair_tickers_indice(html: str, padrao_caminho: str) -> set[str]:
    """Extrai tickers individuais (com sufixo) das páginas índice."""
    soup = BeautifulSoup(html, "lxml")
    tickers: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if padrao_caminho not in href:
            continue
        parts = [p for p in href.strip("/").split("/") if p]
        if not parts:
            continue
        cand = parts[-1].upper()
        if 5 <= len(cand) <= 7:
            i = 0
            while i < len(cand) and cand[i].isalpha():
                i += 1
            letras = cand[:i]
            digitos = cand[i:]
            if 3 <= len(letras) <= 5 and 1 <= len(digitos) <= 2 and digitos.isdigit():
                tickers.add(cand)

    return tickers


def descobrir_do_indice() -> dict[str, set[str]]:
    """Crawl tolerante das páginas índice. Falhas geram aviso, não erro."""
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

    "Novos" são tickers individuais cobertos pelo XP que não estão em
    nenhum array `tickers` do catálogo — ficam pra revisão manual.
    """
    alvos_cat = descobrir_do_catalogo()

    # Conjunto de TODOS os tickers individuais cobertos pelo catálogo
    tickers_cat: set[str] = set()
    for a in alvos_cat:
        tickers_cat.add(a.ticker_url.upper())
        for alt in a.tickers_alternativos:
            tickers_cat.add(alt.upper())

    descobertos = descobrir_do_indice()

    novos: list[AlvoXP] = []
    for tipo, tickers in descobertos.items():
        for tk in tickers:
            if tk not in tickers_cat:
                novos.append(AlvoXP(
                    codigo_b3=tk,            # sem raiz conhecida
                    ticker_url=tk,
                    tipo_ativo=tipo,
                    url=_url_para(tk, tipo),
                    no_catalogo=False,
                ))

    return alvos_cat, novos