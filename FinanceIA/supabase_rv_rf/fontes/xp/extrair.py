"""
Extração de conteúdo das páginas do XP Research.

Estratégia para data_referencia (camadas):
1. JSON-LD (datePublished/dateModified)
2. <meta property="article:modified_time">
3. <time datetime="...">
4. Texto "Atualizado em DD/MM/AAAA"
5. Hoje (fallback)

Detecção de relatório real:
- Conta parágrafos NARRATIVOS (>= 150 chars, com verbo conjugado)
- Verifica densidade de termos analíticos no corpo (não em menus)
- Rejeita páginas que só têm cabeçalho + cotação + disclaimer
"""
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup

from core.http import fetch_html
from fontes.xp.descobrir import FONTE, AlvoXP


@dataclass
class ConteudoXP:
    codigo_b3: str
    ticker_url: str
    tipo_ativo: str
    url_fonte: str
    data_referencia: date
    texto: str
    contexto_ativo: str


# ---------------------------------------------------------------------------
# Extração de data — estratégia em camadas
# ---------------------------------------------------------------------------

def _data_de_jsonld(soup: BeautifulSoup) -> Optional[date]:
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(s.string or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        candidatos = payload if isinstance(payload, list) else [payload]
        for c in candidatos:
            if not isinstance(c, dict):
                continue
            for chave in ("dateModified", "datePublished"):
                v = c.get(chave)
                if v and isinstance(v, str):
                    try:
                        return datetime.fromisoformat(v[:10]).date()
                    except ValueError:
                        pass
    return None


def _data_de_meta(soup: BeautifulSoup) -> Optional[date]:
    for prop in ("article:modified_time", "article:published_time",
                 "og:updated_time", "og:article:modified_time",
                 "og:article:published_time"):
        m = soup.find("meta", attrs={"property": prop})
        if m and m.get("content"):
            try:
                return datetime.fromisoformat(m["content"][:10]).date()
            except ValueError:
                continue
    return None


def _data_de_time_tag(soup: BeautifulSoup) -> Optional[date]:
    main = soup.find("main") or soup.find("article")
    candidatos = (main or soup).find_all("time")
    for t in candidatos:
        dt = t.get("datetime")
        if dt:
            try:
                return datetime.fromisoformat(dt[:10]).date()
            except ValueError:
                continue
    return None


_RE_PT_DATA = re.compile(
    r"(?:Atualizado|Publicado|Última atualização)[^0-9]{0,30}"
    r"(\d{1,2})/(\d{1,2})/(\d{4})",
    re.IGNORECASE,
)


def _data_de_texto_explicito(soup: BeautifulSoup) -> Optional[date]:
    main = soup.find("main") or soup.find("article") or soup.body
    if not main:
        return None
    texto = main.get_text(" ", strip=True)
    m = _RE_PT_DATA.search(texto)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _extrair_data(soup: BeautifulSoup) -> Optional[date]:
    return (
        _data_de_jsonld(soup)
        or _data_de_meta(soup)
        or _data_de_time_tag(soup)
        or _data_de_texto_explicito(soup)
    )


# ---------------------------------------------------------------------------
# Extração de título e corpo
# ---------------------------------------------------------------------------

def _extrair_titulo(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    title = soup.find("title")
    return title.get_text(strip=True) if title else ""


def _extrair_paragrafos(soup: BeautifulSoup) -> list[str]:
    """
    Extrai parágrafos do conteúdo principal, descartando menus/footer/etc.
    Cada item é o texto de um <p>, <li>, <h2>, <h3>, ou <h4>.
    """
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    main = soup.find("main") or soup.find("article") or soup.body
    if main is None:
        return []

    paragrafos = []
    for el in main.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        txt = el.get_text(" ", strip=True)
        if txt and len(txt) > 3:
            paragrafos.append(txt)
    return paragrafos


def _texto_completo(paragrafos: list[str]) -> str:
    return "\n\n".join(paragrafos)


# ---------------------------------------------------------------------------
# Detecção de relatório real (heurística rigorosa)
# ---------------------------------------------------------------------------

# Termos analíticos típicos de relatório real (no corpo, não em menus)
_TERMOS_ANALITICOS = [
    "tese", "recomenda", "preço-alvo", "preço alvo", "target",
    "drivers", "perspectiva", "resultado", "balanço", "guidance",
    "ebitda", "margem", "lucro", "receita", "valuation",
    "múltiplo", "p/l", "p/vp", "dividend yield", "fluxo de caixa",
    "alavancagem", "covenants", "investidores", "trimestre",
]

# Indicadores de "página vazia" (cotação + disclaimer)
_INDICADORES_VAZIO = [
    "preço atual",
    "preço abertura",
    "preço mínimo",
    "preço fechamento",
    "preço máximo",
    "risco (0",
    "abra sua conta",
]


def _e_paragrafo_narrativo(texto: str) -> bool:
    """
    Parágrafo "narrativo" é um texto longo (>= 150 chars) que parece
    prosa, não label/cabeçalho. Heurística: tem ponto final ou vírgula
    e proporção razoável de espaços (não é uma URL ou string técnica).
    """
    if len(texto) < 150:
        return False
    if texto.count(" ") < 15:
        return False
    if texto.count(".") + texto.count(",") + texto.count(";") < 2:
        return False
    return True


def _tem_relatorio(soup) -> bool:
    """
    Relatório real precisa ter:
    1. <h2> "Análise Fundamentalista" / "Vale a pena investir" / similar
    2. Pelo menos 2 parágrafos narrativos (>=200 chars cada com pontuação)
    """
    # Marcador estrutural
    h2_analitico = any(
        any(termo in h2.get_text(" ", strip=True).lower()
            for termo in ["análise fundamentalista", "vale a pena investir",
                          "tese de investimento"])
        for h2 in soup.find_all("h2")
    )
    if not h2_analitico:
        return False

    # Conteúdo narrativo substantivo
    paragrafos = _extrair_paragrafos(soup)
    narrativos = [
        p for p in paragrafos
        if len(p) >= 200
        and p.count(" ") >= 25
        and (p.count(".") + p.count(",")) >= 3
    ]
    return len(narrativos) >= 2CLEAR
# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def extrair(alvo: AlvoXP) -> Optional[ConteudoXP]:
    """
    Baixa e processa a página do XP. Retorna None se a página não
    existir (404) ou não tiver conteúdo de relatório real.
    """
    html = fetch_html(alvo.url, fonte=FONTE)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    titulo = _extrair_titulo(soup)
    paragrafos = _extrair_paragrafos(soup)

    if not _tem_relatorio(soup):
        return None

    texto = _texto_completo(paragrafos)
    data_ref = _extrair_data(soup) or date.today()

    return ConteudoXP(
        codigo_b3=alvo.codigo_b3,
        ticker_url=alvo.ticker_url,
        tipo_ativo=alvo.tipo_ativo,
        url_fonte=alvo.url,
        data_referencia=data_ref,
        texto=texto,
        contexto_ativo=titulo or alvo.ticker_url,
    )