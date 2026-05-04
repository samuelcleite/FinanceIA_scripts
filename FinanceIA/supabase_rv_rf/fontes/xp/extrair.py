@"
"""
Extração de conteúdo das páginas do XP Research.

Suporta dois formatos:
  - Ações: página canônica /acoes/{ticker}/ com H2 "Análise Fundamentalista"
  - FIIs:  relatório avulso /fundos-imobiliarios/relatorios/{slug}/ com
           estrutura mais livre (sem H2 padronizado, mas com corpo narrativo)

Estratégia para data_referencia (camadas):
1. JSON-LD (datePublished/dateModified)
2. <meta property="article:modified_time">
3. <time datetime="...">
4. Texto "Atualizado em DD/MM/AAAA"
5. Hoje (fallback)
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
# Detecção de relatório real
# ---------------------------------------------------------------------------

# H2s típicos de relatório de ação
_TERMOS_H2_ACAO = [
    "análise fundamentalista", "vale a pena investir", "tese de investimento",
]

# Termos analíticos que indicam corpo de relatório real (FII ou ação)
_TERMOS_ANALITICOS = [
    "tese", "recomend", "dividend yield", "dy", "p/vp", "vacância",
    "portfólio", "lajes", "logístic", "recebível", "cri", "lci",
    "gestão", "emissor", "inadimplência", "spread", "ipca", "cdi",
    "resultado", "rendimento", "distribuição", "cota", "patrimônio",
    "ebitda", "margem", "lucro", "receita", "guidance", "valuation",
]


def _contar_termos_analiticos(texto: str) -> int:
    t = texto.lower()
    return sum(1 for termo in _TERMOS_ANALITICOS if termo in t)


def _paragrafos_narrativos(paragrafos: list[str]) -> list[str]:
    return [
        p for p in paragrafos
        if len(p) >= 150
        and p.count(" ") >= 15
        and (p.count(".") + p.count(",")) >= 2
    ]


def _tem_relatorio(soup: BeautifulSoup, tipo_ativo: str = "acao") -> bool:
    """
    Ações: exige H2 analítico + 2 parágrafos narrativos.
    FIIs:  sem H2 padronizado — exige corpo narrativo + densidade de termos.
    """
    paragrafos = _extrair_paragrafos(soup)
    narrativos = _paragrafos_narrativos(paragrafos)
    texto_corpo = " ".join(paragrafos)

    if tipo_ativo == "fii":
        # Relatório avulso de FII: sem H2 fixo, mas precisa de conteúdo real
        if len(narrativos) < 2:
            return False
        if _contar_termos_analiticos(texto_corpo) < 3:
            return False
        return True
    else:
        # Ações: marcador estrutural obrigatório
        h2_analitico = any(
            any(termo in h2.get_text(" ", strip=True).lower()
                for termo in _TERMOS_H2_ACAO)
            for h2 in soup.find_all("h2")
        )
        if not h2_analitico:
            return False
        return len(narrativos) >= 2


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def extrair(alvo: AlvoXP) -> Optional[ConteudoXP]:
    """
    Baixa e processa a página do XP. Retorna None se 404 ou sem relatório real.
    """
    html = fetch_html(alvo.url, fonte=FONTE)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    titulo = _extrair_titulo(soup)
    paragrafos = _extrair_paragrafos(soup)

    if not _tem_relatorio(soup, tipo_ativo=alvo.tipo_ativo):
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
"@ | Out-File -FilePath fontes\xp\extrair.py -Encoding utf8