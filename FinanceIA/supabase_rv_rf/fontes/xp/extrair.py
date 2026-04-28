"""
Extração de conteúdo das páginas do XP Research.

Estrutura típica das páginas:
- Título: nome do ativo + ticker
- Bloco de recomendação e preço-alvo (quando há)
- Corpo do relatório com tese, fundamentos, riscos
- Data de publicação do relatório mais recente

Como o HTML do XP pode mudar, esta função é defensiva: extrai o que
conseguir e devolve `None` quando a página não tem relatório ainda
(ex: ticker existe mas sem cobertura).
"""
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
    tipo_ativo: str
    url_fonte: str
    data_referencia: date
    texto: str
    contexto_ativo: str  # ex: "PETR4 — Petrobras PN"


def _extrair_data(soup: BeautifulSoup) -> Optional[date]:
    """Tenta extrair a data de publicação do relatório."""
    # Tentativa 1: <time datetime="...">
    t = soup.find("time")
    if t and t.get("datetime"):
        try:
            return datetime.fromisoformat(t["datetime"][:10]).date()
        except ValueError:
            pass

    # Tentativa 2: meta property article:published_time
    m = soup.find("meta", attrs={"property": "article:published_time"})
    if m and m.get("content"):
        try:
            return datetime.fromisoformat(m["content"][:10]).date()
        except ValueError:
            pass

    # Tentativa 3: regex em texto "DD/MM/AAAA" ou "DD de mês de AAAA"
    texto = soup.get_text(" ", strip=True)
    m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", texto)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    return None


def _extrair_titulo(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    title = soup.find("title")
    return title.get_text(strip=True) if title else ""


def _limpar_texto(soup: BeautifulSoup) -> str:
    """Remove header/footer/nav/script e devolve texto principal."""
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    # Tenta achar um <main> ou <article>; senão usa body
    main = soup.find("main") or soup.find("article") or soup.body
    if main is None:
        return ""

    # Junta parágrafos preservando quebras
    blocos = []
    for el in main.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        txt = el.get_text(" ", strip=True)
        if txt and len(txt) > 3:
            blocos.append(txt)

    return "\n\n".join(blocos)


def _tem_relatorio(texto: str) -> bool:
    """
    Heurística para detectar se a página tem realmente um relatório
    e não só um placeholder ou cotação solta.
    """
    if len(texto) < 800:
        return False
    palavras_chave = ["tese", "recomenda", "preço-alvo", "preço alvo",
                      "drivers", "riscos", "perspectiva", "resultado"]
    texto_lower = texto.lower()
    return sum(1 for k in palavras_chave if k in texto_lower) >= 2


def extrair(alvo: AlvoXP) -> Optional[ConteudoXP]:
    """
    Baixa e processa a página do XP. Retorna None se a página não
    existir (404) ou não tiver conteúdo de relatório.
    """
    html = fetch_html(alvo.url, fonte=FONTE)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    titulo = _extrair_titulo(soup)
    texto = _limpar_texto(soup)

    if not _tem_relatorio(texto):
        return None

    data_ref = _extrair_data(soup) or date.today()

    return ConteudoXP(
        codigo_b3=alvo.codigo_b3,
        tipo_ativo=alvo.tipo_ativo,
        url_fonte=alvo.url,
        data_referencia=data_ref,
        texto=texto,
        contexto_ativo=titulo or alvo.codigo_b3,
    )
