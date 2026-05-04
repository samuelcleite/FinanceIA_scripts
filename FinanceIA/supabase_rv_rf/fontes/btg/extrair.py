"""
Extração de conteúdo BTG.

Diferente do XP, o BTG não exige fetch externo — o body HTML já vem dentro
do JSON da listagem (`AlvoBTG.body_html`). Aqui apenas convertemos HTML em
texto limpo (descartando <figure>/<img>/scripts) e validamos um mínimo de
substância antes de chamar o Claude.
"""
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup

from fontes.btg.descobrir import AlvoBTG


# Mínimos para considerar que vale a pena enviar para o Claude.
_MIN_CHARS = 400
_MIN_PARAGRAFOS = 2


@dataclass
class ConteudoBTG:
    btg_id: str
    titulo: str
    categoria: str
    data_referencia: date
    texto: str
    ativos_brutos: list[dict]


def _data_de_iso(iso: str) -> Optional[date]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso[:19]).date()
    except ValueError:
        try:
            return datetime.fromisoformat(iso[:10]).date()
        except ValueError:
            return None


def _html_para_texto(html: str) -> tuple[str, int]:
    """
    Converte HTML em texto plano. Retorna (texto, n_paragrafos).
    Remove figuras/imagens/scripts; preserva quebras de parágrafo.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "figure", "img"]):
        tag.decompose()

    paragrafos: list[str] = []
    for el in soup.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        txt = el.get_text(" ", strip=True)
        if txt:
            paragrafos.append(txt)

    texto = "\n\n".join(paragrafos)
    return texto, len(paragrafos)


def extrair(alvo: AlvoBTG) -> Optional[ConteudoBTG]:
    """
    Constrói ConteudoBTG a partir de AlvoBTG. Retorna None se o conteúdo
    for insuficiente para análise.
    """
    texto, n_paragrafos = _html_para_texto(alvo.body_html)

    if len(texto) < _MIN_CHARS or n_paragrafos < _MIN_PARAGRAFOS:
        return None

    data_ref = _data_de_iso(alvo.referencia_iso) or date.today()

    return ConteudoBTG(
        btg_id=alvo.btg_id,
        titulo=alvo.titulo,
        categoria=alvo.categoria,
        data_referencia=data_ref,
        texto=texto,
        ativos_brutos=alvo.ativos_brutos,
    )
