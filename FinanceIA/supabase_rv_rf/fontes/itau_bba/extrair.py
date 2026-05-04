"""
Extração para fonte Itaú BBA.

Padrão URL-based: não baixa o PDF aqui. Apenas valida via HEAD que a URL existe
e repassa para o analisar.py, que delega download/parse ao Claude
(document.url no Anthropic API).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from core import http

from .descobrir import AlvoItauBBA

log = logging.getLogger(__name__)


@dataclass
class ConteudoItauBBA:
    """O que sai do extrair e entra no analisar (sem texto — Claude lê o PDF)."""

    alvo: AlvoItauBBA
    url_pdf: str


def extrair(
    alvo: AlvoItauBBA, *, validar: bool = True
) -> Optional[ConteudoItauBBA]:
    """
    Valida o PDF e retorna ConteudoItauBBA pronto para o analisar.

    Args:
        alvo: alvo descoberto pelo descobrir.py.
        validar: se True (default), faz HEAD na URL pra confirmar 200.

    Returns:
        ConteudoItauBBA, ou None se a URL retornar 404/erro.
    """
    if not validar:
        return ConteudoItauBBA(alvo=alvo, url_pdf=alvo.url)

    if not http.head_ok(alvo.url):
        log.warning(
            "PDF indisponível (HEAD falhou ou !=200): %s — pulando", alvo.url
        )
        return None

    return ConteudoItauBBA(alvo=alvo, url_pdf=alvo.url)
