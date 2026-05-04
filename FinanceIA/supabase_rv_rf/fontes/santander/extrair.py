"""
Extração para fonte Santander.

Padrão URL-based: a função aqui não baixa o PDF. Ela apenas valida a URL
(via HEAD) e repassa para o analisar.py, que delega ao Claude o
download/parse via document.url no Anthropic API.

Vantagem: zero I/O local, sem cache de PDF, sem risco de payloads grandes
em base64.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from core import http

from .descobrir import AlvoSantander

log = logging.getLogger(__name__)


@dataclass
class ConteudoSantander:
    """
    O que sai do extrair e entra no analisar.

    Não contém o texto do PDF — quem lê o PDF é o Claude (URL-based).
    """

    alvo: AlvoSantander
    url_pdf: str  # URL do PDF (não-redirecionada — head_ok não retorna URL final)


def extrair(alvo: AlvoSantander, *, validar: bool = True) -> Optional[ConteudoSantander]:
    """
    Valida o PDF e retorna ConteudoSantander pronto para o analisar.

    Args:
        alvo: alvo descoberto pelo descobrir.py
        validar: se True, faz HEAD na URL para confirmar que existe (200).
            Se False, retorna o conteúdo sem validação (útil para debug).

    Returns:
        ConteudoSantander, ou None se a URL retornar 404/erro.
    """
    if not validar:
        return ConteudoSantander(alvo=alvo, url_pdf=alvo.url)

    if not http.head_ok(alvo.url):
        log.warning("PDF indisponível (HEAD falhou ou !=200): %s — pulando", alvo.url)
        return None

    return ConteudoSantander(alvo=alvo, url_pdf=alvo.url)