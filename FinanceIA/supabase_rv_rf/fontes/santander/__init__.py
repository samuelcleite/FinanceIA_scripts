"""Fonte Santander Research — PDFs públicos URL-based, multi-ativo."""

from .descobrir import AlvoSantander, descobrir, iter_alvos
from .extrair import ConteudoSantander, extrair
from .analisar import FONTE, analisar

__all__ = [
    "AlvoSantander",
    "ConteudoSantander",
    "FONTE",
    "descobrir",
    "iter_alvos",
    "extrair",
    "analisar",
]