"""
Descoberta de PDFs públicos do Itaú BBA Research, em 3 categorias:

1. "setorial"  — relatórios setoriais semestrais de FII (Lajes, Galpões, etc).
                 Página: relatorios-setoriais---fundos-imobiliarios.aspx.
2. "mensal"    — Relatório Informativo Mensal de FII (mais recente do mês).
                 Página: assessoria/relatorio-de-fundos-imobiliarios.aspx.
3. "radar"     — Radar de Preferências de Ações (1 PDF mensal com top picks
                 setoriais). Página: radar-de-preferencias.aspx.

Todas as 3 emitem entradas em `analises` com fonte = "itau_bba_research"; a
distinção entre categorias é feita pelo `url_fonte` (e pela `data_referencia`,
que é semestral pra setorial e mensal pras outras duas).

Escopo: relatórios de empresas individuais (carteiras Top 5, Dividendos,
Small Caps, análises por empresa) exigem login — fora deste pipeline.
"""

from __future__ import annotations

import html as html_lib
import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Iterable

from core import http

log = logging.getLogger(__name__)

FONTE = "itau_bba_research"

URL_SETORIAIS = (
    "https://www.itaucorretora.com.br/"
    "relatorios-setoriais---fundos-imobiliarios.aspx"
)
URL_MENSAL = (
    "https://www.itaucorretora.com.br/"
    "assessoria/relatorio-de-fundos-imobiliarios.aspx"
)
URL_RADAR = (
    "https://www.itaucorretora.com.br/radar-de-preferencias.aspx"
)

CATEGORIAS_VALIDAS = ("setorial", "mensal", "radar")

# ----- Setoriais (FII) -----

# (slug_segmento, nome_humano, palavras_chave_no_texto_do_link)
SEGMENTOS: list[tuple[str, str, tuple[str, ...]]] = [
    ("multissetorial",     "Multissetorial",      ("multissetorial",)),
    ("lajes-corporativas", "Lajes Corporativas",  ("lajes", "escritorio")),
    ("galpoes-logisticos", "Galpões Logísticos",  ("galpoes", "logistic")),
    ("ativos-financeiros", "Ativos Financeiros",  ("ativos financeiros", "cris")),
    ("shopping-centers",   "Shopping Centers",    ("shopping",)),
]

# ----- Meses (mensal) -----

MESES_PT: dict[str, int] = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

# ----- Regex compartilhados -----

_RE_LINK = re.compile(
    r"""<a[^>]*href=["']([^"']+\.pdf[^"']*)["'][^>]*>([^<]*)</a>""",
    re.IGNORECASE,
)
_RE_PERIODO_SEMESTRAL = re.compile(r"\b([12])S(\d{2})\b", re.IGNORECASE)
_RE_PERIODO_ANUAL = re.compile(r"\b(20\d{2})\b")
_RE_MES_ANO = re.compile(
    r"\b(janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|"
    r"setembro|outubro|novembro|dezembro)\s+de\s+(20\d{2})\b",
    re.IGNORECASE,
)
_RE_RADAR_DATA = re.compile(
    r"Radar_de_Preferencias_(\d{4})(\d{2})(\d{2})\.pdf",
    re.IGNORECASE,
)


@dataclass
class AlvoItauBBA:
    """Alvo de extração: um PDF com 1+ ativos dentro."""

    slug: str
    url: str
    categoria: str = "setorial"            # "setorial" | "mensal" | "radar"
    tipo_ativo: str = "fii"                # "fii" | "acao"
    data_referencia: str | None = None     # ISO YYYY-MM-DD (já calculado)
    segmento: str | None = None            # só para setorial
    periodo: str | None = None             # só para setorial (1S26, 2S25, ...)
    extras: dict = field(default_factory=dict)


# ===== helpers =====

def _sem_acento(s: str) -> str:
    tabela = str.maketrans(
        "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
    )
    return s.translate(tabela)


def _norm_texto(raw: str) -> str:
    """Decodifica entidades HTML, troca &nbsp por espaço, colapsa espaços."""
    decoded = html_lib.unescape(raw)
    decoded = decoded.replace("\xa0", " ")
    return re.sub(r"\s+", " ", decoded).strip()


def _force_https(url: str) -> str:
    """
    Substitui http:// por https://. O Anthropic API exige HTTPS para
    document.source.type=url, e os servidores Itaú (ww69.itau.com.br,
    mindassets.cloud.itau.com.br) aceitam HTTPS perfeitamente — só que
    parte dos links no HTML do site usam http://.
    """
    if url.startswith("http://"):
        return "https://" + url[len("http://"):]
    return url


def _parse_links(html: str) -> list[tuple[str, str]]:
    """Retorna lista de (url_pdf, texto_normalizado_do_link)."""
    return [
        (_force_https(u.strip()), _norm_texto(t))
        for u, t in _RE_LINK.findall(html)
    ]


# ===== setoriais =====

def _identificar_segmento(texto_link: str) -> tuple[str, str] | None:
    chave = _sem_acento(texto_link).lower()
    for slug_seg, nome, kws in SEGMENTOS:
        if any(kw in chave for kw in kws):
            return slug_seg, nome
    return None


def _identificar_periodo(texto_link: str) -> str | None:
    m = _RE_PERIODO_SEMESTRAL.search(texto_link)
    if m:
        return f"{m.group(1)}S{m.group(2)}".upper()
    m = _RE_PERIODO_ANUAL.search(texto_link)
    if m:
        return m.group(1)
    return None


def _ordem_periodo(periodo: str | None) -> tuple[int, int]:
    if not periodo:
        return (-1, -1)
    m = _RE_PERIODO_SEMESTRAL.fullmatch(periodo)
    if m:
        return (2000 + int(m.group(2)), int(m.group(1)))
    m = _RE_PERIODO_ANUAL.fullmatch(periodo)
    if m:
        return (int(m.group(1)), 0)
    return (-1, -1)


def _data_ref_setorial(periodo: str | None) -> str:
    """1S26 -> 2026-01-01, 2S25 -> 2025-07-01, 2025 -> 2025-01-01."""
    p = (periodo or "").upper()
    m = _RE_PERIODO_SEMESTRAL.fullmatch(p)
    if m:
        ano = 2000 + int(m.group(2))
        mes = 1 if m.group(1) == "1" else 7
        return date(ano, mes, 1).isoformat()
    m = _RE_PERIODO_ANUAL.fullmatch(p)
    if m:
        return date(int(m.group(1)), 1, 1).isoformat()
    return date.today().replace(day=1).isoformat()


def _descobrir_setoriais(*, historico: bool) -> list[AlvoItauBBA]:
    log.info("Itaú BBA setoriais: crawl de %s", URL_SETORIAIS)
    html = http.fetch_html(URL_SETORIAIS, fonte=FONTE)
    if not html:
        log.error("Página setoriais retornou vazia — abortando")
        return []

    pares = _parse_links(html)
    log.info("Setoriais: %d link(s) .pdf", len(pares))

    alvos: list[AlvoItauBBA] = []
    for url, texto in pares:
        seg = _identificar_segmento(texto)
        if seg is None:
            continue
        slug_seg, nome = seg
        periodo = _identificar_periodo(texto)
        slug = f"{slug_seg}-{periodo.lower()}" if periodo else slug_seg
        alvos.append(AlvoItauBBA(
            slug=slug,
            url=url,
            categoria="setorial",
            tipo_ativo="fii",
            data_referencia=_data_ref_setorial(periodo),
            segmento=nome,
            periodo=periodo,
        ))

    log.info("Setoriais: %d alvo(s) com segmento identificado", len(alvos))

    if not historico:
        # 1 alvo por segmento (o mais recente)
        por_segmento: dict[str, AlvoItauBBA] = {}
        for a in alvos:
            chave = a.slug.rsplit("-", 1)[0]
            atual = por_segmento.get(chave)
            if (atual is None
                or _ordem_periodo(a.periodo) > _ordem_periodo(atual.periodo)):
                por_segmento[chave] = a
        alvos = list(por_segmento.values())
        log.info("Setoriais filtrados (mais recente/segmento): %d", len(alvos))

    alvos.sort(key=lambda a: (
        a.segmento or "",
        -_ordem_periodo(a.periodo)[0],
        -_ordem_periodo(a.periodo)[1],
    ))
    return alvos


# ===== mensal (FII) =====

def _identificar_mes_ano(texto_link: str) -> tuple[int, int] | None:
    """Retorna (ano, mes_int) se texto bate com 'Mês de YYYY'."""
    chave = _sem_acento(texto_link).lower()
    m = _RE_MES_ANO.search(chave)
    if not m:
        return None
    mes_nome = m.group(1).replace("ç", "c")
    ano = int(m.group(2))
    mes = MESES_PT.get(mes_nome)
    if not mes:
        return None
    return (ano, mes)


def _descobrir_mensal() -> list[AlvoItauBBA]:
    """
    Retorna apenas o relatório mensal de FII MAIS RECENTE (1 alvo).

    A lista da página tem entradas tipo "Fevereiro de 2026" → URL. Pega o link
    cujo (ano, mês) é maior. O HTML do site às vezes fragmenta texto em vários
    <a>, mas o item mais recente sempre aparece como par limpo "Mês de YYYY".
    """
    log.info("Itaú BBA mensal: crawl de %s", URL_MENSAL)
    html = http.fetch_html(URL_MENSAL, fonte=FONTE)
    if not html:
        log.error("Página mensal retornou vazia — abortando")
        return []

    pares = _parse_links(html)
    log.info("Mensal: %d link(s) .pdf", len(pares))

    candidatos: list[tuple[tuple[int, int], str, str]] = []
    for url, texto in pares:
        ma = _identificar_mes_ano(texto)
        if ma is None:
            continue
        candidatos.append((ma, url, texto))

    if not candidatos:
        log.warning("Mensal: nenhum link com padrão 'Mês de YYYY' encontrado")
        return []

    candidatos.sort(key=lambda x: x[0], reverse=True)
    (ano, mes), url, texto = candidatos[0]
    data_ref = date(ano, mes, 1).isoformat()
    slug = f"mensal-{ano:04d}-{mes:02d}"
    log.info("Mensal: mais recente é %s (%s)", texto, data_ref)

    return [AlvoItauBBA(
        slug=slug,
        url=url,
        categoria="mensal",
        tipo_ativo="fii",
        data_referencia=data_ref,
        segmento="Relatório Mensal FII",
    )]


# ===== radar (ações) =====

def _descobrir_radar() -> list[AlvoItauBBA]:
    """
    Retorna o Radar de Preferências (top picks de ações) — 1 alvo.

    Filename traz a data: Radar_de_Preferencias_YYYYMMDD.pdf.

    Nota: o link na página tem <img> dentro do <a> (botão), então o regex
    genérico _RE_LINK não casa (espera texto). Aqui buscamos pelo padrão
    do filename direto no HTML — basta a URL.
    """
    log.info("Itaú BBA radar: crawl de %s", URL_RADAR)
    html = http.fetch_html(URL_RADAR, fonte=FONTE)
    if not html:
        log.error("Página radar retornou vazia — abortando")
        return []

    # Procura a URL canônica direto. Aceita https? e qualquer prefixo.
    url_re = re.compile(
        r'https?://[^\s"\'<>]*Radar_de_Preferencias_\d{8}\.pdf',
        re.IGNORECASE,
    )
    urls_encontradas: list[str] = url_re.findall(html)
    if not urls_encontradas:
        log.warning("Radar: nenhuma URL Radar_de_Preferencias_YYYYMMDD.pdf no HTML")
        return []

    # Pega a primeira (geralmente única) e extrai data do filename
    for url in urls_encontradas:
        url = _force_https(url)
        m = _RE_RADAR_DATA.search(url)
        if not m:
            continue
        ano, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # Normaliza a data_referencia pra primeiro dia do mês (mesma convenção
        # usada nas outras categorias) — preserva idempotência se o filename
        # mudar de DD dentro do mesmo mês.
        data_ref = date(ano, mes, 1).isoformat()
        slug = f"radar-{ano:04d}-{mes:02d}"
        log.info(
            "Radar: PDF encontrado data=%s-%02d-%02d -> data_ref=%s",
            ano, mes, dia, data_ref,
        )
        return [AlvoItauBBA(
            slug=slug,
            url=url,
            categoria="radar",
            tipo_ativo="acao",
            data_referencia=data_ref,
            segmento="Radar de Preferências",
        )]

    log.warning(
        "Radar: URL encontrada mas filename não bate "
        "Radar_de_Preferencias_YYYYMMDD.pdf — %s",
        urls_encontradas[0] if urls_encontradas else "(nenhuma)",
    )
    return []


# ===== fachada =====

def descobrir(
    *,
    categorias: Iterable[str] = CATEGORIAS_VALIDAS,
    historico: bool = False,
) -> list[AlvoItauBBA]:
    """
    Faz crawl das categorias selecionadas e retorna alvos.

    Args:
        categorias: subconjunto de ("setorial", "mensal", "radar").
            Default: todas.
        historico: aplica APENAS aos setoriais. Se True, retorna todos os
            ~37 históricos; se False (default), só o mais recente por
            segmento. Mensal e Radar sempre retornam só o mais recente
            (1 alvo cada).
    """
    cats = tuple(categorias)
    invalidas = [c for c in cats if c not in CATEGORIAS_VALIDAS]
    if invalidas:
        raise ValueError(
            f"Categorias inválidas: {invalidas}. "
            f"Válidas: {CATEGORIAS_VALIDAS}"
        )

    alvos: list[AlvoItauBBA] = []
    if "setorial" in cats:
        alvos.extend(_descobrir_setoriais(historico=historico))
    if "mensal" in cats:
        alvos.extend(_descobrir_mensal())
    if "radar" in cats:
        alvos.extend(_descobrir_radar())

    log.info(
        "Itaú BBA: %d alvo(s) total (categorias=%s, historico=%s)",
        len(alvos), cats, historico,
    )
    return alvos


def iter_alvos(
    *,
    categorias: Iterable[str] = CATEGORIAS_VALIDAS,
    historico: bool = False,
) -> Iterable[AlvoItauBBA]:
    """Generator-friendly wrapper sobre descobrir()."""
    yield from descobrir(categorias=categorias, historico=historico)
