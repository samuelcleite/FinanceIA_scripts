"""
Análise de PDFs Itaú BBA via Claude (Sonnet, URL-based, multi-ativo).

Recebe um ConteudoItauBBA, chama claude_client.analisar_pdf_url_multi e devolve
uma lista de dicts canônicos prontos pra upsert na tabela `analises`.

Suporta as 3 categorias do descobrir.py (setorial / mensal / radar):
- tipo_ativo é determinado pelo alvo (fii pra setorial+mensal, acao pra radar).
- O catálogo de mapeamento ticker→raiz é carregado conforme tipo_ativo.
- data_referencia já vem calculada do descobrir.py — apenas reusamos.
- recomendacao NÃO é hardcoded — relatórios cobrem segmentos completos com
  mix compra/neutro/venda; usamos o que o Claude extrair.
- preco_alvo NÃO é zerado — preservamos o que o Claude devolver.
"""

from __future__ import annotations

import logging
from functools import lru_cache

from core import catalog_loader, claude_client

from .descobrir import AlvoItauBBA
from .extrair import ConteudoItauBBA

log = logging.getLogger(__name__)

FONTE = "itau_bba_research"

_INSTRUCAO_FII = (
    "Para cada FII identificado, use no campo 'codigo_b3' o ticker exatamente "
    "como aparece no PDF (ex: HGLG11, KNRI11, BRCO11). Não tente abreviar nem "
    "normalizar. Mantenha a recomendacao individual por ticker "
    "(compra/neutro/venda) conforme o relatório."
)

_INSTRUCAO_ACAO = (
    "Para cada ação identificada, use no campo 'codigo_b3' o ticker exatamente "
    "como aparece no PDF (ex: PETR4, VALE3, ITUB4). Mantenha a recomendacao "
    "individual por ticker conforme o relatório (este é um Radar de "
    "Preferências com top picks setoriais — todos os listados são preferidos)."
)


@lru_cache(maxsize=2)
def _mapa_tickers(tipo_ativo: str) -> dict[str, str]:
    """
    Constrói {ticker_individual_upper: codigo_b3_raiz} para o tipo dado.
    Cache por tipo_ativo para evitar refetch do catálogo no mesmo run.
    """
    if tipo_ativo == "fii":
        rows = catalog_loader.carregar_fiis()
    elif tipo_ativo == "acao":
        rows = catalog_loader.carregar_acoes()
    else:
        log.warning("Tipo_ativo sem catálogo mapeado: %s", tipo_ativo)
        return {}

    mapa: dict[str, str] = {}
    for r in rows:
        raiz = r.get("codigo_b3")
        if not raiz:
            continue
        mapa[raiz.upper()] = raiz
        for tk in r.get("tickers") or []:
            if tk:
                mapa[tk.upper()] = raiz
    log.info(
        "Catálogo %s carregado: %d tickers -> %d ativos raiz",
        tipo_ativo, len(mapa), len(rows),
    )
    return mapa


def _normalizar_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


def _instrucao_extra(tipo_ativo: str) -> str:
    return _INSTRUCAO_ACAO if tipo_ativo == "acao" else _INSTRUCAO_FII


def analisar(conteudo: ConteudoItauBBA) -> list[dict]:
    """
    Roda Claude no PDF e devolve lista de dicts canônicos prontos pra
    upsert em `analises`:
        tipo_ativo, codigo_b3 (raiz), fonte, url_fonte, data_referencia,
        tese_investimento, drivers (list[str]), riscos (list[str]),
        recomendacao, preco_alvo, rating, spread_indicativo
    """
    alvo: AlvoItauBBA = conteudo.alvo
    log.info(
        "Analisando %s [%s/%s] %s",
        alvo.slug, alvo.categoria, alvo.tipo_ativo, conteudo.url_pdf,
    )

    if not alvo.data_referencia:
        log.error(
            "Alvo %s sem data_referencia — abortando analisar()", alvo.slug,
        )
        return []

    try:
        analises_brutas = claude_client.analisar_pdf_url_multi(
            pdf_url=conteudo.url_pdf,
            tipo_ativo=alvo.tipo_ativo,
            instrucao_extra=_instrucao_extra(alvo.tipo_ativo),
        )
    except Exception as exc:
        log.exception("Falha ao chamar Claude para %s: %s", alvo.slug, exc)
        return []

    if not analises_brutas:
        log.warning("Claude retornou lista vazia para %s", alvo.slug)
        return []

    log.info(
        "Claude extraiu %d ativo(s) de %s (data_ref=%s)",
        len(analises_brutas), alvo.slug, alvo.data_referencia,
    )

    tickers_map = _mapa_tickers(alvo.tipo_ativo)
    resultados: list[dict] = []
    descartados: list[str] = []

    for bruto in analises_brutas:
        ticker = _normalizar_ticker(bruto.get("codigo_b3") or "")
        if not ticker:
            log.warning("Análise sem codigo_b3 em %s — descartada", alvo.slug)
            continue

        codigo_raiz = tickers_map.get(ticker)
        if not codigo_raiz:
            descartados.append(ticker)
            continue

        drivers = bruto.get("drivers") or []
        riscos = bruto.get("riscos") or []

        resultados.append({
            "tipo_ativo": alvo.tipo_ativo,
            "codigo_b3": codigo_raiz,
            "emissao_id": None,
            "emissor_id": None,
            "fonte": FONTE,
            "url_fonte": conteudo.url_pdf,
            "data_referencia": alvo.data_referencia,
            "tese_investimento": bruto.get("tese_investimento"),
            "drivers": list(drivers),
            "riscos": list(riscos),
            "recomendacao": bruto.get("recomendacao"),
            "preco_alvo": bruto.get("preco_alvo"),
            "rating": None,
            "spread_indicativo": None,
        })

    if descartados:
        log.warning(
            "%d ticker(s) descartado(s) por não estarem no catálogo "
            "%s (%s): %s",
            len(descartados), alvo.tipo_ativo, alvo.slug,
            ", ".join(descartados),
        )

    return resultados
