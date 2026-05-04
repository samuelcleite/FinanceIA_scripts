"""
Análise de PDFs Santander via Claude (Sonnet, URL-based, multi-ativo).

Recebe um ConteudoSantander, chama claude_client.analisar_pdf_url_multi,
e devolve uma lista de dicts canônicos prontos para upsert na tabela
`analises`.

Schema retornado pelo claude_client.analisar_pdf_url_multi (já canônico):
    codigo_b3, tese_investimento, drivers, riscos,
    recomendacao, preco_alvo, rating, spread_indicativo

Responsabilidades específicas desta camada:
- Validar/normalizar codigo_b3 contra o catálogo (ativos_rv.tickers)
  para mapear ticker individual (PETR4) → raiz (PETR).
- Adicionar campos de contexto: tipo_ativo, fonte, url_fonte,
  data_referencia.
- Hardcodear recomendacao = "compra" (Carteira Recomendada não traz
  rating individual; sobrescreve o que o Claude eventualmente devolva).
- Filtrar tickers que não estão no catálogo (loga e descarta).
"""

from __future__ import annotations

import logging
from datetime import date
from functools import lru_cache

from core import catalog_loader, claude_client

from .extrair import ConteudoSantander

log = logging.getLogger(__name__)

FONTE = "santander_research"

# Pedido extra ao Claude: instruir a usar tickers individuais como vêm no PDF.
# O mapeamento para raiz é feito aqui no analisar.py via catálogo.
INSTRUCAO_EXTRA = (
    "Para cada ativo identificado, use no campo 'codigo_b3' o ticker "
    "exatamente como aparece no PDF (ex: PETR4, FXIA11, MCCI11). "
    "Não tente abreviar nem normalizar."
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
        # Para FII e ação: o próprio raiz também é um ticker válido
        mapa[raiz.upper()] = raiz
        for tk in r.get("tickers") or []:
            if tk:
                mapa[tk.upper()] = raiz
    log.info(
        "Catálogo %s carregado: %d tickers → %d ativos raiz",
        tipo_ativo,
        len(mapa),
        len(rows),
    )
    return mapa


def _normalizar_ticker(ticker: str) -> str:
    """Maiúscula, sem espaços. Não tenta corrigir typos."""
    return (ticker or "").strip().upper()


def _resolver_data_referencia(analises_brutas: list[dict]) -> str:
    """
    Hierarquia:
    1. mes_referencia extraído pelo Claude do header do PDF (todos os ativos
       do mesmo PDF compartilham, então pegamos o primeiro não-null)
    2. Primeiro dia do mês corrente (fallback)

    Retorna data ISO 'YYYY-MM-DD' (sempre dia 01).
    """
    for item in analises_brutas:
        mes = item.get("mes_referencia")
        if not mes or not isinstance(mes, str):
            continue
        try:
            ano, m = mes.strip().split("-")
            return date(int(ano), int(m), 1).isoformat()
        except (ValueError, AttributeError):
            log.warning("mes_referencia inválido: %r — usando fallback", mes)
            break
    return date.today().replace(day=1).isoformat()


def analisar(conteudo: ConteudoSantander) -> list[dict]:
    """
    Roda Claude no PDF e devolve lista de dicts canônicos.

    Cada dict pronto para upsert em `analises` tem:
        tipo_ativo, codigo_b3, fonte, url_fonte, data_referencia,
        tese_investimento, drivers (list[str]), riscos (list[str]),
        recomendacao, preco_alvo, rating, spread_indicativo
    """
    alvo = conteudo.alvo
    log.info(
        "Analisando PDF %s (tipo=%s, url=%s)",
        alvo.slug,
        alvo.tipo_ativo,
        conteudo.url_pdf,
    )

    # Chama Claude — Sonnet por padrão, URL-based, multi-ativo
    try:
        analises_brutas = claude_client.analisar_pdf_url_multi(
            pdf_url=conteudo.url_pdf,
            tipo_ativo=alvo.tipo_ativo,
            instrucao_extra=INSTRUCAO_EXTRA,
        )
    except Exception as exc:
        log.exception("Falha ao chamar Claude para %s: %s", alvo.slug, exc)
        return []

    if not analises_brutas:
        log.warning("Claude retornou lista vazia para %s", alvo.slug)
        return []

    log.info("Claude extraiu %d ativos de %s", len(analises_brutas), alvo.slug)

    tickers_map = _mapa_tickers(alvo.tipo_ativo)
    data_ref = _resolver_data_referencia(analises_brutas)
    log.info("data_referencia resolvida: %s", data_ref)

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

        # Drivers e riscos já vêm como list[str] do _normalizar_resposta
        drivers = bruto.get("drivers") or []
        riscos = bruto.get("riscos") or []

        resultado = {
            "tipo_ativo": alvo.tipo_ativo,
            "codigo_b3": codigo_raiz,
            "emissao_id": None,
            "emissor_id": None,
            "fonte": FONTE,
            "url_fonte": conteudo.url_pdf,
            "data_referencia": data_ref,
            "tese_investimento": bruto.get("tese_investimento"),
            "drivers": list(drivers),
            "riscos": list(riscos),
            "recomendacao": "compra",  # carteira recomendada → todos compra
            "preco_alvo": None,         # PDFs Santander não trazem preço-alvo
            "rating": None,
            "spread_indicativo": None,
        }
        resultados.append(resultado)

    if descartados:
        log.warning(
            "%d ticker(s) descartado(s) por não estarem no catálogo (%s): %s",
            len(descartados),
            alvo.slug,
            ", ".join(descartados),
        )

    return resultados