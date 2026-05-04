"""
Análise BTG — uma chamada Claude (Haiku) por análise BTG, replicada para
cada ativo identificável no catálogo.

Estratégia:
- A API do BTG já entrega ticker individual + tipo (ACAO/FII) + recommendation
  e targetPrice (quando aplicável). Mapeamos ticker_individual → codigo_b3 raiz
  via _mapa_tickers (cache LRU em cima de catalog_loader).
- Claude apenas extrai tese/drivers/riscos/rating/spread do texto. Os campos
  que a API já entrega (recomendacao, preco_alvo) são preferidos sobre o que
  o Claude infere.
- Para análises multi-ativo (ex.: "Setor de Petróleo & Gás" com PRIO3 + RECV3),
  o mesmo qualitativo é replicado N vezes — uma linha por ativo. A chave única
  do banco (fonte, codigo_b3, data_referencia) garante que cada ativo vire um
  registro separado.
"""
from functools import lru_cache
from typing import Optional

from core.catalog_loader import carregar_acoes, carregar_fiis
from core.claude_client import analisar_texto
from core.config import MODEL_HAIKU
from fontes.btg.descobrir import AlvoBTG
from fontes.btg.extrair import ConteudoBTG


FONTE = "btg_research"
URL_FONTE_TEMPLATE = "https://content.btgpactual.com/research/analise/{btg_id}"


# ---------------------------------------------------------------------------
# Mapeamento ticker individual → codigo_b3 raiz
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4)
def _mapa_tickers(tipo_ativo: str) -> dict[str, str]:
    """
    Constrói {ticker_upper: codigo_b3_raiz} a partir do catálogo Supabase.
    Para FIIs, raiz == ticker (HGLG11). Para ações, raiz == 'PETR' e tickers ==
    ['PETR3', 'PETR4'].
    """
    if tipo_ativo == "fii":
        rows = carregar_fiis()
    elif tipo_ativo == "acao":
        rows = carregar_acoes()
    else:
        return {}

    mapa: dict[str, str] = {}
    for r in rows:
        raiz = (r.get("codigo_b3") or "").strip()
        if not raiz:
            continue
        mapa[raiz.upper()] = raiz
        for tk in r.get("tickers") or []:
            if tk:
                mapa[tk.upper()] = raiz
    return mapa


# ---------------------------------------------------------------------------
# Helpers de normalização da resposta da API BTG
# ---------------------------------------------------------------------------

_REC_MAP = {
    "COMPRA": "compra",
    "BUY": "compra",
    "OUTPERFORM": "compra",
    "NEUTRO": "neutro",
    "HOLD": "neutro",
    "MARKETPERFORM": "neutro",
    "VENDA": "venda",
    "SELL": "venda",
    "UNDERPERFORM": "venda",
}


def _normalizar_recomendacao(valor) -> Optional[str]:
    if not valor:
        return None
    return _REC_MAP.get(str(valor).strip().upper())


def _normalizar_preco_alvo(valor) -> Optional[float]:
    if valor is None:
        return None
    try:
        f = float(str(valor).strip())
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _tipo_ativo_de_indicator(indicator: str) -> Optional[str]:
    """Mapeia sectorIndicator da API (ACAO/FII) para tipo_ativo do schema."""
    s = (indicator or "").strip().upper()
    if s == "ACAO":
        return "acao"
    if s == "FII":
        return "fii"
    return None


def _resolver_ativos(ativos_brutos: list[dict]) -> list[dict]:
    """
    Para cada entrada de analyzeAsset[], tenta mapear o ticker individual para
    a raiz do catálogo. Retorna lista de dicts:
        {raiz, tipo, ticker_individual, recomendacao, preco_alvo}

    Ativos fora do MVP (BDR, RF, CPTO) ou ausentes do catálogo são descartados.
    """
    resolvidos: list[dict] = []

    for ab in ativos_brutos or []:
        asset = ab.get("asset") or {}
        ticker = (asset.get("ticker") or "").strip().upper()
        sector = asset.get("sector") or {}
        tipo = _tipo_ativo_de_indicator(sector.get("sectorIndicator"))

        if not ticker or not tipo:
            continue

        mapa = _mapa_tickers(tipo)
        raiz = mapa.get(ticker)
        if not raiz:
            continue  # ticker não está no catálogo

        resolvidos.append({
            "raiz": raiz,
            "tipo": tipo,
            "ticker_individual": ticker,
            "recomendacao": _normalizar_recomendacao(ab.get("recommendation")),
            "preco_alvo": _normalizar_preco_alvo(ab.get("targetPrice")),
        })

    return resolvidos


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def planejar_processamento(
    alvos: list[AlvoBTG],
) -> tuple[list[AlvoBTG], dict[str, str]]:
    """
    Para cada (tipo, codigo_b3 raiz) coberto pela lista de alvos, identifica
    qual alvo BTG é o mais recente — esse é o "vencedor" para aquele ativo.

    Retorna:
    - alvos_winners: alvos que vencem em pelo menos um ativo. Alvos cujos
      ativos foram todos suplantados por outros mais recentes são descartados
      (evita chamadas Claude desperdiçadas).
    - mapa_winner: {f"{tipo}:{raiz}": btg_id_winner}, usado por analisar()
      para filtrar quais ativos processar quando um alvo cobre N ativos mas
      vence em apenas alguns.

    Critério de desempate (mesma referencia_iso): btg_id maior vence
    (ObjectID é timestamp-prefixed, então rank lex == rank temporal).
    """
    melhor: dict[tuple[str, str], tuple[str, str]] = {}

    for alvo in alvos:
        for r in _resolver_ativos(alvo.ativos_brutos):
            chave = (r["tipo"], r["raiz"])
            candidato = (alvo.referencia_iso, alvo.btg_id)
            atual = melhor.get(chave)
            if atual is None or candidato > atual:
                melhor[chave] = candidato

    winner_ids = {btg_id for _, btg_id in melhor.values()}
    alvos_winners = [a for a in alvos if a.btg_id in winner_ids]
    mapa_winner = {
        f"{tipo}:{raiz}": btg_id
        for (tipo, raiz), (_, btg_id) in melhor.items()
    }
    return alvos_winners, mapa_winner


def analisar(
    conteudo: ConteudoBTG,
    *,
    mapa_winner: Optional[dict[str, str]] = None,
) -> list[dict]:
    """
    Faz UMA chamada Claude para o body, e replica o resultado para cada ativo
    do catálogo identificado em conteudo.ativos_brutos.

    Se `mapa_winner` for fornecido (vindo de planejar_processamento), filtra
    os ativos para incluir apenas aqueles em que ESTE alvo é o mais recente —
    ativos cobertos por outro alvo BTG mais novo são descartados aqui.

    Retorna lista de dicts canônicos prontos para upsert_analise_ultima_versao.
    Pode ser vazia (sem ativos no catálogo, ou todos perderam para alvos mais
    recentes em outras análises da mesma rodada).
    """
    resolvidos = _resolver_ativos(conteudo.ativos_brutos)

    if mapa_winner is not None:
        resolvidos = [
            r for r in resolvidos
            if mapa_winner.get(f"{r['tipo']}:{r['raiz']}") == conteudo.btg_id
        ]

    if not resolvidos:
        return []

    # Tipo predominante dita a instrução do prompt. Como BTG raramente mistura
    # ACAO+FII na mesma análise (API separa por categoria), usar o primeiro.
    tipo_predominante = resolvidos[0]["tipo"]

    contexto = conteudo.titulo or ", ".join(
        sorted({a["ticker_individual"] for a in resolvidos})
    )

    qualitativo = analisar_texto(
        texto_bruto=conteudo.texto,
        tipo_ativo=tipo_predominante,
        contexto_ativo=contexto,
        modelo=MODEL_HAIKU,
    )

    url_fonte = URL_FONTE_TEMPLATE.format(btg_id=conteudo.btg_id)

    resultados: list[dict] = []
    for a in resolvidos:
        # API > Claude para campos que vêm estruturados (recomendacao, preco_alvo).
        rec = a["recomendacao"] or qualitativo.get("recomendacao")
        preco = a["preco_alvo"] if a["preco_alvo"] is not None else qualitativo.get("preco_alvo")

        resultados.append({
            "tipo_ativo": a["tipo"],
            "codigo_b3": a["raiz"],
            "fonte": FONTE,
            "url_fonte": url_fonte,
            "data_referencia": conteudo.data_referencia,
            "tese_investimento": qualitativo.get("tese_investimento"),
            "drivers": qualitativo.get("drivers") or [],
            "riscos": qualitativo.get("riscos") or [],
            "recomendacao": rec,
            "preco_alvo": preco,
            "rating": qualitativo.get("rating"),
            "spread_indicativo": qualitativo.get("spread_indicativo"),
            "ativo": True,
        })

    return resultados
