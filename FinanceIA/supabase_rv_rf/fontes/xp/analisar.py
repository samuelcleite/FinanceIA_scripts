"""
Análise XP — fina camada sobre claude_client.analisar_texto().

drivers e riscos agora vêm como list[str] do claude_client e são
gravados como arrays no Postgres (coluna JSONB).
"""
from core.claude_client import analisar_texto
from core.config import MODEL_HAIKU
from fontes.xp.extrair import ConteudoXP


def analisar(conteudo: ConteudoXP) -> dict:
    """
    Recebe o ConteudoXP extraído e devolve o dict canônico já pronto
    para upsert em `analises`.
    """
    qualitativo = analisar_texto(
        texto_bruto=conteudo.texto,
        tipo_ativo=conteudo.tipo_ativo,
        contexto_ativo=conteudo.contexto_ativo,
        modelo=MODEL_HAIKU,
    )

    return {
        "tipo_ativo": conteudo.tipo_ativo,
        "codigo_b3": conteudo.codigo_b3,
        "fonte": "xp_research",
        "url_fonte": conteudo.url_fonte,
        "data_referencia": conteudo.data_referencia,
        "tese_investimento": qualitativo.get("tese_investimento"),
        # Arrays vêm já normalizados como list[str] do claude_client
        "drivers": qualitativo.get("drivers") or [],
        "riscos": qualitativo.get("riscos") or [],
        "recomendacao": qualitativo.get("recomendacao"),
        "preco_alvo": qualitativo.get("preco_alvo"),
        "rating": qualitativo.get("rating"),
        "spread_indicativo": qualitativo.get("spread_indicativo"),
        "ativo": True,
    }