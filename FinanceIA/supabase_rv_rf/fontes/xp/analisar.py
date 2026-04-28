"""
Análise XP — fina camada sobre claude_client.analisar_texto().

Mantida como módulo separado porque outras fontes (BTG, Santander)
chamarão `analisar_pdf_url` em vez de `analisar_texto`, e cada módulo de
fonte concentra a sua decisão (modelo, instruções extras, parsing pós-Claude).
"""
from core.claude_client import analisar_texto
from core.config import MODEL_HAIKU
from fontes.xp.extrair import ConteudoXP


def analisar(conteudo: ConteudoXP) -> dict:
    """
    Recebe o ConteudoXP extraído e devolve o dict canônico já pronto
    para upsert em `analises` (sem os campos de roteamento que o run.py
    adiciona — esses ficam no entrypoint).
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
        "tese_investimento": qualitativo.get("tese_investimento") or None,
        "drivers": qualitativo.get("drivers") or None,
        "riscos": qualitativo.get("riscos") or None,
        "recomendacao": qualitativo.get("recomendacao"),
        "preco_alvo": qualitativo.get("preco_alvo"),
        "rating": qualitativo.get("rating"),
        "spread_indicativo": qualitativo.get("spread_indicativo"),
        "ativo": True,
    }
