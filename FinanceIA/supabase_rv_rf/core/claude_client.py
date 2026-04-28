"""
Wrapper da Anthropic API para gerar análises qualitativas.

Dois modos:
- analisar_texto(): para HTML/texto já extraído (ex: XP Research) — Haiku por padrão
- analisar_pdf_url(): para PDFs públicos via URL (BTG/Santander/Itaú) — Sonnet por padrão

Ambos retornam um dict com:
  tese_investimento, drivers, riscos,
  recomendacao, preco_alvo, rating, spread_indicativo
"""
import json
import time
from typing import Any, Optional

from anthropic import Anthropic, APIError, APIStatusError

from core.config import ANTHROPIC_API_KEY, MODEL_HAIKU, MODEL_SONNET


_client: Optional[Anthropic] = None


def get_client() -> Anthropic:
    global _client
    if _client is None:
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY não configurado no .env")
        _client = Anthropic(api_key=ANTHROPIC_API_KEY)
    return _client


# ---------------------------------------------------------------------------
# Prompt base — persona CFP/3A Riva, parametrizado por tipo_ativo
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Você é um analista CNPI sênior, atuando como consultor de
investimentos no padrão CFP. Seu trabalho é ler relatórios de research de
corretoras brasileiras e extrair, em linguagem objetiva e sem jargão
desnecessário, três blocos de informação sobre o ativo analisado:

1. **tese_investimento**: a tese central — por que comprar/manter/evitar este
   ativo, em 3 a 6 frases. Foco em fundamentos e posicionamento.

2. **drivers**: catalisadores e fatores de valorização concretos
   (3 a 6 pontos, em prosa corrida com travessões).

3. **riscos**: riscos materiais e fatores de atenção
   (3 a 6 pontos, em prosa corrida com travessões).

Regras absolutas:
- Use APENAS o que estiver no relatório fornecido. Não invente dados, números
  ou projeções. Se algo não estiver no texto, omita.
- Não cite a corretora pelo nome dentro dos campos qualitativos.
- Não use markdown, bullets com asterisco, headers ou emojis.
- Português do Brasil, tom profissional conversacional.
- Se o relatório não permitir extrair algum campo qualitativo com segurança,
  retorne string vazia para esse campo.

Além dos qualitativos, extraia metadados quando explicitamente presentes:
- recomendacao: "compra", "neutro" ou "venda" (mapeie sinônimos: outperform→compra,
  marketperform→neutro, underperform→venda, etc.). Null se ausente.
- preco_alvo: número em R$. Null se ausente.
- rating: rating de crédito do emissor (AAA, AA+, etc.). Null se ausente.
- spread_indicativo: spread em pontos percentuais (ex: 1.85 para CDI+1,85%).
  Null se ausente.

Saída: APENAS um objeto JSON válido, sem texto antes ou depois, sem cercas
de código markdown.
"""


def _instrucao_por_tipo(tipo_ativo: str) -> str:
    mapa = {
        "acao": "Este é um relatório sobre uma AÇÃO listada na B3. Foque em "
                "tese fundamentalista, vantagens competitivas, drivers de "
                "receita/margem, e riscos setoriais e idiossincráticos.",
        "fii": "Este é um relatório sobre um FUNDO IMOBILIÁRIO (FII). Foque "
               "em qualidade dos ativos imobiliários, vacância, dividend yield, "
               "gestão, e riscos de inadimplência/vacância/setor.",
        "debenture": "Este é um relatório sobre uma DEBÊNTURE corporativa. "
                     "Foque em qualidade do crédito do emissor, estrutura da "
                     "dívida, garantias, covenants, e riscos de crédito/setor.",
        "cri_cra": "Este é um relatório sobre um CRI ou CRA. Foque em "
                   "qualidade dos lastros, estrutura da securitização, "
                   "garantias, e riscos de inadimplência da carteira.",
        "tesouro": "Este é um título público do Tesouro Direto. Foque em "
                   "indexador, prazo, sensibilidade a juros e perfil de uso.",
    }
    return mapa.get(tipo_ativo, "Ativo financeiro brasileiro.")


def _schema_saida() -> str:
    return """Formato de saída (JSON estrito):
{
  "tese_investimento": "string",
  "drivers": "string",
  "riscos": "string",
  "recomendacao": "compra" | "neutro" | "venda" | null,
  "preco_alvo": number | null,
  "rating": "string" | null,
  "spread_indicativo": number | null
}"""


# ---------------------------------------------------------------------------
# Chamadas
# ---------------------------------------------------------------------------

def _parse_json_resposta(texto: str) -> dict:
    """Extrai JSON da resposta do Claude, tolerando cercas de código."""
    t = texto.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.startswith("json"):
            t = t[4:]
        t = t.strip()
    return json.loads(t)


def _chamar_com_retry(
    *,
    model: str,
    system: str,
    content: list,
    max_tokens: int = 2000,
    tentativas: int = 4,
) -> dict:
    """Chama a API com retry exponencial em erros transitórios."""
    client = get_client()
    ultimo_erro: Optional[Exception] = None

    for i in range(tentativas):
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": content}],
            )
            texto = "".join(
                bloco.text for bloco in resp.content if bloco.type == "text"
            )
            return _parse_json_resposta(texto)

        except (APIStatusError, APIError) as e:
            ultimo_erro = e
            status = getattr(e, "status_code", None)
            # Retry em 429, 500, 502, 503, 529
            if status in (429, 500, 502, 503, 529) or status is None:
                espera = 2 ** i
                time.sleep(espera)
                continue
            raise
        except json.JSONDecodeError as e:
            ultimo_erro = e
            time.sleep(1)
            continue

    raise RuntimeError(f"Claude API falhou após {tentativas} tentativas: {ultimo_erro}")


def analisar_texto(
    texto_bruto: str,
    tipo_ativo: str,
    *,
    contexto_ativo: str = "",
    modelo: str = MODEL_HAIKU,
) -> dict:
    """
    Analisa um texto já extraído (HTML limpo, etc.) e retorna os campos
    qualitativos + metadados.

    Args:
        texto_bruto: conteúdo do relatório em texto puro
        tipo_ativo: "acao" | "fii" | "debenture" | "cri_cra" | "tesouro"
        contexto_ativo: identificação do ativo (ex: "PETR4 — Petrobras PN")
        modelo: claude-haiku-4-5 (default) ou claude-sonnet-4-6
    """
    instrucao = _instrucao_por_tipo(tipo_ativo)
    schema = _schema_saida()

    user_msg = (
        f"{instrucao}\n\n"
        f"Ativo: {contexto_ativo or '(não informado)'}\n\n"
        f"--- INÍCIO DO RELATÓRIO ---\n{texto_bruto}\n--- FIM DO RELATÓRIO ---\n\n"
        f"{schema}"
    )

    return _chamar_com_retry(
        model=modelo,
        system=SYSTEM_PROMPT,
        content=[{"type": "text", "text": user_msg}],
    )


def analisar_pdf_url(
    pdf_url: str,
    tipo_ativo: str,
    *,
    contexto_ativo: str = "",
    instrucao_extra: str = "",
    modelo: str = MODEL_SONNET,
) -> dict:
    """
    Analisa um PDF público referenciado por URL (sem download local).

    Use para BTG/Santander/Itaú BBA. Para PDFs multi-ativo (Santander),
    passe `instrucao_extra` pedindo um array em vez de objeto único e
    parseie o resultado fora desta função.
    """
    instrucao = _instrucao_por_tipo(tipo_ativo)
    schema = _schema_saida()

    texto_user = (
        f"{instrucao}\n\n"
        f"Ativo: {contexto_ativo or '(extrair do PDF)'}\n\n"
        f"{instrucao_extra}\n\n"
        f"{schema}"
    ).strip()

    return _chamar_com_retry(
        model=modelo,
        system=SYSTEM_PROMPT,
        content=[
            {
                "type": "document",
                "source": {"type": "url", "url": pdf_url},
            },
            {"type": "text", "text": texto_user},
        ],
        max_tokens=3000,
    )


def analisar_pdf_url_multi(
    pdf_url: str,
    tipo_ativo: str,
    *,
    instrucao_extra: str = "",
    modelo: str = MODEL_SONNET,
) -> list[dict]:
    """
    Variante para PDFs multi-ativo (ex: relatório Santander de FIIs).
    Retorna uma LISTA de análises, uma por ativo identificado no PDF.
    Cada item inclui um campo extra `codigo_b3` extraído do próprio PDF.
    """
    instrucao = _instrucao_por_tipo(tipo_ativo)

    schema_multi = """Formato de saída (JSON estrito, ARRAY):
[
  {
    "codigo_b3": "string (ticker do ativo)",
    "tese_investimento": "string",
    "drivers": "string",
    "riscos": "string",
    "recomendacao": "compra" | "neutro" | "venda" | null,
    "preco_alvo": number | null,
    "rating": "string" | null,
    "spread_indicativo": number | null
  }
]"""

    texto_user = (
        f"{instrucao}\n\n"
        f"Este PDF contém análises de MÚLTIPLOS ativos. "
        f"Extraia uma análise para CADA ativo identificável.\n\n"
        f"{instrucao_extra}\n\n"
        f"{schema_multi}"
    )

    client = get_client()
    resp = client.messages.create(
        model=modelo,
        max_tokens=8000,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": [
                {"type": "document", "source": {"type": "url", "url": pdf_url}},
                {"type": "text", "text": texto_user},
            ],
        }],
    )
    texto = "".join(b.text for b in resp.content if b.type == "text")
    parsed = _parse_json_resposta(texto)
    if not isinstance(parsed, list):
        raise ValueError("Esperava array JSON, recebeu objeto")
    return parsed
