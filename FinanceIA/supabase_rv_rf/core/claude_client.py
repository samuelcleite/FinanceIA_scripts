"""
Wrapper da Anthropic API para gerar análises qualitativas.

Saída padronizada (dict):
  tese_investimento: str
  drivers: list[str]      # forças de ALTA
  riscos:  list[str]      # forças de QUEDA
  recomendacao, preco_alvo, rating, spread_indicativo
"""
import json
import re
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
# Prompt base — analista CNPI / consultor CFP
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Você é um analista CNPI sênior atuando como consultor financeiro
no padrão CFP, dentro do FinanceIA — um aplicativo que orienta investidores
brasileiros sobre seu portfólio. Sua tarefa é ler relatórios de research de
corretoras brasileiras e destilar três blocos sobre o ativo analisado:

1. tese_investimento (string)
   Um parágrafo de 80 a 150 palavras explicando, em tom OPINATIVO MAS
   EQUILIBRADO, como funciona a operação da empresa epor que comprar/manter/evitar este ativo. Use construções
   como "a tese parece sólida porque...", "o caso de investimento se
   sustenta em...", "vemos espaço para...". Foco em fundamentos, posicionamento
   competitivo e contexto setorial. Não descreva o que o relatório diz —
   sintetize a tese como se você mesmo a estivesse defendendo para um cliente.

2. drivers (array de 4 a 6 strings)
   FATORES QUE FAZEM A AÇÃO/ATIVO PERFORMAR BEM (forças de ALTA).
   Podem ser:
   - macroeconômicos (queda de juros, alta de commodities, ciclo favorável)
   - setoriais (consolidação, demanda estrutural)
   - específicos da empresa (lançamento de produto, ganho de participação,
     redução de alavancagem, evento societário)
   Cada item: frase concreta de 5 a 20 palavras, sem prefixos genéricos.

3. riscos (array de 4 a 6 strings)
   FATORES QUE FAZEM A AÇÃO/ATIVO CAIR (forças de QUEDA).
   Mesma lógica de origem (macro/setorial/idiossincrático), mas no sentido
   inverso. Cada item: frase concreta de 5 a 20 palavras.

Regras absolutas:
- Use APENAS o que estiver no relatório fornecido. Não invente números,
  projeções ou eventos que não estão no texto. Se algo não estiver lá, omita.
- Se for impossível extrair drivers/riscos com segurança, retorne array vazio.
- Não cite a corretora pelo nome dentro dos campos qualitativos.
- Não use markdown, asteriscos, headers ou emojis em nenhum campo.
- Português do Brasil, tom profissional conversacional.
- Drivers e riscos são MUTUAMENTE EXCLUSIVOS: não repita o mesmo fator
  como driver e risco. Se ambíguo, escolha onde o relatório dá mais peso.

Metadados (extraia somente quando explicitamente presentes):
- recomendacao: "compra" | "neutro" | "venda" | null
  Mapeie sinônimos: outperform/buy/comprar→compra,
  marketperform/hold/manter→neutro, underperform/sell/vender→venda.
- preco_alvo: número em R$. Null se ausente.
- rating: rating de crédito do emissor (AAA, AA+, etc.). Null se ausente.
- spread_indicativo: spread em pontos percentuais (ex: 1.85 para CDI+1,85%).
  Null se ausente.

Saída: APENAS UM ÚNICO objeto JSON válido, sem texto antes ou depois,
sem cercas de código markdown, sem comentários, sem objetos adicionais.
"""


def _instrucao_por_tipo(tipo_ativo: str) -> str:
    mapa = {
        "acao": "Este é um relatório sobre uma AÇÃO listada na B3. "
                "Foque em tese fundamentalista, vantagens competitivas, "
                "drivers de receita/margem/lucro, e riscos setoriais e "
                "idiossincráticos.",
        "fii": "Este é um relatório sobre um FUNDO IMOBILIÁRIO (FII). "
               "Foque em qualidade dos ativos imobiliários, vacância, "
               "dividend yield, qualidade da gestão, e riscos de "
               "inadimplência/vacância/setor imobiliário.",
        "debenture": "Este é um relatório sobre uma DEBÊNTURE corporativa. "
                     "Foque em qualidade do crédito do emissor, estrutura "
                     "da dívida, garantias, covenants, e riscos de "
                     "crédito/setor.",
        "cri_cra": "Este é um relatório sobre um CRI ou CRA. Foque em "
                   "qualidade dos lastros, estrutura da securitização, "
                   "garantias, e riscos de inadimplência da carteira.",
        "tesouro": "Este é um título público do Tesouro Direto. Foque em "
                   "indexador, prazo, sensibilidade a juros e perfil de uso.",
    }
    return mapa.get(tipo_ativo, "Ativo financeiro brasileiro.")


def _schema_saida() -> str:
    return """Formato de saída (UM ÚNICO objeto JSON estrito):
{
  "tese_investimento": "string (80-150 palavras, tom opinativo equilibrado)",
  "drivers": ["string", "string", ...],
  "riscos": ["string", "string", ...],
  "recomendacao": "compra" | "neutro" | "venda" | null,
  "preco_alvo": number | null,
  "rating": "string" | null,
  "spread_indicativo": number | null
}"""


# ---------------------------------------------------------------------------
# Validação e normalização da resposta
# ---------------------------------------------------------------------------

def _normalizar_lista(valor: Any) -> list[str]:
    """Aceita lista, string única ou null e retorna sempre lista de strings."""
    if valor is None:
        return []
    if isinstance(valor, list):
        return [str(x).strip() for x in valor if str(x).strip()]
    if isinstance(valor, str):
        s = valor.strip()
        return [s] if s else []
    return []


def _normalizar_resposta(d: dict) -> dict:
    """Garante o schema esperado mesmo com pequenas variações do modelo."""
    return {
        "tese_investimento": (d.get("tese_investimento") or "").strip() or None,
        "drivers": _normalizar_lista(d.get("drivers")),
        "riscos": _normalizar_lista(d.get("riscos")),
        "recomendacao": d.get("recomendacao"),
        "preco_alvo": d.get("preco_alvo"),
        "rating": d.get("rating"),
        "spread_indicativo": d.get("spread_indicativo"),
    }


# ---------------------------------------------------------------------------
# Parser robusto: extrai o PRIMEIRO objeto JSON válido da resposta
# ---------------------------------------------------------------------------

def _achar_primeiro_json(texto: str) -> Optional[str]:
    """Encontra o primeiro objeto JSON balanceado no texto (conta chaves)."""
    inicio = texto.find("{")
    if inicio == -1:
        return None
    profundidade = 0
    em_string = False
    escape = False
    for i in range(inicio, len(texto)):
        c = texto[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            em_string = not em_string
            continue
        if em_string:
            continue
        if c == "{":
            profundidade += 1
        elif c == "}":
            profundidade -= 1
            if profundidade == 0:
                return texto[inicio:i + 1]
    return None


def _parse_json_resposta(texto: str) -> dict:
    """
    Extrai JSON da resposta do Claude.

    Estratégias em ordem:
    1. Strip de cercas de código markdown
    2. Tenta json.loads direto
    3. Se falhar com 'Extra data', usa raw_decode (pega só o primeiro objeto)
    4. Se ainda falhar, usa busca de chaves balanceadas
    """
    t = texto.strip()

    if t.startswith("```"):
        t = t.strip("`")
        if t.startswith("json"):
            t = t[4:]
        t = t.strip()

    # Tentativa 1: parse direto
    try:
        return json.loads(t)
    except json.JSONDecodeError as e:
        # Tentativa 2: raw_decode pega o primeiro objeto, ignora resto
        if "Extra data" in str(e):
            try:
                obj, _ = json.JSONDecoder().raw_decode(t)
                return obj
            except json.JSONDecodeError:
                pass

    # Tentativa 3: busca por chaves balanceadas
    bloco = _achar_primeiro_json(t)
    if bloco:
        try:
            return json.loads(bloco)
        except json.JSONDecodeError:
            pass

    preview = t[:200].replace("\n", " ")
    raise json.JSONDecodeError(
        f"Não foi possível extrair JSON. Início da resposta: {preview!r}",
        t, 0,
    )


# ---------------------------------------------------------------------------
# Chamadas
# ---------------------------------------------------------------------------

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
            return _normalizar_resposta(_parse_json_resposta(texto))

        except (APIStatusError, APIError) as e:
            ultimo_erro = e
            status = getattr(e, "status_code", None)
            if status in (429, 500, 502, 503, 529) or status is None:
                time.sleep(2 ** i)
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
    """Analisa texto já extraído. Retorna dict com qualitativos + metadados."""
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
    """Analisa PDF público via URL (BTG/Santander/Itaú BBA)."""
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
            {"type": "document", "source": {"type": "url", "url": pdf_url}},
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
    """Variante para PDFs multi-ativo (ex: relatório Santander de FIIs)."""
    instrucao = _instrucao_por_tipo(tipo_ativo)

    schema_multi = """Formato de saída (JSON estrito, OBJETO com dois campos):
{
  "mes_referencia": "YYYY-MM" | null,
  "analises": [
    {
      "codigo_b3": "string (ticker raiz do ativo)",
      "tese_investimento": "string (80-150 palavras)",
      "drivers": ["string", ...],
      "riscos": ["string", ...],
      "recomendacao": "compra" | "neutro" | "venda" | null,
      "preco_alvo": number | null,
      "rating": "string" | null,
      "spread_indicativo": number | null
    }
  ]
}

Campo mes_referencia: extraia o MÊS DA CARTEIRA do header do relatório (não a
data de publicação). Exemplos:
- "ABRIL 2026" ou "Carteira de Abril/2026" → "2026-04"
- "MARÇO 2026" → "2026-03"
- Se não houver indicação clara de mês, retorne null.
O mes_referencia é único para o PDF inteiro (todos os ativos compartilham)."""

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
        max_tokens=16000,
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

    # Limpa code fences ```json ... ```
    t = texto.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.startswith("json"):
            t = t[4:]
        t = t.strip()

    parsed = _parse_resposta_multi_tolerante(t)

    if not isinstance(parsed, (list, dict)):
        raise ValueError("Esperava array ou objeto JSON")

    # Suporta dois formatos:
    # - Novo: {"mes_referencia": "YYYY-MM", "analises": [...]}
    # - Legado: [...]  (array direto)
    if isinstance(parsed, dict):
        mes_ref = parsed.get("mes_referencia")
        itens = parsed.get("analises") or []
    else:
        mes_ref = None
        itens = parsed

    if not isinstance(itens, list):
        raise ValueError("Campo 'analises' não é array")

    return [
        _normalizar_resposta(item) | {
            "codigo_b3": item.get("codigo_b3"),
            "mes_referencia": mes_ref,
        }
        for item in itens
    ]


def _parse_resposta_multi_tolerante(t: str) -> dict | list:
    """
    Parseia a resposta JSON de analisar_pdf_url_multi com 3 níveis de tolerância:

    1. json.loads estrito (caminho feliz).
    2. JSONDecoder.raw_decode (tolera lixo após o JSON).
    3. Recuperação parcial: se o Claude truncou no meio (max_tokens estourado),
       extrai os objetos {...} já completos do array de análises e descarta o
       último incompleto. Funciona tanto pro wrapper {mes_referencia, analises}
       quanto pro formato legado (array direto).

    Sempre retorna dict ou list (vazia se nada parseável).
    """
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    try:
        v, _ = json.JSONDecoder().raw_decode(t)
        return v
    except json.JSONDecodeError:
        pass

    # Recuperação parcial — caminhar pelos objetos completos
    decoder = json.JSONDecoder()
    stripped = t.lstrip()

    if stripped.startswith("{"):
        # Formato novo: extrai mes_referencia (no começo) + analises parseáveis
        m = re.search(r'"mes_referencia"\s*:\s*"(\d{4}-\d{2})"', t)
        mes_ref = m.group(1) if m else None
        idx = t.find('"analises"')
        if idx < 0:
            return {"mes_referencia": mes_ref, "analises": []}
        idx = t.find("[", idx)
        if idx < 0:
            return {"mes_referencia": mes_ref, "analises": []}
        return {
            "mes_referencia": mes_ref,
            "analises": _extrair_objs_iterativo(t, idx + 1, decoder),
        }

    if stripped.startswith("["):
        idx = t.find("[")
        return _extrair_objs_iterativo(t, idx + 1, decoder)

    return []


def _extrair_objs_iterativo(
    t: str, start: int, decoder: json.JSONDecoder,
) -> list[dict]:
    """Caminha por um array a partir de `start`, extraindo objetos {...}
    completos um por um. Para no primeiro objeto malformado/truncado."""
    items: list[dict] = []
    i = start
    while i < len(t):
        # Pula whitespace e vírgulas entre objetos
        while i < len(t) and t[i] in " \t\n\r,":
            i += 1
        if i >= len(t) or t[i] == "]":
            break
        try:
            obj, end = decoder.raw_decode(t, i)
        except json.JSONDecodeError:
            break
        if isinstance(obj, dict):
            items.append(obj)
        i = end
    return items