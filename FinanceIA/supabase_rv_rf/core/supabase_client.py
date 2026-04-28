"""
Cliente Supabase para a tabela `analises`.

Convenções:
- Chave única lógica: (codigo_b3 | emissao_id | emissor_id) + fonte + data_referencia
- Upsert idempotente: rodar 2x não duplica
- Antes de chamar Claude, use `existe_analise_completa()` para deduplicar
"""
from datetime import date, datetime
from typing import Any, Optional

from supabase import create_client, Client

from core.config import SUPABASE_URL, SUPABASE_KEY


_client: Optional[Client] = None


def get_client() -> Client:
    """Retorna cliente Supabase singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("SUPABASE_URL/SUPABASE_KEY não configurados no .env")
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def _normalizar_data(d: Any) -> str:
    """Converte date/datetime/string para 'YYYY-MM-DD'."""
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)[:10]


def existe_analise_completa(
    *,
    fonte: str,
    data_referencia: Any,
    codigo_b3: Optional[str] = None,
    emissao_id: Optional[str] = None,
    emissor_id: Optional[str] = None,
) -> bool:
    """
    Retorna True se já existe análise com tese_investimento preenchida
    para essa combinação (ativo, fonte, data). Usado para pular Claude
    em registros já completos.
    """
    sb = get_client()
    q = (
        sb.table("analises")
        .select("id, tese_investimento")
        .eq("fonte", fonte)
        .eq("data_referencia", _normalizar_data(data_referencia))
    )

    if codigo_b3:
        q = q.eq("codigo_b3", codigo_b3)
    if emissao_id:
        q = q.eq("emissao_id", emissao_id)
    if emissor_id:
        q = q.eq("emissor_id", emissor_id)

    res = q.limit(1).execute()
    if not res.data:
        return False
    return bool(res.data[0].get("tese_investimento"))


def upsert_analise(payload: dict) -> dict:
    """
    Insere ou atualiza uma análise. Espera o dict canônico:

    {
      "tipo_ativo": "acao" | "fii" | "debenture" | "cri_cra" | "tesouro",
      "codigo_b3": "PETR4" | None,
      "emissao_id": "..." | None,
      "emissor_id": "..." | None,
      "fonte": "xp_research" | ...,
      "url_fonte": "https://...",
      "data_referencia": "2026-04-15",
      "tese_investimento": "...",
      "drivers": "...",
      "riscos": "...",
      "recomendacao": "compra" | "neutro" | "venda" | None,
      "preco_alvo": 42.50 | None,
      "rating": "AAA" | None,
      "spread_indicativo": 1.85 | None,
      "ativo": True
    }

    Idempotência via índice único composto:
      uq_analises_ativo_fonte_data
        (COALESCE(codigo_b3,''), COALESCE(emissao_id,''),
         COALESCE(emissor_id,''), fonte, data_referencia)
    """
    sb = get_client()

    payload = {**payload}
    payload["data_referencia"] = _normalizar_data(payload["data_referencia"])
    payload.setdefault("ativo", True)
    payload["updated_at"] = datetime.utcnow().isoformat()

    # Remove chaves que não existem na tabela (proteção contra typos)
    campos_validos = {
        "id", "tipo_ativo", "codigo_b3", "emissao_id", "emissor_id",
        "fonte", "url_fonte", "data_referencia",
        "tese_investimento", "drivers", "riscos",
        "recomendacao", "preco_alvo", "rating", "spread_indicativo",
        "ativo", "created_at", "updated_at",
    }
    payload = {k: v for k, v in payload.items() if k in campos_validos}

    res = sb.table("analises").upsert(
        payload,
        on_conflict="codigo_b3,emissao_id,emissor_id,fonte,data_referencia",
    ).execute()

    return res.data[0] if res.data else {}


def marcar_inativa(analise_id: str) -> None:
    """Soft-delete: marca uma análise como inativa."""
    sb = get_client()
    sb.table("analises").update({
        "ativo": False,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", analise_id).execute()
