"""
Cliente Supabase para a tabela `analises`.

Convenções:
- Chave lógica: (codigo_b3 | emissao_id | emissor_id) + fonte + data_referencia
- Upsert manual (select-then-update-or-insert) porque o índice único usa
  COALESCE() e o PostgREST não consegue mapear via on_conflict.
- drivers e riscos são JSONB (arrays Python passam direto pelo postgrest).
"""
from datetime import date, datetime, timezone
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


def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    para essa combinação (ativo, fonte, data).
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
    Insere ou atualiza uma análise.

    Estratégia: select pela chave lógica → update se existe, insert se não.
    Aceita drivers e riscos como list[str] (vão direto pra coluna JSONB).
    """
    sb = get_client()

    payload = {**payload}
    payload["data_referencia"] = _normalizar_data(payload["data_referencia"])
    payload.setdefault("ativo", True)
    payload["updated_at"] = _agora_iso()

    # Garante que drivers/riscos são listas (mesmo que vazias)
    if "drivers" in payload and payload["drivers"] is None:
        payload["drivers"] = []
    if "riscos" in payload and payload["riscos"] is None:
        payload["riscos"] = []

    # Filtra campos válidos
    campos_validos = {
        "id", "tipo_ativo", "codigo_b3", "emissao_id", "emissor_id",
        "fonte", "url_fonte", "data_referencia",
        "tese_investimento", "drivers", "riscos",
        "recomendacao", "preco_alvo", "rating", "spread_indicativo",
        "ativo", "created_at", "updated_at",
    }
    payload = {k: v for k, v in payload.items() if k in campos_validos}

    # Busca registro existente pela chave lógica
    q = (
        sb.table("analises")
        .select("id")
        .eq("fonte", payload["fonte"])
        .eq("data_referencia", payload["data_referencia"])
    )
    if payload.get("codigo_b3"):
        q = q.eq("codigo_b3", payload["codigo_b3"])
    else:
        q = q.is_("codigo_b3", "null")
    if payload.get("emissao_id"):
        q = q.eq("emissao_id", payload["emissao_id"])
    else:
        q = q.is_("emissao_id", "null")
    if payload.get("emissor_id"):
        q = q.eq("emissor_id", payload["emissor_id"])
    else:
        q = q.is_("emissor_id", "null")

    existente = q.limit(1).execute()

    if existente.data:
        analise_id = existente.data[0]["id"]
        payload_update = {k: v for k, v in payload.items()
                          if k not in ("id", "created_at")}
        res = (
            sb.table("analises")
            .update(payload_update)
            .eq("id", analise_id)
            .execute()
        )
    else:
        res = sb.table("analises").insert(payload).execute()

    return res.data[0] if res.data else {}


def marcar_inativa(analise_id: str) -> None:
    """Soft-delete: marca uma análise como inativa."""
    sb = get_client()
    sb.table("analises").update({
        "ativo": False,
        "updated_at": _agora_iso(),
    }).eq("id", analise_id).execute()