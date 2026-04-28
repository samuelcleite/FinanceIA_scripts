"""
Checkpoint/resume padrão para pipelines de coleta.

Formato do arquivo `checkpoints/{fonte}.json`:
{
  "PETR4_xp_research_2026-04-15": {
    "_status": "ok" | "erro" | "pulado_ja_existe",
    "ts": "2026-04-27T14:30:00",
    "erro": "...",        # apenas se _status == "erro"
    "url": "https://...", # opcional, para auditoria
  },
  ...
}
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from core.config import CHECKPOINT_DIR


def _path(fonte: str) -> Path:
    return CHECKPOINT_DIR / f"{fonte}.json"


def carregar(fonte: str) -> dict:
    """Lê o checkpoint da fonte; retorna dict vazio se não existir."""
    p = _path(fonte)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def salvar(fonte: str, dados: dict) -> None:
    """Grava o checkpoint completo (sobrescreve)."""
    p = _path(fonte)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def registrar(
    fonte: str,
    chave: str,
    status: str,
    *,
    erro: Optional[str] = None,
    url: Optional[str] = None,
    extra: Optional[dict] = None,
) -> None:
    """
    Atualiza UMA entrada do checkpoint e persiste imediatamente.
    Status esperados: 'ok' | 'erro' | 'pulado_ja_existe' | 'sem_dados'.
    """
    cp = carregar(fonte)
    entrada = {
        "_status": status,
        "ts": datetime.utcnow().isoformat(),
    }
    if erro:
        entrada["erro"] = erro[:1000]
    if url:
        entrada["url"] = url
    if extra:
        entrada.update(extra)
    cp[chave] = entrada
    salvar(fonte, cp)


def filtrar_pendentes(
    fonte: str,
    chaves: Iterable[str],
    *,
    incluir_erros: bool = False,
) -> list[str]:
    """
    Retorna apenas as chaves que ainda precisam ser processadas.
    Por padrão, pula 'ok', 'pulado_ja_existe' e 'sem_dados'.
    Com `incluir_erros=True`, reincluí 'erro' (para retry).
    """
    cp = carregar(fonte)
    pendentes = []
    for c in chaves:
        entrada = cp.get(c)
        if entrada is None:
            pendentes.append(c)
            continue
        status = entrada.get("_status")
        if status == "erro" and incluir_erros:
            pendentes.append(c)
    return pendentes


def montar_chave(*partes: str) -> str:
    """Monta chave canônica de checkpoint: junta partes não-vazias com '_'."""
    return "_".join(p for p in partes if p)
