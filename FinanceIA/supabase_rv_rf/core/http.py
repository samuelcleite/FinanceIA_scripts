"""
Helper HTTP com retry e cache em disco.

Usa `curl_cffi` em vez de `requests` para imitar o TLS fingerprint do Chrome
e burlar bot managers (Akamai, Cloudflare, PerimeterX) que bloqueiam
fingerprints de Python/requests.

Cache: salva o conteúdo bruto em `cache/{fonte}/{hash_url}.html` (ou .pdf)
para evitar re-download em desenvolvimento e respeitar os servidores das
fontes durante runs repetidos.
"""
import hashlib
import time
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests

from core.config import CACHE_DIR, HTTP_TIMEOUT


# Versão do Chrome a impersonar. "chrome131" é a mais recente disponível
# no curl_cffi 0.7+. Se quebrar com versão futura do site, atualizar aqui.
IMPERSONATE = "chrome131"


def _cache_path(fonte: str, url: str, ext: str) -> Path:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    pasta = CACHE_DIR / fonte
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta / f"{h}.{ext}"


def fetch_html(
    url: str,
    *,
    fonte: str,
    use_cache: bool = True,
    tentativas: int = 3,
) -> Optional[str]:
    """
    Baixa HTML com cache em disco. Retorna None se 404.
    Usa TLS fingerprint do Chrome para evitar bot detection.
    """
    cache = _cache_path(fonte, url, "html")
    if use_cache and cache.exists():
        return cache.read_text(encoding="utf-8")

    headers = {
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }
    ultimo_erro: Optional[Exception] = None

    for i in range(tentativas):
        try:
            r = cffi_requests.get(
                url,
                headers=headers,
                timeout=HTTP_TIMEOUT,
                impersonate=IMPERSONATE,
            )
            if r.status_code == 404:
                return None
            if r.status_code == 403:
                ultimo_erro = RuntimeError(f"403 em {url}")
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            html = r.text
            cache.write_text(html, encoding="utf-8")
            return html
        except Exception as e:
            ultimo_erro = e
            time.sleep(2 ** i)

    raise RuntimeError(f"Falha ao baixar {url}: {ultimo_erro}")


def head_ok(url: str) -> bool:
    """Verifica se URL existe (HEAD request) sem baixar conteúdo."""
    try:
        r = cffi_requests.head(
            url,
            timeout=HTTP_TIMEOUT,
            impersonate=IMPERSONATE,
            allow_redirects=True,
        )
        return r.status_code == 200
    except Exception:
        return False