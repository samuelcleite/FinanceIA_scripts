"""
Helper HTTP com retry e cache em disco.

Cache: salva o conteúdo bruto em `cache/{fonte}/{hash_url}.html` (ou .pdf)
para evitar re-download em desenvolvimento e respeitar os servidores das
fontes durante runs repetidos.
"""
import hashlib
import time
from pathlib import Path
from typing import Optional

import requests

from core.config import CACHE_DIR, HTTP_TIMEOUT, USER_AGENT


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
    Baixa HTML com cache em disco. Retorna None se 404 ou similar.
    """
    cache = _cache_path(fonte, url, "html")
    if use_cache and cache.exists():
        return cache.read_text(encoding="utf-8")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    ultimo_erro: Optional[Exception] = None

    for i in range(tentativas):
        try:
            r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 403:
                # XP às vezes bloqueia uma requisição mas libera a próxima
                ultimo_erro = requests.HTTPError(f"403 em {url}")
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            html = r.text
            cache.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException as e:
            ultimo_erro = e
            time.sleep(2 ** i)

    raise RuntimeError(f"Falha ao baixar {url}: {ultimo_erro}")

def head_ok(url: str) -> bool:
    """Verifica se URL existe (HEAD request) sem baixar conteúdo."""
    try:
        r = requests.head(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
        )
        return r.status_code == 200
    except requests.RequestException:
        return False
