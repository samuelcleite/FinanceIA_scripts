"""
Entrypoint do pipeline Santander.

Orquestra: descobrir → extrair → analisar → upsert.

Particularidade da fonte Santander: 1 PDF gera N registros em `analises`.
O checkpoint opera por PDF (chave = slug); a unicidade dos registros no
Supabase é garantida pela chave lógica (fonte, codigo_b3, data_referencia)
do upsert_analise — então se o run cair entre Claude e DB, o re-run não
duplica.

Status no checkpoint (compatível com core.checkpoint):
    "ok"               — todos os ativos do PDF foram gravados
    "erro"             — falhou (extrair, analisar ou upsert)
    "sem_dados"        — Claude retornou lista vazia
    "pulado_ja_existe" — não usado aqui (a checagem é por chave de PDF)

CLI:
    python run_santander.py                  # roda tudo pendente
    python run_santander.py --so-listar      # só lista alvos descobertos
    python run_santander.py --limite 1       # processa só 1 PDF (debug)
    python run_santander.py --retry-erros    # reprocessa PDFs que falharam
    python run_santander.py --sem-crawl      # só lista fixa, sem heurística
"""

from __future__ import annotations

import argparse
import logging
import sys

from core import checkpoint, supabase_client
from core.config import LOG_DIR, validar_config

from fontes.santander import (
    FONTE,
    AlvoSantander,
    analisar,
    descobrir,
    extrair,
)

log = logging.getLogger(__name__)


def _ja_processado_ok(estado: dict, chave: str, *, retry_erros: bool) -> bool:
    """Decide se pular um alvo com base no estado anterior."""
    entry = estado.get(chave)
    if not entry:
        return False
    status = entry.get("_status")
    if status == "ok":
        return True
    if status == "sem_dados":
        return True  # PDF sem ativos: não vale re-rodar Sonnet
    if status == "erro" and not retry_erros:
        return True
    return False


def processar(
    *,
    limite: int | None = None,
    retry_erros: bool = False,
    so_listar: bool = False,
    usar_crawl: bool = True,
) -> None:
    estado = checkpoint.carregar(FONTE)
    log.info("Checkpoint carregado: %d entradas", len(estado))

    alvos = descobrir(usar_crawl=usar_crawl)
    log.info("Alvos descobertos: %d", len(alvos))

    if so_listar:
        for a in alvos:
            print(f"[{a.origem:5s}] {a.tipo_ativo:4s} {a.slug:30s} {a.url}")
        return

    if limite:
        alvos = alvos[:limite]

    total_pdfs = len(alvos)
    total_registros = 0
    pdfs_ok = 0
    pdfs_skip = 0
    pdfs_erro = 0

    for i, alvo in enumerate(alvos, 1):
        log.info("[%d/%d] %s", i, total_pdfs, alvo.slug)
        chave = alvo.slug  # checkpoint por PDF

        if _ja_processado_ok(estado, chave, retry_erros=retry_erros):
            log.info("Já processado (%s), pulando", chave)
            pdfs_skip += 1
            continue

        # 1) Extrai metadados do PDF (HEAD, sem download)
        try:
            conteudo = extrair(alvo)
        except Exception as exc:
            log.exception("Erro no extrair de %s: %s", alvo.slug, exc)
            checkpoint.registrar(
                FONTE, chave, "erro", erro=str(exc), url=alvo.url,
                extra={"etapa": "extrair"},
            )
            pdfs_erro += 1
            continue

        if conteudo is None:
            log.info("PDF indisponível, pulando: %s", alvo.slug)
            checkpoint.registrar(
                FONTE, chave, "erro",
                erro="PDF indisponível (HEAD falhou)",
                url=alvo.url,
                extra={"etapa": "extrair"},
            )
            pdfs_skip += 1
            continue

        # 2) Chama Claude e gera N dicts canônicos
        try:
            resultados = analisar(conteudo)
        except Exception as exc:
            log.exception("Erro no analisar de %s: %s", alvo.slug, exc)
            checkpoint.registrar(
                FONTE, chave, "erro", erro=str(exc), url=conteudo.url_pdf,
                extra={"etapa": "analisar"},
            )
            pdfs_erro += 1
            continue

        if not resultados:
            log.warning("Nenhum resultado canônico para %s", alvo.slug)
            checkpoint.registrar(
                FONTE, chave, "sem_dados", url=conteudo.url_pdf,
            )
            pdfs_erro += 1
            continue

        # 3) Upsert no Supabase — chave única (fonte, codigo_b3, data_referencia)
        # garante idempotência mesmo se este run cair no meio.
        gravados = 0
        falhas_upsert: list[str] = []
        for r in resultados:
            try:
                supabase_client.upsert_analise(r)
                gravados += 1
            except Exception as exc:
                log.exception(
                    "Falha no upsert de %s/%s: %s",
                    alvo.slug,
                    r.get("codigo_b3"),
                    exc,
                )
                falhas_upsert.append(str(r.get("codigo_b3")))

        if gravados == 0:
            checkpoint.registrar(
                FONTE, chave, "erro",
                erro=f"upsert falhou para todos os {len(resultados)} ativos",
                url=conteudo.url_pdf,
                extra={"etapa": "upsert", "falhas": falhas_upsert},
            )
            pdfs_erro += 1
            continue

        total_registros += gravados
        pdfs_ok += 1

        checkpoint.registrar(
            FONTE, chave, "ok",
            url=conteudo.url_pdf,
            extra={
                "ativos_gravados": gravados,
                "ativos_total": len(resultados),
                "falhas_upsert": falhas_upsert or None,
            },
        )

        log.info(
            "OK %s: %d/%d ativos gravados",
            alvo.slug, gravados, len(resultados),
        )

    log.info(
        "Resumo: %d PDFs ok | %d pulados | %d erros | %d registros gravados",
        pdfs_ok, pdfs_skip, pdfs_erro, total_registros,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline Santander Research.")
    parser.add_argument("--limite", type=int, default=None, help="processa só N PDFs")
    parser.add_argument("--retry-erros", action="store_true", help="reprocessa erros")
    parser.add_argument("--so-listar", action="store_true", help="só lista alvos")
    parser.add_argument("--sem-crawl", action="store_true", help="ignora crawl heurístico")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "santander.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    validar_config()

    try:
        processar(
            limite=args.limite,
            retry_erros=args.retry_erros,
            so_listar=args.so_listar,
            usar_crawl=not args.sem_crawl,
        )
    except KeyboardInterrupt:
        log.warning("Interrompido pelo usuário")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())