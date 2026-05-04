"""
Entrypoint do pipeline Itaú BBA Research.

Cobre 3 categorias de PDFs públicos (todas com fonte = "itau_bba_research";
distinção entre elas via url_fonte e data_referencia):
  - setorial: relatórios setoriais semestrais de FII (Lajes, Galpões, etc).
  - mensal:   Relatório Mensal de FII (mais recente).
  - radar:    Radar de Preferências de Ações (1 PDF mensal com top picks).

Cada PDF é multi-ativo, então o loop itera os payloads internos. O checkpoint
trabalha na granularidade do PDF (slug), e a dedup em `analises` usa a chave
(codigo_b3, fonte, data_referencia) por ativo.

Uso:
    python run_itau_bba.py                          # processa todas as categorias
    python run_itau_bba.py --categorias setorial    # só setoriais
    python run_itau_bba.py --categorias mensal,radar
    python run_itau_bba.py --historico              # backfill setoriais antigos
    python run_itau_bba.py --retry-erros            # reprocessa _status: erro
    python run_itau_bba.py --limite 1               # processa só 1 PDF (debug)
    python run_itau_bba.py --so-listar              # lista candidatos sem Claude
"""
import argparse
import sys
import time
import traceback
from datetime import datetime, timezone

from core.checkpoint import filtrar_pendentes, montar_chave, registrar
from core.config import validar_config
from core.supabase_client import existe_analise_completa, upsert_analise
from fontes.itau_bba.analisar import analisar
from fontes.itau_bba.descobrir import CATEGORIAS_VALIDAS, FONTE, descobrir
from fontes.itau_bba.extrair import extrair


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)


def _parse_categorias(raw: str | None) -> tuple[str, ...]:
    """'setorial,mensal' -> ('setorial', 'mensal'). None -> todas."""
    if not raw:
        return CATEGORIAS_VALIDAS
    cats = tuple(c.strip().lower() for c in raw.split(",") if c.strip())
    invalidas = [c for c in cats if c not in CATEGORIAS_VALIDAS]
    if invalidas:
        raise SystemExit(
            f"Categorias inválidas: {invalidas}. "
            f"Válidas: {', '.join(CATEGORIAS_VALIDAS)}"
        )
    return cats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry-erros", action="store_true",
                        help="Reprocessa entradas com _status: erro")
    parser.add_argument("--limite", type=int, default=None,
                        help="Processa no máximo N PDFs (debug)")
    parser.add_argument("--so-listar", action="store_true",
                        help="Apenas lista candidatos, sem chamar Claude")
    parser.add_argument("--historico", action="store_true",
                        help="Backfill: processa TODOS os históricos de "
                             "setoriais (default: só o mais recente). "
                             "Não afeta mensal/radar.")
    parser.add_argument("--categorias", type=str, default=None,
                        help="Subconjunto separado por vírgula: "
                             f"{','.join(CATEGORIAS_VALIDAS)} "
                             f"(default: todas)")
    args = parser.parse_args()

    validar_config()

    categorias = _parse_categorias(args.categorias)

    _log("=== Pipeline Itaú BBA Research ===")
    _log(
        f"Categorias: {','.join(categorias)} | "
        f"setoriais histórico: {args.historico}"
    )

    alvos = descobrir(categorias=categorias, historico=args.historico)
    _log(f"PDFs descobertos: {len(alvos)}")

    if not alvos:
        _log("Nenhum PDF descoberto — verifique se as páginas índice "
             "estão acessíveis e se os parsers HTML continuam válidos.")
        return 0

    chaves = [montar_chave(a.slug, FONTE, "v2") for a in alvos]
    mapa_chave_alvo = dict(zip(chaves, alvos))

    pendentes = filtrar_pendentes(FONTE, chaves, incluir_erros=args.retry_erros)
    _log(f"Pendentes a processar: {len(pendentes)}")

    if args.so_listar:
        for c in pendentes[: args.limite or 50]:
            a = mapa_chave_alvo[c]
            tag = f"{a.categoria}/{a.tipo_ativo}"
            seg = a.segmento or a.periodo or "?"
            _log(f"  {a.slug:42s} [{tag:14s}] {seg}  ->  {a.url}")
        return 0

    if args.limite:
        pendentes = pendentes[: args.limite]
        _log(f"Limitado a {args.limite} PDFs")

    pdfs_ok = 0
    pdfs_sem_dados = 0
    pdfs_erros = 0
    ativos_inseridos = 0
    ativos_pulados = 0

    for i, chave in enumerate(pendentes, 1):
        alvo = mapa_chave_alvo[chave]
        prefixo = f"[{i}/{len(pendentes)}] {alvo.slug} ({alvo.categoria})"

        try:
            conteudo = extrair(alvo)
            if conteudo is None:
                _log(f"{prefixo} sem PDF (404 ou erro)")
                registrar(FONTE, chave, "sem_dados", url=alvo.url)
                pdfs_sem_dados += 1
                continue

            payloads = analisar(conteudo)
            if not payloads:
                _log(f"{prefixo} Claude não extraiu nenhum ativo válido")
                registrar(FONTE, chave, "sem_dados", url=alvo.url)
                pdfs_sem_dados += 1
                continue

            inseridos = 0
            pulados = 0
            for payload in payloads:
                if existe_analise_completa(
                    fonte=payload["fonte"],
                    data_referencia=payload["data_referencia"],
                    codigo_b3=payload["codigo_b3"],
                ):
                    pulados += 1
                    continue
                upsert_analise(payload)
                inseridos += 1

            ativos_inseridos += inseridos
            ativos_pulados += pulados

            _log(
                f"{prefixo} ok  (data_ref={payloads[0]['data_referencia']}, "
                f"inseridos={inseridos}, pulados={pulados})"
            )
            registrar(
                FONTE, chave, "ok",
                url=alvo.url,
                extra={
                    "data_referencia": payloads[0]["data_referencia"],
                    "n_ativos": len(payloads),
                    "categoria": alvo.categoria,
                    "tipo_ativo": alvo.tipo_ativo,
                    "segmento": alvo.segmento,
                },
            )
            pdfs_ok += 1

            time.sleep(0.5)

        except Exception as e:
            tb = traceback.format_exc()
            _log(f"{prefixo} ERRO: {e}")
            registrar(
                FONTE, chave, "erro",
                erro=f"{type(e).__name__}: {e}\n{tb[:500]}",
                url=alvo.url,
            )
            pdfs_erros += 1

    _log("=== Fim do run ===")
    _log(
        f"  pdfs_ok={pdfs_ok}  pdfs_sem_dados={pdfs_sem_dados}  "
        f"pdfs_erros={pdfs_erros}"
    )
    _log(
        f"  ativos_inseridos={ativos_inseridos}  "
        f"ativos_pulados={ativos_pulados}"
    )
    return 0 if pdfs_erros == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
