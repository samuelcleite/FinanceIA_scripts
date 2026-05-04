"""
Entrypoint do pipeline BTG Research.

Uso:
    python run_btg.py                       # processa pendentes (todas categorias MVP)
    python run_btg.py --paginas 5           # só primeiras 5 páginas por categoria
    python run_btg.py --limite 10           # processa no máximo 10 análises (debug)
    python run_btg.py --so-listar           # só lista candidatos, sem chamar Claude
    python run_btg.py --retry-erros         # reprocessa entradas com _status: erro
    python run_btg.py --categorias ACS      # restringe categorias

Diferenças vs run_xp.py:
- A API do BTG já entrega tudo em JSON (sem PDF, sem fetch HTML por análise).
- Cada análise BTG pode mapear para N ativos do catálogo (multi-ativo) — o
  checkpoint usa o `id` do BTG (objeto único da API) e cada ativo vira uma
  linha em `analises` graças à chave única (fonte, codigo_b3, data_referencia).
"""
import argparse
import sys
import time
import traceback
from datetime import datetime, timezone

from core.checkpoint import filtrar_pendentes, registrar, montar_chave
from core.config import validar_config
from core.supabase_client import upsert_analise
from fontes.btg.descobrir import descobrir, FONTE, CATEGORIAS_MVP
from fontes.btg.extrair import extrair
from fontes.btg.analisar import analisar, planejar_processamento


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline BTG Research.")
    parser.add_argument("--retry-erros", action="store_true",
                        help="Reprocessa entradas com _status: erro")
    parser.add_argument("--limite", type=int, default=None,
                        help="Processa no máximo N análises (debug)")
    parser.add_argument("--so-listar", action="store_true",
                        help="Apenas lista candidatos, sem chamar Claude")
    parser.add_argument("--paginas", type=int, default=None,
                        help="Limita N páginas POR CATEGORIA (default: todas)")
    parser.add_argument("--categorias", nargs="+", default=list(CATEGORIAS_MVP),
                        choices=["ACS", "FII", "BDR", "RF"],
                        help=f"Categorias a processar (default: {' '.join(CATEGORIAS_MVP)})")
    args = parser.parse_args()

    validar_config()

    _log("=== Pipeline BTG Research ===")
    _log(f"Categorias: {args.categorias} | páginas/cat: {args.paginas or 'todas'}")

    alvos_brutos = descobrir(tuple(args.categorias), max_paginas=args.paginas)
    _log(f"Alvos descobertos via API: {len(alvos_brutos)}")

    # Pré-filtro: 1 análise por ativo, sempre a mais recente. Alvos cuja
    # cobertura inteira foi suplantada por outras análises mais novas são
    # descartados antes do Claude.
    alvos, mapa_winner = planejar_processamento(alvos_brutos)
    _log(f"Alvos após dedup por ativo (vencedores): {len(alvos)}")
    _log(f"Ativos únicos cobertos (1 linha cada): {len(mapa_winner)}")

    chaves = [montar_chave(a.btg_id, FONTE) for a in alvos]
    mapa_chave_alvo = dict(zip(chaves, alvos))

    pendentes = filtrar_pendentes(FONTE, chaves, incluir_erros=args.retry_erros)
    _log(f"Pendentes a processar: {len(pendentes)}")

    if args.so_listar:
        for c in pendentes[: args.limite or 50]:
            a = mapa_chave_alvo[c]
            tickers = ",".join(
                (ab.get("asset") or {}).get("ticker", "") for ab in a.ativos_brutos
            ) or "(sem ativos)"
            _log(f"  [{a.categoria}] {a.btg_id} | {tickers} | {a.titulo[:80]}")
        return 0

    if args.limite:
        pendentes = pendentes[: args.limite]
        _log(f"Limitado a {args.limite} análises")

    ok, erros, sem_dados = 0, 0, 0
    total_gravados = 0

    for i, chave in enumerate(pendentes, 1):
        alvo = mapa_chave_alvo[chave]
        prefixo = f"[{i}/{len(pendentes)}] {alvo.btg_id} ({alvo.categoria})"

        try:
            # 1. Extrai texto do body HTML
            conteudo = extrair(alvo)
            if conteudo is None:
                _log(f"{prefixo} body insuficiente — pulando")
                registrar(FONTE, chave, "sem_dados",
                          extra={"motivo": "body_insuficiente"})
                sem_dados += 1
                continue

            # 2. Analisar (resolve ativos + filtro de winners + 1 chamada Claude + replica)
            resultados = analisar(conteudo, mapa_winner=mapa_winner)
            if not resultados:
                _log(f"{prefixo} nenhum ativo deste alvo é o mais recente — pulando")
                registrar(FONTE, chave, "sem_dados",
                          extra={"motivo": "ativos_suplantados_ou_fora_catalogo"})
                sem_dados += 1
                continue

            # 3. Upsert por ativo. Chave única (fonte, codigo_b3, data_referencia)
            # preserva análises de dias diferentes como linhas distintas.
            gravados = 0
            falhas: list[str] = []
            for r in resultados:
                try:
                    upsert_analise(r)
                    gravados += 1
                except Exception as exc:
                    falhas.append(f"{r.get('codigo_b3')}: {exc}")

            if gravados == 0:
                _log(f"{prefixo} ERRO em todos os upserts: {falhas}")
                registrar(FONTE, chave, "erro",
                          erro=f"todos_upsert_falharam: {falhas}",
                          extra={"falhas": falhas})
                erros += 1
            else:
                _log(f"{prefixo} ok ({gravados}/{len(resultados)} ativos gravados, "
                     f"data_ref={conteudo.data_referencia})")
                registrar(FONTE, chave, "ok",
                          extra={
                              "ativos_gravados": gravados,
                              "ativos_total": len(resultados),
                              "falhas": falhas,
                              "data_referencia": str(conteudo.data_referencia),
                          })
                ok += 1
                total_gravados += gravados

            time.sleep(0.4)

        except Exception as e:
            tb = traceback.format_exc()
            _log(f"{prefixo} ERRO: {e}")
            registrar(FONTE, chave, "erro",
                      erro=f"{type(e).__name__}: {e}\n{tb[:500]}")
            erros += 1

    _log("=== Fim do run ===")
    _log(f"  ok={ok}  sem_dados={sem_dados}  erros={erros}  registros_gravados={total_gravados}")
    return 0 if erros == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
