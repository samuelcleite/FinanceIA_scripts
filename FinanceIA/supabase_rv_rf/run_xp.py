"""
Entrypoint do pipeline XP Research.

Uso:
    python run_xp.py                  # processa pendentes do catálogo
    python run_xp.py --retry-erros    # reprocessa também os _status: erro
    python run_xp.py --limite 10      # processa só os 10 primeiros (debug)
    python run_xp.py --so-listar      # só lista candidatos, sem chamar Claude
"""
import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timezone

from core.checkpoint import filtrar_pendentes, registrar, montar_chave
from core.config import LOG_DIR, validar_config
from core.supabase_client import existe_analise_completa, upsert_analise
from fontes.xp.descobrir import descobrir_todos, FONTE
from fontes.xp.extrair import extrair
from fontes.xp.analisar import analisar


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)


def _salvar_descobertos(novos: list) -> None:
    """Persiste tickers descobertos fora do catálogo para revisão manual."""
    if not novos:
        return
    p = LOG_DIR / "xp_descobertos_nao_catalogados.json"
    payload = [
        {"codigo_b3": n.codigo_b3, "tipo_ativo": n.tipo_ativo, "url": n.url}
        for n in novos
    ]
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _log(f"Tickers fora do catálogo gravados em {p} ({len(novos)} itens)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry-erros", action="store_true",
                        help="Reprocessa entradas com _status: erro")
    parser.add_argument("--limite", type=int, default=None,
                        help="Processa no máximo N alvos (debug)")
    parser.add_argument("--so-listar", action="store_true",
                        help="Apenas lista candidatos, sem chamar Claude")
    args = parser.parse_args()

    validar_config()

    _log("=== Pipeline XP Research ===")
    _log("Carregando catálogo + descoberta de índice...")

    alvos_cat, novos = descobrir_todos()
    _log(f"Alvos no catálogo (1 por raiz): {len(alvos_cat)}")
    _log(f"Tickers descobertos fora do catálogo: {len(novos)}")
    _salvar_descobertos(novos)

    chaves = [
        montar_chave(a.codigo_b3, FONTE, "v2") for a in alvos_cat
    ]
    mapa_chave_alvo = dict(zip(chaves, alvos_cat))

    pendentes = filtrar_pendentes(FONTE, chaves, incluir_erros=args.retry_erros)
    _log(f"Pendentes a processar: {len(pendentes)}")

    if args.so_listar:
        for c in pendentes[: args.limite or 50]:
            a = mapa_chave_alvo[c]
            alts = f" (alt: {','.join(a.tickers_alternativos)})" if a.tickers_alternativos else ""
            _log(f"  {a.codigo_b3} via {a.ticker_url} ({a.tipo_ativo}){alts}  →  {a.url}")
        return 0

    if args.limite:
        pendentes = pendentes[: args.limite]
        _log(f"Limitado a {args.limite} alvos")

    ok, erros, pulados, sem_dados = 0, 0, 0, 0

    for i, chave in enumerate(pendentes, 1):
        alvo = mapa_chave_alvo[chave]
        prefixo = f"[{i}/{len(pendentes)}] {alvo.codigo_b3} ({alvo.ticker_url})"

        try:
            # 1. Extrai HTML
            conteudo = extrair(alvo)
            if conteudo is None:
                _log(f"{prefixo} sem relatório (404 ou conteúdo insuficiente)")
                registrar(FONTE, chave, "sem_dados", url=alvo.url)
                sem_dados += 1
                continue

            # 2. Dedup pré-Claude (chave: codigo_b3 raiz + fonte + data)
            if existe_analise_completa(
                fonte="xp_research",
                data_referencia=conteudo.data_referencia,
                codigo_b3=conteudo.codigo_b3,
            ):
                _log(f"{prefixo} já existe no Supabase para "
                     f"{conteudo.data_referencia} — pulando Claude")
                registrar(FONTE, chave, "pulado_ja_existe", url=alvo.url)
                pulados += 1
                continue

            # 3. Claude
            payload = analisar(conteudo)

            # 4. Upsert
            res = upsert_analise(payload)
            _log(f"{prefixo} ok  (data_ref={conteudo.data_referencia}, "
                 f"id={res.get('id', '?')[:8]}...)")
            registrar(FONTE, chave, "ok", url=alvo.url,
                      extra={"data_referencia": str(conteudo.data_referencia)})
            ok += 1

            time.sleep(0.5)

        except Exception as e:
            tb = traceback.format_exc()
            _log(f"{prefixo} ERRO: {e}")
            registrar(FONTE, chave, "erro",
                      erro=f"{type(e).__name__}: {e}\n{tb[:500]}",
                      url=alvo.url)
            erros += 1

    _log("=== Fim do run ===")
    _log(f"  ok={ok}  pulados={pulados}  sem_dados={sem_dados}  erros={erros}")
    return 0 if erros == 0 else 1


if __name__ == "__main__":
    sys.exit(main())