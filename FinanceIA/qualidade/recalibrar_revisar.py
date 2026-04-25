"""
recalibrar_revisar.py
Reprocessa fundos marcados como REVISAR com critério mais rigoroso.

Contexto: a validação inicial pode marcar muitos fundos como REVISAR por diferenças
de ênfase que não são contradições reais. Este script reprocessa apenas esses fundos
com um prompt recalibrado que exige contradições diretas para manter o status REVISAR.

Uso:
    python recalibrar_revisar.py
    python recalibrar_revisar.py --input fundos_revisados.xlsx
"""

import argparse
import json
import time

import anthropic
import openpyxl
import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = "COLE_SUA_CHAVE_AQUI"
MODEL_HAIKU       = "claude-haiku-4-5-20251001"

INPUT_XLSX  = "fundos_revisados.xlsx"
OUTPUT_XLSX = "fundos_recalibrados.xlsx"
CHECKPOINT  = "progress_recalibrar.json"

DELAY_S = 0.2

DIRETRIZES = {
    # (mesmas diretrizes do processar_fundos.py)
    "Soberano": {
        "quando_indicar": "Reserva de emergência, perfil conservador, curto prazo, liquidez imediata.",
        "quando_nao_indicar": "Investidores que buscam rentabilidade acima do CDI.",
        "alertas": "Risco de retorno abaixo da inflação em cenários de juros baixos.",
    },
    "Crédito Privado": {
        "quando_indicar": "Perfil moderado, médio prazo, busca de prêmio sobre CDI.",
        "quando_nao_indicar": "Perfil conservador ou quem necessita de liquidez imediata.",
        "alertas": "Risco de crédito. Liquidez restrita. Come-cotas semestral.",
    },
    "Macro": {
        "quando_indicar": "Perfil moderado a arrojado, médio a longo prazo, diversificação.",
        "quando_nao_indicar": "Perfil conservador ou curto prazo.",
        "alertas": "Volatilidade maior que renda fixa. Resultados podem ser negativos.",
    },
    "Long Only": {
        "quando_indicar": "Perfil arrojado, crescimento patrimonial, horizonte longo (5+ anos).",
        "quando_nao_indicar": "Perfil conservador ou moderado. Não para recursos de curto prazo.",
        "alertas": "Alta volatilidade. Pode ter drawdowns significativos. Tributação ações (15% IR).",
    },
    "Dividendos": {
        "quando_indicar": "Perfil moderado a arrojado, renda passiva, longo prazo.",
        "quando_nao_indicar": "Perfil conservador. Curto prazo.",
        "alertas": "Concentração setorial frequente. Tributação ações.",
    },
}

# ─── PROMPT RECALIBRADO ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """Você é um auditor de qualidade de dados de fundos de investimento. \
Revise com critério rigoroso e conservador. Responda SOMENTE com JSON válido."""

VALIDATE_PROMPT = """Reavalie o fundo "{nome}" (subcategoria: {subcategoria}).

CRITÉRIO ESTRITO — Marque REVISAR APENAS se houver:
1. Contradição direta de perfil (ex: diz "conservador" mas subcategoria é de alto risco)
2. Subcategoria claramente errada (ex: fundo de ações classificado como Renda Fixa)
3. Ausência de alerta crítico obrigatório (ex: fundo de crédito privado sem mencionar risco de crédito)

NÃO marque REVISAR por:
- Diferenças de ênfase ou estilo de escrita
- Texto menos detalhado do que o ideal
- Informações corretas mas incompletas
- Ausência de informações opcionais

Diretrizes da subcategoria {subcategoria}:
{diretrizes}

Informações do fundo:
- quando_indicar: {quando_indicar}
- quando_nao_indicar: {quando_nao_indicar}
- alertas: {alertas}
- descricao_tecnica: {descricao_tecnica}

Retorne JSON: {{"validacao_status": "OK|ATENÇÃO|REVISAR", "validacao_obs": "motivo específico ou vazio se OK"}}"""

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _parse_json(text: str) -> dict:
    text = text.strip()
    if "```" in text:
        for part in text.split("```"):
            part = part.strip()
            if part.startswith("json"):
                part = part[4:]
            if part.strip().startswith("{"):
                text = part.strip()
                break
    return json.loads(text)


def carregar_ck(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def salvar_ck(path: str, ck: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(ck, f, ensure_ascii=False, indent=2)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=INPUT_XLSX)
    args = parser.parse_args()

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    print(f"📂 Carregando {args.input}...")
    df = pd.read_excel(args.input, sheet_name="Fundos Revisados")
    df = df.fillna("")

    # Filtra apenas os REVISAR com conteúdo qualitativo
    mask_revisar = df["validacao_status"] == "REVISAR"
    mask_tem_qual = df["Quando Indicar"].str.strip().astype(bool)
    df_revisar = df[mask_revisar & mask_tem_qual].copy()

    print(f"   Total: {len(df)} fundos | REVISAR com conteúdo: {len(df_revisar)}")

    ck = carregar_ck(CHECKPOINT)
    processados = 0
    mantidos = 0
    rebaixados_ok = 0
    rebaixados_atencao = 0

    for idx, row in df_revisar.iterrows():
        cnpj   = str(row.get("CNPJ", ""))
        nome   = row.get("Nome", "")
        subcat = row.get("Subcategoria", "")

        if cnpj in ck:
            novo_status = ck[cnpj].get("validacao_status", "REVISAR")
            df.at[idx, "validacao_status"] = novo_status
            df.at[idx, "validacao_obs"]    = ck[cnpj].get("validacao_obs", "")
            processados += 1
            continue

        print(f"  [{processados+1}/{len(df_revisar)}] {str(nome)[:50]} — {subcat}")

        if subcat not in DIRETRIZES:
            print(f"    ⚠️  Sem diretriz — mantendo REVISAR")
            ck[cnpj] = {"validacao_status": "REVISAR", "validacao_obs": "Sem diretriz para recalibrar"}
            salvar_ck(CHECKPOINT, ck)
            processados += 1
            mantidos += 1
            continue

        diretriz = DIRETRIZES[subcat]
        diretriz_str = "\n".join([f"- {k}: {v}" for k, v in diretriz.items()])

        try:
            resp = client.messages.create(
                model=MODEL_HAIKU,
                max_tokens=300,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": VALIDATE_PROMPT.format(
                    nome=nome, subcategoria=subcat,
                    diretrizes=diretriz_str,
                    quando_indicar=row.get("Quando Indicar", ""),
                    quando_nao_indicar=row.get("Quando não indicar", ""),
                    alertas=row.get("Alertas", ""),
                    descricao_tecnica=row.get("Descrição Técnica", ""),
                )}]
            )
            resultado   = _parse_json(resp.content[0].text)
            novo_status = resultado.get("validacao_status", "REVISAR")
            nova_obs    = resultado.get("validacao_obs", "")

            df.at[idx, "validacao_status"] = novo_status
            df.at[idx, "validacao_obs"]    = nova_obs
            ck[cnpj] = {"validacao_status": novo_status, "validacao_obs": nova_obs}
            salvar_ck(CHECKPOINT, ck)

            if novo_status == "REVISAR":
                mantidos += 1
                print(f"    → MANTIDO: REVISAR | {nova_obs[:60]}")
            elif novo_status == "ATENÇÃO":
                rebaixados_atencao += 1
                print(f"    → Rebaixado para ATENÇÃO")
            else:
                rebaixados_ok += 1
                print(f"    → Rebaixado para OK")

        except Exception as e:
            print(f"    ❌ Erro: {e}")
            ck[cnpj] = {"validacao_status": "REVISAR", "validacao_obs": f"Erro: {e}"}
            salvar_ck(CHECKPOINT, ck)
            mantidos += 1

        processados += 1
        time.sleep(DELAY_S)

    # Salva output
    df.to_excel(OUTPUT_XLSX, sheet_name="Fundos Revisados", index=False)

    print(f"\n📊 Resultado da recalibração:")
    print(f"   Processados: {processados}")
    print(f"   Mantidos como REVISAR: {mantidos}")
    print(f"   Rebaixados para ATENÇÃO: {rebaixados_atencao}")
    print(f"   Rebaixados para OK: {rebaixados_ok}")
    print(f"\n💾 Salvo em: {OUTPUT_XLSX}")

    # Contagem final
    status_counts = df["validacao_status"].value_counts()
    print(f"\nDistribuição final:")
    for status, count in status_counts.items():
        print(f"   {status}: {count}")


if __name__ == "__main__":
    main()
