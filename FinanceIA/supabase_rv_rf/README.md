# Pipeline de Análises Qualitativas — FinanceIA

Pipeline de coleta e geração de análises qualitativas (tese, drivers, riscos)
para ativos brasileiros, populando a tabela `analises` no Supabase.

## Setup inicial

1. **Tabela e índices no Supabase**

   No SQL Editor, execute `sql/setup_analises.sql` (cria a tabela, índice
   único composto e trigger de updated_at).

2. **Variáveis de ambiente**

   ```bash
   cp .env.example .env
   # Edite .env e preencha SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY
   ```

3. **Dependências**

   ```bash
   pip install -r requirements.txt
   ```

## Estrutura

```
analises_pipeline/
├── core/                  # camada compartilhada
│   ├── config.py          # carrega .env, paths, modelos Claude
│   ├── supabase_client.py # upsert idempotente em `analises`
│   ├── claude_client.py   # wrappers Sonnet/Haiku com retry
│   ├── checkpoint.py      # estado JSON por fonte
│   ├── catalog_loader.py  # leitura de ativos_rv / rf_*
│   └── http.py            # fetch HTML/PDF com cache em disco
├── fontes/
│   └── xp/
│       ├── descobrir.py   # catálogo + crawl índice → AlvoXP
│       ├── extrair.py     # HTML → ConteudoXP
│       └── analisar.py    # ConteudoXP → dict canônico
├── checkpoints/           # JSONs de estado por fonte
├── cache/                 # HTMLs/PDFs baixados (evita re-download)
├── logs/                  # tickers descobertos fora do catálogo, etc.
├── sql/setup_analises.sql
└── run_xp.py              # entrypoint XP
```

## Fluxo de cada fonte

Toda fonte segue o mesmo contrato em três etapas:

1. **descobrir** — junta catálogo (Supabase) + descoberta na própria fonte,
   gera lista de "alvos"
2. **extrair** — baixa o conteúdo (HTML ou PDF), retorna texto + metadados
3. **analisar** — chama Claude (Haiku para HTML, Sonnet para PDF), devolve
   dict canônico para upsert

O entrypoint `run_<fonte>.py` orquestra isso com checkpoint, dedup e logging.

## Rodar XP Research

```bash
# Roda tudo que está pendente
python run_xp.py

# Só lista alvos sem chamar Claude (debug)
python run_xp.py --so-listar --limite 20

# Reprocessa também os que falharam
python run_xp.py --retry-erros

# Limita execução (útil em dev)
python run_xp.py --limite 5
```

## Convenções

- **Idempotência**: chave única é (codigo_b3 ou emissao_id ou emissor_id,
  fonte, data_referencia). Rodar 2x não duplica.
- **Dedup pré-Claude**: antes de chamar a API, verifica se já existe
  análise completa no Supabase para essa combinação — economia direta.
- **Checkpoint**: cada run grava `checkpoints/{fonte}.json` incrementalmente.
  Pode interromper com Ctrl+C e retomar.
- **Cache HTTP**: HTMLs e PDFs ficam em `cache/{fonte}/` por 100% da vida.
  Apague manualmente se quiser forçar re-download.
- **Modelos**: Haiku para texto pré-extraído (XP), Sonnet para PDFs
  (BTG/Santander/Itaú BBA), seguindo padrão do projeto.

## Próximas fontes

A estrutura está pronta para receber:

- `fontes/santander/` — PDFs mensais em URLs fixas, multi-ativo
- `fontes/btg/` — PDFs públicos do `content.btgpactual.com/research`
- `fontes/itau_bba/` — relatórios setoriais públicos de FIIs

Cada uma reusa todo o `core/` e implementa apenas seu próprio
`descobrir.py` / `extrair.py` / `analisar.py` + um `run_<fonte>.py`.
