# FinanceIA — Scripts do Projeto

Repositório de scripts Python do projeto FinanceIA (3A Riva Investimentos).

## Estrutura de Pastas

```
FinanceIA/
├── pipeline_fundos/        # Extração e análise de fundos por plataforma
│   ├── xp_fund_extractor.py
│   ├── btg_fund_extractor.py
│   ├── inter_fund_analyzer.py
│   ├── itau_fund_extractor.py
│   ├── santander_pipeline.py
│   └── bradesco_enrich.py
│
├── qualidade/              # Controle de qualidade dos dados
│   ├── processar_fundos.py     # Mesclagem de duplicatas + validação
│   └── recalibrar_revisar.py   # Recalibração dos marcados como REVISAR
│
├── supabase/               # População do banco de dados
│   ├── populate_supabase.py           # fundos + fundo_plataformas
│   ├── populate_infos_atualizadas.py  # dados quantitativos CVM
│   ├── gerar_embeddings.py            # embeddings para RAG
│   └── cvm_enriquecimento.py          # extração de dados da CVM
│
├── rag_api/                # API intermediária (deploy Railway)
│   ├── main.py
│   └── requirements.txt
│
└── utils/
    └── json_para_excel.py  # Converte JSON → Excel
```

## Fluxo Completo

### 1. Extração por plataforma

Cada plataforma tem seu próprio script de extração:

| Plataforma | Script | Fonte de dados | Modelo |
|---|---|---|---|
| XP | `xp_fund_extractor.py` | Excel + PDFs (cookies) | Sonnet |
| BTG | `btg_fund_extractor.py` | JSON + PDFs (URL pública) | Sonnet |
| Inter | `inter_fund_analyzer.py` | JSON (sem PDF) | Haiku |
| Itaú | `itau_fund_extractor.py` | JSON + PDFs (lamina_produto) | Sonnet |
| Santander | `santander_pipeline.py` | API pública + PDFs | Sonnet |
| Bradesco | `bradesco_enrich.py` | JSON + PDFs (enriquecimento) | Sonnet |

### 2. Consolidação e qualidade

```bash
# 1. Consolide as planilhas de todas as plataformas em consolidado_fundos_total.xlsx

# 2. Processa duplicatas e valida contra diretrizes por subcategoria
python qualidade/processar_fundos.py

# 3. Recalibra os fundos excessivamente marcados como REVISAR
python qualidade/recalibrar_revisar.py
```

### 3. Banco de dados (Supabase)

```bash
# 1. Enriquece com dados de mercado da CVM (rentabilidade, PL histórico)
python supabase/cvm_enriquecimento.py

# 2. Popula tabelas fundos e fundo_plataformas
python supabase/populate_supabase.py

# 3. Popula dados quantitativos
python supabase/populate_infos_atualizadas.py --input dados_mercado_cvm.xlsx

# 4. Gera embeddings (text-embedding-3-large, 1536 dims)
python supabase/gerar_embeddings.py --limit 3  # teste
python supabase/gerar_embeddings.py            # completo
```

### 4. RAG API (Railway)

A API em `rag_api/main.py` é deployada no Railway.

**Endpoints:**
- `POST /buscar` — busca semântica com query expansion + reranking quantitativo
- `GET /premissas` — premissas macro do Supabase
- `GET /stats` — estatísticas do catálogo
- `PUT /premissas/{id}` — atualizar premissa

**Variáveis de ambiente (Railway):**
```
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
API_SECRET_KEY=
```

## Schema Supabase

### Tabela: fundos
CNPJ único por fundo, dados qualitativos e estruturais.

### Tabela: fundo_plataformas
Disponibilidade por plataforma (N linhas por fundo).

### Tabela: fundo_infos_atualizadas
Dados quantitativos dinâmicos: rentabilidade, PL, volatilidade.

### Tabela: fundo_embeddings
Embeddings vetoriais (`vector(1536)` + HNSW index) para busca semântica.

### Função RPC: buscar_fundos
```sql
buscar_fundos(
  query_embedding vector(1536),
  filtro_plataforma text,
  filtro_categoria text,
  filtro_tipo_produto text,
  top_k int
)
```

### Tabela: premissas
Premissas macro e de alocação para o AssessorIA.
Categorias: macro, planejamento, alocacao, regras, alertas.

## Notas Importantes

- **Encoding Windows:** sempre use `encoding="utf-8"` em todos os `open()` — o default `cp1252` causa corrupção silenciosa.
- **API Anthropic:** adicione a chave diretamente no script via `anthropic.Anthropic(api_key="sk-ant-...")`.
- **PDF via URL:** preferir `{"type": "url"}` ao invés de base64 para PDFs grandes (evita timeout).
- **Checkpoint:** todos os scripts têm checkpoint JSON. Fundos com `_status: erro` são retentados no próximo run.
- **Deduplicação antes da IA:** o `processar_fundos.py` faz cross-reference de CNPJs antes de chamar a API.

## Repositório Base44

GitHub: `samuelcleite/consultor-financeiro-pessoal`
API RAG: `https://financeia-rag-api-production.up.railway.app`
