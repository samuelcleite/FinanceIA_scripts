-- =====================================================================
-- Setup da tabela `analises` para o pipeline FinanceIA
-- =====================================================================
-- Execute UMA VEZ no SQL Editor do Supabase, antes de rodar os pipelines.

-- Tabela principal
CREATE TABLE IF NOT EXISTS analises (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo_ativo TEXT NOT NULL,
  codigo_b3 TEXT REFERENCES ativos_rv(codigo_b3),
  emissao_id TEXT REFERENCES rf_emissoes(id),
  emissor_id TEXT REFERENCES rf_emissores(id),
  fonte TEXT NOT NULL,
  url_fonte TEXT,
  data_referencia DATE NOT NULL,
  tese_investimento TEXT,
  drivers TEXT,
  riscos TEXT,
  recomendacao TEXT,
  preco_alvo NUMERIC,
  rating TEXT,
  spread_indicativo NUMERIC,
  ativo BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- =====================================================================
-- Índice único composto (chave de upsert)
-- =====================================================================
-- Um único registro por (ativo, fonte, data_referencia).
-- Como codigo_b3, emissao_id e emissor_id são mutuamente exclusivos
-- (e nullable), usamos COALESCE para tratá-los uniformemente.

CREATE UNIQUE INDEX IF NOT EXISTS uq_analises_ativo_fonte_data
  ON analises (
    COALESCE(codigo_b3, ''),
    COALESCE(emissao_id, ''),
    COALESCE(emissor_id, ''),
    fonte,
    data_referencia
  );

-- =====================================================================
-- Índices de leitura (RAG e dashboards)
-- =====================================================================

CREATE INDEX IF NOT EXISTS idx_analises_codigo_b3
  ON analises (codigo_b3) WHERE codigo_b3 IS NOT NULL AND ativo = true;

CREATE INDEX IF NOT EXISTS idx_analises_emissao
  ON analises (emissao_id) WHERE emissao_id IS NOT NULL AND ativo = true;

CREATE INDEX IF NOT EXISTS idx_analises_emissor
  ON analises (emissor_id) WHERE emissor_id IS NOT NULL AND ativo = true;

CREATE INDEX IF NOT EXISTS idx_analises_fonte_data
  ON analises (fonte, data_referencia DESC);

-- =====================================================================
-- Trigger para manter updated_at
-- =====================================================================

CREATE OR REPLACE FUNCTION trg_analises_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS analises_updated_at ON analises;
CREATE TRIGGER analises_updated_at
  BEFORE UPDATE ON analises
  FOR EACH ROW
  EXECUTE FUNCTION trg_analises_updated_at();
