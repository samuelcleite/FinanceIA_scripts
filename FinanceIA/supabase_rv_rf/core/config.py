"""
Configuração central do pipeline.
Carrega variáveis do .env e expõe constantes usadas em todo o projeto.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Raiz do projeto (analises_pipeline/)
ROOT = Path(__file__).resolve().parent.parent

# Carrega .env da raiz
load_dotenv(ROOT / ".env")

# --- Supabase ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# --- Anthropic ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# --- Modelos Claude (referência única) ---
MODEL_HAIKU = "claude-haiku-4-5-20251001"
MODEL_SONNET = "claude-sonnet-4-6"

# --- Scraping ---
USER_AGENT = os.getenv(
    "SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
)
HTTP_TIMEOUT = 30

# --- Paths ---
CHECKPOINT_DIR = ROOT / "checkpoints"
CACHE_DIR = ROOT / "cache"
LOG_DIR = ROOT / "logs"

CHECKPOINT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)


def validar_config() -> None:
    """Valida que todas as variáveis obrigatórias estão presentes."""
    faltando = []
    if not SUPABASE_URL:
        faltando.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        faltando.append("SUPABASE_KEY")
    if not ANTHROPIC_API_KEY:
        faltando.append("ANTHROPIC_API_KEY")

    if faltando:
        raise RuntimeError(
            f"Variáveis de ambiente faltando: {', '.join(faltando)}. "
            f"Verifique o arquivo .env na raiz do projeto."
        )
