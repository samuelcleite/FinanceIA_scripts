# fix_supabase_secrets.py
import re, os
from pathlib import Path

ROOT = Path(".")  # rode na raiz do projeto

PATTERNS = [
    (r'SUPABASE_URL\s*=\s*"[^"]+"', 'SUPABASE_URL = os.getenv("SUPABASE_URL")'),
    (r'SUPABASE_KEY\s*=\s*"[^"]+"', 'SUPABASE_KEY = os.getenv("SUPABASE_KEY")'),
]

def fix_file(path):
    text = path.read_text(encoding="utf-8")
    new = text
    for pat, repl in PATTERNS:
        new = re.sub(pat, repl, new)
    if new != text and "import os" not in new:
        new = "import os\n" + new
    if new != text:
        path.write_text(new, encoding="utf-8")
        return True
    return False

# Varre todos os .py
fixed = []
for py in ROOT.rglob("*.py"):
    if fix_file(py):
        fixed.append(py)

print(f"\n✅ {len(fixed)} arquivo(s) corrigido(s):")
for f in fixed:
    print(f"   - {f}")

# Cria o .env se não existir
env = ROOT / ".env"
if not env.exists():
    env.write_text('SUPABASE_URL = os.getenv("SUPABASE_URL")\nSUPABASE_KEY = os.getenv("SUPABASE_KEY")\n')
    print("\n📄 .env criado — preencha com suas credenciais reais.")

# Garante .env no .gitignore
gitignore = ROOT / ".gitignore"
content = gitignore.read_text() if gitignore.exists() else ""
if ".env" not in content:
    with gitignore.open("a") as f:
        f.write("\n.env\n")
    print("🔒 .env adicionado ao .gitignore")