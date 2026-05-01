from core.http import fetch_html
from bs4 import BeautifulSoup

for ticker, label in [("hglg11", "COM_RELATORIO"), ("rzak11", "SEM_RELATORIO")]:
    url = f"https://conteudos.xpi.com.br/fundos-imobiliarios/{ticker}/"
    html = fetch_html(url, fonte="xp")
    soup = BeautifulSoup(html, "html.parser")
    print(f"\n{'='*60}")
    print(f"  {label} -- {ticker.upper()}")
    print(f"{'='*60}")
    print("--- H2s ---")
    for tag in soup.find_all("h2"):
        print(repr(tag.get_text(" ", strip=True)[:120]))
    print("--- H3s ---")
    for tag in soup.find_all("h3"):
        print(repr(tag.get_text(" ", strip=True)[:120]))
    print("--- DIVs com class relevante ---")
    for div in soup.find_all("div", class_=True):
        cls = " ".join(div.get("class", []))
        if any(k in cls.lower() for k in ["content","article","analise","report","body","text","post"]):
            print(repr(cls[:80]), "|", repr(div.get_text(" ", strip=True)[:80]))
