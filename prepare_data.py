"""
Script executado no build do Render para baixar o CSV do MEC.
O CSV (~225 MB) fica num GitHub Release e é baixado uma vez por deploy.
Configure a variável de ambiente DATA_CSV_URL no dashboard do Render.
"""
import os
import sys
import urllib.request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
CSV_NAME = "PDA_Dados_Cursos_Graduacao_Brasil.csv"
CSV_PATH = os.path.join(DATA_DIR, CSV_NAME)


def download_csv(url: str) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[prepare_data] Baixando CSV de:\n  {url}")

    def _progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(downloaded / total_size * 100, 100)
            print(f"\r  {pct:.1f}%  ({downloaded // (1024*1024)} MB)", end="", flush=True)

    urllib.request.urlretrieve(url, CSV_PATH, reporthook=_progress)
    size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
    print(f"\n[prepare_data] CSV salvo: {size_mb:.1f} MB → {CSV_PATH}")


if __name__ == "__main__":
    if os.path.exists(CSV_PATH):
        size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
        print(f"[prepare_data] CSV já existe ({size_mb:.1f} MB). Nada a fazer.")
        sys.exit(0)

    url = os.getenv("DATA_CSV_URL", "").strip()
    if not url:
        print("[prepare_data] AVISO: DATA_CSV_URL não configurada.")
        print("  Configure a variável no Render e faça um novo deploy.")
        sys.exit(0)

    download_csv(url)
