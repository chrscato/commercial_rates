# prod/inn/_main.py

import json
import pyarrow.parquet as pq
from pathlib import Path
from prod.scripts.inn import format_check
from prod.scripts.inn.scrapers import grouped_by_provider_reference

MANIFEST_PATH = Path("data/staging/in_network_manifest.json")
OUTPUT_FOLDER = Path("prod/scripts/data/processed/inn_rates/")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

SCRAPER_MAP = {
    "grouped_by_provider_reference": grouped_by_provider_reference.stream_mrf_to_table
}

def main():
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    for entry in manifest:
        url = entry["location"]
        format_style = format_check.detect_format_from_url(url)
        print(f"\n🔍 URL: {url}\n🧠 Format: {format_style}")

        scraper = SCRAPER_MAP.get(format_style)
        if not scraper:
            print("❌ No scraper registered for this format.")
            continue

        try:
            table = scraper(url)
            out_file = OUTPUT_FOLDER / f"{Path(url).stem}.parquet"
            pq.write_table(table, out_file)
            print(f"✅ Saved to {out_file}")
        except Exception as e:
            print(f"❌ Failed to process {url}: {e}")

if __name__ == "__main__":
    main()
