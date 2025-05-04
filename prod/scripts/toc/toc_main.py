import json
import requests
import gzip
from pathlib import Path
from io import BytesIO

from .utils.toc_format_check import detect_toc_format
from .utils import structure_level_inn


TOC_INPUT = "https://tic-mrf.regence.com/mrf/current/2025-05-01_Regence%20BlueShield%20of%20Idaho,%20Inc.-ASO_index.json"
OUTPUT_PATH = Path("prod/data/staging/staging_urls.json")

def smart_load(source: str):
    if source.startswith("http"):
        print(f"🌐 Downloading: {source}")
        r = requests.get(source)
        r.raise_for_status()
        content = r.content
    else:
        print(f"📂 Loading: {source}")
        with open(source, "rb") as f:
            content = f.read()

    if content[:2] == b'\x1f\x8b':
        with gzip.open(BytesIO(content), 'rt', encoding='utf-8') as f:
            return json.load(f)
    else:
        return json.loads(content.decode("utf-8"))

def main():
    print("🔎 Detecting TOC format...")
    format_style = detect_toc_format(TOC_INPUT)
    print(f"🧠 Detected: {format_style}")

    toc_json = smart_load(TOC_INPUT)

    if format_style == "structure_level_inn":
        manifest = structure_level_inn.extract_urls(toc_json)

        print(f"\n🔗 Extracted {len(manifest)} in-network entries:")
        for entry in manifest:
            print("•", entry["location"])

        OUTPUT_PATH = Path("data/staging/in_network_manifest.json")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w") as f:
            json.dump(manifest, f, indent=2)

        print(f"\n✅ Manifest saved to: {OUTPUT_PATH}")

    print(f"\n✅ Saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
