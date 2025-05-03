import argparse
import gzip
import json
import requests
from pathlib import Path
from io import BytesIO

SAMPLE_OUTPUT = Path("https://tic-mrf.regence.com/mrf/current/2025-04-01_Regence%20BlueShield%20of%20Idaho,%20Inc.-non-ASO_index.json")

def smart_open(source: str):
    if source.startswith("http"):
        print(f"🌐 Downloading from: {source}")
        r = requests.get(source)
        r.raise_for_status()
        content = r.content
    else:
        print(f"📂 Opening local file: {source}")
        with open(source, "rb") as f:
            content = f.read()

    if content[:2] == b'\x1f\x8b':  # GZIP magic number
        return gzip.open(BytesIO(content), 'rt', encoding='utf-8')
    else:
        return BytesIO(content)

def trim_structure(data):
    if isinstance(data, dict):
        return {k: trim_structure(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [trim_structure(item) for item in data]
    else:
        return data

def save_sample(data):
    SAMPLE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_OUTPUT, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n✅ Full JSON sample saved to: {SAMPLE_OUTPUT}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="URL or local file path to .json or .json.gz")
    args = parser.parse_args()

    print(f"🚀 Loading and sampling full structure...")
    with smart_open(args.source) as f:
        data = json.load(f)

    trimmed = trim_structure(data)
    save_sample(trimmed)

if __name__ == "__main__":
    main()
