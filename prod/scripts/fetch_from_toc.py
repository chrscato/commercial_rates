import argparse
import gzip
import ijson
import json
import requests
from io import BytesIO
from pathlib import Path
from tqdm import tqdm

TOC_URL = "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-03-26_fidelis_index.json"
OUTPUT_FILE = Path("data/staging/staging_urls.json")
MAX_URLS = 10

def open_possibly_gzipped_response(response):
    start = response.content[:2]
    is_gz = start == b'\x1f\x8b'  # GZIP magic bytes
    stream = BytesIO(response.content)
    return gzip.open(stream, 'rt', encoding='utf-8') if is_gz else stream


def fetch_and_stream_toc(url: str) -> list:
    print(f"📥 Streaming TOC from: {url}")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to download TOC: {response.status_code}")

    f = open_possibly_gzipped_response(response)
    parser = ijson.items(f, "reporting_structure.item")

    urls = []
    count = 0
    for structure in tqdm(parser, desc="🔍 Scanning TOC"):
        reporting_entity = structure.get("reporting_entity_name", "Unknown")
        for plan in structure.get("reporting_plans", []):
            for file in plan.get("in_network_files", []):
                if "location" in file:
                    urls.append({
                        "description": file.get("description", ""),
                        "url": file["location"],
                        "reporting_entity": reporting_entity
                    })
                    count += 1
                    if count >= MAX_URLS:
                        return urls
    return urls

def save_staging_list(entries: list):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(entries, f, indent=2)
    print(f"✅ Saved {len(entries)} URLs to: {OUTPUT_FILE}")

def main():
    print("🚀 Starting TOC fetch (streamed with ijson)...")
    entries = fetch_and_stream_toc(TOC_URL)
    for entry in entries:
        print(f"🔗 {entry['url']}  ({entry['description']})")
    save_staging_list(entries)

if __name__ == "__main__":
    main()
