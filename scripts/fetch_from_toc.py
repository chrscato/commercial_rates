import json
import requests
import gzip
from pathlib import Path

def load_toc_from_url(url: str):
    print(f"🌐 Fetching TOC from URL: {url}")
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"❌ Failed to fetch TOC: {r.status_code}")
    print("✅ TOC loaded from web.")
    return r.json()

def load_toc_from_file(path: str):
    print(f"📄 Loading TOC from file: {path}")
    if path.endswith(".gz"):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            toc = json.load(f)
    else:
        with open(path, 'r', encoding='utf-8') as f:
            toc = json.load(f)
    print("✅ TOC loaded from local file.")
    return toc

def extract_in_network_files(toc_json):
    files = []
    for structure in toc_json.get("reporting_structure", []):
        for file_obj in structure.get("in_network_files", []):
            location = file_obj.get("location")
            if location and location.endswith(".json.gz"):
                files.append({
                    "url": location,
                    "description": file_obj.get("description", ""),
                    "filename": location.split("/")[-1]
                })
    return files

def main():
    source = input("Paste TOC URL or local file path: ").strip()
    try:
        if source.startswith("http://") or source.startswith("https://"):
            toc_json = load_toc_from_url(source)
        else:
            toc_json = load_toc_from_file(source)
    except Exception as e:
        print(f"❌ Failed to load TOC: {e}")
        return

    files = extract_in_network_files(toc_json)
    if not files:
        print("⚠️ No in_network_files found.")
        return

    print(f"✅ Found {len(files)} in_network_files.")
    for i, file in enumerate(files[:10]):  # Preview first 10
        print(f"[{i}] {file['filename']}")
        print(f"     ↳ {file['url']}\n")

if __name__ == "__main__":
    main()
