"""
Script to fetch and process Table of Contents (TOC) files for healthcare transparency data.
"""

import argparse
import gzip
import ijson
import json
import requests
from io import BytesIO
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

TOC_URL = "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-03-26_fidelis_index.json"
OUTPUT_FILE = Path("data/staging/in_network_manifest.json")
MAX_URLS = 10

def open_possibly_gzipped_response(response):
    start = response.content[:2]
    is_gz = start == b'\x1f\x8b'  # GZIP magic bytes
    stream = BytesIO(response.content)
    return gzip.open(stream, 'rt', encoding='utf-8') if is_gz else stream

def extract_plan_info(plan: dict) -> dict:
    """
    Extract relevant plan information from a plan object.
    
    Args:
        plan: Dictionary containing plan information
        
    Returns:
        Dictionary with extracted plan info
    """
    return {
        "plan_name": plan.get("plan_name", "Unknown"),
        "plan_id": plan.get("plan_id", ""),
        "plan_id_type": plan.get("plan_id_type", ""),
        "plan_market_type": plan.get("plan_market_type", "unknown")
    }

def fetch_and_stream_toc(url: str) -> list:
    """
    Fetch and stream a Table of Contents file, extracting relevant information.
    
    Args:
        url: URL of the TOC file
        
    Returns:
        List of dictionaries containing extracted information
    """
    print(f"📥 Streaming TOC from: {url}")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to download TOC: {response.status_code}")

    f = open_possibly_gzipped_response(response)
    parser = ijson.items(f, "reporting_structure.item")

    urls = []
    count = 0
    for structure in tqdm(parser, desc="🔍 Scanning TOC"):
        # Extract entity information
        entity_info = {
            "reporting_entity_name": structure.get("reporting_entity_name", "Unknown"),
            "reporting_entity_type": structure.get("reporting_entity_type", "Unknown"),
            "last_updated": structure.get("last_updated", datetime.now().isoformat())
        }
        
        # Extract plan information
        reporting_plans = []
        for plan in structure.get("reporting_plans", []):
            reporting_plans.append(extract_plan_info(plan))
        
        # Process in-network files
        for file in structure.get("in_network_files", []):
            if "location" in file:
                urls.append({
                    "location": file["location"],
                    "description": file.get("description", ""),
                    "reporting_entity": entity_info["reporting_entity_name"],
                    "reporting_entity_type": entity_info["reporting_entity_type"],
                    "last_updated": entity_info["last_updated"],
                    "reporting_plans": reporting_plans
                })
                count += 1
                if count >= MAX_URLS:
                    return urls
    return urls

def save_staging_list(entries: list):
    """
    Save the extracted information to a JSON file.
    
    Args:
        entries: List of dictionaries to save
    """
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(entries, f, indent=2)
    print(f"✅ Saved {len(entries)} URLs to: {OUTPUT_FILE}")

def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(description="Fetch and process Table of Contents files")
    parser.add_argument("--url", default=TOC_URL, help="URL of the TOC file to process")
    parser.add_argument("--max-urls", type=int, default=MAX_URLS, help="Maximum number of URLs to process")
    args = parser.parse_args()

    print("🚀 Starting TOC fetch (streamed with ijson)...")
    entries = fetch_and_stream_toc(args.url)
    
    print("\nExtracted entries:")
    for entry in entries:
        print(f"\n🔗 {entry['location']}")
        print(f"  Entity: {entry['reporting_entity']} ({entry['reporting_entity_type']})")
        print(f"  Plans: {len(entry['reporting_plans'])}")
        for plan in entry['reporting_plans']:
            print(f"  - {plan['plan_name']} ({plan['plan_market_type']})")
    
    save_staging_list(entries)

if __name__ == "__main__":
    main()
