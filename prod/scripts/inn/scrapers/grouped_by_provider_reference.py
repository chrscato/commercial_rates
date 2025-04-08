# prod/inn/scrapers/grouped_by_provider_reference.py

import requests, gzip, ijson, pyarrow as pa
from io import BytesIO
from tqdm import tqdm

CPT_CODES = {"99213", "73221", "72000", "72156"}
BATCH_SIZE = 10000

def stream_mrf_to_table(url: str) -> pa.Table:
    print(f"📥 Streaming MRF from: {url}")
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise Exception(f"❌ Failed to fetch MRF: {r.status_code}")
    f = gzip.GzipFile(fileobj=BytesIO(r.content))

    # Step 1: provider_references
    f.seek(0)
    provider_map = {}
    refs = ijson.items(f, 'provider_references.item')
    for ref in refs:
        ref_id = ref.get("provider_group_id")
        entries = []
        for group in ref.get("provider_groups", []):
            tin = group.get("tin", {}).get("value", "unknown")
            for npi in group.get("npi", []):
                entries.append((npi, tin))
        provider_map[ref_id] = entries

    # Step 2: in_network streaming
    f.seek(0)
    items = ijson.items(f, 'in_network.item')
    batches, current = [], []

    for item in tqdm(items, desc="CPT matches"):
        code = item.get("billing_code")
        if code not in CPT_CODES:
            continue
        for rate in item.get("negotiated_rates", []):
            for ref_id in rate.get("provider_references", []):
                for npi, tin in provider_map.get(ref_id, [("unknown", "unknown")]):
                    for price in rate.get("negotiated_prices", []):
                        current.append({
                            "cpt": code,
                            "npi": npi,
                            "tin": tin,
                            "pos": price.get("place_of_service", "unknown"),
                            "negotiated_rate": float(price.get("negotiated_rate") or 0.0),
                        })

        if len(current) >= BATCH_SIZE:
            batches.append(pa.Table.from_pylist(current))
            current = []

    if current:
        batches.append(pa.Table.from_pylist(current))

    return pa.concat_tables(batches)
