import json
import pyarrow as pa
import duckdb
import gzip
import requests
from io import BytesIO
from tqdm import tqdm
from pathlib import Path

# Config
CPT_CODES = {"99213", "73560", "20610"}
MRF_URL = "https://mrf.healthsparq.com/bcbsne-egress.nophi.kyruushsq.com/prd/mrf/BCBSNE_I/BCBSNE/2025-04-01/inNetworkRates/2025-04-01_ANTELOPEMEMORIALNETWORK_BCBSNE.json.gz"
OUTPUT_CSV = Path("data/processed/output_cpt_rates.csv")

def stream_and_parse_mrf():
    print(f"📥 Streaming MRF directly from: {MRF_URL}")
    r = requests.get(MRF_URL, stream=True)
    if r.status_code != 200:
        raise Exception(f"❌ Failed to fetch MRF. Status: {r.status_code}")

    # Decompress and load JSON directly into memory
    with gzip.open(BytesIO(r.content), 'rt', encoding='utf-8') as f:
        mrf = json.load(f)

    print("🔍 Parsing MRF and extracting CPT codes...")
    records = []

    # Map provider references
    provider_map = {}
    for provider in tqdm(mrf.get("provider_references", []), desc="Provider refs"):
        ref_id = provider.get("provider_group_id")
        tin = provider.get("tin", {}).get("value", "unknown")
        npis = provider.get("npi", [])
        for npi in npis:
            provider_map[(ref_id, npi)] = tin

    for rate_obj in tqdm(mrf.get("in_network", []), desc="In-network items"):
        code = rate_obj.get("billing_code")
        if code not in CPT_CODES:
            continue

        for item in rate_obj.get("negotiated_rates", []):
            provider_ref = item.get("provider_reference")
            for price in item.get("negotiated_prices", []):
                records.append({
                    "cpt": code,
                    "npi": price.get("provider_npi", "unknown"),
                    "negotiated_rate": price.get("negotiated_rate"),
                    "tin": provider_map.get((provider_ref, price.get("provider_npi")), "unknown"),
                    "pos": price.get("place_of_service", "unknown"),
                })

    table = pa.Table.from_pylist(records)
    return table

def query_and_save(table):
    print("🧠 Running DuckDB query and saving output...")
    con = duckdb.connect()
    con.register("cpt_table", table)
    result = con.execute("""
        SELECT cpt, npi, tin, pos, negotiated_rate
        FROM cpt_table
        WHERE negotiated_rate IS NOT NULL
    """).fetchdf()

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Saved output to {OUTPUT_CSV}")

if __name__ == "__main__":
    table = stream_and_parse_mrf()
    query_and_save(table)
