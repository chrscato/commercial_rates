import json
import pyarrow as pa
import duckdb
import gzip
from pathlib import Path
from tqdm import tqdm
from scripts.download_sample import download_sample_mrf


CPT_CODES = {"99213", "73560", "20610"}  # Sample CPTs
LOCAL_MRF_PATH = Path("data/raw/sample_mrf.json.gz")
OUTPUT_CSV = Path("data/processed/output_cpt_rates.csv")

def parse_and_extract_mrf():
    print("Parsing MRF and extracting CPT codes...")
    records = []
    with gzip.open(LOCAL_MRF_PATH, 'rt', encoding='utf-8') as f:
        mrf = json.load(f)

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
    print("Running DuckDB query and saving output...")
    con = duckdb.connect()
    con.register("cpt_table", table)
    result = con.execute("""
        SELECT cpt, npi, tin, pos, negotiated_rate
        FROM cpt_table
        WHERE negotiated_rate IS NOT NULL
    """).fetchdf()

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved output to {OUTPUT_CSV}")

if __name__ == "__main__":
    if not LOCAL_MRF_PATH.exists():
        from scripts.download_sample import download_sample_mrf
        download_sample_mrf()
    arrow_table = parse_and_extract_mrf()
    query_and_save(arrow_table)
