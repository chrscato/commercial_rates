import requests
from pathlib import Path

def download_sample_mrf():
    url = "https://transparency-in-coverage.aetna.com/2023-12-01/in-network-rates/000166907_aetna-health-inc--pa--pa--individual-on-exchange_in-network-rates.json.gz"
    local_path = Path("data/raw/sample_mrf.json.gz")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if not local_path.exists():
        print("Downloading sample MRF file...")
        r = requests.get(url, stream=True)
        if r.status_code == 200 and r.headers.get("Content-Type") == "application/gzip":
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("✅ Download complete.")
        else:
            print(f"⚠️ Failed to download: {r.status_code}")
            print(f"Content-Type: {r.headers.get('Content-Type')}")
            print("Response:", r.text[:300])
    else:
        print("Sample MRF already exists.")
