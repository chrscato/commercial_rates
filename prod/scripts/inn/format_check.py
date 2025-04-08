# prod/inn/format_check.py

import requests, gzip, ijson
from io import BytesIO, TextIOWrapper

def smart_open(source: str):
    r = requests.get(source, stream=True)
    print(f"📥 Status: {r.status_code}")
    print(f"🔎 First 300 bytes:\n{r.content[:300].decode(errors='ignore')}")

    if b'<html' in r.content[:300].lower():
        raise ValueError("URL returned HTML instead of JSON")

    content_stream = BytesIO(r.content)
    if content_stream.read(2) == b'\x1f\x8b':  # GZIP magic
        content_stream.seek(0)
        return gzip.open(content_stream, 'rt', encoding='utf-8')
    else:
        content_stream.seek(0)
        return TextIOWrapper(content_stream, encoding='utf-8')


def detect_format_from_url(url: str) -> str:
    try:
        f = smart_open(url)
        parser = ijson.parse(f)

        for prefix, event, value in parser:
            # Look for telltale grouped-provider structure
            if prefix.startswith("provider_references.item.provider_groups.item.npi.item") and event == "string":
                return "grouped_by_provider_reference"
            if prefix.startswith("in_network.item.negotiated_rates.item.provider_references") and event == "start_array":
                return "grouped_by_provider_reference"

            # Bail early for efficiency
            if prefix.startswith("in_network.item") and event == "end_map":
                break

    except Exception as e:
        print(f"⚠️ Format detection failed: {e}")
    return "unknown"


