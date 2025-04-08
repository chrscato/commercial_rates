import requests
import gzip
import ijson
from io import BytesIO, TextIOWrapper

def smart_open(source: str):
    if source.startswith("http"):
        r = requests.get(source, stream=True)
        content = BytesIO(r.content)
    else:
        with open(source, "rb") as f:
            content = BytesIO(f.read())

    # Check for gzip magic number
    if content.getbuffer().nbytes >= 2 and content.read(2) == b'\x1f\x8b':
        content.seek(0)
        return gzip.open(content, 'rt', encoding='utf-8')
    else:
        content.seek(0)
        return TextIOWrapper(content, encoding='utf-8')

def detect_toc_format(source: str) -> str:
    f = smart_open(source)
    parser = ijson.parse(f)

    found_in_network = False
    found_plan_level = False
    inside_structure = False

    for prefix, event, value in parser:
        if prefix == "reporting_structure" and event == "start_array":
            inside_structure = True
        elif inside_structure and prefix.endswith(".in_network_files") and event == "start_array":
            found_in_network = True
        elif inside_structure and prefix.endswith(".reporting_plans") and event == "start_array":
            found_plan_level = True

        # if we found what we need, break early
        if found_in_network:
            return "structure_level_inn"

    if found_plan_level:
        return "plan_level_inn"

    return "unknown"

