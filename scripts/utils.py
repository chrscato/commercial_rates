# Placeholder for future shared functions (e.g., logging, validation)
def clean_tin(tin_value):
    if tin_value and isinstance(tin_value, str):
        return tin_value.strip()
    return "unknown"
