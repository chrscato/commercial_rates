def extract_urls(toc_json: dict) -> list[dict]:
    """
    Returns a list of dicts with structure:
    {
        'location': str,
        'description': str,
        'plans': List[str],
        'plan_ids': List[str]
    }
    """
    results = []

    for group in toc_json.get("reporting_structure", []):
        in_network_files = group.get("in_network_files", [])
        reporting_plans = group.get("reporting_plans", [])

        plan_names = [plan.get("plan_name") for plan in reporting_plans if plan.get("plan_name")]
        plan_ids = [plan.get("plan_id") for plan in reporting_plans if plan.get("plan_id")]

        for entry in in_network_files:
            location = entry.get("location")
            if location:
                results.append({
                    "location": location,
                    "description": entry.get("description", "N/A"),
                    "plans": plan_names,
                    "plan_ids": plan_ids
                })

    return results
