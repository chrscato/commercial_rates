"""
Utilities for extracting information from structure-level in-network files.
"""

from datetime import datetime
from typing import List, Dict

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

def extract_urls(toc_json: dict) -> List[Dict]:
    """
    Extract URLs and associated metadata from a TOC JSON structure.
    
    Args:
        toc_json: Dictionary containing the TOC JSON structure
        
    Returns:
        List of dictionaries containing:
        {
            'location': str,
            'description': str,
            'reporting_entity': str,
            'reporting_entity_type': str,
            'last_updated': str,
            'reporting_plans': List[Dict]
        }
    """
    results = []

    for group in toc_json.get("reporting_structure", []):
        # Extract entity information
        entity_info = {
            "reporting_entity_name": group.get("reporting_entity_name", "Unknown"),
            "reporting_entity_type": group.get("reporting_entity_type", "Unknown"),
            "last_updated": group.get("last_updated", datetime.now().isoformat())
        }
        
        # Extract plan information
        reporting_plans = []
        for plan in group.get("reporting_plans", []):
            reporting_plans.append(extract_plan_info(plan))
        
        # Process in-network files
        for entry in group.get("in_network_files", []):
            location = entry.get("location")
            if location:
                results.append({
                    "location": location,
                    "description": entry.get("description", "N/A"),
                    "reporting_entity": entity_info["reporting_entity_name"],
                    "reporting_entity_type": entity_info["reporting_entity_type"],
                    "last_updated": entity_info["last_updated"],
                    "reporting_plans": reporting_plans
                })

    return results
