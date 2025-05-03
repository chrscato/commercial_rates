"""
Main script for processing healthcare transparency data into relational format.
"""

import json
import logging
from pathlib import Path
from typing import Optional, Dict, List
import uuid
import pandas as pd

from . import format_check
from .scrapers import grouped_by_provider_reference
from .transformers.relational import transform_to_relational, save_relational_tables

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
MANIFEST_PATH = Path("data/staging/in_network_manifest.json")
OUTPUT_DIR = Path("prod/data/processed/relational/")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SCRAPER_MAP = {
    "grouped_by_provider_reference": grouped_by_provider_reference.stream_mrf_to_table
}

def extract_entity_and_plans(manifest_entry: Dict) -> tuple[Dict, List[Dict]]:
    """
    Extract entity and plan information from manifest entry.
    
    Args:
        manifest_entry: Dictionary containing manifest entry data
        
    Returns:
        Tuple of (entity_info, plans_info)
    """
    # Extract entity info
    entity_id = str(uuid.uuid4())
    entity_info = {
        "entity_id": entity_id,
        "reporting_entity_name": manifest_entry.get("reporting_entity", "Unknown"),
        "type": "health_plan",  # Default type
        "last_updated": manifest_entry.get("last_updated", ""),
        "version": "2025" if "2025" in manifest_entry.get("location", "") else "2024"
    }
    
    # Extract plan info
    plans_info = []
    for plan in manifest_entry.get("reporting_plans", []):
        plan_id = str(uuid.uuid4())
        plans_info.append({
            "plan_id": plan_id,
            "plan_name": plan.get("plan_name", "Unknown"),
            "entity_id": entity_id,
            "market_type": plan.get("plan_market_type", "unknown")
        })
    
    # If no plans found, create a default plan
    if not plans_info:
        plan_id = str(uuid.uuid4())
        plans_info.append({
            "plan_id": plan_id,
            "plan_name": Path(manifest_entry["location"]).stem,
            "entity_id": entity_id,
            "market_type": "unknown"
        })
    
    return entity_info, plans_info

def process_url(url: str, manifest_entry: Optional[Dict] = None) -> None:
    """
    Process a single URL into relational format.
    
    Args:
        url: URL to process
        manifest_entry: Optional manifest entry with additional metadata
    """
    try:
        # Detect format
        format_style = format_check.detect_format_from_url(url)
        logger.info(f"Processing URL: {url}")
        logger.info(f"Detected format: {format_style}")

        # Get appropriate scraper
        scraper = SCRAPER_MAP.get(format_style)
        if not scraper:
            logger.error(f"No scraper registered for format: {format_style}")
            return

        # Scrape data
        table = scraper(url)
        
        # Extract entity and plan info
        if manifest_entry:
            entity_info, plans_info = extract_entity_and_plans(manifest_entry)
        else:
            # Create default entity and plan if no manifest entry
            entity_id = str(uuid.uuid4())
            entity_info = {
                "entity_id": entity_id,
                "reporting_entity_name": Path(url).stem,
                "type": "health_plan",
                "last_updated": "",
                "version": "2025" if "2025" in url else "2024"
            }
            plan_id = str(uuid.uuid4())
            plans_info = [{
                "plan_id": plan_id,
                "plan_name": Path(url).stem,
                "entity_id": entity_id,
                "market_type": "unknown"
            }]
        
        # Transform to relational format
        tables = transform_to_relational(table, url, entity_info["reporting_entity_name"])
        
        # Add entity and plan info to tables
        tables["reporting_entities"] = pd.DataFrame([entity_info])
        tables["reporting_plans"] = pd.DataFrame(plans_info)
        
        # Save tables
        file_prefix = Path(url).stem
        save_relational_tables(tables, str(OUTPUT_DIR), file_prefix)
        
        logger.info(f"Successfully processed {url}")
        
    except Exception as e:
        logger.error(f"Failed to process {url}: {e}")
        raise

def main():
    """
    Main entry point for processing data into relational format.
    """
    try:
        # Read manifest if it exists
        if MANIFEST_PATH.exists():
            with open(MANIFEST_PATH) as f:
                manifest = json.load(f)
                
            for entry in manifest:
                url = entry["location"]
                process_url(url, entry)
                
        else:
            logger.warning(f"Manifest file not found: {MANIFEST_PATH}")
            logger.info("Please provide a URL to process")
            
    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        raise

if __name__ == "__main__":
    main() 