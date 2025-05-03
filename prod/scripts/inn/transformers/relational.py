"""
Transformers for converting healthcare transparency data into relational format.
"""

import logging
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table

logger = logging.getLogger(__name__)

def extract_entity_info(url: str, entity_name: str) -> Dict:
    """
    Extract entity details from URL and metadata.
    
    Args:
        url: The source URL of the data
        entity_name: Name of the reporting entity
        
    Returns:
        Dict containing entity information
    """
    try:
        # Generate a unique ID for the entity
        entity_id = str(uuid.uuid4())
        
        # Extract version from URL if possible
        version = "unknown"
        if "2025" in url:
            version = "2025"
        elif "2024" in url:
            version = "2024"
            
        return {
            "entity_id": entity_id,
            "reporting_entity_name": entity_name,
            "type": "health_plan",  # Default type
            "last_updated": pd.Timestamp.now().isoformat(),
            "version": version
        }
    except Exception as e:
        logger.error(f"Failed to extract entity info: {e}")
        raise

def transform_to_relational(data: Table, url: str, entity_name: str) -> Dict[str, Table]:
    """
    Convert flat data to 4 relational tables.
    
    Args:
        data: Input PyArrow table with flat data
        url: Source URL
        entity_name: Name of the reporting entity
        
    Returns:
        Dict containing 4 relational tables
    """
    try:
        # Extract entity info
        entity_info = extract_entity_info(url, entity_name)
        entity_id = entity_info["entity_id"]
        
        # Convert to pandas for easier manipulation
        df = data.to_pandas()
        
        # Create reporting_entities table
        reporting_entities = pd.DataFrame([entity_info])
        
        # Create reporting_plans table
        # For now, create a single plan with info from URL
        plan_id = str(uuid.uuid4())
        reporting_plans = pd.DataFrame([{
            "plan_id": plan_id,
            "plan_name": Path(url).stem,
            "entity_id": entity_id,
            "market_type": "unknown"  # Could be extracted from URL/description
        }])
        
        # Create providers table
        providers = df[["npi", "tin"]].drop_duplicates()
        providers["provider_id"] = [str(uuid.uuid4()) for _ in range(len(providers))]
        
        # Create negotiated_rates table
        negotiated_rates = df.merge(
            providers[["npi", "tin", "provider_id"]],
            on=["npi", "tin"]
        )
        negotiated_rates = negotiated_rates.rename(columns={
            "cpt": "cpt_code",
            "pos": "place_of_service"
        })
        negotiated_rates["rate_id"] = [str(uuid.uuid4()) for _ in range(len(negotiated_rates))]
        negotiated_rates["plan_id"] = plan_id
        
        # Convert all to PyArrow tables
        tables = {
            "reporting_entities": pa.Table.from_pandas(reporting_entities),
            "reporting_plans": pa.Table.from_pandas(reporting_plans),
            "providers": pa.Table.from_pandas(providers),
            "negotiated_rates": pa.Table.from_pandas(negotiated_rates)
        }
        
        return tables
        
    except Exception as e:
        logger.error(f"Failed to transform to relational format: {e}")
        raise

def save_relational_tables(tables: Dict[str, Table], output_dir: str, file_prefix: str, format: str = "parquet") -> None:
    """
    Save relational tables to disk.
    
    Args:
        tables: Dict of PyArrow tables to save
        output_dir: Directory to save files
        file_prefix: Prefix for output files
        format: Output format ("parquet" or "csv")
    """
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for table_name, table in tables.items():
            file_path = output_path / f"{file_prefix}_{table_name}"
            
            if format.lower() == "parquet":
                pq.write_table(table, f"{file_path}.parquet")
            elif format.lower() == "csv":
                table.to_pandas().to_csv(f"{file_path}.csv", index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
                
            logger.info(f"Saved {table_name} to {file_path}.{format}")
            
    except Exception as e:
        logger.error(f"Failed to save relational tables: {e}")
        raise 