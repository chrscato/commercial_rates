import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import json
from pathlib import Path
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_mrf_file(file_path: str) -> Dict:
    """Parse a Machine Readable File (MRF) and extract CPT codes and rates."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract relevant information
        # This is a placeholder - adjust based on actual MRF structure
        rates = []
        for item in data.get('in_network', []):
            if 'billing_code' in item and 'negotiated_rates' in item:
                rates.append({
                    'cpt_code': item['billing_code'],
                    'rate': item['negotiated_rates'][0]['negotiated_rate']
                })
        
        return rates
    except Exception as e:
        logger.error(f"Error parsing MRF file: {e}")
        raise

def save_to_parquet(data: List[Dict], output_path: str):
    """Save the extracted data to a Parquet file."""
    try:
        # Convert to PyArrow Table
        table = pa.Table.from_pylist(data)
        
        # Save as Parquet
        pq.write_table(table, output_path)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving to Parquet: {e}")
        raise

def main():
    # Example usage
    input_file = Path("data/raw/sample_mrf.json")
    output_file = Path("data/processed/rates.parquet")
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    # Parse MRF
    rates = parse_mrf_file(str(input_file))
    
    # Save results
    save_to_parquet(rates, str(output_file))

if __name__ == "__main__":
    main() 