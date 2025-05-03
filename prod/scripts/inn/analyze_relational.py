"""
Script for analyzing the relational data outputs from the healthcare transparency data processing.
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import logging
from typing import Dict, List
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = Path("prod/data/processed/relational/")

def get_base_filename(file_path: Path) -> str:
    """
    Extract the base filename without the table suffix.
    
    Args:
        file_path: Path to the parquet file
        
    Returns:
        Base filename without table suffix
    """
    # Remove the last underscore and everything after it
    return str(file_path.stem).rsplit('_', 1)[0]

def load_tables(file_prefix: str) -> Dict[str, pd.DataFrame]:
    """
    Load all tables for a given file prefix.
    
    Args:
        file_prefix: Prefix of the files to load
        
    Returns:
        Dict of DataFrames for each table
    """
    tables = {}
    for table_name in ["reporting_entities", "reporting_plans", "providers", "negotiated_rates"]:
        file_path = DATA_DIR / f"{file_prefix}_{table_name}.parquet"
        if file_path.exists():
            tables[table_name] = pq.read_table(file_path).to_pandas()
            logger.info(f"Loaded {table_name} with {len(tables[table_name])} rows")
        else:
            logger.warning(f"File not found: {file_path}")
    return tables

def analyze_relationships(tables: Dict[str, pd.DataFrame]) -> None:
    """
    Analyze relationships between tables.
    
    Args:
        tables: Dict of DataFrames containing all tables
    """
    if not all(table in tables for table in ["reporting_entities", "reporting_plans", "providers", "negotiated_rates"]):
        logger.error("Missing required tables for relationship analysis")
        return
        
    entities = tables["reporting_entities"]
    plans = tables["reporting_plans"]
    providers = tables["providers"]
    rates = tables["negotiated_rates"]
    
    print("\nRelationship Analysis:")
    
    # Entity to Plans
    print("\nEntity to Plans:")
    for _, entity in entities.iterrows():
        entity_plans = plans[plans["entity_id"] == entity["entity_id"]]
        print(f"\nEntity: {entity['reporting_entity_name']}")
        print(f"Number of plans: {len(entity_plans)}")
        for _, plan in entity_plans.iterrows():
            print(f"- Plan: {plan['plan_name']} (Market: {plan['market_type']})")
            
            # Plans to Rates
            plan_rates = rates[rates["plan_id"] == plan["plan_id"]]
            print(f"  Number of rates: {len(plan_rates):,}")
            print(f"  Unique providers: {plan_rates['provider_id'].nunique():,}")
            print(f"  Unique CPT codes: {plan_rates['cpt_code'].nunique():,}")
            
            # Average rates by CPT
            if not plan_rates.empty:
                avg_rates = plan_rates.groupby("cpt_code")["negotiated_rate"].agg([
                    'count', 'mean', 'min', 'max'
                ]).round(2)
                print("\n  Average Rates by CPT:")
                print(avg_rates)
    
    # Provider Coverage
    print("\nProvider Coverage:")
    total_providers = len(providers)
    providers_with_rates = rates["provider_id"].nunique()
    print(f"Total providers: {total_providers:,}")
    print(f"Providers with rates: {providers_with_rates:,}")
    print(f"Coverage: {(providers_with_rates/total_providers*100):.1f}%")
    
    # Rates by Entity
    print("\nRates by Entity:")
    for _, entity in entities.iterrows():
        entity_plans = plans[plans["entity_id"] == entity["entity_id"]]
        entity_rates = rates[rates["plan_id"].isin(entity_plans["plan_id"])]
        print(f"\nEntity: {entity['reporting_entity_name']}")
        print(f"Total rates: {len(entity_rates):,}")
        print(f"Average rate: ${entity_rates['negotiated_rate'].mean():.2f}")
        print(f"Rate range: ${entity_rates['negotiated_rate'].min():.2f} - ${entity_rates['negotiated_rate'].max():.2f}")

def analyze_rates(tables: Dict[str, pd.DataFrame]) -> None:
    """
    Analyze negotiated rates data.
    
    Args:
        tables: Dict of DataFrames containing all tables
    """
    if "negotiated_rates" not in tables:
        logger.error("No negotiated rates data found")
        return
        
    rates = tables["negotiated_rates"]
    
    # Basic statistics
    print("\nNegotiated Rates Analysis:")
    print(f"Total number of rates: {len(rates):,}")
    print(f"Unique CPT codes: {rates['cpt_code'].nunique():,}")
    print(f"Unique providers: {rates['provider_id'].nunique():,}")
    
    # Rate statistics by CPT code
    cpt_stats = rates.groupby('cpt_code')['negotiated_rate'].agg([
        'count', 'mean', 'min', 'max', 'std'
    ]).round(2)
    print("\nRate Statistics by CPT Code:")
    print(cpt_stats)
    
    # Distribution of rates
    plt.figure(figsize=(12, 6))
    sns.histplot(data=rates, x='negotiated_rate', bins=50)
    plt.title('Distribution of Negotiated Rates')
    plt.xlabel('Rate ($)')
    plt.ylabel('Count')
    plt.savefig('rate_distribution.png')
    plt.close()
    
    # Rates by place of service
    pos_stats = rates.groupby('place_of_service')['negotiated_rate'].agg([
        'count', 'mean', 'min', 'max'
    ]).round(2)
    print("\nRate Statistics by Place of Service:")
    print(pos_stats)

def analyze_providers(tables: Dict[str, pd.DataFrame]) -> None:
    """
    Analyze provider data.
    
    Args:
        tables: Dict of DataFrames containing all tables
    """
    if "providers" not in tables:
        logger.error("No provider data found")
        return
        
    providers = tables["providers"]
    
    print("\nProvider Analysis:")
    print(f"Total number of providers: {len(providers):,}")
    print(f"Unique NPIs: {providers['npi'].nunique():,}")
    print(f"Unique TINs: {providers['tin'].nunique():,}")
    
    # Providers per TIN
    providers_per_tin = providers.groupby('tin').size()
    print("\nProviders per TIN:")
    print(providers_per_tin.describe().round(2))

def analyze_reporting_entities(tables: Dict[str, pd.DataFrame]) -> None:
    """
    Analyze reporting entity data.
    
    Args:
        tables: Dict of DataFrames containing all tables
    """
    if "reporting_entities" not in tables:
        logger.error("No reporting entity data found")
        return
        
    entities = tables["reporting_entities"]
    
    print("\nReporting Entity Analysis:")
    print("Entities:")
    for _, row in entities.iterrows():
        print(f"- {row['reporting_entity_name']} ({row['type']})")
        print(f"  Version: {row['version']}")
        print(f"  Last Updated: {row['last_updated']}")

def main():
    """
    Main entry point for analysis.
    """
    try:
        # Get all unique file prefixes
        file_prefixes = set()
        for file in DATA_DIR.glob("*.parquet"):
            base_name = get_base_filename(file)
            file_prefixes.add(base_name)
            
        for prefix in sorted(file_prefixes):
            print(f"\n{'='*50}")
            print(f"Analyzing data for: {prefix}")
            print(f"{'='*50}")
            
            tables = load_tables(prefix)
            analyze_reporting_entities(tables)
            analyze_providers(tables)
            analyze_rates(tables)
            analyze_relationships(tables)
            
    except Exception as e:
        logger.error(f"Failed to analyze data: {e}")
        raise

if __name__ == "__main__":
    main() 