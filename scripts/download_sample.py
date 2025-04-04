import requests
import json
from pathlib import Path
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_mrf(url: str, output_path: Path) -> Optional[Path]:
    """Download a Machine Readable File (MRF) from a given URL."""
    try:
        logger.info(f"Downloading MRF from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Successfully downloaded to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error downloading MRF: {e}")
        return None

def main():
    # Example usage
    sample_url = "https://example.com/sample.mrf.json"  # Replace with actual URL
    output_file = Path("data/raw/sample_mrf.json")
    
    downloaded_file = download_mrf(sample_url, output_file)
    if downloaded_file:
        logger.info("Download completed successfully")
    else:
        logger.error("Download failed")

if __name__ == "__main__":
    main() 