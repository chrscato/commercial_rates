import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def ensure_directory(path: str) -> None:
    """Ensure a directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)

def load_json(file_path: str) -> Dict[str, Any]:
    """Load and return JSON data from a file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON from {file_path}: {e}")
        raise

def save_json(data: Dict[str, Any], file_path: str) -> None:
    """Save data to a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving JSON to {file_path}: {e}")
        raise

def get_env_var(key: str, default: Optional[str] = None) -> str:
    """Get environment variable with error handling."""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable {key} is not set")
    return value

def setup_data_directories() -> None:
    """Create necessary data directories if they don't exist."""
    base_dir = Path(__file__).parent.parent
    directories = [
        base_dir / 'data' / 'raw',
        base_dir / 'data' / 'processed',
        base_dir / 'data' / 'index_cache'
    ]
    for directory in directories:
        ensure_directory(str(directory))
        # Create .gitkeep file to preserve empty directories
        (directory / '.gitkeep').touch(exist_ok=True) 