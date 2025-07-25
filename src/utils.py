import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tronscan_data.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_address(address: str) -> bool:
    """Validate Tron address format"""
    return isinstance(address, str) and len(address) == 34 and address.startswith('T')

def validate_date_format(date_str: str) -> bool:
    """Validate date format (YYYY-MM-DD)"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def convert_timestamp_to_date(timestamp: int) -> str:
    """Convert timestamp to readable date"""
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

def rate_limit_delay():
    """Apply rate limiting delay"""
    delay = float(os.getenv('API_RATE_LIMIT_DELAY', 1))
    time.sleep(delay)

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def get_env_variable(var_name: str, default: str = None) -> str:
    """Get environment variable with error handling"""
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(f"Environment variable {var_name} is required")
    return value