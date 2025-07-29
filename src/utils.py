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

# Add this function to your existing src/utils.py file

def get_batch_timestamp_as_datetime(batch_id: str) -> str:
    """
    Convert batch ID (YYYYMMDDHHMMSS) back to formatted datetime string
    
    Args:
        batch_id: Batch ID in format YYYYMMDDHHMMSS (e.g., "20250728140530")
    
    Returns:
        Formatted datetime string (e.g., "2025-07-28 14:05:30")
    """
    try:
        from datetime import datetime
        import pytz
        
        # Parse the batch ID timestamp
        dt = datetime.strptime(batch_id, '%Y%m%d%H%M%S')
        
        # Convert to Bangkok timezone (assuming that's what was used originally)
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        dt_bangkok = bangkok_tz.localize(dt)
        
        # Return formatted string
        return dt_bangkok.strftime('%Y-%m-%d %H:%M:%S')
        
    except Exception as e:
        # Fallback to current time if parsing fails
        from datetime import datetime
        import pytz
        return datetime.now(pytz.timezone('Asia/Bangkok')).strftime('%Y-%m-%d %H:%M:%S')