import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from src.tronscan_api import TronScanAPI
from src.utils import setup_logging

logger = setup_logging()

def debug_amounts():
    api = TronScanAPI()
    
    # Test with one of your addresses for recent dates
    address = "TSpAswScHnu6WqJDaZzjWEA4ztPSzPRtPZ"  # From your transaction
    
    print(f"🔍 Debugging amounts for address: {address}")
    print("Looking for recent transactions...")
    
    # Get recent transactions (last 7 days)
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)
    
    try:
        transactions = api.get_account_transactions(address, start_timestamp, end_timestamp, limit=5)
        
        for i, tx in enumerate(transactions[:3], 1):
            print(f"\n--- Transaction {i} ---")
            print(f"Hash: {tx.get('hash', 'N/A')}")
            print(f"Raw amount: {tx.get('amount_raw', 'N/A')}")
            print(f"Formatted: {tx.get('amount_formatted', 'N/A')}")
            print(f"Token: {tx.get('token_symbol', 'N/A')}")
            print(f"USDT value: {tx.get('amount_usdt', 'N/A')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_amounts()
