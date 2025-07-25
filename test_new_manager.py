import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from src.sheets_manager import GoogleSheetsManager
from src.utils import setup_logging

logger = setup_logging()

def test_manager():
    try:
        sheets_manager = GoogleSheetsManager()
        
        # List all worksheets
        print("=== All Worksheets ===")
        worksheets = sheets_manager.list_all_worksheets()
        for ws in worksheets:
            print(f"- {ws['title']} ({ws['row_count']} rows, {ws['col_count']} cols)")
        
        print("\n=== WALLET_LIST Info ===")
        info = sheets_manager.get_worksheet_info("WALLET_LIST")
        print(f"Headers: {info['headers']}")
        
        print("\n=== Reading Addresses from Column C ===")
        addresses = sheets_manager.read_addresses_from_sheet("WALLET_LIST", "C")
        print(f"Found {len(addresses)} valid addresses")
        
        # Show first 5 addresses
        for i, addr in enumerate(addresses[:5], 1):
            print(f"{i}. {addr}")
        
        if len(addresses) > 5:
            print(f"... and {len(addresses) - 5} more")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_manager()
