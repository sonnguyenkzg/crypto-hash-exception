import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from src.sheets_manager import GoogleSheetsManager
from src.utils import setup_logging

logger = setup_logging()

def test_read_addresses():
    try:
        sheets_manager = GoogleSheetsManager()
        
        # Test reading from column C (Address column)
        addresses = sheets_manager.read_addresses_from_sheet("WALLET_LIST", "C")
        
        print(f"Found {len(addresses)} valid Tron addresses:")
        for i, addr in enumerate(addresses[:10], 1):  # Show first 10
            print(f"{i:2d}. {addr}")
        
        if len(addresses) > 10:
            print(f"... and {len(addresses) - 10} more addresses")
        
        # Validate a few addresses
        from src.utils import validate_address
        valid_count = sum(1 for addr in addresses if validate_address(addr))
        print(f"\nValidation: {valid_count}/{len(addresses)} addresses are valid Tron format")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_read_addresses()
