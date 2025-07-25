#!/usr/bin/env python3
"""
Ad-hoc script to fetch transactions for specific address and date range
Usage: python scripts/adhoc_load.py --address TXXXxxx --date_from 2024-01-01 --date_to 2024-01-31
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.sheets_manager import GoogleSheetsManager
from src.tronscan_api import TronScanAPI
from src.utils import setup_logging, validate_date_format, validate_address

logger = setup_logging()

def main():
    parser = argparse.ArgumentParser(description='Ad-hoc TronScan transaction retrieval')
    parser.add_argument('--address', required=True, help='Tron address to query')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--target_sheet', default='TRONSCAN', help='Target sheet name')
    parser.add_argument('--append', action='store_true', help='Append to existing data instead of overwriting')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not validate_address(args.address):
        logger.error("Invalid Tron address format")
        sys.exit(1)
    
    if not validate_date_format(args.date_from) or not validate_date_format(args.date_to):
        logger.error("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    if datetime.strptime(args.date_from, '%Y-%m-%d') >= datetime.strptime(args.date_to, '%Y-%m-%d'):
        logger.error("date_from must be earlier than date_to")
        sys.exit(1)
    
    try:
        # Initialize managers
        logger.info("Initializing managers...")
        sheets_manager = GoogleSheetsManager()
        tronscan_api = TronScanAPI()
        
        # Fetch transactions for the address
        logger.info(f"Fetching transactions for {args.address} from {args.date_from} to {args.date_to}...")
        transactions = tronscan_api.get_transactions_for_multiple_addresses(
            [args.address], args.date_from, args.date_to
        )
        
        if not transactions:
            logger.warning("No transactions found for the specified criteria")
            return
        
        # Write or append transactions to sheet
        if args.append:
            logger.info(f"Appending {len(transactions)} transactions to {args.target_sheet}...")
            sheets_manager.append_transactions_to_sheet(transactions, args.target_sheet)
        else:
            logger.info(f"Writing {len(transactions)} transactions to {args.target_sheet}...")
            sheets_manager.write_transactions_to_sheet(transactions, args.target_sheet)
        
        logger.info("Ad-hoc load completed successfully!")
        logger.info(f"Summary:")
        logger.info(f"  - Address: {args.address}")
        logger.info(f"  - Transactions found: {len(transactions)}")
        logger.info(f"  - Date range: {args.date_from} to {args.date_to}")
        logger.info(f"  - Mode: {'Append' if args.append else 'Overwrite'}")
        
    except Exception as e:
        logger.error(f"Ad-hoc load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()