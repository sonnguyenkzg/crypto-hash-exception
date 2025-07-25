#!/usr/bin/env python3
"""
Historical load script to fetch all transactions for addresses from Google Sheet
Usage: python scripts/historical_load.py --date_from 2024-01-01 --date_to 2024-12-31
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.sheets_manager import GoogleSheetsManager
from src.tronscan_api import TronScanAPI
from src.utils import setup_logging, validate_date_format

logger = setup_logging()

def main():
    parser = argparse.ArgumentParser(description='Historical load of TronScan transactions')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--source_sheet', default='Sheet1', help='Source sheet name for addresses')
    parser.add_argument('--target_sheet', default='TRONSCAN', help='Target sheet name for transactions')
    
    args = parser.parse_args()
    
    # Validate dates
    if not validate_date_format(args.date_from) or not validate_date_format(args.date_to):
        logger.error("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    # Validate date range
    if datetime.strptime(args.date_from, '%Y-%m-%d') >= datetime.strptime(args.date_to, '%Y-%m-%d'):
        logger.error("date_from must be earlier than date_to")
        sys.exit(1)
    
    try:
        # Initialize managers
        logger.info("Initializing Google Sheets manager...")
        sheets_manager = GoogleSheetsManager()
        
        logger.info("Initializing TronScan API client...")
        tronscan_api = TronScanAPI()
        
        # Read addresses from source sheet
        logger.info(f"Reading addresses from {args.source_sheet}...")
        addresses = sheets_manager.read_addresses_from_sheet(args.source_sheet)
        
        if not addresses:
            logger.error("No addresses found in source sheet")
            sys.exit(1)
        
        logger.info(f"Found {len(addresses)} addresses")
        
        # Fetch transactions for all addresses
        logger.info(f"Fetching transactions from {args.date_from} to {args.date_to}...")
        transactions = tronscan_api.get_transactions_for_multiple_addresses(
            addresses, args.date_from, args.date_to
        )
        
        if not transactions:
            logger.warning("No transactions found for the specified criteria")
            return
        
        # Write transactions to target sheet
        logger.info(f"Writing {len(transactions)} transactions to {args.target_sheet}...")
        sheets_manager.write_transactions_to_sheet(transactions, args.target_sheet)
        
        logger.info("Historical load completed successfully!")
        logger.info(f"Summary:")
        logger.info(f"  - Addresses processed: {len(addresses)}")
        logger.info(f"  - Transactions found: {len(transactions)}")
        logger.info(f"  - Date range: {args.date_from} to {args.date_to}")
        
    except Exception as e:
        logger.error(f"Historical load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()