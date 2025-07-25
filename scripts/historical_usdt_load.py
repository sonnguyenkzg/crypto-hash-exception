#!/usr/bin/env python3
"""
Historical USDT load script - simplified 3-column output
Usage: python scripts/historical_usdt_load.py --date_from 2025-07-01 --date_to 2025-07-24
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
    parser = argparse.ArgumentParser(description='Historical USDT load from WALLET_LIST')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--source_sheet', default='WALLET_LIST', help='Source sheet name for addresses')
    parser.add_argument('--target_sheet', default='TRONSCAN', help='Target sheet name for USDT transactions')
    parser.add_argument('--process_count', type=int, default=None, help='Number of addresses to process (for testing)')
    
    args = parser.parse_args()
    
    # Validate dates
    if not validate_date_format(args.date_from) or not validate_date_format(args.date_to):
        logger.error("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    if datetime.strptime(args.date_from, '%Y-%m-%d') >= datetime.strptime(args.date_to, '%Y-%m-%d'):
        logger.error("date_from must be earlier than date_to")
        sys.exit(1)
    
    try:
        # Initialize managers
        logger.info("🔄 Initializing Google Sheets manager...")
        sheets_manager = GoogleSheetsManager()
        
        logger.info("🔄 Initializing TronScan API client...")
        tronscan_api = TronScanAPI()
        
        # Read addresses from WALLET_LIST (Column C = Address)
        logger.info(f"📖 Reading addresses from {args.source_sheet} column C...")
        addresses = sheets_manager.read_addresses_from_sheet(args.source_sheet, "C")
        
        if not addresses:
            logger.error("❌ No valid addresses found in source sheet")
            sys.exit(1)
        
        # Limit addresses if specified
        if args.process_count:
            addresses = addresses[:args.process_count]
            logger.info(f"🔢 Limited to first {args.process_count} addresses")
        
        logger.info(f"✅ Found {len(addresses)} valid Tron addresses")
        
        # Fetch USDT transactions for all addresses
        logger.info(f"🔍 Fetching USDT transactions from {args.date_from} to {args.date_to}...")
        logger.info("="*60)
        
        usdt_transactions = tronscan_api.get_usdt_for_multiple_addresses(
            addresses, args.date_from, args.date_to
        )
        
        logger.info("="*60)
        
        if not usdt_transactions:
            logger.warning("⚠️  No USDT transactions found for the specified criteria")
            logger.info("This could mean:")
            logger.info("  1. No USDT transactions occurred in this period")
            logger.info("  2. The date range is in the future") 
            logger.info("  3. All transaction amounts were 0")
            return
        
        # Write USDT transactions to target sheet
        logger.info(f"💾 Writing {len(usdt_transactions)} USDT transactions to {args.target_sheet}...")
        sheets_manager.write_usdt_transactions_to_sheet(usdt_transactions, args.target_sheet)
        
        # Final summary
        total_usdt = sum(tx['amt_usdt'] for tx in usdt_transactions)
        unique_wallets = len(set(tx['wallet'] for tx in usdt_transactions))
        
        logger.info("🎉 Historical USDT load completed successfully!")
        logger.info("📊 Summary:")
        logger.info(f"  📅 Date range: {args.date_from} to {args.date_to}")
        logger.info(f"  🏦 Wallets processed: {len(addresses)}")
        logger.info(f"  🏦 Wallets with USDT: {unique_wallets}")
        logger.info(f"  💰 USDT transactions: {len(usdt_transactions)}")
        logger.info(f"  💵 Total USDT value: ${total_usdt:,.2f}")
        logger.info(f"  📝 Output sheet: {args.target_sheet}")
        
    except Exception as e:
        logger.error(f"❌ Historical USDT load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()