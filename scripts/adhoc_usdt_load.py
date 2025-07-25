#!/usr/bin/env python3
"""
Ad-hoc USDT load script for specific address and date range
Usage: python scripts/adhoc_usdt_load.py --address TXXXxxx --date_from 2025-07-01 --date_to 2025-07-24
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
    parser = argparse.ArgumentParser(description='Ad-hoc USDT transaction retrieval for specific address')
    parser.add_argument('--address', required=True, help='Tron address to query')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--target_sheet', default='TRONSCAN', help='Target sheet name')
    parser.add_argument('--append', action='store_true', help='Append to existing data instead of overwriting')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not validate_address(args.address):
        logger.error("❌ Invalid Tron address format")
        logger.error("Tron addresses should start with 'T' and be 34 characters long")
        sys.exit(1)
    
    if not validate_date_format(args.date_from) or not validate_date_format(args.date_to):
        logger.error("❌ Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    if datetime.strptime(args.date_from, '%Y-%m-%d') >= datetime.strptime(args.date_to, '%Y-%m-%d'):
        logger.error("❌ date_from must be earlier than date_to")
        sys.exit(1)
    
    try:
        # Initialize managers
        logger.info("🔄 Initializing managers...")
        sheets_manager = GoogleSheetsManager()
        tronscan_api = TronScanAPI()
        
        # Fetch USDT transactions for the address
        logger.info(f"🔍 Fetching USDT transactions for {args.address}")
        logger.info(f"📅 Date range: {args.date_from} to {args.date_to}")
        logger.info("="*60)
        
        usdt_transactions = tronscan_api.get_usdt_for_single_address(
            args.address, args.date_from, args.date_to
        )
        
        logger.info("="*60)
        
        if not usdt_transactions:
            logger.warning("⚠️  No USDT transactions found for the specified criteria")
            logger.info("This could mean:")
            logger.info("  1. No USDT transactions occurred in this period")
            logger.info("  2. The date range is in the future")
            logger.info("  3. All transaction amounts were 0")
            return
        
        # Write or append USDT transactions to sheet
        if args.append:
            logger.info(f"➕ Appending {len(usdt_transactions)} USDT transactions to {args.target_sheet}...")
            # For append mode, we'll use a simple approach
            try:
                worksheet = sheets_manager.workbook.worksheet(args.target_sheet)
                
                # Prepare data rows
                rows_to_append = []
                for tx in usdt_transactions:
                    row = [
                        tx.get('hash', ''),
                        tx.get('wallet', ''),
                        float(tx.get('amt_usdt', 0))
                    ]
                    rows_to_append.append(row)
                
                # Append rows
                worksheet.append_rows(rows_to_append)
                logger.info(f"✅ Appended {len(rows_to_append)} USDT transactions")
                
            except Exception as e:
                logger.error(f"❌ Failed to append: {e}")
                logger.info("💡 Creating new sheet instead...")
                sheets_manager.write_usdt_transactions_to_sheet(usdt_transactions, args.target_sheet)
        else:
            logger.info(f"💾 Writing {len(usdt_transactions)} USDT transactions to {args.target_sheet}...")
            sheets_manager.write_usdt_transactions_to_sheet(usdt_transactions, args.target_sheet)
        
        # Final summary
        total_usdt = sum(tx['amt_usdt'] for tx in usdt_transactions)
        
        logger.info("🎉 Ad-hoc USDT load completed successfully!")
        logger.info("📊 Summary:")
        logger.info(f"  🏦 Address: {args.address}")
        logger.info(f"  📅 Date range: {args.date_from} to {args.date_to}")
        logger.info(f"  💰 USDT transactions: {len(usdt_transactions)}")
        logger.info(f"  💵 Total USDT value: ${total_usdt:,.2f}")
        logger.info(f"  📝 Output sheet: {args.target_sheet}")
        logger.info(f"  🔄 Mode: {'Append' if args.append else 'Overwrite'}")
        
    except Exception as e:
        logger.error(f"❌ Ad-hoc USDT load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()