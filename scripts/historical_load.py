#!/usr/bin/env python3
"""
Improved Historical load script with better error handling and one-by-one processing
Usage: python scripts/historical_load.py --date_from 2025-07-01 --date_to 2025-07-24
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path
import time

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.sheets_manager import GoogleSheetsManager
from src.tronscan_api import TronScanAPI
from src.utils import setup_logging, validate_date_format, validate_address

logger = setup_logging()

def process_single_address(tronscan_api, address, date_from, date_to, address_index, total_addresses):
    """Process a single address and return transactions"""
    logger.info(f"Processing address {address_index}/{total_addresses}: {address}")
    
    # Validate address format first
    if not validate_address(address):
        logger.error(f"Invalid Tron address format: {address}")
        logger.error(f"Tron addresses should start with 'T' and be 34 characters long")
        return []
    
    try:
        # Convert dates to timestamps
        start_timestamp = int(datetime.strptime(date_from, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int((datetime.strptime(date_to, '%Y-%m-%d')).timestamp() * 1000) + 86400000  # Add 1 day
        
        transactions = tronscan_api.get_account_transactions(
            address, start_timestamp, end_timestamp
        )
        
        logger.info(f"✅ Retrieved {len(transactions)} transactions for {address}")
        return transactions
        
    except Exception as e:
        logger.error(f"❌ Failed to get transactions for {address}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description='Historical load of TronScan transactions (one by one)')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--source_sheet', default='WALLET_LIST', help='Source sheet name for addresses')
    parser.add_argument('--target_sheet', default='TRONSCAN', help='Target sheet name for transactions')
    parser.add_argument('--start_from', type=int, default=1, help='Start from address number (for resuming)')
    parser.add_argument('--process_count', type=int, default=None, help='Number of addresses to process (for testing)')
    parser.add_argument('--skip_validation', action='store_true', help='Skip address validation (use with caution)')
    
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
        
        # Read addresses from source sheet (Column C = Address column)
        logger.info(f"Reading addresses from {args.source_sheet} column C (Address)...")
        all_addresses = sheets_manager.read_addresses_from_sheet(args.source_sheet, "C")
        
        if not all_addresses:
            logger.error("No addresses found in source sheet")
            sys.exit(1)
        
        logger.info(f"Found {len(all_addresses)} total addresses")
        
        # Check for invalid addresses first
        valid_addresses = []
        invalid_addresses = []
        
        for addr in all_addresses:
            if args.skip_validation or validate_address(addr):
                valid_addresses.append(addr)
            else:
                invalid_addresses.append(addr)
        
        if invalid_addresses:
            logger.warning(f"Found {len(invalid_addresses)} invalid addresses:")
            for addr in invalid_addresses[:10]:  # Show first 10
                logger.warning(f"  - {addr}")
            if len(invalid_addresses) > 10:
                logger.warning(f"  ... and {len(invalid_addresses) - 10} more")
            
            logger.warning("These look like wallet names/labels, not Tron addresses")
            logger.warning("Tron addresses should start with 'T' and be 34 characters long")
            logger.warning("Please check your WALLET_LIST sheet - it should contain actual Tron addresses")
            
            if not args.skip_validation:
                logger.error("Cannot proceed with invalid addresses. Use --skip_validation to bypass (not recommended)")
                sys.exit(1)
        
        # Determine which addresses to process
        start_index = args.start_from - 1
        addresses_to_process = valid_addresses[start_index:]
        
        if args.process_count:
            addresses_to_process = addresses_to_process[:args.process_count]
        
        logger.info(f"Processing {len(addresses_to_process)} addresses (starting from #{args.start_from})")
        
        # Process addresses one by one
        all_transactions = []
        successful_addresses = 0
        failed_addresses = 0
        
        for i, address in enumerate(addresses_to_process):
            current_address_num = start_index + i + 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing address {current_address_num}/{len(all_addresses)}: {address}")
            logger.info(f"{'='*60}")
            
            try:
                transactions = process_single_address(
                    tronscan_api, address, args.date_from, args.date_to,
                    current_address_num, len(all_addresses)
                )
                
                if transactions:
                    all_transactions.extend(transactions)
                    successful_addresses += 1
                    logger.info(f"✅ Success: {len(transactions)} transactions added")
                else:
                    logger.info(f"ℹ️  No transactions found for this address")
                
            except KeyboardInterrupt:
                logger.info(f"\n🛑 Process interrupted by user")
                logger.info(f"Processed {i} addresses so far")
                logger.info(f"Successful: {successful_addresses}, Failed: {failed_addresses}")
                if all_transactions:
                    logger.info(f"Saving {len(all_transactions)} transactions collected so far...")
                    sheets_manager.write_transactions_to_sheet(all_transactions, args.target_sheet)
                sys.exit(0)
                
            except Exception as e:
                logger.error(f"❌ Error processing address {address}: {e}")
                failed_addresses += 1
                continue
            
            # Show progress summary every 5 addresses
            if (i + 1) % 5 == 0:
                logger.info(f"\n📊 Progress Summary:")
                logger.info(f"   Processed: {i + 1}/{len(addresses_to_process)} addresses")
                logger.info(f"   Successful: {successful_addresses}")
                logger.info(f"   Failed: {failed_addresses}")
                logger.info(f"   Total transactions: {len(all_transactions)}")
                logger.info(f"   Remaining: {len(addresses_to_process) - (i + 1)} addresses")
        
        # Final summary
        logger.info(f"\n🎉 Processing completed!")
        logger.info(f"📊 Final Summary:")
        logger.info(f"   - Total addresses processed: {len(addresses_to_process)}")
        logger.info(f"   - Successful: {successful_addresses}")
        logger.info(f"   - Failed: {failed_addresses}")
        logger.info(f"   - Total transactions found: {len(all_transactions)}")
        logger.info(f"   - Date range: {args.date_from} to {args.date_to}")
        
        if not all_transactions:
            logger.warning("No transactions found for any address in the specified date range")
            logger.warning("This could mean:")
            logger.warning("1. No transactions occurred in this period")
            logger.warning("2. The addresses in your sheet are wallet names, not actual Tron addresses")
            logger.warning("3. The date range is in the future (you specified 2025 dates)")
            return
        
        # Write transactions to target sheet
        logger.info(f"💾 Writing {len(all_transactions)} transactions to {args.target_sheet}...")
        sheets_manager.write_transactions_to_sheet(all_transactions, args.target_sheet)
        
        logger.info("✅ Historical load completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Historical load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()