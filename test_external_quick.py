#!/usr/bin/env python3
"""
Quick test for external form sheet - hardcoded values
"""

import sys
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import setup_logging, get_env_variable

logger = setup_logging()

def quick_test():
    """Quick test with hardcoded values"""
    
    # Hardcoded values
    SHEET_ID = "1Ve0YiSdoKDcdjxUkmPnbfTKaRQOFZTRHOLO3C1TlydA"
    WORKSHEET_NAME = "Submitted Crypto Expenses/Transfer"
    
    logger.info("🔍 Quick test of external form sheet")
    logger.info("="*60)
    logger.info(f"Sheet ID: {SHEET_ID}")
    logger.info(f"Worksheet: {WORKSHEET_NAME}")
    logger.info("="*60)
    
    try:
        # Authenticate
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials_path = get_env_variable('GOOGLE_SHEETS_CREDENTIALS_PATH', 'config/credentials.json')
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(credentials)
        
        logger.info("✅ Authentication successful")
        
        # Open the sheet
        logger.info("🔍 Opening external sheet...")
        workbook = client.open_by_key(SHEET_ID)
        logger.info(f"✅ Successfully opened: {workbook.title}")
        
        # List all worksheets
        worksheets = workbook.worksheets()
        logger.info(f"\n📋 Available worksheets ({len(worksheets)}):")
        for i, ws in enumerate(worksheets, 1):
            logger.info(f"  {i}. '{ws.title}' ({ws.row_count} rows, {ws.col_count} cols)")
        
        # Try to access the specific worksheet
        logger.info(f"\n📖 Accessing worksheet: '{WORKSHEET_NAME}'")
        try:
            worksheet = workbook.worksheet(WORKSHEET_NAME)
            logger.info(f"✅ Successfully accessed worksheet")
        except Exception as e:
            logger.error(f"❌ Failed to access worksheet '{WORKSHEET_NAME}': {e}")
            logger.info("Available worksheet names:")
            for ws in worksheets:
                logger.info(f"  - '{ws.title}'")
            return
        
        # Get data
        logger.info("\n📊 Reading data...")
        all_values = worksheet.get_all_values()
        
        if not all_values:
            logger.warning("⚠️ No data found in worksheet")
            return
        
        # Show headers
        headers = all_values[0]
        logger.info(f"\n📋 Headers ({len(headers)} columns):")
        for i, header in enumerate(headers):
            logger.info(f"  {i+1:2d}. '{header}'")
        
        # Show sample data
        sample_rows = all_values[1:6]  # First 5 data rows
        logger.info(f"\n📊 Sample data ({len(sample_rows)} rows):")
        
        for row_idx, row in enumerate(sample_rows, 1):
            logger.info(f"\n--- Row {row_idx} ---")
            for col_idx, value in enumerate(row):
                if col_idx < len(headers):
                    # Only show non-empty values or first few columns
                    if value.strip() or col_idx < 5:
                        logger.info(f"  {headers[col_idx]}: '{value}'")
        
        # Summary
        total_rows = len(all_values) - 1  # Exclude header
        logger.info(f"\n📈 Summary:")
        logger.info(f"  Total rows: {total_rows}")
        logger.info(f"  Total columns: {len(headers)}")
        
        # Look for key columns
        logger.info(f"\n🔍 Looking for key columns:")
        
        key_terms = {
            'hash': ['hash', 'transaction', 'trx', 'tx'],
            'amount': ['amount', 'usdt', 'value', 'total'],
            'wallet': ['wallet', 'address', 'from', 'to'],
            'date': ['date', 'time', 'timestamp', 'created']
        }
        
        found_columns = {}
        for category, terms in key_terms.items():
            for i, header in enumerate(headers):
                header_lower = header.lower()
                for term in terms:
                    if term in header_lower:
                        found_columns[category] = (i, header)
                        logger.info(f"  {category.upper()}: Column {i+1} - '{header}'")
                        break
                if category in found_columns:
                    break
        
        logger.info("\n🎉 External sheet access successful!")
        logger.info("="*60)
        logger.info("📋 Next: Create sync script based on these column mappings")
        
    except Exception as e:
        logger.error(f"❌ Failed to access external sheet: {e}")
        logger.error("Possible issues:")
        logger.error("1. Service account not shared with the sheet")
        logger.error("2. Sheet ID is incorrect") 
        logger.error("3. Worksheet name is incorrect")

if __name__ == "__main__":
    quick_test()