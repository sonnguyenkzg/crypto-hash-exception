#!/usr/bin/env python3
"""
Exception Analysis Script - Compare MS_FORM vs TRONSCAN tabs
Usage: python scripts/exception_analysis.py
"""

import sys
import argparse
from pathlib import Path
from typing import List, Dict, Set, Tuple
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.sheets_manager import GoogleSheetsManager
from src.utils import setup_logging

logger = setup_logging()

class ExceptionAnalyzer:
    """Analyze exceptions between MS_FORM and TRONSCAN data"""
    
    def __init__(self, sheets_manager: GoogleSheetsManager):
        self.sheets_manager = sheets_manager
        self.tolerance = 0.01  # Amount tolerance for matching (1 cent)
    
    def read_ms_form_data(self, sheet_name: str = "MS_FORM") -> Dict[str, Dict]:
        """Read MS_FORM data and return dict keyed by TrxHash"""
        try:
            logger.info(f"📖 Reading data from {sheet_name}...")
            
            worksheet = self.sheets_manager.workbook.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning(f"No data found in {sheet_name}")
                return {}
            
            headers = [h.strip().lower() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find required columns
            hash_col = None
            amount_col = None
            
            for i, header in enumerate(headers):
                if 'trxhash' in header or 'hash' in header:
                    hash_col = i
                if 'amount' in header or 'usdt' in header:
                    amount_col = i
            
            if hash_col is None:
                logger.error(f"Could not find TrxHash column in {sheet_name}")
                logger.info(f"Available headers: {headers}")
                return {}
            
            ms_form_data = {}
            
            for row_idx, row in enumerate(data_rows, 2):  # Start from row 2 (after header)
                if len(row) > hash_col and row[hash_col].strip():
                    tx_hash = row[hash_col].strip()
                    
                    # Extract amount if available
                    amount = 0.0
                    if amount_col is not None and len(row) > amount_col:
                        try:
                            amount_str = row[amount_col].strip().replace(',', '').replace('$', '')
                            amount = float(amount_str) if amount_str else 0.0
                        except:
                            amount = 0.0
                    
                    ms_form_data[tx_hash] = {
                        'hash': tx_hash,
                        'amount': amount,
                        'row_number': row_idx,
                        'raw_row': row
                    }
            
            logger.info(f"✅ Found {len(ms_form_data)} records in {sheet_name}")
            return ms_form_data
            
        except Exception as e:
            logger.error(f"Failed to read {sheet_name}: {e}")
            return {}
    
    def read_tronscan_data(self, sheet_name: str = "TRONSCAN") -> Dict[str, Dict]:
        """Read TRONSCAN data and return dict keyed by HASH"""
        try:
            logger.info(f"📖 Reading data from {sheet_name}...")
            
            worksheet = self.sheets_manager.workbook.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning(f"No data found in {sheet_name}")
                return {}
            
            headers = [h.strip().lower() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find required columns (HASH, WALLET, AMT)
            hash_col = None
            wallet_col = None
            amt_col = None
            
            for i, header in enumerate(headers):
                if header == 'hash':
                    hash_col = i
                elif header == 'wallet':
                    wallet_col = i
                elif header == 'amt':
                    amt_col = i
            
            if hash_col is None or amt_col is None:
                logger.error(f"Could not find required columns in {sheet_name}")
                logger.info(f"Available headers: {headers}")
                logger.info(f"Expected: HASH, WALLET, AMT")
                return {}
            
            tronscan_data = {}
            
            for row_idx, row in enumerate(data_rows, 2):  # Start from row 2
                if len(row) > hash_col and row[hash_col].strip():
                    tx_hash = row[hash_col].strip()
                    
                    # Skip total row
                    if tx_hash.upper() == 'TOTAL':
                        continue
                    
                    # Extract wallet and amount
                    wallet = row[wallet_col].strip() if len(row) > wallet_col else ''
                    
                    amount = 0.0
                    if len(row) > amt_col:
                        try:
                            amount_str = row[amt_col].strip().replace(',', '')
                            amount = float(amount_str) if amount_str else 0.0
                        except:
                            amount = 0.0
                    
                    tronscan_data[tx_hash] = {
                        'hash': tx_hash,
                        'wallet': wallet,
                        'amount': amount,
                        'row_number': row_idx,
                        'raw_row': row
                    }
            
            logger.info(f"✅ Found {len(tronscan_data)} records in {sheet_name}")
            return tronscan_data
            
        except Exception as e:
            logger.error(f"Failed to read {sheet_name}: {e}")
            return {}
    
    def analyze_exceptions(self, ms_form_data: Dict, tronscan_data: Dict) -> List[Dict]:
        """Analyze and categorize all exceptions"""
        exceptions = []
        
        # Get all unique hashes
        all_hashes = set(ms_form_data.keys()) | set(tronscan_data.keys())
        
        for tx_hash in all_hashes:
            in_ms_form = tx_hash in ms_form_data
            in_tronscan = tx_hash in tronscan_data
            
            if in_ms_form and in_tronscan:
                # Both exist - check amounts
                ms_amount = ms_form_data[tx_hash]['amount']
                ts_amount = tronscan_data[tx_hash]['amount']
                
                if abs(ms_amount - ts_amount) <= self.tolerance:
                    # Amounts match
                    exception = {
                        'hash': tx_hash,
                        'exception_type': 'MATCHED',
                        'ms_form_amount': ms_amount,
                        'tronscan_amount': ts_amount,
                        'difference': 0.0,
                        'tronscan_wallet': tronscan_data[tx_hash]['wallet'],
                        'ms_form_row': ms_form_data[tx_hash]['row_number'],
                        'tronscan_row': tronscan_data[tx_hash]['row_number']
                    }
                else:
                    # Amounts differ
                    exception = {
                        'hash': tx_hash,
                        'exception_type': 'AMOUNT_DIFFERENT',
                        'ms_form_amount': ms_amount,
                        'tronscan_amount': ts_amount,
                        'difference': abs(ms_amount - ts_amount),
                        'tronscan_wallet': tronscan_data[tx_hash]['wallet'],
                        'ms_form_row': ms_form_data[tx_hash]['row_number'],
                        'tronscan_row': tronscan_data[tx_hash]['row_number']
                    }
                
            elif in_ms_form and not in_tronscan:
                # Only in MS_FORM
                exception = {
                    'hash': tx_hash,
                    'exception_type': 'IN_FORM_NOT_TRONSCAN',
                    'ms_form_amount': ms_form_data[tx_hash]['amount'],
                    'tronscan_amount': 0.0,
                    'difference': ms_form_data[tx_hash]['amount'],
                    'tronscan_wallet': '',
                    'ms_form_row': ms_form_data[tx_hash]['row_number'],
                    'tronscan_row': 'N/A'
                }
                
            elif not in_ms_form and in_tronscan:
                # Only in TRONSCAN
                exception = {
                    'hash': tx_hash,
                    'exception_type': 'IN_TRONSCAN_NOT_FORM',
                    'ms_form_amount': 0.0,
                    'tronscan_amount': tronscan_data[tx_hash]['amount'],
                    'difference': tronscan_data[tx_hash]['amount'],
                    'tronscan_wallet': tronscan_data[tx_hash]['wallet'],
                    'ms_form_row': 'N/A',
                    'tronscan_row': tronscan_data[tx_hash]['row_number']
                }
            
            exceptions.append(exception)
        
        return exceptions
    
    def write_exceptions_to_sheet(self, exceptions: List[Dict], sheet_name: str = "EXCEPTION"):
        """Write exception analysis to Google Sheet"""
        try:
            # Try to get existing worksheet or create new one
            try:
                worksheet = self.sheets_manager.workbook.worksheet(sheet_name)
                logger.info(f"Using existing worksheet: {sheet_name}")
            except:
                worksheet = self.sheets_manager.workbook.add_worksheet(title=sheet_name, rows=1000, cols=15)
                logger.info(f"Created new worksheet: {sheet_name}")
            
            # Clear existing data
            worksheet.clear()
            
            # Write headers
            headers = [
                'HASH',
                'EXCEPTION_TYPE', 
                'MS_FORM_AMOUNT',
                'TRONSCAN_AMOUNT',
                'DIFFERENCE',
                'TRONSCAN_WALLET',
                'MS_FORM_ROW',
                'TRONSCAN_ROW',
                'SEVERITY',
                'NOTES'
            ]
            worksheet.append_row(headers)
            
            # Sort exceptions by type for better organization
            type_order = {
                'AMOUNT_DIFFERENT': 1,
                'IN_FORM_NOT_TRONSCAN': 2, 
                'IN_TRONSCAN_NOT_FORM': 3,
                'MATCHED': 4
            }
            
            exceptions.sort(key=lambda x: (type_order.get(x['exception_type'], 5), -x['difference']))
            
            # Write exception data
            rows_to_write = []
            for exc in exceptions:
                # Determine severity
                severity = self._get_severity(exc)
                notes = self._get_notes(exc)
                
                row = [
                    exc['hash'],
                    exc['exception_type'],
                    exc['ms_form_amount'],
                    exc['tronscan_amount'],
                    exc['difference'],
                    exc['tronscan_wallet'],
                    exc['ms_form_row'],
                    exc['tronscan_row'],
                    severity,
                    notes
                ]
                rows_to_write.append(row)
            
            # Write in batches
            batch_size = 100
            for i in range(0, len(rows_to_write), batch_size):
                batch = rows_to_write[i:i+batch_size]
                worksheet.append_rows(batch)
                logger.info(f"Written batch {i//batch_size + 1}")
            
            # Add summary at the bottom
            summary_rows = [
                [''], ['=== SUMMARY ==='], ['']
            ]
            
            # Count by exception type
            type_counts = {}
            total_diff = 0
            for exc in exceptions:
                exc_type = exc['exception_type']
                type_counts[exc_type] = type_counts.get(exc_type, 0) + 1
                if exc_type != 'MATCHED':
                    total_diff += exc['difference']
            
            for exc_type, count in type_counts.items():
                summary_rows.append([exc_type, count])
            
            summary_rows.extend([
                [''],
                ['TOTAL_DIFFERENCE_AMOUNT', total_diff],
                ['ANALYSIS_TIMESTAMP', str(datetime.now())]
            ])
            
            worksheet.append_rows(summary_rows)
            
            logger.info(f"✅ Exception analysis written to {sheet_name}")
            return type_counts
            
        except Exception as e:
            logger.error(f"Failed to write exceptions to {sheet_name}: {e}")
            raise
    
    def _get_severity(self, exception: Dict) -> str:
        """Determine severity level of exception"""
        exc_type = exception['exception_type']
        difference = exception['difference']
        
        if exc_type == 'MATCHED':
            return 'OK'
        elif exc_type == 'AMOUNT_DIFFERENT':
            if difference < 1:
                return 'LOW'
            elif difference < 100:
                return 'MEDIUM'
            else:
                return 'HIGH'
        else:  # Missing transactions
            if difference < 100:
                return 'MEDIUM'
            else:
                return 'HIGH'
    
    def _get_notes(self, exception: Dict) -> str:
        """Generate notes for exception"""
        exc_type = exception['exception_type']
        
        if exc_type == 'MATCHED':
            return 'Perfect match'
        elif exc_type == 'AMOUNT_DIFFERENT':
            return f'Amount differs by ${exception["difference"]:.2f}'
        elif exc_type == 'IN_FORM_NOT_TRONSCAN':
            return 'Transaction recorded in form but not found in blockchain'
        elif exc_type == 'IN_TRONSCAN_NOT_FORM':
            return 'Blockchain transaction not recorded in form'
        else:
            return ''

def main():
    parser = argparse.ArgumentParser(description='Exception Analysis: Compare MS_FORM vs TRONSCAN')
    parser.add_argument('--ms_form_sheet', default='MS_FORM', help='MS Form sheet name')
    parser.add_argument('--tronscan_sheet', default='TRONSCAN', help='TronScan sheet name')
    parser.add_argument('--exception_sheet', default='EXCEPTION', help='Exception output sheet name')
    parser.add_argument('--tolerance', type=float, default=0.01, help='Amount tolerance for matching')
    
    args = parser.parse_args()
    
    try:
        # Initialize Google Sheets manager
        logger.info("🔄 Initializing Google Sheets manager...")
        sheets_manager = GoogleSheetsManager()
        
        # Initialize analyzer
        analyzer = ExceptionAnalyzer(sheets_manager)
        analyzer.tolerance = args.tolerance
        
        logger.info("🔍 Starting exception analysis...")
        logger.info("="*60)
        
        # Read data from both sheets
        ms_form_data = analyzer.read_ms_form_data(args.ms_form_sheet)
        tronscan_data = analyzer.read_tronscan_data(args.tronscan_sheet)
        
        if not ms_form_data and not tronscan_data:
            logger.error("❌ No data found in either sheet")
            sys.exit(1)
        
        # Analyze exceptions
        logger.info("🔍 Analyzing exceptions...")
        exceptions = analyzer.analyze_exceptions(ms_form_data, tronscan_data)
        
        # Write results to sheet
        logger.info(f"💾 Writing {len(exceptions)} exceptions to {args.exception_sheet}...")
        type_counts = analyzer.write_exceptions_to_sheet(exceptions, args.exception_sheet)
        
        # Final summary
        logger.info("="*60)
        logger.info("🎉 Exception analysis completed!")
        logger.info("📊 Summary:")
        logger.info(f"  📝 MS_FORM records: {len(ms_form_data)}")
        logger.info(f"  🔗 TRONSCAN records: {len(tronscan_data)}")
        logger.info(f"  ⚠️  Total exceptions: {len(exceptions)}")
        
        for exc_type, count in type_counts.items():
            logger.info(f"    - {exc_type}: {count}")
        
        logger.info(f"  📋 Output sheet: {args.exception_sheet}")
        
        # Calculate match rate
        matched_count = type_counts.get('MATCHED', 0)
        total_unique = len(set(ms_form_data.keys()) | set(tronscan_data.keys()))
        match_rate = (matched_count / total_unique * 100) if total_unique > 0 else 0
        
        logger.info(f"  ✅ Match rate: {match_rate:.1f}%")
        
    except Exception as e:
        logger.error(f"❌ Exception analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()