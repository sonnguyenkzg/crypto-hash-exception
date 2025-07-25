import gspread
import json
import os
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any, Optional
import pandas as pd
from src.utils import setup_logging, get_env_variable

logger = setup_logging()

class GoogleSheetsManager:
    """Manages Google Sheets operations with secure authentication"""
    
    def __init__(self):
        self.sheet_id = get_env_variable('GOOGLE_SHEET_ID')
        self.client = self._authenticate()
        self.workbook = self.client.open_by_key(self.sheet_id)
    
    def _authenticate(self) -> gspread.Client:
        """Authenticate with Google Sheets API using environment variables (secure method)"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            # Method 1: Try environment variable first (most secure)
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                logger.info("Using credentials from environment variable")
                creds_data = json.loads(creds_json)
                credentials = Credentials.from_service_account_info(creds_data, scopes=scope)
                client = gspread.authorize(credentials)
                
                # Test access
                workbook = client.open_by_key(self.sheet_id)
                logger.info(f"Successfully authenticated and opened sheet: {workbook.title}")
                return client
            
            # Method 2: Fallback to file (less secure, only for development)
            creds_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', 'config/credentials.json')
            if os.path.exists(creds_path):
                logger.warning("Using credentials from file - consider using environment variables for production")
                
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                
                if creds_data.get('type') != 'service_account':
                    raise ValueError(f"Expected service_account, got: {creds_data.get('type')}")
                
                logger.info(f"Using service account: {creds_data.get('client_email')}")
                
                credentials = Credentials.from_service_account_file(creds_path, scopes=scope)
                client = gspread.authorize(credentials)
                
                # Test access
                workbook = client.open_by_key(self.sheet_id)
                logger.info(f"Successfully opened sheet: {workbook.title}")
                return client
            
            # No credentials found
            raise ValueError("No credentials found. Set GOOGLE_CREDENTIALS_JSON environment variable or provide credentials file")
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in credentials: {e}")
            raise
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            logger.error("Please check:")
            logger.error("1. GOOGLE_CREDENTIALS_JSON environment variable is set correctly")
            logger.error("2. Service account has access to your sheet")
            logger.error("3. Google Sheets + Drive APIs are enabled")
            logger.error("4. Service account key is not disabled/revoked")
            raise
    
    def read_addresses_from_sheet(self, worksheet_name: str = "Sheet1", address_column: str = "C") -> List[str]:
        """Read addresses from the specified column of worksheet"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            
            # Convert column letter to number (A=1, B=2, C=3, etc.)
            column_num = ord(address_column.upper()) - ord('A') + 1
            
            # Get all values from the specified column
            addresses = worksheet.col_values(column_num)
            
            # Filter out empty cells and header
            addresses = [addr.strip() for addr in addresses if addr.strip()]
            
            # Remove header if present (first row should be "Address")
            if addresses and addresses[0].lower() in ['address', 'wallet address', 'tron address']:
                addresses = addresses[1:]
            
            # Filter only valid Tron addresses (start with T and 34 chars)
            valid_addresses = []
            invalid_addresses = []
            
            for addr in addresses:
                if addr.startswith('T') and len(addr) == 34:
                    valid_addresses.append(addr)
                else:
                    invalid_addresses.append(addr)
                    logger.warning(f"Skipping invalid address format: {addr}")
            
            if invalid_addresses:
                logger.warning(f"Found {len(invalid_addresses)} invalid addresses in column {address_column}")
            
            logger.info(f"Retrieved {len(valid_addresses)} valid addresses from {worksheet_name} column {address_column}")
            return valid_addresses
            
        except Exception as e:
            logger.error(f"Failed to read addresses from {worksheet_name}: {e}")
            raise
    
    def read_all_data_from_sheet(self, worksheet_name: str = "WALLET_LIST") -> List[Dict]:
        """Read all data from worksheet and return as list of dictionaries"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            
            # Get all data as list of lists
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning(f"No data found in worksheet {worksheet_name}")
                return []
            
            # First row should be headers
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # Convert to list of dictionaries
            data = []
            for row in data_rows:
                if any(cell.strip() for cell in row):  # Skip empty rows
                    row_dict = {}
                    for i, header in enumerate(headers):
                        if i < len(row):
                            row_dict[header] = row[i].strip()
                        else:
                            row_dict[header] = ''
                    data.append(row_dict)
            
            logger.info(f"Retrieved {len(data)} rows from {worksheet_name}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read data from {worksheet_name}: {e}")
            raise
    
    def get_worksheet_info(self, worksheet_name: str) -> Dict[str, Any]:
        """Get information about a worksheet"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            
            info = {
                'title': worksheet.title,
                'row_count': worksheet.row_count,
                'col_count': worksheet.col_count,
                'id': worksheet.id,
                'url': worksheet.url
            }
            
            # Get headers
            try:
                headers = worksheet.row_values(1)
                info['headers'] = headers
            except:
                info['headers'] = []
            
            logger.info(f"Worksheet {worksheet_name} info: {info}")
            return info
            
        except Exception as e:
            logger.error(f"Failed to get info for {worksheet_name}: {e}")
            raise
    
    def list_all_worksheets(self) -> List[Dict[str, Any]]:
        """List all worksheets in the workbook"""
        try:
            worksheets = self.workbook.worksheets()
            
            worksheet_info = []
            for ws in worksheets:
                info = {
                    'title': ws.title,
                    'id': ws.id,
                    'row_count': ws.row_count,
                    'col_count': ws.col_count
                }
                worksheet_info.append(info)
            
            logger.info(f"Found {len(worksheet_info)} worksheets")
            return worksheet_info
            
        except Exception as e:
            logger.error(f"Failed to list worksheets: {e}")
            raise
    
    def write_transactions_to_sheet(self, transactions: List[Dict], worksheet_name: str = "TRONSCAN"):
        """Write transaction data to specified worksheet"""
        try:
            # Try to get existing worksheet or create new one
            try:
                worksheet = self.workbook.worksheet(worksheet_name)
                logger.info(f"Using existing worksheet: {worksheet_name}")
            except gspread.WorksheetNotFound:
                worksheet = self.workbook.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                logger.info(f"Created new worksheet: {worksheet_name}")
            
            if not transactions:
                logger.warning("No transactions to write")
                return
            
            # Prepare data for Google Sheets
            df = pd.DataFrame(transactions)
            
            # Define column order and headers with USDT conversion and transfer type
            columns = [
                'hash', 'block_number', 'timestamp', 'from_address', 'to_address',
                'amount_raw', 'amount_formatted', 'amount_usdt', 'token_price_usdt',
                'token_name', 'token_symbol', 'contract_address', 'transfer_type', 'fee',
                'status', 'transaction_type', 'date_formatted', 'address_queried'
            ]
            
            # Ensure all columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns
            df = df[columns]
            
            # Clear existing data and write headers
            worksheet.clear()
            
            # Write headers with clear descriptions
            headers = [
                'Hash', 'Block Number', 'Timestamp', 'From Address', 'To Address',
                'Amount (Raw)', 'Amount (Formatted)', 'Amount (USDT)', 'Token Price (USDT)',
                'Token Name', 'Token Symbol', 'Contract Address', 'Transfer Type', 'Fee',
                'Status', 'Transaction Type', 'Date', 'Queried Address'
            ]
            worksheet.append_row(headers)
            
            # Write data in batches
            batch_size = int(get_env_variable('BATCH_SIZE', '100'))
            
            total_batches = (len(df) + batch_size - 1) // batch_size
            
            for i in range(0, len(df), batch_size):
                batch_num = i // batch_size + 1
                batch = df.iloc[i:i+batch_size]
                values = batch.values.tolist()
                
                # Convert all values to strings to avoid type issues
                values = [[str(cell) if cell is not None else '' for cell in row] for row in values]
                
                worksheet.append_rows(values)
                logger.info(f"Written batch {batch_num}/{total_batches}, rows {i+1} to {min(i+batch_size, len(df))}")
            
            logger.info(f"Successfully wrote {len(transactions)} transactions to {worksheet_name}")
            
        except Exception as e:
            logger.error(f"Failed to write transactions to {worksheet_name}: {e}")
            raise
    
    def append_transactions_to_sheet(self, transactions: List[Dict], worksheet_name: str = "TRONSCAN"):
        """Append new transaction data to existing worksheet"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            
            if not transactions:
                logger.warning("No transactions to append")
                return
            
            df = pd.DataFrame(transactions)
            
            # Define column order
            columns = [
                'hash', 'block_number', 'timestamp', 'from_address', 'to_address',
                'amount_raw', 'amount_formatted', 'amount_usdt', 'token_price_usdt',
                'token_name', 'token_symbol', 'contract_address', 'transfer_type', 'fee',
                'status', 'transaction_type', 'date_formatted', 'address_queried'
            ]
            
            # Ensure all columns exist and reorder
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            df = df[columns]
            
            # Append data
            values = df.values.tolist()
            values = [[str(cell) if cell is not None else '' for cell in row] for row in values]
            
            batch_size = int(get_env_variable('BATCH_SIZE', '100'))
            total_batches = (len(values) + batch_size - 1) // batch_size
            
            for i in range(0, len(values), batch_size):
                batch_num = i // batch_size + 1
                batch = values[i:i+batch_size]
                worksheet.append_rows(batch)
                logger.info(f"Appended batch {batch_num}/{total_batches}")
            
            logger.info(f"Appended {len(transactions)} transactions to {worksheet_name}")
            
        except Exception as e:
            logger.error(f"Failed to append transactions to {worksheet_name}: {e}")
            raise
    
    def get_existing_transaction_hashes(self, worksheet_name: str = "TRONSCAN") -> set:
        """Get set of existing transaction hashes to avoid duplicates"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            
            # Get all values from the hash column (assuming it's column A after headers)
            all_values = worksheet.get_all_values()
            
            if len(all_values) <= 1:  # Only headers or empty
                return set()
            
            # Find the hash column index
            headers = all_values[0]
            try:
                hash_col_index = headers.index('Hash')
            except ValueError:
                logger.warning("Hash column not found, assuming column 0")
                hash_col_index = 0
            
            # Extract hashes (skip header row)
            existing_hashes = set()
            for row in all_values[1:]:
                if hash_col_index < len(row) and row[hash_col_index]:
                    existing_hashes.add(row[hash_col_index])
            
            logger.info(f"Found {len(existing_hashes)} existing transaction hashes")
            return existing_hashes
            
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet {worksheet_name} not found, no existing hashes")
            return set()
        except Exception as e:
            logger.error(f"Failed to get existing hashes from {worksheet_name}: {e}")
            return set()
    
    def write_unique_transactions_to_sheet(self, transactions: List[Dict], worksheet_name: str = "TRONSCAN"):
        """Write only new transactions that don't already exist"""
        try:
            if not transactions:
                logger.warning("No transactions to write")
                return
            
            # Get existing transaction hashes
            existing_hashes = self.get_existing_transaction_hashes(worksheet_name)
            
            # Filter out duplicate transactions
            new_transactions = []
            duplicate_count = 0
            
            for tx in transactions:
                tx_hash = tx.get('hash', '')
                if tx_hash and tx_hash not in existing_hashes:
                    new_transactions.append(tx)
                else:
                    duplicate_count += 1
            
            logger.info(f"Filtered {duplicate_count} duplicate transactions")
            logger.info(f"Writing {len(new_transactions)} new transactions")
            
            if new_transactions:
                if existing_hashes:  # Append to existing data
                    self.append_transactions_to_sheet(new_transactions, worksheet_name)
                else:  # Write fresh data
                    self.write_transactions_to_sheet(new_transactions, worksheet_name)
            else:
                logger.info("No new transactions to write")
            
        except Exception as e:
            logger.error(f"Failed to write unique transactions to {worksheet_name}: {e}")
            raise
    
    def create_summary_sheet(self, transactions: List[Dict], worksheet_name: str = "SUMMARY"):
        """Create a summary sheet with transaction statistics"""
        try:
            if not transactions:
                logger.warning("No transactions to summarize")
                return
            
            # Try to get existing worksheet or create new one
            try:
                worksheet = self.workbook.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                worksheet = self.workbook.add_worksheet(title=worksheet_name, rows=100, cols=10)
                logger.info(f"Created new summary worksheet: {worksheet_name}")
            
            # Clear existing data
            worksheet.clear()
            
            df = pd.DataFrame(transactions)
            
            # Create summary data
            summary_data = []
            
            # Total transactions
            summary_data.append(['Total Transactions', len(df)])
            summary_data.append([''])  # Empty row
            
            # By address
            if 'address_queried' in df.columns:
                address_counts = df['address_queried'].value_counts()
                summary_data.append(['Transactions by Address:', ''])
                for address, count in address_counts.head(20).items():  # Top 20
                    summary_data.append([address, count])
                summary_data.append([''])  # Empty row
            
            # By token
            if 'token_symbol' in df.columns:
                token_counts = df['token_symbol'].value_counts()
                summary_data.append(['Transactions by Token:', ''])
                for token, count in token_counts.head(20).items():  # Top 20
                    summary_data.append([token, count])
                summary_data.append([''])  # Empty row
            
            # By status
            if 'status' in df.columns:
                status_counts = df['status'].value_counts()
                summary_data.append(['Transactions by Status:', ''])
                for status, count in status_counts.items():
                    summary_data.append([status, count])
                summary_data.append([''])  # Empty row
            
            # Date range
            if 'date_formatted' in df.columns:
                dates = pd.to_datetime(df['date_formatted'], errors='coerce')
                dates = dates.dropna()
                if not dates.empty:
                    summary_data.append(['Date Range:', ''])
                    summary_data.append(['Earliest Transaction', dates.min().strftime('%Y-%m-%d')])
                    summary_data.append(['Latest Transaction', dates.max().strftime('%Y-%m-%d')])
            
            # Write summary data
            for row in summary_data:
                worksheet.append_row(row)
            
            logger.info(f"Created summary sheet with {len(summary_data)} rows")
            
        except Exception as e:
            logger.error(f"Failed to create summary sheet: {e}")
            raise