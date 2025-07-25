import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any, Optional
import pandas as pd
from src.utils import setup_logging, get_env_variable

logger = setup_logging()

class GoogleSheetsManager:
    """Manages Google Sheets operations"""
    
    def __init__(self):
        self.credentials_path = get_env_variable('GOOGLE_SHEETS_CREDENTIALS_PATH')
        self.sheet_id = get_env_variable('GOOGLE_SHEET_ID')
        self.client = self._authenticate()
        self.workbook = self.client.open_by_key(self.sheet_id)
    
    def _authenticate(self) -> gspread.Client:
        """Authenticate with Google Sheets API"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=scope
            )
            return gspread.authorize(credentials)
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets: {e}")
            raise
    
    def read_addresses_from_sheet(self, worksheet_name: str = "Sheet1") -> List[str]:
        """Read addresses from the first column of specified worksheet"""
        try:
            worksheet = self.workbook.worksheet(worksheet_name)
            # Get all values from column A (addresses)
            addresses = worksheet.col_values(1)
            
            # Filter out empty cells and header
            addresses = [addr.strip() for addr in addresses if addr.strip()]
            
            # Remove header if present
            if addresses and not addresses[0].startswith('T'):
                addresses = addresses[1:]
            
            logger.info(f"Retrieved {len(addresses)} addresses from {worksheet_name}")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to read addresses from {worksheet_name}: {e}")
            raise
    
    def write_transactions_to_sheet(self, transactions: List[Dict], worksheet_name: str = "TRONSCAN"):
        """Write transaction data to specified worksheet"""
        try:
            # Try to get existing worksheet or create new one
            try:
                worksheet = self.workbook.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                worksheet = self.workbook.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                logger.info(f"Created new worksheet: {worksheet_name}")
            
            if not transactions:
                logger.warning("No transactions to write")
                return
            
            # Prepare data for Google Sheets
            df = pd.DataFrame(transactions)
            
            # Define column order and headers
            columns = [
                'hash', 'block_number', 'timestamp', 'from_address', 'to_address',
                'value', 'token_name', 'token_symbol', 'contract_address', 'fee',
                'status', 'transaction_type', 'date_formatted'
            ]
            
            # Ensure all columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns
            df = df[columns]
            
            # Clear existing data and write headers
            worksheet.clear()
            
            # Write headers
            headers = [
                'Hash', 'Block Number', 'Timestamp', 'From Address', 'To Address',
                'Value', 'Token Name', 'Token Symbol', 'Contract Address', 'Fee',
                'Status', 'Transaction Type', 'Date'
            ]
            worksheet.append_row(headers)
            
            # Write data in batches
            batch_size = int(get_env_variable('BATCH_SIZE', '100'))
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                values = batch.values.tolist()
                
                # Convert all values to strings to avoid type issues
                values = [[str(cell) if cell is not None else '' for cell in row] for row in values]
                
                worksheet.append_rows(values)
                logger.info(f"Written batch {i//batch_size + 1}, rows {i+1} to {min(i+batch_size, len(df))}")
            
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
                'value', 'token_name', 'token_symbol', 'contract_address', 'fee',
                'status', 'transaction_type', 'date_formatted'
            ]
            
            # Ensure all columns exist and reorder
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            df = df[columns]
            
            # Append data
            values = df.values.tolist()
            values = [[str(cell) if cell is not None else '' for cell in row] for row in values]
            
            worksheet.append_rows(values)
            logger.info(f"Appended {len(transactions)} transactions to {worksheet_name}")
            
        except Exception as e:
            logger.error(f"Failed to append transactions to {worksheet_name}: {e}")
            raise