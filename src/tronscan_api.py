import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from decimal import Decimal, getcontext
from src.utils import setup_logging, rate_limit_delay, get_env_variable

logger = setup_logging()
getcontext().prec = 28

class TronScanAPI:
    """Optimized TronScan API client for HASH, WALLET, AMT output"""
    
    def __init__(self):
        self.base_url = get_env_variable('TRONSCAN_API_BASE_URL')
        self.api_key = get_env_variable('TRONSCAN_API_KEY', '')
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with error handling"""
        url = f"{self.base_url}/{endpoint}"
        try:
            rate_limit_delay()
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            raise
    
    def _extract_usdt_transfers(self, tx: Dict, wallet_address: str) -> List[Dict]:
        """Extract USDT transfers and return simplified format"""
        results = []
        
        # Look for TRC20 transfers (USDT, USDC, etc.)
        if 'trc20TransferInfo' in tx and tx['trc20TransferInfo']:
            for transfer in tx['trc20TransferInfo']:
                token_symbol = transfer.get('symbol', '').upper()
                
                # Only process USDT transfers
                if token_symbol == 'USDT':
                    amount_str = transfer.get('amount_str', '0')
                    decimals = transfer.get('decimals', 6)
                    
                    # Convert to USDT amount
                    usdt_amount = 0
                    if amount_str and amount_str != '0':
                        try:
                            raw_decimal = Decimal(str(amount_str))
                            divisor = Decimal(10) ** decimals
                            usdt_amount = float(raw_decimal / divisor)
                        except:
                            usdt_amount = 0
                    
                    # Only include if amount > 0
                    if usdt_amount > 0:
                        results.append({
                            'hash': tx.get('hash', ''),
                            'wallet': wallet_address,
                            'amt_usdt': usdt_amount
                        })
        
        return results
    
    def get_usdt_transactions(self, address: str, start_timestamp: int, end_timestamp: int) -> List[Dict]:
        """Get USDT transactions for address - simplified output"""
        all_transactions = []
        start = 0
        limit = 200
        
        while True:
            params = {
                'address': address,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp,
                'start': start,
                'limit': limit,
                'sort': '-timestamp'
            }
            
            try:
                response = self._make_request('transaction', params)
                
                if 'data' not in response:
                    break
                
                transactions = response['data']
                if not transactions:
                    break
                
                # Extract USDT transfers from each transaction
                for tx in transactions:
                    usdt_transfers = self._extract_usdt_transfers(tx, address)
                    all_transactions.extend(usdt_transfers)
                
                logger.info(f"Processed {len(transactions)} transactions for {address}, found {len([t for t in all_transactions if t['wallet'] == address])} USDT transfers")
                
                if len(transactions) < limit:
                    break
                
                start += limit
                
                # Safety limit
                if start > 50000:  # Max 250 API calls per address
                    logger.warning(f"Reached API limit for address {address}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching transactions for {address}: {e}")
                break
        
        return all_transactions
    
    def get_usdt_for_multiple_addresses(self, addresses: List[str], date_from: str, date_to: str) -> List[Dict]:
        """Get USDT transactions for multiple addresses"""
        # Convert dates to timestamps
        start_timestamp = int(datetime.strptime(date_from, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int((datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000)
        
        all_usdt_transactions = []
        
        for i, address in enumerate(addresses, 1):
            logger.info(f"Processing address {i}/{len(addresses)}: {address}")
            
            try:
                usdt_transactions = self.get_usdt_transactions(address, start_timestamp, end_timestamp)
                all_usdt_transactions.extend(usdt_transactions)
                
                usdt_count = len(usdt_transactions)
                total_usdt = sum(tx['amt_usdt'] for tx in usdt_transactions)
                logger.info(f"✅ Address {address}: {usdt_count} USDT transactions, Total: ${total_usdt:,.2f}")
                
            except Exception as e:
                logger.error(f"Failed to get USDT transactions for address {address}: {e}")
                continue
        
        logger.info(f"🎉 Total USDT transactions found: {len(all_usdt_transactions)}")
        logger.info(f"💰 Total USDT value: ${sum(tx['amt_usdt'] for tx in all_usdt_transactions):,.2f}")
        
        return all_usdt_transactions
    
    def get_usdt_for_single_address(self, address: str, date_from: str, date_to: str) -> List[Dict]:
        """Get USDT transactions for single address"""
        return self.get_usdt_for_multiple_addresses([address], date_from, date_to)