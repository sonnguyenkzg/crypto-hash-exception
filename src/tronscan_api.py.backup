import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from src.utils import setup_logging, rate_limit_delay, convert_timestamp_to_date, get_env_variable

logger = setup_logging()

class TronScanAPI:
    """TronScan API client for fetching transaction data"""
    
    def __init__(self):
        self.base_url = get_env_variable('TRONSCAN_API_BASE_URL')
        self.api_key = get_env_variable('TRONSCAN_API_KEY', '')
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with error handling and rate limiting"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            rate_limit_delay()
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            raise
    
    def get_account_transactions(
        self, 
        address: str, 
        start_timestamp: int, 
        end_timestamp: int,
        limit: int = 200
    ) -> List[Dict]:
        """Get transactions for a specific address within date range"""
        
        all_transactions = []
        start = 0
        
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
                    logger.warning(f"No data field in response for address {address}")
                    break
                
                transactions = response['data']
                
                if not transactions:
                    logger.info(f"No more transactions found for address {address}")
                    break
                
                # Process and normalize transaction data
                processed_transactions = []
                for tx in transactions:
                    processed_tx = self._normalize_transaction(tx, address)
                    processed_transactions.append(processed_tx)
                
                all_transactions.extend(processed_transactions)
                
                logger.info(f"Retrieved {len(transactions)} transactions for {address}, total: {len(all_transactions)}")
                
                # Check if we got fewer results than requested (end of data)
                if len(transactions) < limit:
                    break
                
                start += limit
                
                # Safety limit to prevent infinite loops
                if len(all_transactions) > 10000:
                    logger.warning(f"Reached safety limit of 10000 transactions for address {address}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching transactions for {address}: {e}")
                break
        
        return all_transactions
    
    def _normalize_transaction(self, tx: Dict, address: str) -> Dict:
        """Normalize transaction data to consistent format"""
        
        return {
            'hash': tx.get('hash', ''),
            'block_number': tx.get('block', ''),
            'timestamp': tx.get('timestamp', 0),
            'from_address': tx.get('ownerAddress', ''),
            'to_address': tx.get('toAddress', ''),
            'value': self._extract_value(tx),
            'token_name': self._extract_token_name(tx),
            'token_symbol': self._extract_token_symbol(tx),
            'contract_address': tx.get('contractAddress', ''),
            'fee': tx.get('cost', {}).get('net_fee', 0),
            'status': 'SUCCESS' if tx.get('confirmed') else 'FAILED',
            'transaction_type': self._determine_transaction_type(tx),
            'date_formatted': convert_timestamp_to_date(tx.get('timestamp', 0)),
            'address_queried': address
        }
    
    def _extract_value(self, tx: Dict) -> str:
        """Extract transaction value"""
        if 'amount' in tx:
            return str(tx['amount'])
        elif 'value' in tx:
            return str(tx['value'])
        elif 'contractData' in tx and tx['contractData']:
            return str(tx['contractData'].get('amount', 0))
        return '0'
    
    def _extract_token_name(self, tx: Dict) -> str:
        """Extract token name"""
        if 'tokenInfo' in tx and tx['tokenInfo']:
            return tx['tokenInfo'].get('tokenName', 'TRX')
        return 'TRX'
    
    def _extract_token_symbol(self, tx: Dict) -> str:
        """Extract token symbol"""
        if 'tokenInfo' in tx and tx['tokenInfo']:
            return tx['tokenInfo'].get('tokenAbbr', 'TRX')
        return 'TRX'
    
    def _determine_transaction_type(self, tx: Dict) -> str:
        """Determine transaction type"""
        tx_type = tx.get('contractType', 0)
        
        type_mapping = {
            1: 'TransferContract',
            2: 'TransferAssetContract',
            31: 'TriggerSmartContract',
            11: 'FreezeBalanceContract',
            12: 'UnfreezeBalanceContract'
        }
        
        return type_mapping.get(tx_type, f'Unknown({tx_type})')
    
    def get_transactions_for_multiple_addresses(
        self, 
        addresses: List[str], 
        date_from: str, 
        date_to: str
    ) -> List[Dict]:
        """Get transactions for multiple addresses"""
        
        # Convert dates to timestamps
        start_timestamp = int(datetime.strptime(date_from, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int((datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000)
        
        all_transactions = []
        
        for i, address in enumerate(addresses, 1):
            logger.info(f"Processing address {i}/{len(addresses)}: {address}")
            
            try:
                transactions = self.get_account_transactions(
                    address, start_timestamp, end_timestamp
                )
                all_transactions.extend(transactions)
                
                logger.info(f"Retrieved {len(transactions)} transactions for {address}")
                
            except Exception as e:
                logger.error(f"Failed to get transactions for address {address}: {e}")
                continue
        
        logger.info(f"Total transactions retrieved: {len(all_transactions)}")
        return all_transactions