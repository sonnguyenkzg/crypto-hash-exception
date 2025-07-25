import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext
from src.utils import setup_logging, rate_limit_delay, convert_timestamp_to_date, get_env_variable
from src.historical_price_fetcher import HistoricalPriceFetcher

logger = setup_logging()
getcontext().prec = 28

class TronScanAPI:
    """TronScan API client with proper USDT/TRC20 token extraction"""
    
    def __init__(self):
        self.base_url = get_env_variable('TRONSCAN_API_BASE_URL')
        self.api_key = get_env_variable('TRONSCAN_API_KEY', '')
        self.session = requests.Session()
        self.price_fetcher = HistoricalPriceFetcher()
        
        if self.api_key:
            self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
        
        self.token_decimals = {
            'TRX': 6, 'USDT': 6, 'USDC': 6, 'BTT': 18, 'JST': 18,
            'SUN': 18, 'WIN': 6, 'JUST': 18, 'NFT': 6, 'USDJ': 18,
            'TUSD': 18, 'LIVE': 6, 'DEFAULT': 6
        }
        
        self.token_contracts = {
            'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t': 'USDT',
            'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz81': 'USDC',
            'TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR': 'BTT',
            'TCFLL5dx5ZJdKnWuesXxi1VPwjLVmWZZy9': 'JST'
        }
    
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
    
    def _extract_token_transfers(self, tx: Dict) -> List[Dict]:
        """Extract TRC20 token transfers from transaction"""
        transfers = []
        
        # Check for TRC20 transfers (USDT, USDC, etc.)
        if 'trc20TransferInfo' in tx and tx['trc20TransferInfo']:
            for transfer in tx['trc20TransferInfo']:
                transfers.append({
                    'type': 'TRC20',
                    'token_symbol': transfer.get('symbol', '').upper(),
                    'token_name': transfer.get('name', ''),
                    'contract_address': transfer.get('contract_address', ''),
                    'amount_str': transfer.get('amount_str', '0'),
                    'decimals': transfer.get('decimals', 6),
                    'from_address': transfer.get('from_address', ''),
                    'to_address': transfer.get('to_address', ''),
                    'icon_url': transfer.get('icon_url', '')
                })
        
        # Check for TRC10 transfers (if any)
        if 'tokenTransferInfo' in tx and tx['tokenTransferInfo']:
            for transfer in tx['tokenTransferInfo']:
                transfers.append({
                    'type': 'TRC10',
                    'token_symbol': transfer.get('symbol', '').upper(),
                    'token_name': transfer.get('name', ''),
                    'contract_address': '',
                    'amount_str': transfer.get('amount_str', '0'),
                    'decimals': transfer.get('decimals', 6),
                    'from_address': transfer.get('from_address', ''),
                    'to_address': transfer.get('to_address', ''),
                    'icon_url': transfer.get('icon_url', '')
                })
        
        return transfers
    
    def _convert_amount_to_readable(self, raw_amount: str, token_symbol: str, 
                                  decimals: int = None, timestamp: int = 0) -> Dict[str, Any]:
        """Convert amount with historical USDT value"""
        try:
            if not raw_amount or raw_amount == '0':
                return {
                    'amount_raw': '0',
                    'amount_formatted': '0',
                    'amount_usdt': '0.0',
                    'token_price_usdt': '0.0'
                }
            
            # Use provided decimals or fallback to token defaults
            if decimals is None:
                decimals = self.token_decimals.get(token_symbol.upper(), 6)
            
            # Convert amount
            raw_decimal = Decimal(str(raw_amount))
            divisor = Decimal(10) ** decimals
            formatted_amount = raw_decimal / divisor
            
            # Get historical price
            if timestamp > 0:
                token_price = self.price_fetcher.get_historical_price(token_symbol, timestamp)
            else:
                token_price = self._get_fallback_price(token_symbol)
            
            usdt_value = float(formatted_amount) * token_price
            
            return {
                'amount_raw': str(raw_amount),
                'amount_formatted': str(formatted_amount),
                'amount_usdt': f"{usdt_value:.6f}",
                'token_price_usdt': f"{token_price:.8f}"
            }
            
        except Exception as e:
            logger.error(f"Error converting amount {raw_amount}: {e}")
            return {
                'amount_raw': str(raw_amount),
                'amount_formatted': str(raw_amount),
                'amount_usdt': '0.0',
                'token_price_usdt': '0.0'
            }
    
    def _get_fallback_price(self, token_symbol: str) -> float:
        """Fallback prices"""
        prices = {
            'TRX': 0.12, 'USDT': 1.0, 'USDC': 1.0, 'BTT': 0.0000008,
            'JST': 0.025, 'SUN': 0.006, 'WIN': 0.00008
        }
        return prices.get(token_symbol.upper(), 0.0)
    
    def get_account_transactions(self, address: str, start_timestamp: int, 
                               end_timestamp: int, limit: int = 200) -> List[Dict]:
        """Get transactions with proper token extraction"""
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
                    break
                
                transactions = response['data']
                if not transactions:
                    break
                
                for tx in transactions:
                    processed_txs = self._normalize_transaction(tx, address)
                    all_transactions.extend(processed_txs)  # Note: extend, not append
                
                logger.info(f"Retrieved {len(transactions)} transactions for {address}")
                
                if len(transactions) < limit:
                    break
                
                start += limit
                
                if len(all_transactions) > 10000:
                    logger.warning(f"Reached limit for address {address}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching transactions for {address}: {e}")
                break
        
        return all_transactions
    
    def _normalize_transaction(self, tx: Dict, address: str) -> List[Dict]:
        """Normalize transaction - may return multiple entries for multi-token transfers"""
        base_info = {
            'hash': tx.get('hash', ''),
            'block_number': tx.get('block', ''),
            'timestamp': tx.get('timestamp', 0),
            'fee': tx.get('cost', {}).get('net_fee', 0),
            'status': 'SUCCESS' if tx.get('confirmed') else 'FAILED',
            'transaction_type': self._determine_transaction_type(tx),
            'date_formatted': convert_timestamp_to_date(tx.get('timestamp', 0)),
            'address_queried': address
        }
        
        results = []
        
        # Extract token transfers (USDT, USDC, etc.)
        token_transfers = self._extract_token_transfers(tx)
        
        if token_transfers:
            # Process each token transfer separately
            for transfer in token_transfers:
                amount_data = self._convert_amount_to_readable(
                    transfer['amount_str'],
                    transfer['token_symbol'],
                    transfer['decimals'],
                    base_info['timestamp']
                )
                
                transfer_tx = base_info.copy()
                transfer_tx.update({
                    'from_address': transfer['from_address'],
                    'to_address': transfer['to_address'],
                    'amount_raw': amount_data['amount_raw'],
                    'amount_formatted': amount_data['amount_formatted'],
                    'amount_usdt': amount_data['amount_usdt'],
                    'token_price_usdt': amount_data['token_price_usdt'],
                    'token_name': transfer['token_name'],
                    'token_symbol': transfer['token_symbol'],
                    'contract_address': transfer['contract_address'],
                    'transfer_type': transfer['type']
                })
                
                results.append(transfer_tx)
        
        else:
            # Regular TRX transaction
            raw_amount = self._extract_trx_value(tx)
            amount_data = self._convert_amount_to_readable(
                raw_amount, 'TRX', 6, base_info['timestamp']
            )
            
            trx_tx = base_info.copy()
            trx_tx.update({
                'from_address': tx.get('ownerAddress', ''),
                'to_address': tx.get('toAddress', ''),
                'amount_raw': amount_data['amount_raw'],
                'amount_formatted': amount_data['amount_formatted'],
                'amount_usdt': amount_data['amount_usdt'],
                'token_price_usdt': amount_data['token_price_usdt'],
                'token_name': 'TRX',
                'token_symbol': 'TRX',
                'contract_address': '',
                'transfer_type': 'TRX'
            })
            
            results.append(trx_tx)
        
        return results
    
    def _extract_trx_value(self, tx: Dict) -> str:
        """Extract TRX transaction value"""
        if 'amount' in tx:
            return str(tx['amount'])
        elif 'value' in tx:
            return str(tx['value'])
        elif 'contractData' in tx and tx['contractData']:
            return str(tx['contractData'].get('amount', 0))
        return '0'
    
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
    
    def get_transactions_for_multiple_addresses(self, addresses: List[str], 
                                              date_from: str, date_to: str) -> List[Dict]:
        """Get transactions for multiple addresses"""
        start_timestamp = int(datetime.strptime(date_from, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int((datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000)
        
        all_transactions = []
        
        for i, address in enumerate(addresses, 1):
            logger.info(f"Processing address {i}/{len(addresses)}: {address}")
            
            try:
                transactions = self.get_account_transactions(address, start_timestamp, end_timestamp)
                all_transactions.extend(transactions)
                logger.info(f"Retrieved {len(transactions)} transactions for {address}")
                
            except Exception as e:
                logger.error(f"Failed to get transactions for address {address}: {e}")
                continue
        
        logger.info(f"Total transactions retrieved: {len(all_transactions)}")
        return all_transactions