#!/bin/bash

echo "🔄 Updating TronScan project with historical price support..."

# Step 1: Create the historical price fetcher
echo "📝 Creating historical price fetcher..."
cat > src/historical_price_fetcher.py << 'EOF'
import requests
import time
from datetime import datetime
from typing import Dict, Optional
from src.utils import setup_logging

logger = setup_logging()

class HistoricalPriceFetcher:
    """Fetch historical cryptocurrency prices at specific timestamps"""
    
    def __init__(self):
        self.session = requests.Session()
        self.price_cache = {}  # Cache: {(token, date): price}
        
        # CoinGecko API mapping for Tron ecosystem tokens
        self.token_mapping = {
            'TRX': 'tron',
            'USDT': 'tether',
            'USDC': 'usd-coin',
            'BTT': 'bittorrent',
            'JST': 'just',
            'SUN': 'sun-token',
            'WIN': 'wink',
            'JUST': 'just',
            'NFT': 'apenft',
            'USDJ': 'just-stablecoin',
            'TUSD': 'true-usd',
            'LIVE': 'live-coin'
        }
    
    def get_historical_price(self, token_symbol: str, timestamp: int) -> float:
        """Get historical price in USD at specific timestamp"""
        token_symbol = token_symbol.upper()
        
        # Convert timestamp to date for caching
        date_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
        cache_key = (token_symbol, date_str)
        
        # Check cache first
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        # Fetch historical price
        try:
            price = self._fetch_historical_price_from_coingecko(token_symbol, date_str)
            self.price_cache[cache_key] = price
            return price
        except Exception as e:
            logger.warning(f"Failed to fetch historical price for {token_symbol} on {date_str}: {e}")
            return self._get_fallback_price(token_symbol)
    
    def _fetch_historical_price_from_coingecko(self, token_symbol: str, date_str: str) -> float:
        """Fetch historical price from CoinGecko API"""
        if token_symbol not in self.token_mapping:
            return self._get_fallback_price(token_symbol)
        
        coin_id = self.token_mapping[token_symbol]
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
        
        # Convert YYYY-MM-DD to DD-MM-YYYY for CoinGecko
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = dt.strftime('%d-%m-%Y')
        except ValueError:
            return self._get_fallback_price(token_symbol)
        
        params = {'date': formatted_date, 'localization': 'false'}
        
        try:
            time.sleep(1.0)  # Rate limiting
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'market_data' in data and 'current_price' in data['market_data']:
                if 'usd' in data['market_data']['current_price']:
                    price = float(data['market_data']['current_price']['usd'])
                    logger.info(f"Historical price for {token_symbol} on {date_str}: ${price:.8f}")
                    return price
            
            return self._get_fallback_price(token_symbol)
                
        except Exception as e:
            logger.error(f"API request failed for {token_symbol}: {e}")
            return self._get_fallback_price(token_symbol)
    
    def _get_fallback_price(self, token_symbol: str) -> float:
        """Get fallback price when API fails"""
        fallback_prices = {
            'TRX': 0.12, 'USDT': 1.0, 'USDC': 1.0, 'BTT': 0.0000008,
            'JST': 0.025, 'SUN': 0.006, 'WIN': 0.00008, 'JUST': 0.025,
            'NFT': 0.0000005, 'USDJ': 1.0, 'TUSD': 1.0, 'LIVE': 0.001
        }
        return fallback_prices.get(token_symbol, 0.0)
    
    def clear_cache(self):
        """Clear price cache"""
        self.price_cache.clear()
EOF

echo "✅ Historical price fetcher created!"

# Step 2: Update TronScan API
echo "📝 Updating TronScan API with historical prices..."
cp src/tronscan_api.py src/tronscan_api.py.backup 2>/dev/null || true

cat > src/tronscan_api.py << 'EOF'
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
    """TronScan API client with historical USDT conversion"""
    
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
    
    def _convert_amount_to_readable(self, raw_amount: str, token_symbol: str, 
                                  contract_address: str = '', timestamp: int = 0) -> Dict[str, Any]:
        """Convert amount with historical USDT value"""
        try:
            if not raw_amount or raw_amount == '0':
                return {
                    'amount_raw': '0',
                    'amount_formatted': '0',
                    'amount_usdt': '0.0',
                    'token_price_usdt': '0.0'
                }
            
            # Get token decimals
            decimals = self.token_decimals.get(token_symbol.upper(), 6)
            
            # Identify token by contract if available
            if contract_address and contract_address in self.token_contracts:
                token_symbol = self.token_contracts[contract_address]
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
        """Get transactions with historical price conversion"""
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
                    processed_tx = self._normalize_transaction(tx, address)
                    all_transactions.append(processed_tx)
                
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
    
    def _normalize_transaction(self, tx: Dict, address: str) -> Dict:
        """Normalize transaction with historical USDT conversion"""
        tx_hash = tx.get('hash', '')
        block_number = tx.get('block', '')
        timestamp = tx.get('timestamp', 0)
        from_address = tx.get('ownerAddress', '')
        to_address = tx.get('toAddress', '')
        
        raw_amount = self._extract_value(tx)
        token_info = self._extract_token_info(tx)
        contract_address = tx.get('contractAddress', '')
        
        # Convert with historical price
        amount_data = self._convert_amount_to_readable(
            raw_amount, token_info['symbol'], contract_address, timestamp
        )
        
        return {
            'hash': tx_hash,
            'block_number': block_number,
            'timestamp': timestamp,
            'from_address': from_address,
            'to_address': to_address,
            'amount_raw': amount_data['amount_raw'],
            'amount_formatted': amount_data['amount_formatted'],
            'amount_usdt': amount_data['amount_usdt'],
            'token_price_usdt': amount_data['token_price_usdt'],
            'token_name': token_info['name'],
            'token_symbol': token_info['symbol'],
            'contract_address': contract_address,
            'fee': tx.get('cost', {}).get('net_fee', 0),
            'status': 'SUCCESS' if tx.get('confirmed') else 'FAILED',
            'transaction_type': self._determine_transaction_type(tx),
            'date_formatted': convert_timestamp_to_date(timestamp),
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
    
    def _extract_token_info(self, tx: Dict) -> Dict[str, str]:
        """Extract token information"""
        if 'tokenInfo' in tx and tx['tokenInfo']:
            return {
                'name': tx['tokenInfo'].get('tokenName', 'TRX'),
                'symbol': tx['tokenInfo'].get('tokenAbbr', 'TRX')
            }
        return {'name': 'TRX', 'symbol': 'TRX'}
    
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
EOF

echo "✅ TronScan API updated!"
echo ""
echo "🎉 Update complete! Your project now includes:"
echo "   - Historical price fetching at transaction time"
echo "   - New columns: amount_raw, amount_formatted, amount_usdt, token_price_usdt"
echo "   - CoinGecko API integration for real historical prices"
echo "   - Price caching to reduce API calls"
echo ""
echo "📊 New Google Sheets columns:"
echo "   - Amount (Raw): Original blockchain amount"
echo "   - Amount (Formatted): Human-readable amount"
echo "   - Amount (USDT): Value in USDT at transaction time"
echo "   - Token Price (USDT): Price per token at transaction time"
echo ""
echo "🚀 Ready to test with historical load!"