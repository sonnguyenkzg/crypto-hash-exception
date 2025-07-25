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
