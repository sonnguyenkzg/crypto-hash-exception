#!/usr/bin/env python3
"""
USDT Transaction Fetcher using TronGrid API
More reliable endpoint that directly fetches TRC20 transactions
"""

import requests
import time
import csv
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from decimal import Decimal, getcontext

# Set high precision for decimal calculations
getcontext().prec = 28

class USDTTransactionFetcher:
    """TronGrid API client for direct USDT transaction retrieval"""
    
    def __init__(self, api_key: str = None):
        self.base_url = "https://api.trongrid.io/v1"
        self.api_key = api_key
        self.session = requests.Session()
        
        # USDT TRC20 contract address
        self.usdt_contract = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        
        if self.api_key:
            self.session.headers.update({'TRON-PRO-API-KEY': self.api_key})
        
        # Rate limiting - be conservative with requests
        self.rate_limit_delay = 0.2 if api_key else 0.5
    
    def _rate_limit(self):
        """Apply rate limiting delay"""
        time.sleep(self.rate_limit_delay)
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with error handling and rate limiting"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            self._rate_limit()
            response = self.session.get(url, params=params or {}, timeout=30)
            response.raise_for_status()
            
            # Check if response is JSON
            try:
                return response.json()
            except ValueError:
                print(f"❌ Non-JSON response from {url}")
                print(f"Response: {response.text[:500]}...")
                raise
                
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed for {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text[:500]}...")
            raise
    
    def _convert_timestamp_to_gmt7(self, timestamp_ms: int) -> str:
        """Convert timestamp to GMT+7 timezone string"""
        # Convert milliseconds to seconds
        timestamp_s = timestamp_ms / 1000
        
        # Create datetime object in UTC
        dt_utc = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
        
        # Convert to GMT+7
        gmt7 = timezone(timedelta(hours=7))
        dt_gmt7 = dt_utc.astimezone(gmt7)
        
        return dt_gmt7.strftime('%Y-%m-%d %H:%M:%S GMT+7')
    
    def _date_to_timestamp(self, date_str: str, is_end_date: bool = False) -> int:
        """Convert date string to timestamp in milliseconds"""
        # Parse date
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        # If end date, set to end of day (23:59:59)
        if is_end_date:
            dt = dt.replace(hour=23, minute=59, second=59)
        
        # Convert to GMT+7 timezone first
        gmt7 = timezone(timedelta(hours=7))
        dt_gmt7 = dt.replace(tzinfo=gmt7)
        
        # Convert to UTC timestamp in milliseconds
        timestamp_ms = int(dt_gmt7.timestamp() * 1000)
        return timestamp_ms
    
    def get_usdt_transactions(self, wallet_address: str, date_from: str, date_to: str) -> List[Dict]:
        """
        Get USDT transactions for a wallet address using TronGrid API
        Returns transactions with wallet, hash, amount, date, and direction
        """
        print(f"🔍 Fetching USDT transactions for wallet: {wallet_address}")
        print(f"📅 Date range: {date_from} to {date_to} (GMT+7)")
        
        # Convert dates to timestamps  
        min_timestamp = self._date_to_timestamp(date_from)
        max_timestamp = self._date_to_timestamp(date_to, is_end_date=True)
        
        print(f"🕐 Timestamp range: {min_timestamp} to {max_timestamp}")
        
        all_transactions = []
        fingerprint = None
        limit = 200  # Maximum allowed by TronGrid
        
        while True:
            # Build the endpoint URL
            endpoint = f"accounts/{wallet_address}/transactions/trc20"
            
            params = {
                'limit': limit,
                'contract_address': self.usdt_contract,
                'only_confirmed': 'true',
                'min_timestamp': min_timestamp,
                'max_timestamp': max_timestamp
            }
            
            # Add fingerprint for pagination if we have one
            if fingerprint:
                params['fingerprint'] = fingerprint
            
            try:
                print(f"📡 Fetching TRC20 transactions (limit: {limit})...")
                if fingerprint:
                    print(f"   Using fingerprint for pagination: {fingerprint[:16]}...")
                
                response = self._make_request(endpoint, params)
                
                # Check if response has data
                if 'data' not in response:
                    print("⚠️  No 'data' field in response")
                    print(f"Response keys: {list(response.keys())}")
                    break
                
                transactions = response['data']
                if not transactions:
                    print("✅ No more transactions found")
                    break
                
                print(f"📊 Processing {len(transactions)} transactions...")
                
                # Process each transaction
                for tx in transactions:
                    # Extract transaction details
                    tx_hash = tx.get('transaction_id', '')
                    from_address = tx.get('from', '')
                    to_address = tx.get('to', '')
                    value_str = tx.get('value', '0')
                    block_timestamp = tx.get('block_timestamp', 0)
                    token_info = tx.get('token_info', {})
                    decimals = token_info.get('decimals', 6)
                    
                    # Verify this is USDT
                    if token_info.get('symbol', '').upper() != 'USDT':
                        continue
                    
                    # Calculate USDT amount 
                    if value_str and value_str != '0':
                        try:
                            raw_decimal = Decimal(str(value_str))
                            usdt_amount = float(raw_decimal / Decimal(10**decimals))
                        except:
                            usdt_amount = 0
                    else:
                        usdt_amount = 0
                    
                    # Skip zero amounts
                    if usdt_amount == 0:
                        continue
                    
                    # Determine direction and sign
                    if from_address.lower() == wallet_address.lower():
                        # Outgoing transaction (negative)
                        direction = "OUT"
                        signed_amount = -usdt_amount
                    elif to_address.lower() == wallet_address.lower():
                        # Incoming transaction (positive)
                        direction = "IN"  
                        signed_amount = usdt_amount
                    else:
                        # This shouldn't happen but just in case
                        continue
                    
                    # Convert timestamp to GMT+7
                    date_gmt7 = self._convert_timestamp_to_gmt7(block_timestamp)
                    
                    # Create transaction record
                    transaction = {
                        'wallet': wallet_address,
                        'hash': tx_hash,
                        'amount_usdt': signed_amount,
                        'amount_abs': usdt_amount,  # Absolute amount for reference
                        'date_gmt7': date_gmt7,
                        'direction': direction,
                        'from_address': from_address,
                        'to_address': to_address,
                        'timestamp': block_timestamp
                    }
                    
                    all_transactions.append(transaction)
                    
                    # Log the transaction
                    sign = "+" if signed_amount > 0 else ""
                    print(f"💰 {direction}: {sign}{signed_amount:,.6f} USDT | {date_gmt7.split(' ')[0]} {date_gmt7.split(' ')[1]} | {tx_hash[:16]}...")
                
                # Get the fingerprint for next page
                if 'meta' in response and 'fingerprint' in response['meta']:
                    new_fingerprint = response['meta']['fingerprint']
                    if new_fingerprint == fingerprint:
                        # Same fingerprint means no more data
                        break
                    fingerprint = new_fingerprint
                elif len(transactions) < limit:
                    # Less than limit means we're done
                    break
                else:
                    # No fingerprint and full limit - might be more data but we can't paginate
                    print("⚠️  No fingerprint for pagination, stopping here")
                    break
                    
            except Exception as e:
                print(f"❌ Error fetching transactions: {e}")
                break
        
        return all_transactions

    def save_to_csv(self, transactions: List[Dict], filename: str = None) -> str:
        """Save transactions to CSV file"""
        if not filename:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"usdt_transactions_{timestamp}.csv"
        
        fieldnames = [
            'wallet_address',
            'transaction_hash', 
            'date_gmt7',
            'direction',
            'amount_usdt',
            'amount_abs',
            'from_address',
            'to_address',
            'timestamp'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for tx in transactions:
                    writer.writerow({
                        'wallet_address': tx['wallet'],
                        'transaction_hash': tx['hash'],  # Full hash here
                        'date_gmt7': tx['date_gmt7'],
                        'direction': tx['direction'],
                        'amount_usdt': tx['amount_usdt'],
                        'amount_abs': tx['amount_abs'],
                        'from_address': tx['from_address'],
                        'to_address': tx['to_address'],
                        'timestamp': tx['timestamp']
                    })
            
            print(f"💾 Transactions saved to: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
            raise

def main():
    """Main function to fetch USDT transactions for the specified wallet and date range"""
    
    # Configuration
    wallet_address = "TRaa8yHXzJ5FoVqJNxzBFtbUNKaVwXx5E1"
    date_from = "2025-07-14"
    date_to = "2025-07-30"
    
    # Optional: Add your TronGrid API key here for higher rate limits
    # Get API key from: https://www.trongrid.io/
    api_key = None  # Replace with your API key: "your-api-key-here"
    
    print("🚀 Starting USDT Transaction Fetcher (TronGrid API)")
    print("=" * 60)
    
    try:
        # Initialize the fetcher
        fetcher = USDTTransactionFetcher(api_key=api_key)
        
        # Fetch USDT transactions
        transactions = fetcher.get_usdt_transactions(wallet_address, date_from, date_to)
        
        print("=" * 60)
        
        if not transactions:
            print("⚠️  No USDT transactions found for the specified criteria")
            print("\n🔍 This could mean:")
            print("  1. No USDT transactions occurred in this period")
            print("  2. The wallet address is incorrect")
            print("  3. All transaction amounts were 0")
            print("  4. Date range is outside transaction history")
            
            # Let's try to verify the wallet exists
            print(f"\n🔍 Let me check if this wallet has any TRC20 activity...")
            try:
                test_response = fetcher._make_request(f"accounts/{wallet_address}/transactions/trc20", {'limit': 1})
                if 'data' in test_response and test_response['data']:
                    print("✅ Wallet has TRC20 transactions, but none match your criteria")
                else:
                    print("❌ No TRC20 transactions found for this wallet at all")
            except:
                print("❌ Unable to verify wallet activity")
            
            return
        
        # Sort transactions by timestamp (newest first)
        transactions.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Summary statistics
        total_in = sum(tx['amount_usdt'] for tx in transactions if tx['amount_usdt'] > 0)
        total_out = sum(abs(tx['amount_usdt']) for tx in transactions if tx['amount_usdt'] < 0)
        net_change = total_in - total_out
        
        print("🎉 USDT Transaction Fetch Completed!")
        print("\n📊 SUMMARY:")
        print(f"  🏦 Wallet: {wallet_address}")
        print(f"  📅 Period: {date_from} to {date_to} (GMT+7)")
        print(f"  📝 Total transactions: {len(transactions)}")
        print(f"  📈 Total incoming: +{total_in:,.6f} USDT")
        print(f"  📉 Total outgoing: -{total_out:,.6f} USDT")
        print(f"  💰 Net change: {net_change:+,.6f} USDT")
        
        print("\n📋 TRANSACTION DETAILS:")
        print("-" * 90)
        print(f"{'Date (GMT+7)':<20} {'Dir':<3} {'Amount (USDT)':<15} {'Hash':<20} {'Counterparty':<20}")
        print("-" * 90)
        
        for tx in transactions:
            amount_str = f"{tx['amount_usdt']:+,.6f}"
            date_str = tx['date_gmt7'].split(' ')[0] + " " + tx['date_gmt7'].split(' ')[1]
            hash_short = tx['hash'][:16] + "..."
            
            # Show counterparty (the other address)
            if tx['direction'] == 'IN':
                counterparty = tx['from_address'][:8] + "..." + tx['from_address'][-4:]
            else:
                counterparty = tx['to_address'][:8] + "..." + tx['to_address'][-4:]
            
            print(f"{date_str:<20} {tx['direction']:<3} {amount_str:<15} {hash_short:<20} {counterparty:<20}")
        
        print("-" * 90)
        print(f"💡 Tip: Use TronGrid API key for faster processing (current: {'✅ Authenticated' if api_key else '❌ No API key'})")
        print(f"🔗 Get your free API key at: https://www.trongrid.io/")
        
        # Save to CSV
        csv_filename = fetcher.save_to_csv(transactions)
        print(f"📁 CSV file saved: {csv_filename}")
        
    except Exception as e:
        print(f"❌ Failed to fetch USDT transactions: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())