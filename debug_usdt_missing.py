#!/usr/bin/env python3
"""
Debug why USDT transactions are missing
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

import requests
import time
from datetime import datetime, timedelta
from decimal import Decimal

# Test the specific addresses and date range
def debug_missing_usdt():
    print("🔍 Debugging missing USDT transactions")
    print("="*60)
    
    # The addresses from your known transaction
    addresses = [
        "TARvAP993BSFBuQhjc8oG4gviskNDftB7Z",  # FROM address (hash sender)
        "TF2GVKwjVchpEWs1TonJW8yP6HAcvAvG93"   # TO address (hash receiver)
    ]
    
    # Date range: 2025-07-16 to 2025-07-23
    date_from = "2025-07-16"
    date_to = "2025-07-23"
    
    # Convert to timestamps (same logic as your script)
    start_timestamp = int(datetime.strptime(date_from, '%Y-%m-%d').timestamp() * 1000)
    end_timestamp = int((datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000)
    
    print(f"Date range: {date_from} to {date_to}")
    print(f"Start timestamp: {start_timestamp}")
    print(f"End timestamp: {end_timestamp}")
    print(f"Known transaction date: 2025-07-23 06:56:15 (timestamp: 1753253775000)")
    print(f"Is known transaction in range? {start_timestamp <= 1753253775000 <= end_timestamp}")
    print()
    
    # Test each address
    for i, address in enumerate(addresses, 1):
        print(f"🔍 Testing address {i}: {address}")
        
        url = "https://apilist.tronscan.org/api/transaction"
        params = {
            'address': address,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp,
            'start': 0,
            'limit': 50,
            'sort': '-timestamp'
        }
        
        try:
            time.sleep(1)
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'data' not in data:
                print(f"❌ No data field in response")
                continue
                
            transactions = data['data']
            print(f"✅ Found {len(transactions)} transactions")
            
            usdt_count = 0
            target_hash = "1dad52d991ba6963777ae069276e01d67ba6e9786811739cb463b405c51a2213"
            
            for tx in transactions:
                tx_hash = tx.get('hash', '')
                timestamp = tx.get('timestamp', 0)
                date_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"   Transaction: {tx_hash[:16]}... on {date_str}")
                
                # Check if this is our target transaction
                if tx_hash == target_hash:
                    print(f"   🎯 FOUND TARGET TRANSACTION!")
                    
                    # Check for TRC20 transfers
                    if 'trc20TransferInfo' in tx and tx['trc20TransferInfo']:
                        print(f"   ✅ Has trc20TransferInfo: {len(tx['trc20TransferInfo'])} transfers")
                        for transfer in tx['trc20TransferInfo']:
                            symbol = transfer.get('symbol', '')
                            amount_str = transfer.get('amount_str', '0')
                            decimals = transfer.get('decimals', 6)
                            
                            if symbol.upper() == 'USDT' and amount_str != '0':
                                raw_decimal = Decimal(str(amount_str))
                                divisor = Decimal(10) ** decimals
                                usdt_amount = float(raw_decimal / divisor)
                                print(f"   💰 USDT Transfer: {usdt_amount:,.2f} USDT")
                                usdt_count += 1
                    else:
                        print(f"   ❌ No trc20TransferInfo found")
                
                # Check any transaction for USDT
                if 'trc20TransferInfo' in tx and tx['trc20TransferInfo']:
                    for transfer in tx['trc20TransferInfo']:
                        if transfer.get('symbol', '').upper() == 'USDT':
                            amount_str = transfer.get('amount_str', '0')
                            if amount_str != '0':
                                usdt_count += 1
                                break
            
            print(f"   📊 Total USDT transactions found: {usdt_count}")
            print()
            
        except Exception as e:
            print(f"❌ Error testing {address}: {e}")
            print()

if __name__ == "__main__":
    debug_missing_usdt()
