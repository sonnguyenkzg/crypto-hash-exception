#!/usr/bin/env python3
"""
Test USDT conversion for specific transaction hash
"""

import sys
from pathlib import Path
from decimal import Decimal, getcontext
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import setup_logging

logger = setup_logging()
getcontext().prec = 28

def test_usdt_conversion():
    """Test USDT conversion for the specific transaction"""
    
    # Transaction details from TronScan
    tx_hash = "7e62e7f03c6bcf0aa8a4c22f0f1ba8a200cef2b2984dc2882c9257846d45482e"
    expected_usdt_amount = 20000  # 20,000 USDT
    timestamp = int(datetime(2025, 7, 16, 19, 26, 12).timestamp() * 1000)  # 2025-07-16 19:26:12
    
    print(f"Testing USDT conversion for transaction: {tx_hash}")
    print(f"Expected amount: {expected_usdt_amount:,} USDT")
    print(f"Transaction timestamp: {timestamp}")
    print(f"Transaction date: {datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Test different possible raw amounts that could represent 20,000 USDT
    test_scenarios = [
        {
            "description": "20,000 USDT with 6 decimals (standard USDT format)",
            "raw_amount": "20000000000",  # 20,000 * 10^6
            "token_symbol": "USDT",
            "decimals": 6
        },
        {
            "description": "20,000 USDT with 8 decimals (Bitcoin-style)",
            "raw_amount": "2000000000000",  # 20,000 * 10^8
            "token_symbol": "USDT", 
            "decimals": 8
        },
        {
            "description": "20,000 USDT with 18 decimals (Ethereum-style)",
            "raw_amount": "20000000000000000000000",  # 20,000 * 10^18
            "token_symbol": "USDT",
            "decimals": 18
        },
        {
            "description": "Raw amount exactly as shown: 20000",
            "raw_amount": "20000",
            "token_symbol": "USDT",
            "decimals": 0
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n📊 {scenario['description']}")
        print(f"   Raw amount: {scenario['raw_amount']}")
        print(f"   Token: {scenario['token_symbol']}")
        print(f"   Decimals: {scenario['decimals']}")
        
        try:
            # Convert raw amount to readable format
            raw_decimal = Decimal(scenario['raw_amount'])
            divisor = Decimal(10) ** scenario['decimals']
            formatted_amount = raw_decimal / divisor
            
            # USDT should always be 1:1 with USD
            token_price = 1.0
            usdt_value = float(formatted_amount) * token_price
            
            print(f"   ✅ Formatted amount: {formatted_amount}")
            print(f"   ✅ USDT value: ${usdt_value:,.6f}")
            
            # Check if this matches expected amount
            if abs(usdt_value - expected_usdt_amount) < 0.01:
                print(f"   🎉 MATCH! This conversion gives us the expected {expected_usdt_amount:,} USDT")
            else:
                print(f"   ❌ No match. Expected {expected_usdt_amount:,}, got {usdt_value:,.6f}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "="*60)
    print("🔍 USDT Token Information:")
    print("   - Contract Address: TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
    print("   - Standard Decimals: 6 (most common for USDT on Tron)")
    print("   - Price: Always $1.00 USD (by design)")
    print("\n💡 Based on TronScan showing 20,000 USDT, the raw amount should be:")
    print("   20,000 * 10^6 = 20,000,000,000 (with 6 decimals)")

def test_with_tronscan_api():
    """Test by making actual API call to TronScan"""
    import requests
    import time
    
    print("\n" + "="*60)
    print("🌐 Testing with actual TronScan API call...")
    
    # TronScan API endpoint for single transaction
    tx_hash = "7e62e7f03c6bcf0aa8a4c22f0f1ba8a200cef2b2984dc2882c9257846d45482e"
    url = f"https://apilist.tronscan.org/api/transaction-info"
    
    params = {"hash": tx_hash}
    
    try:
        time.sleep(1)  # Rate limiting
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"✅ API Response received")
        print(f"📄 Raw response keys: {list(data.keys())}")
        
        # Look for amount/value fields
        if 'amount' in data:
            print(f"   Amount field: {data['amount']}")
        if 'value' in data:
            print(f"   Value field: {data['value']}")
        if 'contractData' in data:
            print(f"   Contract data: {data['contractData']}")
        if 'tokenInfo' in data:
            print(f"   Token info: {data['tokenInfo']}")
        
        # Print the full response for debugging
        print("\n📋 Full API Response:")
        import json
        print(json.dumps(data, indent=2)[:1000] + "..." if len(str(data)) > 1000 else json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ API call failed: {e}")
        print("   This might be due to API limits or network issues")

if __name__ == "__main__":
    test_usdt_conversion()
    test_with_tronscan_api()