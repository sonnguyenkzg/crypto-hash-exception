#!/usr/bin/env python3
"""
Optimized TronScan Transaction Hash Checker
Usage: python hash_checker.py <transaction_hash>
Example: python hash_checker.py 1dad52d991ba6963777ae069276e01d67ba6e9786811739cb463b405c51a2213
"""

import requests
import json
import time
import sys
import argparse
from decimal import Decimal, getcontext
from datetime import datetime

# Set precision for decimal calculations
getcontext().prec = 28

def extract_trc20_transfers(tx_hash: str, verbose: bool = False):
    """Extract TRC20 transfer information from transaction hash"""
    
    if verbose:
        print(f"🔍 Checking transaction: {tx_hash}")
    
    # TronScan API endpoint
    url = "https://apilist.tronscan.org/api/transaction-info"
    params = {"hash": tx_hash}
    
    try:
        if verbose:
            print("🌐 Fetching data from TronScan API...")
        
        time.sleep(0.5)  # Rate limiting
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        # Check if transaction exists
        if not data or 'hash' not in data:
            if verbose:
                print("❌ Transaction not found")
            return None
        
        # Extract basic transaction info
        result = {
            'hash': tx_hash,
            'block': data.get('block', ''),
            'timestamp': data.get('timestamp', 0),
            'date': '',
            'status': 'SUCCESS' if data.get('confirmed') else 'FAILED',
            'contract_type': data.get('contractType', 0),
            'transfers': []
        }
        
        # Convert timestamp to readable date
        if result['timestamp']:
            result['date'] = datetime.fromtimestamp(result['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract TRC20 transfers
        if 'trc20TransferInfo' in data and data['trc20TransferInfo']:
            for transfer in data['trc20TransferInfo']:
                token_symbol = transfer.get('symbol', '').upper()
                amount_str = transfer.get('amount_str', '0')
                decimals = transfer.get('decimals', 6)
                
                # Convert amount
                formatted_amount = 0
                if amount_str and amount_str != '0':
                    try:
                        raw_decimal = Decimal(str(amount_str))
                        divisor = Decimal(10) ** decimals
                        formatted_amount = float(raw_decimal / divisor)
                    except:
                        formatted_amount = 0
                
                transfer_info = {
                    'token_symbol': token_symbol,
                    'token_name': transfer.get('name', ''),
                    'contract_address': transfer.get('contract_address', ''),
                    'amount_raw': amount_str,
                    'amount_formatted': formatted_amount,
                    'decimals': decimals,
                    'from_address': transfer.get('from_address', ''),
                    'to_address': transfer.get('to_address', ''),
                    'usdt_value': formatted_amount if token_symbol == 'USDT' else 0
                }
                
                result['transfers'].append(transfer_info)
        
        # Also check for TRX transfers
        trx_amount = data.get('amount', data.get('value', 0))
        if trx_amount and trx_amount != 0:
            trx_formatted = float(Decimal(str(trx_amount)) / Decimal(10**6))
            trx_transfer = {
                'token_symbol': 'TRX',
                'token_name': 'TRX',
                'contract_address': '',
                'amount_raw': str(trx_amount),
                'amount_formatted': trx_formatted,
                'decimals': 6,
                'from_address': data.get('ownerAddress', ''),
                'to_address': data.get('toAddress', ''),
                'usdt_value': 0  # Would need price lookup
            }
            result['transfers'].append(trx_transfer)
        
        return result
        
    except requests.RequestException as e:
        if verbose:
            print(f"❌ API request failed: {e}")
        return None
    except Exception as e:
        if verbose:
            print(f"❌ Error: {e}")
        return None

def print_table_format(results):
    """Print results in table format"""
    if not results:
        print("No results to display")
        return
    
    # Header
    print("="*120)
    print(f"{'Hash':<20} {'Date':<20} {'Token':<8} {'Amount':<15} {'USDT Value':<15} {'From':<20} {'To':<20}")
    print("="*120)
    
    for result in results:
        if result and result['transfers']:
            for transfer in result['transfers']:
                hash_short = result['hash'][:16] + "..."
                from_short = transfer['from_address'][:16] + "..." if transfer['from_address'] else ""
                to_short = transfer['to_address'][:16] + "..." if transfer['to_address'] else ""
                
                print(f"{hash_short:<20} {result['date']:<20} {transfer['token_symbol']:<8} "
                      f"{transfer['amount_formatted']:<15,.2f} {transfer['usdt_value']:<15,.2f} "
                      f"{from_short:<20} {to_short:<20}")
        else:
            hash_short = result['hash'][:16] + "..." if result else "Error"
            print(f"{hash_short:<20} {'N/A':<20} {'N/A':<8} {'0':<15} {'0':<15} {'N/A':<20} {'N/A':<20}")

def print_csv_format(results):
    """Print results in CSV format"""
    print("Hash,Date,Token,Amount,USDT_Value,From_Address,To_Address,Contract_Address")
    
    for result in results:
        if result and result['transfers']:
            for transfer in result['transfers']:
                print(f"{result['hash']},{result['date']},{transfer['token_symbol']},"
                      f"{transfer['amount_formatted']},{transfer['usdt_value']},"
                      f"{transfer['from_address']},{transfer['to_address']},{transfer['contract_address']}")
        else:
            hash_val = result['hash'] if result else "ERROR"
            print(f"{hash_val},N/A,N/A,0,0,N/A,N/A,N/A")

def print_json_format(results):
    """Print results in JSON format"""
    print(json.dumps(results, indent=2))

def main():
    parser = argparse.ArgumentParser(description='TronScan Transaction Hash Checker')
    parser.add_argument('hashes', nargs='+', help='Transaction hash(es) to check')
    parser.add_argument('--format', choices=['table', 'csv', 'json'], default='table',
                       help='Output format (default: table)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--usdt-only', action='store_true',
                       help='Show only USDT transfers')
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"🔍 Processing {len(args.hashes)} transaction hash(es)...")
    
    results = []
    
    for i, tx_hash in enumerate(args.hashes, 1):
        if args.verbose:
            print(f"\n--- Processing {i}/{len(args.hashes)}: {tx_hash} ---")
        
        result = extract_trc20_transfers(tx_hash, args.verbose)
        
        # Filter USDT only if requested
        if args.usdt_only and result and result['transfers']:
            result['transfers'] = [t for t in result['transfers'] if t['token_symbol'] == 'USDT']
        
        results.append(result)
        
        if args.verbose and result:
            print(f"✅ Found {len(result['transfers'])} transfer(s)")
            for transfer in result['transfers']:
                print(f"   {transfer['token_symbol']}: {transfer['amount_formatted']:,.2f}")
    
    # Output results
    if args.verbose:
        print(f"\n{'='*60}")
        print("RESULTS:")
        print(f"{'='*60}")
    
    if args.format == 'table':
        print_table_format(results)
    elif args.format == 'csv':
        print_csv_format(results)
    elif args.format == 'json':
        print_json_format(results)
    
    # Summary
    if args.verbose:
        total_usdt = sum(
            transfer['usdt_value'] 
            for result in results if result 
            for transfer in result['transfers']
        )
        print(f"\n💰 Total USDT Value: ${total_usdt:,.2f}")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python hash_checker.py <transaction_hash> [<hash2> <hash3> ...]")
        print("\nOptions:")
        print("  --format table|csv|json  Output format")
        print("  --verbose               Verbose output")
        print("  --usdt-only            Show only USDT transfers")
        print("\nExamples:")
        print("  python hash_checker.py 1dad52d991ba6963777ae069276e01d67ba6e9786811739cb463b405c51a2213")
        print("  python hash_checker.py hash1 hash2 hash3 --format csv")
        print("  python hash_checker.py hash1 --usdt-only --verbose")
        sys.exit(1)
    
    main()