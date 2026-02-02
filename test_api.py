from src.api import ApiClient
from src.config import API_BASE_URL
import json

client = ApiClient("Bari", "Sopa")

print("="*60)
print("Testing API Structure")
print("="*60)

# Test 1: Get all symbols
print("\n1. Testing /symbols endpoint:")
try:
    symbols_data = client.get(f"{API_BASE_URL}/api/v1/symbols")
    print(f"✓ Success!")
    print(f"Keys: {list(symbols_data.keys())}")
    print(f"First symbol: {symbols_data.get('symbols', [])[:3]}")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 2: Get general info for a known symbol
test_symbol = "AAPL"  # Apple - likely to have data
print(f"\n2. Testing /general/{test_symbol}:")
try:
    general = client.get(f"{API_BASE_URL}/api/v1/general/{test_symbol}")
    print(f"✓ Success!")
    print(f"Keys: {list(general.keys())}")
    print(f"Full response:\n{json.dumps(general, indent=2)[:500]}...")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 3: EOD data
print(f"\n3. Testing /eod/{test_symbol}:")
try:
    eod = client.get(f"{API_BASE_URL}/api/v1/eod/{test_symbol}")
    print(f"✓ Success!")
    print(f"Keys: {list(eod.keys())}")
    print(f"Sample: {str(eod)[:200]}...")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 4: Income data
print(f"\n4. Testing /financials/{test_symbol}/income:")
try:
    income = client.get(f"{API_BASE_URL}/api/v1/financials/{test_symbol}/income")
    print(f"✓ Success!")
    print(f"Keys: {list(income.keys())}")
    print(f"Sample: {str(income)[:200]}...")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 5: Balance data
print(f"\n5. Testing /financials/{test_symbol}/balance:")
try:
    balance = client.get(f"{API_BASE_URL}/api/v1/financials/{test_symbol}/balance")
    print(f"✓ Success!")
    print(f"Keys: {list(balance.keys())}")
    print(f"Sample: {str(balance)[:200]}...")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "="*60)