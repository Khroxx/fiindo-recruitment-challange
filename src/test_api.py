from src.api import ApiClient
from src.config import API_BASE_URL
import json

client = ApiClient("Bari", "Sopa")

print("="*60)
print("Finding Working Symbols")
print("="*60)

# Get all symbols
symbols_data = client.get(f"{API_BASE_URL}/api/v1/symbols")
symbols = symbols_data.get("symbols", [])[:50]  # Test first 50

print(f"\nTesting first 50 symbols to find working ones...\n")

working_symbols = []

for symbol in symbols:
    try:
        general = client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")
        
        # Check what structure we get
        print(f"✓ {symbol} works!")
        print(f"  Keys: {list(general.keys())}")
        
        # Try to find industry
        industry = None
        if "industry" in general:
            industry = general.get("industry")
        elif "fundamentals" in general:
            fund = general.get("fundamentals", {})
            if "profile" in fund:
                profile = fund.get("profile", {})
                if isinstance(profile, dict) and "industry" in profile:
                    industry = profile.get("industry")
                elif isinstance(profile, dict) and "data" in profile:
                    data = profile.get("data", [])
                    if data:
                        industry = data[0].get("industry")
        
        print(f"  Industry: {industry}")
        print(f"  Sample: {str(general)[:150]}...\n")
        
        working_symbols.append({
            "symbol": symbol,
            "industry": industry,
            "structure": list(general.keys())
        })
        
        if len(working_symbols) >= 5:  # Stop after finding 5 working symbols
            break
            
    except Exception as e:
        # Skip broken symbols silently
        pass

print(f"\n{'='*60}")
print(f"Found {len(working_symbols)} working symbols:")
for ws in working_symbols:
    print(f"  • {ws['symbol']}: {ws['industry']}")
    print(f"    Structure: {ws['structure']}")
print(f"{'='*60}")