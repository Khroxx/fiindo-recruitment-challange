from src.api import ApiClient
from src.data_fetcher import DataFetcher, ETL
from src.config import API_BASE_URL, INDUSTRIES
from src.db.session import SessionLocal
from src.db.symbol_industry import SymbolIndustry
from sqlalchemy.orm import Session

def load_required_symbols():
    client = ApiClient("Bari", "Sopa")
    session: Session = SessionLocal()
    existing_count = session.query(SymbolIndustry).count()

    if existing_count > 0:
        session.close()
        return
    
    print("Fetching all symbols...")
    data = client.get(f"{API_BASE_URL}/api/v1/symbols")
    symbols = data.get("symbols", [])

    print(f"Got {len(symbols)} symbols, filtering for required industries...")
    
    inserted = 0
    skipped = 0

    #fetcher = DataFetcher(client)
    #symbols = fetcher.fetch_symbols()
    #etl = ETL(client)
    
    #if existing_count > 0:
    #    print(f"Database already has {existing_count} symbols, skipping load")
    #    session.close()
    #    return

    #inserted = 0

    for index, symbol in enumerate(symbols, 1):
        try:
            general = client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")
            profile = general.get("fundamentals", {}).get("profile", {}).get("data", [])
            
            if not profile:
                skipped += 1
                continue
            
            industry = profile[0].get("industry")
            if industry not in INDUSTRIES:
                skipped += 1
                print(f"Skipped {symbol} ({industry})")
                continue

            session.add(SymbolIndustry(symbol=symbol, industry=industry))
            inserted += 1
            print(f"Added {symbol} ({industry})")
            
            #if industry in INDUSTRIES:
            #    session.add(SymbolIndustry(symbol=symbol, industry=industry))
            #    inserted += 1
            #    print(f"Added {symbol} ({industry})")
            
            if index % 100 == 0:
                session.commit()
                print(f"Progress: {index}/{len(symbols)} | Added: {inserted} | Skipped: {skipped}")
                
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            skipped += 1
            continue

    session.commit()
    session.close()
    health = client.get(f"{API_BASE_URL}/health")

    #processed_data = etl.run()

    #for item in processed_data[:2]:
    #    print(item)


    #print(f"Processed {len(processed_data)} symbols")
    print("Health:", health)
    #print(f"Total symbols: {len(symbols)}")

def main():
    load_required_symbols()

    client = ApiClient("Bari", "Sopa")
    etl = ETL(client)

    processed_data = etl.run()
    if processed_data:
        print("Sample data (first 2 records):")
        for item in processed_data[:2]:
            print(f"  {item['symbol']}: PE={item['pe_ratio']}, Rev Growth={item['revenue_growth']}")

    print(f"Proxessed {len(processed_data)} symbols") 


if __name__ == "__main__":
    main()
