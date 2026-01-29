from src.api import ApiClient
from src.data_fetcher import DataFetcher, ETL
from src.config import API_BASE_URL, INDUSTRIES
from src.db.session import SessionLocal
from src.db.symbol_industry import SymbolIndustry
from sqlalchemy.orm import Session

def load_required_symbols():
    client = ApiClient("Bari", "Sopa")
    fetcher = DataFetcher(client)
    symbols = fetcher.fetch_symbols()
    session: Session = SessionLocal()
    existing_count = session.query(SymbolIndustry).count()
    #etl = ETL(client)
    
    if existing_count > 0:
        print(f"Database already has {existing_count} symbols, skipping load")
        session.close()
        return

    inserted = 0

    for index, symbol in enumerate(symbols, 1):
        try:
            general = client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")
            profile = general.get("fundamentals", {}).get("profile", {}).get("data", [])
            
            if not profile:
                continue
            
            industry = profile[0].get("industry")
            if industry in INDUSTRIES:
                session.add(SymbolIndustry(symbol=symbol, industry=industry))
                inserted += 1
                print(f"Added {symbol} ({industry})")
            
            if index % 100 == 0:
                session.commit()
                print(f"Checked {index}/{len(symbols)} symbols, inserted {inserted}")
                
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue

    health = client.get(f"{API_BASE_URL}/health")

    #processed_data = etl.run()

    for item in processed_data[:2]:
        print(item)


    print(f"Processed {len(processed_data)} symbols")
    print("Health:", health)
    #print(f"Total symbols: {len(symbols)}")

def main():
    load_required_symbols()

    client = ApiClient("Bari", "Sopa")
    etl = ETL(client)

    processed_data = etl.run()
    print(f"Proxessed {len(processed_data)} symbols")


if __name__ == "__main__":
    main()
