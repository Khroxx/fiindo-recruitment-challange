import time
from urllib.error import HTTPError
from src.api import ApiClient
from src.data_fetcher import ETL, DataFetcher
from src.config import API_BASE_URL, INDUSTRIES
from src.db.session import SessionLocal
from src.db.symbol_industry import SymbolIndustry
from sqlalchemy.orm import Session

def load_required_symbols():
    client = ApiClient("Bari", "Sopa")
    #fetcher = DataFetcher(client)
    #symbols = fetcher.fetch_symbols()
    session: Session = SessionLocal()
    existing_count = session.query(SymbolIndustry).count()

    if existing_count > 0:
        session.close()
        return
    
    print("Fetching all symbols...")
    data = client.get(f"{API_BASE_URL}/api/v1/symbols")
    symbols = data.get("symbols", [])

    print(f"Got {len(symbols)} symbols, filtering industries...")
    
    inserted = 0

    for symbol in symbols:
        session.add(SymbolIndustry(symbol=symbol, industry="UNKNOWN"))
        inserted += 1
        
        if inserted % 1000 == 0:
            session.commit()
            print(f"Stored {inserted}/{len(symbols)} symbols...")

    session.commit()
    session.close()
    #health = client.get(f"{API_BASE_URL}/health")

    #processed_data = etl.run()

    #for item in processed_data[:2]:
    #    print(item)


    #print(f"Processed {len(processed_data)} symbols")
    #print("Health:", health)
    #print(f"Total symbols: {len(symbols)}")

    print(f"Loading complete!")
    print(f"Stored {inserted} symbols for processing")

def main():
    load_required_symbols()

    client = ApiClient("Bari", "Sopa")
    etl = ETL(client)

    print("Starting ETL process...")
    processed_data = etl.run()
    print(f"ETL Complete!")

    if processed_data:
        print("Sample data (first 2 records):")
        for item in processed_data[:2]:
            print(f"  {item['symbol']}: PE={item['pe_ratio']}, Rev Growth={item['revenue_growth']}")

    print(f"Proxessed {len(processed_data)} symbols") 


if __name__ == "__main__":
    main()
