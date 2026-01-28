from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.symbol_industry import SymbolIndustry
from src.data_fetcher import DataFetcher
from src.config import INDUSTRIES, API_BASE_URL
from src.api import ApiClient

def load_symbol_industries(limit: int):
    client = ApiClient(first_name="John", last_name="Doe")
    fetcher = DataFetcher(client)
    symbols = fetcher.fetch_symbols()[:limit]

    session: Session = SessionLocal()
    inserted = 0

    for symbol in symbols:
        general = fetcher.client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")

        profile = (
            general.get("fundamentals", {})
            .get("profile", {})
            .get("data", [])
        )
        if not profile:
            continue

        industry = profile[0].get("industry")
        if industry not in INDUSTRIES:
            continue

        session.add(SymbolIndustry(symbol=symbol, industry=industry))
        inserted += 1

    session.commit()
    session.close()
    print(f"Inserted {inserted} symbols")

if __name__ == "__main__":
    load_symbol_industries(limit=14000)

