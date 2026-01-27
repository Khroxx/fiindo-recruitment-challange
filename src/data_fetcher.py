from config import API_BASE_URL, INDUSTRIES
from api import ApiClient

class DataFetcher:
    def __init__(self, client: ApiClient):
        self.client = client

    def fetch_symbols(self) -> list[str]:
        data = self.client.get(f"{API_BASE_URL}/api/v1/symbols")
        if isinstance(data, dict) and "symbols" in data:
            return data["symbols"]

    def fetch_industry_data(self, symbol: str) -> dict:
        general = self.client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")

        if general.get("industry") not in INDUSTRIES:
            return {}

        eod = self.client.get(f"{API_BASE_URL}/api/v1/eod/{symbol}")
        income = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/income")
        balance = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/balance")

        return {
            "symbol": symbol,
            "industry": general["industry"],
            "price": eod.get("close"),
            "income": income,
            "balance": balance,
        }
