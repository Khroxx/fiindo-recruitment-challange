from urllib.error import HTTPError
from config import API_BASE_URL, INDUSTRIES
from api import ApiClient
from collections import defaultdict

class DataFetcher:
    def __init__(self, client: ApiClient):
        self.client = client

    def fetch_symbols(self) -> list[str]:
        data = self.client.get(f"{API_BASE_URL}/api/v1/symbols")
        if isinstance(data, dict) and "symbols" in data:
            return data["symbols"]

#    def fetch_symbols_by_industry(self, limit: int = 200) -> dict[str, list[str]]:
#        symbols = self.fetch_symbols()[:limit]
#
#        symbols_by_industry = {}
#
#        for symbol in symbols:
#            general = self.client.get(
#                f"{API_BASE_URL}/api/v1/general/{symbol}"
#            )
#
#            industry = general.get("industry")
#
#            if industry in INDUSTRIES:
#                symbols_by_industry.setdefault(industry, []).append(symbol)
#
#        return symbols_by_industry

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
