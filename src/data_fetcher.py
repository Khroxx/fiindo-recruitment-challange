from urllib.error import HTTPError
from config import API_BASE_URL, INDUSTRIES
from api import ApiClient
from collections import defaultdict
from calculations import pe_ratio, revenue_growth, net_income_ttm, debt_ratio

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

class ETL:
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def process_symbol(self, symbol: str) -> dict| None:
        try:
            general = self.fetcher.client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")
            #print(symbol, general)

            profile_data = (
                general
                .get("fundamentals", {})
                .get("profile", {})
                .get("data", [])
            )
            if not profile_data:
                return None

            industry = profile_data[0].get ("industry")
            if industry not in INDUSTRIES:
                return None

            print(f"{symbol} → '{industry}'")

            eod = self.fetcher.client.get(f"{API_BASE_URL}/api/v1/eod/{symbol}")
            income = self.fetcher.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/income")
            balance = self.fetcher.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/balance")

            pe = pe_ratio(eod.get("close", 0), income.get("eps", 0))
            rev_growth = revenue_growth(income.get("revenue_q1", 0), income.get("revenue_q2", 0))
            net_ttm = net_income_ttm(income.get("net_income_quarters", []))
            debt_rat = debt_ratio(balance.get("total_debt", 0), balance.get("equity", 0))

            #print(eod, income, balance)
            #print(pe, rev_growth, net_ttm, debt_rat)

            return {
                "symbol": symbol,
                "industry": industry,
                "price": eod.get("close"),
                "pe_ratio": pe,
                "revenue_growth": rev_growth,
                "net_income_ttm": net_ttm,
                "debt_ratio": debt_rat,
            }

        except HTTPError as e:
            #print(f"Skipping {symbol} due to HTTP {e.code}")
            return None

        except Exception as e:
            #print(f"Skipping {symbol} due to error: {e}")
            return None

    def run(self, limit: int = 14000) -> list[dict]:
#        symbols = self.fetcher.fetch_symbols()[:limit]
#        result = []
#
#        for index, symbol in enumerate(symbols, start=1):
#            data = self.process_symbol(symbol)
#            if data:
#                result.append(data)
#            if index % 50 == 0:
#                print(f"Processed {index}/{len(symbols)} symbols", flush=True)
#
#       return result

        symbols = self.fetcher.fetch_symbols()

        for index, symbol in enumerate(symbols):
            general = self.fetcher.client.get(f"{API_BASE_URL}/api/v1/general/{symbol}")

            profile_data = (
                general.get("fundamentals", {})
                       .get("profile", {})
                       .get("data", [])
            )

            if not profile_data:
                continue

            industry = profile_data[0].get("industry")

            if industry in INDUSTRIES:
                print(f"[{index}] {symbol} → {industry}")
