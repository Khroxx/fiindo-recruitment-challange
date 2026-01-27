from api import ApiClient
from data_fetcher import DataFetcher
from config import API_BASE_URL
from calculations import pe_ratio, revenue_growth, net_income_ttm, debt_ratio

def main():
    client = ApiClient("john", "doe")
    fetcher = DataFetcher(client)
    symbols = fetcher.fetch_symbols()
    health = client.get(f"{API_BASE_URL}/health")

    print("Health:", health)
    #print(symbols)
    print(f"Total symbols: {len(symbols)}")
    #symbols_by_industry = fetcher.fetch_symbols_by_industry(limit=200)

    #for industry, symbols in symbols_by_industry.items():
        #print(f"\n{industry}: {len(symbols)}")

    print("PE Ratio:", pe_ratio(100, 5))
    print("Revenue Growth:", revenue_growth(120, 100))
    print("NetIncomeTTM:", net_income_ttm([10, 12, 11, 13]))
    print("Debt Ratio:", debt_ratio(50, 100))

if __name__ == "__main__":
    main()
