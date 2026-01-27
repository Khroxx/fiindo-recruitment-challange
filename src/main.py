from api import ApiClient
from data_fetcher import DataFetcher
from config import API_BASE_URL

def main():
    client = ApiClient("john", "doe")
    fetcher = DataFetcher(client)
    symbols = fetcher.fetch_symbols()
    print(symbols)
    print(f"Total symbols: {len(symbols)}")

    for symbol in symbols[:5]:
        data = fetcher.fetch_industry_data(symbol)
        if data:
            print(data["symbol"], data["industry"], data["price"])

    health = client.get(f"{API_BASE_URL}/health")
    print("Health:", health)

if __name__ == "__main__":
    main()
