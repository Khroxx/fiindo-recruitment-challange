from api import ApiClient
from data_fetcher import DataFetcher, ETL
from config import API_BASE_URL
from calculations import pe_ratio, revenue_growth, net_income_ttm, debt_ratio

def main():
    client = ApiClient("Bari", "Sopa")
    etl = ETL(client)
    health = client.get(f"{API_BASE_URL}/health")

    processed_data = etl.run()

    for item in processed_data[:2]:
        print(item)


    print(f"Processed {len(processed_data)} symbols")
    print("Health:", health)
    #print(f"Total symbols: {len(symbols)}")

if __name__ == "__main__":
    main()
