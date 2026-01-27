from api import ApiClient
from config import API_BASE_URL

def main():
    client = ApiClient("john", "doe")

    health = client.get(f"{API_BASE_URL}/health")
    print("Health:", health)

if __name__ == "__main__":
    main()
