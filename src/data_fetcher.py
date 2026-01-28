from urllib.error import HTTPError
from src.config import API_BASE_URL, INDUSTRIES
from src.api import ApiClient
from collections import defaultdict
from src.calculations import pe_ratio, revenue_growth, net_income_ttm, debt_ratio
from sqlalchemy.orm import Session
from sqlalchemy import func
from src.db.session import SessionLocal
from src.db.symbol_industry import SymbolIndustry
from src.db.symbol_stats import SymbolStats
from src.db.industry_aggregates import IndustryAggregates

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

class ETL:
    def __init__(self, client: ApiClient):
        self.client = client

    def process_symbol(self, symbol: str, industry: str) -> dict | None:
        try:
            eod = self.client.get(f"{API_BASE_URL}/api/v1/eod/{symbol}")
            income = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/income")
            balance = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/balance")

            pe = pe_ratio(eod.get("close", 0), income.get("eps", 0))
            rev_growth = revenue_growth(income.get("revenue_q1", 0), income.get("revenue_q2", 0))
            net_ttm = net_income_ttm(income.get("net_income_quarters", []))
            debt_rat = debt_ratio(balance.get("total_debt", 0), balance.get("equity", 0))

            return {
                "symbol": symbol,
                "industry": industry,
                "price": eod.get("close"),
                "pe_ratio": pe,
                "revenue_growth": rev_growth,
                "net_income_ttm": net_ttm,
                "debt_ratio": debt_rat,
            }

        except Exception as e:
            print(f"Skipping {symbol} due to error: {e}")
            return None

    def calculate_industry_aggregates(self):
        session: Session = SessionLocal()
        
        session.query(IndustryAggregates).delete()
        
        industries = session.query(SymbolStats.industry).distinct().all()
        
        for (industry,) in industries:
            stats = session.query(
                func.avg(SymbolStats.pe_ratio).label('avg_pe'),
                func.avg(SymbolStats.revenue_growth).label('avg_rev_growth'),
                func.sum(SymbolStats.net_income_ttm).label('sum_revenue')
            ).filter(SymbolStats.industry == industry).first()
            
            aggregate = IndustryAggregates(
                industry=industry,
                avg_pe_ratio=stats.avg_pe,
                avg_revenue_growth=stats.avg_rev_growth,
                sum_revenue=stats.sum_revenue
            )
            session.add(aggregate)
        
        session.commit()
        session.close()
        print("Industry aggregates calculated")

    def run(self):
        session: Session = SessionLocal()
        symbols = session.query(SymbolIndustry).all()
        results = []

        for index, s in enumerate(symbols, 1):
            data = self.process_symbol(s.symbol, s.industry)
            if data:
                results.append(data)
                stat = SymbolStats(**data)
                session.add(stat)

            if index % 20 == 0:
                session.commit()
                print(f"Processed {index}/{len(symbols)} symbols")

        session.commit()
        session.close()
        print(f"ETL done: {len(results)} symbols processed")
        
        self.calculate_industry_aggregates()

        return results