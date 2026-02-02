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

    def process_symbol(self, symbol: str) -> dict | None:
        try:
            eod = self.client.get(f"{API_BASE_URL}/api/v1/eod/{symbol}")
            income_response = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/income")
            balance_response = self.client.get(f"{API_BASE_URL}/api/v1/financials/{symbol}/balance")

            if self._debug_count < 2:
                print(f"\n{'='*60}")
                print(f"DEBUG: API Response for {symbol}")
                print(f"{'='*60}")
                print(f"EOD keys: {list(eod.keys())}")
                print(f"Income keys: {list(income_response.keys())}")
                print(f"Balance keys: {list(balance_response.keys())}")
                
                industry_found = None
                if "industry" in income_response:
                    industry_found = income_response.get("industry")
                elif "fundamentals" in income_response:
                    fund = income_response.get("fundamentals", {})
                    if "general_data" in fund:
                        industry_found = fund.get("general_data", {}).get("industry")
                    elif "profile" in fund:
                        industry_found = fund.get("profile", {}).get("industry")
                
                print(f"Industry found: {industry_found}")
                print(f"{'='*60}\n")
                self._debug_count += 1

            industry = None
            
            for response in [income_response, balance_response]:
                if "industry" in response:
                    industry = response.get("industry")
                    break
                elif "fundamentals" in response:
                    fund = response.get("fundamentals", {})
                    if "general_data" in fund:
                        industry = fund.get("general_data", {}).get("industry")
                        break
                    elif "profile" in fund:
                        prof = fund.get("profile", {})
                        if isinstance(prof, dict) and "industry" in prof:
                            industry = prof.get("industry")
                            break
            
            if not industry or industry not in INDUSTRIES:
                return None

            eod_data = eod.get("data", [{}])[0] if isinstance(eod.get("data"), list) else eod
            income_data = income_response.get("fundamentals", {}).get("income_statement", {}).get("quarterly", {})
            balance_data = balance_response.get("fundamentals", {}).get("balance_sheet", {}).get("yearly", {})

            close_price = eod_data.get("close", 0)
            eps = income_data.get("eps", 0)
            
            total_revenue = income_data.get("totalRevenue", [])
            revenue_q1 = total_revenue[0] if isinstance(total_revenue, list) and len(total_revenue) > 0 else 0
            revenue_q2 = total_revenue[1] if isinstance(total_revenue, list) and len(total_revenue) > 1 else 0
            
            net_income_quarters = income_data.get("netIncome", []) if isinstance(income_data.get("netIncome"), list) else []
            
            total_debt_list = balance_data.get("totalDebt", [])
            total_debt = total_debt_list[0] if isinstance(total_debt_list, list) and total_debt_list else 0
            
            equity_list = balance_data.get("totalStockholderEquity", [])
            equity = equity_list[0] if isinstance(equity_list, list) and equity_list else 0

            pe = pe_ratio(close_price, eps)
            rev_growth = revenue_growth(revenue_q1, revenue_q2)
            net_ttm = net_income_ttm(net_income_quarters)
            debt_rat = debt_ratio(total_debt, equity)

            return {
                "symbol": symbol,
                "industry": industry,
                "price": close_price,
                "pe_ratio": pe,
                "revenue_growth": rev_growth,
                "net_income_ttm": net_ttm,
                "debt_ratio": debt_rat,
            }

        except HTTPError as e:
            # Silently skip symbols with API errors (500s)
            return None
        except Exception as e:
            if self._debug_count < 5:
                print(f"Error processing {symbol}: {type(e).__name__} - {e}")
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
        
        print(f"Processing {len(symbols)} symbols...")
        
        processed = 0
        skipped = 0

        for index, s in enumerate(symbols, 1):
            data = self.process_symbol(s.symbol)
            
            if data:
                results.append(data)
                stat = SymbolStats(**data)
                session.add(stat)
                
                s.industry = data['industry']
                
                processed += 1
                if processed <= 10 or processed % 10 == 0:
                    print(f"Processed {data['symbol']} ({data['industry']})")
            else:
                skipped += 1

            if index % 100 == 0:
                session.commit()
                print(f"\n[Progress: {index}/{len(symbols)}] Processed: {processed} | Skipped: {skipped}\n")

        session.commit()
        session.close()
        
        print(f"ETL Processing Complete")
        print(f"Processed: {processed} symbols")
        print(f"Skipped: {skipped} symbols")
        
        if processed > 0:
            self.calculate_industry_aggregates()

        return results