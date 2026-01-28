from sqlalchemy import Column, Integer, String, Float
from models import Base

class SymbolStats(Base):
    __tablename__ = "symbol_stats"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True, nullable=False)
    industry = Column(String, index=True, nullable=False)

    pe_ratio = Column(Float)
    revenue_growth = Column(Float)
    net_income_ttm = Column(Float)
    debt_ratio = Column(Float)

