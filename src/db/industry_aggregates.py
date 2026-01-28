from sqlalchemy import Column, Integer, String, Float
from src.models import Base

class IndustryAggregates(Base):
    __tablename__ = "industry_aggregates"

    id = Column(Integer, primary_key=True)
    industry = Column(String, nullable=False, unique=True)
    avg_pe_ratio = Column(Float)
    avg_revenue_growth = Column(Float)
    sum_revenue = Column(Float)

