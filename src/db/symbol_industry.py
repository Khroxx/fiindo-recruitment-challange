from sqlalchemy import Column, Integer, String, UniqueConstraint
from src.models import Base

class SymbolIndustry(Base):
    __tablename__ = "symbol_industries"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False, index=True)
    industry = Column(String, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", name="uq_symbol"),
    )

