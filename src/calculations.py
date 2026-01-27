def pe_ratio(price: float, earnings_last_quarter: float) -> float | None:
    if not earnings_last_quarter or earnings_last_quarter == 0:
        return None
    return price / earnings_last_quarter

def revenue_growth(revenue_q1: float, revenue_q2: float) -> float | None:
    if not revenue_q2 or revenue_q2 == 0:
        return None
    return (revenue_q1 - revenue_q2) / revenue_q2

def net_income_ttm(net_incomes_quarters: list[float]) -> float:
    return sum(net_incomes_quarters[-4:])

def debt_ratio(total_debt: float, equity: float) -> float | None:
    if not equity or equity == 0:
        return None
    return total_debt / equity
