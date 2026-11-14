"""Small deterministic synthetic Taiwan market used by the rule tests."""
import numpy as np
import pandas as pd

AS_OF = pd.Timestamp("2026-11-13")  # a Friday


def make_market(sessions: int = 330, names: int = 40, seed: int = 7, end: pd.Timestamp = AS_OF):
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(end=end, periods=sessions)
    codes = [str(1100 + i) for i in range(names)]
    price_rows, inst_rows, margin_rows = [], [], []
    for code in codes:
        close = 100 * np.exp(np.cumsum(rng.normal(0, 0.012, sessions)))
        vol = 10_000 * rng.uniform(0.8, 1.2, sessions)
        margin_bal = 10_000 * np.exp(np.cumsum(rng.normal(0, 0.02, sessions)))
        short_ratio = rng.uniform(0, 40, sessions)
        for i, day in enumerate(days):
            price_rows.append([day, code, close[i], vol[i]])
            foreign, trust = rng.normal(0, 1_500_000, 2)
            inst_rows.append([day, code, foreign, trust, foreign + trust + rng.normal(0, 400_000)])
            margin_rows.append([day, code, margin_bal[i], short_ratio[i]])
    fridays = [d for d in days if d.weekday() == 4]
    tdcc_rows = []
    for code in codes:
        big = 50 + np.cumsum(rng.normal(0, 1.0, len(fridays)))
        for friday, value in zip(fridays, big):
            tdcc_rows.append([friday, code, value])
    return {
        "price": pd.DataFrame(price_rows, columns=["date", "code", "close", "vol"]),
        "inst": pd.DataFrame(inst_rows, columns=["date", "code", "foreign", "trust", "total"]),
        "margin": pd.DataFrame(margin_rows, columns=["date", "code", "margin_bal", "short_ratio"]),
        "tdcc": pd.DataFrame(tdcc_rows, columns=["date", "code", "big_pct"]),
        "days": days,
        "codes": codes,
    }


def truncate(market: dict, last_day: pd.Timestamp, first_day: pd.Timestamp | None = None) -> dict:
    """The data a job would see on ``last_day`` (and not before ``first_day``)."""
    out = dict(market)
    for key in ("price", "inst", "margin", "tdcc"):
        frame = market[key]
        mask = frame["date"] <= last_day
        if first_day is not None:
            mask &= frame["date"] >= first_day
        out[key] = frame[mask].reset_index(drop=True)
    return out
