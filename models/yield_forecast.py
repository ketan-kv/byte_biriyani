from __future__ import annotations

import pandas as pd


def forecast_yield_simple(prod_df: pd.DataFrame, periods: int = 7) -> pd.DataFrame:
    if prod_df.empty:
        return pd.DataFrame(columns=["ds", "yhat"])
    working = prod_df.copy()
    working["log_date"] = pd.to_datetime(working["log_date"], errors="coerce")
    working = working.dropna(subset=["log_date", "yield_tonnes"]).sort_values("log_date")
    if working.empty:
        return pd.DataFrame(columns=["ds", "yhat"])

    daily = working.groupby("log_date", as_index=False)["yield_tonnes"].sum()
    baseline = daily["yield_tonnes"].tail(7).mean()
    future = [daily["log_date"].max() + pd.Timedelta(days=i) for i in range(1, periods + 1)]
    return pd.DataFrame({"ds": future, "yhat": baseline})
