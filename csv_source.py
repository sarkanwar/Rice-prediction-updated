
from __future__ import annotations
import pandas as pd

def load_price_csv(path: str, date_col: str = "Date", price_col: str = "Price", freq: str = "D") -> pd.Series:
    df = pd.read_csv(path)
    if date_col not in df.columns or price_col not in df.columns:
        raise ValueError(f"CSV must contain columns '{date_col}' and '{price_col}'. Found: {df.columns.tolist()}")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)
    s = df[price_col].astype(float).asfreq(freq).ffill()
    return s.rename("price")
