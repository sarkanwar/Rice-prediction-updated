
from __future__ import annotations
import pandas as pd, yfinance as yf
from datetime import datetime, timedelta

def fetch_yf(ticker: str, lookback_days: int = 365) -> pd.Series:
    end = datetime.utcnow(); start = end - timedelta(days=lookback_days + 10)
    data = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    if data is None or data.empty: return pd.Series(dtype=float)
    s = data['Close'].copy(); s.name = ticker
    return s.asfreq('D').ffill()
