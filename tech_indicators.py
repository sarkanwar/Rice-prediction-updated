
from __future__ import annotations
import pandas as pd, numpy as np

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def rolling_features(s: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({'price': s})
    df['ret'] = df['price'].pct_change()
    for win in [3,7,14,30]:
        df[f'sma_{win}'] = df['price'].rolling(win).mean()
        df[f'ema_{win}'] = df['price'].ewm(span=win, adjust=False).mean()
        df[f'vol_{win}'] = df['ret'].rolling(win).std()
    df['rsi_14'] = rsi(df['price'], 14)
    for l in [1,2,3,7,14,30]:
        df[f'lag_{l}'] = df['price'].shift(l)
    return df
